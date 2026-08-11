# ARCHITECTURE — archive-governor 架构总览

> 详细设计见 [exchange/320-hermes-session-governor-design-v3.md](../exchange/320-hermes-session-governor-design-v3.md)(本仓库交换文件)
> 本文档为产品级摘要 + v3.1 增量。

## 1. 核心理念

| 原则 | 实现 |
|---|---|
| 零 token | 不调任何 LLM;仅 SHA256 + 元数据匹配 |
| 阈值触发 | state.db ≥ 阈值 → 归档;不依赖 cron |
| 内容级整合 | 按会话粒度;同主题多会话不去重合并(v1.1 候选) |
| 硬归档永久留存 | cold_archived 不删,restore 可逆 |
| 60/80MB 阈值 | 每 profile 独立;default 特例 80MB |
| 零视图 | 不做 Web UI;配置 + 日志 + CLI |

## 2. 两态生命周期(v3 §5.2)

```
                    ┌─────────────────┐
                    │     active      │ ←────────┐
                    │  (热库可写)     │          │
                    └────────┬────────┘          │
                             │                   │ restore (operator=manual|auto)
                             │ 冷归档             │ (仅当活跃度满足恢复条件)
                             │ (L0 阈值触发)      │
                             ▼                   │
                    ┌─────────────────┐          │
                    │  cold_archived  │──────────┘
                    │ (.json.gz + 索引)│
                    └─────────────────┘
```

**准入 cold_archived**:
- `state='ended' OR ended_at IS NOT NULL`(已结束)
- 最后消息写入 ≥ 24h 前(僵尸会话)
- 未被 pinned / exempt
- 重复检查未命中(L1 sha256 全等除外)

**保留 active**(不可被归档):
- 最近 24h 有消息写入(活跃保护)
- `exempt=1`(pinned)
- 在修复链中(repair_chain 不为空)

## 3. 治理四表(SQLite Schema)

```sql
-- 治理元数据:每会话一行
CREATE TABLE governance_meta (
    session_id        TEXT PRIMARY KEY,
    profile_name      TEXT,
    state             TEXT NOT NULL CHECK (state IN ('active','cold_archived')),
    cold_archived_at  REAL,
    reason            TEXT,
    cluster_ids       TEXT,           -- JSON 数组,v1.1 主题聚类
    exempt            INTEGER DEFAULT 0,
    archive_file      TEXT,
    archive_sha256    TEXT,
    archive_size_bytes INTEGER,
    storage_saved_bytes INTEGER,
    created_at        REAL,
    updated_at        REAL
);

-- 治理审计:who/when/why/evidence
CREATE TABLE governance_log (
    id         INTEGER PRIMARY KEY AUTOINCREMENT,
    session_id TEXT,
    operator   TEXT NOT NULL,         -- auto|auto_startup|manual
    op         TEXT NOT NULL,         -- cold_archive|restore|dedup_mark|cluster_write|threshold_check
    reason     TEXT NOT NULL,         -- threshold|startup_threshold|dedup|manual
    evidence   TEXT,                  -- sha256/计数/规则匹配,无消息原文(AC-D5.4)
    bytes_before INTEGER,
    bytes_after  INTEGER,
    created_at REAL
);

-- 归档索引 + FTS(全文搜索)
CREATE TABLE governance_archive_index (
    archive_file   TEXT PRIMARY KEY,
    session_id     TEXT,
    profile_name   TEXT,
    archived_at    REAL,
    content_sha256 TEXT,
    file_size      INTEGER,
    msg_count      INTEGER,
    first_msg_at   REAL,
    last_msg_at    REAL
);
-- 配套 messages_fts(external-content 关联 messages)
```

## 4. 冷归档原子流(M2, AC-D1.2/3/4)

```
┌──────────────────────┐
│ 1. 选候选            │ L0 阈值检测 → over_limit → 选最旧会话
└──────────┬───────────┘
           ▼
┌──────────────────────┐
│ 2. 写事务外临时文件   │ .gz.tmp + manifest(content_sha256)
│    gzip 序列化        │ + 文件级 sha256(双层)
└──────────┬───────────┘
           ▼
┌──────────────────────┐
│ 3. 双校验            │ 文件级 sha256 == 计算值
│                      │ 解压比对 sha256 == manifest.content_sha256
└──────────┬───────────┘
           ▼
┌──────────────────────┐
│ 4. 事务内            │ os.replace .gz.tmp → .gz (原子)
│                      │ INSERT governance_archive_index
│                      │ UPDATE governance_meta state='cold_archived'
│                      │ DELETE FROM messages WHERE session_id=?
│                      │ COMMIT
└──────────┬───────────┘
           ▼
┌──────────────────────┐
│ 5. 失败回滚          │ ROLLBACK + 清理 .gz/.gz.tmp
│                      │ governance_log op=cold_archive, reason=rolled_back
└──────────────────────┘
```

**安全保证**:
- 任何步骤失败 → 零数据变更(AC-D1.5)
- AC-D5.1 全 SQL 参数化
- AC-D5.3 输入白名单(session_id / profile_name `^[A-Za-z0-9_-]+$`)
- AC-D5.4 governance_log 无消息原文
- AC-D5.5 WAL 模式 + checkpoint 后 VACUUM

## 5. 触发方式(v3.1 新增)

| 模式 | 何时 | 启用 |
|---|---|---|
| **startup** | 程序启动后自检(每个 agent 启动钩子) | ✅ v3.1 默认 |
| threshold_watch | 文件大小 ≥ 阈值时(后台 watchdog) | ⏳ v3.1 候选 |
| cron | 每日凌晨 | ❌ user 否决(非实时) |

**启动自检流程**(governance_startup_check.py):

```
agent 启动
   ↓
governance_startup_check.py --profile <p> --db <real_path>
   ↓
1. ensure-schema (幂等)
2. detect_threshold --apply (L0 + L1)
3. over_limit ? → 调 governance_reclaim_run 归档到 ≤ 阈值×0.85
4. 写 governance_log operator=auto_startup
5. exit 0/1/2
   ↓
启动继续(治理是 best-effort,不阻塞)
```

退出码语义:
- `0`:无需归档(库 ≤ 阈值)
- `1`:已归档完成(库 ≤ 阈值 × 0.85)
- `2`:归档后仍 > 80MB 天花板(告警,人工介入)

## 6. 模块清单

```
archive-governor/
├── scripts/
│   ├── session_governor.py          # 主控:ensure-schema/archive/restore/threshold
│   ├── governance_detector.py       # L0 阈值 + L1 精确去重
│   ├── governance_archiver.py       # VACUUM + FTS 同步
│   ├── governance_reclaim_run.py    # 真库回收执行器
│   ├── governance_search.py         # 冷库全文检索
│   └── governance_startup_check.py  # ← v3.1 新增,启动钩子
├── tests/
│   ├── test_governance_m1m2.py      # MVP-1:治理表+冷归档
│   ├── test_governance_m3m4.py      # MVP-2:空间回收+去重
│   └── test_governance_m5m8.py      # MVP-3:检索+安全+Dry-run
├── config/
│   └── governance.yaml              # ← v3.1 新增,统一配置
├── exchange/
│   └── 320-hermes-session-governor-design-v3.md  # 设计原稿
├── docs/
│   ├── README.md
│   ├── ARCHITECTURE.md              # 本文档
│   ├── OPERATIONS.md
│   ├── ACCEPTANCE.md
│   └── CHANGELOG.md
└── MVP2-reclaim-report-20260807.txt # 真库实测(MVP-2)
```

## 7. 已知边界(v3 不做)

- v1.1 主题聚类 / 向量召回(cosine ≥ 0.95):只生成候选,不入库
- L2 跨会话主题合并:不做
- 删除态(只 cold_archived,保留可逆)
- Web UI / 交互面板