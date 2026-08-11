# ACCEPTANCE — 验收清单

> xuanwu 验收标准。所有 v3 / v3.1 任务交付前必须勾选。

## 1. v3 验收(已通过, 2026-08-07)

### 1.1 MVP-1 (M1-M2 治理基础设施 + 冷归档)

来源:[exchange/330-xuanwu-session-summary-20260807.md](exchange/330-xuanwu-session-summary-20260807.md) (Hermes-Salon 仓库)

- [x] **AC-D1.1** 治理四表幂等创建 — `ensure-schema` 重复执行不报错(10/10 测试)
- [x] **AC-D1.2** 冷归档原子性 — .gz.tmp → os.replace → 删热行 事务一致
- [x] **AC-D1.3** restore round-trip sha256 一致(K2 验收项)
- [x] **AC-D1.4** 双层 sha256(文件级 + 内容级 manifest)
- [x] **AC-D1.5** 失败回滚(任一步失败 → 零数据变更)
- [x] **AC-D5.1** 全 SQL 参数化(无字符串拼接)
- [x] **AC-D5.3** 输入白名单正则 `^[A-Za-z0-9_-]+$`
- [x] **AC-D5.4** governance_log 无消息原文
- [x] **WAL** 模式开启 + checkpoint

### 1.2 MVP-2 (M3-M4 空间回收 + 精确去重)

来源:[MVP2-reclaim-report-20260807.txt](MVP2-reclaim-report-20260807.txt)

- [x] **M3 空间回收闭环** — 测试库实测 158MB → 68.97MB(default) / 82MB → 18.92MB(athena)
- [x] **M3 FTS 同步** — messages=messages_fts(default 11249/11249, athena 1118/1118)
- [x] **M3 不可变保护** — default 活跃 101 个 / 被归档 0;athena 活跃 13 个 / 被归档 0
- [x] **M4 L1 精确去重** — FPR=0%(仅 content sha256 全等 / session_key 全等)
- [x] **--apply 幂等** — 二次跑新增 0
- [x] **双库备份** — state.db.bak-mvp2-20260807-110451.db
- [x] **M3-M4 测试** — 8/8 单测过 + 1 复用 M1 测试
- [x] **跨模块联动** — governance_search --archived 命中归档会话(<3s, K4 达标)

### 1.3 MVP-3 (M5-M8 检索 + 安全 + Dry-run)

- [x] **M5 archive_index + FTS** — `governance_search.search --archived <kw>` <3s
- [x] **M6 不可变保护完整版** — 活跃/pinned/修复链 全保护
- [x] **M6 审计日志** — what/when/why/evidence 完整
- [x] **M7 安全基线** — 脱敏/白名单/权限/备份纳入 DUR-1
- [x] **M8 dry-run** — `governance_reclaim_run.py --dry-run` 输出候选清单
- [x] **测试** — 22/22 单测过

**v3 验收总览**:19 AC + 3 验收项 全过(xuanwu 拍板)

## 2. v3.1 验收(开发中, hermes task `t_434552e2`)

### 2.1 阈值与触发

- [ ] **滞后带 15%** — `LAG_RATIO = 0.85`,单测覆盖 default 80→68 / 60→51
- [ ] **阈值驱动** — 不依赖 cron;启动自检触发
- [ ] **启动自检钩子** — `governance_startup_check.py` 接入 `auto_start_agents.py`

### 2.2 生产化(关键阻塞)

- [ ] **生产 sessions.db 真实路径** — 写进 `config/governance.yaml`(非占位)
- [ ] **真库表结构匹配** — sessions/messages 字段 + profile_name 字段确认
- [ ] **真库实测** — 找到生产库后跑一遍 `governance_reclaim_run.py --dry-run`,输出候选清单

### 2.3 验收要求

xuanwu 验收前 hermes 必须交付:

1. **changed_files** 列表
2. **跑过的命令输出**(包括失败用例,不只是 happy path)
3. **治理日志截图**(确认 `operator=auto_startup` 条目)
4. **真库实测前后体积对比**

## 3. 验收执行清单(xuanwu 跑)

```bash
cd <project_root>

# 1. 静态检查:所有脚本 py_compile 通过
python -m py_compile scripts/*.py

# 2. 单元测试
python tests/test_governance_m1m2.py
python tests/test_governance_m3m4.py
python tests/test_governance_m5m8.py
# 预期: 41/41 通过

# 3. v3.1 滞后带 15% 验证
grep "LAG_RATIO = 0.85" scripts/governance_detector.py

# 4. 启动自检脚本存在 + 可独立运行
python scripts/governance_startup_check.py --help

# 5. config/governance.yaml 存在 + 字段完整
python -c "import yaml; print(yaml.safe_load(open('config/governance.yaml')))"

# 6. governance_log 含 operator=auto_startup 枚举
grep "auto_startup" scripts/governance_startup_check.py
```

## 4. 验收未通过的处理

| 现象 | 处置 |
|---|---|
| 单测挂 | xuanwu 写失败清单 → hermes 修 |
| 真库跑通但路径写死 | xuanwu 标"阻塞",要求改 YAML |
| 启动自检未集成 | xuanwu 标"未集成",等 hermes 重做 |
| 治理日志缺 operator 字段 | xuanwu 拒收,要求 AC-D5.4 重测 |

## 5. 验收通过标志

- [ ] 41/41 旧测 + v3.1 新测 全过
- [ ] 真库实测数据写入 [docs/PRODUCTION-RECLAIM-REPORT.md](PRODUCTION-RECLAIM-REPORT.md)
- [ ] config/governance.yaml 已含生产库路径
- [ ] auto_start_agents.py 已集成(交付方提供 diff)
- [ ] user 拍板(A/B/C/D)