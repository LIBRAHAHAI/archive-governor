# OPERATIONS — archive-governor 运行手册

## 1. CLI 速查

### 1.1 主控(session_governor.py)

```bash
python scripts/session_governor.py ensure-schema --db <state.db>
python scripts/session_governor.py archive     --db <state.db> --session <id> [--profile p] [--archive-root dir]
python scripts/session_governor.py restore     --db <state.db> --session <id> [--archive-root dir]
python scripts/session_governor.py verify      --db <state.db> --session <id> [--archive-root dir]
python scripts/session_governor.py threshold   --db <state.db> [--limit-mb 60]   # ← L0 只读检测
python scripts/session_governor.py status      --db <state.db>                  # ← 库概览
```

### 1.2 检测器(governance_detector.py)

```bash
python scripts/governance_detector.py \
    --db <state.db> \
    --profile <p> \
    [--threshold-mb 60]    # override 阈值(默认按 profile_overrides)
    [--apply]              # 实际写 dedup_mark(默认 dry-run)
    [--json]               # 机器可读输出
    [--no-l0] [--no-l1]    # 跳过某层
```

### 1.3 回收执行器(governance_reclaim_run.py)

```bash
python scripts/governance_reclaim_run.py \
    --db <state.db> \
    --profile <p> \
    [--threshold-mb 80]    # 阈值
    [--dry-run]            # ← 推荐先跑,看候选清单
```

### 1.4 检索(governance_search.py)

```bash
python scripts/governance_search.py search --archived "关键词"   # 冷库 FTS
python scripts/governance_search.py search "关键词"              # 热库 FTS
python scripts/governance_search.py load --session <id>         # load 冷包回热库
```

### 1.5 启动自检(v3.1 新增, governance_startup_check.py)

```bash
python scripts/governance_startup_check.py \
    --profile <p> \
    --db <state.db> \
    [--verbose]            # 打印每个步骤
    [--report-json]        # 机器可读 JSON 报告
    [--threshold-mb 80]    # 覆盖阈值(只影响本次调用, 不改文件)
    [--target-ratio 0.85]  # 覆盖滞后带比例(只影响本次调用)
```

退出码:`0` 无需归档 / `1` 已归档完成 / `2` 归档后仍超天花板

> **改默认阈值**: 编辑 `governance_detector.py` 第 33-35 行常量
> (`DEFAULT_THRESHOLD_MB` / `PROFILE_OVERRIDE_MB` / `LAG_RATIO`), 改后重启。
> `governance.yaml` 的阈值字段当前仅作运维文档, 不被代码读取(v3.2 计划加 loader)。

## 2. 配置(config/governance.yaml)

```yaml
governance:
  storage_threshold_mb: 60
  storage_ceiling_mb: 80
  profile_overrides:
    default: 80
  target_ratio: 0.85              # ← v3.1 滞后带 15%
  trigger:
    mode: startup                # ← v3.1 启动自检
    enabled: true
  archive_root: "<archive_root>"
```

> `target_ratio` 取值范围 [0.5, 0.95];阈值 × (1 - target_ratio) 为允许超阈值空间。
> v3 默认 target_ratio=0.9(10% 滞后带);v3.1 改为 0.85(15%)。

## 3. 阈值与目标(v3.1)

| Profile | 阈值 | 目标 (阈值 × 0.85) | 天花板 | 路径 |
|---|---|---|---|---|
| **default** | 80 MB | **≤ 68 MB** | 80 MB | <archive_root>/state/sessions.db |
| hermes | 60 MB | ≤ 51 MB | 80 MB | (同上,共享库) |
| athena | 60 MB | ≤ 51 MB | 80 MB | (同上) |
| xuanwu | 60 MB | ≤ 51 MB | 80 MB | (同上) |
| duanmu | 60 MB | ≤ 51 MB | 80 MB | (同上) |

> ⚠️ **2026-08-08 实测**:生产库 `<archive_root>/state/sessions.db` 当前 **0 字节空文件**,实际生产库路径待 hermes 定位(task `t_434552e2`)。

## 4. 启动自检接入

### 4.1 auto_start_agents.py 集成

启动任一 agent 前调:

```python
# 在 start_agents() 中,调 /start-agent API 前插入:
import subprocess
result = subprocess.run([
    "python", "scripts/governance_startup_check.py",
    "--profile", agent_id,
    "--db", REAL_DB_PATH,
], capture_output=True)
# exit 1 不阻塞启动,仅写日志
```

### 4.2 systemd / cron 兜底(可选)

```ini
# /etc/cron.d/archive-governor-watchdog (Linux)
*/30 * * * * cd /d/AItools/Project/archive-governor && \
    python scripts/governance_startup_check.py \
        --profile default --db <real_path> \
    >> logs/watchdog.log 2>&1
```

> Windows:用 cronjob(action='create', schedule='*/30m', script=...)

## 5. 紧急恢复

### 5.1 单会话 restore

```bash
python scripts/session_governor.py restore \
    --db <state.db> \
    --session <id> \
    --archive-root "<archive_root>"
```

> restore 拒绝覆盖热库已有会话(AC 设计)。

### 5.2 批量 restore

```python
from governance_search import load_archive
load_archive(session_id_list, db=<state.db>)
```

### 5.3 备份策略

- 每次 archive 前,`governance_archiver.py` 自动写 `state.db.bak-mvp<N>-<timestamp>.db`
- 备份目录:`<archive_root>/<profile>/backups/`
- 保留:最近 5 次自动备份 + 人工备份不清理

## 6. 监控指标

| 指标 | 来源 | 告警阈值 |
|---|---|---|
| `state.db size` | `governance.status` | > 阈值 × 0.85 |
| `cold_archived count` | `governance_archive_index` | > 1000 时检索慢 |
| `restore 失败率` | `governance_log` | > 0% |
| `归档后剩余空间` | L0 检测 | < 阈值 × 0.5 |

## 7. 故障排查

| 症状 | 可能原因 | 排查 |
|---|---|---|
| `L0 threshold check` 报 db lock | room_server 持锁 | 排查 process holding state.db |
| archive 后 verify 失败 | 磁盘满 / 权限 | `ls -la <archive_root>/<profile>/session-archive/` |
| 启动自检 exit=2 | 归档后仍超天花板 | 检查候选是否耗尽(全部都是活跃) |
| governance_search --archived 无结果 | FTS 索引未同步 | `python scripts/governance_archiver.py sync-fts --db <db>` |