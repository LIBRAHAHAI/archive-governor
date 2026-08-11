# archive-governor

> 会话冷归档治理工具 — 自动阈值触发 + 内容级整合，**零 LLM、零交互、零视图**。
> 当 SQLite 会话库超过每 profile 阈值时，自动把最旧的、已结束的、非活跃的会话原子冷归档到 `.json.gz`，让热库维持在工作阈值之下。

[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)

## 为什么需要它

Hermes Agent（及类似用 SQLite 存会话的 agent）的 `state.db` 会随会话累积持续膨胀：
- 热库过大 → 启动变慢、备份变慢、检索变慢
- 手动清理 → 容易误删活跃会话 / pinned 会话
- LLM 摘要压缩 → 有 token 成本且破坏原文

archive-governor 用**纯规则机械执行**解决：超阈值 → 自动归档最旧非活跃会话 → 内容守恒（零丢失，可检索可恢复）。

## 核心特性

| 特性 | 说明 |
|---|---|
| **零 token / 零 LLM** | 纯规则机械执行，不调用任何模型 |
| **阈值触发** | 库 ≥ 阈值 → 自动归档至 `阈值 × 0.85`（15% 滞后带） |
| **两态生命周期** | `active` ↔ `cold_archived`，归档可逆 |
| **内容守恒** | 归档前后消息总数一致，零数据丢失（sha256 双层校验） |
| **不可变保护** | 活跃 / pinned / 修复链中的会话不可被归档 |
| **独立检索索引** | 冷数据经 `governance_search.py` 检索，按需加载 |
| **零写入探测** | `detector` / `startup_check` 默认 dry-run（`mode=ro`），绝不写生产库 |

## 快速开始

### 环境要求

- Python ≥ 3.11
- PyYAML ≥ 6.0（`pip install pyyaml`）

### 安装

```bash
git clone <your-fork-url> archive-governor
cd archive-governor
pip install -e .            # 或: pip install pyyaml
```

### 配置

复制模板并修改：

```bash
cp config/governance.yaml config/governance.local.yaml   # 本地配置 (git 已忽略)
```

`config/governance.local.yaml` 是权威覆盖源（优先于模板）。核心项：

```yaml
governance:
  storage_threshold_mb: 60     # 每 profile 阈值 (MB)
  storage_ceiling_mb: 80       # 硬顶 (超过只告警不丢数据)
  profile_overrides:
    default: 80                # profile 特例
  target_ratio: 0.15           # 滞后带 (归档后 ≤ 阈值×0.85)
  archive_root: "/path/to/archives"   # 归档包存放根目录

production_db:
  paths: {}                    # 留空 = 按 HERMES_HOME 自动推导
```

**路径推导规则**（`scripts/governance_config.py`）：未显式配置时按 Hermes 目录布局推导：
- `default` → `$HERMES_HOME/state.db`
- 其他 profile → `$HERMES_HOME/profiles/<name>/state.db`
- `HERMES_HOME` 取环境变量，未设置时用默认布局（Windows: `~/AppData/Local/hermes`，POSIX: `~/.hermes`）

### 使用

```bash
# CLI 总览
python scripts/session_governor.py --help

# 阈值探测 (dry-run, 零写入)
python scripts/governance_detector.py --db <state.db> --profile default --json

# 启动自检 (dry-run)
python scripts/governance_startup_check.py

# 冷数据检索
python scripts/governance_search.py search --query "关键词"

# 全量 dry-run 报告
python scripts/governance_search.py dry-run
```

> ⚠️ 所有命令默认 **dry-run / 只读**。真实归档（`--apply`）仅在你确认阈值与候选集后手动执行。

### 测试

```bash
python tests/test_governance_b1_dryrun.py    # B1 dry-run 安全闸门
python tests/test_governance_m1m2.py         # 治理核心 (10 用例)
python tests/test_governance_m3m4.py         # 归档流 (8 用例)
python tests/test_governance_m5m8.py         # 检索+安全 (22 用例)
python tests/test_governance_v31_fixes.py    # v3.1 回归
```

全部测试使用临时库，**不碰任何生产数据**。

## 架构速览

```
scripts/
├── governance_config.py        # 阈值/路径单一事实源 (FIX-B3)
├── governance_detector.py      # L0 阈值 + L1 重复检测 (dry-run 默认)
├── governance_archiver.py      # 原子冷归档 (sha256 双层校验)
├── governance_reclaim_run.py   # 超限回收执行
├── governance_search.py        # 冷数据检索 + 安全自检
├── governance_startup_check.py # 启动自检
├── governance_status_server.py # 治理状态端点 (可选, 已退役)
└── session_governor.py         # 治理主入口 CLI
```

详细设计见 [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md)，运行手册见 [docs/OPERATIONS.md](docs/OPERATIONS.md)。

## 安全边界

- ✅ 生产库默认只读（`mode=ro` URI），`--apply` 才写
- ✅ SQL 全参数化，输入白名单 `^[A-Za-z0-9_-]+$`
- ✅ 审计日志不含消息原文
- ✅ 归档前自动备份，备份保留 5 份
- ✅ 任何异常 fail-open（绝不阻断 agent 启动）

## 不做什么（scope 边界）

- ❌ 不调用任何 LLM 做"语义整合"（零 token 原则）
- ❌ 不提供 Web UI / 交互面板（零视图原则）
- ❌ 不删除数据（只 `cold_archived`，restore 可逆）
- ❌ 不跨 profile 抵扣空间（各 profile 独立）
- ❌ 不动 room_server / room_client 业务逻辑

## License

[MIT](LICENSE) © 2026 白泽墨家 (BaiZe MoJia) & Hermes Agent

---

*由多 agent 协作体系（Hermes / Athena / Xuanwu / Duanmu）开发并验证。*
