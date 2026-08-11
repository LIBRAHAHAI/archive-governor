# CHANGELOG — archive-governor

## [Unreleased] v3.1 — 生产化改造(待发布, 331 决策后定版)

### 新增
- `scripts/governance_startup_check.py` — 启动钩子,自动检测 + 归档
- `config/governance.yaml` — 统一配置文件(阈值 / 滞后带 / 触发模式)
- `docs/` 正式文档(README/ARCHITECTURE/OPERATIONS/ACCEPTANCE/CHANGELOG)

### 变更
- `LAG_RATIO`: 0.9 → **0.85**(滞后带 10% → **15%**,user 拍板)
- `scripts/governance_reclaim_run.py`: `LAG_RATIO` 0.9 → **0.85**(2026-08-08 修复,
  与 governance_detector 保持一致; 此前独立跑 reclaim 会用 0.9 违反 user 拍板)
- 触发模式: cron 每日 → **启动自检**(user 拍板)
- 归档目标: 阈值 × 0.9 → **阈值 × 0.85**
  - default 80MB → ≤ 68MB
  - 其他 60MB → ≤ 51MB
- `governance_startup_check.py` CLI 新增 `--threshold-mb` / `--target-ratio`
  (只覆盖本次调用, 不改文件; 331 决策 1.3)
- `auto_start_agents_integration.py` 透传 `threshold_mb` / `target_ratio` 可选参数

### 任务
- 派给 hermes: `t_434552e2` — 定位生产 sessions.db + 实施 LAG_RATIO 0.85 + 启动自检
- xuanwu 验收(独立跑 + 看 db 路径 + 看治理日志)

### FIX-B3 (2026-08-10): 四 profile 阈值解析口径统一
- 新增 `scripts/governance_config.py` 单一事实源: `DEFAULT_THRESHOLD_MB` (60) /
  `PROFILE_OVERRIDE_MB` (`{"default": 80}`) / `LAG_RATIO` (0.85) /
  `ACTIVE_WINDOW_S` (24h) / `STORAGE_CEILING_MB` (80) + `resolve_threshold_mb(profile, override)`
- 五入口 (session_governor.threshold / detector / startup_check / reclaim / search)
  全部复用 `cfg.resolve_threshold_mb`, 禁止本地复制常量
- 修复 `governance_search.py` 漂移: `dry_run_profile` 默认 `lag_ratio=0.9` → `cfg.LAG_RATIO`
  (0.85), 汇总行 `threshold*0.9` → `threshold*cfg.LAG_RATIO`; 硬顶 `storage_ceiling_mb=80`
  两处复制收敛到 `cfg.STORAGE_CEILING_MB`
- `threshold` 子命令支持 `--profile` (缺省 default=80MB, 不得回落 60MB)
- 测试: `tests/test_governance_b3_thresholds.py` 14 项 (新增 search 入口 ratio 一致性 +
  storage_ceiling 单一事实源 + 源码级 0.9 防漂移)

### Known gaps(v3.2 backlog)
- governance.yaml 阈值/滞后带字段当前不被代码读取, 改阈值需改
  `scripts/governance_config.py` 常量后重启(331 决策 1.2 选 A; FIX-B3 收敛常量,
  不再散落各入口)
- 生产 sessions.db 实测 0 字节(hermes 未启动写入), 真库 127→68MB 场景待 hermes
  启动后 dry-run 验证(331 决策 1.1 选 A)
- synthetic db 需含 `sessions.message_count` 列(detector 查询依赖; 项目内
  tests/ 造库均有此列, 331 决策 1.5 确认设计稿 320:223 本就有该列)

## [2026-08-07] v3 — 完整实现(已发布)

### 完成度
- 19 AC + 3 验收项 全过(xuanwu 拍板)
- 41/41 单测过(独立 runner + pytest 双通道)
- 真库实测(测试副本):default 158MB→68.97MB / athena 82MB→18.92MB

### 模块
- M1: 治理四表 + WAL + 参数化(session_governor.py)
- M2: 冷归档原子流(sha256 双层 + 事务内 os.replace)
- M3: 空间回收(FTS 同步 + VACUUM)
- M4: L1 精确去重(FPR=0%)
- M5: archive_index + FTS + governance_search --archived
- M6: 不可变保护 + 审计日志
- M7: 安全基线(脱敏/白名单/权限/备份)
- M8: dry-run + 首次部署清单

### 设计稿
- `exchange/320-hermes-session-governor-design-v3.md`(v3 定稿)

## 立项

### [2026-08-08] 项目独立化

**决策**: 从 Hermes-Salon/scripts/ 拆出,在 `<project_root>/` 建独立项目。

**原因**(user 拍板):
- archive-governor 是通用产品(任何 SQLite 状态库 + 多 agent 架构都能用)
- 与 Hermes-Salon(具体业务:Room 多 agent 协作) 无强耦合
- 文档/发布/分享独立,避免业务代码污染
- 新建项目均按此规范,放 `<project_root>/`

**项目名由来**: "会话压缩归档"语义 → archive-governor(直白 + 与 v3 设计稿 `session_governor` 一脉相承)

**迁移内容**(2026-08-08 09:15):
- scripts/ — 5 个治理脚本
- tests/ — 3 个测试套件
- exchange/320-hermes-session-governor-design-v3.md — 设计原稿
- MVP2-reclaim-report-20260807.txt — 真库实测报告

**Hermes-Salon/scripts/ 原文件保留**(不破坏已通过验收的调用方)。