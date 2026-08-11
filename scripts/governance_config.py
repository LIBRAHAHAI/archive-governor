#!/usr/bin/env python3
"""governance_config.py — archive-governor 阈值配置单一事实源 (FIX-B3, 2026-08-09).

B3 背景: session_governor.threshold / governance_detector / governance_startup_check /
governance_reclaim_run / governance_search 五处入口各自硬编码 60/80/0.85 常量,
曾致同一 default 生产库 threshold 报 60MB 而 detector 报 80MB, 数值口径打架
(docs/PRODUCTION-DB-DISCOVERY.md B3)。本模块收敛全部阈值常量与解析函数,
各入口只 `import governance_config as cfg`, 不再复制常量, 杜绝漂移。

语义约定 (2026-08-07/08 user 拍板):
  - DEFAULT_THRESHOLD_MB = 60  : 每 profile 默认阈值
  - PROFILE_OVERRIDE_MB        : profile 特例, default=80MB
  - LAG_RATIO = 0.85           : 保留比例 (target = threshold × 0.85, 15% 滞后带)
  - ACTIVE_WINDOW_S = 24h      : 活跃判定窗口 (24h 内有写入视为活跃, §6.3)

解析优先级 (resolve_threshold_mb):
  CLI 显式覆盖 (--threshold-mb / --limit-mb) > profile override > 默认值。
"""
from __future__ import annotations

import sqlite3
from typing import Optional

DEFAULT_THRESHOLD_MB = 60
PROFILE_OVERRIDE_MB = {"default": 80}
LAG_RATIO = 0.85
ACTIVE_WINDOW_S = 24 * 3600
# 硬顶 (仅告警不丢数据, 非归档阈值): 超过即停止/告警, 不删除任何会话 (AC-D2.2)。
# 独立于 PROFILE_OVERRIDE_MB — 阈值可调, 天花板是安全阀, 不得随之漂移。
STORAGE_CEILING_MB = 80

# ---------------------------------------------------------------- C3-A (2026-08-10)
# est_saved 全文本列估算 — 单一事实源 (detector / reclaim / startup_check 共用)。
# C3 实测: 旧 `content×0.9` 只算 content 列 (14.2MB), 漏 reasoning/reasoning_content/
# tool_calls/api_content (~35MB) 与 FTS 索引 (57.6MB, 占库 40%), 低估真实释放 12 倍。
MESSAGES_TEXT_COLS = (
    "content",
    "reasoning",
    "reasoning_content",
    "tool_calls",
    "api_content",
)
# 压缩系数: JSON 序列化+索引开销的保守估计 (C3 报告 §5 方向 A, 实测 ~0.9)。
# 注意: est_saved 只是报告参考量, 达标判断已改周期实测 (db_logical_used_bytes),
# 不再依赖 est 累加 — 见 governance_reclaim_run._reach_target / startup_check。
EST_COMPRESSION_RATIO = 0.9
# reclaim 达标判断周期: 每归档 N 个会话实测一次逻辑占用 (C3 报告 §5 方向 A 建议 20)。
TARGET_CHECK_INTERVAL = 20


def text_bytes_sum_expr(cur: sqlite3.Cursor, alias: str = "m") -> str:
    """生成 messages 全文本列字节和的 SQL 表达式 (动态检测列存在, 兼容旧 schema).

    例: ``COALESCE(SUM(COALESCE(LENGTH(m.content),0) + COALESCE(LENGTH(m.reasoning),0) + ...),0)``
    仅对 messages 表中实际存在的列求和 (测试 fixture 可能只有 content 列)。
    """
    exist = {r[1] for r in cur.execute("PRAGMA table_info(messages)")}
    cols = [c for c in MESSAGES_TEXT_COLS if c in exist]
    if not cols:
        return "COALESCE(SUM(0),0)"
    expr = " + ".join(f"COALESCE(LENGTH({alias}.{c}),0)" for c in cols)
    return f"COALESCE(SUM({expr}),0)"


def est_saved_bytes(text_bytes: int) -> int:
    """全文本列字节 → est_saved (压缩系数收敛于此, 单一事实源)."""
    return int(text_bytes * EST_COMPRESSION_RATIO)


def db_logical_used_bytes(db_path: str) -> int:
    """实测逻辑占用 = (page_count - freelist_count) × page_size.

    auto_vacuum=0 库 DELETE 释放页进入 freelist 但文件不缩, 裸 os.path.getsize
    在归档循环中不会变小 (C3 实验佐证)。逻辑占用等价于 VACUUM 后文件真实大小,
    是达标判断的唯一可靠实测口径 (C3 §3.2 曲线即此口径)。

    WAL 处理: 未 checkpoint 的 DELETE 不反映在主库 page_count/freelist 里
    (WAL 中是追加日志)。先 PRAGMA wal_checkpoint(TRUNCATE) 把 WAL 合并进主库
    并截断 (需写锁; --apply 分支本来就持有写权, 无并发写者时必成功), 再读
    PRAGMA → 逻辑占用精确反映真实释放。并发写者导致 TRUNCATE busy 时降级
    PASSIVE 并计入残留 WAL 大小 (保守偏高, 只会多归档不会少归档)。
    """
    con = sqlite3.connect(db_path, timeout=30)
    truncated = False
    try:
        try:
            row = con.execute("PRAGMA wal_checkpoint(TRUNCATE)").fetchone()
            truncated = bool(row) and row[0] == 0  # busy=0 → 成功
        except sqlite3.OperationalError:
            pass
        if not truncated:
            try:
                con.execute("PRAGMA wal_checkpoint(PASSIVE)")
            except sqlite3.OperationalError:
                pass
        cur = con.cursor()
        pc = cur.execute("PRAGMA page_count").fetchone()[0]
        fc = cur.execute("PRAGMA freelist_count").fetchone()[0]
        ps = cur.execute("PRAGMA page_size").fetchone()[0]
        used = (pc - fc) * ps
    finally:
        con.close()
    if not truncated:
        wal = db_path + "-wal"
        if os.path.exists(wal):
            used += os.path.getsize(wal)
    return used


def resolve_threshold_mb(profile: str, override_mb: Optional[float] = None) -> float:
    """统一阈值解析 (FIX-B3): 显式覆盖 > profile override > 默认值.

    - override_mb 传入 (如 --threshold-mb / --limit-mb) 时直接采用, 不做类型强制,
      保留 float 语义 (兼容 threshold_check(limit_mb=0.05) 旧调用);
    - 否则查 PROFILE_OVERRIDE_MB (default=80), 未命中用 DEFAULT_THRESHOLD_MB (60).
    """
    if override_mb is not None:
        return override_mb
    return PROFILE_OVERRIDE_MB.get(profile, DEFAULT_THRESHOLD_MB)


def retention_target_bytes(threshold_bytes: int, ratio: Optional[float] = None) -> int:
    """滞后带目标体积 (保留比例): threshold × ratio, 默认 LAG_RATIO=0.85.

    例: 80MB → 68MB; 60MB → 51MB (15% 滞后带, 2026-08-08 user 拍板)。
    """
    if ratio is None:
        ratio = LAG_RATIO
    return int(threshold_bytes * ratio)


# ---------------------------------------------------------------------------
# profile → state.db 路径推导 (2026-08-11 开源化, 消除硬编码内部路径)
# ---------------------------------------------------------------------------
# 历史: 各脚本硬编码 $HERMES_HOME/... 绝对路径,
#       既暴露机器结构, 也导致无法移植。现统一按 Hermes 目录布局推导:
#         default profile  → $HERMES_HOME/state.db
#         其他 profile     → $HERMES_HOME/profiles/<name>/state.db
#       HERMES_HOME 优先取环境变量, 其次取默认布局 (~/AppData/Local/hermes,
#       Windows) 或 ~/.hermes (POSIX)。config/governance.yaml 的
#       production_db.paths 仍是权威覆盖源 (status_server 用), 本函数是
#       无配置/无环境变量时的确定性兜底, 与 P3-1 插件 resolve_profile_db 同源。
import os as _os

DEFAULT_HERMES_HOME = _os.path.join(
    _os.path.expanduser("~"),
    "AppData", "Local", "hermes" if _os.name == "nt" else ".hermes",
)


def hermes_home() -> str:
    """解析 HERMES_HOME (环境变量优先, 其次默认布局)."""
    return _os.environ.get("HERMES_HOME") or DEFAULT_HERMES_HOME


def default_profile_db_paths() -> dict:
    """按 Hermes 目录布局推导 4 profile 的 state.db 路径 (无硬编码)."""
    home = hermes_home()
    return {
        "default": _os.path.join(home, "state.db"),
        "athena": _os.path.join(home, "profiles", "athena", "state.db"),
        "xuanwu": _os.path.join(home, "profiles", "xuanwu", "state.db"),
        "duanmu": _os.path.join(home, "profiles", "duanmu", "state.db"),
    }


def resolve_db_paths(yaml_paths: Optional[dict] = None) -> dict:
    """合并 yaml 权威路径 + 推导兜底 (yaml 缺失字段 → 推导值)."""
    paths = default_profile_db_paths()
    if yaml_paths and isinstance(yaml_paths, dict):
        for k in paths:
            if yaml_paths.get(k):
                paths[k] = str(yaml_paths[k])
    return paths
