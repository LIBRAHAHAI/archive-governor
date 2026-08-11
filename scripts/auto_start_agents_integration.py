"""auto_start_agents_integration.py — 与 auto_start_agents.py 对接的薄胶水层.

目的:
    5.15 startup_checklist (auto_start_agents.py) 调用此文件:
        from auto_start_agents_integration import run_startup_check
        code = run_startup_check(profile='athena', db_path='.../sessions.db')
    避免 startup_checklist 直接 import governance_startup_check (会拉起
    session_governor 整个依赖链 — 启动阶段可能 sqlite 还在初始化).

设计 (321 §3 + 2026-08-08 user 拍板):
  - 不修改 auto_start_agents.py
  - 仅暴露 run_startup_check(profile, db_path) -> int 三个返回:
        0 = OK (或已 reclaim)
        1 = 警告 (reclaim_partial / insufficient)
        2 = 致命 (db 缺失 / schema 失败)
  - 默认 verbose=False; startup_checklist 想看细节用 run_startup_check(.., verbose=True)
"""
from __future__ import annotations

import importlib
import os
import sys
from typing import Optional

# 让 governance 子脚本可被 import
_HERE = os.path.dirname(os.path.abspath(__file__))
_SCRIPT_DIR = os.path.join(_HERE)  # scripts 自身与集成同目录
if _SCRIPT_DIR not in sys.path:
    sys.path.insert(0, _SCRIPT_DIR)


def _get_startup_module():
    """惰性 import — 避免模块级副作用."""
    return importlib.import_module("governance_startup_check")


def run_startup_check(
    profile: str,
    db_path: str,
    verbose: bool = False,
    threshold_mb: Optional[int] = None,
    target_ratio: Optional[float] = None,
) -> int:
    """统一入口: 启动自检 (governance_startup_check.run_startup_check).

    返回值退出码:
      0 = OK  - 未超阈值 / 已 reclaim 达标
      1 = WARN - reclaim 不完全或 insufficient
      2 = FATAL - db 缺失 / schema 失败 / detect 失败

    Args:
        profile: profile 名 (默认 'default', 阈值 80; 其他 profile 60)
        db_path: state.db 完整路径
        verbose: True 时 stdout 打过程日志 (默认 False, 静默)
        threshold_mb: 可选, 覆盖阈值 (MB), 缺省用 detector 常量
        target_ratio: 可选, 覆盖滞后带比例, 缺省 0.85 (331 决策 1.3)
    """
    mod = _get_startup_module()
    rep = mod.run_startup_check(profile, db_path, verbose=verbose,
                                threshold_mb=threshold_mb,
                                target_ratio=target_ratio)

    action = rep.get("action", "")
    rec_ok = rep.get("reclaim_space_ok", False)
    errs = rep.get("errors", [])

    # 致命路径
    if action in {"skipped_no_db", "schema_failed", "detect_failed"}:
        sys.stderr.write(f"[auto-start-integration] FATAL {action}: {errs}\n")
        return 2

    # reclaim 相关 warn
    if action.startswith("reclaimed"):
        if action == "reclaimed":
            return 0
        if not rec_ok:
            sys.stderr.write(f"[auto-start-integration] WARN {action}: {errs[:3]}\n")
            return 1
        return 0

    # no_op / skipped / no_candidates → OK
    return 0


def health_summary(profile: str, db_path: str) -> Optional[dict]:
    """辅助函数: 给 startup_checklist 拿一份只读摘要 (不动数据).

    Returns:
        dict 或 None (致命错误时 None)
    """
    mod = _get_startup_module()
    return mod.run_startup_check(profile, db_path, verbose=False)


__all__ = ["run_startup_check", "health_summary"]


if __name__ == "__main__":
    # 直接运行这个集成文件时, 透传给 governance_startup_check.main
    mod = _get_startup_module()
    sys.exit(mod.main())
