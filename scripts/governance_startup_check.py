#!/usr/bin/env python3
"""governance_startup_check.py — archive-governor 启动自检 (v3.1, 2026-08-08)

用法:
    python governance_startup_check.py --profile <p> --db <state.db> [--verbose]
    python governance_startup_check.py --profile <p> --db <state.db> --report-json

触发场景 (governance.yaml trigger.mode=startup):
  - 5.15 startup_checklist.py 在 server 重启后调用
  - 冷启动时若 state.db 超阈值 → 自动 reclaim → 写 governance_log

流程:
  1. ensure-schema: 调用 governance_detector.ensure_governance_schema
                     (CREATE TABLE IF NOT EXISTS governance_meta / governance_log / governance_cluster*)
  2. detect_threshold --apply:  找超限候选清单
  3. 若 over_limit → 调 governance_reclaim_run 回收
  4. 写 governance_log(operator='auto_startup', op='startup_check')
     字段: status='done' / 'skipped' / 'reclaimed' / 'rolled_back'
  5. 输出报告 (stdout, --report-json 时 JSON)

退出码:
  0 = OK (未超阈值, 或已收回达标)
  1 = 检测通过但未达标 (warn, 但不致命 — 让 server 启动)
  2 = 致命 (db 不存在 / schema 失败 / 配置缺失)

安全 (AC-D5):
  - 全操作参数化 SQL
  - 审计日志不含消息原文 (仅 metadata + evidence)
  - 不可变保护从 governance_detector 继承 (24h 写入 / pinned / exempt)
"""
from __future__ import annotations

import argparse
import json
import os
import sqlite3
import sys
import time
from typing import Optional

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, SCRIPT_DIR)

import governance_config as cfg  # noqa: E402  # FIX-B3: 阈值单一事实源
import governance_detector as det  # noqa: E402
import governance_reclaim_run as rec  # noqa: E402

# v3.1 (2026-08-08): 默认从 governance_config 继承 LAG_RATIO=0.85
# FIX-B3: 常量直取单一事实源, 不再经 detector 中转
LAG_RATIO = cfg.LAG_RATIO
ACTIVE_WINDOW_S = cfg.ACTIVE_WINDOW_S


# ---------------------------------------------------------------- schema
def ensure_governance_schema(db_path: str) -> None:
    """幂等执行全部 governance DDL.

    涵盖两套 DDL:
      - governance_detector.GOVERNANCE_DDL  (meta / log / cluster / cluster_member)
      - session_governor._SQL_CREATE_TABLES  (额外含 governance_archive_index + FTS)

    任何一套漏了都会导致 archive_session 抛 OperationalError
    (常见: governance_archive_index 不存在).
    """
    con = sqlite3.connect(db_path)
    try:
        cur = con.cursor()
        for ddl in det.GOVERNANCE_DDL:
            cur.execute(ddl)
        # 补 session_governor 的 archive_index + FTS (对 archive_session 必需)
        import session_governor as sg  # noqa: E402
        for ddl in sg._SQL_CREATE_TABLES:
            cur.execute(ddl)
        con.commit()
    finally:
        con.close()


# ---------------------------------------------------------------- 阈值探测
def _detect_for_profile(
    db_path: str,
    profile: str,
    threshold_mb: Optional[int] = None,
    target_ratio: Optional[float] = None,
) -> tuple[int, int, bool, list[dict]]:
    """复用 detector.detect_threshold 拿到 (size, target, over_limit, candidates).

    参数覆盖语义 (v3.1 + CLI 补丁):
      - threshold_mb 缺省 → cfg.resolve_threshold_mb(profile)  (yaml 未读, 常量兜底)
      - target_ratio 缺省 → LAG_RATIO (0.85)
      CLI --threshold-mb/--target-ratio 只覆盖本次调用, 不改文件 (331 决策 1.3).
    """
    thr = threshold_mb if threshold_mb is not None else cfg.resolve_threshold_mb(profile)
    ratio = target_ratio if target_ratio is not None else LAG_RATIO
    l0 = det.detect_threshold(db_path, profile, thr,
                              dry_run=True, target_ratio=ratio)
    return (l0.db_size_bytes, int(l0.threshold_bytes * ratio),
            l0.over_limit, l0.candidates)


# ---------------------------------------------------------------- 启动自检主流程
def run_startup_check(
    profile: str,
    db_path: str,
    verbose: bool = False,
    threshold_mb: Optional[int] = None,
    target_ratio: Optional[float] = None,
) -> dict:
    """启动自检主入口. 返回报告 dict, 同时写 governance_log.

    返回字段:
      profile, db_path, size_bytes, threshold_bytes, target_bytes,
      over_limit, need_release_bytes, candidates_n,
      action: 'no_op' / 'reclaimed' / 'reclaim_failed' / 'skipped_no_db',
      reclaimed_bytes, archived_n, log_id, errors, elapsed_s
    """
    t0 = time.time()
    report: dict = {
        "profile": profile,
        "db_path": db_path,
        "size_bytes": 0,
        "threshold_bytes": 0,
        "target_bytes": 0,
        "over_limit": False,
        "need_release_bytes": 0,
        "candidates_n": 0,
        "action": "no_op",
        "reclaimed_bytes": 0,
        "archived_n": 0,
        "log_id": None,
        "errors": [],
        "elapsed_s": 0.0,
    }

    # ── 1. db 不存在 / 0 字节: 视为 "刚启动无数据", 跳过 ────────────────
    if not os.path.exists(db_path):
        report["action"] = "skipped_no_db"
        report["errors"].append(f"db not found: {db_path}")
        _write_log(db_path, profile, "startup_check", "no_db", "skipped",
                   f"db_missing path={db_path}", report["errors"], )
        return report

    size = os.path.getsize(db_path)
    report["size_bytes"] = size
    if size == 0:
        report["action"] = "skipped_empty_db"
        _write_log(db_path, profile, "startup_check", "empty_db", "skipped",
                   f"size=0 path={db_path}", [])
        return report

    # ── 2. ensure-schema (幂等) ────────────────────────────────────────
    try:
        ensure_governance_schema(db_path)
    except Exception as e:  # noqa: BLE001
        report["action"] = "schema_failed"
        report["errors"].append(f"schema: {type(e).__name__}: {e}")
        # schema 失败也算致命, 让 caller 决定
        return report

    # ── 3. detect_threshold (dry-run) ──────────────────────────────────
    try:
        sz, target, over_limit, candidates = _detect_for_profile(
            db_path, profile, threshold_mb=threshold_mb, target_ratio=target_ratio)
        thr_mb = threshold_mb if threshold_mb is not None else cfg.resolve_threshold_mb(profile)
        threshold_bytes = thr_mb * 1024 * 1024
        report["threshold_bytes"] = threshold_bytes
        report["target_bytes"] = target
        report["over_limit"] = over_limit
        report["candidates_n"] = len(candidates)
        if over_limit:
            report["need_release_bytes"] = max(sz - target, 0)
    except Exception as e:  # noqa: BLE001
        report["action"] = "detect_failed"
        report["errors"].append(f"detect: {type(e).__name__}: {e}")
        return report

    if not over_limit:
        report["action"] = "no_op"
        if verbose:
            print(f"[startup-check] {profile}: {sz/1048576:.1f}MB < "
                  f"target {target/1048576:.1f}MB ✓ no action "
                  f"(candidates={len(candidates)})")
        # C4 (2026-08-10): 日志 candidates 用真实候选数, 不再硬编码 0 —
        # 与 report["candidates_n"] / detector 报告一致 (独立复核 11:14 困惑根因之一)
        _write_log(db_path, profile, "startup_check", "ok", "done",
                   f"size={sz}B target={target}B "
                   f"ratio={LAG_RATIO} candidates={len(candidates)}", [])
        report["elapsed_s"] = round(time.time() - t0, 2)
        return report

    # ── 4. 超限 → 调 reclaim_run 回收 (沿用同 profile 同阈值同滞后带) ────
    if verbose:
        print(f"[startup-check] {profile}: {sz/1048576:.1f}MB >= "
              f"{threshold_bytes/1048576:.0f}MB threshold, "
              f"need release ~{report['need_release_bytes']/1048576:.1f}MB, "
              f"candidates={len(candidates)}, reclaiming ...")

    archived_n, reclaimed_bytes, rec_errors = 0, 0, []
    reclaimed_during_loop = False  # 周期实测已完整回收 → 收尾跳过
    try:
        # governance_reclaim_run.reclaim_once 走最简化回收 (不打印 JSON)
        # 直接利用其已有 _collect_candidates + archive_session 路径
        from session_governor import archive_session as _archive  # noqa: E402
        cands, info = rec._collect_candidates(db_path, thr_mb)
        # v3.1 (2026-08-08 修复): reclaim_run 模块顶部 LAG_RATIO 已同步 0.85,
        # 与 detector 一致, 不再有 0.9/0.85 差异. 仍显式覆盖 need 用本模块 target.
        info["need"] = report["need_release_bytes"]
        # C3-A (2026-08-10): 达标判断改周期性实测 (与 reclaim_run 一致),
        # 不再依赖 est_saved 累加 (低估 12 倍 → 永远判不达标 → 过度归档)。
        # 每 N 个归档后完整回收 (FTS optimize+checkpoint+VACUUM) 再 getsize 实测;
        # auto_vacuum=0 + FTS5 删除标记 → 不回收则大小不降, 判断不触发。
        for c in cands:
            # 周期性实测: 每归档 TARGET_CHECK_INTERVAL 个完整回收后检查
            if archived_n > 0 and archived_n % cfg.TARGET_CHECK_INTERVAL == 0:
                import governance_archiver as _arc  # noqa: E402
                _arc.reclaim_space(db_path, mode="full", apply=True)
                reclaimed_during_loop = True
                if os.path.getsize(db_path) <= target:
                    break
            try:
                r = _archive(db_path, c["session_id"], None,
                             profile, reason="threshold_startup",
                             operator="auto_startup")
                archived_n += 1
                reclaimed_bytes += r.get("storage_saved_bytes", 0)
            except Exception as e:  # noqa: BLE001
                rec_errors.append(f"{c['session_id'][:24]}: {type(e).__name__}: {e}")

        # 空间回收 (FTS optimize + checkpoint + VACUUM); 周期实测已回收则跳过
        rec_ok = False
        if archived_n > 0 and not reclaimed_during_loop:
            import governance_archiver as arc  # noqa: E402
            rec_info = arc.reclaim_space(db_path, mode="full", apply=True)
            rec_ok = bool(rec_info)
    except Exception as e:  # noqa: BLE001
        report["action"] = "reclaim_failed"
        report["errors"].append(f"reclaim: {type(e).__name__}: {e}")
        rec_errors.extend(report["errors"])

    report["archived_n"] = archived_n
    report["reclaimed_bytes"] = reclaimed_bytes

    # ── 5. 验收: 回收后若仍超阈值 → 标记 reclaimed_partial/insufficient ─
    size_after = os.path.getsize(db_path)
    still_over = size_after >= report["threshold_bytes"]
    # C3-A (2026-08-10): 达标判定改实测 — VACUUM 后文件大小 ≤ target 即达标,
    # 不再用 est_saved 累加 (低估 12 倍 → 永远判不达标 → 过度归档)。
    enough = size_after <= report["target_bytes"]

    if archived_n == 0:
        report["action"] = "no_candidates"
        status = "skipped"
    elif still_over and not enough:
        report["action"] = "reclaimed_partial"
        status = "warn"
    elif still_over:
        report["action"] = "reclaimed_insufficient"
        status = "warn"
    else:
        report["action"] = "reclaimed"
        status = "done"
        rec_ok = True

    report["errors"].extend(rec_errors)
    report["size_after_bytes"] = size_after
    report["reclaim_space_ok"] = rec_ok

    # 写 governance_log (operator=auto_startup, 不含消息原文 — AC-D5.4)
    evidence = (
        f"size_before={sz}B size_after={size_after}B "
        f"threshold={report['threshold_bytes']}B target={target}B "
        f"ratio={LAG_RATIO} candidates={len(candidates)} archived={archived_n} "
        f"reclaimed={reclaimed_bytes}B status={report['action']}"
    )
    log_id = _write_log(db_path, profile, "startup_check",
                        report["action"], status, evidence,
                        report["errors"], )
    report["log_id"] = log_id

    report["elapsed_s"] = round(time.time() - t0, 2)
    if verbose:
        print(f"[startup-check] {profile}: action={report['action']} "
              f"archived={archived_n} reclaimed≈{reclaimed_bytes/1048576:.1f}MB "
              f"size={sz/1048576:.1f}MB→{size_after/1048576:.1f}MB "
              f"({report['elapsed_s']}s)")
    return report


# ---------------------------------------------------------------- log writer
def _write_log(
    db_path: str,
    profile: str,
    op: str,
    reason: str,
    status: str,
    evidence: str,
    errors: list[str],
) -> Optional[int]:
    """写 governance_log (operator=auto_startup, AC-D5.4 不含原文).

    若 evidence 过长 (>4KB) 截断到 4KB (保护审计表不被撑爆).
    返回新行 id (int) 或 None (写失败).
    """
    if len(evidence) > 4096:
        evidence = evidence[:4000] + "...[truncated]"
    # 错误拼接为 reason 后缀 (但避免单条过长)
    if errors:
        e_summary = "; ".join(errors[:5])
        if len(e_summary) > 500:
            e_summary = e_summary[:500] + "..."
        reason = f"{reason} errors={e_summary}"
    try:
        con = sqlite3.connect(db_path)
        cur = con.cursor()
        cur.execute(
            """INSERT INTO governance_log
               (op, profile_name, before_state, after_state,
                reason, operator, evidence, status, created_at)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (op, profile, "active", "active",
             reason[:500], "auto_startup", evidence, status, time.time()),
        )
        log_id = cur.lastrowid
        con.commit()
        con.close()
        return log_id
    except Exception:
        return None


# ---------------------------------------------------------------- CLI
def main(argv: Optional[list[str]] = None) -> int:
    ap = argparse.ArgumentParser(description="archive-governor 启动自检 (v3.1)")
    ap.add_argument("--profile", required=True, help="profile 名 (用于查 PROFILE_OVERRIDE_MB)")
    ap.add_argument("--db", required=True, help="state.db 路径")
    ap.add_argument("--verbose", "-v", action="store_true", help="输出过程日志")
    ap.add_argument("--report-json", action="store_true", help="机器可读 JSON 报告")
    ap.add_argument("--threshold-mb", type=int, default=None,
                    help="覆盖阈值 (MB), 缺省用 detector 常量 (default=80, 其他=60). 只影响本次调用")
    ap.add_argument("--target-ratio", type=float, default=None,
                    help="覆盖滞后带比例, 缺省 0.85. 只影响本次调用 (331 决策 1.3)")
    args = ap.parse_args(argv)

    rep = run_startup_check(args.profile, args.db, verbose=args.verbose,
                            threshold_mb=args.threshold_mb,
                            target_ratio=args.target_ratio)

    if args.report_json:
        # errors 字段转为 string (json 不能直接序列化 list[exception] 类型)
        out = dict(rep)
        out["errors"] = [str(e) for e in rep["errors"]]
        print(json.dumps(out, ensure_ascii=False, indent=2))
    else:
        if args.verbose:
            print(json.dumps({k: v for k, v in rep.items() if k != "errors"},
                             ensure_ascii=False, indent=2))

    # 退出码: 致命 (no_db/schema/detect_failed) = 2; 其它 warn/skip = 0/1
    fatal = {"skipped_no_db", "schema_failed", "detect_failed"}
    if rep["action"] in fatal:
        return 2
    if rep["action"].startswith("reclaimed") and not rep.get("reclaim_space_ok"):
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
