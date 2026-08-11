#!/usr/bin/env python3
"""governance_reclaim_run.py — MVP-2 真库回收执行器（一次性工具，不落 scripts/）

流程（设计 §10.2/§8.2 安全流）:
  1. 读取 detector 语义的候选清单（最旧优先，活跃/pinned/exempt 保护）
  2. 逐个调用 session_governor.archive_session() 原子归档（写 .gz+sha256 → 删热行）
  3. 批量归档完成后: FTS optimize + WAL checkpoint + VACUUM（governance_archiver 逻辑）
  4. 输出指标: 归档 N 会话 / 释放字节 / 剩余热存储 / 耗时

安全（FIX-B1, 2026-08-09）:
  - 默认 dry-run: 无论是否传参，未显式 `--apply` 一律只输出 plan，零写库。
    plan 输出完整候选清单后立即 return，任何 archive_session()/DELETE/VACUUM/写事务都不会执行。
  - `--dry-run` 与 `--apply` 互斥（argparse mutually exclusive group），同时给出报错退出。
  - 兼容性说明: 旧版裸跑（无 --apply）即写库的行为是 PRODUCTION-DB-DISCOVERY.md B1 漏洞本身
    （--json 输出 plan 后仍继续执行生产归档），因此不保留；要执行回收必须显式 --apply。
    governance_startup_check 的自动回收不经过本脚本 main()（直接复用 _collect_candidates +
    archive_session），不受此 CLI 变更影响。
  - 每个会话归档前 verify 无需重复——archive_session 内已含 sha256 双层校验
  - 执行前必须有备份（外部流程已备份 state.db.bak-mvp2-*）

用法:
  # dry-run（默认，零写入）: 只输出完整回收 plan
  python governance_reclaim_run.py --db <state.db> --profile <p> --threshold-mb 80
  python governance_reclaim_run.py --db <state.db> --profile <p> --threshold-mb 80 --dry-run

  # apply（显式开启，会写库）:
  python governance_reclaim_run.py --db <state.db> --profile <p> --threshold-mb 80 --apply
"""
from __future__ import annotations

import argparse
import json
import os
import sqlite3
import sys
import time

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, SCRIPT_DIR)

import governance_config as cfg  # noqa: E402  # FIX-B3: 阈值单一事实源
import session_governor as gov  # noqa: E402
import governance_archiver as arc  # noqa: E402

# FIX-B3: 常量由 governance_config 派生 (0.85 / 24h), 不再本地复制防漂移
ACTIVE_WINDOW_S = cfg.ACTIVE_WINDOW_S
LAG_RATIO = cfg.LAG_RATIO        # v3.1 (2026-08-08 user 拍板): 15% 滞后带, 与 detector 一致


def _collect_candidates(db_path: str, threshold_mb: int) -> tuple[list[dict], dict]:
    """与 governance_detector.detect_threshold 相同语义的完整候选集（不截断 30 行）。

    B2 (2026-08-09): 与 detector 保持同一约束 — `COALESCE(s.archived,0)=0` 排除
    已归档会话 (热行已删, est_saved=0 虚候选), 零热消息(空)会话按 empty 跳过。
    """
    size = os.path.getsize(db_path)
    target = int(threshold_mb * 1024 * 1024 * LAG_RATIO)
    need = size - target
    con = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True)
    cur = con.cursor()
    rows = cur.execute(
        f"""SELECT s.id, s.started_at, s.ended_at, s.pinned,
                  (SELECT COUNT(*) FROM messages m WHERE m.session_id=s.id) AS msg_n,
                  (SELECT COALESCE(SUM(LENGTH(m.content)),0) FROM messages m WHERE m.session_id=s.id) AS content_bytes,
                  (SELECT {cfg.text_bytes_sum_expr(cur, 'm')} FROM messages m WHERE m.session_id=s.id) AS text_bytes
           FROM sessions s WHERE s.pinned = 0 AND COALESCE(s.archived, 0) = 0
           ORDER BY s.started_at ASC"""
    ).fetchall()
    cands, active_n, protected_n, empty_n = [], 0, 0, 0
    for (sid, started, ended, pinned, msg_n, cb, tb) in rows:
        # 活跃判定与 detector._session_is_active 完全同序: 先 active 再 empty
        # (B2 2026-08-10: 顺序统一, 无消息+未结束+started<24h 会话归 active 而非 empty,
        #  与 detector 分类计数一致, 防 info 计数漂移)
        last = cur.execute("SELECT MAX(timestamp) FROM messages WHERE session_id=?", (sid,)).fetchone()[0]
        if last and (time.time() - last) < ACTIVE_WINDOW_S:
            active_n += 1
            continue
        if not last:
            r = cur.execute("SELECT ended_at FROM sessions WHERE id=?", (sid,)).fetchone()
            if r and r[0] is None and (time.time() - (started or 0)) < ACTIVE_WINDOW_S:
                active_n += 1
                continue
        if msg_n <= 0:
            empty_n += 1
            continue
        gm = cur.execute("SELECT exempt FROM governance_meta WHERE session_id=?", (sid,)).fetchone()
        if gm and gm[0]:
            protected_n += 1
            continue
        # C3-A (2026-08-10): est_saved 全文本列和 × 压缩系数 (单一事实源 cfg)
        cands.append({"session_id": sid, "started_at": started, "messages": msg_n,
                      "content_bytes": cb, "text_bytes": tb,
                      "est_saved_bytes": cfg.est_saved_bytes(tb)})
    con.close()
    info = {"size": size, "need": max(need, 0), "target": target,
            "active": active_n, "protected": protected_n, "empty": empty_n,
            "candidates": len(cands)}
    return cands, info


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(
        description="会话冷归档回收执行器 — 默认 dry-run（零写入），仅显式 --apply 才执行归档/回收。")
    ap.add_argument("--db", required=True)
    ap.add_argument("--profile", default="default")
    ap.add_argument("--threshold-mb", type=int, default=None)
    ap.add_argument("--max-sessions", type=int, default=10**9, help="本次归档上限")
    ap.add_argument("--archive-root", default=None)
    ap.add_argument("--json", action="store_true")
    mode = ap.add_mutually_exclusive_group()
    mode.add_argument("--dry-run", action="store_true",
                      help="只输出完整回收 plan 后立即返回，零写库（默认行为，安全闸门）")
    mode.add_argument("--apply", action="store_true",
                      help="显式开启执行：按 plan 实际归档会话 + FTS optimize + VACUUM（会写库，慎用）")
    args = ap.parse_args(argv)

    is_apply = args.apply  # 未显式 --apply（含 --dry-run 或两者皆无）→ dry-run

    t0 = time.time()
    # FIX-B3: 统一解析入口 (显式覆盖 > profile override > 默认), 修复 0 值回退 bug
    thr = int(cfg.resolve_threshold_mb(args.profile, args.threshold_mb))

    cands, info = _collect_candidates(args.db, thr)

    # ── plan 输出（dry-run 与 apply 共用；dry-run 时即最终输出）──
    if args.json:
        print(json.dumps({"phase": "plan", "dry_run": not is_apply, "info": info,
                          "candidates": cands}, ensure_ascii=False))
    else:
        print(f"[plan] size={info['size']/1048576:.1f}MB need_release={info['need']/1048576:.1f}MB "
              f"candidates={info['candidates']} active={info['active']} protected={info['protected']}")
        for c in cands:
            print(f"  - {c['session_id']} msgs={c['messages']} "
                  f"bytes={c['content_bytes']} text={c['text_bytes']} "
                  f"est_saved≈{c['est_saved_bytes']}B")

    if not is_apply:
        # ── FIX-B1 安全闸门：plan 已完整输出，在任何 archive_session()/DELETE/VACUUM/写事务之前返回 ──
        if not args.json:
            print("[dry-run] 零写入：未执行任何归档/回收（要真正回收请显式加 --apply）")
        return 0

    # ── 以下仅 --apply 显式开启时可达（写分支）──────────────────────────
    archived, released = 0, 0
    errors = []
    reclaimed_during_loop = False  # 周期实测已做过完整 reclaim → 收尾跳过 (幂等优化)
    # C3-A (2026-08-10): 达标判断改周期性实测 — 每归档 TARGET_CHECK_INTERVAL 个会话
    # 做一次完整空间回收 (FTS optimize + checkpoint + VACUUM), 再以 os.path.getsize
    # 实测 ≤ target 即停。不再依赖 est_saved 累加 (旧 content×0.9 低估 12 倍 →
    # 永远判不达标 → 过度归档 220 vs 实际 ~130)。
    # 为什么必须周期 reclaim: auto_vacuum=0 库 DELETE 不缩文件; FTS5 的 DELETE
    # 只写删除标记, 空间直到 optimize 才释放 — 不回收则 getsize/逻辑占用均不降,
    # 达标判断永远不触发 (C3 实验 3.2 "真实 VACUUM 后大小" 即此口径)。
    need = info["need"]
    for c in cands:
        if archived >= args.max_sessions:
            break
        # 防空转: 库已在目标内 (need≤0) 时归档 1 个即停 (保持旧语义)
        if archived > 0 and need <= 0:
            break
        # 周期性实测达标: 每 N 个归档后完整回收再实测, 达标即停
        if archived > 0 and archived % cfg.TARGET_CHECK_INTERVAL == 0:
            arc.reclaim_space(args.db, mode="full", apply=True)
            reclaimed_during_loop = True
            size_now = os.path.getsize(args.db)
            if size_now <= info["target"]:
                if not args.json:
                    print(f"  ...达标 (实测 {size_now/1048576:.1f}MB ≤ target "
                          f"{info['target']/1048576:.1f}MB) @archived={archived}")
                break
        try:
            r = gov.archive_session(args.db, c["session_id"], args.archive_root,
                                    args.profile, reason="threshold", operator="auto")
            archived += 1
            released += r["storage_saved_bytes"]
            if not args.json and archived % 10 == 0:
                print(f"  ...archived={archived} released≈{released/1048576:.1f}MB "
                      f"(last={c['session_id'][:20]})")
        except Exception as e:  # noqa: BLE001
            # 已归档/重复归档等业务拒绝不算错误, 记录后跳过
            errors.append(f"{c['session_id']}: {type(e).__name__}: {e}")
            if args.json:
                continue
            if "禁止重复归档" in str(e) or "已置 archived" in str(e):
                print(f"  skip(already) {c['session_id'][:20]}")
            else:
                print(f"  ERR {c['session_id'][:20]}: {e}")

    # 空间回收: FTS optimize + checkpoint + VACUUM(full, auto_vacuum=0 需 full)
    # 周期实测已回收过则跳过 (幂等, 避免重复长锁 VACUUM)
    rec = None
    if archived > 0 and not reclaimed_during_loop:
        rec = arc.reclaim_space(args.db, mode="full", apply=True)
    size_after = os.path.getsize(args.db)
    elapsed = round(time.time() - t0, 1)

    out = {
        "phase": "done",
        "dry_run": False,
        "profile": args.profile,
        "threshold_mb": thr,
        "archived": archived,
        "released_est_bytes": released,
        "size_before_mb": round(info["size"] / 1048576, 2),
        "size_after_mb": round(size_after / 1048576, 2),
        "reclaim": rec,
        "errors_count": len(errors),
        "errors_sample": errors[:10],
        "elapsed_s": elapsed,
    }
    if args.json:
        print(json.dumps(out, ensure_ascii=False, default=str))
    else:
        print(f"[done] archived={archived} released≈{released/1048576:.1f}MB "
              f"size {info['size']/1048576:.1f}MB -> {size_after/1048576:.1f}MB "
              f"({elapsed}s)")
        if rec:
            print(f"  reclaim: {rec.get('size_before')} -> {rec.get('size_after')} "
                  f"steps={[s.get('step') for s in rec.get('steps', [])]}")
        if errors:
            print(f"  errors({len(errors)}): {errors[:5]}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
