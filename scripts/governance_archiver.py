#!/usr/bin/env python3
"""
governance_archiver.py — 会话语义治理 v3 (MVP-2, M3) 空间回收补充逻辑

范围 (设计文档 §8.2):
  冷归档 DELETE 热行后, SQLite 文件不会自动缩小。本脚本补充四步空间回收:
    1. FTS 删除同步校验 (触发器已自动同步, 此处核对 + 兜底 rebuild)
    2. FTS optimize (合并段)
    3. WAL checkpoint(TRUNCATE) (回收 WAL)
    4. incremental_vacuum / full VACUUM (压缩 db 文件)

关键现场事实 (2026-08-07 实测):
  - default/athena 两库 auto_vacuum=0 -> incremental_vacuum 默认无效
    * PRAGMA incremental_vacuum 仅在 auto_vacuum=INCREMENTAL 时有效
    * 因此脚本支持两种模式: --incremental (需先转 auto_vacuum) / --full (VACUUM)
  - messages_fts / messages_fts_trigram 均为 external-content FTS, 触发器
    (messages_fts_delete 等) 已存在且工作正常 (验证库 139 归档后 FTS=0)
  - 默认 dry-run: 只报告将执行的动作, 不写库; --apply 才真正执行
  - VACUUM 会短暂锁写, 设计文档 §8.2: 放 cron 低峰, 报告中记录耗时

用法:
  python governance_archiver.py --db <state.db> [--apply] [--full|--incremental]
  python governance_archiver.py --db <state.db> --check          # 仅检查/诊断
"""
from __future__ import annotations

import argparse
import json
import os
import sqlite3
import sys
import time
from typing import Dict, List, Optional, Tuple

# 缺省分批增量回收页数 (每次调用回收 N 页, 避免长锁)
INCREMENTAL_PAGES_DEFAULT = 4096


# ---------------------------------------------------------------- 诊断
def diagnose(db_path: str) -> dict:
    """只读诊断: 库大小 / journal / auto_vacuum / FTS 行数一致性."""
    out: dict = {"db_path": db_path}
    out["size_bytes"] = os.path.getsize(db_path)
    con = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True)
    try:
        cur = con.cursor()
        out["journal_mode"] = cur.execute("PRAGMA journal_mode").fetchone()[0]
        out["auto_vacuum"] = cur.execute("PRAGMA auto_vacuum").fetchone()[0]
        out["page_size"] = cur.execute("PRAGMA page_size").fetchone()[0]
        out["page_count"] = cur.execute("PRAGMA page_count").fetchone()[0]
        out["freelist_count"] = cur.execute("PRAGMA freelist_count").fetchone()[0]
        # FTS 行数一致性: messages vs messages_fts (external content)
        # 每张表独立容错：迷你库/旧库可能缺某张 FTS 表，不能整体失败
        m = f = ft = None
        try:
            m = cur.execute("SELECT COUNT(*) FROM messages").fetchone()[0]
        except sqlite3.OperationalError as e:
            out["messages_error"] = str(e)
        try:
            f = cur.execute("SELECT COUNT(*) FROM messages_fts").fetchone()[0]
        except sqlite3.OperationalError as e:
            out["messages_fts_error"] = str(e)
        try:
            ft = cur.execute("SELECT COUNT(*) FROM messages_fts_trigram").fetchone()[0]
        except sqlite3.OperationalError as e:
            out["messages_fts_trigram_error"] = str(e)
        if m is not None:
            out["messages"] = m
        if f is not None:
            out["messages_fts"] = f
        if ft is not None:
            out["messages_fts_trigram"] = ft
        if m is not None and f is not None:
            out["fts_synced"] = (m == f)
        # governance 表是否存在
        out["governance_tables"] = [
            r[0]
            for r in cur.execute(
                "SELECT name FROM sqlite_master WHERE type='table' AND name LIKE 'governance%'"
            ).fetchall()
        ]
    finally:
        con.close()
    return out


# ---------------------------------------------------------------- FTS 同步校验
def verify_fts_sync(db_path: str, apply: bool = False) -> Tuple[bool, dict]:
    """核对 messages 与 FTS 索引行数。

    触发器正常情况下删除已同步; 若失配 (历史库/触发器缺失), 提供兜底:
      --apply 时对失配部分做 FTS rebuild (external-content 全量重建, 幂等).
    返回 (是否一致, 详情).
    """
    con = sqlite3.connect(db_path)
    try:
        cur = con.cursor()
        m = cur.execute("SELECT COUNT(*) FROM messages").fetchone()[0]
        try:
            f = cur.execute("SELECT COUNT(*) FROM messages_fts").fetchone()[0]
        except sqlite3.OperationalError:
            f = None  # 无 FTS 表 (迷你库/旧库), 跳过 FTS 同步校验
        try:
            ft = cur.execute("SELECT COUNT(*) FROM messages_fts_trigram").fetchone()[0]
        except sqlite3.OperationalError:
            ft = None
        ok = (f is None) or (m == f)
        detail = {"messages": m, "fts": f, "fts_trigram": ft, "synced": ok}
        if not ok and apply:
            # 兜底重建 (external content FTS 标准做法)
            t0 = time.time()
            cur.execute("INSERT INTO messages_fts(messages_fts) VALUES('rebuild')")
            cur.execute("INSERT INTO messages_fts_trigram(messages_fts_trigram) VALUES('rebuild')")
            detail["rebuild"] = True
            detail["rebuild_seconds"] = round(time.time() - t0, 2)
            m2 = cur.execute("SELECT COUNT(*) FROM messages").fetchone()[0]
            f2 = cur.execute("SELECT COUNT(*) FROM messages_fts").fetchone()[0]
            detail["after"] = {"messages": m2, "fts": f2}
            con.commit()
            ok = (m2 == f2)
            detail["synced"] = ok
        return ok, detail
    finally:
        con.close()


# ---------------------------------------------------------------- 空间回收
def reclaim_space(
    db_path: str,
    mode: str = "incremental",       # incremental | full
    pages: int = INCREMENTAL_PAGES_DEFAULT,
    apply: bool = False,
) -> dict:
    """四步空间回收 (设计 §8.2). 返回执行报告.

    mode=incremental: 需 auto_vacuum=INCREMENTAL, 分批 incremental_vacuum (短锁)
    mode=full:        VACUUM 全量压缩 (长锁, 建议 cron 低峰)
    """
    start = time.time()
    report: dict = {
        "db_path": db_path,
        "mode": mode,
        "applied": apply,
        "size_before": os.path.getsize(db_path),
        "steps": [],
    }

    con = sqlite3.connect(db_path, timeout=60)
    try:
        cur = con.cursor()
        av = cur.execute("PRAGMA auto_vacuum").fetchone()[0]

        # 1) FTS 同步校验 (先核对; 不自动 rebuild, 由调用方决定)
        ok, fts_detail = verify_fts_sync(db_path, apply=False)
        report["fts_sync_check"] = fts_detail

        if apply:
            # 2) FTS optimize (合并段); 无 FTS 表的库跳过
            #    注意: fts5 optimize 是 INSERT 语句, python sqlite3 隐式开事务,
            #    必须先 commit 释放写锁, 否则后续 checkpoint(EXCLUSIVE) 会 self-lock
            t0 = time.time()
            try:
                cur.execute("INSERT INTO messages_fts(messages_fts) VALUES('optimize')")
                con.commit()
                report["steps"].append({"step": "fts_optimize", "seconds": round(time.time() - t0, 2)})
            except sqlite3.OperationalError as e:
                con.rollback()
                report["steps"].append({"step": "fts_optimize", "skipped": True, "reason": f"no fts table: {e}"})

            # trigram 索引是空间大头 (实测 64.8MB vs fts 24.5MB), 必须 optimize, 失败要报告
            t0 = time.time()
            try:
                cur.execute("INSERT INTO messages_fts_trigram(messages_fts_trigram) VALUES('optimize')")
                con.commit()
                report["steps"].append({"step": "fts_trigram_optimize", "seconds": round(time.time() - t0, 2)})
            except sqlite3.OperationalError as e:
                con.rollback()
                report["steps"].append({"step": "fts_trigram_optimize", "skipped": True, "reason": str(e)})

            # 3) WAL checkpoint(TRUNCATE) 回收 WAL; 运行中库可能 busy -> 降级 PASSIVE 不中断
            t0 = time.time()
            try:
                ck = cur.execute("PRAGMA wal_checkpoint(TRUNCATE)").fetchone()
                report["steps"].append({"step": "wal_checkpoint", "result": ck, "seconds": round(time.time() - t0, 2)})
            except sqlite3.OperationalError:
                try:
                    ck = cur.execute("PRAGMA wal_checkpoint(PASSIVE)").fetchone()
                    report["steps"].append({"step": "wal_checkpoint", "result": ck, "degraded": "TRUNCATE->PASSIVE", "seconds": round(time.time() - t0, 2)})
                except sqlite3.OperationalError as e:
                    report["steps"].append({"step": "wal_checkpoint", "skipped": True, "reason": str(e)})

            # 4) 空间回收: incremental 或 full
            if mode == "full":
                t0 = time.time()
                cur.execute("VACUUM")
                con.commit()
                report["steps"].append({"step": "vacuum_full", "seconds": round(time.time() - t0, 2)})
            else:  # incremental
                if av == 2:  # INCREMENTAL
                    t0 = time.time()
                    cur.execute(f"PRAGMA incremental_vacuum({pages})")
                    con.commit()
                    report["steps"].append({
                        "step": "incremental_vacuum",
                        "pages": pages,
                        "seconds": round(time.time() - t0, 2),
                    })
                else:
                    report["steps"].append({
                        "step": "incremental_vacuum",
                        "skipped": True,
                        "reason": f"auto_vacuum={av} (需 INCREMENTAL=2); 建议 --full VACUUM 或先转换",
                    })
            con.commit()
        else:
            report["steps"].append({"step": "dry_run", "note": "未执行任何写操作"})
    finally:
        con.close()

    report["size_after"] = os.path.getsize(db_path)
    report["saved_bytes"] = report["size_before"] - report["size_after"]
    report["elapsed_seconds"] = round(time.time() - start, 2)
    return report


# ---------------------------------------------------------------- CLI
def main(argv: Optional[List[str]] = None) -> int:
    ap = argparse.ArgumentParser(description="Governance archiver: space reclaim (M3)")
    ap.add_argument("--db", required=True, help="state.db path")
    ap.add_argument("--check", action="store_true", help="只诊断, 不执行回收")
    ap.add_argument("--apply", action="store_true", help="真正执行 (默认 dry-run)")
    ap.add_argument("--full", action="store_true", help="full VACUUM 模式 (默认 incremental)")
    ap.add_argument("--pages", type=int, default=INCREMENTAL_PAGES_DEFAULT, help="incremental pages")
    ap.add_argument("--json", action="store_true", help="JSON 输出")
    args = ap.parse_args(argv)

    if not os.path.exists(args.db):
        print(f"ERROR: db not found: {args.db}", file=sys.stderr)
        return 2

    if args.check:
        d = diagnose(args.db)
        if args.json:
            print(json.dumps(d, ensure_ascii=False, indent=2))
        else:
            print(f"=== Diagnose {args.db} ===")
            for k, v in d.items():
                print(f"  {k}: {v}")
        return 0

    mode = "full" if args.full else "incremental"
    r = reclaim_space(args.db, mode=mode, pages=args.pages, apply=args.apply)
    if args.json:
        print(json.dumps(r, ensure_ascii=False, indent=2, default=str))
    else:
        print(f"=== Reclaim {args.db} (mode={mode}, apply={args.apply}) ===")
        print(f"  size: {r['size_before']/1048576:.1f}MB -> {r['size_after']/1048576:.1f}MB "
              f"(saved {r['saved_bytes']/1048576:.2f}MB, {r['elapsed_seconds']}s)")
        for s in r["steps"]:
            print(f"  step: {s}")
        print(f"  fts_sync_check: {r['fts_sync_check']}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
