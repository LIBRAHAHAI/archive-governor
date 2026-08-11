#!/usr/bin/env python3
"""
governance_detector.py — 会话语义治理 v3 (MVP-2, M4)

范围:
  L0 阈值检测: state.db 单文件大小 vs 阈值 (60MB 默认 / default 80MB override)
  L1 精确去重: content sha256 全等 + session_key 全等 (FPR=0%)

安全模型 (设计文档 §6/§10):
  - 默认 dry-run: 只读, 不写任何数据; --apply 才写 dedup 标记
  - 自动动作仅限确定性证据 (布尔阈值 / 全等匹配), 无 LLM
  - 不可变保护: 活跃会话 (ended_at IS NULL / 24h 内写入) / pinned / exempt 永不参与
  - 全操作参数化 SQL, 防注入; 日志不含原文 (脱敏)

用法:
  python governance_detector.py --db <state.db> [--threshold-mb 60] [--apply]
  python governance_detector.py --db <state.db> --json        # 机器可读报告
"""
from __future__ import annotations

import argparse
import hashlib
import json
import os
import sqlite3
import sys
import time
from collections import defaultdict
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Tuple

import governance_config as cfg  # FIX-B3: 阈值单一事实源

SCHEMA_VERSION = "1"
# FIX-B3: 常量由 governance_config 派生, 不再本地硬编码 (防五入口漂移)
DEFAULT_THRESHOLD_MB = cfg.DEFAULT_THRESHOLD_MB            # 60
PROFILE_OVERRIDE_MB = dict(cfg.PROFILE_OVERRIDE_MB)        # {"default": 80}
LAG_RATIO = cfg.LAG_RATIO         # 0.85: 保留比例, 归档到阈值×0.85 防抖动 (§10.2, 2026-08-08 user 拍板)
ACTIVE_WINDOW_S = cfg.ACTIVE_WINDOW_S  # 24h 内写入视为活跃 (§6.3)

# ---------------------------------------------------------------- DDL (幂等)
GOVERNANCE_DDL = [
    """CREATE TABLE IF NOT EXISTS governance_meta (
        session_id   TEXT PRIMARY KEY,
        profile_name TEXT,
        state        TEXT NOT NULL CHECK (state IN ('active','cold_archived')),
        cold_archived_at REAL,
        reason       TEXT,
        cluster_ids  TEXT,
        exempt       INTEGER DEFAULT 0,
        archive_file TEXT,
        archive_sha256 TEXT,
        archive_size_bytes INTEGER,
        storage_saved_bytes INTEGER,
        created_at   REAL,
        updated_at   REAL
    )""",
    """CREATE TABLE IF NOT EXISTS governance_log (
        id           INTEGER PRIMARY KEY AUTOINCREMENT,
        op           TEXT NOT NULL,
        session_id   TEXT,
        profile_name TEXT,
        before_state TEXT,
        after_state  TEXT,
        reason       TEXT,
        operator     TEXT,
        evidence     TEXT,
        status       TEXT,
        created_at   REAL
    )""",
    """CREATE TABLE IF NOT EXISTS governance_cluster (
        cluster_id   TEXT PRIMARY KEY,
        topic        TEXT,
        topic_kind   TEXT,
        profile_name TEXT,
        source_hint  TEXT,
        member_count INTEGER,
        created_at   REAL
    )""",
    """CREATE TABLE IF NOT EXISTS governance_cluster_member (
        cluster_id   TEXT,
        session_id   TEXT,
        message_id   INTEGER,
        dedup_flag   INTEGER DEFAULT 0,
        keep_chain   INTEGER DEFAULT 1,
        PRIMARY KEY (cluster_id, message_id)
    )""",
]


# ---------------------------------------------------------------- 数据类
@dataclass
class L1DupPair:
    kind: str                 # content_hash | session_key
    key: str                  # sha256 前16 或 session_key
    sessions: List[str] = field(default_factory=list)
    message_ids: List[int] = field(default_factory=list)
    content_preview: str = ""  # 脱敏预览 (仅 dry-run 展示用, 不落日志)


@dataclass
class L0Result:
    db_size_bytes: int
    threshold_bytes: int
    over_limit: bool
    need_release_bytes: int = 0
    candidates: List[dict] = field(default_factory=list)  # 待归档候选 (最旧优先)
    protected: int = 0          # 不可变保护跳过的会话数
    active: int = 0             # 活跃会话数
    empty: int = 0              # B2: 零热消息(空)会话, 无内容可归档


@dataclass
class DetectorReport:
    profile: str
    db_path: str
    l0: Optional[L0Result] = None
    dup_pairs: List[L1DupPair] = field(default_factory=list)
    applied_dedup: int = 0
    errors: List[str] = field(default_factory=list)


# ---------------------------------------------------------------- 工具
def _now() -> float:
    return time.time()


def sha256_hex(s: Optional[str]) -> Optional[str]:
    if not s:
        return None
    return hashlib.sha256(s.encode("utf-8", errors="replace")).hexdigest()


def _session_is_active(cur: sqlite3.Cursor, session_id: str, window_s: int = ACTIVE_WINDOW_S) -> bool:
    """活跃判定 (§6.3): 最近 window_s 秒内有消息写入 = 活跃 -> 保护.

    现场修正 (2026-08-07 实测): 大量 ended_at IS NULL 的会话最后写入在
    3~30 天前 (僵尸会话), 若按 ended_at 判活跃会导致阈值归档无法达标
    (hermes 只能到 118MB)。因此活跃以「最后写入时间」为准:
      - 最近有写入 -> 活跃 (保护, 不可归档)
      - 长期无写入 (即便 ended_at 为空) -> 视为僵尸, 可归档
    """
    last = cur.execute(
        "SELECT MAX(timestamp) FROM messages WHERE session_id=?", (session_id,)
    ).fetchone()
    if last and last[0]:
        return (time.time() - last[0]) < window_s
    # 无消息会话: 未结束且启动不久 -> 视为活跃 (可能刚创建正在写入)
    row = cur.execute("SELECT started_at, ended_at FROM sessions WHERE id=?", (session_id,)).fetchone()
    if row is None:
        return True  # 查不到 = 无法回溯来源 -> 保护
    if row[1] is not None:
        return False  # 已结束且无消息, 空会话可归档
    return (time.time() - (row[0] or 0)) < window_s


# ---------------------------------------------------------------- L0 阈值检测
def detect_threshold(
    db_path: str,
    profile: str,
    threshold_mb: int,
    dry_run: bool = True,
    target_ratio: Optional[float] = None,
) -> L0Result:
    """L0: state.db 文件大小 vs 阈值. 始终列出最旧优先可归档会话 (含未超限, C4).

    target_ratio: 滞后带比例 (默认 LAG_RATIO=0.85, 2026-08-08 user 拍板)
                  target = threshold * target_ratio
                  例: 阈值 80MB × 0.85 = 68MB (15% 滞后带)

    C4 (2026-08-10, user 拍板方案 1): 未超限也收集完整候选集, 与
    governance_reclaim_run._collect_candidates 语义一致 (不再提前返回)。
    over_limit 仍是唯一回收判据: need_release_bytes 仅超限时 > 0,
    未超限时候选集仅作报告/预判指标, 不触发任何回收动作。
    """
    if target_ratio is None:
        target_ratio = LAG_RATIO
    size = os.path.getsize(db_path)
    threshold = threshold_mb * 1024 * 1024
    target = int(threshold * target_ratio)
    res = L0Result(
        db_size_bytes=size,
        threshold_bytes=threshold,
        over_limit=size >= threshold,
    )
    # C4 (2026-08-10, user 拍板方案 1): 删除提前返回 —— 未超限也收集完整候选集,
    # 与 governance_reclaim_run._collect_candidates 语义一致 (独立复核 11:14 实测
    # 非超限 profile detector 0 vs reclaim 67/33/19, 根因即此跳过)。
    # need_release_bytes 保持仅超限才设 (未超限=0) —— "未超限无需回收"业务结论不变,
    # over_limit 仍是唯一回收判据; 候选集仅作报告/预判指标, 只读毫秒级扫描。
    if res.over_limit:
        res.need_release_bytes = size - target

    con = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True)
    try:
        cur = con.cursor()
        # 不可变保护: 活跃 (窗口内写入) / pinned / 已归档 不参与
        # B2 (2026-08-09): 统一约束 COALESCE(s.archived,0)=0, 已归档会话 (热行已删)
        #   排除在可归档候选之外; 零热消息会话在循环内按 empty 跳过 (est_saved=0 虚候选)
        # 候选 = 全部会话按最旧优先, 逐条用 _session_is_active 过滤 (写入时间口径)
        rows = cur.execute(
            f"""SELECT s.id, s.started_at, s.ended_at, s.message_count, s.pinned,
                      (SELECT COUNT(*) FROM messages m WHERE m.session_id=s.id) AS msg_n,
                      (SELECT COALESCE(SUM(LENGTH(m.content)),0) FROM messages m WHERE m.session_id=s.id) AS content_bytes,
                      (SELECT {cfg.text_bytes_sum_expr(cur, 'm')} FROM messages m WHERE m.session_id=s.id) AS text_bytes
               FROM sessions s
               WHERE s.pinned = 0 AND COALESCE(s.archived, 0) = 0
               ORDER BY s.started_at ASC"""
        ).fetchall()
        protected = 0
        active = 0
        empty = 0
        for (sid, started, ended, mc, pinned, msg_n, content_bytes, text_bytes) in rows:
            if _session_is_active(cur, sid):
                active += 1
                continue
            # B2: 零热消息(空)会话 — 无内容可归档, est_saved=0, 排除 (虚候选)
            if msg_n <= 0:
                empty += 1
                continue
            # exempt 保护 (governance_meta 存在时)
            gm = cur.execute(
                "SELECT exempt FROM governance_meta WHERE session_id=?", (sid,)
            ).fetchone()
            if gm and gm[0]:
                protected += 1
                continue
            # C3-A (2026-08-10): est_saved 改全文本列和 × 压缩系数 (单一事实源 cfg),
            # 旧 content×0.9 漏 reasoning/tool_calls 列与 FTS 索引, 低估真实释放 12 倍。
            res.candidates.append(
                {
                    "session_id": sid,
                    "started_at": started,
                    "ended_at": ended,
                    "messages": msg_n,
                    "content_bytes": content_bytes,
                    "text_bytes": text_bytes,
                    "est_saved_bytes": cfg.est_saved_bytes(text_bytes),
                }
            )
        res.protected = protected
        res.active = active
        res.empty = empty
        # 统计预估释放, 标记足以达标的候选
        acc = 0
        for c in res.candidates:
            acc += c["est_saved_bytes"]
            c["cumulative_release"] = acc
    finally:
        con.close()
    return res


# ---------------------------------------------------------------- L1 精确去重
def detect_l1_duplicates(
    db_path: str,
    profile: str,
    dry_run: bool = True,
) -> Tuple[List[L1DupPair], int]:
    """L1: content sha256 全等 + session_key 全等 -> 精确重复对 (FPR=0%).

    - content 级: 仅当消息正文逐字节一致 (sha256 碰撞可忽略) 才视为重复
    - session_key 级: 字符串全等 (仅同 profile 内比较)
    - 活跃会话内消息不参与去重 (保护进行中内容)
    返回 (重复对列表, 可应用标记数).
    """
    con = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True)
    pairs: List[L1DupPair] = []
    try:
        cur = con.cursor()
        # 活跃会话排除集: 最近窗口内有写入 = 活跃 (§6.3, 写入时间口径)
        # 与 _session_is_active 同一语义; 无消息的新会话 (started 不久) 也算活跃
        active_ids = {
            r[0]
            for r in cur.execute(
                """SELECT s.id FROM sessions s
                   LEFT JOIN messages m ON m.session_id = s.id
                   GROUP BY s.id
                   HAVING (MAX(m.timestamp) IS NOT NULL AND MAX(m.timestamp) > ?)
                       OR (COUNT(m.id) = 0 AND s.ended_at IS NULL AND s.started_at > ?)""",
                (time.time() - ACTIVE_WINDOW_S, time.time() - ACTIVE_WINDOW_S),
            ).fetchall()
        }
        # 1) content hash: 消息级 (非 tool 行避免 trigram 噪音, 与 FTS 口径一致)
        cur.execute(
            """SELECT m.id, m.session_id, m.content
               FROM messages m
               WHERE m.content IS NOT NULL AND LENGTH(m.content) > 0"""
        )
        hash_groups: Dict[str, L1DupPair] = {}
        for mid, sid, content in cur.fetchall():
            if sid in active_ids:
                continue
            h = sha256_hex(content)
            if h is None:
                continue
            key = h[:16]
            if key not in hash_groups:
                hash_groups[key] = L1DupPair(
                    kind="content_hash",
                    key=key,
                    content_preview=content[:60].replace("\n", " "),
                )
            g = hash_groups[key]
            if mid not in g.message_ids:
                g.message_ids.append(mid)
            if sid not in g.sessions:
                g.sessions.append(sid)
        for g in hash_groups.values():
            if len(g.message_ids) >= 2 and len(g.sessions) >= 1:
                pairs.append(g)

        # 2) session_key 全等 (同 profile, 排除活跃)
        cur.execute(
            """SELECT id, session_key FROM sessions
               WHERE session_key IS NOT NULL AND session_key != ''"""
        )
        sk_groups: Dict[str, List[str]] = defaultdict(list)
        for sid, sk in cur.fetchall():
            if sid in active_ids:
                continue
            sk_groups[sk].append(sid)
        for sk, sids in sk_groups.items():
            if len(sids) >= 2:
                pairs.append(L1DupPair(kind="session_key", key=sk, sessions=sids))
    finally:
        con.close()

    # 计算可标记数: 每个 pair 中除第一条 keep_chain 外都可标 dedup_flag=1
    applicable = sum(max(0, len(p.message_ids) - 1) for p in pairs if p.kind == "content_hash")
    applicable += sum(max(0, len(p.sessions) - 1) for p in pairs if p.kind == "session_key")
    return pairs, applicable


# ---------------------------------------------------------------- 写 dedup 标记
def apply_dedup_marks(
    db_path: str,
    profile: str,
    pairs: List[L1DupPair],
    operator: str = "auto",
) -> int:
    """写 L1 去重标记到 governance_cluster_member (dedup_flag=1).

    设计 §7.3: 只标记不删除; 组内第一条 keep_chain=1, 其余 dedup_flag=1.
    幂等: 相同 (cluster_id, message_id) 再次写入为 UPDATE 同值.
    """
    con = sqlite3.connect(db_path)
    try:
        cur = con.cursor()
        for ddl in GOVERNANCE_DDL:
            cur.execute(ddl)
        applied = 0
        t = _now()
        for p in pairs:
            cid = f"dedup_{p.kind}_{p.key}"
            new_applied = 0  # 本 pair 真实新增数 (幂等日志依据)
            # 簇头
            cur.execute(
                """INSERT OR IGNORE INTO governance_cluster
                   (cluster_id, topic, topic_kind, profile_name, source_hint, member_count, created_at)
                   VALUES (?,?,?,?,?,?,?)""",
                (cid, f"L1 dedup ({p.kind})", "experience", profile, f"L1_{p.kind}", 0, t),
            )
            if p.kind == "content_hash":
                # 组内第一条 keep_chain, 其余 dedup_flag=1 (幂等: rowcount 计真实插入)
                keep = True
                for mid in p.message_ids:
                    sid = _session_of_message(cur, mid)
                    cur.execute(
                        """INSERT OR IGNORE INTO governance_cluster_member
                           (cluster_id, session_id, message_id, dedup_flag, keep_chain)
                           VALUES (?,?,?,?,?)""",
                        (cid, sid, mid, 0 if keep else 1, 1 if keep else 0),
                    )
                    if not keep and cur.rowcount > 0:
                        applied += 1
                        new_applied += 1
                    keep = False
            else:  # session_key: session 级重复 -> 标记后写 sessions 无列可标, 仅记录 cluster
                # 注意: 主键 (cluster_id, message_id), 用负索引占位保证每条 session 唯一
                for i, sid in enumerate(p.sessions):
                    keep = i == 0
                    cur.execute(
                        """INSERT OR IGNORE INTO governance_cluster_member
                           (cluster_id, session_id, message_id, dedup_flag, keep_chain)
                           VALUES (?,?,?,?,?)""",
                        (cid, sid, -(i + 1), 0 if keep else 1, 1 if keep else 0),
                    )
                    if not keep and cur.rowcount > 0:
                        applied += 1
                        new_applied += 1
            # 更新 member_count
            n = cur.execute(
                "SELECT COUNT(*) FROM governance_cluster_member WHERE cluster_id=?", (cid,)
            ).fetchone()[0]
            cur.execute(
                "UPDATE governance_cluster SET member_count=? WHERE cluster_id=?", (n, cid)
            )
            # 幂等: 仅当该 pair 有新插入 (applied 增加) 才写审计日志
            if new_applied > 0:
                cur.execute(
                    """INSERT INTO governance_log
                       (op, session_id, profile_name, before_state, after_state,
                        reason, operator, evidence, status, created_at)
                       VALUES ('dedup_mark', ?, ?, 'active', 'active',
                                'L1_exact', ?, ?, 'done', ?)""",
                    (p.sessions[0] if p.sessions else None, profile, operator,
                     f"kind={p.kind} key={p.key} members={len(p.message_ids) or len(p.sessions)}", t),
                )
        con.commit()
        return applied
    finally:
        con.close()


def _session_of_message(cur: sqlite3.Cursor, message_id: int) -> Optional[str]:
    r = cur.execute("SELECT session_id FROM messages WHERE id=?", (message_id,)).fetchone()
    return r[0] if r else None


# ---------------------------------------------------------------- 报告
def build_report(
    profile: str,
    db_path: str,
    l0: Optional[L0Result],
    pairs: List[L1DupPair],
    applied: int,
    errors: List[str],
) -> DetectorReport:
    return DetectorReport(
        profile=profile,
        db_path=db_path,
        l0=l0,
        dup_pairs=pairs,
        applied_dedup=applied,
        errors=errors,
    )


def report_to_dict(r: DetectorReport) -> dict:
    d = {
        "profile": r.profile,
        "db_path": r.db_path,
        "applied_dedup": r.applied_dedup,
        "errors": r.errors,
    }
    if r.l0:
        d["l0"] = {
            "db_size_bytes": r.l0.db_size_bytes,
            "db_size_mb": round(r.l0.db_size_bytes / 1048576, 2),
            "threshold_mb": round(r.l0.threshold_bytes / 1048576, 1),
            "over_limit": r.l0.over_limit,
            "need_release_mb": round(r.l0.need_release_bytes / 1048576, 2),
            "active_sessions": r.l0.active,
            "protected_sessions": r.l0.protected,
            "empty_sessions": r.l0.empty,
            "archive_candidates": len(r.l0.candidates),
            "candidates": [
                {
                    "session_id": c["session_id"],
                    "started_at": c["started_at"],
                    "messages": c["messages"],
                    "content_bytes": c["content_bytes"],
                    "text_bytes": c["text_bytes"],  # C3-A: 全文本列和 (content+reasoning+reasoning_content+tool_calls+api_content)
                    "est_saved_mb": round(c["est_saved_bytes"] / 1048576, 2),
                }
                for c in r.l0.candidates[:30]  # 预览 30 行 (Q2: 首次 dry-run 清单)
            ],
        }
    d["dup_pairs"] = [
        {
            "kind": p.kind,
            "key": p.key,
            "sessions": p.sessions,
            "message_count": len(p.message_ids),
            "content_preview": p.content_preview,
        }
        for p in r.dup_pairs
    ]
    return d


# ---------------------------------------------------------------- CLI
def _default_threshold(profile: str) -> int:
    # FIX-B3: 统一解析入口 (governance_config.resolve_threshold_mb)
    return int(cfg.resolve_threshold_mb(profile))


def main(argv: Optional[List[str]] = None) -> int:
    ap = argparse.ArgumentParser(description="Governance detector: L0 threshold + L1 exact dedup")
    ap.add_argument("--db", required=True, help="state.db path")
    ap.add_argument("--profile", default="default", help="profile name (default: default)")
    ap.add_argument("--threshold-mb", type=int, default=None, help="override threshold MB")
    ap.add_argument("--apply", action="store_true", help="actually write dedup marks (default: dry-run)")
    ap.add_argument("--json", action="store_true", help="machine-readable JSON output")
    ap.add_argument("--no-l0", action="store_true", help="skip L0 threshold check")
    ap.add_argument("--no-l1", action="store_true", help="skip L1 dedup scan")
    ap.add_argument("--target-ratio", type=float, default=None,
                    help=f"滞后带比例 (默认 {LAG_RATIO}, 阈值×ratio = 目标体积)")
    args = ap.parse_args(argv)

    if not os.path.exists(args.db):
        print(f"ERROR: db not found: {args.db}", file=sys.stderr)
        return 2

    profile = args.profile
    threshold = args.threshold_mb or _default_threshold(profile)
    errors: List[str] = []

    l0 = None
    if not args.no_l0:
        try:
            l0 = detect_threshold(args.db, profile, threshold, dry_run=not args.apply,
                                  target_ratio=args.target_ratio)
        except Exception as e:  # noqa: BLE001
            errors.append(f"L0 failed: {e}")

    pairs: List[L1DupPair] = []
    applied = 0
    if not args.no_l1:
        try:
            pairs, applicable = detect_l1_duplicates(args.db, profile, dry_run=not args.apply)
            if args.apply and applicable > 0:
                applied = apply_dedup_marks(args.db, profile, pairs)
        except Exception as e:  # noqa: BLE001
            errors.append(f"L1 failed: {e}")

    rep = build_report(profile, args.db, l0, pairs, applied, errors)
    if args.json:
        print(json.dumps(report_to_dict(rep), ensure_ascii=False, indent=2))
    else:
        print(f"=== Governance detector [{profile}] {args.db} ===")
        if l0:
            print(f"L0: {l0.db_size_bytes/1048576:.1f}MB / threshold {l0.threshold_bytes/1048576:.0f}MB "
                  f"{'OVER-LIMIT' if l0.over_limit else 'OK'}")
            # C4 (2026-08-10): 未超限也输出完整候选集 (方案 1, 与 reclaim 语义一致)
            print(f"    candidates={len(l0.candidates)} (active={l0.active}, "
                  f"protected={l0.protected}, empty={l0.empty})")
            if l0.over_limit:
                print(f"    need release ~{l0.need_release_bytes/1048576:.1f}MB")
            for c in l0.candidates[:10]:
                print(f"    - {c['session_id'][:24]:24s} msgs={c['messages']:4d} "
                      f"content={c['content_bytes']/1048576:.2f}MB "
                      f"text={c['text_bytes']/1048576:.2f}MB "
                      f"est_saved={c['est_saved_bytes']/1048576:.2f}MB")
        print(f"L1: {len(pairs)} exact dup pair(s), applied={applied}"
              f"{' (dry-run)' if not args.apply else ''}")
        for p in pairs[:10]:
            print(f"    [{p.kind}] {p.key} sessions={p.sessions} "
                  f"msgs={len(p.message_ids)} preview={p.content_preview[:40]!r}")
        if errors:
            print("ERRORS:", errors, file=sys.stderr)
    return 0 if not errors else 1


if __name__ == "__main__":
    sys.exit(main())
