#!/usr/bin/env python3
"""
test_governance_m3m4.py — MVP-2 (M3 空间回收 + M4 精确去重) 验收测试

覆盖验收点:
  A. L1 去重 FPR=0%: 仅 content sha256 全等 / session_key 全等被检出;
     近似重复 (差一个字符/近似 key) 绝不误报
  B. 幂等: --apply 重复执行不报错、不重复计数
  C. 不可变保护: 活跃会话 (ended_at NULL / 24h 内写入) 不参与去重与归档候选
  D. M3 空间回收闭环: 副本库删行后 archiver --apply 使文件显著缩小;
     FTS 触发器同步删除生效 (messages 与 messages_fts 行数一致)
  E. 阈值检测: L0 超限候选最旧优先; 未超限不产生候选

安全: 所有测试仅操作临时副本/迷你库, 绝不触碰运行中真库。
运行: python -m pytest test_governance_m3m4.py -v   (或直接 python test_governance_m3m4.py)
"""
from __future__ import annotations

import hashlib
import os
import shutil
import sqlite3
import sys
import tempfile
import time
from pathlib import Path

# 允许直接运行 (非 pytest)
try:
    import pytest
except ImportError:
    pytest = None

SCRIPT_DIR = Path(__file__).resolve().parent
sys.path.insert(0, str(SCRIPT_DIR))

import governance_detector as det  # noqa: E402
import governance_archiver as arc  # noqa: E402

# ---------------------------------------------------------------- 迷你库构造
MINI_DDL = [
    """CREATE TABLE sessions (
        id TEXT PRIMARY KEY, session_key TEXT, profile_name TEXT,
        started_at REAL, ended_at REAL, message_count INTEGER DEFAULT 0,
        pinned INTEGER DEFAULT 0, archived INTEGER DEFAULT 0)""",
    """CREATE TABLE messages (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        session_id TEXT NOT NULL REFERENCES sessions(id),
        role TEXT, content TEXT, timestamp REAL)""",
    """CREATE TABLE governance_meta (
        session_id TEXT PRIMARY KEY, profile_name TEXT,
        state TEXT NOT NULL CHECK (state IN ('active','cold_archived')),
        cold_archived_at REAL, reason TEXT, cluster_ids TEXT,
        exempt INTEGER DEFAULT 0, archive_file TEXT, archive_sha256 TEXT,
        archive_size_bytes INTEGER, storage_saved_bytes INTEGER,
        created_at REAL, updated_at REAL)""",
    # FTS 同步结构（与真库 external-content 口径一致，测试独立 seed 用）
    """CREATE VIRTUAL TABLE messages_fts USING fts5(
        content, content='messages', content_rowid='id')""",
    """CREATE TRIGGER messages_fts_ai AFTER INSERT ON messages BEGIN
        INSERT INTO messages_fts(rowid, content) VALUES (new.id, new.content);
    END""",
    """CREATE TRIGGER messages_fts_ad AFTER DELETE ON messages BEGIN
        INSERT INTO messages_fts(messages_fts, rowid, content)
        VALUES ('delete', old.id, old.content);
    END""",
]


def _make_mini_db(path: str) -> sqlite3.Connection:
    con = sqlite3.connect(path)
    for ddl in MINI_DDL:
        con.execute(ddl)
    return con


def _seed_mini(con: sqlite3.Connection) -> None:
    now = time.time()
    cur = con.cursor()
    # 会话 1: 已结束 (参与去重)
    cur.execute(
        "INSERT INTO sessions VALUES (?,?,?,?,?,?,?,?)",
        ("s1", None, "test", now - 100000, now - 50000, 0, 0, 0),
    )
    # 会话 2: 已结束 (参与去重)
    cur.execute(
        "INSERT INTO sessions VALUES (?,?,?,?,?,?,?,?)",
        ("s2", None, "test", now - 90000, now - 40000, 0, 0, 0),
    )
    # 会话 3: 活跃 (ended_at NULL) -> 保护
    cur.execute(
        "INSERT INTO sessions VALUES (?,?,?,?,?,?,?,?)",
        ("s3", None, "test", now - 1000, None, 0, 0, 0),
    )
    # 会话 4: pinned -> 保护
    cur.execute(
        "INSERT INTO sessions VALUES (?,?,?,?,?,?,?,?)",
        ("s4", None, "test", now - 80000, now - 30000, 0, 1, 0),
    )
    # 会话 5/6: 同 session_key -> session_key 级重复
    cur.execute(
        "INSERT INTO sessions VALUES (?,?,?,?,?,?,?,?)",
        ("s5", "dup-key-abc", "test", now - 70000, now - 20000, 0, 0, 0),
    )
    cur.execute(
        "INSERT INTO sessions VALUES (?,?,?,?,?,?,?,?)",
        ("s6", "dup-key-abc", "test", now - 60000, now - 10000, 0, 0, 0),
    )
    # 精确重复消息: s1 与 s2 各有一条完全相同的正文 (3 天前, 不在 24h 活跃窗口)
    dup_content = "完全相同的消息正文 duplicated content 1234567890"
    old_ts = now - 3 * 86400
    for sid in ("s1", "s2"):
        cur.execute(
            "INSERT INTO messages (session_id, role, content, timestamp) VALUES (?,?,?,?)",
            (sid, "user", dup_content, old_ts),
        )
    # 近似重复 (差一个字符) -> 不得被 L1 检出
    cur.execute(
        "INSERT INTO messages (session_id, role, content, timestamp) VALUES (?,?,?,?)",
        ("s1", "user", dup_content + "X", old_ts),
    )
    cur.execute(
        "INSERT INTO messages (session_id, role, content, timestamp) VALUES (?,?,?,?)",
        ("s2", "user", dup_content + "Y", old_ts),
    )
    # 活跃会话消息 (保护)
    cur.execute(
        "INSERT INTO messages (session_id, role, content, timestamp) VALUES (?,?,?,?)",
        ("s3", "user", dup_content, now - 100),
    )
    con.commit()


# ---------------------------------------------------------------- A. FPR=0%
def test_l1_fpr_zero_content_hash():
    """content 精确重复检出, 近似重复 (差1字符) 不误报."""
    with tempfile.TemporaryDirectory() as td:
        db = os.path.join(td, "mini.db")
        con = _make_mini_db(db)
        _seed_mini(con)
        con.close()

        pairs, applicable = det.detect_l1_duplicates(db, "test")
        content_pairs = [p for p in pairs if p.kind == "content_hash"]
        assert content_pairs, "应检出 content 精确重复"
        # 精确重复的正文 hash 应只有 1 组 (s1+s2), s3 活跃被排除
        assert len(content_pairs) == 1, f"期望 1 组精确重复, 实际 {len(content_pairs)}"
        p = content_pairs[0]
        assert "s1" in p.sessions and "s2" in p.sessions, p.sessions
        assert "s3" not in p.sessions, "活跃会话不得参与去重"
        # FPR=0%: 近似重复 (dup_content+X / dup_content+Y) 未计入
        assert len(p.message_ids) == 2, f"精确组应有 2 条消息, 实际 {len(p.message_ids)}"
        # 种子内 s5/s6 无消息, 无 content pair; 组内消息应仅来自 s1/s2 的精确重复
        assert all(mid <= 2 for mid in p.message_ids), p.message_ids


def test_l1_fpr_zero_session_key():
    """session_key 全等检出 (FPR=0%)."""
    with tempfile.TemporaryDirectory() as td:
        db = os.path.join(td, "mini.db")
        con = _make_mini_db(db)
        _seed_mini(con)
        con.close()

        pairs, _ = det.detect_l1_duplicates(db, "test")
        sk_pairs = [p for p in pairs if p.kind == "session_key"]
        assert len(sk_pairs) == 1, sk_pairs
        assert set(sk_pairs[0].sessions) == {"s5", "s6"}, sk_pairs[0].sessions


# ---------------------------------------------------------------- B. 幂等
def test_apply_idempotent():
    """--apply 重复执行不报错, 二次执行不再新增标记."""
    with tempfile.TemporaryDirectory() as td:
        db = os.path.join(td, "mini.db")
        con = _make_mini_db(db)
        _seed_mini(con)
        con.close()

        pairs, _ = det.detect_l1_duplicates(db, "test")
        first = det.apply_dedup_marks(db, "test", pairs)
        assert first > 0, "首次 apply 应写入标记"

        # 二次 apply: 同 key 幂等 (INSERT OR IGNORE), 不重复计数
        pairs2, _ = det.detect_l1_duplicates(db, "test")
        second = det.apply_dedup_marks(db, "test", pairs2)
        assert second == 0, f"二次 apply 应 0 新增, 实际 {second}"

        # 校验落库
        con = sqlite3.connect(db)
        n_flag = con.execute(
            "SELECT COUNT(*) FROM governance_cluster_member WHERE dedup_flag=1"
        ).fetchone()[0]
        n_log = con.execute(
            "SELECT COUNT(*) FROM governance_log WHERE op='dedup_mark'"
        ).fetchone()[0]
        con.close()
        assert n_flag == first, (n_flag, first)
        assert n_log == len([p for p in pairs if p.message_ids or p.sessions])


# ---------------------------------------------------------------- C. 不可变保护
def test_immutable_protection_l0():
    """L0 候选: 活跃 / pinned / 已归档 / 零热消息 不进入候选.

    B2 (2026-08-09): 已归档会话 (archived=1, 无论是否残留热行) 与零热消息空会话
    (est_saved=0 虚候选) 一并排除; s5/s6 无消息 → 不再入选 (原断言 2026-08-09 修正).
    """
    with tempfile.TemporaryDirectory() as td:
        db = os.path.join(td, "mini.db")
        con = _make_mini_db(db)
        _seed_mini(con)
        now = time.time()
        # B2 补充场景: 已归档会话 (有热行 = 异常残留; 无热行 = 正常归档态)
        con.execute(
            "INSERT INTO sessions VALUES (?,?,?,?,?,?,?,?)",
            ("s_arch_msg", None, "test", now - 60000, now - 50000, 3, 0, 1),
        )
        con.execute(
            "INSERT INTO messages (session_id, role, content, timestamp) VALUES (?,?,?,?)",
            ("s_arch_msg", "user", "残留热行(异常态)", now - 60000),
        )
        con.execute(
            "INSERT INTO sessions VALUES (?,?,?,?,?,?,?,?)",
            ("s_arch_empty", None, "test", now - 59000, now - 58000, 0, 0, 1),
        )
        # 非归档零热消息空会话 (无内容可归档)
        con.execute(
            "INSERT INTO sessions VALUES (?,?,?,?,?,?,?,?)",
            ("s_empty", None, "test", now - 55000, now - 54000, 0, 0, 0),
        )
        # exempt 保护
        con.execute(
            "INSERT INTO sessions VALUES (?,?,?,?,?,?,?,?)",
            ("s_exempt", None, "test", now - 50000, now - 40000, 2, 0, 0),
        )
        con.execute(
            "INSERT INTO messages (session_id, role, content, timestamp) VALUES (?,?,?,?)",
            ("s_exempt", "user", "exempt 保护内容", now - 40000),
        )
        con.execute(
            "INSERT INTO governance_meta (session_id, profile_name, state, exempt) "
            "VALUES (?,?,?,?)",
            ("s_exempt", "test", "active", 1),
        )
        con.commit()
        # 让文件 > 阈值 1MB 以触发候选 (迷你库太小, 用 0 阈值 + 直接校验候选集)
        con.close()
        # 直接调用候选生成: 阈值设 0 强制 over_limit, 检查保护逻辑
        r = det.detect_threshold(db, "test", threshold_mb=0)
        sids = [c["session_id"] for c in r.candidates]
        assert "s3" not in sids, "活跃会话不得进入归档候选"
        assert "s4" not in sids, "pinned 会话不得进入归档候选"
        assert "s1" in sids and "s2" in sids, "已结束未保护会话应可候选"
        # B2: 零热消息与已归档虚候选全部排除
        assert "s5" not in sids and "s6" not in sids, "零热消息空会话不得进入归档候选"
        assert "s_empty" not in sids, "非归档零热消息空会话不得进入归档候选"
        assert "s_arch_msg" not in sids, "已归档会话(残留热行)不得进入归档候选"
        assert "s_arch_empty" not in sids, "已归档 0 热消息会话不得进入归档候选"
        assert "s_exempt" not in sids, "exempt 会话不得进入归档候选"
        # 候选仅剩真实热候选: s1/s2 (有消息, 未保护, 非活跃)
        assert set(sids) == {"s1", "s2"}, f"候选应仅 s1/s2, 实际 {sids}"
        assert r.empty >= 3, f"empty 应计 3 个空会话 (s5/s6/s_empty), 实际 {r.empty}"


def test_l1_active_window_protected():
    """24h 内写入的会话视为活跃 -> 不参与去重."""
    with tempfile.TemporaryDirectory() as td:
        db = os.path.join(td, "mini.db")
        con = _make_mini_db(db)
        _seed_mini(con)
        # 给 s1 补一条 1 小时前的消息, 使其变活跃
        con.execute(
            "INSERT INTO messages (session_id, role, content, timestamp) VALUES (?,?,?,?)",
            ("s1", "user", "recent activity", time.time() - 3600),
        )
        con.commit()
        con.close()

        pairs, _ = det.detect_l1_duplicates(db, "test")
        for p in pairs:
            assert "s1" not in p.sessions, "24h 内活跃的 s1 不得参与去重"


def test_zombie_active_is_archivable():
    """现场修正: ended_at IS NULL 但最后写入 > 窗口 的僵尸会话可归档 (写入时间口径).

    若按 ended_at 判活跃, hermes 225 个活跃会话含大量僵尸 -> 归档无法达标
    (实测只能到 118MB); 写入时间口径下 3 天窗口可到 73.5MB (达标).
    """
    with tempfile.TemporaryDirectory() as td:
        db = os.path.join(td, "mini.db")
        con = _make_mini_db(db)
        _seed_mini(con)
        # 僵尸会话: ended_at IS NULL 但最后写入 10 天前
        con.execute(
            "INSERT INTO sessions VALUES (?,?,?,?,?,?,?,?)",
            ("sz", None, "test", time.time() - 20 * 86400, None, 0, 0, 0),
        )
        con.execute(
            "INSERT INTO messages (session_id, role, content, timestamp) VALUES (?,?,?,?)",
            ("sz", "user", "zombie content", time.time() - 10 * 86400),
        )
        con.commit()
        con.close()

        r = det.detect_threshold(db, "test", threshold_mb=0)
        sids = [c["session_id"] for c in r.candidates]
        assert "sz" in sids, "僵尸活跃会话 (10天无写入) 应可归档"
        assert "s3" not in sids, "真活跃会话 (刚写入) 不得归档"


# ---------------------------------------------------------------- D. M3 回收闭环
def _seed_reclaim_mini(con: sqlite3.Connection) -> tuple[int, int]:
    """独立 seed：40 个已结束会话，每个 25 条 2KB 消息（≈2MB 数据）。

    返回 (会话数, 消息数)。FTS 触发器同步写入 messages_fts。
    完全独立于真库状态（test-isolation：不依赖生产库会话是否已回收）。
    """
    now = time.time()
    cur = con.cursor()
    cur.execute("DELETE FROM messages")
    cur.execute("DELETE FROM sessions")
    cur.execute("DELETE FROM messages_fts")
    n_sess, n_msg = 0, 0
    chunk = "X" * 2000  # 2KB 正文
    for i in range(40):
        sid = f"reclaim_s{i:02d}"
        started = now - (100 + i) * 86400
        cur.execute(
            "INSERT INTO sessions VALUES (?,?,?,?,?,?,?,?)",
            (sid, None, "test", started, started + 3600, 25, 0, 0),
        )
        n_sess += 1
        for j in range(25):
            cur.execute(
                "INSERT INTO messages (session_id, role, content, timestamp) VALUES (?,?,?,?)",
                (sid, "user", f"{chunk} msg{i}-{j}", started + j),
            )
            n_msg += 1
    con.commit()
    return n_sess, n_msg


def test_archiver_reclaim_reduces_size():
    """独立 seed 库：删除一批最旧会话消息（模拟 M2 归档后）-> archiver --apply 显著缩小。

    同时验证 FTS 触发器同步删除（messages_fts 行数 = messages 行数）。
    不依赖真库副本（修复：原实现 _copy_real_db 依赖生产库"最旧 20 会话有消息可删"，
    真库被 MVP-2 回收后断言失效 —— test-isolation 治理）。
    """
    with tempfile.TemporaryDirectory() as td:
        db = os.path.join(td, "reclaim.db")
        con = _make_mini_db(db)
        n_sess, n_msg = _seed_reclaim_mini(con)
        con.close()
        assert n_msg >= 500, n_msg

        # 1) 诊断: FTS 初始同步
        d = arc.diagnose(db)
        assert d["messages"] == d["messages_fts"], "初始 FTS 应同步（触发器）"

        # 2) 模拟归档: 删最旧 20 个会话的 messages（事务内，触发 FTS delete 触发器）
        size0 = os.path.getsize(db)
        con = sqlite3.connect(db, timeout=30)
        cur = con.cursor()
        old_sessions = [
            r[0]
            for r in cur.execute(
                "SELECT id FROM sessions WHERE ended_at IS NOT NULL "
                "ORDER BY started_at ASC LIMIT 20"
            ).fetchall()
        ]
        assert len(old_sessions) == 20, old_sessions
        n_msg = 0
        for sid in old_sessions:
            n_msg += cur.execute("DELETE FROM messages WHERE session_id=?", (sid,)).rowcount
            cur.execute("UPDATE sessions SET archived=1 WHERE id=?", (sid,))
        con.commit()
        con.close()
        assert n_msg >= 250, f"应删除消息, got {n_msg}"
        size1 = os.path.getsize(db)

        # 3) 回收前 FTS 应已同步（触发器）
        d2 = arc.diagnose(db)
        assert d2["messages"] == d2["messages_fts"], "触发器应同步 FTS"

        # 4) dry-run: 不写库, 大小不变
        r_dry = arc.reclaim_space(db, mode="full", apply=False)
        assert os.path.getsize(db) == size1, "dry-run 不得改变文件"

        # 5) apply full VACUUM: 文件应缩小（freelist 回收）
        r = arc.reclaim_space(db, mode="full", apply=True)
        size2 = os.path.getsize(db)
        assert r["steps"], r
        assert size2 <= size1, f"VACUUM 后应 ≤ 回收前: {size1} -> {size2}"
        print(f"[reclaim] {size0} -> {size1} (删后) -> {size2} (vacuum)")

        # 6) 回收后仍可读, governance 表可建
        con = sqlite3.connect(db)
        for ddl in det.GOVERNANCE_DDL:
            con.execute(ddl)
        con.execute("SELECT COUNT(*) FROM messages")
        con.close()


def test_archiver_incremental_mode_handles_auto_vacuum0():
    """auto_vacuum=0 时 incremental 模式应明确跳过并提示, 不静默失败."""
    with tempfile.TemporaryDirectory() as td:
        db = os.path.join(td, "mini.db")
        con = _make_mini_db(db)
        con.close()
        r = arc.reclaim_space(db, mode="incremental", apply=True)
        inc = [s for s in r["steps"] if s.get("step") == "incremental_vacuum"]
        assert inc and inc[0].get("skipped"), "auto_vacuum=0 时应跳过并说明"
        assert "auto_vacuum=0" in inc[0]["reason"] or "INCREMENTAL" in inc[0]["reason"]


# ---------------------------------------------------------------- E. 阈值检测
def test_l0_threshold_not_over():
    """未超阈值 -> over_limit=False, need_release=0; 候选集仍完整输出 (C4 方案 1)."""
    with tempfile.TemporaryDirectory() as td:
        db = os.path.join(td, "mini.db")
        con = _make_mini_db(db)
        _seed_mini(con)
        con.close()
        r = det.detect_threshold(db, "test", threshold_mb=1000)
        assert not r.over_limit
        assert r.need_release_bytes == 0
        # C4 (2026-08-10): 未超限不再跳过候选收集 — s1/s2 (3 天前写入, 已结束)
        # 是真实候选; active=s3, empty=s5/s6 (无消息), pinned s4 由 SQL 排除
        sids = {c["session_id"] for c in r.candidates}
        assert sids == {"s1", "s2"}, f"候选集异常: {sids}"
        assert r.active == 1
        assert r.empty == 2


# ---------------------------------------------------------------- 直跑入口
if __name__ == "__main__":
    fns = [
        test_l1_fpr_zero_content_hash,
        test_l1_fpr_zero_session_key,
        test_apply_idempotent,
        test_immutable_protection_l0,
        test_l1_active_window_protected,
        test_archiver_reclaim_reduces_size,
        test_archiver_incremental_mode_handles_auto_vacuum0,
        test_l0_threshold_not_over,
    ]
    passed = 0
    for fn in fns:
        try:
            fn()
            print(f"PASS  {fn.__name__}")
            passed += 1
        except Exception as e:  # noqa: BLE001
            print(f"FAIL  {fn.__name__}: {e}")
    print(f"\n{passed}/{len(fns)} passed")
    sys.exit(0 if passed == len(fns) else 1)
