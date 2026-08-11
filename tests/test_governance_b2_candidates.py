#!/usr/bin/env python3
"""test_governance_b2_candidates.py — FIX-B2: 排除已归档与零热消息虚候选隔离回归 (2026-08-09).

背景: PRODUCTION-DB-DISCOVERY.md B2 — detector/reclaim 候选 SQL 未排除 archived=1
     会话, default 库 477 候选中有 292 个已归档 0 热消息会话 (est_saved=0 虚候选),
     虚增候选数并触发"已置 archived"跳过噪音。

本测试验证 FIX-B2 落地 (detector 与 reclaim 同步修复, 不漂移):
  1. 已归档 0-message 会话不入候选 (archived=1 + 无热行);
  2. 已归档但残留热行的会话 (异常态) 也不入候选;
  3. 非归档零热消息空会话不入候选 (est_saved=0 虚候选);
  4. 真实热候选 (未保护 / 非活跃 / 有消息) 仍入选;
  5. 保护对象 (active / pinned / exempt) 仍排除;
  6. detector 与 reclaim 候选集完全一致 (SQL/语义一致性);
  7. reclaim CLI --json --dry-run 输出无 msgs=0 噪声。

运行: PYTHONPATH=scripts python tests/test_governance_b2_candidates.py
"""
import json as _json
import os
import sqlite3
import subprocess
import sys
import tempfile
import time
import unittest

TESTS_DIR = os.path.dirname(os.path.abspath(__file__))
SCRIPTS_DIR = os.path.join(TESTS_DIR, "..", "scripts")
sys.path.insert(0, SCRIPTS_DIR)

import governance_detector as det  # noqa: E402
import governance_reclaim_run as rec  # noqa: E402

RECLAIM_PY = os.path.join(SCRIPTS_DIR, "governance_reclaim_run.py")

OLD = 7 * 86400  # 7 天前 → 超出 24h 活跃窗口


def build_db(path: str) -> None:
    """造 synthetic 库, 覆盖 B2 全部场景:

    - hot_old_1/2 : 真实热候选 (已结束, 3 条消息, 非活跃, 未保护) -> 应入选
    - empty_ended : 非归档零热消息空会话                        -> 排除 (empty)
    - arch_empty  : 已归档 0 热消息会话                         -> 排除 (archived)
    - arch_hot    : 已归档残留热行 (异常态)                     -> 排除 (archived)
    - active_new  : 1h 内写入 (活跃保护)                        -> 排除 (active)
    - pinned_old  : pinned 保护                                 -> 排除 (pinned)
    - exempt_old  : governance_meta.exempt=1 保护               -> 排除 (exempt)
    """
    if os.path.exists(path):
        os.remove(path)
    con = sqlite3.connect(path)
    cur = con.cursor()
    cur.execute("""CREATE TABLE sessions (
        id TEXT PRIMARY KEY, session_key TEXT, profile_name TEXT,
        started_at REAL, ended_at REAL, message_count INTEGER DEFAULT 0,
        pinned INTEGER DEFAULT 0, archived INTEGER DEFAULT 0)""")
    cur.execute("""CREATE TABLE messages (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        session_id TEXT, role TEXT, content TEXT, timestamp REAL)""")
    cur.execute("""CREATE TABLE governance_meta (
        session_id TEXT PRIMARY KEY, profile_name TEXT,
        state TEXT, cold_archived_at REAL, reason TEXT, cluster_ids TEXT,
        exempt INTEGER DEFAULT 0, archive_file TEXT, archive_sha256 TEXT,
        archive_size_bytes INTEGER, storage_saved_bytes INTEGER,
        created_at REAL, updated_at REAL)""")
    cur.execute("CREATE INDEX idx_msg_session ON messages(session_id)")
    now = time.time()
    blob = "y" * 2000

    def _sess(sid, started_ago, ended_ago, pinned=0, archived=0, msgs=3):
        cur.execute(
            "INSERT INTO sessions (id, session_key, profile_name, started_at, ended_at,"
            " message_count, pinned, archived) VALUES (?,?,?,?,?,?,?,?)",
            (sid, None, "default", now - started_ago,
             None if ended_ago is None else now - ended_ago, msgs, pinned, archived))

    def _msgs(sid, started_ago):
        for j in range(3):
            cur.execute(
                "INSERT INTO messages (session_id, role, content, timestamp) VALUES (?,?,?,?)",
                (sid, "user", f"{blob} {sid}-{j}", now - started_ago + j))

    _sess("hot_old_1", OLD + 3600, OLD - 3600)
    _msgs("hot_old_1", OLD + 3600)
    _sess("hot_old_2", OLD + 7200, OLD - 7200)
    _msgs("hot_old_2", OLD + 7200)
    _sess("empty_ended", OLD + 9000, OLD - 9000, msgs=0)          # 空会话
    _sess("arch_empty", OLD + 10000, OLD - 10000, archived=1, msgs=0)  # 已归档空
    _sess("arch_hot", OLD + 11000, OLD - 11000, archived=1)       # 已归档残留热行
    _msgs("arch_hot", OLD + 11000)
    _sess("active_new", 3600, 1800)                               # 活跃 (1h 内)
    _msgs("active_new", 3600)
    _sess("pinned_old", OLD + 12000, OLD - 12000, pinned=1)
    _msgs("pinned_old", OLD + 12000)
    _sess("exempt_old", OLD + 13000, OLD - 13000)
    _msgs("exempt_old", OLD + 13000)
    cur.execute(
        "INSERT INTO governance_meta (session_id, profile_name, state, exempt)"
        " VALUES (?,?,?,?)", ("exempt_old", "default", "active", 1))
    con.commit()
    con.close()


def build_db_edge(path: str) -> None:
    """边界 fixture: 无消息+未结束+started<24h 会话 (归 active 而非 empty).

    用于验证 detector 与 reclaim 对「空但新开」会话的分类计数一致
    (detector._session_is_active: 无消息 + ended_at IS NULL + started 在窗口内 → active)。
    """
    if os.path.exists(path):
        os.remove(path)
    con = sqlite3.connect(path)
    cur = con.cursor()
    cur.execute("""CREATE TABLE sessions (
        id TEXT PRIMARY KEY, session_key TEXT, profile_name TEXT,
        started_at REAL, ended_at REAL, message_count INTEGER DEFAULT 0,
        pinned INTEGER DEFAULT 0, archived INTEGER DEFAULT 0)""")
    cur.execute("""CREATE TABLE messages (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        session_id TEXT, role TEXT, content TEXT, timestamp REAL)""")
    cur.execute("""CREATE TABLE governance_meta (
        session_id TEXT PRIMARY KEY, profile_name TEXT,
        state TEXT, cold_archived_at REAL, reason TEXT, cluster_ids TEXT,
        exempt INTEGER DEFAULT 0, archive_file TEXT, archive_sha256 TEXT,
        archive_size_bytes INTEGER, storage_saved_bytes INTEGER,
        created_at REAL, updated_at REAL)""")
    cur.execute("CREATE INDEX idx_msg_session ON messages(session_id)")
    now = time.time()
    blob = "y" * 2000
    # 已结束 + 3 消息 → 真实候选
    cur.execute(
        "INSERT INTO sessions (id, session_key, profile_name, started_at, ended_at,"
        " message_count, pinned, archived) VALUES (?,?,?,?,?,?,?,?)",
        ("hot_old_1", None, "default", now - (OLD + 3600), now - (OLD - 3600), 3, 0, 0))
    for j in range(3):
        cur.execute(
            "INSERT INTO messages (session_id, role, content, timestamp) VALUES (?,?,?,?)",
            ("hot_old_1", "user", f"{blob} hot_old_1-{j}", now - (OLD + 3600) + j))
    # 无消息 + 未结束 + started 1h 前 → 活跃保护 (非 empty)
    cur.execute(
        "INSERT INTO sessions (id, session_key, profile_name, started_at, ended_at,"
        " message_count, pinned, archived) VALUES (?,?,?,?,?,?,?,?)",
        ("empty_new_open", None, "default", now - 3600, None, 0, 0, 0))
    # 无消息 + 已结束 (7 天前) → empty
    cur.execute(
        "INSERT INTO sessions (id, session_key, profile_name, started_at, ended_at,"
        " message_count, pinned, archived) VALUES (?,?,?,?,?,?,?,?)",
        ("empty_ended", None, "default", now - (OLD + 9000), now - (OLD - 9000), 0, 0, 0))
    con.commit()
    con.close()


def _det_candidate_ids(db: str):
    r = det.detect_threshold(db, "default", threshold_mb=0)
    return {c["session_id"] for c in r.candidates}, r


def _rec_candidate_ids(db: str):
    cands, info = rec._collect_candidates(db, 0)
    return {c["session_id"] for c in cands}, cands, info


class TestDetectorCandidates(unittest.TestCase):
    """detector L0 候选: 已归档 / 零热消息 排除, 真实热候选保留, 保护不弱化."""

    def setUp(self):
        self.tmpdir = tempfile.mkdtemp(prefix="gov_b2_det_")
        self.db = os.path.join(self.tmpdir, "det.db")
        build_db(self.db)

    def tearDown(self):
        import shutil
        shutil.rmtree(self.tmpdir, ignore_errors=True)

    def test_archived_and_empty_excluded_hot_kept(self):
        sids, r = _det_candidate_ids(self.db)
        # 真实热候选仍入选
        self.assertIn("hot_old_1", sids)
        self.assertIn("hot_old_2", sids)
        # 已归档 / 空会话全部排除
        for bad in ("arch_empty", "arch_hot", "empty_ended"):
            self.assertNotIn(bad, sids, f"{bad} 不得进入候选")
        # 保护对象仍排除
        for prot in ("active_new", "pinned_old", "exempt_old"):
            self.assertNotIn(prot, sids, f"{prot} 保护不得被弱化")
        # 候选 = 恰好 2 个真实热候选
        self.assertEqual(sids, {"hot_old_1", "hot_old_2"}, f"候选集合异常: {sids}")
        # 计数: empty≥1 (empty_ended; 已归档空会话由 SQL 层排除, 不进 empty 计数), active=1, protected=1
        self.assertGreaterEqual(r.empty, 1, f"empty 计数: {r.empty}")
        self.assertGreaterEqual(r.active, 1)
        self.assertGreaterEqual(r.protected, 1)
        # 无 est_saved=0 噪声
        self.assertTrue(all(c["est_saved_bytes"] > 0 and c["messages"] > 0
                            for c in r.candidates),
                        "候选不得出现 msgs=0 / est_saved=0 噪声")

    def test_report_dict_has_empty_sessions(self):
        _, r = _det_candidate_ids(self.db)
        d = det.report_to_dict(det.build_report("default", self.db, r, [], 0, []))
        self.assertIn("empty_sessions", d["l0"])
        self.assertGreaterEqual(d["l0"]["empty_sessions"], 1)
        for c in d["l0"]["candidates"]:
            self.assertGreater(c["messages"], 0)

    def test_new_open_empty_session_counts_as_active_both_entries(self):
        """无消息+未结束+started<24h 会话: detector 与 reclaim 均归 active, 计数一致.

        使用独立边缘 fixture (不污染共享 build_db, 防 C4 复用断言漂移).
        """
        db = os.path.join(self.tmpdir, "edge.db")
        build_db_edge(db)
        det_ids, r = _det_candidate_ids(db)
        rec_ids, cands, info = _rec_candidate_ids(db)
        self.assertNotIn("empty_new_open", det_ids)   # 不入候选 (活跃保护)
        self.assertNotIn("empty_new_open", rec_ids)
        self.assertNotIn("empty_new_open", [c["session_id"] for c in cands])
        # 归 active 而非 empty: 与 detector._session_is_active 语义一致
        self.assertEqual(r.active, 1, f"empty_new_open 应计 active: {r.active}")
        self.assertEqual(r.empty, 1, f"empty_ended 应计 empty: {r.empty}")
        self.assertEqual(info["empty"], r.empty, "reclaim 与 detector empty 计数必须一致")
        self.assertEqual(info["active"], r.active, "reclaim 与 detector active 计数必须一致")
        # 候选集仍一致: 只有 hot_old_1
        self.assertEqual(det_ids, rec_ids, "detector 与 reclaim 候选集必须一致")
        self.assertEqual(det_ids, {"hot_old_1"})


class TestReclaimConsistency(unittest.TestCase):
    """reclaim 与 detector 候选语义一致 (SQL/常量不漂移)."""

    def setUp(self):
        self.tmpdir = tempfile.mkdtemp(prefix="gov_b2_rec_")
        self.db = os.path.join(self.tmpdir, "rec.db")
        build_db(self.db)

    def tearDown(self):
        import shutil
        shutil.rmtree(self.tmpdir, ignore_errors=True)

    def test_reclaim_matches_detector_candidate_set(self):
        det_ids, r = _det_candidate_ids(self.db)
        rec_ids, cands, info = _rec_candidate_ids(self.db)
        self.assertEqual(rec_ids, det_ids, "reclaim 与 detector 候选集不一致 (逻辑漂移)")
        self.assertEqual(info["empty"], r.empty)
        self.assertEqual(info["candidates"], len(cands))
        for c in cands:
            self.assertGreater(c["messages"], 0, "reclaim 候选不得含 msgs=0")
            self.assertGreater(c["est_saved_bytes"], 0, "reclaim 候选不得含 est_saved=0")
        for bad in ("arch_empty", "arch_hot", "empty_ended",
                    "active_new", "pinned_old", "exempt_old"):
            self.assertNotIn(bad, rec_ids)


class TestReclaimCliJsonDryRun(unittest.TestCase):
    """reclaim CLI --json --dry-run: plan 输出无已归档/零热消息噪声."""

    def setUp(self):
        self.tmpdir = tempfile.mkdtemp(prefix="gov_b2_cli_")
        self.db = os.path.join(self.tmpdir, "cli.db")
        build_db(self.db)

    def tearDown(self):
        import shutil
        shutil.rmtree(self.tmpdir, ignore_errors=True)

    def test_json_plan_has_no_msgs0_noise(self):
        env = dict(os.environ)
        env["PYTHONPATH"] = SCRIPTS_DIR + os.pathsep + env.get("PYTHONPATH", "")
        proc = subprocess.run(
            [sys.executable, RECLAIM_PY, "--db", self.db, "--profile", "default",
             "--threshold-mb", "0", "--archive-root", self.tmpdir, "--json", "--dry-run"],
            capture_output=True, text=True, env=env)
        self.assertEqual(proc.returncode, 0, proc.stderr)
        payload = _json.loads(proc.stdout)
        self.assertEqual(payload["phase"], "plan")
        self.assertIs(payload["dry_run"], True)
        info = payload["info"]
        self.assertIn("empty", info)
        self.assertGreaterEqual(info["empty"], 1)
        self.assertEqual(info["candidates"], 2)
        for c in payload["candidates"]:
            self.assertGreater(c["messages"], 0)
            self.assertGreater(c["est_saved_bytes"], 0)
            self.assertNotIn(c["session_id"], ("arch_empty", "arch_hot", "empty_ended",
                                               "active_new", "pinned_old", "exempt_old"))


if __name__ == "__main__":
    unittest.main(verbosity=2)
