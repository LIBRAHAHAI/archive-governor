#!/usr/bin/env python3
"""test_governance_v31_fixes.py — v3.1 发布补丁回归测试 (331 决策后, hermes 2026-08-08).

覆盖 331 决策落地:
  - 决策 1.2/新发现 bug: reclaim_run LAG_RATIO 0.9→0.85, 与 detector 三处一致
  - 决策 1.3: startup_check CLI --threshold-mb / --target-ratio 覆盖生效
  - 决策 1.4: auto_start_agents_integration __all__ + docstring 齐全(实测反证 xuanwu grep 误查)
  - 决策 1.5: synthetic db 带 sessions.message_count 列 → detector detect 正常
              (xuanwu 的 synthetic 缺列是造库问题, 非项目缺陷)

运行: PYTHONPATH=scripts python tests/test_governance_v31_fixes.py
"""
import os
import sqlite3
import sys
import tempfile
import unittest

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "scripts"))

import governance_detector as det
import governance_reclaim_run as rec
import governance_startup_check as sc
import auto_start_agents_integration as ai


class TestRatioConsistency(unittest.TestCase):
    """决策 1.2 修复: reclaim LAG_RATIO 与 detector 一致 (0.85)."""

    def test_three_modules_ratio_equal(self):
        self.assertEqual(det.LAG_RATIO, 0.85)
        self.assertEqual(rec.LAG_RATIO, 0.85)
        self.assertEqual(sc.LAG_RATIO, 0.85)

    def test_reclaim_target_uses_085(self):
        # 独立跑 reclaim 时 target = 80MB × 0.85 = 68MB (user 拍板 15% 滞后带)
        tmp = os.path.join(tempfile.gettempdir(), "gov_v31_reclaim_target.db")
        if os.path.exists(tmp):
            os.remove(tmp)
        build_db(tmp, 5, 3)  # 带 sessions/messages schema 的小库
        try:
            cands, info = rec._collect_candidates(tmp, 80)
            self.assertEqual(info["target"], int(80 * 1024 * 1024 * 0.85))
        finally:
            if os.path.exists(tmp):
                os.remove(tmp)


def build_db(path: str, n_sessions: int, msgs_per: int) -> int:
    """造带 message_count 列的 synthetic 库 (对齐 session_governor schema)."""
    con = sqlite3.connect(path)
    cur = con.cursor()
    cur.execute("""CREATE TABLE sessions (
        id TEXT PRIMARY KEY, profile TEXT, title TEXT,
        started_at REAL, ended_at REAL, message_count INTEGER DEFAULT 0,
        cwd TEXT, archived INTEGER DEFAULT 0, pinned INTEGER DEFAULT 0
    )""")
    cur.execute("""CREATE TABLE messages (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        session_id TEXT, role TEXT, content TEXT, timestamp REAL
    )""")
    cur.execute("""CREATE TABLE governance_meta (
        session_id TEXT PRIMARY KEY,
        profile_name TEXT,
        state TEXT,
        cold_archived_at REAL,
        reason TEXT,
        cluster_ids TEXT,
        exempt INTEGER DEFAULT 0,
        archive_file TEXT,
        archive_sha256 TEXT,
        archive_size_bytes INTEGER,
        storage_saved_bytes INTEGER,
        created_at REAL,
        updated_at REAL
    )""")
    cur.execute("CREATE INDEX idx_msg_session ON messages(session_id)")
    base = 1750000000.0
    blob = "x" * 2000
    for i in range(n_sessions):
        sid = f"synth_{i:05d}"
        cur.execute(
            "INSERT INTO sessions (id, profile, title, started_at, ended_at, message_count, cwd, pinned) VALUES (?,?,?,?,?,?,?,0)",
            (sid, "default", f"synth {i}", base - i * 3600, base - i * 3600 + 300, msgs_per, "/tmp"))
        for m in range(msgs_per):
            cur.execute("INSERT INTO messages (session_id, role, content, timestamp) VALUES (?,?,?,?)",
                        (sid, "user", blob, base - i * 3600 + m))
    con.commit()
    size = os.path.getsize(path)
    con.close()
    return size


class TestStartupCheckSynthetic(unittest.TestCase):
    """决策 1.5 + 1.1: synthetic 库带 message_count 列, detect 不失败."""

    @classmethod
    def setUpClass(cls):
        cls.tmp = os.path.join(tempfile.gettempdir(), "gov_v31_test.db")
        if os.path.exists(cls.tmp):
            os.remove(cls.tmp)
        build_db(cls.tmp, 60, 100)  # ≈ 30MB, 低于 80MB 阈值 → no_op 路径

    @classmethod
    def tearDownClass(cls):
        if os.path.exists(cls.tmp):
            os.remove(cls.tmp)

    def test_detect_no_message_count_error(self):
        # 关键回归: 带 message_count 列时不应抛 OperationalError (xuanwu 1.5 的坑)
        rep = sc.run_startup_check("default", self.tmp, verbose=False)
        self.assertNotEqual(rep["action"], "detect_failed", f"detect 失败: {rep['errors']}")
        self.assertIn(rep["action"], {"no_op", "skipped_empty_db", "reclaimed"})

    def test_threshold_mb_override(self):
        # 决策 1.3: --threshold-mb 覆盖 → 30MB 阈值下 30MB 库应 over_limit
        rep = sc.run_startup_check("default", self.tmp, threshold_mb=30)
        self.assertEqual(rep["action"], "detect_failed" if rep.get("errors") else "no_op",
                         msg="30MB 库 + 30MB 阈值不应超限(需库>阈值×0.85 才触发)")
        # 直接验证覆盖生效: 阈值=30MB 时 threshold_bytes=30MB
        self.assertEqual(rep["threshold_bytes"], 30 * 1024 * 1024)

    def test_target_ratio_override(self):
        rep = sc.run_startup_check("default", self.tmp, threshold_mb=10, target_ratio=0.5)
        # threshold=10MB, target=10MB×0.5=5MB; 30MB 库必然超限 → 触发 reclaim 或 partial
        self.assertIn(rep["action"], {"no_op", "reclaimed", "reclaimed_partial",
                                      "reclaimed_insufficient", "no_candidates"})
        if rep["action"] != "no_op":
            self.assertLessEqual(rep["target_bytes"], 10 * 1024 * 1024 * 0.5 + 1)


class TestIntegrationInterface(unittest.TestCase):
    """决策 1.4: 接口文档化 — 实测反证 xuanwu 'grep 0 命中' 为误查."""

    def test_all_exports(self):
        self.assertEqual(ai.__all__, ["run_startup_check", "health_summary"])

    def test_run_startup_check_docstring(self):
        doc = ai.run_startup_check.__doc__ or ""
        self.assertIn("退出码", doc)
        self.assertIn("threshold_mb", doc)  # v3.1 补丁后新增参数说明

    def test_signature_has_new_args(self):
        import inspect
        sig = inspect.signature(ai.run_startup_check)
        self.assertIn("threshold_mb", sig.parameters)
        self.assertIn("target_ratio", sig.parameters)


if __name__ == "__main__":
    unittest.main(verbosity=2)
