#!/usr/bin/env python3
"""test_governance_c4_semantics.py — C4: 未超限候选集语义统一回归 (2026-08-10).

背景: PRODUCTION-DB-DISCOVERY.md C4 — detector `detect_threshold` L181-182
     `if not res.over_limit: return res` 未超限直接返回, 跳过候选收集,
     与 reclaim `_collect_candidates` 无条件收集语义不一致。
     独立复核 11:14 实测: 3 个非超限 profile detector archive_candidates=0,
     而 reclaim dry-run 完整候选 67/33/19。

修复 (user 拍板方案 1): 删除提前返回, 未超限也输出完整候选集报告。
本测试验证:
  1. 未超限时 detector.candidates == reclaim 候选集 (ID 集合一致);
  2. active/protected/empty 统计两模块一致;
  3. over_limit=False 时 need_release_bytes=0, "未超限无需回收"业务结论不变;
  4. report_to_dict JSON 全量 (archive_candidates 反映真实候选数, 非 0);
  5. detector CLI 文本输出未超限时包含候选统计;
  6. startup_check 未超限日志 candidates 用真实数 (不再硬编码 0)。

运行: PYTHONPATH=scripts python -m unittest discover -s tests -v
"""
import json as _json
import os
import subprocess
import sys
import tempfile
import unittest

TESTS_DIR = os.path.dirname(os.path.abspath(__file__))
SCRIPTS_DIR = os.path.join(TESTS_DIR, "..", "scripts")
sys.path.insert(0, SCRIPTS_DIR)
sys.path.insert(0, TESTS_DIR)  # 复用 b2 build_db

import governance_detector as det  # noqa: E402
import governance_reclaim_run as rec  # noqa: E402
from test_governance_b2_candidates import build_db  # noqa: E402

DETECTOR_PY = os.path.join(SCRIPTS_DIR, "governance_detector.py")
STARTUP_PY = os.path.join(SCRIPTS_DIR, "governance_startup_check.py")

# 未超限阈值: b2 造库仅几 KB, threshold_mb=1000 必不超限 (库大小 << 1GB)
NOT_OVER_MB = 1000


class TestC4NotOverCandidateSemantics(unittest.TestCase):
    """C4: 未超限时 detector/reclaim 候选集与统计完全一致."""

    def setUp(self):
        self.tmpdir = tempfile.mkdtemp(prefix="gov_c4_")
        self.db = os.path.join(self.tmpdir, "c4.db")
        build_db(self.db)

    def tearDown(self):
        import shutil
        shutil.rmtree(self.tmpdir, ignore_errors=True)

    def test_not_over_limit_still_collects_candidates(self):
        """未超限但候选集完整 (不再跳过收集), 业务判据 over_limit=False."""
        r = det.detect_threshold(self.db, "default", threshold_mb=NOT_OVER_MB)
        self.assertFalse(r.over_limit)
        self.assertEqual(r.need_release_bytes, 0)  # 未超限无需回收 (业务结论不变)
        # b2 库: hot_old_1/2 是唯一真实候选
        sids = {c["session_id"] for c in r.candidates}
        self.assertEqual(sids, {"hot_old_1", "hot_old_2"}, f"候选集异常: {sids}")
        self.assertEqual(r.active, 1)     # active_new
        self.assertEqual(r.protected, 1)  # exempt_old (pinned_old 由 SQL 排除)
        self.assertEqual(r.empty, 1)      # empty_ended (已归档空会话由 SQL 排除)

    def test_detector_matches_reclaim_not_over(self):
        """未超限时 detector.candidates == reclaim 候选集, 统计一致 (C4 核心回归)."""
        r = det.detect_threshold(self.db, "default", threshold_mb=NOT_OVER_MB)
        cands, info = rec._collect_candidates(self.db, NOT_OVER_MB)
        det_ids = {c["session_id"] for c in r.candidates}
        rec_ids = {c["session_id"] for c in cands}
        self.assertEqual(rec_ids, det_ids, "未超限: reclaim 与 detector 候选集不一致 (语义漂移)")
        self.assertEqual(info["candidates"], len(det_ids))
        self.assertEqual(info["active"], r.active)
        self.assertEqual(info["protected"], r.protected)
        self.assertEqual(info["empty"], r.empty)
        # 未超限时 reclaim info.need = max(size-target, 0) = 0
        self.assertEqual(info["need"], 0)

    def test_report_json_full_candidate_count(self):
        """JSON 报告 archive_candidates 反映真实候选数 (非 0), 预览 30 行上限保留."""
        r = det.detect_threshold(self.db, "default", threshold_mb=NOT_OVER_MB)
        d = det.report_to_dict(det.build_report("default", self.db, r, [], 0, []))
        l0 = d["l0"]
        self.assertFalse(l0["over_limit"])
        self.assertEqual(l0["need_release_mb"], 0.0)
        self.assertEqual(l0["archive_candidates"], 2)  # 修复前为 0
        self.assertEqual(l0["active_sessions"], 1)
        self.assertEqual(l0["empty_sessions"], 1)
        self.assertEqual(l0["protected_sessions"], 1)

    def test_detector_cli_text_not_over_shows_candidates(self):
        """CLI 文本输出未超限时也打印候选统计 (方案 1, 非保守版)."""
        env = dict(os.environ)
        env["PYTHONPATH"] = SCRIPTS_DIR + os.pathsep + env.get("PYTHONPATH", "")
        proc = subprocess.run(
            [sys.executable, DETECTOR_PY, "--db", self.db, "--profile", "default",
             "--threshold-mb", str(NOT_OVER_MB)],
            capture_output=True, text=True, env=env)
        self.assertEqual(proc.returncode, 0, proc.stderr)
        self.assertIn("OK", proc.stdout)
        self.assertIn("candidates=2", proc.stdout)
        self.assertIn("active=1", proc.stdout)
        self.assertIn("empty=1", proc.stdout)
        # 明细行存在 (方案 1: 未超限也打印候选明细)
        self.assertIn("hot_old_1", proc.stdout)

    def test_detector_cli_json_not_over_full(self):
        """CLI --json 未超限时输出完整候选 (archive_candidates=2)."""
        env = dict(os.environ)
        env["PYTHONPATH"] = SCRIPTS_DIR + os.pathsep + env.get("PYTHONPATH", "")
        proc = subprocess.run(
            [sys.executable, DETECTOR_PY, "--db", self.db, "--profile", "default",
             "--threshold-mb", str(NOT_OVER_MB), "--json"],
            capture_output=True, text=True, env=env)
        self.assertEqual(proc.returncode, 0, proc.stderr)
        payload = _json.loads(proc.stdout)
        l0 = payload["l0"]
        self.assertFalse(l0["over_limit"])
        self.assertEqual(l0["archive_candidates"], 2)
        self.assertEqual(l0["active_sessions"], 1)

    def test_startup_check_not_over_logs_real_candidates(self):
        """startup_check 未超限日志 candidates 用真实数 (联动修复, 不再硬编码 0)."""
        env = dict(os.environ)
        env["PYTHONPATH"] = SCRIPTS_DIR + os.pathsep + env.get("PYTHONPATH", "")
        proc = subprocess.run(
            [sys.executable, STARTUP_PY, "--db", self.db, "--profile", "default",
             "--threshold-mb", str(NOT_OVER_MB), "--report-json", "--verbose"],
            capture_output=True, text=True, env=env)
        self.assertEqual(proc.returncode, 0, proc.stderr)
        # 报告 candidates_n 为真实数
        self.assertIn("candidates_n", proc.stdout)
        report = _json.loads(proc.stdout) if proc.stdout.strip().startswith("{") else {}
        if report:
            self.assertEqual(report.get("candidates_n"), 2)
            self.assertEqual(report.get("over_limit"), False)
            self.assertEqual(report.get("action"), "no_op")
        # governance_log 中 candidates 真实 (查库验证)
        import sqlite3
        con = sqlite3.connect(f"file:{self.db}?mode=ro", uri=True)
        row = con.execute(
            "SELECT evidence FROM governance_log WHERE op='startup_check' "
            "ORDER BY created_at DESC LIMIT 1").fetchone()
        con.close()
        if row:
            self.assertIn("candidates=2", row[0], f"日志 candidates 硬编码残留: {row[0]}")


if __name__ == "__main__":
    unittest.main(verbosity=2)
