#!/usr/bin/env python3
"""test_governance_b3_thresholds.py — FIX-B3: 四 profile 阈值解析口径统一隔离回归 (2026-08-09).

背景: PRODUCTION-DB-DISCOVERY.md B3 — session_governor.threshold 用 DEFAULT_CONFIG
      storage_limit_mb=60 且无 profile 概念, 同一 default 生产库 detector 报 80MB 而
      threshold 报 60MB, 数值口径打架; 且 60/80/0.85 常量在五入口各自复制, 有漂移风险。

本测试验证 FIX-B3 落地:
  1. 四 profile (default/athena/xuanwu/duanmu) 阈值 = 80/60/60/60, 保留目标 = 68/51/51/51;
  2. 五入口 (cfg / session_governor / detector / startup_check / reclaim / search)
     同一 profile 解析完全一致, 常量单一事实源不漂移;
  3. threshold 子命令显式接收 --profile, default 生产库口径不得回落 60MB;
  4. target_ratio=0.85 语义 = 保留比例 (target = 阈值 × 0.85);
  5. CLI 实跑 (session_governor threshold / detector) 报数一致且只读。

运行: PYTHONPATH=scripts python tests/test_governance_b3_thresholds.py
"""
import ast
import inspect
import json as _json
import os
import sqlite3
import subprocess
import sys
import tempfile
import time
import unittest
from unittest import mock

TESTS_DIR = os.path.dirname(os.path.abspath(__file__))
SCRIPTS_DIR = os.path.join(TESTS_DIR, "..", "scripts")
sys.path.insert(0, SCRIPTS_DIR)

import governance_config as cfg  # noqa: E402
import governance_detector as det  # noqa: E402
import governance_reclaim_run as rec  # noqa: E402
import governance_search as gs  # noqa: E402
import governance_startup_check as sc  # noqa: E402
import session_governor as gov  # noqa: E402

SESSION_GOVERNOR_PY = os.path.join(SCRIPTS_DIR, "session_governor.py")
DETECTOR_PY = os.path.join(SCRIPTS_DIR, "governance_detector.py")

PROFILES = ["default", "athena", "xuanwu", "duanmu"]
EXPECTED_MB = {"default": 80, "athena": 60, "xuanwu": 60, "duanmu": 60}
# 保留目标 = 阈值 × 0.85: 80→68MB, 60→51MB (2026-08-08 user 拍板 15% 滞后带)
EXPECTED_TARGET_MB = {p: int(mb * 0.85) for p, mb in EXPECTED_MB.items()}


def build_db(path: str) -> None:
    """造最小 synthetic 库 (sessions/messages/governance_meta), 尺寸 ~几十 KB 低于阈值。"""
    if os.path.exists(path):
        os.remove(path)
    con = sqlite3.connect(path)
    cur = con.cursor()
    cur.execute("""CREATE TABLE sessions (
        id TEXT PRIMARY KEY, session_key TEXT, profile_name TEXT, title TEXT,
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
    for i in range(3):
        sid = f"b3_synth_{i}"
        cur.execute(
            "INSERT INTO sessions (id, session_key, profile_name, started_at, ended_at,"
            " message_count, pinned, archived) VALUES (?,?,?,?,?,?,?,?)",
            (sid, None, "default", now - 7 * 86400 - i * 3600, now - 7 * 86400 + i * 3600, 3, 0, 0))
        for j in range(3):
            cur.execute(
                "INSERT INTO messages (session_id, role, content, timestamp) VALUES (?,?,?,?)",
                (sid, "user", f"{blob} {sid}-{j}", now - 7 * 86400 + j))
    con.commit()
    con.close()


class TestFourProfileResolution(unittest.TestCase):
    """需求 3: 四 profile 阈值 80/60/60/60 与保留目标 68/51/51/51 断言齐全."""

    def setUp(self):
        self.tmpdir = tempfile.mkdtemp(prefix="gov_b3_prof_")
        self.db = os.path.join(self.tmpdir, "prof.db")
        build_db(self.db)

    def tearDown(self):
        import shutil
        shutil.rmtree(self.tmpdir, ignore_errors=True)

    def test_resolve_threshold_four_profiles(self):
        for p in PROFILES:
            self.assertEqual(cfg.resolve_threshold_mb(p), EXPECTED_MB[p],
                             f"cfg.resolve_threshold_mb({p}) 应为 {EXPECTED_MB[p]}MB")
            self.assertEqual(det._default_threshold(p), EXPECTED_MB[p],
                             f"detector 对 {p} 应解析 {EXPECTED_MB[p]}MB")
            self.assertEqual(gs._profile_threshold_mb(p), EXPECTED_MB[p],
                             f"search 对 {p} 应解析 {EXPECTED_MB[p]}MB")

    def test_retention_target_four_profiles(self):
        # 保留目标 = 阈值 × 0.85: 80→68, 60→51
        for p in PROFILES:
            thr_mb = EXPECTED_MB[p]
            self.assertEqual(
                cfg.retention_target_bytes(int(thr_mb * 1024 * 1024)) // (1024 * 1024),
                EXPECTED_TARGET_MB[p], f"{p} 保留目标应 {EXPECTED_TARGET_MB[p]}MB")
        # detector detect_threshold 目标口径
        r = det.detect_threshold(self.db, "default", 80)
        self.assertEqual(r.threshold_bytes, 80 * 1024 * 1024)
        self.assertEqual(r.threshold_bytes * 0.85 // (1024 * 1024), 68)
        # reclaim _collect_candidates 目标口径 (68MB / 51MB)
        _, info80 = rec._collect_candidates(self.db, 80)
        self.assertEqual(info80["target"] // (1024 * 1024), 68)
        _, info60 = rec._collect_candidates(self.db, 60)
        self.assertEqual(info60["target"] // (1024 * 1024), 51)
        # session_governor.threshold_check target_mb 口径
        r = gov.threshold_check(self.db, profile="default")
        self.assertEqual(r["target_mb"], 68.0)
        r = gov.threshold_check(self.db, profile="athena")
        self.assertEqual(r["target_mb"], 51.0)

    def test_threshold_check_profile_limits(self):
        # 同一 DB, 各 profile 阈值不同: default=80, 其他=60 (B3 核心: 不得按 60 判定 default)
        for p in PROFILES:
            r = gov.threshold_check(self.db, profile=p)
            self.assertEqual(r["limit_mb"], EXPECTED_MB[p],
                             f"threshold_check({p}) 阈值应 {EXPECTED_MB[p]}MB, 实得 {r['limit_mb']}")
        # 显式 limit_mb 覆盖优先 (兼容旧调用: 位置参数仍是 limit_mb)
        r = gov.threshold_check(self.db, 0.05)
        self.assertEqual(r["limit_mb"], 0.05)


class TestCrossEntryConsistency(unittest.TestCase):
    """验收 1: 同一 profile / 同一 DB 在各入口阈值完全一致 + 常量单一事实源."""

    def setUp(self):
        self.tmpdir = tempfile.mkdtemp(prefix="gov_b3_consist_")
        self.db = os.path.join(self.tmpdir, "consist.db")
        build_db(self.db)

    def tearDown(self):
        import shutil
        shutil.rmtree(self.tmpdir, ignore_errors=True)

    def test_all_entries_agree_per_profile(self):
        for p in PROFILES:
            exp = EXPECTED_MB[p]
            exp_target = EXPECTED_TARGET_MB[p]
            self.assertEqual(cfg.resolve_threshold_mb(p), exp,
                             f"cfg({p}) 应 {exp}MB")
            self.assertEqual(det._default_threshold(p), exp,
                             f"detector({p}) 应 {exp}MB")
            self.assertEqual(gov.threshold_check(self.db, profile=p)["limit_mb"], exp,
                             f"threshold_check({p}) 应 {exp}MB")
            self.assertEqual(gs._profile_threshold_mb(p), exp,
                             f"search({p}) 应 {exp}MB")
            # startup_check 经 _detect_for_profile 解析, 目标 = 阈值×0.85
            _, target, _, _ = sc._detect_for_profile(self.db, p)
            self.assertEqual(target // (1024 * 1024), exp_target,
                             f"startup({p}) 保留目标应 {exp_target}MB")

    def test_search_entry_ratio_matches_cfg(self):
        # FIX-B3 补测: search dry_run_profile 默认 lag_ratio 必须走 cfg.LAG_RATIO (0.85),
        # 不得回落旧硬编码 0.9 (B3 审查发现的原漂移点)。
        sig = inspect.signature(gs.dry_run_profile)
        self.assertIsNone(sig.parameters["lag_ratio"].default,
                         "dry_run_profile.lag_ratio 默认必须为 None (走 cfg.LAG_RATIO)")
        # 行为验证: 默认 ratio → 目标 = 阈值×0.85; 显式 0.5 → 阈值×0.5
        with mock.patch.object(gs, "_profile_threshold_mb", return_value=80.0):
            r85 = gs.dry_run_profile(self.db, "default", [])
            r50 = gs.dry_run_profile(self.db, "default", [], lag_ratio=0.5)
        # 小库不超阈值时返回的 over=False, 无法直接看 target — 用 0.85 vs 0.5 的
        # out_lines 滞后带描述 + 阈值口径间接断言 (candidates 为空, target 不落盘)。
        # 核心断言: 模块无 0.9 字面量残留 (源码级防漂移)。
        with open(os.path.join(SCRIPTS_DIR, "governance_search.py"), encoding="utf-8") as fh:
            src = fh.read()
        tree = ast.parse(src)
        for node in ast.walk(tree):
            if isinstance(node, ast.Constant) and isinstance(node.value, float):
                self.assertNotAlmostEqual(node.value, 0.9, places=6,
                                          msg=f"search 源码不得残留 0.9 常量 (L{node.lineno})")
        self.assertIsNone(r85.get("error"))
        self.assertIsNone(r50.get("error"))

    def test_storage_ceiling_single_source(self):
        # 硬顶天花板同样收敛到 cfg (原 session_governor / search 各复制 80)
        self.assertEqual(cfg.STORAGE_CEILING_MB, 80)
        self.assertEqual(gov.DEFAULT_CONFIG["storage_ceiling_mb"], cfg.STORAGE_CEILING_MB)
        self.assertEqual(gs.DEFAULT_CONFIG["storage_ceiling_mb"], cfg.STORAGE_CEILING_MB)

    def test_single_source_constants(self):
        # 常量单一事实源: 五入口无本地复制漂移
        self.assertEqual(cfg.DEFAULT_THRESHOLD_MB, 60)
        self.assertEqual(cfg.PROFILE_OVERRIDE_MB, {"default": 80})
        self.assertEqual(cfg.LAG_RATIO, 0.85)
        self.assertEqual(cfg.ACTIVE_WINDOW_S, 24 * 3600)
        # 模块级别名与事实源一致 (det/rec/sc 均派生自 cfg)
        self.assertEqual(det.DEFAULT_THRESHOLD_MB, cfg.DEFAULT_THRESHOLD_MB)
        self.assertEqual(det.PROFILE_OVERRIDE_MB, cfg.PROFILE_OVERRIDE_MB)
        self.assertEqual(det.LAG_RATIO, cfg.LAG_RATIO)
        self.assertEqual(det.ACTIVE_WINDOW_S, cfg.ACTIVE_WINDOW_S)
        self.assertEqual(rec.LAG_RATIO, cfg.LAG_RATIO)
        self.assertEqual(rec.ACTIVE_WINDOW_S, cfg.ACTIVE_WINDOW_S)
        self.assertEqual(sc.LAG_RATIO, cfg.LAG_RATIO)
        self.assertEqual(sc.ACTIVE_WINDOW_S, cfg.ACTIVE_WINDOW_S)
        # session_governor / search 的 DEFAULT_CONFIG 由 cfg 派生
        self.assertEqual(gov.DEFAULT_CONFIG["storage_limit_mb"], cfg.DEFAULT_THRESHOLD_MB)
        self.assertEqual(gov.DEFAULT_CONFIG["profile_overrides"], cfg.PROFILE_OVERRIDE_MB)
        self.assertEqual(gs.DEFAULT_CONFIG["storage_limit_mb"], cfg.DEFAULT_THRESHOLD_MB)
        self.assertEqual(gs.DEFAULT_CONFIG["profile_overrides"], cfg.PROFILE_OVERRIDE_MB)
        # v3.1 既有回归: 三模块 ratio 均为 0.85
        self.assertEqual(det.LAG_RATIO, 0.85)
        self.assertEqual(rec.LAG_RATIO, 0.85)
        self.assertEqual(sc.LAG_RATIO, 0.85)

    def test_target_ratio_semantics_is_retention_ratio(self):
        # 语义: target_ratio=0.85 = 保留比例 (归档到 阈值×0.85, 15% 滞后带)
        r = det.detect_threshold(self.db, "default", 80, target_ratio=0.85)
        self.assertEqual(r.threshold_bytes, 80 * 1024 * 1024)
        self.assertEqual(int(r.threshold_bytes * 0.85) // (1024 * 1024), 68,
                         "80MB 阈值保留目标应为 68MB")
        # 用极小阈值让小库超限, 通过 need_release = size - target 观测 ratio 生效
        #   (threshold = 0.001×1048576 ≈ 1048.6B; 目标 ≈ 891B@0.85 vs 524B@0.5)
        thr_b = 0.001 * 1024 * 1024
        r85 = det.detect_threshold(self.db, "default", 0.001, target_ratio=0.85)
        r50 = det.detect_threshold(self.db, "default", 0.001, target_ratio=0.5)
        self.assertTrue(r85.over_limit and r50.over_limit, "0.001MB 阈值下小库应超限")
        self.assertEqual(r85.need_release_bytes - r50.need_release_bytes,
                         int(thr_b * 0.5) - int(thr_b * 0.85),
                         "need_release 差应等于两 ratio 的目标差 (保留比例语义)")


class TestThresholdCliProfile(unittest.TestCase):
    """需求 1 + 4: threshold 子命令显式接收 --profile, CLI 报数与 detector 一致且只读."""

    def setUp(self):
        self.tmpdir = tempfile.mkdtemp(prefix="gov_b3_cli_")
        self.db = os.path.join(self.tmpdir, "cli.db")
        build_db(self.db)

    def tearDown(self):
        import shutil
        shutil.rmtree(self.tmpdir, ignore_errors=True)

    def _run(self, py, *args):
        env = dict(os.environ)
        env["PYTHONPATH"] = SCRIPTS_DIR + os.pathsep + env.get("PYTHONPATH", "")
        proc = subprocess.run([sys.executable, py, *args],
                              capture_output=True, text=True, env=env)
        self.assertEqual(proc.returncode, 0, f"CLI 失败: {proc.stderr}")
        return proc.stdout

    def test_threshold_cli_reports_80_for_default(self):
        out = self._run(SESSION_GOVERNOR_PY, "threshold", "--db", self.db,
                        "--profile", "default")
        self.assertIn("阈值 80MB", out, f"default 应报 80MB: {out}")
        self.assertIn("保留目标 68MB", out, f"default 保留目标应 68MB: {out}")

    def test_threshold_cli_reports_60_for_other_profiles(self):
        for p in ("athena", "xuanwu", "duanmu"):
            out = self._run(SESSION_GOVERNOR_PY, "threshold", "--db", self.db,
                            "--profile", p)
            self.assertIn("阈值 60MB", out, f"{p} 应报 60MB: {out}")
            self.assertIn("保留目标 51MB", out, f"{p} 保留目标应 51MB: {out}")

    def test_threshold_cli_no_profile_defaults_to_default_80(self):
        # 不传 --profile → 默认 default → 80MB (不得回落 60MB)
        out = self._run(SESSION_GOVERNOR_PY, "threshold", "--db", self.db)
        self.assertIn("阈值 80MB", out, f"缺省 profile 应按 default 报 80MB: {out}")

    def test_threshold_cli_limit_mb_override(self):
        out = self._run(SESSION_GOVERNOR_PY, "threshold", "--db", self.db,
                        "--profile", "default", "--limit-mb", "10")
        self.assertIn("阈值 10MB", out, f"显式 --limit-mb 应覆盖: {out}")

    def test_detector_cli_matches_threshold_cli(self):
        # 需求 4: 同一 DB 同一 profile, threshold 与 detector 报数一致
        for p, mb in EXPECTED_MB.items():
            thr_out = self._run(SESSION_GOVERNOR_PY, "threshold", "--db", self.db,
                                "--profile", p)
            det_out = self._run(DETECTOR_PY, "--db", self.db, "--profile", p, "--no-l1")
            self.assertIn(f"阈值 {mb}MB", thr_out, f"threshold({p}) 应报 {mb}MB")
            self.assertIn(f"threshold {mb}MB", det_out, f"detector({p}) 应报 {mb}MB")

    def test_threshold_cli_readonly_no_writes(self):
        before = {f: os.path.getsize(self.db + f) if os.path.exists(self.db + f) else 0
                  for f in ("", "-wal", "-shm")}
        self._run(SESSION_GOVERNOR_PY, "threshold", "--db", self.db, "--profile", "default")
        self._run(DETECTOR_PY, "--db", self.db, "--profile", "default", "--no-l1")
        after = {f: os.path.getsize(self.db + f) if os.path.exists(self.db + f) else 0
                 for f in ("", "-wal", "-shm")}
        self.assertEqual(before, after, f"threshold/detector 不得写库: {before} -> {after}")


if __name__ == "__main__":
    unittest.main(verbosity=2)
