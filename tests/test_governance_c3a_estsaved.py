#!/usr/bin/env python3
"""test_governance_c3a_estsaved.py — C3-A 修复回归测试 (2026-08-10).

背景: C3 副本实测证明旧 `est_saved = content×0.9` 低估真实释放 12 倍
(漏 reasoning/reasoning_content/tool_calls/api_content 列 ~35MB 与 FTS 索引
57.6MB); reclaim 达标判断 `released >= need` 用低估累加 → 永远判不达标 →
过度归档 (220 全量 vs 实际 ~130)。

覆盖:
  1. est_saved 全文本列估算: 带 reasoning/tool_calls 列的库
     est_saved = (content+reasoning+tool_calls)×0.9 (非 content×0.9);
  2. 旧 schema 兼容: 仅 content 列的库不崩, text_bytes=content 和;
  3. db_logical_used_bytes: auto_vacuum=0 下 DELETE 后逻辑占用下降
     (freelist 增加), 且 ≤ 文件大小 — 达标判断的实测口径;
  4. 达标即停: 构造多会话库, target 落在中间档 → 归档 N<候选总数即停
     (不再全量), VACUUM 后 size ≤ target;
  5. detector/reclaim est 一致性: 同库同候选 est_saved_bytes 相同.

运行: PYTHONPATH=scripts python -m unittest tests.test_governance_c3a_estsaved -v
"""
import json
import os
import shutil
import sqlite3
import subprocess
import sys
import tempfile
import time
import unittest

TESTS_DIR = os.path.dirname(os.path.abspath(__file__))
SCRIPTS_DIR = os.path.join(TESTS_DIR, "..", "scripts")
sys.path.insert(0, SCRIPTS_DIR)

import governance_config as cfg  # noqa: E402
import governance_detector as det  # noqa: E402
import governance_reclaim_run as rec  # noqa: E402
import session_governor as gov  # noqa: E402

RECLAIM_PY = os.path.join(SCRIPTS_DIR, "governance_reclaim_run.py")

# 对齐生产 messages 表全文本列 (C3 实测列)
FULL_COLS = ["content", "reasoning", "reasoning_content", "tool_calls", "api_content"]


def _mk_full_schema_db(path: str, n_sessions: int = 40, msgs_per: int = 25,
                       blob_kb: int = 4, extra_cols: bool = True) -> tuple[int, int]:
    """造带全文本列的 synthetic 库 (会话全部 7 天前 → 均为候选).

    每会话 msgs_per 条 blob_kb KB 消息 + 等量 reasoning 文本 →
    全列和 ≈ content×2 量级 (验证 est_saved 翻倍, 不再只算 content)。
    返回 (会话数, 消息数)。
    """
    if os.path.exists(path):
        os.remove(path)
    con = sqlite3.connect(path)
    cur = con.cursor()
    cur.execute("""CREATE TABLE sessions (
        id TEXT PRIMARY KEY, profile TEXT, title TEXT,
        started_at REAL, ended_at REAL, message_count INTEGER DEFAULT 0,
        cwd TEXT, archived INTEGER DEFAULT 0, pinned INTEGER DEFAULT 0
    )""")
    if extra_cols:
        cur.execute("""CREATE TABLE messages (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            session_id TEXT, role TEXT, content TEXT,
            reasoning TEXT, reasoning_content TEXT, tool_calls TEXT,
            api_content TEXT, timestamp REAL
        )""")
    else:
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
    base = time.time() - 7 * 86400  # 7 天前 → 非活跃
    blob = "x" * (blob_kb * 1024)
    n_msg = 0
    for i in range(n_sessions):
        sid = f"c3a_{i:03d}"
        cur.execute(
            "INSERT INTO sessions (id, profile, title, started_at, ended_at, message_count, cwd, pinned) "
            "VALUES (?,?,?,?,?,?,?,0)",
            (sid, "default", f"c3a {i}", base - i * 3600, base - i * 3600 + 300, msgs_per, "/tmp"))
        for m in range(msgs_per):
            if extra_cols:
                cur.execute(
                    "INSERT INTO messages (session_id, role, content, reasoning, reasoning_content, tool_calls, api_content, timestamp) "
                    "VALUES (?,?,?,?,?,?,?,?)",
                    (sid, "user", blob, blob, blob[: len(blob) // 2], blob[: len(blob) // 2], None,
                     base - i * 3600 + m))
            else:
                cur.execute(
                    "INSERT INTO messages (session_id, role, content, timestamp) VALUES (?,?,?,?)",
                    (sid, "user", blob, base - i * 3600 + m))
            n_msg += 1
    con.commit()
    con.close()
    # 补齐治理表/FTS (幂等) — archive_session 写分支依赖
    gov.ensure_schema(path)
    return n_sessions, n_msg


class TestEstSavedFullTextCols(unittest.TestCase):
    """C3-A #1: est_saved 改全文本列和 × 压缩系数 (单一事实源 cfg)."""

    def setUp(self):
        self.tmpdir = tempfile.mkdtemp(prefix="gov_c3a_est_")

    def tearDown(self):
        shutil.rmtree(self.tmpdir, ignore_errors=True)

    def test_est_saved_counts_reasoning_and_tool_calls(self):
        """带 reasoning/tool_calls 列 → est_saved ≈ 全列和×0.9, 明显大于 content×0.9."""
        db = os.path.join(self.tmpdir, "full.db")
        n_sess, n_msg = _mk_full_schema_db(db, n_sessions=5, msgs_per=3, blob_kb=2)
        r = det.detect_threshold(db, "default", threshold_mb=0)
        self.assertEqual(len(r.candidates), n_sess)
        for c in r.candidates:
            # 全列和 = content + reasoning + reasoning_content + tool_calls (+0 api_content)
            expected_text = c["content_bytes"] * (1 + 1 + 0.5 + 0.5)
            self.assertAlmostEqual(c["text_bytes"], expected_text, delta=len(c["session_id"]) * 3)
            self.assertEqual(c["est_saved_bytes"], cfg.est_saved_bytes(c["text_bytes"]))
            # 关键: est_saved 必须显著大于旧 content×0.9 (全列包含 reasoning 等)
            self.assertGreater(c["est_saved_bytes"], int(c["content_bytes"] * 0.9) * 2,
                               "est_saved 应计入 reasoning/reasoning_content/tool_calls")

    def test_old_schema_content_only_no_crash(self):
        """旧 schema (仅 content 列) 兼容: text_bytes = content 和, est_saved 正常."""
        db = os.path.join(self.tmpdir, "old.db")
        _mk_full_schema_db(db, n_sessions=3, msgs_per=2, blob_kb=1, extra_cols=False)
        r = det.detect_threshold(db, "default", threshold_mb=0)
        self.assertEqual(len(r.candidates), 3)
        for c in r.candidates:
            self.assertEqual(c["text_bytes"], c["content_bytes"])
            self.assertEqual(c["est_saved_bytes"], cfg.est_saved_bytes(c["content_bytes"]))

    def test_detector_reclaim_est_consistent(self):
        """同一事实源: detector 与 reclaim 对同候选 est_saved_bytes 一致."""
        db = os.path.join(self.tmpdir, "both.db")
        _mk_full_schema_db(db, n_sessions=4, msgs_per=2, blob_kb=1)
        det_res = det.detect_threshold(db, "default", threshold_mb=0)
        cands, info = rec._collect_candidates(db, 0)
        det_map = {c["session_id"]: c for c in det_res.candidates}
        self.assertEqual(set(det_map), {c["session_id"] for c in cands})
        for c in cands:
            self.assertEqual(det_map[c["session_id"]]["est_saved_bytes"], c["est_saved_bytes"],
                             f"{c['session_id']} est_saved 不一致 (单一事实源破坏)")
            self.assertEqual(det_map[c["session_id"]]["text_bytes"], c["text_bytes"])


class TestLogicalUsedBytes(unittest.TestCase):
    """C3-A: db_logical_used_bytes — auto_vacuum=0 下 DELETE 后逻辑占用下降."""

    def setUp(self):
        self.tmpdir = tempfile.mkdtemp(prefix="gov_c3a_logical_")

    def tearDown(self):
        shutil.rmtree(self.tmpdir, ignore_errors=True)

    def test_logical_used_drops_after_delete_no_vacuum(self):
        """DELETE 不缩文件 (auto_vacuum=0), 但逻辑占用下降 → 达标判断实测口径."""
        db = os.path.join(self.tmpdir, "logical.db")
        _mk_full_schema_db(db, n_sessions=20, msgs_per=10, blob_kb=4)
        size_file = os.path.getsize(db)
        used0 = cfg.db_logical_used_bytes(db)
        self.assertLessEqual(used0, size_file, "逻辑占用应 ≤ 文件大小")

        # 删一半会话消息 (模拟归档), 不 VACUUM
        con = sqlite3.connect(db)
        cur = con.cursor()
        sids = [r[0] for r in cur.execute(
            "SELECT id FROM sessions WHERE archived=0 ORDER BY started_at ASC LIMIT 10")]
        for sid in sids:
            cur.execute("DELETE FROM messages WHERE session_id=?", (sid,))
        con.commit()
        con.close()

        size_file2 = os.path.getsize(db)
        used1 = cfg.db_logical_used_bytes(db)
        # 文件可能不缩 (auto_vacuum=0), 但逻辑占用必须降 (freelist 增加)
        self.assertLess(used1, used0, f"DELETE 后逻辑占用应下降: {used0} -> {used1}")
        self.assertLessEqual(used1, size_file2)
        print(f"[logical] file {size_file} -> {size_file2} ; "
              f"used {used0} -> {used1} ({(used0-used1)/1048576:.2f}MB 释放)")


class TestReclaimReachTargetByMeasure(unittest.TestCase):
    """C3-A #2: 达标判断改周期性实测 → 归档部分即停, 不再全量."""

    def setUp(self):
        self.tmpdir = tempfile.mkdtemp(prefix="gov_c3a_reach_")

    def tearDown(self):
        shutil.rmtree(self.tmpdir, ignore_errors=True)

    def test_apply_stops_at_target_not_all_candidates(self):
        """40 候选, target 落在中间档 → --apply 归档 < 40 即停, VACUUM 后 ≤ target."""
        db = os.path.join(self.tmpdir, "reach.db")
        arc_root = os.path.join(self.tmpdir, "arc")
        _mk_full_schema_db(db, n_sessions=40, msgs_per=25, blob_kb=4)  # ~8MB 文本

        # 先量两档逻辑占用: 归档前 vs 归档 20 个后
        used0 = cfg.db_logical_used_bytes(db)
        con = sqlite3.connect(db)
        cur = con.cursor()
        half = [r[0] for r in cur.execute(
            "SELECT id FROM sessions WHERE archived=0 ORDER BY started_at ASC LIMIT 20")]
        for sid in half:
            cur.execute("DELETE FROM messages WHERE session_id=?", (sid,))
            cur.execute("UPDATE sessions SET archived=1 WHERE id=?", (sid,))
        con.commit()
        con.close()
        used20 = cfg.db_logical_used_bytes(db)
        self.assertLess(used20, used0)
        # target 取中间: 归档 20 个时必达标, 归档前必未达标。
        # CLI --threshold-mb 只收 int, 反推时向上取整并校验实际 target 落在区间。
        target_bytes = (used0 + used20) // 2
        threshold_mb = int(target_bytes / 0.85 / (1024 * 1024)) + 1
        actual_target = int(threshold_mb * 1024 * 1024 * 0.85)
        self.assertGreater(actual_target, used20, "取整后 target 必须 > used20")
        self.assertLess(actual_target, used0, "取整后 target 必须 < used0")

        # 重建干净库再跑 --apply
        _mk_full_schema_db(db, n_sessions=40, msgs_per=25, blob_kb=4)
        env = dict(os.environ)
        env["PYTHONPATH"] = SCRIPTS_DIR + os.pathsep + env.get("PYTHONPATH", "")
        proc = subprocess.run(
            [sys.executable, RECLAIM_PY, "--db", db, "--profile", "default",
             "--threshold-mb", str(threshold_mb), "--archive-root", arc_root,
             "--apply", "--json"],
            capture_output=True, text=True, env=env, timeout=300)
        self.assertEqual(proc.returncode, 0, f"reclaim 失败: {proc.stderr}\n{proc.stdout}")

        # 解析 JSON: stdout 每行一个 JSON 对象 (plan+done), 取最后一行 (phase=done)
        payload = json.loads([l for l in proc.stdout.splitlines() if l.strip()][-1])
        self.assertEqual(payload["phase"], "done")
        archived = payload["archived"]
        self.assertGreater(archived, 0, "至少归档 1 个")
        self.assertLess(archived, 40, f"达标即停应归档 < 40, 实际 {archived} (过度归档?)")
        # VACUUM 后文件大小 ≤ target (实测达标, 非 est 累加)
        size_after = os.path.getsize(db)
        self.assertLessEqual(size_after, actual_target,
                             f"VACUUM 后 {size_after} 应 ≤ target {actual_target}")
        # 归档数应按检查周期 20 的倍数停 (防空转/周期实测语义)
        self.assertEqual(archived % cfg.TARGET_CHECK_INTERVAL, 0,
                         f"archived={archived} 应为 {cfg.TARGET_CHECK_INTERVAL} 的倍数")
        print(f"[reach] archived={archived}/40, size_after={size_after/1048576:.2f}MB "
              f"target={actual_target/1048576:.2f}MB")

    def test_need_zero_archives_one_then_stops(self):
        """防空转: 库未超限 (need≤0) 时归档 1 个即停 (b1 语义保持)."""
        db = os.path.join(self.tmpdir, "need0.db")
        arc_root = os.path.join(self.tmpdir, "arc0")
        _mk_full_schema_db(db, n_sessions=5, msgs_per=3, blob_kb=2)
        env = dict(os.environ)
        env["PYTHONPATH"] = SCRIPTS_DIR + os.pathsep + env.get("PYTHONPATH", "")
        # threshold 巨大 → target 巨大 → need = size - target ≤ 0
        proc = subprocess.run(
            [sys.executable, RECLAIM_PY, "--db", db, "--profile", "default",
             "--threshold-mb", "8000", "--archive-root", arc_root,
             "--apply", "--json"],
            capture_output=True, text=True, env=env, timeout=120)
        self.assertEqual(proc.returncode, 0, proc.stderr)
        payload = json.loads([l for l in proc.stdout.splitlines() if l.strip()][-1])
        self.assertEqual(payload["archived"], 1,
                         f"need≤0 防空转应归档 1 个, 实际 {payload['archived']}")


if __name__ == "__main__":
    unittest.main()
