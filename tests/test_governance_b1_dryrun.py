#!/usr/bin/env python3
"""test_governance_b1_dryrun.py — FIX-B1: reclaim 默认 dry-run 安全闸门隔离测试 (2026-08-09).

背景: PRODUCTION-DB-DISCOVERY.md B1 — 旧版 governance_reclaim_run.py 无 --dry-run,
      --json 输出 plan 后仍继续执行 archive_session() 写库, 仅查看 plan 即触发生产归档。

本测试验证 FIX-B1 落地:
  1. --help 显示 dry-run/apply 安全语义;
  2. 无参数 或 --dry-run 均零写入: DB 主文件 sha256 pre==post (字节级),
     WAL/SHM 与 sessions/messages/governance_* 行数前后完全一致;
  3. 仅显式 --apply 才进入写分支（在临时副本上验证, 归档文件/行数/状态确实变化）;
  4. --dry-run 与 --apply 互斥, 同时给出 argparse 报错退出码 2;
  5. 全程只用 tempfile 临时库, 绝不触碰任何生产 state.db / 默认 archive_root。

运行: PYTHONPATH=scripts python tests/test_governance_b1_dryrun.py
"""
import hashlib
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

import session_governor as gov  # noqa: E402

RECLAIM_PY = os.path.join(SCRIPTS_DIR, "governance_reclaim_run.py")

GOV_TABLES = ("sessions", "messages", "governance_meta",
              "governance_log", "governance_archive_index")


def build_db(path: str, n_sessions: int = 5, msgs_per: int = 3) -> None:
    """造 synthetic 库: 会话全部在 7 天前（非活跃 → 均为候选）。"""
    if os.path.exists(path):
        os.remove(path)
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
    base = time.time() - 7 * 86400  # 7 天前 → 超出 24h 活跃窗口
    blob = "x" * 2000
    for i in range(n_sessions):
        sid = f"synth_{i:05d}"
        cur.execute(
            "INSERT INTO sessions (id, profile, title, started_at, ended_at, message_count, cwd, pinned) "
            "VALUES (?,?,?,?,?,?,?,0)",
            (sid, "default", f"synth {i}", base - i * 3600, base - i * 3600 + 300, msgs_per, "/tmp"))
        for m in range(msgs_per):
            cur.execute("INSERT INTO messages (session_id, role, content, timestamp) VALUES (?,?,?,?)",
                        (sid, "user", blob, base - i * 3600 + m))
    con.commit()
    con.close()
    # 补齐治理表/FTS（幂等）——保证 archive_session 写分支可跑、governance_* 行数可查
    gov.ensure_schema(path)


def _snapshot_rows(db_path: str) -> dict:
    """只读连接统计各表行数（表不存在则记为 None，表示"不存在"本身也是一种不变）。"""
    out = {}
    con = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True)
    try:
        for t in GOV_TABLES:
            try:
                out[t] = con.execute(f"SELECT COUNT(*) FROM {t}").fetchone()[0]
            except sqlite3.OperationalError:
                out[t] = None
    finally:
        con.close()
    return out


def _snapshot_files(db_path: str) -> dict:
    """DB 主文件 sha256+大小, 以及同目录 -wal/-shm 的 (大小, mtime) 清单。

    FIX-B1 验收标准要求 sha256 比对: dry-run/默认模式下主文件哈希 pre==post,
    任何字节级改动（含 WAL 落盘、页写入、header 变更）都会被捕获。
    """
    def _sha256(p):
        h = hashlib.sha256()
        with open(p, "rb") as f:
            for chunk in iter(lambda: f.read(65536), b""):
                h.update(chunk)
        return h.hexdigest()

    base = db_path[:-3] if db_path.endswith(".db") else db_path
    side = {}
    for suffix in ("-wal", "-shm"):
        p = base + suffix
        side[suffix] = (os.path.getsize(p), os.path.getmtime(p)) if os.path.exists(p) else None
    return {"sha256": _sha256(db_path), "size": os.path.getsize(db_path), "side": side}


def run_reclaim(db_path: str, *extra_args: str) -> subprocess.CompletedProcess:
    """subprocess 隔离运行 reclaim 脚本（独立进程，零 import 污染）。"""
    env = dict(os.environ)
    env["PYTHONPATH"] = SCRIPTS_DIR + os.pathsep + env.get("PYTHONPATH", "")
    cmd = [sys.executable, RECLAIM_PY,
           "--db", db_path, "--profile", "default", "--threshold-mb", "80",
           "--archive-root", os.path.dirname(db_path)]
    cmd += list(extra_args)
    return subprocess.run(cmd, capture_output=True, text=True, env=env)


class TestB1DryRunGate(unittest.TestCase):
    """正向: dry-run / 无参数 → 零写入。"""

    def setUp(self):
        self.tmpdir = tempfile.mkdtemp(prefix="gov_b1_dryrun_")

    def tearDown(self):
        shutil.rmtree(self.tmpdir, ignore_errors=True)

    def _new_db(self):
        p = os.path.join(self.tmpdir, "test.db")
        build_db(p, 5, 3)
        return p

    def _assert_zero_write(self, db_path, proc, label):
        self.assertEqual(proc.returncode, 0, f"{label} 退出码非0: {proc.stderr}")
        # stdout 含 plan 与 dry-run 标记
        self.assertIn("[plan]", proc.stdout, f"{label}: 缺 plan 输出")
        self.assertIn("[dry-run]", proc.stdout, f"{label}: 缺 dry-run 标记")
        # 完整输出留档（失败时一并展示）
        self.assertIn("未执行任何归档/回收", proc.stdout, f"{label}: 缺安全提示")
        # 运行前后 DB/WAL/SHM 与行数完全一致 (sha256 字节级比对)
        f2 = _snapshot_files(db_path)
        r2 = _snapshot_rows(db_path)
        self.assertEqual(self.f1, f2, f"{label}: DB 文件被改动")
        self.assertEqual(self.f1["sha256"], f2["sha256"],
                         f"{label}: DB 主文件 sha256 pre!=post (零写入违规)")
        self.assertEqual(self.r1, r2, f"{label}: 行数被改动")

    def test_no_args_is_zero_write(self):
        """无 --dry-run 也无 --apply → 默认 dry-run，零写入。"""
        db = self._new_db()
        self.f1, self.r1 = _snapshot_files(db), _snapshot_rows(db)
        proc = run_reclaim(db)
        self._assert_zero_write(db, proc, "no-args")

    def test_explicit_dry_run_is_zero_write(self):
        db = self._new_db()
        self.f1, self.r1 = _snapshot_files(db), _snapshot_rows(db)
        proc = run_reclaim(db, "--dry-run")
        self._assert_zero_write(db, proc, "--dry-run")

    def test_json_dry_run_zero_write_and_marks_dry(self):
        """--json 模式: phase=plan + dry_run=true，且零写入。"""
        import json as _json
        db = self._new_db()
        self.f1, self.r1 = _snapshot_files(db), _snapshot_rows(db)
        proc = run_reclaim(db, "--json", "--dry-run")
        self.assertEqual(proc.returncode, 0, proc.stderr)
        payload = _json.loads(proc.stdout)
        self.assertEqual(payload["phase"], "plan")
        self.assertIs(payload["dry_run"], True)
        self.assertGreaterEqual(payload["info"]["candidates"], 1)
        f2, r2 = _snapshot_files(db), _snapshot_rows(db)
        self.assertEqual(self.f1, f2, "json dry-run: DB 文件被改动")
        self.assertEqual(self.r1, r2, "json dry-run: 行数被改动")


class TestB1ApplyBranch(unittest.TestCase):
    """反向: 仅显式 --apply 才写库（临时副本，不碰生产）。"""

    def setUp(self):
        self.tmpdir = tempfile.mkdtemp(prefix="gov_b1_apply_")

    def tearDown(self):
        shutil.rmtree(self.tmpdir, ignore_errors=True)

    def test_apply_alone_writes(self):
        db = os.path.join(self.tmpdir, "apply.db")
        build_db(db, 5, 3)
        r_before = _snapshot_rows(db)

        proc = run_reclaim(db, "--apply")
        self.assertEqual(proc.returncode, 0, f"--apply 失败: {proc.stderr}")
        self.assertIn("[done]", proc.stdout, "--apply 应进入写分支输出 done")
        self.assertNotIn("[dry-run] 零写入", proc.stdout)

        r_after = _snapshot_rows(db)
        # 归档恰好 1 个会话（need=0 时归档 1 个即达标 break）
        self.assertEqual(r_after["sessions"] - r_before["sessions"], 0, "sessions 总数不变")
        self.assertEqual(r_before["messages"] - r_after["messages"], 3, "归档会话的 3 条 messages 应被删除")
        # governance_meta 状态翻转 + governance_log / archive_index 写入
        con = sqlite3.connect(f"file:{db}?mode=ro", uri=True)
        try:
            n_cold = con.execute("SELECT COUNT(*) FROM governance_meta WHERE state='cold_archived'").fetchone()[0]
            n_log = con.execute("SELECT COUNT(*) FROM governance_log").fetchone()[0]
            n_idx = con.execute("SELECT COUNT(*) FROM governance_archive_index").fetchone()[0]
        finally:
            con.close()
        self.assertGreaterEqual(n_cold, 1, "apply 后应有会话进入 cold_archived")
        self.assertGreaterEqual(n_log, 1, "apply 后应有审计日志")
        self.assertGreaterEqual(n_idx, 1, "apply 后应有归档索引")
        # 归档 .gz 文件落在临时 archive-root 下（绝不写默认 /tmp/ag-test-archive）
        gzs = [os.path.join(dp, f) for dp, _, fs in os.walk(self.tmpdir) for f in fs if f.endswith(".gz")]
        self.assertGreaterEqual(len(gzs), 1, "临时 archive-root 下应生成 .gz 归档文件")


class TestB1CliSafety(unittest.TestCase):
    """CLI 安全语义: --help 与互斥。"""

    def test_help_shows_dryrun_apply_semantics(self):
        env = dict(os.environ)
        env["PYTHONPATH"] = SCRIPTS_DIR + os.pathsep + env.get("PYTHONPATH", "")
        proc = subprocess.run([sys.executable, RECLAIM_PY, "--help"],
                              capture_output=True, text=True, env=env)
        self.assertEqual(proc.returncode, 0)
        self.assertIn("--dry-run", proc.stdout)
        self.assertIn("--apply", proc.stdout)
        # 安全语义: 默认零写入 / 显式开启
        self.assertIn("零写入", proc.stdout)
        self.assertIn("显式开启", proc.stdout)

    def test_dry_run_and_apply_mutually_exclusive(self):
        db = os.path.join(tempfile.mkdtemp(prefix="gov_b1_mutex_"), "x.db")
        build_db(db, 2, 1)
        proc = run_reclaim(db, "--dry-run", "--apply")
        self.assertEqual(proc.returncode, 2, "互斥参数应 argparse 报错退出")
        self.assertIn("not allowed with", proc.stderr)
        os.remove(db)


if __name__ == "__main__":
    unittest.main(verbosity=2)
