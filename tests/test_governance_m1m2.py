#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
test_governance_m1m2.py — MVP-1（M1-M2）验收测试

覆盖验收标准：
  - 建表幂等（重复执行 ensure_schema 不报错）
  - 归档 → restore round-trip sha256 一致（K2，320 §13）
  - WAL 模式开启（AC-D5.2）
  - 冷归档原子流程：先写 .gz + manifest + sha256 → 后删热行（AC-D1.2）
  - restore 原位恢复：消息逐字节一致（AC-D1.3）
  - 归档包完整性校验 sha256（AC-D1.4）
  - 输入白名单（AC-D5.3）/ 全参数化查询（AC-D5.1）/ 审计日志（AC-D4.1）

运行：
  python scripts/test_governance_m1m2.py        # 独立运行
  pytest scripts/test_governance_m1m2.py        # pytest 收集
"""

from __future__ import annotations

import gzip
import json
import os
import sqlite3
import sys
import tempfile
import time
from pathlib import Path

# 允许从 scripts/ 直接导入 session_governor（无论 cwd 在哪）
sys.path.insert(0, str(Path(__file__).resolve().parent))
import session_governor as gov  # noqa: E402


# ─────────────────────────── 测试基础设施 ───────────────────────────

def _make_db(path: str) -> None:
    """建一个与真实 state.db 兼容的最小 sessions/messages schema（含 archived/pinned）。"""
    conn = sqlite3.connect(path)
    try:
        conn.executescript(
            """
            CREATE TABLE sessions (
                id TEXT PRIMARY KEY,
                profile_name TEXT,
                session_key TEXT,
                title TEXT,
                started_at REAL NOT NULL,
                ended_at REAL,
                message_count INTEGER,
                cwd TEXT,
                archived INTEGER NOT NULL DEFAULT 0,
                pinned INTEGER NOT NULL DEFAULT 0
            );
            CREATE TABLE messages (
                id INTEGER PRIMARY KEY,
                session_id TEXT NOT NULL,
                role TEXT NOT NULL,
                content TEXT,
                tool_call_id TEXT,
                timestamp REAL NOT NULL,
                token_count INTEGER,
                active INTEGER NOT NULL DEFAULT 1,
                compacted INTEGER NOT NULL DEFAULT 0
            );
            """
        )
    finally:
        conn.close()


def _seed(db_path: str) -> tuple[list[dict], list[dict]]:
    """插入 3 个会话（含中文正文/多角色/多时间戳），返回 (sessions, messages) 原始快照。"""
    conn = sqlite3.connect(db_path)
    try:
        now = time.time()
        sessions = [
            {"id": "20260801_100000_aaa111", "profile_name": "default",
             "session_key": "SAFETY修订", "title": "SAFETY 修订讨论",
             "started_at": now - 86400 * 3, "ended_at": now - 86400 * 3 + 3600,
             "message_count": 3, "cwd": "/tmp/ag-test-project", "archived": 0, "pinned": 0},
            {"id": "20260802_100000_bbb222", "profile_name": "default",
             "session_key": "bug排查", "title": "Room MVP 验收 bug",
             "started_at": now - 86400 * 2, "ended_at": None,
             "message_count": 2, "cwd": None, "archived": 0, "pinned": 0},
            {"id": "20260803_100000_ccc333", "profile_name": "default",
             "session_key": "pinned会话", "title": "不可动",
             "started_at": now - 86400, "ended_at": None,
             "message_count": 1, "cwd": None, "archived": 0, "pinned": 1},
        ]
        messages = [
            # session aaa111：3 条消息，跨角色
            {"id": 1, "session_id": "20260801_100000_aaa111", "role": "user",
             "content": "SAFETY 需要加 ENV-6 目录规范", "timestamp": now - 86400 * 3,
             "token_count": 12, "active": 1, "compacted": 0},
            {"id": 2, "session_id": "20260801_100000_aaa111", "role": "assistant",
             "content": "已确认，容器→/tmp/ag-test-dockers", "timestamp": now - 86400 * 3 + 60,
             "token_count": 20, "active": 1, "compacted": 0},
            {"id": 3, "session_id": "20260801_100000_aaa111", "role": "system",
             "content": "规则已写入 SAFETY.md v1.1.0", "timestamp": now - 86400 * 3 + 120,
             "token_count": 8, "active": 1, "compacted": 0},
            # session bbb222：2 条消息
            {"id": 4, "session_id": "20260802_100000_bbb222", "role": "user",
             "content": "ws_room_v2 kind=agent 分支报错", "timestamp": now - 86400 * 2,
             "token_count": 10, "active": 1, "compacted": 0},
            {"id": 5, "session_id": "20260802_100000_bbb222", "role": "assistant",
             "content": "根因是 log() NameError，已修复", "timestamp": now - 86400 * 2 + 90,
             "token_count": 15, "active": 1, "compacted": 0},
            # session ccc333：1 条（pinned，不应被归档）
            {"id": 6, "session_id": "20260803_100000_ccc333", "role": "user",
             "content": "pinned 内容不可归档", "timestamp": now - 86400,
             "token_count": 5, "active": 1, "compacted": 0},
        ]
        for s in sessions:
            conn.execute(
                "INSERT INTO sessions (id, profile_name, session_key, title, started_at,"
                " ended_at, message_count, cwd, archived, pinned)"
                " VALUES (?,?,?,?,?,?,?,?,?,?)",
                (s["id"], s["profile_name"], s["session_key"], s["title"], s["started_at"],
                 s["ended_at"], s["message_count"], s["cwd"], s["archived"], s["pinned"]),
            )
        for m in messages:
            conn.execute(
                "INSERT INTO messages (id, session_id, role, content, tool_call_id, timestamp,"
                " token_count, active, compacted)"
                " VALUES (?,?,?,?,?,?,?,?,?)",
                (m["id"], m["session_id"], m["role"], m["content"], m.get("tool_call_id"),
                 m["timestamp"], m["token_count"], m["active"], m["compacted"]),
            )
        conn.commit()
        return sessions, messages
    finally:
        conn.close()


class _Ctx:
    """每测试独立临时目录 + db + archive_root。"""

    def __init__(self):
        self.tmp = tempfile.mkdtemp(prefix="gov_m1m2_")
        self.db = os.path.join(self.tmp, "state.db")
        self.archive_root = os.path.join(self.tmp, "hermes workspace")
        _make_db(self.db)

    def cleanup(self):
        import shutil
        shutil.rmtree(self.tmp, ignore_errors=True)


# ─────────────────────────── 验收测试 ───────────────────────────

def test_ensure_schema_idempotent():
    """M1 验收：建表幂等，重复执行不报错且表集合一致。"""
    ctx = _Ctx()
    try:
        first = gov.ensure_schema(ctx.db)
        second = gov.ensure_schema(ctx.db)
        assert first == second, f"两次建表结果不一致: {first} vs {second}"
        required = {
            "governance_meta", "governance_log", "governance_archive_index",
            "governance_archive_index_fts", "governance_cluster",
            "governance_cluster_member",
        }
        assert required.issubset(set(first)), f"缺表: {required - set(first)}"
        # 三跑依然不报错（极端幂等）
        gov.ensure_schema(ctx.db)
    finally:
        ctx.cleanup()


def test_wal_enabled():
    """M1 验收：WAL 模式开启（AC-D5.2）。"""
    ctx = _Ctx()
    try:
        gov.ensure_schema(ctx.db)
        conn = sqlite3.connect(ctx.db)
        try:
            mode = conn.execute("PRAGMA journal_mode").fetchone()[0]
            assert mode == "wal", f"journal_mode={mode}, 期望 wal"
        finally:
            conn.close()
    finally:
        ctx.cleanup()


def test_archive_then_restore_roundtrip_sha256():
    """M2 验收（K2 核心）：归档→restore round-trip，sha256 一致 + 消息逐字节一致。"""
    ctx = _Ctx()
    try:
        sessions, messages = _seed(ctx.db)
        gov.ensure_schema(ctx.db)
        sid = "20260801_100000_aaa111"
        src_msgs = [m for m in messages if m["session_id"] == sid]

        # 归档
        r = gov.archive_session(ctx.db, sid, ctx.archive_root, reason="test")
        assert r["message_count"] == len(src_msgs) == 3
        assert os.path.exists(r["archive_file"]), "归档文件不存在"
        assert r["sha256"] == gov._sha256_file(r["archive_file"]), "归档返回 sha256 不一致"

        # 归档后热行已删 + archived=1 + state=cold_archived
        conn = sqlite3.connect(ctx.db)
        try:
            n = conn.execute("SELECT COUNT(*) FROM messages WHERE session_id=?", (sid,)).fetchone()[0]
            assert n == 0, f"归档后热消息未删: {n}"
            arch = conn.execute("SELECT archived FROM sessions WHERE id=?", (sid,)).fetchone()[0]
            assert arch == 1, "sessions.archived 未置 1"
            st = conn.execute(
                "SELECT state FROM governance_meta WHERE session_id=?", (sid,)).fetchone()[0]
            assert st == "cold_archived", f"state={st}"
        finally:
            conn.close()

        # verify：文件级 + 内容级 sha256 均通过
        v = gov.verify_archive(ctx.db, sid, ctx.archive_root)
        assert v["ok"] and v["message_count"] == 3

        # restore：原位恢复
        r2 = gov.restore_session(ctx.db, sid, ctx.archive_root)
        assert r2["restored_messages"] == 3

        # restore 后消息逐字节一致（顺序/正文/时间戳/角色）
        conn = sqlite3.connect(ctx.db)
        try:
            rows = conn.execute(
                "SELECT id, session_id, role, content, timestamp, token_count"
                " FROM messages WHERE session_id=? ORDER BY id", (sid,)).fetchall()
            assert len(rows) == 3
            for got, want in zip(rows, sorted(src_msgs, key=lambda m: m["id"])):
                assert got[0] == want["id"], f"id 不一致: {got[0]} vs {want['id']}"
                assert got[1] == want["session_id"]
                assert got[2] == want["role"]
                assert got[3] == want["content"], f"正文不一致: {got[3]!r} vs {want['content']!r}"
                assert abs(got[4] - want["timestamp"]) < 1e-6, "时间戳不一致"
            # state 回 active + archived=0
            st = conn.execute(
                "SELECT state FROM governance_meta WHERE session_id=?", (sid,)).fetchone()[0]
            assert st == "active", f"restore 后 state={st}"
            arch = conn.execute("SELECT archived FROM sessions WHERE id=?", (sid,)).fetchone()[0]
            assert arch == 0, "restore 后 archived 未复位"
        finally:
            conn.close()

        # K2 闭环：归档 gz sha256 前后一致（verify 已做文件级比对；此处再确认记录未被篡改）
        meta = None
        conn = sqlite3.connect(ctx.db)
        try:
            meta = conn.execute(
                "SELECT archive_sha256 FROM governance_meta WHERE session_id=?", (sid,)).fetchone()
        finally:
            conn.close()
        assert meta and meta[0] == r["sha256"], "K2 sha256 闭环不一致"
    finally:
        ctx.cleanup()


def test_archive_rejects_double_and_pinned():
    """两态状态机：重复归档拒绝；pinned 会话拒绝（320 §6.3）。"""
    ctx = _Ctx()
    try:
        _, _ = _seed(ctx.db)
        gov.ensure_schema(ctx.db)
        sid = "20260801_100000_aaa111"

        gov.archive_session(ctx.db, sid, ctx.archive_root, reason="test")
        # 二次归档 → 必须拒绝（已 cold_archived / archived=1）
        try:
            gov.archive_session(ctx.db, sid, ctx.archive_root, reason="test")
            raise AssertionError("重复归档未被拒绝")
        except ValueError:
            pass

        # pinned 会话 → 拒绝
        pinned = "20260803_100000_ccc333"
        try:
            gov.archive_session(ctx.db, pinned, ctx.archive_root, reason="test")
            raise AssertionError("pinned 会话被归档")
        except ValueError:
            pass
    finally:
        ctx.cleanup()


def test_restore_rejects_corrupted_gz():
    """AC-D1.4：归档包被篡改（sha256 不匹配）→ 拒绝恢复。"""
    ctx = _Ctx()
    try:
        _, _ = _seed(ctx.db)
        gov.ensure_schema(ctx.db)
        sid = "20260801_100000_aaa111"
        r = gov.archive_session(ctx.db, sid, ctx.archive_root, reason="test")

        # 篡改 .gz（追加字节破坏 gzip+hash）
        with open(r["archive_file"], "ab") as f:
            f.write(b"corrupt")
        try:
            gov.verify_archive(ctx.db, sid, ctx.archive_root)
            raise AssertionError("篡改后 verify 未报错")
        except RuntimeError:
            pass
        try:
            gov.restore_session(ctx.db, sid, ctx.archive_root)
            raise AssertionError("篡改后 restore 未拒绝")
        except (RuntimeError, gzip.BadGzipFile):
            pass
    finally:
        ctx.cleanup()


def test_input_validation_and_audit_log():
    """AC-D5.3 输入白名单 / AC-D5.1 参数化 / AC-D4.1 审计日志。"""
    ctx = _Ctx()
    try:
        _, _ = _seed(ctx.db)
        gov.ensure_schema(ctx.db)

        # SQL 注入串 / 非法字符 → 白名单拒绝（不进入 SQL）
        for bad in ("x'; DROP TABLE governance_meta;--", "../etc/passwd", "a b", "a\tb"):
            try:
                gov.archive_session(ctx.db, bad, ctx.archive_root, reason="test")
                raise AssertionError(f"非法 session_id 未被拒绝: {bad!r}")
            except ValueError:
                pass
            try:
                gov.restore_session(ctx.db, bad, ctx.archive_root)
                raise AssertionError(f"非法 restore 输入未被拒绝: {bad!r}")
            except ValueError:
                pass

        # 正常归档 → 审计日志有记录（不含原文，AC-D5.4）
        sid = "20260802_100000_bbb222"
        gov.archive_session(ctx.db, sid, ctx.archive_root, reason="test")
        conn = sqlite3.connect(ctx.db)
        try:
            rows = conn.execute(
                "SELECT op, session_id, before_state, after_state, reason, operator,"
                " status, evidence FROM governance_log WHERE session_id=? ORDER BY id",
                (sid,)).fetchall()
            assert len(rows) == 1
            op, _, before, after, reason, operator, status, evidence = rows[0]
            assert op == "cold_archive" and before == "active" and after == "cold_archived"
            assert status == "done" and reason == "test" and operator == "auto"
            assert "SAFETY" not in evidence and "错误" not in evidence, "审计日志泄露原文"
        finally:
            conn.close()
    finally:
        ctx.cleanup()


def test_threshold_check():
    """L0 阈值检测（320 §10）：只读、超阈值给出最旧候选。"""
    ctx = _Ctx()
    try:
        _, _ = _seed(ctx.db)
        gov.ensure_schema(ctx.db)
        r = gov.threshold_check(ctx.db, limit_mb=0.05)  # 50KB 阈值，测试库 ~80KB 必然超
        assert r["over"] is True and r["db_size_mb"] > 0.05
        assert r["oldest_sessions"], "超阈值未返回候选会话"
        assert all("session_id" in s for s in r["oldest_sessions"])
        # 不触发任何写操作：治理表仍为空
        conn = sqlite3.connect(ctx.db)
        try:
            n = conn.execute("SELECT COUNT(*) FROM governance_log").fetchone()[0]
            assert n == 0, "threshold_check 不应写日志"
        finally:
            conn.close()
    finally:
        ctx.cleanup()


def test_restore_idempotent_rejects_existing_hot_rows():
    """restore 保护：目标 session 已有热消息时拒绝覆盖式恢复。"""
    ctx = _Ctx()
    try:
        _, _ = _seed(ctx.db)
        gov.ensure_schema(ctx.db)
        sid = "20260801_100000_aaa111"
        gov.archive_session(ctx.db, sid, ctx.archive_root, reason="test")

        # 人为写入一条新热消息（模拟归档后又写入）
        conn = sqlite3.connect(ctx.db)
        try:
            conn.execute(
                "INSERT INTO messages (session_id, role, content, timestamp)"
                " VALUES (?, 'user', '归档后的新消息', ?)", (sid, time.time()))
            conn.commit()
        finally:
            conn.close()

        try:
            gov.restore_session(ctx.db, sid, ctx.archive_root)
            raise AssertionError("存在热消息时 restore 未被拒绝")
        except RuntimeError:
            pass
    finally:
        ctx.cleanup()


def test_archive_failure_rolls_back_and_cleans_tmp():
    """失败路径：会话不存在 → 报错 + 无残留 .tmp / .gz 文件。"""
    ctx = _Ctx()
    try:
        _, _ = _seed(ctx.db)
        gov.ensure_schema(ctx.db)
        archive_dir = os.path.join(ctx.archive_root, "default", "session-archive")
        try:
            gov.archive_session(ctx.db, "no_such_session_xyz", ctx.archive_root, reason="test")
            raise AssertionError("归档不存在会话未报错")
        except ValueError:
            pass
        # 无残留文件
        if os.path.exists(archive_dir):
            leftovers = os.listdir(archive_dir)
            assert leftovers == [], f"失败路径残留文件: {leftovers}"
    finally:
        ctx.cleanup()


def test_manifest_contains_schema_version_and_counts():
    """320 §8.3：manifest 内嵌 schema 版本 / 原 id / 计数 / 时间范围 / sha256。"""
    ctx = _Ctx()
    try:
        sessions, _ = _seed(ctx.db)
        gov.ensure_schema(ctx.db)
        sid = "20260801_100000_aaa111"
        r = gov.archive_session(ctx.db, sid, ctx.archive_root, reason="test")
        with gzip.open(r["archive_file"], "rt", encoding="utf-8") as f:
            manifest = json.loads(f.readline())
        assert manifest["schema_version"] == gov.SCHEMA_VERSION
        assert manifest["session_id"] == sid
        assert manifest["message_count"] == 3
        assert manifest["started_at"] == sessions[0]["started_at"]
        assert manifest["ended_at"] == sessions[0]["ended_at"]
        assert manifest["content_sha256"], "manifest 缺 content_sha256"
    finally:
        ctx.cleanup()


# ─────────────────────────── 运行入口 ───────────────────────────

def _run_all() -> int:
    tests = [(name, fn) for name, fn in sorted(globals().items())
             if name.startswith("test_") and callable(fn)]
    passed, failed = 0, []
    for name, fn in tests:
        try:
            fn()
            print(f"  ✅ {name}")
            passed += 1
        except Exception as exc:  # noqa: BLE001
            print(f"  ❌ {name}: {type(exc).__name__}: {exc}")
            failed.append(name)
    print(f"\n结果: {passed}/{len(tests)} 通过" + (f"，失败: {failed}" if failed else ""))
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(_run_all())
