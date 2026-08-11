#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
test_governance_m5m8.py — MVP-3（M5-M8）验收测试

覆盖验收标准：
  M5: govern search --archived "关键词" <3s（K4）+ 来源状态标注 + FTS 短语转义
  M6: 不可变保护（活跃/pinned/修复链/不可回溯）+ 审计日志（AC-D4.1 what/when/why）
  M7: 安全基线（脱敏 AC-D3.4 / 白名单 AC-D5.3 / 权限 AC-D5.5 / DUR-1 备份 AC-D4.3）
  M8: 首次 dry-run 只读扫描 → 候选清单日志文件（320 §10.5 Q2）

运行：
  python scripts/test_governance_m5m8.py        # 独立运行
  pytest scripts/test_governance_m5m8.py        # pytest 收集
"""

from __future__ import annotations

import json
import os
import sqlite3
import sys
import tempfile
import time
from pathlib import Path

# 允许从 scripts/ 直接导入（无论 cwd 在哪）
sys.path.insert(0, str(Path(__file__).resolve().parent))
import governance_config as cfg  # noqa: E402  # FIX-B3: 阈值单一事实源
import governance_search as gs  # noqa: E402
import session_governor as gov  # noqa: E402


# ─────────────────────────── 测试基础设施 ───────────────────────────

def _make_db(path: str) -> None:
    """建与真实 state.db 兼容的最小 schema（含 is_protected 用到的全部列 + FTS trigram）。"""
    conn = sqlite3.connect(path)
    try:
        conn.executescript(
            """
            CREATE TABLE sessions (
                id TEXT PRIMARY KEY,
                source TEXT,
                profile_name TEXT,
                session_key TEXT,
                title TEXT,
                started_at REAL NOT NULL,
                ended_at REAL,
                message_count INTEGER,
                cwd TEXT,
                origin_json TEXT,
                archived INTEGER NOT NULL DEFAULT 0,
                pinned INTEGER NOT NULL DEFAULT 0
            );
            CREATE TABLE messages (
                id INTEGER PRIMARY KEY,
                session_id TEXT NOT NULL,
                role TEXT NOT NULL,
                content TEXT,
                tool_call_id TEXT,
                tool_calls TEXT,
                reasoning TEXT,
                timestamp REAL NOT NULL,
                token_count INTEGER,
                active INTEGER NOT NULL DEFAULT 1,
                compacted INTEGER NOT NULL DEFAULT 0
            );
            -- 真实库 FTS 指向 messages_fts_trigram_src 镜像表（三列），rowid 与 messages.id 一致
            CREATE TABLE messages_fts_trigram_src (
                id INTEGER PRIMARY KEY,
                content TEXT,
                tool_name TEXT,
                tool_calls TEXT
            );
            CREATE VIRTUAL TABLE messages_fts_trigram USING fts5(
                content, tool_name, tool_calls,
                content='messages_fts_trigram_src', content_rowid='id',
                tokenize='trigram'
            );
            """
        )
    finally:
        conn.close()


def _seed(db_path: str) -> list[dict]:
    """插入 3 个会话（含中文正文/跨角色/pinned/已结束），返回 sessions 列表。"""
    conn = sqlite3.connect(db_path)
    try:
        now = time.time()
        sessions = [
            {"id": "20260801_100000_aaa111", "source": "cli", "profile_name": "default",
             "session_key": "SAFETY修订", "title": "SAFETY 修订讨论",
             "started_at": now - 86400 * 3, "ended_at": now - 86400 * 3 + 3600,
             "message_count": 3, "cwd": "/tmp/ag-test-project",
             "origin_json": "{}", "archived": 0, "pinned": 0},
            {"id": "20260802_100000_bbb222", "source": "cli", "profile_name": "default",
             "session_key": "bug排查", "title": "Room MVP 验收 bug",
             "started_at": now - 86400 * 2, "ended_at": None,
             "message_count": 2, "cwd": None, "origin_json": "{}",
             "archived": 0, "pinned": 0},
            {"id": "20260803_100000_ccc333", "source": "cli", "profile_name": "default",
             "session_key": "pinned会话", "title": "不可动",
             "started_at": now - 86400, "ended_at": None,
             "message_count": 1, "cwd": None, "origin_json": "{}",
             "archived": 0, "pinned": 1},
        ]
        messages = [
            {"id": 1, "session_id": "20260801_100000_aaa111", "role": "user",
             "content": "SAFETY 需要加 ENV-6 目录规范 api_key=sk-abc123",
             "timestamp": now - 86400 * 3, "token_count": 12},
            {"id": 2, "session_id": "20260801_100000_aaa111", "role": "assistant",
             "content": "已确认，容器→/tmp/ag-test-dockers", "timestamp": now - 86400 * 3 + 60},
            {"id": 3, "session_id": "20260801_100000_aaa111", "role": "system",
             "content": "规则已写入 SAFETY.md v1.1.0", "timestamp": now - 86400 * 3 + 120},
            {"id": 4, "session_id": "20260802_100000_bbb222", "role": "user",
             "content": "ws_room_v2 kind=agent 分支报错", "timestamp": now - 86400 * 2},
            {"id": 5, "session_id": "20260802_100000_bbb222", "role": "assistant",
             "content": "根因是 log() NameError，已修复", "timestamp": now - 86400 * 2 + 90},
            {"id": 6, "session_id": "20260803_100000_ccc333", "role": "user",
             "content": "SAFETY pinned 内容不可归档", "timestamp": now - 86400},
        ]
        for s in sessions:
            conn.execute(
                "INSERT INTO sessions (id, source, profile_name, session_key, title,"
                " started_at, ended_at, message_count, cwd, origin_json, archived, pinned)"
                " VALUES (?,?,?,?,?,?,?,?,?,?,?,?)",
                (s["id"], s["source"], s["profile_name"], s["session_key"], s["title"],
                 s["started_at"], s["ended_at"], s["message_count"], s["cwd"],
                 s["origin_json"], s["archived"], s["pinned"]),
            )
        for m in messages:
            conn.execute(
                "INSERT INTO messages (id, session_id, role, content, tool_call_id,"
                " timestamp, token_count, active, compacted)"
                " VALUES (?,?,?,?,?,?,?,?,?)",
                (m["id"], m["session_id"], m["role"], m["content"], m.get("tool_call_id"),
                 m["timestamp"], m.get("token_count"), 1, 0),
            )
        conn.commit()
        # FTS trigram 同步：external content 表需显式填充 src + FTS 索引（真实库由 Hermes 触发器维护）
        conn.execute(
            "INSERT INTO messages_fts_trigram_src (id, content)"
            " SELECT id, content FROM messages"
        )
        conn.execute(
            "INSERT INTO messages_fts_trigram (rowid, content)"
            " SELECT id, content FROM messages_fts_trigram_src"
        )
        conn.commit()
        return sessions
    finally:
        conn.close()


class _Ctx:
    """每测试独立临时目录 + db + archive_root。"""

    def __init__(self):
        self.tmp = tempfile.mkdtemp(prefix="gov_m5m8_")
        self.db = os.path.join(self.tmp, "state.db")
        self.archive_root = os.path.join(self.tmp, "hermes workspace")
        _make_db(self.db)

    def cleanup(self):
        import shutil
        shutil.rmtree(self.tmp, ignore_errors=True)


def _archive_one(ctx: _Ctx, sid: str) -> dict:
    """辅助：建治理表 + 归档单个会话（供检索 cold / 审计 / dry-run 前置）。"""
    gov.ensure_schema(ctx.db)
    return gov.archive_session(ctx.db, sid, ctx.archive_root, reason="test")


# ─────────────────────────── M5：检索 ───────────────────────────

def test_search_scope_all_returns_active_and_cold():
    """M5 验收：scope=all 同时命中 active 与 cold，且来源状态标注正确。"""
    ctx = _Ctx()
    try:
        _seed(ctx.db)
        _archive_one(ctx, "20260801_100000_aaa111")  # 归档 aaa111 → cold
        # bbb222 保持 active
        r = gs.search(ctx.db, "SAFETY", scope="all", limit=20)
        assert r["hits"], "scope=all 无命中"
        states = {h["state"] for h in r["hits"]}
        assert "active" in states, f"缺 active 命中: {r['hits']}"
        assert "cold_archived" in states, f"缺 cold 命中: {r['hits']}"
        for h in r["hits"]:
            assert h["title"] is not None, "title 应为 str（可为空）"
        # cold 命中带 archive_file，active 不带
        cold = [h for h in r["hits"] if h["state"] == "cold_archived"]
        assert all(h["archive_file"] for h in cold), "cold 命中应标注 archive_file"
    finally:
        ctx.cleanup()


def test_search_scope_archived_only():
    """M5 验收：--archived 只返回 cold 来源。"""
    ctx = _Ctx()
    try:
        _seed(ctx.db)
        _archive_one(ctx, "20260801_100000_aaa111")
        r = gs.search(ctx.db, "SAFETY", scope="archived", limit=20)
        assert r["hits"], "--archived 无命中"
        assert all(h["state"] == "cold_archived" for h in r["hits"]), \
            f"--archived 混入非 cold: {r['hits']}"
    finally:
        ctx.cleanup()


def test_search_scope_active_only():
    """M5：--active 只返回 active 来源。"""
    ctx = _Ctx()
    try:
        _seed(ctx.db)
        _archive_one(ctx, "20260801_100000_aaa111")
        r = gs.search(ctx.db, "SAFETY", scope="active", limit=20)
        assert r["hits"], "--active 无命中"
        assert all(h["state"] == "active" for h in r["hits"]), \
            f"--active 混入非 active: {r['hits']}"
    finally:
        ctx.cleanup()


def test_search_fts_phrase_escaping():
    """M5 + AC-D5.1：关键词含双引号/星号被 FTS 短语转义，不报错且无注入。"""
    ctx = _Ctx()
    try:
        _seed(ctx.db)
        # 注入尝试串不应引发异常，也不应返回伪造命中
        r = gs.search(ctx.db, '" OR * FROM messages --', scope="all", limit=10)
        assert isinstance(r["hits"], list), "注入串导致非列表返回"
        assert "query_ms" in r
    finally:
        ctx.cleanup()


def test_search_keyword_validation():
    """M5 + AC-D5.3：空关键词/超长关键词拒绝，非法 id 拒绝。"""
    ctx = _Ctx()
    try:
        _seed(ctx.db)
        try:
            gs.search(ctx.db, "   ", scope="all")
            raise AssertionError("空关键词未被拒绝")
        except ValueError:
            pass
        try:
            gs.search(ctx.db, "长" * 201, scope="all")
            raise AssertionError("超长关键词未被拒绝")
        except ValueError:
            pass
        try:
            gs.is_protected(ctx.db, "bad id!@#")
            raise AssertionError("非法 session_id 未被拒绝")
        except ValueError:
            pass
    finally:
        ctx.cleanup()


def test_search_redacts_credentials():
    """M7 AC-D3.4：检索结果 snippet/title 脱敏，凭据原文不泄露。"""
    ctx = _Ctx()
    try:
        _seed(ctx.db)
        r = gs.search(ctx.db, "ENV-6", scope="active", limit=20)
        assert r["hits"], "未命中含凭据样本"
        joined = " ".join(f"{h['title'] or ''} {h['snippet'] or ''}" for h in r["hits"])
        assert "sk-abc123" not in joined, "检索结果泄露 api_key 原文"
        assert "[REDACTED]" in joined, "脱敏占位符未生效"
    finally:
        ctx.cleanup()


def test_search_perf_under_3s():
    """M5 验收（K4）：查询耗时 <3000ms。"""
    ctx = _Ctx()
    try:
        _seed(ctx.db)
        _archive_one(ctx, "20260801_100000_aaa111")
        r = gs.search(ctx.db, "SAFETY", scope="all", limit=20)
        assert r["query_ms"] < 3000, f"查询耗时 {r['query_ms']}ms 超 K4 目标"
    finally:
        ctx.cleanup()


def test_search_missing_tables_returns_empty_not_error():
    """M5 健壮性：治理表不存在时 --archived 返回空而非崩溃（真实库建表前的安全行为）。"""
    ctx = _Ctx()
    try:
        _seed(ctx.db)  # 不建 governance 表
        r = gs.search(ctx.db, "SAFETY", scope="archived", limit=10)
        assert r["hits"] == [], "无治理表时 archived 应返回空"
        assert r["query_ms"] < 3000
    finally:
        ctx.cleanup()


# ─────────────────────────── M6：不可变保护 + 审计 ───────────────────────────

def test_protect_active_open_session():
    """M6：ended_at 为空的进行中会话 → 受保护。"""
    ctx = _Ctx()
    try:
        _seed(ctx.db)
        prot, reasons = gs.is_protected(ctx.db, "20260802_100000_bbb222")
        assert prot, "进行中会话未被保护"
        assert any("ended_at为空" in r for r in reasons), f"原因缺失: {reasons}"
    finally:
        ctx.cleanup()


def test_protect_pinned_session():
    """M6：pinned=1 → 受保护。"""
    ctx = _Ctx()
    try:
        _seed(ctx.db)
        prot, reasons = gs.is_protected(ctx.db, "20260803_100000_ccc333")
        assert prot, "pinned 会话未被保护"
        assert any("pinned" in r for r in reasons), f"原因缺失: {reasons}"
    finally:
        ctx.cleanup()


def test_protect_recent_writes_24h():
    """M6：已结束但最近 24h 内有消息写入 → 受保护。"""
    ctx = _Ctx()
    try:
        _seed(ctx.db)
        conn = sqlite3.connect(ctx.db)
        try:
            # aaa111 已结束（3 天前），把最后一条消息改为 1 小时前 → 活跃
            conn.execute("UPDATE messages SET timestamp = ? WHERE id = 3",
                         (time.time() - 3600,))
            conn.commit()
        finally:
            conn.close()
        prot, reasons = gs.is_protected(ctx.db, "20260801_100000_aaa111")
        assert prot, "最近 24h 写入的会话未被保护"
        assert any("24h" in r for r in reasons), f"原因缺失: {reasons}"
    finally:
        ctx.cleanup()


def test_protect_exempt_meta():
    """M6：governance_meta.exempt=1（不可变白名单）→ 受保护。"""
    ctx = _Ctx()
    try:
        _seed(ctx.db)
        gov.ensure_schema(ctx.db)
        conn = sqlite3.connect(ctx.db)
        try:
            now = time.time()
            conn.execute(
                "INSERT INTO governance_meta (session_id, profile_name, state, exempt,"
                " created_at, updated_at) VALUES (?,?,?,?,?,?)",
                ("20260801_100000_aaa111", "default", "active", 1, now, now),
            )
            conn.commit()
        finally:
            conn.close()
        prot, reasons = gs.is_protected(ctx.db, "20260801_100000_aaa111")
        assert prot, "exempt=1 会话未被保护"
        assert any("exempt" in r for r in reasons), f"原因缺失: {reasons}"
    finally:
        ctx.cleanup()


def test_protect_fix_chain():
    """M6：修复链（cluster_member.keep_chain=1）→ 受保护。"""
    ctx = _Ctx()
    try:
        _seed(ctx.db)
        gov.ensure_schema(ctx.db)
        conn = sqlite3.connect(ctx.db)
        try:
            conn.execute(
                "INSERT INTO governance_cluster (cluster_id, topic, topic_kind,"
                " profile_name, member_count, created_at)"
                " VALUES (?,?,?,?,?,?)",
                ("cl_fix_001", "NameError 修复链", "fix_chain", "default", 2, time.time()),
            )
            conn.execute(
                "INSERT INTO governance_cluster_member (cluster_id, session_id, message_id,"
                " dedup_flag, keep_chain) VALUES (?,?,?,?,?)",
                ("cl_fix_001", "20260801_100000_aaa111", 4, 0, 1),
            )
            conn.commit()
        finally:
            conn.close()
        prot, reasons = gs.is_protected(ctx.db, "20260801_100000_aaa111")
        assert prot, "修复链会话未被保护"
        assert any("修复链" in r for r in reasons), f"原因缺失: {reasons}"
    finally:
        ctx.cleanup()


def test_protect_unrecoverable_source():
    """M6：无 source/cwd/session_key/origin_json（无法溯源）→ 受保护。"""
    ctx = _Ctx()
    try:
        _seed(ctx.db)
        conn = sqlite3.connect(ctx.db)
        try:
            conn.execute(
                "INSERT INTO sessions (id, profile_name, title, started_at, ended_at,"
                " message_count, archived, pinned)"
                " VALUES (?,?,?,?,?,?,?,?)",
                ("20260804_100000_ddd444", "default", "无源会话",
                 time.time() - 86400 * 10, time.time() - 86400 * 10 + 60, 0, 0, 0),
            )
            conn.commit()
        finally:
            conn.close()
        prot, reasons = gs.is_protected(ctx.db, "20260804_100000_ddd444")
        assert prot, "不可回溯会话未被保护"
        assert any("不可回溯" in r for r in reasons), f"原因缺失: {reasons}"
    finally:
        ctx.cleanup()


def test_protect_old_closed_archivable():
    """M6 反向：老、已结束、无保护 → 可归档（非受保护）。"""
    ctx = _Ctx()
    try:
        _seed(ctx.db)
        conn = sqlite3.connect(ctx.db)
        try:
            conn.execute(
                "INSERT INTO sessions (id, source, profile_name, session_key, title,"
                " started_at, ended_at, message_count, cwd, origin_json, archived, pinned)"
                " VALUES (?,?,?,?,?,?,?,?,?,?,?,?)",
                ("20260701_100000_eee555", "cli", "default", "旧任务", "30 天前旧会话",
                 time.time() - 86400 * 40, time.time() - 86400 * 40 + 3600, 0,
                 "/tmp/ag-test-root", "{}", 0, 0),
            )
            conn.commit()
        finally:
            conn.close()
        prot, _ = gs.is_protected(ctx.db, "20260701_100000_eee555")
        assert not prot, "可归档旧会话被误判为受保护"
    finally:
        ctx.cleanup()


def test_audit_log_complete_fields():
    """M6 验收：审计日志完整（what/when/why = op/时间/原因 + 状态 + 操作者）。"""
    ctx = _Ctx()
    try:
        _seed(ctx.db)
        _archive_one(ctx, "20260801_100000_aaa111")
        entries = gs.audit_log(ctx.db, last=50)
        assert entries, "审计日志为空"
        e = entries[0]
        for field in ("op", "session_id", "profile", "before", "after",
                      "reason", "operator", "evidence", "status", "when"):
            assert e.get(field) is not None, f"审计字段缺失: {field}"
        assert e["op"] == "cold_archive"
        assert e["before"] == "active" and e["after"] == "cold_archived"
        assert e["when"], "审计时间缺失（when）"
        assert e["reason"] == "test", "审计原因缺失（why）"
        # AC-D5.4：审计不含消息原文
        assert "SAFETY" not in e["evidence"] and "ENV-6" not in e["evidence"], \
            "审计日志泄露消息原文"
    finally:
        ctx.cleanup()


# ─────────────────────────── M7：安全基线 ───────────────────────────

def test_security_check_redaction_and_whitelist():
    """M7：脱敏（AC-D3.4）+ 白名单（AC-D5.3）自检通过。"""
    ctx = _Ctx()
    try:
        _seed(ctx.db)
        gov.ensure_schema(ctx.db)
        r = gs.security_check(ctx.db)
        by_id = {c["id"]: c for c in r["checks"]}
        assert by_id["AC-D3.4"]["pass"], f"脱敏自检失败: {by_id['AC-D3.4']['detail']}"
        assert by_id["AC-D5.3"]["pass"], f"白名单自检失败: {by_id['AC-D5.3']['detail']}"
    finally:
        ctx.cleanup()


def test_security_check_dur1_backup_coverage():
    """M7 验收（AC-D4.3）：session-archive/ 已纳入 DUR-1 备份覆盖。"""
    ctx = _Ctx()
    try:
        _seed(ctx.db)
        gov.ensure_schema(ctx.db)
        r = gs.security_check(ctx.db)
        by_id = {c["id"]: c for c in r["checks"]}
        assert by_id["AC-D4.3"]["pass"], \
            f"DUR-1 备份覆盖失败: {by_id['AC-D4.3']['detail']}"
    finally:
        ctx.cleanup()


# ─────────────────────────── M8：首次 dry-run ───────────────────────────

def _patch_profile_paths(ctx: _Ctx) -> dict:
    """把 dry-run 的 profile db 路径临时指向测试库（隔离：不碰真实 state.db）。"""
    orig = dict(gs.PROFILE_DB_PATHS)
    for p in gs.DRY_RUN_ORDER:
        gs.PROFILE_DB_PATHS[p] = ctx.db
    return orig


def test_dry_run_profile_readonly_no_writes():
    """M8：dry-run 全程只读，不写库（事务数/治理表计数不变）。"""
    ctx = _Ctx()
    try:
        _seed(ctx.db)
        gov.ensure_schema(ctx.db)
        conn = sqlite3.connect(ctx.db)
        before_logs = conn.execute("SELECT COUNT(*) FROM governance_log").fetchone()[0]
        conn.close()

        out_lines: list[str] = []
        gs.dry_run_profile(ctx.db, "default", out_lines, lag_ratio=0.9)

        conn = sqlite3.connect(ctx.db)
        try:
            after_logs = conn.execute("SELECT COUNT(*) FROM governance_log").fetchone()[0]
            assert after_logs == before_logs, "dry-run 写了审计日志（应只读）"
            arch = conn.execute("SELECT COUNT(*) FROM sessions WHERE archived=1").fetchone()[0]
            assert arch == 0, "dry-run 不应改动 archived 标记"
        finally:
            conn.close()
    finally:
        ctx.cleanup()


def test_dry_run_profile_under_threshold():
    """M8：未超阈值 → 无候选，标注 ✅。"""
    ctx = _Ctx()
    try:
        _seed(ctx.db)
        gov.ensure_schema(ctx.db)
        out_lines: list[str] = []
        # 测试库 ~几十 KB，阈值设 60MB → 必然未超
        r = gs.dry_run_profile(ctx.db, "default", out_lines, lag_ratio=0.9)
        assert r["over"] is False, "小库不应超阈值"
        assert r["candidates"] == [], "未超阈值不应有候选"
        assert any("无需归档" in l for l in out_lines), f"输出缺标注: {out_lines}"
    finally:
        ctx.cleanup()


def test_dry_run_profile_over_threshold_candidates_and_skip_protected():
    """M8：超阈值 → 最旧优先候选 + 受保护会话跳过 + 达标判定。"""
    ctx = _Ctx()
    try:
        _seed(ctx.db)
        gov.ensure_schema(ctx.db)
        # 加一个 40 天前可归档旧会话，作为首选候选
        conn = sqlite3.connect(ctx.db)
        try:
            conn.execute(
                "INSERT INTO sessions (id, source, profile_name, session_key, title,"
                " started_at, ended_at, message_count, cwd, origin_json, archived, pinned)"
                " VALUES (?,?,?,?,?,?,?,?,?,?,?,?)",
                ("20260601_100000_fff666", "cli", "default", "老任务", "60 天前旧会话",
                 time.time() - 86400 * 60, time.time() - 86400 * 60 + 3600, 0,
                 "/tmp/ag-test-root", "{}", 0, 0),
            )
            conn.commit()
        finally:
            conn.close()

        out_lines: list[str] = []
        # 阈值设 0.001MB（~1KB）→ 必然超；目标 ≤0.0009MB
        # FIX-B3: 解析入口统一走 governance_config, 测试改打 cfg 而非 gs.DEFAULT_CONFIG
        orig_limit = cfg.DEFAULT_THRESHOLD_MB
        orig_overrides = dict(cfg.PROFILE_OVERRIDE_MB)
        cfg.DEFAULT_THRESHOLD_MB = 0.001
        cfg.PROFILE_OVERRIDE_MB = {}
        try:
            r = gs.dry_run_profile(ctx.db, "default", out_lines, lag_ratio=0.9)
        finally:
            cfg.DEFAULT_THRESHOLD_MB = orig_limit
            cfg.PROFILE_OVERRIDE_MB = orig_overrides

        assert r["over"] is True, "大库应超阈值"
        # 候选中最旧优先：fff666 应先出现；pinned ccc333 与进行中 bbb222 被跳过
        sids = [c["session_id"] for c in r["candidates"]]
        assert "20260601_100000_fff666" in sids, f"最旧会话未入候选: {sids}"
        assert "20260803_100000_ccc333" not in sids, "pinned 会话不应入候选"
        assert "20260802_100000_bbb222" not in sids, "进行中会话不应入候选"
        assert r["protected_skipped"] >= 2, f"受保护跳过计数异常: {r['protected_skipped']}"
    finally:
        ctx.cleanup()


def test_dry_run_all_writes_log_file():
    """M8 验收：dry-run 生成日志文件（候选清单 + 汇总分析 + 明确只读声明）。"""
    ctx = _Ctx()
    try:
        _seed(ctx.db)
        gov.ensure_schema(ctx.db)
        orig = _patch_profile_paths(ctx)
        try:
            out_file = os.path.join(ctx.tmp, "governance-dry-run.log")
            results = gs.dry_run_all(out_file=out_file)
        finally:
            gs.PROFILE_DB_PATHS.clear()
            gs.PROFILE_DB_PATHS.update(orig)

        assert os.path.exists(out_file), "dry-run 日志文件未生成"
        text = open(out_file, encoding="utf-8").read()
        assert "只读" in text, "日志缺只读声明"
        assert "汇总分析" in text, "日志缺汇总分析"
        assert "duanmu" in text and "default" in text, "日志缺 profile 顺序"
        assert "未执行任何归档" in text or "无需归档" in text, "日志缺未执行声明"
        meta = results[-1]
        assert "out_file" in meta and "total_candidates" in meta
        assert meta["total_candidates"] >= 0
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
