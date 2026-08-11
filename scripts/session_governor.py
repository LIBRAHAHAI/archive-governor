#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
session_governor.py — 会话语义治理 MVP-1（M1-M2）

范围：
  M1: 治理四表（governance_meta / governance_log / governance_archive_index(+FTS) /
      governance_cluster(+member)）+ WAL 模式 + 全参数化查询（AC-D5.1）
  M2: 冷归档原子流程：先写 .gz + manifest + sha256 → 后删热行 → restore 原位恢复
      （AC-D1.2 / D1.3 / D1.4，K2 round-trip sha256 一致）

设计来源：内部设计文档 (Hermes-Salon exchange 320)
  §4 两态生命周期 / §5 数据模型 / §8 冷归档原子流程 / §10 阈值 / §12 19 AC

安全原则（xuanwu 19 AC）：
  - AC-D5.1 全库参数化查询（禁止字符串拼接 SQL）
  - AC-D5.2 WAL + .gz 原子 rename（先写 .tmp 后 os.replace）
  - AC-D5.3 输入白名单校验（session_id/profile_name 正则）
  - AC-D5.4 governance_log 不含消息原文（只记证据链/哈希/计数）
  - AC-D1.4 .gz sha256 双层校验（文件级 + 内容级 manifest）

用法：
  python session_governor.py ensure-schema --db <state.db>
  python session_governor.py archive    --db <state.db> --session <id> [--profile p] [--archive-root dir]
  python session_governor.py restore    --db <state.db> --session <id> [--archive-root dir]
  python session_governor.py verify     --db <state.db> --session <id> [--archive-root dir]
  python session_governor.py threshold  --db <state.db> [--profile default] [--limit-mb 80]
  python session_governor.py status     --db <state.db>

阈值口径 (FIX-B3): 与 governance_detector / startup_check / reclaim 统一走
  governance_config 单一事实源 — default=80MB, 其他 profile=60MB, 保留比例 0.85。
"""

from __future__ import annotations

import argparse
import gzip
import hashlib
import json
import os
import re
import sqlite3
import sys
import time
from typing import Any, Optional

import governance_config as cfg  # FIX-B3: 阈值单一事实源

# ─────────────────────────── 常量与配置 ───────────────────────────

SCHEMA_VERSION = 1

# 默认配置（与 320 §10 一致；archive_root 可由 --archive-root 覆盖）
# FIX-B3: storage_limit_mb / profile_overrides 由 governance_config 派生, 不复制常量
DEFAULT_CONFIG = {
    "storage_limit_mb": cfg.DEFAULT_THRESHOLD_MB,   # 每 profile 阈值 (60)
    "storage_ceiling_mb": cfg.STORAGE_CEILING_MB,    # 硬顶（仅告警不丢数据）
    "profile_overrides": dict(cfg.PROFILE_OVERRIDE_MB),  # {"default": 80}
    "trigger": "threshold",
    # 2026-08-11 开源化: archive_root 可配置默认 (原硬编码内部工作区, 发布后自行配置)
    "archive_root": os.path.join(cfg.hermes_home(), "..", "archive-governor-data", "archives"),
}

# 输入白名单（AC-D5.3）：session_id / profile_name 只允许字母数字 _ -
_ID_RE = re.compile(r"^[A-Za-z0-9_-]+$")

# ─────────────────────────── SQL（全部参数化） ───────────────────────────

_SQL_CREATE_TABLES = [
    # 治理状态表：两态（320 §5.2）
    """
    CREATE TABLE IF NOT EXISTS governance_meta (
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
    )
    """,
    # 审计日志：所有治理操作（AC-D4.1 / D5.4 不含原文）
    """
    CREATE TABLE IF NOT EXISTS governance_log (
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
    )
    """,
    # 归档独立索引（320 §5.2）：指向 .gz，不触冷数据
    """
    CREATE TABLE IF NOT EXISTS governance_archive_index (
        session_id   TEXT PRIMARY KEY,
        profile_name TEXT,
        title        TEXT,
        keywords     TEXT,
        project_tags TEXT,
        topic_kind   TEXT,
        message_count INTEGER,
        started_at   REAL,
        ended_at     REAL,
        archive_file TEXT,
        sha256       TEXT
    )
    """,
    # 归档 FTS（external content，内容指向 governance_archive_index）
    """
    CREATE VIRTUAL TABLE IF NOT EXISTS governance_archive_index_fts USING fts5(
        title, keywords, project_tags,
        content='governance_archive_index', content_rowid='rowid'
    )
    """,
    # 内容簇（M1 仅建表；M4/v1.1 聚类时使用）
    """
    CREATE TABLE IF NOT EXISTS governance_cluster (
        cluster_id   TEXT PRIMARY KEY,
        topic        TEXT,
        topic_kind   TEXT,
        profile_name TEXT,
        source_hint  TEXT,
        member_count INTEGER,
        created_at   REAL
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS governance_cluster_member (
        cluster_id   TEXT,
        session_id   TEXT,
        message_id   INTEGER,
        dedup_flag   INTEGER DEFAULT 0,
        keep_chain   INTEGER DEFAULT 1,
        PRIMARY KEY (cluster_id, message_id)
    )
    """,
]

# ─────────────────────────── 工具函数 ───────────────────────────

def _validate_id(value: Optional[str], field: str = "id") -> str:
    """输入白名单校验（AC-D5.3）：非法输入直接拒绝，不进入 SQL。"""
    if not value or not _ID_RE.match(value):
        raise ValueError(f"非法 {field}: {value!r}（仅允许字母/数字/下划线/连字符）")
    return value


def _log(conn: sqlite3.Connection, *, op: str, session_id: Optional[str],
         profile_name: Optional[str], before_state: Optional[str],
         after_state: Optional[str], reason: str, operator: str,
         evidence: str, status: str) -> None:
    """写审计日志（AC-D4.1）。evidence 只放哈希/计数/阈值等元数据，绝不放原文（AC-D5.4）。"""
    conn.execute(
        "INSERT INTO governance_log (op, session_id, profile_name, before_state, after_state,"
        " reason, operator, evidence, status, created_at)"
        " VALUES (?,?,?,?,?,?,?,?,?,?)",
        (op, session_id, profile_name, before_state, after_state,
         reason, operator, evidence, status, time.time()),
    )


def _connect(db_path: str) -> sqlite3.Connection:
    """打开连接：WAL + 手动事务控制 + Row 工厂。"""
    conn = sqlite3.connect(db_path, isolation_level=None)  # 手动 BEGIN/COMMIT
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA busy_timeout = 5000")
    return conn


def _ensure_wal(db_path: str) -> str:
    """开启 WAL 模式（幂等，AC-D5.2）。返回当前 journal_mode。"""
    conn = _connect(db_path)
    try:
        mode = conn.execute("PRAGMA journal_mode=WAL").fetchone()[0]
        return mode
    finally:
        conn.close()


# ─────────────────────────── M1：建表 + WAL ───────────────────────────

def ensure_schema(db_path: str) -> list[str]:
    """建治理四表 + FTS + 簇表（幂等：重复执行不报错）。返回已创建/已存在的表名列表。"""
    if not os.path.exists(db_path):
        raise FileNotFoundError(f"state.db 不存在: {db_path}")
    _ensure_wal(db_path)
    conn = _connect(db_path)
    try:
        conn.execute("BEGIN")
        for sql in _SQL_CREATE_TABLES:
            conn.execute(sql)
        conn.commit()
        rows = conn.execute(
            "SELECT name FROM sqlite_master WHERE type IN ('table','virtual table')"
            " AND name LIKE 'governance_%' ORDER BY name"
        ).fetchall()
        return [r[0] for r in rows]
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


# ─────────────────────────── M2：冷归档原子流程 ───────────────────────────

def _extract_keywords(title: Optional[str], session_key: Optional[str],
                      cwd: Optional[str]) -> tuple[str, str]:
    """零 LLM 规则提取 keywords + project_tags（320 §7.2）。

    规则：
      - project_tags: 任务号 t_\\d+ / 协议引用 exchange/\\d+ / 路径片段
      - keywords: title 与 session_key 分词（空白/标点切分），去重去空
    """
    tags: list[str] = []
    text = " ".join(x for x in (title or "", session_key or "", cwd or "") if x)
    tags += re.findall(r"t_\d+", text)
    tags += re.findall(r"exchange/\d+", text)
    tags += re.findall(r"[A-Za-z0-9_-]+", text)[:8]
    seen: list[str] = []
    for t in tags:
        if t not in seen:
            seen.append(t)
    keywords = " ".join(seen[:12])
    project_tags = " ".join(tags[:6])
    return keywords, project_tags


def _export_session(conn: sqlite3.Connection, session_id: str) -> tuple[sqlite3.Row, list[sqlite3.Row]]:
    """导出 session 元数据 + 全部 messages（按 id 升序，保持原始顺序）。"""
    session = conn.execute("SELECT * FROM sessions WHERE id = ?", (session_id,)).fetchone()
    if session is None:
        raise ValueError(f"会话不存在: {session_id}")
    messages = conn.execute(
        "SELECT * FROM messages WHERE session_id = ? ORDER BY id", (session_id,)
    ).fetchall()
    return session, messages


def _build_jsonl(session: sqlite3.Row, messages: list[sqlite3.Row]) -> tuple[str, str]:
    """构造 JSONL：首行 manifest，次行 session 元数据，随后逐行 message。

    content_sha256 定义为「数据行（session + 全部 messages）的拼接哈希」——
    不含 manifest 自身，避免鸡生蛋（manifest 内嵌自身哈希无法自洽）。

    返回 (jsonl_text, content_sha256)。
    """
    session_dict = {k: session[k] for k in session.keys()}
    data_lines = [json.dumps({"type": "session", "data": session_dict},
                             ensure_ascii=False, default=str)]
    for m in messages:
        data_lines.append(json.dumps({"type": "message",
                                      "data": {k: m[k] for k in m.keys()}},
                                     ensure_ascii=False, default=str))
    content_sha256 = _sha256_bytes("\n".join(data_lines).encode("utf-8"))

    manifest = {
        "schema_version": SCHEMA_VERSION,
        "type": "manifest",
        "session_id": session_dict.get("id"),
        "profile_name": session_dict.get("profile_name"),
        "message_count": len(messages),
        "started_at": session_dict.get("started_at"),
        "ended_at": session_dict.get("ended_at"),
        "content_sha256": content_sha256,
        "exported_at": time.time(),
    }
    lines = [json.dumps(manifest, ensure_ascii=False, default=str)] + data_lines
    return "\n".join(lines) + "\n", content_sha256


def _sha256_bytes(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def _sha256_file(path: str) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(65536), b""):
            h.update(chunk)
    return h.hexdigest()


def archive_session(db_path: str, session_id: str, archive_root: Optional[str] = None,
                    profile_name: Optional[str] = None, reason: str = "threshold",
                    operator: str = "auto") -> dict[str, Any]:
    """冷归档原子流程（320 §8.1 六步，AC-D1.2）：

      1. 导出 session 元数据 + messages → JSONL（含 schema 版本）
      2. gzip → session-archive/{profile}/{session_id}.json.gz.tmp
      3. 计算 sha256（.gz.tmp）
      4. 校验 sha256 == 预期（文件级；内容级由 manifest 内嵌 content_sha256 承载）
      5. 原子 rename .gz.tmp → .gz（os.replace）
      6. 写 governance_archive_index + FTS
      7. UPDATE governance_meta SET state='cold_archived'
      8. UPDATE sessions SET archived=1
      9. DELETE messages WHERE session_id=?   -- 释放热存储
     10. COMMIT

    任一步失败 → ROLLBACK + 清理 .gz/.tmp → governance_log status=rolled_back。
    返回 {"archive_file", "sha256", "message_count", "storage_saved_bytes"}。
    """
    session_id = _validate_id(session_id, "session_id")
    archive_root = archive_root or DEFAULT_CONFIG["archive_root"]
    profile = profile_name or "default"
    profile = _validate_id(profile, "profile_name")

    conn = _connect(db_path)
    tmp_gz: Optional[str] = None
    final_gz: Optional[str] = None
    try:
        # ── 事务外：导出 + 压缩 + 写 .tmp + 计算 sha256（确保文件先完整落盘）──
        session, messages = _export_session(conn, session_id)

        # 已归档拒绝重复归档（两态状态机：active ↔ cold_archived）
        meta = conn.execute(
            "SELECT state, archive_file FROM governance_meta WHERE session_id = ?",
            (session_id,),
        ).fetchone()
        archived_flag = session["archived"] if "archived" in session.keys() else 0
        if meta is not None and meta["state"] == "cold_archived":
            raise ValueError(f"会话已归档（cold_archived），禁止重复归档: {session_id}")
        if archived_flag:
            raise ValueError(f"会话已置 archived=1，禁止重复归档: {session_id}")

        # 不可变保护基础版（320 §6.3；M6 完善 pinned/活跃/修复链）
        if "pinned" in session.keys() and session["pinned"]:
            raise ValueError(f"会话 pinned，不可自动归档: {session_id}")

        # 序列化 → 内容级 sha256（数据行）→ manifest → gzip
        raw_jsonl, content_sha = _build_jsonl(session, messages)
        gz_bytes = gzip.compress(raw_jsonl.encode("utf-8"), compresslevel=6)

        archive_dir = os.path.join(archive_root, profile, "session-archive")
        os.makedirs(archive_dir, exist_ok=True)
        tmp_gz = os.path.join(archive_dir, f"{session_id}.json.gz.tmp")
        final_gz = os.path.join(archive_dir, f"{session_id}.json.gz")

        with open(tmp_gz, "wb") as f:
            f.write(gz_bytes)
        file_sha = _sha256_file(tmp_gz)

        # 步骤 4：文件级校验（写回再读，防静默损坏；AC-D1.4）
        if _sha256_file(tmp_gz) != file_sha:
            raise RuntimeError(f"sha256 校验失败（写盘损坏）: {session_id}")

        with gzip.open(tmp_gz, "rt", encoding="utf-8") as f:
            header = json.loads(f.readline())
            data_lines = [line.rstrip("\n") for line in f]
        if header.get("content_sha256") != _sha256_bytes("\n".join(data_lines).encode("utf-8")):
            raise RuntimeError(f"内容级 sha256 校验失败: {session_id}")

        # ── 事务内：rename + 索引 + 状态 + 删热行 ──
        conn.execute("BEGIN IMMEDIATE")
        os.replace(tmp_gz, final_gz)  # 原子 rename（AC-D5.2）
        tmp_gz = None  # 已消费

        title = session["title"] if "title" in session.keys() else None
        session_key = session["session_key"] if "session_key" in session.keys() else None
        cwd = session["cwd"] if "cwd" in session.keys() else None
        keywords, project_tags = _extract_keywords(title, session_key, cwd)
        msg_count = len(messages)
        started = session["started_at"] if "started_at" in session.keys() else None
        ended = session["ended_at"] if "ended_at" in session.keys() else None
        gz_size = os.path.getsize(final_gz)

        # 6. 归档索引 + FTS
        conn.execute(
            "INSERT OR REPLACE INTO governance_archive_index"
            " (session_id, profile_name, title, keywords, project_tags, topic_kind,"
            "  message_count, started_at, ended_at, archive_file, sha256)"
            " VALUES (?,?,?,?,?,?,?,?,?,?,?)",
            (session_id, profile, title, keywords, project_tags, "none",
             msg_count, started, ended, os.path.relpath(final_gz, archive_root), file_sha),
        )
        conn.execute(
            "INSERT OR REPLACE INTO governance_archive_index_fts (rowid, title, keywords, project_tags)"
            " SELECT rowid, title, keywords, project_tags FROM governance_archive_index"
            " WHERE session_id = ?",
            (session_id,),
        )

        # 7. 治理状态 → cold_archived
        now = time.time()
        conn.execute(
            "INSERT INTO governance_meta (session_id, profile_name, state, cold_archived_at,"
            " reason, archive_file, archive_sha256, archive_size_bytes, created_at, updated_at)"
            " VALUES (?,?,?,?,?,?,?,?,?,?)"
            " ON CONFLICT(session_id) DO UPDATE SET"
            " state='cold_archived', cold_archived_at=excluded.cold_archived_at,"
            " reason=excluded.reason, archive_file=excluded.archive_file,"
            " archive_sha256=excluded.archive_sha256,"
            " archive_size_bytes=excluded.archive_size_bytes, updated_at=excluded.updated_at",
            (session_id, profile, "cold_archived", now, reason,
             os.path.relpath(final_gz, archive_root), file_sha, gz_size, now, now),
        )

        # 8. 复用现有 archived 列（零 ALTER 核心表，320 §4.2）
        if "archived" in session.keys():
            conn.execute("UPDATE sessions SET archived = 1 WHERE id = ?", (session_id,))
        else:
            raise RuntimeError("sessions 表缺少 archived 列，无法标记归档状态")

        # 9. 删热行释放热存储（先写后删：文件已在步骤 5 落盘）
        conn.execute("DELETE FROM messages WHERE session_id = ?", (session_id,))

        # 审计
        _log(conn, op="cold_archive", session_id=session_id, profile_name=profile,
             before_state="active", after_state="cold_archived",
             reason=reason, operator=operator,
             evidence=f"sha256={file_sha[:16]}.. msgs={msg_count} bytes={gz_size}",
             status="done")

        conn.commit()

        storage_saved = _estimate_row_bytes(messages)
        return {
            "archive_file": final_gz,
            "rel_archive_file": os.path.relpath(final_gz, archive_root),
            "sha256": file_sha,
            "message_count": msg_count,
            "storage_saved_bytes": storage_saved,
        }
    except Exception as exc:
        try:
            conn.rollback()
        except Exception:
            pass
        # 清理残留文件（.tmp 未消费则删；已 rename 的 .gz 若事务回滚也一并清理）
        for p in (tmp_gz, final_gz):
            if p and os.path.exists(p):
                try:
                    os.remove(p)
                except OSError:
                    pass
        # 审计失败（best-effort，若表已建）
        try:
            _log(conn, op="cold_archive", session_id=session_id,
                 profile_name=profile_name or "default",
                 before_state="active", after_state="active",
                 reason=reason, operator=operator,
                 evidence=f"error={type(exc).__name__}: {exc}",
                 status="rolled_back")
            conn.commit()
        except Exception:
            pass
        raise
    finally:
        conn.close()


def _estimate_row_bytes(messages: list[sqlite3.Row]) -> int:
    """粗估释放字节：JSON 序列化大小（真实回收以 VACUUM 后为准，M3 空间回收负责）。"""
    return sum(len(json.dumps({k: m[k] for k in m.keys()}, ensure_ascii=False, default=str))
               for m in messages)


def verify_archive(db_path: str, session_id: str, archive_root: Optional[str] = None,
                   profile_name: Optional[str] = None) -> dict[str, Any]:
    """校验归档包完整性（AC-D1.4）：文件级 sha256 与 governance_meta 记录一致 + 内容级校验。"""
    session_id = _validate_id(session_id, "session_id")
    archive_root = archive_root or DEFAULT_CONFIG["archive_root"]
    profile = profile_name or "default"
    profile = _validate_id(profile, "profile_name")

    conn = _connect(db_path)
    try:
        meta = conn.execute(
            "SELECT archive_file, archive_sha256, archive_size_bytes FROM governance_meta"
            " WHERE session_id = ? AND state = 'cold_archived'",
            (session_id,),
        ).fetchone()
        if meta is None:
            raise ValueError(f"无归档记录: {session_id}（state 非 cold_archived 或不存在）")
        abs_gz = os.path.join(archive_root, meta["archive_file"])
        if not os.path.exists(abs_gz):
            raise FileNotFoundError(f"归档文件缺失: {abs_gz}")

        file_sha = _sha256_file(abs_gz)
        if file_sha != meta["archive_sha256"]:
            raise RuntimeError(
                f"文件级 sha256 不匹配: {session_id} 记录={meta['archive_sha256'][:16]}.. "
                f"实际={file_sha[:16]}.."
            )

        with gzip.open(abs_gz, "rt", encoding="utf-8") as f:
            header = json.loads(f.readline())
        if header.get("schema_version") != SCHEMA_VERSION:
            raise RuntimeError(f"schema 版本不匹配: {session_id}")
        # 内容级校验：重算数据行 sha256 与 manifest 内嵌值比对（口径与归档一致）
        with gzip.open(abs_gz, "rt", encoding="utf-8") as f:
            f.readline()  # 跳过 manifest
            data_lines = [line.rstrip("\n") for line in f]
        content_sha = _sha256_bytes("\n".join(data_lines).encode("utf-8"))
        if header.get("content_sha256") != content_sha:
            raise RuntimeError(f"内容级 sha256 不匹配: {session_id}")

        return {
            "ok": True,
            "archive_file": abs_gz,
            "file_sha256": file_sha,
            "message_count": header.get("message_count"),
            "size_bytes": os.path.getsize(abs_gz),
        }
    finally:
        conn.close()


def restore_session(db_path: str, session_id: str, archive_root: Optional[str] = None,
                    profile_name: Optional[str] = None,
                    operator: str = "program") -> dict[str, Any]:
    """原位恢复（320 §4.3，AC-D1.3）：

      1. 校验 .gz sha256 == governance_meta.archive_sha256（不匹配拒绝恢复）
      2. 解压 → JSONL → 校验 manifest（session_id 一致、message_count 一致、内容 sha256 一致）
      3. 事务内重建 messages 行（原 id/顺序/正文/时间戳逐字节一致）
      4. UPDATE sessions SET archived=0；UPDATE governance_meta SET state='active'
      5. 审计日志
    返回 {"restored_messages", "session_id"}。
    """
    session_id = _validate_id(session_id, "session_id")
    archive_root = archive_root or DEFAULT_CONFIG["archive_root"]
    profile = profile_name or "default"
    profile = _validate_id(profile, "profile_name")

    conn = _connect(db_path)
    try:
        meta = conn.execute(
            "SELECT archive_file, archive_sha256 FROM governance_meta"
            " WHERE session_id = ? AND state = 'cold_archived'",
            (session_id,),
        ).fetchone()
        if meta is None:
            raise ValueError(f"无 cold_archived 归档记录: {session_id}")
        abs_gz = os.path.join(archive_root, meta["archive_file"])
        if not os.path.exists(abs_gz):
            raise FileNotFoundError(f"归档文件缺失: {abs_gz}")

        # 1. 文件级 sha256 校验
        if _sha256_file(abs_gz) != meta["archive_sha256"]:
            raise RuntimeError(f"归档包 sha256 校验失败，拒绝恢复: {session_id}")

        # 2. 解压 + manifest 校验
        with gzip.open(abs_gz, "rt", encoding="utf-8") as f:
            header = json.loads(f.readline())
            session_row = json.loads(f.readline())
            message_rows = [json.loads(line) for line in f]

        if header.get("session_id") != session_id:
            raise RuntimeError(f"manifest session_id 不一致: {session_id}")
        if header.get("message_count") != len(message_rows):
            raise RuntimeError(
                f"manifest message_count={header.get('message_count')} "
                f"实际={len(message_rows)}，拒绝恢复"
            )
        # 内容级校验（与归档时相同口径：数据行拼接 sha256，不含 manifest）
        with gzip.open(abs_gz, "rt", encoding="utf-8") as f:
            f.readline()  # 跳过 manifest
            data_lines = [line.rstrip("\n") for line in f]
        if header.get("content_sha256") != _sha256_bytes("\n".join(data_lines).encode("utf-8")):
            raise RuntimeError(f"内容级 sha256 校验失败，拒绝恢复: {session_id}")

        # 3-4. 事务内重建
        conn.execute("BEGIN IMMEDIATE")
        # 安全：目标 session 不应残留热消息（归档时已删；若有人写入则拒绝覆盖）
        existing = conn.execute(
            "SELECT COUNT(*) AS c FROM messages WHERE session_id = ?", (session_id,)
        ).fetchone()["c"]
        if existing:
            raise RuntimeError(
                f"目标 session 仍有 {existing} 条热消息，拒绝覆盖式恢复（先人工确认）: {session_id}"
            )

        # 重建 messages（保留原 id/顺序/正文/时间戳）
        for row in message_rows:
            data = row["data"]
            keys = list(data.keys())
            placeholders = ",".join("?" * len(keys))
            conn.execute(
                f"INSERT OR REPLACE INTO messages ({','.join(keys)}) VALUES ({placeholders})",
                [data[k] for k in keys],
            )

        # 原位恢复：sessions 行若还在则置 archived=0
        sess = conn.execute("SELECT id FROM sessions WHERE id = ?", (session_id,)).fetchone()
        if sess is not None and "archived" in [r[1] for r in
                                               conn.execute("PRAGMA table_info(sessions)")]:
            conn.execute("UPDATE sessions SET archived = 0 WHERE id = ?", (session_id,))

        # 治理状态回 active
        now = time.time()
        conn.execute(
            "UPDATE governance_meta SET state = 'active', updated_at = ? WHERE session_id = ?",
            (now, session_id),
        )
        _log(conn, op="restore", session_id=session_id, profile_name=profile,
             before_state="cold_archived", after_state="active",
             reason="manual_restore", operator=operator,
             evidence=f"msgs={len(message_rows)} sha256={meta['archive_sha256'][:16]}..",
             status="done")
        conn.commit()

        return {"restored_messages": len(message_rows), "session_id": session_id}
    except Exception:
        try:
            conn.rollback()
        except Exception:
            pass
        raise
    finally:
        conn.close()


# ─────────────────────────── L0 阈值检测（320 §10） ───────────────────────────

def threshold_check(db_path: str, limit_mb: Optional[float] = None,
                    profile: str = "default") -> dict[str, Any]:
    """L0 阈值检测：state.db 单文件大小 vs 阈值（AC-D2.1/D2.2）。

    FIX-B3: 阈值按 profile 解析 (default=80MB, 其他=60MB, 显式 limit_mb 覆盖),
    与 governance_detector / startup_check / reclaim 共用 governance_config 单一事实源。
    target_mb = 阈值 × 保留比例 0.85 (15% 滞后带), 明确保留比例语义。

    返回 {"db_size_mb", "limit_mb", "target_mb", "over", "oldest_sessions"}；
    不触发任何写操作。
    """
    limit_mb = cfg.resolve_threshold_mb(profile, limit_mb)
    # 热存储 = state.db + WAL（WAL 模式下未 checkpoint 的数据在 -wal 文件）
    size_bytes = os.path.getsize(db_path)
    for suffix in ("-wal", "-shm"):
        p = db_path + suffix
        if os.path.exists(p):
            size_bytes += os.path.getsize(p)
    size_mb = size_bytes / (1024 * 1024)
    target_mb = cfg.retention_target_bytes(int(limit_mb * 1024 * 1024)) / (1024 * 1024)
    over = size_mb >= limit_mb
    oldest: list[dict[str, Any]] = []
    if over:
        conn = _connect(db_path)
        try:
            rows = conn.execute(
                "SELECT id, profile_name, title, started_at, archived, pinned"
                " FROM sessions WHERE archived = 0 ORDER BY started_at LIMIT 20"
            ).fetchall()
            oldest = [{"session_id": r["id"], "title": r["title"],
                       "started_at": r["started_at"], "pinned": r["pinned"]} for r in rows]
        finally:
            conn.close()
    return {
        "db_size_mb": round(size_mb, 1),
        "limit_mb": limit_mb,
        "target_mb": round(target_mb, 1),
        "over": over,
        "oldest_sessions": oldest,
    }


def status(db_path: str) -> dict[str, Any]:
    """治理状态总览：表存在性 / WAL / 计数。"""
    conn = _connect(db_path)
    try:
        tables = [r[0] for r in conn.execute(
            "SELECT name FROM sqlite_master WHERE type IN ('table','virtual table')"
            " AND name LIKE 'governance_%' ORDER BY name").fetchall()]
        wal = conn.execute("PRAGMA journal_mode").fetchone()[0]
        counts: dict[str, int] = {}
        for t in ("governance_meta", "governance_log", "governance_archive_index",
                  "governance_cluster"):
            if t in tables:
                counts[t] = conn.execute(f"SELECT COUNT(*) FROM {t}").fetchone()[0]
        cold = conn.execute(
            "SELECT COUNT(*) FROM governance_meta WHERE state = 'cold_archived'"
        ).fetchone()[0] if "governance_meta" in tables else 0
        return {
            "tables": tables,
            "journal_mode": wal,
            "row_counts": counts,
            "cold_archived": cold,
            "db_size_mb": round(os.path.getsize(db_path) / (1024 * 1024), 1),
        }
    finally:
        conn.close()


# ─────────────────────────── CLI ───────────────────────────

def main(argv: Optional[list[str]] = None) -> int:
    parser = argparse.ArgumentParser(description="会话语义治理 MVP-1（M1-M2）")
    sub = parser.add_subparsers(dest="cmd", required=True)

    p = sub.add_parser("ensure-schema", help="建治理四表 + FTS + WAL（幂等）")
    p.add_argument("--db", required=True, help="state.db 路径")

    p = sub.add_parser("archive", help="冷归档原子流程（先写 .gz+manifest+sha256，后删热行）")
    p.add_argument("--db", required=True)
    p.add_argument("--session", required=True)
    p.add_argument("--profile", default=None)
    p.add_argument("--archive-root", default=None)
    p.add_argument("--reason", default="threshold")

    p = sub.add_parser("restore", help="原位恢复（校验 sha256 → 解压 → 重建 messages）")
    p.add_argument("--db", required=True)
    p.add_argument("--session", required=True)
    p.add_argument("--profile", default=None)
    p.add_argument("--archive-root", default=None)

    p = sub.add_parser("verify", help="校验归档包完整性（sha256 双层）")
    p.add_argument("--db", required=True)
    p.add_argument("--session", required=True)
    p.add_argument("--profile", default=None)
    p.add_argument("--archive-root", default=None)

    p = sub.add_parser("threshold", help="L0 阈值检测（只读; 阈值按 profile: default=80MB, 其他=60MB）")
    p.add_argument("--db", required=True)
    p.add_argument("--profile", default="default",
                   help="profile 名 (决定阈值: default=80MB, 其他=60MB; 可被 --limit-mb 覆盖)")
    p.add_argument("--limit-mb", type=float, default=None, help="显式覆盖阈值 (MB)")

    p = sub.add_parser("status", help="治理状态总览")
    p.add_argument("--db", required=True)

    args = parser.parse_args(argv)

    try:
        if args.cmd == "ensure-schema":
            tables = ensure_schema(args.db)
            print(f"OK ensure-schema: {len(tables)} 表就绪 → {tables}")
        elif args.cmd == "archive":
            r = archive_session(args.db, args.session, args.archive_root,
                                args.profile, args.reason)
            print(f"OK archive {args.session}: {r['message_count']} 条消息 → {r['archive_file']}")
            print(f"   sha256={r['sha256']} 释放≈{r['storage_saved_bytes']}B")
        elif args.cmd == "restore":
            r = restore_session(args.db, args.session, args.archive_root, args.profile)
            print(f"OK restore {args.session}: 恢复 {r['restored_messages']} 条消息")
        elif args.cmd == "verify":
            r = verify_archive(args.db, args.session, args.archive_root, args.profile)
            print(f"OK verify {args.session}: sha256={r['file_sha256'][:16]}.. "
                  f"msgs={r['message_count']} size={r['size_bytes']}B")
        elif args.cmd == "threshold":
            r = threshold_check(args.db, args.limit_mb, args.profile)
            print(f"state.db = {r['db_size_mb']}MB / 阈值 {r['limit_mb']:g}MB "
                  f"(profile={args.profile}, 保留目标 {r['target_mb']:g}MB = 阈值×0.85) "
                  f"{'⚠ 超阈值' if r['over'] else '✅ 正常'}")
            if r["over"]:
                print(f"最旧会话候选（{len(r['oldest_sessions'])} 个）:")
                for s in r["oldest_sessions"]:
                    print(f"  {s['session_id']}  {s['title'] or ''}  "
                          f"pinned={s['pinned']}")
        elif args.cmd == "status":
            s = status(args.db)
            print(f"journal_mode={s['journal_mode']}  db={s['db_size_mb']}MB")
            print(f"tables={s['tables']}")
            print(f"rows={s['row_counts']}  cold_archived={s['cold_archived']}")
        return 0
    except Exception as exc:  # noqa: BLE001
        print(f"ERROR: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
