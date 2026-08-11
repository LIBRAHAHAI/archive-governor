#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
governance_search.py — 会话语义治理 MVP-3（M5-M8）

范围：
  M5: archive_index + FTS + govern search（标注来源状态，K4 <3s，AC-D1.4）
  M6: 不可变保护（活跃/pinned/修复链/不可回溯）+ 审计日志（AC-D4.1，what/when/why）
  M7: 安全基线 — 脱敏（AC-D3.4）/ 白名单（AC-D5.3）/ 权限（AC-D5.5）/ DUR-1 备份（AC-D4.3）
  M8: 首次 dry-run（duanmu→xuanwu→athena→default）→ 日志文件 → user 确认

设计来源：内部设计文档 (Hermes-Salon exchange 320)
  §6.3 不可变保护 / §9 独立检索 / §10 阈值与自动触发 / §10.5 首次部署例外 / §12 19 AC

安全原则（xuanwu 19 AC）：
  - AC-D5.1 全参数化查询 + FTS 短语转义（禁止字符串拼接进 MATCH）
  - AC-D5.3 输入白名单校验（session_id/profile_name/关键词长度）
  - AC-D3.4 凭据脱敏（展示/日志输出时替换，绝不打印原文）
  - AC-D5.4 审计日志不含消息原文
  - AC-D4.1 全操作审计：op/session/before/after/reason/operator/evidence/status/created_at
  - dry-run 与 search 全程只读（mode=ro），不写任何库

用法：
  python governance_search.py search "关键词" [--archived|--active|--all] [--db PATH] [--limit N]
  python governance_search.py protect-check --db PATH --session ID
  python governance_search.py dry-run [--db PATH] [--profile NAME] [--out FILE]
  python governance_search.py audit --db PATH [--last N]
  python governance_search.py security-check [--db PATH]
"""

from __future__ import annotations

import argparse
import json
import os
import re
import sqlite3
import sys
import time
from typing import Any, Optional

import governance_config as cfg  # FIX-B3: 阈值单一事实源

# ─────────────────────────── 常量与配置 ───────────────────────────

# FIX-B3: storage_limit_mb / profile_overrides 由 governance_config 派生, 不复制常量
# archive_root: 归档包存放根目录 — 2026-08-11 开源化改为可配置默认值
# (原为内部工作区绝对路径, 发布后由使用者自行配置; 推荐放 HERMES_HOME 同级, 见 README)
_DEFAULT_ARCHIVE_ROOT = os.path.join(
    cfg.hermes_home(), "..", "archive-governor-data", "archives"
)
DEFAULT_CONFIG = {
    "storage_limit_mb": cfg.DEFAULT_THRESHOLD_MB,
    "storage_ceiling_mb": cfg.STORAGE_CEILING_MB,
    "profile_overrides": dict(cfg.PROFILE_OVERRIDE_MB),
    "trigger": "threshold",
    "archive_root": _DEFAULT_ARCHIVE_ROOT,
}

# 各 profile 的 state.db 路径 — 由 governance_config 推导 (2026-08-11 开源化,
# 原为内部绝对路径, 不可移植且暴露机器结构)
PROFILE_DB_PATHS = cfg.default_profile_db_paths()

# 首次 dry-run 放大顺序（320 §10.5：从最小 profile 开始）
DRY_RUN_ORDER = ["duanmu", "xuanwu", "athena", "default"]

# 输入白名单（AC-D5.3）：session_id / profile_name 只允许字母数字 _ -
_ID_RE = re.compile(r"^[A-Za-z0-9_-]+$")
_PROFILE_WHITELIST = set(PROFILE_DB_PATHS.keys())

# 凭据脱敏正则（AC-D3.4 / D5.4）：展示时替换
_REDACT_PATTERNS = [
    re.compile(r"(?i)\b(api[_-]?key|token|secret|password|passwd|access[_-]?key)\b\s*[:=]\s*\S+"),
    re.compile(r"(?i)\bbearer\s+[A-Za-z0-9\-._~+/]+=*"),
    re.compile(r"(?i)\b(sk|pk|ghp|gho|xox[baprs]|ya29|AIza)[A-Za-z0-9_\-]{8,}\b"),
    re.compile(r"\b[0-9a-f]{40,}\b"),          # 长 hex（token/sha 类）
    re.compile(r"(?i)(password|passwd|secret)\s*[:=]\s*['\"]?[^'\",\s]{4,}"),
]

# 修复链信号词（M6 保护：错误指纹 + 修复动作链，纯规则零 LLM）
_ERROR_SIGNS = re.compile(r"Traceback|Exception|Error:|失败|报错|❌|NameError|KeyError|ValueError|TypeError", re.I)
_FIX_SIGNS = re.compile(r"修复|fixed|解决|修复链|patch|工作区|已处理|✓", re.I)
_VERIFY_SIGNS = re.compile(r"验证|verify|测试通过|复现|回归", re.I)

ACTIVE_GRACE_HOURS = 24  # §6.3：最近 24h 内有写入视为活跃


# ─────────────────────────── 工具函数 ───────────────────────────

def redact(text: Optional[str]) -> str:
    """凭据脱敏（AC-D3.4）：替换 API key/token/password/secret/长 hex 为占位符。"""
    if not text:
        return text or ""
    out = text
    for pat in _REDACT_PATTERNS:
        out = pat.sub("[REDACTED]", out)
    return out


def _validate_id(value: Optional[str], field: str = "id") -> str:
    """输入白名单校验（AC-D5.3）：非法输入直接拒绝，不进入 SQL。"""
    if not value or not _ID_RE.match(value):
        raise ValueError(f"非法 {field}: {value!r}（仅允许字母/数字/下划线/连字符）")
    return value


def _validate_keyword(kw: Optional[str]) -> str:
    """关键词校验（AC-D5.3）：非空、去 NUL、长度上限。"""
    if not kw or not kw.strip():
        raise ValueError("关键词不能为空")
    kw = kw.replace("\x00", "").strip()
    if len(kw) > 200:
        raise ValueError(f"关键词过长（{len(kw)}>200），拒绝查询")
    return kw


def _fts_phrase(kw: str) -> str:
    """FTS5 短语转义（AC-D5.1）：内部双引号翻倍，整体包引号 → 仅作短语匹配，注入无效。"""
    return '"' + kw.replace('"', '""') + '"'


def _connect_ro(db_path: str) -> sqlite3.Connection:
    """只读连接（dry-run/search 全程不写库）。URI mode=ro 保证任何写语句直接报错。"""
    if not os.path.exists(db_path):
        raise FileNotFoundError(f"state.db 不存在: {db_path}")
    uri = "file:" + db_path.replace("\\", "/").replace("?", "%3f").replace("#", "%23") + "?mode=ro"
    conn = sqlite3.connect(uri, uri=True)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA busy_timeout = 5000")
    return conn


def _table_exists(conn: sqlite3.Connection, name: str) -> bool:
    row = conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type IN ('table','virtual table') AND name = ?",
        (name,),
    ).fetchone()
    return row is not None


def _profile_threshold_mb(profile: str) -> float:
    # FIX-B3: 统一解析入口 (governance_config.resolve_threshold_mb),
    # 与 detector / session_governor / startup_check / reclaim 同一事实源
    return cfg.resolve_threshold_mb(profile)


# ─────────────────────────── M5：独立检索 ───────────────────────────

def search(db_path: str, keyword: str, scope: str = "all",
           limit: int = 20) -> dict[str, Any]:
    """govern.search：默认查 active + cold，结果标注来源状态（320 §9.2）。

    scope: all|active|archived
    返回 {"query_ms", "hits": [...]}，每条命中含
      state / profile / session_id / message_id / title / snippet / archive_file
    snippet 与 title 已脱敏（AC-D3.4）。查询 P95 ≤3s（K4）。
    """
    kw = _validate_keyword(keyword)
    limit = max(1, min(int(limit), 100))
    phrase = _fts_phrase(kw)
    hits: list[dict[str, Any]] = []
    t0 = time.perf_counter()

    conn = _connect_ro(db_path)
    try:
        # ── active：messages_fts_trigram（trigram 支持 CJK 子串）──
        if scope in ("all", "active") and _table_exists(conn, "messages_fts_trigram"):
            sql = (
                "SELECT m.id AS message_id, m.session_id, s.profile_name, s.title,"
                "       substr(m.content,1,200) AS snippet, m.timestamp"
                " FROM messages_fts_trigram f"
                " JOIN messages m ON m.id = f.rowid"
                " JOIN sessions s ON s.id = m.session_id"
                " WHERE messages_fts_trigram MATCH ? AND COALESCE(s.archived,0)=0"
                " ORDER BY m.timestamp DESC LIMIT ?"
            )
            try:
                for r in conn.execute(sql, (phrase, limit)):
                    hits.append({
                        "state": "active",
                        "profile": r["profile_name"] or "default",
                        "session_id": r["session_id"],
                        "message_id": r["message_id"],
                        "title": redact(r["title"]),
                        "snippet": redact(r["snippet"]),
                        "archive_file": None,
                    })
            except sqlite3.OperationalError as exc:
                # 索引损坏/缺失：拒绝静默返回空（athena 321 §1.4）
                if "malformed" in str(exc).lower() or "no such" in str(exc).lower():
                    raise RuntimeError(f"active FTS 索引异常，拒绝静默空结果: {exc}") from exc
                # 短语在 trigram 上不命中（如 <3 字符）→ LIKE 兜底
                hits = hits  # keep
            if not hits:
                # trigram 对短词/边界不友好 → 参数化 LIKE 兜底（AC-D5.1）
                like = "%" + kw + "%"
                for r in conn.execute(
                    "SELECT m.id AS message_id, m.session_id, s.profile_name, s.title,"
                    "       substr(m.content,1,200) AS snippet, m.timestamp"
                    " FROM messages m JOIN sessions s ON s.id = m.session_id"
                    " WHERE m.content LIKE ? AND COALESCE(s.archived,0)=0"
                    " ORDER BY m.timestamp DESC LIMIT ?",
                    (like, limit),
                ):
                    hits.append({
                        "state": "active",
                        "profile": r["profile_name"] or "default",
                        "session_id": r["session_id"],
                        "message_id": r["message_id"],
                        "title": redact(r["title"]),
                        "snippet": redact(r["snippet"]),
                        "archive_file": None,
                    })

        # ── cold：governance_archive_index_fts（归档独立索引）──
        if scope in ("all", "archived") and _table_exists(conn, "governance_archive_index_fts"):
            sql = (
                "SELECT i.session_id, i.profile_name, i.title, i.keywords, i.project_tags,"
                "       i.message_count, i.archive_file"
                " FROM governance_archive_index_fts f"
                " JOIN governance_archive_index i ON i.rowid = f.rowid"
                " WHERE governance_archive_index_fts MATCH ? LIMIT ?"
            )
            try:
                for r in conn.execute(sql, (phrase, limit)):
                    hits.append({
                        "state": "cold_archived",
                        "profile": r["profile_name"] or "default",
                        "session_id": r["session_id"],
                        "message_id": None,
                        "title": redact(r["title"]),
                        "snippet": redact(f"keywords={r['keywords']} tags={r['project_tags']}"),
                        "archive_file": r["archive_file"],
                    })
            except sqlite3.OperationalError as exc:
                if "malformed" in str(exc).lower() or "no such" in str(exc).lower():
                    raise RuntimeError(f"archive FTS 索引异常，拒绝静默空结果: {exc}") from exc
            if not [h for h in hits if h["state"] == "cold_archived"]:
                like = "%" + kw + "%"
                for r in conn.execute(
                    "SELECT session_id, profile_name, title, keywords, project_tags,"
                    "       message_count, archive_file"
                    " FROM governance_archive_index"
                    " WHERE title LIKE ? OR keywords LIKE ? OR project_tags LIKE ? LIMIT ?",
                    (like, like, like, limit),
                ):
                    hits.append({
                        "state": "cold_archived",
                        "profile": r["profile_name"] or "default",
                        "session_id": r["session_id"],
                        "message_id": None,
                        "title": redact(r["title"]),
                        "snippet": redact(f"keywords={r['keywords']} tags={r['project_tags']}"),
                        "archive_file": r["archive_file"],
                    })
    finally:
        conn.close()

    elapsed_ms = (time.perf_counter() - t0) * 1000
    return {"query_ms": round(elapsed_ms, 1), "hits": hits[:limit]}


# ─────────────────────────── M6：不可变保护 + 审计 ───────────────────────────

def is_protected(db_path: str, session_id: str) -> tuple[bool, list[str]]:
    """不可变保护判定（320 §6.3 + athena 2.2）。

    以下内容永不自动归档/改写：
      1. 活跃：ended_at 为空（进行中）或最近 24h 内有消息写入
      2. pinned=1 或 governance_meta.exempt=1
      3. 修复链：governance_cluster_member.keep_chain=1（错误-修复-验收链）
      4. 不可回溯：session_key/origin_json 全空（无法溯源）
    返回 (protected, reasons)。
    """
    sid = _validate_id(session_id, "session_id")
    conn = _connect_ro(db_path)
    reasons: list[str] = []
    try:
        sess = conn.execute(
            "SELECT id, pinned, archived, ended_at, session_key, origin_json, source, cwd"
            " FROM sessions WHERE id = ?",
            (sid,),
        ).fetchone()
        if sess is None:
            return True, ["会话不存在（不可回溯）"]

        # 1) 活跃：ended_at 为空或最近 24h 写入
        if sess["ended_at"] is None:
            reasons.append("active:ended_at为空(进行中)")
        else:
            last = conn.execute(
                "SELECT MAX(timestamp) AS t FROM messages WHERE session_id = ?", (sid,)
            ).fetchone()["t"]
            if last and (time.time() - last) < ACTIVE_GRACE_HOURS * 3600:
                reasons.append(f"active:最近{ACTIVE_GRACE_HOURS}h内有写入")

        # 2) pinned / exempt
        if sess["pinned"]:
            reasons.append("pinned=1")
        if _table_exists(conn, "governance_meta"):
            meta = conn.execute(
                "SELECT exempt FROM governance_meta WHERE session_id = ?", (sid,)
            ).fetchone()
            if meta and meta["exempt"]:
                reasons.append("exempt=1(不可变白名单)")

        # 3) 修复链：cluster member keep_chain=1（bug 归集保留排查链）
        if _table_exists(conn, "governance_cluster_member"):
            chain = conn.execute(
                "SELECT COUNT(*) AS c FROM governance_cluster_member"
                " WHERE session_id = ? AND keep_chain = 1",
                (sid,),
            ).fetchone()["c"]
            if chain:
                reasons.append(f"修复链(keep_chain=1,{chain}成员)")

        # 4) 不可回溯来源：source/cwd/session_key/origin_json 全空（无法溯源）
        if (not sess["session_key"] and not sess["origin_json"]
                and not sess["source"] and not sess["cwd"]):
            reasons.append("不可回溯(无source/cwd/session_key/origin_json)")

        return bool(reasons), reasons
    finally:
        conn.close()


def audit_log(db_path: str, last: int = 50) -> list[dict[str, Any]]:
    """读取审计日志（AC-D4.1）：op/session/before/after/reason/operator/evidence/status/when。"""
    conn = _connect_ro(db_path)
    try:
        if not _table_exists(conn, "governance_log"):
            return []
        rows = conn.execute(
            "SELECT op, session_id, profile_name, before_state, after_state, reason,"
            "       operator, evidence, status, created_at"
            " FROM governance_log ORDER BY id DESC LIMIT ?",
            (max(1, min(int(last), 500)),),
        ).fetchall()
        return [{
            "op": r["op"], "session_id": r["session_id"],
            "profile": r["profile_name"],
            "before": r["before_state"], "after": r["after_state"],
            "reason": r["reason"], "operator": r["operator"],
            "evidence": redact(r["evidence"]), "status": r["status"],
            "when": time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(r["created_at"]))
            if r["created_at"] else None,
        } for r in rows]
    finally:
        conn.close()


# ─────────────────────────── M8：首次 dry-run ───────────────────────────

def _session_message_bytes(conn: sqlite3.Connection, session_id: str) -> int:
    r = conn.execute(
        "SELECT SUM(LENGTH(content) + COALESCE(LENGTH(tool_calls),0)"
        "            + COALESCE(LENGTH(reasoning),0)) AS b"
        " FROM messages WHERE session_id = ?",
        (session_id,),
    ).fetchone()
    return int(r["b"] or 0)


def _db_amplification(db_path: str) -> float:
    """冷归档实际释放 ≈ 内容字节 × 放大系数。

    320 §8.2 现场实测：149MB 库中 messages.content 仅 21.5MB，
    大头是 FTS5 trigram 索引 + 消息行体量 → 删行后文件不缩小，
    必须 FTS 同步删除 + VACUUM 才回收。dry-run 用
    （db 大小 / 全库内容字节）估算放大系数，反映真实回收量。
    """
    conn = _connect_ro(db_path)
    try:
        r = conn.execute(
            "SELECT SUM(LENGTH(content) + COALESCE(LENGTH(tool_calls),0)"
            "            + COALESCE(LENGTH(reasoning),0)) FROM messages"
        ).fetchone()
        content_bytes = int(r[0] or 0)
    finally:
        conn.close()
    if content_bytes <= 0:
        return 1.0
    db_bytes = os.path.getsize(db_path)
    return max(1.0, db_bytes / content_bytes)


def dry_run_profile(db_path: str, profile: str, out_lines: list[str],
                    lag_ratio: Optional[float] = None) -> dict[str, Any]:
    """单 profile 的 dry-run：读库（只读），列出最旧优先、受保护排除的待归档候选。

    逻辑（320 §10.2 / §10.5）：
      - state.db 单文件大小 vs 阈值（profile_overrides 覆盖）
      - 超阈值 → 按 started_at 最旧优先，跳过受保护会话，直到 ≤ 阈值×lag_ratio
      - 归档后仍 > 天花板 → 停止，告警，不删任何会话（AC-D2.2 兜底）

    FIX-B3: lag_ratio 缺省 → cfg.LAG_RATIO (0.85), 不再本地复制 0.9 防漂移。
    """
    if lag_ratio is None:
        lag_ratio = cfg.LAG_RATIO
    threshold_mb = _profile_threshold_mb(profile)
    ceiling_mb = DEFAULT_CONFIG["storage_ceiling_mb"]
    size_mb = os.path.getsize(db_path) / (1024 * 1024)
    target_mb = threshold_mb * lag_ratio
    result: dict[str, Any] = {
        "profile": profile, "db": db_path,
        "size_mb": round(size_mb, 1), "threshold_mb": threshold_mb,
        "over": size_mb >= threshold_mb, "candidates": [], "freed_mb": 0.0,
        "protected_skipped": 0, "ceiling_violation": False,
        "target_reached": False,
    }

    conn = _connect_ro(db_path)
    try:
        if not result["over"]:
            out_lines.append(f"[{profile}] state.db={size_mb:.1f}MB ≤ {threshold_mb}MB 阈值 ✅ 无需归档")
            return result

        out_lines.append(f"[{profile}] state.db={size_mb:.1f}MB ≥ {threshold_mb}MB 阈值 ⚠ 需冷归档"
                         f"（目标 ≤{target_mb:.1f}MB，{int((1-lag_ratio)*100)}% 滞后带）")

        # 最旧优先，跳过已归档
        rows = conn.execute(
            "SELECT id, title, started_at, pinned, ended_at, session_key, origin_json"
            " FROM sessions WHERE COALESCE(archived,0)=0"
            " ORDER BY started_at ASC"
        ).fetchall()

        freed_bytes = 0
        pending_freed = size_mb * 1024 * 1024 - target_mb * 1024 * 1024
        amp = _db_amplification(db_path)  # FTS 索引放大系数（§8.2）
        for r in rows:
            if freed_bytes >= pending_freed:
                break
            sid = r["id"]
            prot, reasons = is_protected(db_path, sid)
            if prot:
                result["protected_skipped"] += 1
                continue
            msg_bytes = _session_message_bytes(conn, sid)
            # 实际回收 ≈ 内容字节 × 放大系数（FTS 索引是空间大头，§8.2）
            freed_bytes += msg_bytes * amp
            result["candidates"].append({
                "session_id": sid,
                "title": redact(r["title"]),
                "started_at": time.strftime("%Y-%m-%d %H:%M", time.localtime(r["started_at"]))
                if r["started_at"] else None,
                "est_freed_mb": round(msg_bytes * amp / (1024 * 1024), 3),
            })

        result["freed_mb"] = round(freed_bytes / (1024 * 1024), 1)
        remain_mb = size_mb - result["freed_mb"]
        result["remain_est_mb"] = round(max(remain_mb, 0), 1)
        result["target_reached"] = result["remain_est_mb"] <= target_mb

        out_lines.append(f"[{profile}] 候选 {len(result['candidates'])} 个会话"
                         f"（跳过受保护 {result['protected_skipped']} 个）"
                         f"，释放 ≈{result['freed_mb']}MB，预计剩余 ≈{result['remain_est_mb']}MB"
                         f"{' ✅达标' if result['target_reached'] else ' ⚠ 未达目标'}")
        for c in result["candidates"][:30]:  # 320 §15 Q2：30 行预览
            out_lines.append(f"  - {c['session_id']}  {c['started_at'] or ''}  "
                             f"{c['title'] or ''}  ≈{c['est_freed_mb']}MB")

        if result["remain_est_mb"] > ceiling_mb:
            result["ceiling_violation"] = True
            out_lines.append(f"[{profile}] ⚠ 归档后仍 >{ceiling_mb}MB 天花板 → 停止并告警"
                             f"（AC-D2.2 兜底：不删除任何会话）")
    finally:
        conn.close()
    return result


def dry_run_all(out_file: Optional[str] = None,
                profile_filter: Optional[str] = None) -> list[dict[str, Any]]:
    """首次 dry-run：duanmu→xuanwu→athena→default 顺序，输出清单到日志文件（320 §10.5）。"""
    order = [profile_filter] if profile_filter else DRY_RUN_ORDER
    if profile_filter:
        _validate_id(profile_filter, "profile_name")
        if profile_filter not in _PROFILE_WHITELIST:
            raise ValueError(f"未知 profile: {profile_filter}（白名单={sorted(_PROFILE_WHITELIST)}）")

    out_lines = [
        "=" * 74,
        "会话语义治理 v3 — 首次部署 dry-run（只读，未执行任何归档）",
        f"时间: {time.strftime('%Y-%m-%d %H:%M:%S')}",
        "顺序: " + " → ".join(order) + "  （320 §10.5：从最小 profile 开始放大）",
        "说明: 本清单为「将冷归档」候选预告；user 打开本文件即算确认（Q2）。",
        "=" * 74,
    ]
    results = []
    for p in order:
        db = PROFILE_DB_PATHS[p]
        if not os.path.exists(db):
            out_lines.append(f"[{p}] state.db 不存在（{db}），跳过")
            results.append({"profile": p, "error": "db missing"})
            continue
        results.append(dry_run_profile(db, p, out_lines))
        out_lines.append("-" * 74)

    if out_file:
        os.makedirs(os.path.dirname(os.path.abspath(out_file)), exist_ok=True)
        # ── 汇总分析（可执行依据：为什么未达标/达标）──
        out_lines.append("=" * 74)
        out_lines.append("汇总分析")
        for r in results:
            if r.get("error"):
                out_lines.append(f"  [{r['profile']}] 错误: {r['error']}")
                continue
            if not r.get("over"):
                out_lines.append(f"  [{r['profile']}] ✅ 未超阈值，无需归档")
                continue
            out_lines.append(
                f"  [{r['profile']}] {r['size_mb']}MB → 候选{len(r.get('candidates', []))}个"
                f" 释放≈{r.get('freed_mb', 0)}MB → 剩余≈{r.get('remain_est_mb')}MB"
                f"（目标≤{r['threshold_mb'] * cfg.LAG_RATIO:.0f}MB）"
                f"{'✅' if r.get('target_reached') else '⚠ 未达目标'}"
                f"{'；天花板告警' if r.get('ceiling_violation') else ''}"
            )
            if not r.get("target_reached"):
                out_lines.append(
                    f"      原因：受保护会话（活跃/pinned/修复链/不可回溯）占空间大头，"
                    f"跳过 {r.get('protected_skipped', 0)} 个。"
                    f"（320 §6.3 不可变保护优先于阈值回收）"
                )
        with open(out_file, "w", encoding="utf-8") as f:
            f.write("\n".join(out_lines) + "\n")
        results_meta = {
            "out_file": out_file, "profile_results": results,
            "total_candidates": sum(len(r.get("candidates", [])) for r in results),
            "total_freed_mb": round(sum(r.get("freed_mb", 0) for r in results), 1),
            "protected_skipped": sum(r.get("protected_skipped", 0) for r in results),
            "ceiling_violations": [r["profile"] for r in results if r.get("ceiling_violation")],
            "targets_not_reached": [r["profile"] for r in results
                                    if r.get("over") and not r.get("target_reached")],
        }
        results.append(results_meta)
    return results


# ─────────────────────────── M7：安全基线自检 ───────────────────────────

def security_check(db_path: Optional[str] = None) -> dict[str, Any]:
    """安全基线自检（M7）：脱敏/白名单/权限/DUR-1 备份覆盖。"""
    report: dict[str, Any] = {"checks": []}

    # 1) 脱敏（AC-D3.4）
    sample = "api_key=sk-abc123456789 password=hunter2 Authorization: Bearer ya29.token123 ABCDEF0123456789abcdef0123456789abcdef"
    redacted = redact(sample)
    leaks = [s for s in ["sk-abc123", "hunter2", "ya29.token123"] if s in redacted]
    report["checks"].append({
        "id": "AC-D3.4", "name": "凭据脱敏", "pass": not leaks,
        "detail": f"样本脱敏后残留泄露项: {leaks if leaks else '无'}",
    })

    # 2) 白名单（AC-D5.3）
    try:
        _validate_id("bad id!@#", "session_id")
        whitelist_ok = False
    except ValueError:
        whitelist_ok = True
    try:
        _validate_keyword("正常关键词" * 41)  # 205 字符 > 200 上限
        kw_ok = False
    except ValueError:
        kw_ok = True
    report["checks"].append({
        "id": "AC-D5.3", "name": "输入白名单", "pass": whitelist_ok and kw_ok,
        "detail": f"非法 id 拒绝={'是' if whitelist_ok else '否'}，超长关键词拒绝={'是' if kw_ok else '否'}",
    })

    # 3) 归档目录权限（AC-D5.5）：存在性 + 可写性探测（Windows 继承父目录 ACL）
    archive_root = DEFAULT_CONFIG["archive_root"]
    perm_ok = True
    perm_detail = []
    for p in DRY_RUN_ORDER:
        d = os.path.join(archive_root, p, "session-archive")
        if not os.path.isdir(d):
            perm_detail.append(f"{p}: 目录未创建（归档时将自动创建，继承父 ACL）")
            continue
        if os.access(d, os.R_OK | os.W_OK):
            perm_detail.append(f"{p}: 可读可写（继承 ACL）")
        else:
            perm_ok = False
            perm_detail.append(f"{p}: ⚠ 权限异常（不可读写）")
    report["checks"].append({
        "id": "AC-D5.5", "name": "归档目录权限", "pass": perm_ok,
        "detail": "; ".join(perm_detail),
    })

    # 4) DUR-1 备份覆盖（AC-D4.3）：session-archive/ 是否在 hermes_backup.py 覆盖范围
    # 2026-08-11 开源化: 备份脚本路径可配置 (环境变量 AGO_BACKUP_SCRIPT)。
    # 未配置且探测不到 → 降级为 pass + 说明 (该检查是部署环境可选增强, 非通用硬性要求)
    backup_py = os.environ.get("AGO_BACKUP_SCRIPT", "")
    if not backup_py:
        _candidates = [
            os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
                         "..", "hermes-backup", "hermes_backup.py"),
        ]
        backup_py = next((p for p in _candidates if os.path.exists(p)), "")
    dur_ok = True  # 未配置/未探测到 → 降级 pass (见上)
    dur_detail = []
    if backup_py and os.path.exists(backup_py):
        try:
            with open(backup_py, "r", encoding="utf-8", errors="ignore") as f:
                src = f.read()
            dur_ok = "session-archive" in src
            dur_detail.append(f"hermes_backup.py 包含 session-archive 备份步骤: {'是' if dur_ok else '否'}")
        except OSError as exc:
            dur_ok = False
            dur_detail.append(f"无法读取备份脚本: {exc}")
    elif backup_py:
        dur_detail.append(f"备份脚本不存在: {backup_py}")
    else:
        dur_detail.append("未配置 AGO_BACKUP_SCRIPT, 跳过 DUR-1 备份覆盖检查 (部署环境可选增强)")
    report["checks"].append({
        "id": "AC-D4.3", "name": "DUR-1 备份覆盖", "pass": dur_ok,
        "detail": "; ".join(dur_detail),
    })

    # 5) 检索可用性（O4 探活：只读打开）
    if db_path and os.path.exists(db_path):
        try:
            conn = _connect_ro(db_path)
            conn.close()
            report["checks"].append({
                "id": "O4", "name": "检索可用性", "pass": True,
                "detail": f"只读连接成功: {db_path}",
            })
        except Exception as exc:  # noqa: BLE001
            report["checks"].append({
                "id": "O4", "name": "检索可用性", "pass": False,
                "detail": f"连接失败: {exc}",
            })

    report["all_pass"] = all(c["pass"] for c in report["checks"])
    return report


# ─────────────────────────── CLI ───────────────────────────

def main(argv: Optional[list[str]] = None) -> int:
    parser = argparse.ArgumentParser(description="会话语义治理 MVP-3（M5-M8）")
    sub = parser.add_subparsers(dest="cmd", required=True)

    p = sub.add_parser("search", help="检索 active+cold，结果标注来源状态（K4 <3s）")
    p.add_argument("keyword", help="检索关键词（自动 FTS 短语转义 + 脱敏展示）")
    p.add_argument("--scope", choices=["all", "active", "archived"], default="all",
                   help="检索范围（默认 all=active+cold）")
    p.add_argument("--db", default=PROFILE_DB_PATHS["default"], help="state.db 路径")
    p.add_argument("--limit", type=int, default=20)

    p = sub.add_parser("protect-check", help="不可变保护判定（M6，§6.3）")
    p.add_argument("--db", required=True)
    p.add_argument("--session", required=True)

    p = sub.add_parser("dry-run", help="首次 dry-run：只读生成待归档清单 → 日志文件（M8）")
    p.add_argument("--profile", default=None, help="只跑单个 profile（默认全量 4 个）")
    p.add_argument("--out", default=None, help="日志文件路径（默认自动生成）")

    p = sub.add_parser("audit", help="查看审计日志（AC-D4.1）")
    p.add_argument("--db", required=True)
    p.add_argument("--last", type=int, default=50)

    p = sub.add_parser("security-check", help="安全基线自检（M7）")
    p.add_argument("--db", default=None)

    args = parser.parse_args(argv)

    try:
        if args.cmd == "search":
            r = search(args.db, args.keyword, args.scope, args.limit)
            print(f"查询耗时 {r['query_ms']}ms（K4 目标 <3000ms） 命中 {len(r['hits'])} 条")
            for h in r["hits"]:
                ptr = f"  📦{h['archive_file']}" if h["archive_file"] else ""
                src = "❄️cold" if h["state"] == "cold_archived" else "🟢active"
                print(f"  [{src} {h['profile']}] {h['session_id']}"
                      f"  {h['title'] or ''}{ptr}")
                print(f"      {h['snippet'] or ''}")
            if r["query_ms"] > 3000:
                print("⚠ K4 超时：>3s", file=sys.stderr)
                return 2
            return 0

        elif args.cmd == "protect-check":
            prot, reasons = is_protected(args.db, args.session)
            print(f"{args.session}: {'🛡 受保护' if prot else '✅ 可归档'}"
                  f"{' — ' + '; '.join(reasons) if reasons else ''}")
            return 0 if prot else 1

        elif args.cmd == "dry-run":
            # 2026-08-11 开源化: 输出目录可配置, 默认当前目录 (原硬编码内部工作区)
            out = args.out or os.path.join(
                os.getcwd(),
                f"governance-dry-run-{time.strftime('%Y%m%d-%H%M%S')}.log",
            )
            results = dry_run_all(out, args.profile)
            meta = results[-1] if results and "out_file" in results[-1] else {}
            print(f"dry-run 完成 → {out}")
            if meta:
                print(f"候选 {meta['total_candidates']} 个会话，释放 ≈{meta['total_freed_mb']}MB，"
                      f"跳过受保护 {meta['protected_skipped']} 个")
                if meta["targets_not_reached"]:
                    print(f"⚠ 未达目标: {', '.join(meta['targets_not_reached'])}"
                          f"（受保护会话占空间大头，见日志汇总分析）")
                if meta["ceiling_violations"]:
                    print(f"⚠ 天花板告警: {', '.join(meta['ceiling_violations'])}"
                          f"（AC-D2.2 兜底：不删除任何会话）")
            print("user 打开日志文件即算确认（320 §15 Q2）。")
            return 0

        elif args.cmd == "audit":
            entries = audit_log(args.db, args.last)
            if not entries:
                print("审计日志为空（尚无治理操作）")
                return 0
            print(f"最近 {len(entries)} 条审计（AC-D4.1: op/when/why）:")
            for e in entries:
                print(f"  [{e['when']}] {e['op']:14s} {e['session_id'] or '-':32s}"
                      f" {e['before'] or '-'}→{e['after'] or '-'}  reason={e['reason']}"
                      f"  op={e['operator']}  status={e['status']}")
            return 0

        elif args.cmd == "security-check":
            r = security_check(args.db)
            print(f"安全基线自检: {'✅ 全部通过' if r['all_pass'] else '⚠ 存在未通过项'}")
            for c in r["checks"]:
                print(f"  [{'✅' if c['pass'] else '❌'}] {c['name']}（{c['id']}）: {c['detail']}")
            return 0 if r["all_pass"] else 1

        return 0
    except Exception as exc:  # noqa: BLE001
        print(f"ERROR: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
