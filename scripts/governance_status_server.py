#!/usr/bin/env python3
"""governance_status_server.py — 治理状态只读快照 HTTP 端点 (监控探针).

2026-08-10 用户拍板 A/A/A (kanban t_5502c4c6):
  30 分钟轮询 / monitor_url hash 抑制 (零 token) / exchange 归档 + CLI 推送。

设计要点:
  - 监听 127.0.0.1:8768 (避开 salon 已用端口 8765/8766/8767)
  - GET /governance_snapshot.json: 对 4 个生产库 (default/athena/xuanwu/duanmu)
    只读 `file:...?mode=ro` URI 连接, 跑 detector dry-run (L0 阈值 + L1 去重),
    输出固定 schema JSON
  - 零写入: 无 --apply; detector 内部 mode=ro URI, 仅 PRAGMA + SELECT
  - 字节序稳定 (monitor_url hash 抑制静默的关键):
      * ts 用「服务启动时间」而非每请求当前时间 → 数据未变时逐字节相同
      * profiles 字段固定顺序 (default/athena/xuanwu/duanmu)
      * 数值 round 固定小数位, 无随机/无排序抖动
    → 仅 4 库 size/candidates/dup_pairs 真实变化才改变输出, 触发 agent
  - 端口单一事实源: config/governance.yaml monitor.port;
    脚本头常量 DEFAULT_PORT 仅作 yaml 缺失时的兜底 (两处保持一致)

用法:
  python governance_status_server.py            # 前台运行 (127.0.0.1:8768)
  python governance_status_server.py --port N   # 显式覆盖端口 (仍以 yaml 为默认)
"""
from __future__ import annotations

import json
import os
import sys
import threading
import time
import traceback
from concurrent.futures import ThreadPoolExecutor, TimeoutError as _FutTimeoutError
from datetime import datetime
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Dict, List, Optional

# 端口兜底常量 — 权威值在 config/governance.yaml monitor.port
DEFAULT_PORT = 8768

# ---- 防御性加固 (独立复核后, 2026-08-10) ----
# 根因复跑确认: 进程僵死 = terminal background 启动 → 会话拆除 → 孤儿冻结,
# 非 detector 死锁 (实测 150MB 库 L0+L1 合计 0.3s)。加固仍保留以覆盖
# 未来误用 terminal background 启动 / 库继续增长变慢的场景:
#   * 快照 TTL 缓存 60s: 30min 监控频率下 detector 实际极少执行
#   * 单飞锁: 同一时刻仅 1 个快照计算 (并发请求自然合并)
#   * 单库超时 8s + 整快照超时 30s: 卡死不拖垮端点, 超时输出故障可见
#   * /healthz: 独立于 DB 的 liveness 探针
SNAPSHOT_TTL_S = 60.0        # 快照缓存有效期 (秒)
PROFILE_TIMEOUT_S = 5.0      # 单库 detector 超时 (秒), 防 SQLite 锁无限等 (kanban A1 任务书 b)
SNAPSHOT_TIMEOUT_S = 30.0    # 整快照超时 (秒), 超时返回 503 结构
SOCKET_TIMEOUT_S = 15.0      # socket 超时 (秒), 慢客户端/半开连接不阻塞服务 (kanban A1 任务书 a)

# 生产库路径兜底常量 — 权威值在 config/governance.yaml production_db.paths
# 2026-08-11 开源化: 兜底由 governance_config 按 HERMES_HOME 布局推导,
# 不再硬编码内部绝对路径 (来源: docs/PRODUCTION-DB-DISCOVERY.md §1)
# 注: 定义在 import governance_config 之后 (需 cfg, 见下方加载区)
# 固定输出顺序 (monitor_url hash 抑制要求逐字节稳定)
PROFILE_ORDER: List[str] = ["default", "athena", "xuanwu", "duanmu"]

# 项目根 = 本文件上一级 (scripts/)
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
# 配置路径: 本地覆盖 (governance.local.yaml, 不进 git) 优先, 回落模板 (governance.yaml)
# 2026-08-11 开源化: 模板 yaml 保持干净可发布, 本机真实路径放 local 文件
CONFIG_PATH = os.path.join(PROJECT_ROOT, "config", "governance.yaml")
CONFIG_LOCAL_PATH = os.path.join(PROJECT_ROOT, "config", "governance.local.yaml")


def _config_path() -> str:
    return CONFIG_LOCAL_PATH if os.path.exists(CONFIG_LOCAL_PATH) else CONFIG_PATH
sys.path.insert(0, os.path.join(PROJECT_ROOT, "scripts"))

import governance_config as cfg          # noqa: E402  阈值单一事实源 (FIX-B3)
import governance_detector as detector   # noqa: E402  L0/L1 dry-run 只读检测

# 生产库路径兜底常量 (需 cfg, 故定义在 import 之后) — 权威值在 governance.yaml
PROFILE_DB_PATHS_FALLBACK: Dict[str, str] = dict(cfg.default_profile_db_paths())

# 服务启动时间戳 (固定) — 保证数据未变化时输出逐字节相同
BOOT_TS = datetime.now().astimezone().isoformat(timespec="seconds")


def load_config() -> tuple:
    """读 config/governance.yaml → (port, db_paths)。yaml 缺失/字段缺失时回退常量。

    返回 (port: int, db_paths: Dict[str, str])。
    yaml 是权威源: 修改端口/路径只改 yaml, 脚本常量仅兜底防启动失败。
    """
    port = DEFAULT_PORT
    paths = dict(PROFILE_DB_PATHS_FALLBACK)
    try:
        import yaml
        with open(_config_path(), "r", encoding="utf-8") as f:
            conf = yaml.safe_load(f) or {}
        mon = (conf.get("monitor") or {})
        if isinstance(mon.get("port"), int):
            port = mon["port"]
        pdb = (conf.get("production_db") or {})
        p = pdb.get("paths") or {}
        if isinstance(p, dict):
            for k in PROFILE_ORDER:
                if p.get(k):
                    paths[k] = str(p[k])
    except FileNotFoundError:
        pass  # yaml 不存在 → 纯常量兜底
    except Exception as e:  # noqa: BLE001  yaml 解析失败不阻断服务
        print(f"WARN: config load failed ({e}), using fallback constants", file=sys.stderr)
    return port, paths


# 快照缓存状态 (单飞 + TTL, 独立复核后加固)
_snap_lock = threading.Lock()                       # 单飞: 同一时刻仅 1 个快照计算
_snap_cache: Optional[tuple] = None                  # (timestamp, payload) 上次成功结果
_snap_executor = ThreadPoolExecutor(max_workers=1)   # 快照计算专用线程 (队列单飞)


def _detect_profile(db_path: str, profile: str, thr_mb: int) -> tuple:
    """单库 L0+L1 dry-run (只读). 独立线程内跑, 便于超时控制."""
    l0 = detector.detect_threshold(db_path, profile, thr_mb, dry_run=True)
    pairs, _applicable = detector.detect_l1_duplicates(db_path, profile, dry_run=True)
    return l0, pairs


def _snapshot_compute() -> dict:
    """跑 4 profile 只读 dry-run, 返回固定 schema 快照 dict (纯计算, 无锁).

    零写入保证: detector.detect_threshold / detect_l1_duplicates 内部均以
    `file:...?mode=ro` URI 连接, 只执行 PRAGMA + SELECT (见 governance_detector.py)。
    单库超时 → 该库记 error, 不拖垮整端点。
    """
    profiles: Dict[str, dict] = {}
    alerts: List[str] = []
    for profile in PROFILE_ORDER:
        db_path = PROFILE_DB_PATHS[profile]
        entry: dict = {
            "size_mb": None,
            "threshold_mb": None,
            "over_limit": False,
            "need_release_mb": None,
            "candidates": None,
            "dup_pairs": None,
            "errors": [],
        }
        if not os.path.exists(db_path):
            entry["errors"].append(f"db not found: {db_path}")
            profiles[profile] = entry
            alerts.append(f"{profile}: db not found ({db_path})")
            continue
        try:
            thr_mb = int(cfg.resolve_threshold_mb(profile))  # default=80, 其余=60
            with ThreadPoolExecutor(max_workers=1) as ex:
                fut = ex.submit(_detect_profile, db_path, profile, thr_mb)
                try:
                    l0, pairs = fut.result(timeout=PROFILE_TIMEOUT_S)
                except _FutTimeoutError:
                    raise TimeoutError(f"detector timeout >{PROFILE_TIMEOUT_S:.0f}s")
            entry.update(
                {
                    "size_mb": round(l0.db_size_bytes / 1048576, 1),
                    "threshold_mb": float(thr_mb),
                    "over_limit": l0.over_limit,
                    "need_release_mb": round(l0.need_release_bytes / 1048576, 2),
                    "candidates": len(l0.candidates),
                    "dup_pairs": len(pairs),
                }
            )
            if l0.over_limit:
                over_by = round(l0.db_size_bytes / 1048576 - thr_mb, 1)
                alerts.append(
                    f"{profile} {entry['size_mb']}MB > {thr_mb}MB threshold "
                    f"(over by {over_by}MB)"
                )
        except Exception as e:  # noqa: BLE001  单 profile 失败不拖垮整端点
            entry["errors"].append(f"{type(e).__name__}: {e}")
            alerts.append(f"{profile}: detector failed ({type(e).__name__}: {e})")
        profiles[profile] = entry

    return {
        "ts": BOOT_TS,
        "profiles": profiles,
        "alerts": alerts,
    }


def snapshot() -> dict:
    """单飞 + TTL 缓存 + 整体超时 (加固版入口).

    - 缓存命中 (TTL 60s 内): 直接返回, 零 detector 调用 → 字节稳定
    - 未命中: 提交到单飞执行器 (同一时刻仅 1 个计算在跑), 等整体超时
    - 整体超时: 返回故障 JSON (alerts 含 timeout) — 故障可见而非静默挂死
    """
    global _snap_cache
    now = time.time()
    with _snap_lock:
        if _snap_cache is not None and (now - _snap_cache[0]) < SNAPSHOT_TTL_S:
            _log(f"[snapshot] cache HIT (age {now - _snap_cache[0]:.1f}s)")
            return _snap_cache[1]
        _log("[snapshot] cache MISS -> compute")
        fut = _snap_executor.submit(_snapshot_compute)
        try:
            payload = fut.result(timeout=SNAPSHOT_TIMEOUT_S)
        except _FutTimeoutError:
            _log(f"[snapshot] FULL TIMEOUT after {SNAPSHOT_TIMEOUT_S:.0f}s")
            return {
                "ts": BOOT_TS,
                "profiles": {},
                "alerts": [
                    f"snapshot timeout after {SNAPSHOT_TIMEOUT_S:.0f}s "
                    "(detector stalled on live DB)"
                ],
            }
        _snap_cache = (time.time(), payload)
        _log(f"[snapshot] computed OK, cached (ts={BOOT_TS})")
        return payload


def _log(msg: str) -> None:
    """stdout 日志 (带时间戳). 孤儿化后管道失效时静默, 不崩线程."""
    try:
        print(f"{datetime.now().strftime('%H:%M:%S')} {msg}", flush=True)
    except Exception:  # noqa: BLE001
        pass


def _safe_traceback() -> None:
    """安全打印 traceback: 孤儿进程 stderr 管道失效时静默 (不崩线程)."""
    try:
        traceback.print_exc(file=sys.stderr)
        sys.stderr.flush()
    except Exception:  # noqa: BLE001
        pass


class TimeoutHTTPServer(ThreadingHTTPServer):
    """任务书 a: accept 后立即给连接 socket 设超时 (双保险, 不依赖 handler 内设置).

    Windows 下 recv/send 阻塞若无超时会无限挂线程; 15s 兜底保证
    半开连接/慢客户端不拖垮服务 (handler 内 self.connection.settimeout 重复设, 无副作用).
    """

    def get_request(self):
        request, client_address = super().get_request()
        try:
            request.settimeout(SOCKET_TIMEOUT_S)
        except Exception:  # noqa: BLE001  socket 已坏则交给 handler 处理
            pass
        return request, client_address


class Handler(BaseHTTPRequestHandler):
    server_version = "GovernanceStatus/1.0"   # 固定, 避免默认版本号噪音

    def do_GET(self):  # noqa: N802 (http.server 命名约定)
        # socket 超时: 慢客户端/半开连接不阻塞服务线程
        try:
            self.connection.settimeout(SOCKET_TIMEOUT_S)
        except Exception:  # noqa: BLE001
            pass
        path = self.path.split("?", 1)[0]
        if path == "/governance_snapshot.json":
            try:
                payload = snapshot()
                body = json.dumps(payload, ensure_ascii=False, indent=2).encode("utf-8")
                self.send_response(200)
                self.send_header("Content-Type", "application/json; charset=utf-8")
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)
            except Exception as e:  # noqa: BLE001
                _safe_traceback()
                body = json.dumps(
                    {"ts": BOOT_TS, "profiles": {}, "alerts": [f"server error: {e}"]},
                    ensure_ascii=False,
                ).encode("utf-8")
                try:
                    self.send_response(500)
                    self.send_header("Content-Type", "application/json; charset=utf-8")
                    self.send_header("Content-Length", str(len(body)))
                    self.end_headers()
                    self.wfile.write(body)
                except Exception:  # noqa: BLE001  响应已不可写, 放弃即可
                    pass
        elif path == "/healthz":
            # 独立于 DB 的 liveness 探针 (固定字节, 不跑 detector)
            body = json.dumps(
                {"status": "ok", "ts": BOOT_TS}, ensure_ascii=False
            ).encode("utf-8")
            self.send_response(200)
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)
        else:
            body = b"404 not found"
            self.send_response(404)
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

    def log_message(self, fmt: str, *args):  # 静默访问日志 (服务是后台探针)
        try:  # 孤儿化后 stderr 管道可能失效, 写失败不崩线程 (独立复核复跑根因链)
            sys.stderr.write("[%s] %s\n" % (self.log_date_time_string(), fmt % args))
            sys.stderr.flush()
        except Exception:  # noqa: BLE001
            pass


# 模块级配置 (load_config 优先读 yaml, 缺失回退常量); main 里可被 --port 覆盖
PORT, PROFILE_DB_PATHS = load_config()


def main(argv: Optional[List[str]] = None) -> int:
    global PORT, PROFILE_DB_PATHS
    argv = list(sys.argv[1:] if argv is None else argv)
    port, PROFILE_DB_PATHS = PORT, dict(PROFILE_DB_PATHS)
    if "--port" in argv:
        i = argv.index("--port")
        port = int(argv[i + 1])
    srv = TimeoutHTTPServer(("127.0.0.1", port), Handler)
    srv.daemon_threads = True  # 显式: handler 线程不阻止进程退出 (孤儿化防御)
    print(f"governance_status_server listening on 127.0.0.1:{port} "
          f"(boot_ts={BOOT_TS})", flush=True)
    print(f"profiles: {', '.join(PROFILE_ORDER)}", flush=True)
    try:
        srv.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        srv.server_close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
