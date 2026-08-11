# C3-A — est_saved 全列估算修复 + reclaim 达标判断改周期实测

- 任务: t_eff97eda ([archive-governor C3-A], assignee default)
- 执行: 2026-08-10 (run 248)
- 前置: t_d1edd45e (C3 分析, docs/C3-HOT-DATA-THRESHOLD-EVALUATION.md)
- 模式: **只读分析 + 副本实验** — 生产库全程 mode=ro; 写操作仅发生在 docs/.tmp/c3a/ 副本

---

## 0. 问题复述 (C3 实测证据)

| 缺陷 | 位置 | 后果 |
|------|------|------|
| `est_saved = content_bytes × 0.9` 只算 messages.content 列 (14.2MB) | governance_detector.py:225, governance_reclaim_run.py:87 | 漏 reasoning/reasoning_content/tool_calls/api_content (~35MB) 与 FTS 索引 (57.6MB, 占库 40%) → 低估真实释放 **12 倍** (预估 9.61MB vs 实测 115.2MB) |
| reclaim 达标判断 `released >= info["need"]` 用低估 est 累加 | governance_reclaim_run.py:142 | 永远判不达标 → 过度归档 (220 全量 vs 实际需 ~130-180) |

---

## 1. 变更文件清单

| 文件 | 变更 |
|------|------|
| `scripts/governance_config.py` | **单一事实源新增**: `MESSAGES_TEXT_COLS` (5 全文本列), `EST_COMPRESSION_RATIO=0.9`, `TARGET_CHECK_INTERVAL=20`; 共享函数 `text_bytes_sum_expr()` (动态检测列, 兼容旧 schema), `est_saved_bytes()` (压缩系数收敛点), `db_logical_used_bytes()` (逻辑占用实测: page_count−freelist)×page_size + WAL) |
| `scripts/governance_detector.py` | SQL 新增 `text_bytes` (全文本列和, 经 cfg.text_bytes_sum_expr); `est_saved_bytes = cfg.est_saved_bytes(text_bytes)`; JSON/CLI 报告带 text_bytes |
| `scripts/governance_reclaim_run.py` | 同上全列估算; **达标判断改周期实测**: 每归档 20 个 → 完整 reclaim (FTS optimize+checkpoint+VACUUM) → `os.path.getsize ≤ target` 即停; 防空转 (need≤0 归档 1 个即停) 保留; 周期已回收则收尾跳过 |
| `scripts/governance_startup_check.py` | 同款周期实测达标判断; 验收 `enough` 改用 VACUUM 后文件大小 ≤ target (实测) |
| `tests/test_governance_c3a_estsaved.py` | **新增 6 测试**: 全列估算 (reasoning/tool_calls 计入) / 旧 schema 兼容 / detector-reclaim 一致性 / 逻辑占用下降 / 达标即停 (40 候选归档 20 即停, 非全量) / need≤0 防空转 |

> 边界遵守: 未动归档原子流 (session_governor.archive_session / data), 未改任何生产配置, 未执行生产 --apply。

---

## 2. 关键工程决策: 为什么达标判断必须"周期 reclaim + getsize"而非裸 getsize

C3 报告建议"每归档 N 个检查 os.path.getsize(db) ≤ target", 但实测发现裸 getsize 不可行:

1. **auto_vacuum=0** (生产库): DELETE 释放页进 freelist 但文件不缩, 直到 VACUUM;
2. **FTS5 删除标记**: messages_fts 的 DELETE 只写删除标记, 空间直到 `optimize` 才真正释放 — 归档循环中即便逻辑占用也几乎不降 (实测归档 25 会话仅 −5MB)。

→ 正确实现: 每归档 TARGET_CHECK_INTERVAL=20 个会话执行一次完整 `reclaim_space(mode="full")` (FTS optimize + checkpoint + VACUUM), 再 `os.path.getsize` 实测。这与 C3 实验 §3.2 "真实 VACUUM 后大小" 曲线口径完全一致。

---

## 3. 副本实测对照 (docs/.tmp/c3a/, 生产库 backup API 副本)

### 3.1 est_saved 估算修复 (dry-run, 干净副本 147.1MB)

| 指标 | 修复前 | 修复后 | 说明 |
|------|--------|--------|------|
| content 合计 | 11.09MB | 11.09MB | 未变 |
| **text 全列合计** | — | **44.04MB** | content+reasoning+reasoning_content+tool_calls+api_content |
| **est_saved 合计** | **9.61MB** | **39.64MB** (4.1x) | 仍低于 C3 实测 115.2MB, 因 FTS 索引 (57.6MB) 释放无法从文本列估算 — 由达标判断实测兜底 |
| detector/reclaim 候选 | — | 223/223 一致, est_saved 零不一致 | 单一事实源生效 |

### 3.2 达标判断周期实测 (副本 --apply, threshold 80MB → target 68MB)

| 指标 | 修复前 | 修复后 | C3 预期 |
|------|--------|--------|---------|
| 归档会话数 | 222 (全量) | **140** | ~130-180 ✅ |
| 归档后大小 | 29.74MB (过度) | **66.67MB ≤ 68MB** | ≤68MB ✅ |
| 释放 | 115.2MB | 80.4MB | 达 target 即止, 写入量 **−37%** |
| errors | 0 | 0 | — |
| FTS 同步 | — | messages_fts 9576 == messages 9576 ✅ | — |

### 3.3 达标档位实证 (40 候选合成库, 单测)

`archived=20/40 即停 (20 的倍数, 检查周期), VACUUM 后 6.19MB ≤ target 9.35MB` — 不再依赖 est 累加, 达标即止语义恢复。

---

## 4. 回归

- 全量 unittest: **42/42 通过** (30 原有 + 6 C3-A 新增 + 6 C4 并行修复新增, 后者与 C3-A 正交无冲突)
- 生产库零写入: governance 表 292/585/292/1 与 P2-R 基线一致; 未执行生产 --apply
- 生产库只读复核: messages 五列确认存在 (content 14.6MB / reasoning 12.5MB / reasoning_content 12.5MB / tool_calls 10.9MB / api_content 0.6MB)

---

## 5. 遗留事项

1. **生产回收执行 (C3 方向 B)**: 需 user 拍板 — 命令 `governance_reclaim_run.py --db <生产库> --profile default --threshold-mb 80 --apply`, 预期 147MB → ~67MB (达标即止, 不再全量);
2. **周期触发 (C3 方向 C)**: 启动自检接入或 cron watchdog, 否则 25MB/天增速下 2.5 天回弹 (任务 t_ea098b73 已排队);
3. **FTS trigram 策略 (C3 方向 D)**: 需 user 权衡检索需求后再定;
4. **db_logical_used_bytes** 现仅测试/诊断用 (达标判断已改 getsize+reclaim 口径), 保留作未来增量回收判断基础。

---

*C3-A 完成 — 变更 5 文件 + 新增 6 测试, 副本实测达标即停 140/222 (−37% 写入量), 生产库零写入。*
