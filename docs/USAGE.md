# archive-governor 使用说明书

> 会话冷归档治理工具 — 自动阈值触发 + 内容级整合，零 LLM / 零交互 / 零视图。
> 本说明书面向使用者：安装 → 配置 → 日常使用 → 故障排查。

---

## 1. 它解决什么问题

Hermes Agent（或任何用 SQLite 存会话的 agent）的会话库会随使用持续膨胀：

| 症状 | 后果 |
|---|---|
| `state.db` 超过 100MB+ | 启动慢、备份慢、检索慢 |
| 手动删会话 | 误删活跃/重要会话，不可逆 |
| LLM 摘要压缩 | 有 token 成本，破坏原文 |

archive-governor 用**纯规则**解决：库超阈值 → 自动把最旧的**已结束、非活跃**会话**原子冷归档**到 `.json.gz` → 热库瘦身，冷数据可检索、可恢复。

**核心理念：零丢失、零成本、可逆。**

---

## 2. 环境要求

| 依赖 | 版本 | 说明 |
|---|---|---|
| Python | ≥ 3.11 | 建议 3.11+ |
| PyYAML | ≥ 6.0 | `pip install pyyaml` |
| 操作系统 | Windows / macOS / Linux | 路径推导已跨平台（HERMES_HOME 布局） |

---

## 3. 安装

```bash
# 1. 获取代码
git clone <your-fork-or-release-url> archive-governor
cd archive-governor

# 2. 安装依赖
pip install -e .        # 或仅安装核心依赖: pip install pyyaml

# 3. 验证安装
python scripts/session_governor.py --help     # 应显示 CLI 帮助
```

---

## 4. 配置

### 4.1 两步配置

```bash
# ① 复制模板为本地配置（模板本身干净，本地配置含你的真实路径）
cp config/governance.yaml config/governance.local.yaml
```

> `governance.local.yaml` 优先级最高（代码先读它）。它已被 `.gitignore` 排除，不会误提交你的路径。

### 4.2 配置项说明

```yaml
governance:
  storage_threshold_mb: 60      # 每 profile 阈值 (MB)：库超过它就触发归档
  storage_ceiling_mb: 80        # 硬顶 (MB)：超过只告警，绝不丢数据（安全阀）
  profile_overrides:
    default: 80                 # 给某个 profile 单独设阈值（可多个）
  target_ratio: 0.15            # 滞后带：归档后体积 ≤ 阈值 × (1-0.15) = ×0.85
  archive_root: "/data/ag-archives"   # 冷归档包存放目录（各 profile 自动建子目录）

production_db:
  path: ""                      # default profile 兜底路径（留空 = 自动推导）
  paths: {}                     # 显式覆盖（留空 = 自动推导，见 4.3）
```

### 4.3 库路径推导规则（重要）

未显式配置时，代码按 **Hermes 目录布局**自动推导：

| Profile | 推导路径 |
|---|---|
| default | `$HERMES_HOME/state.db` |
| 其他 (athena 等) | `$HERMES_HOME/profiles/<name>/state.db` |

- `HERMES_HOME` 取环境变量；未设置时用平台默认：
  - Windows: `~/AppData/Local/hermes`
  - macOS/Linux: `~/.hermes`
- 如你的布局不同，在 `governance.local.yaml` 的 `production_db.paths` 显式指定即可（优先级最高）。

---

## 5. 使用

### 5.1 日常巡检（推荐：启动自检）

archive-governor 提供 **Hermes 插件钩子**（`plugins/archive-governor-autostart`），agent 每次启动自动检查：

```bash
# 把插件放到 Hermes 插件目录（Windows 示例）：
#   ~/AppData/Local/hermes/plugins/archive-governor-autostart/
#   （其他 profile: ~/AppData/Local/hermes/profiles/<name>/plugins/...）

# 在 Hermes 配置启用：
#   plugins:
#     enabled:
#       - archive-governor-autostart
```

启动后日志（按 profile 分目录）：
```
[时间] hook registered: on_session_start
[时间] profile=default db=.../state.db threshold=80MB ratio=0.85
        → size=76.9MB target=68MB over_limit=False candidates=274
```

- **零写入**：只读检查（mode=ro），绝不自动归档
- **fail-open**：任何异常只记日志，绝不阻断 agent 启动
- 超限时**只告警**，真实归档需你手动确认（见 5.3）

### 5.2 手动检查

```bash
# 单库阈值检测 (dry-run, 只读)
python scripts/governance_detector.py --db <你的state.db> --profile default --json

# 全 profile dry-run 报告
python scripts/governance_search.py dry-run
```

### 5.3 真实归档（超限时执行，需谨慎）

```bash
# ① 先 dry-run 看会归档哪些、释放多少
python scripts/governance_reclaim_run.py --db <你的state.db> --profile default --dry-run

# ② 确认候选集无误后，执行归档
python scripts/governance_reclaim_run.py --db <你的state.db> --profile default --apply
```

> ⚠️ **安全提示**：`--apply` 是唯一会写库的操作。生产环境建议：
> - 先备份（`cp state.db state.db.bak`）
> - 在非高峰时段执行
> - 归档后可用 `governance_search.py search` 验证冷数据可检索

### 5.4 冷数据检索

```bash
# 按关键词搜索（含已归档冷数据）
python scripts/governance_search.py search --query "关键议题"

# 只看活跃库
python scripts/governance_search.py search --scope active --query "..."
```

### 5.5 恢复会话

```bash
python scripts/session_governor.py restore \
    --db <你的state.db> \
    --session <session-id> \
    --archive-root <archive_root>
```

> restore 拒绝覆盖热库已有会话（防冲突）。

---

## 6. 阈值建议（经验值）

| 场景 | 建议阈值 | 说明 |
|---|---|---|
| 个人日常使用 | 60-80 MB | 默认即可 |
| 高频使用/多 agent | 100-150 MB | 归档频繁则调高，减少操作 |
| 服务器/长期运行 | 200 MB+ | 配合 weekly 归档计划 |

**原则**：阈值不是越小越好——归档本身有 IO 成本，且 30 天内的活跃会话不该被归档。`target_ratio=0.15`（15% 滞后带）是测试过的平衡点，一般不用改。

---

## 7. 与备份的关系

archive-governor **不替代备份**：
- 它做的是"热库瘦身"，冷数据仍在同一磁盘
- 建议搭配定期备份（把 `archive_root/` 也纳入备份范围）
- 工具内置 `security_check` 可检查归档目录是否在备份覆盖内（需设置 `AGO_BACKUP_SCRIPT` 环境变量指向你的备份脚本）

---

## 8. 故障排查

| 现象 | 可能原因 | 处理 |
|---|---|---|
| 启动慢 | 库过大 | 跑 dry-run，考虑归档 |
| `over_limit=True` 持续 | 库增长快 | 评估调高阈值或执行归档 |
| 日志有 `FAIL-OPEN` | 检测异常 | 不影响启动，检查配置/路径 |
| 日志有 `THRESHOLD-SUSPECT` | 验收文档被改 | 检查 `docs/PRODUCTION-DB-ACCEPTANCE.md` 的阈值行 |
| 找不到日志文件 | 插件未触发 | 确认是新会话启动（续接旧会话不触发） |
| `file is not a database` | 路径指向错误 | 检查 `production_db.paths` |

---

## 9. 常见问题 (FAQ)

**Q: 归档会丢数据吗？**
A: 不会。归档 = 原子移动到 `.json.gz` + 冷索引记录，消息总数不变（sha256 双层校验）。恢复可逆。

**Q: 会误删活跃会话吗？**
A: 不会。活跃（24h 内有写入）/ pinned / 修复链中的会话被硬保护，不可归档。

**Q: 需要 LLM API 吗？**
A: 完全不需要。纯规则机械执行，零 token 成本。

**Q: 多个 agent 能共用吗？**
A: 可以。每个 profile 独立库、独立阈值、独立归档目录，互不干扰（不跨 profile 抵扣空间）。

**Q: 和 Hermes 自带的会话清理冲突吗？**
A: 不冲突。archive-governor 是"冷归档"（可逆），Hermes 自带清理是"删除"（不可逆）。两者可共存，建议以 archive-governor 为主。

---

## 10. 安全与边界

- ✅ 生产库默认只读（`mode=ro`），`--apply` 才写
- ✅ SQL 全参数化 + 输入白名单（防注入）
- ✅ 审计日志不含消息原文（隐私）
- ✅ 归档前自动备份（保留 5 份）
- ❌ 不调用 LLM（零 token）
- ❌ 不提供 UI（零视图）
- ❌ 不跨 profile 抵扣空间
- ❌ 不删数据（只冷归档，可恢复）

---

*问题反馈：请在 GitHub 仓库开 Issue，附上日志尾部 + 配置（脱敏）。*
