# Code Review: codex-gateway-lite Windows 历史会话恢复修复

## 判定：**CONDITIONAL PASS**

---

## P0 — 必须在发布前解决

1. **app-server 回填覆盖 model_provider**
   - 回填逻辑会把旧 rollout/jsonl 中的 `model_provider` 写回 `custom`/`openai`，覆盖 gateway 刚修正的值。这意味着用户重启或 reload 后，部分线程会再次被 UI provider 过滤器隐藏。
   - **gateway 侧修复本身是正确的，但如果 app-server 回填在 gateway sync 之后执行，修复等于无效。**
   - 要求：确认回填时序（gateway sync 必须在 app-server backfill **之后**再跑一次），或在 app-server 侧同步修正 model_provider 写入逻辑。

2. **UI 仍只显示 2 条，未完全恢复**
   - 这是用户可感知的 regression，修复的核心目标未达成。在无法复现"全部恢复"之前，不能声称修复完成。
   - 要求：在干净进程级重启（kill renderer + app-server → 重新启动）后，验证 UI 线程数与 sqlite active rows 数一致。如果仍不一致，需要排查 renderer 缓存或 IPC 层。

---

## P1 — 可在后续迭代修复

1. **时间字段 RFC3339 → INTEGER 的向后兼容**
   - 如果用户已有旧 state.sqlite 中存的是 TEXT 格式时间，gateway 新版读取时是否有 migration/容错？测试只覆盖了新写入路径。建议加一个读取旧 TEXT 格式行的防御性测试或 `CAST` 兼容。

2. **config.toml 兜底读取的健壮性**
   - 从 `CODEX_HOME/config.toml` 读 `model_provider` 作为 fallback 是合理的，但需确认：文件不存在 / 字段缺失 / 值为空字符串时不会 panic 或写入空值。

3. **测试覆盖**
   - 3/3 通过，但只覆盖了 happy path。建议补充：
     - 旧 TEXT 时间字段行的 sync 行为
     - `model_provider` 为 `custom`/`openai`/空/缺失时的 fallback 链
     - app-server 回填后再次 sync 的端到端场景

---

## 总结

| 维度 | 状态 |
|---|---|
| 时间字段 INTEGER 修复 | ✅ 代码正确，测试通过 |
| model_provider fallback | ✅ 逻辑合理 |
| build & binary hash | ✅ 确认 |
| sqlite 数据验证 | ✅ integer 类型确认 |
| **UI 端到端恢复** | ❌ **未证明** |
| **app-server 回填竞态** | ❌ **未解决** |

**结论：gateway 代码变更本身质量 PASS，但端到端修复未闭环。在 P0-1（回填时序）和 P0-2（进程级重启验证 UI 全量恢复）解决前，不建议作为"修复完成"发布给用户。解决后可直接 GO。**
