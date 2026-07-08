# Final Diff Review

## 逐项确认

| 项目 | 状态 | 备注 |
|------|------|------|
| Windows 任务卡片延迟修复 | ✅ | `mutationTouchesRightRail` 检测右侧 rail 变化走 `scheduleApplyImmediate`（下一帧），其余保留 750ms 节流，resize 也走即时路径。逻辑合理。 |
| 历史会话任务快照恢复 | ✅ | `last_plan_history_seed` 在首次注入、CDP 重连、config_changed、120s 周期到期时触发 `seed_history=true`，传入 `inject_plan_ui_inner` 的 `verbose=false, seed_history` 参数。闭环。 |
| mac 本地数据影响 | ✅ | 无 mac 专属路径/脚本改动；写入限定 CODEX_HOME 范围；`rollout_path_belongs_to_home_sessions` 使用 `canonicalize + starts_with(home/sessions)` 做路径安全校验。无误伤风险。 |
| timestamp_to_seconds P2 | ✅ | 已实现 `>10_000_000_000.0` 时除以 1000 的防御，单测覆盖秒/毫秒/零三种输入。 |

## 逐点审查

### P0 — 无

无数据破坏或路径越界问题：
- `normalize_rollout_session_meta_provider` 使用 temp file + rename 原子写入，不会产生半写文件。
- `rollout_path_belongs_to_home_sessions` 双向 canonicalize 后检查 `starts_with(home/sessions)`，防止路径穿越。
- `native_model_provider` 现在优先使用 `active_model_provider`，回退逻辑不变，不会产生空值写入（`non_empty_string` 过滤）。

### P1 — 无

- `mutationTouchesRightRail` 中 `getBoundingClientRect` 调用在 MutationObserver 回调内执行，可能触发强制 reflow，但有 `rect.width > 16 && rect.height > 16` 前置过滤和 `managedNode(target)` 排除自身节点，加上 `scheduleApplyImmediate` 本身也是 rAF 延迟而非同步 apply，CPU 风险可控。
- `sync_local_thread_catalog_for_provider` 签名改动：`sync_local_thread_catalog` 标记 `#[cfg(test)]` 保留给测试用，生产调用点唯一切到 `sync_local_thread_catalog_for_provider`，无遗漏。
- `created_at`/`updated_at`/`recency_at` 从 `text_sql_value(rfc3339)` 改为 `integer_sql_value(seconds)`，测试用 `typeof()` 断言确认写入类型为 `"integer"`，与 native schema 一致。

### P2 — 1 项（仅观察）

**`normalize_rollout_session_meta_providers` 遍历所有非归档 entries 做文件 I/O**：如果 entries 量大（数百），首次 sync 会串行读写大量 rollout 文件。当前用 `HashSet<seen>` 去重，且只处理 `home/sessions` 下的文件，实际规模有限。不阻塞发布，后续如果用户反馈启动慢可加并行或懒处理。

## 其他确认

- `inject_plan_ui_quiet` 已删除，无残留引用。
- `SCRIPT_VERSION=41` 与静态断言一致。
- `PLAN_UI_HISTORY_RESEED_INTERVAL_SECS=120` 断言 `<= 300` 通过。
- 测试全部通过：timestamp 1 passed, catalog 3 passed, plan_ui 22 passed, release build passed。

---

**VERDICT = PASS**

| 等级 | 数量 | 详情 |
|------|------|------|
| P0 | 0 | — |
| P1 | 0 | — |
| P2 | 1 | rollout normalize 串行 I/O，规模可控，不阻塞 |
