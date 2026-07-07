# Contributing

感谢你愿意改进 Codex Gateway Lite。

## 开发环境

- Rust stable，edition 2024。
- macOS 或 Windows 都可以开发；CI 会覆盖 Linux / macOS / Windows。
- 不需要 Node.js、Python、pnpm、SQLite 或 OpenSSL 作为运行依赖。

## 本地验证

提交 PR 前请运行：

```bash
cargo fmt -- --check
cargo test
cargo run --quiet -- --help
```

如果改动涉及启动脚本，也请至少在对应平台手动跑一次：

```bash
./"Codex Gateway Lite.command"
```

或：

```powershell
& ".\Codex Gateway Lite.cmd"
```

## PR 要求

- 保持改动聚焦，一次 PR 尽量解决一个问题。
- 不要提交真实 API Key、auth 文件、本机私有路径或本地调试接力文件。
- 不要提交 `target/`、`.env*`、`config.json` 等本地文件。
- 如果修改协议转换、model catalog 或 task plan 行为，请补充或更新测试。
- 文档中的示例路径请使用 `~`、`%USERPROFILE%`、`/Users/demo`、`C:\Users\demo` 这类占位路径。

## Issue 建议

报告 bug 时请尽量提供：

- 操作系统与版本。
- Codex App 安装方式和版本信息。
- `codex-gateway-lite --help` 或相关命令的最小输出。
- 复现步骤。
- 已做过的排查。

请先删掉日志里的 API Key、cookie、auth token、真实 Base URL、个人路径和会话正文。
