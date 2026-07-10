# Codex Gateway Lite

[![CI](https://github.com/yanmoais/codex-gateway-lite/actions/workflows/ci.yml/badge.svg)](https://github.com/yanmoais/codex-gateway-lite/actions/workflows/ci.yml)
[![License: MIT](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE)
[![Rust 2024](https://img.shields.io/badge/Rust-2024-orange.svg)](https://www.rust-lang.org/)
[![Platform](https://img.shields.io/badge/platform-macOS%20%7C%20Windows-lightgrey.svg)](#quick-start)

Codex Gateway Lite 是一个轻量 Rust CLI，用来把第三方模型供应商更顺滑地接入 Codex App：写入 Codex 原生 provider/model catalog 配置，按需启动本地 Responses-compatible proxy，并通过 CDP 注入轻量任务清单 UI 修正。

它只做 gateway/bootstrap 这件事，不包含桌面管理器 UI、会话删除、插件市场、安装器、云同步或其它产品面。

## Highlights

- **Codex 原生配置写入**：保留现有 `~/.codex/config.toml`，只合并 provider/model/catalog 相关字段。
- **模型目录自动生成**：从供应商 `/v1/models` 拉取模型，生成 Codex 可识别的 `model_catalog_json`。
- **双协议接入**：支持上游 `responses` 直连，也支持上游只有 `chat_completions` 时通过本地代理转换。
- **上下文预算保护**：可配置 `provider.contextBudget`，在发给上游前裁剪超长上下文。
- **Codex App 保活**：自动识别并启动 Codex App，保持 CDP 可访问，配置变化后热应用。
- **Task Plan UI 修正**：让 Codex 的任务清单卡片在历史会话、侧栏 hover、媒体预览等场景下更稳定。
- **跨平台引导**：macOS / Windows 双击脚本会检查依赖，缺失时自动安装或打开官方入口。

## Quick start

### 1. Clone

```bash
git clone https://github.com/yanmoais/codex-gateway-lite.git
cd codex-gateway-lite
```

### 2. Run bootstrap

macOS：

```bash
chmod +x "Codex Gateway Lite.command"
./"Codex Gateway Lite.command"
```

Windows：

```powershell
& ".\Codex Gateway Lite.cmd"
```

如果 PowerShell 直接运行脚本被策略拦截，可以临时放行当前进程：

```powershell
Set-ExecutionPolicy -Scope Process Bypass
& ".\Codex Gateway Lite.ps1"
```

脚本会按顺序完成：

1. 检查系统构建工具。
2. 检查 / 安装 Rust stable toolchain。
3. 预拉取 Rust crates；官方源不可达时自动写入 Cargo 国内 sparse 镜像。
4. 检查 / 安装 Codex App。
5. 执行 `stop-agent`，避免旧 agent 占用 lock、继续注入旧 UI 脚本或占用 `57321` 本地代理端口。
6. 执行 `init --config ~/.codex-gateway-lite/config.json`。
7. 启动 `agent` 并拉起 Codex App。

## Requirements

| 平台 | 必要项 | 脚本行为 |
| --- | --- | --- |
| macOS | Xcode Command Line Tools | 缺失时调用 `xcode-select --install`，需要用户在系统弹窗里确认安装 |
| macOS | Rust stable / Cargo | 缺失时下载 `rustup-init` 并安装 minimal stable toolchain |
| macOS | Codex App | 缺失时下载 OpenAI 官方 DMG，并安装到 `/Applications` 或 `~/Applications` |
| Windows | Git for Windows | 缺失时优先用 `winget install Git.Git` 安装 |
| Windows | Microsoft C++ Build Tools | 缺失时用 `winget` 或 `vs_BuildTools.exe` 安装 VC++ build tools |
| Windows | Rust stable / Cargo | 缺失时下载 `rustup-init.exe` 并安装 minimal stable MSVC toolchain |
| Windows | Codex App | 缺失时优先用 Microsoft Store / `winget` 安装，失败时打开官方商店页 |

不需要单独安装 Node.js、Python、pnpm、npm、SQLite 或 OpenSSL。SQLite 使用 `rusqlite` 的 bundled 构建，HTTP TLS 使用 `rustls`。

## Mirrors and network options

脚本内置的下载源如下：

| 依赖 | 官方地址 | 国内/备用地址 |
| --- | --- | --- |
| Rustup | `https://static.rust-lang.org/rustup/dist/<triple>/rustup-init[.exe]` | `https://mirrors.ustc.edu.cn/rust-static/rustup/dist/<triple>/rustup-init[.exe]` |
| Cargo crates | `https://index.crates.io/config.json` | `sparse+https://rsproxy.cn/index/` |
| macOS Codex App Apple Silicon | `https://persistent.oaistatic.com/codex-app-prod/Codex.dmg` | 无可信镜像，保持 OpenAI 官方源 |
| macOS Codex App Intel | `https://persistent.oaistatic.com/codex-app-prod/Codex-latest-x64.dmg` | 无可信镜像，保持 OpenAI 官方源 |
| Windows Codex App | `https://apps.microsoft.com/detail/9PLM9XGG6VKS` | 无可信镜像，保持 Microsoft Store 官方源 |
| Visual Studio Build Tools | `https://aka.ms/vs/17/release/vs_BuildTools.exe` | 无可信镜像，保持 Microsoft 官方源 |
| Git for Windows | `winget install Git.Git` / `https://git-scm.com/download/win` | 可由用户自行配置系统代理或软件源 |

默认网络策略：

- `CODEX_GATEWAY_LITE_USE_CN_MIRROR=auto`：先探测 Rust 官方源，失败时自动切到 USTC。
- `CODEX_GATEWAY_LITE_CARGO_MIRROR=auto`：先探测 crates.io sparse index，失败时写入 rsproxy；如果用户已有 Cargo source 配置，脚本会保留。

macOS / zsh：

```bash
export CODEX_GATEWAY_LITE_USE_CN_MIRROR=1
export CODEX_GATEWAY_LITE_CARGO_MIRROR=off
export CODEX_GATEWAY_LITE_DEBUG_PORT=9229
export CODEX_GATEWAY_LITE_APP="/Applications/Codex.app"
```

Windows / PowerShell：

```powershell
$env:CODEX_GATEWAY_LITE_USE_CN_MIRROR = "1"
$env:CODEX_GATEWAY_LITE_CARGO_MIRROR = "off"
$env:CODEX_GATEWAY_LITE_DEBUG_PORT = "9229"
$env:CODEX_GATEWAY_LITE_APP = "C:\Path\To\Codex.exe"
```

## Configuration

第一次建议直接运行双击脚本或手动执行：

```bash
cargo run --manifest-path Cargo.toml -- init
```

初始化会询问：

- provider name / id
- Base URL
- API Key 或 API Key 环境变量
- protocol：`responses` 或 `chat_completions`
- context window / context budget
- 是否开启 Task Plan hints

也可以复制 `config.example.json` 后手动修改。建议把 API Key 放进环境变量：

```bash
export CODEX_GATEWAY_API_KEY="sk-..."
```

本地真实配置默认写到：

```text
~/.codex-gateway-lite/config.json
```

该文件不会写进 Git。macOS/Linux 上会尽量设置为 `600` 权限。

### Protocol modes

| `provider.protocol` | Codex 看到的地址 | 适用场景 | 本地代理 |
| --- | --- | --- | --- |
| `responses` | 上游 Base URL；配置 `provider.contextBudget` 时改为 `http://127.0.0.1:57321/v1` | 供应商已支持 `/v1/responses` | 默认不启用，保持直连；只有显式填写硬裁剪预算时启用 |
| `chat_completions` | `http://127.0.0.1:57321/v1` | 供应商只支持 `/v1/chat/completions` | 必须启用，由 agent 做协议转换 |

如果显式填写 `provider.contextBudget`，Codex 会改走本地代理 `http://127.0.0.1:57321/v1`，由 agent 先裁剪上下文再转发到 Responses 上游。

在 `chat_completions` 模式下，Codex config 里仍保持 `wire_api = "responses"`，真实上游 Base URL 和 API Key 只保存在用户自己的 `~/.codex-gateway-lite/config.json`。

### Context windows

配置里的 `contextWindow` / `models[].contextWindow` 支持：

- `1M`
- `200K`
- `1000000`
- 其它整数 token 数

自动拉取到的模型会默认写入上下文窗口：GPT/ChatGPT 系列为 `258400`，其他模型为 `1M`。

`provider.contextBudget` 是发送上游前的本地裁剪余量，不是文件大小。Responses 模式默认留空并保持直连；只有显式填写预算时才会改走本地代理 `http://127.0.0.1:57321/v1`。例如 `contextWindow=1M` 且 `provider.contextBudget=200K` 时，代理会把发送目标控制在约 `800K`，超出时优先裁掉旧上下文，而不是把请求裁到只剩 `200K`。支持这些写法：

- `200`：按 `200K` 处理，方便交互里直接填常见预算值。注意：不带单位的纯数字只要 ≤ 512 都会按「千 token」自动放大（`500` 会被当成 `500K`）。目前无法配置几百 token 级别的极小预算；填写后请核对命令回显里的实际值。
- `200K` / `200KB`：都表示 200,000 个估算输入 token；这里的 `KB` 不是字节。
- `200000`：明确 token 数。
- `off` / `none` / `0`：关闭显式预算。`responses` 模式会回到直连，不做本地硬裁剪；`chat_completions` 仍会走本地代理，并按 `contextWindow` 自动推导预算。

## CLI commands

```text
codex-gateway-lite apply --config <config.json> [--codex-home <dir>] [--reload] [--debug-port 9229] [--no-plan-ui]
codex-gateway-lite doctor --config <config.json>
codex-gateway-lite reload [--debug-port 9229] [--no-plan-ui]
codex-gateway-lite inject-plan-ui [--debug-port 9229]
codex-gateway-lite watch --config <config.json> [--codex-home <dir>] [--debug-port 9229] [--interval-ms 1200]
codex-gateway-lite agent [--config ~/.codex-gateway-lite/config.json] [--codex-home <dir>] [--app <Codex.app|ChatGPT.app|Codex.exe|app dir>] [--debug-port 9229] [--interval-ms 1000] [--no-plan-ui]
codex-gateway-lite launch [--config <config.json>] [--codex-home <dir>] [--app <Codex.app|ChatGPT.app|Codex.exe|app dir>] [--debug-port 9229] [--no-plan-ui]
codex-gateway-lite install-agent [--config ~/.codex-gateway-lite/config.json] [--codex-home <dir>] [--app <Codex.app|ChatGPT.app|Codex.exe|app dir>] [--debug-port 9229] [--interval-ms 1000] [--no-plan-ui]
codex-gateway-lite stop-agent
codex-gateway-lite uninstall-agent
codex-gateway-lite init [--config ~/.codex-gateway-lite/config.json] [--force]
codex-gateway-lite providers [--config <config.json>]
codex-gateway-lite add-provider [--config <config.json>]
codex-gateway-lite use-provider <id> [--config <config.json>] [--codex-home <dir>] [--no-apply] [--debug-port 9229] [--no-plan-ui]
codex-gateway-lite remove-provider <id> [--config <config.json>]
codex-gateway-lite where-app [--app <Codex.app|ChatGPT.app|Codex.exe|app dir>]
```

常用命令：

```bash
# 写入 Codex 配置
cargo run --manifest-path Cargo.toml -- apply --config config.example.json

# 验证 provider models endpoint，不打印 API Key
cargo run --manifest-path Cargo.toml -- doctor --config config.example.json

# 常驻模式：启动 Codex、按需启动本地代理、监听配置并注入 UI 修正
cargo run --manifest-path Cargo.toml -- agent

# 只停止旧 agent / 登录项任务
cargo run --manifest-path Cargo.toml -- stop-agent

# 卸载登录保活
cargo run --manifest-path Cargo.toml -- uninstall-agent

# 多供应商：列出 / 新增 / 切换 / 删除
cargo run --manifest-path Cargo.toml -- providers
cargo run --manifest-path Cargo.toml -- add-provider
cargo run --manifest-path Cargo.toml -- use-provider my-gateway
cargo run --manifest-path Cargo.toml -- remove-provider old-gateway
```

## Multiple providers

配置文件支持保存多套供应商档案（各自独立的 Base URL、API Key、协议、模型列表和上下文窗口）：

- 顶层 `provider`/`model`/`models`/`contextWindow` 始终是**当前激活**的供应商；
- `providers` 数组保存未激活的档案，`use-provider <id>` 切换时先把当前激活档案回填进列表，再把目标档案提升到顶层；
- 切换后自动重新 apply Codex 配置并软刷新（`--no-apply` 可跳过，交给运行中的 agent 处理）；
- API Key 只保存在用户目录私有配置文件里，不进入 Codex config 或 Git。

## Account session sync

从官方账号登录切换到 API Key 供应商时，旧会话的 `model_provider` 还停留在 `openai`，Codex App 侧栏按当前 provider 过滤会导致这些会话消失。每次 apply（含 agent 启动、配置变更、use-provider）会自动做全量会话同步：

- 扫描 `sessions/` 与 `archived_sessions/` 下所有 rollout 文件，把每一条 `session_meta` 的 `model_provider` 改写为当前激活供应商（保留文件 mtime，不打乱侧栏排序）；
- 把所有 thread 数据库（`sqlite/*.sqlite3` 与旧版 `state_5.sqlite`）里 `threads.model_provider` 批量同步为当前供应商；
- 已同步过的文件和行不会重复改写，重复运行是无操作。

## Agent behavior

`agent` 会持续做这些事：

- 启动或重新拉起 Codex App，并带上 `--remote-debugging-port`、`CODEX_HOME` 和 macOS `--user-data-dir`。
- 仅在 `chat_completions` 或已启用 `provider.contextBudget` 时启动本地协议代理 `http://127.0.0.1:57321/v1`。
- 启动时 apply gateway/provider、`commonConfig` 公共配置和按当前 Codex schema 生成的 `model_catalog_json`。
- 从同一个 `CODEX_HOME` 的 session/state 元数据重建本地 `local_thread_catalog`，减少切换启动方式后侧栏历史为空的问题。
- 监听 `~/.codex-gateway-lite/config.json`，配置变化后重新 apply，并尝试通过 CDP 软刷新 Codex renderer。
- 周期性检查 CDP 是否可用；断开后节流重启 Codex App。
- 周期性重注入任务清单 UI 常驻修正。

需要真正后台常驻时：

```bash
cargo run --manifest-path Cargo.toml -- install-agent
```

安装后：

- macOS：`~/Library/LaunchAgents/com.codex.gateway-lite.agent.plist`
- Windows Scheduled Task：`CodexGatewayLiteAgent`
- 私有二进制副本：`~/.codex-gateway-lite/bin/codex-gateway-lite` 或 `%USERPROFILE%\.codex-gateway-lite\bin\codex-gateway-lite.exe`

双击启动脚本会在启动前自动执行 `stop-agent`，避免旧版本 agent 继续占用 `agent.lock`、继续向 Codex 注入旧 UI 脚本，或占用本地代理端口。如果已经安装登录保活，`stop-agent` 只停止当前任务，不删除登录项；彻底删除请执行 `uninstall-agent`。

## Task Plan hints

Codex App 会在请求中带上 `update_plan` 工具定义，模型调用它就能在右侧显示任务进度面板。但第三方模型默认不一定知道什么时候该调用这个工具。

推荐在 `~/.codex-gateway-lite/config.json` 中显式开启：

```json
{
  "planHints": true
}
```

开启后，生成的 model catalog 会在 `base_instructions` 中追加一段任务清单使用指引。该内容写在本地 `~/.codex/model-catalogs/<provider>.json` 明文文件中，用户可以随时查看和修改。默认关闭，需要用户主动 opt-in。

也可以在项目或用户级 `AGENTS.md` 中添加自己的规则，两种方式可以同时使用。

## Project boundaries

本工具默认使用用户主目录下的 `~/.codex`；只有显式传 `--codex-home <dir>` 时才会切到自定义 Codex home。

如需让 gateway 环境调用另一个已登录 Codex profile 的内置生图能力，参考
[`docs/auth-image-bridge.md`](docs/auth-image-bridge.md)。该流程要求独立的
`CODEX_HOME` 和独立的 Codex App user-data-dir，避免登录态、会话和生成结果互相污染。

它会写入或读取：

- `~/.codex/config.toml`
- `~/.codex/model-catalogs/*.json`
- 同一 Codex home 下的 session/state 元数据
- `~/.codex-gateway-lite/config.json`
- `~/.codex-gateway-lite/bin/*`

它不会：

- 复制或迁移 `auth.json`
- 打印 API Key
- 迁移 Electron profile
- 清理或重写历史会话正文
- 执行历史会话 sanitize 逻辑
- 把真实用户配置写入仓库

## Troubleshooting

### 旧 agent 仍在运行

先执行：

```bash
cargo run --manifest-path Cargo.toml -- stop-agent
```

如果安装过登录保活并且不再需要：

```bash
cargo run --manifest-path Cargo.toml -- uninstall-agent
```

### `127.0.0.1:57321` 是什么

这是本工具在需要时启动的本地协议代理地址：

- `chat_completions`：Responses ⇄ Chat Completions 协议转换。
- `responses + provider.contextBudget`：在转发上游前做上下文裁剪。

普通 `responses` 直连模式不会监听这个端口。

### Codex App 没有被自动识别

查看识别结果：

```bash
cargo run --manifest-path Cargo.toml -- where-app
```

或者显式指定：

```bash
cargo run --manifest-path Cargo.toml -- launch --app "/Applications/Codex.app"
```

Windows 可用：

```powershell
$env:CODEX_GATEWAY_LITE_APP = "C:\Path\To\Codex.exe"
& ".\Codex Gateway Lite.cmd"
```

## Development

```bash
cargo fmt -- --check
cargo test
cargo run --quiet -- --help
```

CI 会在 Linux / macOS / Windows 上运行格式检查、测试和 CLI help smoke test。

提交前请确认不要把以下内容加入 Git：

- `.codex-local-handoff.md`
- `target/`
- `.env*`
- `config.json`
- 真实 API Key / auth 文件 / 私有机器路径

## Contributing

欢迎提交 issue 和 pull request。请先看 [CONTRIBUTING.md](CONTRIBUTING.md)。安全问题请按 [SECURITY.md](SECURITY.md) 说明私下报告，不要直接公开贴密钥、日志或用户配置。

## License

MIT. See [LICENSE](LICENSE).
