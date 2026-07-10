#!/bin/zsh
set -euo pipefail

SCRIPT_DIR="${0:A:h}"
CONFIG_DIR="$HOME/.codex-gateway-lite"
CONFIG_FILE="$CONFIG_DIR/config.json"
DEBUG_PORT="${CODEX_GATEWAY_LITE_DEBUG_PORT:-9229}"
APP_PATH="${CODEX_GATEWAY_LITE_APP:-}"
AGENT_STARTED=0
LITE_BIN="${CODEX_GATEWAY_LITE_BIN:-$SCRIPT_DIR/target/release/codex-gateway-lite}"
RUSTUP_OFFICIAL_BASE="https://static.rust-lang.org"
RUSTUP_USTC_BASE="https://mirrors.ustc.edu.cn/rust-static"
CRATES_INDEX_URL="https://index.crates.io/config.json"
CODEX_DMG_ARM64="https://persistent.oaistatic.com/codex-app-prod/Codex.dmg"
CODEX_DMG_X64="https://persistent.oaistatic.com/codex-app-prod/Codex-latest-x64.dmg"
CODEX_DOWNLOAD_PAGE="https://developers.openai.com/codex/app"

if [[ -t 1 ]] && command -v tput >/dev/null 2>&1 && [[ "$(tput colors 2>/dev/null || echo 0)" -ge 8 ]]; then
  BOLD="$(tput bold)"; DIM="$(tput dim)"; RESET="$(tput sgr0)"
  GREEN="$(tput setaf 2)"; YELLOW="$(tput setaf 3)"; RED="$(tput setaf 1)"; BLUE="$(tput setaf 4)"
else
  BOLD=""; DIM=""; RESET=""; GREEN=""; YELLOW=""; RED=""; BLUE=""
fi

print_header() {
  clear 2>/dev/null || true
  print "${BOLD}╭────────────────────────────────────────────╮${RESET}"
  print "${BOLD}│        Codex Gateway Lite Bootstrap        │${RESET}"
  print "${BOLD}╰────────────────────────────────────────────╯${RESET}"
  print "${DIM}项目目录：$SCRIPT_DIR${RESET}"
  print "${DIM}配置文件：$CONFIG_FILE${RESET}"
  print ""
}

section() { print "\n${BLUE}${BOLD}▶ $1${RESET}"; }
ok() { print "  ${GREEN}✓${RESET} $1"; }
info() { print "  ${DIM}•${RESET} $1"; }
warn() { print "  ${YELLOW}!${RESET} $1"; }
fail() { print "\n  ${RED}✗${RESET} $1"; exit 1; }

command_exists() { command -v "$1" >/dev/null 2>&1; }

cleanup_agent_on_exit() {
  local status=$?
  trap - EXIT INT TERM HUP
  if [[ "${AGENT_STARTED:-0}" == "1" ]]; then
    print "\n${YELLOW}${BOLD}脚本退出，停止 Codex Gateway Lite agent...${RESET}"
    if [[ -x "$LITE_BIN" ]]; then
      "$LITE_BIN" stop-agent >/dev/null 2>&1 || true
    else
      (cd "$SCRIPT_DIR" && cargo run --quiet --manifest-path Cargo.toml -- stop-agent) >/dev/null 2>&1 || true
    fi
  fi
  exit "$status"
}

trap cleanup_agent_on_exit EXIT INT TERM HUP

url_ok() {
  curl -fsSL --connect-timeout 6 --max-time 10 "$1" -o /dev/null >/dev/null 2>&1
}

use_cn_mirror() {
  case "${CODEX_GATEWAY_LITE_USE_CN_MIRROR:-auto}" in
    1|true|TRUE|yes|YES|cn|CN|china|CHINA) return 0 ;;
    0|false|FALSE|no|NO|off|OFF) return 1 ;;
  esac
  url_ok "$RUSTUP_OFFICIAL_BASE/rustup/dist/channel-rust-stable.toml" && return 1
  return 0
}

rust_host_triple() {
  case "$(uname -m)" in
    arm64|aarch64) print "aarch64-apple-darwin" ;;
    x86_64|amd64) print "x86_64-apple-darwin" ;;
    *) fail "不支持的 macOS CPU 架构：$(uname -m)" ;;
  esac
}

download_file() {
  local url="$1" dest="$2" label="$3"
  info "下载 $label"
  info "$url"
  curl -fL --retry 2 --connect-timeout 15 --progress-bar -o "$dest" "$url"
}

ensure_xcode_tools() {
  if xcrun -find clang >/dev/null 2>&1; then
    ok "Xcode Command Line Tools 已可用"
    return
  fi

  warn "未检测到 Xcode Command Line Tools，开始调用系统安装器"
  xcode-select --install >/dev/null 2>&1 || true
  print ""
  warn "请在弹出的系统窗口中完成安装；完成后回到这个窗口按 Enter 继续。"
  read -r "?按 Enter 继续检测..."
  xcrun -find clang >/dev/null 2>&1 || fail "Xcode Command Line Tools 仍不可用，请安装完成后重新运行脚本。"
  ok "Xcode Command Line Tools 已安装"
}

ensure_rust() {
  if command_exists cargo && command_exists rustc; then
    ok "Rust toolchain 已可用：$(cargo --version 2>/dev/null)"
    return
  fi

  local triple base url tmp installer
  triple="$(rust_host_triple)"
  if use_cn_mirror; then
    base="$RUSTUP_USTC_BASE"
    warn "官方 Rust 源连通性较差或已强制启用国内镜像，使用 USTC Rustup 镜像"
  else
    base="$RUSTUP_OFFICIAL_BASE"
  fi
  url="$base/rustup/dist/$triple/rustup-init"
  tmp="$(mktemp -d)"
  installer="$tmp/rustup-init"
  download_file "$url" "$installer" "Rustup ($triple)"
  chmod +x "$installer"
  RUSTUP_DIST_SERVER="$base" RUSTUP_UPDATE_ROOT="$base/rustup" "$installer" -y --default-toolchain stable --profile minimal
  export PATH="$HOME/.cargo/bin:$PATH"
  command_exists cargo || fail "Rust 安装后仍找不到 cargo，请检查 ~/.cargo/bin 是否在 PATH 中。"
  ok "Rust toolchain 安装完成：$(cargo --version)"
}

cargo_config_path() {
  print "${CARGO_HOME:-$HOME/.cargo}/config.toml"
}

cargo_config_has_source_override() {
  local cfg; cfg="$(cargo_config_path)"
  [[ -f "$cfg" ]] || return 1
  grep -Eq '^\[source\.crates-io\]|replace-with\s*=' "$cfg" 2>/dev/null
}

configure_cargo_mirror() {
  local force="${1:-auto}" cfg dir
  cfg="$(cargo_config_path)"
  dir="${cfg:h}"
  mkdir -p "$dir"
  if cargo_config_has_source_override; then
    ok "Cargo registry 配置已存在，保持用户现有设置"
    return
  fi
  if [[ "$force" != "force" ]]; then
    case "${CODEX_GATEWAY_LITE_CARGO_MIRROR:-auto}" in
      0|false|FALSE|no|NO|off|OFF) info "已按环境变量跳过 Cargo 国内镜像配置"; return ;;
      1|true|TRUE|yes|YES|cn|CN|china|CHINA) ;;
      *) url_ok "$CRATES_INDEX_URL" && { ok "crates.io sparse index 可访问"; return; } ;;
    esac
  fi
  warn "为 Cargo 配置 rsproxy 国内 sparse 镜像：$cfg"
  {
    print ""
    print "# Added by Codex Gateway Lite bootstrap"
    print "[source.crates-io]"
    print 'replace-with = "rsproxy-sparse"'
    print ""
    print "[source.rsproxy-sparse]"
    print 'registry = "sparse+https://rsproxy.cn/index/"'
  } >> "$cfg"
}

ensure_cargo_deps() {
  configure_cargo_mirror auto
  info "预拉取 Rust 依赖（cargo fetch）"
  if cargo fetch --manifest-path Cargo.toml; then
    ok "Rust 依赖已就绪"
    return
  fi
  warn "首次 cargo fetch 失败，尝试启用 Cargo 国内镜像后重试"
  configure_cargo_mirror force
  cargo fetch --manifest-path Cargo.toml || fail "Rust 依赖拉取失败。可设置代理，或检查 ~/.cargo/config.toml 镜像配置。"
  ok "Rust 依赖已就绪"
}

# 新版官方 app 复用 ChatGPT.app 这个名字（bundle id 仍是 com.openai.codex）；
# 纯聊天版 ChatGPT（com.openai.chat）不能当 Codex App。用 bundle id 或内嵌的
# Codex Framework 佐证，与 Rust 侧 resolve_codex_app_dir 的判定保持一致。
chatgpt_bundle_is_codex() {
  local app_dir="$1" bundle_id
  [[ -d "$app_dir" ]] || return 1
  bundle_id="$(defaults read "$app_dir/Contents/Info.plist" CFBundleIdentifier 2>/dev/null || true)"
  [[ "$bundle_id" == "com.openai.codex" || "$bundle_id" == com.openai.codex.* ]] && return 0
  [[ -d "$app_dir/Contents/Frameworks/Codex Framework.framework" ]]
}

codex_app_exists() {
  [[ -n "$APP_PATH" && -e "$APP_PATH" ]] && return 0
  [[ -d "/Applications/Codex.app" ]] && return 0
  [[ -d "$HOME/Applications/Codex.app" ]] && return 0
  [[ -d "/Applications/OpenAI Codex.app" ]] && return 0
  [[ -d "$HOME/Applications/OpenAI Codex.app" ]] && return 0
  chatgpt_bundle_is_codex "/Applications/ChatGPT.app" && return 0
  chatgpt_bundle_is_codex "$HOME/Applications/ChatGPT.app" && return 0
  return 1
}

install_codex_app() {
  local arch url tmp dmg mount app target_parent target_app
  arch="$(uname -m)"
  if [[ "$arch" == "arm64" || "$arch" == "aarch64" ]]; then
    url="$CODEX_DMG_ARM64"
  else
    url="$CODEX_DMG_X64"
  fi
  warn "未检测到 Codex App，开始从 OpenAI 官方地址下载。"
  info "如下载失败，可手动打开：$CODEX_DOWNLOAD_PAGE"
  tmp="$(mktemp -d)"
  dmg="$tmp/Codex.dmg"
  mount="$tmp/mount"
  mkdir -p "$mount"
  download_file "$url" "$dmg" "Codex App DMG"
  hdiutil attach "$dmg" -nobrowse -readonly -mountpoint "$mount" >/dev/null
  app="$(find "$mount" -maxdepth 2 \( -name 'Codex.app' -o -name 'ChatGPT.app' \) -type d -print -quit)"
  [[ -n "$app" ]] || { hdiutil detach "$mount" >/dev/null || true; fail "DMG 中未找到 Codex.app / ChatGPT.app。"; }
  if [[ -w "/Applications" ]]; then
    target_parent="/Applications"
  else
    target_parent="$HOME/Applications"
    mkdir -p "$target_parent"
  fi
  target_app="$target_parent/$(basename "$app")"
  [[ -e "$target_app" ]] && fail "$target_app 已存在但自动识别失败，请手动检查后重试。"
  ditto "$app" "$target_app"
  hdiutil detach "$mount" >/dev/null || true
  ok "Codex App 已安装到：$target_app"
}

ensure_codex_app() {
  if codex_app_exists; then
    ok "Codex App 已可用"
    return
  fi
  install_codex_app
  codex_app_exists || fail "Codex App 安装后仍不可用，请手动安装：$CODEX_DOWNLOAD_PAGE"
}

lite_binary_stale() {
  [[ ! -x "$LITE_BIN" ]] && return 0
  [[ "$SCRIPT_DIR/Cargo.toml" -nt "$LITE_BIN" ]] && return 0
  [[ "$SCRIPT_DIR/Cargo.lock" -nt "$LITE_BIN" ]] && return 0
  local newer_source
  newer_source="$(find "$SCRIPT_DIR/src" -type f -name '*.rs' -newer "$LITE_BIN" -print -quit 2>/dev/null || true)"
  [[ -n "$newer_source" ]]
}

ensure_lite_binary() {
  if lite_binary_stale; then
    info "构建 release 二进制（后续源码未变化会直接复用）"
    cargo build --quiet --release --manifest-path Cargo.toml
    ok "release 二进制已就绪：$LITE_BIN"
  fi
}

run_lite() {
  ensure_lite_binary
  "$LITE_BIN" "$@"
}

stop_stale_agent_processes() {
  # `run_lite stop-agent` (below) also asks the Rust binary itself to sweep
  # and kill any other codex-gateway-lite agent process system-wide, but
  # that only runs *after* `ensure_lite_binary` has already built/located a
  # binary. This is a script-level safety net that runs first, independent
  # of whether the binary exists or builds cleanly yet, so a stray agent
  # left running from an earlier session/manual invocation (e.g. the
  # terminal window got closed without the exit trap running, or the agent
  # was started outside this script) always gets cleared before this run
  # continues — otherwise it keeps serving stale code on every relaunch.
  local pattern="${LITE_BIN} agent"
  if ! pgrep -f -- "$pattern" >/dev/null 2>&1; then
    return 0
  fi
  warn "检测到残留的 codex-gateway-lite agent 进程，先停止再继续"
  pkill -TERM -f -- "$pattern" >/dev/null 2>&1 || true
  local waited=0
  while (( waited < 10 )) && pgrep -f -- "$pattern" >/dev/null 2>&1; do
    sleep 0.5
    waited=$((waited + 1))
  done
  if pgrep -f -- "$pattern" >/dev/null 2>&1; then
    pkill -KILL -f -- "$pattern" >/dev/null 2>&1 || true
  fi
}

main() {
  print_header
  cd "$SCRIPT_DIR"
  stop_stale_agent_processes

  section "1/3 环境检测与依赖准备"
  ensure_xcode_tools
  ensure_rust
  ensure_cargo_deps
  ensure_codex_app

  section "2/3 初始化 Codex Gateway Lite 配置"
  mkdir -p "$CONFIG_DIR"
  run_lite stop-agent
  run_lite init --config "$CONFIG_FILE"

  section "3/3 启动 agent 并拉起 Codex App"
  info "常用配置速查（改完无需重跑 init，agent 运行中会自动生效）："
  info "  set-context-budget <200K|off>  单独调整上下文裁剪余量"
  info "  set-aggregate <on|off>  单独开关多供应商聚合模式"
  info "  add-provider  新增供应商，可选 modelFilter 限定它贡献的模型前缀"
  info "  edit-provider <id>  编辑已保存供应商的连接信息/协议/modelFilter/裁剪余量"
  info "  use-provider <id>  切换当前激活的供应商"

  AGENT_STARTED=1
  if [[ -n "$APP_PATH" ]]; then
    run_lite agent --config "$CONFIG_FILE" --app "$APP_PATH" --debug-port "$DEBUG_PORT"
  else
    run_lite agent --config "$CONFIG_FILE" --debug-port "$DEBUG_PORT"
  fi

  print "\n${GREEN}${BOLD}Codex Gateway Lite agent 已退出。${RESET}"
}

main "$@"
