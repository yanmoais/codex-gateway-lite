#!/bin/zsh
set -euo pipefail

# ---------------------------------------------------------------------
# Three run modes, dispatched at the very bottom of this file:
#
#   1. Default (no flags, `CGL_BACKGROUND_CHILD` unset) -> `main_background_launch`.
#      Double-clicking this .command file in Finder (or running it bare
#      from a terminal with no arguments) lands here. Does a log rotation
#      check + writes a startup banner into `agent.terminal.log`, then
#      re-execs *itself* as a fully detached background process
#      (`CGL_BACKGROUND_CHILD=1 nohup "$SCRIPT_PATH" >> "$AGENT_LOG" 2>&1 &`),
#      prints a few short status lines, and `exit 0`s — the Terminal
#      window Finder opened closes itself almost immediately instead of
#      staying pinned to a long-running process.
#
#   2. Background child (`CGL_BACKGROUND_CHILD=1` in the environment —
#      always set by mode 1's own re-exec above; not meant to be set by
#      hand) -> `main_background_child`. Does the actual work: dependency
#      checks, config init, then runs `run_lite agent ...` directly in
#      this process's own foreground. No `tee` (stdout/stderr already land
#      in `agent.terminal.log`, via mode 1's `>> ... 2>&1` redirect) and no
#      boxed terminal header/`clear`, but the same `section`/`ok`/`info`/
#      `warn` textual status lines as every other mode still print — they
#      just land in the log file instead of on a terminal.
#
#   3. `--foreground` (explicit flag) -> `main_foreground`. Fully preserves
#      the original interactive behavior this script used to have
#      unconditionally: boxed header, `tee -a` to both the terminal and
#      `agent.terminal.log`, occupies the invoking terminal until the
#      agent exits. Useful for debugging or watching live output without
#      opening the log file separately.
#
# The Web UI's one-click restart (`POST /api/ui/restart`, see
# `handle_ui_restart_post` in src/main.rs) re-launches this script with
# neither flag nor `CGL_BACKGROUND_CHILD` set, so a restart always
# re-enters through mode 1 -> mode 2. Mode 2's own pre-launch
# `stop_stale_agent_processes` sweep is what actually kills the *previous*
# agent process — this script never makes a running instance kill itself;
# the new instance kills the old one on its way up.
#
# NOTE: `Codex Gateway Lite.ps1` (the Windows launcher) intentionally still
# has only one interactive mode; it is not part of this redesign.
# ---------------------------------------------------------------------

SCRIPT_DIR="${0:A:h}"
SCRIPT_PATH="${0:A}"
CONFIG_DIR="$HOME/.codex-gateway-lite"
CONFIG_FILE="$CONFIG_DIR/config.json"
AGENT_LOG="$CONFIG_DIR/agent.terminal.log"
DEBUG_PORT="${CODEX_GATEWAY_LITE_DEBUG_PORT:-9229}"
# Mirrors `protocol_proxy::DEFAULT_PROTOCOL_PROXY_PORT` in src/protocol_proxy.rs
# (both hardcode 57321; keep them in sync if either ever changes) — the port
# the local protocol proxy / Web UI binds. Centralized here so
# `wait_for_old_instance_handover`'s port-release wait and the status
# message below share one source of truth instead of two hardcoded copies.
PROXY_PORT="${CODEX_GATEWAY_LITE_PROXY_PORT:-57321}"
APP_PATH="${CODEX_GATEWAY_LITE_APP:-}"
AGENT_STARTED=0
LITE_BIN="${CODEX_GATEWAY_LITE_BIN:-$SCRIPT_DIR/target/release/codex-gateway-lite}"
RUSTUP_OFFICIAL_BASE="https://static.rust-lang.org"
RUSTUP_USTC_BASE="https://mirrors.ustc.edu.cn/rust-static"
CRATES_INDEX_URL="https://index.crates.io/config.json"
CODEX_DMG_ARM64="https://persistent.oaistatic.com/codex-app-prod/Codex.dmg"
CODEX_DMG_X64="https://persistent.oaistatic.com/codex-app-prod/Codex-latest-x64.dmg"
CODEX_DOWNLOAD_PAGE="https://developers.openai.com/codex/app"

FOREGROUND=0
for arg in "$@"; do
  case "$arg" in
    --foreground) FOREGROUND=1 ;;
  esac
done

# `[[ -t 1 ]]` (stdout is a real TTY) is false whenever this script's own
# stdout has been redirected to a file or pipe — which is exactly what both
# the background-child mode and mode 1's `nohup ... >> "$AGENT_LOG"`
# redirect do. So this check already does the right thing with no extra
# handling: every mode re-evaluates it fresh (a brand new process image,
# never cached across the mode 1 -> mode 2 re-exec), and it naturally comes
# out false — no colors, no stray ANSI escapes written into the log file —
# whenever stdout isn't an actual terminal.
if [[ -t 1 ]] && command -v tput >/dev/null 2>&1 && [[ "$(tput colors 2>/dev/null || echo 0)" -ge 8 ]]; then
  BOLD="$(tput bold)"; DIM="$(tput dim)"; RESET="$(tput sgr0)"
  GREEN="$(tput setaf 2)"; YELLOW="$(tput setaf 3)"; RED="$(tput setaf 1)"; BLUE="$(tput setaf 4)"
  CYAN="$(tput setaf 6)"
else
  BOLD=""; DIM=""; RESET=""; GREEN=""; YELLOW=""; RED=""; BLUE=""; CYAN=""
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

# Confirmation banner for mode 1 (background launch) — the only output most
# users ever see, since Finder's Terminal window closes moments after this
# prints. Deliberately not a bordered box like `print_header`: every value
# here (URL, log path, this script's own absolute path) is variable-length
# and can run long, and a fixed-width box drawn around it would either
# overflow or need real terminal-width-aware wrapping to still look right.
# A plain label/value list sidesteps that entirely. The three Chinese labels
# below are deliberately kept at 4 characters each (8 terminal columns, since
# CJK glyphs render double-width) so they line up without any padding math.
print_background_launch_summary() {
  local url="http://127.0.0.1:$PROXY_PORT/ui"
  print ""
  print "  ${GREEN}${BOLD}✓  Codex Gateway Lite 已在后台启动${RESET}"
  print ""
  print "  ${DIM}配置页面${RESET}  ${BOLD}${CYAN}${url}${RESET}"
  print "  ${DIM}日志文件${RESET}  ${AGENT_LOG}"
  print "  ${DIM}终端调试${RESET}  \"${SCRIPT_PATH}\" --foreground"
  print ""
}

section() { print "\n${BLUE}${BOLD}▶ $1${RESET}"; }
ok() { print "  ${GREEN}✓${RESET} $1"; }
info() { print "  ${DIM}•${RESET} $1"; }
warn() { print "  ${YELLOW}!${RESET} $1"; }
fail() { print "\n  ${RED}✗${RESET} $1"; exit 1; }

command_exists() { command -v "$1" >/dev/null 2>&1; }

cleanup_agent_on_exit() {
  # zsh 里 status 是只读特殊变量（$? 的别名），不能用作 local 变量名，
  # 否则 trap 第一行就报错、后面的 stop-agent 根本不会执行。
  local exit_code=$?
  trap - EXIT INT TERM HUP
  if [[ "${AGENT_STARTED:-0}" == "1" ]]; then
    print "\n${YELLOW}${BOLD}脚本退出，停止 Codex Gateway Lite agent 并还原直连上游...${RESET}"
    # 还原直连的输出保留在终端上，让用户看到 Codex 配置已经指回上游。
    if [[ -x "$LITE_BIN" ]]; then
      "$LITE_BIN" stop-agent --debug-port "$DEBUG_PORT" 2>&1 || true
    else
      (cd "$SCRIPT_DIR" && cargo run --quiet --manifest-path Cargo.toml -- stop-agent --debug-port "$DEBUG_PORT") 2>&1 || true
    fi
  fi
  exit "$exit_code"
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
  elif [[ "${LITE_BIN_FRESH_REPORTED:-0}" != "1" ]]; then
    # 静默跳过构建时给一行可见反馈，避免误以为“没有自动构建”。
    ok "release 二进制已是最新（构建于 $(stat -f %Sm -t '%m-%d %H:%M' "$LITE_BIN" 2>/dev/null || echo '未知时间')），跳过重新构建"
    LITE_BIN_FRESH_REPORTED=1
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

# Poll (checking immediately, then every 0.5s) until no process matches
# `pattern`, or until `max_iters` half-second steps have passed — the same
# check-then-sleep idiom `stop_stale_agent_processes` above already uses.
# Returns 0 once the pattern is gone, 1 on timeout.
#
# IMPORTANT: like every other non-trivial-exit-status helper in this
# script, only ever call this from an `if`/`while` (or `!`/`&&`/`||`) slot.
# Under `set -e`, a bare call that happens to return 1 — which is the
# *common* case here, since "pattern already gone" is the normal outcome —
# would abort the whole script.
wait_until_pattern_gone() {
  local pattern="$1" max_iters="$2" waited=0
  while (( waited < max_iters )) && pgrep -f -- "$pattern" >/dev/null 2>&1; do
    sleep 0.5
    waited=$((waited + 1))
  done
  ! pgrep -f -- "$pattern" >/dev/null 2>&1
}

# Print (one per line) the pids matching `pattern` other than this process
# (`$$`) and its immediate parent (`$PPID`). The parent exclusion matters
# only for the default background-launch mode (mode 1 in the top-of-file
# comment): the background child's own parent is briefly mode 1 of *this
# very* launch — it re-execs itself into the background and exits within
# moments — and mode 1's command line matches this same script's own
# pattern. Without excluding it, this process would wait on (and
# potentially TERM/KILL) its own harmless, already-exiting parent instead
# of an actual leftover instance from an *earlier* restart cycle, which has
# unrelated pids and is what this check actually needs to wait out.
other_instance_pids() {
  local pattern="$1" pids_output pid
  # `|| true` keeps a no-match pgrep (exit 1) from tripping `set -e` here —
  # this is a plain assignment, not an if/while test slot.
  pids_output="$(pgrep -f -- "$pattern" 2>/dev/null || true)"
  [[ -z "$pids_output" ]] && return 0
  for pid in ${(f)pids_output}; do
    [[ "$pid" == "$$" || "$pid" == "$PPID" ]] && continue
    print -- "$pid"
  done
}

other_instances_running() {
  [[ -n "$(other_instance_pids "$1")" ]]
}

wait_until_other_instances_gone() {
  local pattern="$1" max_iters="$2" waited=0
  while (( waited < max_iters )) && other_instances_running "$pattern"; do
    sleep 0.5
    waited=$((waited + 1))
  done
  ! other_instances_running "$pattern"
}

# Send `signal` (TERM or KILL) to every pid `other_instance_pids` reports
# for `pattern`. Best-effort: a pid that's already gone by the time `kill`
# actually runs is not an error.
signal_other_instances() {
  local pattern="$1" signal="$2" pid pids_output
  pids_output="$(other_instance_pids "$pattern" || true)"
  [[ -z "$pids_output" ]] && return 0
  for pid in ${(f)pids_output}; do
    kill "-$signal" "$pid" >/dev/null 2>&1 || true
  done
}

port_in_use() {
  lsof -iTCP:"$1" -sTCP:LISTEN >/dev/null 2>&1
}

# ---------------------------------------------------------------------
# The Web UI's one-click restart (`handle_ui_restart_post` in src/main.rs)
# starts a brand new script instance while the old one is still fully
# alive, which opens a real handoff race:
#
#   - The old instance's `cleanup_agent_on_exit` trap only fires *after*
#     its agent process actually dies, and that trap runs `stop-agent`,
#     which points Codex back at the direct upstream. If that runs *after*
#     the new instance's own `run_lite init` (which points Codex back at
#     the local proxy), Codex's config briefly flips to direct and back
#     again — no visible error, just a possible one-off failed request
#     landing in the gap.
#   - The old agent only releases 127.0.0.1:$PROXY_PORT when it actually
#     exits. If the new agent tries to bind that port before the release
#     completes, it fails outright — this is not theoretical, it is exactly
#     what produced the "绑定本地协议代理端口失败：127.0.0.1:57321" entries
#     seen for real in agent.terminal.log.
#
# This function runs right after `stop_stale_agent_processes` (which
# already TERM/KILL's any stale agent process) and before anything else in
# `run_agent_and_supporting_steps`, so it applies on the one code path
# every mode that actually launches the agent (`main_foreground` and
# `main_background_child`) shares. Mode 1 never calls
# `run_agent_and_supporting_steps` itself, but every mode 1 run re-execs
# into mode 2, which does — so all three modes are covered.
#
# Four sequential checks, each independently timeout-bounded — never an
# unconditional wait:
#   1. old agent process gone                        (up to  5s)
#   2. old script shell gone, exit trap included      (up to 15s, then
#      TERM + up to 5s more, then KILL as a last resort)
#   3. old trap's own `stop-agent` cleanup child gone  (up to 10s)
#   4. proxy port actually free                       (up to 15s)
# Worst case across all four is bounded to ~50s total; the ordinary case
# (no stale instance at all) falls through in well under a second, since
# every wait loop checks *before* ever sleeping.
#
# The KILL fallback in step 2 skips the old trap's direct-upstream restore
# entirely. That is an accepted, deliberate trade-off, not an oversight:
# this same function's caller reaches `run_lite init` moments later and
# rewrites Codex's config back to the proxy anyway, so the restore that got
# skipped would have been immediately undone regardless.
# ---------------------------------------------------------------------
wait_for_old_instance_handover() {
  local agent_pattern="${LITE_BIN} agent"
  local stop_agent_pattern="${LITE_BIN} stop-agent"
  local script_pattern="$SCRIPT_PATH"

  section "交接旧实例"

  info "1/4 确认旧 agent 进程已退出"
  if wait_until_pattern_gone "$agent_pattern" 10; then
    ok "旧 agent 进程已确认退出"
  else
    warn "旧 agent 进程 5 秒内仍未退出，继续往下走（后续步骤可能报端口冲突）"
  fi

  info "2/4 等待旧脚本 shell 退出（含它退出时的直连还原 trap 跑完）"
  if wait_until_other_instances_gone "$script_pattern" 30; then
    ok "旧脚本 shell 已退出"
  else
    warn "旧脚本 shell 15 秒内未自行退出，发送 TERM 后再等 5 秒"
    signal_other_instances "$script_pattern" TERM
    if wait_until_other_instances_gone "$script_pattern" 10; then
      ok "旧脚本 shell 已在 TERM 后退出"
    else
      warn "旧脚本 shell 仍未退出，强制 KILL（会跳过它退出时的直连还原逻辑——反正新实例马上会用 run_lite init 重写配置，不影响最终配置状态）"
      signal_other_instances "$script_pattern" KILL
    fi
  fi

  info "3/4 等待旧 trap 里的 stop-agent 清理子进程结束"
  if wait_until_pattern_gone "$stop_agent_pattern" 20; then
    ok "旧 stop-agent 清理子进程已结束（或本来就没有）"
  else
    warn "旧 stop-agent 清理子进程 10 秒内未结束，继续往下走"
  fi

  info "4/4 等待本地协议代理端口 $PROXY_PORT 释放"
  if ! command_exists lsof; then
    warn "系统没有 lsof 命令，跳过端口占用检测（真冲突时 agent 自己会报绑定错误）"
  else
    local waited=0
    while (( waited < 30 )) && port_in_use "$PROXY_PORT"; do
      sleep 0.5
      waited=$((waited + 1))
    done
    if port_in_use "$PROXY_PORT"; then
      warn "端口 $PROXY_PORT 仍被占用，继续启动（agent 自身会报清晰的绑定错误）"
    else
      ok "端口 $PROXY_PORT 已释放"
    fi
  fi

  info "旧实例已完全退出，开始拉起新服务"
}

# Roll `agent.terminal.log` to `.1` once it crosses 5MB (simple
# two-generation rotation, no logrotate dependency) and stamp a fresh
# "agent starting" banner line, so anyone tailing the file (a human, or the
# Web UI's `/api/ui/logs` viewer) can always find where the *current* run's
# output begins. Called exactly once per real launch: from
# `main_foreground` right before its own `tee`'d run, and from
# `main_background_launch` right before it hands off to the background
# child — never from `main_background_child` itself, so one restart cycle
# never stamps two banners for a single actual agent startup.
rotate_and_stamp_agent_log() {
  if [[ -f "$AGENT_LOG" ]] && (( $(stat -f %z "$AGENT_LOG" 2>/dev/null || echo 0) > 5242880 )); then
    mv -f "$AGENT_LOG" "$AGENT_LOG.1"
  fi
  print "" >> "$AGENT_LOG"
  print "===== $(date '+%Y-%m-%d %H:%M:%S') Codex Gateway Lite agent 启动 =====" >> "$AGENT_LOG"
}

# Shared by every mode that actually launches the agent (`main_foreground`,
# `main_background_child`): environment/dependency checks, local config
# init, and the printed config-command cheat sheet. Deliberately does *not*
# touch `AGENT_STARTED`, log rotation, or the final `run_lite agent`
# invocation itself — each caller handles those its own way (piped through
# `tee` or not; behind a boxed terminal header or not).
run_agent_and_supporting_steps() {
  cd "$SCRIPT_DIR"
  stop_stale_agent_processes
  wait_for_old_instance_handover

  section "1/3 环境检测与依赖准备"
  ensure_xcode_tools
  ensure_rust
  ensure_cargo_deps
  ensure_codex_app

  section "2/3 初始化 Codex Gateway Lite 配置"
  mkdir -p "$CONFIG_DIR"
  # 启动前只清残留进程，不做直连还原——马上就要重新 apply 代理配置了。
  run_lite stop-agent --no-restore
  run_lite init --config "$CONFIG_FILE"

  section "3/3 启动 agent 并拉起 Codex App"
  info "常用配置速查（改完无需重跑 init，agent 运行中会自动生效）："
  info "  set-context-budget <200K|off>  单独调整上下文裁剪余量"
  info "  set-aggregate <on|off>  单独开关多供应商聚合模式"
  info "  set-plan-hints <on|off>  单独开关第三方模型的任务清单指引"
  info "  add-provider  新增供应商，可选 modelFilter 限定它贡献的模型前缀"
  info "  edit-provider <id>  编辑已保存供应商的连接信息/协议/modelFilter/裁剪余量"
  info "  use-provider <id>  切换当前激活的供应商"
}

# Mode 3 (`--foreground`): unchanged from this script's original,
# always-interactive behavior — boxed header, `tee -a` duplicating output
# to both this terminal and `agent.terminal.log`, occupies the terminal
# until the agent exits.
main_foreground() {
  print_header
  run_agent_and_supporting_steps

  AGENT_STARTED=1
  rotate_and_stamp_agent_log
  if [[ -n "$APP_PATH" ]]; then
    run_lite agent --config "$CONFIG_FILE" --app "$APP_PATH" --debug-port "$DEBUG_PORT" 2>&1 | tee -a "$AGENT_LOG"
  else
    run_lite agent --config "$CONFIG_FILE" --debug-port "$DEBUG_PORT" 2>&1 | tee -a "$AGENT_LOG"
  fi

  print "\n${GREEN}${BOLD}Codex Gateway Lite agent 已退出。${RESET}"
}

# Mode 2 (background child, `CGL_BACKGROUND_CHILD=1`): the real work, with
# no `tee` (stdout/stderr already land in `agent.terminal.log` via mode 1's
# redirect) and no `print_header`/`clear` (nothing is watching an
# interactive terminal), but the same textual `section`/`ok`/`info`
# progress output as every other mode.
main_background_child() {
  run_agent_and_supporting_steps

  AGENT_STARTED=1
  if [[ -n "$APP_PATH" ]]; then
    run_lite agent --config "$CONFIG_FILE" --app "$APP_PATH" --debug-port "$DEBUG_PORT"
  else
    run_lite agent --config "$CONFIG_FILE" --debug-port "$DEBUG_PORT"
  fi

  print "\n${GREEN}${BOLD}Codex Gateway Lite agent 已退出。${RESET}"
}

# Mode 1 (default background launch): the only mode Finder's double-click
# and a bare `./Codex Gateway Lite.command` ever reach. Intentionally tiny
# — no dependency checks here, those belong to mode 2 alone — just a log
# rotation/banner pass, a detached self-relaunch, and a short status
# message before this process exits.
main_background_launch() {
  rotate_and_stamp_agent_log
  CGL_BACKGROUND_CHILD=1 nohup "$SCRIPT_PATH" >> "$AGENT_LOG" 2>&1 &
  disown 2>/dev/null || true

  print_background_launch_summary
}

if [[ "${CGL_BACKGROUND_CHILD:-0}" == "1" ]]; then
  main_background_child
elif [[ "$FOREGROUND" == "1" ]]; then
  main_foreground
else
  main_background_launch
fi
