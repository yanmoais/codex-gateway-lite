use std::collections::{HashMap, HashSet};
use std::ffi::OsStr;
use std::fs;
use std::path::{Path, PathBuf};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use anyhow::{Context, bail};
use futures_util::{SinkExt, StreamExt};
use serde::Deserialize;
use serde_json::{Value, json};
use tokio_tungstenite::connect_async;
use tokio_tungstenite::tungstenite::Message;

const CDP_HTTP_TIMEOUT: Duration = Duration::from_secs(3);
const CDP_CONNECT_TIMEOUT: Duration = Duration::from_secs(5);
const CDP_COMMAND_TIMEOUT: Duration = Duration::from_secs(5);
const CODEX_PACKAGE_IDENTITIES: &[&str] = &["OpenAI.Codex", "OpenAI.CodexBeta"];
const DEFAULT_BASE_INSTRUCTIONS: &str = "You are Codex, a coding agent. Follow the system, developer, and user instructions supplied by the host application. Use tools carefully and report verified results.";

const PLAN_HINTS_SUPPLEMENT: &str = "\n\nWhen working on multi-step tasks, use the update_plan tool to create and maintain a visible progress checklist. Create the plan early when you start a task. Keep at most one step in_progress at a time and update statuses as you complete them. Do not wait until the end to update all steps at once.";

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RelayProfile {
    pub id: String,
    pub name: String,
    pub model: String,
    pub base_url: String,
    pub api_key: String,
    pub context_window: String,
    pub auto_compact_limit: String,
    pub model_list: String,
    pub model_windows: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RelayApplyResult {
    pub backup_path: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ModelCatalogEntry {
    pub slug: String,
    pub display_name: String,
    pub suffix_window: Option<u64>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq)]
pub struct CdpTarget {
    pub id: String,
    #[serde(rename = "type")]
    pub target_type: String,
    #[serde(default)]
    pub title: String,
    #[serde(default)]
    pub url: String,
    #[serde(default, rename = "webSocketDebuggerUrl")]
    pub web_socket_debugger_url: Option<String>,
}

pub fn apply_relay_config_file_to_home(
    home: &Path,
    config_contents: &str,
) -> anyhow::Result<RelayApplyResult> {
    let config_contents = config_contents
        .strip_prefix('\u{feff}')
        .unwrap_or(config_contents);
    if config_contents.trim().is_empty() {
        bail!("config.toml 内容不能为空");
    }

    fs::create_dir_all(home)
        .with_context(|| format!("创建 Codex home 失败：{}", home.display()))?;
    let config_path = home.join("config.toml");
    let backup_path = backup_existing_config(home, &config_path)?;
    let temp_path = home.join(format!(
        ".config.toml.codex-gateway-lite-{}.tmp",
        unix_timestamp()
    ));
    fs::write(&temp_path, config_contents)
        .with_context(|| format!("写入临时 config 失败：{}", temp_path.display()))?;
    fs::rename(&temp_path, &config_path).with_context(|| {
        format!(
            "替换 Codex config 失败：{} -> {}",
            temp_path.display(),
            config_path.display()
        )
    })?;

    Ok(RelayApplyResult { backup_path })
}

fn backup_existing_config(home: &Path, config_path: &Path) -> anyhow::Result<Option<String>> {
    if !config_path.exists() {
        return Ok(None);
    }
    let backup_dir = home.join("backups").join("codex-gateway-lite");
    fs::create_dir_all(&backup_dir)
        .with_context(|| format!("创建 config 备份目录失败：{}", backup_dir.display()))?;
    let backup_path = backup_dir.join(format!("config-{}.toml", unix_timestamp()));
    fs::copy(config_path, &backup_path).with_context(|| {
        format!(
            "备份 Codex config 失败：{} -> {}",
            config_path.display(),
            backup_path.display()
        )
    })?;
    Ok(Some(backup_path.to_string_lossy().to_string()))
}

fn unix_timestamp() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

pub fn parse_model_suffix(raw: &str) -> (String, Option<u64>) {
    let raw = raw.trim();
    if let Some(close) = raw.rfind(']') {
        if close == raw.len() - 1 {
            if let Some(open) = raw[..close].rfind('[') {
                let inner = raw[open + 1..close].trim();
                let slug = raw[..open].trim();
                if !slug.is_empty() {
                    if let Some(window) = parse_window_token(inner) {
                        return (slug.to_string(), Some(window));
                    }
                }
            }
        }
    }
    (raw.to_string(), None)
}

/// Preferred model-picker ordering: newest flagship families first (user
/// preference), matched by prefix against the lowercased slug. Models not
/// listed keep their upstream relative order after all listed ones, which
/// naturally sinks embedding/image models to the bottom. Add new flagship
/// generations to the head of this list as they ship.
const MODEL_PICKER_PRIORITY: &[&str] = &[
    "gpt-5.6",
    "claude-fable-5",
    "claude-sonnet-5",
    "grok-4.5",
    "grok-4.3",
    "claude-opus-4-8",
    "claude-opus-4-7",
    "claude-opus-4-6",
];

fn model_picker_rank(slug: &str) -> usize {
    let slug = slug
        .trim()
        .rsplit('/')
        .next()
        .unwrap_or(slug)
        .to_ascii_lowercase();
    MODEL_PICKER_PRIORITY
        .iter()
        .position(|prefix| slug.starts_with(prefix))
        .unwrap_or(MODEL_PICKER_PRIORITY.len())
}

pub fn collect_catalog_entries(
    model_list: &str,
    model_windows: &HashMap<String, String>,
    current_model: &str,
) -> Vec<ModelCatalogEntry> {
    let mut seen = HashSet::new();
    let mut list_entries = Vec::new();
    for raw in model_list
        .split(['\r', '\n', ','])
        .map(str::trim)
        .filter(|value| !value.is_empty())
    {
        let (slug, _) = parse_model_suffix(raw);
        if slug.is_empty() || !seen.insert(slug.clone()) {
            continue;
        }
        let suffix_window = model_windows
            .get(&slug)
            .and_then(|token| parse_window_token(token));
        list_entries.push(ModelCatalogEntry {
            display_name: slug.clone(),
            slug,
            suffix_window,
        });
    }

    let mut entries = Vec::new();
    let current_model = current_model.trim();
    if !current_model.is_empty() {
        let (slug, _) = parse_model_suffix(current_model);
        if !slug.is_empty() {
            let suffix_window = model_windows
                .get(&slug)
                .and_then(|token| parse_window_token(token));
            entries.push(ModelCatalogEntry {
                display_name: slug.clone(),
                slug: slug.clone(),
                suffix_window,
            });
            list_entries.retain(|entry| entry.slug != slug);
        }
    }

    list_entries.sort_by_key(|entry| model_picker_rank(&entry.slug));
    entries.append(&mut list_entries);
    entries
}

pub fn build_model_catalog_json(
    entries: &[ModelCatalogEntry],
    fallback_window: Option<u64>,
    plan_hints: bool,
    // 本地代理实际会发送的 token 上限，按模型 slug 区分（见 main.rs 的
    // `context_budget_cap_for_model`）。这是代理硬裁前允许通过的量，跟
    // Codex 自己算出来的 auto_compact 阈值是两套独立的数字：如果代理的
    // 上限比 auto_compact 阈值更紧，Codex 永远等不到自己触发压缩就先被
    // 代理裁剪，裁不动时上游就会拒绝。这里把有 cap 的模型的阈值收紧到
    // cap 以内，让 Codex 先手压缩。没有 cap（未启用本地代理 / 未设置
    // context_budget）的模型不受影响。
    budget_caps: &HashMap<String, u64>,
) -> String {
    let base_instructions = if plan_hints {
        format!("{DEFAULT_BASE_INSTRUCTIONS}{PLAN_HINTS_SUPPLEMENT}")
    } else {
        DEFAULT_BASE_INSTRUCTIONS.to_string()
    };
    let models: Vec<Value> = entries
        .iter()
        .enumerate()
        .map(|(index, entry)| {
            // Same fallback table used when auto-populating `contextWindow`
            // for freshly-discovered models (`default_context_window_for_model_id`
            // in main.rs), so an unknown model doesn't get a different
            // default window depending on which code path fills it in.
            let context_window = entry.suffix_window.or(fallback_window).unwrap_or_else(|| {
                parse_window_token(crate::default_context_window_for_model_id(&entry.slug))
                    .unwrap_or(272_000)
            });
            // Third-party models behind the gateway have no reliable sense of
            // their own identity: the "You are Codex" system prompt makes e.g.
            // grok-4.5 introduce itself as a GPT-Codex model. Pin the real id.
            let model_instructions = format!(
                "{base_instructions}\n\nThe underlying model serving this session is `{}`. When asked what model you are, state this exact model id instead of inferring one from context.",
                entry.slug
            );
            json!({
                "slug": entry.slug,
                "id": entry.slug,
                "model": entry.slug,
                "object": "model",
                "owned_by": "custom",
                "created": 0,
                "display_name": entry.display_name,
                "description": entry.display_name,
                "base_instructions": &model_instructions,
                "model_messages": {
                    "instructions_template": &model_instructions,
                    "instructions_variables": {}
                },
                "context_window": context_window,
                "max_context_window": context_window,
                "effective_context_window_percent": 100,
                "auto_compact_token_limit": auto_compact_token_limit_for_model(
                    &entry.slug,
                    context_window,
                    budget_caps.get(&entry.slug).copied(),
                ),
                "priority": 1000 + index,
                "visibility": "list",
                "supported_in_api": true,
                "supported_reasoning_levels": supported_reasoning_levels_for_model(&entry.slug),
                "default_reasoning_level": "medium",
                "default_reasoning_summary": "none",
                "default_verbosity": "low",
                "support_verbosity": true,
                "supports_reasoning_summaries": true,
                "supports_parallel_tool_calls": true,
                "supports_search_tool": true,
                // false so Codex downscales attached images instead of
                // embedding originals: multi-MB base64 images survive
                // compaction forever and blow the upstream request-size
                // cap (measured: two 4.2MB images -> upstream 400).
                "supports_image_detail_original": false,
                "web_search_tool_type": "text_and_image",
                "apply_patch_tool_type": "freeform",
                "shell_type": "shell_command",
                "input_modalities": ["text", "image"],
                "experimental_supported_tools": [],
                "truncation_policy": {
                    "mode": "tokens",
                    "limit": 10000
                },
                // `additional_speed_tiers` is the deprecated Codex catalog field
                // for the "Fast" speed pill; Codex's frontend still reads it
                // even though it's marked deprecated upstream. The intended
                // replacement (`service_tiers` / `default_service_tier`) has
                // an unexplored schema, so it's left untouched below and the
                // legacy field keeps carrying speed-tier info for now.
                "additional_speed_tiers": additional_speed_tiers_for_model(&entry.slug),
                "service_tiers": [],
                "availability_nux": Value::Null,
                "upgrade": Value::Null
            })
        })
        .collect();
    serde_json::to_string_pretty(&json!({ "models": models })).unwrap_or_default()
}

fn parse_window_token(token: &str) -> Option<u64> {
    let token = token.trim();
    if token.is_empty() {
        return None;
    }
    let (num_part, multiplier) = match token.chars().last() {
        Some('K' | 'k') => (&token[..token.len() - 1], 1_000u64),
        Some('M' | 'm') => (&token[..token.len() - 1], 1_000_000u64),
        Some(_) => (token, 1u64),
        None => return None,
    };
    num_part
        .trim()
        .parse::<u64>()
        .ok()
        .map(|value| value * multiplier)
        .filter(|value| *value > 0)
}

fn auto_compact_token_limit_for_model(
    slug: &str,
    context_window: u64,
    budget_cap: Option<u64>,
) -> Value {
    let model = slug
        .trim()
        .rsplit('/')
        .next()
        .unwrap_or(slug)
        .to_ascii_lowercase();
    // The ChatGPT/Codex product lane has smaller effective windows than the
    // native GPT-5.6 API. Leave room for tool schemas, output, and estimation
    // drift so compaction starts before the endpoint returns context_too_large.
    let base = if model.starts_with("gpt-5.6-terra") || model.starts_with("gpt-5.6-luna") {
        Some(100_000.min(context_window.saturating_mul(80) / 100))
    } else if model == "gpt-5.6" || model.starts_with("gpt-5.6-sol") {
        Some(220_000.min(context_window.saturating_mul(85) / 100))
    } else if is_anthropic_family_model(slug) {
        Some(context_window.saturating_mul(50) / 100)
    } else {
        None
    };
    // The model-native window (used above) has no idea the local proxy hard-
    // trims every outgoing request to a much smaller `max_input_tokens` (see
    // `ContextBudgetConfig` in protocol_proxy.rs). Without this clamp Codex
    // never reaches its own auto_compact_token_limit before the proxy's
    // trimmer bites, so a session that can't be trimmed any further just
    // gets rejected upstream as "prompt too long" instead of compacting.
    // Clamp to 90% of the proxy's cap, leaving headroom for the difference
    // between Codex's token estimate and the proxy's own estimator.
    //
    // Families with no native threshold (grok, gpt-5.5, …) must not skip
    // the clamp either: a Null limit disables Codex's auto-compact
    // entirely, so with the proxy cap in force those sessions grew
    // unbounded (1M+ tokens observed) until the still-open turn alone
    // exceeded the cap — untrimmable by design — and every send died
    // upstream, seen as grok-4.5 turns silently ending mid-loop while a
    // 1M-window model on the same thread kept working.
    let capped = budget_cap.map(|cap| cap.saturating_mul(90) / 100);
    match (base, capped) {
        (Some(base), Some(cap)) => json!(base.min(cap)),
        (Some(base), None) => json!(base),
        (None, Some(cap)) => json!(cap),
        (None, None) => Value::Null,
    }
}

fn is_anthropic_family_model(slug: &str) -> bool {
    let model = slug
        .trim()
        .rsplit('/')
        .next()
        .unwrap_or(slug)
        .to_ascii_lowercase();
    model.contains("claude") || model.contains("anthropic")
}

fn reasoning_level_entry(effort: &str, description: &str) -> Value {
    json!({ "effort": effort, "description": description })
}

/// Default four-tier reasoning ladder shared by every model family except
/// the ones with bespoke copy below (gpt-5.6-sol's extra max/ultra tiers,
/// and Claude's thinking-budget phrasing).
fn default_reasoning_levels() -> Vec<Value> {
    vec![
        reasoning_level_entry("low", "Fast responses with lighter reasoning"),
        reasoning_level_entry(
            "medium",
            "Balances speed and reasoning depth for everyday tasks",
        ),
        reasoning_level_entry("high", "Greater reasoning depth for complex problems"),
        reasoning_level_entry("xhigh", "Extra high reasoning depth for complex problems"),
    ]
}

/// Per-model-family ladder for the catalog `supported_reasoning_levels`
/// field. Has to line up with the Codex frontend's own hardcoded per-model
/// ceilings: gpt-5.6-sol (and its bare `gpt-5.6` alias, which routes to Sol
/// — see the `MODEL_CONTEXT_WINDOW_TABLE` comment in main.rs) goes up to
/// `ultra`; gpt-5.6-terra/luna top out at `xhigh` like everything else.
/// Claude gets thinking-budget-flavored copy because the upstream wire
/// representation is Anthropic's native `thinking` object rather than a
/// `reasoning.effort` string (see `apply_chat_reasoning_options` in
/// protocol_proxy.rs).
fn supported_reasoning_levels_for_model(slug: &str) -> Vec<Value> {
    let model = slug
        .trim()
        .rsplit('/')
        .next()
        .unwrap_or(slug)
        .to_ascii_lowercase();

    if model == "gpt-5.6" || model.starts_with("gpt-5.6-sol") {
        let mut levels = default_reasoning_levels();
        levels.push(reasoning_level_entry(
            "max",
            "Maximum reasoning depth for the hardest problems",
        ));
        levels.push(reasoning_level_entry(
            "ultra",
            "Ultra reasoning that consumes usage faster",
        ));
        return levels;
    }

    if is_anthropic_family_model(&model) {
        return vec![
            reasoning_level_entry("low", "Fast responses with minimal thinking budget"),
            reasoning_level_entry("medium", "Balanced thinking budget for everyday tasks"),
            reasoning_level_entry("high", "Deep thinking for complex problems"),
            reasoning_level_entry("xhigh", "Maximum thinking budget for the hardest problems"),
        ];
    }

    default_reasoning_levels()
}

/// Claude flagship families that get the "Fast" speed pill in Codex's model
/// picker (`additional_speed_tiers`). Matched by prefix against the
/// lowercased, provider-stripped slug, so dated/suffixed variants still
/// match — including a `[1m]` long-context suffix, which `parse_model_suffix`
/// already strips into `suffix_window` before the slug gets here, but a
/// prefix match would tolerate it either way.
const CLAUDE_FAST_TIER_PREFIXES: &[&str] = &[
    "claude-fable-5",
    "claude-sonnet-5",
    "claude-opus-4-6",
    "claude-opus-4-7",
    "claude-opus-4-8",
];

fn additional_speed_tiers_for_model(slug: &str) -> Vec<Value> {
    let model = slug
        .trim()
        .rsplit('/')
        .next()
        .unwrap_or(slug)
        .to_ascii_lowercase();
    if CLAUDE_FAST_TIER_PREFIXES
        .iter()
        .any(|prefix| model.starts_with(prefix))
    {
        return vec![json!("fast")];
    }
    Vec::new()
}

pub async fn list_targets(debug_port: u16) -> anyhow::Result<Vec<CdpTarget>> {
    let client = reqwest::Client::builder()
        .no_proxy()
        .timeout(CDP_HTTP_TIMEOUT)
        .build()
        .context("failed to build CDP HTTP client")?;
    let urls = [
        format!("http://127.0.0.1:{debug_port}/json"),
        format!("http://[::1]:{debug_port}/json"),
    ];
    let mut errors = Vec::new();
    for url in urls {
        match query_targets_url(&client, &url).await {
            Ok(targets) => return Ok(targets),
            Err(error) => errors.push(format!("{url}: {error:#}")),
        }
    }
    bail!(
        "failed to query CDP targets on loopback addresses: {}",
        errors.join("; ")
    )
}

async fn query_targets_url(client: &reqwest::Client, url: &str) -> anyhow::Result<Vec<CdpTarget>> {
    let response = client
        .get(url)
        .send()
        .await
        .context("failed to query CDP targets")?
        .error_for_status()
        .context("CDP target query failed")?;
    response
        .json::<Vec<CdpTarget>>()
        .await
        .context("failed to deserialize CDP targets")
}

pub fn is_injectable_page_target(target: &CdpTarget) -> bool {
    target.target_type == "page"
        && target
            .web_socket_debugger_url
            .as_deref()
            .is_some_and(|url| !url.is_empty())
}

pub fn pick_injectable_codex_page_target(targets: &[CdpTarget]) -> anyhow::Result<CdpTarget> {
    for target in targets
        .iter()
        .filter(|target| is_injectable_page_target(target))
    {
        if is_codex_page_target(target) {
            return Ok(target.clone());
        }
    }
    bail!("No injectable Codex page target found")
}

fn is_codex_page_target(target: &CdpTarget) -> bool {
    if target.target_type != "page" {
        return false;
    }
    let haystack = format!("{} {}", target.title, target.url).to_lowercase();
    haystack.contains("codex") || target.url.starts_with("app://-/index.html")
}

pub async fn evaluate_script(websocket_url: &str, script: &str) -> anyhow::Result<Value> {
    let (mut socket, _) = tokio::time::timeout(CDP_CONNECT_TIMEOUT, connect_async(websocket_url))
        .await
        .with_context(|| {
            format!(
                "timed out connecting CDP websocket after {}s",
                CDP_CONNECT_TIMEOUT.as_secs()
            )
        })?
        .context("failed to connect CDP websocket")?;
    cdp_send_command(
        &mut socket,
        1,
        "Runtime.evaluate",
        runtime_evaluate_params(script),
    )
    .await
}

pub fn runtime_evaluate_params(script: &str) -> Value {
    json!({
        "expression": script,
        "awaitPromise": false,
        "allowUnsafeEvalBlockedByCSP": true,
    })
}

async fn cdp_send_command(
    socket: &mut tokio_tungstenite::WebSocketStream<
        tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>,
    >,
    message_id: u64,
    method: &str,
    params: Value,
) -> anyhow::Result<Value> {
    socket
        .send(Message::Text(
            json!({
                "id": message_id,
                "method": method,
                "params": params,
            })
            .to_string()
            .into(),
        ))
        .await
        .with_context(|| format!("failed to send CDP command {method} id {message_id}"))?;

    loop {
        let message = tokio::time::timeout(CDP_COMMAND_TIMEOUT, socket.next())
            .await
            .with_context(|| {
                format!(
                    "timed out waiting for CDP command {method} id {message_id} response after {}s",
                    CDP_COMMAND_TIMEOUT.as_secs()
                )
            })?
            .ok_or_else(|| anyhow::anyhow!("CDP websocket closed before response for {method}"))?
            .context("failed to read CDP websocket message")?;
        let Message::Text(text) = message else {
            continue;
        };
        let value: Value = serde_json::from_str(&text).context("failed to parse CDP message")?;
        if value.get("id").and_then(Value::as_u64) != Some(message_id) {
            continue;
        }
        if let Some(error) = value.get("error") {
            bail!("CDP command {method} id {message_id} failed: {error}");
        }
        return Ok(value);
    }
}

pub fn resolve_codex_app_dir(app_dir: Option<&Path>) -> Option<PathBuf> {
    if let Some(app_dir) = app_dir {
        return normalize_codex_app_path(app_dir);
    }
    #[cfg(target_os = "macos")]
    {
        return find_macos_codex_app_default();
    }
    #[cfg(windows)]
    {
        return find_latest_codex_app_dir_default().or_else(find_standalone_codex_app_dir);
    }
    #[cfg(not(any(target_os = "macos", windows)))]
    {
        find_latest_codex_app_dir_default().or_else(find_standalone_codex_app_dir)
    }
}

pub fn codex_app_version(app_dir: &Path) -> Option<String> {
    if app_dir.extension() == Some(OsStr::new("app")) {
        return macos_app_version(app_dir);
    }
    let package_dir = if app_dir
        .file_name()
        .and_then(OsStr::to_str)
        .is_some_and(|name| name.eq_ignore_ascii_case("app"))
    {
        app_dir.parent()?
    } else {
        app_dir
    };
    codex_package_version(package_dir)
}

#[cfg(any(windows, test))]
pub fn packaged_app_user_model_id(app_dir: &Path) -> Option<String> {
    let package_name = package_name_from_app_dir(app_dir)?;
    let (identity_name, _, publisher_id) = codex_package_parts(&package_name)?;
    if publisher_id.is_empty() {
        return None;
    }
    Some(format!("{identity_name}_{publisher_id}!App"))
}

#[cfg(any(not(target_os = "macos"), test))]
pub fn build_codex_arguments(debug_port: u16, extra_args: &[String]) -> Vec<String> {
    let mut args = vec![
        format!("--remote-debugging-port={debug_port}"),
        format!("--remote-allow-origins=http://127.0.0.1:{debug_port}"),
    ];
    args.extend(
        extra_args
            .iter()
            .filter(|arg| !arg.trim().is_empty())
            .cloned(),
    );
    args
}

#[cfg(not(target_os = "macos"))]
pub fn build_codex_command(app_dir: &Path, debug_port: u16, extra_args: &[String]) -> Vec<String> {
    let mut command = vec![
        build_codex_executable(app_dir)
            .to_string_lossy()
            .to_string(),
    ];
    command.extend(build_codex_arguments(debug_port, extra_args));
    command
}
#[cfg(windows)]
pub async fn activate_packaged_app(
    app_user_model_id: &str,
    arguments: &str,
) -> anyhow::Result<u32> {
    let app_user_model_id = app_user_model_id.to_string();
    let arguments = arguments.to_string();
    tokio::task::spawn_blocking(move || {
        activate_packaged_app_blocking(&app_user_model_id, &arguments)
    })
    .await
    .context("packaged Codex App activation task failed")?
}

#[cfg(windows)]
fn activate_packaged_app_blocking(app_user_model_id: &str, arguments: &str) -> anyhow::Result<u32> {
    use windows::Win32::System::Com::{
        CLSCTX_LOCAL_SERVER, COINIT_APARTMENTTHREADED, CoCreateInstance, CoInitializeEx,
        CoUninitialize,
    };
    use windows::Win32::UI::Shell::{ApplicationActivationManager, IApplicationActivationManager};
    use windows::core::HSTRING;

    unsafe {
        let coinit = CoInitializeEx(None, COINIT_APARTMENTTHREADED);
        let should_uninitialize = coinit.is_ok();
        coinit.ok().or_else(|error| {
            const RPC_E_CHANGED_MODE: i32 = -2147417850;
            if error.code().0 == RPC_E_CHANGED_MODE {
                Ok(())
            } else {
                Err(error)
            }
        })?;

        let result: windows::core::Result<u32> = (|| {
            let manager: IApplicationActivationManager =
                CoCreateInstance(&ApplicationActivationManager, None, CLSCTX_LOCAL_SERVER)?;
            let process_id = manager.ActivateApplication(
                &HSTRING::from(app_user_model_id),
                &HSTRING::from(arguments),
                windows::Win32::UI::Shell::ACTIVATEOPTIONS(0),
            )?;
            Ok(process_id)
        })();

        if should_uninitialize {
            CoUninitialize();
        }
        result.map_err(Into::into)
    }
}

#[cfg(any(windows, test))]
pub fn command_line_arguments(args: &[String]) -> String {
    args.iter()
        .map(|arg| quote_windows_argument(arg))
        .collect::<Vec<_>>()
        .join(" ")
}

#[cfg(any(windows, test))]
fn quote_windows_argument(arg: &str) -> String {
    if !arg.is_empty() && !arg.bytes().any(|byte| matches!(byte, b' ' | b'\t' | b'"')) {
        return arg.to_string();
    }
    let mut output = String::from("\"");
    let mut backslashes = 0;
    for ch in arg.chars() {
        match ch {
            '\\' => backslashes += 1,
            '"' => {
                output.push_str(&"\\".repeat(backslashes * 2 + 1));
                output.push('"');
                backslashes = 0;
            }
            _ => {
                output.push_str(&"\\".repeat(backslashes));
                output.push(ch);
                backslashes = 0;
            }
        }
    }
    output.push_str(&"\\".repeat(backslashes * 2));
    output.push('"');
    output
}

#[cfg(target_os = "macos")]
fn find_macos_codex_app_default() -> Option<PathBuf> {
    let mut roots = vec![PathBuf::from("/Applications")];
    if let Some(home) = home_dir() {
        roots.push(home.join("Applications"));
    }
    for root in roots {
        for candidate in macos_app_candidates(&root) {
            if candidate.is_dir() && macos_candidate_is_codex_app(&candidate) {
                return Some(candidate);
            }
        }
    }
    None
}

/// 新版官方 app 复用了 `ChatGPT.app` 这个 bundle 名（CFBundleIdentifier 仍是
/// `com.openai.codex`），而纯聊天版 ChatGPT（`com.openai.chat`）也叫这个名字，
/// 不能当 Codex App 用。所以对 `ChatGPT.app` 这个名字要求 bundle id 或内嵌的
/// Codex Framework 佐证；其余名字保持原有的按名即中行为。
#[cfg(target_os = "macos")]
fn macos_candidate_is_codex_app(app_dir: &Path) -> bool {
    let is_chatgpt_name = app_dir
        .file_name()
        .and_then(OsStr::to_str)
        .is_some_and(|name| name.eq_ignore_ascii_case("ChatGPT.app"));
    if !is_chatgpt_name {
        return true;
    }
    macos_bundle_identifier_is_codex(app_dir)
        || app_dir
            .join("Contents")
            .join("Frameworks")
            .join("Codex Framework.framework")
            .is_dir()
}

#[cfg(target_os = "macos")]
fn macos_bundle_identifier_is_codex(app_dir: &Path) -> bool {
    let Ok(plist) = fs::read_to_string(app_dir.join("Contents").join("Info.plist")) else {
        return false;
    };
    plist_string_value(&plist, "CFBundleIdentifier")
        .is_some_and(|id| id == "com.openai.codex" || id.starts_with("com.openai.codex."))
}

#[cfg(not(target_os = "macos"))]
fn find_latest_codex_app_dir_default() -> Option<PathBuf> {
    #[cfg(windows)]
    {
        find_latest_codex_app_dir_from_roots(&windows_app_package_roots())
            .or_else(find_latest_codex_app_dir_from_appx_package)
    }

    #[cfg(not(windows))]
    {
        None
    }
}

#[cfg(windows)]
fn find_latest_codex_app_dir_from_appx_package() -> Option<PathBuf> {
    let output = std::process::Command::new("powershell")
        .args([
            "-NoProfile",
            "-ExecutionPolicy",
            "Bypass",
            "-Command",
            "Get-AppxPackage -Name OpenAI.Codex* | Where-Object { @('OpenAI.Codex','OpenAI.CodexBeta') -contains $_.Name } | Sort-Object Version -Descending | Select-Object -First 1 -ExpandProperty InstallLocation",
        ])
        .output()
        .ok()?;
    if !output.status.success() {
        return None;
    }
    String::from_utf8_lossy(&output.stdout)
        .lines()
        .map(str::trim)
        .find(|line| !line.is_empty())
        .and_then(|line| normalize_codex_app_path(Path::new(line)))
}

#[cfg(windows)]
fn windows_app_package_roots() -> Vec<PathBuf> {
    let mut roots = Vec::new();
    if let Some(program_files) = std::env::var_os("ProgramFiles") {
        roots.push(PathBuf::from(program_files).join("WindowsApps"));
    }
    if let Some(program_files) = std::env::var_os("ProgramW6432") {
        roots.push(PathBuf::from(program_files).join("WindowsApps"));
    }
    roots.push(PathBuf::from(r"C:\Program Files\WindowsApps"));
    roots.sort();
    roots.dedup();
    roots
}

#[cfg(windows)]
fn find_latest_codex_app_dir(root: &Path) -> Option<PathBuf> {
    let mut matches = fs::read_dir(root)
        .ok()?
        .filter_map(Result::ok)
        .map(|entry| entry.path())
        .filter(|path| path.is_dir())
        .filter_map(|path| version_tuple(&path).map(|version| (version, path)))
        .collect::<Vec<_>>();
    matches.sort_by(|left, right| left.0.cmp(&right.0));
    let (_, latest) = matches.pop()?;
    let app = latest.join("app");
    Some(if app.is_dir() { app } else { latest })
}

#[cfg(windows)]
fn find_latest_codex_app_dir_from_roots(roots: &[PathBuf]) -> Option<PathBuf> {
    roots
        .iter()
        .filter_map(|root| find_latest_codex_app_dir(root))
        .max_by(|left, right| {
            version_tuple(left.parent().unwrap_or(left))
                .cmp(&version_tuple(right.parent().unwrap_or(right)))
        })
}

#[cfg(not(target_os = "macos"))]
fn find_standalone_codex_app_dir() -> Option<PathBuf> {
    let local_appdata = std::env::var_os("LOCALAPPDATA")?;
    let candidates = [
        PathBuf::from(&local_appdata)
            .join("OpenAI")
            .join("Codex")
            .join("bin"),
        PathBuf::from(&local_appdata).join("OpenAI").join("Codex"),
        PathBuf::from(&local_appdata)
            .join("Programs")
            .join("OpenAI")
            .join("Codex"),
    ];
    for candidate in candidates {
        if let Some(path) = normalize_codex_app_path(&candidate) {
            if build_codex_executable(&path).exists() {
                return Some(path);
            }
        }
    }
    None
}

fn normalize_codex_app_path(path: &Path) -> Option<PathBuf> {
    if path.as_os_str().is_empty() {
        return None;
    }
    let file_name = path.file_name().and_then(OsStr::to_str).unwrap_or_default();
    if file_name.eq_ignore_ascii_case("Codex.exe") || file_name.eq_ignore_ascii_case("codex.exe") {
        return path.parent().map(Path::to_path_buf);
    }
    if path.extension() == Some(OsStr::new("app")) {
        return Some(path.to_path_buf());
    }
    if path.is_file() {
        return path.parent().map(Path::to_path_buf);
    }
    let upper = path.join("Codex.exe");
    let lower = path.join("codex.exe");
    if upper.exists() || lower.exists() {
        return Some(path.to_path_buf());
    }
    let nested_app = path.join("app");
    if nested_app.is_dir() {
        let upper = nested_app.join("Codex.exe");
        let lower = nested_app.join("codex.exe");
        if upper.exists() || lower.exists() {
            return Some(nested_app);
        }
    }
    if path.is_dir() {
        return Some(path.to_path_buf());
    }
    None
}

#[cfg(not(target_os = "macos"))]
fn build_codex_executable(app_dir: &Path) -> PathBuf {
    if app_dir.extension() == Some(OsStr::new("app")) {
        return app_dir.join("Contents").join("MacOS").join("Codex");
    }
    let upper = app_dir.join("Codex.exe");
    if upper.exists() {
        upper
    } else {
        app_dir.join("codex.exe")
    }
}

fn macos_app_version(app_dir: &Path) -> Option<String> {
    let plist = fs::read_to_string(app_dir.join("Contents").join("Info.plist")).ok()?;
    plist_string_value(&plist, "CFBundleShortVersionString")
        .or_else(|| plist_string_value(&plist, "CFBundleVersion"))
}

#[cfg(any(target_os = "macos", test))]
pub fn macos_app_executable_name(app_dir: &Path) -> Option<String> {
    let plist = fs::read_to_string(app_dir.join("Contents").join("Info.plist")).ok()?;
    plist_string_value(&plist, "CFBundleExecutable")
}

fn plist_string_value(plist: &str, key: &str) -> Option<String> {
    let (_, after_key) = plist.split_once(&format!("<key>{key}</key>"))?;
    let (_, after_string_open) = after_key.split_once("<string>")?;
    let (value, _) = after_string_open.split_once("</string>")?;
    let trimmed = value.trim();
    if trimmed.is_empty() {
        None
    } else {
        Some(trimmed.to_string())
    }
}

#[cfg(target_os = "macos")]
fn macos_app_candidates(root: &Path) -> Vec<PathBuf> {
    if root.extension() == Some(OsStr::new("app")) {
        return vec![root.to_path_buf()];
    }
    [
        "Codex.app",
        "OpenAI Codex.app",
        "OpenAI.Codex.app",
        "ChatGPT.app",
    ]
    .into_iter()
    .map(|name| root.join(name))
    .collect()
}

#[cfg(any(windows, test))]
fn package_name_from_app_dir(app_dir: &Path) -> Option<String> {
    let path = app_dir.to_string_lossy().replace('\\', "/");
    let mut parts = path.split('/').filter(|part| !part.is_empty());
    let mut package_name = parts.next_back()?;
    if package_name.eq_ignore_ascii_case("app") {
        package_name = parts.next_back()?;
    }
    Some(package_name.to_string())
}

fn codex_package_version(package_dir: &Path) -> Option<String> {
    let path = package_dir.to_string_lossy().replace('\\', "/");
    let name = path
        .split('/')
        .rev()
        .find(|part| codex_package_parts(part).is_some())?;
    let (_, version, _) = codex_package_parts(name)?;
    if version.is_empty() {
        None
    } else {
        Some(version.to_string())
    }
}

#[cfg(windows)]
fn version_tuple(path: &Path) -> Option<Vec<u32>> {
    let name = path.file_name()?.to_str()?;
    let (_, version, _) = codex_package_parts(name)?;
    let parts = version
        .split('.')
        .map(str::parse::<u32>)
        .collect::<Result<Vec<_>, _>>()
        .ok()?;
    if parts.is_empty() { None } else { Some(parts) }
}

fn codex_package_parts(package_name: &str) -> Option<(&str, &str, &str)> {
    for identity in CODEX_PACKAGE_IDENTITIES {
        let Some(rest) = package_name.strip_prefix(identity) else {
            continue;
        };
        let Some(rest) = rest.strip_prefix('_') else {
            continue;
        };
        let Some((version, rest)) = rest.split_once('_') else {
            continue;
        };
        let Some((_, publisher_id)) = rest.rsplit_once("__") else {
            continue;
        };
        return Some((*identity, version, publisher_id));
    }
    None
}

#[cfg(target_os = "macos")]
fn home_dir() -> Option<PathBuf> {
    if let Some(home) = std::env::var_os("HOME") {
        let path = PathBuf::from(home);
        if !path.as_os_str().is_empty() {
            return Some(path);
        }
    }
    #[cfg(windows)]
    {
        if let Some(profile) = std::env::var_os("USERPROFILE") {
            let path = PathBuf::from(profile);
            if !path.as_os_str().is_empty() {
                return Some(path);
            }
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;

    fn scratch_dir(name: &str) -> PathBuf {
        let dir = std::env::temp_dir()
            .join("codex-gateway-lite-tests")
            .join(format!("{}-{name}", std::process::id()));
        let _ = fs::remove_dir_all(&dir);
        fs::create_dir_all(&dir).expect("create scratch dir");
        dir
    }

    fn write_info_plist(app_dir: &Path, entries: &[(&str, &str)]) {
        let contents_dir = app_dir.join("Contents");
        fs::create_dir_all(&contents_dir).expect("create Contents dir");
        let mut body = String::from(
            "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n<plist version=\"1.0\">\n<dict>\n",
        );
        for (key, value) in entries {
            body.push_str(&format!("  <key>{key}</key>\n  <string>{value}</string>\n"));
        }
        body.push_str("</dict>\n</plist>\n");
        fs::write(contents_dir.join("Info.plist"), body).expect("write Info.plist");
    }

    #[test]
    fn collect_catalog_entries_orders_newest_flagship_families_first() {
        let model_list = "claude-fable-5\nclaude-opus-4-8\nclaude-opus-4-5-20251101\ngrok-4.3\ngrok-4.20-multi-agent-0309\ngrok-3-mini-fast\ngrok-imagine-image\ngpt-5.6-sol\ngpt-5.6-terra\ntext-embedding-3-small\nclaude-sonnet-5\ngrok-4.5\nclaude-opus-4-7";
        let entries = collect_catalog_entries(model_list, &HashMap::new(), "");
        let slugs: Vec<&str> = entries.iter().map(|entry| entry.slug.as_str()).collect();
        assert_eq!(
            slugs,
            vec![
                "gpt-5.6-sol",
                "gpt-5.6-terra",
                "claude-fable-5",
                "claude-sonnet-5",
                "grok-4.5",
                "grok-4.3",
                "claude-opus-4-8",
                "claude-opus-4-7",
                // Unlisted models keep upstream relative order at the bottom.
                "claude-opus-4-5-20251101",
                "grok-4.20-multi-agent-0309",
                "grok-3-mini-fast",
                "grok-imagine-image",
                "text-embedding-3-small",
            ]
        );
    }

    #[test]
    fn collect_catalog_entries_keeps_current_model_first_despite_ranking() {
        let model_list = "gpt-5.6-sol\ngrok-4.3\nclaude-fable-5";
        let entries = collect_catalog_entries(model_list, &HashMap::new(), "grok-4.3");
        let slugs: Vec<&str> = entries.iter().map(|entry| entry.slug.as_str()).collect();
        assert_eq!(slugs, vec!["grok-4.3", "gpt-5.6-sol", "claude-fable-5"]);
    }

    fn reasoning_efforts(levels: &[Value]) -> Vec<&str> {
        levels
            .iter()
            .map(|level| level["effort"].as_str().expect("effort is a string"))
            .collect()
    }

    #[test]
    fn supported_reasoning_levels_gpt_5_6_sol_gets_six_tiers_with_bespoke_max_ultra_copy() {
        for slug in ["gpt-5.6-sol", "gpt-5.6", "gpt-5.6-sol-2026-07-09"] {
            let levels = supported_reasoning_levels_for_model(slug);
            assert_eq!(
                reasoning_efforts(&levels),
                vec!["low", "medium", "high", "xhigh", "max", "ultra"],
                "slug {slug} should expose all six tiers"
            );
            assert_eq!(
                levels[4]["description"].as_str(),
                Some("Maximum reasoning depth for the hardest problems")
            );
            assert_eq!(
                levels[5]["description"].as_str(),
                Some("Ultra reasoning that consumes usage faster")
            );
        }
    }

    #[test]
    fn supported_reasoning_levels_gpt_5_6_terra_and_luna_stay_at_four_tiers() {
        for slug in ["gpt-5.6-terra", "gpt-5.6-luna"] {
            let levels = supported_reasoning_levels_for_model(slug);
            assert_eq!(
                reasoning_efforts(&levels),
                vec!["low", "medium", "high", "xhigh"],
                "slug {slug} should stay capped at xhigh like the official Terra ceiling"
            );
        }
    }

    #[test]
    fn supported_reasoning_levels_claude_family_uses_thinking_budget_copy() {
        for slug in [
            "claude-sonnet-5",
            "claude-opus-4-8",
            "openai/claude-fable-5",
        ] {
            let levels = supported_reasoning_levels_for_model(slug);
            assert_eq!(
                reasoning_efforts(&levels),
                vec!["low", "medium", "high", "xhigh"]
            );
            assert_eq!(
                levels[0]["description"].as_str(),
                Some("Fast responses with minimal thinking budget")
            );
            assert_eq!(
                levels[1]["description"].as_str(),
                Some("Balanced thinking budget for everyday tasks")
            );
            assert_eq!(
                levels[2]["description"].as_str(),
                Some("Deep thinking for complex problems")
            );
            assert_eq!(
                levels[3]["description"].as_str(),
                Some("Maximum thinking budget for the hardest problems")
            );
        }
    }

    #[test]
    fn supported_reasoning_levels_other_models_keep_generic_four_tier_copy() {
        for slug in ["grok-4.5", "text-embedding-3-small"] {
            let levels = supported_reasoning_levels_for_model(slug);
            assert_eq!(
                reasoning_efforts(&levels),
                vec!["low", "medium", "high", "xhigh"]
            );
            assert_eq!(
                levels[0]["description"].as_str(),
                Some("Fast responses with lighter reasoning")
            );
        }
    }

    #[test]
    fn additional_speed_tiers_claude_flagship_models_get_fast_tier() {
        for slug in [
            "claude-fable-5",
            "claude-sonnet-5",
            "claude-opus-4-6",
            "claude-opus-4-7",
            "claude-opus-4-8",
            "openai/claude-sonnet-5",
        ] {
            assert_eq!(
                additional_speed_tiers_for_model(slug),
                vec![json!("fast")],
                "slug {slug} should expose the fast speed tier"
            );
        }
    }

    #[test]
    fn additional_speed_tiers_non_flagship_models_stay_empty() {
        for slug in ["grok-4.5", "claude-opus-4-5-20251101", "gpt-5.6-sol"] {
            assert!(
                additional_speed_tiers_for_model(slug).is_empty(),
                "slug {slug} should not get the fast speed tier"
            );
        }
    }

    #[test]
    fn gpt_5_6_catalog_entries_use_codex_product_windows_and_compact_early() {
        let entries = ["gpt-5.6", "gpt-5.6-sol", "gpt-5.6-terra", "gpt-5.6-luna"]
            .into_iter()
            .map(|slug| ModelCatalogEntry {
                slug: slug.to_string(),
                display_name: slug.to_string(),
                suffix_window: None,
            })
            .collect::<Vec<_>>();
        let catalog: Value = serde_json::from_str(&build_model_catalog_json(
            &entries,
            None,
            false,
            &HashMap::new(),
        ))
        .expect("catalog parses");

        let models = catalog["models"].as_array().expect("models array");
        assert_eq!(models[0]["context_window"].as_u64(), Some(258_400));
        assert_eq!(
            models[0]["auto_compact_token_limit"].as_u64(),
            Some(219_640)
        );
        assert_eq!(models[1]["context_window"].as_u64(), Some(258_400));
        assert_eq!(
            models[1]["auto_compact_token_limit"].as_u64(),
            Some(219_640)
        );
        assert_eq!(models[2]["context_window"].as_u64(), Some(128_000));
        assert_eq!(
            models[2]["auto_compact_token_limit"].as_u64(),
            Some(100_000)
        );
        assert_eq!(models[3]["context_window"].as_u64(), Some(128_000));
        assert_eq!(
            models[3]["auto_compact_token_limit"].as_u64(),
            Some(100_000)
        );
    }

    #[test]
    fn auto_compact_token_limit_clamps_to_proxy_budget_cap_when_tighter() {
        // Real-world numbers from the bug this guards against: a 1M-window
        // fable model normally compacts at 50% (500_000), but the local
        // proxy's actual send cap was ~180_880 tokens — far below that.
        // Codex would never see 500_000 tokens to trigger its own compact,
        // so it must be clamped down into range instead.
        let entries = vec![ModelCatalogEntry {
            slug: "claude-fable-5".to_string(),
            display_name: "claude-fable-5".to_string(),
            suffix_window: Some(1_000_000),
        }];
        let mut budget_caps = HashMap::new();
        budget_caps.insert("claude-fable-5".to_string(), 180_880);
        let catalog: Value = serde_json::from_str(&build_model_catalog_json(
            &entries,
            None,
            false,
            &budget_caps,
        ))
        .expect("catalog parses");
        assert_eq!(
            catalog["models"][0]["auto_compact_token_limit"].as_u64(),
            // 90% of the cap, leaving headroom for tokenizer estimation drift.
            Some(180_880 * 90 / 100)
        );
    }

    #[test]
    fn auto_compact_token_limit_unclamped_when_no_budget_cap_present() {
        // No cap entry for this slug (e.g. provider doesn't use the local
        // proxy, or context_budget is off) must reproduce the pre-clamp
        // behavior exactly.
        let entries = vec![ModelCatalogEntry {
            slug: "claude-fable-5".to_string(),
            display_name: "claude-fable-5".to_string(),
            suffix_window: Some(1_000_000),
        }];
        let catalog: Value = serde_json::from_str(&build_model_catalog_json(
            &entries,
            None,
            false,
            &HashMap::new(),
        ))
        .expect("catalog parses");
        assert_eq!(
            catalog["models"][0]["auto_compact_token_limit"].as_u64(),
            Some(500_000)
        );
    }

    #[test]
    fn auto_compact_token_limit_ignores_cap_looser_than_native_threshold() {
        // A generous cap (bigger than the model-native auto_compact
        // threshold once scaled by 90%) must never raise the limit — only
        // ever tighten it. `min()` keeps this a one-way clamp.
        let entries = vec![ModelCatalogEntry {
            slug: "claude-fable-5".to_string(),
            display_name: "claude-fable-5".to_string(),
            suffix_window: Some(1_000_000),
        }];
        let mut budget_caps = HashMap::new();
        budget_caps.insert("claude-fable-5".to_string(), 800_000);
        let catalog: Value = serde_json::from_str(&build_model_catalog_json(
            &entries,
            None,
            false,
            &budget_caps,
        ))
        .expect("catalog parses");
        assert_eq!(
            catalog["models"][0]["auto_compact_token_limit"].as_u64(),
            Some(500_000)
        );
    }

    #[test]
    fn auto_compact_token_limit_applies_cap_to_families_without_native_threshold() {
        // grok-4.5 (256K window) has no model-native compact threshold, so
        // before the fix its limit was Null — Codex never auto-compacted,
        // sessions grew past 1M tokens, and once the still-open turn alone
        // exceeded the proxy's 179_200-token send cap every request died
        // upstream (turns silently ending mid-loop). With a cap present the
        // limit must fall back to 90% of the cap instead of Null.
        let entries = vec![ModelCatalogEntry {
            slug: "grok-4.5".to_string(),
            display_name: "grok-4.5".to_string(),
            suffix_window: Some(256_000),
        }];
        let mut budget_caps = HashMap::new();
        budget_caps.insert("grok-4.5".to_string(), 179_200);
        let catalog: Value = serde_json::from_str(&build_model_catalog_json(
            &entries,
            None,
            false,
            &budget_caps,
        ))
        .expect("catalog parses");
        assert_eq!(
            catalog["models"][0]["auto_compact_token_limit"].as_u64(),
            Some(179_200 * 90 / 100)
        );
    }

    #[test]
    fn auto_compact_token_limit_stays_null_without_native_threshold_or_cap() {
        // No native threshold and no proxy cap (proxy unused / budget off):
        // the pre-fix behavior — no auto-compact limit — must be preserved.
        let entries = vec![ModelCatalogEntry {
            slug: "grok-4.5".to_string(),
            display_name: "grok-4.5".to_string(),
            suffix_window: Some(256_000),
        }];
        let catalog: Value = serde_json::from_str(&build_model_catalog_json(
            &entries,
            None,
            false,
            &HashMap::new(),
        ))
        .expect("catalog parses");
        assert!(catalog["models"][0]["auto_compact_token_limit"].is_null());
    }

    #[test]
    fn macos_app_executable_name_reads_bundle_executable() {
        let root = scratch_dir("executable-name");
        let app_dir = root.join("ChatGPT.app");
        write_info_plist(
            &app_dir,
            &[
                ("CFBundleIdentifier", "com.openai.codex"),
                ("CFBundleExecutable", "ChatGPT"),
            ],
        );
        assert_eq!(
            macos_app_executable_name(&app_dir).as_deref(),
            Some("ChatGPT")
        );
        let _ = fs::remove_dir_all(&root);
    }

    #[cfg(target_os = "macos")]
    #[test]
    fn macos_candidate_accepts_legacy_codex_bundle_names_without_plist() {
        let root = scratch_dir("legacy-names");
        let app_dir = root.join("Codex.app");
        fs::create_dir_all(&app_dir).expect("create app dir");
        assert!(macos_candidate_is_codex_app(&app_dir));
        let _ = fs::remove_dir_all(&root);
    }

    #[cfg(target_os = "macos")]
    #[test]
    fn macos_candidate_accepts_chatgpt_bundle_with_codex_identifier() {
        let root = scratch_dir("chatgpt-codex-id");
        let app_dir = root.join("ChatGPT.app");
        write_info_plist(&app_dir, &[("CFBundleIdentifier", "com.openai.codex")]);
        assert!(macos_candidate_is_codex_app(&app_dir));
        let _ = fs::remove_dir_all(&root);
    }

    #[cfg(target_os = "macos")]
    #[test]
    fn macos_candidate_rejects_plain_chatgpt_chat_app() {
        let root = scratch_dir("chatgpt-chat-id");
        let app_dir = root.join("ChatGPT.app");
        write_info_plist(&app_dir, &[("CFBundleIdentifier", "com.openai.chat")]);
        assert!(!macos_candidate_is_codex_app(&app_dir));
        let _ = fs::remove_dir_all(&root);
    }

    #[cfg(target_os = "macos")]
    #[test]
    fn macos_candidate_accepts_chatgpt_bundle_with_codex_framework() {
        let root = scratch_dir("chatgpt-framework");
        let app_dir = root.join("ChatGPT.app");
        fs::create_dir_all(
            app_dir
                .join("Contents")
                .join("Frameworks")
                .join("Codex Framework.framework"),
        )
        .expect("create framework dir");
        assert!(macos_candidate_is_codex_app(&app_dir));
        let _ = fs::remove_dir_all(&root);
    }
}
