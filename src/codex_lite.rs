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

    entries.append(&mut list_entries);
    entries
}

pub fn build_model_catalog_json(
    entries: &[ModelCatalogEntry],
    fallback_window: Option<u64>,
    plan_hints: bool,
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
            let context_window = entry.suffix_window.or(fallback_window).unwrap_or(272_000);
            json!({
                "slug": entry.slug,
                "id": entry.slug,
                "model": entry.slug,
                "object": "model",
                "owned_by": "custom",
                "created": 0,
                "display_name": entry.display_name,
                "description": entry.display_name,
                "base_instructions": &base_instructions,
                "model_messages": {
                    "instructions_template": &base_instructions,
                    "instructions_variables": {}
                },
                "context_window": context_window,
                "max_context_window": context_window,
                "effective_context_window_percent": 100,
                "auto_compact_token_limit": auto_compact_token_limit_for_model(&entry.slug, context_window),
                "priority": 1000 + index,
                "visibility": "list",
                "supported_in_api": true,
                "supported_reasoning_levels": [
                    {
                        "effort": "low",
                        "description": "Fast responses with lighter reasoning"
                    },
                    {
                        "effort": "medium",
                        "description": "Balances speed and reasoning depth for everyday tasks"
                    },
                    {
                        "effort": "high",
                        "description": "Greater reasoning depth for complex problems"
                    },
                    {
                        "effort": "xhigh",
                        "description": "Extra high reasoning depth for complex problems"
                    }
                ],
                "default_reasoning_level": "medium",
                "default_reasoning_summary": "none",
                "default_verbosity": "low",
                "support_verbosity": true,
                "supports_reasoning_summaries": true,
                "supports_parallel_tool_calls": true,
                "supports_search_tool": true,
                "supports_image_detail_original": true,
                "web_search_tool_type": "text_and_image",
                "apply_patch_tool_type": "freeform",
                "shell_type": "shell_command",
                "input_modalities": ["text", "image"],
                "experimental_supported_tools": [],
                "truncation_policy": {
                    "mode": "tokens",
                    "limit": 10000
                },
                "additional_speed_tiers": [],
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

fn auto_compact_token_limit_for_model(slug: &str, context_window: u64) -> Value {
    if is_anthropic_family_model(slug) {
        return json!(context_window.saturating_mul(65) / 100);
    }
    Value::Null
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

fn find_macos_codex_app_default() -> Option<PathBuf> {
    let mut roots = vec![PathBuf::from("/Applications")];
    if let Some(home) = home_dir() {
        roots.push(home.join("Applications"));
    }
    for root in roots {
        for candidate in macos_app_candidates(&root) {
            if candidate.is_dir() {
                return Some(candidate);
            }
        }
    }
    None
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

fn macos_app_candidates(root: &Path) -> Vec<PathBuf> {
    if root.extension() == Some(OsStr::new("app")) {
        return vec![root.to_path_buf()];
    }
    ["Codex.app", "OpenAI Codex.app", "OpenAI.Codex.app"]
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
