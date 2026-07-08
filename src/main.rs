use std::collections::{BTreeMap, HashMap, HashSet};
use std::ffi::OsStr;
use std::fs;
use std::io::{self, BufRead, Write};
use std::path::{Path, PathBuf};
use std::process::{Command as ProcessCommand, Stdio};
use std::time::{Duration, Instant, SystemTime};

use anyhow::{Context, bail};
use chrono::DateTime;
use codex_lite::RelayProfile;
use futures_util::{SinkExt, StreamExt};
use rusqlite::{Connection, OpenFlags, OptionalExtension, params};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio_tungstenite::connect_async;
use tokio_tungstenite::tungstenite::Message;

mod codex_lite;
mod protocol_proxy;

const LOCAL_PROXY_CODEX_BEARER_TOKEN: &str = "codex-gateway-lite-local-proxy";
const DEFAULT_CONTEXT_BUDGET: &str = "off";
const SUGGESTED_CONTEXT_BUDGET: &str = "200K";
const PLAN_UI_REINJECT_INTERVAL_SECS: u64 = 30;
const PLAN_UI_ACTIVE_HISTORY_SEED_RETRY_SECS: u64 = 30;
const PLAN_UI_INITIAL_HISTORY_SEED: bool = true;

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
struct LiteConfig {
    provider: LiteProvider,
    #[serde(default)]
    model: String,
    #[serde(default)]
    models: Vec<LiteModel>,
    #[serde(default)]
    context_window: String,
    #[serde(default)]
    auto_compact_token_limit: String,
    #[serde(default)]
    common_config: String,
    #[serde(default)]
    plan_hints: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
struct LiteProvider {
    id: String,
    #[serde(default)]
    name: String,
    base_url: String,
    #[serde(default)]
    api_key: String,
    #[serde(default)]
    api_key_env: String,
    #[serde(default)]
    mode: LiteMode,
    #[serde(default)]
    protocol: LiteProtocol,
    #[serde(default)]
    context_budget: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
#[serde(untagged)]
enum LiteModel {
    Id(String),
    Detailed {
        id: String,
        #[serde(default, rename = "contextWindow")]
        context_window: String,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Serialize, Default)]
#[serde(rename_all = "snake_case")]
enum LiteMode {
    PureApi,
    #[default]
    MixedApi,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Serialize, Default)]
#[serde(rename_all = "snake_case")]
enum LiteProtocol {
    #[default]
    Responses,
    #[serde(alias = "chatCompletions", alias = "chat-completions")]
    ChatCompletions,
}

impl LiteProtocol {
    fn to_proxy(self) -> protocol_proxy::RelayProtocol {
        match self {
            Self::Responses => protocol_proxy::RelayProtocol::Responses,
            Self::ChatCompletions => protocol_proxy::RelayProtocol::ChatCompletions,
        }
    }
}

#[derive(Debug)]
enum Command {
    Apply {
        config_path: PathBuf,
        codex_home: Option<PathBuf>,
        reload: bool,
        debug_port: u16,
        plan_ui: bool,
    },
    Doctor {
        config_path: PathBuf,
    },
    Reload {
        debug_port: u16,
        plan_ui: bool,
    },
    InjectPlanUi {
        debug_port: u16,
    },
    Watch {
        config_path: PathBuf,
        codex_home: Option<PathBuf>,
        debug_port: u16,
        interval_ms: u64,
    },
    Agent {
        config_path: PathBuf,
        codex_home: Option<PathBuf>,
        app_path: Option<PathBuf>,
        debug_port: u16,
        plan_ui: bool,
        interval_ms: u64,
    },
    Launch {
        config_path: Option<PathBuf>,
        codex_home: Option<PathBuf>,
        app_path: Option<PathBuf>,
        debug_port: u16,
        plan_ui: bool,
    },
    InstallAgent {
        config_path: PathBuf,
        codex_home: Option<PathBuf>,
        app_path: Option<PathBuf>,
        debug_port: u16,
        plan_ui: bool,
        interval_ms: u64,
    },
    StopAgent,
    UninstallAgent,
    Init {
        config_path: PathBuf,
        force: bool,
    },
    WhereApp {
        app_path: Option<PathBuf>,
    },
    Help,
}

#[tokio::main]
async fn main() {
    if let Err(error) = run_cli().await {
        eprintln!("codex-gateway-lite 失败：{error:#}");
        std::process::exit(1);
    }
}

async fn run_cli() -> anyhow::Result<()> {
    match parse_args()? {
        Command::Apply {
            config_path,
            codex_home,
            reload,
            debug_port,
            plan_ui,
        } => {
            maybe_refresh_config_models(&config_path).await;
            let config = read_lite_config(&config_path)?;
            let report = apply_config(&config, codex_home.as_deref(), config.plan_hints)?;
            print_apply_report(&report);
            if reload {
                soft_reload_codex(debug_port).await?;
                wait_and_inject_lite_model_whitelist(debug_port, codex_home.as_deref()).await?;
                if plan_ui {
                    wait_and_inject_plan_ui(debug_port, codex_home.as_deref()).await?;
                }
            }
        }
        Command::Doctor { config_path } => {
            let config = read_lite_config(&config_path)?;
            doctor(&config).await?;
        }
        Command::Reload {
            debug_port,
            plan_ui,
        } => {
            soft_reload_codex(debug_port).await?;
            wait_and_inject_lite_model_whitelist(debug_port, None).await?;
            if plan_ui {
                wait_and_inject_plan_ui(debug_port, None).await?;
            }
        }
        Command::InjectPlanUi { debug_port } => {
            wait_and_inject_plan_ui(debug_port, None).await?;
        }
        Command::Watch {
            config_path,
            codex_home,
            debug_port,
            interval_ms,
        } => {
            watch(config_path, codex_home, debug_port, interval_ms).await?;
        }
        Command::Agent {
            config_path,
            codex_home,
            app_path,
            debug_port,
            plan_ui,
            interval_ms,
        } => {
            run_agent(
                config_path,
                codex_home,
                app_path,
                debug_port,
                plan_ui,
                interval_ms,
            )
            .await?;
        }
        Command::Launch {
            config_path,
            codex_home,
            app_path,
            debug_port,
            plan_ui,
        } => {
            if let Some(config_path) = config_path {
                maybe_refresh_config_models(&config_path).await;
                let config = read_lite_config(&config_path)?;
                let report = apply_config(&config, codex_home.as_deref(), config.plan_hints)?;
                print_apply_report(&report);
            }
            let app_dir = resolve_codex_app(app_path.as_deref())?;
            launch_codex(&app_dir, debug_port, codex_home.as_deref()).await?;
            wait_and_inject_lite_model_whitelist(debug_port, codex_home.as_deref()).await?;
            if plan_ui {
                wait_and_inject_plan_ui(debug_port, codex_home.as_deref()).await?;
            }
        }
        Command::InstallAgent {
            config_path,
            codex_home,
            app_path,
            debug_port,
            plan_ui,
            interval_ms,
        } => {
            install_persistent_agent(
                &config_path,
                codex_home.as_deref(),
                app_path.as_deref(),
                debug_port,
                plan_ui,
                interval_ms,
            )?;
        }
        Command::StopAgent => {
            stop_agent_service()?;
        }
        Command::UninstallAgent => {
            uninstall_persistent_agent()?;
        }
        Command::Init { config_path, force } => {
            init_config(&config_path, force).await?;
        }
        Command::WhereApp { app_path } => {
            let app_dir = resolve_codex_app(app_path.as_deref())?;
            println!("{}", app_dir.display());
            if let Some(version) = codex_lite::codex_app_version(&app_dir) {
                println!("version: {version}");
            }
        }
        Command::Help => print_help(),
    }
    Ok(())
}

fn parse_args() -> anyhow::Result<Command> {
    let mut args = std::env::args().skip(1).collect::<Vec<_>>();
    if args.is_empty() {
        return Ok(Command::Help);
    }
    let command = args.remove(0);
    match command.as_str() {
        "apply" => {
            let config_path = take_path_arg(&mut args, "--config")?;
            let codex_home = take_optional_path_arg(&mut args, "--codex-home")?;
            let reload = take_flag(&mut args, "--reload");
            let debug_port = take_u16_arg(&mut args, "--debug-port", 9229)?;
            let plan_ui = !take_flag(&mut args, "--no-plan-ui");
            reject_unknown_args(&args)?;
            Ok(Command::Apply {
                config_path,
                codex_home,
                reload,
                debug_port,
                plan_ui,
            })
        }
        "doctor" => {
            let config_path = take_path_arg(&mut args, "--config")?;
            reject_unknown_args(&args)?;
            Ok(Command::Doctor { config_path })
        }
        "reload" => {
            let debug_port = take_u16_arg(&mut args, "--debug-port", 9229)?;
            let plan_ui = !take_flag(&mut args, "--no-plan-ui");
            reject_unknown_args(&args)?;
            Ok(Command::Reload {
                debug_port,
                plan_ui,
            })
        }
        "inject-plan-ui" => {
            let debug_port = take_u16_arg(&mut args, "--debug-port", 9229)?;
            reject_unknown_args(&args)?;
            Ok(Command::InjectPlanUi { debug_port })
        }
        "watch" => {
            let config_path = take_path_arg(&mut args, "--config")?;
            let codex_home = take_optional_path_arg(&mut args, "--codex-home")?;
            let debug_port = take_u16_arg(&mut args, "--debug-port", 9229)?;
            let interval_ms = take_u64_arg(&mut args, "--interval-ms", 1200)?;
            reject_unknown_args(&args)?;
            Ok(Command::Watch {
                config_path,
                codex_home,
                debug_port,
                interval_ms,
            })
        }
        "agent" => {
            let config_path = take_optional_path_arg(&mut args, "--config")?
                .unwrap_or_else(default_user_config_path);
            let codex_home = take_optional_path_arg(&mut args, "--codex-home")?;
            let app_path = take_optional_path_arg(&mut args, "--app")?;
            let debug_port = take_u16_arg(&mut args, "--debug-port", 9229)?;
            let interval_ms = take_u64_arg(&mut args, "--interval-ms", 1000)?;
            let plan_ui = !take_flag(&mut args, "--no-plan-ui");
            reject_unknown_args(&args)?;
            Ok(Command::Agent {
                config_path,
                codex_home,
                app_path,
                debug_port,
                plan_ui,
                interval_ms,
            })
        }
        "launch" => {
            let config_path = take_optional_path_arg(&mut args, "--config")?;
            let codex_home = take_optional_path_arg(&mut args, "--codex-home")?;
            let app_path = take_optional_path_arg(&mut args, "--app")?;
            let debug_port = take_u16_arg(&mut args, "--debug-port", 9229)?;
            let plan_ui = !take_flag(&mut args, "--no-plan-ui");
            reject_unknown_args(&args)?;
            Ok(Command::Launch {
                config_path,
                codex_home,
                app_path,
                debug_port,
                plan_ui,
            })
        }
        "install-agent" => {
            let config_path = take_optional_path_arg(&mut args, "--config")?
                .unwrap_or_else(default_user_config_path);
            let codex_home = take_optional_path_arg(&mut args, "--codex-home")?;
            let app_path = take_optional_path_arg(&mut args, "--app")?;
            let debug_port = take_u16_arg(&mut args, "--debug-port", 9229)?;
            let interval_ms = take_u64_arg(&mut args, "--interval-ms", 1000)?;
            let plan_ui = !take_flag(&mut args, "--no-plan-ui");
            reject_unknown_args(&args)?;
            Ok(Command::InstallAgent {
                config_path,
                codex_home,
                app_path,
                debug_port,
                plan_ui,
                interval_ms,
            })
        }
        "uninstall-agent" => {
            reject_unknown_args(&args)?;
            Ok(Command::UninstallAgent)
        }
        "stop-agent" => {
            reject_unknown_args(&args)?;
            Ok(Command::StopAgent)
        }
        "init" => {
            let config_path = take_optional_path_arg(&mut args, "--config")?
                .unwrap_or_else(default_user_config_path);
            let force = take_flag(&mut args, "--force");
            reject_unknown_args(&args)?;
            Ok(Command::Init { config_path, force })
        }
        "where-app" => {
            let app_path = take_optional_path_arg(&mut args, "--app")?;
            reject_unknown_args(&args)?;
            Ok(Command::WhereApp { app_path })
        }
        "-h" | "--help" | "help" => Ok(Command::Help),
        other => bail!("未知命令：{other}"),
    }
}

fn take_path_arg(args: &mut Vec<String>, name: &str) -> anyhow::Result<PathBuf> {
    take_string_arg(args, name)
        .map(PathBuf::from)
        .with_context(|| format!("缺少 {name} 参数"))
}

fn take_optional_path_arg(args: &mut Vec<String>, name: &str) -> anyhow::Result<Option<PathBuf>> {
    Ok(take_optional_string_arg(args, name)?.map(PathBuf::from))
}

fn take_u16_arg(args: &mut Vec<String>, name: &str, default: u16) -> anyhow::Result<u16> {
    match take_optional_string_arg(args, name)? {
        Some(value) => value
            .parse::<u16>()
            .with_context(|| format!("{name} 必须是 1-65535 的端口号")),
        None => Ok(default),
    }
}

fn take_u64_arg(args: &mut Vec<String>, name: &str, default: u64) -> anyhow::Result<u64> {
    match take_optional_string_arg(args, name)? {
        Some(value) => value
            .parse::<u64>()
            .with_context(|| format!("{name} 必须是正整数")),
        None => Ok(default),
    }
}

fn take_string_arg(args: &mut Vec<String>, name: &str) -> anyhow::Result<String> {
    take_optional_string_arg(args, name)?.ok_or_else(|| anyhow::anyhow!("缺少 {name} 参数"))
}

fn take_optional_string_arg(args: &mut Vec<String>, name: &str) -> anyhow::Result<Option<String>> {
    let Some(index) = args.iter().position(|arg| arg == name) else {
        return Ok(None);
    };
    args.remove(index);
    if index >= args.len() {
        bail!("{name} 后面需要跟一个值");
    }
    Ok(Some(args.remove(index)))
}

fn take_flag(args: &mut Vec<String>, name: &str) -> bool {
    if let Some(index) = args.iter().position(|arg| arg == name) {
        args.remove(index);
        true
    } else {
        false
    }
}

fn reject_unknown_args(args: &[String]) -> anyhow::Result<()> {
    if let Some(arg) = args.first() {
        bail!("未知参数：{arg}");
    }
    Ok(())
}

fn read_lite_config(path: &Path) -> anyhow::Result<LiteConfig> {
    let contents =
        fs::read_to_string(path).with_context(|| format!("读取配置失败：{}", path.display()))?;
    serde_json::from_str(&contents)
        .with_context(|| format!("配置不是合法 JSON：{}", path.display()))
}

fn default_user_config_path() -> PathBuf {
    user_home_dir()
        .unwrap_or_else(|| PathBuf::from("."))
        .join(".codex-gateway-lite")
        .join("config.json")
}

fn user_home_dir() -> Option<PathBuf> {
    std::env::var_os("HOME")
        .or_else(|| std::env::var_os("USERPROFILE"))
        .map(PathBuf::from)
}

struct AgentInstanceLock {
    path: PathBuf,
    pid: u32,
}

impl Drop for AgentInstanceLock {
    fn drop(&mut self) {
        let current = fs::read_to_string(&self.path)
            .ok()
            .and_then(|value| value.trim().parse::<u32>().ok());
        if current == Some(self.pid) {
            let _ = fs::remove_file(&self.path);
        }
    }
}

fn agent_lock_path() -> PathBuf {
    user_home_dir()
        .unwrap_or_else(|| PathBuf::from("."))
        .join(".codex-gateway-lite")
        .join("agent.lock")
}

fn acquire_agent_instance_lock() -> anyhow::Result<Option<AgentInstanceLock>> {
    let path = agent_lock_path();
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)
            .with_context(|| format!("创建 agent lock 目录失败：{}", parent.display()))?;
    }
    let pid = std::process::id();
    loop {
        match fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&path)
        {
            Ok(mut file) => {
                writeln!(file, "{pid}")?;
                return Ok(Some(AgentInstanceLock { path, pid }));
            }
            Err(error) if error.kind() == io::ErrorKind::AlreadyExists => {
                let existing_pid = fs::read_to_string(&path)
                    .ok()
                    .and_then(|value| value.trim().parse::<u32>().ok());
                if existing_pid.is_some_and(process_is_running) {
                    println!(
                        "Codex Gateway Lite agent 已在运行，跳过重复启动：pid={}",
                        existing_pid.unwrap()
                    );
                    return Ok(None);
                }
                let _ = fs::remove_file(&path);
            }
            Err(error) => {
                return Err(error)
                    .with_context(|| format!("创建 agent lock 失败：{}", path.display()));
            }
        }
    }
}

fn process_is_running(pid: u32) -> bool {
    if pid == std::process::id() {
        return true;
    }
    #[cfg(unix)]
    {
        ProcessCommand::new("kill")
            .arg("-0")
            .arg(pid.to_string())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .status()
            .map(|status| status.success())
            .unwrap_or(false)
    }
    #[cfg(windows)]
    {
        process_is_running_windows(pid)
    }
    #[cfg(all(not(unix), not(windows)))]
    {
        false
    }
}

#[cfg(windows)]
fn process_is_running_windows(pid: u32) -> bool {
    let output = ProcessCommand::new("tasklist")
        .args(["/FI", &format!("PID eq {pid}"), "/FO", "CSV", "/NH"])
        .output();
    let Ok(output) = output else {
        return false;
    };
    if !output.status.success() {
        return false;
    }
    let text = String::from_utf8_lossy(&output.stdout);
    windows_tasklist_pid_exists(&text, pid)
}

#[cfg(any(windows, test))]
fn windows_tasklist_pid_exists(output: &str, pid: u32) -> bool {
    output.lines().any(|line| {
        parse_windows_csv_line(line)
            .get(1)
            .and_then(|field| field.trim().parse::<u32>().ok())
            == Some(pid)
    })
}

#[cfg(any(windows, test))]
fn parse_windows_csv_line(line: &str) -> Vec<String> {
    let mut fields = Vec::new();
    let mut field = String::new();
    let mut chars = line.chars().peekable();
    let mut in_quotes = false;
    while let Some(ch) = chars.next() {
        match ch {
            '"' if in_quotes && chars.peek() == Some(&'"') => {
                field.push('"');
                chars.next();
            }
            '"' => in_quotes = !in_quotes,
            ',' if !in_quotes => {
                fields.push(field.trim().to_string());
                field.clear();
            }
            _ => field.push(ch),
        }
    }
    fields.push(field.trim().to_string());
    fields
}

fn remove_agent_lock_file() -> anyhow::Result<bool> {
    let path = agent_lock_path();
    match fs::remove_file(&path) {
        Ok(()) => Ok(true),
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(false),
        Err(error) => {
            Err(error).with_context(|| format!("删除 agent lock 失败：{}", path.display()))
        }
    }
}

fn default_user_codex_home_dir() -> PathBuf {
    user_home_dir()
        .unwrap_or_else(|| PathBuf::from("."))
        .join(".codex")
}

fn resolve_codex_home(codex_home: Option<&Path>) -> PathBuf {
    codex_home
        .map(Path::to_path_buf)
        .unwrap_or_else(default_user_codex_home_dir)
}

struct ApplyReport {
    codex_home: PathBuf,
    config_path: PathBuf,
    auth_path: PathBuf,
    catalog_path: PathBuf,
    provider_id: String,
    model_count: usize,
    session_file_count: Option<usize>,
    thread_catalog_sync: Option<ThreadCatalogSyncReport>,
    backup_path: Option<String>,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
struct ThreadCatalogSyncReport {
    sources_seen: usize,
    threads_seen: usize,
    catalog_targets: usize,
    catalog_inserted: usize,
    catalog_updated: usize,
}

fn apply_config(
    config: &LiteConfig,
    codex_home: Option<&Path>,
    plan_hints: bool,
) -> anyhow::Result<ApplyReport> {
    let home = resolve_codex_home(codex_home);
    let profile = build_profile(config)?;
    let config_contents = build_preserving_config_contents(&home, &profile, &config.common_config)?;
    write_lite_model_catalog(&home, &profile, plan_hints)?;
    let result = codex_lite::apply_relay_config_file_to_home(&home, &config_contents)?;
    let catalog_path = home
        .join("model-catalogs")
        .join(format!("{}.json", sanitize_catalog_filename(&profile.id)));
    let session_file_count = count_session_files(&home).ok();
    let thread_catalog_sync = match sync_local_thread_catalog_for_provider(&home, Some(&profile.id))
    {
        Ok(report) => Some(report),
        Err(error) => {
            eprintln!("同步 Codex 历史会话索引失败，继续启动：{error:#}");
            None
        }
    };
    Ok(ApplyReport {
        config_path: home.join("config.toml"),
        auth_path: home.join("auth.json"),
        catalog_path,
        codex_home: home,
        provider_id: profile.id,
        model_count: profile
            .model_list
            .lines()
            .filter(|line| !line.trim().is_empty())
            .count(),
        session_file_count,
        thread_catalog_sync,
        backup_path: result.backup_path,
    })
}

fn build_preserving_config_contents(
    home: &Path,
    profile: &RelayProfile,
    common_config: &str,
) -> anyhow::Result<String> {
    let config_path = home.join("config.toml");
    let existing = fs::read_to_string(&config_path)
        .or_else(|error| {
            if error.kind() == io::ErrorKind::NotFound {
                Ok(String::new())
            } else {
                Err(error)
            }
        })
        .with_context(|| format!("读取现有 Codex 配置失败：{}", config_path.display()))?;
    let with_common = merge_common_config_into_config(&existing, common_config)?;
    merge_lite_profile_into_config(&with_common, profile)
}

fn merge_lite_profile_into_config(
    existing_config: &str,
    profile: &RelayProfile,
) -> anyhow::Result<String> {
    let mut doc = parse_toml_document(existing_config, "现有 Codex config.toml")?;
    let provider_id = profile.id.trim();
    if provider_id.is_empty() {
        bail!("provider id 不能为空");
    }

    doc["model_provider"] = toml_edit::value(provider_id);
    if !profile.model.trim().is_empty() {
        doc["model"] = toml_edit::value(profile.model.trim());
    }
    if let Some(value) = parse_window_token(&profile.context_window) {
        doc["model_context_window"] = toml_edit::value(value as i64);
    }
    if let Some(value) = parse_window_token(&profile.auto_compact_limit) {
        doc["model_auto_compact_token_limit"] = toml_edit::value(value as i64);
    }
    doc["model_catalog_json"] = toml_edit::value(format!(
        "model-catalogs/{}.json",
        sanitize_catalog_filename(provider_id)
    ));

    if doc
        .get("model_providers")
        .and_then(toml_edit::Item::as_table)
        .is_none()
    {
        doc["model_providers"] = toml_edit::table();
    }
    let providers = doc["model_providers"]
        .as_table_mut()
        .ok_or_else(|| anyhow::anyhow!("model_providers 必须是 TOML table"))?;
    if providers
        .get(provider_id)
        .and_then(toml_edit::Item::as_table)
        .is_none()
    {
        providers.insert(provider_id, toml_edit::table());
    }
    let provider = providers
        .get_mut(provider_id)
        .and_then(toml_edit::Item::as_table_mut)
        .ok_or_else(|| anyhow::anyhow!("model_providers.{provider_id} 必须是 TOML table"))?;
    provider["name"] = toml_edit::value(if profile.name.trim().is_empty() {
        provider_id
    } else {
        profile.name.trim()
    });
    provider["wire_api"] = toml_edit::value("responses");
    provider["requires_openai_auth"] = toml_edit::value(true);
    if !profile.base_url.trim().is_empty() {
        provider["base_url"] = toml_edit::value(profile.base_url.trim());
    }
    if !profile.api_key.trim().is_empty() {
        provider["experimental_bearer_token"] = toml_edit::value(profile.api_key.trim());
    }

    Ok(ensure_trailing_newline(doc.to_string()))
}

fn parse_toml_document(contents: &str, label: &str) -> anyhow::Result<toml_edit::DocumentMut> {
    let contents = contents.strip_prefix('\u{feff}').unwrap_or(contents);
    if contents.trim().is_empty() {
        Ok(toml_edit::DocumentMut::new())
    } else {
        contents
            .parse::<toml_edit::DocumentMut>()
            .with_context(|| format!("解析 {label} 失败"))
    }
}

fn merge_common_config_into_config(
    config_text: &str,
    common_config: &str,
) -> anyhow::Result<String> {
    let sanitized = sanitize_common_config_contents(common_config)?;
    if sanitized.trim().is_empty() {
        return Ok(ensure_trailing_newline(config_text.to_string()));
    }

    let mut target = parse_toml_document(config_text, "现有 Codex config.toml")?;
    let source = parse_toml_document(&sanitized, "commonConfig")?;
    merge_toml_table_like(target.as_table_mut(), source.as_table());
    Ok(ensure_trailing_newline(target.to_string()))
}

fn extract_common_config_from_config(config_text: &str) -> anyhow::Result<String> {
    let mut doc = parse_toml_document(config_text, "Codex config.toml")?;
    remove_provider_specific_common_keys(doc.as_table_mut());
    let contents = doc.to_string();
    if contents.trim().is_empty() {
        Ok(String::new())
    } else {
        Ok(ensure_trailing_newline(contents))
    }
}

fn extract_common_config_from_home(home: &Path) -> anyhow::Result<String> {
    let config_path = home.join("config.toml");
    let contents = match fs::read_to_string(&config_path) {
        Ok(contents) => contents,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(String::new()),
        Err(error) => {
            return Err(error)
                .with_context(|| format!("读取 Codex 公共配置失败：{}", config_path.display()));
        }
    };
    extract_common_config_from_config(&contents)
}

fn sanitize_common_config_contents(common_config: &str) -> anyhow::Result<String> {
    if common_config.trim().is_empty() {
        return Ok(String::new());
    }
    let mut doc = parse_toml_document(common_config, "commonConfig")?;
    remove_provider_specific_common_keys(doc.as_table_mut());
    let contents = doc.to_string();
    if contents.trim().is_empty() {
        Ok(String::new())
    } else {
        Ok(ensure_trailing_newline(contents))
    }
}

fn remove_provider_specific_common_keys(table: &mut toml_edit::Table) {
    for key in ["model", "model_provider", "base_url", "model_catalog_json"] {
        table.remove(key);
    }
    table.remove("model_providers");
}

fn merge_toml_item(target: &mut toml_edit::Item, source: &toml_edit::Item) {
    if let Some(source_table) = source.as_table_like() {
        if let Some(target_table) = target.as_table_like_mut() {
            merge_toml_table_like(target_table, source_table);
            return;
        }
    }
    *target = source.clone();
}

fn merge_toml_table_like(target: &mut dyn toml_edit::TableLike, source: &dyn toml_edit::TableLike) {
    for (key, source_item) in source.iter() {
        match target.get_mut(key) {
            Some(target_item) => merge_toml_item(target_item, source_item),
            None => {
                target.insert(key, source_item.clone());
            }
        }
    }
}

fn write_lite_model_catalog(
    home: &Path,
    profile: &RelayProfile,
    plan_hints: bool,
) -> anyhow::Result<()> {
    let catalog_path = home.join("model-catalogs").join(format!(
        "{}.json",
        sanitize_catalog_filename(profile.id.trim())
    ));
    if let Some(parent) = catalog_path.parent() {
        fs::create_dir_all(parent)
            .with_context(|| format!("创建 catalog 目录失败：{}", parent.display()))?;
    }
    let model_windows: HashMap<String, String> =
        serde_json::from_str(&profile.model_windows).unwrap_or_default();
    let entries =
        codex_lite::collect_catalog_entries(&profile.model_list, &model_windows, &profile.model);
    let fallback_window = parse_window_token(&profile.context_window);
    let catalog_json = codex_lite::build_model_catalog_json(&entries, fallback_window, plan_hints);
    fs::write(&catalog_path, catalog_json)
        .with_context(|| format!("写入模型 catalog 失败：{}", catalog_path.display()))?;
    Ok(())
}

fn ensure_trailing_newline(value: String) -> String {
    if value.ends_with('\n') {
        value
    } else {
        format!("{value}\n")
    }
}

fn build_profile(config: &LiteConfig) -> anyhow::Result<RelayProfile> {
    validate_config(config)?;
    let upstream_api_key = resolve_api_key(&config.provider)?;
    let codex_api_key = if provider_uses_local_proxy(config) {
        LOCAL_PROXY_CODEX_BEARER_TOKEN.to_string()
    } else {
        upstream_api_key
    };
    let (model_list, model_windows) = split_models(config);
    let model_windows_json = serde_json::to_string(&model_windows)?;
    let context_window = normalize_window_for_config(&config.context_window)?;
    let auto_compact_token_limit = normalize_window_for_config(&config.auto_compact_token_limit)?;
    if matches!(config.provider.mode, LiteMode::PureApi) {
        bail!("lite 已禁用 pure_api，避免覆盖 Codex 登录态；请重新运行 init 同步为 mixed_api");
    }
    let first_model = model_list
        .lines()
        .next()
        .map(str::trim)
        .unwrap_or("")
        .to_string();
    let requested_model = strip_suffix(config.model.trim()).0;
    let model = if requested_model.is_empty() {
        first_model
    } else if model_list
        .lines()
        .any(|line| line.trim() == requested_model)
    {
        requested_model
    } else {
        eprintln!(
            "配置里的默认模型不在供应商模型列表中，已改用第一项真实模型：{}",
            first_model
        );
        first_model
    };

    Ok(RelayProfile {
        id: sanitize_provider_id(&config.provider.id),
        name: config
            .provider
            .name
            .trim()
            .is_empty()
            .then(|| config.provider.id.trim().to_string())
            .unwrap_or_else(|| config.provider.name.trim().to_string()),
        model,
        base_url: codex_provider_base_url(config),
        api_key: codex_api_key,
        context_window,
        auto_compact_limit: auto_compact_token_limit,
        model_list,
        model_windows: model_windows_json,
    })
}

fn provider_uses_local_proxy(config: &LiteConfig) -> bool {
    config.provider.protocol == LiteProtocol::ChatCompletions
        || parse_context_budget_token(&config.provider.context_budget).is_some()
}

fn codex_provider_base_url(config: &LiteConfig) -> String {
    if provider_uses_local_proxy(config) {
        protocol_proxy::local_responses_proxy_base_url(protocol_proxy::DEFAULT_PROTOCOL_PROXY_PORT)
    } else {
        config
            .provider
            .base_url
            .trim()
            .trim_end_matches('/')
            .to_string()
    }
}

fn validate_config(config: &LiteConfig) -> anyhow::Result<()> {
    if config.provider.id.trim().is_empty() {
        bail!("provider.id 不能为空");
    }
    if config.provider.base_url.trim().is_empty() {
        bail!("provider.baseUrl 不能为空");
    }
    if config.models.is_empty() {
        bail!("models 为空；请先运行 init 或确认供应商 /models 可访问");
    }
    if resolve_api_key(&config.provider)?.trim().is_empty() {
        bail!("API Key 为空；请设置 provider.apiKey 或 provider.apiKeyEnv");
    }
    validate_context_budget_for_config(&config.provider.context_budget)?;
    Ok(())
}

async fn init_config(path: &Path, force: bool) -> anyhow::Result<()> {
    if path.exists() && !force {
        match read_lite_config(path) {
            Ok(config) => match validate_config(&config) {
                Ok(()) => {
                    println!("配置已存在，开始同步供应商模型列表：{}", path.display());
                    print_context_budget_notice(&config);
                    match refresh_config_models(path).await {
                        Ok(count) => println!("供应商模型列表已同步：{count} 个模型"),
                        Err(error) => eprintln!("供应商模型列表同步失败，保留现有配置：{error:#}"),
                    }
                    return Ok(());
                }
                Err(error) => {
                    println!("检测到配置不完整或不可用：{error:#}");
                    println!("进入首次配置流程。");
                }
            },
            Err(error) => {
                println!("检测到配置不完整或不可用：{error:#}");
                println!("进入首次配置流程。");
            }
        }
    }

    println!("Codex Gateway Lite 首次配置");
    println!("配置会写入：{}", path.display());
    let provider_id = prompt_default("供应商 ID", "gateway")?;
    let provider_name = prompt_default("供应商名称", "Gateway")?;
    let base_url = prompt_required("Base URL，例如 https://api.example.com/v1")?;
    let api_key = prompt_required("API Key（会保存到用户目录私有配置文件，不写入 Git）")?;
    println!("供应商协议：");
    println!("  1. Responses API — 供应商原生支持 OpenAI Responses 格式，Codex 直连");
    println!("  2. Chat Completions — 供应商只支持 Chat 格式，由本地代理自动转换");
    let protocol_choice = prompt_default("请选择", "1")?;
    let protocol = match protocol_choice.trim() {
        "2" => LiteProtocol::ChatCompletions,
        _ => LiteProtocol::Responses,
    };
    let context_budget = prompt_context_budget()?;

    println!("任务清单指引（Plan Hints）：");
    println!("  开启后，模型 catalog 的 base_instructions 会追加一段指引，");
    println!("  告诉第三方模型在多步骤任务中主动调用 update_plan 显示进度面板。");
    println!("  内容写在本地 ~/.codex/model-catalogs/ 明文文件中，可随时查看和修改。");
    let plan_hints_choice = prompt_default("是否开启 planHints (y/n)", "n")?;
    let plan_hints = matches!(plan_hints_choice.trim(), "y" | "Y" | "yes" | "Yes" | "YES");

    let provider = LiteProvider {
        id: provider_id,
        name: provider_name,
        base_url,
        api_key,
        api_key_env: String::new(),
        mode: LiteMode::MixedApi,
        protocol,
        context_budget,
    };
    println!("正在从供应商拉取模型列表...");
    let model_ids = fetch_provider_model_ids(&provider).await?;
    if model_ids.is_empty() {
        bail!("供应商 /models 没有返回任何模型");
    }
    let models = models_from_ids(&model_ids);
    let common_config = match extract_common_config_from_home(&default_user_codex_home_dir()) {
        Ok(common_config) => {
            if !common_config.trim().is_empty() {
                println!("已从默认 Codex home 捕获公共配置片段，后续会随 apply 保守合并。");
            }
            common_config
        }
        Err(error) => {
            eprintln!("捕获默认 Codex 公共配置失败，继续创建基础配置：{error:#}");
            String::new()
        }
    };
    println!("已拉取 {} 个模型。", models.len());
    let default_model = choose_default_model(&model_ids)?;
    println!("默认模型：{}", default_model);

    let config = LiteConfig {
        provider,
        model: default_model,
        models,
        context_window: "1M".to_string(),
        auto_compact_token_limit: String::new(),
        common_config,
        plan_hints,
    };

    write_lite_config(path, &config)?;
    println!("配置已写入：{}", path.display());
    Ok(())
}

fn choose_default_model(model_ids: &[String]) -> anyhow::Result<String> {
    println!("默认模型列表：");
    for (index, model_id) in model_ids.iter().enumerate() {
        println!("  {}. {}", index + 1, model_id);
    }
    loop {
        let choice = prompt_default("请输入默认模型编号", "1")?;
        if let Some(model) = model_from_numbered_choice(model_ids, &choice) {
            return Ok(model);
        }
        println!("输入无效，请输入 1~{} 之间的数字。", model_ids.len());
    }
}

fn model_from_numbered_choice(model_ids: &[String], choice: &str) -> Option<String> {
    let choice = choice.trim();
    if choice.is_empty() {
        return model_ids.first().cloned();
    }
    let index = choice.parse::<usize>().ok()?;
    if index == 0 || index > model_ids.len() {
        return None;
    }
    model_ids.get(index - 1).cloned()
}

fn print_context_budget_notice(config: &LiteConfig) {
    let budget = config.provider.context_budget.trim();
    if let Some(tokens) = parse_context_budget_token(budget) {
        let limit = explicit_context_budget_limit(tokens, &config.context_window);
        println!(
            "本地裁剪余量：{}；agent 会启动 127.0.0.1:57321，并在估算输入超过窗口安全线时裁掉旧上下文。当前发送目标上限约 {}。若要 Responses 完全直连，请将 provider.contextBudget 设为空或 off。",
            format_token_budget(tokens),
            format_token_budget(limit)
        );
        return;
    }
    if config.provider.protocol == LiteProtocol::Responses {
        println!(
            "上下文裁剪余量：未设置；Responses 保持直连，不启动本地代理。如需发送前兜底裁剪，请运行 init --force 并填写 {SUGGESTED_CONTEXT_BUDGET}。"
        );
    } else {
        println!(
            "上下文裁剪余量：未设置；Chat Completions 本地代理会按 contextWindow 自动推导预算；如需更大安全余量可显式填写 {SUGGESTED_CONTEXT_BUDGET}。"
        );
    }
}

fn prompt_context_budget() -> anyhow::Result<String> {
    println!("本地裁剪余量说明：");
    println!(
        "  - 这是发送给上游前预留的上下文安全余量，不是 KB 文件大小；启用后一定会走 127.0.0.1:57321 本地代理。"
    );
    println!(
        "  - 例如 contextWindow=1M 且填写 {SUGGESTED_CONTEXT_BUDGET}，代理会在发送目标超过约 800K 时裁掉旧上下文。"
    );
    println!("  - 可输入 200、200K、200KB、200000；纯数字 200 会按 200K 处理。");
    println!("  - 关闭显式预算请输入 off，Responses 模式会继续直连。");
    let raw = prompt_default("本地裁剪余量", DEFAULT_CONTEXT_BUDGET)?;
    normalize_context_budget_for_config(&raw)
}

fn write_lite_config(path: &Path, config: &LiteConfig) -> anyhow::Result<()> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)
            .with_context(|| format!("创建配置目录失败：{}", parent.display()))?;
    }
    let contents = serde_json::to_string_pretty(config)?;
    fs::write(path, format!("{contents}\n"))
        .with_context(|| format!("写入配置失败：{}", path.display()))?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        fs::set_permissions(path, fs::Permissions::from_mode(0o600))
            .with_context(|| format!("设置配置权限失败：{}", path.display()))?;
    }
    Ok(())
}

async fn refresh_config_models(path: &Path) -> anyhow::Result<usize> {
    let mut config = read_lite_config(path)?;
    let original_config = config.clone();
    let model_ids = fetch_provider_model_ids(&config.provider).await?;
    if model_ids.is_empty() {
        bail!("供应商 /models 没有返回任何模型");
    }
    let current_model = strip_suffix(config.model.trim()).0;
    config.models = models_from_ids(&model_ids);
    config.model = if model_ids.iter().any(|id| id == &current_model) {
        current_model
    } else {
        model_ids[0].clone()
    };
    if matches!(config.provider.mode, LiteMode::PureApi) {
        println!("已将 provider.mode 从 pure_api 调整为 mixed_api，以保留 Codex 登录态和历史入口");
        config.provider.mode = LiteMode::MixedApi;
    }
    match populate_missing_common_config_from_home(&mut config, &default_user_codex_home_dir()) {
        Ok(true) => println!("已从默认 Codex home 补齐 commonConfig 公共配置片段"),
        Ok(false) => {}
        Err(error) => eprintln!("补齐 commonConfig 失败，保留现有配置：{error:#}"),
    }
    if config != original_config {
        write_lite_config(path, &config)?;
    }
    Ok(model_ids.len())
}

fn populate_missing_common_config_from_home(
    config: &mut LiteConfig,
    home: &Path,
) -> anyhow::Result<bool> {
    if !config.common_config.trim().is_empty() {
        return Ok(false);
    }
    let common_config = extract_common_config_from_home(home)?;
    if common_config.trim().is_empty() {
        return Ok(false);
    }
    config.common_config = common_config;
    Ok(true)
}

fn models_from_ids(ids: &[String]) -> Vec<LiteModel> {
    ids.iter()
        .map(|id| LiteModel::Detailed {
            id: id.to_string(),
            context_window: default_context_window_for_model_id(id).to_string(),
        })
        .collect()
}

fn default_context_window_for_model_id(id: &str) -> &'static str {
    if is_gpt_family_model_id(id) {
        "258400"
    } else {
        "1M"
    }
}

fn is_gpt_family_model_id(id: &str) -> bool {
    let model = id
        .trim()
        .rsplit('/')
        .next()
        .unwrap_or("")
        .to_ascii_lowercase();
    model.starts_with("gpt-") || model.starts_with("chatgpt-")
}

fn prompt_required(label: &str) -> anyhow::Result<String> {
    loop {
        let value = prompt(label)?;
        if !value.trim().is_empty() {
            return Ok(value.trim().to_string());
        }
        println!("不能为空，请重新输入。");
    }
}

fn prompt_default(label: &str, default: &str) -> anyhow::Result<String> {
    let value = prompt(&format!("{label} [{default}]"))?;
    let trimmed = value.trim();
    if trimmed.is_empty() {
        Ok(default.to_string())
    } else {
        Ok(trimmed.to_string())
    }
}

fn prompt(label: &str) -> anyhow::Result<String> {
    print!("{label}: ");
    io::stdout().flush().context("刷新 stdout 失败")?;
    let mut input = String::new();
    io::stdin().read_line(&mut input).context("读取输入失败")?;
    Ok(input.trim_end_matches(['\r', '\n']).to_string())
}

fn resolve_api_key(provider: &LiteProvider) -> anyhow::Result<String> {
    if !provider.api_key.trim().is_empty() {
        return Ok(provider.api_key.trim().to_string());
    }
    if provider.api_key_env.trim().is_empty() {
        return Ok(String::new());
    }
    std::env::var(provider.api_key_env.trim())
        .with_context(|| format!("环境变量 {} 未设置", provider.api_key_env.trim()))
}

async fn fetch_provider_model_ids(provider: &LiteProvider) -> anyhow::Result<Vec<String>> {
    let api_key = resolve_api_key(provider)?;
    if api_key.trim().is_empty() {
        bail!("API Key 为空；请设置 provider.apiKey 或 provider.apiKeyEnv");
    }
    let base_url = provider.base_url.trim().trim_end_matches('/');
    if base_url.is_empty() {
        bail!("provider.baseUrl 不能为空");
    }
    let url = protocol_proxy::models_url(base_url);
    let response = reqwest::Client::builder()
        .timeout(Duration::from_secs(30))
        .build()?
        .get(&url)
        .bearer_auth(api_key)
        .send()
        .await
        .with_context(|| format!("请求模型列表失败：{url}"))?;
    let status = response.status();
    let body = response.text().await.unwrap_or_default();
    if !status.is_success() {
        bail!(
            "请求模型列表失败：HTTP {}，响应预览：{}",
            status.as_u16(),
            body.chars().take(500).collect::<String>()
        );
    }
    let value = serde_json::from_str::<serde_json::Value>(&body)
        .with_context(|| format!("模型列表响应不是合法 JSON：{url}"))?;
    let model_ids = parse_provider_model_ids(&value);
    if model_ids.is_empty() {
        bail!("模型列表响应里没有可用 id：{url}");
    }
    Ok(model_ids)
}

fn parse_provider_model_ids(value: &serde_json::Value) -> Vec<String> {
    let candidates = value
        .get("data")
        .or_else(|| value.get("models"))
        .and_then(serde_json::Value::as_array)
        .or_else(|| value.as_array());
    let Some(candidates) = candidates else {
        return Vec::new();
    };

    let mut ids = Vec::new();
    for item in candidates {
        let id = item
            .as_str()
            .or_else(|| item.get("id").and_then(serde_json::Value::as_str))
            .map(str::trim)
            .filter(|id| !id.is_empty());
        if let Some(id) = id {
            let id = id.to_string();
            if !ids.iter().any(|existing| existing == &id) {
                ids.push(id);
            }
        }
    }
    ids
}

fn split_models(config: &LiteConfig) -> (String, BTreeMap<String, String>) {
    let mut models = Vec::new();
    let mut windows = BTreeMap::new();
    for item in &config.models {
        let (id, window) = match item {
            LiteModel::Id(value) => {
                let (id, suffix_window) = strip_suffix(value);
                (id, suffix_window.unwrap_or_default())
            }
            LiteModel::Detailed { id, context_window } => {
                let (id, suffix_window) = strip_suffix(id);
                let window = if context_window.trim().is_empty() {
                    suffix_window.unwrap_or_default()
                } else {
                    context_window.trim().to_string()
                };
                (id, window)
            }
        };
        let id = id.trim();
        if id.is_empty() {
            continue;
        }
        if !models.iter().any(|existing: &String| existing == id) {
            models.push(id.to_string());
        }
        if !window.trim().is_empty() {
            windows.insert(id.to_string(), window.trim().to_string());
        }
    }
    if models.is_empty() && !config.model.trim().is_empty() {
        let (model, suffix_window) = strip_suffix(&config.model);
        models.push(model);
        if let Some(window) = suffix_window {
            windows.insert(models[0].clone(), window);
        }
    }
    (models.join("\n"), windows)
}

fn strip_suffix(value: &str) -> (String, Option<String>) {
    let (slug, window) = codex_lite::parse_model_suffix(value);
    (slug, window.map(|value| value.to_string()))
}

fn normalize_window_for_config(value: &str) -> anyhow::Result<String> {
    let trimmed = value.trim();
    if trimmed.is_empty() {
        return Ok(String::new());
    }
    parse_window_token(trimmed)
        .map(|value| value.to_string())
        .ok_or_else(|| anyhow::anyhow!("窗口值不合法：{trimmed}，请使用 1M / 200K / 1000000"))
}

fn normalize_context_budget_for_config(value: &str) -> anyhow::Result<String> {
    let trimmed = value.trim();
    if context_budget_is_disabled(trimmed) {
        return Ok(String::new());
    }
    parse_context_budget_token(trimmed)
        .map(format_token_budget)
        .ok_or_else(|| {
            anyhow::anyhow!(
                "上下文预算不合法：{trimmed}，请使用 200 / 200K / 200KB / 200000，或输入 off 关闭"
            )
        })
}

fn validate_context_budget_for_config(value: &str) -> anyhow::Result<()> {
    let trimmed = value.trim();
    if trimmed.is_empty() || context_budget_is_disabled(trimmed) {
        return Ok(());
    }
    parse_context_budget_token(trimmed).map(|_| ()).ok_or_else(|| {
        anyhow::anyhow!(
            "provider.contextBudget 不合法：{trimmed}，请使用 200 / 200K / 200KB / 200000，或 off/none/0 关闭"
        )
    })
}

fn parse_window_token(token: &str) -> Option<u64> {
    let token = token.trim();
    if token.is_empty() {
        return None;
    }
    let (number, multiplier) = match token.chars().last() {
        Some('K' | 'k') => (&token[..token.len() - 1], 1_000u64),
        Some('M' | 'm') => (&token[..token.len() - 1], 1_000_000u64),
        Some(_) => (token, 1u64),
        None => return None,
    };
    number
        .trim()
        .parse::<u64>()
        .ok()
        .map(|value| value * multiplier)
        .filter(|value| *value > 0)
}

fn parse_context_budget_token(token: &str) -> Option<u64> {
    let compact = token
        .trim()
        .chars()
        .filter(|ch| !ch.is_whitespace() && *ch != '_')
        .collect::<String>()
        .to_ascii_lowercase();
    if compact.is_empty() || context_budget_is_disabled(&compact) {
        return None;
    }

    let (number, multiplier) = if let Some(number) = compact.strip_suffix("kb") {
        (number, 1_000u64)
    } else if let Some(number) = compact.strip_suffix('k') {
        (number, 1_000u64)
    } else if let Some(number) = compact.strip_suffix("mb") {
        (number, 1_000_000u64)
    } else if let Some(number) = compact.strip_suffix('m') {
        (number, 1_000_000u64)
    } else {
        (compact.as_str(), 1u64)
    };

    let raw = number.trim().parse::<u64>().ok()?;
    let tokens = if multiplier == 1 && raw <= 512 {
        raw.saturating_mul(1_000)
    } else {
        raw.saturating_mul(multiplier)
    };
    (tokens > 0).then_some(tokens)
}

fn context_budget_is_disabled(value: &str) -> bool {
    matches!(
        value.trim().to_ascii_lowercase().as_str(),
        "" | "0" | "off" | "none" | "no" | "false" | "disable" | "disabled"
    )
}

fn format_token_budget(tokens: u64) -> String {
    if tokens >= 1_000_000 && tokens % 1_000_000 == 0 {
        format!("{}M", tokens / 1_000_000)
    } else if tokens >= 1_000 && tokens % 1_000 == 0 {
        format!("{}K", tokens / 1_000)
    } else {
        tokens.to_string()
    }
}

fn sanitize_provider_id(value: &str) -> String {
    let sanitized = value
        .trim()
        .chars()
        .map(|ch| {
            if ch.is_ascii_alphanumeric() || ch == '-' || ch == '_' {
                ch
            } else {
                '-'
            }
        })
        .collect::<String>();
    if sanitized.is_empty() {
        "gateway".to_string()
    } else {
        sanitized
    }
}

fn sanitize_catalog_filename(id: &str) -> String {
    sanitize_provider_id(id)
}

fn print_apply_report(report: &ApplyReport) {
    println!("已写入 Codex gateway 配置");
    println!("  provider: {}", report.provider_id);
    println!("  codex_home: {}", report.codex_home.display());
    println!("  config: {}", report.config_path.display());
    println!("  auth: {} (未修改)", report.auth_path.display());
    println!("  catalog: {}", report.catalog_path.display());
    println!("  models: {}", report.model_count);
    if let Some(count) = report.session_file_count {
        println!("  session_files_seen: {count}");
    }
    if let Some(sync) = report.thread_catalog_sync {
        println!(
            "  thread_catalog_sync: sources={}, threads={}, dbs={}, inserted={}, updated={}",
            sync.sources_seen,
            sync.threads_seen,
            sync.catalog_targets,
            sync.catalog_inserted,
            sync.catalog_updated
        );
    }
    if let Some(path) = &report.backup_path {
        println!("  backup: {path}");
    }
}

fn count_session_files(home: &Path) -> anyhow::Result<usize> {
    let sessions = home.join("sessions");
    if !sessions.exists() {
        return Ok(0);
    }
    count_files_recursive(&sessions)
}

fn count_files_recursive(path: &Path) -> anyhow::Result<usize> {
    let mut count = 0;
    for entry in fs::read_dir(path).with_context(|| format!("读取目录失败：{}", path.display()))?
    {
        let entry = entry?;
        let file_type = entry.file_type()?;
        if file_type.is_dir() {
            count += count_files_recursive(&entry.path())?;
        } else if file_type.is_file() {
            count += 1;
        }
    }
    Ok(count)
}

#[derive(Debug, Clone)]
struct SessionIndexEntry {
    id: String,
    thread_name: Option<String>,
    updated_at: Option<f64>,
}

#[derive(Debug, Clone)]
struct ThreadCatalogEntry {
    thread_id: String,
    display_title: String,
    preview: Option<String>,
    first_user_message: Option<String>,
    rollout_path: Option<String>,
    source_created_at: f64,
    source_updated_at: f64,
    cwd: String,
    source_kind: String,
    model_provider: String,
    git_branch: Option<String>,
    archived: bool,
}

#[cfg(test)]
fn sync_local_thread_catalog(home: &Path) -> anyhow::Result<ThreadCatalogSyncReport> {
    sync_local_thread_catalog_for_provider(home, None)
}

fn sync_local_thread_catalog_for_provider(
    home: &Path,
    active_model_provider: Option<&str>,
) -> anyhow::Result<ThreadCatalogSyncReport> {
    let state_entries = collect_thread_entries_from_state_dbs(home)?;
    let index_entries = read_session_index_entries(home)?;
    let mut report = ThreadCatalogSyncReport {
        sources_seen: thread_state_db_paths(home).len()
            + usize::from(home.join("session_index.jsonl").exists()),
        threads_seen: 0,
        catalog_targets: 0,
        catalog_inserted: 0,
        catalog_updated: 0,
    };

    let mut selected = Vec::new();
    let mut seen = HashSet::new();
    if !index_entries.is_empty() {
        for index in index_entries {
            let Some(base) = state_entries.get(&index.id) else {
                continue;
            };
            let mut entry = base.clone();
            if let Some(name) = index.thread_name.as_deref().and_then(non_empty_string) {
                entry.display_title = name;
            }
            if let Some(updated_at) = index.updated_at {
                entry.source_updated_at = updated_at;
            }
            seen.insert(entry.thread_id.clone());
            selected.push(entry);
        }
        for entry in state_entries.values().filter(|entry| !entry.archived) {
            if seen.insert(entry.thread_id.clone()) {
                selected.push(entry.clone());
            }
        }
    } else {
        let mut values = state_entries.into_values().collect::<Vec<_>>();
        if values.iter().any(|entry| !entry.archived) {
            values.retain(|entry| !entry.archived);
        }
        selected = values;
    }

    selected.sort_by(|left, right| {
        left.source_updated_at
            .partial_cmp(&right.source_updated_at)
            .unwrap_or(std::cmp::Ordering::Equal)
            .then_with(|| left.thread_id.cmp(&right.thread_id))
    });
    report.threads_seen = selected.len();
    if selected.is_empty() {
        return Ok(report);
    }
    let active_model_provider = active_model_provider
        .and_then(non_empty_string)
        .or_else(|| codex_home_model_provider(home));
    if let Some(provider) = active_model_provider.as_deref() {
        normalize_rollout_session_meta_providers(home, &selected, provider)?;
    }

    for db_path in local_thread_catalog_db_paths(home) {
        if let Some(parent) = db_path.parent() {
            fs::create_dir_all(parent)
                .with_context(|| format!("创建 Codex sqlite 目录失败：{}", parent.display()))?;
        }
        let mut conn = Connection::open(&db_path)
            .with_context(|| format!("打开 Codex catalog 数据库失败：{}", db_path.display()))?;
        conn.busy_timeout(Duration::from_secs(2))?;
        ensure_local_thread_catalog_schema(&conn)?;
        let (inserted, updated) = upsert_local_thread_catalog(&mut conn, &selected)?;
        report.catalog_targets += 1;
        report.catalog_inserted = report.catalog_inserted.max(inserted);
        report.catalog_updated = report.catalog_updated.max(updated);
    }
    if let Some((inserted, updated)) =
        sync_native_threads_table(home, &selected, active_model_provider.as_deref())?
    {
        report.catalog_targets += 1;
        report.catalog_inserted = report.catalog_inserted.max(inserted);
        report.catalog_updated = report.catalog_updated.max(updated);
    }
    Ok(report)
}

fn local_thread_catalog_db_paths(home: &Path) -> Vec<PathBuf> {
    let sqlite_dir = home.join("sqlite");
    let mut paths = vec![sqlite_dir.join("codex.db"), sqlite_dir.join("codex-dev.db")];
    if let Ok(entries) = fs::read_dir(&sqlite_dir) {
        for entry in entries.filter_map(Result::ok) {
            let path = entry.path();
            if path.is_file()
                && is_sqlite_candidate(&path)
                && (has_session_table(&path)
                    || sqlite_has_table(&path, "local_thread_catalog")
                    || sqlite_has_table(&path, "local_thread_catalog_sync_state"))
            {
                paths.push(path);
            }
        }
    }
    paths.sort();
    paths.dedup();
    paths
}

fn collect_thread_entries_from_state_dbs(
    home: &Path,
) -> anyhow::Result<HashMap<String, ThreadCatalogEntry>> {
    let mut entries = HashMap::new();
    for db_path in thread_state_db_paths(home) {
        if !db_path.exists() || !sqlite_has_table(&db_path, "threads") {
            continue;
        }
        let conn = Connection::open_with_flags(&db_path, OpenFlags::SQLITE_OPEN_READ_ONLY)
            .with_context(|| format!("读取 Codex thread 数据库失败：{}", db_path.display()))?;
        let columns = sqlite_table_columns(&conn, "threads")?;
        let selected = [
            "id",
            "title",
            "preview",
            "first_user_message",
            "rollout_path",
            "created_at",
            "created_at_ms",
            "updated_at",
            "updated_at_ms",
            "recency_at",
            "recency_at_ms",
            "cwd",
            "source",
            "thread_source",
            "model_provider",
            "git_branch",
            "archived",
        ]
        .into_iter()
        .map(|name| {
            if columns.contains(name) {
                format!("{name} AS {name}")
            } else {
                format!("NULL AS {name}")
            }
        })
        .collect::<Vec<_>>()
        .join(", ");
        let mut statement = conn.prepare(&format!("SELECT {selected} FROM threads"))?;
        let rows = statement.query_map([], |row| {
            let id = row_string(row, 0).unwrap_or_default();
            let title = row_string(row, 1)
                .or_else(|| row_string(row, 2))
                .or_else(|| row_string(row, 3))
                .and_then(|value| non_empty_string(&value))
                .unwrap_or_else(|| id.clone());
            let preview = row_string(row, 2).and_then(|value| non_empty_string(&value));
            let first_user_message = row_string(row, 3).and_then(|value| non_empty_string(&value));
            let rollout_path = row_string(row, 4).and_then(|value| non_empty_string(&value));
            let created_at = row_timestamp(row, 5)
                .or_else(|| row_timestamp_millis(row, 6))
                .unwrap_or(0.0);
            let updated_at = row_timestamp(row, 9)
                .or_else(|| row_timestamp_millis(row, 10))
                .or_else(|| row_timestamp(row, 7))
                .or_else(|| row_timestamp_millis(row, 8))
                .unwrap_or(created_at);
            let cwd = row_string(row, 11)
                .and_then(|value| non_empty_string(&value))
                .unwrap_or_else(|| home.display().to_string());
            let source_kind = row_string(row, 12)
                .or_else(|| row_string(row, 13))
                .and_then(|value| non_empty_string(&value))
                .unwrap_or_else(|| "session".to_string());
            let model_provider = row_string(row, 14)
                .and_then(|value| non_empty_string(&value))
                .unwrap_or_else(|| "openai".to_string());
            let archived = row_bool(row, 16).unwrap_or(false);
            Ok(ThreadCatalogEntry {
                thread_id: id,
                display_title: title,
                preview,
                first_user_message,
                rollout_path,
                source_created_at: created_at,
                source_updated_at: updated_at,
                cwd,
                source_kind,
                model_provider,
                git_branch: row_string(row, 15).and_then(|value| non_empty_string(&value)),
                archived,
            })
        })?;
        for entry in rows {
            let entry = entry?;
            if entry.thread_id.trim().is_empty() {
                continue;
            }
            entries
                .entry(entry.thread_id.clone())
                .and_modify(|existing: &mut ThreadCatalogEntry| {
                    if entry.source_updated_at >= existing.source_updated_at {
                        *existing = entry.clone();
                    }
                })
                .or_insert(entry);
        }
    }
    Ok(entries)
}

fn thread_state_db_paths(home: &Path) -> Vec<PathBuf> {
    let mut paths = codex_sqlite_dir_session_dbs(home);
    let legacy = home.join("state_5.sqlite");
    if !paths.iter().any(|path| path == &legacy) {
        paths.push(legacy);
    }
    paths.sort();
    paths.dedup();
    paths
}

fn codex_sqlite_dir_session_dbs(home: &Path) -> Vec<PathBuf> {
    let sqlite_dir = home.join("sqlite");
    let Ok(entries) = fs::read_dir(sqlite_dir) else {
        return Vec::new();
    };
    let mut candidates = entries
        .filter_map(Result::ok)
        .map(|entry| entry.path())
        .filter(|path| path.is_file())
        .filter(|path| is_sqlite_candidate(path))
        .filter(|path| has_session_table(path))
        .collect::<Vec<_>>();
    candidates.sort_by_key(|path| {
        (
            path.file_name()
                .map(|name| name != OsStr::new("codex-dev.db"))
                .unwrap_or(true),
            path.file_name().map(|name| name.to_os_string()),
        )
    });
    candidates
}

fn is_sqlite_candidate(path: &Path) -> bool {
    matches!(
        path.extension().and_then(OsStr::to_str),
        Some("db") | Some("sqlite") | Some("sqlite3")
    )
}

fn has_session_table(path: &Path) -> bool {
    ["threads", "automation_runs", "inbox_items"]
        .iter()
        .any(|table| sqlite_has_table(path, table))
}

fn read_session_index_entries(home: &Path) -> anyhow::Result<Vec<SessionIndexEntry>> {
    let path = home.join("session_index.jsonl");
    let text = match fs::read_to_string(&path) {
        Ok(text) => text,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(Vec::new()),
        Err(error) => {
            return Err(error)
                .with_context(|| format!("读取 session_index 失败：{}", path.display()));
        }
    };
    let mut entries = Vec::new();
    for line in text.lines().map(str::trim).filter(|line| !line.is_empty()) {
        let Ok(value) = serde_json::from_str::<Value>(line) else {
            continue;
        };
        let Some(id) = value
            .get("id")
            .or_else(|| value.get("thread_id"))
            .and_then(Value::as_str)
            .and_then(non_empty_string)
        else {
            continue;
        };
        entries.push(SessionIndexEntry {
            id,
            thread_name: value
                .get("thread_name")
                .or_else(|| value.get("title"))
                .and_then(Value::as_str)
                .and_then(non_empty_string),
            updated_at: value
                .get("updated_at")
                .or_else(|| value.get("updatedAt"))
                .and_then(Value::as_str)
                .and_then(parse_timestamp),
        });
    }
    Ok(entries)
}

fn ensure_local_thread_catalog_schema(conn: &Connection) -> anyhow::Result<()> {
    conn.execute_batch(
        r#"
CREATE TABLE IF NOT EXISTS local_thread_catalog_hosts (
  host_id TEXT PRIMARY KEY,
  host_kind TEXT NOT NULL CHECK (host_kind IN ('local', 'ssh', 'wsl', 'remote-control'))
);
CREATE TABLE IF NOT EXISTS local_thread_catalog (
  host_id TEXT NOT NULL,
  thread_id TEXT NOT NULL,
  display_title TEXT NOT NULL,
  source_created_at REAL NOT NULL,
  source_updated_at REAL NOT NULL,
  cwd TEXT NOT NULL,
  source_kind TEXT NOT NULL,
  source_detail TEXT,
  model_provider TEXT NOT NULL,
  git_branch TEXT,
  observation_sequence INTEGER NOT NULL,
  missing_candidate INTEGER NOT NULL DEFAULT 0 CHECK (missing_candidate IN (0, 1)),
  PRIMARY KEY (host_id, thread_id)
);
CREATE INDEX IF NOT EXISTS local_thread_catalog_updated_idx
  ON local_thread_catalog(
    host_id,
    source_updated_at DESC,
    source_created_at DESC,
    thread_id
  ) WHERE missing_candidate = 0;
CREATE TABLE IF NOT EXISTS local_thread_catalog_metadata (
  id INTEGER PRIMARY KEY CHECK (id = 1),
  catalog_revision INTEGER NOT NULL DEFAULT 0
);
CREATE TABLE IF NOT EXISTS local_thread_catalog_sync_state (
  host_id TEXT PRIMARY KEY,
  watermark_updated_at REAL,
  initial_build_complete INTEGER NOT NULL DEFAULT 0,
  observation_sequence INTEGER NOT NULL DEFAULT 0
);
"#,
    )?;
    ensure_sqlite_column(
        conn,
        "local_thread_catalog_sync_state",
        "watermark_updated_at",
        "REAL",
    )?;
    ensure_sqlite_column(
        conn,
        "local_thread_catalog_sync_state",
        "initial_build_complete",
        "INTEGER NOT NULL DEFAULT 0",
    )?;
    ensure_sqlite_column(
        conn,
        "local_thread_catalog_sync_state",
        "observation_sequence",
        "INTEGER NOT NULL DEFAULT 0",
    )?;
    conn.execute(
        "INSERT OR IGNORE INTO local_thread_catalog_hosts (host_id, host_kind) VALUES ('local', 'local')",
        [],
    )?;
    conn.execute(
        "INSERT OR IGNORE INTO local_thread_catalog_metadata (id, catalog_revision) VALUES (1, 0)",
        [],
    )?;
    conn.execute(
        "INSERT OR IGNORE INTO local_thread_catalog_sync_state (host_id, watermark_updated_at, initial_build_complete, observation_sequence) VALUES ('local', NULL, 0, 0)",
        [],
    )?;
    Ok(())
}

fn ensure_sqlite_column(
    conn: &Connection,
    table: &str,
    column: &str,
    definition: &str,
) -> anyhow::Result<()> {
    if !sqlite_table_columns(conn, table)?.contains(column) {
        conn.execute_batch(&format!(
            "ALTER TABLE {table} ADD COLUMN {column} {definition}"
        ))?;
    }
    Ok(())
}

fn upsert_local_thread_catalog(
    conn: &mut Connection,
    entries: &[ThreadCatalogEntry],
) -> anyhow::Result<(usize, usize)> {
    let mut next_sequence = conn
        .query_row(
            "SELECT COALESCE(MAX(observation_sequence), 0) FROM local_thread_catalog WHERE host_id = 'local'",
            [],
            |row| row.get::<_, i64>(0),
        )
        .unwrap_or(0);
    let tx = conn.transaction()?;
    let mut inserted = 0;
    let mut updated = 0;
    let watermark = entries
        .iter()
        .map(|entry| entry.source_updated_at)
        .fold(0.0_f64, f64::max);
    for entry in entries {
        let existing = tx
            .query_row(
                "SELECT display_title, source_updated_at, cwd, source_kind, model_provider, git_branch, missing_candidate FROM local_thread_catalog WHERE host_id = 'local' AND thread_id = ?1",
                [&entry.thread_id],
                |row| {
                    Ok((
                        row.get::<_, String>(0)?,
                        row.get::<_, f64>(1)?,
                        row.get::<_, String>(2)?,
                        row.get::<_, String>(3)?,
                        row.get::<_, String>(4)?,
                        row.get::<_, Option<String>>(5)?,
                        row.get::<_, i64>(6)?,
                    ))
                },
            )
            .optional()?;
        let changed = match existing {
            None => {
                inserted += 1;
                true
            }
            Some((title, updated_at, cwd, source_kind, model_provider, git_branch, missing)) => {
                let changed = title != entry.display_title
                    || (updated_at - entry.source_updated_at).abs() > 0.001
                    || cwd != entry.cwd
                    || source_kind != entry.source_kind
                    || model_provider != entry.model_provider
                    || git_branch != entry.git_branch
                    || missing != 0;
                if changed {
                    updated += 1;
                }
                changed
            }
        };
        if !changed {
            continue;
        }
        next_sequence += 1;
        tx.execute(
            r#"
INSERT INTO local_thread_catalog (
  host_id, thread_id, display_title, source_created_at, source_updated_at, cwd,
  source_kind, source_detail, model_provider, git_branch, observation_sequence, missing_candidate
) VALUES (
  'local', ?1, ?2, ?3, ?4, ?5, ?6, NULL, ?7, ?8, ?9, 0
)
ON CONFLICT(host_id, thread_id) DO UPDATE SET
  display_title = excluded.display_title,
  source_created_at = excluded.source_created_at,
  source_updated_at = excluded.source_updated_at,
  cwd = excluded.cwd,
  source_kind = excluded.source_kind,
  source_detail = excluded.source_detail,
  model_provider = excluded.model_provider,
  git_branch = excluded.git_branch,
  observation_sequence = excluded.observation_sequence,
  missing_candidate = 0
"#,
            params![
                entry.thread_id,
                entry.display_title,
                entry.source_created_at,
                entry.source_updated_at,
                entry.cwd,
                entry.source_kind,
                entry.model_provider,
                entry.git_branch,
                next_sequence,
            ],
        )?;
    }
    let sync_state = tx
        .query_row(
            "SELECT watermark_updated_at, initial_build_complete, observation_sequence FROM local_thread_catalog_sync_state WHERE host_id = 'local'",
            [],
            |row| {
                Ok((
                    row.get::<_, Option<f64>>(0)?,
                    row.get::<_, i64>(1)?,
                    row.get::<_, i64>(2)?,
                ))
            },
        )
        .optional()?;
    let sync_state_changed = match sync_state {
        None => true,
        Some((existing_watermark, complete, observation_sequence)) => {
            complete == 0
                || existing_watermark
                    .map(|value| (value - watermark).abs() > 0.001)
                    .unwrap_or(true)
                || observation_sequence < next_sequence
        }
    };
    if inserted > 0 || updated > 0 || sync_state_changed {
        tx.execute(
            "UPDATE local_thread_catalog_metadata SET catalog_revision = catalog_revision + 1 WHERE id = 1",
            [],
        )?;
    }
    tx.execute(
        "INSERT INTO local_thread_catalog_sync_state (host_id, watermark_updated_at, initial_build_complete, observation_sequence) VALUES ('local', ?1, 1, ?2) ON CONFLICT(host_id) DO UPDATE SET watermark_updated_at = excluded.watermark_updated_at, initial_build_complete = 1, observation_sequence = MAX(local_thread_catalog_sync_state.observation_sequence, excluded.observation_sequence)",
        params![watermark, next_sequence],
    )?;
    tx.commit()?;
    Ok((inserted, updated))
}

fn sync_native_threads_table(
    home: &Path,
    entries: &[ThreadCatalogEntry],
    active_model_provider: Option<&str>,
) -> anyhow::Result<Option<(usize, usize)>> {
    let db_path = home.join("state_5.sqlite");
    if !db_path.exists() || !sqlite_has_table(&db_path, "threads") {
        return Ok(None);
    }
    let mut conn = Connection::open(&db_path).with_context(|| {
        format!(
            "打开 Codex native threads 数据库失败：{}",
            db_path.display()
        )
    })?;
    conn.busy_timeout(Duration::from_secs(2))?;
    let columns = sqlite_table_columns(&conn, "threads")?;
    if !columns.contains("id") {
        return Ok(None);
    }
    let tx = conn.transaction()?;
    let mut inserted = 0;
    let mut updated = 0;
    for entry in entries.iter().filter(|entry| !entry.archived) {
        let source_created_at = positive_timestamp(entry.source_created_at)
            .unwrap_or_else(current_unix_timestamp_seconds);
        let source_updated_at = positive_timestamp(entry.source_updated_at)
            .or_else(|| positive_timestamp(entry.source_created_at))
            .unwrap_or_else(current_unix_timestamp_seconds);
        let exists = tx
            .query_row(
                "SELECT 1 FROM threads WHERE id = ?1 LIMIT 1",
                [&entry.thread_id],
                |_| Ok(()),
            )
            .optional()?
            .is_some();
        let mut names = Vec::new();
        let mut values = Vec::<rusqlite::types::Value>::new();
        push_native_thread_value(
            &columns,
            &mut names,
            &mut values,
            "id",
            text_sql_value(&entry.thread_id),
        );
        push_native_thread_value(
            &columns,
            &mut names,
            &mut values,
            "rollout_path",
            option_text_sql_value(entry.rollout_path.as_deref()),
        );
        push_native_thread_value(
            &columns,
            &mut names,
            &mut values,
            "created_at",
            integer_sql_value(timestamp_to_seconds(source_created_at)),
        );
        push_native_thread_value(
            &columns,
            &mut names,
            &mut values,
            "created_at_ms",
            integer_sql_value(timestamp_to_millis(source_created_at)),
        );
        push_native_thread_value(
            &columns,
            &mut names,
            &mut values,
            "updated_at",
            integer_sql_value(timestamp_to_seconds(source_updated_at)),
        );
        push_native_thread_value(
            &columns,
            &mut names,
            &mut values,
            "updated_at_ms",
            integer_sql_value(timestamp_to_millis(source_updated_at)),
        );
        push_native_thread_value(
            &columns,
            &mut names,
            &mut values,
            "recency_at",
            integer_sql_value(timestamp_to_seconds(source_updated_at)),
        );
        push_native_thread_value(
            &columns,
            &mut names,
            &mut values,
            "recency_at_ms",
            integer_sql_value(timestamp_to_millis(source_updated_at)),
        );
        push_native_thread_value(
            &columns,
            &mut names,
            &mut values,
            "cwd",
            text_sql_value(&entry.cwd),
        );
        push_native_thread_value(
            &columns,
            &mut names,
            &mut values,
            "title",
            text_sql_value(&entry.display_title),
        );
        push_native_thread_value(
            &columns,
            &mut names,
            &mut values,
            "first_user_message",
            option_text_sql_value(
                entry
                    .first_user_message
                    .as_deref()
                    .or(Some(entry.display_title.as_str())),
            ),
        );
        push_native_thread_value(
            &columns,
            &mut names,
            &mut values,
            "preview",
            option_text_sql_value(
                entry
                    .preview
                    .as_deref()
                    .or(Some(entry.display_title.as_str())),
            ),
        );
        push_native_thread_value(
            &columns,
            &mut names,
            &mut values,
            "archived",
            integer_sql_value(0),
        );
        push_native_thread_value(
            &columns,
            &mut names,
            &mut values,
            "archived_at",
            rusqlite::types::Value::Null,
        );
        push_native_thread_value(
            &columns,
            &mut names,
            &mut values,
            "thread_source",
            text_sql_value("user"),
        );
        push_native_thread_value(
            &columns,
            &mut names,
            &mut values,
            "source",
            text_sql_value(&entry.source_kind),
        );
        push_native_thread_value(
            &columns,
            &mut names,
            &mut values,
            "model_provider",
            text_sql_value(&native_model_provider(
                entry.model_provider.as_str(),
                active_model_provider,
            )),
        );
        push_native_thread_value(
            &columns,
            &mut names,
            &mut values,
            "git_branch",
            option_text_sql_value(entry.git_branch.as_deref()),
        );
        if names.len() <= 1 {
            continue;
        }
        if exists {
            let update_names = names
                .iter()
                .filter(|name| name.as_str() != "id")
                .cloned()
                .collect::<Vec<_>>();
            if update_names.is_empty() {
                continue;
            }
            let assignments = update_names
                .iter()
                .map(|name| format!("{name} = ?"))
                .collect::<Vec<_>>()
                .join(", ");
            let mut update_values = names
                .iter()
                .zip(values.iter())
                .filter_map(|(name, value)| {
                    if name == "id" {
                        None
                    } else {
                        Some(value.clone())
                    }
                })
                .collect::<Vec<_>>();
            update_values.push(text_sql_value(&entry.thread_id));
            let sql = format!("UPDATE threads SET {assignments} WHERE id = ?");
            tx.execute(&sql, rusqlite::params_from_iter(update_values.iter()))?;
            updated += 1;
        } else {
            let placeholders = std::iter::repeat_n("?", names.len())
                .collect::<Vec<_>>()
                .join(", ");
            let sql = format!(
                "INSERT INTO threads ({}) VALUES ({})",
                names.join(", "),
                placeholders
            );
            tx.execute(&sql, rusqlite::params_from_iter(values.iter()))?;
            inserted += 1;
        }
    }
    tx.commit()?;
    Ok(Some((inserted, updated)))
}

fn push_native_thread_value(
    columns: &HashSet<String>,
    names: &mut Vec<String>,
    values: &mut Vec<rusqlite::types::Value>,
    name: &str,
    value: rusqlite::types::Value,
) {
    if columns.contains(name) {
        names.push(name.to_string());
        values.push(value);
    }
}

fn text_sql_value(value: &str) -> rusqlite::types::Value {
    rusqlite::types::Value::Text(value.to_string())
}

fn option_text_sql_value(value: Option<&str>) -> rusqlite::types::Value {
    value
        .and_then(non_empty_string)
        .map(rusqlite::types::Value::Text)
        .unwrap_or(rusqlite::types::Value::Null)
}

fn integer_sql_value(value: i64) -> rusqlite::types::Value {
    rusqlite::types::Value::Integer(value)
}

fn timestamp_to_millis(value: f64) -> i64 {
    if value <= 0.0 {
        0
    } else {
        (value * 1000.0).round() as i64
    }
}

fn timestamp_to_seconds(value: f64) -> i64 {
    if value <= 0.0 {
        0
    } else if value > 10_000_000_000.0 {
        (value / 1000.0).floor() as i64
    } else {
        value.floor() as i64
    }
}

fn positive_timestamp(value: f64) -> Option<f64> {
    value
        .is_finite()
        .then_some(value)
        .filter(|value| *value > 0.0)
}

fn current_unix_timestamp_seconds() -> f64 {
    SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .map(|duration| duration.as_secs_f64())
        .unwrap_or(0.0)
}

fn normalize_rollout_session_meta_providers(
    home: &Path,
    entries: &[ThreadCatalogEntry],
    active_model_provider: &str,
) -> anyhow::Result<usize> {
    let Some(active_model_provider) = non_empty_string(active_model_provider) else {
        return Ok(0);
    };
    let mut changed = 0;
    let mut seen = HashSet::new();
    for entry in entries.iter().filter(|entry| !entry.archived) {
        let Some(raw_path) = entry.rollout_path.as_deref().and_then(non_empty_string) else {
            continue;
        };
        if !seen.insert(raw_path.clone()) {
            continue;
        }
        let path = PathBuf::from(strip_windows_extended_prefix(&raw_path));
        if !rollout_path_belongs_to_home_sessions(home, &path) {
            continue;
        }
        if normalize_rollout_session_meta_provider(&path, &active_model_provider)? {
            changed += 1;
        }
    }
    Ok(changed)
}

fn normalize_rollout_session_meta_provider(
    path: &Path,
    active_model_provider: &str,
) -> anyhow::Result<bool> {
    let text = match fs::read_to_string(path) {
        Ok(text) => text,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(false),
        Err(error) => {
            return Err(error)
                .with_context(|| format!("读取 Codex rollout 失败：{}", path.display()));
        }
    };
    let (first_line, rest) = match text.split_once('\n') {
        Some((first, rest)) => (first.trim_end_matches('\r'), Some(rest)),
        None => (text.trim_end_matches('\r'), None),
    };
    if first_line.trim().is_empty() {
        return Ok(false);
    }
    let mut value: Value = match serde_json::from_str(first_line) {
        Ok(value) => value,
        Err(_) => return Ok(false),
    };
    if value.get("type").and_then(Value::as_str) != Some("session_meta") {
        return Ok(false);
    }
    let Some(payload) = value.get_mut("payload").and_then(Value::as_object_mut) else {
        return Ok(false);
    };
    if payload
        .get("model_provider")
        .and_then(Value::as_str)
        .is_some_and(|value| value == active_model_provider)
    {
        return Ok(false);
    }
    payload.insert(
        "model_provider".to_string(),
        Value::String(active_model_provider.to_string()),
    );
    let mut rewritten = serde_json::to_string(&value)?;
    rewritten.push('\n');
    if let Some(rest) = rest {
        rewritten.push_str(rest);
    }
    let temp_path = path.with_extension(format!(
        "{}.tmp",
        path.extension().and_then(OsStr::to_str).unwrap_or("jsonl")
    ));
    fs::write(&temp_path, rewritten)
        .with_context(|| format!("写入临时 rollout 失败：{}", temp_path.display()))?;
    fs::rename(&temp_path, path)
        .with_context(|| format!("替换 Codex rollout 失败：{}", path.display()))?;
    Ok(true)
}

fn rollout_path_belongs_to_home_sessions(home: &Path, path: &Path) -> bool {
    let Ok(home) = home.canonicalize() else {
        return false;
    };
    let Ok(path) = path.canonicalize() else {
        return false;
    };
    path.starts_with(home.join("sessions"))
}

fn strip_windows_extended_prefix(value: &str) -> String {
    value.strip_prefix(r"\\?\").unwrap_or(value).to_string()
}

fn codex_home_model_provider(home: &Path) -> Option<String> {
    let config_text = fs::read_to_string(home.join("config.toml")).ok()?;
    root_config_string_value(&config_text, "model_provider")
        .and_then(|value| non_empty_string(&value))
}

fn native_model_provider(value: &str, active_model_provider: Option<&str>) -> String {
    if let Some(active) = active_model_provider.and_then(non_empty_string) {
        return active;
    }
    let trimmed = value.trim();
    if trimmed.is_empty() || trimmed.eq_ignore_ascii_case("custom") {
        "openai".to_string()
    } else {
        trimmed.to_string()
    }
}

fn sqlite_has_table(path: &Path, table: &str) -> bool {
    let Ok(conn) = Connection::open_with_flags(path, OpenFlags::SQLITE_OPEN_READ_ONLY) else {
        return false;
    };
    conn.query_row(
        "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = ?1 LIMIT 1",
        [table],
        |_| Ok(()),
    )
    .is_ok()
}

fn sqlite_table_columns(conn: &Connection, table: &str) -> anyhow::Result<HashSet<String>> {
    let mut statement = conn.prepare(&format!("PRAGMA table_info({table})"))?;
    let rows = statement.query_map([], |row| row.get::<_, String>(1))?;
    let mut columns = HashSet::new();
    for row in rows {
        columns.insert(row?);
    }
    Ok(columns)
}

fn row_string(row: &rusqlite::Row<'_>, index: usize) -> Option<String> {
    match row.get_ref(index).ok()? {
        rusqlite::types::ValueRef::Null => None,
        rusqlite::types::ValueRef::Integer(value) => Some(value.to_string()),
        rusqlite::types::ValueRef::Real(value) => Some(value.to_string()),
        rusqlite::types::ValueRef::Text(value) => {
            Some(String::from_utf8_lossy(value).trim().to_string())
        }
        rusqlite::types::ValueRef::Blob(_) => None,
    }
    .and_then(|value| non_empty_string(&value))
}

fn row_timestamp(row: &rusqlite::Row<'_>, index: usize) -> Option<f64> {
    row_string(row, index).and_then(|value| parse_timestamp(&value))
}

fn row_timestamp_millis(row: &rusqlite::Row<'_>, index: usize) -> Option<f64> {
    row_string(row, index).and_then(|value| parse_timestamp_millis(&value))
}

fn row_bool(row: &rusqlite::Row<'_>, index: usize) -> Option<bool> {
    row_string(row, index).map(|value| {
        let normalized = value.trim().to_ascii_lowercase();
        normalized == "1" || normalized == "true" || normalized == "yes"
    })
}

fn parse_timestamp(value: &str) -> Option<f64> {
    let trimmed = value.trim();
    if trimmed.is_empty() {
        return None;
    }
    if let Ok(number) = trimmed.parse::<f64>() {
        return Some(if number > 10_000_000_000.0 {
            number / 1000.0
        } else {
            number
        });
    }
    DateTime::parse_from_rfc3339(trimmed)
        .ok()
        .map(|value| value.timestamp_millis() as f64 / 1000.0)
}

fn parse_timestamp_millis(value: &str) -> Option<f64> {
    value.trim().parse::<f64>().ok().map(|value| value / 1000.0)
}

async fn doctor(config: &LiteConfig) -> anyhow::Result<()> {
    validate_config(config)?;
    let base_url = config.provider.base_url.trim().trim_end_matches('/');
    let url = protocol_proxy::models_url(base_url);
    println!("provider: {}", config.provider.id);
    println!("protocol: {:?}", config.provider.protocol);
    println!("endpoint: {url}");
    let models = fetch_provider_model_ids(&config.provider).await?;
    println!("http_status: ok (2xx)");
    println!("models_seen: {}", models.len());
    if let Some(first) = models.first() {
        println!("first_model: {first}");
    }
    Ok(())
}

async fn soft_reload_codex(debug_port: u16) -> anyhow::Result<()> {
    let targets = codex_lite::list_targets(debug_port)
        .await
        .with_context(|| {
            format!(
                "无法连接 Codex CDP 端口 {debug_port}；Codex 需要带 --remote-debugging-port 启动"
            )
        })?;
    let target = pick_lite_main_codex_page_target(&targets)?;
    let websocket = target
        .web_socket_debugger_url
        .as_deref()
        .ok_or_else(|| anyhow::anyhow!("Codex CDP target 没有 websocket URL"))?;
    codex_lite::evaluate_script(
        websocket,
        r#"(() => { window.location.reload(); return true; })()"#,
    )
    .await?;
    println!("已触发 Codex renderer 软刷新");
    Ok(())
}

async fn wait_and_inject_plan_ui(debug_port: u16, codex_home: Option<&Path>) -> anyhow::Result<()> {
    let mut last_error = None;
    for _ in 0..40 {
        match inject_plan_ui_inner(debug_port, true, PLAN_UI_INITIAL_HISTORY_SEED, codex_home).await
        {
            Ok(()) => return Ok(()),
            Err(error) => {
                last_error = Some(error);
                tokio::time::sleep(Duration::from_millis(500)).await;
            }
        }
    }
    Err(last_error.unwrap_or_else(|| anyhow::anyhow!("任务清单 UI 注入失败")))
}

async fn inject_plan_ui_inner(
    debug_port: u16,
    verbose: bool,
    seed_history: bool,
    codex_home: Option<&Path>,
) -> anyhow::Result<()> {
    let targets = codex_lite::list_targets(debug_port)
        .await
        .with_context(|| {
            format!(
                "无法连接 Codex CDP 端口 {debug_port}；Codex 需要带 --remote-debugging-port 启动"
            )
        })?;
    let target = pick_lite_main_codex_page_target(&targets)?;
    let websocket = target
        .web_socket_debugger_url
        .as_deref()
        .ok_or_else(|| anyhow::anyhow!("Codex CDP target 没有 websocket URL"))?;
    codex_lite::evaluate_script(websocket, PLAN_UI_SCRIPT).await?;
    if seed_history {
        let home = resolve_codex_home(codex_home);
        match plan_ui_history_seed_script(&home) {
            Ok(Some(script)) => {
                if let Err(error) = codex_lite::evaluate_script(websocket, &script).await {
                    if verbose {
                        eprintln!("任务清单历史快照注入失败，继续使用当前页面快照：{error:#}");
                    }
                }
            }
            Ok(None) => {}
            Err(error) if verbose => {
                eprintln!("读取任务清单历史快照失败，继续使用当前页面快照：{error:#}")
            }
            Err(_) => {}
        }
    }
    match sample_plan_ui_snapshot(websocket).await {
        Ok(true) => {}
        Ok(false) => {}
        Err(error) if verbose => eprintln!("任务清单详情采样失败：{error:#}"),
        Err(_) => {}
    }
    if verbose {
        println!("已注入任务清单 UI 定位修正");
    }
    Ok(())
}

async fn seed_active_plan_ui_history_snapshot_if_needed(
    debug_port: u16,
    codex_home: Option<&Path>,
    recent_attempts: &mut HashMap<String, Instant>,
) -> anyhow::Result<()> {
    let targets = codex_lite::list_targets(debug_port)
        .await
        .with_context(|| {
            format!(
                "无法连接 Codex CDP 端口 {debug_port}；Codex 需要带 --remote-debugging-port 启动"
            )
        })?;
    let target = pick_lite_main_codex_page_target(&targets)?;
    let websocket = target
        .web_socket_debugger_url
        .as_deref()
        .ok_or_else(|| anyhow::anyhow!("Codex CDP target 没有 websocket URL"))?;
    let result = codex_lite::evaluate_script(websocket, PLAN_UI_ACTIVE_THREAD_NEEDS_SEED_SCRIPT)
        .await
        .context("检查当前任务清单历史快照状态失败")?;
    let Some(value) = cdp_value(&result) else {
        return Ok(());
    };
    if !value
        .get("needsSeed")
        .and_then(Value::as_bool)
        .unwrap_or(false)
    {
        return Ok(());
    }
    let Some(thread_id) = value
        .get("threadId")
        .and_then(Value::as_str)
        .and_then(non_empty_string)
    else {
        return Ok(());
    };
    let now = Instant::now();
    if recent_attempts.get(&thread_id).is_some_and(|previous| {
        previous.elapsed() < Duration::from_secs(PLAN_UI_ACTIVE_HISTORY_SEED_RETRY_SECS)
    }) {
        return Ok(());
    }
    recent_attempts.insert(thread_id.clone(), now);
    let home = resolve_codex_home(codex_home);
    let Some(snapshot) = collect_plan_ui_history_snapshot_for_thread(&home, &thread_id)? else {
        return Ok(());
    };
    let Some(script) = plan_ui_history_seed_script_for_snapshots(&[snapshot])? else {
        return Ok(());
    };
    codex_lite::evaluate_script(websocket, &script)
        .await
        .context("按需注入当前任务清单历史快照失败")?;
    recent_attempts.remove(&thread_id);
    Ok(())
}

async fn sample_plan_ui_snapshot(websocket_url: &str) -> anyhow::Result<bool> {
    let (mut socket, _) =
        tokio::time::timeout(Duration::from_secs(5), connect_async(websocket_url))
            .await
            .context("连接 Codex CDP websocket 超时")?
            .context("连接 Codex CDP websocket 失败")?;

    let snapshot_result = cdp_send_command(
        &mut socket,
        1,
        "Runtime.evaluate",
        runtime_evaluate_params_return_by_value(PLAN_TOOLTIP_SAMPLE_READ_SCRIPT),
    )
    .await?;

    Ok(cdp_value(&snapshot_result)
        .and_then(|value| value.get("items"))
        .and_then(Value::as_array)
        .map(|items| !items.is_empty())
        .unwrap_or(false))
}

const PLAN_UI_HISTORY_SEED_LIMIT: usize = 200;

#[derive(Debug, Clone)]
struct ThreadRolloutEntry {
    thread_id: String,
    rollout_path: PathBuf,
    updated_at_ms: i64,
}

#[derive(Debug, Clone)]
struct UpdatePlanCall {
    arguments: Value,
    call_id: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
struct PlanUiSnapshotSeed {
    thread_id: String,
    progress: String,
    detail: String,
    items: Vec<String>,
    rows: Vec<PlanUiSnapshotSeedRow>,
    source_conversation_id: String,
    source_turn_id: String,
    source_todo_id: String,
    at: i64,
    source: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
struct PlanUiSnapshotSeedRow {
    text: String,
    status: String,
    icon_html: String,
    text_color: String,
    icon_color: String,
}

fn plan_ui_history_seed_script(home: &Path) -> anyhow::Result<Option<String>> {
    let snapshots = collect_plan_ui_history_snapshots(home, PLAN_UI_HISTORY_SEED_LIMIT)?;
    plan_ui_history_seed_script_for_snapshots(&snapshots)
}

fn plan_ui_history_seed_script_for_snapshots(
    snapshots: &[PlanUiSnapshotSeed],
) -> anyhow::Result<Option<String>> {
    if snapshots.is_empty() {
        return Ok(None);
    }
    let snapshots_json = serde_json::to_string(&snapshots)?;
    Ok(Some(format!(
        r#"
(() => {{
  const KEY = "__codexGatewayLitePlanUiExternalSnapshots";
  const STORAGE_KEY = "codex-gateway-lite-plan-ui-snapshots-v1";
  const STORAGE_LIMIT = 200;
  const incoming = {snapshots_json};
  const current = window[KEY] && typeof window[KEY] === "object" ? window[KEY] : {{}};
  const merged = {{ ...current }};
  for (const snapshot of incoming) {{
    if (!snapshot || !snapshot.threadId || !Array.isArray(snapshot.rows) || !snapshot.rows.length) continue;
    const previous = merged[snapshot.threadId];
    if (!previous || Number(snapshot.at || 0) >= Number(previous.at || 0)) {{
      merged[snapshot.threadId] = snapshot;
    }}
  }}
  window[KEY] = merged;
  const state = window.__codexGatewayLitePlanUiState;
  if (state && typeof state === "object") {{
    const stateSnapshots = state.snapshotsByThread && typeof state.snapshotsByThread === "object"
      ? state.snapshotsByThread
      : {{}};
    for (const [threadId, snapshot] of Object.entries(merged)) {{
      if (!snapshot || !Array.isArray(snapshot.rows) || !snapshot.rows.length) continue;
      const previous = stateSnapshots[threadId];
      if (!previous || Number(snapshot.at || 0) >= Number(previous.at || 0)) {{
        stateSnapshots[threadId] = snapshot;
      }}
    }}
    state.snapshotsByThread = Object.fromEntries(Object.entries(stateSnapshots)
      .sort((a, b) => Number(b[1]?.at || 0) - Number(a[1]?.at || 0))
      .slice(0, STORAGE_LIMIT));
  }}
  if (typeof window.__codexGatewayLitePlanUiApply === "function") {{
    try {{ window.__codexGatewayLitePlanUiApply(); }} catch {{}}
  }}
  try {{
    const stored = JSON.parse(localStorage.getItem(STORAGE_KEY) || "{{}}");
    for (const [threadId, snapshot] of Object.entries(merged)) {{
      if (!snapshot || !Array.isArray(snapshot.rows) || !snapshot.rows.length) continue;
      const previous = stored[threadId];
      if (!previous || Number(snapshot.at || 0) >= Number(previous.at || 0)) {{
        stored[threadId] = snapshot;
      }}
    }}
    const entries = Object.entries(stored)
      .sort((a, b) => Number(b[1]?.at || 0) - Number(a[1]?.at || 0))
      .slice(0, STORAGE_LIMIT);
    localStorage.setItem(STORAGE_KEY, JSON.stringify(Object.fromEntries(entries)));
  }} catch {{
  }}
  return Object.keys(merged).length;
}})()
"#
    )))
}

fn collect_plan_ui_history_snapshots(
    home: &Path,
    limit: usize,
) -> anyhow::Result<Vec<PlanUiSnapshotSeed>> {
    let mut snapshots = Vec::new();
    for entry in collect_recent_thread_rollout_entries(home, limit.saturating_mul(3).max(limit))? {
        match latest_plan_snapshot_from_rollout(
            &entry.thread_id,
            &entry.rollout_path,
            entry.updated_at_ms,
        ) {
            Ok(Some(snapshot)) => {
                snapshots.push(snapshot);
                if snapshots.len() >= limit {
                    break;
                }
            }
            Ok(None) => {}
            Err(_) => {}
        }
    }
    Ok(snapshots)
}

fn collect_plan_ui_history_snapshot_for_thread(
    home: &Path,
    thread_id: &str,
) -> anyhow::Result<Option<PlanUiSnapshotSeed>> {
    let normalized = normalize_seed_thread_id(thread_id);
    let raw = raw_seed_conversation_id(thread_id);
    let mut candidates = Vec::new();
    candidates.push(normalized.clone());
    if raw != normalized {
        candidates.push(raw.clone());
    }
    candidates.sort();
    candidates.dedup();

    let mut best: Option<ThreadRolloutEntry> = None;
    for db_path in thread_state_db_paths(home) {
        if !db_path.exists() || !sqlite_has_table(&db_path, "threads") {
            continue;
        }
        let conn = Connection::open_with_flags(&db_path, OpenFlags::SQLITE_OPEN_READ_ONLY)
            .with_context(|| format!("读取 Codex thread 数据库失败：{}", db_path.display()))?;
        let columns = sqlite_table_columns(&conn, "threads")?;
        if !columns.contains("id") {
            continue;
        }
        let selected = [
            "id",
            "rollout_path",
            "updated_at",
            "updated_at_ms",
            "archived",
        ]
        .into_iter()
        .map(|name| {
            if columns.contains(name) {
                format!("{name} AS {name}")
            } else {
                format!("NULL AS {name}")
            }
        })
        .collect::<Vec<_>>()
        .join(", ");
        for candidate_id in &candidates {
            let mut statement = conn.prepare(&format!(
                "SELECT {selected} FROM threads WHERE id = ?1 LIMIT 1"
            ))?;
            let row = statement
                .query_row([candidate_id], |row| {
                    let found_id = row_string(row, 0).unwrap_or_else(|| candidate_id.clone());
                    let rollout_path = row_string(row, 1).unwrap_or_default();
                    let updated_at_ms = row_timestamp(row, 2)
                        .or_else(|| row_timestamp_millis(row, 3))
                        .map(|value| (value * 1000.0).round() as i64)
                        .unwrap_or(0);
                    let archived = row_bool(row, 4).unwrap_or(false);
                    Ok((found_id, rollout_path, updated_at_ms, archived))
                })
                .optional()?;
            let Some((found_id, rollout_path, updated_at_ms, archived)) = row else {
                continue;
            };
            if archived || rollout_path.trim().is_empty() {
                continue;
            }
            let path = resolve_rollout_path(home, &rollout_path);
            if !path.exists() {
                continue;
            }
            let updated_at_ms = if updated_at_ms > 0 {
                updated_at_ms
            } else {
                file_modified(&path)
                    .ok()
                    .and_then(system_time_to_unix_ms)
                    .unwrap_or(0)
            };
            let entry = ThreadRolloutEntry {
                thread_id: found_id,
                rollout_path: path,
                updated_at_ms,
            };
            if best.as_ref().map_or(true, |previous| {
                entry.updated_at_ms >= previous.updated_at_ms
            }) {
                best = Some(entry);
            }
        }
    }
    let Some(entry) = best else {
        return Ok(None);
    };
    latest_plan_snapshot_from_rollout(&entry.thread_id, &entry.rollout_path, entry.updated_at_ms)
}

fn collect_recent_thread_rollout_entries(
    home: &Path,
    limit: usize,
) -> anyhow::Result<Vec<ThreadRolloutEntry>> {
    let mut by_thread: HashMap<String, ThreadRolloutEntry> = HashMap::new();
    for db_path in thread_state_db_paths(home) {
        if !db_path.exists() || !sqlite_has_table(&db_path, "threads") {
            continue;
        }
        let conn = Connection::open_with_flags(&db_path, OpenFlags::SQLITE_OPEN_READ_ONLY)
            .with_context(|| format!("读取 Codex thread 数据库失败：{}", db_path.display()))?;
        let columns = sqlite_table_columns(&conn, "threads")?;
        let selected = [
            "id",
            "rollout_path",
            "updated_at",
            "updated_at_ms",
            "archived",
        ]
        .into_iter()
        .map(|name| {
            if columns.contains(name) {
                format!("{name} AS {name}")
            } else {
                format!("NULL AS {name}")
            }
        })
        .collect::<Vec<_>>()
        .join(", ");
        let mut statement = conn.prepare(&format!("SELECT {selected} FROM threads"))?;
        let rows = statement.query_map([], |row| {
            let thread_id = row_string(row, 0).unwrap_or_default();
            let rollout_path = row_string(row, 1).unwrap_or_default();
            let updated_at_ms = row_timestamp(row, 2)
                .or_else(|| row_timestamp_millis(row, 3))
                .map(|value| (value * 1000.0).round() as i64)
                .unwrap_or(0);
            let archived = row_bool(row, 4).unwrap_or(false);
            Ok((thread_id, rollout_path, updated_at_ms, archived))
        })?;
        for row in rows {
            let (thread_id, rollout_path, updated_at_ms, archived) = row?;
            if archived || thread_id.trim().is_empty() || rollout_path.trim().is_empty() {
                continue;
            }
            let path = resolve_rollout_path(home, &rollout_path);
            if !path.exists() {
                continue;
            }
            let updated_at_ms = if updated_at_ms > 0 {
                updated_at_ms
            } else {
                file_modified(&path)
                    .ok()
                    .and_then(system_time_to_unix_ms)
                    .unwrap_or(0)
            };
            let entry = ThreadRolloutEntry {
                thread_id: thread_id.clone(),
                rollout_path: path,
                updated_at_ms,
            };
            by_thread
                .entry(thread_id)
                .and_modify(|existing| {
                    if entry.updated_at_ms >= existing.updated_at_ms {
                        *existing = entry.clone();
                    }
                })
                .or_insert(entry);
        }
    }
    let mut entries = by_thread.into_values().collect::<Vec<_>>();
    entries.sort_by(|left, right| {
        right
            .updated_at_ms
            .cmp(&left.updated_at_ms)
            .then_with(|| left.thread_id.cmp(&right.thread_id))
    });
    entries.truncate(limit);
    Ok(entries)
}

fn resolve_rollout_path(home: &Path, value: &str) -> PathBuf {
    let path = PathBuf::from(value);
    if path.is_absolute() {
        path
    } else {
        home.join(path)
    }
}

fn system_time_to_unix_ms(value: SystemTime) -> Option<i64> {
    value
        .duration_since(SystemTime::UNIX_EPOCH)
        .ok()
        .map(|duration| duration.as_millis() as i64)
}

fn latest_plan_snapshot_from_rollout(
    thread_id: &str,
    rollout_path: &Path,
    updated_at_ms: i64,
) -> anyhow::Result<Option<PlanUiSnapshotSeed>> {
    let file = fs::File::open(rollout_path)
        .with_context(|| format!("读取 rollout 失败：{}", rollout_path.display()))?;
    let reader = io::BufReader::new(file);
    let mut latest: Option<PlanUiSnapshotSeed> = None;
    let mut sequence: i64 = 0;
    for (line_index, line) in reader.lines().enumerate() {
        let line = line?;
        if !line.contains("update_plan") {
            continue;
        }
        let Ok(value) = serde_json::from_str::<Value>(&line) else {
            continue;
        };
        let mut calls = Vec::new();
        collect_update_plan_calls(&value, &mut calls);
        for call in calls {
            let Some(rows) = rows_from_update_plan_arguments(&call.arguments) else {
                continue;
            };
            sequence += 1;
            let at = updated_at_ms
                .saturating_add(sequence)
                .saturating_add(line_index as i64);
            let source_id = if call.call_id.trim().is_empty() {
                format!("rollout-plan-{}-{sequence}", line_index + 1)
            } else {
                call.call_id.clone()
            };
            if rows.is_empty() {
                if let Some(previous) = latest.as_mut() {
                    for row in &mut previous.rows {
                        row.status = "done".to_string();
                        row.icon_html.clear();
                    }
                    previous.items = previous.rows.iter().map(|row| row.text.clone()).collect();
                    previous.progress = progress_from_plan_rows(&previous.rows);
                    previous.source_turn_id = source_id.clone();
                    previous.source_todo_id = source_id;
                    previous.at = at;
                    previous.source = "rollout-update-plan-cleared".to_string();
                }
                continue;
            }
            latest = Some(PlanUiSnapshotSeed {
                thread_id: normalize_seed_thread_id(thread_id),
                progress: progress_from_plan_rows(&rows),
                detail: String::new(),
                items: rows.iter().map(|row| row.text.clone()).collect(),
                rows,
                source_conversation_id: raw_seed_conversation_id(thread_id),
                source_turn_id: source_id.clone(),
                source_todo_id: source_id,
                at,
                source: "rollout-update-plan".to_string(),
            });
        }
    }
    Ok(latest)
}

fn collect_update_plan_calls(value: &Value, calls: &mut Vec<UpdatePlanCall>) {
    match value {
        Value::Object(map) => {
            if map
                .get("name")
                .and_then(Value::as_str)
                .is_some_and(|name| name == "update_plan")
            {
                if let Some(arguments) = map.get("arguments").and_then(parse_update_plan_arguments)
                {
                    let call_id = map
                        .get("call_id")
                        .or_else(|| map.get("id"))
                        .and_then(Value::as_str)
                        .unwrap_or_default()
                        .to_string();
                    calls.push(UpdatePlanCall { arguments, call_id });
                }
            }
            for nested in map.values() {
                if matches!(nested, Value::Object(_) | Value::Array(_)) {
                    collect_update_plan_calls(nested, calls);
                }
            }
        }
        Value::Array(items) => {
            for nested in items {
                collect_update_plan_calls(nested, calls);
            }
        }
        _ => {}
    }
}

fn parse_update_plan_arguments(value: &Value) -> Option<Value> {
    match value {
        Value::String(text) => serde_json::from_str(text).ok(),
        Value::Object(_) => Some(value.clone()),
        _ => None,
    }
}

fn rows_from_update_plan_arguments(arguments: &Value) -> Option<Vec<PlanUiSnapshotSeedRow>> {
    let plan = arguments.get("plan")?.as_array()?;
    let mut rows = Vec::new();
    let mut seen = HashSet::new();
    for row in plan.iter().take(12) {
        let text = row
            .get("step")
            .or_else(|| row.get("text"))
            .or_else(|| row.get("title"))
            .and_then(Value::as_str)
            .and_then(normalize_plan_step_text);
        let Some(text) = text else {
            continue;
        };
        if !seen.insert(text.clone()) {
            continue;
        }
        rows.push(PlanUiSnapshotSeedRow {
            text,
            status: normalize_plan_status(row.get("status").and_then(Value::as_str)),
            icon_html: String::new(),
            text_color: String::new(),
            icon_color: String::new(),
        });
    }
    Some(rows)
}

fn normalize_plan_step_text(value: &str) -> Option<String> {
    let text = value.split_whitespace().collect::<Vec<_>>().join(" ");
    if text.is_empty() || text.chars().count() > 180 {
        None
    } else {
        Some(text)
    }
}

fn normalize_plan_status(status: Option<&str>) -> String {
    let normalized = status
        .unwrap_or_default()
        .chars()
        .filter(|ch| !ch.is_whitespace() && *ch != '_' && *ch != '-')
        .collect::<String>()
        .to_ascii_lowercase();
    match normalized.as_str() {
        "completed" | "complete" | "done" => "done",
        "inprogress" | "running" | "active" => "running",
        _ => "pending",
    }
    .to_string()
}

fn progress_from_plan_rows(rows: &[PlanUiSnapshotSeedRow]) -> String {
    if rows.is_empty() {
        return "任务清单".to_string();
    }
    let current = rows
        .iter()
        .position(|row| row.status == "running")
        .map(|index| index + 1)
        .unwrap_or_else(|| {
            let done = rows.iter().filter(|row| row.status == "done").count();
            done.max(1)
        });
    format!("第 {} / {} 步", current.min(rows.len()).max(1), rows.len())
}

fn normalize_seed_thread_id(thread_id: &str) -> String {
    let value = thread_id.trim();
    if value.starts_with("local:") || value.starts_with("remote:") || value.starts_with("visible:")
    {
        value.to_string()
    } else {
        format!("local:{value}")
    }
}

fn raw_seed_conversation_id(thread_id: &str) -> String {
    thread_id
        .trim()
        .strip_prefix("local:")
        .or_else(|| thread_id.trim().strip_prefix("remote:"))
        .unwrap_or_else(|| thread_id.trim())
        .to_string()
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
        .with_context(|| format!("发送 CDP 命令失败：{method}"))?;

    loop {
        let message = tokio::time::timeout(Duration::from_secs(5), socket.next())
            .await
            .with_context(|| format!("等待 CDP 命令返回超时：{method}"))?
            .ok_or_else(|| anyhow::anyhow!("CDP websocket 已关闭：{method}"))?
            .with_context(|| format!("读取 CDP 命令返回失败：{method}"))?;
        let Message::Text(text) = message else {
            continue;
        };
        let value: Value =
            serde_json::from_str(&text).with_context(|| format!("解析 CDP 返回失败：{method}"))?;
        if value.get("id").and_then(Value::as_u64) != Some(message_id) {
            continue;
        }
        if let Some(error) = value.get("error") {
            bail!("CDP 命令失败 {method}: {error}");
        }
        return Ok(value.get("result").cloned().unwrap_or(Value::Null));
    }
}

fn cdp_value(result: &Value) -> Option<&Value> {
    result.get("result").and_then(|entry| entry.get("value"))
}

fn runtime_evaluate_params_return_by_value(script: &str) -> Value {
    let mut params = codex_lite::runtime_evaluate_params(script);
    if let Some(object) = params.as_object_mut() {
        object.insert("returnByValue".to_string(), Value::Bool(true));
    }
    params
}

fn pick_lite_main_codex_page_target(
    targets: &[codex_lite::CdpTarget],
) -> anyhow::Result<codex_lite::CdpTarget> {
    let injectable =
        |target: &&codex_lite::CdpTarget| codex_lite::is_injectable_page_target(target);
    if let Some(target) = targets
        .iter()
        .filter(injectable)
        .find(|target| target.url == "app://-/index.html")
    {
        return Ok(target.clone());
    }
    if let Some(target) = targets.iter().filter(injectable).find(|target| {
        target.url.starts_with("app://-/index.html") && !target.url.contains("avatar-overlay")
    }) {
        return Ok(target.clone());
    }
    codex_lite::pick_injectable_codex_page_target(targets)
}

async fn cdp_ready(debug_port: u16) -> anyhow::Result<()> {
    let targets = codex_lite::list_targets(debug_port).await?;
    pick_lite_main_codex_page_target(&targets)?;
    Ok(())
}

async fn wait_and_inject_lite_model_whitelist(
    debug_port: u16,
    codex_home: Option<&Path>,
) -> anyhow::Result<()> {
    let mut last_error = None;
    for _ in 0..40 {
        match inject_lite_model_whitelist(debug_port, codex_home).await {
            Ok(()) => return Ok(()),
            Err(error) => {
                last_error = Some(error);
                tokio::time::sleep(Duration::from_millis(500)).await;
            }
        }
    }
    Err(last_error.unwrap_or_else(|| anyhow::anyhow!("Lite 模型白名单注入失败")))
}

async fn inject_lite_model_whitelist(
    debug_port: u16,
    codex_home: Option<&Path>,
) -> anyhow::Result<()> {
    inject_lite_model_whitelist_inner(debug_port, codex_home, true).await
}

async fn inject_lite_model_whitelist_quiet(
    debug_port: u16,
    codex_home: Option<&Path>,
) -> anyhow::Result<()> {
    inject_lite_model_whitelist_inner(debug_port, codex_home, false).await
}

async fn inject_lite_model_whitelist_inner(
    debug_port: u16,
    codex_home: Option<&Path>,
    verbose: bool,
) -> anyhow::Result<()> {
    let targets = codex_lite::list_targets(debug_port)
        .await
        .with_context(|| {
            format!(
                "无法连接 Codex CDP 端口 {debug_port}；Codex 需要带 --remote-debugging-port 启动"
            )
        })?;
    let target = pick_lite_main_codex_page_target(&targets)?;
    let websocket = target
        .web_socket_debugger_url
        .as_deref()
        .ok_or_else(|| anyhow::anyhow!("Codex CDP target 没有 websocket URL"))?;
    let home = resolve_codex_home(codex_home);
    let script = lite_model_whitelist_script(&home)?;
    codex_lite::evaluate_script(websocket, &script).await?;
    if verbose {
        println!("已注入 Lite 模型白名单补丁");
    }
    Ok(())
}

fn lite_model_whitelist_script(home: &Path) -> anyhow::Result<String> {
    let catalog = lite_model_catalog_from_home(home);
    let catalog_json = serde_json::to_string(&catalog)?;
    Ok(LITE_MODEL_WHITELIST_SCRIPT.replace("__LITE_MODEL_CATALOG__", &catalog_json))
}

fn lite_model_catalog_from_home(home: &Path) -> Value {
    let config_path = home.join("config.toml");
    let config_text = match fs::read_to_string(&config_path) {
        Ok(value) => value,
        Err(error) => {
            return json!({
                "status": "failed",
                "path": config_path.to_string_lossy(),
                "message": error.to_string(),
                "model": "",
                "model_provider": "",
                "provider_name": "",
                "default_model": "",
                "models": []
            });
        }
    };
    let model = root_config_string_value(&config_text, "model").unwrap_or_default();
    let model_provider =
        root_config_string_value(&config_text, "model_provider").unwrap_or_default();
    let catalog_path = root_config_string_value(&config_text, "model_catalog_json")
        .map(|value| resolve_config_relative_path(home, &value));
    let mut model_entries = catalog_path
        .as_deref()
        .and_then(|path| fs::read_to_string(path).ok())
        .and_then(|contents| serde_json::from_str::<Value>(&contents).ok())
        .map(|payload| model_entries_from_catalog_payload(&payload))
        .unwrap_or_default();
    if model_entries.is_empty() && !model.trim().is_empty() {
        model_entries.push(model_entry_from_name(model.trim()));
    }
    let model_names = unique_model_ids(
        model_entries
            .iter()
            .filter_map(catalog_model_name)
            .collect::<Vec<_>>(),
    );
    let default_model = if model_names.iter().any(|item| item == &model) {
        model.clone()
    } else {
        model_names.first().cloned().unwrap_or_default()
    };
    json!({
        "status": if model_names.is_empty() { "not_configured" } else { "ok" },
        "path": config_path.to_string_lossy(),
        "catalog_path": catalog_path.map(|path| path.to_string_lossy().to_string()).unwrap_or_default(),
        "model": model,
        "model_provider": model_provider,
        "provider_name": model_provider,
        "default_model": default_model,
        "model_names": model_names,
        "models": model_entries
    })
}

fn resolve_config_relative_path(home: &Path, value: &str) -> PathBuf {
    let path = PathBuf::from(value);
    if path.is_absolute() {
        path
    } else {
        home.join(path)
    }
}

fn root_config_string_value(config_text: &str, key: &str) -> Option<String> {
    for line in config_text.lines() {
        let trimmed = line.trim();
        if trimmed.starts_with('[') {
            return None;
        }
        if trimmed.is_empty() || trimmed.starts_with('#') {
            continue;
        }
        let Some((raw_key, raw_value)) = trimmed.split_once('=') else {
            continue;
        };
        if raw_key.trim() != key {
            continue;
        }
        return parse_simple_config_string(raw_value.trim());
    }
    None
}

fn parse_simple_config_string(raw: &str) -> Option<String> {
    let raw = raw.trim();
    if raw.is_empty() {
        return Some(String::new());
    }
    if raw.starts_with('"') {
        let mut escaped = false;
        for (index, ch) in raw.char_indices().skip(1) {
            if escaped {
                escaped = false;
                continue;
            }
            if ch == '\\' {
                escaped = true;
                continue;
            }
            if ch == '"' {
                return serde_json::from_str::<String>(&raw[..=index]).ok();
            }
        }
        return None;
    }
    let value = raw
        .split('#')
        .next()
        .unwrap_or("")
        .trim()
        .trim_matches('\'')
        .to_string();
    Some(value)
}

fn model_entries_from_catalog_payload(payload: &Value) -> Vec<Value> {
    let Some(models) = payload.get("models").and_then(Value::as_array) else {
        return Vec::new();
    };
    let mut entries = Vec::new();
    let mut seen = Vec::<String>::new();
    for model in models
        .iter()
        .filter(|model| catalog_model_visible_in_lite(model))
    {
        let Some(name) = catalog_model_name(model) else {
            continue;
        };
        if seen.iter().any(|existing| existing == &name) {
            continue;
        }
        seen.push(name.clone());
        entries.push(normalize_lite_catalog_model_entry(model, &name));
    }
    entries
}

fn catalog_model_name(model: &Value) -> Option<String> {
    if let Some(value) = model.as_str() {
        return non_empty_string(value);
    }
    model
        .get("slug")
        .or_else(|| model.get("id"))
        .or_else(|| model.get("model"))
        .or_else(|| model.get("name"))
        .and_then(Value::as_str)
        .and_then(non_empty_string)
}

fn non_empty_string(value: &str) -> Option<String> {
    let trimmed = value.trim();
    (!trimmed.is_empty()).then(|| trimmed.to_string())
}

fn model_entry_from_name(name: &str) -> Value {
    normalize_lite_catalog_model_entry(&json!({ "model": name }), name)
}

fn normalize_lite_catalog_model_entry(model: &Value, name: &str) -> Value {
    let mut entry = json!({});
    let object = entry
        .as_object_mut()
        .expect("normalized model entry is an object");
    if let Some(source) = model.as_object() {
        for key in [
            "id",
            "slug",
            "model",
            "name",
            "displayName",
            "display_name",
            "description",
            "context_window",
            "contextWindow",
            "model_context_window",
            "modelContextWindow",
            "max_context_window",
            "maxContextWindow",
            "effective_context_window_percent",
            "effectiveContextWindowPercent",
            "auto_compact_token_limit",
            "autoCompactTokenLimit",
            "model_auto_compact_token_limit",
            "modelAutoCompactTokenLimit",
            "defaultReasoningEffort",
            "default_reasoning_effort",
            "supportedReasoningEfforts",
            "supported_reasoning_efforts",
            "supported_reasoning_levels",
            "support_verbosity",
            "supports_reasoning_summaries",
            "supports_parallel_tool_calls",
            "supports_search_tool",
            "supports_image_detail_original",
            "shell_type",
            "apply_patch_tool_type",
            "visibility",
            "supported_in_api",
        ] {
            if let Some(value) = source.get(key) {
                object.insert(key.to_string(), value.clone());
            }
        }
    }
    object.insert("model".to_string(), json!(name));
    object
        .entry("id".to_string())
        .or_insert_with(|| json!(name));
    object
        .entry("slug".to_string())
        .or_insert_with(|| json!(name));
    object
        .entry("name".to_string())
        .or_insert_with(|| json!(name));
    let display_name = object
        .get("displayName")
        .or_else(|| object.get("display_name"))
        .and_then(Value::as_str)
        .and_then(non_empty_string)
        .unwrap_or_else(|| name.to_string());
    object
        .entry("displayName".to_string())
        .or_insert_with(|| json!(display_name.clone()));
    object
        .entry("display_name".to_string())
        .or_insert_with(|| json!(display_name));
    object.insert("hidden".to_string(), json!(false));
    entry
}

fn catalog_model_visible_in_lite(model: &Value) -> bool {
    if !model
        .get("supported_in_api")
        .and_then(Value::as_bool)
        .unwrap_or(true)
    {
        return false;
    }
    model
        .get("visibility")
        .and_then(Value::as_str)
        .unwrap_or("list")
        .trim()
        .eq_ignore_ascii_case("list")
}

fn unique_model_ids(values: Vec<String>) -> Vec<String> {
    let mut unique = Vec::new();
    for value in values {
        let value = value.trim();
        if value.is_empty() || unique.iter().any(|existing: &String| existing == value) {
            continue;
        }
        unique.push(value.to_string());
    }
    unique
}

const LITE_MODEL_WHITELIST_SCRIPT: &str = r##"
(() => {
  const SCRIPT_VERSION = 8;
  const REQUEST_GUARD = "__codex_gateway_lite_model_list_only__";
  const SEND_REQUEST_PATCH_MARK = `codex-gateway-lite-send-request-v${SCRIPT_VERSION}`;
  const catalog = __LITE_MODEL_CATALOG__;
  const state = window.__codexGatewayLiteModelWhitelist || {};
  window.__codexGatewayLiteModelWhitelist = state;
  function restoreAppServerModelRequestPatches(targetState) {
    const clients = Array.isArray(targetState?.appServerPatchedClients) ? targetState.appServerPatchedClients : [];
    clients.forEach((client) => restoreAppServerModelRequestClient(client));
    if (targetState) targetState.appServerPatchedClients = [];
  }
  function restoreAppServerModelRequestClient(client) {
    if (!client || typeof client !== "object") return false;
    const original = client.__codexGatewayLiteModelOriginalSendRequest;
    if (typeof original !== "function") return false;
    try {
      if (client.sendRequest !== original) client.sendRequest = original;
      delete client.__codexGatewayLiteModelRequestPatch;
      return true;
    } catch {
      return false;
    }
  }
  if (state.scriptVersion !== SCRIPT_VERSION) {
    try {
      restoreAppServerModelRequestPatches(state);
    } catch (_) {}
    try {
      if (state.originals?.responseJson && Response.prototype.json !== state.originals.responseJson) {
        Response.prototype.json = state.originals.responseJson;
      }
    } catch (_) {}
    try {
      if (state.originals?.dispatchEvent && window.dispatchEvent !== state.originals.dispatchEvent) {
        window.dispatchEvent = state.originals.dispatchEvent;
      }
    } catch (_) {}
    try {
      if (state.refreshTimer) window.clearTimeout(state.refreshTimer);
    } catch (_) {}
    try {
      state.observer?.disconnect?.();
    } catch (_) {}
    state.refreshTimer = 0;
    state.refreshUntil = 0;
    state.observer = null;
    state.observerStarted = false;
    state.responseJsonPatched = false;
    state.dispatchPatched = false;
    state.appServerPatchInstalled = false;
    state.appServerPatchInFlight = false;
    state.appServerPatchUnavailable = false;
    state.appServerPatchLastAttemptAt = 0;
    state.modulePromises = new Map();
  }
  state.scriptVersion = SCRIPT_VERSION;
  state.catalog = catalog && typeof catalog === "object" ? catalog : { models: [] };
  state.requestIds = state.requestIds instanceof Set ? state.requestIds : new Set();
  state.requestIds.add(REQUEST_GUARD);
  state.failures = state.failures || [];
  state.modulePromises = state.modulePromises || new Map();
  state.failures = state.failures.filter((failure) => !String(failure).includes("app-server-manager-signals-"));

  function recordFailure(error) {
    try {
      state.failures.push(String(error?.stack || error));
      if (state.failures.length > 20) state.failures.splice(0, state.failures.length - 20);
    } catch (_) {}
  }

  function uniqueValues(values) {
    return Array.from(new Set(values.filter((value) => typeof value === "string" && value.trim().length > 0)));
  }

  function catalogModelName(entry) {
    if (typeof entry === "string") return entry.trim();
    if (!entry || typeof entry !== "object") return "";
    return String(entry.model || entry.id || entry.slug || entry.name || "").trim();
  }

  function catalogModelEntries() {
    const source = state.catalog || {};
    const entries = [];
    if (Array.isArray(source.models)) entries.push(...source.models);
    if (source.model && !entries.some((entry) => catalogModelName(entry) === source.model)) {
      entries.unshift({ model: source.model });
    }
    return entries;
  }

  function modelNames() {
    const source = state.catalog || {};
    return uniqueValues([
      source.default_model,
      source.model,
      ...(Array.isArray(source.model_names) ? source.model_names : []),
      ...catalogModelEntries().map(catalogModelName),
    ]);
  }

  function modelReasoningEfforts() {
    return ["minimal", "low", "medium", "high", "xhigh"].map((reasoningEffort) => ({ reasoningEffort, description: `${reasoningEffort} effort` }));
  }

  function positiveInteger(value) {
    if (typeof value === "number" && Number.isFinite(value) && value > 0) return Math.floor(value);
    if (typeof value === "string" && value.trim()) {
      const parsed = Number(value.trim());
      if (Number.isFinite(parsed) && parsed > 0) return Math.floor(parsed);
    }
    return null;
  }

  function normalizedReasoningEfforts(value) {
    if (!Array.isArray(value) || !value.length) return null;
    const efforts = value.map((item) => {
      if (typeof item === "string") return { reasoningEffort: item, description: `${item} effort` };
      if (item && typeof item === "object" && typeof item.reasoningEffort === "string") return item;
      if (item && typeof item === "object" && typeof item.reasoning_effort === "string") {
        return { reasoningEffort: item.reasoning_effort, description: item.description || `${item.reasoning_effort} effort` };
      }
      return null;
    }).filter(Boolean);
    return efforts.length ? efforts : null;
  }

  function catalogEntryForModel(modelName) {
    const entries = catalogModelEntries();
    return entries.find((entry) => {
      if (typeof entry === "string") return entry.trim() === modelName;
      if (!entry || typeof entry !== "object") return false;
      return [entry.model, entry.id, entry.slug, entry.name].some((value) => String(value || "").trim() === modelName);
    }) || null;
  }

  function modelMetadata(modelName) {
    const entry = catalogEntryForModel(modelName);
    if (!entry || typeof entry !== "object") return {};
    const metadata = { ...entry };
    const contextWindow = positiveInteger(entry.context_window ?? entry.contextWindow ?? entry.model_context_window ?? entry.modelContextWindow);
    const maxContextWindow = positiveInteger(entry.max_context_window ?? entry.maxContextWindow) || contextWindow;
    const autoCompactLimit = positiveInteger(entry.auto_compact_token_limit ?? entry.autoCompactTokenLimit ?? entry.model_auto_compact_token_limit ?? entry.modelAutoCompactTokenLimit);
    const percent = positiveInteger(entry.effective_context_window_percent ?? entry.effectiveContextWindowPercent);
    if (contextWindow) {
      metadata.context_window = contextWindow;
      metadata.contextWindow = contextWindow;
      metadata.model_context_window = contextWindow;
      metadata.modelContextWindow = contextWindow;
    }
    if (maxContextWindow) {
      metadata.max_context_window = maxContextWindow;
      metadata.maxContextWindow = maxContextWindow;
    }
    if (autoCompactLimit) {
      metadata.auto_compact_token_limit = autoCompactLimit;
      metadata.autoCompactTokenLimit = autoCompactLimit;
      metadata.model_auto_compact_token_limit = autoCompactLimit;
      metadata.modelAutoCompactTokenLimit = autoCompactLimit;
    }
    if (percent) {
      metadata.effective_context_window_percent = percent;
      metadata.effectiveContextWindowPercent = percent;
    }
    const displayName = String(entry.displayName || entry.display_name || modelName).trim();
    metadata.displayName = displayName || modelName;
    metadata.display_name = displayName || modelName;
    const reasoningEfforts = normalizedReasoningEfforts(entry.supportedReasoningEfforts || entry.supported_reasoning_efforts);
    if (reasoningEfforts) metadata.supportedReasoningEfforts = reasoningEfforts;
    if (typeof entry.defaultReasoningEffort !== "string" && typeof entry.default_reasoning_effort === "string") {
      metadata.defaultReasoningEffort = entry.default_reasoning_effort;
    }
    return metadata;
  }

  function modelDescriptor(modelName) {
    const source = state.catalog || {};
    const metadata = modelMetadata(modelName);
    const supportedReasoningEfforts = Array.isArray(metadata.supportedReasoningEfforts)
      ? metadata.supportedReasoningEfforts
      : modelReasoningEfforts();
    return {
      ...metadata,
      model: modelName,
      id: metadata.id || modelName,
      slug: metadata.slug || modelName,
      name: metadata.name || modelName,
      displayName: metadata.displayName || modelName,
      display_name: metadata.display_name || metadata.displayName || modelName,
      description: metadata.description || source.provider_name || source.model_provider || "Custom model",
      hidden: false,
      isDefault: (source.default_model || source.model) === modelName,
      defaultReasoningEffort: metadata.defaultReasoningEffort || "medium",
      supportedReasoningEfforts,
    };
  }

  function assignModelDescriptor(target, modelName) {
    if (!target || typeof target !== "object") return false;
    const descriptor = modelDescriptor(modelName);
    let changed = false;
    for (const [key, value] of Object.entries(descriptor)) {
      if (value === undefined) continue;
      const current = target[key];
      const same = current === value || JSON.stringify(current) === JSON.stringify(value);
      if (!same) {
        target[key] = value;
        changed = true;
      }
    }
    return changed;
  }

  function hasOwn(target, key) {
    return !!target && Object.prototype.hasOwnProperty.call(Object(target), key);
  }

  function modelEntryLooksPatchable(value, allowBareModel = false) {
    if (!value || typeof value !== "object" || typeof value.model !== "string") return false;
    if (allowBareModel) return true;
    return [
      "id",
      "slug",
      "name",
      "displayName",
      "display_name",
      "description",
      "context_window",
      "contextWindow",
      "model_context_window",
      "modelContextWindow",
      "max_context_window",
      "maxContextWindow",
      "supportedReasoningEfforts",
      "supported_reasoning_efforts",
      "defaultReasoningEffort",
      "default_reasoning_effort",
    ].some((key) => hasOwn(value, key));
  }

  function modelArrayLooksPatchable(value, allowEmpty = false, allowBareModel = false) {
    return Array.isArray(value)
      && (allowEmpty || value.length > 0)
      && value.every((item) => modelEntryLooksPatchable(item, allowBareModel));
  }

  function stringArrayLooksPatchable(value) {
    return Array.isArray(value) && value.length > 0 && value.every((item) => typeof item === "string");
  }

  function stringModelArrayLooksPatchable(value) {
    if (!stringArrayLooksPatchable(value)) return false;
    const custom = modelNames();
    return value.some((item) => custom.includes(item) || /^(gpt|o\d|claude|gemini|grok|deepseek|qwen|llama|mistral|kimi|doubao|glm)[\w.:-]*/i.test(item));
  }

  function patchModelNameArray(models) {
    if (!stringModelArrayLooksPatchable(models)) return false;
    const customModels = modelNames();
    if (!customModels.length) return false;
    let changed = false;
    customModels.forEach((modelName) => {
      if (!models.includes(modelName)) {
        models.push(modelName);
        changed = true;
      }
    });
    return changed;
  }

  function patchModelArray(models, allowEmpty = false, allowBareModel = false) {
    if (!modelArrayLooksPatchable(models, allowEmpty, allowBareModel)) return false;
    const customModels = modelNames();
    if (!customModels.length) return false;
    let changed = false;
    const existing = new Map(models.map((item) => [item.model, item]));
    models.forEach((item) => {
      const modelName = catalogModelName(item);
      if (customModels.includes(modelName)) {
        if (assignModelDescriptor(item, modelName)) changed = true;
      }
    });
    customModels.forEach((modelName) => {
      if (!existing.has(modelName)) {
        models.push(modelDescriptor(modelName));
        changed = true;
      }
    });
    return changed;
  }

  function patchModelContainer(value, allowBareModelArrays = false) {
    if (!value || typeof value !== "object") return false;
    let changed = false;
    const modelContainerAllowsBare = allowBareModelArrays || hasOwn(value, "defaultModel") || hasOwn(value, "availableModels");
    if (hasOwn(value, "models") && patchModelArray(value.models, modelContainerAllowsBare, modelContainerAllowsBare)) changed = true;
    if (hasOwn(value, "models") && patchModelNameArray(value.models)) changed = true;
    if (hasOwn(value, "data") && patchModelArray(value.data, false, allowBareModelArrays)) changed = true;
    if (hasOwn(value, "result") && patchModelArray(value.result, false, allowBareModelArrays)) changed = true;
    if (hasOwn(value, "pages") && patchModelArray(value.pages?.[0]?.data, false, allowBareModelArrays)) changed = true;
    if (hasOwn(value, "result") && patchModelArray(value.result?.data, false, allowBareModelArrays)) changed = true;
    if (hasOwn(value, "result") && patchModelArray(value.result?.models, false, allowBareModelArrays)) changed = true;
    if (hasOwn(value, "message") && patchModelArray(value.message?.result?.data, false, allowBareModelArrays)) changed = true;
    if (hasOwn(value, "message") && patchModelArray(value.message?.result?.models, false, allowBareModelArrays)) changed = true;
    const names = modelNames();
    if (hasOwn(value, "availableModels") && value.availableModels instanceof Set) {
      names.forEach((name) => {
        if (!value.availableModels.has(name)) {
          value.availableModels.add(name);
          changed = true;
        }
      });
    }
    if (hasOwn(value, "available_models") && value.available_models instanceof Set) {
      names.forEach((name) => {
        if (!value.available_models.has(name)) {
          value.available_models.add(name);
          changed = true;
        }
      });
    }
    if (hasOwn(value, "availableModels") && Array.isArray(value.availableModels)) {
      names.forEach((name) => {
        if (!value.availableModels.includes(name)) {
          value.availableModels.push(name);
          changed = true;
        }
      });
    }
    if (hasOwn(value, "available_models") && Array.isArray(value.available_models)) {
      names.forEach((name) => {
        if (!value.available_models.includes(name)) {
          value.available_models.push(name);
          changed = true;
        }
      });
    }
    if (hasOwn(value, "hiddenModels") && Array.isArray(value.hiddenModels)) {
      const before = value.hiddenModels.length;
      value.hiddenModels = value.hiddenModels.filter((name) => !names.includes(name));
      if (value.hiddenModels.length !== before) changed = true;
    }
    if (hasOwn(value, "hidden_models") && Array.isArray(value.hidden_models)) {
      const before = value.hidden_models.length;
      value.hidden_models = value.hidden_models.filter((name) => !names.includes(name));
      if (value.hidden_models.length !== before) changed = true;
    }
    if (hasOwn(value, "defaultModel") && value.defaultModel == null && names.length > 0) {
      value.defaultModel = modelDescriptor(names[0]);
      changed = true;
    } else if (hasOwn(value, "defaultModel") && typeof value.defaultModel === "string" && names.includes(value.defaultModel) && !hasOwn(value, "model")) {
      value.model = value.defaultModel;
      changed = true;
    }
    return changed;
  }

  function patchStatsigModelDynamicConfig(config) {
    const names = modelNames();
    const value = config?.value;
    if (!names.length || !value || typeof value !== "object") return config;
    const availableModels = Array.isArray(value.available_models) ? [...value.available_models] : [];
    let changed = false;
    names.forEach((name) => {
      if (!availableModels.includes(name)) {
        availableModels.push(name);
        changed = true;
      }
    });
    const nextValue = {
      ...value,
      available_models: availableModels,
      default_model: names[0] || value.default_model,
    };
    if (!changed && nextValue.default_model === value.default_model) return config;
    try {
      config.value = nextValue;
    } catch {
      return { ...config, value: nextValue };
    }
    return config;
  }

  function statsigClients() {
    const root = window.__STATSIG__ || globalThis.__STATSIG__;
    if (!root || typeof root !== "object") return [];
    const clients = [root.firstInstance, typeof root.instance === "function" ? root.instance() : null];
    if (root.instances && typeof root.instances === "object") clients.push(...Object.values(root.instances));
    return clients.filter((client, index, array) => client && typeof client === "object" && array.indexOf(client) === index);
  }

  function patchStatsigModelWhitelist() {
    statsigClients().forEach((client) => {
      if (typeof client.getDynamicConfig !== "function") return;
      if (!client.__codexGatewayLiteModelWhitelistPatched) {
        const originalGetDynamicConfig = client.getDynamicConfig.bind(client);
        client.getDynamicConfig = (name, options) => {
          const result = originalGetDynamicConfig(name, options);
          return patchStatsigModelDynamicConfig(result);
        };
        client.__codexGatewayLiteModelWhitelistPatched = true;
      }
      try {
        patchStatsigModelDynamicConfig(client.getDynamicConfig("107580212", { disableExposureLog: true }));
      } catch {
      }
    });
  }

  function isModelListRequestMethod(method) {
    return method === "model/list" || method === "list-models-for-host";
  }

  function patchOutboundModelRequestMessage(message) {
    const request = message?.request;
    if (message?.type !== "mcp-request" || !isModelListRequestMethod(request?.method)) return false;
    request.params = { ...(request.params || {}), includeHidden: true };
    if (request.id != null) state.requestIds.add(String(request.id));
    return true;
  }

  function patchMcpModelResponseData(data) {
    if (data?.type !== "mcp-response") return false;
    const message = data.message || data.response;
    const requestId = message?.id != null ? String(message.id) : "";
    if (!requestId || !state.requestIds.has(requestId)) return false;
    state.requestIds.delete(requestId);
    state.requestIds.add(REQUEST_GUARD);
    return patchModelContainer(data, true) || patchModelContainer(message, true) || patchModelContainer(message?.result, true) || patchModelContainer(message?.result?.data, true);
  }

  function patchElectronBridgeModelMessages() {
    const bridge = window.electronBridge;
    if (!bridge || typeof bridge.sendMessageFromView !== "function") return false;
    if (bridge.__codexGatewayLiteModelSendMessagePatch === SEND_REQUEST_PATCH_MARK) return true;
    const original = bridge.__codexGatewayLiteModelOriginalSendMessageFromView || bridge.sendMessageFromView.bind(bridge);
    bridge.__codexGatewayLiteModelOriginalSendMessageFromView = original;
    bridge.sendMessageFromView = function codexGatewayLitePatchedSendMessageFromView(message) {
      try {
        patchOutboundModelRequestMessage(message);
      } catch (error) {
        recordFailure(error);
      }
      return original(message);
    };
    bridge.__codexGatewayLiteModelSendMessagePatch = SEND_REQUEST_PATCH_MARK;
    return true;
  }

  function patchAppServerModelMessages() {
    patchElectronBridgeModelMessages();
    if (state.dispatchPatched) return;
    state.dispatchPatched = true;
    state.originals = state.originals || {};
    state.originals.dispatchEvent = state.originals.dispatchEvent || window.dispatchEvent;
    window.dispatchEvent = function codexGatewayLitePatchedDispatchEvent(event) {
      try {
        const detail = event?.detail;
        patchOutboundModelRequestMessage(detail);
        if (event?.type === "message") patchMcpModelResponseData(event.data);
        if (event?.type === "codex-message-to-view" || event?.type === "codex-message-from-view") patchMcpModelResponseData(detail);
      } catch (error) {
        recordFailure(error);
      }
      return state.originals.dispatchEvent.call(this, event);
    };
    window.addEventListener("message", (event) => {
      try {
        patchMcpModelResponseData(event?.data);
      } catch (error) {
        recordFailure(error);
      }
    }, true);
  }

  function codexAppAssetUrl(namePart) {
    const urls = [
      ...Array.from(document.scripts || []).map((script) => script.src),
      ...Array.from(document.querySelectorAll("link[href]") || []).map((link) => link.href),
      ...performance.getEntriesByType("resource").map((entry) => entry.name),
    ].filter(Boolean);
    return urls.find((url) => url.includes("/assets/") && url.includes(namePart) && url.split("?")[0].endsWith(".js")) || "";
  }

  async function fetchAssetText(url) {
    state.assetTextCache = state.assetTextCache || new Map();
    if (state.assetTextCache.has(url)) return state.assetTextCache.get(url);
    const text = await fetch(url).then((response) => response.ok ? response.text() : "");
    state.assetTextCache.set(url, text);
    if (state.assetTextCache.size > 100) {
      state.assetTextCache.delete(state.assetTextCache.keys().next().value);
    }
    return text;
  }

  function assetReferencesFromText(text, baseUrl) {
    const urls = [];
    const pattern = /["'`]([^"'`]+?\.js)["'`]/g;
    let match;
    while ((match = pattern.exec(text))) {
      try {
        const url = new URL(match[1], baseUrl).href;
        if (url.includes("/assets/") && url.split("?")[0].endsWith(".js")) urls.push(url);
      } catch {
      }
    }
    return urls;
  }

  function loadedAssetUrls() {
    return uniqueValues([
      ...Array.from(document.scripts || []).map((script) => script.src),
      ...Array.from(document.querySelectorAll("link[href]") || []).map((link) => link.href),
      ...performance.getEntriesByType("resource").map((entry) => entry.name),
    ]).filter((url) => url.includes("/assets/") && url.split("?")[0].endsWith(".js"));
  }

  async function discoveredAssetUrls() {
    const urls = [...loadedAssetUrls()];
    for (const src of loadedAssetUrls()) {
      try {
        const text = await fetchAssetText(src);
        urls.push(...assetReferencesFromText(text, src));
      } catch {
      }
    }
    return uniqueValues(urls);
  }

  async function codexAppAssetUrlFromScriptText(namePart) {
    for (const src of await discoveredAssetUrls()) {
      if (!src.includes(namePart)) continue;
      try {
        await fetchAssetText(src);
        return src;
      } catch {
      }
    }
    return "";
  }

  async function codexAppAssetUrlByText(needles) {
    const required = (needles || []).filter(Boolean);
    if (!required.length) return "";
    for (const src of await discoveredAssetUrls()) {
      try {
        const text = await fetchAssetText(src);
        if (required.every((needle) => text.includes(needle))) return src;
      } catch {
      }
    }
    return "";
  }

  async function loadCodexAppModule(namePart, textNeedles = []) {
    const cacheKey = `${namePart || ""}|${textNeedles.join("|")}`;
    if (!state.modulePromises.has(cacheKey)) {
      const promise = Promise.resolve().then(async () => {
        const url = codexAppAssetUrl(namePart)
          || await codexAppAssetUrlFromScriptText(namePart)
          || await codexAppAssetUrlByText(textNeedles);
        if (!url) throw new Error(`未找到 Codex App asset: ${namePart || textNeedles.join(",")}`);
        state.appServerPatchAssetUrl = url;
        return await import(url);
      }).catch((error) => {
        state.modulePromises.delete(cacheKey);
        throw error;
      });
      state.modulePromises.set(cacheKey, promise);
    }
    return await state.modulePromises.get(cacheKey);
  }

  function appServerModelRequestMethod(method, params) {
    if (method === "send-cli-request-for-host" && params?.method) return String(params.method);
    return String(method || "");
  }

  function patchAppServerModelResult(method, result) {
    if (method !== "list-models-for-host") return result;
    try {
      if (Array.isArray(result)) patchModelArray(result, true, true);
      if (Array.isArray(result?.data)) patchModelArray(result.data, true, true);
      if (Array.isArray(result?.models)) patchModelArray(result.models, true, true);
      patchModelContainer(result, true);
    } catch (error) {
      recordFailure(error);
    }
    return result;
  }

  function rememberAppServerPatchedClient(client) {
    const clients = Array.isArray(state.appServerPatchedClients) ? state.appServerPatchedClients : [];
    if (!clients.includes(client)) clients.push(client);
    state.appServerPatchedClients = clients.slice(-20);
  }

  function patchAppServerModelRequestClient(client) {
    if (!client || typeof client.sendRequest !== "function") return false;
    if (client.__codexGatewayLiteModelRequestPatch === SEND_REQUEST_PATCH_MARK) {
      rememberAppServerPatchedClient(client);
      return true;
    }
    if (client.__codexGatewayLiteModelRequestPatch) {
      restoreAppServerModelRequestClient(client);
    }
    const originalSendRequest = client.__codexGatewayLiteModelOriginalSendRequest || client.sendRequest.bind(client);
    client.__codexGatewayLiteModelOriginalSendRequest = originalSendRequest;
    client.sendRequest = async function codexGatewayLiteModelPatchedSendRequest(method, params, options) {
      const result = await originalSendRequest(method, params, options);
      return patchAppServerModelResult(appServerModelRequestMethod(String(method || ""), params), result);
    };
    client.__codexGatewayLiteModelRequestPatch = SEND_REQUEST_PATCH_MARK;
    rememberAppServerPatchedClient(client);
    return true;
  }

  function installAppServerModelRequestPatch() {
    if (state.appServerPatchInstalled || state.appServerPatchInFlight) return;
    const now = Date.now();
    if (state.appServerPatchLastAttemptAt && now - state.appServerPatchLastAttemptAt < 3_000) return;
    state.appServerPatchLastAttemptAt = now;
    state.appServerPatchUnavailable = false;
    state.appServerPatchInFlight = true;
    const patch = async () => {
      try {
        const module = await loadCodexAppModule("app-server-manager-signals-", [
          "list-models-for-host",
          "send-cli-request-for-host",
          "sendRequest",
        ]);
        const candidates = Object.values(module).filter((value) => value && typeof value === "object");
        let patchedCount = 0;
        for (const candidate of candidates) {
          if (patchAppServerModelRequestClient(candidate)) patchedCount += 1;
          if (typeof candidate.sendRequest !== "function" && typeof candidate.get === "function") {
            try {
              if (patchAppServerModelRequestClient(candidate.get())) patchedCount += 1;
            } catch {
            }
          }
        }
        if (patchedCount > 0) {
          state.appServerPatchInstalled = true;
          state.failures = (state.failures || []).filter((failure) => !String(failure).includes("app-server-manager-signals-"));
        }
        if (patchedCount === 0) throw new Error("未找到可 patch 的 app-server request client");
      } catch (error) {
        state.appServerPatchUnavailable = true;
        recordFailure(error);
      } finally {
        state.appServerPatchInFlight = false;
      }
    };
    void patch();
  }

  function cleanupCodexPlusArtifacts() {
    try {
      const raw = localStorage.getItem("codexPlusSettings");
      if (raw) {
        const settings = JSON.parse(raw);
        if (settings && settings.conversationView) {
          settings.conversationView = false;
          localStorage.setItem("codexPlusSettings", JSON.stringify(settings));
        }
      }
      localStorage.removeItem("codexPlus.threadCenter.maxWidth");
    } catch (_) {}
    try {
      window.__codexPlusConversationViewCleanup?.();
    } catch (_) {}
    try {
      document.querySelectorAll("#codex-plus-menu, [data-codex-plus-menu='true'], .codex-plus-modal-overlay, .codex-plus-modal-content").forEach((node) => node.remove());
      document.querySelectorAll("button").forEach((button) => {
        const text = (button.textContent || "").trim();
        if (/^Codex\+\+\s+\S+/.test(text)) button.remove();
      });
    } catch (_) {}
  }

  function installMutationObserver() {
    if (state.observer) return;
    state.observer = new MutationObserver((mutations) => {
      cleanupCodexPlusArtifacts();
    });
    const start = () => {
      if (!document.body || state.observerStarted) return;
      state.observer.observe(document.body, { childList: true, subtree: true });
      state.observerStarted = true;
    };
    start();
    if (!state.observerStarted) {
      document.addEventListener("DOMContentLoaded", start, { once: true });
    }
  }

  function appServerPatchedClientsHealthy() {
    const clients = Array.isArray(state.appServerPatchedClients) ? state.appServerPatchedClients : [];
    return clients.some((client) => (
      client
      && typeof client.sendRequest === "function"
      && client.__codexGatewayLiteModelRequestPatch === SEND_REQUEST_PATCH_MARK
    ));
  }

  function runRefreshPass() {
    if (!modelNames().length) return false;
    let changed = false;
    try {
      patchStatsigModelWhitelist();
      patchElectronBridgeModelMessages();
      if (state.appServerPatchInstalled && !appServerPatchedClientsHealthy()) {
        // 之前追踪的 app-server request client 全部失效了（例如底层模块内部
        // 重新创建了 client 实例），继续认为“已安装”只会让新的 client 漏 patch。
        // 重置安装标记，让下面这次调用重新找一遍当前真正生效的 client 并 patch。
        state.appServerPatchInstalled = false;
        state.appServerPatchedClients = [];
      }
      installAppServerModelRequestPatch();
    } catch (error) {
      recordFailure(error);
    }
    return changed;
  }

  function boot() {
    cleanupCodexPlusArtifacts();
    patchAppServerModelMessages();
    installAppServerModelRequestPatch();
    installMutationObserver();
    window.setTimeout(runRefreshPass, 250);
    window.setTimeout(runRefreshPass, 1000);
    window.setTimeout(runRefreshPass, 2500);
    let cleanupRuns = 0;
    const cleanupTimer = window.setInterval(() => {
      cleanupCodexPlusArtifacts();
      cleanupRuns += 1;
      if (cleanupRuns >= 8) window.clearInterval(cleanupTimer);
    }, 500);
  }

  boot();
})();
"##;

struct ProtocolProxyRuntime {
    port: u16,
    _handle: tokio::task::JoinHandle<()>,
}

impl Drop for ProtocolProxyRuntime {
    fn drop(&mut self) {
        println!("本地协议代理已停止：127.0.0.1:{}", self.port);
    }
}

struct LiteHttpRequest {
    method: String,
    path: String,
    headers: Vec<(String, String)>,
    body: String,
}

async fn start_protocol_proxy(config_path: PathBuf) -> anyhow::Result<ProtocolProxyRuntime> {
    let port = protocol_proxy::DEFAULT_PROTOCOL_PROXY_PORT;
    let bind_addr = format!("127.0.0.1:{port}");
    let listener = TcpListener::bind(&bind_addr).await.with_context(|| {
        format!(
            "绑定本地协议代理端口失败：{bind_addr}；请先运行 stop-agent，或在 Windows 执行 netstat -ano | findstr :{port} 检查端口占用"
        )
    })?;
    let local_addr = listener
        .local_addr()
        .context("读取本地协议代理监听地址失败")?;
    let handle = tokio::spawn(async move {
        loop {
            match listener.accept().await {
                Ok((stream, _)) => {
                    let config_path = config_path.clone();
                    tokio::spawn(async move {
                        if let Err(error) =
                            handle_protocol_proxy_connection(stream, config_path).await
                        {
                            eprintln!("本地协议代理请求失败：{error:#}");
                        }
                    });
                }
                Err(error) => {
                    eprintln!("本地协议代理 accept 失败：{error:#}");
                    tokio::time::sleep(Duration::from_millis(200)).await;
                }
            }
        }
    });
    println!("本地协议代理已启动：http://{local_addr}/v1");
    Ok(ProtocolProxyRuntime {
        port: local_addr.port(),
        _handle: handle,
    })
}

async fn handle_protocol_proxy_connection(
    mut stream: TcpStream,
    config_path: PathBuf,
) -> anyhow::Result<()> {
    let request = match read_lite_http_request(&mut stream).await {
        Ok(request) => request,
        Err(error) if protocol_proxy_request_closed_before_headers(&error) => return Ok(()),
        Err(error) => return Err(error),
    };

    // Streaming path for responses proxy (Chat Completions SSE → Responses SSE)
    if protocol_proxy::is_responses_proxy_path(&request.path) && request.method == "POST" {
        return handle_streaming_responses_proxy(stream, request, &config_path).await;
    }

    let response = match route_protocol_proxy_request(request, &config_path).await {
        Ok(response) => response,
        Err(error) => protocol_proxy_error_response(&error),
    };
    stream
        .write_all(&lite_http_response_bytes(
            &response.status,
            &response.content_type,
            &response.body,
        ))
        .await
        .context("写入本地协议代理响应失败")?;
    stream.shutdown().await.ok();
    Ok(())
}

fn protocol_proxy_request_closed_before_headers(error: &anyhow::Error) -> bool {
    error
        .chain()
        .any(|cause| cause.to_string().contains("本地协议代理连接提前关闭"))
}

async fn handle_streaming_responses_proxy(
    mut stream: TcpStream,
    request: LiteHttpRequest,
    config_path: &Path,
) -> anyhow::Result<()> {
    let config = read_lite_config(config_path)?;
    let upstream_config = protocol_proxy_upstream_from_config(&config)?;
    let user_agent = request
        .headers
        .iter()
        .find(|(name, _)| name.eq_ignore_ascii_case("user-agent"))
        .map(|(_, value)| value.as_str());

    let request_json: Value =
        serde_json::from_str(&request.body).context("解析 Responses 请求 JSON 失败")?;

    let upstream = match protocol_proxy::open_responses_proxy_request(
        &request.body,
        &upstream_config,
        user_agent,
    )
    .await
    {
        Ok(u) => u,
        Err(error) => {
            let response = protocol_proxy_error_response(&error);
            stream
                .write_all(&lite_http_response_bytes(
                    &response.status,
                    &response.content_type,
                    &response.body,
                ))
                .await?;
            stream.shutdown().await.ok();
            return Ok(());
        }
    };

    let can_passthrough_responses_stream = upstream.is_stream
        && upstream.is_success()
        && upstream.wire_api == protocol_proxy::UpstreamWireApi::Responses;
    let can_stream = upstream.is_stream
        && upstream.is_success()
        && upstream.wire_api == protocol_proxy::UpstreamWireApi::ChatCompletions;

    if can_passthrough_responses_stream {
        let content_type = if upstream.content_type.trim().is_empty() {
            "text/event-stream; charset=utf-8".to_string()
        } else {
            upstream.content_type.clone()
        };
        let header = format!(
            "HTTP/1.1 200 OK\r\nContent-Type: {content_type}\r\nTransfer-Encoding: chunked\r\nConnection: close\r\nAccess-Control-Allow-Origin: *\r\nAccess-Control-Allow-Headers: authorization,content-type\r\nAccess-Control-Allow-Methods: GET,POST,OPTIONS\r\n\r\n"
        );
        stream.write_all(header.as_bytes()).await?;
        let mut response = upstream.response;
        loop {
            match response.chunk().await {
                Ok(Some(chunk)) => write_http_chunk(&mut stream, &chunk).await?,
                Ok(None) => break,
                Err(error) => {
                    eprintln!("Responses 上游流中断: {error}");
                    break;
                }
            }
        }
        stream.write_all(b"0\r\n\r\n").await?;
    } else if can_stream {
        let header = "HTTP/1.1 200 OK\r\nContent-Type: text/event-stream; charset=utf-8\r\nTransfer-Encoding: chunked\r\nConnection: close\r\nAccess-Control-Allow-Origin: *\r\nAccess-Control-Allow-Headers: authorization,content-type\r\nAccess-Control-Allow-Methods: GET,POST,OPTIONS\r\n\r\n";
        stream.write_all(header.as_bytes()).await?;

        let mut converter =
            protocol_proxy::ChatSseToResponsesConverter::with_request(&request_json);
        let mut response = upstream.response;

        loop {
            match response.chunk().await {
                Ok(Some(chunk)) => {
                    let converted = converter.push_bytes(&chunk);
                    if !converted.is_empty() {
                        write_http_chunk(&mut stream, &converted).await?;
                    }
                }
                Ok(None) => break,
                Err(error) => {
                    let error_data = converter.fail(
                        format!("上游流中断: {error}"),
                        Some("upstream_error".to_string()),
                    );
                    if !error_data.is_empty() {
                        write_http_chunk(&mut stream, &error_data).await.ok();
                    }
                    break;
                }
            }
        }

        let final_data = converter.finish();
        if !final_data.is_empty() {
            write_http_chunk(&mut stream, &final_data).await?;
        }
        stream.write_all(b"0\r\n\r\n").await?;
    } else {
        let response = protocol_proxy::handle_responses_upstream(upstream, &request_json)
            .await
            .unwrap_or_else(|error| protocol_proxy_error_response(&error));
        stream
            .write_all(&lite_http_response_bytes(
                &response.status,
                &response.content_type,
                &response.body,
            ))
            .await?;
    }

    stream.shutdown().await.ok();
    Ok(())
}

async fn write_http_chunk(stream: &mut TcpStream, data: &[u8]) -> anyhow::Result<()> {
    if data.is_empty() {
        return Ok(());
    }
    let header = format!("{:x}\r\n", data.len());
    stream.write_all(header.as_bytes()).await?;
    stream.write_all(data).await?;
    stream.write_all(b"\r\n").await?;
    stream.flush().await?;
    Ok(())
}

async fn route_protocol_proxy_request(
    request: LiteHttpRequest,
    config_path: &Path,
) -> anyhow::Result<protocol_proxy::ProxyHttpResponse> {
    if request.method == "OPTIONS" {
        return Ok(protocol_proxy::ProxyHttpResponse {
            status: "204 No Content".to_string(),
            content_type: "text/plain; charset=utf-8".to_string(),
            body: Vec::new(),
        });
    }

    let config = read_lite_config(config_path)?;
    let upstream = protocol_proxy_upstream_from_config(&config)?;
    let user_agent = request
        .headers
        .iter()
        .find(|(name, _)| name.eq_ignore_ascii_case("user-agent"))
        .map(|(_, value)| value.as_str());

    if protocol_proxy::is_models_proxy_path(&request.path) {
        if request.method != "GET" {
            return Ok(method_not_allowed_response());
        }
        let upstream_response = protocol_proxy::open_models_proxy_request(&upstream, user_agent)
            .await
            .context("请求上游模型列表失败")?;
        return proxy_upstream_raw_response(upstream_response).await;
    }

    if protocol_proxy::is_responses_proxy_path(&request.path) {
        if request.method != "POST" {
            return Ok(method_not_allowed_response());
        }
        return protocol_proxy::handle_responses_proxy_request(
            &request.body,
            &upstream,
            user_agent,
        )
        .await;
    }

    if protocol_proxy::is_chat_completions_proxy_path(&request.path) {
        if request.method != "POST" {
            return Ok(method_not_allowed_response());
        }
        let upstream_response = protocol_proxy::open_chat_completions_proxy_request(
            &request.body,
            &upstream,
            user_agent,
        )
        .await
        .context("请求上游 Chat Completions 失败")?;
        return proxy_upstream_raw_response(upstream_response).await;
    }

    Ok(protocol_proxy::ProxyHttpResponse {
        status: "404 Not Found".to_string(),
        content_type: "application/json; charset=utf-8".to_string(),
        body: serde_json::to_vec(&json!({
            "error": {
                "message": format!("unsupported proxy path: {}", request.path),
                "type": "not_found"
            }
        }))?,
    })
}

async fn proxy_upstream_raw_response(
    upstream: protocol_proxy::UpstreamProxyResponse,
) -> anyhow::Result<protocol_proxy::ProxyHttpResponse> {
    let status = upstream.status();
    let content_type = if upstream.content_type.trim().is_empty() {
        "application/json; charset=utf-8".to_string()
    } else {
        upstream.content_type
    };
    let body = upstream.response.bytes().await?.to_vec();
    Ok(protocol_proxy::ProxyHttpResponse {
        status,
        content_type,
        body,
    })
}

fn protocol_proxy_upstream_from_config(
    config: &LiteConfig,
) -> anyhow::Result<protocol_proxy::ProtocolProxyUpstream> {
    let context_budget = resolve_context_budget(config);
    Ok(protocol_proxy::ProtocolProxyUpstream {
        id: sanitize_provider_id(&config.provider.id),
        name: if config.provider.name.trim().is_empty() {
            config.provider.id.trim().to_string()
        } else {
            config.provider.name.trim().to_string()
        },
        base_url: config
            .provider
            .base_url
            .trim()
            .trim_end_matches('/')
            .to_string(),
        api_key: resolve_api_key(&config.provider)?,
        protocol: config.provider.protocol.to_proxy(),
        user_agent: String::new(),
        context_budget,
    })
}

fn resolve_context_budget(config: &LiteConfig) -> protocol_proxy::ContextBudgetConfig {
    if let Some(reserve_tokens) = parse_context_budget_token(&config.provider.context_budget) {
        return protocol_proxy::ContextBudgetConfig::with_max_tokens(
            explicit_context_budget_limit(reserve_tokens, &config.context_window),
        );
    }
    if !config.context_window.trim().is_empty() {
        if let Some(tokens) = parse_window_token(config.context_window.trim()) {
            return protocol_proxy::ContextBudgetConfig::from_context_window(tokens);
        }
    }
    protocol_proxy::ContextBudgetConfig::default()
}

fn explicit_context_budget_limit(reserve_tokens: u64, context_window: &str) -> u64 {
    parse_window_token(context_window)
        .filter(|window_tokens| *window_tokens > reserve_tokens)
        .map(|window_tokens| window_tokens - reserve_tokens)
        .unwrap_or(reserve_tokens)
}

async fn read_lite_http_request(stream: &mut TcpStream) -> anyhow::Result<LiteHttpRequest> {
    let mut buffer = Vec::new();
    let mut chunk = [0u8; 8192];
    let header_end = loop {
        let read = stream
            .read(&mut chunk)
            .await
            .context("读取本地协议代理请求失败")?;
        if read == 0 {
            bail!("本地协议代理连接提前关闭");
        }
        buffer.extend_from_slice(&chunk[..read]);
        if let Some(index) = find_http_header_end(&buffer) {
            break index;
        }
        if buffer.len() > 1024 * 1024 {
            bail!("本地协议代理请求头过大");
        }
    };

    let header_bytes = &buffer[..header_end];
    let header_text = String::from_utf8_lossy(header_bytes);
    let mut lines = header_text.split("\r\n");
    let request_line = lines
        .next()
        .map(str::trim)
        .filter(|line| !line.is_empty())
        .ok_or_else(|| anyhow::anyhow!("本地协议代理请求行为空"))?;
    let mut request_parts = request_line.split_whitespace();
    let method = request_parts
        .next()
        .ok_or_else(|| anyhow::anyhow!("本地协议代理请求缺少 method"))?
        .to_ascii_uppercase();
    let path = request_parts
        .next()
        .ok_or_else(|| anyhow::anyhow!("本地协议代理请求缺少 path"))?
        .to_string();
    let headers = lines
        .filter_map(|line| line.split_once(':'))
        .map(|(name, value)| (name.trim().to_string(), value.trim().to_string()))
        .collect::<Vec<_>>();
    let content_length = headers
        .iter()
        .find(|(name, _)| name.eq_ignore_ascii_case("content-length"))
        .and_then(|(_, value)| value.parse::<usize>().ok())
        .unwrap_or(0);

    let body_start = header_end + 4;
    while buffer.len().saturating_sub(body_start) < content_length {
        let read = stream
            .read(&mut chunk)
            .await
            .context("读取本地协议代理请求体失败")?;
        if read == 0 {
            break;
        }
        buffer.extend_from_slice(&chunk[..read]);
        if buffer.len() > content_length + body_start + 1024 * 1024 {
            bail!("本地协议代理请求体过大");
        }
    }
    let body_end = body_start + content_length.min(buffer.len().saturating_sub(body_start));
    let body = String::from_utf8_lossy(&buffer[body_start..body_end]).to_string();

    Ok(LiteHttpRequest {
        method,
        path,
        headers,
        body,
    })
}

fn find_http_header_end(buffer: &[u8]) -> Option<usize> {
    buffer.windows(4).position(|window| window == b"\r\n\r\n")
}

fn lite_http_response_bytes(status: &str, content_type: &str, body: &[u8]) -> Vec<u8> {
    let headers = format!(
        "HTTP/1.1 {status}\r\nContent-Type: {content_type}\r\nContent-Length: {}\r\nConnection: close\r\nAccess-Control-Allow-Origin: *\r\nAccess-Control-Allow-Headers: authorization,content-type\r\nAccess-Control-Allow-Methods: GET,POST,OPTIONS\r\n\r\n",
        body.len()
    );
    let mut response = headers.into_bytes();
    response.extend_from_slice(body);
    response
}

fn method_not_allowed_response() -> protocol_proxy::ProxyHttpResponse {
    protocol_proxy::ProxyHttpResponse {
        status: "405 Method Not Allowed".to_string(),
        content_type: "application/json; charset=utf-8".to_string(),
        body: serde_json::to_vec(&json!({
            "error": {
                "message": "method not allowed",
                "type": "method_not_allowed"
            }
        }))
        .unwrap_or_default(),
    }
}

fn protocol_proxy_error_response(error: &anyhow::Error) -> protocol_proxy::ProxyHttpResponse {
    protocol_proxy::ProxyHttpResponse {
        status: "500 Internal Server Error".to_string(),
        content_type: "application/json; charset=utf-8".to_string(),
        body: serde_json::to_vec(&json!({
            "error": {
                "message": error.to_string(),
                "type": "proxy_error"
            }
        }))
        .unwrap_or_default(),
    }
}

async fn run_agent(
    config_path: PathBuf,
    codex_home: Option<PathBuf>,
    app_path: Option<PathBuf>,
    debug_port: u16,
    plan_ui: bool,
    interval_ms: u64,
) -> anyhow::Result<()> {
    println!("准备启动 Codex Gateway Lite agent...");
    let replaced_agent_count = terminate_other_agent_processes();
    if replaced_agent_count > 0 {
        let _ = remove_agent_lock_file();
    }
    let Some(_agent_lock) = acquire_agent_instance_lock()? else {
        return Ok(());
    };
    let config = read_lite_config(&config_path)
        .with_context(|| format!("读取 Lite 配置失败：{}", config_path.display()))?;
    validate_context_budget_for_config(&config.provider.context_budget)?;
    if provider_uses_local_proxy(&config) {
        println!(
            "当前配置需要本地协议代理，准备监听 127.0.0.1:{}",
            protocol_proxy::DEFAULT_PROTOCOL_PROXY_PORT
        );
    }
    let _protocol_proxy = if provider_uses_local_proxy(&config) {
        Some(
            start_protocol_proxy(config_path.clone())
                .await
                .context("当前配置需要本地协议代理")?,
        )
    } else {
        println!("Responses 直连模式：不启动本地协议代理 127.0.0.1:57321");
        None
    };
    let interval = Duration::from_millis(interval_ms.max(1000));
    let app_dir = resolve_codex_app(app_path.as_deref()).with_context(|| {
        "识别 Codex App 路径失败；请运行 where-app，或通过 CODEX_GATEWAY_LITE_APP / --app 指定 Codex.exe"
    })?;
    println!("Codex Gateway Lite agent 已启动");
    println!("  config: {}", config_path.display());
    println!("  app: {}", app_dir.display());
    println!(
        "  codex_home: {}",
        resolve_codex_home(codex_home.as_deref()).display()
    );
    println!("  debug_port: {debug_port}");
    println!("  interval_ms: {}", interval.as_millis());
    if codex_home.is_none() {
        if let Some(env_home) = std::env::var_os("CODEX_HOME") {
            println!(
                "  note: 已忽略 CODEX_HOME={}；如需自定义目录请显式传 --codex-home",
                PathBuf::from(env_home).display()
            );
        }
    }

    let mut last_modified = None;
    apply_config_from_path(&config_path, codex_home.as_deref(), &mut last_modified).await;

    let now = Instant::now();
    let mut last_launch_attempt = now;
    let mut last_bridge_injection = now.checked_sub(Duration::from_secs(60)).unwrap_or(now);
    let mut last_plan_injection = now.checked_sub(Duration::from_secs(60)).unwrap_or(now);
    let mut active_plan_seed_attempts: HashMap<String, Instant> = HashMap::new();
    let mut cdp_was_ready = false;
    let mut cdp_has_connected = false;
    let mut auto_launch_paused_after_close = false;
    let mut inject_immediately_after_connect = false;

    if let Err(error) = launch_codex(&app_dir, debug_port, codex_home.as_deref()).await {
        eprintln!("启动 Codex App 失败：{error:#}");
    }

    loop {
        match cdp_ready(debug_port).await {
            Ok(()) => {
                if !cdp_was_ready {
                    if auto_launch_paused_after_close {
                        println!("Codex CDP 已重新连接；继续注入 Lite 补丁");
                    } else {
                        println!("Codex CDP 已连接");
                    }
                    auto_launch_paused_after_close = false;
                    inject_immediately_after_connect = true;
                    // Codex App 重启后 renderer 是全新的 JS 上下文，之前注入的模型白名单
                    // 和任务清单 UI 全部失效；把计时器往回拨，确保下面的判断立刻触发一次
                    // 重新注入，而不是继续沿用重连前的滑动窗口去等到 10s/1s 才补注入。
                    let rewind = Instant::now()
                        .checked_sub(Duration::from_secs(60))
                        .unwrap_or_else(Instant::now);
                    last_bridge_injection = rewind;
                    last_plan_injection = rewind;
                    active_plan_seed_attempts.clear();
                }
                cdp_was_ready = true;
                cdp_has_connected = true;
            }
            Err(error) => {
                if cdp_was_ready {
                    eprintln!("Codex CDP 连接断开：{error:#}");
                    if cdp_has_connected {
                        auto_launch_paused_after_close = true;
                        eprintln!("检测到 Codex App 已关闭；暂停自动拉起，等待用户手动重新打开");
                    }
                }
                cdp_was_ready = false;
                if should_retry_launch_codex(
                    auto_launch_paused_after_close,
                    last_launch_attempt.elapsed() >= Duration::from_secs(15),
                ) {
                    last_launch_attempt = Instant::now();
                    if let Err(launch_error) =
                        launch_codex(&app_dir, debug_port, codex_home.as_deref()).await
                    {
                        eprintln!("重新启动 Codex App 失败：{launch_error:#}");
                    }
                }
            }
        }

        if cdp_was_ready && inject_immediately_after_connect {
            inject_immediately_after_connect = false;
            match wait_and_inject_lite_model_whitelist(debug_port, codex_home.as_deref()).await {
                Ok(()) => last_bridge_injection = Instant::now(),
                Err(error) => {
                    eprintln!("Lite 模型白名单首次注入失败：{error:#}");
                    cdp_was_ready = false;
                }
            }
            if plan_ui && cdp_was_ready {
                match wait_and_inject_plan_ui(debug_port, codex_home.as_deref()).await {
                    Ok(()) => last_plan_injection = Instant::now(),
                    Err(error) => {
                        eprintln!("任务清单 UI 首次注入失败：{error:#}");
                        cdp_was_ready = false;
                    }
                }
            }
        }

        let mut config_changed = false;
        match file_modified(&config_path) {
            Ok(modified) if last_modified.map(|last| last != modified).unwrap_or(true) => {
                last_modified = Some(modified);
                config_changed = true;
                maybe_refresh_config_models(&config_path).await;
                if let Ok(modified) = file_modified(&config_path) {
                    last_modified = Some(modified);
                }
                match read_lite_config(&config_path).and_then(|config| {
                    apply_config(&config, codex_home.as_deref(), config.plan_hints)
                }) {
                    Ok(report) => {
                        print_apply_report(&report);
                        if cdp_was_ready {
                            if let Err(error) = soft_reload_codex(debug_port).await {
                                eprintln!("配置已写入，但软刷新失败：{error:#}");
                                cdp_was_ready = false;
                            } else if let Err(error) = wait_and_inject_lite_model_whitelist(
                                debug_port,
                                codex_home.as_deref(),
                            )
                            .await
                            {
                                eprintln!("Lite 模型白名单重注入失败：{error:#}");
                                cdp_was_ready = false;
                            } else {
                                last_bridge_injection = Instant::now();
                            }
                        }
                    }
                    Err(error) => eprintln!("应用配置失败：{error:#}"),
                }
            }
            Ok(_) => {}
            Err(error) => eprintln!("读取配置修改时间失败：{error:#}"),
        }

        if cdp_was_ready
            && (config_changed || last_bridge_injection.elapsed() >= Duration::from_secs(10))
        {
            last_bridge_injection = Instant::now();
            if let Err(error) =
                inject_lite_model_whitelist_quiet(debug_port, codex_home.as_deref()).await
            {
                eprintln!("Lite 模型白名单重注入失败：{error:#}");
                cdp_was_ready = false;
            }
        }

        if plan_ui
            && cdp_was_ready
            && (config_changed
                || last_plan_injection.elapsed()
                    >= Duration::from_secs(PLAN_UI_REINJECT_INTERVAL_SECS))
        {
            last_plan_injection = Instant::now();
            if config_changed {
                active_plan_seed_attempts.clear();
            }
            let seed_history = should_seed_plan_ui_history_on_reinject(config_changed);
            if let Err(error) =
                inject_plan_ui_inner(debug_port, false, seed_history, codex_home.as_deref()).await
            {
                eprintln!("任务清单 UI 重注入失败：{error:#}");
                cdp_was_ready = false;
            }
        }

        if plan_ui && cdp_was_ready {
            if let Err(error) = seed_active_plan_ui_history_snapshot_if_needed(
                debug_port,
                codex_home.as_deref(),
                &mut active_plan_seed_attempts,
            )
            .await
            {
                eprintln!("任务清单历史快照按需补灌失败：{error:#}");
            }
        }

        tokio::time::sleep(interval).await;
    }
}

fn should_retry_launch_codex(auto_launch_paused_after_close: bool, retry_due: bool) -> bool {
    !auto_launch_paused_after_close && retry_due
}

async fn apply_config_from_path(
    config_path: &Path,
    codex_home: Option<&Path>,
    last_modified: &mut Option<SystemTime>,
) {
    match file_modified(config_path) {
        Ok(modified) => *last_modified = Some(modified),
        Err(error) => eprintln!("读取配置修改时间失败：{error:#}"),
    }
    maybe_refresh_config_models(config_path).await;
    if let Ok(modified) = file_modified(config_path) {
        *last_modified = Some(modified);
    }
    match read_lite_config(config_path)
        .and_then(|config| apply_config(&config, codex_home, config.plan_hints))
    {
        Ok(report) => print_apply_report(&report),
        Err(error) => eprintln!("应用配置失败：{error:#}"),
    }
}

async fn maybe_refresh_config_models(config_path: &Path) {
    match refresh_config_models(config_path).await {
        Ok(count) => println!("供应商模型列表已同步：{count} 个模型"),
        Err(error) => eprintln!("供应商模型列表同步失败，继续使用现有配置：{error:#}"),
    }
}

async fn watch(
    config_path: PathBuf,
    codex_home: Option<PathBuf>,
    debug_port: u16,
    interval_ms: u64,
) -> anyhow::Result<()> {
    println!("开始监听配置：{}", config_path.display());
    let mut last_modified = None;
    loop {
        let modified = file_modified(&config_path)?;
        if last_modified.map(|last| last != modified).unwrap_or(true) {
            last_modified = Some(modified);
            maybe_refresh_config_models(&config_path).await;
            if let Ok(modified) = file_modified(&config_path) {
                last_modified = Some(modified);
            }
            match read_lite_config(&config_path)
                .and_then(|config| apply_config(&config, codex_home.as_deref(), config.plan_hints))
            {
                Ok(report) => {
                    print_apply_report(&report);
                    if let Err(error) = soft_reload_codex(debug_port).await {
                        eprintln!("软刷新失败：{error:#}");
                    }
                    if let Err(error) =
                        wait_and_inject_lite_model_whitelist(debug_port, codex_home.as_deref())
                            .await
                    {
                        eprintln!("Lite 模型白名单注入失败：{error:#}");
                    }
                    if let Err(error) = inject_plan_ui_inner(
                        debug_port,
                        true,
                        PLAN_UI_INITIAL_HISTORY_SEED,
                        codex_home.as_deref(),
                    )
                    .await
                    {
                        eprintln!("任务清单 UI 注入失败：{error:#}");
                    }
                }
                Err(error) => eprintln!("应用配置失败：{error:#}"),
            }
        }
        tokio::time::sleep(Duration::from_millis(interval_ms.max(250))).await;
    }
}

fn should_seed_plan_ui_history_on_reinject(config_changed: bool) -> bool {
    config_changed
}

fn file_modified(path: &Path) -> anyhow::Result<SystemTime> {
    fs::metadata(path)
        .with_context(|| format!("读取文件状态失败：{}", path.display()))?
        .modified()
        .with_context(|| format!("读取修改时间失败：{}", path.display()))
}

fn resolve_codex_app(app_path: Option<&Path>) -> anyhow::Result<PathBuf> {
    codex_lite::resolve_codex_app_dir(app_path).ok_or_else(|| {
        anyhow::anyhow!("未找到 Codex App；请用 --app 指定 Codex.app 或 Codex.exe 所在路径")
    })
}

async fn launch_codex(
    app_dir: &Path,
    debug_port: u16,
    codex_home: Option<&Path>,
) -> anyhow::Result<()> {
    if !app_dir.exists() {
        bail!("Codex App 不存在：{}", app_dir.display());
    }
    #[cfg(not(target_os = "macos"))]
    let _ = codex_home;
    #[cfg(target_os = "macos")]
    {
        let home = resolve_codex_home(codex_home);
        let user_data_dir = macos_codex_user_data_dir(&home)?;
        if cdp_ready(debug_port).await.is_ok() {
            if macos_codex_app_process_matches_profile(app_dir, debug_port, &home, &user_data_dir) {
                return Ok(());
            }
            let terminated = terminate_mismatched_codex_app_processes(
                app_dir,
                debug_port,
                &home,
                &user_data_dir,
            );
            if terminated == 0 {
                bail!(
                    "Codex CDP 端口 {debug_port} 已被占用，但未找到匹配 profile 的 Codex 主进程；请完全退出 Codex App 后重试"
                );
            }
            println!("检测到 {terminated} 个 profile 不匹配的 Codex App 进程，已清理并重新拉起");
        }
        spawn_codex_app_macos(app_dir, debug_port, &home, &user_data_dir)?;
        if wait_for_cdp_ready(debug_port, Duration::from_millis(500), 20).await {
            println!(
                "已启动 Codex App：{}，CDP 端口：{}",
                app_dir.display(),
                debug_port
            );
            println!("  codex_home: {}", home.display());
            println!("  user_data_dir: {}", user_data_dir.display());
            return Ok(());
        }
        let terminated = terminate_stale_codex_app_processes(app_dir);
        if terminated > 0 {
            println!("检测到 {terminated} 个未监听调试端口的 Codex App 残留进程，已清理并重新拉起");
            spawn_codex_app_macos(app_dir, debug_port, &home, &user_data_dir)?;
            wait_for_cdp_ready(debug_port, Duration::from_millis(500), 20).await;
        }
        if cdp_ready(debug_port).await.is_ok() {
            println!(
                "已启动 Codex App：{}，CDP 端口：{}",
                app_dir.display(),
                debug_port
            );
            println!("  codex_home: {}", home.display());
            println!("  user_data_dir: {}", user_data_dir.display());
            return Ok(());
        }
        bail!("Codex App 已启动，但 CDP 端口 {debug_port} 未就绪")
    }
    #[cfg(windows)]
    {
        if cdp_ready(debug_port).await.is_ok() {
            println!("Codex App 已在 CDP 端口 {debug_port} 就绪");
            return Ok(());
        }
        let terminated = terminate_windows_codex_app_processes();
        if terminated > 0 {
            println!("检测到 {terminated} 个未带调试端口的 Codex App 进程，已清理并重新拉起");
        }
        if let Some(app_user_model_id) = codex_lite::packaged_app_user_model_id(app_dir) {
            let args = codex_lite::command_line_arguments(&codex_lite::build_codex_arguments(
                debug_port,
                &[],
            ));
            let process_id = codex_lite::activate_packaged_app(&app_user_model_id, &args).await?;
            println!(
                "已通过 Store/MSIX 激活 Codex App：{}，CDP 端口：{}，pid：{}",
                app_dir.display(),
                debug_port,
                process_id
            );
            if wait_for_cdp_ready(debug_port, Duration::from_millis(500), 40).await {
                return Ok(());
            }
            bail!(
                "Codex App 已通过 Store/MSIX 激活，但 CDP 端口 {debug_port} 未就绪；请确认没有旧 Codex 窗口残留后重试"
            );
        }
        let command = codex_lite::build_codex_command(app_dir, debug_port, &[]);
        let executable = command
            .first()
            .ok_or_else(|| anyhow::anyhow!("Codex 启动命令为空"))?;
        std::process::Command::new(executable)
            .args(&command[1..])
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .with_context(|| format!("启动 Codex 失败：{executable}"))?;
        println!(
            "已启动 Codex App：{}，CDP 端口：{}",
            app_dir.display(),
            debug_port
        );
        if !wait_for_cdp_ready(debug_port, Duration::from_millis(500), 40).await {
            bail!("Codex App 已启动，但 CDP 端口 {debug_port} 未就绪")
        }
        Ok(())
    }
    #[cfg(not(any(target_os = "macos", windows)))]
    {
        let command = codex_lite::build_codex_command(app_dir, debug_port, &[]);
        let executable = command
            .first()
            .ok_or_else(|| anyhow::anyhow!("Codex 启动命令为空"))?;
        std::process::Command::new(executable)
            .args(&command[1..])
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .with_context(|| format!("启动 Codex 失败：{executable}"))?;
        Ok(())
    }
}

#[cfg(target_os = "macos")]
fn spawn_codex_app_macos(
    app_dir: &Path,
    debug_port: u16,
    codex_home: &Path,
    user_data_dir: &Path,
) -> anyhow::Result<()> {
    fs::create_dir_all(user_data_dir)
        .with_context(|| format!("创建 Codex user-data-dir 失败：{}", user_data_dir.display()))?;
    let executable = macos_codex_executable(app_dir);
    if !executable.exists() {
        bail!("Codex App 可执行文件不存在：{}", executable.display());
    }
    let remote_debug_arg = format!("--remote-debugging-port={debug_port}");
    let remote_origin_arg = format!("--remote-allow-origins=http://127.0.0.1:{debug_port}");
    let user_data_arg = format!("--user-data-dir={}", user_data_dir.display());

    if macos_codex_home_requires_env(codex_home) {
        ProcessCommand::new(&executable)
            .env("CODEX_HOME", codex_home)
            .arg(&remote_debug_arg)
            .arg(&remote_origin_arg)
            .arg(&user_data_arg)
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .with_context(|| format!("启动 Codex App 失败：{}", executable.display()))?;
        return Ok(());
    }

    ProcessCommand::new("open")
        .arg("-n")
        .arg("-a")
        .arg(app_dir)
        .arg("--args")
        .arg(&remote_debug_arg)
        .arg(&remote_origin_arg)
        .arg(&user_data_arg)
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .with_context(|| {
            format!(
                "通过 LaunchServices 启动 Codex App 失败：{}",
                app_dir.display()
            )
        })?;
    Ok(())
}

#[cfg(target_os = "macos")]
fn macos_codex_executable(app_dir: &Path) -> PathBuf {
    app_dir.join("Contents").join("MacOS").join("Codex")
}

#[cfg(target_os = "macos")]
fn macos_codex_user_data_dir(codex_home: &Path) -> anyhow::Result<PathBuf> {
    let home = user_home_dir().ok_or_else(|| anyhow::anyhow!("无法识别用户 HOME"))?;
    if path_eq_loose(codex_home, &default_user_codex_home_dir()) {
        return Ok(home
            .join("Library")
            .join("Application Support")
            .join("Codex"));
    }
    let component = codex_home
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or("custom");
    Ok(home
        .join("Library")
        .join("Application Support")
        .join(codex_profile_name_from_home_component(component)))
}

#[cfg(target_os = "macos")]
fn codex_profile_name_from_home_component(component: &str) -> String {
    let mut suffix = component.trim_start_matches('.');
    if let Some(rest) = suffix.strip_prefix("codex-") {
        suffix = rest;
    } else if suffix == "codex" {
        suffix = "";
    }
    if suffix.is_empty() {
        return "Codex".to_string();
    }

    let mut name = String::from("Codex");
    for part in suffix
        .split(|ch: char| !ch.is_ascii_alphanumeric())
        .filter(|part| !part.is_empty())
    {
        name.push('-');
        let mut chars = part.chars();
        if let Some(first) = chars.next() {
            name.push(first.to_ascii_uppercase());
            for ch in chars {
                name.push(ch.to_ascii_lowercase());
            }
        }
    }
    if name == "Codex" {
        "Codex-Custom".to_string()
    } else {
        name
    }
}

#[cfg(target_os = "macos")]
fn macos_codex_home_requires_env(codex_home: &Path) -> bool {
    !path_eq_loose(codex_home, &default_user_codex_home_dir())
}

#[cfg(target_os = "macos")]
fn path_eq_loose(left: &Path, right: &Path) -> bool {
    left == right || absolute_path(left) == absolute_path(right)
}

async fn wait_for_cdp_ready(debug_port: u16, interval: Duration, attempts: u32) -> bool {
    for _ in 0..attempts {
        if cdp_ready(debug_port).await.is_ok() {
            return true;
        }
        tokio::time::sleep(interval).await;
    }
    false
}

#[cfg(windows)]
fn terminate_windows_codex_app_processes() -> usize {
    let pids = windows_codex_app_process_pids();
    for pid in &pids {
        let _ = ProcessCommand::new("taskkill")
            .args(["/PID", &pid.to_string(), "/F"])
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .status();
    }
    if !pids.is_empty() {
        let _ = wait_for_processes_to_exit(&pids, Duration::from_secs(5));
    }
    pids.len()
}

#[cfg(windows)]
fn windows_codex_app_process_pids() -> Vec<u32> {
    let script = r#"
$ErrorActionPreference = 'SilentlyContinue'
Get-CimInstance Win32_Process |
  Where-Object {
    ($_.Name -ieq 'Codex.exe') -or
    ($_.ExecutablePath -and $_.ExecutablePath -like '*\WindowsApps\OpenAI.Codex_*') -or
    ($_.ExecutablePath -and $_.ExecutablePath -like '*\WindowsApps\OpenAI.CodexBeta_*')
  } |
  Select-Object ProcessId,CommandLine,ExecutablePath,Name |
  ConvertTo-Json -Compress
"#;
    let output = ProcessCommand::new("powershell")
        .args([
            "-NoProfile",
            "-ExecutionPolicy",
            "Bypass",
            "-Command",
            script,
        ])
        .output();
    let Ok(output) = output else {
        return Vec::new();
    };
    if !output.status.success() {
        return Vec::new();
    }
    codex_app_pids_from_windows_process_json(&String::from_utf8_lossy(&output.stdout))
}

#[cfg(any(windows, test))]
fn codex_app_pids_from_windows_process_json(json_text: &str) -> Vec<u32> {
    let trimmed = json_text.trim();
    if trimmed.is_empty() {
        return Vec::new();
    }
    let Ok(value) = serde_json::from_str::<Value>(trimmed) else {
        return Vec::new();
    };
    let items = match &value {
        Value::Array(values) => values.iter().collect::<Vec<_>>(),
        Value::Object(_) => vec![&value],
        _ => Vec::new(),
    };
    let mut pids = Vec::new();
    for item in items {
        let Some(pid) = json_field_u32(item, &["ProcessId", "processId", "PID", "pid"]) else {
            continue;
        };
        let name = json_field_string(item, &["Name", "name"]).unwrap_or_default();
        let executable =
            json_field_string(item, &["ExecutablePath", "executablePath"]).unwrap_or_default();
        let command =
            json_field_string(item, &["CommandLine", "commandLine", "Command", "command"])
                .unwrap_or_default();
        if windows_process_looks_like_codex_app(name, executable, command) {
            pids.push(pid);
        }
    }
    pids.sort_unstable();
    pids.dedup();
    pids
}

#[cfg(any(windows, test))]
fn windows_process_looks_like_codex_app(name: &str, executable: &str, command: &str) -> bool {
    name.eq_ignore_ascii_case("Codex.exe")
        || executable
            .replace('\\', "/")
            .to_ascii_lowercase()
            .contains("/windowsapps/openai.codex_")
        || executable
            .replace('\\', "/")
            .to_ascii_lowercase()
            .contains("/windowsapps/openai.codexbeta_")
        || command
            .replace('\\', "/")
            .to_ascii_lowercase()
            .contains("/windowsapps/openai.codex_")
        || command
            .replace('\\', "/")
            .to_ascii_lowercase()
            .contains("/windowsapps/openai.codexbeta_")
}

#[cfg(target_os = "macos")]
#[derive(Debug, Clone, PartialEq, Eq)]
struct CodexAppProcess {
    pid: u32,
    command: String,
}

#[cfg(target_os = "macos")]
fn macos_codex_app_process_matches_profile(
    app_dir: &Path,
    debug_port: u16,
    codex_home: &Path,
    user_data_dir: &Path,
) -> bool {
    codex_app_main_processes(app_dir, true)
        .into_iter()
        .any(|process| {
            codex_app_command_matches_profile(
                &process.command,
                debug_port,
                codex_home,
                user_data_dir,
            )
        })
}

#[cfg(target_os = "macos")]
fn terminate_mismatched_codex_app_processes(
    app_dir: &Path,
    debug_port: u16,
    codex_home: &Path,
    user_data_dir: &Path,
) -> usize {
    let processes = codex_app_main_processes(app_dir, true);
    let pids = processes
        .into_iter()
        .filter(|process| {
            !codex_app_command_matches_profile(
                &process.command,
                debug_port,
                codex_home,
                user_data_dir,
            )
        })
        .map(|process| process.pid)
        .collect::<Vec<_>>();
    terminate_pids(&pids);
    pids.len()
}

#[cfg(target_os = "macos")]
fn terminate_pids(pids: &[u32]) {
    for pid in pids {
        let _ = ProcessCommand::new("kill")
            .arg("-TERM")
            .arg(pid.to_string())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .status();
    }
    if !pids.is_empty() {
        if !wait_for_processes_to_exit(pids, Duration::from_secs(3)) {
            for pid in pids.iter().filter(|pid| process_is_running(**pid)) {
                let _ = ProcessCommand::new("kill")
                    .arg("-KILL")
                    .arg(pid.to_string())
                    .stdout(Stdio::null())
                    .stderr(Stdio::null())
                    .status();
            }
            let _ = wait_for_processes_to_exit(pids, Duration::from_secs(2));
        }
    }
}

#[cfg(target_os = "macos")]
fn codex_app_command_matches_profile(
    command: &str,
    debug_port: u16,
    codex_home: &Path,
    user_data_dir: &Path,
) -> bool {
    command.contains("/Contents/MacOS/Codex")
        && command.contains(&format!("--remote-debugging-port={debug_port}"))
        && command.contains(&format!("--user-data-dir={}", user_data_dir.display()))
        && (!macos_codex_home_requires_env(codex_home)
            || command.contains(&format!("CODEX_HOME={}", codex_home.display())))
}

#[cfg(target_os = "macos")]
fn codex_app_main_processes(app_dir: &Path, include_env: bool) -> Vec<CodexAppProcess> {
    let marker = format!("{}/Contents/MacOS/", app_dir.display());
    let mut command = ProcessCommand::new("ps");
    if include_env {
        command.args(["eww", "-axo", "pid=,command="]);
    } else {
        command.args(["-axo", "pid=,command="]);
    }
    let Ok(output) = command.output() else {
        return Vec::new();
    };
    let text = String::from_utf8_lossy(&output.stdout);
    codex_app_main_processes_from_ps_output(&text, &marker)
}

#[cfg(target_os = "macos")]
fn codex_app_main_processes_from_ps_output(ps_output: &str, marker: &str) -> Vec<CodexAppProcess> {
    let mut processes = Vec::new();
    for line in ps_output.lines() {
        let trimmed = line.trim_start();
        let Some((pid_text, command)) = trimmed.split_once(char::is_whitespace) else {
            continue;
        };
        let Ok(pid) = pid_text.trim().parse::<u32>() else {
            continue;
        };
        if !command.trim_start().starts_with(marker) {
            continue;
        }
        processes.push(CodexAppProcess {
            pid,
            command: command.trim_start().to_string(),
        });
    }
    processes
}

// 只匹配主可执行文件本身（Contents/MacOS/<name>），不误伤 renderer/GPU/utility 等
// 由主进程管理的子进程——它们会随主进程一起退出，不需要单独处理。纯函数，方便单测。
#[cfg(any(target_os = "macos", test))]
fn stale_codex_app_pids(ps_output: &str, marker: &str) -> Vec<u32> {
    let mut pids = Vec::new();
    for line in ps_output.lines() {
        let trimmed = line.trim_start();
        let Some((pid_text, command)) = trimmed.split_once(char::is_whitespace) else {
            continue;
        };
        let Ok(pid) = pid_text.trim().parse::<u32>() else {
            continue;
        };
        if !command.trim_start().starts_with(marker) {
            continue;
        }
        pids.push(pid);
    }
    pids
}

#[cfg(target_os = "macos")]
fn terminate_stale_codex_app_processes(app_dir: &Path) -> usize {
    let marker = format!("{}/Contents/MacOS/", app_dir.display());
    let Ok(output) = ProcessCommand::new("ps")
        .args(["-axo", "pid=,command="])
        .output()
    else {
        return 0;
    };
    let text = String::from_utf8_lossy(&output.stdout);
    let pids = stale_codex_app_pids(&text, &marker);
    for pid in &pids {
        let _ = ProcessCommand::new("kill")
            .arg("-TERM")
            .arg(pid.to_string())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .status();
    }
    if !pids.is_empty() {
        std::thread::sleep(Duration::from_millis(800));
    }
    pids.len()
}

fn install_persistent_agent(
    config_path: &Path,
    codex_home: Option<&Path>,
    app_path: Option<&Path>,
    debug_port: u16,
    plan_ui: bool,
    interval_ms: u64,
) -> anyhow::Result<()> {
    #[cfg(target_os = "macos")]
    {
        install_macos_launch_agent(
            config_path,
            codex_home,
            app_path,
            debug_port,
            plan_ui,
            interval_ms,
        )
    }
    #[cfg(windows)]
    {
        install_windows_scheduled_task(
            config_path,
            codex_home,
            app_path,
            debug_port,
            plan_ui,
            interval_ms,
        )
    }
    #[cfg(all(not(target_os = "macos"), not(windows)))]
    {
        let _ = (
            config_path,
            codex_home,
            app_path,
            debug_port,
            plan_ui,
            interval_ms,
        );
        bail!("install-agent 当前仅支持 macOS 和 Windows")
    }
}

fn uninstall_persistent_agent() -> anyhow::Result<()> {
    #[cfg(target_os = "macos")]
    {
        uninstall_macos_launch_agent()
    }
    #[cfg(windows)]
    {
        uninstall_windows_scheduled_task()
    }
    #[cfg(all(not(target_os = "macos"), not(windows)))]
    {
        bail!("uninstall-agent 当前仅支持 macOS 和 Windows")
    }
}

#[cfg(target_os = "macos")]
fn install_macos_launch_agent(
    config_path: &Path,
    codex_home: Option<&Path>,
    app_path: Option<&Path>,
    debug_port: u16,
    plan_ui: bool,
    interval_ms: u64,
) -> anyhow::Result<()> {
    let home = user_home_dir().ok_or_else(|| anyhow::anyhow!("无法识别用户 HOME"))?;
    let support_dir = home.join(".codex-gateway-lite");
    fs::create_dir_all(&support_dir)
        .with_context(|| format!("创建 agent 支持目录失败：{}", support_dir.display()))?;
    let bin_dir = support_dir.join("bin");
    fs::create_dir_all(&bin_dir)
        .with_context(|| format!("创建 agent bin 目录失败：{}", bin_dir.display()))?;
    let plist_dir = home.join("Library").join("LaunchAgents");
    fs::create_dir_all(&plist_dir)
        .with_context(|| format!("创建 LaunchAgents 目录失败：{}", plist_dir.display()))?;
    let plist_path = plist_dir.join(format!("{}.plist", launch_agent_label()));
    let executable = install_agent_binary(&bin_dir)?;
    let mut program_args = vec![
        executable.to_string_lossy().to_string(),
        "agent".to_string(),
        "--config".to_string(),
        absolute_path(config_path).to_string_lossy().to_string(),
        "--debug-port".to_string(),
        debug_port.to_string(),
        "--interval-ms".to_string(),
        interval_ms.max(1000).to_string(),
    ];
    if let Some(home) = codex_home {
        program_args.push("--codex-home".to_string());
        program_args.push(absolute_path(home).to_string_lossy().to_string());
    }
    if let Some(app) = app_path {
        program_args.push("--app".to_string());
        program_args.push(absolute_path(app).to_string_lossy().to_string());
    }
    if !plan_ui {
        program_args.push("--no-plan-ui".to_string());
    }

    let working_dir = support_dir.clone();
    let stdout_path = support_dir.join("agent.out.log");
    let stderr_path = support_dir.join("agent.err.log");
    fs::write(
        &plist_path,
        launch_agent_plist(&program_args, &working_dir, &stdout_path, &stderr_path),
    )
    .with_context(|| format!("写入 LaunchAgent 失败：{}", plist_path.display()))?;

    let domain = launchctl_gui_domain()?;
    let _ = run_launchctl(&["bootout", &domain, &plist_path.to_string_lossy()]);
    terminate_other_agent_processes();
    let _ = remove_agent_lock_file();
    run_launchctl(&["bootstrap", &domain, &plist_path.to_string_lossy()])
        .context("加载 LaunchAgent 失败")?;
    run_launchctl(&[
        "kickstart",
        "-k",
        &format!("{domain}/{}", launch_agent_label()),
    ])
    .context("启动 LaunchAgent 失败")?;

    println!("Codex Gateway Lite agent 已安装并启动");
    println!("  plist: {}", plist_path.display());
    println!("  bin: {}", executable.display());
    println!("  log: {}", stdout_path.display());
    println!("  err: {}", stderr_path.display());
    Ok(())
}

fn stop_agent_service() -> anyhow::Result<()> {
    #[cfg(target_os = "macos")]
    let launch_agent_stopped = stop_macos_launch_agent_if_loaded()?;
    #[cfg(not(target_os = "macos"))]
    let launch_agent_stopped = false;
    #[cfg(windows)]
    let scheduled_task_stopped = stop_windows_scheduled_task_if_running()?;
    #[cfg(target_os = "macos")]
    let launch_agent_plist = macos_launch_agent_plist_path()?;

    let terminated = terminate_other_agent_processes();
    let lock_removed = remove_agent_lock_file()?;

    if launch_agent_stopped {
        println!("已停止 Codex Gateway Lite LaunchAgent");
    }
    #[cfg(windows)]
    if scheduled_task_stopped {
        println!("已停止 Codex Gateway Lite Scheduled Task");
    }
    if terminated == 0 {
        println!("未发现正在运行的 Codex Gateway Lite agent 进程");
    }
    if lock_removed {
        println!("已清理 Codex Gateway Lite agent lock");
    }
    #[cfg(target_os = "macos")]
    if launch_agent_plist.exists() {
        println!(
            "LaunchAgent 登录项文件仍存在：{}；如需禁止下次登录自动拉起，请运行 uninstall-agent",
            launch_agent_plist.display()
        );
    }
    #[cfg(windows)]
    if windows_scheduled_task_exists() {
        println!(
            "Scheduled Task 登录项仍存在：{}；如需禁止下次登录自动拉起，请运行 uninstall-agent",
            windows_scheduled_task_name()
        );
    }
    println!("Codex Gateway Lite agent 已停止");
    Ok(())
}

#[cfg(target_os = "macos")]
fn macos_launch_agent_plist_path() -> anyhow::Result<PathBuf> {
    let home = user_home_dir().ok_or_else(|| anyhow::anyhow!("无法识别用户 HOME"))?;
    Ok(home
        .join("Library")
        .join("LaunchAgents")
        .join(format!("{}.plist", launch_agent_label())))
}

#[cfg(target_os = "macos")]
fn stop_macos_launch_agent_if_loaded() -> anyhow::Result<bool> {
    let plist_path = macos_launch_agent_plist_path()?;
    if !plist_path.exists() {
        return Ok(false);
    }

    let domain = launchctl_gui_domain()?;
    let output = ProcessCommand::new("launchctl")
        .arg("bootout")
        .arg(&domain)
        .arg(plist_path.to_string_lossy().to_string())
        .output()
        .with_context(|| format!("launchctl bootout 执行失败：{}", plist_path.display()))?;
    Ok(output.status.success())
}

fn install_agent_binary(bin_dir: &Path) -> anyhow::Result<PathBuf> {
    let source = std::env::current_exe().context("获取当前 codex-gateway-lite 可执行文件失败")?;
    let target = bin_dir.join(if cfg!(windows) {
        "codex-gateway-lite.exe"
    } else {
        "codex-gateway-lite"
    });
    if source != target {
        fs::copy(&source, &target).with_context(|| {
            format!(
                "复制 agent 二进制失败：{} -> {}",
                source.display(),
                target.display()
            )
        })?;
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            fs::set_permissions(&target, fs::Permissions::from_mode(0o755))
                .with_context(|| format!("设置 agent 二进制权限失败：{}", target.display()))?;
        }
    }
    Ok(target)
}

#[cfg(windows)]
fn install_windows_scheduled_task(
    config_path: &Path,
    codex_home: Option<&Path>,
    app_path: Option<&Path>,
    debug_port: u16,
    plan_ui: bool,
    interval_ms: u64,
) -> anyhow::Result<()> {
    let home = user_home_dir().ok_or_else(|| anyhow::anyhow!("无法识别用户 USERPROFILE/HOME"))?;
    let support_dir = home.join(".codex-gateway-lite");
    fs::create_dir_all(&support_dir)
        .with_context(|| format!("创建 agent 支持目录失败：{}", support_dir.display()))?;
    let bin_dir = support_dir.join("bin");
    fs::create_dir_all(&bin_dir)
        .with_context(|| format!("创建 agent bin 目录失败：{}", bin_dir.display()))?;
    let executable = install_agent_binary(&bin_dir)?;

    let mut program_args = vec![
        executable.to_string_lossy().to_string(),
        "agent".to_string(),
        "--config".to_string(),
        absolute_path(config_path).to_string_lossy().to_string(),
        "--debug-port".to_string(),
        debug_port.to_string(),
        "--interval-ms".to_string(),
        interval_ms.max(1000).to_string(),
    ];
    if let Some(home) = codex_home {
        program_args.push("--codex-home".to_string());
        program_args.push(absolute_path(home).to_string_lossy().to_string());
    }
    if let Some(app) = app_path {
        program_args.push("--app".to_string());
        program_args.push(absolute_path(app).to_string_lossy().to_string());
    }
    if !plan_ui {
        program_args.push("--no-plan-ui".to_string());
    }

    let task_command = windows_command_line(&program_args);
    let task_name = windows_scheduled_task_name();
    let _ = run_schtasks(&["/End", "/TN", task_name]);
    let _ = run_schtasks(&["/Delete", "/TN", task_name, "/F"]);
    terminate_other_agent_processes();
    let _ = remove_agent_lock_file();

    run_schtasks(&[
        "/Create",
        "/SC",
        "ONLOGON",
        "/TN",
        task_name,
        "/TR",
        &task_command,
        "/F",
    ])
    .context("创建 Windows Scheduled Task 失败")?;
    run_schtasks(&["/Run", "/TN", task_name]).context("启动 Windows Scheduled Task 失败")?;

    println!("Codex Gateway Lite agent 已安装并启动");
    println!("  scheduled_task: {task_name}");
    println!("  bin: {}", executable.display());
    Ok(())
}

#[cfg(windows)]
fn stop_windows_scheduled_task_if_running() -> anyhow::Result<bool> {
    if !windows_scheduled_task_exists() {
        return Ok(false);
    }
    let output = ProcessCommand::new("schtasks")
        .args(["/End", "/TN", windows_scheduled_task_name()])
        .output()
        .context("停止 Windows Scheduled Task 失败")?;
    Ok(output.status.success())
}

#[cfg(windows)]
fn uninstall_windows_scheduled_task() -> anyhow::Result<()> {
    let task_name = windows_scheduled_task_name();
    let existed = windows_scheduled_task_exists();
    let _ = run_schtasks(&["/End", "/TN", task_name]);
    if existed {
        run_schtasks(&["/Delete", "/TN", task_name, "/F"])
            .context("删除 Windows Scheduled Task 失败")?;
    } else {
        let _ = run_schtasks(&["/Delete", "/TN", task_name, "/F"]);
    }
    terminate_other_agent_processes();
    let _ = remove_agent_lock_file();
    println!("Codex Gateway Lite agent 已卸载");
    println!("  scheduled_task: {task_name}");
    Ok(())
}

#[cfg(windows)]
fn windows_scheduled_task_exists() -> bool {
    ProcessCommand::new("schtasks")
        .args(["/Query", "/TN", windows_scheduled_task_name()])
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()
        .map(|status| status.success())
        .unwrap_or(false)
}

#[cfg(windows)]
fn run_schtasks(args: &[&str]) -> anyhow::Result<()> {
    let status = ProcessCommand::new("schtasks")
        .args(args)
        .status()
        .with_context(|| format!("schtasks {} 执行失败", args.join(" ")))?;
    if !status.success() {
        bail!("schtasks {} 退出失败：{status}", args.join(" "));
    }
    Ok(())
}

#[cfg(windows)]
fn windows_scheduled_task_name() -> &'static str {
    "CodexGatewayLiteAgent"
}

#[cfg(any(windows, test))]
fn windows_command_line(args: &[String]) -> String {
    args.iter()
        .map(|arg| windows_command_line_arg(arg))
        .collect::<Vec<_>>()
        .join(" ")
}

#[cfg(any(windows, test))]
fn windows_command_line_arg(arg: &str) -> String {
    if !arg.is_empty()
        && !arg
            .chars()
            .any(|ch| ch.is_whitespace() || matches!(ch, '"' | '\\'))
    {
        return arg.to_string();
    }

    let mut quoted = String::from("\"");
    let mut backslashes = 0usize;
    for ch in arg.chars() {
        match ch {
            '\\' => backslashes += 1,
            '"' => {
                quoted.push_str(&"\\".repeat(backslashes * 2 + 1));
                quoted.push('"');
                backslashes = 0;
            }
            _ => {
                quoted.push_str(&"\\".repeat(backslashes));
                backslashes = 0;
                quoted.push(ch);
            }
        }
    }
    quoted.push_str(&"\\".repeat(backslashes * 2));
    quoted.push('"');
    quoted
}

#[cfg(target_os = "macos")]
fn uninstall_macos_launch_agent() -> anyhow::Result<()> {
    let plist_path = macos_launch_agent_plist_path()?;
    let domain = launchctl_gui_domain()?;
    let _ = run_launchctl(&["bootout", &domain, &plist_path.to_string_lossy()]);
    match fs::remove_file(&plist_path) {
        Ok(()) => {}
        Err(error) if error.kind() == io::ErrorKind::NotFound => {}
        Err(error) => {
            return Err(error)
                .with_context(|| format!("删除 LaunchAgent 失败：{}", plist_path.display()));
        }
    }
    println!("Codex Gateway Lite agent 已卸载");
    println!("  plist: {}", plist_path.display());
    Ok(())
}

#[cfg(target_os = "macos")]
fn launch_agent_label() -> &'static str {
    "com.codex.gateway-lite.agent"
}

#[cfg(target_os = "macos")]
fn launch_agent_plist(
    program_args: &[String],
    working_dir: &Path,
    stdout_path: &Path,
    stderr_path: &Path,
) -> String {
    let args = program_args
        .iter()
        .map(|arg| format!("    <string>{}</string>", xml_escape(arg)))
        .collect::<Vec<_>>()
        .join("\n");
    format!(
        r#"<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
  <key>Label</key>
  <string>{}</string>
  <key>ProgramArguments</key>
  <array>
{}
  </array>
  <key>WorkingDirectory</key>
  <string>{}</string>
  <key>RunAtLoad</key>
  <true/>
  <key>KeepAlive</key>
  <true/>
  <key>StandardOutPath</key>
  <string>{}</string>
  <key>StandardErrorPath</key>
  <string>{}</string>
</dict>
</plist>
"#,
        launch_agent_label(),
        args,
        xml_escape(&working_dir.to_string_lossy()),
        xml_escape(&stdout_path.to_string_lossy()),
        xml_escape(&stderr_path.to_string_lossy()),
    )
}

#[cfg(target_os = "macos")]
fn launchctl_gui_domain() -> anyhow::Result<String> {
    let output = std::process::Command::new("id")
        .arg("-u")
        .output()
        .context("读取当前用户 uid 失败")?;
    if !output.status.success() {
        bail!("id -u 失败：{}", output.status);
    }
    let uid = String::from_utf8_lossy(&output.stdout).trim().to_string();
    if uid.is_empty() {
        bail!("id -u 返回为空");
    }
    Ok(format!("gui/{uid}"))
}

#[cfg(target_os = "macos")]
fn run_launchctl(args: &[&str]) -> anyhow::Result<()> {
    let status = std::process::Command::new("launchctl")
        .args(args)
        .status()
        .with_context(|| format!("launchctl {} 执行失败", args.join(" ")))?;
    if !status.success() {
        bail!("launchctl {} 退出失败：{status}", args.join(" "));
    }
    Ok(())
}

fn terminate_other_agent_processes() -> usize {
    #[cfg(unix)]
    {
        let current_pid = std::process::id();
        let output = ProcessCommand::new("ps")
            .args(["-axo", "pid=,command="])
            .output();
        let Ok(output) = output else {
            return 0;
        };
        let text = String::from_utf8_lossy(&output.stdout);
        let pids = codex_gateway_agent_pids(&text, current_pid);
        for pid in &pids {
            let _ = ProcessCommand::new("kill")
                .arg("-TERM")
                .arg(pid.to_string())
                .stdout(Stdio::null())
                .stderr(Stdio::null())
                .status();
        }
        if !pids.is_empty() {
            if !wait_for_processes_to_exit(&pids, Duration::from_secs(3)) {
                for pid in pids.iter().filter(|pid| process_is_running(**pid)) {
                    let _ = ProcessCommand::new("kill")
                        .arg("-KILL")
                        .arg(pid.to_string())
                        .stdout(Stdio::null())
                        .stderr(Stdio::null())
                        .status();
                }
                let _ = wait_for_processes_to_exit(&pids, Duration::from_secs(2));
            }
            println!("已停止旧的 Codex Gateway Lite agent 进程：{}", pids.len());
        }
        pids.len()
    }
    #[cfg(windows)]
    {
        let current_pid = std::process::id();
        let mut pids = windows_codex_gateway_agent_pids(current_pid);
        pids.extend(windows_codex_gateway_listener_pids(
            protocol_proxy::DEFAULT_PROTOCOL_PROXY_PORT,
            current_pid,
        ));
        pids.sort_unstable();
        pids.dedup();
        for pid in &pids {
            let _ = ProcessCommand::new("taskkill")
                .args(["/PID", &pid.to_string()])
                .stdout(Stdio::null())
                .stderr(Stdio::null())
                .status();
        }
        if !pids.is_empty() {
            if !wait_for_processes_to_exit(&pids, Duration::from_secs(3)) {
                for pid in pids.iter().filter(|pid| process_is_running(**pid)) {
                    let _ = ProcessCommand::new("taskkill")
                        .args(["/PID", &pid.to_string(), "/F"])
                        .stdout(Stdio::null())
                        .stderr(Stdio::null())
                        .status();
                }
                let _ = wait_for_processes_to_exit(&pids, Duration::from_secs(2));
            }
            println!("已停止旧的 Codex Gateway Lite agent 进程：{}", pids.len());
        }
        pids.len()
    }
    #[cfg(all(not(unix), not(windows)))]
    {
        0
    }
}

fn wait_for_processes_to_exit(pids: &[u32], timeout: Duration) -> bool {
    let started = Instant::now();
    loop {
        if pids.iter().all(|pid| !process_is_running(*pid)) {
            return true;
        }
        if started.elapsed() >= timeout {
            return false;
        }
        std::thread::sleep(Duration::from_millis(100));
    }
}

#[cfg(any(unix, test))]
fn codex_gateway_agent_pids(ps_output: &str, current_pid: u32) -> Vec<u32> {
    let mut pids = Vec::new();
    for line in ps_output.lines() {
        let trimmed = line.trim_start();
        let Some((pid_text, command)) = trimmed.split_once(char::is_whitespace) else {
            continue;
        };
        let Ok(pid) = pid_text.trim().parse::<u32>() else {
            continue;
        };
        if pid == current_pid {
            continue;
        }
        if !command_line_is_codex_gateway_agent(command) {
            continue;
        }
        pids.push(pid);
    }
    pids
}

fn command_line_is_codex_gateway_agent(command: &str) -> bool {
    let tokens = command_line_tokens(command);
    let Some(executable) = tokens.first() else {
        return false;
    };
    if !is_codex_gateway_lite_executable_token(executable) {
        return false;
    }
    tokens.iter().skip(1).any(|token| token == "agent")
}

fn command_line_tokens(command: &str) -> Vec<String> {
    let mut tokens = Vec::new();
    let mut current = String::new();
    let mut in_single = false;
    let mut in_double = false;
    for ch in command.chars() {
        match ch {
            '\'' if !in_double => in_single = !in_single,
            '"' if !in_single => in_double = !in_double,
            ch if ch.is_whitespace() && !in_single && !in_double => {
                if !current.is_empty() {
                    tokens.push(std::mem::take(&mut current));
                }
            }
            _ => current.push(ch),
        }
    }
    if !current.is_empty() {
        tokens.push(current);
    }
    tokens
}

fn is_codex_gateway_lite_executable_token(token: &str) -> bool {
    let normalized = token.trim_matches(['"', '\'']).replace('\\', "/");
    let file_name = normalized
        .rsplit('/')
        .next()
        .unwrap_or(normalized.as_str())
        .to_ascii_lowercase();
    file_name == "codex-gateway-lite" || file_name == "codex-gateway-lite.exe"
}

#[cfg(windows)]
fn windows_codex_gateway_agent_pids(current_pid: u32) -> Vec<u32> {
    let script = r#"
$ErrorActionPreference = 'SilentlyContinue'
Get-CimInstance Win32_Process |
  Where-Object { $_.CommandLine -and $_.CommandLine -like '*codex-gateway-lite*' } |
  Select-Object ProcessId,CommandLine |
  ConvertTo-Json -Compress
"#;
    let output = ProcessCommand::new("powershell")
        .args([
            "-NoProfile",
            "-ExecutionPolicy",
            "Bypass",
            "-Command",
            script,
        ])
        .output();
    let Ok(output) = output else {
        return Vec::new();
    };
    if !output.status.success() {
        return Vec::new();
    }
    let text = String::from_utf8_lossy(&output.stdout);
    codex_gateway_agent_pids_from_windows_process_json(&text, current_pid)
}

#[cfg(windows)]
fn windows_codex_gateway_listener_pids(port: u16, current_pid: u32) -> Vec<u32> {
    let script = format!(
        r#"
$ErrorActionPreference = 'SilentlyContinue'
$pids = @(Get-NetTCPConnection -LocalPort {port} -State Listen | Select-Object -ExpandProperty OwningProcess -Unique)
if ($pids.Count -gt 0) {{
  Get-CimInstance Win32_Process |
    Where-Object {{ $pids -contains [int]$_.ProcessId -and $_.CommandLine -and $_.CommandLine -like '*codex-gateway-lite*' }} |
    Select-Object ProcessId,CommandLine |
    ConvertTo-Json -Compress
}}
"#
    );
    let output = ProcessCommand::new("powershell")
        .args([
            "-NoProfile",
            "-ExecutionPolicy",
            "Bypass",
            "-Command",
            &script,
        ])
        .output();
    let Ok(output) = output else {
        return Vec::new();
    };
    if !output.status.success() {
        return Vec::new();
    }
    let text = String::from_utf8_lossy(&output.stdout);
    codex_gateway_agent_pids_from_windows_process_json(&text, current_pid)
}

#[cfg(any(windows, test))]
fn codex_gateway_agent_pids_from_windows_process_json(
    json_text: &str,
    current_pid: u32,
) -> Vec<u32> {
    let trimmed = json_text.trim();
    if trimmed.is_empty() {
        return Vec::new();
    }
    let Ok(value) = serde_json::from_str::<Value>(trimmed) else {
        return Vec::new();
    };
    let mut pids = Vec::new();
    let items = match &value {
        Value::Array(values) => values.iter().collect::<Vec<_>>(),
        Value::Object(_) => vec![&value],
        _ => Vec::new(),
    };
    for item in items {
        let Some(pid) = json_field_u32(item, &["ProcessId", "processId", "PID", "pid"]) else {
            continue;
        };
        if pid == current_pid {
            continue;
        }
        let command =
            json_field_string(item, &["CommandLine", "commandLine", "Command", "command"])
                .unwrap_or_default();
        if command_line_is_codex_gateway_agent(command) {
            pids.push(pid);
        }
    }
    pids
}

#[cfg(any(windows, test))]
fn json_field_u32(value: &Value, keys: &[&str]) -> Option<u32> {
    for key in keys {
        if let Some(field) = value.get(*key) {
            if let Some(number) = field.as_u64().and_then(|number| u32::try_from(number).ok()) {
                return Some(number);
            }
            if let Some(text) = field
                .as_str()
                .and_then(|text| text.trim().parse::<u32>().ok())
            {
                return Some(text);
            }
        }
    }
    None
}

#[cfg(any(windows, test))]
fn json_field_string<'a>(value: &'a Value, keys: &[&str]) -> Option<&'a str> {
    for key in keys {
        if let Some(text) = value.get(*key).and_then(Value::as_str) {
            return Some(text);
        }
    }
    None
}

fn absolute_path(path: &Path) -> PathBuf {
    if path.is_absolute() {
        path.to_path_buf()
    } else {
        std::env::current_dir()
            .unwrap_or_else(|_| PathBuf::from("."))
            .join(path)
    }
}

#[cfg(target_os = "macos")]
fn xml_escape(value: &str) -> String {
    value
        .replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
        .replace('"', "&quot;")
        .replace('\'', "&apos;")
}

#[cfg(test)]
const PLAN_TOOLTIP_SAMPLE_PREP_SCRIPT: &str = r#"
(() => {
  const PROBE_STYLE_ID = "codex-gateway-lite-plan-tooltip-probe-style";
  const PROBE_KEY = "__codexGatewayLitePlanUiProbe";
  const PREEXISTING_ATTR = "data-codex-gateway-lite-preexisting-tooltip";
  const SYNTHETIC_TOOLTIP_ATTR = "data-codex-gateway-lite-synthetic-tooltip";
  const PROBING_ATTR = "data-codex-gateway-lite-tooltip-probing";
  const CLEANUP_TOKEN_KEY = "__codexGatewayLitePlanUiProbeCleanupToken";
  document.getElementById("codex-gateway-lite-hover-probe-style")?.remove();
  document.getElementById(PROBE_STYLE_ID)?.remove();
  document.querySelectorAll(`[${PREEXISTING_ATTR}]`).forEach((node) => node.removeAttribute(PREEXISTING_ATTR));
  document.querySelectorAll(`[${SYNTHETIC_TOOLTIP_ATTR}]`).forEach((node) => node.remove());
  document.documentElement.removeAttribute(PROBING_ATTR);
  try { delete window[PROBE_KEY]; } catch { window[PROBE_KEY] = null; }
  try { delete window[CLEANUP_TOKEN_KEY]; } catch { window[CLEANUP_TOKEN_KEY] = ""; }
  return null;
})()
"#;

const PLAN_TOOLTIP_SAMPLE_READ_SCRIPT: &str = r#"
(() => {
  const SNAPSHOT_KEY = "__codexGatewayLitePlanUiExternalSnapshot";
  const APPLY_KEY = "__codexGatewayLitePlanUiApply";
  const PROBE_KEY = "__codexGatewayLitePlanUiProbe";
  const PREEXISTING_ATTR = "data-codex-gateway-lite-preexisting-tooltip";
  const SYNTHETIC_TOOLTIP_ATTR = "data-codex-gateway-lite-synthetic-tooltip";
  const progressPattern = /第\s*\d+\s*\/\s*\d+\s*步/;
  const text = (el) => String(el?.textContent || "").replace(/\s+/g, " ").trim();
  const visible = (el) => {
    const rect = el?.getBoundingClientRect?.();
    if (!rect || rect.width <= 4 || rect.height <= 4) return false;
    const style = getComputedStyle(el);
    return style.display !== "none" && style.visibility !== "hidden" && Number(style.opacity || 1) > 0.01;
  };
  const probe = window[PROBE_KEY] || null;
  const currentThreadId = () => document
    .querySelector('[data-app-action-sidebar-thread-active="true"][data-app-action-sidebar-thread-id]')
    ?.getAttribute?.("data-app-action-sidebar-thread-id") || "";
  const threadId = probe?.threadId || currentThreadId();
  const nativePill = Array.from(document.querySelectorAll('[data-codex-gateway-lite-native-pill="true"]'))
    .map((node) => ({ node, value: text(node) }))
    .filter((entry) => {
      const pillThreadId = entry.node.getAttribute?.("data-codex-gateway-lite-native-pill-thread-id") || "";
      return !threadId || !pillThreadId || pillThreadId === threadId;
    })
    .filter((entry) => progressPattern.test(entry.value))
    .sort((a, b) => {
      const ar = a.node.getBoundingClientRect();
      const br = b.node.getBoundingClientRect();
      return (ar.width * ar.height) - (br.width * br.height);
    })[0];
  const pillText = nativePill?.value || "";
  const progress = pillText.match(progressPattern)?.[0] || "任务清单";
  const detail = pillText.replace(progress, "").replace(/^·\s*/, "").trim();

  function normalizeItem(value) {
    let item = String(value || "").replace(/\s+/g, " ").trim();
    item = item.replace(progressPattern, "").trim();
    item = item.replace(/^[·•\-\s✓✔○◯]+/, "").trim();
    if (!item || item.length < 3 || item.length > 180) return "";
    if (/^(gpt|grok|claude|gemini|deepseek|qwen|llama|codex|o\d)[\w.\-\s]*$/i.test(item)) return "";
    if (/文件已更改|正在思考|要求后续变更|完全访问|环境信息|提交或推送|暂无来源/.test(item)) return "";
    if (/^(打开位置|打开图片|审查)$/.test(item)) return "";
    if (/^(引导|删除|更多)$/.test(item)) return "";
    return item;
  }

  function statusForRow(row) {
    if (row.querySelector(".animate-spin")) return "running";
    const label = Array.from(row.children)
      .find((child) => child.tagName === "SPAN" && text(child));
    if (String(label?.className || "").includes("text-token-text-tertiary")) return "done";
    return "pending";
  }

  function uniqueRows(values) {
    const rows = [];
    for (const value of values) {
      const item = normalizeItem(value?.text ?? value);
      if (!item) continue;
      if (rows.some((existing) => existing.text === item || existing.text.includes(item) || item.includes(existing.text))) continue;
      rows.push({
        text: item,
        status: value?.status || "pending",
        iconHtml: value?.iconHtml || "",
        textColor: value?.textColor || "",
        iconColor: value?.iconColor || "",
      });
      if (rows.length >= 12) break;
    }
    return rows;
  }

  function rowSnapshot(row) {
    const label = Array.from(row.children)
      .find((child) => child.tagName === "SPAN" && text(child));
    const icon = Array.from(row.children)
      .find((child) => child.querySelector?.("svg"));
    const iconHtml = String(icon?.innerHTML || "")
      .replace(/\sstyle=(["'])[^"']*animation-delay:[^"']*\1/gi, "");
    return {
      text: text(row),
      status: statusForRow(row),
      iconHtml: iconHtml.includes("<svg") && !/<script|on\w+=/i.test(iconHtml) ? iconHtml : "",
      textColor: label ? getComputedStyle(label).color : "",
      iconColor: icon ? getComputedStyle(icon).color : "",
    };
  }

  function rowsFromTooltip(tooltip) {
    const rowNodes = Array.from(tooltip.querySelectorAll("div"))
      .filter((node) => {
        const rect = node.getBoundingClientRect();
        if (rect.width < 80 || rect.width > 360 || rect.height < 10 || rect.height > 96) return false;
        const label = Array.from(node.children)
          .find((child) => child.tagName === "SPAN" && text(child));
        if (!label || text(label) !== text(node)) return false;
        return true;
      });
    const rows = uniqueRows(rowNodes.map(rowSnapshot));
    if (rows.length) return rows;
    return uniqueRows(Array.from(tooltip.querySelectorAll("span")).map((node) => ({ text: text(node), status: "pending" })));
  }

  function tooltipNearProbe(tooltip) {
    if (!probe || !Number.isFinite(Number(probe.x)) || !Number.isFinite(Number(probe.y))) return true;
    const rect = tooltip.getBoundingClientRect();
    if (rect.width < 96 || rect.width > 430 || rect.height < 24 || rect.height > 420) return false;
    const centerX = rect.left + rect.width / 2;
    const xLimit = Math.max(180, Math.min(380, Number(probe.width || 80) * 3.5));
    const aboveTrigger = rect.bottom <= Number(probe.y) + 36 && rect.bottom >= Number(probe.y) - 420;
    const belowTrigger = rect.top >= Number(probe.y) - 24 && rect.top <= Number(probe.y) + 320;
    return Math.abs(centerX - Number(probe.x)) <= xLimit && (aboveTrigger || belowTrigger);
  }

  const tooltips = Array.from(document.querySelectorAll('[role="tooltip"]'))
    .filter((node) => !node.closest?.('[data-codex-gateway-lite-plan-ui="dock"]'))
    .filter(visible)
    .filter(tooltipNearProbe)
    .filter((node) => text(node).length > 0);
  let rows = [];
  for (const tooltip of tooltips) {
    rows = rowsFromTooltip(tooltip);
    if (rows.length) break;
  }
  if (!rows.length) return null;

  const snapshot = {
    threadId,
    progress,
    detail,
    items: rows.map((row) => row.text),
    rows,
    at: Date.now(),
    source: probe ? "cdp-tooltip" : "visible-tooltip",
  };
  window[SNAPSHOT_KEY] = snapshot;
  if (typeof window[APPLY_KEY] === "function") {
    try { window[APPLY_KEY](); } catch {}
  }
  return snapshot;
})()
"#;

#[cfg(test)]
const PLAN_TOOLTIP_SAMPLE_CLEANUP_SCRIPT: &str = r#"
(() => {
  const PROBE_KEY = "__codexGatewayLitePlanUiProbe";
  const PROBE_STYLE_ID = "codex-gateway-lite-plan-tooltip-probe-style";
  const PREEXISTING_ATTR = "data-codex-gateway-lite-preexisting-tooltip";
  const SYNTHETIC_TOOLTIP_ATTR = "data-codex-gateway-lite-synthetic-tooltip";
  const PROBING_ATTR = "data-codex-gateway-lite-tooltip-probing";
  const CLEANUP_TOKEN_KEY = "__codexGatewayLitePlanUiProbeCleanupToken";
  document.querySelectorAll(`[${SYNTHETIC_TOOLTIP_ATTR}]`).forEach((node) => node.remove());
  document.getElementById("codex-gateway-lite-hover-probe-style")?.remove();
  window.setTimeout(() => {
    document.querySelectorAll(`[${SYNTHETIC_TOOLTIP_ATTR}]`).forEach((node) => node.remove());
    document.documentElement.removeAttribute(PROBING_ATTR);
    document.getElementById(PROBE_STYLE_ID)?.remove();
    document.querySelectorAll(`[${PREEXISTING_ATTR}]`).forEach((node) => node.removeAttribute(PREEXISTING_ATTR));
    try { delete window[CLEANUP_TOKEN_KEY]; } catch { window[CLEANUP_TOKEN_KEY] = ""; }
  }, 120);
  document.querySelectorAll("[data-codex-gateway-lite-suppressed-plan-tooltip]").forEach((node) => node.removeAttribute("data-codex-gateway-lite-suppressed-plan-tooltip"));
  try { delete window.__codexGatewayLitePlanUiProbe; } catch { window.__codexGatewayLitePlanUiProbe = null; }
  return true;
})()
"#;

const PLAN_UI_ACTIVE_THREAD_NEEDS_SEED_SCRIPT: &str = r#"
(() => {
  const request = window.__codexGatewayLitePlanUiActiveSeedRequest;
  if (typeof request !== "function") {
    return { threadId: "", needsSeed: false, reason: "not-injected" };
  }
  try {
    return request() || { threadId: "", needsSeed: false, reason: "empty" };
  } catch (error) {
    return {
      threadId: "",
      needsSeed: false,
      reason: "error",
      error: String(error && error.message || error),
    };
  }
})()
"#;

const PLAN_UI_SCRIPT: &str = r#"
(() => {
  const STYLE_ID = "codex-gateway-lite-plan-ui-style";
  const MARK = "data-codex-gateway-lite-plan-ui";
  const DOCK_ID = "codex-gateway-lite-plan-ui-dock";
  const SOURCE_ATTR = "data-codex-gateway-lite-native-pill";
  const SNAPSHOT_KEY = "__codexGatewayLitePlanUiExternalSnapshot";
  const EXTERNAL_SNAPSHOTS_KEY = "__codexGatewayLitePlanUiExternalSnapshots";
  const STORAGE_KEY = "codex-gateway-lite-plan-ui-snapshots-v1";
  const STORAGE_LIMIT = 200;
  const STATE_LIMIT = 80;
  const SCRIPT_VERSION = 44;
  const progressPattern = /第\s*\d+\s*\/\s*\d+\s*步/;
  const COMPLETE_SETTLE_MS = 1_500;
  const STALE_RUNNING_SETTLE_MS = 8_000;
  const RIGHT_PANEL_DISMISS_GRACE_MS = 1_500;
  const RIGHT_RAIL_FOLLOW_FRAME_MS = 420;

  const previousState = window.__codexGatewayLitePlanUiState;
  const previousSnapshots = previousState?.snapshotsByThread && typeof previousState.snapshotsByThread === "object"
    ? previousState.snapshotsByThread
    : {};
  const previousNativeSeen = previousState?.lastNativePillSeenAtByThread && typeof previousState.lastNativePillSeenAtByThread === "object"
    ? previousState.lastNativePillSeenAtByThread
    : {};
  const state = previousState?.version === SCRIPT_VERSION ? previousState : {
    version: SCRIPT_VERSION,
    lastSnapshot: previousState?.lastSnapshot || null,
    lastSourceSeenAt: 0,
    lastNativePillSeenAt: 0,
    lastRenderSignature: "",
    lastThreadId: "",
    snapshotsByThread: previousSnapshots,
    lastNativePillSeenAtByThread: previousNativeSeen,
    rightPanelDismissedThreadId: previousState?.rightPanelDismissedThreadId || "",
    rightPanelDismissedAt: Number(previousState?.rightPanelDismissedAt || 0),
    rightPanelDismissedPanelSignature: previousState?.rightPanelDismissedPanelSignature || "",
    rightRailFollowUntil: 0,
  };
  window.__codexGatewayLitePlanUiState = state;
  if (previousState?.version === SCRIPT_VERSION
      && window.__codexGatewayLitePlanUiObserver
      && typeof window.__codexGatewayLitePlanUiApply === "function") {
    try { window.__codexGatewayLitePlanUiApply(); } catch {}
    return;
  }

  function installStyle() {
    let style = document.getElementById(STYLE_ID);
    if (!style) {
      style = document.createElement("style");
      style.id = STYLE_ID;
      document.documentElement.appendChild(style);
    }
    style.textContent = `
      @keyframes cgl-plan-spin {
        to { transform: rotate(360deg); }
      }
      [${MARK}="dock"] {
        position: fixed !important;
        top: var(--cgl-plan-top, 92px) !important;
        right: var(--cgl-plan-right, 24px) !important;
        left: auto !important;
        bottom: auto !important;
        z-index: 50 !important;
        width: var(--cgl-plan-width, min(360px, calc(100vw - 48px))) !important;
        max-width: calc(100vw - 48px) !important;
        max-height: min(46vh, calc(100vh - var(--cgl-plan-top, 92px) - 24px)) !important;
        overflow: auto !important;
        box-sizing: border-box !important;
        border: 0.5px solid color-mix(in srgb, CanvasText 14%, transparent) !important;
        border-radius: 12px !important;
        background: color-mix(in srgb, Canvas 94%, transparent) !important;
        color: CanvasText !important;
        box-shadow: 0 14px 34px rgba(0, 0, 0, 0.16) !important;
        backdrop-filter: blur(14px) !important;
        font: 13px/1.25 system-ui, -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif !important;
        pointer-events: auto !important;
        contain: layout style !important;
      }
      [${MARK}="dock"][hidden] {
        display: none !important;
      }
      [${MARK}="dock"] .cgl-plan-meta {
        display: flex !important;
        align-items: center !important;
        justify-content: flex-start !important;
        gap: 6px !important;
        min-width: 0 !important;
        padding: 10px 12px 8px !important;
        border-bottom: 0.5px solid color-mix(in srgb, CanvasText 10%, transparent) !important;
        color: color-mix(in srgb, CanvasText 62%, transparent) !important;
        font-size: 12px !important;
        font-weight: 500 !important;
        line-height: 1.25 !important;
        white-space: normal !important;
        overflow-wrap: anywhere !important;
      }
      [${MARK}="dock"] .cgl-plan-meta-spinner {
        width: 16px !important;
        height: 16px !important;
        flex: 0 0 16px !important;
        color: #0a84ff !important;
      }
      [${MARK}="dock"] .cgl-plan-meta-spinner svg {
        display: block !important;
        width: 16px !important;
        height: 16px !important;
      }
      [${MARK}="dock"] .cgl-plan-meta-spinner .cgl-plan-spinner {
        animation: cgl-plan-spin 0.9s linear infinite !important;
        transform-origin: center !important;
        transform-box: fill-box !important;
      }
      [${MARK}="dock"] .cgl-plan-meta-content {
        min-width: 0 !important;
        white-space: normal !important;
        overflow-wrap: anywhere !important;
      }
      [${MARK}="dock"] .cgl-plan-list {
        display: flex !important;
        flex-direction: column !important;
        gap: 8px !important;
        padding: 9px 10px 10px !important;
      }
      [${MARK}="dock"] .cgl-plan-item {
        display: flex !important;
        gap: 8px !important;
        align-items: start !important;
        min-width: 0 !important;
      }
      [${MARK}="dock"] .cgl-plan-icon {
        width: 16px !important;
        height: 16px !important;
        flex: 0 0 16px !important;
        margin-top: 0 !important;
        color: color-mix(in srgb, CanvasText 58%, transparent) !important;
      }
      [${MARK}="dock"] .cgl-plan-icon svg {
        display: block !important;
        width: 16px !important;
        height: 16px !important;
      }
      [${MARK}="dock"] .cgl-plan-item.is-running .cgl-plan-icon {
        color: #0a84ff !important;
      }
      [${MARK}="dock"] .cgl-plan-item.is-running .cgl-plan-spinner {
        animation: cgl-plan-spin 0.9s linear infinite !important;
        transform-origin: center !important;
        transform-box: fill-box !important;
      }
      [${MARK}="dock"] .cgl-plan-icon .animate-spin {
        animation: cgl-plan-spin 0.9s linear infinite !important;
        transform-origin: center !important;
        transform-box: fill-box !important;
      }
      [${MARK}="dock"] .cgl-plan-item.is-done .cgl-plan-icon,
      [${MARK}="dock"] .cgl-plan-item.is-done .cgl-plan-text {
        color: color-mix(in srgb, CanvasText 48%, transparent) !important;
      }
      [${MARK}="dock"] .cgl-plan-text {
        min-width: 0 !important;
        color: color-mix(in srgb, CanvasText 72%, transparent) !important;
        line-height: 16px !important;
        white-space: normal !important;
        overflow-wrap: anywhere !important;
      }
      [${MARK}="dock"] .cgl-plan-empty {
        padding: 9px 11px 11px !important;
        color: color-mix(in srgb, CanvasText 62%, transparent) !important;
      }
      @media (max-width: 760px) {
        [${MARK}="dock"] {
          top: 72px !important;
          right: 12px !important;
          width: min(360px, calc(100vw - 24px)) !important;
          max-width: calc(100vw - 24px) !important;
          max-height: 38vh !important;
        }
      }
    `;
  }

  function text(el) {
    return String(el?.textContent || "").replace(/\s+/g, " ").trim();
  }

  function visible(el) {
    const rect = el?.getBoundingClientRect?.();
    if (!rect || rect.width <= 8 || rect.height <= 8) return false;
    const style = getComputedStyle(el);
    return style.display !== "none" && style.visibility !== "hidden" && Number(style.opacity || 1) > 0.01;
  }

  function appBusy() {
    return Array.from(document.querySelectorAll("button"))
      .filter(visible)
      .some((button) => /停止|Stop|Cancel|中止/.test([
        button.getAttribute("aria-label") || "",
        button.getAttribute("title") || "",
        text(button),
      ].join(" ")));
  }

  function alphaOf(color) {
    const match = String(color || "").match(/rgba?\(([^)]+)\)/);
    if (!match) return 0;
    const parts = match[1].split(",").map((part) => Number(part.trim()));
    if (parts.length < 4) return 1;
    return Number.isFinite(parts[3]) ? parts[3] : 0;
  }

  function mediaPreviewControlsInside(node) {
    return Array.from(node.querySelectorAll?.("button,[role='button']") || [])
      .filter(visible)
      .some((control) => /关闭|Close|下载|Download|放大|缩小|Zoom/.test([
        control.getAttribute("aria-label") || "",
        control.getAttribute("title") || "",
        text(control),
      ].join(" ")));
  }

  function blockingOverlayActive() {
    const viewportArea = Math.max(1, window.innerWidth * window.innerHeight);
    return Array.from(document.querySelectorAll('[role="dialog"], [aria-modal="true"], div, section'))
      .some((node) => {
        if (!visible(node) || managedNode(node) || node === document.body || node === document.documentElement || node.id === "root") return false;
        const rect = node.getBoundingClientRect();
        const areaRatio = (rect.width * rect.height) / viewportArea;
        if (node.getAttribute("aria-modal") === "true" || node.getAttribute("role") === "dialog") {
          return areaRatio > 0.18;
        }
        const style = getComputedStyle(node);
        const zIndex = Number.parseInt(style.zIndex, 10);
        if (rect.width < window.innerWidth * 0.5 || rect.height < window.innerHeight * 0.45) return false;
        const darkBackdrop = alphaOf(style.backgroundColor) > 0.15 || style.backdropFilter !== "none";
        if (!darkBackdrop) return false;
        if (style.position === "fixed" && Number.isFinite(zIndex) && zIndex >= 40) return true;
        return areaRatio > 0.55 && mediaPreviewControlsInside(node);
      });
  }

  function topLayerOwns(panel, rect) {
    const points = [
      [rect.left + rect.width / 2, rect.top + Math.min(28, rect.height / 2)],
      [rect.left + rect.width / 2, rect.top + rect.height / 2],
    ];
    return points.some(([x, y]) => {
      const top = document.elementFromPoint(Math.round(x), Math.round(y));
      return top && (panel === top || panel.contains(top));
    });
  }

  function escapeHtml(value) {
    return String(value || "")
      .replace(/&/g, "&amp;")
      .replace(/</g, "&lt;")
      .replace(/>/g, "&gt;")
      .replace(/"/g, "&quot;");
  }

  function safeCssColor(value) {
    const color = String(value || "").trim();
    if (/^rgba?\(\s*\d+(\.\d+)?\s*,\s*\d+(\.\d+)?\s*,\s*\d+(\.\d+)?\s*(,\s*(0|1|0?\.\d+))?\s*\)$/i.test(color)) return color;
    if (/^color\(\s*srgb\s+[\d.]+\s+[\d.]+\s+[\d.]+(\s*\/\s*(0|1|0?\.\d+))?\s*\)$/i.test(color)) return color;
    if (/^#[0-9a-f]{3,8}$/i.test(color)) return color;
    return "";
  }

  function colorStyle(value) {
    const color = safeCssColor(value);
    return color ? `color: ${color} !important;` : "";
  }

  function colorForToken(root, token) {
    const value = String(token || "").trim();
    if (!root || !value) return "";
    const candidates = Array.from(root.querySelectorAll("*"))
      .map((node) => ({ node, value: text(node) }))
      .filter((entry) => entry.value && entry.value.includes(value))
      .sort((a, b) => a.value.length - b.value.length);
    return candidates[0] ? getComputedStyle(candidates[0].node).color : "";
  }

  function fallbackDeltaColor(token) {
    if (/^\+/.test(token)) return "rgb(0, 166, 65)";
    if (/^-/.test(token)) return "rgb(220, 38, 38)";
    return "";
  }

  function existingMetaColor(snapshot, token) {
    const value = String(token || "");
    const parts = Array.isArray(snapshot?.metaParts) ? snapshot.metaParts : [];
    const exact = parts.find((part) => String(part?.text || "") === value && safeCssColor(part?.color));
    if (exact) return exact.color;
    if (progressPattern.test(value)) {
      const progressPart = parts.find((part) => progressPattern.test(String(part?.text || "")) && safeCssColor(part?.color));
      if (progressPart) return progressPart.color;
    }
    return "";
  }

  function metaPartsFromProgressDetail(progress, detail, colorForPart = () => "") {
    const parts = [];
    if (progress) {
      parts.push({ text: progress, color: colorForPart(progress) });
    }
    if (detail) {
      if (parts.length) parts.push({ text: " · ", color: "" });
      String(detail).split(/([+-]\d[\d,]*)/g).forEach((part) => {
        if (!part) return;
        const color = /^[+-]\d[\d,]*$/.test(part)
          ? (colorForPart(part) || fallbackDeltaColor(part))
          : colorForPart(part);
        parts.push({ text: part, color });
      });
    }
    return parts;
  }

  function safeIconHtml(value) {
    const html = String(value || "");
    if (!html.includes("<svg") || /<script|on\w+=/i.test(html)) return "";
    return html;
  }

  function managedNode(el) {
    return !!el?.closest?.(`[${MARK}="dock"]`);
  }

  function elementFromEventTarget(target) {
    if (!target) return null;
    return target.nodeType === 1 ? target : target.parentElement || null;
  }

  function rightSideExpandedContentRect(rect) {
    if (!rect) return false;
    const minWidth = Math.min(420, Math.max(300, window.innerWidth * 0.24));
    const minHeight = Math.min(420, Math.max(240, window.innerHeight * 0.35));
    return rect.width >= minWidth
      && rect.height >= minHeight
      && rect.left >= window.innerWidth * 0.38
      && rect.right > window.innerWidth * 0.72
      && rect.top < window.innerHeight * 0.28
      && rect.bottom > window.innerHeight * 0.55;
  }

  function rightSideExpandedContentSignature(rect) {
    if (!rect) return "";
    return [
      Math.round(rect.left),
      Math.round(rect.top),
      Math.round(rect.width),
      Math.round(rect.height),
    ].join(":");
  }

  function rightSideExpandedContentInfoFromTarget(target) {
    let node = elementFromEventTarget(target);
    for (let depth = 0; node && node !== document.body && node !== document.documentElement && depth < 14; depth += 1) {
      if (managedNode(node)) return null;
      if (node.closest?.(".app-shell-left-panel, [data-app-action-sidebar-thread-id], [data-app-action-sidebar-section-heading]")) return null;
      if (node.closest?.("[data-thread-find-target='conversation'], [data-turn-key], [data-content-search-unit-key], [data-selected-text-overlay-target]")) return null;
      if (node.closest?.("[data-avatar-overlay-content-frame], [data-avatar-overlay-hit-region], [data-avatar-mascot], [data-avatar-overlay-measure], [data-testid^='avatar-']")) return null;
      if (node.id === "root") return null;
      if (visible(node)) {
        const rect = node.getBoundingClientRect();
        if (rightSideExpandedContentRect(rect)) {
          return { node, rect, signature: rightSideExpandedContentSignature(rect) };
        }
      }
      node = node.parentElement;
    }
    return null;
  }

  function rightSideExpandedContentInfoFromPoint(x, y) {
    if (!Number.isFinite(Number(x)) || !Number.isFinite(Number(y))) return null;
    const nodes = document.elementsFromPoint?.(Math.round(Number(x)), Math.round(Number(y)))
      || [document.elementFromPoint?.(Math.round(Number(x)), Math.round(Number(y)))].filter(Boolean);
    for (const node of nodes) {
      if (!node || managedNode(node)) continue;
      const info = rightSideExpandedContentInfoFromTarget(node);
      if (info) return info;
    }
    return null;
  }

  function activeRightSideExpandedContentInfo() {
    const queryCandidates = Array.from(document.querySelectorAll("main,aside,section,article,div,iframe,webview,[role='tabpanel']"))
      .map((node) => {
        if (!visible(node) || managedNode(node) || node === document.body || node === document.documentElement || node.id === "root") return null;
        if (node.closest?.(".app-shell-left-panel, [data-app-action-sidebar-thread-id], [data-app-action-sidebar-section-heading]")) return null;
        if (node.closest?.("[data-thread-find-target='conversation'], [data-turn-key], [data-content-search-unit-key], [data-selected-text-overlay-target]")) return null;
        const rect = node.getBoundingClientRect();
        return rightSideExpandedContentRect(rect)
          ? { node, rect, signature: rightSideExpandedContentSignature(rect), area: rect.width * rect.height }
          : null;
      })
      .filter(Boolean);
    const pointCandidates = [
      [window.innerWidth * 0.82, window.innerHeight * 0.18],
      [window.innerWidth * 0.82, window.innerHeight * 0.45],
      [window.innerWidth * 0.82, window.innerHeight * 0.72],
      [window.innerWidth * 0.94, window.innerHeight * 0.45],
    ].map(([x, y]) => {
      const info = rightSideExpandedContentInfoFromPoint(x, y);
      return info ? { ...info, area: info.rect.width * info.rect.height } : null;
    }).filter(Boolean);
    return [...queryCandidates, ...pointCandidates]
      .sort((a, b) => b.area - a.area)[0] || null;
  }

  function clearRightPanelDismissal() {
    state.rightPanelDismissedThreadId = "";
    state.rightPanelDismissedAt = 0;
    state.rightPanelDismissedPanelSignature = "";
  }

  function rightPanelDismissalActive() {
    const threadId = currentThreadId();
    if (state.rightPanelDismissedThreadId !== threadId) {
      clearRightPanelDismissal();
      return false;
    }
    if (environmentPanelInfo()) {
      clearRightPanelDismissal();
      return false;
    }
    const dismissedAt = Number(state.rightPanelDismissedAt || 0);
    if (!dismissedAt) return false;
    const active = activeRightSideExpandedContentInfo();
    if (active && state.rightPanelDismissedPanelSignature && active.signature === state.rightPanelDismissedPanelSignature) return true;
    if (Date.now() - dismissedAt < RIGHT_PANEL_DISMISS_GRACE_MS) return true;
    clearRightPanelDismissal();
    return false;
  }

  function hideDockForRightPanel(reason) {
    if (environmentPanelInfo()) return false;
    const dock = document.getElementById(DOCK_ID);
    if (dock) {
      dock.hidden = true;
      dock.dataset.cglPlanHiddenBy = reason || "right-panel";
    }
    return true;
  }

  function hideDockForRightPanelTarget(target) {
    if (environmentPanelInfo()) return false;
    const info = rightSideExpandedContentInfoFromTarget(target);
    if (!info) return false;
    state.rightPanelDismissedThreadId = currentThreadId();
    state.rightPanelDismissedAt = Date.now();
    state.rightPanelDismissedPanelSignature = info.signature || "";
    hideDockForRightPanel("right-panel-click");
    return true;
  }

  function handleRightPanelDismissEvent(event) {
    if (eventTouchesRightRail(event)) {
      scheduleApplyBurst();
    }
    if (environmentPanelInfo()) return;
    if (hideDockForRightPanelTarget(event?.target)) return;
    const pointInfo = rightSideExpandedContentInfoFromPoint(event?.clientX, event?.clientY);
    if (!pointInfo) return;
    state.rightPanelDismissedThreadId = currentThreadId();
    state.rightPanelDismissedAt = Date.now();
    state.rightPanelDismissedPanelSignature = pointInfo.signature || "";
    hideDockForRightPanel("right-panel-click-point");
  }

  function currentThreadId() {
    const conversationId = currentConversationThreadId();
    if (conversationId) return conversationId;
    const sidebar = currentSidebarThreadInfo();
    const titleId = currentVisibleTitleThreadId();
    if (sidebar.id && shouldPreferSidebarThreadId(sidebar, titleId)) return sidebar.id;
    if (titleId) return titleId;
    if (sidebar.id) return sidebar.id;
    return "visible:unknown";
  }

  function activeSeedThreadId() {
    const sidebar = currentSidebarThreadInfo();
    if (/^(local|remote):/.test(sidebar.id || "")) return sidebar.id;
    const conversationId = currentConversationThreadId();
    if (/^(local|remote):/.test(conversationId || "")) return conversationId;
    const current = currentThreadId();
    return /^(local|remote):/.test(current || "") ? current : "";
  }

  function currentConversationThreadId() {
    const portals = Array.from(document.querySelectorAll('[data-above-composer-portal="true"][data-above-composer-conversation-id]'))
      .filter(visible)
      .sort((a, b) => {
        const ar = a.getBoundingClientRect();
        const br = b.getBoundingClientRect();
        return (br.width * br.height) - (ar.width * ar.height);
      });
    const id = portals[0]?.getAttribute?.("data-above-composer-conversation-id") || "";
    return normalizeConversationThreadId(id);
  }

  function normalizeConversationThreadId(id) {
    const value = String(id || "").trim();
    if (!value) return "";
    return /^(local|remote|visible):/.test(value) ? value : `local:${value}`;
  }

  function currentVisibleTitleThreadId() {
    const title = text(document.querySelector("[data-testid='app-shell-header-context-menu-surface']"))
      || text(document.querySelector("header"));
    return title ? `visible:${title.slice(0, 120)}` : "";
  }

  function currentSidebarThreadId() {
    return currentSidebarThreadInfo().id;
  }

  function currentSidebarThreadInfo() {
    const active = document.querySelector('[data-app-action-sidebar-thread-active="true"][data-app-action-sidebar-thread-id]');
    const id = active?.getAttribute?.("data-app-action-sidebar-thread-id") || "";
    return {
      id: id ? normalizeConversationThreadId(id) : "",
      title: text(active),
    };
  }

  function shouldPreferSidebarThreadId(sidebar, titleId) {
    if (!sidebar?.id) return false;
    if (!titleId) return true;
    if (visibleTitleLooksTransient(titleId)) return true;
    if (visibleTitleMatchesSidebar(titleId, sidebar.title)) return true;
    return knownSnapshotForThreadId(sidebar.id) && !knownSnapshotForThreadId(titleId);
  }

  function visibleTitleLooksTransient(titleId) {
    const title = String(titleId || "").replace(/^visible:/, "").trim();
    return /^(新对话|新会话|New chat|Untitled)$/i.test(title);
  }

  function visibleTitleMatchesSidebar(titleId, sidebarTitle) {
    const title = String(titleId || "").replace(/^visible:/, "").trim();
    const sidebar = String(sidebarTitle || "").trim();
    if (!title || !sidebar) return false;
    return title.includes(sidebar) || sidebar.includes(title) || title.slice(0, 24) === sidebar.slice(0, 24);
  }

  function knownSnapshotForThreadId(threadId) {
    const id = String(threadId || "");
    if (!id) return false;
    const snapshots = state.snapshotsByThread && typeof state.snapshotsByThread === "object"
      ? state.snapshotsByThread
      : {};
    if (snapshots[id]) return true;
    return !!externalSnapshotForThread(id) || !!storedSnapshotForThread(id);
  }

  function currentThreadRoot() {
    const conversation = document.querySelector('[data-thread-find-target="conversation"]');
    const main = conversation?.closest?.("main")
      || Array.from(document.querySelectorAll("main"))
        .find((node) => visible(node) && !node.closest?.(".app-shell-left-panel"));
    return main || document.body;
  }

  function storedSnapshots() {
    try {
      const parsed = JSON.parse(localStorage.getItem(STORAGE_KEY) || "{}");
      return parsed && typeof parsed === "object" ? parsed : {};
    } catch {
      return {};
    }
  }

  function sanitizeSnapshotForStorage(snapshot) {
    if (!snapshot || typeof snapshot !== "object") return null;
    const rows = rowsForSnapshot(snapshot);
    if (!rows.length) return null;
    const normalized = normalizedSnapshotForRows(snapshot, rows);
    return {
      threadId: String(snapshot.threadId || ""),
      progress: String(normalized.progress || ""),
      detail: String(normalized.detail || ""),
      items: rows.map((row) => row.text),
      rows: rows.map((row) => ({
        text: String(row.text || ""),
        status: String(row.status || "pending"),
        iconHtml: String(row.iconHtml || ""),
        textColor: safeCssColor(row.textColor),
        iconColor: safeCssColor(row.iconColor),
      })),
      metaParts: Array.isArray(snapshot.metaParts)
        ? snapshot.metaParts.slice(0, 8).map((part) => ({ text: String(part.text || ""), color: safeCssColor(part.color) }))
        : [],
      sourceConversationId: String(snapshot.sourceConversationId || ""),
      sourceTurnId: String(snapshot.sourceTurnId || ""),
      sourceTodoId: String(snapshot.sourceTodoId || ""),
      at: Number(snapshot.at || Date.now()),
      source: String(snapshot.source || "stored"),
    };
  }

  function persistThreadSnapshots(snapshots) {
    try {
      const entries = Object.entries(snapshots || {})
        .map(([threadId, snapshot]) => [threadId, sanitizeSnapshotForStorage({ ...snapshot, threadId })])
        .filter((entry) => entry[1])
        .sort((a, b) => Number(b[1]?.at || 0) - Number(a[1]?.at || 0))
        .slice(0, STORAGE_LIMIT);
      localStorage.setItem(STORAGE_KEY, JSON.stringify(Object.fromEntries(entries)));
    } catch {
    }
  }

  function storedSnapshotForThread(threadId) {
    const id = String(threadId || "");
    if (!id) return null;
    const snapshot = storedSnapshots()[id];
    if (!snapshot || typeof snapshot !== "object") return null;
    if (Date.now() - Number(snapshot.at || 0) > 7 * 24 * 60 * 60 * 1000) return null;
    return sanitizeSnapshotForStorage({ ...snapshot, threadId: id, source: snapshot.source || "stored" });
  }

  function externalSnapshots() {
    const snapshots = window[EXTERNAL_SNAPSHOTS_KEY];
    return snapshots && typeof snapshots === "object" ? snapshots : {};
  }

  function externalSnapshotForThread(threadId) {
    const id = String(threadId || "");
    if (!id) return null;
    const snapshot = externalSnapshots()[id];
    if (!snapshot || typeof snapshot !== "object") return null;
    if (Date.now() - Number(snapshot.at || 0) > 30 * 24 * 60 * 60 * 1000) return null;
    return sanitizeSnapshotForStorage({ ...snapshot, threadId: id, source: snapshot.source || "history-rollout" });
  }

  function snapshotForThread(threadId) {
    const id = String(threadId || currentThreadId());
    const snapshots = state.snapshotsByThread && typeof state.snapshotsByThread === "object"
      ? state.snapshotsByThread
      : {};
    state.snapshotsByThread = snapshots;
    const candidateIds = [id, ...snapshotAliasIdsForThread(id)];
    const candidates = [];
    candidateIds.forEach((candidateId) => {
      const alias = candidateId !== id;
      if (snapshots[candidateId]) candidates.push({ snapshot: snapshots[candidateId], alias, source: "memory" });
      const external = externalSnapshotForThread(candidateId);
      if (external) candidates.push({ snapshot: external, alias, source: "history-rollout" });
      const stored = storedSnapshotForThread(candidateId);
      if (stored) candidates.push({ snapshot: stored, alias, source: "stored" });
    });
    if (candidates.length) {
      const best = candidates
        .sort((a, b) => Number(b.snapshot?.at || 0) - Number(a.snapshot?.at || 0))[0];
      const existingAt = Number(snapshots[id]?.at || 0);
      const bestAt = Number(best.snapshot?.at || 0);
      if (best.alias || best.source !== "memory" || !snapshots[id] || bestAt >= existingAt) {
        snapshots[id] = { ...best.snapshot, threadId: id, source: best.snapshot.source || best.source };
      }
    }
    const finalSnapshot = snapshots[id] || null;
    const finalRows = rowsForSnapshot(finalSnapshot);
    if (finalSnapshot && finalRows.length) {
      const normalized = normalizedSnapshotForRows(finalSnapshot, finalRows);
      snapshots[id] = normalized;
      return normalized;
    }
    return finalSnapshot;
  }

  function directSnapshotForSeed(threadId) {
    const id = String(threadId || "");
    if (!id) return null;
    const snapshots = state.snapshotsByThread && typeof state.snapshotsByThread === "object"
      ? state.snapshotsByThread
      : {};
    const candidates = [];
    if (snapshots[id]) candidates.push({ snapshot: snapshots[id], source: "memory" });
    const external = externalSnapshotForThread(id);
    if (external) candidates.push({ snapshot: external, source: "history-rollout" });
    const stored = storedSnapshotForThread(id);
    if (stored) candidates.push({ snapshot: stored, source: "stored" });
    if (!candidates.length) return null;
    const best = candidates
      .sort((a, b) => Number(b.snapshot?.at || 0) - Number(a.snapshot?.at || 0))[0];
    return sanitizeSnapshotForStorage({ ...best.snapshot, threadId: id, source: best.snapshot.source || best.source });
  }

  function snapshotAliasIdsForThread(threadId) {
    const id = String(threadId || "");
    const aliases = [];
    const titleId = currentVisibleTitleThreadId();
    const conversationId = currentConversationThreadId();
    const sidebar = currentSidebarThreadInfo();
    const sidebarId = sidebar.id;
    if (id.startsWith("local:") || id.startsWith("remote:")) {
      if (titleId && titleId !== id) aliases.push(titleId);
    } else {
      if (conversationId && conversationId !== id) aliases.push(conversationId);
      if (sidebarId && sidebarId !== id && shouldPreferSidebarThreadId(sidebar, id)) aliases.push(sidebarId);
      if (titleId && titleId !== id) aliases.push(titleId);
    }
    return Array.from(new Set(aliases));
  }

  function rememberThreadSnapshot(snapshot) {
    const threadId = String(snapshot?.threadId || currentThreadId());
    if (!threadId || !snapshot) return;
    const rows = rowsForSnapshot(snapshot);
    const normalized = normalizedSnapshotForRows(snapshot, rows);
    const snapshots = state.snapshotsByThread && typeof state.snapshotsByThread === "object"
      ? state.snapshotsByThread
      : {};
    snapshots[threadId] = { ...normalized, threadId };
    snapshotAliasIdsForThread(threadId).forEach((aliasId) => {
      snapshots[aliasId] = { ...normalized, threadId: aliasId, source: snapshot.source || "alias" };
    });
    const entries = Object.entries(snapshots)
      .sort((a, b) => Number(b[1]?.at || 0) - Number(a[1]?.at || 0));
    state.snapshotsByThread = Object.fromEntries(entries.slice(0, STATE_LIMIT));
    persistThreadSnapshots(state.snapshotsByThread);
  }

  function lastNativePillSeenAt(threadId) {
    const map = state.lastNativePillSeenAtByThread && typeof state.lastNativePillSeenAtByThread === "object"
      ? state.lastNativePillSeenAtByThread
      : {};
    state.lastNativePillSeenAtByThread = map;
    return Number(map[String(threadId || "")] || 0);
  }

  function markNativePillSeen(threadId) {
    const id = String(threadId || currentThreadId());
    const map = state.lastNativePillSeenAtByThread && typeof state.lastNativePillSeenAtByThread === "object"
      ? state.lastNativePillSeenAtByThread
      : {};
    map[id] = Date.now();
    state.lastNativePillSeenAtByThread = map;
    state.lastNativePillSeenAt = Date.now();
  }

  function insideIgnoredArea(el) {
    if (!el || el === document.documentElement || el === document.body || el.id === "root") return true;
    if (managedNode(el)) return true;
    if (el.closest?.('[role="tooltip"]')) return true;
    if (el.closest?.("[data-avatar-overlay-content-frame], [data-avatar-overlay-hit-region], [data-avatar-mascot], [data-avatar-overlay-measure], [data-testid^='avatar-']")) return true;
    if (el.closest?.("pre, code, textarea, input, [contenteditable='true']")) return true;
    // 消息正文渲染在 data-turn-key / data-content-search-unit-key / 富文本选区容器内；
    // 原生进度 pill 实际渲染在独立的 above-composer portal 里，不会出现在这些容器中，
    // 排除它们能从结构上避免误抓聊天正文，不再需要靠文本关键词硬编码去猜。
    if (el.closest?.("[data-turn-key], [data-content-search-unit-key], [data-selected-text-overlay-target]")) return true;
    return !!el.closest?.(
      ".app-shell-left-panel, [data-app-action-sidebar-thread-id], [data-app-action-sidebar-section-heading]"
    );
  }

  function parseNativePillValue(value) {
    const normalized = String(value || "").replace(/\s+/g, " ").trim();
    if (!progressPattern.test(normalized)) return null;
    if (normalized.length > 96) return null;
    if (/环境信息|提交或推送|来源|暂无来源|完全访问|要求后续变更|超高/.test(normalized)) return null;
    if (/引导|打开位置|打开图片|审查|删除|更多/.test(normalized)) return null;
    const match = normalized.match(/^(第\s*\d+\s*\/\s*\d+\s*步)(?:\s*[·•]?\s*(.{1,72}))?$/);
    if (!match) return null;
    const progress = match[1].replace(/\s+/g, " ").trim();
    const detail = String(match[2] || "").replace(/\s+/g, " ").trim();
    if (detail && !/^(?:\d+\s*个文件已更改(?:\s*[+-]\d[\d,]*){0,4}|[+-]\d[\d,]*(?:\s*[+-]\d[\d,]*){0,3})$/.test(detail)) return null;
    return { value: detail ? `${progress} · ${detail}` : progress, progress, detail };
  }

  function extractFileChangeDetail(value) {
    const normalized = String(value || "").replace(/\s+/g, " ").trim();
    const match = normalized.match(/(?:^|[·•\s])(\d+\s*个文件已更改(?:\s*[+-]\d[\d,]*){0,4}|[+-]\d[\d,]*(?:\s*[+-]\d[\d,]*){0,3})(?:$|\s)/);
    if (!match) return "";
    return match[1].replace(/\s+/g, " ").trim();
  }

  function nearbyPillDetail(pill, progress) {
    if (!pill) return "";
    const nodes = [];
    let node = pill;
    for (let depth = 0; depth < 5; depth += 1) {
      if (!node || node === document.body || node === document.documentElement) break;
      if (managedNode(node) || insideIgnoredArea(node)) break;
      const rect = node.getBoundingClientRect?.();
      if (rect && (rect.height > 104 || rect.width > Math.min(620, Math.max(260, window.innerWidth - 48)))) break;
      nodes.push(node);
      node = node.parentElement;
    }
    return nodes
      .map((entry) => text(entry).replace(progressPattern, " "))
      .map(extractFileChangeDetail)
      .find(Boolean) || "";
  }

  function cleanupLegacyMarks() {
    document.querySelectorAll(`[${MARK}]`).forEach((node) => {
      if (node.getAttribute(MARK) !== "dock") node.removeAttribute(MARK);
    });
    document.querySelectorAll(`[${SOURCE_ATTR}]`).forEach((node) => {
      if (!node.isConnected || !nativePillText(node) || nativePillHost(node) !== node) {
        node.removeAttribute(SOURCE_ATTR);
        node.removeAttribute("data-codex-gateway-lite-native-pill-thread-id");
      }
    });
    document.querySelectorAll("[data-codex-gateway-lite-suppressed-plan-tooltip]").forEach((node) => {
      node.removeAttribute("data-codex-gateway-lite-suppressed-plan-tooltip");
    });
  }

  function nativePillText(el) {
    const parts = parseNativePillValue(text(el));
    if (!parts) return "";
    const rect = el?.getBoundingClientRect?.();
    if (rect) {
      const maxWidth = Math.min(520, Math.max(220, window.innerWidth - 48));
      if (rect.width > maxWidth || rect.height > 72) return "";
    }
    return parts.value;
  }

  function planPillSurfaceScore(el) {
    const className = String(el?.className || "");
    const rect = el?.getBoundingClientRect?.();
    if (!rect || rect.width < 48 || rect.height < 18 || rect.height > 72 || rect.width > Math.min(520, Math.max(220, window.innerWidth - 48))) return 0;
    if (!nativePillText(el) || insideIgnoredArea(el)) return 0;
    const style = getComputedStyle(el);
    const radius = Math.max(
      Number.parseFloat(style.borderTopLeftRadius) || 0,
      Number.parseFloat(style.borderTopRightRadius) || 0,
      Number.parseFloat(style.borderBottomLeftRadius) || 0,
      Number.parseFloat(style.borderBottomRightRadius) || 0
    );
    const borderWidth = [
      style.borderTopWidth,
      style.borderRightWidth,
      style.borderBottomWidth,
      style.borderLeftWidth,
    ].reduce((sum, value) => sum + (Number.parseFloat(value) || 0), 0);
    const bgAlpha = alphaOf(style.backgroundColor);
    const interactive = el.matches?.("button,[role='button'],[data-state],[aria-expanded],[aria-haspopup]");
    const compactTextOnly = text(el) === nativePillText(el);
    let score = 0;
    if (interactive) score += 4;
    if (radius >= 8) score += 3;
    if (borderWidth > 0) score += 2;
    if (bgAlpha > 0.02 || style.boxShadow !== "none") score += 2;
    if (/rounded-|border-token|bg-token|shadow|popover|tooltip/i.test(className)) score += 1;
    if (compactTextOnly) score += 1;
    return score;
  }

  function isPlanPillSurface(el) {
    return planPillSurfaceScore(el) >= 3;
  }

  function nativePillHost(el) {
    const candidates = [];
    let node = el;
    for (let depth = 0; depth < 5; depth += 1) {
      if (!node || node === document.body || node === document.documentElement) break;
      if (managedNode(node) || insideIgnoredArea(node)) break;
      if (nativePillText(node)) {
        const score = planPillSurfaceScore(node);
        if (score > 0) {
          const rect = node.getBoundingClientRect?.();
          candidates.push({ node, score, area: rect ? rect.width * rect.height : 0 });
        }
      }
      const parent = node?.parentElement;
      if (!parent || parent === document.body || parent === document.documentElement) break;
      if (managedNode(parent) || insideIgnoredArea(parent)) break;
      if (!nativePillText(parent)) break;
      const parentScore = planPillSurfaceScore(parent);
      if (parentScore > 0) {
        const parentRect = parent.getBoundingClientRect?.();
        candidates.push({ node: parent, score: parentScore, area: parentRect ? parentRect.width * parentRect.height : 0 });
      }
      const rect = parent.getBoundingClientRect?.();
      const nodeRect = node.getBoundingClientRect?.();
      if (rect && nodeRect && !isPlanPillSurface(parent)) {
        const widthJump = rect.width > Math.max(nodeRect.width + 36, nodeRect.width * 1.4);
        const heightJump = rect.height > Math.max(nodeRect.height + 24, nodeRect.height * 2.2);
        if (widthJump || heightJump) break;
      }
      if (rect && (rect.height > 72 || rect.width > 520)) break;
      if (isPlanPillSurface(parent)) break;
      node = parent;
    }
    return candidates
      .sort((a, b) => (b.score - a.score) || (b.area - a.area))[0]?.node || null;
  }

  function composerPortalRoot(threadId) {
    const portals = Array.from(document.querySelectorAll('[data-above-composer-portal="true"]')).filter(visible);
    if (!portals.length) return null;
    const id = String(threadId || "");
    const matched = portals.find((node) => {
      const conversationId = node.getAttribute("data-above-composer-conversation-id") || "";
      return conversationId && id.includes(conversationId);
    });
    return matched || (portals.length === 1 ? portals[0] : null);
  }

  function nativePills() {
    // 原生进度 pill 实际渲染在输入框上方的独立 portal 里，跟消息列表/引导层完全隔离；
    // 命中这个 portal 时只在其中扫描，从根源上避免误抓聊天正文或其它浮层内容。
    // 找不到 portal（例如版本差异）时才退回旧的全会话根节点扫描兜底。
    const portalRoot = composerPortalRoot(currentThreadId());
    const root = portalRoot || currentThreadRoot();
    const hosts = new Set();
    Array.from(root.querySelectorAll("button,div,span,[role='button']"))
      .filter((node) => !insideIgnoredArea(node) && nativePillText(node))
      .forEach((node) => {
        const host = nativePillHost(node);
        if (host) hosts.add(host);
      });
    const nodes = Array.from(hosts)
      .filter((node) => !insideIgnoredArea(node) && nativePillText(node))
      .filter((node, _index, all) => !all.some((other) => other !== node && node.contains(other) && nativePillText(other)));
    return nodes
      .sort((a, b) => {
        const ar = a.getBoundingClientRect();
        const br = b.getBoundingClientRect();
        return (br.bottom - ar.bottom) || (ar.left - br.left);
      });
  }

  function normalizeItem(value) {
    let item = String(value || "").replace(/\s+/g, " ").trim();
    item = item.replace(progressPattern, "").replace(/·?\s*\d+\s*个文件已更改\s*[+-]?\d*\s*[+-]?\d*/g, "").trim();
    item = item.replace(/^[·•\-\s✓✔○◯]+/, "").trim();
    if (!item || item.length < 3 || item.length > 180) return "";
    if (/^\+?\d+\s*-?\d*$/.test(item)) return "";
    if (/^(gpt|grok|claude|gemini|deepseek|qwen|llama|codex|o\d)[\w.\-\s]*$/i.test(item)) return "";
    if (/文件已更改|正在思考|要求后续变更|完全访问|环境信息|提交或推送|暂无来源/.test(item)) return "";
    if (/^(打开位置|打开图片|审查)$/.test(item)) return "";
    if (/^(引导|删除|更多)$/.test(item)) return "";
    return item;
  }

  function uniqueRows(values) {
    const rows = [];
    for (const value of values) {
      const item = normalizeItem(value?.text ?? value);
      if (!item) continue;
      if (rows.some((existing) => existing.text === item || existing.text.includes(item) || item.includes(existing.text))) continue;
      rows.push({
        text: item,
        status: value?.status || "pending",
        iconHtml: value?.iconHtml || "",
        textColor: value?.textColor || "",
        iconColor: value?.iconColor || "",
      });
      if (rows.length >= 12) break;
    }
    return rows;
  }

  function rowsForSnapshot(snapshot) {
    const match = String(snapshot?.progress || "").match(/第\s*(\d+)\s*\/\s*\d+\s*步/);
    const activeIndex = match ? Math.max(0, Number(match[1]) - 1) : -1;
    const sourceRows = Array.isArray(snapshot?.rows) && snapshot.rows.length
      ? snapshot.rows
      : (Array.isArray(snapshot?.items) ? snapshot.items.map((text, index) => ({
          text,
          status: activeIndex < 0 ? "pending" : (index < activeIndex ? "done" : (index === activeIndex ? "running" : "pending")),
          iconHtml: "",
          textColor: "",
          iconColor: "",
        })) : []);
    return uniqueRows(sourceRows);
  }

  function rowsSettled(rows) {
    return Array.isArray(rows) && rows.length && rows.every((row) => row.status === "done");
  }

  function snapshotAgeMs(snapshot) {
    const at = Number(snapshot?.at || 0);
    return at > 0 ? Date.now() - at : Number.POSITIVE_INFINITY;
  }

  function progressComplete(progress) {
    const match = String(progress || "").match(/第\s*(\d+)\s*\/\s*(\d+)\s*步/);
    return !!match && Number(match[1]) >= Number(match[2]);
  }

  function nativeSourceMissingMs(threadId, snapshot) {
    const lastSeen = lastNativePillSeenAt(threadId);
    if (lastSeen > 0) return Date.now() - lastSeen;
    return snapshotAgeMs(snapshot);
  }

  function shouldSettleMissingNativeSnapshot(threadId, snapshot, rows) {
    if (!Array.isArray(rows) || !rows.length || rowsSettled(rows) || appBusy()) return false;
    const missingMs = nativeSourceMissingMs(threadId, snapshot);
    const settleAfterMs = progressComplete(snapshot?.progress) ? COMPLETE_SETTLE_MS : STALE_RUNNING_SETTLE_MS;
    return missingMs > settleAfterMs && snapshotAgeMs(snapshot) > settleAfterMs;
  }

  function progressNumbers(progress) {
    const match = String(progress || "").match(/第\s*(\d+)\s*\/\s*(\d+)\s*步/);
    if (!match) return null;
    return { current: Number(match[1]), total: Number(match[2]) };
  }

  function retargetRowsForProgress(rows, progress) {
    const numbers = progressNumbers(progress);
    if (!numbers || !Array.isArray(rows) || !rows.length) return rows;
    return rows.map((row, index) => ({
      ...row,
      status: index < numbers.current - 1 ? "done" : (index === numbers.current - 1 ? "running" : "pending"),
      iconHtml: "",
    }));
  }

  function rowsCompatibleWithProgress(rows, progress, previousProgress) {
    const next = progressNumbers(progress);
    if (!next || !Array.isArray(rows) || !rows.length) return false;
    const previous = progressNumbers(previousProgress);
    if (previous && previous.total !== next.total) return false;
    return rows.length === next.total;
  }

  function statusForTodoPlanStatus(status) {
    const normalized = String(status || "").replace(/[\s_-]+/g, "").toLowerCase();
    if (normalized === "completed" || normalized === "complete" || normalized === "done") return "done";
    if (normalized === "inprogress" || normalized === "running" || normalized === "active") return "running";
    return "pending";
  }

  function progressFromRows(rows) {
    if (!Array.isArray(rows) || !rows.length) return "";
    const runningIndex = rows.findIndex((row) => row.status === "running");
    if (runningIndex >= 0) return `第 ${runningIndex + 1} / ${rows.length} 步`;
    const doneCount = rows.filter((row) => row.status === "done").length;
    return `第 ${Math.max(1, doneCount || 1)} / ${rows.length} 步`;
  }

  function normalizedProgressForRows(snapshot, rows) {
    const rowProgress = progressFromRows(rows);
    if (!rowProgress) return String(snapshot?.progress || "");
    const existingProgress = String(snapshot?.progress || "");
    const existingNumbers = progressNumbers(existingProgress);
    if (existingNumbers && existingNumbers.total !== rows.length) return existingProgress;
    const hasRowSignal = rows.some((row) => row.status === "done" || row.status === "running");
    return hasRowSignal ? rowProgress : String(snapshot?.progress || rowProgress);
  }

  function normalizedSnapshotForRows(snapshot, rows = rowsForSnapshot(snapshot)) {
    if (!snapshot || typeof snapshot !== "object") return snapshot;
    if (!Array.isArray(rows) || !rows.length) return snapshot;
    return {
      ...snapshot,
      progress: normalizedProgressForRows(snapshot, rows),
      items: rows.map((row) => row.text),
      rows,
    };
  }

  function reactFiberForElement(el) {
    if (!el) return null;
    const key = Object.getOwnPropertyNames(el).find((name) => name.startsWith("__reactFiber$"));
    return key ? el[key] : null;
  }

  function turnInfoFromProps(props) {
    if (!props || typeof props !== "object") return null;
    const candidates = [
      props.mcpTurn,
      props.turn,
      props.entry?.turn,
    ];
    for (const turn of candidates) {
      if (!turn || !Array.isArray(turn.items)) continue;
      const params = turn.params && typeof turn.params === "object" ? turn.params : {};
      const entry = props.entry && typeof props.entry === "object" ? props.entry : {};
      return {
        turn,
        conversationId: String(props.conversationId || entry.conversationId || params.threadId || ""),
        hostId: String(props.hostId || entry.hostId || params.hostId || ""),
        turnId: String(turn.turnId || props.turnId || props.turnSearchKey || ""),
        status: String(turn.status || props.turnState?.status || ""),
      };
    }
    return null;
  }

  function nativeTurnFromPill(pill) {
    const seenFibers = new Set();
    let node = pill;
    for (let nodeDepth = 0; node && nodeDepth < 8; nodeDepth += 1, node = node.parentElement) {
      let fiber = reactFiberForElement(node);
      for (let fiberDepth = 0; fiber && fiberDepth < 36; fiberDepth += 1, fiber = fiber.return) {
        if (seenFibers.has(fiber)) continue;
        seenFibers.add(fiber);
        const turnInfo = turnInfoFromProps(fiber.memoizedProps || {});
        if (turnInfo) return turnInfo;
      }
    }
    return null;
  }

  function latestTodoFromTurn(turnInfo) {
    const items = Array.isArray(turnInfo?.turn?.items) ? turnInfo.turn.items : [];
    for (let index = items.length - 1; index >= 0; index -= 1) {
      const item = items[index];
      if (item?.type !== "todo-list" || !Array.isArray(item.plan)) continue;
      const rows = uniqueRows(item.plan.map((row) => ({
        text: row?.step || row?.text || row?.title || "",
        status: statusForTodoPlanStatus(row?.status),
        iconHtml: "",
        textColor: "",
        iconColor: "",
      })));
      // plan 数组本身为空说明模型显式调用了 update_plan([]) 清空任务；
      // 直接返回这个最新条目（哪怕 rows 是空的），不要继续往前找更早的历史快照。
      return { item, rows, cleared: item.plan.length === 0 };
    }
    return null;
  }

  function todoSnapshotFromNativeTurn(base, threadId, turnInfo) {
    const latest = latestTodoFromTurn(turnInfo);
    if (!latest) return null;
    if (!latest.rows.length) {
      // rows 为空但 plan 本身不是空数组：说明是解析/过滤异常，不代表清空，交给外部兜底路径。
      if (!latest.cleared) return null;
      // 模型显式清空了任务清单，说明这一轮任务已完成；
      // 把上一次记住的行全部结算为完成态，而不是回退显示清空前的旧进度（例如仍卡在"执行中"）。
      const previous = snapshotForThread(threadId);
      const previousRows = rowsForSnapshot(previous);
      if (!previousRows.length) return null;
      const settledRows = previousRows.map((row) => ({ ...row, status: "done", iconHtml: "" }));
      return {
        ...base,
        threadId,
        sourceConversationId: turnInfo?.conversationId || "",
        sourceTurnId: turnInfo?.turnId || "",
        sourceTodoId: String(latest.item?.id || ""),
        progress: normalizedProgressForRows({ progress: base.progress || previous?.progress || "" }, settledRows) || "任务清单",
        items: settledRows.map((row) => row.text),
        rows: settledRows,
        pendingRefresh: false,
        at: Date.now(),
        source: "native-turn-todo-cleared",
      };
    }
    const rows = latest.rows;
    return {
      ...base,
      threadId,
      sourceConversationId: turnInfo?.conversationId || "",
      sourceTurnId: turnInfo?.turnId || "",
      sourceTodoId: String(latest.item?.id || ""),
      progress: normalizedProgressForRows(base, rows) || "任务清单",
      items: rows.map((row) => row.text),
      rows,
      pendingRefresh: false,
      at: Date.now(),
      source: "native-turn-todo",
    };
  }

  function snapshotContextMatches(snapshot, base) {
    const currentTurnId = String(base?.sourceTurnId || "");
    const snapshotTurnId = String(snapshot?.sourceTurnId || "");
    if (currentTurnId) return snapshotTurnId === currentTurnId;
    const currentConversationId = String(base?.sourceConversationId || "");
    const snapshotConversationId = String(snapshot?.sourceConversationId || "");
    if (currentConversationId && snapshotConversationId && currentConversationId !== snapshotConversationId) return false;
    return true;
  }

  function settledSnapshot(snapshot, source) {
    const rows = rowsForSnapshot(snapshot);
    if (!rows.length) return null;
    const settledRows = rows.map((row) => ({
      ...row,
      status: "done",
      iconHtml: "",
    }));
    return {
      ...snapshot,
      items: settledRows.map((row) => row.text),
      rows: settledRows,
      at: Date.now(),
      source,
    };
  }

  function metaPartsFromPill(pill, progress, detail) {
    return metaPartsFromProgressDetail(progress, detail, (part) => colorForToken(pill, part));
  }

  function metaPartsFromSnapshot(snapshot) {
    return metaPartsFromProgressDetail(
      snapshot?.progress || "",
      snapshot?.detail || "",
      (part) => existingMetaColor(snapshot, part)
    );
  }

  function environmentChangeDetail() {
    const panel = environmentPanelInfo()?.node;
    if (!panel) return "";
    return extractFileChangeDetail(text(panel));
  }

  function renderSnapshotForRows(snapshot, rows) {
    const normalized = normalizedSnapshotForRows(snapshot, rows);
    if (!normalized || typeof normalized !== "object") return normalized;
    const detail = environmentChangeDetail() || normalized.detail || "";
    const next = { ...normalized, detail };
    return { ...next, metaParts: metaPartsFromSnapshot(next) };
  }

  function snapshotFromExternal(base, threadId) {
    const snapshot = window[SNAPSHOT_KEY];
    if (!snapshot || !Array.isArray(snapshot.items) || !snapshot.items.length) return null;
    if (snapshot.threadId && threadId && snapshot.threadId !== threadId) return null;
    if (snapshot.sourceTurnId && base.sourceTurnId && snapshot.sourceTurnId !== base.sourceTurnId) return null;
    if (!snapshot.sourceTurnId && base.sourceTurnId && Date.now() - Number(snapshot.at || 0) > 3_000) return null;
    if (Date.now() - Number(snapshot.at || 0) > 60_000) return null;
    const rows = rowsForSnapshot(snapshot);
    if (!rows.length) return null;
    let nextRows = rows;
    if (base.progress && snapshot.progress && base.progress !== snapshot.progress) {
      if (!rowsCompatibleWithProgress(rows, base.progress, snapshot.progress)) return null;
      nextRows = retargetRowsForProgress(rows, base.progress);
    }
    return {
      ...base,
      threadId,
      progress: normalizedProgressForRows(base.progress ? { progress: base.progress } : snapshot, nextRows) || "任务清单",
      detail: snapshot.detail || base.detail || "",
      sourceConversationId: base.sourceConversationId || snapshot.sourceConversationId || "",
      sourceTurnId: base.sourceTurnId || snapshot.sourceTurnId || "",
      sourceTodoId: base.sourceTodoId || snapshot.sourceTodoId || "",
      items: nextRows.map((row) => row.text),
      rows: nextRows,
      at: snapshot.at || Date.now(),
      pendingRefresh: base.progress && snapshot.progress && base.progress !== snapshot.progress,
      source: base.progress && snapshot.progress && base.progress !== snapshot.progress ? "external-retargeted" : (snapshot.source || "external"),
    };
  }

  function readSnapshot() {
    const threadId = currentThreadId();
    if (state.lastThreadId !== threadId) {
      state.lastThreadId = threadId;
      state.lastSnapshot = snapshotForThread(threadId);
    }
    const previousSnapshot = snapshotForThread(threadId);
    const pills = nativePills();
    const pill = pills[0] || null;
    const pillText = nativePillText(pill);
    if (!pill) {
      const lastRows = rowsForSnapshot(previousSnapshot);
      if (!lastRows.length) return null;
      if (shouldSettleMissingNativeSnapshot(threadId, previousSnapshot, lastRows)) {
        return settledSnapshot(previousSnapshot, progressComplete(previousSnapshot?.progress) ? "native-source-gone" : "stale-native-source-gone");
      }
      return previousSnapshot;
    }
    markNativePillSeen(threadId);
    const pillParts = parseNativePillValue(pillText);
    const progress = pillParts?.progress || previousSnapshot?.progress || "任务清单";
    const detail = pillParts?.detail || nearbyPillDetail(pill, progress);
    const turnInfo = nativeTurnFromPill(pill);
    const base = {
      threadId,
      progress,
      detail,
      sourceConversationId: turnInfo?.conversationId || "",
      sourceTurnId: turnInfo?.turnId || "",
      items: [],
      at: Date.now(),
    };
    base.metaParts = metaPartsFromPill(pill, progress, base.detail);
    const nativeTurn = todoSnapshotFromNativeTurn(base, threadId, turnInfo);
    if (nativeTurn) return nativeTurn;
    const external = snapshotFromExternal(base, threadId);
    if (external) return external;
    if (previousSnapshot?.items?.length && snapshotContextMatches(previousSnapshot, base)) {
      const rows = rowsForSnapshot(previousSnapshot);
      const sameProgress = !previousSnapshot.progress || previousSnapshot.progress === progress;
      const compatibleProgress = rowsCompatibleWithProgress(rows, progress, previousSnapshot.progress);
      if (rows.length) {
        const freshEnough = rowsSettled(rows) || snapshotAgeMs(previousSnapshot) < 5_000;
        const nextRows = compatibleProgress ? retargetRowsForProgress(rows, progress) : rows;
        return {
          ...base,
          detail: base.detail || previousSnapshot.detail || "",
          items: nextRows.map((row) => row.text),
          rows: nextRows,
          pendingRefresh: previousSnapshot.progress !== progress || previousSnapshot.detail !== base.detail || !freshEnough,
          source: sameProgress ? "last-snapshot-pending" : (compatibleProgress ? "retargeted-last-snapshot" : "last-snapshot-awaiting-current-tooltip"),
        };
      }
    }
    return {
      ...base,
      pendingRefresh: true,
      source: base.sourceTurnId ? "awaiting-current-todo" : "awaiting-current-tooltip",
    };
  }

  function ensureDock() {
    let dock = document.getElementById(DOCK_ID);
    if (!dock) {
      dock = document.createElement("div");
      dock.id = DOCK_ID;
      dock.setAttribute(MARK, "dock");
      dock.hidden = true;
      document.documentElement.appendChild(dock);
    }
    return dock;
  }

  function iconForStatus(status) {
    if (status === "done") {
      return `<svg viewBox="0 0 20 20" aria-hidden="true"><path d="M12.16 7.14a.67.67 0 0 1 .93-.16.67.67 0 0 1 .16.93l-3.96 5.62a.66.66 0 0 1-.5.28.67.67 0 0 1-.53-.22l-2.08-2.29a.67.67 0 1 1 .99-.9l1.51 1.67 3.48-4.93Z" fill="currentColor"/><path fill-rule="evenodd" clip-rule="evenodd" d="M10 2.08a7.92 7.92 0 1 1 0 15.84 7.92 7.92 0 0 1 0-15.84Zm0 1.33a6.59 6.59 0 1 0 0 13.18 6.59 6.59 0 0 0 0-13.18Z" fill="currentColor"/></svg>`;
    }
    if (status === "running") {
      return `<svg class="cgl-plan-spinner" viewBox="0 0 24 24" aria-hidden="true"><path opacity="0.28" d="M18 12a6 6 0 1 0-6 6 6 6 0 0 0 6-6Zm2 0a8 8 0 1 1-8-8 8 8 0 0 1 8 8Z" fill="currentColor"/><path d="M12 4a8 8 0 0 1 8 8h-2a6 6 0 0 0-6-6V4Z" fill="currentColor"/></svg>`;
    }
    return `<svg viewBox="0 0 20 20" aria-hidden="true"><path fill-rule="evenodd" clip-rule="evenodd" d="M10 2.08a7.92 7.92 0 1 1 0 15.84 7.92 7.92 0 0 1 0-15.84Zm0 1.33a6.59 6.59 0 1 0 0 13.18 6.59 6.59 0 0 0 0-13.18Z" fill="currentColor"/></svg>`;
  }

  function environmentPanelInfo() {
    if (blockingOverlayActive()) return null;
    const candidates = Array.from(document.querySelectorAll("aside,section,div"))
      .filter((node) => {
        if (!visible(node) || managedNode(node)) return false;
        const value = text(node);
        if (!/环境信息|Environment/.test(value) || !/变更|Changes/.test(value)) return false;
        if (!/本地|Local/.test(value) || !/提交或推送|Commit|Push/.test(value)) return false;
        if (value.length > 360) return false;
        if (/function|querySelector|const\s|return\s|=>|PLAN_UI_SCRIPT|codex-gateway-lite/.test(value)) return false;
        const rect = node.getBoundingClientRect();
        return rect.width >= 220
          && rect.width <= 520
          && rect.height >= 120
          && rect.height <= Math.min(640, window.innerHeight * 0.72)
          && rect.right > window.innerWidth * 0.55;
      })
      .sort((a, b) => {
        const ar = a.getBoundingClientRect();
        const br = b.getBoundingClientRect();
        const rightDelta = Math.abs(br.right - ar.right);
        if (rightDelta > 16) return br.right - ar.right;
        return br.bottom - ar.bottom;
      });
    const panel = candidates[0] || null;
    if (!panel) return null;
    const rect = panel.getBoundingClientRect();
    if (!topLayerOwns(panel, rect)) return null;
    let contentBottom = rect.top;
    Array.from(panel.querySelectorAll("*")).forEach((node) => {
      if (!visible(node) || managedNode(node)) return;
      const value = text(node);
      if (!value || value.length > 80) return;
      if (!/环境信息|Environment|变更|Changes|本地|main|提交或推送|来源|Source|暂无来源/.test(value)) return;
      const childRect = node.getBoundingClientRect();
      contentBottom = Math.max(contentBottom, childRect.bottom);
    });
    return { node: panel, rect, contentBottom: Math.min(Math.max(contentBottom, rect.top), rect.bottom) };
  }

  function rightRailFallbackPanelInfo() {
    if (blockingOverlayActive()) return null;
    const candidates = Array.from(document.querySelectorAll("aside,section,div"))
      .map((node) => {
        if (!visible(node) || managedNode(node)) return null;
        const value = text(node);
        const hasOutput = /输出|Output|产物|Artifacts?|暂无产物|No artifacts?|No output/i.test(value);
        const hasSource = /来源|Source|暂无来源|No sources?/i.test(value);
        const hasOutputSourcePair = hasOutput && hasSource;
        const looksLikeOutputPanel = hasOutputSourcePair || hasOutput || hasSource;
        if (!looksLikeOutputPanel) return null;
        const rect = node.getBoundingClientRect();
        const smallRightRailPanel = rect.width >= 220
          && rect.width <= 520
          && rect.height >= 56
          && rect.height <= Math.min(560, window.innerHeight * 0.72)
          && rect.right > window.innerWidth * 0.55
          && rect.top < window.innerHeight * 0.72;
        if (!smallRightRailPanel) return null;
        if (!hasOutputSourcePair) {
          if (value.length > 260) return null;
          if (/function|querySelector|const\s|return\s|=>|PLAN_UI_SCRIPT/.test(value)) return null;
        }
        return { node, rect, hasOutputSourcePair };
      })
      .filter(Boolean)
      .sort((a, b) => {
        const pairDelta = Number(b.hasOutputSourcePair) - Number(a.hasOutputSourcePair);
        if (pairDelta) return pairDelta;
        const topDelta = a.rect.top - b.rect.top;
        if (Math.abs(topDelta) > 12) return topDelta;
        const heightDelta = b.rect.height - a.rect.height;
        if (Math.abs(heightDelta) > 12) return heightDelta;
        return b.rect.right - a.rect.right;
      });
    const candidate = candidates[0] || null;
    if (!candidate) return null;
    const panel = candidate.node;
    const rect = candidate.rect;
    if (!topLayerOwns(panel, rect)) return null;
    let contentBottom = candidate.hasOutputSourcePair ? rect.bottom : rect.top;
    Array.from(panel.querySelectorAll("*")).forEach((node) => {
      if (!visible(node) || managedNode(node)) return;
      const value = text(node);
      if (!value || value.length > 260) return;
      if (!/输出|Output|产物|Artifact|暂无产物|No artifact|No output|来源|Source|暂无来源|No source|任务|Task/.test(value)) return;
      const childRect = node.getBoundingClientRect();
      if (childRect.right < window.innerWidth * 0.55) return;
      contentBottom = Math.max(contentBottom, childRect.bottom);
    });
    return { rect, contentBottom: Math.min(Math.max(contentBottom, rect.top), rect.bottom) };
  }

  function placeDock(dock) {
    const anchor = environmentPanelInfo();
    if (!anchor) return false;
    const rect = anchor.rect;
    const right = Math.max(12, Math.round(window.innerWidth - rect.right + 12));
    const width = Math.max(220, Math.min(360, Math.round(rect.width - 12)));
    const anchorBottom = Number(anchor.contentBottom || rect.bottom || 90);
    const top = Math.min(
      Math.max(72, Math.round(anchorBottom + 12)),
      Math.max(72, window.innerHeight - 96)
    );
    setDockVar(dock, "--cgl-plan-top", `${top}px`);
    setDockVar(dock, "--cgl-plan-right", `${right}px`);
    setDockVar(dock, "--cgl-plan-width", `${width}px`);
    return true;
  }

  function setDockVar(dock, name, value) {
    if (dock.style.getPropertyValue(name) !== value) {
      dock.style.setProperty(name, value);
    }
  }

  function aggregatePlanStatus(rows) {
    if (!rows.length) return "running";
    if (rows.some((row) => row.status === "running")) return "running";
    if (rows.every((row) => row.status === "done")) return "done";
    return "pending";
  }

  function renderMeta(snapshot, rows) {
    const parts = Array.isArray(snapshot?.metaParts) ? snapshot.metaParts : [];
    const statusIcon = snapshot?.progress ? `<span class="cgl-plan-meta-spinner">${iconForStatus(aggregatePlanStatus(rows))}</span>` : "";
    if (parts.length) {
      return `<div class="cgl-plan-meta">${statusIcon}<span class="cgl-plan-meta-content">${parts.map((part) => (
        `<span style="${colorStyle(part.color)}">${escapeHtml(part.text)}</span>`
      )).join("")}</span></div>`;
    }
    const meta = [snapshot.progress, snapshot.detail].filter(Boolean).join(" · ");
    return meta ? `<div class="cgl-plan-meta">${statusIcon}<span class="cgl-plan-meta-content">${escapeHtml(meta)}</span></div>` : "";
  }

  function renderSignature(snapshot, rows) {
    const metaParts = Array.isArray(snapshot?.metaParts)
      ? snapshot.metaParts.map((part) => ({ text: part.text || "", color: safeCssColor(part.color) }))
      : [];
    return JSON.stringify({
      sourceConversationId: snapshot?.sourceConversationId || "",
      sourceTurnId: snapshot?.sourceTurnId || "",
      sourceTodoId: snapshot?.sourceTodoId || "",
      progress: snapshot?.progress || "",
      detail: snapshot?.detail || "",
      metaParts,
      rows: rows.map((row) => ({
        text: row.text || "",
        status: row.status || "",
        iconHtml: row.iconHtml || "",
        textColor: safeCssColor(row.textColor),
        iconColor: safeCssColor(row.iconColor),
      })),
    });
  }

  function renderDock(snapshot) {
    const dock = ensureDock();
    if (snapshot?.threadId && snapshot.threadId !== currentThreadId()) {
      dock.hidden = true;
      state.lastRenderSignature = "";
      dock.dataset.cglPlanSignature = "";
      dock.dataset.cglPlanSourceTurnId = "";
      return;
    }
    if (blockingOverlayActive()) {
      dock.hidden = true;
      dock.dataset.cglPlanHiddenBy = "blocking-overlay";
      state.lastRenderSignature = "";
      dock.dataset.cglPlanSignature = "";
      dock.dataset.cglPlanSourceTurnId = "";
      return;
    }
    if (rightPanelDismissalActive()) {
      dock.hidden = true;
      dock.dataset.cglPlanHiddenBy = "right-panel-active";
      state.lastRenderSignature = "";
      dock.dataset.cglPlanSignature = "";
      dock.dataset.cglPlanSourceTurnId = "";
      return;
    }
    if (!snapshot) {
      dock.hidden = true;
      state.lastRenderSignature = "";
      dock.dataset.cglPlanSignature = "";
      dock.dataset.cglPlanSourceTurnId = "";
      dock.innerHTML = "";
      return;
    }
    if (!placeDock(dock)) {
      dock.hidden = true;
      dock.dataset.cglPlanHiddenBy = "no-native-anchor";
      state.lastRenderSignature = "";
      dock.dataset.cglPlanSignature = "";
      dock.dataset.cglPlanSourceTurnId = "";
      return;
    }
    const rows = rowsForSnapshot(snapshot);
    const normalizedSnapshot = renderSnapshotForRows(snapshot, rows);
    const signature = renderSignature(normalizedSnapshot, rows);
    const sourceTurnId = normalizedSnapshot?.sourceTurnId || "";
    dock.hidden = false;
    delete dock.dataset.cglPlanHiddenBy;
    if (dock.dataset.cglPlanSignature === signature) return;
    if (!rows.length && dock.dataset.cglPlanSignature && dock.innerHTML.trim()) {
      const sameSourceTurn = sourceTurnId && dock.dataset.cglPlanSourceTurnId === sourceTurnId;
      if (sameSourceTurn) return;
    }
    dock.dataset.cglPlanSignature = signature;
    dock.dataset.cglPlanSourceTurnId = sourceTurnId;
    state.lastRenderSignature = signature;
    dock.innerHTML = `
      ${renderMeta(normalizedSnapshot, rows)}
      ${
        rows.length
          ? `<div class="cgl-plan-list">${rows.map((row) => `
              <div class="cgl-plan-item is-${escapeHtml(row.status || "pending")}">
                <span class="cgl-plan-icon" style="${colorStyle(row.iconColor)}">${safeIconHtml(row.iconHtml) || iconForStatus(row.status)}</span>
                <span class="cgl-plan-text" style="${colorStyle(row.textColor)}">${escapeHtml(row.text)}</span>
              </div>
            `).join("")}</div>`
          : `<div class="cgl-plan-empty">暂无详情</div>`
      }
    `;
  }

  function markNativePills() {
    const threadId = currentThreadId();
    const pills = nativePills().slice(0, 1);
    const pillSet = new Set(pills);
    document.querySelectorAll(`[${SOURCE_ATTR}="true"]`).forEach((node) => {
      if (!pillSet.has(node)) {
        node.removeAttribute(SOURCE_ATTR);
        node.removeAttribute("data-codex-gateway-lite-native-pill-thread-id");
      }
    });
    pills.forEach((node) => {
      node.setAttribute(SOURCE_ATTR, "true");
      node.setAttribute("data-codex-gateway-lite-native-pill-thread-id", threadId);
    });
  }

  function apply() {
    installStyle();
    cleanupLegacyMarks();
    const threadId = currentThreadId();
    const snapshot = readSnapshot();
    if (snapshot) {
      state.lastSourceSeenAt = Date.now();
      state.lastSnapshot = snapshot;
      rememberThreadSnapshot(snapshot);
    }
    markNativePills();
    renderDock(snapshot || snapshotForThread(threadId));
  }

  function activeHistorySeedRequest() {
    const threadId = activeSeedThreadId();
    if (!threadId) {
      return { threadId: "", needsSeed: false, reason: "no-thread" };
    }
    const snapshot = directSnapshotForSeed(threadId);
    const rows = rowsForSnapshot(snapshot);
    return {
      threadId,
      needsSeed: rows.length === 0,
      reason: rows.length ? "has-direct-snapshot" : "missing-direct-snapshot",
      rows: rows.length,
    };
  }

  const APPLY_THROTTLE_MS = 750;
  let applyThrottleTimer = 0;
  let lastApplyAt = 0;

  function runApplyOnNextFrame() {
    window.__codexGatewayLitePlanUiFrame = requestAnimationFrame(() => {
      window.__codexGatewayLitePlanUiFrame = 0;
      lastApplyAt = Date.now();
      apply();
      continueRightRailFollow();
    });
  }

  function scheduleApplyImmediate() {
    if (applyThrottleTimer) {
      window.clearTimeout(applyThrottleTimer);
      applyThrottleTimer = 0;
    }
    if (window.__codexGatewayLitePlanUiFrame) return;
    runApplyOnNextFrame();
  }

  function scheduleApplyBurst() {
    const until = Date.now() + RIGHT_RAIL_FOLLOW_FRAME_MS;
    state.rightRailFollowUntil = Math.max(Number(state.rightRailFollowUntil || 0), until);
    scheduleApplyImmediate();
  }

  function continueRightRailFollow() {
    if (Date.now() >= Number(state.rightRailFollowUntil || 0)) return;
    scheduleApplyImmediate();
  }

  function eventTouchesRightRail(event) {
    const target = elementFromEventTarget(event?.target);
    let node = target;
    for (let depth = 0; node && node !== document.body && node !== document.documentElement && depth < 10; depth += 1) {
      if (managedNode(node)) return false;
      if (visible(node)) {
        const rect = node.getBoundingClientRect();
        if (rect.width > 12 && rect.height > 12 && rect.right > window.innerWidth * 0.55) return true;
      }
      node = node.parentElement;
    }
    return !!rightSideExpandedContentInfoFromPoint(event?.clientX, event?.clientY);
  }

  function mutationTouchesRightRail(mutations) {
    return Array.from(mutations || []).some((mutation) => {
      const target = mutation.target;
      if (!target || target.nodeType !== 1 || managedNode(target)) return false;
      const rect = target.getBoundingClientRect?.();
      const nearRightRail = rect
        && rect.width > 16
        && rect.height > 16
        && rect.right > window.innerWidth * 0.55;
      if (!nearRightRail) return false;
      if (mutation.type === "attributes") {
        return true;
      }
      const value = text(target);
      return /环境信息|Environment|变更|Changes|本地|Local|来源|Source|输出|Output|任务|Task/.test(value);
    });
  }

  function scheduleApply() {
    // 真实页面上一次流式回复会在几秒内触发成百上千次 DOM 变更（MutationObserver
    // 监听了 childList/subtree/attributes），如果每次都同步跑一遍 apply()（内含
    // getBoundingClientRect/getComputedStyle 全量扫描），会把 renderer 进程的 CPU 打满。
    // 这里把连续触发合并到最多每 APPLY_THROTTLE_MS 执行一次。
    if (window.__codexGatewayLitePlanUiFrame || applyThrottleTimer) return;
    const elapsed = Date.now() - lastApplyAt;
    if (elapsed >= APPLY_THROTTLE_MS) {
      runApplyOnNextFrame();
      return;
    }
    applyThrottleTimer = window.setTimeout(() => {
      applyThrottleTimer = 0;
      runApplyOnNextFrame();
    }, APPLY_THROTTLE_MS - elapsed);
  }

  function scheduleApplyForMutations(mutations) {
    if (mutationTouchesRightRail(mutations)) {
      scheduleApplyBurst();
      return;
    }
    scheduleApply();
  }

  if (window.__codexGatewayLitePlanUiObserver) {
    try { window.__codexGatewayLitePlanUiObserver.disconnect(); } catch {}
  }
  if (window.__codexGatewayLitePlanUiResize) {
    try { window.removeEventListener("resize", window.__codexGatewayLitePlanUiResize); } catch {}
  }
  if (window.__codexGatewayLitePlanUiScroll) {
    try { window.removeEventListener("scroll", window.__codexGatewayLitePlanUiScroll, true); } catch {}
  }
  if (window.__codexGatewayLitePlanUiRightPanelDismiss) {
    try { window.removeEventListener("pointerdown", window.__codexGatewayLitePlanUiRightPanelDismiss, true); } catch {}
    try { window.removeEventListener("click", window.__codexGatewayLitePlanUiRightPanelDismiss, true); } catch {}
    try { window.removeEventListener("transitionrun", window.__codexGatewayLitePlanUiRightPanelDismiss, true); } catch {}
    try { window.removeEventListener("transitionstart", window.__codexGatewayLitePlanUiRightPanelDismiss, true); } catch {}
    try { window.removeEventListener("transitioncancel", window.__codexGatewayLitePlanUiRightPanelDismiss, true); } catch {}
    try { window.removeEventListener("transitionend", window.__codexGatewayLitePlanUiRightPanelDismiss, true); } catch {}
    try { window.removeEventListener("animationstart", window.__codexGatewayLitePlanUiRightPanelDismiss, true); } catch {}
    try { window.removeEventListener("animationend", window.__codexGatewayLitePlanUiRightPanelDismiss, true); } catch {}
  }
  if (window.__codexGatewayLitePlanUiTimer) {
    try { window.clearInterval(window.__codexGatewayLitePlanUiTimer); } catch {}
  }
  apply();
  const observer = new MutationObserver(scheduleApplyForMutations);
  observer.observe(document.documentElement, { childList: true, subtree: true, attributes: true, attributeFilter: ["style", "class", "data-state", "hidden", "role", "aria-modal"] });
  window.__codexGatewayLitePlanUiObserver = observer;
  window.__codexGatewayLitePlanUiResize = scheduleApplyImmediate;
  window.__codexGatewayLitePlanUiScroll = scheduleApply;
  window.__codexGatewayLitePlanUiRightPanelDismiss = handleRightPanelDismissEvent;
  window.__codexGatewayLitePlanUiTimer = window.setInterval(scheduleApply, 5000);
  window.addEventListener("resize", scheduleApplyImmediate, { passive: true });
  window.addEventListener("scroll", scheduleApply, { passive: true, capture: true });
  window.addEventListener("pointerdown", handleRightPanelDismissEvent, { passive: true, capture: true });
  window.addEventListener("click", handleRightPanelDismissEvent, { passive: true, capture: true });
  window.addEventListener("transitionrun", handleRightPanelDismissEvent, { passive: true, capture: true });
  window.addEventListener("transitionstart", handleRightPanelDismissEvent, { passive: true, capture: true });
  window.addEventListener("transitioncancel", handleRightPanelDismissEvent, { passive: true, capture: true });
  window.addEventListener("transitionend", handleRightPanelDismissEvent, { passive: true, capture: true });
  window.addEventListener("animationstart", handleRightPanelDismissEvent, { passive: true, capture: true });
  window.addEventListener("animationend", handleRightPanelDismissEvent, { passive: true, capture: true });
  window.__codexGatewayLitePlanUiApply = apply;
  window.__codexGatewayLitePlanUiActiveSeedRequest = activeHistorySeedRequest;
  if (typeof globalThis.__CODEX_GATEWAY_LITE_TEST__ !== "undefined") {
    // 仅测试环境暴露的内部纯函数钩子；真实 Codex App 里不会定义
    // globalThis.__CODEX_GATEWAY_LITE_TEST__，这段代码是死代码，不会执行。
    window.__codexGatewayLitePlanUiTestHooks = {
      latestTodoFromTurn,
      todoSnapshotFromNativeTurn,
      statusForTodoPlanStatus,
      normalizeItem,
      uniqueRows,
      snapshotContextMatches,
      progressFromRows,
      normalizedProgressForRows,
      normalizedSnapshotForRows,
      rowsForSnapshot,
      turnInfoFromProps,
      currentThreadId,
      activeSeedThreadId,
      currentConversationThreadId,
      currentVisibleTitleThreadId,
      currentSidebarThreadId,
      currentSidebarThreadInfo,
      knownSnapshotForThreadId,
      snapshotAliasIdsForThread,
      snapshotForThread,
      directSnapshotForSeed,
      externalSnapshotForThread,
      rightSideExpandedContentRect,
      rightSideExpandedContentSignature,
      rightSideExpandedContentInfoFromPoint,
      activeRightSideExpandedContentInfo,
      hideDockForRightPanelTarget,
      rightPanelDismissalActive,
      renderDock,
      scheduleApply,
      activeHistorySeedRequest,
      scheduleApplyImmediate,
      scheduleApplyBurst,
      eventTouchesRightRail,
      mutationTouchesRightRail,
      scheduleApplyForMutations,
    };
  }
  return true;
})()
"#;

fn print_help() {
    println!(
        r#"codex-gateway-lite

用法：
  codex-gateway-lite apply --config <config.json> [--codex-home <dir>] [--reload] [--debug-port 9229] [--no-plan-ui]
  codex-gateway-lite doctor --config <config.json>
  codex-gateway-lite reload [--debug-port 9229] [--no-plan-ui]
  codex-gateway-lite inject-plan-ui [--debug-port 9229]
  codex-gateway-lite watch --config <config.json> [--codex-home <dir>] [--debug-port 9229] [--interval-ms 1200]
  codex-gateway-lite agent [--config ~/.codex-gateway-lite/config.json] [--codex-home <dir>] [--app <Codex.app|Codex.exe|app dir>] [--debug-port 9229] [--interval-ms 1000] [--no-plan-ui]
  codex-gateway-lite launch [--config <config.json>] [--codex-home <dir>] [--app <Codex.app|Codex.exe|app dir>] [--debug-port 9229] [--no-plan-ui]
  codex-gateway-lite install-agent [--config ~/.codex-gateway-lite/config.json] [--codex-home <dir>] [--app <Codex.app|Codex.exe|app dir>] [--debug-port 9229] [--interval-ms 1000] [--no-plan-ui]
  codex-gateway-lite stop-agent
  codex-gateway-lite uninstall-agent
  codex-gateway-lite init [--config ~/.codex-gateway-lite/config.json] [--force]
  codex-gateway-lite where-app [--app <Codex.app|Codex.exe|app dir>]

说明：
  apply   保留现有 ~/.codex/config.toml，合并 commonConfig，并更新模型/provider/catalog 相关字段
  doctor  用 /v1/models 验证 gateway 可访问性，不打印 API Key
  reload  通过 Codex CDP 端口触发 renderer 软刷新
  inject-plan-ui  不刷新页面，直接向当前 Codex renderer 注入任务清单常驻修正
  watch   监听配置变更，自动 apply + reload
  agent   常驻模式：启动 Codex、按需启动本地协议代理、监听配置、保活 CDP、持续重注入任务清单 UI
  launch  自动识别并用 remote-debugging-port 启动 Codex App；macOS 同步固定 CODEX_HOME 和 user-data-dir，可选先 apply 配置
  install-agent  macOS 写入 LaunchAgent；Windows 写入 Scheduled Task，让 agent 登录后保活
  stop-agent  停止当前用户会话里的 LaunchAgent/Scheduled Task 和旧 agent 进程，并清理 agent lock
  uninstall-agent  macOS 卸载 LaunchAgent；Windows 删除 Scheduled Task
  init    首次交互式配置 Base URL/API Key，自动同步 1M 模型列表，并从默认 Codex home 抽取公共配置
  where-app  打印自动识别到的 Codex App 路径
"#
    );
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn timestamp_to_seconds_accepts_seconds_and_milliseconds() {
        assert_eq!(timestamp_to_seconds(1_700_000_200.9), 1_700_000_200);
        assert_eq!(timestamp_to_seconds(1_700_000_200_999.0), 1_700_000_200);
        assert_eq!(timestamp_to_seconds(0.0), 0);
    }

    // 让 PLAN_UI_SCRIPT 这个 IIFE 能在没有真实浏览器的情况下跑到底所需要的最小 DOM/BOM
    // mock。只覆盖脚本安装阶段实际会调用到的那几个 API，够用即可，不追求完整实现。
    #[test]
    fn codex_launch_arguments_include_required_cdp_origin() {
        let args = codex_lite::build_codex_arguments(9229, &[]);
        assert_eq!(
            args,
            vec![
                "--remote-debugging-port=9229".to_string(),
                "--remote-allow-origins=http://127.0.0.1:9229".to_string(),
            ]
        );
        assert_eq!(
            codex_lite::command_line_arguments(&args),
            "--remote-debugging-port=9229 --remote-allow-origins=http://127.0.0.1:9229"
        );
    }

    #[test]
    fn windows_packaged_app_user_model_id_matches_store_package() {
        let app_dir = PathBuf::from(
            r"C:\Program Files\WindowsApps\OpenAI.Codex_26.506.2212.0_x64__2p2nqsd0c76g0\app",
        );
        assert_eq!(
            codex_lite::packaged_app_user_model_id(&app_dir).as_deref(),
            Some("OpenAI.Codex_2p2nqsd0c76g0!App")
        );
    }

    #[test]
    fn numbered_model_choice_selects_expected_model() {
        let models = vec![
            "model-a".to_string(),
            "model-b".to_string(),
            "model-c".to_string(),
        ];
        assert_eq!(
            model_from_numbered_choice(&models, "1").as_deref(),
            Some("model-a")
        );
        assert_eq!(
            model_from_numbered_choice(&models, " 3 ").as_deref(),
            Some("model-c")
        );
        assert_eq!(
            model_from_numbered_choice(&models, "").as_deref(),
            Some("model-a")
        );
        assert!(model_from_numbered_choice(&models, "0").is_none());
        assert!(model_from_numbered_choice(&models, "4").is_none());
        assert!(model_from_numbered_choice(&models, "model-b").is_none());
    }

    const PLAN_UI_MOCK_DOM_SCRIPT: &str = r#"
    class __MockElement {
      constructor(tagName) {
        this.tagName = String(tagName || "DIV").toUpperCase();
        this._attrs = {};
        this.style = { setProperty() {}, getPropertyValue() { return ""; } };
        this.dataset = {};
        this.children = [];
        this.parentElement = null;
        this.hidden = false;
        this.innerHTML = "";
        this.textContent = "";
        this.id = "";
        this.className = "";
      }
      getAttribute(name) { return Object.prototype.hasOwnProperty.call(this._attrs, name) ? this._attrs[name] : null; }
      setAttribute(name, value) { this._attrs[name] = String(value); }
      removeAttribute(name) { delete this._attrs[name]; }
      appendChild(child) {
        this.children.push(child);
        child.parentElement = this;
        if (child.id) { __elementsById.set(child.id, child); }
        return child;
      }
      addEventListener() {}
      removeEventListener() {}
      querySelectorAll() { return []; }
      querySelector() { return null; }
      closest() { return null; }
      contains() { return false; }
      matches() { return false; }
      getBoundingClientRect() { return { width: 0, height: 0, top: 0, left: 0, right: 0, bottom: 0 }; }
    }

    const __elementsById = new Map();
    const __documentElement = new __MockElement("html");
    const __body = new __MockElement("body");

    const document = {
      documentElement: __documentElement,
      body: __body,
      getElementById(id) { return __elementsById.get(id) || null; },
      createElement(tag) { return new __MockElement(tag); },
      querySelector() { return null; },
      querySelectorAll() { return []; },
      addEventListener() {},
      removeEventListener() {},
    };

    class __MockMutationObserver {
      observe() {}
      disconnect() {}
    }

    const __localStorageStore = {};
    const localStorage = {
      getItem(key) { return Object.prototype.hasOwnProperty.call(__localStorageStore, key) ? __localStorageStore[key] : null; },
      setItem(key, value) { __localStorageStore[key] = String(value); },
      removeItem(key) { delete __localStorageStore[key]; },
    };

    function getComputedStyle() {
      return {
        color: "", display: "block", visibility: "visible", opacity: "1",
        backgroundColor: "rgba(0,0,0,0)", boxShadow: "none", zIndex: "auto",
        position: "static", backdropFilter: "none",
        borderTopLeftRadius: "0px", borderTopRightRadius: "0px",
        borderBottomLeftRadius: "0px", borderBottomRightRadius: "0px",
        borderTopWidth: "0px", borderRightWidth: "0px", borderBottomWidth: "0px", borderLeftWidth: "0px",
      };
    }

    globalThis.window = globalThis;
    globalThis.document = document;
    globalThis.localStorage = localStorage;
    globalThis.MutationObserver = __MockMutationObserver;
    globalThis.innerWidth = 1440;
    globalThis.innerHeight = 900;
    globalThis.setInterval = function () { return 0; };
    globalThis.clearInterval = function () {};
    globalThis.setTimeout = function () { return 0; };
    globalThis.clearTimeout = function () {};
    globalThis.requestAnimationFrame = function () { return 0; };
    globalThis.getComputedStyle = getComputedStyle;
    globalThis.addEventListener = function () {};
    globalThis.removeEventListener = function () {};
    globalThis.__CODEX_GATEWAY_LITE_TEST__ = true;
    "#;

    fn plan_ui_test_context() -> boa_engine::Context {
        let mut ctx = boa_engine::Context::default();
        let setup = format!("{PLAN_UI_MOCK_DOM_SCRIPT}\n{PLAN_UI_SCRIPT}");
        ctx.eval(boa_engine::Source::from_bytes(&setup))
            .expect("PLAN_UI_SCRIPT should evaluate cleanly under the mock DOM");
        ctx
    }

    /// 调用 PLAN_UI_SCRIPT 暴露的测试钩子函数：入参/出参都走 JSON 序列化，
    /// 避免直接摆弄 boa 的 JsValue，贴近真实调用时“纯数据进、纯数据出”的用法。
    fn eval_json(ctx: &mut boa_engine::Context, expr: &str) -> serde_json::Value {
        let result = ctx
            .eval(boa_engine::Source::from_bytes(expr))
            .unwrap_or_else(|error| panic!("eval failed: {error}\nexpr: {expr}"));
        let json_text = result
            .to_string(ctx)
            .expect("result should stringify")
            .to_std_string_escaped();
        serde_json::from_str(&json_text)
            .unwrap_or_else(|error| panic!("result is not valid json: {error}: {json_text}"))
    }

    fn call_plan_ui_hook_in(
        ctx: &mut boa_engine::Context,
        hook: &str,
        args_json: &[&str],
    ) -> serde_json::Value {
        let args = args_json
            .iter()
            .map(|value| format!("JSON.parse({})", serde_json::to_string(value).unwrap()))
            .collect::<Vec<_>>()
            .join(", ");
        let call_expr = format!(
            "(() => {{ const r = window.__codexGatewayLitePlanUiTestHooks.{hook}({args}); return JSON.stringify(r === undefined ? null : r); }})()"
        );
        eval_json(ctx, &call_expr)
    }

    fn call_plan_ui_hook(hook: &str, args_json: &[&str]) -> serde_json::Value {
        let mut ctx = plan_ui_test_context();
        call_plan_ui_hook_in(&mut ctx, hook, args_json)
    }

    #[test]
    fn plan_ui_history_snapshot_seed_reads_latest_update_plan_from_rollout() {
        let root = std::env::temp_dir().join(format!(
            "codex-gateway-lite-plan-history-{}-{}",
            std::process::id(),
            SystemTime::now()
                .duration_since(SystemTime::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        fs::create_dir_all(&root).expect("temp root");
        let rollout_path = root.join("rollout.jsonl");
        let plan_one = json!({
            "plan": [
                { "step": "读取历史会话", "status": "completed" },
                { "step": "恢复任务清单", "status": "in_progress" }
            ]
        });
        let plan_cleared = json!({ "plan": [] });
        let lines = [
            json!({
                "type": "response_item",
                "payload": {
                    "type": "function_call",
                    "name": "update_plan",
                    "arguments": serde_json::to_string(&plan_one).unwrap(),
                    "call_id": "call-plan-1"
                }
            }),
            json!({
                "type": "response_item",
                "payload": {
                    "type": "function_call",
                    "name": "update_plan",
                    "arguments": serde_json::to_string(&plan_cleared).unwrap(),
                    "call_id": "call-plan-2"
                }
            }),
        ]
        .into_iter()
        .map(|value| value.to_string())
        .collect::<Vec<_>>()
        .join("\n");
        fs::write(&rollout_path, format!("{lines}\n")).expect("write rollout");

        let db_path = root.join("state_5.sqlite");
        let conn = Connection::open(&db_path).expect("open sqlite");
        conn.execute_batch(
            r#"
CREATE TABLE threads (
  id TEXT PRIMARY KEY,
  rollout_path TEXT,
  updated_at INTEGER,
  archived INTEGER
);
"#,
        )
        .expect("schema");
        conn.execute(
            "INSERT INTO threads (id, rollout_path, updated_at, archived) VALUES (?1, ?2, ?3, 0)",
            params![
                "thread-history",
                rollout_path.to_string_lossy().to_string(),
                1_783_399_904_i64
            ],
        )
        .expect("insert thread");
        drop(conn);

        let snapshots = collect_plan_ui_history_snapshots(&root, 10).expect("collect snapshots");
        let single_snapshot =
            collect_plan_ui_history_snapshot_for_thread(&root, "local:thread-history")
                .expect("collect single snapshot")
                .expect("single snapshot exists");
        fs::remove_dir_all(&root).ok();

        assert_eq!(snapshots.len(), 1);
        let snapshot = &snapshots[0];
        assert_eq!(snapshot.thread_id, "local:thread-history");
        assert_eq!(snapshot.progress, "第 2 / 2 步");
        assert_eq!(
            snapshot
                .rows
                .iter()
                .map(|row| (row.text.as_str(), row.status.as_str()))
                .collect::<Vec<_>>(),
            vec![("读取历史会话", "done"), ("恢复任务清单", "done")]
        );
        assert_eq!(snapshot.source, "rollout-update-plan-cleared");
        assert_eq!(single_snapshot.thread_id, "local:thread-history");
        assert_eq!(single_snapshot.rows.len(), 2);
        assert_eq!(single_snapshot.source, "rollout-update-plan-cleared");
    }

    #[test]
    fn plan_ui_latest_todo_from_turn_treats_cleared_plan_as_latest_state() {
        // 模型先给了一份三步 plan（最后一步 in_progress），随后调用 update_plan([]) 清空。
        // latestTodoFromTurn 不应该跳过这次清空、倒退回旧的非空 plan。
        let turn_info = r#"{
            "turn": {
                "items": [
                    { "type": "todo-list", "id": "todo-1", "plan": [
                        { "step": "读取代码", "status": "completed" },
                        { "step": "定位问题", "status": "completed" },
                        { "step": "编写修复", "status": "in_progress" }
                    ]},
                    { "type": "todo-list", "id": "todo-2", "plan": [] }
                ]
            }
        }"#;
        let result = call_plan_ui_hook("latestTodoFromTurn", &[turn_info]);
        assert_eq!(result["cleared"], serde_json::json!(true));
        assert_eq!(result["item"]["id"], serde_json::json!("todo-2"));
        assert_eq!(result["rows"], serde_json::json!([]));
    }

    #[test]
    fn plan_ui_latest_todo_from_turn_returns_latest_non_cleared_plan() {
        let turn_info = r#"{
            "turn": {
                "items": [
                    { "type": "todo-list", "id": "todo-1", "plan": [
                        { "step": "第一步", "status": "in_progress" }
                    ]},
                    { "type": "todo-list", "id": "todo-2", "plan": [
                        { "step": "第一步", "status": "completed" },
                        { "step": "第二步", "status": "in_progress" }
                    ]}
                ]
            }
        }"#;
        let result = call_plan_ui_hook("latestTodoFromTurn", &[turn_info]);
        assert_eq!(result["cleared"], serde_json::json!(false));
        assert_eq!(result["item"]["id"], serde_json::json!("todo-2"));
        assert_eq!(
            result["rows"]
                .as_array()
                .unwrap()
                .iter()
                .map(|r| r["status"].clone())
                .collect::<Vec<_>>(),
            vec![serde_json::json!("done"), serde_json::json!("running")]
        );
    }

    #[test]
    fn plan_ui_todo_snapshot_from_native_turn_settles_cleared_plan_to_done() {
        // 复刻真实场景：右侧面板已经记住了这个 thread 上一次的三行快照（最后一步还在
        // running），随后原生 turn 里最新的 todo-list 被模型清空。此时应该把上一次的
        // 行全部结算为 done，而不是回退显示清空前、仍卡在 running 的旧状态。
        let mut ctx = plan_ui_test_context();
        eval_json(
            &mut ctx,
            r#"(() => {
                window.__codexGatewayLitePlanUiState.snapshotsByThread["thread-1"] = {
                    threadId: "thread-1",
                    progress: "第 2 / 3 步",
                    rows: [
                        { text: "读取代码", status: "done", iconHtml: "" },
                        { text: "定位问题", status: "done", iconHtml: "" },
                        { text: "编写修复", status: "running", iconHtml: "" }
                    ],
                };
                return "null";
            })()"#,
        );
        let base = r#"{
            "threadId": "thread-1",
            "progress": "",
            "detail": "",
            "items": [],
            "at": 0
        }"#;
        let turn_info = r#"{
            "turnId": "turn-abc",
            "conversationId": "conv-abc",
            "turn": {
                "items": [
                    { "type": "todo-list", "id": "todo-1", "plan": [
                        { "step": "读取代码", "status": "completed" },
                        { "step": "定位问题", "status": "completed" },
                        { "step": "编写修复", "status": "in_progress" }
                    ]},
                    { "type": "todo-list", "id": "todo-2", "plan": [] }
                ]
            }
        }"#;
        let result = call_plan_ui_hook_in(
            &mut ctx,
            "todoSnapshotFromNativeTurn",
            &[base, "\"thread-1\"", turn_info],
        );
        assert_eq!(
            result["source"],
            serde_json::json!("native-turn-todo-cleared")
        );
        assert_eq!(result["sourceTodoId"], serde_json::json!("todo-2"));
        let statuses: Vec<_> = result["rows"]
            .as_array()
            .expect("rows should be an array")
            .iter()
            .map(|row| row["status"].clone())
            .collect();
        assert_eq!(
            statuses,
            vec![
                serde_json::json!("done"),
                serde_json::json!("done"),
                serde_json::json!("done"),
            ],
            "清空后所有行都应该结算为 done，而不是卡在 running"
        );
    }

    #[test]
    fn plan_ui_normalizes_progress_when_all_rows_are_done() {
        // 复刻 BOSS 截图：卡片头部还停在旧的“第 1 / 3 步”，但下面三行都已经
        // 是完成态。此时应该以 rows 为准，把头部修正为“第 3 / 3 步”。
        let result = call_plan_ui_hook(
            "normalizedSnapshotForRows",
            &[r#"{
                "threadId": "thread-done",
                "progress": "第 1 / 3 步",
                "rows": [
                    { "text": "对照 Codex++ 会话定位实现", "status": "done" },
                    { "text": "补强本项目跨环境会话数据定位", "status": "done" },
                    { "text": "验证并推送修复", "status": "done" }
                ]
            }"#],
        );
        assert_eq!(result["progress"], serde_json::json!("第 3 / 3 步"));
        assert_eq!(
            result["rows"]
                .as_array()
                .unwrap()
                .iter()
                .map(|row| row["status"].clone())
                .collect::<Vec<_>>(),
            vec![
                serde_json::json!("done"),
                serde_json::json!("done"),
                serde_json::json!("done"),
            ]
        );
    }

    #[test]
    fn plan_ui_todo_snapshot_from_native_turn_returns_null_when_nothing_to_settle() {
        // 从来没有 todo-list、也没有历史快照可以结算时，应该老老实实返回 null，
        // 交给外部兜底路径处理，而不是凭空造一个空快照出来。
        let base = r#"{"threadId":"thread-2","progress":"","detail":"","items":[],"at":0}"#;
        let turn_info = r#"{"turn": {"items": []}}"#;
        let result = call_plan_ui_hook(
            "todoSnapshotFromNativeTurn",
            &[base, "\"thread-2\"", turn_info],
        );
        assert!(result.is_null());
    }

    #[test]
    fn plan_ui_current_thread_id_prefers_visible_conversation_over_sidebar_hover() {
        // 左侧功能栏 hover 展开时会临时出现 active sidebar thread id；任务卡片不应该
        // 因为这个临时 DOM 状态切到其它 session 的快照，当前可见 composer conversation
        // 才是稳定身份。
        let mut ctx = plan_ui_test_context();
        let result = eval_json(
            &mut ctx,
            r#"(() => {
                const portal = {
                    getAttribute(name) {
                        return name === "data-above-composer-conversation-id"
                            ? "019f3b0c-1da5-7e13-967e-a9fc35e77652"
                            : null;
                    },
                    getBoundingClientRect() {
                        return { width: 736, height: 32, left: 532, right: 1268, top: 924, bottom: 956 };
                    },
                    textContent: "第 1 / 4 步"
                };
                const sidebar = {
                    getAttribute(name) {
                        return name === "data-app-action-sidebar-thread-id"
                            ? "local:wrong-hover-thread"
                            : null;
                    },
                    getBoundingClientRect() {
                        return { width: 320, height: 44, left: 12, right: 332, top: 480, bottom: 524 };
                    },
                    textContent: "其它会话"
                };
                const title = {
                    getBoundingClientRect() {
                        return { width: 800, height: 46, left: 224, right: 1024, top: 0, bottom: 46 };
                    },
                    textContent: "当前会话标题"
                };
                document.querySelectorAll = function (selector) {
                    return String(selector).includes("data-above-composer-portal") ? [portal] : [];
                };
                document.querySelector = function (selector) {
                    const value = String(selector);
                    if (value.includes("data-app-action-sidebar-thread-active")) return sidebar;
                    if (value.includes("app-shell-header-context-menu-surface")) return title;
                    if (value === "header") return title;
                    return null;
                };
                const hooks = window.__codexGatewayLitePlanUiTestHooks;
                return JSON.stringify({
                    current: hooks.currentThreadId(),
                    conversation: hooks.currentConversationThreadId(),
                    sidebar: hooks.currentSidebarThreadId(),
                    title: hooks.currentVisibleTitleThreadId()
                });
            })()"#,
        );

        assert_eq!(
            result["current"],
            serde_json::json!("local:019f3b0c-1da5-7e13-967e-a9fc35e77652")
        );
        assert_eq!(
            result["conversation"],
            serde_json::json!("local:019f3b0c-1da5-7e13-967e-a9fc35e77652")
        );
        assert_eq!(
            result["sidebar"],
            serde_json::json!("local:wrong-hover-thread")
        );
        assert_eq!(result["title"], serde_json::json!("visible:当前会话标题"));
    }

    #[test]
    fn plan_ui_current_thread_id_prefers_known_sidebar_thread_during_history_title_update() {
        // 历史会话切换时 composer conversation id 可能还没挂上，标题也会从“新会话”
        // 异步更新为真实标题；此时应使用左侧 active row 的 local thread id 去命中
        // rollout/localStorage 历史快照，否则任务卡片会出现一下又因为 visible:title key 变化而消失。
        let mut ctx = plan_ui_test_context();
        let result = eval_json(
            &mut ctx,
            r#"(() => {
                const sidebar = {
                    getAttribute(name) {
                        return name === "data-app-action-sidebar-thread-id"
                            ? "local:history-thread"
                            : null;
                    },
                    getBoundingClientRect() {
                        return { width: 320, height: 44, left: 12, right: 332, top: 480, bottom: 524 };
                    },
                    textContent: "新会话"
                };
                const title = {
                    getBoundingClientRect() {
                        return { width: 800, height: 46, left: 224, right: 1024, top: 0, bottom: 46 };
                    },
                    textContent: "好问题 BOSS～图图来说清楚来源"
                };
                document.querySelectorAll = function (selector) {
                    return String(selector).includes("data-above-composer-portal") ? [] : [];
                };
                document.querySelector = function (selector) {
                    const value = String(selector);
                    if (value.includes("data-app-action-sidebar-thread-active")) return sidebar;
                    if (value.includes("app-shell-header-context-menu-surface")) return title;
                    if (value === "header") return title;
                    return null;
                };
                const state = window.__codexGatewayLitePlanUiState;
                state.snapshotsByThread = {
                    "local:history-thread": {
                        threadId: "local:history-thread",
                        progress: "第 7 / 7 步",
                        rows: [{ text: "历史会话任务", status: "done", iconHtml: "" }],
                        at: Date.now()
                    }
                };
                const hooks = window.__codexGatewayLitePlanUiTestHooks;
                const current = hooks.currentThreadId();
                const aliases = hooks.snapshotAliasIdsForThread("visible:好问题 BOSS～图图来说清楚来源");
                const snapshot = hooks.snapshotForThread(current);
                return JSON.stringify({
                    current,
                    sidebar: hooks.currentSidebarThreadId(),
                    title: hooks.currentVisibleTitleThreadId(),
                    aliases,
                    snapshotThreadId: snapshot.threadId,
                    row: snapshot.rows[0].text
                });
            })()"#,
        );

        assert_eq!(result["current"], serde_json::json!("local:history-thread"));
        assert_eq!(result["sidebar"], serde_json::json!("local:history-thread"));
        assert_eq!(
            result["title"],
            serde_json::json!("visible:好问题 BOSS～图图来说清楚来源")
        );
        assert_eq!(
            result["aliases"],
            serde_json::json!(["local:history-thread"])
        );
        assert_eq!(
            result["snapshotThreadId"],
            serde_json::json!("local:history-thread")
        );
        assert_eq!(result["row"], serde_json::json!("历史会话任务"));
    }

    #[test]
    fn plan_ui_active_seed_request_uses_real_local_thread_without_visible_alias_cache() {
        // 点击历史会话时标题区可能先拼出 visible:title，并且旧逻辑会把 visible
        // 别名缓存当成“已有快照”。按需恢复必须以真实 local/remote thread id
        // 的直接快照为准，否则 Rust 侧不会去读取对应 rollout。
        let mut ctx = plan_ui_test_context();
        let result = eval_json(
            &mut ctx,
            r#"(() => {
                const sidebar = {
                    getAttribute(name) {
                        return name === "data-app-action-sidebar-thread-id"
                            ? "local:history-thread"
                            : null;
                    },
                    getBoundingClientRect() {
                        return { width: 320, height: 44, left: 12, right: 332, top: 480, bottom: 524 };
                    },
                    textContent: "历史问题"
                };
                const title = {
                    getBoundingClientRect() {
                        return { width: 800, height: 46, left: 224, right: 1024, top: 0, bottom: 46 };
                    },
                    textContent: "历史问题打开位置"
                };
                document.querySelectorAll = function (selector) {
                    return String(selector).includes("data-above-composer-portal") ? [] : [];
                };
                document.querySelector = function (selector) {
                    const value = String(selector);
                    if (value.includes("data-app-action-sidebar-thread-active")) return sidebar;
                    if (value.includes("app-shell-header-context-menu-surface")) return title;
                    if (value === "header") return title;
                    return null;
                };
                const state = window.__codexGatewayLitePlanUiState;
                state.snapshotsByThread = {
                    "visible:历史问题打开位置": {
                        threadId: "visible:历史问题打开位置",
                        progress: "第 4 / 4 步",
                        rows: [{ text: "别名缓存任务", status: "done", iconHtml: "" }],
                        at: Date.now()
                    }
                };
                const hooks = window.__codexGatewayLitePlanUiTestHooks;
                const seed = hooks.activeHistorySeedRequest();
                return JSON.stringify({
                    current: hooks.currentThreadId(),
                    activeSeed: hooks.activeSeedThreadId(),
                    directRows: hooks.rowsForSnapshot(hooks.directSnapshotForSeed("local:history-thread")).length,
                    seed,
                });
            })()"#,
        );

        assert_eq!(result["current"], serde_json::json!("local:history-thread"));
        assert_eq!(
            result["activeSeed"],
            serde_json::json!("local:history-thread")
        );
        assert_eq!(result["directRows"], serde_json::json!(0));
        assert_eq!(
            result["seed"]["threadId"],
            serde_json::json!("local:history-thread")
        );
        assert_eq!(result["seed"]["needsSeed"], serde_json::json!(true));
        assert_eq!(
            result["seed"]["reason"],
            serde_json::json!("missing-direct-snapshot")
        );
    }

    #[test]
    fn plan_ui_current_thread_id_keeps_visible_title_when_sidebar_hover_points_elsewhere() {
        // 没有 composer id 的过渡状态下，如果 sidebar active id 与当前标题不匹配，
        // 且当前 visible title 已经有快照，就不能被 hover 展开的其它会话抢走。
        let mut ctx = plan_ui_test_context();
        let result = eval_json(
            &mut ctx,
            r#"(() => {
                const sidebar = {
                    getAttribute(name) {
                        return name === "data-app-action-sidebar-thread-id"
                            ? "local:wrong-hover-thread"
                            : null;
                    },
                    getBoundingClientRect() {
                        return { width: 320, height: 44, left: 12, right: 332, top: 480, bottom: 524 };
                    },
                    textContent: "其它会话"
                };
                const title = {
                    getBoundingClientRect() {
                        return { width: 800, height: 46, left: 224, right: 1024, top: 0, bottom: 46 };
                    },
                    textContent: "当前会话标题"
                };
                document.querySelectorAll = function (selector) {
                    return String(selector).includes("data-above-composer-portal") ? [] : [];
                };
                document.querySelector = function (selector) {
                    const value = String(selector);
                    if (value.includes("data-app-action-sidebar-thread-active")) return sidebar;
                    if (value.includes("app-shell-header-context-menu-surface")) return title;
                    if (value === "header") return title;
                    return null;
                };
                const state = window.__codexGatewayLitePlanUiState;
                state.snapshotsByThread = {
                    "local:wrong-hover-thread": {
                        threadId: "local:wrong-hover-thread",
                        progress: "第 3 / 3 步",
                        rows: [{ text: "错误 hover 任务", status: "running", iconHtml: "" }],
                        at: 200
                    },
                    "visible:当前会话标题": {
                        threadId: "visible:当前会话标题",
                        progress: "第 2 / 2 步",
                        rows: [{ text: "当前可见任务", status: "running", iconHtml: "" }],
                        at: 100
                    }
                };
                const hooks = window.__codexGatewayLitePlanUiTestHooks;
                const current = hooks.currentThreadId();
                const snapshot = hooks.snapshotForThread(current);
                return JSON.stringify({
                    current,
                    row: snapshot.rows[0].text
                });
            })()"#,
        );

        assert_eq!(result["current"], serde_json::json!("visible:当前会话标题"));
        assert_eq!(result["row"], serde_json::json!("当前可见任务"));
    }

    #[test]
    fn plan_ui_snapshot_for_thread_uses_newer_visible_alias_over_stale_local_cache() {
        // 复现截图里的核心风险：sidebar 展开后 threadId 从 visible:title 切到 local:id，
        // local:id 下可能有上一轮旧任务。若当前 visible:title 快照更新，应该迁移/覆盖
        // local:id，而不是显示旧 session/旧任务清单。
        let mut ctx = plan_ui_test_context();
        let result = eval_json(
            &mut ctx,
            r#"(() => {
                const portal = {
                    getAttribute(name) {
                        return name === "data-above-composer-conversation-id" ? "conv-current" : null;
                    },
                    getBoundingClientRect() {
                        return { width: 736, height: 32, left: 532, right: 1268, top: 924, bottom: 956 };
                    },
                    textContent: "第 4 / 4 步"
                };
                const title = {
                    getBoundingClientRect() {
                        return { width: 800, height: 46, left: 224, right: 1024, top: 0, bottom: 46 };
                    },
                    textContent: "当前会话标题"
                };
                document.querySelectorAll = function (selector) {
                    return String(selector).includes("data-above-composer-portal") ? [portal] : [];
                };
                document.querySelector = function (selector) {
                    const value = String(selector);
                    if (value.includes("app-shell-header-context-menu-surface")) return title;
                    if (value === "header") return title;
                    return null;
                };
                const state = window.__codexGatewayLitePlanUiState;
                state.snapshotsByThread = {
                    "local:conv-current": {
                        threadId: "local:conv-current",
                        progress: "第 3 / 3 步",
                        rows: [{ text: "旧 session 任务", status: "running", iconHtml: "" }],
                        at: 100
                    },
                    "visible:当前会话标题": {
                        threadId: "visible:当前会话标题",
                        progress: "第 4 / 4 步",
                        rows: [{ text: "当前可见任务", status: "running", iconHtml: "" }],
                        at: 200
                    }
                };
                const hooks = window.__codexGatewayLitePlanUiTestHooks;
                const snapshot = hooks.snapshotForThread("local:conv-current");
                return JSON.stringify({
                    threadId: snapshot.threadId,
                    progress: snapshot.progress,
                    row: snapshot.rows[0].text,
                    aliases: hooks.snapshotAliasIdsForThread("local:conv-current")
                });
            })()"#,
        );

        assert_eq!(result["threadId"], serde_json::json!("local:conv-current"));
        assert_eq!(result["progress"], serde_json::json!("第 4 / 4 步"));
        assert_eq!(result["row"], serde_json::json!("当前可见任务"));
        assert_eq!(
            result["aliases"],
            serde_json::json!(["visible:当前会话标题"])
        );
    }

    #[test]
    fn plan_ui_snapshot_for_thread_uses_seeded_history_snapshot_after_restart() {
        // renderer/App 重启后，当前内存里可能没有这个历史会话的快照；
        // agent 会从本地 rollout 解析 update_plan 并注入 external snapshot map，
        // snapshotForThread 应该能直接恢复对应会话的任务卡片。
        let mut ctx = plan_ui_test_context();
        let result = eval_json(
            &mut ctx,
            r#"(() => {
                window.__codexGatewayLitePlanUiExternalSnapshots = {
                    "local:history-thread": {
                        threadId: "local:history-thread",
                        progress: "第 2 / 3 步",
                        rows: [
                            { text: "读取 rollout", status: "done", iconHtml: "" },
                            { text: "恢复历史卡片", status: "running", iconHtml: "" },
                            { text: "注入当前 renderer", status: "pending", iconHtml: "" }
                        ],
                        items: ["读取 rollout", "恢复历史卡片", "注入当前 renderer"],
                        sourceConversationId: "history-thread",
                        sourceTurnId: "call-history",
                        sourceTodoId: "call-history",
                        at: Date.now(),
                        source: "rollout-update-plan"
                    }
                };
                const hooks = window.__codexGatewayLitePlanUiTestHooks;
                const snapshot = hooks.snapshotForThread("local:history-thread");
                return JSON.stringify({
                    threadId: snapshot.threadId,
                    progress: snapshot.progress,
                    source: snapshot.source,
                    rows: snapshot.rows.map((row) => [row.text, row.status])
                });
            })()"#,
        );

        assert_eq!(
            result["threadId"],
            serde_json::json!("local:history-thread")
        );
        assert_eq!(result["progress"], serde_json::json!("第 2 / 3 步"));
        assert_eq!(result["source"], serde_json::json!("rollout-update-plan"));
        assert_eq!(
            result["rows"],
            serde_json::json!([
                ["读取 rollout", "done"],
                ["恢复历史卡片", "running"],
                ["注入当前 renderer", "pending"]
            ])
        );
    }

    #[test]
    fn plan_ui_right_side_expanded_content_rect_detects_large_right_panel() {
        let positive = call_plan_ui_hook(
            "rightSideExpandedContentRect",
            &[r#"{"width":520,"height":500,"left":640,"right":1160,"top":40,"bottom":760}"#],
        );
        assert_eq!(positive, serde_json::json!(true));

        let negative = call_plan_ui_hook(
            "rightSideExpandedContentRect",
            &[r#"{"width":520,"height":500,"left":240,"right":760,"top":40,"bottom":760}"#],
        );
        assert_eq!(negative, serde_json::json!(false));
    }

    #[test]
    fn plan_ui_right_panel_click_cannot_hide_dock_while_environment_panel_exists() {
        // BOSS 现场确认：任务卡片必须和 Codex 原生“环境信息”卡片共生。
        // 只要原生卡片仍然存在且可见，点击右侧其它按钮/空白/展开内容都不能隐藏 dock。
        let mut ctx = plan_ui_test_context();
        let result = eval_json(
            &mut ctx,
            r#"(() => {
                const envPanel = {
                    nodeType: 1,
                    id: "",
                    parentElement: null,
                    closest() { return null; },
                    contains(node) { return node === this; },
                    querySelectorAll() { return []; },
                    getBoundingClientRect() {
                        return { width: 320, height: 240, left: 1600, right: 1928, top: 90, bottom: 330 };
                    },
                    textContent: "环境信息 变更 +11 -18 本地 main 提交或推送"
                };
                document.querySelectorAll = function (selector) {
                    return String(selector).includes("aside") ? [envPanel] : [];
                };
                document.elementFromPoint = function () { return envPanel; };
                const dock = document.getElementById("codex-gateway-lite-plan-ui-dock");
                const hooks = window.__codexGatewayLitePlanUiTestHooks;
                const snapshot = {
                    threadId: "visible:unknown",
                    progress: "第 1 / 1 步",
                    detail: "",
                    items: ["验证右侧面板隐藏"],
                    rows: [{ text: "验证右侧面板隐藏", status: "running", iconHtml: "" }],
                    at: Date.now()
                };
                hooks.renderDock(snapshot);
                const beforeHidden = dock.hidden;
                const panel = {
                    nodeType: 1,
                    id: "",
                    parentElement: null,
                    closest() { return null; },
                    contains() { return false; },
                    querySelectorAll() { return []; },
                    getBoundingClientRect() {
                        return { width: 620, height: 760, left: 760, right: 1380, top: 60, bottom: 820 };
                    }
                };
                const dismissed = hooks.hideDockForRightPanelTarget(panel);
                hooks.renderDock(snapshot);
                return JSON.stringify({
                    beforeHidden,
                    dismissed,
                    afterHidden: dock.hidden,
                    hiddenBy: dock.dataset.cglPlanHiddenBy || "",
                    signature: dock.dataset.cglPlanSignature || "",
                    active: hooks.rightPanelDismissalActive()
                });
            })()"#,
        );

        assert_eq!(result["beforeHidden"], serde_json::json!(false));
        assert_eq!(result["dismissed"], serde_json::json!(false));
        assert_eq!(result["afterHidden"], serde_json::json!(false));
        assert_eq!(result["hiddenBy"], serde_json::json!(""));
        assert_ne!(result["signature"], serde_json::json!(""));
        assert_eq!(result["active"], serde_json::json!(false));
    }

    #[test]
    fn plan_ui_render_meta_uses_normalized_progress_and_environment_delta() {
        let mut ctx = plan_ui_test_context();
        let result = eval_json(
            &mut ctx,
            r#"(() => {
                const envPanel = {
                    nodeType: 1,
                    id: "",
                    parentElement: null,
                    closest() { return null; },
                    contains(node) { return node === this; },
                    querySelectorAll() { return []; },
                    getBoundingClientRect() {
                        return { width: 320, height: 240, left: 1100, right: 1428, top: 90, bottom: 330 };
                    },
                    textContent: "环境信息 变更 +431 -34 本地 main 提交或推送 来源 暂无来源"
                };
                document.querySelectorAll = function (selector) {
                    return String(selector).includes("aside") ? [envPanel] : [];
                };
                document.elementFromPoint = function () { return envPanel; };
                const dock = document.getElementById("codex-gateway-lite-plan-ui-dock");
                const hooks = window.__codexGatewayLitePlanUiTestHooks;
                hooks.renderDock({
                    threadId: "visible:unknown",
                    progress: "第 1 / 4 步",
                    detail: "",
                    metaParts: [{ text: "第 1 / 4 步", color: "rgb(1, 2, 3)" }],
                    rows: [
                        { text: "读取本地交接和启动脚本线索", status: "done", iconHtml: "" },
                        { text: "定位 Windows 卡住与中文重复输出原因", status: "done", iconHtml: "" },
                        { text: "修复脚本并补充轻量验证", status: "done", iconHtml: "" },
                        { text: "运行格式化/测试并汇报变更", status: "done", iconHtml: "" }
                    ],
                    at: Date.now()
                });
                return JSON.stringify({
                    hidden: dock.hidden,
                    html: dock.innerHTML
                });
            })()"#,
        );

        assert_eq!(result["hidden"], serde_json::json!(false));
        let html = result["html"].as_str().unwrap_or_default();
        assert!(html.contains("第 4 / 4 步"), "{html}");
        assert!(!html.contains("第 1 / 4 步"), "{html}");
        assert!(html.contains("+431"), "{html}");
        assert!(html.contains("-34"), "{html}");
    }

    #[test]
    fn plan_ui_keeps_dock_visible_when_right_panel_exists_without_click() {
        // 历史会话打开时，Codex 右侧可能已有“输出/来源”面板；这个面板只是被动存在，
        // 不应该被当成用户点击右侧展开内容，否则任务卡片会先出现、下一轮重渲染又消失。
        let mut ctx = plan_ui_test_context();
        let result = eval_json(
            &mut ctx,
            r#"(() => {
                const panel = {
                    nodeType: 1,
                    id: "",
                    parentElement: null,
                    closest() { return null; },
                    contains(node) { return node === this; },
                    querySelectorAll() { return []; },
                    getBoundingClientRect() {
                        return { width: 620, height: 820, left: 760, right: 1380, top: 40, bottom: 860 };
                    },
                    textContent: "README.md Codex Gateway Lite 依赖与一键引导"
                };
                const envPanel = {
                    nodeType: 1,
                    id: "",
                    parentElement: null,
                    closest() { return null; },
                    contains(node) { return node === this; },
                    querySelectorAll() { return []; },
                    getBoundingClientRect() {
                        return { width: 320, height: 240, left: 1600, right: 1928, top: 90, bottom: 330 };
                    },
                    textContent: "环境信息 变更 +11 -18 本地 main 提交或推送"
                };
                document.querySelectorAll = function (selector) {
                    if (String(selector).includes("webview")) return [panel];
                    if (String(selector).includes("aside")) return [envPanel];
                    return [];
                };
                document.elementFromPoint = function () { return envPanel; };
                const dock = document.getElementById("codex-gateway-lite-plan-ui-dock");
                const hooks = window.__codexGatewayLitePlanUiTestHooks;
                const snapshot = {
                    threadId: "visible:unknown",
                    progress: "第 1 / 1 步",
                    rows: [{ text: "打开右侧 README", status: "running", iconHtml: "" }],
                    at: Date.now()
                };
                hooks.renderDock(snapshot);
                return JSON.stringify({
                    active: hooks.rightPanelDismissalActive(),
                    hidden: dock.hidden,
                    hiddenBy: dock.dataset.cglPlanHiddenBy || "",
                    signature: dock.dataset.cglPlanSignature || ""
                });
            })()"#,
        );

        assert_eq!(result["active"], serde_json::json!(false));
        assert_eq!(result["hidden"], serde_json::json!(false));
        assert_eq!(result["hiddenBy"], serde_json::json!(""));
        assert_ne!(result["signature"], serde_json::json!(""));
    }

    #[test]
    fn plan_ui_hides_dock_when_native_environment_panel_disappears() {
        // 原生“环境信息”卡片被弹窗/侧边菜单顶掉后（比如切到 移交至工作树 弹窗，
        // 或者点开 审查/终端/浏览器 侧栏切换菜单），dock 不能落到一个瞎猜的默认位置，
        // 必须跟随原生卡片一起隐藏；卡片恢复后 dock 也要能重新贴回去。
        let mut ctx = plan_ui_test_context();
        let result = eval_json(
            &mut ctx,
            r#"(() => {
                const envPanel = {
                    nodeType: 1,
                    id: "",
                    parentElement: null,
                    closest() { return null; },
                    contains(node) { return node === this; },
                    querySelectorAll() { return []; },
                    getBoundingClientRect() {
                        return { width: 320, height: 240, left: 1600, right: 1928, top: 90, bottom: 330 };
                    },
                    textContent: "环境信息 变更 +11 -18 本地 main 提交或推送"
                };
                let hasEnvPanel = true;
                document.querySelectorAll = function (selector) {
                    return hasEnvPanel && String(selector).includes("aside") ? [envPanel] : [];
                };
                document.elementFromPoint = function () { return envPanel; };
                const dock = document.getElementById("codex-gateway-lite-plan-ui-dock");
                const hooks = window.__codexGatewayLitePlanUiTestHooks;
                const snapshot = {
                    threadId: "visible:unknown",
                    progress: "第 1 / 1 步",
                    rows: [{ text: "验证面板跟随环境信息卡片", status: "running", iconHtml: "" }],
                    at: Date.now()
                };
                hooks.renderDock(snapshot);
                const hiddenWithPanel = dock.hidden;
                hasEnvPanel = false;
                hooks.renderDock(snapshot);
                const hiddenWithoutPanel = dock.hidden;
                const hiddenByWithoutPanel = dock.dataset.cglPlanHiddenBy || "";
                hasEnvPanel = true;
                hooks.renderDock(snapshot);
                const hiddenAfterRestore = dock.hidden;
                return JSON.stringify({
                    hiddenWithPanel,
                    hiddenWithoutPanel,
                    hiddenByWithoutPanel,
                    hiddenAfterRestore
                });
            })()"#,
        );

        assert_eq!(result["hiddenWithPanel"], serde_json::json!(false));
        assert_eq!(result["hiddenWithoutPanel"], serde_json::json!(true));
        assert_eq!(
            result["hiddenByWithoutPanel"],
            serde_json::json!("no-native-anchor")
        );
        assert_eq!(result["hiddenAfterRestore"], serde_json::json!(false));
    }

    #[test]
    fn plan_ui_schedule_apply_throttles_high_frequency_mutation_bursts() {
        // 真实页面流式回复期间，MutationObserver 会在几秒内触发成百上千次
        // scheduleApply（childList/subtree/attributes 全部在监听范围内）。
        // 必须把这类爆发合并成一次节流定时器，不能每次都同步跑一遍 apply()，
        // 否则 renderer 进程 CPU 会被打满（真实现场表现为温度飙升）。
        let mut ctx = plan_ui_test_context();
        let result = eval_json(
            &mut ctx,
            r#"(() => {
                let rafCalls = 0;
                let pendingRafCb = null;
                window.requestAnimationFrame = function (cb) {
                    rafCalls += 1;
                    pendingRafCb = cb;
                    return rafCalls;
                };
                let timeoutCalls = 0;
                let pendingTimeoutCb = null;
                window.setTimeout = function (cb) {
                    timeoutCalls += 1;
                    pendingTimeoutCb = cb;
                    return timeoutCalls;
                };
                const hooks = window.__codexGatewayLitePlanUiTestHooks;

                hooks.scheduleApply();
                const rafAfterFirst = rafCalls;
                const timeoutAfterFirst = timeoutCalls;

                // 模拟浏览器在下一帧真正跑了这次 rAF 回调：更新 lastApplyAt 并跑一次 apply()。
                pendingRafCb();

                // 紧接着（同一瞬间）模拟 MutationObserver 连续爆发 50 次。
                for (let i = 0; i < 50; i += 1) {
                    hooks.scheduleApply();
                }
                const rafAfterBurst = rafCalls;
                const timeoutAfterBurst = timeoutCalls;

                // 节流窗口结束后应当补跑一次，变更不会被吞掉。
                pendingTimeoutCb();
                const rafAfterThrottleFires = rafCalls;

                return JSON.stringify({
                    rafAfterFirst,
                    timeoutAfterFirst,
                    rafAfterBurst,
                    timeoutAfterBurst,
                    rafAfterThrottleFires
                });
            })()"#,
        );

        assert_eq!(result["rafAfterFirst"], serde_json::json!(1));
        assert_eq!(result["timeoutAfterFirst"], serde_json::json!(0));
        assert_eq!(
            result["rafAfterBurst"],
            serde_json::json!(1),
            "50 次连续触发不应该各自排队一次 rAF"
        );
        assert_eq!(
            result["timeoutAfterBurst"],
            serde_json::json!(1),
            "50 次连续触发应该被合并成一次节流定时器"
        );
        assert_eq!(
            result["rafAfterThrottleFires"],
            serde_json::json!(2),
            "节流窗口结束后应该补跑一次，不能把变更吞掉"
        );
    }

    #[test]
    fn plan_ui_hides_dock_behind_blocking_media_preview_overlay() {
        // 图片/视频预览会打开全屏 overlay；任务卡片不能以固定 z-index 压在预览层上。
        let mut ctx = plan_ui_test_context();
        let result = eval_json(
            &mut ctx,
            r#"(() => {
                const overlay = {
                    nodeType: 1,
                    id: "",
                    parentElement: null,
                    closest() { return null; },
                    contains() { return false; },
                    querySelectorAll() { return []; },
                    getAttribute(name) { return name === "role" ? "dialog" : null; },
                    getBoundingClientRect() {
                        return { width: 1260, height: 760, left: 80, right: 1340, top: 70, bottom: 830 };
                    },
                    textContent: ""
                };
                document.querySelectorAll = function (selector) {
                    return String(selector).includes("role=\"dialog\"") ? [overlay] : [];
                };
                const dock = document.getElementById("codex-gateway-lite-plan-ui-dock");
                const hooks = window.__codexGatewayLitePlanUiTestHooks;
                const snapshot = {
                    threadId: "visible:unknown",
                    progress: "第 1 / 1 步",
                    rows: [{ text: "不要覆盖媒体预览", status: "running", iconHtml: "" }],
                    at: Date.now()
                };
                hooks.renderDock(snapshot);
                return JSON.stringify({
                    hidden: dock.hidden,
                    hiddenBy: dock.dataset.cglPlanHiddenBy || "",
                    signature: dock.dataset.cglPlanSignature || ""
                });
            })()"#,
        );

        assert_eq!(result["hidden"], serde_json::json!(true));
        assert_eq!(result["hiddenBy"], serde_json::json!("blocking-overlay"));
        assert_eq!(result["signature"], serde_json::json!(""));
    }

    #[test]
    fn plan_ui_hides_dock_behind_dark_media_preview_even_without_dialog_role() {
        // 有些媒体预览不是标准 dialog，而是大黑 backdrop + 关闭/下载控件；
        // 这类也不能让任务卡片浮到最上层。
        let mut ctx = plan_ui_test_context();
        let result = eval_json(
            &mut ctx,
            r#"(() => {
                const closeButton = {
                    nodeType: 1,
                    id: "",
                    parentElement: null,
                    closest() { return null; },
                    contains() { return false; },
                    querySelectorAll() { return []; },
                    getAttribute(name) { return name === "aria-label" ? "关闭" : null; },
                    getBoundingClientRect() {
                        return { width: 44, height: 44, left: 1360, right: 1404, top: 32, bottom: 76 };
                    },
                    textContent: ""
                };
                const overlay = {
                    nodeType: 1,
                    id: "",
                    parentElement: null,
                    closest() { return null; },
                    contains(node) { return node === closeButton; },
                    querySelectorAll(selector) {
                        return String(selector).includes("button") ? [closeButton] : [];
                    },
                    getAttribute() { return null; },
                    getBoundingClientRect() {
                        return { width: 1440, height: 900, left: 0, right: 1440, top: 0, bottom: 900 };
                    },
                    textContent: ""
                };
                const defaultStyle = {
                    color: "", display: "block", visibility: "visible", opacity: "1",
                    backgroundColor: "rgba(0,0,0,0)", boxShadow: "none", zIndex: "auto",
                    position: "static", backdropFilter: "none",
                    borderTopLeftRadius: "0px", borderTopRightRadius: "0px",
                    borderBottomLeftRadius: "0px", borderBottomRightRadius: "0px",
                    borderTopWidth: "0px", borderRightWidth: "0px", borderBottomWidth: "0px", borderLeftWidth: "0px",
                };
                getComputedStyle = function (node) {
                    return node === overlay
                        ? { ...defaultStyle, backgroundColor: "rgba(0,0,0,0.82)" }
                        : defaultStyle;
                };
                document.querySelectorAll = function (selector) {
                    return String(selector).includes("div") ? [overlay] : [];
                };
                const dock = document.getElementById("codex-gateway-lite-plan-ui-dock");
                const hooks = window.__codexGatewayLitePlanUiTestHooks;
                hooks.renderDock({
                    threadId: "visible:unknown",
                    progress: "第 1 / 1 步",
                    rows: [{ text: "不要盖住大图预览", status: "running", iconHtml: "" }],
                    at: Date.now()
                });
                return JSON.stringify({
                    hidden: dock.hidden,
                    hiddenBy: dock.dataset.cglPlanHiddenBy || "",
                    signature: dock.dataset.cglPlanSignature || ""
                });
            })()"#,
        );

        assert_eq!(result["hidden"], serde_json::json!(true));
        assert_eq!(result["hiddenBy"], serde_json::json!("blocking-overlay"));
        assert_eq!(result["signature"], serde_json::json!(""));
    }

    #[test]
    fn plan_ui_status_for_todo_plan_status_maps_variants() {
        let cases = [
            ("completed", "done"),
            ("complete", "done"),
            ("done", "done"),
            ("in_progress", "running"),
            ("in-progress", "running"),
            ("running", "running"),
            ("active", "running"),
            ("pending", "pending"),
            ("", "pending"),
            ("unknown-status", "pending"),
        ];
        for (input, expected) in cases {
            let result = call_plan_ui_hook("statusForTodoPlanStatus", &[&format!("\"{input}\"")]);
            assert_eq!(
                result,
                serde_json::json!(expected),
                "status input {input:?}"
            );
        }
    }

    #[test]
    fn plan_ui_normalize_item_rejects_denylisted_ui_labels() {
        for label in ["打开位置", "打开图片", "审查", "引导", "删除", "更多"] {
            let result = call_plan_ui_hook("normalizeItem", &[&format!("\"{label}\"")]);
            assert_eq!(
                result,
                serde_json::json!(""),
                "label {label:?} should be filtered out"
            );
        }
        let result =
            call_plan_ui_hook("normalizeItem", &["\"核对 Gateway 与 Manager 的鉴权边界\""]);
        assert_eq!(
            result,
            serde_json::json!("核对 Gateway 与 Manager 的鉴权边界")
        );
    }

    #[test]
    fn plan_ui_snapshot_context_matches_requires_same_turn_id() {
        let same_turn = call_plan_ui_hook(
            "snapshotContextMatches",
            &[
                r#"{"sourceTurnId":"turn-1"}"#,
                r#"{"sourceTurnId":"turn-1"}"#,
            ],
        );
        assert_eq!(same_turn, serde_json::json!(true));

        let different_turn = call_plan_ui_hook(
            "snapshotContextMatches",
            &[
                r#"{"sourceTurnId":"turn-1"}"#,
                r#"{"sourceTurnId":"turn-2"}"#,
            ],
        );
        assert_eq!(different_turn, serde_json::json!(false));

        let different_conversation_no_turn_id = call_plan_ui_hook(
            "snapshotContextMatches",
            &[
                r#"{"sourceConversationId":"conv-1"}"#,
                r#"{"sourceConversationId":"conv-2"}"#,
            ],
        );
        assert_eq!(different_conversation_no_turn_id, serde_json::json!(false));
    }

    #[test]
    fn example_config_parses_without_hardcoded_models() {
        let config: LiteConfig = serde_json::from_str(include_str!("../config.example.json"))
            .expect("example config parses");
        assert_eq!(config.provider.protocol, LiteProtocol::Responses);
        assert!(config.model.is_empty());
        assert!(config.models.is_empty());
    }

    #[test]
    fn model_catalog_entries_include_current_codex_required_fields() {
        let entries = vec![codex_lite::ModelCatalogEntry {
            slug: "claude-sonnet-5".to_string(),
            display_name: "Claude Sonnet 5".to_string(),
            suffix_window: Some(1_000_000),
        }];
        let catalog: Value =
            serde_json::from_str(&codex_lite::build_model_catalog_json(&entries, None, false))
                .expect("catalog json parses");
        let model = &catalog["models"][0];

        assert_eq!(model["model"].as_str(), Some("claude-sonnet-5"));
        assert_eq!(
            model["supported_reasoning_levels"].as_array().map(Vec::len),
            Some(4)
        );
        assert_eq!(model["default_reasoning_level"].as_str(), Some("medium"));
        assert_eq!(model["default_verbosity"].as_str(), Some("low"));
        assert_eq!(model["apply_patch_tool_type"].as_str(), Some("freeform"));
        assert_eq!(model["shell_type"].as_str(), Some("shell_command"));
        assert!(
            model["base_instructions"]
                .as_str()
                .is_some_and(|value| value.contains("You are Codex"))
        );
        assert!(
            model["model_messages"]["instructions_template"]
                .as_str()
                .is_some_and(|value| value.contains("You are Codex"))
        );
    }

    #[test]
    fn sync_local_thread_catalog_rebuilds_sidebar_index_from_session_index() {
        let unique = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .expect("clock is after unix epoch")
            .as_nanos();
        let home = std::env::temp_dir().join(format!("codex-gateway-lite-catalog-test-{unique}"));
        let sqlite_dir = home.join("sqlite");
        fs::create_dir_all(&sqlite_dir).expect("creates sqlite dir");
        let state_db = sqlite_dir.join("state_5.sqlite");
        let conn = Connection::open(&state_db).expect("opens state db");
        conn.execute_batch(
            r#"
CREATE TABLE threads (
  id TEXT PRIMARY KEY,
  title TEXT,
  created_at TEXT,
  updated_at TEXT,
  cwd TEXT,
  source TEXT,
  model_provider TEXT,
  git_branch TEXT,
  archived INTEGER
);
"#,
        )
        .expect("creates threads table");
        conn.execute(
            "INSERT INTO threads VALUES (?1, ?2, '100', '200', ?3, 'vscode', 'gateway', 'main', 0)",
            params!["thread-1", "Original Title", "/tmp/project-one"],
        )
        .expect("inserts thread 1");
        conn.execute(
            "INSERT INTO threads VALUES (?1, ?2, '110', '210', ?3, 'vscode', 'gateway', NULL, 0)",
            params!["thread-2", "Recent Title", "/tmp/project-two"],
        )
        .expect("inserts thread 2");
        conn.execute(
            "INSERT INTO threads VALUES (?1, ?2, '120', '220', ?3, 'vscode', 'gateway', NULL, 1)",
            params!["thread-3", "Archived Title", "/tmp/project-three"],
        )
        .expect("inserts thread 3");
        drop(conn);
        fs::write(
            home.join("session_index.jsonl"),
            r#"{"id":"thread-1","thread_name":"Index Title","updated_at":"2026-01-01T00:00:00Z"}
"#,
        )
        .expect("writes session index");

        let report = sync_local_thread_catalog(&home).expect("sync succeeds");
        assert_eq!(report.threads_seen, 2);
        assert_eq!(report.catalog_targets, 3);
        assert_eq!(report.catalog_inserted, 2);

        let catalog_db =
            Connection::open(home.join("sqlite").join("codex.db")).expect("opens catalog db");
        let rows = catalog_db
            .query_row("SELECT COUNT(*) FROM local_thread_catalog", [], |row| {
                row.get::<_, i64>(0)
            })
            .expect("counts catalog rows");
        assert_eq!(rows, 2);
        let title = catalog_db
            .query_row(
                "SELECT display_title FROM local_thread_catalog WHERE thread_id = 'thread-1'",
                [],
                |row| row.get::<_, String>(0),
            )
            .expect("reads indexed title");
        assert_eq!(title, "Index Title");
        let archived = catalog_db
            .query_row(
                "SELECT COUNT(*) FROM local_thread_catalog WHERE thread_id = 'thread-3'",
                [],
                |row| row.get::<_, i64>(0),
            )
            .expect("counts archived thread");
        assert_eq!(archived, 0);
        let initial_complete = catalog_db
            .query_row(
                "SELECT initial_build_complete FROM local_thread_catalog_sync_state WHERE host_id = 'local'",
                [],
                |row| row.get::<_, i64>(0),
            )
            .expect("reads sync state");
        assert_eq!(initial_complete, 1);

        catalog_db
            .execute(
                "UPDATE local_thread_catalog_sync_state SET initial_build_complete = 0 WHERE host_id = 'local'",
                [],
            )
            .expect("marks sync incomplete");
        drop(catalog_db);
        let second_report = sync_local_thread_catalog(&home).expect("second sync succeeds");
        assert_eq!(second_report.catalog_inserted, 0);
        assert_eq!(second_report.catalog_updated, 0);
        let catalog_db =
            Connection::open(home.join("sqlite").join("codex.db")).expect("reopens catalog db");
        let repaired_complete = catalog_db
            .query_row(
                "SELECT initial_build_complete FROM local_thread_catalog_sync_state WHERE host_id = 'local'",
                [],
                |row| row.get::<_, i64>(0),
            )
            .expect("reads repaired sync state");
        assert_eq!(repaired_complete, 1);
        for db_name in ["codex.db", "codex-dev.db", "state_5.sqlite"] {
            let catalog_db = Connection::open(home.join("sqlite").join(db_name))
                .expect("opens mirrored catalog db");
            let rows = catalog_db
                .query_row("SELECT COUNT(*) FROM local_thread_catalog", [], |row| {
                    row.get::<_, i64>(0)
                })
                .expect("counts mirrored catalog rows");
            assert_eq!(rows, 2, "{db_name} should mirror the sidebar catalog");
        }
    }

    #[test]
    fn sync_local_thread_catalog_writes_windows_native_threads_table() {
        let unique = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .expect("clock is after unix epoch")
            .as_nanos();
        let home =
            std::env::temp_dir().join(format!("codex-gateway-lite-native-thread-test-{unique}"));
        let sqlite_dir = home.join("sqlite");
        fs::create_dir_all(&sqlite_dir).expect("creates sqlite dir");
        fs::write(home.join("config.toml"), r#"model_provider = "CPA""#)
            .expect("writes config provider");
        let sessions_dir = home.join("sessions").join("2026").join("07").join("08");
        fs::create_dir_all(&sessions_dir).expect("creates sessions dir");
        let rollout_path = sessions_dir.join("rollout-test.jsonl");
        fs::write(
            &rollout_path,
            r#"{"timestamp":"2026-07-08T00:00:00Z","type":"session_meta","payload":{"id":"local:thread-native","model_provider":"custom","cwd":"D:\\codex-gateway-lite","source":"vscode"}}"#
                .to_string()
                + "\n",
        )
        .expect("writes rollout session meta");

        let native_db = Connection::open(home.join("state_5.sqlite")).expect("opens native db");
        native_db
            .execute_batch(
                r#"
CREATE TABLE threads (
  id TEXT PRIMARY KEY,
  rollout_path TEXT,
  created_at INTEGER,
  created_at_ms INTEGER,
  updated_at INTEGER,
  updated_at_ms INTEGER,
  cwd TEXT,
  title TEXT,
  first_user_message TEXT,
  preview TEXT,
  archived INTEGER,
  archived_at INTEGER,
  recency_at INTEGER,
  recency_at_ms INTEGER,
  thread_source TEXT,
  source TEXT,
  model_provider TEXT,
  git_branch TEXT
);
"#,
            )
            .expect("creates native threads table");
        drop(native_db);

        let source_db = Connection::open(sqlite_dir.join("custom.sqlite3")).expect("opens source");
        source_db
            .execute_batch(
                r#"
CREATE TABLE threads (
  id TEXT PRIMARY KEY,
  title TEXT,
  preview TEXT,
  first_user_message TEXT,
  rollout_path TEXT,
  created_at INTEGER,
  updated_at INTEGER,
  recency_at INTEGER,
  cwd TEXT,
  source TEXT,
  model_provider TEXT,
  git_branch TEXT,
  archived INTEGER
);
"#,
            )
            .expect("creates source threads table");
        source_db
            .execute(
                "INSERT INTO threads VALUES (?1, ?2, ?3, ?4, ?5, 1700000000, 1700000100, 1700000200, ?6, 'vscode', 'custom', 'main', 0)",
                params![
                    "local:thread-native",
                    "Imported Title",
                    "Preview from source",
                    "First user prompt",
                    rollout_path.to_string_lossy().as_ref(),
                    r#"\\?\D:\codex-gateway-lite"#,
                ],
            )
            .expect("inserts source thread");
        source_db
            .execute(
                "INSERT INTO threads VALUES (?1, ?2, NULL, NULL, NULL, 1700000000, 1700000300, 1700000300, ?3, 'vscode', 'gateway', NULL, 1)",
                params!["local:archived", "Archived Title", r#"D:\archived"#],
            )
            .expect("inserts archived source thread");
        drop(source_db);

        let report = sync_local_thread_catalog(&home).expect("sync succeeds");
        assert_eq!(report.threads_seen, 1);
        assert_eq!(report.catalog_targets, 4);
        assert_eq!(report.catalog_inserted, 1);

        let native_db = Connection::open(home.join("state_5.sqlite")).expect("reopens native db");
        let row = native_db
            .query_row(
                "SELECT title, preview, first_user_message, rollout_path, cwd, archived, archived_at, thread_source, source, model_provider, git_branch, recency_at, recency_at_ms, typeof(recency_at), updated_at, typeof(updated_at), created_at, typeof(created_at) FROM threads WHERE id = 'local:thread-native'",
                [],
                |row| {
                    Ok((
                        row.get::<_, String>(0)?,
                        row.get::<_, String>(1)?,
                        row.get::<_, String>(2)?,
                        row.get::<_, String>(3)?,
                        row.get::<_, String>(4)?,
                        row.get::<_, i64>(5)?,
                        row.get::<_, Option<String>>(6)?,
                        row.get::<_, String>(7)?,
                        row.get::<_, String>(8)?,
                        row.get::<_, String>(9)?,
                        row.get::<_, String>(10)?,
                        row.get::<_, i64>(11)?,
                        row.get::<_, i64>(12)?,
                        row.get::<_, String>(13)?,
                        row.get::<_, i64>(14)?,
                        row.get::<_, String>(15)?,
                        row.get::<_, i64>(16)?,
                        row.get::<_, String>(17)?,
                    ))
                },
            )
            .expect("reads native thread");
        assert_eq!(row.0, "Imported Title");
        assert_eq!(row.1, "Preview from source");
        assert_eq!(row.2, "First user prompt");
        assert_eq!(row.3, rollout_path.to_string_lossy());
        assert_eq!(row.4, r#"\\?\D:\codex-gateway-lite"#);
        assert_eq!(row.5, 0);
        assert_eq!(row.6, None);
        assert_eq!(row.7, "user");
        assert_eq!(row.8, "vscode");
        assert_eq!(row.9, "CPA");
        assert_eq!(row.10, "main");
        assert_eq!(row.11, 1_700_000_200);
        assert_eq!(row.12, 1_700_000_200_000);
        assert_eq!(row.13, "integer");
        assert_eq!(row.14, 1_700_000_200);
        assert_eq!(row.15, "integer");
        assert_eq!(row.16, 1_700_000_000);
        assert_eq!(row.17, "integer");
        let rollout_text = fs::read_to_string(&rollout_path).expect("reads rewritten rollout");
        let first_line = rollout_text.lines().next().expect("has first line");
        let session_meta: Value = serde_json::from_str(first_line).expect("parses session meta");
        assert_eq!(
            session_meta
                .get("payload")
                .and_then(|payload| payload.get("model_provider"))
                .and_then(Value::as_str),
            Some("CPA")
        );
        let archived_count = native_db
            .query_row(
                "SELECT COUNT(*) FROM threads WHERE id = 'local:archived'",
                [],
                |row| row.get::<_, i64>(0),
            )
            .expect("counts archived native rows");
        assert_eq!(archived_count, 0);
    }

    #[test]
    fn sync_local_thread_catalog_discovers_current_and_legacy_session_dbs() {
        let unique = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .expect("clock is after unix epoch")
            .as_nanos();
        let home =
            std::env::temp_dir().join(format!("codex-gateway-lite-catalog-scan-test-{unique}"));
        let sqlite_dir = home.join("sqlite");
        fs::create_dir_all(&sqlite_dir).expect("creates sqlite dir");

        create_thread_source_db(
            &sqlite_dir.join("custom.sqlite3"),
            "thread-new",
            "Custom Current",
            300,
            "/tmp/current",
        );
        create_thread_source_db(
            &sqlite_dir.join("codex-dev.db"),
            "thread-dup",
            "Stale Dev Copy",
            100,
            "/tmp/stale",
        );
        create_thread_source_db(
            &home.join("state_5.sqlite"),
            "thread-dup",
            "Fresh Legacy Copy",
            400,
            "/tmp/fresh",
        );

        let target_db = sqlite_dir.join("custom-app.sqlite3");
        let target = Connection::open(&target_db).expect("opens custom app db");
        target
            .execute(
                "CREATE TABLE local_thread_catalog_sync_state (host_id TEXT PRIMARY KEY)",
                [],
            )
            .expect("creates custom target marker");
        drop(target);

        let report = sync_local_thread_catalog(&home).expect("sync succeeds");
        assert_eq!(report.threads_seen, 2);
        assert_eq!(report.catalog_targets, 5);

        for db_name in [
            "codex.db",
            "codex-dev.db",
            "custom.sqlite3",
            "custom-app.sqlite3",
        ] {
            let catalog_db =
                Connection::open(sqlite_dir.join(db_name)).expect("opens catalog target db");
            let rows = catalog_db
                .query_row("SELECT COUNT(*) FROM local_thread_catalog", [], |row| {
                    row.get::<_, i64>(0)
                })
                .expect("counts catalog rows");
            assert_eq!(rows, 2, "{db_name} should receive mirrored catalog rows");
            let title = catalog_db
                .query_row(
                    "SELECT display_title FROM local_thread_catalog WHERE thread_id = 'thread-dup'",
                    [],
                    |row| row.get::<_, String>(0),
                )
                .expect("reads deduped title");
            assert_eq!(title, "Fresh Legacy Copy");
        }
    }

    fn create_thread_source_db(path: &Path, id: &str, title: &str, updated_at: i64, cwd: &str) {
        let db = Connection::open(path).expect("opens source db");
        db.execute_batch(
            r#"
CREATE TABLE threads (
  id TEXT PRIMARY KEY,
  title TEXT,
  created_at INTEGER,
  updated_at INTEGER,
  cwd TEXT,
  source TEXT,
  model_provider TEXT,
  git_branch TEXT,
  archived INTEGER
);
"#,
        )
        .expect("creates source threads table");
        db.execute(
            "INSERT INTO threads VALUES (?1, ?2, ?3, ?4, ?5, 'vscode', 'gateway', NULL, 0)",
            params![id, title, updated_at - 10, updated_at, cwd],
        )
        .expect("inserts source thread");
    }

    #[test]
    fn lite_model_catalog_preserves_context_metadata_for_injected_models() {
        let unique = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .expect("clock is after unix epoch")
            .as_nanos();
        let home = std::env::temp_dir().join(format!("codex-gateway-lite-test-{unique}"));
        let catalog_dir = home.join("model-catalogs");
        fs::create_dir_all(&catalog_dir).expect("creates catalog dir");
        fs::write(
            home.join("config.toml"),
            r#"model_provider = "gateway"
model = "claude-sonnet-5"
model_catalog_json = "model-catalogs/gateway.json"
"#,
        )
        .expect("writes config");
        fs::write(
            catalog_dir.join("gateway.json"),
            r#"{
  "models": [
    {
      "id": "claude-sonnet-5",
      "slug": "claude-sonnet-5",
      "model": "claude-sonnet-5",
      "display_name": "Claude Sonnet 5",
      "base_instructions": "large unrelated prompt",
      "context_window": 1000000,
      "max_context_window": 1000000,
      "effective_context_window_percent": 100,
      "auto_compact_token_limit": 650000
    }
  ]
}"#,
        )
        .expect("writes catalog");

        let catalog = lite_model_catalog_from_home(&home);
        let models = catalog
            .get("models")
            .and_then(Value::as_array)
            .expect("models array");
        let names = catalog
            .get("model_names")
            .and_then(Value::as_array)
            .expect("model names array");

        assert_eq!(names[0].as_str(), Some("claude-sonnet-5"));
        assert_eq!(models[0]["model"].as_str(), Some("claude-sonnet-5"));
        assert_eq!(models[0]["context_window"].as_u64(), Some(1_000_000));
        assert_eq!(
            models[0]["auto_compact_token_limit"].as_u64(),
            Some(650_000)
        );
        assert!(models[0].get("base_instructions").is_none());
        assert_eq!(models[0]["hidden"].as_bool(), Some(false));
    }

    #[test]
    fn lite_model_whitelist_script_merges_context_metadata_and_dynamic_assets() {
        assert!(LITE_MODEL_WHITELIST_SCRIPT.contains("modelMetadata(modelName)"));
        assert!(
            LITE_MODEL_WHITELIST_SCRIPT
                .contains("function modelEntryLooksPatchable(value, allowBareModel = false)")
        );
        assert!(LITE_MODEL_WHITELIST_SCRIPT.contains("allowBareModelArrays"));
        assert!(LITE_MODEL_WHITELIST_SCRIPT.contains("model_auto_compact_token_limit"));
        assert!(LITE_MODEL_WHITELIST_SCRIPT.contains("autoCompactTokenLimit"));
        assert!(LITE_MODEL_WHITELIST_SCRIPT.contains("assignModelDescriptor"));
        assert!(LITE_MODEL_WHITELIST_SCRIPT.contains("codexAppAssetUrlByText"));
        assert!(LITE_MODEL_WHITELIST_SCRIPT.contains("assetReferencesFromText"));
        assert!(LITE_MODEL_WHITELIST_SCRIPT.contains("list-models-for-host"));
        assert!(LITE_MODEL_WHITELIST_SCRIPT.contains("sendMessageFromView"));
        assert!(LITE_MODEL_WHITELIST_SCRIPT.contains("patchOutboundModelRequestMessage"));
        assert!(LITE_MODEL_WHITELIST_SCRIPT.contains("appServerPatchUnavailable = false"));
        assert!(LITE_MODEL_WHITELIST_SCRIPT.contains("appServerPatchUnavailable = true"));
        assert!(LITE_MODEL_WHITELIST_SCRIPT.contains("const SCRIPT_VERSION = 8"));
        assert!(LITE_MODEL_WHITELIST_SCRIPT.contains("state.observer?.disconnect?.()"));
        assert!(LITE_MODEL_WHITELIST_SCRIPT.contains("window.clearTimeout(state.refreshTimer)"));
        assert!(LITE_MODEL_WHITELIST_SCRIPT.contains("restoreAppServerModelRequestPatches(state)"));
        assert!(LITE_MODEL_WHITELIST_SCRIPT.contains("state.appServerPatchInstalled = false"));
        assert!(LITE_MODEL_WHITELIST_SCRIPT.contains("SEND_REQUEST_PATCH_MARK"));
        assert!(LITE_MODEL_WHITELIST_SCRIPT.contains("function appServerPatchedClientsHealthy()"));
        assert!(
            LITE_MODEL_WHITELIST_SCRIPT.contains(
                "if (state.appServerPatchInstalled && !appServerPatchedClientsHealthy())"
            )
        );
        assert!(
            LITE_MODEL_WHITELIST_SCRIPT
                .contains("__codexGatewayLiteModelRequestPatch === SEND_REQUEST_PATCH_MARK")
        );
        assert!(
            LITE_MODEL_WHITELIST_SCRIPT
                .contains("function patchAppServerModelResult(method, result)")
        );
        assert!(!LITE_MODEL_WHITELIST_SCRIPT.contains("dynamic_config|model/i"));
        assert!(LITE_MODEL_WHITELIST_SCRIPT.contains("REQUEST_GUARD"));
        assert!(LITE_MODEL_WHITELIST_SCRIPT.contains("hasOwn(value, \"defaultModel\")"));
        assert!(LITE_MODEL_WHITELIST_SCRIPT.contains("state.requestIds.add(REQUEST_GUARD)"));
        assert!(!LITE_MODEL_WHITELIST_SCRIPT.contains("installModelJsonResponsePatch"));
        assert!(!LITE_MODEL_WHITELIST_SCRIPT.contains("payloadMayContainModelList"));
        assert!(!LITE_MODEL_WHITELIST_SCRIPT.contains("codexGatewayLitePatchedResponseJson"));
        assert!(!LITE_MODEL_WHITELIST_SCRIPT.contains("patchObjectGraphForModels"));
        assert!(!LITE_MODEL_WHITELIST_SCRIPT.contains("patchReactModelState"));
        assert!(!LITE_MODEL_WHITELIST_SCRIPT.contains("reactFiberKeys"));
        assert!(!LITE_MODEL_WHITELIST_SCRIPT.contains("__reactFiber"));
        assert!(!LITE_MODEL_WHITELIST_SCRIPT.contains("return [document.body"));
        assert!(!LITE_MODEL_WHITELIST_SCRIPT.contains("if (value.defaultModel == null"));
    }

    #[test]
    fn plan_ui_script_uses_stable_dock_and_snapshot_instead_of_hover_rebinding() {
        assert!(PLAN_UI_SCRIPT.contains("const SCRIPT_VERSION = 44"));
        assert!(PLAN_UI_SCRIPT.contains("codex-gateway-lite-plan-ui-dock"));
        assert!(PLAN_UI_SCRIPT.contains("codex-gateway-lite-plan-ui-snapshots-v1"));
        assert!(PLAN_UI_SCRIPT.contains("__codexGatewayLitePlanUiExternalSnapshots"));
        assert!(PLAN_UI_SCRIPT.contains("STORAGE_LIMIT = 200"));
        assert!(PLAN_UI_SCRIPT.contains("data-codex-gateway-lite-native-pill"));
        assert!(PLAN_UI_SCRIPT.contains("data-codex-gateway-lite-native-pill-thread-id"));
        assert!(PLAN_UI_SCRIPT.contains("__codexGatewayLitePlanUiExternalSnapshot"));
        assert!(PLAN_UI_SCRIPT.contains("lastSnapshot"));
        assert!(PLAN_UI_SCRIPT.contains("lastNativePillSeenAt"));
        assert!(PLAN_UI_SCRIPT.contains("snapshotsByThread"));
        assert!(PLAN_UI_SCRIPT.contains("currentThreadId"));
        assert!(PLAN_UI_SCRIPT.contains("currentConversationThreadId"));
        assert!(PLAN_UI_SCRIPT.contains("currentVisibleTitleThreadId"));
        assert!(PLAN_UI_SCRIPT.contains("currentSidebarThreadId"));
        assert!(PLAN_UI_SCRIPT.contains("currentSidebarThreadInfo"));
        assert!(PLAN_UI_SCRIPT.contains("shouldPreferSidebarThreadId"));
        assert!(PLAN_UI_SCRIPT.contains("knownSnapshotForThreadId"));
        assert!(PLAN_UI_SCRIPT.contains("snapshotAliasIdsForThread"));
        assert!(PLAN_UI_SCRIPT.contains(
            "[data-above-composer-portal=\"true\"][data-above-composer-conversation-id]"
        ));
        assert!(PLAN_UI_SCRIPT.contains("snapshotForThread"));
        assert!(PLAN_UI_SCRIPT.contains("externalSnapshotForThread"));
        assert!(PLAN_UI_SCRIPT.contains("rememberThreadSnapshot"));
        assert!(PLAN_UI_SCRIPT.contains("storedSnapshotForThread"));
        assert!(PLAN_UI_SCRIPT.contains("persistThreadSnapshots"));
        assert!(PLAN_UI_SCRIPT.contains("localStorage.setItem(STORAGE_KEY"));
        assert!(PLAN_UI_SCRIPT.contains("localStorage.getItem(STORAGE_KEY"));
        assert!(PLAN_UI_SCRIPT.contains("lastNativePillSeenAtByThread"));
        assert!(PLAN_UI_SCRIPT.contains("lastRenderSignature"));
        assert!(PLAN_UI_SCRIPT.contains("nativePillHost"));
        assert!(PLAN_UI_SCRIPT.contains("planPillSurfaceScore"));
        assert!(PLAN_UI_SCRIPT.contains("snapshotFromExternal"));
        assert!(PLAN_UI_SCRIPT.contains("blockingOverlayActive"));
        assert!(PLAN_UI_SCRIPT.contains("topLayerOwns"));
        assert!(PLAN_UI_SCRIPT.contains("cgl-plan-spinner"));
        assert!(PLAN_UI_SCRIPT.contains("transform-box: fill-box"));
        assert!(PLAN_UI_SCRIPT.contains(".cgl-plan-icon .animate-spin"));
        assert!(PLAN_UI_SCRIPT.contains("safeIconHtml(row.iconHtml) || iconForStatus"));
        assert!(PLAN_UI_SCRIPT.contains("colorStyle(row.textColor)"));
        assert!(PLAN_UI_SCRIPT.contains("color\\(\\s*srgb"));
        assert!(PLAN_UI_SCRIPT.contains("isPlanPillSurface"));
        assert!(PLAN_UI_SCRIPT.contains("planPillSurfaceScore(el) >= 3"));
        assert!(PLAN_UI_SCRIPT.contains("currentThreadRoot"));
        assert!(PLAN_UI_SCRIPT.contains("root.querySelectorAll"));
        assert!(PLAN_UI_SCRIPT.contains("[data-avatar-overlay-content-frame]"));
        assert!(PLAN_UI_SCRIPT.contains("node.contains(other)"));
        assert!(PLAN_UI_SCRIPT.contains("rect.width > maxWidth"));
        assert!(PLAN_UI_SCRIPT.contains("const pills = nativePills().slice(0, 1)"));
        assert!(PLAN_UI_SCRIPT.contains("pillSet.has(node)"));
        assert!(PLAN_UI_SCRIPT.contains("metaPartsFromPill"));
        assert!(PLAN_UI_SCRIPT.contains("colorForToken"));
        assert!(PLAN_UI_SCRIPT.contains("fallbackDeltaColor"));
        assert!(PLAN_UI_SCRIPT.contains("parseNativePillValue"));
        assert!(PLAN_UI_SCRIPT.contains("extractFileChangeDetail"));
        assert!(PLAN_UI_SCRIPT.contains("nearbyPillDetail(pill, progress)"));
        assert!(PLAN_UI_SCRIPT.contains("function nativeTurnFromPill(pill)"));
        assert!(PLAN_UI_SCRIPT.contains("function turnInfoFromProps(props)"));
        assert!(PLAN_UI_SCRIPT.contains("function latestTodoFromTurn(turnInfo)"));
        assert!(
            PLAN_UI_SCRIPT
                .contains("function todoSnapshotFromNativeTurn(base, threadId, turnInfo)")
        );
        assert!(PLAN_UI_SCRIPT.contains("function snapshotContextMatches(snapshot, base)"));
        assert!(PLAN_UI_SCRIPT.contains("item?.type !== \"todo-list\""));
        assert!(PLAN_UI_SCRIPT.contains("cleared: item.plan.length === 0"));
        assert!(PLAN_UI_SCRIPT.contains("source: \"native-turn-todo-cleared\""));
        assert!(PLAN_UI_SCRIPT.contains("sourceTurnId"));
        assert!(PLAN_UI_SCRIPT.contains("sourceTodoId"));
        assert!(PLAN_UI_SCRIPT.contains("source: \"native-turn-todo\""));
        assert!(PLAN_UI_SCRIPT.contains("const turnInfo = nativeTurnFromPill(pill)"));
        assert!(PLAN_UI_SCRIPT.contains("if (nativeTurn) return nativeTurn"));
        assert!(PLAN_UI_SCRIPT.contains(
            "previousSnapshot?.items?.length && snapshotContextMatches(previousSnapshot, base)"
        ));
        assert!(PLAN_UI_SCRIPT.contains("awaiting-current-todo"));
        assert!(PLAN_UI_SCRIPT.contains("dock.dataset.cglPlanSourceTurnId"));
        assert!(PLAN_UI_SCRIPT.contains("个文件已更改"));
        assert!(PLAN_UI_SCRIPT.contains("function appBusy()"));
        assert!(PLAN_UI_SCRIPT.contains("function mediaPreviewControlsInside(node)"));
        assert!(PLAN_UI_SCRIPT.contains("button.getAttribute(\"aria-label\")"));
        assert!(PLAN_UI_SCRIPT.contains("rowsSettled(rows) || appBusy()"));
        assert!(PLAN_UI_SCRIPT.contains("function progressComplete(progress)"));
        assert!(PLAN_UI_SCRIPT.contains("function nativeSourceMissingMs(threadId, snapshot)"));
        assert!(
            PLAN_UI_SCRIPT
                .contains("function shouldSettleMissingNativeSnapshot(threadId, snapshot, rows)")
        );
        assert!(PLAN_UI_SCRIPT.contains("STALE_RUNNING_SETTLE_MS"));
        assert!(PLAN_UI_SCRIPT.contains("__codexGatewayLitePlanUiTimer"));
        assert!(PLAN_UI_SCRIPT.contains("window.setInterval(scheduleApply, 5000)"));
        assert!(PLAN_UI_SCRIPT.contains("const SCRIPT_VERSION = 44"));
        assert!(PLAN_UI_SCRIPT.contains("function scheduleApplyImmediate()"));
        assert!(PLAN_UI_SCRIPT.contains("function mutationTouchesRightRail(mutations)"));
        assert!(
            PLAN_UI_SCRIPT
                .contains("const observer = new MutationObserver(scheduleApplyForMutations)")
        );
        assert!(
            PLAN_UI_SCRIPT
                .contains("window.__codexGatewayLitePlanUiResize = scheduleApplyImmediate")
        );
        assert!(PLAN_UI_ACTIVE_HISTORY_SEED_RETRY_SECS >= 10);
        assert!(
            PLAN_UI_ACTIVE_THREAD_NEEDS_SEED_SCRIPT
                .contains("__codexGatewayLitePlanUiActiveSeedRequest")
        );
        assert!(PLAN_UI_SCRIPT.contains("activeHistorySeedRequest"));
        assert!(PLAN_UI_SCRIPT.contains(
            "window.__codexGatewayLitePlanUiActiveSeedRequest = activeHistorySeedRequest"
        ));
        assert!(PLAN_UI_SCRIPT.contains("RIGHT_RAIL_FOLLOW_FRAME_MS"));
        assert!(PLAN_UI_SCRIPT.contains("function scheduleApplyBurst()"));
        assert!(PLAN_UI_SCRIPT.contains("continueRightRailFollow()"));
        assert!(
            PLAN_UI_SCRIPT.contains(
                "window.addEventListener(\"transitionend\", handleRightPanelDismissEvent"
            )
        );
        assert!(!PLAN_UI_SCRIPT.contains("PLAN_UI_HISTORY_RESEED_INTERVAL_SECS"));
        assert!(!PLAN_UI_SCRIPT.contains("characterData: true"));
        assert!(PLAN_UI_REINJECT_INTERVAL_SECS >= 30);
        assert!(PLAN_UI_SCRIPT.contains("^(打开位置|打开图片|审查)$"));
        assert!(PLAN_TOOLTIP_SAMPLE_READ_SCRIPT.contains("^(打开位置|打开图片|审查)$"));
        assert!(!PLAN_UI_SCRIPT.contains("if (/打开位置|打开图片|审查/.test(item))"));
        assert!(
            !PLAN_TOOLTIP_SAMPLE_READ_SCRIPT.contains("if (/打开位置|打开图片|审查/.test(item))")
        );
        assert!(PLAN_UI_SCRIPT.contains("cgl-plan-meta-spinner"));
        assert!(PLAN_UI_SCRIPT.contains("aggregatePlanStatus"));
        assert!(PLAN_UI_SCRIPT.contains("iconForStatus(aggregatePlanStatus(rows))"));
        assert!(PLAN_UI_SCRIPT.contains("renderMeta(snapshot, rows)"));
        assert!(PLAN_UI_SCRIPT.contains("pendingRefresh"));
        assert!(PLAN_UI_SCRIPT.contains("last-snapshot-pending"));
        assert!(PLAN_UI_SCRIPT.contains("retargetRowsForProgress"));
        assert!(PLAN_UI_SCRIPT.contains("rowsCompatibleWithProgress"));
        assert!(PLAN_UI_SCRIPT.contains("retargeted-last-snapshot"));
        assert!(PLAN_UI_SCRIPT.contains("external-retargeted"));
        assert!(PLAN_UI_SCRIPT.contains("last-snapshot-awaiting-current-tooltip"));
        assert!(PLAN_UI_SCRIPT.contains("awaiting-current-tooltip"));
        assert!(PLAN_UI_SCRIPT.contains("snapshotAgeMs(previousSnapshot) < 5_000"));
        assert!(PLAN_UI_SCRIPT.contains("\"native-source-gone\""));
        assert!(PLAN_UI_SCRIPT.contains("\"stale-native-source-gone\""));
        assert!(PLAN_UI_SCRIPT.contains("renderSignature"));
        assert!(PLAN_UI_SCRIPT.contains("dock.dataset.cglPlanSignature"));
        assert!(PLAN_UI_SCRIPT.contains("dock.innerHTML = \"\""));
        assert!(PLAN_UI_SCRIPT.contains("if (!snapshot)"));
        assert!(PLAN_UI_SCRIPT.contains("if (!placeDock(dock))"));
        assert!(PLAN_UI_SCRIPT.contains("el.closest?.('[role=\"tooltip\"]')"));
        assert!(PLAN_UI_SCRIPT.contains("function rightRailFallbackPanelInfo()"));
        assert!(PLAN_UI_SCRIPT.contains("hasOutputSourcePair"));
        assert!(PLAN_UI_SCRIPT.contains("candidate.hasOutputSourcePair ? rect.bottom : rect.top"));
        assert!(!PLAN_UI_SCRIPT.contains("function defaultRightRailDockInfo()"));
        assert!(PLAN_UI_SCRIPT.contains("const anchor = environmentPanelInfo();"));
        assert!(PLAN_UI_SCRIPT.contains("if (!anchor) return false;"));
        assert!(PLAN_UI_SCRIPT.contains("Math.max(72, window.innerHeight - 96)"));
        assert!(PLAN_UI_SCRIPT.contains("Math.round(rect.width - 12)"));
        assert!(!PLAN_UI_SCRIPT.contains("Math.round(rect.width - 24)"));
        assert!(PLAN_UI_SCRIPT.contains("detail: snapshot.detail || base.detail || \"\""));
        assert!(PLAN_UI_SCRIPT.contains("renderDock(snapshot || snapshotForThread(threadId))"));
        assert!(PLAN_UI_SCRIPT.contains("rightPanelDismissedThreadId"));
        assert!(PLAN_UI_SCRIPT.contains("rightPanelDismissalActive"));
        assert!(PLAN_UI_SCRIPT.contains("dock.dataset.cglPlanHiddenBy = \"blocking-overlay\""));
        assert!(PLAN_UI_SCRIPT.contains("handleRightPanelDismissEvent"));
        assert!(PLAN_UI_SCRIPT.contains("hideDockForRightPanelTarget"));
        assert!(PLAN_UI_SCRIPT.contains("rightSideExpandedContentInfoFromPoint"));
        assert!(PLAN_UI_SCRIPT.contains("activeRightSideExpandedContentInfo"));
        assert!(PLAN_UI_SCRIPT.contains("RIGHT_PANEL_DISMISS_GRACE_MS"));
        assert!(
            PLAN_UI_SCRIPT.contains(
                "document.querySelectorAll(\"main,aside,section,article,div,iframe,webview,[role='tabpanel']\")"
            )
        );
        assert!(PLAN_UI_SCRIPT.contains("state.rightPanelDismissedAt = Date.now()"));
        assert!(
            PLAN_UI_SCRIPT
                .contains("window.addEventListener(\"pointerdown\", handleRightPanelDismissEvent")
        );
        assert!(
            PLAN_UI_SCRIPT
                .contains("window.addEventListener(\"click\", handleRightPanelDismissEvent")
        );
        assert!(PLAN_UI_SCRIPT.contains("window.__codexGatewayLitePlanUiRightPanelDismiss"));
        assert!(PLAN_UI_SCRIPT.contains("dock.dataset.cglPlanHiddenBy = \"right-panel-active\""));
        assert!(
            PLAN_UI_SCRIPT
                .contains("snapshot?.threadId && snapshot.threadId !== currentThreadId()")
        );
        assert!(
            !PLAN_UI_SCRIPT
                .contains(".app-shell-left-panel, [data-thread-find-target='conversation']")
        );
        assert!(!PLAN_UI_SCRIPT.contains("snapshotFromTooltip"));
        assert!(!PLAN_UI_SCRIPT.contains("native-tooltip"));
        assert!(!PLAN_UI_SCRIPT.contains("SUPPRESSED_TOOLTIP_ATTR"));
        assert!(!PLAN_UI_SCRIPT.contains("suppressNativePlanTooltips"));
        assert!(!PLAN_UI_SCRIPT.contains("<span>任务清单</span>"));
        assert!(!PLAN_UI_SCRIPT.contains("[${SOURCE_ATTR}=\"true\"] {"));
        assert!(!PLAN_UI_SCRIPT.contains("installHoverGuard"));
        assert!(!PLAN_UI_SCRIPT.contains("cleanupUnsafeMarks"));
        assert!(!PLAN_UI_SCRIPT.contains("[${MARK}=\"pill\"]"));
        assert!(!PLAN_UI_SCRIPT.contains("[${MARK}=\"container\"]"));
    }

    #[test]
    fn plan_ui_history_seed_runs_for_initial_and_reload_injection() {
        assert!(PLAN_UI_INITIAL_HISTORY_SEED);
        assert!(should_seed_plan_ui_history_on_reinject(true));
        assert!(!should_seed_plan_ui_history_on_reinject(false));
    }

    #[test]
    fn protocol_proxy_ignores_connections_closed_before_http_headers() {
        let early_close = anyhow::anyhow!("本地协议代理连接提前关闭");
        assert!(protocol_proxy_request_closed_before_headers(&early_close));

        let other = anyhow::anyhow!("读取本地协议代理请求失败");
        assert!(!protocol_proxy_request_closed_before_headers(&other));
    }

    #[test]
    fn responses_proxy_passthroughs_responses_sse_without_chat_fallback() {
        let source = include_str!("main.rs");
        assert!(source.contains("can_passthrough_responses_stream"));
        assert!(source.contains("upstream.wire_api == protocol_proxy::UpstreamWireApi::Responses"));
        let proxy_source = include_str!("protocol_proxy.rs");
        assert!(!proxy_source.contains("should_retry_responses_as_chat"));
        assert!(!proxy_source.contains("回退到 Chat Completions"));
    }

    #[test]
    fn agent_starts_local_protocol_proxy_only_when_needed() {
        let source = include_str!("main.rs");
        assert!(source.contains("fn provider_uses_local_proxy(config: &LiteConfig) -> bool"));
        assert!(
            source.contains(
                "|| parse_context_budget_token(&config.provider.context_budget).is_some()"
            )
        );
        assert!(source.contains("Responses 直连模式：不启动本地协议代理 127.0.0.1:57321"));
        assert!(source.contains("fn print_context_budget_notice(config: &LiteConfig)"));
        assert!(source.contains("Responses 保持直连，不启动本地代理"));
        let readme = include_str!("../README.md");
        assert!(
            readme.contains(
                "如果显式填写 `provider.contextBudget`，Codex 会改走本地代理 `http://127.0.0.1:57321/v1`，由 agent 先裁剪上下文再转发到 Responses 上游"
            )
        );
    }

    #[test]
    fn bootstrap_scripts_stop_agent_on_exit() {
        let macos_script = include_str!("../Codex Gateway Lite.command");
        assert!(macos_script.contains("cleanup_agent_on_exit"));
        assert!(macos_script.contains("trap cleanup_agent_on_exit EXIT INT TERM HUP"));
        assert!(macos_script.contains("AGENT_STARTED=1"));
        assert!(
            macos_script.contains("cargo run --quiet --manifest-path Cargo.toml -- stop-agent")
        );

        let windows_script = include_str!("../Codex Gateway Lite.ps1");
        assert!(windows_script.contains("function Stop-AgentOnExit"));
        assert!(windows_script.contains("$script:AgentStarted = $true"));
        assert!(windows_script.contains("} finally {"));
        assert!(windows_script.contains("$LiteBin stop-agent"));
        assert!(
            windows_script.contains("cargo build --quiet --release --manifest-path \"Cargo.toml\"")
        );
        assert!(windows_script.contains("function Test-InteractiveLiteCommand"));
        assert!(windows_script.contains("@(\"agent\", \"init\")"));
        assert!(windows_script.contains("if (Test-InteractiveLiteCommand $ArgsList)"));
        assert!(windows_script.contains("Project dir: $RepoRoot"));
        assert!(windows_script.contains("Config file: $ConfigFile"));
        assert!(!windows_script.contains("项目目录：$RepoRoot"));
        assert!(!windows_script.contains("配置文件：$ConfigFile"));
        assert!(windows_script.contains("function Add-CargoBinToPath"));
        assert!(windows_script.contains("Add-CargoBinToPath\n  if ((Test-Command cargo)"));
        assert!(windows_script.contains("function Ensure-CargoBinUserPath"));
        assert!(windows_script.contains("function Test-CargoDepsFresh"));
        assert!(windows_script.contains("target\\.codex-gateway-lite\\cargo-fetch.stamp"));
        assert!(windows_script.contains("Rust 依赖已就绪（跳过 cargo fetch）"));
    }

    #[test]
    fn agent_does_not_relaunch_codex_after_manual_close_pause() {
        assert!(should_retry_launch_codex(false, true));
        assert!(!should_retry_launch_codex(false, false));
        assert!(!should_retry_launch_codex(true, true));
    }

    #[test]
    fn codex_gateway_agent_pids_match_only_agent_processes() {
        let ps_output = "\
  100 /Users/demo/.codex-gateway-lite/bin/codex-gateway-lite agent --config /tmp/config.json
  101 /Users/demo/project/target/debug/codex-gateway-lite stop-agent
  102 /Users/demo/project/target/debug/codex-gateway-lite install-agent
  103 /Users/demo/project/target/debug/codex-gateway-lite agent --config /tmp/config.json
  104 /Applications/Codex.app/Contents/MacOS/Codex --remote-debugging-port=9229
  105 /Users/demo/old/nested/codex-gateway-lite/target/debug/codex-gateway-lite agent --config /tmp/config.json
  106 /bin/zsh /Users/demo/old/nested/codex-gateway-lite/Codex Gateway Lite.command
";
        let pids = codex_gateway_agent_pids(ps_output, 103);
        assert_eq!(pids, vec![100, 105]);
    }

    #[test]
    fn command_line_agent_match_handles_windows_paths_without_false_positives() {
        assert!(command_line_is_codex_gateway_agent(
            r#""C:\Users\demo\.codex-gateway-lite\bin\codex-gateway-lite.exe" agent --config C:\tmp\config.json"#
        ));
        assert!(command_line_is_codex_gateway_agent(
            r#"C:\repo\target\debug\codex-gateway-lite.exe agent --debug-port 9229"#
        ));
        assert!(!command_line_is_codex_gateway_agent(
            r#"C:\repo\target\debug\codex-gateway-lite.exe stop-agent"#
        ));
        assert!(!command_line_is_codex_gateway_agent(
            r#"C:\repo\target\debug\codex-gateway-lite.exe install-agent"#
        ));
        assert!(!command_line_is_codex_gateway_agent(
            r#"cargo run --quiet --manifest-path C:\repo\codex-gateway-lite\Cargo.toml -- agent --config C:\tmp\config.json"#
        ));
        assert!(!command_line_is_codex_gateway_agent(
            r#"powershell -File "C:\repo\Codex Gateway Lite.ps1""#
        ));
    }

    #[test]
    fn windows_tasklist_pid_exists_parses_csv_output() {
        let output = "\"codex-gateway-lite.exe\",\"1234\",\"Console\",\"1\",\"12,345 K\"\n\
\"Code.exe\",\"5678\",\"Console\",\"1\",\"200,000 K\"\n";
        assert!(windows_tasklist_pid_exists(output, 1234));
        assert!(!windows_tasklist_pid_exists(output, 4321));
        assert!(!windows_tasklist_pid_exists(
            "INFO: No tasks are running which match the specified criteria.",
            1234
        ));
    }

    #[test]
    fn windows_process_json_matches_only_agent_processes() {
        let processes = r#"[
  {
    "ProcessId": 200,
    "CommandLine": "\"C:\\Users\\demo\\.codex-gateway-lite\\bin\\codex-gateway-lite.exe\" agent --config C:\\tmp\\config.json"
  },
  {
    "ProcessId": 201,
    "CommandLine": "\"C:\\repo\\target\\debug\\codex-gateway-lite.exe\" stop-agent"
  },
  {
    "ProcessId": 202,
    "CommandLine": "\"C:\\repo\\target\\debug\\codex-gateway-lite.exe\" agent --debug-port 9229"
  },
  {
    "ProcessId": 203,
    "CommandLine": "powershell -File \"C:\\repo\\Codex Gateway Lite.ps1\""
  }
]"#;
        assert_eq!(
            codex_gateway_agent_pids_from_windows_process_json(processes, 202),
            vec![200]
        );

        let single = r#"{
  "ProcessId": "204",
  "CommandLine": "\"C:\\repo\\target\\debug\\codex-gateway-lite.exe\" agent --debug-port 9229"
}"#;
        assert_eq!(
            codex_gateway_agent_pids_from_windows_process_json(single, 0),
            vec![204]
        );

        let lower_case_fields = r#"{
  "pid": 205,
  "command": "\"C:\\repo\\target\\debug\\codex-gateway-lite.exe\" agent --debug-port 9229"
}"#;
        assert_eq!(
            codex_gateway_agent_pids_from_windows_process_json(lower_case_fields, 0),
            vec![205]
        );
    }

    #[test]
    fn windows_codex_app_process_json_matches_store_codex_only() {
        let processes = r#"[
  {
    "ProcessId": 310,
    "Name": "Codex.exe",
    "ExecutablePath": "C:\\Program Files\\WindowsApps\\OpenAI.Codex_26.623.13972.0_x64__2p2nqsd0c76g0\\app\\Codex.exe",
    "CommandLine": "\"C:\\Program Files\\WindowsApps\\OpenAI.Codex_26.623.13972.0_x64__2p2nqsd0c76g0\\app\\Codex.exe\""
  },
  {
    "ProcessId": 311,
    "Name": "codex-gateway-lite.exe",
    "ExecutablePath": "D:\\repo\\target\\debug\\codex-gateway-lite.exe",
    "CommandLine": "codex-gateway-lite.exe agent"
  },
  {
    "ProcessId": 312,
    "Name": "Code.exe",
    "ExecutablePath": "C:\\Users\\demo\\AppData\\Local\\Programs\\Microsoft VS Code\\Code.exe",
    "CommandLine": "Code.exe"
  }
]"#;
        assert_eq!(
            codex_app_pids_from_windows_process_json(processes),
            vec![310]
        );
    }

    #[test]
    fn windows_command_line_quotes_paths_and_embedded_quotes() {
        let args = vec![
            r#"C:\Program Files\Codex Gateway Lite\codex-gateway-lite.exe"#.to_string(),
            "agent".to_string(),
            "--config".to_string(),
            r#"C:\Users\demo\.codex-gateway-lite\config.json"#.to_string(),
            r#"quote"inside"#.to_string(),
        ];
        let command = windows_command_line(&args);
        assert!(
            command.contains(r#""C:\Program Files\Codex Gateway Lite\codex-gateway-lite.exe""#)
        );
        assert!(command.contains(" agent --config "));
        assert!(command.contains(r#""quote\"inside""#));
    }

    #[test]
    fn stale_codex_app_pids_matches_only_main_executable() {
        let marker = "/Applications/Codex.app/Contents/MacOS/";
        let ps_output = "\
  56351 /Applications/Codex.app/Contents/MacOS/Codex --remote-debugging-port=9229
  56358 /Applications/Codex.app/Contents/Frameworks/Codex Framework.framework/Versions/149.0.0/Helpers/Codex (Service).app/Contents/MacOS/Codex (Service) --type=gpu-process
  56393 /Applications/Codex.app/Contents/Frameworks/Codex Framework.framework/Versions/149.0.0/Helpers/Codex (Renderer).app/Contents/MacOS/Codex (Renderer) --type=renderer
  12345 /Applications/Slack.app/Contents/MacOS/Slack
not-a-pid garbage line without valid pid
";
        let pids = stale_codex_app_pids(ps_output, marker);
        assert_eq!(pids, vec![56351]);
    }

    #[test]
    fn stale_codex_app_pids_empty_when_no_match() {
        let marker = "/Applications/Codex.app/Contents/MacOS/";
        let ps_output = "  12345 /Applications/Slack.app/Contents/MacOS/Slack\n";
        assert!(stale_codex_app_pids(ps_output, marker).is_empty());
    }

    #[cfg(target_os = "macos")]
    #[test]
    fn macos_codex_profile_name_tracks_codex_home() {
        assert_eq!(codex_profile_name_from_home_component(".codex"), "Codex");
        assert_eq!(
            codex_profile_name_from_home_component(".codex-gateway"),
            "Codex-Gateway"
        );
        assert_eq!(
            codex_profile_name_from_home_component(".codex-gateway-lite"),
            "Codex-Gateway-Lite"
        );
    }

    #[cfg(target_os = "macos")]
    #[test]
    fn macos_codex_process_profile_match_requires_home_and_user_data_dir() {
        let codex_home = Path::new("/Users/demo/.codex-gateway");
        let user_data_dir = Path::new("/Users/demo/Library/Application Support/Codex-Gateway");
        let matching = "/Applications/Codex.app/Contents/MacOS/Codex --remote-debugging-port=9229 --user-data-dir=/Users/demo/Library/Application Support/Codex-Gateway CODEX_HOME=/Users/demo/.codex-gateway";
        let missing_profile =
            "/Applications/Codex.app/Contents/MacOS/Codex --remote-debugging-port=9229";

        assert!(codex_app_command_matches_profile(
            matching,
            9229,
            codex_home,
            user_data_dir
        ));
        assert!(!codex_app_command_matches_profile(
            missing_profile,
            9229,
            codex_home,
            user_data_dir
        ));
    }

    #[cfg(target_os = "macos")]
    #[test]
    fn macos_default_codex_profile_does_not_require_codex_home_env() {
        let codex_home = default_user_codex_home_dir();
        let user_data_dir =
            macos_codex_user_data_dir(&codex_home).expect("default user data dir resolves");
        let command = format!(
            "/Applications/Codex.app/Contents/MacOS/Codex --remote-debugging-port=9229 --user-data-dir={}",
            user_data_dir.display()
        );

        assert!(!macos_codex_home_requires_env(&codex_home));
        assert!(codex_app_command_matches_profile(
            &command,
            9229,
            &codex_home,
            &user_data_dir
        ));
    }

    #[cfg(target_os = "macos")]
    #[test]
    fn codex_app_main_processes_ignore_renderer_helpers() {
        let marker = "/Applications/Codex.app/Contents/MacOS/";
        let ps_output = "\
  56351 /Applications/Codex.app/Contents/MacOS/Codex --remote-debugging-port=9229 CODEX_HOME=/Users/demo/.codex
  56358 /Applications/Codex.app/Contents/Frameworks/Codex Framework.framework/Versions/149.0.0/Helpers/Codex (Service).app/Contents/MacOS/Codex (Service) --type=gpu-process
  56393 /Applications/Codex.app/Contents/Frameworks/Codex Framework.framework/Versions/149.0.0/Helpers/Codex (Renderer).app/Contents/MacOS/Codex (Renderer) --type=renderer
";
        let processes = codex_app_main_processes_from_ps_output(ps_output, marker);
        assert_eq!(
            processes,
            vec![CodexAppProcess {
                pid: 56351,
                command: "/Applications/Codex.app/Contents/MacOS/Codex --remote-debugging-port=9229 CODEX_HOME=/Users/demo/.codex".to_string()
            }]
        );
    }

    #[test]
    fn plan_tooltip_sampler_reads_real_native_tooltip() {
        assert!(PLAN_TOOLTIP_SAMPLE_PREP_SCRIPT.contains("Input.dispatchMouseEvent") == false);
        assert!(!PLAN_TOOLTIP_SAMPLE_PREP_SCRIPT.contains("dispatchEvent"));
        assert!(!PLAN_TOOLTIP_SAMPLE_PREP_SCRIPT.contains("pointerover"));
        assert!(!PLAN_TOOLTIP_SAMPLE_PREP_SCRIPT.contains("mouseenter"));
        assert!(!PLAN_TOOLTIP_SAMPLE_PREP_SCRIPT.contains("mousemove"));
        assert!(
            PLAN_TOOLTIP_SAMPLE_PREP_SCRIPT.contains("data-codex-gateway-lite-synthetic-tooltip")
        );
        assert!(
            PLAN_TOOLTIP_SAMPLE_PREP_SCRIPT.contains("data-codex-gateway-lite-tooltip-probing")
        );
        assert!(
            PLAN_TOOLTIP_SAMPLE_PREP_SCRIPT.contains("__codexGatewayLitePlanUiProbeCleanupToken")
        );
        assert!(!PLAN_TOOLTIP_SAMPLE_CLEANUP_SCRIPT.contains("pointerleave"));
        assert!(!PLAN_TOOLTIP_SAMPLE_CLEANUP_SCRIPT.contains("dispatchEvent"));
        assert!(PLAN_TOOLTIP_SAMPLE_CLEANUP_SCRIPT.contains("window.setTimeout"));
        assert!(!PLAN_TOOLTIP_SAMPLE_CLEANUP_SCRIPT.contains("userIsHovering"));
        assert!(
            PLAN_TOOLTIP_SAMPLE_PREP_SCRIPT.contains("codex-gateway-lite-plan-tooltip-probe-style")
        );
        assert!(PLAN_TOOLTIP_SAMPLE_PREP_SCRIPT.contains("__codexGatewayLitePlanUiProbe"));
        assert!(
            PLAN_TOOLTIP_SAMPLE_PREP_SCRIPT.contains("data-codex-gateway-lite-preexisting-tooltip")
        );
        assert!(PLAN_TOOLTIP_SAMPLE_READ_SCRIPT.contains("role=\"tooltip\""));
        assert!(PLAN_TOOLTIP_SAMPLE_READ_SCRIPT.contains("probe?.threadId || currentThreadId()"));
        assert!(
            PLAN_TOOLTIP_SAMPLE_READ_SCRIPT
                .contains("data-codex-gateway-lite-native-pill-thread-id")
        );
        assert!(PLAN_TOOLTIP_SAMPLE_READ_SCRIPT.contains("threadId,"));
        assert!(PLAN_TOOLTIP_SAMPLE_READ_SCRIPT.contains("rowsFromTooltip"));
        assert!(PLAN_TOOLTIP_SAMPLE_READ_SCRIPT.contains("statusForRow"));
        assert!(PLAN_TOOLTIP_SAMPLE_READ_SCRIPT.contains("tooltipNearProbe"));
        assert!(PLAN_TOOLTIP_SAMPLE_READ_SCRIPT.contains("iconHtml"));
        assert!(PLAN_TOOLTIP_SAMPLE_READ_SCRIPT.contains("animation-delay"));
        assert!(PLAN_TOOLTIP_SAMPLE_READ_SCRIPT.contains("textColor"));
        assert!(PLAN_TOOLTIP_SAMPLE_READ_SCRIPT.contains("text(label) !== text(node)"));
        assert!(
            PLAN_TOOLTIP_SAMPLE_READ_SCRIPT
                .contains("if (/^(引导|删除|更多)$/.test(item)) return \"\"")
        );
        assert!(
            PLAN_TOOLTIP_SAMPLE_READ_SCRIPT.contains("__codexGatewayLitePlanUiExternalSnapshot")
        );
        assert!(PLAN_TOOLTIP_SAMPLE_READ_SCRIPT.contains("cdp-tooltip"));
        assert!(PLAN_TOOLTIP_SAMPLE_READ_SCRIPT.contains("visible-tooltip"));
    }

    #[test]
    fn lite_cdp_target_picker_prefers_main_window_over_avatar_overlay() {
        let targets = vec![
            codex_lite::CdpTarget {
                id: "avatar".to_string(),
                target_type: "page".to_string(),
                title: "Codex".to_string(),
                url: "app://-/index.html?initialRoute=%2Favatar-overlay".to_string(),
                web_socket_debugger_url: Some("ws://avatar".to_string()),
            },
            codex_lite::CdpTarget {
                id: "main".to_string(),
                target_type: "page".to_string(),
                title: "Codex".to_string(),
                url: "app://-/index.html".to_string(),
                web_socket_debugger_url: Some("ws://main".to_string()),
            },
        ];

        let target = pick_lite_main_codex_page_target(&targets).expect("target picked");
        assert_eq!(target.id, "main");
    }

    #[cfg(target_os = "macos")]
    #[test]
    fn launch_agent_plist_keeps_agent_alive() {
        let args = vec![
            "/tmp/codex-gateway-lite".to_string(),
            "agent".to_string(),
            "--config".to_string(),
            "/tmp/config.json".to_string(),
        ];
        let plist = launch_agent_plist(
            &args,
            Path::new("/tmp"),
            Path::new("/tmp/out.log"),
            Path::new("/tmp/err.log"),
        );

        assert!(plist.contains("<key>KeepAlive</key>"));
        assert!(plist.contains("<true/>"));
        assert!(plist.contains("<string>agent</string>"));
        assert!(plist.contains("<string>/tmp/config.json</string>"));
    }

    #[test]
    fn parses_provider_model_ids_from_openai_shape() {
        let value = serde_json::json!({
            "data": [
                { "id": "gpt-5.5" },
                { "id": "claude-fable-5" },
                { "id": "gpt-5.5" }
            ]
        });

        assert_eq!(
            parse_provider_model_ids(&value),
            vec!["gpt-5.5".to_string(), "claude-fable-5".to_string()]
        );
    }

    #[test]
    fn model_windows_use_258400_for_gpt_and_1m_for_other_fetched_models() {
        let config = LiteConfig {
            provider: LiteProvider {
                id: "gateway".to_string(),
                name: String::new(),
                base_url: "https://example.test/v1".to_string(),
                api_key: "test".to_string(),
                api_key_env: String::new(),
                mode: LiteMode::PureApi,
                protocol: LiteProtocol::Responses,
                context_budget: String::new(),
            },
            model: "gpt-5.5".to_string(),
            models: models_from_ids(&[
                "gpt-5.5".to_string(),
                "openai/chatgpt-4o-latest".to_string(),
                "claude-fable-5".to_string(),
            ]),
            context_window: "1M".to_string(),
            auto_compact_token_limit: String::new(),
            common_config: String::new(),
            plan_hints: false,
        };
        let (models, windows) = split_models(&config);

        assert!(models.lines().any(|line| line == "gpt-5.5"));
        assert_eq!(windows.get("gpt-5.5").map(String::as_str), Some("258400"));
        assert_eq!(
            windows.get("openai/chatgpt-4o-latest").map(String::as_str),
            Some("258400")
        );
        assert_eq!(
            windows.get("claude-fable-5").map(String::as_str),
            Some("1M")
        );
    }

    #[test]
    fn chat_completions_provider_writes_local_responses_proxy_url_for_codex() {
        let config = LiteConfig {
            provider: LiteProvider {
                id: "gateway".to_string(),
                name: "Gateway".to_string(),
                base_url: "https://chat.example/v1".to_string(),
                api_key: "secret".to_string(),
                api_key_env: String::new(),
                mode: LiteMode::MixedApi,
                protocol: LiteProtocol::ChatCompletions,
                context_budget: String::new(),
            },
            model: "claude-fable-5".to_string(),
            models: models_from_ids(&["claude-fable-5".to_string()]),
            context_window: "1M".to_string(),
            auto_compact_token_limit: String::new(),
            common_config: String::new(),
            plan_hints: false,
        };

        let profile = build_profile(&config).expect("profile builds");
        assert_eq!(
            profile.base_url,
            protocol_proxy::local_responses_proxy_base_url(
                protocol_proxy::DEFAULT_PROTOCOL_PROXY_PORT
            )
        );
        let merged = merge_lite_profile_into_config("", &profile).expect("config merges");

        assert!(merged.contains("wire_api = \"responses\""));
        assert!(merged.contains("base_url = \"http://127.0.0.1:57321/v1\""));
        assert!(merged.contains(&format!(
            "experimental_bearer_token = \"{LOCAL_PROXY_CODEX_BEARER_TOKEN}\""
        )));
        assert!(!merged.contains("https://chat.example/v1"));
        assert!(!merged.contains("secret"));
    }

    #[test]
    fn responses_provider_with_context_budget_uses_local_proxy_for_trimming() {
        let config = LiteConfig {
            provider: LiteProvider {
                id: "gateway".to_string(),
                name: "Gateway".to_string(),
                base_url: "https://responses.example/v1".to_string(),
                api_key: "secret".to_string(),
                api_key_env: String::new(),
                mode: LiteMode::MixedApi,
                protocol: LiteProtocol::Responses,
                context_budget: "200K".to_string(),
            },
            model: "claude-fable-5".to_string(),
            models: models_from_ids(&["claude-fable-5".to_string()]),
            context_window: "1M".to_string(),
            auto_compact_token_limit: String::new(),
            common_config: String::new(),
            plan_hints: false,
        };

        let profile = build_profile(&config).expect("profile builds");
        assert_eq!(
            profile.base_url,
            protocol_proxy::local_responses_proxy_base_url(
                protocol_proxy::DEFAULT_PROTOCOL_PROXY_PORT
            )
        );
        let merged = merge_lite_profile_into_config("", &profile).expect("config merges");

        assert!(merged.contains("wire_api = \"responses\""));
        assert!(merged.contains("base_url = \"http://127.0.0.1:57321/v1\""));
        assert!(merged.contains(&format!(
            "experimental_bearer_token = \"{LOCAL_PROXY_CODEX_BEARER_TOKEN}\""
        )));
        assert!(!merged.contains("https://responses.example/v1"));
        assert!(!merged.contains("secret"));
    }

    #[test]
    fn common_config_merges_portable_settings_and_drops_provider_specific_keys() {
        let existing = r#"
model_provider = "old"
model = "old-model"

[features]
memories = false
"#;
        let common = r#"
model_provider = "private"
model = "private-model"
base_url = "https://private.example/v1"
model_catalog_json = "model-catalogs/private.json"

[model_providers.private]
name = "Private"
base_url = "https://private.example/v1"
experimental_bearer_token = "secret"

[features]
memories = true

[memories]
use_memories = true
generate_memories = true

[mcp_servers."demo"]
command = "npx"
args = ["demo-server"]

[plugins."browser@openai-bundled"]
enabled = true

[hooks.state."/tmp/hooks.json:session_start:0:0"]
trusted_hash = "abc"
"#;
        let merged = merge_common_config_into_config(existing, common).expect("common merges");

        assert!(merged.contains("model_provider = \"old\""));
        assert!(merged.contains("model = \"old-model\""));
        assert!(merged.contains("[features]"));
        assert!(merged.contains("memories = true"));
        assert!(merged.contains("[memories]"));
        assert!(merged.contains("use_memories = true"));
        assert!(merged.contains("mcp_servers"));
        assert!(merged.contains("command = \"npx\""));
        assert!(merged.contains("[plugins.\"browser@openai-bundled\"]"));
        assert!(merged.contains("[hooks.state.\"/tmp/hooks.json:session_start:0:0\"]"));
        assert!(!merged.contains("private.example"));
        assert!(!merged.contains("[model_providers.private]"));
        assert!(!merged.contains("secret"));
    }

    #[test]
    fn extract_common_config_keeps_plugins_hooks_memory_and_removes_provider_config() {
        let live = r#"
model_provider = "gateway"
model = "claude-fable-5"
model_catalog_json = "model-catalogs/gateway.json"

[model_providers.gateway]
name = "Gateway"
base_url = "https://gateway.example/v1"
experimental_bearer_token = "secret"

[features]
memories = true

[memories]
use_memories = true

[plugins."browser@openai-bundled"]
enabled = true

[hooks.state."/tmp/hooks.json:session_start:0:0"]
trusted_hash = "abc"
"#;
        let common = extract_common_config_from_config(live).expect("common extracts");

        assert!(common.contains("[features]"));
        assert!(common.contains("memories = true"));
        assert!(common.contains("[memories]"));
        assert!(common.contains("[plugins.\"browser@openai-bundled\"]"));
        assert!(common.contains("[hooks.state.\"/tmp/hooks.json:session_start:0:0\"]"));
        assert!(!common.contains("model_provider"));
        assert!(!common.contains("model_catalog_json"));
        assert!(!common.contains("[model_providers.gateway]"));
        assert!(!common.contains("secret"));
    }

    #[test]
    fn populate_missing_common_config_only_when_private_config_is_empty() {
        let unique = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .expect("clock is after unix epoch")
            .as_nanos();
        let home = std::env::temp_dir().join(format!("codex-gateway-lite-common-test-{unique}"));
        fs::create_dir_all(&home).expect("creates temp codex home");
        fs::write(
            home.join("config.toml"),
            r#"
model_provider = "gateway"
model = "claude-fable-5"

[model_providers.gateway]
experimental_bearer_token = "secret"

[memories]
use_memories = true

[plugins."browser@openai-bundled"]
enabled = true
"#,
        )
        .expect("writes config");
        let mut config = LiteConfig {
            provider: LiteProvider {
                id: "gateway".to_string(),
                name: "Gateway".to_string(),
                base_url: "https://gateway.example/v1".to_string(),
                api_key: "secret".to_string(),
                api_key_env: String::new(),
                mode: LiteMode::MixedApi,
                protocol: LiteProtocol::Responses,
                context_budget: String::new(),
            },
            model: "claude-fable-5".to_string(),
            models: models_from_ids(&["claude-fable-5".to_string()]),
            context_window: "1M".to_string(),
            auto_compact_token_limit: String::new(),
            common_config: String::new(),
            plan_hints: false,
        };

        assert!(
            populate_missing_common_config_from_home(&mut config, &home).expect("common populates")
        );
        assert!(config.common_config.contains("[memories]"));
        assert!(
            config
                .common_config
                .contains("[plugins.\"browser@openai-bundled\"]")
        );
        assert!(!config.common_config.contains("secret"));

        config.common_config = "[features]\nmemories = false\n".to_string();
        assert!(
            !populate_missing_common_config_from_home(&mut config, &home)
                .expect("existing common is preserved")
        );
        assert_eq!(config.common_config, "[features]\nmemories = false\n");
    }

    #[test]
    fn responses_request_defaults_tool_choice_auto_for_update_plan() {
        let converted = protocol_proxy::responses_to_chat_completions(json!({
            "model": "claude-sonnet-5",
            "input": "hi",
            "tools": [
                {
                    "type": "function",
                    "name": "update_plan",
                    "description": "Updates the task plan.",
                    "parameters": {
                        "type": "object",
                        "properties": {
                            "plan": {
                                "type": "array",
                                "items": {
                                    "type": "object",
                                    "properties": {
                                        "step": { "type": "string" },
                                        "status": { "type": "string" }
                                    },
                                    "required": ["step", "status"]
                                }
                            },
                            "explanation": { "type": "string" }
                        },
                        "required": ["plan"]
                    }
                }
            ]
        }))
        .expect("responses request converts");

        assert_eq!(converted["tool_choice"], "auto");
        assert_eq!(converted["tools"][0]["function"]["name"], "update_plan");
        assert_eq!(
            converted["tools"][0]["function"]["parameters"]["required"],
            json!(["plan"])
        );
    }

    #[test]
    fn responses_request_maps_responses_tool_choice_controls_to_chat_strings() {
        for value in ["auto", "required", "none"] {
            let converted = protocol_proxy::responses_to_chat_completions(json!({
                "model": "gpt-5-mini",
                "input": "hi",
                "tools": [
                    { "type": "function", "name": "lookup", "parameters": {} }
                ],
                "tool_choice": { "type": value }
            }))
            .expect("responses request converts");

            assert_eq!(converted["tool_choice"], value);
        }
    }

    #[test]
    fn chat_sse_maps_update_plan_as_regular_function_call() {
        let converted = protocol_proxy::chat_sse_to_responses_sse_with_request(
            r#"data: {"id":"chatcmpl_plan","model":"claude-sonnet-5","choices":[{"delta":{"tool_calls":[{"index":0,"id":"call_plan","type":"function","function":{"name":"update_plan"}}]}}]}

data: {"id":"chatcmpl_plan","model":"claude-sonnet-5","choices":[{"delta":{"tool_calls":[{"index":0,"function":{"arguments":"{\"plan\":[{\"step\":\"Inspect\",\"status\":\"in_progress\"}]"}}]}}]}

data: {"id":"chatcmpl_plan","model":"claude-sonnet-5","choices":[{"delta":{"tool_calls":[{"index":0,"function":{"arguments":",\"explanation\":\"starting\"}"}}]},"finish_reason":"tool_calls"}]}

data: [DONE]

"#,
            &json!({
                "model": "claude-sonnet-5",
                "tools": [{
                    "type": "function",
                    "name": "update_plan",
                    "parameters": {
                        "type": "object",
                        "properties": {
                            "plan": { "type": "array" },
                            "explanation": { "type": "string" }
                        },
                        "required": ["plan"]
                    }
                }]
            }),
        );

        assert!(converted.contains("event: response.function_call_arguments.done"));
        assert!(converted.contains("\"type\":\"function_call\""));
        assert!(converted.contains("\"name\":\"update_plan\""));
        assert!(converted.contains("\"call_id\":\"call_plan\""));
        assert!(converted.contains("\"arguments\":\"{\\\"plan\\\":[{\\\"step\\\":\\\"Inspect\\\",\\\"status\\\":\\\"in_progress\\\"}],\\\"explanation\\\":\\\"starting\\\"}\""));
        assert!(!converted.contains("\"type\":\"custom_tool_call\""));
    }

    #[test]
    fn lite_config_merge_preserves_user_settings_plugins_hooks_and_other_providers() {
        let existing = r#"
model_provider = "old"
model = "old-model"
notify = ["python", "notify.py"]

[model_providers.old]
name = "Old Provider"
base_url = "https://old.example/v1"

[plugins."browser@openai-bundled"]
enabled = true

[features]
memories = true

[memories]
use_memories = true
generate_memories = true

[hooks.state."/Users/example/.codex/hooks.json:session_start:0:0"]
trusted_hash = "abc"
"#;
        let config = LiteConfig {
            provider: LiteProvider {
                id: "gateway".to_string(),
                name: "Gateway".to_string(),
                base_url: "https://gateway.example/v1".to_string(),
                api_key: "secret".to_string(),
                api_key_env: String::new(),
                mode: LiteMode::MixedApi,
                protocol: LiteProtocol::Responses,
                context_budget: String::new(),
            },
            model: "claude-fable-5".to_string(),
            models: models_from_ids(&["claude-fable-5".to_string(), "gpt-5.5".to_string()]),
            context_window: "1M".to_string(),
            auto_compact_token_limit: String::new(),
            common_config: String::new(),
            plan_hints: false,
        };
        let profile = build_profile(&config).expect("profile builds");
        let merged = merge_lite_profile_into_config(existing, &profile).expect("config merges");

        assert!(merged.contains("model_provider = \"gateway\""));
        assert!(merged.contains("model = \"claude-fable-5\""));
        assert!(merged.contains("model_catalog_json = \"model-catalogs/gateway.json\""));
        assert!(merged.contains("[model_providers.gateway]"));
        assert!(merged.contains("base_url = \"https://gateway.example/v1\""));
        assert!(merged.contains("[model_providers.old]"));
        assert!(merged.contains("[plugins.\"browser@openai-bundled\"]"));
        assert!(merged.contains("[features]"));
        assert!(merged.contains("memories = true"));
        assert!(merged.contains("[memories]"));
        assert!(merged.contains("use_memories = true"));
        assert!(
            merged.contains("[hooks.state.\"/Users/example/.codex/hooks.json:session_start:0:0\"]")
        );
    }
}

#[cfg(test)]
mod plan_hints_tests {
    use super::*;

    #[test]
    fn plan_hints_false_keeps_default_base_instructions() {
        let entries = vec![codex_lite::ModelCatalogEntry {
            slug: "test-model".to_string(),
            display_name: "test-model".to_string(),
            suffix_window: None,
        }];
        let catalog_json = codex_lite::build_model_catalog_json(&entries, None, false);
        let catalog: serde_json::Value =
            serde_json::from_str(&catalog_json).expect("catalog is valid JSON");
        let base = catalog["models"][0]["base_instructions"]
            .as_str()
            .expect("base_instructions is a string");
        assert!(
            !base.contains("update_plan"),
            "plan hints should NOT appear when planHints is false"
        );
        assert!(base.contains("coding agent"));
    }

    #[test]
    fn plan_hints_true_appends_plan_guidance() {
        let entries = vec![codex_lite::ModelCatalogEntry {
            slug: "test-model".to_string(),
            display_name: "test-model".to_string(),
            suffix_window: None,
        }];
        let catalog_json = codex_lite::build_model_catalog_json(&entries, None, true);
        let catalog: serde_json::Value =
            serde_json::from_str(&catalog_json).expect("catalog is valid JSON");
        let base = catalog["models"][0]["base_instructions"]
            .as_str()
            .expect("base_instructions is a string");
        assert!(
            base.contains("update_plan"),
            "plan hints should appear when planHints is true"
        );
        assert!(base.contains("in_progress"));
        assert!(base.contains("coding agent"));

        let template = catalog["models"][0]["model_messages"]["instructions_template"]
            .as_str()
            .expect("instructions_template is a string");
        assert_eq!(
            base, template,
            "base_instructions and instructions_template should match"
        );
    }

    #[test]
    fn plan_hints_config_defaults_to_false() {
        let json = r#"{
            "provider": {
                "id": "test",
                "baseUrl": "https://example.test/v1",
                "apiKey": "sk-test"
            }
        }"#;
        let config: LiteConfig = serde_json::from_str(json).expect("config parses");
        assert!(!config.plan_hints);
    }

    #[test]
    fn plan_hints_config_parses_true() {
        let json = r#"{
            "provider": {
                "id": "test",
                "baseUrl": "https://example.test/v1",
                "apiKey": "sk-test"
            },
            "planHints": true
        }"#;
        let config: LiteConfig = serde_json::from_str(json).expect("config parses");
        assert!(config.plan_hints);
    }
}

#[cfg(test)]
mod context_budget_tests {
    use super::*;
    use serde_json::json;

    fn make_user_msg(text: &str) -> Value {
        json!({ "role": "user", "content": text })
    }

    fn make_assistant_msg(text: &str) -> Value {
        json!({ "role": "assistant", "content": text })
    }

    fn make_system_msg(text: &str) -> Value {
        json!({ "role": "system", "content": text })
    }

    fn make_image_msg() -> Value {
        json!({
            "role": "user",
            "content": [
                { "type": "text", "text": "Look at this image" },
                { "type": "image_url", "image_url": { "url": "data:image/png;base64,AAAA" } }
            ]
        })
    }

    #[test]
    fn context_budget_unlimited_does_not_trim() {
        let budget = protocol_proxy::ContextBudgetConfig::default();
        assert!(budget.is_unlimited());
        let mut body = json!({
            "messages": [
                make_system_msg("You are helpful."),
                make_user_msg(&"x".repeat(100_000)),
            ]
        });
        let report = protocol_proxy::apply_context_budget(&mut body, &budget);
        assert!(!report.was_trimmed);
    }

    #[test]
    fn context_budget_under_limit_passes_through() {
        let budget = protocol_proxy::ContextBudgetConfig::with_max_tokens(100_000);
        let mut body = json!({
            "messages": [
                make_system_msg("You are helpful."),
                make_user_msg("Hello"),
                make_assistant_msg("Hi there"),
            ]
        });
        let report = protocol_proxy::apply_context_budget(&mut body, &budget);
        assert!(!report.was_trimmed);
        assert_eq!(body["messages"].as_array().unwrap().len(), 3);
    }

    #[test]
    fn context_budget_strips_old_images_first() {
        let budget = protocol_proxy::ContextBudgetConfig {
            max_input_tokens: 200,
            image_token_estimate: 800,
        };
        let mut body = json!({
            "messages": [
                make_system_msg("System prompt"),
                make_image_msg(),
                make_assistant_msg("I see the image"),
                make_user_msg("What about now?"),
                make_assistant_msg("Sure"),
                make_image_msg(),
                make_assistant_msg("Another image"),
                make_user_msg("Final question"),
            ]
        });
        let report = protocol_proxy::apply_context_budget(&mut body, &budget);
        assert!(report.was_trimmed);
        assert!(report.images_stripped > 0);

        // The first image message should have been stripped
        let msgs = body["messages"].as_array().unwrap();
        let first_user = &msgs[1];
        let content = first_user["content"].as_str().unwrap_or("");
        assert!(
            content.contains("omitted"),
            "First image should be replaced with omitted text, got: {content}"
        );
    }

    #[test]
    fn context_budget_removes_old_messages_when_images_not_enough() {
        let budget = protocol_proxy::ContextBudgetConfig {
            max_input_tokens: 100,
            image_token_estimate: 800,
        };
        // Create many messages that exceed budget even without images
        let mut messages = vec![make_system_msg("System prompt")];
        for i in 0..20 {
            messages.push(make_user_msg(&format!(
                "User message {i} with some padding text to take up tokens xxxxxxxxxx"
            )));
            messages.push(make_assistant_msg(&format!(
                "Assistant reply {i} with padding text to use tokens yyyyyyyyyy"
            )));
        }
        messages.push(make_user_msg("Final question"));

        let mut body = json!({ "messages": messages });
        let original_count = body["messages"].as_array().unwrap().len();
        let report = protocol_proxy::apply_context_budget(&mut body, &budget);

        assert!(report.was_trimmed);
        assert!(report.messages_removed > 0);
        let final_count = body["messages"].as_array().unwrap().len();
        assert!(final_count < original_count);

        // System message should still be first
        assert_eq!(body["messages"][0]["role"].as_str().unwrap(), "system");
        // Last message should still be the final question
        let last = body["messages"].as_array().unwrap().last().unwrap();
        assert_eq!(last["content"].as_str().unwrap(), "Final question");
    }

    #[test]
    fn context_budget_preserves_recent_turns() {
        let budget = protocol_proxy::ContextBudgetConfig {
            max_input_tokens: 80,
            image_token_estimate: 800,
        };
        let mut messages = vec![make_system_msg("Be helpful")];
        for i in 0..10 {
            messages.push(make_user_msg(&format!("Old question {i} padding xxxxx")));
            messages.push(make_assistant_msg(&format!("Old answer {i} padding yyyyy")));
        }
        messages.push(make_user_msg("Recent Q1"));
        messages.push(make_assistant_msg("Recent A1"));
        messages.push(make_user_msg("Recent Q2"));
        messages.push(make_assistant_msg("Recent A2"));
        messages.push(make_user_msg("Current question"));

        let mut body = json!({ "messages": messages });
        let report = protocol_proxy::apply_context_budget(&mut body, &budget);
        assert!(report.was_trimmed);

        let msgs = body["messages"].as_array().unwrap();
        let texts: Vec<&str> = msgs.iter().filter_map(|m| m["content"].as_str()).collect();
        assert!(texts.contains(&"Current question"));
        assert!(
            texts.iter().any(|t| t.contains("trimmed")),
            "Should have a trim marker"
        );
    }

    #[test]
    fn context_budget_from_context_window_uses_85_percent() {
        let budget = protocol_proxy::ContextBudgetConfig::from_context_window(200_000);
        assert_eq!(budget.max_input_tokens, 170_000);
    }

    #[test]
    fn resolve_context_budget_from_provider_config() {
        let config = LiteConfig {
            provider: LiteProvider {
                id: "test".to_string(),
                name: String::new(),
                base_url: "https://api.example.com".to_string(),
                api_key: "sk-test".to_string(),
                api_key_env: String::new(),
                mode: LiteMode::MixedApi,
                protocol: LiteProtocol::ChatCompletions,
                context_budget: "200K".to_string(),
            },
            model: String::new(),
            models: vec![],
            context_window: "1M".to_string(),
            auto_compact_token_limit: String::new(),
            common_config: String::new(),
            plan_hints: true,
        };
        let budget = resolve_context_budget(&config);
        assert_eq!(budget.max_input_tokens, 800_000);
        assert_eq!(explicit_context_budget_limit(200_000, "1M"), 800_000);
    }

    #[test]
    fn context_budget_accepts_user_friendly_units() {
        assert_eq!(parse_context_budget_token("200"), Some(200_000));
        assert_eq!(parse_context_budget_token("200K"), Some(200_000));
        assert_eq!(parse_context_budget_token("200kb"), Some(200_000));
        assert_eq!(parse_context_budget_token("200000"), Some(200_000));
        assert_eq!(
            normalize_context_budget_for_config("200 kb").unwrap(),
            "200K"
        );
    }

    #[test]
    fn context_budget_off_disables_explicit_proxy_for_responses() {
        let config = LiteConfig {
            provider: LiteProvider {
                id: "test".to_string(),
                name: String::new(),
                base_url: "https://api.example.com".to_string(),
                api_key: "sk-test".to_string(),
                api_key_env: String::new(),
                mode: LiteMode::MixedApi,
                protocol: LiteProtocol::Responses,
                context_budget: "off".to_string(),
            },
            model: String::new(),
            models: vec![LiteModel::Id("model".to_string())],
            context_window: "1M".to_string(),
            auto_compact_token_limit: String::new(),
            common_config: String::new(),
            plan_hints: false,
        };
        assert!(!provider_uses_local_proxy(&config));
        assert_eq!(normalize_context_budget_for_config("off").unwrap(), "");
        assert!(validate_context_budget_for_config("off").is_ok());
    }

    #[test]
    fn resolve_context_budget_falls_back_to_context_window() {
        let config = LiteConfig {
            provider: LiteProvider {
                id: "test".to_string(),
                name: String::new(),
                base_url: "https://api.example.com".to_string(),
                api_key: "sk-test".to_string(),
                api_key_env: String::new(),
                mode: LiteMode::MixedApi,
                protocol: LiteProtocol::ChatCompletions,
                context_budget: String::new(),
            },
            model: String::new(),
            models: vec![],
            context_window: "200K".to_string(),
            auto_compact_token_limit: String::new(),
            common_config: String::new(),
            plan_hints: true,
        };
        let budget = resolve_context_budget(&config);
        // 85% of 200K = 170K
        assert_eq!(budget.max_input_tokens, 170_000);
    }

    #[test]
    fn token_estimation_handles_mixed_content() {
        let ascii = protocol_proxy::estimate_text_tokens("Hello, world!");
        assert!(ascii > 0 && ascii < 20);

        let cjk = protocol_proxy::estimate_text_tokens("你好世界");
        assert!(cjk > 0 && cjk < 20);

        let long_text = protocol_proxy::estimate_text_tokens(&"x".repeat(3500));
        assert!(long_text >= 900 && long_text <= 1200);
    }

    #[test]
    fn responses_content_handles_unknown_types_gracefully() {
        let body = json!({
            "model": "test",
            "input": [
                {
                    "role": "user",
                    "content": [
                        { "type": "input_text", "text": "Hello" },
                        { "type": "input_audio", "transcript": "spoken words" },
                        { "type": "input_file", "filename": "data.csv", "text": "a,b,c" },
                        { "type": "unknown_new_type", "text": "fallback text" }
                    ]
                }
            ]
        });
        let chat = protocol_proxy::responses_to_chat_completions(body).unwrap();
        let messages = chat["messages"].as_array().unwrap();
        let user_msg = &messages[0];
        let content = user_msg["content"].as_str().unwrap_or("");
        assert!(content.contains("Hello"), "Should contain original text");
        assert!(content.contains("Audio"), "Should contain audio fallback");
        assert!(content.contains("data.csv"), "Should contain file name");
        assert!(
            content.contains("fallback text"),
            "Should contain unknown type text"
        );
    }
}

#[cfg(test)]
mod responses_budget_tests {
    use serde_json::json;

    #[test]
    fn responses_budget_strips_old_images_from_input() {
        let budget = crate::protocol_proxy::ContextBudgetConfig {
            max_input_tokens: 200,
            image_token_estimate: 800,
        };
        let mut body = json!({
            "model": "test",
            "instructions": "Be helpful",
            "input": [
                {
                    "role": "user",
                    "content": [
                        { "type": "input_text", "text": "old message" },
                        { "type": "input_image", "image_url": "data:image/png;base64,OLD" }
                    ]
                },
                { "role": "assistant", "content": "I see the old image" },
                {
                    "role": "user",
                    "content": [
                        { "type": "input_text", "text": "new message" },
                        { "type": "input_image", "image_url": "data:image/png;base64,NEW" }
                    ]
                }
            ]
        });
        let report = crate::protocol_proxy::apply_responses_context_budget(&mut body, &budget);
        assert!(report.was_trimmed);
        assert!(report.images_stripped > 0);

        let items = body["input"].as_array().unwrap();
        let old_user = &items[0];
        let parts = old_user["content"].as_array().unwrap();
        let has_old_image = parts
            .iter()
            .any(|p| p.get("type").and_then(|v| v.as_str()) == Some("input_image"));
        assert!(!has_old_image, "Old image should have been stripped");
    }

    #[test]
    fn responses_budget_removes_old_items_when_still_over() {
        let budget = crate::protocol_proxy::ContextBudgetConfig {
            max_input_tokens: 60,
            image_token_estimate: 800,
        };
        let mut items = Vec::new();
        for i in 0..15 {
            items.push(json!({
                "role": "user",
                "content": format!("Question {i} with padding text xxxxxxxxxxxx")
            }));
            items.push(json!({
                "role": "assistant",
                "content": format!("Answer {i} with padding text yyyyyyyyyyyy")
            }));
        }
        items.push(json!({ "role": "user", "content": "Final question" }));

        let mut body = json!({
            "model": "test",
            "input": items
        });
        let original_len = body["input"].as_array().unwrap().len();
        let report = crate::protocol_proxy::apply_responses_context_budget(&mut body, &budget);
        assert!(report.was_trimmed);
        assert!(report.messages_removed > 0);

        let final_items = body["input"].as_array().unwrap();
        assert!(final_items.len() < original_len);

        let last = final_items.last().unwrap();
        assert_eq!(last["content"].as_str().unwrap(), "Final question");
    }

    #[test]
    fn responses_budget_unlimited_passes_through() {
        let budget = crate::protocol_proxy::ContextBudgetConfig::default();
        let mut body = json!({
            "model": "test",
            "input": [
                { "role": "user", "content": "Hello" }
            ]
        });
        let report = crate::protocol_proxy::apply_responses_context_budget(&mut body, &budget);
        assert!(!report.was_trimmed);
    }

    #[test]
    fn responses_budget_preserves_function_call_output_pairs_in_recent_turns() {
        let budget = crate::protocol_proxy::ContextBudgetConfig {
            max_input_tokens: 150,
            image_token_estimate: 800,
        };
        let mut body = json!({
            "model": "test",
            "input": [
                { "role": "user", "content": "old message with lots of padding xxxxxxxxxxxxxxxxx" },
                { "role": "assistant", "content": "old reply with lots of padding yyyyyyyyyyyyyyyyy" },
                { "role": "user", "content": "Recent question" },
                {
                    "type": "function_call",
                    "call_id": "call_1",
                    "name": "read_file",
                    "arguments": "{\"path\": \"test.txt\"}"
                },
                {
                    "type": "function_call_output",
                    "call_id": "call_1",
                    "output": "file contents"
                },
                { "role": "user", "content": "Current question" }
            ]
        });
        let report = crate::protocol_proxy::apply_responses_context_budget(&mut body, &budget);
        let items = body["input"].as_array().unwrap();
        let has_call = items
            .iter()
            .any(|i| i.get("type").and_then(|v| v.as_str()) == Some("function_call"));
        let has_output = items
            .iter()
            .any(|i| i.get("type").and_then(|v| v.as_str()) == Some("function_call_output"));
        // Recent function_call pairs should be preserved together
        assert_eq!(
            has_call, has_output,
            "function_call and output should both be present or both removed"
        );
        // Final question must survive
        let last = items.last().unwrap();
        assert_eq!(last["content"].as_str().unwrap(), "Current question");
        let _ = report;
    }
}
