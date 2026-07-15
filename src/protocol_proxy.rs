//! Codex Responses API 与 OpenAI Chat Completions 的本地协议转换。
//!
//! Codex Chat 与 Responses 协议之间的转换实现。

#![allow(dead_code)]

use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::collections::HashMap;
use std::sync::{LazyLock, Mutex};
use std::time::Duration;

use anyhow::Context;
use serde_json::{Value, json};

pub const DEFAULT_PROTOCOL_PROXY_PORT: u16 = 57321;
const UPSTREAM_CONNECT_TIMEOUT: Duration = Duration::from_secs(5);
const UPSTREAM_HEADER_TIMEOUT: Duration = Duration::from_secs(30);
/// Maximum gap allowed between two SSE chunks while relaying an already-open
/// upstream stream, so a truly dead upstream doesn't hold the loop forever.
/// This must comfortably exceed legitimate silent stretches: Claude-family
/// models at high reasoning effort think for minutes, and the VPS-side
/// thinking-filter swallows those thinking deltas, so the proxy sees zero
/// bytes the whole time (observed >60s regularly on claude-fable-5; grok/gpt
/// never hit this because nothing filters their stream). The client side is
/// safe during the wait: the proxy emits SSE keepalive comments and the
/// provider config pins Codex's own idle timeout at 900s, so this just needs
/// to stay below that.
pub const UPSTREAM_STREAM_IDLE_TIMEOUT: Duration = Duration::from_secs(720);
const THINK_OPEN_TAG: &str = "<think>";
const THINK_CLOSE_TAG: &str = "</think>";
const EXTRA_CHAT_PASSTHROUGH_FIELDS: &[&str] = &[
    "frequency_penalty",
    "logit_bias",
    "logprobs",
    "metadata",
    "n",
    "presence_penalty",
    "response_format",
    "seed",
    "service_tier",
    "stop",
    "stream_options",
    "top_logprobs",
    "user",
];
const ERROR_BODY_PREVIEW_LIMIT: usize = 1024;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ChatReasoningStyle {
    Default,
    AnthropicThinking,
    DeepSeek,
    LowHigh,
    OpenRouter,
    Thinking,
    EnableThinking,
    ReasoningSplit,
}

#[derive(Debug, Clone, Default)]
struct CodexToolContext {
    custom_tools: BTreeMap<String, CodexCustomToolSpec>,
    function_tools: BTreeMap<String, CodexFunctionToolSpec>,
    has_custom_tools: bool,
    has_namespace_tools: bool,
}

#[derive(Debug, Clone)]
struct CodexCustomToolSpec {
    openai_name: String,
    kind: CodexCustomToolKind,
    proxy_action: Option<CodexPatchProxyAction>,
}

#[derive(Debug, Clone, Default)]
struct CodexFunctionToolSpec {
    namespace: String,
    name: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum CodexCustomToolKind {
    Raw,
    ApplyPatch,
    BuiltIn,
}

impl Default for CodexCustomToolKind {
    fn default() -> Self {
        Self::Raw
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum CodexPatchProxyAction {
    AddFile,
    DeleteFile,
    UpdateFile,
    ReplaceFile,
    Batch,
}

impl CodexPatchProxyAction {
    fn suffix(self) -> &'static str {
        match self {
            Self::AddFile => "add_file",
            Self::DeleteFile => "delete_file",
            Self::UpdateFile => "update_file",
            Self::ReplaceFile => "replace_file",
            Self::Batch => "batch",
        }
    }
}

impl CodexToolContext {
    fn is_custom_tool_proxy(&self, upstream_name: &str) -> bool {
        self.custom_tools.contains_key(upstream_name)
    }

    fn original_custom_tool_name(&self, upstream_name: &str) -> String {
        self.custom_tools
            .get(upstream_name)
            .map(|spec| spec.openai_name.clone())
            .unwrap_or_else(|| upstream_name.to_string())
    }

    fn openai_name_for_function_tool(&self, upstream_name: &str) -> (String, String) {
        let Some(spec) = self.function_tools.get(upstream_name) else {
            return (upstream_name.to_string(), String::new());
        };
        let name = if spec.name.is_empty() {
            upstream_name.to_string()
        } else {
            spec.name.clone()
        };
        (name, spec.namespace.clone())
    }
}

pub fn local_responses_proxy_base_url(port: u16) -> String {
    format!("http://127.0.0.1:{port}/v1")
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize)]
pub enum RelayProtocol {
    Responses,
    ChatCompletions,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProtocolProxyUpstream {
    pub id: String,
    pub name: String,
    pub base_url: String,
    pub api_key: String,
    pub protocol: RelayProtocol,
    pub user_agent: String,
    pub context_budget: ContextBudgetConfig,
    /// Mirrors `LiteConfig::plan_hints` (see main.rs): gates both the
    /// one-time `PLAN_HINTS_SUPPLEMENT` baked into `base_instructions` at
    /// catalog-build time (codex_lite.rs) and the mid-conversation
    /// stale-plan nudge injected per request below
    /// (`should_inject_plan_reminder`) — same toggle, two different points
    /// in the model's context where it needs reinforcing.
    pub plan_hints: bool,
    /// Multi-provider aggregation routing table: a normalized model slug
    /// (see `crate::normalized_route_key`) mapped to whichever upstream
    /// actually serves that model. Populated only when the gateway config
    /// has `aggregate: true` (see `LiteConfig::aggregate` / `main::
    /// aggregate_model_routes` in main.rs); empty otherwise, in which case
    /// every request just uses this struct's own `base_url`/`api_key`
    /// (see `resolve_model_upstream`).
    pub model_routes: BTreeMap<String, ModelRoute>,
}

/// A single aggregate-mode routing target: which upstream a request for one
/// specific model should actually be sent to. Deliberately carries only
/// connection info — protocol and context-budget behavior always follow
/// the active/default provider (`ProtocolProxyUpstream::protocol` /
/// `::context_budget`), never a per-route override; see
/// `resolve_model_upstream`.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct ModelRoute {
    pub base_url: String,
    pub api_key: String,
}

/// Context budget configuration for trimming oversized input before upstream requests.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ContextBudgetConfig {
    /// Maximum estimated input tokens for messages sent upstream. 0 = unlimited.
    pub max_input_tokens: u64,
    /// Estimated tokens per image for budget calculation.
    pub image_token_estimate: u64,
}

impl Default for ContextBudgetConfig {
    fn default() -> Self {
        Self {
            max_input_tokens: 0,
            image_token_estimate: 800,
        }
    }
}

impl ContextBudgetConfig {
    pub fn from_context_window(window_tokens: u64) -> Self {
        Self {
            max_input_tokens: window_tokens.saturating_mul(85) / 100,
            image_token_estimate: 800,
        }
    }

    pub fn with_max_tokens(max_input_tokens: u64) -> Self {
        Self {
            max_input_tokens,
            image_token_estimate: 800,
        }
    }

    pub fn is_unlimited(&self) -> bool {
        self.max_input_tokens == 0
    }
}

/// Summary of context budget trimming applied to a request.
#[derive(Debug, Default)]
pub struct ContextBudgetReport {
    pub original_estimated_tokens: u64,
    pub final_estimated_tokens: u64,
    pub images_stripped: usize,
    pub messages_removed: usize,
    pub was_trimmed: bool,
    /// Count of stale `reasoning` items stripped from completed prior turns
    /// (see `strip_stale_reasoning_items`). Independent of `was_trimmed`,
    /// which only reflects token-budget trimming.
    pub stale_reasoning_items_stripped: usize,
}

pub fn responses_to_chat_completions(body: Value) -> anyhow::Result<Value> {
    let mut result = json!({});

    if let Some(model) = body.get("model") {
        result["model"] = model.clone();
    }

    let mut messages = Vec::new();
    if let Some(instructions) = body.get("instructions") {
        let text = instruction_text(instructions);
        if !text.is_empty() {
            messages.push(json!({ "role": "system", "content": text }));
        }
    }

    if let Some(input) = body.get("input") {
        append_responses_input(input, &mut messages);
    }
    normalize_chat_messages(&mut messages);
    let messages = collapse_system_messages_to_head(messages);
    result["messages"] = json!(messages);

    let model = body.get("model").and_then(Value::as_str).unwrap_or("");
    if let Some(value) = body.get("max_output_tokens") {
        if is_openai_o_series(model) {
            result["max_completion_tokens"] = value.clone();
        } else {
            result["max_tokens"] = value.clone();
        }
    }
    if let Some(value) = body.get("max_tokens") {
        result["max_tokens"] = value.clone();
    }
    if let Some(value) = body.get("max_completion_tokens") {
        result["max_completion_tokens"] = value.clone();
    }

    for key in ["temperature", "top_p", "stream"] {
        if let Some(value) = body.get(key) {
            result[key] = value.clone();
        }
    }
    if body.get("stream").and_then(Value::as_bool).unwrap_or(false) {
        let mut stream_options = body
            .get("stream_options")
            .cloned()
            .unwrap_or_else(|| json!({}));
        stream_options["include_usage"] = json!(true);
        result["stream_options"] = stream_options;
    }

    apply_chat_reasoning_options(&mut result, &body, model);

    let tool_context = build_codex_tool_context(body.get("tools"));
    let mut has_chat_tools = false;
    if let Some(tools) = body.get("tools").and_then(Value::as_array) {
        let converted = responses_tools_to_chat_tools(tools, &tool_context);
        if !converted.is_empty() {
            has_chat_tools = true;
            result["tools"] = json!(converted);
        }
    }

    if has_chat_tools {
        if let Some(tool_choice) = body
            .get("tool_choice")
            .and_then(|value| responses_tool_choice_to_chat(value, &tool_context))
        {
            result["tool_choice"] = tool_choice;
        } else {
            result["tool_choice"] = json!("auto");
        }
        if let Some(value) = body.get("parallel_tool_calls") {
            result["parallel_tool_calls"] = value.clone();
        }
    }

    for key in EXTRA_CHAT_PASSTHROUGH_FIELDS {
        if *key == "stream_options" && result.get("stream_options").is_some() {
            continue;
        }
        if let Some(value) = body.get(*key) {
            result[*key] = value.clone();
        }
    }

    Ok(result)
}

pub fn chat_completion_to_response(body: Value) -> anyhow::Result<Value> {
    chat_completion_to_response_with_context(body, &CodexToolContext::default(), None)
}

pub fn chat_completion_to_response_with_request(
    body: Value,
    original_request: &Value,
) -> anyhow::Result<Value> {
    let context = build_codex_tool_context(original_request.get("tools"));
    chat_completion_to_response_with_context(body, &context, Some(original_request))
}

fn chat_completion_to_response_with_context(
    body: Value,
    tool_context: &CodexToolContext,
    original_request: Option<&Value>,
) -> anyhow::Result<Value> {
    let choices = body
        .get("choices")
        .and_then(Value::as_array)
        .ok_or_else(|| anyhow::anyhow!("chat response missing choices"))?;
    let choice = choices
        .first()
        .ok_or_else(|| anyhow::anyhow!("chat response choices is empty"))?;
    let message = choice
        .get("message")
        .ok_or_else(|| anyhow::anyhow!("chat response choice missing message"))?;

    let response_id = response_id_from_chat_id(body.get("id").and_then(Value::as_str));
    let mut output = Vec::new();
    if let Some(reasoning) = chat_reasoning_to_response_output_item(message, &response_id) {
        output.push(reasoning);
    }
    if let Some(message) = chat_message_to_response_output_item(message, &response_id) {
        output.push(message);
    }
    output.extend(chat_tool_calls_to_response_output_items(
        message,
        tool_context,
    ));

    let mut response = json!({
        "id": response_id,
        "object": "response",
        "created_at": body.get("created").and_then(Value::as_u64).unwrap_or(0),
        "status": response_status(choice.get("finish_reason").and_then(Value::as_str)),
        "model": body.get("model").and_then(Value::as_str).unwrap_or(""),
        "output": output,
        "usage": chat_usage_to_responses_usage(body.get("usage"))
    });

    if let Some(reason) =
        incomplete_details_reason(choice.get("finish_reason").and_then(Value::as_str))
    {
        response["incomplete_details"] = json!({ "reason": reason });
    }
    copy_response_request_fields(&mut response, original_request);

    Ok(response)
}

pub struct ProxyHttpResponse {
    pub status: String,
    pub content_type: String,
    pub body: Vec<u8>,
}

pub struct UpstreamProxyResponse {
    pub status_code: u16,
    pub content_type: String,
    pub is_stream: bool,
    pub wire_api: UpstreamWireApi,
    /// Lowest-level attribution headers captured at open time (`server` /
    /// `cf-ray`), so error logs can say who actually answered.
    pub server_header: String,
    pub cf_ray: String,
    /// Total send attempts made for this response (1 = no retry).
    pub attempts: u32,
    pub response: reqwest::Response,
    /// Trace sequence number allocated at request-open time (see
    /// `crate::trace::next_seq`), `Some` only for requests opened through
    /// `open_responses_proxy_request` — the only path the request/response
    /// tracing feature (`crate::trace`) covers. Lets the caller (the
    /// Responses streaming loop, or `handle_responses_upstream`) append
    /// response bytes to, and later finish, the same numbered trace files
    /// this request's request-body dump was written under.
    pub trace_seq: Option<u64>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize)]
pub enum UpstreamWireApi {
    Responses,
    ChatCompletions,
}

impl UpstreamProxyResponse {
    pub fn status(&self) -> String {
        http_status_line(self.status_code)
    }

    pub fn is_success(&self) -> bool {
        (200..300).contains(&self.status_code)
    }
}

pub fn upstream_header_timeout() -> Duration {
    UPSTREAM_HEADER_TIMEOUT
}

/// Extra attempts (beyond the first) when the upstream answers 502/503/504.
/// These gateway-layer statuses are frequently generated by an edge (e.g.
/// Cloudflare) or by a stale pooled connection, not by the origin service —
/// the origin often never sees the request at all. A fresh-connection retry
/// resolves the common transient cases without waiting for Codex's own much
/// slower turn-level retry loop.
const UPSTREAM_5XX_RETRIES: usize = 2;
const UPSTREAM_5XX_RETRY_BACKOFF: [Duration; UPSTREAM_5XX_RETRIES] =
    [Duration::from_millis(300), Duration::from_millis(800)];

pub fn upstream_status_is_retryable(status_code: u16) -> bool {
    matches!(status_code, 502..=504)
}

/// Whether attempt number `attempt` (1-based, counting the initial send)
/// should bypass the proxy environment and go direct. Only the final retry
/// switches egress, and only when a proxy is configured at all: earlier
/// retries stay on the user's intended routing for transient blips, while
/// the last attempt gets a genuinely different network path to dodge an
/// edge-PoP-specific failure.
fn upstream_attempt_should_bypass_proxy(attempt: u32, proxy_configured: bool) -> bool {
    proxy_configured && attempt >= upstream_5xx_max_attempts(proxy_configured)
}

/// Total attempts for a 5xx-retryable request: the initial send, the
/// same-path retries, plus one extra direct-egress attempt when a proxy is
/// configured (that extra attempt is the one `upstream_attempt_should_bypass_proxy`
/// switches to direct).
fn upstream_5xx_max_attempts(proxy_configured: bool) -> u32 {
    1 + UPSTREAM_5XX_RETRIES as u32 + u32::from(proxy_configured)
}

fn upstream_edge_headers(headers: &reqwest::header::HeaderMap) -> (String, String) {
    let value = |name: &str| {
        headers
            .get(name)
            .and_then(|value| value.to_str().ok())
            .unwrap_or("")
            .trim()
            .to_string()
    };
    (value("server"), value("cf-ray"))
}

/// Human-readable label for *who* actually produced an upstream HTTP error,
/// derived from response headers. Distinguishing "the origin answered" from
/// "a CDN/edge answered on the origin's behalf" matters for diagnostics: an
/// `error code: 502` page with `server: cloudflare` means the origin most
/// likely never received the request.
pub fn upstream_layer_label(server_header: &str, cf_ray: &str) -> Option<String> {
    let server = server_header.trim();
    if server.to_ascii_lowercase().contains("cloudflare") {
        let cf_ray = cf_ray.trim();
        if cf_ray.is_empty() {
            Some("Cloudflare 边缘网关".to_string())
        } else {
            Some(format!("Cloudflare 边缘网关, cf-ray: {cf_ray}"))
        }
    } else if !server.is_empty() {
        Some(format!("server: {server}"))
    } else {
        None
    }
}

/// Diagnostic suffix explaining an upstream error status: which layer
/// answered, how many fresh-connection retries the local proxy already
/// burned, and — for Cloudflare-generated 502/504 — that the origin likely
/// never saw the request.
pub fn upstream_error_attribution(
    server_header: &str,
    cf_ray: &str,
    attempts: u32,
    status_code: u16,
) -> String {
    let mut parts: Vec<String> = Vec::new();
    if let Some(label) = upstream_layer_label(server_header, cf_ray) {
        parts.push(format!("响应方: {label}"));
    }
    if attempts > 1 {
        parts.push(format!(
            "本地代理已换新连接自动重试 {} 次仍收到相同状态",
            attempts - 1
        ));
    }
    if matches!(status_code, 502 | 504) && server_header.to_ascii_lowercase().contains("cloudflare")
    {
        parts.push("该错误由边缘网关生成，源站可能未收到这笔请求".to_string());
    }
    if parts.is_empty() {
        String::new()
    } else {
        format!("（{}）", parts.join("；"))
    }
}

/// The proxy honors standard proxy environment variables via reqwest; report
/// the effective one so egress is explicit in retry diagnostics.
fn egress_proxy_from_env() -> Option<String> {
    for key in [
        "HTTPS_PROXY",
        "https_proxy",
        "ALL_PROXY",
        "all_proxy",
        "HTTP_PROXY",
        "http_proxy",
    ] {
        if let Ok(value) = std::env::var(key) {
            let value = value.trim().to_string();
            if !value.is_empty() {
                return Some(value);
            }
        }
    }
    None
}

fn log_upstream_5xx_retry(
    status_code: u16,
    headers: &reqwest::header::HeaderMap,
    attempt: u32,
    proxy_configured: bool,
) {
    let (server_header, cf_ray) = upstream_edge_headers(headers);
    let layer = upstream_layer_label(&server_header, &cf_ray)
        .map(|label| format!("，响应方: {label}"))
        .unwrap_or_default();
    let egress = egress_proxy_from_env()
        .map(|proxy| format!("，本机出口: {proxy}"))
        .unwrap_or_default();
    let max_retries = upstream_5xx_max_attempts(proxy_configured) - 1;
    let next_hop = if upstream_attempt_should_bypass_proxy(attempt + 1, proxy_configured) {
        "，下一次改为绕过本机代理直连上游（换网络路径避开出问题的边缘节点）"
    } else {
        ""
    };
    log_upstream_event_deduped(
        "upstream_5xx_retry",
        LogLevel::Warn,
        format!(
            "上游返回 HTTP {status_code}{layer}{egress}；正在换新连接自动重试（第 {attempt}/{max_retries} 次）{next_hop}"
        ),
    );
}

pub fn upstream_http_client() -> anyhow::Result<reqwest::Client> {
    upstream_http_client_with_user_agent("")
}

pub fn upstream_http_client_with_user_agent(user_agent: &str) -> anyhow::Result<reqwest::Client> {
    upstream_http_client_with_egress(user_agent, false)
}

fn upstream_client_builder(user_agent: &str, bypass_env_proxy: bool) -> reqwest::ClientBuilder {
    let mut builder = reqwest::Client::builder().connect_timeout(UPSTREAM_CONNECT_TIMEOUT);
    if bypass_env_proxy {
        builder = builder.no_proxy();
    }
    if user_agent.trim().is_empty() {
        builder.user_agent("CodexGatewayLite/ProtocolProxy")
    } else {
        builder.user_agent(user_agent.trim())
    }
}

/// Build a single-shot upstream client, optionally ignoring proxy
/// environment variables so the request egresses directly from this
/// machine. Used for retries: a 502/504 minted by a CDN edge is tied to the
/// specific edge PoP the current egress path lands on, so a brand-new
/// client (and, on the final attempt, a direct connection) reroutes to a
/// different path and can clear an edge-side failure that same-connection
/// retries keep hitting.
fn upstream_http_client_with_egress(
    user_agent: &str,
    bypass_env_proxy: bool,
) -> anyhow::Result<reqwest::Client> {
    upstream_client_builder(user_agent, bypass_env_proxy)
        .build()
        .context("failed to build upstream HTTP client")
}

/// Every request is MBs of replayed history, so the CONNECT + TLS handshake
/// a brand-new client pays per call is pure added first-token latency;
/// keep-alive reuse removes it on the hot path. Idle connections are
/// dropped before typical edge/forward-proxy idle cutoffs (~100s+) so the
/// pool rarely hands out a connection the far side already closed; when
/// that race does happen the send fails fast and the request falls back to
/// a fresh connection (`pooled_send_error_is_retryable`).
const UPSTREAM_POOL_IDLE_TIMEOUT: Duration = Duration::from_secs(90);
const UPSTREAM_POOL_MAX_IDLE_PER_HOST: usize = 4;
/// Distinct user agents get their own pooled client (the user agent is
/// baked into the client). Beyond this cap, requests fall back to
/// single-shot clients instead of growing the registry unbounded.
const POOLED_UPSTREAM_CLIENTS_MAX: usize = 8;

static POOLED_UPSTREAM_CLIENTS: LazyLock<Mutex<HashMap<String, reqwest::Client>>> =
    LazyLock::new(|| Mutex::new(HashMap::new()));

/// Shared keep-alive client for first-attempt sends on the normal egress
/// path. Retries never come here: they use `upstream_http_client_with_egress`
/// so a poisoned connection is never reused.
fn pooled_upstream_client(user_agent: &str) -> anyhow::Result<reqwest::Client> {
    let key = user_agent.trim().to_string();
    {
        let clients = POOLED_UPSTREAM_CLIENTS
            .lock()
            .expect("pooled upstream client registry poisoned");
        if let Some(client) = clients.get(&key) {
            return Ok(client.clone());
        }
    }
    let client = upstream_client_builder(user_agent, false)
        .pool_max_idle_per_host(UPSTREAM_POOL_MAX_IDLE_PER_HOST)
        .pool_idle_timeout(UPSTREAM_POOL_IDLE_TIMEOUT)
        .build()
        .context("failed to build pooled upstream HTTP client")?;
    let mut clients = POOLED_UPSTREAM_CLIENTS
        .lock()
        .expect("pooled upstream client registry poisoned");
    if clients.len() >= POOLED_UPSTREAM_CLIENTS_MAX && !clients.contains_key(&key) {
        return Ok(client);
    }
    Ok(clients.entry(key).or_insert(client).clone())
}

/// Only the pooled first attempt may transparently retry a failed send:
/// reuse can race the far side closing the idle connection, which surfaces
/// as a fast connection-level error. Header-timeout waits carry no
/// `reqwest::Error` in their chain (and connect timeouts report
/// `is_timeout`); repeating those would double an already long wait, so
/// they surface immediately instead.
fn pooled_send_error_is_retryable(error: &anyhow::Error) -> bool {
    error.chain().any(|cause| {
        cause
            .downcast_ref::<reqwest::Error>()
            .is_some_and(|error| !error.is_timeout())
    })
}

/// The pooled client serves exactly one send per request: the first attempt
/// on the normal (proxy-honoring) path, and only if a pooled send has not
/// already failed for this request. Everything else — 5xx retries and the
/// final direct-egress attempt — uses a fresh single-shot client, keeping
/// the "换新连接自动重试" semantics intact.
fn upstream_attempt_uses_pooled_client(
    attempt: u32,
    bypass_proxy: bool,
    pooled_send_failed: bool,
) -> bool {
    attempt == 1 && !bypass_proxy && !pooled_send_failed
}

pub async fn send_upstream_request(
    request: reqwest::RequestBuilder,
) -> anyhow::Result<reqwest::Response> {
    send_upstream_request_with_header_timeout(request, Some(UPSTREAM_HEADER_TIMEOUT)).await
}

pub async fn send_upstream_request_for_responses(
    request: reqwest::RequestBuilder,
    is_stream: bool,
) -> anyhow::Result<reqwest::Response> {
    let timeout = response_header_timeout(is_stream);
    send_upstream_request_with_header_timeout(request, timeout).await
}

/// `timeout` of `None` means: wait as long as it takes for upstream to
/// answer, with no application-level cutoff. Giant (600K+ token) turns can
/// legitimately sit in the upstream queue for well over any timeout we could
/// pick, and cutting the wait early just makes Codex re-send the same huge
/// request in a retry loop, wasting the time already spent queued upstream.
/// The client connection is kept alive during the wait via SSE keepalive
/// comments, so an unbounded wait costs nothing client-side; only reqwest's
/// own connect timeout and the OS/TCP layer bound how long a truly dead
/// upstream can hang this call.
pub async fn send_upstream_request_with_header_timeout(
    request: reqwest::RequestBuilder,
    timeout: Option<Duration>,
) -> anyhow::Result<reqwest::Response> {
    let Some(timeout) = timeout else {
        return request.send().await.context("上游请求失败");
    };
    match tokio::time::timeout(timeout, request.send()).await {
        Ok(result) => result.context("上游请求失败"),
        Err(elapsed) => {
            log_upstream_event_deduped(
                "upstream_header_timeout",
                LogLevel::Error,
                format!(
                    "上游等待 {} 秒仍未返回响应头（连一个字节都没有），已放弃本次请求；多半是隧道/上游整条链路挂死，不是本地代理的问题",
                    timeout.as_secs()
                ),
            );
            Err(elapsed)
                .with_context(|| format!("上游请求超过 {} 秒未返回响应头", timeout.as_secs()))
        }
    }
}

pub struct ChatSseToResponsesConverter {
    buffer: String,
    utf8_remainder: Vec<u8>,
    state: ChatSseState,
    failed: bool,
}

impl Default for ChatSseToResponsesConverter {
    fn default() -> Self {
        Self {
            buffer: String::new(),
            utf8_remainder: Vec::new(),
            state: ChatSseState::default(),
            failed: false,
        }
    }
}

impl ChatSseToResponsesConverter {
    pub fn with_request(original_request: &Value) -> Self {
        Self {
            state: ChatSseState::with_request(original_request),
            ..Self::default()
        }
    }

    pub fn push_bytes(&mut self, bytes: &[u8]) -> Vec<u8> {
        append_utf8_safe(&mut self.buffer, &mut self.utf8_remainder, bytes);
        let mut output = String::new();
        while let Some(block) = take_sse_block(&mut self.buffer) {
            if block.trim().is_empty() {
                continue;
            }
            self.handle_block(&block, &mut output);
            if self.failed {
                break;
            }
        }
        output.into_bytes()
    }

    pub fn finish(&mut self) -> Vec<u8> {
        if !self.utf8_remainder.is_empty() {
            self.buffer
                .push_str(&String::from_utf8_lossy(&self.utf8_remainder));
            self.utf8_remainder.clear();
        }

        let mut output = String::new();
        if !self.failed {
            self.state.finalize_into(&mut output);
        }
        output.into_bytes()
    }

    pub fn fail(&mut self, message: String, error_type: Option<String>) -> Vec<u8> {
        let mut output = String::new();
        self.state.failed_into(&mut output, message, error_type);
        self.failed = true;
        output.into_bytes()
    }

    fn handle_block(&mut self, block: &str, output: &mut String) {
        let mut event_name: Option<String> = None;
        let mut data_parts = Vec::new();
        for line in block.lines() {
            if let Some(event) = strip_sse_field(line, "event") {
                event_name = Some(event.trim().to_string());
            }
            if let Some(data) = strip_sse_field(line, "data") {
                data_parts.push(data.to_string());
            }
        }

        if data_parts.is_empty() {
            return;
        }
        let data = data_parts.join("\n");
        if data.trim() == "[DONE]" {
            self.state.finalize_into(output);
            return;
        }

        let Ok(chunk) = serde_json::from_str::<Value>(&data) else {
            return;
        };
        if event_name.as_deref() == Some("error") || chunk.get("error").is_some() {
            let (message, error_type) = extract_chat_sse_error(&chunk);
            log_upstream_event_deduped(
                "chat_sse_upstream_failure_signal",
                LogLevel::Error,
                format!(
                    "Chat Completions 流里收到上游内嵌错误事件：{message}（type={}）",
                    error_type.as_deref().unwrap_or("unknown")
                ),
            );
            self.state.failed_into(output, message, error_type);
            self.failed = true;
            return;
        }
        self.state.handle_chat_chunk_into(&chunk, output);
    }
}

pub fn is_responses_proxy_path(path: &str) -> bool {
    let path = path.split_once('?').map_or(path, |(path, _)| path);
    matches!(
        path,
        "/responses"
            | "/v1/responses"
            | "/v1/v1/responses"
            | "/codex/v1/responses"
            | "/responses/compact"
            | "/v1/responses/compact"
            | "/v1/v1/responses/compact"
            | "/codex/v1/responses/compact"
    )
}

pub fn is_chat_completions_proxy_path(path: &str) -> bool {
    let path = path.split_once('?').map_or(path, |(path, _)| path);
    matches!(
        path,
        "/chat/completions"
            | "/v1/chat/completions"
            | "/v1/v1/chat/completions"
            | "/codex/v1/chat/completions"
    )
}

pub fn is_models_proxy_path(path: &str) -> bool {
    let path = path.split_once('?').map_or(path, |(path, _)| path);
    matches!(
        path,
        "/models" | "/v1/models" | "/v1/v1/models" | "/codex/v1/models"
    )
}

pub async fn open_responses_proxy_request(
    request_json: &Value,
    relay: &ProtocolProxyUpstream,
    original_user_agent: Option<&str>,
) -> anyhow::Result<UpstreamProxyResponse> {
    let is_stream = request_json
        .get("stream")
        .and_then(Value::as_bool)
        .unwrap_or(false);
    validate_upstream(relay)?;
    let (endpoint, upstream_body, wire_api, api_key) =
        upstream_request_parts(relay, request_json.clone())?;
    // Allocate this request's trace sequence number as soon as the actual
    // (cleaned) upstream body is known, and dump it immediately: this is the
    // only place that has `upstream_body` before it goes out over the wire,
    // and dumping here means even a request that never gets a response
    // (connection failure, timeout) still leaves the outgoing body on disk.
    let trace_seq = crate::trace::next_seq();
    crate::trace::write_full_request_dump(trace_seq, &upstream_body);
    // First attempt rides the shared keep-alive pool to skip the per-turn
    // CONNECT + TLS handshake. 502/503/504 are commonly minted by an edge
    // gateway or a dead pooled connection without the origin ever seeing
    // the request, so every retry switches to a brand-new single-shot
    // client that guarantees no stale keep-alive reuse.
    let mut attempts: u32 = 0;
    let proxy_configured = egress_proxy_from_env().is_some();
    let mut pooled_send_failed = false;
    let upstream = loop {
        attempts += 1;
        let bypass_proxy = upstream_attempt_should_bypass_proxy(attempts, proxy_configured);
        let user_agent = effective_user_agent(&relay.user_agent, original_user_agent);
        let use_pooled =
            upstream_attempt_uses_pooled_client(attempts, bypass_proxy, pooled_send_failed);
        let client = if use_pooled {
            pooled_upstream_client(&user_agent)?
        } else {
            upstream_http_client_with_egress(&user_agent, bypass_proxy)?
        };
        let send_result = send_upstream_request_for_responses(
            upstream_request_builder(client, &endpoint, api_key.trim(), is_stream, &upstream_body),
            is_stream,
        )
        .await;
        let response = match send_result {
            Ok(response) => response,
            Err(error) if use_pooled && pooled_send_error_is_retryable(&error) => {
                // A reused idle connection can lose the race with the far
                // side closing it; resend once on a fresh connection
                // without consuming a 5xx retry attempt.
                pooled_send_failed = true;
                attempts -= 1;
                log_upstream_event_deduped(
                    "pooled_connection_refresh",
                    LogLevel::Warn,
                    "复用的上游连接已失效，正在换新连接重发本次请求".to_string(),
                );
                continue;
            }
            Err(error) => {
                return Err(error.context(format!(
                    "供应商「{}」请求上游失败，endpoint: {}",
                    relay.name, endpoint
                )));
            }
        };
        let status_code = response.status().as_u16();
        if upstream_status_is_retryable(status_code)
            && attempts < upstream_5xx_max_attempts(proxy_configured)
        {
            log_upstream_5xx_retry(status_code, response.headers(), attempts, proxy_configured);
            drop(response);
            let backoff_index = (attempts as usize - 1).min(UPSTREAM_5XX_RETRY_BACKOFF.len() - 1);
            tokio::time::sleep(UPSTREAM_5XX_RETRY_BACKOFF[backoff_index]).await;
            continue;
        }
        break response;
    };
    let status_code = upstream.status().as_u16();
    let (server_header, cf_ray) = upstream_edge_headers(upstream.headers());
    let content_type = upstream
        .headers()
        .get(reqwest::header::CONTENT_TYPE)
        .and_then(|value| value.to_str().ok())
        .unwrap_or("")
        .to_string();

    Ok(UpstreamProxyResponse {
        status_code,
        is_stream: is_stream || content_type.contains("text/event-stream"),
        content_type,
        wire_api,
        server_header,
        cf_ray,
        attempts,
        response: upstream,
        trace_seq: Some(trace_seq),
    })
}

pub async fn open_models_proxy_request(
    relay: &ProtocolProxyUpstream,
    original_user_agent: Option<&str>,
) -> anyhow::Result<UpstreamProxyResponse> {
    validate_upstream(&relay)?;

    let endpoint = models_url(&relay.base_url);
    let upstream = send_upstream_request(
        upstream_http_client_with_user_agent(&effective_user_agent(
            &relay.user_agent,
            original_user_agent,
        ))?
        .get(endpoint)
        .bearer_auth(relay.api_key.trim()),
    )
    .await?;
    let status_code = upstream.status().as_u16();
    let (server_header, cf_ray) = upstream_edge_headers(upstream.headers());
    let content_type = upstream
        .headers()
        .get(reqwest::header::CONTENT_TYPE)
        .and_then(|value| value.to_str().ok())
        .unwrap_or("application/json; charset=utf-8")
        .to_string();

    Ok(UpstreamProxyResponse {
        status_code,
        is_stream: false,
        content_type,
        wire_api: UpstreamWireApi::Responses,
        server_header,
        cf_ray,
        attempts: 1,
        response: upstream,
        // Models listing isn't on the Responses passthrough path `crate::
        // trace` covers.
        trace_seq: None,
    })
}

pub async fn open_chat_completions_proxy_request(
    body: &str,
    relay: &ProtocolProxyUpstream,
    original_user_agent: Option<&str>,
) -> anyhow::Result<UpstreamProxyResponse> {
    if relay.protocol != RelayProtocol::ChatCompletions {
        anyhow::bail!("当前供应商未启用 Chat Completions 协议代理");
    }
    validate_upstream(relay)?;

    let mut request_json: Value = serde_json::from_str(body)?;
    let is_stream = request_json
        .get("stream")
        .and_then(Value::as_bool)
        .unwrap_or(false);
    // Aggregate-mode routing: resolve before the request is mutated below,
    // using the model exactly as the caller sent it. This is the direct
    // `/v1/chat/completions` entry point (as opposed to Codex's own traffic,
    // which always goes through `/v1/responses` and is routed inside
    // `upstream_request_parts`), so it needs its own resolution call.
    let request_model = request_json
        .get("model")
        .and_then(Value::as_str)
        .map(str::to_string);
    let (base_url, api_key) = resolve_model_upstream(relay, request_model.as_deref());
    let base_url = base_url.to_string();
    let api_key = api_key.to_string();
    // Codex's own traffic always goes through /v1/responses (converted
    // above in `upstream_request_parts`); this path is for direct
    // `/v1/chat/completions` callers bypassing that conversion, so the
    // context budget has to be applied here too or it silently never runs
    // for those callers.
    apply_chat_completions_direct_context_budget(
        &mut request_json,
        request_model.as_deref(),
        &relay.context_budget,
    );
    let mut attempts: u32 = 0;
    let proxy_configured = egress_proxy_from_env().is_some();
    let mut pooled_send_failed = false;
    let upstream = loop {
        attempts += 1;
        let bypass_proxy = upstream_attempt_should_bypass_proxy(attempts, proxy_configured);
        let user_agent = effective_user_agent(&relay.user_agent, original_user_agent);
        let use_pooled =
            upstream_attempt_uses_pooled_client(attempts, bypass_proxy, pooled_send_failed);
        let client = if use_pooled {
            pooled_upstream_client(&user_agent)?
        } else {
            upstream_http_client_with_egress(&user_agent, bypass_proxy)?
        };
        let request_builder = client
            .post(chat_completions_url(&base_url))
            .bearer_auth(api_key.trim())
            .header(reqwest::header::CONTENT_TYPE, "application/json")
            .json(&request_json);
        let send_result = send_upstream_request_with_header_timeout(
            request_builder,
            response_header_timeout(is_stream),
        )
        .await;
        let response = match send_result {
            Ok(response) => response,
            Err(error) if use_pooled && pooled_send_error_is_retryable(&error) => {
                pooled_send_failed = true;
                attempts -= 1;
                log_upstream_event_deduped(
                    "pooled_connection_refresh",
                    LogLevel::Warn,
                    "复用的上游连接已失效，正在换新连接重发本次请求".to_string(),
                );
                continue;
            }
            Err(error) => {
                return Err(error.context(format!(
                    "供应商「{}」请求上游 Chat Completions 失败，endpoint: {}",
                    relay.name,
                    chat_completions_url(&base_url)
                )));
            }
        };
        let status_code = response.status().as_u16();
        if upstream_status_is_retryable(status_code)
            && attempts < upstream_5xx_max_attempts(proxy_configured)
        {
            log_upstream_5xx_retry(status_code, response.headers(), attempts, proxy_configured);
            drop(response);
            let backoff_index = (attempts as usize - 1).min(UPSTREAM_5XX_RETRY_BACKOFF.len() - 1);
            tokio::time::sleep(UPSTREAM_5XX_RETRY_BACKOFF[backoff_index]).await;
            continue;
        }
        break response;
    };
    let status_code = upstream.status().as_u16();
    let (server_header, cf_ray) = upstream_edge_headers(upstream.headers());
    let content_type = upstream
        .headers()
        .get(reqwest::header::CONTENT_TYPE)
        .and_then(|value| value.to_str().ok())
        .unwrap_or("")
        .to_string();

    Ok(UpstreamProxyResponse {
        status_code,
        is_stream: is_stream || content_type.contains("text/event-stream"),
        content_type,
        wire_api: UpstreamWireApi::ChatCompletions,
        server_header,
        cf_ray,
        attempts,
        response: upstream,
        // Direct `/v1/chat/completions` callers aren't on the Responses
        // passthrough path `crate::trace` covers.
        trace_seq: None,
    })
}

/// Apply the same context-budget trimming Codex's own `/v1/responses`
/// traffic gets in `upstream_request_parts`, but for the direct
/// `/v1/chat/completions` entry point (`open_chat_completions_proxy_request`)
/// — a body that's already Chat-Completions-shaped instead of
/// Responses-shaped. Split out as a plain `&mut Value` transform, independent
/// of that function's network call, so it can be unit-tested without a mock
/// upstream server. Native OpenAI models (see `is_native_openai_model`) skip
/// trimming entirely, same as the Responses/Chat Completions branches in
/// `upstream_request_parts`.
fn apply_chat_completions_direct_context_budget(
    request_json: &mut Value,
    request_model: Option<&str>,
    context_budget: &ContextBudgetConfig,
) {
    if request_model.is_some_and(is_native_openai_model) {
        log_upstream_event_deduped(
            "chat_direct_native_openai_skip",
            LogLevel::Info,
            format!(
                "OpenAI 原生模型 {}，跳过 Chat Completions 直连的上下文预算裁剪",
                request_model.unwrap_or("unknown"),
            ),
        );
        return;
    }
    let report = apply_context_budget(request_json, context_budget);
    if report.was_trimmed {
        log_upstream_event_deduped(
            "chat_direct_budget_trim",
            LogLevel::Info,
            format!(
                "Chat Completions 直连上下文预算裁剪：{} → {} tokens（剥离 {} 张图片，移除 {} 条消息）",
                report.original_estimated_tokens,
                report.final_estimated_tokens,
                report.images_stripped,
                report.messages_removed,
            ),
        );
    }
    warn_if_still_over_budget(
        "chat_direct_budget_still_over_after_trim",
        &report,
        context_budget.max_input_tokens,
    );
}

/// `None` for streaming requests: no application-level cap on how long we
/// wait for upstream's response headers (see
/// `send_upstream_request_with_header_timeout`). Non-streaming calls (e.g.
/// `/v1/models`) keep the short 30s cap — they're small, fast lookups where
/// an unbounded wait would just hide a genuinely broken upstream.
fn response_header_timeout(is_stream: bool) -> Option<Duration> {
    if is_stream {
        None
    } else {
        Some(UPSTREAM_HEADER_TIMEOUT)
    }
}

/// Request-scoped logging context — which model/session a proxied request
/// belongs to — threaded through `tokio::task_local!` rather than an
/// explicit parameter, so every `log_upstream_event_deduped` call anywhere
/// in a request's call graph (context-budget trimming, reasoning-item
/// stripping, upstream retries, ...) can tag its line without every one of
/// those functions taking a context parameter just to pass it along.
#[derive(Debug, Clone)]
struct RequestLogContext {
    model: String,
    session_label: String,
}

tokio::task_local! {
    static REQUEST_LOG_CONTEXT: RequestLogContext;
}

/// Run `fut` with `model`/`session_label` available to every diagnostic line
/// logged anywhere underneath it (see `log_upstream_event_deduped`'s
/// `[HH:MM:SS model · session_label]` prefix, built by `current_log_prefix`).
///
/// Must be called once per inbound proxy request, from within the single
/// tokio task that owns that connection/request end-to-end — see
/// `handle_streaming_responses_proxy` and the `/v1/responses` branch of
/// `route_protocol_proxy_request` in `main.rs`, both of which run inside one
/// `tokio::spawn`ed task per accepted connection (see
/// `handle_protocol_proxy_connection`'s call sites). Task-local state never
/// crosses a `tokio::spawn` boundary, which is exactly the isolation wanted
/// here: concurrent Codex sessions proxied at the same time never see each
/// other's label.
pub async fn with_request_log_context<F: std::future::Future>(
    model: String,
    session_label: String,
    fut: F,
) -> F::Output {
    REQUEST_LOG_CONTEXT
        .scope(
            RequestLogContext {
                model,
                session_label,
            },
            fut,
        )
        .await
}

/// `[HH:MM:SS model · session_label] ` (trailing space included) when called
/// from inside `with_request_log_context`'s scope, or just `[HH:MM:SS] `
/// outside of it (startup logging, or any diagnostic not tied to a single
/// proxied request).
fn current_log_prefix() -> String {
    let now = chrono::Local::now().format("%H:%M:%S").to_string();
    match REQUEST_LOG_CONTEXT.try_with(|context| format_log_prefix(&now, Some(context))) {
        Ok(prefix) => prefix,
        Err(_) => format_log_prefix(&now, None),
    }
}

/// Pure formatting behind `current_log_prefix`, split out so the prefix
/// format itself can be unit-tested deterministically — mirrors
/// `throttle_repeated_log_line`'s split from `log_upstream_event_deduped`
/// for the same reason (no real wall-clock time or task-local machinery
/// needed just to check the string shape).
fn format_log_prefix(now_hms: &str, context: Option<&RequestLogContext>) -> String {
    match context {
        Some(context) => format!("[{now_hms} {} · {}] ", context.model, context.session_label),
        None => format!("[{now_hms}] "),
    }
}

/// Throttle state for `log_upstream_event_deduped`, keyed by a fixed
/// call-site identifier so different kinds of diagnostics never share a
/// throttle window.
struct RepeatedLogState {
    last_printed_at: std::time::Instant,
    suppressed_since_print: u32,
}

/// Minimum gap between two printed lines for the same diagnostic `key`.
/// Chosen to comfortably cover Codex's fast automatic-retry bursts (which
/// can fire multiple attempts per second) while still surfacing an update
/// quickly enough that the user can tell the retry loop is still alive.
const REPEATED_LOG_THROTTLE: Duration = Duration::from_secs(3);

static REPEATED_LOG_STATE: LazyLock<Mutex<HashMap<&'static str, RepeatedLogState>>> =
    LazyLock::new(|| Mutex::new(HashMap::new()));

/// Severity of one diagnostic line, used only to pick its ANSI color in
/// `render_log_line` — it has no effect on the throttling/dedup behavior
/// above, which is keyed purely by the call site's `key`. Assigned per call
/// site by what it means for the *current request*: `Error` for a request
/// that has just failed or ended abnormally (upstream error status, stream
/// cut off, header timeout, a reasoning-only dead end, ...), `Warn` for
/// something degraded or risky but still in flight (an automatic retry, a
/// trim that's still over budget, ...), and `Info` for routine bookkeeping
/// (context trimming, reasoning-item cleanup, ...) that isn't on its own a
/// sign of trouble.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LogLevel {
    Error,
    Warn,
    Info,
    /// Routine, not a sign of trouble, but worth standing out from the rest
    /// of the uncolored `Info` stream because it's an actionable nudge the
    /// user may want to notice mid-scroll (e.g. the stale-plan reminder) —
    /// cyan, so it reads as "look here" without implying degraded/failed
    /// like `Warn`/`Error` do.
    Notice,
}

/// Print a diagnostic line to stderr, throttled per `key` to at most once
/// every `REPEATED_LOG_THROTTLE`.
///
/// Codex automatically retries a request when the upstream provider returns
/// a transient-looking error (e.g. a persistent 502 from the third-party
/// gateway), and it can also have more than one conversation/thread retrying
/// against a struggling upstream at the same time. Retries of the exact same
/// request produce byte-identical diagnostics, but interleaved retries from
/// *different* conversations produce diagnostics that keep changing in
/// small ways (e.g. a differing reasoning-item count) even though they're
/// really the same underlying "upstream is unhappy" situation. Throttling by
/// elapsed time per `key`, instead of by exact message equality, collapses
/// both cases: it prints the first occurrence immediately, then goes quiet
/// for the rest of the throttle window regardless of the exact wording, and
/// on the next print folds in how many occurrences were suppressed in
/// between so the user can still see the retry loop is active.
pub fn log_upstream_event_deduped(key: &'static str, level: LogLevel, message: String) {
    let mut state = REPEATED_LOG_STATE
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    if let Some(line) = throttle_repeated_log_line(
        &mut state,
        key,
        message,
        std::time::Instant::now(),
        REPEATED_LOG_THROTTLE,
    ) {
        eprint!(
            "{}",
            render_log_line(&current_log_prefix(), level, &line, colors_enabled())
        );
    }
}

/// Whether diagnostic lines should carry ANSI color. Deliberately *not* an
/// `isatty` check: the launch script (`Codex Gateway Lite.command`) always
/// runs the agent as `run_lite agent ... 2>&1 | tee -a agent.terminal.log`,
/// so stderr is piped straight into `tee`, never a real TTY — checking
/// `isatty` here would read "not a terminal" on every single run and colors
/// would never turn on, even though both the terminal the user is watching
/// and `tee`'s passthrough render ANSI escapes just fine on the other end of
/// that pipe. So colors default to **on** unconditionally, and only the
/// explicit opt-out convention from https://no-color.org turns them off:
/// `NO_COLOR` set to any non-empty value.
fn colors_enabled() -> bool {
    match std::env::var_os("NO_COLOR") {
        Some(value) => value.is_empty(),
        None => true,
    }
}

/// Pure formatter behind `log_upstream_event_deduped`'s stderr line, split
/// out so the coloring rules and line-spacing can be unit-tested
/// deterministically instead of capturing real stderr (mirrors
/// `throttle_repeated_log_line`'s split from the same function). `prefix` is
/// the already-built `[HH:MM:SS model · session]` string (see
/// `current_log_prefix`); `line` is the already throttle-deduped message
/// body.
///
/// The prefix is always dimmed — it's context, not the point. The message
/// body is colored by `level`: red for `Error`, yellow for `Warn`, and left
/// completely uncolored for `Info` (most lines are routine, so only the two
/// levels worth a second look get tinted). A trailing blank line is always
/// appended, colors or not, as the visual separator between consecutive
/// diagnostics — `colors_enabled = false` (i.e. `NO_COLOR` is set) strips
/// every ANSI escape but never that spacing.
fn render_log_line(prefix: &str, level: LogLevel, line: &str, colors_enabled: bool) -> String {
    if !colors_enabled {
        return format!("{prefix}{line}\n\n");
    }
    const DIM: &str = "\x1b[2m";
    const RED: &str = "\x1b[31m";
    const YELLOW: &str = "\x1b[33m";
    const CYAN: &str = "\x1b[36m";
    const RESET: &str = "\x1b[0m";
    let colored_body = match level {
        LogLevel::Error => format!("{RED}{line}{RESET}"),
        LogLevel::Warn => format!("{YELLOW}{line}{RESET}"),
        LogLevel::Notice => format!("{CYAN}{line}{RESET}"),
        LogLevel::Info => line.to_string(),
    };
    format!("{DIM}{prefix}{RESET}{colored_body}\n\n")
}

/// Pure decision logic behind `log_upstream_event_deduped`, separated out so
/// the throttling behavior can be unit-tested with a controlled clock
/// instead of capturing process stderr or sleeping in tests.
fn throttle_repeated_log_line(
    state: &mut HashMap<&'static str, RepeatedLogState>,
    key: &'static str,
    message: String,
    now: std::time::Instant,
    throttle: Duration,
) -> Option<String> {
    match state.get_mut(key) {
        Some(entry) if now.saturating_duration_since(entry.last_printed_at) < throttle => {
            entry.suppressed_since_print += 1;
            None
        }
        Some(entry) => {
            let suppressed = entry.suppressed_since_print;
            entry.last_printed_at = now;
            entry.suppressed_since_print = 0;
            Some(if suppressed > 0 {
                format!(
                    "{message}（过去 {}s 内同类提示又出现了 {suppressed} 次，已合并显示；Codex 仍在自动重试，多半是上游供应商暂时不稳定，不是本地代理的问题）",
                    throttle.as_secs()
                )
            } else {
                message
            })
        }
        None => {
            state.insert(
                key,
                RepeatedLogState {
                    last_printed_at: now,
                    suppressed_since_print: 0,
                },
            );
            Some(message)
        }
    }
}

/// After best-effort trimming, warn (deduped) if the request is still over
/// budget instead of staying silent. Trimming only removes whole
/// messages/images; a single oversized turn (e.g. one huge tool output) can
/// still leave the request over `max_input_tokens`, and the upstream is then
/// left to reject it with its own context-overflow error. Surfacing this
/// here at least makes the cause visible instead of Codex seeing an opaque
/// upstream 400 and blindly retrying.
fn warn_if_still_over_budget(
    event_key: &'static str,
    report: &ContextBudgetReport,
    max_input_tokens: u64,
) {
    if max_input_tokens == 0 || report.final_estimated_tokens <= max_input_tokens {
        return;
    }
    log_upstream_event_deduped(
        event_key,
        LogLevel::Warn,
        format!(
            "上下文预算裁剪后仍超出预算：{} > {} tokens（可能被上游以 context overflow 拒绝，建议减小单轮内容或调大 contextWindow/contextBudget）",
            report.final_estimated_tokens, max_input_tokens
        ),
    );
}

/// Resolve which `(base_url, api_key)` a request for `model` should
/// actually be sent to. `relay.model_routes` (populated only in aggregate
/// mode — see `LiteConfig::aggregate` in main.rs) maps a normalized model
/// slug to whichever provider actually serves it; a model absent from the
/// table, or aggregate mode being off (an empty table), falls back to
/// `relay`'s own `base_url`/`api_key` (the default/active provider). This
/// never overrides `relay.protocol` or `relay.context_budget` — aggregate
/// mode always forwards using the active provider's wire protocol and
/// context budget (see module-level design notes on `ModelRoute`).
fn resolve_model_upstream<'a>(
    relay: &'a ProtocolProxyUpstream,
    model: Option<&str>,
) -> (&'a str, &'a str) {
    let routed = model.and_then(|raw| relay.model_routes.get(&crate::normalized_route_key(raw)));
    match routed {
        Some(route) => (route.base_url.as_str(), route.api_key.as_str()),
        None => (relay.base_url.as_str(), relay.api_key.as_str()),
    }
}

/// Resolve the exact upstream URL a Responses request for `request_json`
/// would be routed to. Exists so trace metadata (recorded well after
/// `open_responses_proxy_request` already made this same routing decision)
/// can report it without threading another field through
/// `UpstreamProxyResponse` — this recomputes the same routing
/// `upstream_request_parts` does, without any of its budget/cleanup side
/// effects.
fn resolve_responses_endpoint(relay: &ProtocolProxyUpstream, request_json: &Value) -> String {
    let model = request_json.get("model").and_then(Value::as_str);
    let (base_url, _api_key) = resolve_model_upstream(relay, model);
    match relay.protocol {
        RelayProtocol::Responses => responses_url(base_url),
        RelayProtocol::ChatCompletions => chat_completions_url(base_url),
    }
}

/// Build and append one `trace/requests.jsonl` metadata row for a Responses
/// passthrough request, resolving `model`/`endpoint` from `relay`/
/// `request_json` so callers don't have to duplicate that logic. Shared by
/// the streaming passthrough loop (`main.rs`) and the buffered fallback
/// (`handle_responses_upstream`), so "how do we describe this request" for
/// tracing purposes lives in one place instead of two. Best-effort: see
/// `crate::trace::record_responses_request`.
#[allow(clippy::too_many_arguments)]
pub fn record_responses_trace(
    relay: &ProtocolProxyUpstream,
    request_json: &Value,
    trace_seq: Option<u64>,
    request_bytes: usize,
    is_stream: bool,
    status_code: u16,
    attempts: u32,
    response_bytes: u64,
    diagnosis_label: &'static str,
) {
    let model = request_json
        .get("model")
        .and_then(Value::as_str)
        .unwrap_or("unknown");
    let session = resolve_session_label(request_json);
    let endpoint = resolve_responses_endpoint(relay, request_json);
    crate::trace::record_responses_request(
        trace_seq,
        model,
        &session,
        &endpoint,
        request_bytes,
        is_stream,
        status_code,
        attempts,
        response_bytes,
        diagnosis_label,
    );
}

/// Threshold for the stale-plan nudge below: roughly the point where a stuck
/// step starts feeling invisible to the user without being a false-positive
/// on normal short multi-tool turns (a few tool calls in a row for one step
/// is completely ordinary; six in a row with no plan touch is not).
const STALE_PLAN_REMINDER_THRESHOLD: usize = 6;

/// Wrapped in the same `<tag>`-style convention Codex's own injected content
/// uses (`<environment_context>`, `<user_instructions>`, ...) so the model
/// recognizes this as harness-injected, not the human typing.
const PLAN_REMINDER_TEXT: &str = "<plan_reminder>Reminder: you have made several tool calls without updating your plan via update_plan. If the current step is done, mark it complete and move to the next one; if you haven't created a plan yet for this multi-step task, create one now.</plan_reminder>";

/// Count `function_call` items after the most recent `update_plan` call in a
/// Responses `input` array — or every `function_call` item if `update_plan`
/// has never been called this conversation (i.e. no plan exists yet).
fn count_function_calls_since_last_update_plan(items: &[Value]) -> usize {
    let last_update_plan_idx = items.iter().rposition(|item| {
        item.get("type").and_then(Value::as_str) == Some("function_call")
            && item.get("name").and_then(Value::as_str) == Some("update_plan")
    });
    let start = last_update_plan_idx.map(|idx| idx + 1).unwrap_or(0);
    items[start..]
        .iter()
        .filter(|item| item.get("type").and_then(Value::as_str) == Some("function_call"))
        .count()
}

/// Whether the outgoing request should carry a stale-plan nudge: only when
/// `plan_hints` is on for the active config and the model has gone
/// `STALE_PLAN_REMINDER_THRESHOLD` tool calls without touching `update_plan`.
fn should_inject_plan_reminder(items: &[Value], plan_hints: bool) -> bool {
    plan_hints
        && count_function_calls_since_last_update_plan(items) >= STALE_PLAN_REMINDER_THRESHOLD
}

/// Append the ephemeral reminder to the outgoing Responses `input` array.
/// Shaped as `role: "user"` — the same role this gateway's own context-budget
/// trim marker (see `apply_responses_context_budget`) and Codex's own
/// injected instruction messages already use for synthetic, non-literal-user
/// content, since a Responses `message` item has no role meant for "the
/// harness talking, not the user or the model". Never persisted: Codex owns
/// and resends its own full history every turn, so this only ever affects
/// the single outgoing call it's appended to.
fn inject_plan_reminder_item(items: &mut Vec<Value>) {
    items.push(json!({
        "role": "user",
        "content": PLAN_REMINDER_TEXT,
    }));
}

fn upstream_request_parts(
    relay: &ProtocolProxyUpstream,
    mut request_json: Value,
) -> anyhow::Result<(String, Value, UpstreamWireApi, String)> {
    // Resolve routing before anything else touches `request_json`, using
    // the model exactly as Codex sent it (pre-cleanup) — routing depends
    // only on which model was requested, not on any of the budget/cleanup
    // transforms below.
    let request_model = request_json
        .get("model")
        .and_then(Value::as_str)
        .map(str::to_string);
    let is_native_openai = request_model.as_deref().is_some_and(is_native_openai_model);
    let (routed_base_url, routed_api_key) = resolve_model_upstream(relay, request_model.as_deref());
    let routed_base_url = routed_base_url.to_string();
    let routed_api_key = routed_api_key.to_string();

    // Runs ahead of the Responses/Chat Completions branch below so both wire
    // formats share a single cleaning pass instead of duplicating it: the
    // Chat Completions body is derived from this same Responses-shaped
    // `request_json` via `responses_to_chat_completions`, so any
    // `input_image` left in here would reach upstream through either path
    // unchanged — and through the Chat Completions path even worse, since a
    // `function_call_output.output` array is JSON-serialized straight into
    // message text, base64 and all (see `response_output_text`).
    let images_over_budget_replaced = if is_native_openai {
        0
    } else {
        enforce_input_image_budget(&mut request_json)
    };
    if images_over_budget_replaced > 0 {
        log_upstream_event_deduped(
            "input_image_budget_strip",
            LogLevel::Warn,
            format!(
                "历史图片总体积超出 {}MB 预算，已将 {} 张较旧的历史图片替换为文本占位，避免上游因请求体过大拒绝（400 invalid_request_error）",
                INPUT_IMAGE_TOTAL_BUDGET_BYTES / (1024 * 1024),
                images_over_budget_replaced,
            ),
        );
    }
    if is_native_openai {
        log_upstream_event_deduped(
            "native_openai_skip_cleanup",
            LogLevel::Info,
            format!(
                "OpenAI 原生模型 {}，跳过图片预算、reasoning 清理与上下文裁剪",
                request_model.as_deref().unwrap_or("unknown"),
            ),
        );
    }

    // Stale-plan nudge: runs ahead of the Responses/Chat Completions branch
    // (like the image-budget pass above) and independent of `is_native_openai`
    // — `plan_hints`'s one-time `PLAN_HINTS_SUPPLEMENT` is baked into every
    // model's base_instructions regardless of native/third-party (see
    // codex_lite::build_model_catalog_json), so the mid-conversation nudge
    // that reinforces it follows the same toggle unconditionally.
    let stale_tool_calls_before_nudge = request_json
        .get("input")
        .and_then(Value::as_array)
        .filter(|items| should_inject_plan_reminder(items, relay.plan_hints))
        .map(|items| count_function_calls_since_last_update_plan(items));
    if let Some(stale_tool_calls) = stale_tool_calls_before_nudge {
        if let Some(items) = request_json.get_mut("input").and_then(Value::as_array_mut) {
            inject_plan_reminder_item(items);
        }
        log_upstream_event_deduped(
            "plan_reminder_nudge",
            LogLevel::Notice,
            format!("已提醒模型更新 update_plan（距上次调用已过 {stale_tool_calls} 次工具调用）"),
        );
    }

    match relay.protocol {
        RelayProtocol::Responses => {
            let mut body = request_json;
            if is_native_openai {
                return Ok((
                    responses_url(&routed_base_url),
                    body,
                    UpstreamWireApi::Responses,
                    routed_api_key,
                ));
            }
            let internal_items_normalized = body
                .get_mut("input")
                .and_then(Value::as_array_mut)
                .map(normalize_codex_internal_items_for_upstream)
                .unwrap_or(0);
            if internal_items_normalized > 0 {
                log_upstream_event_deduped(
                    "responses_internal_item_normalize",
                    LogLevel::Info,
                    format!(
                        "Responses 已把 {internal_items_normalized} 条 Codex 内部历史项（agent_message/custom_tool_call/加密块）转换为标准项，避免第三方上游 422 拒绝",
                    ),
                );
            }
            let report = apply_responses_context_budget(&mut body, &relay.context_budget);
            if report.stale_reasoning_items_stripped > 0 {
                log_upstream_event_deduped(
                    "responses_reasoning_strip",
                    LogLevel::Info,
                    format!(
                        "Responses 已清理 {} 条历史轮次里的过期 reasoning/thinking 块，避免切换模型后被上游拒绝",
                        report.stale_reasoning_items_stripped,
                    ),
                );
            }
            if report.was_trimmed {
                log_upstream_event_deduped(
                    "responses_budget_trim",
                    LogLevel::Info,
                    format!(
                        "Responses 上下文预算裁剪：{} → {} tokens（剥离 {} 张图片，移除 {} 条项目）",
                        report.original_estimated_tokens,
                        report.final_estimated_tokens,
                        report.images_stripped,
                        report.messages_removed,
                    ),
                );
            }
            warn_if_still_over_budget(
                "responses_budget_still_over_after_trim",
                &report,
                relay.context_budget.max_input_tokens,
            );
            Ok((
                responses_url(&routed_base_url),
                body,
                UpstreamWireApi::Responses,
                routed_api_key,
            ))
        }
        RelayProtocol::ChatCompletions => {
            if is_native_openai {
                let chat_body = responses_to_chat_completions(request_json)?;
                return Ok((
                    chat_completions_url(&routed_base_url),
                    chat_body,
                    UpstreamWireApi::ChatCompletions,
                    routed_api_key,
                ));
            }
            let stale_reasoning_items_stripped = request_json
                .get_mut("input")
                .and_then(Value::as_array_mut)
                .map(|items| strip_stale_reasoning_items(items))
                .unwrap_or(0);
            if stale_reasoning_items_stripped > 0 {
                log_upstream_event_deduped(
                    "chat_reasoning_strip",
                    LogLevel::Info,
                    format!(
                        "Chat Completions 已清理 {} 条历史轮次里的过期 reasoning/thinking 块，避免切换模型后被上游拒绝",
                        stale_reasoning_items_stripped,
                    ),
                );
            }
            let mut chat_body = responses_to_chat_completions(request_json)?;
            let report = apply_context_budget(&mut chat_body, &relay.context_budget);
            if report.was_trimmed {
                log_upstream_event_deduped(
                    "chat_budget_trim",
                    LogLevel::Info,
                    format!(
                        "上下文预算裁剪：{} → {} tokens（剥离 {} 张图片，移除 {} 条消息）",
                        report.original_estimated_tokens,
                        report.final_estimated_tokens,
                        report.images_stripped,
                        report.messages_removed,
                    ),
                );
            }
            warn_if_still_over_budget(
                "chat_budget_still_over_after_trim",
                &report,
                relay.context_budget.max_input_tokens,
            );
            Ok((
                chat_completions_url(&routed_base_url),
                chat_body,
                UpstreamWireApi::ChatCompletions,
                routed_api_key,
            ))
        }
    }
}

fn upstream_request_builder(
    client: reqwest::Client,
    endpoint: &str,
    api_key: &str,
    is_stream: bool,
    upstream_body: &Value,
) -> reqwest::RequestBuilder {
    let mut builder = client
        .post(endpoint)
        .bearer_auth(api_key)
        .header(reqwest::header::CONTENT_TYPE, "application/json");
    if is_stream {
        builder = builder
            .header(reqwest::header::ACCEPT, "text/event-stream")
            .header(reqwest::header::CACHE_CONTROL, "no-cache");
    }
    builder.json(upstream_body)
}

fn validate_upstream(relay: &ProtocolProxyUpstream) -> anyhow::Result<()> {
    if relay.base_url.trim().is_empty() {
        anyhow::bail!("上游 Base URL 不能为空");
    }
    if relay.api_key.trim().is_empty() {
        anyhow::bail!("上游 Key 不能为空");
    }
    Ok(())
}

/// Longest session label kept in a proxy diagnostic log line (see
/// `with_request_log_context`'s `[HH:MM:SS model · session_label]` prefix)
/// — long enough to stay recognizable, short enough that a long thread
/// title or first message can't push the actual diagnostic text off a
/// normal terminal width.
const SESSION_LABEL_MAX_CHARS: usize = 24;

/// Extract whichever request-scoped identifier is most likely to match a row
/// in Codex's own thread database (`~/.codex/state_5.sqlite` et al.'s
/// `threads` table — see `crate::cached_session_title`), trying fields in
/// order of how reliable they are on the actual wire format Codex's HTTP
/// Responses client sends (verified against Codex's own `codex-rs` client
/// source, since none of this is documented on the wire-format side):
///
/// - `client_metadata.thread_id`: every Responses HTTP request Codex's
///   `ModelClient` builds carries a `client_metadata` object with the
///   thread's bare UUID under this key, and it is never overridden — this is
///   exactly the value stored as `threads.id`.
/// - `prompt_cache_key`: also the thread UUID by default (Codex's
///   `ModelClient::prompt_cache_key()` falls back to the thread id), but it
///   *can* be pinned to a different stable value across a manual/remote
///   context-compaction turn, so it's a slightly less certain match than
///   `client_metadata.thread_id`.
/// - `conversation` / `conversation_id` / `previous_response_id`: not present
///   on Codex's actual Responses HTTP request body at all (those only exist
///   on its WebSocket-transport sibling struct) — kept only as a defensive
///   fallback in case some other client speaks this same passthrough
///   protocol.
fn session_id_from_responses_request(body: &Value) -> Option<String> {
    let nested_thread_id = body
        .get("client_metadata")
        .and_then(Value::as_object)
        .and_then(|metadata| metadata.get("thread_id"))
        .and_then(Value::as_str);
    if let Some(thread_id) = nested_thread_id {
        let thread_id = thread_id.trim();
        if !thread_id.is_empty() {
            return Some(thread_id.to_string());
        }
    }
    for key in [
        "prompt_cache_key",
        "conversation",
        "conversation_id",
        "previous_response_id",
    ] {
        if let Some(value) = body.get(key).and_then(Value::as_str) {
            let value = value.trim();
            if !value.is_empty() {
                return Some(value.to_string());
            }
        }
    }
    None
}

/// Fallback tier of `resolve_session_label`: the first real user message in
/// the request, skipping Codex's injected leading instruction turns
/// (environment context, permissions, AGENTS.md, ...; see
/// `item_is_instruction_message`/`LEADING_INSTRUCTION_MARKERS`). A thread's
/// own title is itself normally derived from this same message, so this
/// approximates the sqlite-lookup tier reasonably well on the paths where
/// that tier can't run (id missing from the request, thread not yet
/// committed to sqlite, database locked by the Codex app, ...).
fn first_user_message_label(request_json: &Value) -> Option<String> {
    let items = request_json.get("input")?.as_array()?;
    let message = items.iter().find(|item| {
        item.get("role").and_then(Value::as_str) == Some("user")
            && !item_is_instruction_message(item)
    })?;
    let text = instruction_text(message.get("content")?);
    let trimmed = text.trim();
    if trimmed.is_empty() {
        None
    } else {
        Some(trimmed.chars().take(SESSION_LABEL_MAX_CHARS).collect())
    }
}

/// Best-effort human-readable label for whichever Codex session/thread sent
/// `request_json`, used only to tell proxy diagnostic log lines apart when
/// several Codex conversations are proxied at once (see
/// `with_request_log_context`). Always returns something — `"?"` if neither
/// tier below produces anything — and is always at most
/// `SESSION_LABEL_MAX_CHARS` characters with newlines flattened to spaces,
/// so it can never break the one-line log format it's embedded in.
///
/// Two tiers, cheapest and most reliable first:
/// 1. Resolve an id off the request body (`session_id_from_responses_request`)
///    and look its title up in Codex's own thread database
///    (`crate::cached_session_title`) — exactly the title Codex's own UI
///    shows for that thread.
/// 2. Otherwise, the first 24 characters of the request's own first real
///    user message (`first_user_message_label`).
pub fn resolve_session_label(request_json: &Value) -> String {
    let label = session_id_from_responses_request(request_json)
        .and_then(|id| crate::cached_session_title(&id))
        .or_else(|| first_user_message_label(request_json));
    normalize_session_label(label.as_deref().unwrap_or("?"))
}

/// Enforces `resolve_session_label`'s output contract (see its doc comment)
/// regardless of which tier produced `label` — a thread title pulled from
/// sqlite is free-form text a user typed/generated and isn't pre-truncated
/// the way the `first_user_message_label` fallback already is, so this is
/// the one place both tiers' output is guaranteed to converge.
fn normalize_session_label(label: &str) -> String {
    let flattened = label.replace(['\n', '\r'], " ");
    let trimmed = flattened.trim();
    let truncated: String = trimmed.chars().take(SESSION_LABEL_MAX_CHARS).collect();
    if truncated.is_empty() {
        "?".to_string()
    } else {
        truncated
    }
}

#[cfg(test)]
mod request_log_context_tests {
    use super::*;

    #[test]
    fn format_log_prefix_without_context_is_bare_timestamp() {
        assert_eq!(format_log_prefix("22:39:41", None), "[22:39:41] ");
    }

    #[test]
    fn format_log_prefix_with_context_includes_model_and_session() {
        let context = RequestLogContext {
            model: "grok-4.5".to_string(),
            session_label: "会话摘要文本".to_string(),
        };
        assert_eq!(
            format_log_prefix("22:39:41", Some(&context)),
            "[22:39:41 grok-4.5 · 会话摘要文本] "
        );
    }

    /// `current_log_prefix`/`with_request_log_context` 是异步的，但 task_local 的
    /// `LocalKey::sync_scope` 允许在同步代码里临时搭一段作用域 —— 足够验证
    /// `current_log_prefix` 确实读到了作用域里塞进去的模型/会话标签，不需要为此专门
    /// 起一个 tokio runtime。
    #[test]
    fn current_log_prefix_reads_scoped_context() {
        REQUEST_LOG_CONTEXT.sync_scope(
            RequestLogContext {
                model: "grok-4.5".to_string(),
                session_label: "调试会话".to_string(),
            },
            || {
                let prefix = current_log_prefix();
                assert!(
                    prefix.contains("grok-4.5 · 调试会话"),
                    "前缀里应带上作用域内的模型和会话标签，实际是: {prefix}"
                );
            },
        );
    }

    #[test]
    fn current_log_prefix_without_scope_is_bare_timestamp() {
        let prefix = current_log_prefix();
        assert!(
            !prefix.contains('·'),
            "作用域外不应该带模型/会话标签，实际是: {prefix}"
        );
        assert!(prefix.starts_with('['));
        assert!(prefix.ends_with("] "));
    }

    /// 需求里点名的“resolve_session_label 兜底路径”单测：请求体里没有任何可用于
    /// sqlite 反查的 id 字段，第一条 user 消息又是注入的环境说明（应被
    /// `item_is_instruction_message` 跳过），因此必须落到第二条真实用户消息，取前
    /// 24 个字符。
    #[test]
    fn resolve_session_label_falls_back_to_first_real_user_message() {
        let request_json = json!({
            "model": "grok-4.5",
            "input": [
                {
                    "role": "user",
                    "content": [
                        {
                            "type": "input_text",
                            "text": "<environment_context>cwd=/tmp</environment_context>"
                        }
                    ]
                },
                {
                    "role": "user",
                    "content": [
                        {
                            "type": "input_text",
                            "text": "帮我看看这个 Rust 报错到底是哪里出的问题，我这边跑不通"
                        }
                    ]
                }
            ]
        });

        let label = resolve_session_label(&request_json);
        let expected: String = "帮我看看这个 Rust 报错到底是哪里出的问题，我这边跑不通"
            .chars()
            .take(SESSION_LABEL_MAX_CHARS)
            .collect();
        assert_eq!(label, expected);
    }

    #[test]
    fn resolve_session_label_defaults_to_placeholder_when_nothing_matches() {
        let request_json = json!({ "model": "grok-4.5", "input": [] });
        assert_eq!(resolve_session_label(&request_json), "?");
    }

    #[test]
    fn normalize_session_label_flattens_newlines_and_truncates() {
        let label =
            normalize_session_label("第一行\n第二行带一些额外文字用来把它撑到超过二十四个字符");
        assert!(!label.contains('\n'));
        assert!(label.chars().count() <= SESSION_LABEL_MAX_CHARS);
    }
}

fn effective_user_agent(configured_user_agent: &str, original_user_agent: Option<&str>) -> String {
    let configured_user_agent = configured_user_agent.trim();
    if !configured_user_agent.is_empty() {
        return configured_user_agent.to_string();
    }
    original_user_agent
        .map(str::trim)
        .filter(|user_agent| !user_agent.is_empty())
        .unwrap_or("")
        .to_string()
}

pub async fn handle_responses_proxy_request(
    body: &str,
    relay: &ProtocolProxyUpstream,
    original_user_agent: Option<&str>,
) -> anyhow::Result<ProxyHttpResponse> {
    let request_json: Value = serde_json::from_str(body)?;
    let upstream = open_responses_proxy_request(&request_json, relay, original_user_agent).await?;
    handle_responses_upstream(upstream, &request_json, relay).await
}

/// Read an upstream response body chunk-by-chunk, applying the idle timeout
/// between chunks rather than to the whole download. A slow-but-still-active
/// non-streaming generation can legitimately take longer than
/// `UPSTREAM_STREAM_IDLE_TIMEOUT` in total; what actually indicates a stuck
/// upstream is no new bytes arriving for that long.
async fn read_upstream_body_with_idle_timeout(
    response: &mut reqwest::Response,
) -> anyhow::Result<Vec<u8>> {
    let mut body = Vec::new();
    loop {
        match tokio::time::timeout(UPSTREAM_STREAM_IDLE_TIMEOUT, response.chunk()).await {
            Ok(Ok(Some(chunk))) => body.extend_from_slice(&chunk),
            Ok(Ok(None)) => break,
            Ok(Err(error)) => {
                return Err(anyhow::Error::new(error).context("读取上游响应体失败"));
            }
            Err(_) => {
                anyhow::bail!(
                    "上游响应体超过 {} 秒没有新数据，读取超时",
                    UPSTREAM_STREAM_IDLE_TIMEOUT.as_secs()
                );
            }
        }
    }
    Ok(body)
}

/// Convert an already-opened upstream response into a buffered `ProxyHttpResponse`.
/// Useful as a fallback when true streaming cannot be used.
///
/// Also runs the reasoning-only-completion diagnosis (see
/// `ResponsesDiagnosisOutcome`) on whichever shape of body this branch ends
/// up with, and records one `trace/requests.jsonl` row before returning —
/// the streaming passthrough path (`main.rs`) does the equivalent via
/// `ResponsesPassthroughDiagnosis` chunk-by-chunk instead, since it never
/// buffers the whole body here.
pub async fn handle_responses_upstream(
    mut upstream: UpstreamProxyResponse,
    request_json: &Value,
    relay: &ProtocolProxyUpstream,
) -> anyhow::Result<ProxyHttpResponse> {
    let status_code = upstream.status_code;
    let upstream_content_type = upstream.content_type;
    let is_stream = upstream.is_stream;
    let wire_api = upstream.wire_api;
    let server_header = upstream.server_header;
    let cf_ray = upstream.cf_ray;
    let attempts = upstream.attempts;
    let trace_seq = upstream.trace_seq;
    let upstream_body = read_upstream_body_with_idle_timeout(&mut upstream.response).await?;
    let response_bytes = upstream_body.len() as u64;
    // The raw outgoing request bytes aren't threaded down into this
    // buffered path, so `request_bytes` is a re-serialized approximation
    // rather than the exact wire size — acceptable for a diagnostic trace
    // (the streaming passthrough path in main.rs uses the exact byte count
    // instead, since it has the original request bytes in hand).
    let request_bytes = serde_json::to_vec(request_json)
        .map(|bytes| bytes.len())
        .unwrap_or(0);

    let record_trace = |outcome: ResponsesDiagnosisOutcome| {
        record_responses_trace(
            relay,
            request_json,
            trace_seq,
            request_bytes,
            is_stream,
            status_code,
            attempts,
            response_bytes,
            outcome.trace_label(),
        );
    };
    let warn_if_reasoning_only = |details: &Option<ReasoningOnlyDetails>| {
        if let Some(details) = details {
            log_reasoning_only_warning(details);
        }
    };

    if !(200..300).contains(&status_code) {
        let error =
            responses_error_from_upstream(status_code, &upstream_content_type, &upstream_body);
        let message = error
            .get("error")
            .and_then(|value| value.get("message"))
            .and_then(Value::as_str)
            .unwrap_or("Unknown error");
        let attribution =
            upstream_error_attribution(&server_header, &cf_ray, attempts, status_code);
        log_upstream_event_deduped(
            "upstream_error_status",
            LogLevel::Error,
            format!("上游返回 HTTP {status_code}: {message}{attribution}"),
        );
        // An error response never carries a `response.completed` event, so
        // there is nothing to diagnose beyond the error itself.
        record_trace(ResponsesDiagnosisOutcome::NoCompletedEvent);
        return Ok(ProxyHttpResponse {
            status: http_status_line(status_code),
            content_type: "application/json; charset=utf-8".to_string(),
            body: serde_json::to_vec(&error)?,
        });
    }

    if wire_api == UpstreamWireApi::Responses {
        // `is_stream` here reflects whether the upstream *actually* replied
        // with SSE (see `open_responses_proxy_request`), independent of
        // `wire_api` — a Responses-protocol upstream can still be buffered
        // into raw SSE text when true passthrough streaming wasn't used, so
        // `upstream_body` needs the same "find response.completed" scan as
        // the streaming path rather than being parsed as bare JSON.
        let (outcome, details) = if is_stream {
            let text = String::from_utf8_lossy(&upstream_body);
            diagnose_responses_sse_text(&text)
        } else {
            serde_json::from_slice::<Value>(&upstream_body)
                .ok()
                .map(|parsed| diagnose_response_object(&parsed))
                .unwrap_or((ResponsesDiagnosisOutcome::NoCompletedEvent, None))
        };
        warn_if_reasoning_only(&details);
        record_trace(outcome);
        return Ok(ProxyHttpResponse {
            status: "200 OK".to_string(),
            content_type: if upstream_content_type.is_empty() {
                "application/json; charset=utf-8".to_string()
            } else {
                upstream_content_type
            },
            body: upstream_body.to_vec(),
        });
    }

    if is_stream {
        let text = String::from_utf8_lossy(&upstream_body);
        let converted = chat_sse_to_responses_sse_with_request(&text, request_json);
        let (outcome, details) = diagnose_responses_sse_text(&converted);
        warn_if_reasoning_only(&details);
        record_trace(outcome);
        return Ok(ProxyHttpResponse {
            status: "200 OK".to_string(),
            content_type: "text/event-stream; charset=utf-8".to_string(),
            body: converted.into_bytes(),
        });
    }

    let chat_json: Value = serde_json::from_slice(&upstream_body)?;
    let response_json = chat_completion_to_response_with_request(chat_json, request_json)?;
    let (outcome, details) = diagnose_response_object(&response_json);
    warn_if_reasoning_only(&details);
    record_trace(outcome);
    Ok(ProxyHttpResponse {
        status: "200 OK".to_string(),
        content_type: "application/json; charset=utf-8".to_string(),
        body: serde_json::to_vec(&response_json)?,
    })
}

pub fn chat_completions_url(base_url: &str) -> String {
    let skip_version_prefix = base_url.trim().ends_with('#');
    let base = base_url.trim().trim_end_matches('#').trim_end_matches('/');
    if base.to_ascii_lowercase().ends_with("/chat/completions") {
        return base.to_string();
    }
    let origin_only = base
        .split_once("://")
        .map_or(!base.contains('/'), |(_, rest)| !rest.contains('/'));
    let mut url = if skip_version_prefix || has_version_suffix(base) || !origin_only {
        format!("{base}/chat/completions")
    } else {
        format!("{base}/v1/chat/completions")
    };
    while url.contains("/v1/v1") {
        url = url.replace("/v1/v1", "/v1");
    }
    url
}

pub fn responses_url(base_url: &str) -> String {
    let skip_version_prefix = base_url.trim().ends_with('#');
    let base = base_url.trim().trim_end_matches('#').trim_end_matches('/');
    if base.to_ascii_lowercase().ends_with("/responses") {
        return base.to_string();
    }
    let origin_only = base
        .split_once("://")
        .map_or(!base.contains('/'), |(_, rest)| !rest.contains('/'));
    let mut url = if skip_version_prefix || has_version_suffix(base) || !origin_only {
        format!("{base}/responses")
    } else {
        format!("{base}/v1/responses")
    };
    while url.contains("/v1/v1") {
        url = url.replace("/v1/v1", "/v1");
    }
    url
}

pub fn models_url(base_url: &str) -> String {
    let skip_version_prefix = base_url.trim().ends_with('#');
    let mut base = base_url
        .trim()
        .trim_end_matches('#')
        .trim_end_matches('/')
        .to_string();
    if base.to_ascii_lowercase().ends_with("/chat/completions") {
        base.truncate(base.len() - "/chat/completions".len());
    }
    if base.to_ascii_lowercase().ends_with("/models") {
        return base;
    }
    let origin_only = base
        .split_once("://")
        .map_or(!base.contains('/'), |(_, rest)| !rest.contains('/'));
    let mut url = if skip_version_prefix || has_version_suffix(&base) || !origin_only {
        format!("{base}/models")
    } else {
        format!("{base}/v1/models")
    };
    while url.contains("/v1/v1") {
        url = url.replace("/v1/v1", "/v1");
    }
    url
}

fn has_version_suffix(base_url: &str) -> bool {
    let segment = base_url.rsplit('/').next().unwrap_or(base_url);
    let Some(rest) = segment.strip_prefix('v') else {
        return false;
    };
    rest.chars().next().is_some_and(|ch| ch.is_ascii_digit())
}

pub fn chat_sse_to_responses_sse(input: &str) -> String {
    let mut converter = ChatSseToResponsesConverter::default();
    let mut output = converter.push_bytes(input.as_bytes());
    output.extend(converter.finish());
    String::from_utf8(output).unwrap_or_default()
}

pub fn chat_sse_to_responses_sse_with_request(input: &str, original_request: &Value) -> String {
    let mut converter = ChatSseToResponsesConverter::with_request(original_request);
    let mut output = converter.push_bytes(input.as_bytes());
    output.extend(converter.finish());
    String::from_utf8(output).unwrap_or_default()
}

pub fn response_id_from_chat_id(id: Option<&str>) -> String {
    let id = id.unwrap_or("compat");
    if id.starts_with("resp_") {
        id.to_string()
    } else {
        format!("resp_{id}")
    }
}

fn push_sse(output: &mut String, event: &str, data: Value) {
    output.push_str("event: ");
    output.push_str(event);
    output.push_str("\ndata: ");
    output.push_str(&serde_json::to_string(&data).unwrap_or_default());
    output.push_str("\n\n");
}

/// Build a standalone `response.failed` SSE frame for the Responses
/// passthrough path, where upstream bytes are relayed verbatim and never
/// parsed into a `ChatSseState`. Used when the upstream stream is
/// interrupted or idle-times-out, so Codex sees an explicit terminal
/// failure event instead of the chunked stream just ending mid-flight.
pub fn responses_passthrough_failure_sse(message: &str, error_type: &str) -> Vec<u8> {
    let mut output = String::new();
    let response = json!({
        "id": "resp_compat",
        "object": "response",
        "status": "failed",
        "output": [],
        "error": {
            "message": message,
            "type": error_type
        }
    });
    push_sse(
        &mut output,
        "response.failed",
        json!({
            "type": "response.failed",
            "response": response
        }),
    );
    output.into_bytes()
}

/// Terminal-failure / anomaly markers that can appear *inside* an
/// otherwise-200-OK Responses stream body. The passthrough path relays
/// chunks verbatim without parsing them (unlike the Chat Completions path,
/// which parses every SSE block), so without this scan a
/// `response.failed`/`response.incomplete` event embedded mid-stream — or a
/// bare `error` object — sails through unlogged: Codex just sees the turn
/// end abruptly and there is nothing in `agent.terminal.log` to explain why.
const RESPONSES_PASSTHROUGH_FAILURE_MARKERS: &[&str] = &[
    "response.failed",
    "response.incomplete",
    "\"type\":\"error\"",
    "\"type\": \"error\"",
    "event: error",
];

/// Best-effort scan of a raw passthrough chunk for the markers above.
/// Purely diagnostic — never alters what gets forwarded to Codex, only logs
/// (deduped) so a silent mid-stream failure leaves a trace instead of
/// vanishing without explanation. A marker split across a chunk boundary is
/// simply missed, which is acceptable for a diagnostic aid and not worth
/// buffering the whole stream to catch.
pub fn scan_responses_passthrough_chunk_for_upstream_failure(chunk: &[u8]) {
    let text = String::from_utf8_lossy(chunk);
    if !RESPONSES_PASSTHROUGH_FAILURE_MARKERS
        .iter()
        .any(|marker| text.contains(marker))
    {
        return;
    }
    let detail: String = text.chars().take(500).collect();
    log_upstream_event_deduped(
        "responses_passthrough_upstream_failure_signal",
        LogLevel::Error,
        format!(
            "Responses 透传流里检测到上游内嵌失败/错误信号（HTTP 200 但流内容标记失败），原始片段（截断至 500 字符）：{detail}"
        ),
    );
}

/// Buffer cap for `ResponsesPassthroughDiagnosis`. The failure mode this
/// diagnoses (upstream returns HTTP 200, stream ends cleanly, but `output`
/// contains only `reasoning` items — no message/tool call) always produces a
/// small response: there is, by definition, no substantial message body.
/// Anything this large is assumed to already contain a real message/tool
/// call, so diagnosis is abandoned rather than pay to buffer arbitrarily
/// large streams just to prove a negative.
const RESPONSES_DIAGNOSIS_MAX_BYTES: usize = 4 * 1024 * 1024;

/// Cap on the reasoning-text snippet included in the warning log line
/// (`format_reasoning_only_message`), so a long chain-of-thought doesn't
/// flood the terminal.
const REASONING_SNIPPET_MAX_CHARS: usize = 300;

/// Outcome of diagnosing one finished Responses passthrough stream (or
/// buffered response) for the "HTTP 200, stream ended normally, but
/// `output` contains only `reasoning` items" failure mode — Codex silently
/// ends the turn in this case because there is no message/tool call to act
/// on, but nothing in `agent.terminal.log` explains why. Also doubles as the
/// label recorded in `trace/requests.jsonl` (`trace_label`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ResponsesDiagnosisOutcome {
    /// Saw `response.completed` and its `output` either was empty or
    /// contained at least one non-`reasoning` item — nothing suspicious.
    Normal,
    /// Saw `response.completed` and its `output` was non-empty but every
    /// item had `type == "reasoning"` — the bug this diagnosis exists to
    /// catch.
    ReasoningOnly,
    /// Buffered bytes would have exceeded `RESPONSES_DIAGNOSIS_MAX_BYTES`
    /// before the stream finished, so diagnosis was abandoned (see that
    /// constant's doc comment for why this is treated as "assumed healthy"
    /// rather than as its own warning).
    SkippedTooLarge,
    /// The stream ended (cleanly or otherwise) without ever observing a
    /// `response.completed` event — e.g. it was interrupted or idle-timed
    /// out before completing. Existing logging already covers those cases
    /// (`responses_passthrough_upstream_failure_signal`, idle-timeout /
    /// read-error handling in the streaming loop), so this variant
    /// deliberately never triggers its own warning.
    NoCompletedEvent,
}

impl ResponsesDiagnosisOutcome {
    /// Short machine-readable label recorded in `trace/requests.jsonl`'s
    /// `diagnosis` field.
    pub fn trace_label(self) -> &'static str {
        match self {
            Self::Normal => "normal",
            Self::ReasoningOnly => "reasoning_only",
            Self::SkippedTooLarge => "skipped_too_large",
            Self::NoCompletedEvent => "no_completed_event",
        }
    }
}

/// Details extracted from a `response.completed` event whose `output` was
/// reasoning-only, used to build the warning log line — see
/// `format_reasoning_only_message`.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ReasoningOnlyDetails {
    pub response_id: String,
    pub model: String,
    pub output_tokens: Option<u64>,
    pub reasoning_tokens: Option<u64>,
    /// Best-effort text snippet pulled from the reasoning item(s)'
    /// `summary`/`content` fields, already truncated to
    /// `REASONING_SNIPPET_MAX_CHARS`.
    pub reasoning_snippet: String,
}

/// Per-stream diagnosis state for the Responses passthrough path (raw bytes
/// relayed verbatim, never parsed — see
/// `scan_responses_passthrough_chunk_for_upstream_failure` above). Buffers
/// raw SSE bytes as they arrive so `finish()` can, once the stream ends,
/// look for a `response.completed` event and check whether its `output` was
/// reasoning-only.
///
/// Independent of the stateless failure-marker scan above: that one catches
/// embedded failure/error markers, this one catches a *different* shape of
/// silent failure — HTTP 200, clean stream end, but an `output` with
/// nothing actionable in it.
pub struct ResponsesPassthroughDiagnosis {
    buffer: Vec<u8>,
    /// Total bytes observed, independent of `abandoned` — kept even after
    /// buffering stops so callers can still report an accurate
    /// `response_bytes` count in `trace/requests.jsonl`.
    total_bytes: u64,
    /// Set once buffering would exceed `RESPONSES_DIAGNOSIS_MAX_BYTES`; from
    /// that point on `push_chunk` stops copying bytes (the outcome is
    /// already decided) so a huge stream doesn't keep reallocating.
    abandoned: bool,
}

impl ResponsesPassthroughDiagnosis {
    pub fn new() -> Self {
        Self {
            buffer: Vec::new(),
            total_bytes: 0,
            abandoned: false,
        }
    }

    /// Feed one more raw chunk, exactly as relayed to Codex.
    pub fn push_chunk(&mut self, chunk: &[u8]) {
        self.total_bytes += chunk.len() as u64;
        if self.abandoned {
            return;
        }
        if self.buffer.len() + chunk.len() > RESPONSES_DIAGNOSIS_MAX_BYTES {
            self.abandoned = true;
            self.buffer.clear();
            self.buffer.shrink_to_fit();
            return;
        }
        self.buffer.extend_from_slice(chunk);
    }

    /// Total bytes observed so far, for `trace/requests.jsonl`'s
    /// `response_bytes` field.
    pub fn total_bytes(&self) -> u64 {
        self.total_bytes
    }

    /// Finish the diagnosis once the stream has ended, returning the
    /// outcome and — only for `ReasoningOnly` — the details to log.
    pub fn finish(self) -> (ResponsesDiagnosisOutcome, Option<ReasoningOnlyDetails>) {
        if self.abandoned {
            return (ResponsesDiagnosisOutcome::SkippedTooLarge, None);
        }
        let text = String::from_utf8_lossy(&self.buffer);
        diagnose_responses_sse_text(&text)
    }
}

impl Default for ResponsesPassthroughDiagnosis {
    fn default() -> Self {
        Self::new()
    }
}

/// Split raw SSE text into event blocks, find the `response` object carried
/// by the last `response.completed` event, and diagnose its `output`.
/// Shared between the passthrough path (`ResponsesPassthroughDiagnosis::
/// finish`, raw upstream bytes) and the buffered Chat-Completions-streaming
/// fallback (`handle_responses_upstream`, after converting to
/// Responses-shaped SSE text), so both funnel through the same decision
/// logic instead of duplicating it.
fn diagnose_responses_sse_text(
    text: &str,
) -> (ResponsesDiagnosisOutcome, Option<ReasoningOnlyDetails>) {
    match find_responses_completed_event(text) {
        Some(response) => diagnose_response_object(&response),
        None => (ResponsesDiagnosisOutcome::NoCompletedEvent, None),
    }
}

/// Scan `text` for the `response` object carried by the last
/// `response.completed` SSE event (`{"type": "response.completed",
/// "response": {...}}`, see `ChatSseState::finalize_into`). Blocks are
/// separated per the SSE spec by a blank line — `take_sse_block` already
/// implements that splitting and is shared with the streaming converter.
/// Unparseable or irrelevant blocks are skipped silently: this is a
/// best-effort diagnostic, not a strict parser, and real upstreams
/// occasionally interleave comment/keepalive lines that are not JSON.
fn find_responses_completed_event(text: &str) -> Option<Value> {
    let mut buffer = text.to_string();
    let mut found = None;
    while let Some(block) = take_sse_block(&mut buffer) {
        for line in block.lines() {
            let Some(data) = strip_sse_field(line, "data") else {
                continue;
            };
            let Ok(parsed) = serde_json::from_str::<Value>(data) else {
                continue;
            };
            if parsed.get("type").and_then(Value::as_str) == Some("response.completed") {
                if let Some(response) = parsed.get("response") {
                    found = Some(response.clone());
                }
            }
        }
    }
    found
}

/// Given the `response` object from a `response.completed` event, decide
/// whether its `output` array is reasoning-only.
fn diagnose_response_object(
    response: &Value,
) -> (ResponsesDiagnosisOutcome, Option<ReasoningOnlyDetails>) {
    let Some(output) = response.get("output").and_then(Value::as_array) else {
        return (ResponsesDiagnosisOutcome::Normal, None);
    };
    if output.is_empty() {
        return (ResponsesDiagnosisOutcome::Normal, None);
    }
    let all_reasoning = output
        .iter()
        .all(|item| item.get("type").and_then(Value::as_str) == Some("reasoning"));
    if !all_reasoning {
        return (ResponsesDiagnosisOutcome::Normal, None);
    }

    let response_id = response
        .get("id")
        .and_then(Value::as_str)
        .unwrap_or("")
        .to_string();
    let model = response
        .get("model")
        .and_then(Value::as_str)
        .unwrap_or("")
        .to_string();
    let output_tokens = response
        .pointer("/usage/output_tokens")
        .and_then(Value::as_u64);
    let reasoning_tokens = response
        .pointer("/usage/output_tokens_details/reasoning_tokens")
        .and_then(Value::as_u64);
    // `extract_reasoning_summary_text` already knows how to pull readable
    // text out of a reasoning item's `summary`/`content`/`reasoning_content`
    // fields (see its use in `chat_reasoning_to_response_output_item`);
    // reused here instead of duplicating that extraction logic.
    let reasoning_snippet: String = output
        .iter()
        .find_map(extract_reasoning_summary_text)
        .unwrap_or_default()
        .chars()
        .take(REASONING_SNIPPET_MAX_CHARS)
        .collect();

    (
        ResponsesDiagnosisOutcome::ReasoningOnly,
        Some(ReasoningOnlyDetails {
            response_id,
            model,
            output_tokens,
            reasoning_tokens,
            reasoning_snippet,
        }),
    )
}

/// Pure formatter for the reasoning-only warning line, split out from
/// `log_reasoning_only_warning` so the exact wording is unit-testable
/// without capturing stderr (mirrors how `throttle_repeated_log_line` is
/// tested separately from `log_upstream_event_deduped`).
fn format_reasoning_only_message(details: &ReasoningOnlyDetails) -> String {
    let output_tokens = details
        .output_tokens
        .map(|value| value.to_string())
        .unwrap_or_else(|| "未知".to_string());
    let reasoning_tokens = details
        .reasoning_tokens
        .map(|value| value.to_string())
        .unwrap_or_else(|| "未知".to_string());
    format!(
        "Responses 上游流正常结束但输出只有 reasoning（模型只思考没答复）：model={}, response_id={}, output_tokens={}, reasoning_tokens={}, reasoning 片段={}",
        details.model,
        details.response_id,
        output_tokens,
        reasoning_tokens,
        details.reasoning_snippet
    )
}

/// Log (deduped, via the existing throttled logger) a reasoning-only
/// diagnosis, so repeated hits across Codex's automatic retries collapse
/// the same way other upstream diagnostics do. `pub` because the streaming
/// passthrough loop in main.rs runs its own `ResponsesPassthroughDiagnosis`
/// and needs to trigger this same warning directly (see
/// `handle_streaming_responses_proxy`).
pub fn log_reasoning_only_warning(details: &ReasoningOnlyDetails) {
    log_upstream_event_deduped(
        "responses_reasoning_only_completion",
        LogLevel::Error,
        format_reasoning_only_message(details),
    );
}

#[cfg(test)]
mod responses_diagnosis_tests {
    use super::*;

    /// Build a `response.completed` SSE block in the same shape
    /// `ChatSseState::finalize_into` emits in real traffic, so these tests
    /// exercise the same parsing path (`find_responses_completed_event`)
    /// real streams do rather than a hand-simplified shape.
    fn completed_event_sse(response: &Value) -> String {
        let envelope = json!({
            "type": "response.completed",
            "response": response
        });
        format!(
            "event: response.completed\ndata: {}\n\n",
            serde_json::to_string(&envelope).unwrap()
        )
    }

    fn reasoning_item(text: &str) -> Value {
        json!({
            "type": "reasoning",
            "summary": [{ "type": "summary_text", "text": text }]
        })
    }

    #[test]
    fn finish_flags_reasoning_only_completion_with_full_details() {
        let response = json!({
            "id": "resp_abc123",
            "model": "gpt-5-codex",
            "output": [reasoning_item("thinking hard about the problem")],
            "usage": {
                "output_tokens": 42,
                "output_tokens_details": { "reasoning_tokens": 40 }
            }
        });
        let sse = completed_event_sse(&response);

        let mut diagnosis = ResponsesPassthroughDiagnosis::new();
        // Split across two chunks to mirror a real chunked upstream body
        // where the `response.completed` event can straddle a chunk
        // boundary — `finish()` only parses after all bytes are buffered,
        // so this must behave identically to a single push.
        let (first, second) = sse.split_at(sse.len() / 2);
        diagnosis.push_chunk(first.as_bytes());
        diagnosis.push_chunk(second.as_bytes());
        assert_eq!(diagnosis.total_bytes(), sse.len() as u64);

        let (outcome, details) = diagnosis.finish();
        assert_eq!(outcome, ResponsesDiagnosisOutcome::ReasoningOnly);
        assert_eq!(outcome.trace_label(), "reasoning_only");
        let details = details.expect("reasoning-only outcome must carry details");
        assert_eq!(
            details,
            ReasoningOnlyDetails {
                response_id: "resp_abc123".to_string(),
                model: "gpt-5-codex".to_string(),
                output_tokens: Some(42),
                reasoning_tokens: Some(40),
                reasoning_snippet: "thinking hard about the problem".to_string(),
            }
        );
    }

    #[test]
    fn finish_leaves_message_bearing_completion_unflagged() {
        let response = json!({
            "id": "resp_def456",
            "model": "gpt-5-codex",
            "output": [
                reasoning_item("thinking..."),
                { "type": "message", "content": [{ "type": "output_text", "text": "here you go" }] }
            ],
            "usage": { "output_tokens": 10, "output_tokens_details": { "reasoning_tokens": 4 } }
        });
        let mut diagnosis = ResponsesPassthroughDiagnosis::new();
        diagnosis.push_chunk(completed_event_sse(&response).as_bytes());

        let (outcome, details) = diagnosis.finish();
        assert_eq!(outcome, ResponsesDiagnosisOutcome::Normal);
        assert_eq!(outcome.trace_label(), "normal");
        assert!(details.is_none());
    }

    #[test]
    fn finish_leaves_empty_output_unflagged() {
        let response = json!({
            "id": "resp_empty",
            "model": "gpt-5-codex",
            "output": [],
            "usage": { "output_tokens": 0 }
        });
        let mut diagnosis = ResponsesPassthroughDiagnosis::new();
        diagnosis.push_chunk(completed_event_sse(&response).as_bytes());

        let (outcome, details) = diagnosis.finish();
        assert_eq!(outcome, ResponsesDiagnosisOutcome::Normal);
        assert!(details.is_none());
    }

    #[test]
    fn finish_reports_no_completed_event_when_stream_never_completed() {
        // Simulates an interrupted/timed-out stream: some in-progress delta
        // events arrived, but no `response.completed` ever showed up — must
        // not warn (see `ResponsesDiagnosisOutcome::NoCompletedEvent`'s doc
        // comment).
        let mut diagnosis = ResponsesPassthroughDiagnosis::new();
        diagnosis.push_chunk(
            b"event: response.output_text.delta\ndata: {\"type\":\"response.output_text.delta\",\"delta\":\"hi\"}\n\n",
        );

        let (outcome, details) = diagnosis.finish();
        assert_eq!(outcome, ResponsesDiagnosisOutcome::NoCompletedEvent);
        assert_eq!(outcome.trace_label(), "no_completed_event");
        assert!(details.is_none());
    }

    #[test]
    fn finish_reports_no_completed_event_for_an_empty_stream() {
        let diagnosis = ResponsesPassthroughDiagnosis::new();
        let (outcome, details) = diagnosis.finish();
        assert_eq!(outcome, ResponsesDiagnosisOutcome::NoCompletedEvent);
        assert!(details.is_none());
    }

    #[test]
    fn push_chunk_abandons_buffering_past_the_size_cap_but_keeps_counting_bytes() {
        let mut diagnosis = ResponsesPassthroughDiagnosis::new();
        let big_chunk = vec![b'a'; RESPONSES_DIAGNOSIS_MAX_BYTES + 1];
        diagnosis.push_chunk(&big_chunk);
        // Total byte count must stay accurate for `trace/requests.jsonl`'s
        // `response_bytes` even once diagnosis itself is abandoned.
        assert_eq!(
            diagnosis.total_bytes(),
            (RESPONSES_DIAGNOSIS_MAX_BYTES + 1) as u64
        );

        // Feeding a real `response.completed` afterwards must not resurrect
        // diagnosis: once abandoned, always abandoned for this stream.
        let response = json!({
            "id": "resp_big",
            "model": "gpt-5-codex",
            "output": [reasoning_item("late arrival")],
            "usage": { "output_tokens": 1, "output_tokens_details": { "reasoning_tokens": 1 } }
        });
        diagnosis.push_chunk(completed_event_sse(&response).as_bytes());

        let (outcome, details) = diagnosis.finish();
        assert_eq!(outcome, ResponsesDiagnosisOutcome::SkippedTooLarge);
        assert_eq!(outcome.trace_label(), "skipped_too_large");
        assert!(details.is_none());
    }

    #[test]
    fn format_reasoning_only_message_includes_all_fields_when_present() {
        let details = ReasoningOnlyDetails {
            response_id: "resp_abc123".to_string(),
            model: "gpt-5-codex".to_string(),
            output_tokens: Some(42),
            reasoning_tokens: Some(40),
            reasoning_snippet: "thinking hard about the problem".to_string(),
        };
        assert_eq!(
            format_reasoning_only_message(&details),
            "Responses 上游流正常结束但输出只有 reasoning（模型只思考没答复）：model=gpt-5-codex, response_id=resp_abc123, output_tokens=42, reasoning_tokens=40, reasoning 片段=thinking hard about the problem"
        );
    }

    #[test]
    fn format_reasoning_only_message_uses_placeholder_for_missing_numbers() {
        let details = ReasoningOnlyDetails {
            response_id: "resp_xyz".to_string(),
            model: "custom-model".to_string(),
            output_tokens: None,
            reasoning_tokens: None,
            reasoning_snippet: String::new(),
        };
        assert_eq!(
            format_reasoning_only_message(&details),
            "Responses 上游流正常结束但输出只有 reasoning（模型只思考没答复）：model=custom-model, response_id=resp_xyz, output_tokens=未知, reasoning_tokens=未知, reasoning 片段="
        );
    }
}

#[derive(Debug, Default)]
struct TextItemState {
    output_index: Option<u32>,
    item_id: String,
    text: String,
    added: bool,
    done: bool,
}

#[derive(Debug, Default)]
struct ReasoningItemState {
    output_index: Option<u32>,
    item_id: String,
    text: String,
    added: bool,
    done: bool,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
enum InlineThinkMode {
    #[default]
    Detecting,
    Reasoning,
    Text,
}

#[derive(Debug, Default)]
struct InlineThinkState {
    mode: InlineThinkMode,
    buffer: String,
}

#[derive(Debug, Default)]
struct ToolCallState {
    output_index: Option<u32>,
    item_id: String,
    call_id: String,
    name: String,
    arguments: String,
    added: bool,
    done: bool,
}

#[derive(Debug)]
struct ChatSseState {
    response_started: bool,
    completed: bool,
    response_id: String,
    model: String,
    created_at: u64,
    next_output_index: u32,
    text: TextItemState,
    reasoning: ReasoningItemState,
    inline_think: InlineThinkState,
    tools: BTreeMap<usize, ToolCallState>,
    output_items: Vec<(u32, Value)>,
    latest_usage: Option<Value>,
    finish_reason: Option<String>,
    tool_context: CodexToolContext,
    original_request: Option<Value>,
}

impl Default for ChatSseState {
    fn default() -> Self {
        Self {
            response_started: false,
            completed: false,
            response_id: "resp_compat".to_string(),
            model: String::new(),
            created_at: 0,
            next_output_index: 0,
            text: TextItemState::default(),
            reasoning: ReasoningItemState::default(),
            inline_think: InlineThinkState::default(),
            tools: BTreeMap::new(),
            output_items: Vec::new(),
            latest_usage: None,
            finish_reason: None,
            tool_context: CodexToolContext::default(),
            original_request: None,
        }
    }
}

impl ChatSseState {
    fn with_request(original_request: &Value) -> Self {
        Self {
            tool_context: build_codex_tool_context(original_request.get("tools")),
            original_request: Some(original_request.clone()),
            ..Self::default()
        }
    }

    fn handle_chat_chunk_into(&mut self, chunk: &Value, output: &mut String) {
        // Some gateways keep sending chunks (usage frames, keep-alives)
        // after `[DONE]`. Once the Responses stream is finalized we must
        // not append more events after `response.completed`/`data: [DONE]`.
        if self.completed {
            return;
        }
        if let Some(id) = chunk.get("id").and_then(Value::as_str) {
            self.response_id = response_id_from_chat_id(Some(id));
        }
        if let Some(model) = chunk.get("model").and_then(Value::as_str) {
            if !model.is_empty() {
                self.model = model.to_string();
            }
        }
        if let Some(created) = chunk.get("created").and_then(Value::as_u64) {
            self.created_at = created;
        }
        self.ensure_response_started_into(output);

        if let Some(usage) = chunk.get("usage").filter(|value| !value.is_null()) {
            self.latest_usage = Some(chat_usage_to_responses_usage(Some(usage)));
        }

        let Some(choice) = chunk
            .get("choices")
            .and_then(Value::as_array)
            .and_then(|choices| choices.first())
        else {
            return;
        };

        if let Some(delta) = choice.get("delta") {
            if let Some(reasoning) = chat_delta_reasoning_text(delta) {
                self.push_reasoning_delta_into(&reasoning, output);
            }

            if let Some(content) = delta.get("content").and_then(Value::as_str) {
                if !content.is_empty() {
                    self.push_content_delta_into(content, output);
                }
            }

            if let Some(tool_calls) = delta.get("tool_calls").and_then(Value::as_array) {
                self.flush_inline_think_at_boundary_into(output);
                self.finalize_reasoning_into(output);
                for tool_call in tool_calls {
                    self.push_tool_call_delta_into(tool_call, output);
                }
            }
        }

        if let Some(finish_reason) = choice.get("finish_reason").and_then(Value::as_str) {
            self.finish_reason = Some(finish_reason.to_string());
        }
    }

    fn push_content_delta_into(&mut self, delta: &str, output: &mut String) {
        match self.inline_think.mode {
            InlineThinkMode::Text => {
                self.finalize_reasoning_into(output);
                self.push_text_delta_into(delta, output);
            }
            InlineThinkMode::Detecting => {
                self.inline_think.buffer.push_str(delta);
                match leading_think_prefix_decision(&self.inline_think.buffer) {
                    ThinkPrefixDecision::NeedMore => {}
                    ThinkPrefixDecision::Reasoning => {
                        self.inline_think.mode = InlineThinkMode::Reasoning;
                        self.drain_complete_inline_think_into(output);
                    }
                    ThinkPrefixDecision::Text => {
                        self.inline_think.mode = InlineThinkMode::Text;
                        let text = std::mem::take(&mut self.inline_think.buffer);
                        self.finalize_reasoning_into(output);
                        self.push_text_delta_into(&text, output);
                    }
                }
            }
            InlineThinkMode::Reasoning => {
                self.inline_think.buffer.push_str(delta);
                self.drain_complete_inline_think_into(output);
            }
        }
    }

    fn drain_complete_inline_think_into(&mut self, output: &mut String) {
        let Some((reasoning, answer)) = split_leading_think_block(&self.inline_think.buffer) else {
            return;
        };
        self.inline_think.mode = InlineThinkMode::Text;
        self.inline_think.buffer.clear();
        if !reasoning.is_empty() {
            self.push_reasoning_delta_into(&reasoning, output);
            self.finalize_reasoning_into(output);
        }
        if !answer.is_empty() {
            self.push_text_delta_into(&answer, output);
        }
    }

    fn flush_inline_think_at_boundary_into(&mut self, output: &mut String) {
        match self.inline_think.mode {
            InlineThinkMode::Text => {}
            InlineThinkMode::Detecting => {
                self.inline_think.mode = InlineThinkMode::Text;
                let text = std::mem::take(&mut self.inline_think.buffer);
                if !text.is_empty() {
                    self.finalize_reasoning_into(output);
                    self.push_text_delta_into(&text, output);
                }
            }
            InlineThinkMode::Reasoning => {
                let buffered = std::mem::take(&mut self.inline_think.buffer);
                self.inline_think.mode = InlineThinkMode::Text;
                if let Some((reasoning, answer)) = split_leading_think_block(&buffered) {
                    if !reasoning.is_empty() {
                        self.push_reasoning_delta_into(&reasoning, output);
                        self.finalize_reasoning_into(output);
                    }
                    if !answer.is_empty() {
                        self.push_text_delta_into(&answer, output);
                    }
                    return;
                }
                let reasoning = strip_leading_think_open_tag(&buffered).unwrap_or(buffered);
                if !reasoning.is_empty() {
                    self.push_reasoning_delta_into(&reasoning, output);
                    self.finalize_reasoning_into(output);
                }
            }
        }
    }

    fn ensure_response_started_into(&mut self, output: &mut String) {
        if self.response_started {
            return;
        }
        self.response_started = true;
        push_sse(
            output,
            "response.created",
            json!({
                "type": "response.created",
                "response": self.base_response("in_progress", Vec::new())
            }),
        );
        push_sse(
            output,
            "response.in_progress",
            json!({
                "type": "response.in_progress",
                "response": self.base_response("in_progress", Vec::new())
            }),
        );
    }

    fn push_reasoning_delta_into(&mut self, delta: &str, output: &mut String) {
        if !self.reasoning.added {
            let output_index = self.next_output_index();
            let item_id = format!("rs_{}", self.response_id);
            self.reasoning.output_index = Some(output_index);
            self.reasoning.item_id = item_id.clone();
            self.reasoning.added = true;

            push_sse(
                output,
                "response.output_item.added",
                json!({
                    "type": "response.output_item.added",
                    "output_index": output_index,
                    "item": {
                        "id": item_id,
                        "type": "reasoning",
                        "status": "in_progress",
                        "reasoning_content": "",
                        "summary": []
                    }
                }),
            );
            push_sse(
                output,
                "response.reasoning_summary_part.added",
                json!({
                    "type": "response.reasoning_summary_part.added",
                    "item_id": self.reasoning.item_id,
                    "output_index": output_index,
                    "summary_index": 0,
                    "part": { "type": "summary_text", "text": "" }
                }),
            );
        }

        self.reasoning.text.push_str(delta);
        let output_index = self.reasoning.output_index.unwrap_or(0);
        push_sse(
            output,
            "response.reasoning_summary_text.delta",
            json!({
                "type": "response.reasoning_summary_text.delta",
                "item_id": self.reasoning.item_id,
                "output_index": output_index,
                "summary_index": 0,
                "delta": delta
            }),
        );
    }

    fn push_text_delta_into(&mut self, delta: &str, output: &mut String) {
        if !self.text.added {
            let output_index = self.next_output_index();
            let item_id = format!("{}_msg", self.response_id);
            self.text.output_index = Some(output_index);
            self.text.item_id = item_id.clone();
            self.text.added = true;
            push_sse(
                output,
                "response.output_item.added",
                json!({
                    "type": "response.output_item.added",
                    "output_index": output_index,
                    "item": {
                        "id": item_id,
                        "type": "message",
                        "status": "in_progress",
                        "role": "assistant",
                        "content": []
                    }
                }),
            );
            push_sse(
                output,
                "response.content_part.added",
                json!({
                    "type": "response.content_part.added",
                    "item_id": self.text.item_id,
                    "output_index": output_index,
                    "content_index": 0,
                    "part": { "type": "output_text", "text": "", "annotations": [] }
                }),
            );
        }

        self.text.text.push_str(delta);
        let output_index = self.text.output_index.unwrap_or(0);
        push_sse(
            output,
            "response.output_text.delta",
            json!({
                "type": "response.output_text.delta",
                "item_id": self.text.item_id,
                "output_index": output_index,
                "content_index": 0,
                "delta": delta
            }),
        );
    }

    fn push_tool_call_delta_into(&mut self, tool_call: &Value, output: &mut String) {
        let chat_index = tool_call.get("index").and_then(Value::as_u64).unwrap_or(0) as usize;
        let id_delta = tool_call
            .get("id")
            .and_then(Value::as_str)
            .map(str::to_string);
        let function = tool_call.get("function").unwrap_or(&Value::Null);
        let name_delta = function
            .get("name")
            .and_then(Value::as_str)
            .map(str::to_string);
        let args_delta = function
            .get("arguments")
            .and_then(Value::as_str)
            .unwrap_or("")
            .to_string();

        let mut should_add = false;
        let mut output_index = None;
        let mut item_id = String::new();
        let mut pending_arguments = String::new();

        {
            let state = self.tools.entry(chat_index).or_default();
            if let Some(id) = id_delta {
                state.call_id = id;
            }
            if let Some(name) = name_delta {
                if !name.is_empty() {
                    state.name = name;
                }
            }
            if !args_delta.is_empty() {
                state.arguments.push_str(&args_delta);
            }

            if !state.added && (!state.call_id.is_empty() || !state.name.is_empty()) {
                should_add = true;
                pending_arguments = state.arguments.clone();
            } else if state.added {
                output_index = state.output_index;
                item_id = state.item_id.clone();
            }
        }

        if should_add {
            let assigned = self.next_output_index();
            let state = self.tools.get_mut(&chat_index).expect("tool state exists");
            state.added = true;
            if state.call_id.is_empty() {
                state.call_id = format!("call_{chat_index}");
            }
            if state.name.is_empty() {
                state.name = "unknown_tool".to_string();
            }
            state.output_index = Some(assigned);
            state.item_id = format!("fc_{}", state.call_id);
            let added_item = tool_call_added_item(state, assigned, &self.tool_context);
            push_sse(output, "response.output_item.added", added_item);
            if !pending_arguments.is_empty() {
                push_tool_call_delta_sse(
                    output,
                    state,
                    assigned,
                    &pending_arguments,
                    &self.tool_context,
                );
            }
        } else if !args_delta.is_empty() {
            if let Some(output_index) = output_index {
                let state = ToolCallState {
                    output_index: Some(output_index),
                    item_id,
                    name: self
                        .tools
                        .get(&chat_index)
                        .map(|state| state.name.clone())
                        .unwrap_or_default(),
                    call_id: self
                        .tools
                        .get(&chat_index)
                        .map(|state| state.call_id.clone())
                        .unwrap_or_default(),
                    ..ToolCallState::default()
                };
                push_tool_call_delta_sse(
                    output,
                    &state,
                    output_index,
                    &args_delta,
                    &self.tool_context,
                );
            }
        }
    }

    fn finalize_into(&mut self, output: &mut String) {
        if self.completed {
            return;
        }
        self.ensure_response_started_into(output);
        self.flush_inline_think_at_boundary_into(output);
        self.finalize_reasoning_into(output);
        self.finalize_text_into(output);
        self.finalize_tools_into(output);

        let status = response_status(self.finish_reason.as_deref());
        let mut response = self.base_response(status, self.completed_output_items());
        if let Some(reason) = incomplete_details_reason(self.finish_reason.as_deref()) {
            response["incomplete_details"] = json!({ "reason": reason });
        }
        copy_response_request_fields(&mut response, self.original_request.as_ref());
        push_sse(
            output,
            "response.completed",
            json!({
                "type": "response.completed",
                "response": response
            }),
        );
        output.push_str("data: [DONE]\n\n");
        self.completed = true;
    }

    fn finalize_reasoning_into(&mut self, output: &mut String) {
        if !self.reasoning.added || self.reasoning.done {
            return;
        }
        let output_index = self.reasoning.output_index.unwrap_or(0);
        let item = json!({
            "id": self.reasoning.item_id,
            "type": "reasoning",
            "reasoning_content": self.reasoning.text,
            "summary": [{ "type": "summary_text", "text": self.reasoning.text }]
        });
        self.output_items.push((output_index, item.clone()));
        self.reasoning.done = true;
        push_sse(
            output,
            "response.reasoning_summary_text.done",
            json!({
                "type": "response.reasoning_summary_text.done",
                "item_id": self.reasoning.item_id,
                "output_index": output_index,
                "summary_index": 0,
                "text": self.reasoning.text
            }),
        );
        push_sse(
            output,
            "response.reasoning_summary_part.done",
            json!({
                "type": "response.reasoning_summary_part.done",
                "item_id": self.reasoning.item_id,
                "output_index": output_index,
                "summary_index": 0,
                "part": { "type": "summary_text", "text": self.reasoning.text }
            }),
        );
        push_sse(
            output,
            "response.output_item.done",
            json!({
                "type": "response.output_item.done",
                "output_index": output_index,
                "item": item
            }),
        );
    }

    fn finalize_text_into(&mut self, output: &mut String) {
        if !self.text.added || self.text.done {
            return;
        }
        let output_index = self.text.output_index.unwrap_or(0);
        let item = json!({
            "id": self.text.item_id,
            "type": "message",
            "status": "completed",
            "role": "assistant",
            "content": [{ "type": "output_text", "text": self.text.text, "annotations": [] }]
        });
        self.output_items.push((output_index, item.clone()));
        self.text.done = true;
        push_sse(
            output,
            "response.output_text.done",
            json!({
                "type": "response.output_text.done",
                "item_id": self.text.item_id,
                "output_index": output_index,
                "content_index": 0,
                "text": self.text.text
            }),
        );
        push_sse(
            output,
            "response.content_part.done",
            json!({
                "type": "response.content_part.done",
                "item_id": self.text.item_id,
                "output_index": output_index,
                "content_index": 0,
                "part": { "type": "output_text", "text": self.text.text, "annotations": [] }
            }),
        );
        push_sse(
            output,
            "response.output_item.done",
            json!({
                "type": "response.output_item.done",
                "output_index": output_index,
                "item": item
            }),
        );
    }

    fn finalize_tools_into(&mut self, output: &mut String) {
        let keys: Vec<usize> = self.tools.keys().copied().collect();
        for key in keys {
            if self.tools.get(&key).map(|state| state.done).unwrap_or(true) {
                continue;
            }
            if self
                .tools
                .get(&key)
                .map(|state| !state.added && !state.done)
                .unwrap_or(false)
            {
                let assigned = self.next_output_index();
                let state = self.tools.get_mut(&key).expect("tool state exists");
                state.added = true;
                if state.call_id.is_empty() {
                    state.call_id = format!("call_{key}");
                }
                if state.name.is_empty() {
                    state.name = "unknown_tool".to_string();
                }
                state.output_index = Some(assigned);
                state.item_id = format!("fc_{}", state.call_id);
                let added_item = tool_call_added_item(state, assigned, &self.tool_context);
                push_sse(output, "response.output_item.added", added_item);
            }

            let state = self.tools.get_mut(&key).expect("tool state exists");
            let output_index = state.output_index.unwrap_or(0);
            let item = tool_call_done_item(state, &self.tool_context);
            state.done = true;
            self.output_items.push((output_index, item.clone()));
            push_tool_call_done_sse(output, state, output_index, &self.tool_context);
            push_sse(
                output,
                "response.output_item.done",
                json!({
                    "type": "response.output_item.done",
                    "output_index": output_index,
                    "item": item
                }),
            );
        }
    }

    fn failed_into(&mut self, output: &mut String, message: String, error_type: Option<String>) {
        self.completed = true;
        let mut error = json!({ "message": message });
        if let Some(error_type) = error_type.filter(|value| !value.is_empty()) {
            error["type"] = json!(error_type);
        }
        let mut response = self.base_response("failed", self.completed_output_items());
        response["error"] = error;
        push_sse(
            output,
            "response.failed",
            json!({
                "type": "response.failed",
                "response": response
            }),
        );
    }

    fn completed_output_items(&self) -> Vec<Value> {
        let mut output_items = self.output_items.clone();
        output_items.sort_by_key(|(output_index, _)| *output_index);
        output_items.into_iter().map(|(_, item)| item).collect()
    }

    fn base_response(&self, status: &str, output: Vec<Value>) -> Value {
        json!({
            "id": self.response_id,
            "object": "response",
            "created_at": self.created_at,
            "status": status,
            "model": self.model,
            "output": output,
            "usage": self.latest_usage.clone().unwrap_or_else(default_responses_usage)
        })
    }

    fn next_output_index(&mut self) -> u32 {
        let index = self.next_output_index;
        self.next_output_index += 1;
        index
    }
}

fn take_sse_block(buffer: &mut String) -> Option<String> {
    let lf = buffer.find("\n\n").map(|index| (index, 2));
    let crlf = buffer.find("\r\n\r\n").map(|index| (index, 4));
    let (index, delimiter_len) = match (lf, crlf) {
        (Some(left), Some(right)) => {
            if left.0 <= right.0 {
                left
            } else {
                right
            }
        }
        (Some(value), None) | (None, Some(value)) => value,
        (None, None) => return None,
    };
    let block = buffer[..index].to_string();
    buffer.drain(..index + delimiter_len);
    Some(block)
}

fn append_utf8_safe(buffer: &mut String, remainder: &mut Vec<u8>, bytes: &[u8]) {
    if bytes.is_empty() {
        return;
    }
    let mut combined = Vec::new();
    if !remainder.is_empty() {
        combined.extend_from_slice(remainder);
        remainder.clear();
    }
    combined.extend_from_slice(bytes);

    match std::str::from_utf8(&combined) {
        Ok(text) => buffer.push_str(text),
        Err(error) => {
            let valid = error.valid_up_to();
            if valid > 0 {
                buffer.push_str(std::str::from_utf8(&combined[..valid]).unwrap_or_default());
            }
            if error.error_len().is_none() {
                remainder.extend_from_slice(&combined[valid..]);
            } else {
                buffer.push_str(&String::from_utf8_lossy(&combined[valid..]));
            }
        }
    }
}

fn strip_sse_field<'a>(line: &'a str, field: &str) -> Option<&'a str> {
    let rest = line.strip_prefix(field)?.strip_prefix(':')?;
    Some(rest.strip_prefix(' ').unwrap_or(rest))
}

fn chat_delta_reasoning_text(delta: &Value) -> Option<String> {
    extract_reasoning_field_text(delta)
}

enum ThinkPrefixDecision {
    NeedMore,
    Reasoning,
    Text,
}

fn leading_think_prefix_decision(buffer: &str) -> ThinkPrefixDecision {
    let trimmed = buffer.trim_start();
    if trimmed.is_empty() {
        return ThinkPrefixDecision::NeedMore;
    }
    if trimmed.starts_with(THINK_OPEN_TAG) {
        return ThinkPrefixDecision::Reasoning;
    }
    if THINK_OPEN_TAG.starts_with(trimmed) {
        return ThinkPrefixDecision::NeedMore;
    }
    ThinkPrefixDecision::Text
}

fn extract_chat_sse_error(value: &Value) -> (String, Option<String>) {
    let error = value.get("error").unwrap_or(value);
    let message = error
        .as_str()
        .map(ToString::to_string)
        .or_else(|| {
            error
                .get("message")
                .or_else(|| error.get("detail"))
                .and_then(Value::as_str)
                .map(ToString::to_string)
        })
        .unwrap_or_else(|| error.to_string());
    let error_type = error
        .get("type")
        .or_else(|| error.get("code"))
        .and_then(Value::as_str)
        .map(ToString::to_string);
    (message, error_type)
}

fn http_status_line(status: u16) -> String {
    match status {
        200 => "200 OK".to_string(),
        400 => "400 Bad Request".to_string(),
        401 => "401 Unauthorized".to_string(),
        403 => "403 Forbidden".to_string(),
        404 => "404 Not Found".to_string(),
        429 => "429 Too Many Requests".to_string(),
        500 => "500 Internal Server Error".to_string(),
        502 => "502 Bad Gateway".to_string(),
        503 => "503 Service Unavailable".to_string(),
        _ => format!("{status} Upstream"),
    }
}

pub fn responses_error_from_upstream(status_code: u16, content_type: &str, body: &[u8]) -> Value {
    let (message, error_type, code, param) = upstream_error_parts(status_code, content_type, body);
    let mut error = json!({
        "message": message,
        "type": error_type.unwrap_or_else(|| "upstream_error".to_string()),
    });
    if let Some(code) = code {
        error["code"] = json!(code);
    }
    if let Some(param) = param {
        error["param"] = json!(param);
    }
    json!({ "error": error })
}

fn upstream_error_parts(
    status_code: u16,
    content_type: &str,
    body: &[u8],
) -> (String, Option<String>, Option<String>, Option<String>) {
    if content_type.to_ascii_lowercase().contains("json") {
        if let Ok(value) = serde_json::from_slice::<Value>(body) {
            let error = value.get("error").unwrap_or(&value);
            let message = error
                .get("message")
                .or_else(|| error.get("detail"))
                .or_else(|| error.get("error"))
                .and_then(Value::as_str)
                .map(str::trim)
                .filter(|value| !value.is_empty())
                .map(ToString::to_string)
                .unwrap_or_else(|| truncate_error_preview(&value.to_string()));
            let error_type = error
                .get("type")
                .or_else(|| error.get("error_type"))
                .and_then(Value::as_str)
                .map(ToString::to_string);
            let code = error.get("code").and_then(|value| {
                value
                    .as_str()
                    .map(ToString::to_string)
                    .or_else(|| value.as_i64().map(|number| number.to_string()))
            });
            let param = error
                .get("param")
                .and_then(Value::as_str)
                .map(ToString::to_string);
            return (message, error_type, code, param);
        }
    }

    let preview = truncate_error_preview(&String::from_utf8_lossy(body));
    let message = if preview.trim().is_empty() {
        format!("Upstream returned HTTP {status_code}")
    } else {
        preview
    };
    (message, None, Some(status_code.to_string()), None)
}

fn truncate_error_preview(input: &str) -> String {
    input.chars().take(ERROR_BODY_PREVIEW_LIMIT).collect()
}

fn append_responses_input(input: &Value, messages: &mut Vec<Value>) {
    match input {
        Value::String(text) => messages.push(json!({ "role": "user", "content": text })),
        Value::Array(items) => {
            let mut pending_tool_calls = Vec::new();
            let mut pending_reasoning = Vec::new();
            let mut seen_tool_call_ids = BTreeSet::new();
            for item in items {
                append_responses_item(
                    item,
                    messages,
                    &mut pending_tool_calls,
                    &mut pending_reasoning,
                    &mut seen_tool_call_ids,
                );
            }
            flush_tool_calls(messages, &mut pending_tool_calls, &mut pending_reasoning);
            flush_reasoning(messages, &mut pending_reasoning);
        }
        Value::Object(_) => {
            let mut pending_tool_calls = Vec::new();
            let mut pending_reasoning = Vec::new();
            let mut seen_tool_call_ids = BTreeSet::new();
            append_responses_item(
                input,
                messages,
                &mut pending_tool_calls,
                &mut pending_reasoning,
                &mut seen_tool_call_ids,
            );
            flush_tool_calls(messages, &mut pending_tool_calls, &mut pending_reasoning);
            flush_reasoning(messages, &mut pending_reasoning);
        }
        _ => {}
    }
}

fn append_responses_item(
    item: &Value,
    messages: &mut Vec<Value>,
    pending_tool_calls: &mut Vec<Value>,
    pending_reasoning: &mut Vec<String>,
    seen_tool_call_ids: &mut BTreeSet<String>,
) {
    match item.get("type").and_then(Value::as_str) {
        Some("function_call") => {
            let name = responses_history_function_name(item);
            if name.is_empty() {
                return;
            }
            let call_id = item
                .get("call_id")
                .or_else(|| item.get("id"))
                .and_then(Value::as_str)
                .unwrap_or("");
            if call_id.is_empty() {
                return;
            }
            seen_tool_call_ids.insert(call_id.to_string());
            pending_tool_calls.push(json!({
                "id": call_id,
                "type": "function",
                "function": {
                    "name": name,
                    "arguments": responses_arguments_to_chat(item.get("arguments").unwrap_or(&json!({})))
                }
            }));
        }
        Some("function_call_output") => {
            let call_id = item.get("call_id").and_then(Value::as_str).unwrap_or("");
            if call_id.is_empty() {
                return;
            }
            if !seen_tool_call_ids.contains(call_id) {
                flush_tool_calls(messages, pending_tool_calls, pending_reasoning);
                flush_reasoning(messages, pending_reasoning);
                messages.push(orphan_tool_output_message(
                    call_id,
                    item.get("output").unwrap_or(&Value::Null),
                ));
                return;
            }
            flush_tool_calls(messages, pending_tool_calls, pending_reasoning);
            messages.push(json!({
                "role": "tool",
                "tool_call_id": call_id,
                "content": response_output_text(item.get("output").unwrap_or(&Value::Null))
            }));
        }
        Some("custom_tool_call") => {
            let name = item.get("name").and_then(Value::as_str).unwrap_or("");
            let input = item
                .get("input")
                .or_else(|| item.get("arguments"))
                .unwrap_or(&Value::Null);
            let (name, arguments) = build_custom_tool_call_history(name, input);
            let call_id = item
                .get("call_id")
                .or_else(|| item.get("id"))
                .and_then(Value::as_str)
                .unwrap_or("");
            if call_id.is_empty() {
                return;
            }
            seen_tool_call_ids.insert(call_id.to_string());
            pending_tool_calls.push(json!({
                "id": call_id,
                "type": "function",
                "function": {
                    "name": name,
                    "arguments": arguments
                }
            }));
        }
        Some("custom_tool_call_output") => {
            let call_id = item.get("call_id").and_then(Value::as_str).unwrap_or("");
            if call_id.is_empty() {
                return;
            }
            if !seen_tool_call_ids.contains(call_id) {
                flush_tool_calls(messages, pending_tool_calls, pending_reasoning);
                flush_reasoning(messages, pending_reasoning);
                messages.push(orphan_tool_output_message(
                    call_id,
                    item.get("output").unwrap_or(&Value::Null),
                ));
                return;
            }
            flush_tool_calls(messages, pending_tool_calls, pending_reasoning);
            messages.push(json!({
                "role": "tool",
                "tool_call_id": call_id,
                "content": response_output_text(item.get("output").unwrap_or(&Value::Null))
            }));
        }
        Some("tool_call") => {
            if let Some(tool_use) = item.get("tool_use") {
                let call_id = tool_use
                    .get("id")
                    .or_else(|| item.get("call_id"))
                    .or_else(|| item.get("id"))
                    .and_then(Value::as_str)
                    .unwrap_or("");
                if call_id.is_empty() {
                    return;
                }
                seen_tool_call_ids.insert(call_id.to_string());
                pending_tool_calls.push(json!({
                    "id": call_id,
                    "type": "function",
                    "function": {
                        "name": tool_use.get("name").and_then(Value::as_str).unwrap_or(""),
                        "arguments": responses_arguments_to_chat(tool_use.get("input").unwrap_or(&json!({})))
                    }
                }));
            }
        }
        Some("tool_result") => {
            flush_tool_calls(messages, pending_tool_calls, pending_reasoning);
            let content = item.get("content").unwrap_or(&Value::Null);
            let call_id = content
                .get("tool_use_id")
                .or_else(|| item.get("tool_call_id"))
                .or_else(|| item.get("call_id"))
                .and_then(Value::as_str)
                .unwrap_or("");
            if call_id.is_empty() {
                return;
            }
            let output = content.get("content").unwrap_or(content);
            if !seen_tool_call_ids.contains(call_id) {
                flush_reasoning(messages, pending_reasoning);
                messages.push(orphan_tool_output_message(call_id, output));
                return;
            }
            messages.push(json!({
                "role": "tool",
                "tool_call_id": call_id,
                "content": response_output_text(output)
            }));
        }
        Some("reasoning") => {
            if let Some(text) = responses_reasoning_text(item) {
                if !text.is_empty() {
                    pending_reasoning.push(text);
                }
            }
        }
        _ => {
            flush_tool_calls(messages, pending_tool_calls, pending_reasoning);
            if item.get("role").is_some() || item.get("content").is_some() {
                let role = responses_role_to_chat_role(item.get("role").and_then(Value::as_str));
                let mut message = json!({
                    "role": role,
                    "content": responses_content_to_chat_content(
                        role,
                        item.get("content").unwrap_or(&Value::Null)
                        )
                });
                if role == "assistant" {
                    if !pending_reasoning.is_empty() && pending_tool_calls.is_empty() {
                        message["reasoning_content"] =
                            json!(std::mem::take(pending_reasoning).join("\n"));
                    }
                } else if !pending_reasoning.is_empty() {
                    flush_tool_calls(messages, pending_tool_calls, pending_reasoning);
                    flush_reasoning(messages, pending_reasoning);
                }
                messages.push(message);
            }
        }
    }
}

fn orphan_tool_output_message(call_id: &str, output: &Value) -> Value {
    json!({
        "role": "user",
        "content": format!(
            "Function call output ({call_id}): {}",
            response_output_text(output)
        )
    })
}

fn normalize_chat_messages(messages: &mut [Value]) {
    for message in messages {
        if message.get("role").and_then(Value::as_str) != Some("assistant") {
            continue;
        }
        let has_content = match message.get("content") {
            Some(Value::Null) | None => false,
            Some(Value::String(_)) => true,
            Some(Value::Array(parts)) => !parts.is_empty(),
            Some(_) => true,
        };
        let has_tool_calls = message
            .get("tool_calls")
            .and_then(Value::as_array)
            .is_some_and(|tool_calls| !tool_calls.is_empty());
        if !has_content && !has_tool_calls {
            message["content"] = json!("");
        }
    }
}

fn collapse_system_messages_to_head(messages: Vec<Value>) -> Vec<Value> {
    let mut system_chunks = Vec::new();
    let mut rest = Vec::with_capacity(messages.len());

    for message in messages {
        if message.get("role").and_then(Value::as_str) == Some("system") {
            if let Some(text) = message.get("content").and_then(Value::as_str) {
                if !text.trim().is_empty() {
                    system_chunks.push(text.to_string());
                }
                continue;
            }
        }
        rest.push(message);
    }

    let mut output = Vec::with_capacity(rest.len() + usize::from(!system_chunks.is_empty()));
    if !system_chunks.is_empty() {
        output.push(json!({
            "role": "system",
            "content": system_chunks.join("\n\n")
        }));
    }
    output.extend(rest);
    output
}

fn responses_role_to_chat_role(role: Option<&str>) -> &'static str {
    match role {
        Some("developer") | Some("system") => "system",
        Some("assistant") => "assistant",
        Some("tool") => "tool",
        Some("latest_reminder") => "user",
        Some("user") | None => "user",
        Some(_) => "user",
    }
}

fn flush_tool_calls(
    messages: &mut Vec<Value>,
    pending_tool_calls: &mut Vec<Value>,
    pending_reasoning: &mut Vec<String>,
) {
    if pending_tool_calls.is_empty() {
        return;
    }

    if let Some(last) = messages.last_mut() {
        if last.get("role").and_then(Value::as_str) == Some("assistant") {
            merge_tool_calls_into_message(last, std::mem::take(pending_tool_calls));
            if !pending_reasoning.is_empty() {
                let reasoning = std::mem::take(pending_reasoning).join("\n");
                append_reasoning_to_assistant_message(last, &reasoning);
            }
            return;
        }
    }

    let mut message = json!({
        "role": "assistant",
        "content": "",
        "tool_calls": std::mem::take(pending_tool_calls)
    });
    if !pending_reasoning.is_empty() {
        message["reasoning_content"] = json!(std::mem::take(pending_reasoning).join("\n"));
    }
    messages.push(message);
}

fn flush_reasoning(messages: &mut Vec<Value>, pending_reasoning: &mut Vec<String>) {
    if pending_reasoning.is_empty() {
        return;
    }
    let reasoning = std::mem::take(pending_reasoning).join("\n");
    if let Some(last) = messages.last_mut() {
        if last.get("role").and_then(Value::as_str) == Some("assistant") {
            append_reasoning_to_assistant_message(last, &reasoning);
            return;
        }
    }
    messages.push(json!({
        "role": "assistant",
        "content": "",
        "reasoning_content": reasoning
    }));
}

fn append_reasoning_to_assistant_message(message: &mut Value, reasoning: &str) {
    if reasoning.is_empty() {
        return;
    }
    let existing = message
        .get("reasoning_content")
        .and_then(Value::as_str)
        .unwrap_or("");
    message["reasoning_content"] = if existing.is_empty() {
        json!(reasoning)
    } else {
        json!(format!("{existing}\n{reasoning}"))
    };
    if message.get("content").is_none() || message.get("content") == Some(&Value::Null) {
        message["content"] = json!("");
    }
}

fn merge_tool_calls_into_message(message: &mut Value, incoming: Vec<Value>) {
    let Some(object) = message.as_object_mut() else {
        return;
    };
    let existing = object
        .entry("tool_calls".to_string())
        .or_insert_with(|| json!([]));
    let Some(existing_array) = existing.as_array_mut() else {
        *existing = json!(incoming);
        return;
    };
    for tool_call in incoming {
        let id = tool_call.get("id").and_then(Value::as_str).unwrap_or("");
        if !id.is_empty()
            && existing_array
                .iter()
                .any(|item| item.get("id").and_then(Value::as_str) == Some(id))
        {
            continue;
        }
        existing_array.push(tool_call);
    }
    if message.get("content").is_none() || message.get("content") == Some(&Value::Null) {
        message["content"] = json!("");
    }
}

fn responses_reasoning_text(item: &Value) -> Option<String> {
    extract_reasoning_summary_text(item).or_else(|| extract_reasoning_field_text(item))
}

fn responses_content_to_chat_content(_role: &str, content: &Value) -> Value {
    if content.is_null() || content.is_string() {
        return content.clone();
    }

    let Some(parts) = content.as_array() else {
        return content.clone();
    };
    let mut chat_parts = Vec::new();
    let mut has_non_text_part = false;

    for part in parts {
        match part.get("type").and_then(Value::as_str).unwrap_or("") {
            "input_text" | "output_text" | "text" => {
                if let Some(value) = part.get("text").and_then(Value::as_str) {
                    if !value.is_empty() {
                        chat_parts.push(json!({ "type": "text", "text": value }));
                    }
                }
            }
            "refusal" => {
                if let Some(value) = part.get("refusal").and_then(Value::as_str) {
                    if !value.is_empty() {
                        chat_parts.push(json!({ "type": "text", "text": value }));
                    }
                }
            }
            "input_image" => {
                if let Some(image_url) = part.get("image_url") {
                    let image_url = if image_url.is_object() {
                        image_url.clone()
                    } else {
                        json!({ "url": image_url.as_str().unwrap_or_default() })
                    };
                    chat_parts.push(json!({ "type": "image_url", "image_url": image_url }));
                    has_non_text_part = true;
                }
            }
            "input_audio" => {
                let desc = part
                    .get("transcript")
                    .and_then(Value::as_str)
                    .unwrap_or("[audio content]");
                chat_parts.push(json!({ "type": "text", "text": format!("[Audio: {desc}]") }));
            }
            "input_file" => {
                let name = part
                    .get("filename")
                    .or_else(|| part.get("name"))
                    .and_then(Value::as_str)
                    .unwrap_or("file");
                if let Some(text) = part
                    .get("text")
                    .and_then(Value::as_str)
                    .filter(|t| !t.is_empty())
                {
                    chat_parts
                        .push(json!({ "type": "text", "text": format!("[File: {name}]\n{text}") }));
                } else {
                    chat_parts.push(json!({ "type": "text", "text": format!("[File: {name}]") }));
                }
            }
            other => {
                if let Some(text) = part
                    .get("text")
                    .and_then(Value::as_str)
                    .filter(|t| !t.is_empty())
                {
                    chat_parts.push(json!({ "type": "text", "text": text }));
                } else if !other.is_empty() {
                    let fallback = part
                        .get("content")
                        .or_else(|| part.get("value"))
                        .and_then(Value::as_str)
                        .unwrap_or("");
                    if !fallback.is_empty() {
                        chat_parts.push(json!({ "type": "text", "text": fallback }));
                    }
                }
            }
        }
    }

    if !has_non_text_part {
        return Value::String(
            chat_parts
                .iter()
                .filter_map(|part| part.get("text").and_then(Value::as_str))
                .collect::<Vec<_>>()
                .join("\n"),
        );
    }

    Value::Array(chat_parts)
}

fn responses_history_function_name(item: &Value) -> String {
    let name = item.get("name").and_then(Value::as_str).unwrap_or("");
    let namespace = item.get("namespace").and_then(Value::as_str).unwrap_or("");
    if name.is_empty() {
        String::new()
    } else if namespace.is_empty() {
        name.to_string()
    } else {
        flatten_namespace_tool_name(namespace, name)
    }
}

fn build_codex_tool_context(tools: Option<&Value>) -> CodexToolContext {
    let mut context = CodexToolContext::default();
    let Some(tools) = tools.and_then(Value::as_array) else {
        return context;
    };

    for tool in tools {
        if let Some(name) = tool.as_str().filter(|name| !name.is_empty()) {
            if let Some(action) = proxy_action_from_upstream_name(name) {
                context.custom_tools.insert(
                    name.to_string(),
                    CodexCustomToolSpec {
                        openai_name: "apply_patch".to_string(),
                        kind: CodexCustomToolKind::ApplyPatch,
                        proxy_action: Some(action),
                    },
                );
                context.has_custom_tools = true;
                continue;
            }
            context.custom_tools.insert(
                name.to_string(),
                CodexCustomToolSpec {
                    openai_name: name.to_string(),
                    kind: CodexCustomToolKind::Raw,
                    proxy_action: None,
                },
            );
            context.has_custom_tools = true;
            continue;
        }
        let tool_type = tool.get("type").and_then(Value::as_str).unwrap_or("");
        match tool_type {
            "custom" => {
                let Some(name) = tool
                    .get("name")
                    .and_then(Value::as_str)
                    .filter(|v| !v.is_empty())
                else {
                    continue;
                };
                let kind = detect_codex_custom_tool_kind(tool, name);
                context.custom_tools.insert(
                    name.to_string(),
                    CodexCustomToolSpec {
                        openai_name: name.to_string(),
                        kind,
                        proxy_action: None,
                    },
                );
                if kind == CodexCustomToolKind::ApplyPatch {
                    for action in [
                        CodexPatchProxyAction::AddFile,
                        CodexPatchProxyAction::DeleteFile,
                        CodexPatchProxyAction::UpdateFile,
                        CodexPatchProxyAction::ReplaceFile,
                        CodexPatchProxyAction::Batch,
                    ] {
                        let proxy_name = format!("{name}_{}", action.suffix());
                        context.custom_tools.insert(
                            proxy_name,
                            CodexCustomToolSpec {
                                openai_name: name.to_string(),
                                kind: CodexCustomToolKind::ApplyPatch,
                                proxy_action: Some(action),
                            },
                        );
                    }
                }
                context.has_custom_tools = true;
            }
            "function" => {
                if let Some(name) = tool
                    .get("name")
                    .and_then(Value::as_str)
                    .filter(|v| !v.is_empty())
                {
                    context.function_tools.insert(
                        name.to_string(),
                        CodexFunctionToolSpec {
                            name: name.to_string(),
                            namespace: String::new(),
                        },
                    );
                }
            }
            "namespace" => add_namespace_tools_to_context(&mut context, tool),
            "web_search" | "local_shell" | "computer_use" => {
                let name = tool
                    .get("name")
                    .and_then(Value::as_str)
                    .filter(|v| !v.is_empty())
                    .unwrap_or(tool_type);
                context.custom_tools.insert(
                    name.to_string(),
                    CodexCustomToolSpec {
                        openai_name: name.to_string(),
                        kind: CodexCustomToolKind::BuiltIn,
                        proxy_action: None,
                    },
                );
                context.has_custom_tools = true;
            }
            _ => {}
        }
    }

    context
}

fn add_namespace_tools_to_context(context: &mut CodexToolContext, namespace_tool: &Value) {
    let namespace = namespace_tool
        .get("name")
        .and_then(Value::as_str)
        .unwrap_or("");
    let Some(children) = namespace_tool.get("tools").and_then(Value::as_array) else {
        return;
    };
    for child in children {
        if child.get("type").and_then(Value::as_str) != Some("function") {
            continue;
        }
        let Some(name) = child
            .get("name")
            .and_then(Value::as_str)
            .filter(|v| !v.is_empty())
        else {
            continue;
        };
        let flat = flatten_namespace_tool_name(namespace, name);
        if namespace.is_empty() {
            context.function_tools.insert(
                flat,
                CodexFunctionToolSpec {
                    namespace: namespace.to_string(),
                    name: name.to_string(),
                },
            );
        } else if context
            .function_tools
            .get(&flat)
            .is_none_or(|spec| !spec.namespace.is_empty())
        {
            context.function_tools.insert(
                flat,
                CodexFunctionToolSpec {
                    namespace: namespace.to_string(),
                    name: name.to_string(),
                },
            );
            context.has_namespace_tools = true;
        }
    }
}

fn responses_tools_to_chat_tools(tools: &[Value], context: &CodexToolContext) -> Vec<Value> {
    let mut converted = Vec::new();
    for tool in tools {
        if let Some(name) = tool.as_str().filter(|name| !name.is_empty()) {
            converted.push(generic_custom_proxy_tool(name, ""));
            continue;
        }
        match tool.get("type").and_then(Value::as_str).unwrap_or("") {
            "function" => {
                if let Some(tool) = responses_function_tool_to_chat_tool(tool) {
                    converted.push(tool);
                }
            }
            "custom" | "web_search" | "local_shell" | "computer_use" => {
                let tool_type = tool.get("type").and_then(Value::as_str).unwrap_or("");
                let name = tool
                    .get("name")
                    .and_then(Value::as_str)
                    .filter(|v| !v.is_empty())
                    .unwrap_or(tool_type);
                let description = tool
                    .get("description")
                    .and_then(Value::as_str)
                    .unwrap_or("");
                if detect_codex_custom_tool_kind(tool, name) == CodexCustomToolKind::ApplyPatch {
                    converted.extend(apply_patch_proxy_tools(name, description));
                } else {
                    converted.push(generic_custom_proxy_tool(name, description));
                }
            }
            "namespace" => converted.extend(namespace_tool_to_chat_tools(tool, context)),
            _ => {}
        }
    }
    converted
}

fn detect_codex_custom_tool_kind(tool: &Value, name: &str) -> CodexCustomToolKind {
    if name == "apply_patch" {
        return CodexCustomToolKind::ApplyPatch;
    }
    if let Some(definition) = tool.pointer("/format/definition").and_then(Value::as_str) {
        if definition.contains("begin_patch")
            && definition.contains("end_patch")
            && definition.contains("add_hunk")
        {
            return CodexCustomToolKind::ApplyPatch;
        }
    }
    if matches!(
        tool.get("type").and_then(Value::as_str),
        Some("web_search" | "local_shell" | "computer_use")
    ) {
        CodexCustomToolKind::BuiltIn
    } else {
        CodexCustomToolKind::Raw
    }
}

fn responses_function_tool_to_chat_tool(tool: &Value) -> Option<Value> {
    if tool.get("type").and_then(Value::as_str) != Some("function") {
        return None;
    }
    if tool.get("function").is_some() {
        let mut chat_tool = tool.clone();
        if let Some(strict) = tool.get("strict").cloned() {
            if let Some(function) = chat_tool.get_mut("function").and_then(Value::as_object_mut) {
                function.entry("strict".to_string()).or_insert(strict);
            }
            if let Some(object) = chat_tool.as_object_mut() {
                object.remove("strict");
            }
        }
        if let Some(function) = chat_tool.get_mut("function").and_then(Value::as_object_mut) {
            let normalized =
                normalize_chat_tool_parameters(function.get("parameters").unwrap_or(&json!({})));
            function.insert("parameters".to_string(), normalized);
        }
        return Some(chat_tool);
    }
    let mut function = json!({
        "name": tool.get("name").and_then(Value::as_str).unwrap_or(""),
        "description": tool.get("description").cloned().unwrap_or(Value::Null),
        "parameters": normalize_chat_tool_parameters(tool.get("parameters").unwrap_or(&json!({})))
    });
    if let Some(strict) = tool.get("strict") {
        function["strict"] = strict.clone();
    }
    Some(json!({
        "type": "function",
        "function": function
    }))
}

fn namespace_tool_to_chat_tools(namespace_tool: &Value, context: &CodexToolContext) -> Vec<Value> {
    let namespace = namespace_tool
        .get("name")
        .and_then(Value::as_str)
        .unwrap_or("");
    let namespace_description = namespace_tool
        .get("description")
        .and_then(Value::as_str)
        .unwrap_or("");
    let Some(children) = namespace_tool.get("tools").and_then(Value::as_array) else {
        return Vec::new();
    };
    let mut converted = Vec::new();
    for child in children {
        if child.get("type").and_then(Value::as_str) != Some("function") {
            continue;
        }
        let Some(name) = child
            .get("name")
            .and_then(Value::as_str)
            .filter(|v| !v.is_empty())
        else {
            continue;
        };
        let flat = flatten_namespace_tool_name(namespace, name);
        if namespace != ""
            && context
                .function_tools
                .get(&flat)
                .is_some_and(|spec| spec.namespace.is_empty())
        {
            continue;
        }
        let description = combine_namespace_description(
            namespace_description,
            child
                .get("description")
                .and_then(Value::as_str)
                .unwrap_or(""),
        );
        let mut function = json!({
            "name": flat,
            "parameters": normalize_chat_tool_parameters(child.get("parameters").unwrap_or(&json!({})))
        });
        if !description.is_empty() {
            function["description"] = json!(description);
        }
        converted.push(json!({
            "type": "function",
            "function": function
        }));
    }
    converted
}

fn normalize_chat_tool_parameters(parameters: &Value) -> Value {
    let mut normalized = if parameters.is_object() {
        parameters.clone()
    } else {
        json!({})
    };
    if normalized.get("type").is_none() {
        normalized["type"] = json!("object");
    }
    if normalized.get("properties").is_none() {
        normalized["properties"] = json!({});
    }
    if normalized.get("required").is_none() {
        normalized["required"] = json!([]);
    }
    normalized
}

fn generic_custom_proxy_tool(name: &str, description: &str) -> Value {
    let description = if description.trim().is_empty() {
        format!("FREEFORM custom tool: {name}. Put only the tool input text here.")
    } else {
        format!(
            "{}\n\nThis is a FREEFORM tool. Do not wrap the input in JSON or markdown.",
            description.trim()
        )
    };
    json!({
        "type": "function",
        "function": {
            "name": name,
            "description": description,
            "parameters": {
                "type": "object",
                "additionalProperties": false,
                "properties": {
                    "input": {
                        "type": "string",
                        "description": "Raw freeform input for this custom tool."
                    }
                },
                "required": ["input"]
            }
        }
    })
}

fn apply_patch_proxy_tools(name: &str, description: &str) -> Vec<Value> {
    vec![
        function_tool(
            &format!("{name}_add_file"),
            &patch_proxy_description(
                description,
                "add_file",
                "Create one new file by providing a target path and full file content.",
            ),
            apply_patch_add_file_schema(),
        ),
        function_tool(
            &format!("{name}_delete_file"),
            &patch_proxy_description(
                description,
                "delete_file",
                "Delete one file by providing a target path.",
            ),
            apply_patch_delete_file_schema(),
        ),
        function_tool(
            &format!("{name}_update_file"),
            &patch_proxy_description(
                description,
                "update_file",
                "Edit one existing file with structured hunks.",
            ),
            apply_patch_update_file_schema(),
        ),
        function_tool(
            &format!("{name}_replace_file"),
            &patch_proxy_description(
                description,
                "replace_file",
                "Replace one existing file by providing a target path and full new file content.",
            ),
            apply_patch_replace_file_schema(),
        ),
        function_tool(
            &format!("{name}_batch"),
            &patch_proxy_description(
                description,
                "batch",
                "Edit files by providing structured JSON patch operations.",
            ),
            apply_patch_batch_schema(),
        ),
    ]
}

fn function_tool(name: &str, description: &str, parameters: Value) -> Value {
    json!({
        "type": "function",
        "function": {
            "name": name,
            "description": description,
            "parameters": parameters
        }
    })
}

fn patch_proxy_description(description: &str, action: &str, default_description: &str) -> String {
    if description.trim().is_empty() {
        default_description.to_string()
    } else {
        format!("{} (proxy action: {action})", description.trim())
    }
}

fn apply_patch_add_file_schema() -> Value {
    json!({
        "type": "object",
        "additionalProperties": false,
        "properties": {
            "path": { "type": "string", "description": "Target file path." },
            "content": { "type": "string", "description": "Full file content without patch '+' prefixes." }
        },
        "required": ["path", "content"]
    })
}

fn apply_patch_delete_file_schema() -> Value {
    json!({
        "type": "object",
        "additionalProperties": false,
        "properties": {
            "path": { "type": "string", "description": "Target file path." }
        },
        "required": ["path"]
    })
}

fn apply_patch_update_file_schema() -> Value {
    json!({
        "type": "object",
        "additionalProperties": false,
        "properties": {
            "path": { "type": "string", "description": "Target file path." },
            "move_to": { "type": "string", "description": "Optional destination path for move operations." },
            "hunks": apply_patch_hunks_schema()
        },
        "required": ["path", "hunks"]
    })
}

fn apply_patch_replace_file_schema() -> Value {
    json!({
        "type": "object",
        "additionalProperties": false,
        "properties": {
            "path": { "type": "string", "description": "Target file path." },
            "content": { "type": "string", "description": "Full replacement content." }
        },
        "required": ["path", "content"]
    })
}

fn apply_patch_batch_schema() -> Value {
    json!({
        "type": "object",
        "additionalProperties": false,
        "properties": {
            "operations": {
                "type": "array",
                "description": "Ordered list of file patch operations.",
                "items": {
                    "type": "object",
                    "additionalProperties": false,
                    "properties": {
                        "type": { "type": "string", "enum": ["add_file", "delete_file", "update_file", "replace_file"] },
                        "path": { "type": "string" },
                        "move_to": { "type": "string", "description": "Optional destination path for move operations (update_file only)." },
                        "content": { "type": "string", "description": "Full file content for add_file / replace_file." },
                        "hunks": apply_patch_hunks_schema()
                    },
                    "required": ["type", "path"]
                }
            }
        },
        "required": ["operations"]
    })
}

fn apply_patch_hunks_schema() -> Value {
    json!({
        "type": "array",
        "description": "Structured update hunks (required when type=update_file).",
        "items": {
            "type": "object",
            "additionalProperties": false,
            "properties": {
                "context": { "type": "string", "description": "Optional @@ context header text." },
                "lines": {
                    "type": "array",
                    "items": {
                        "type": "object",
                        "additionalProperties": false,
                        "properties": {
                            "op": { "type": "string", "enum": ["context", "add", "remove"] },
                            "text": { "type": "string" }
                        },
                        "required": ["op", "text"]
                    }
                }
            },
            "required": ["lines"]
        }
    })
}

fn proxy_action_from_upstream_name(name: &str) -> Option<CodexPatchProxyAction> {
    if name.ends_with("_add_file") {
        Some(CodexPatchProxyAction::AddFile)
    } else if name.ends_with("_delete_file") {
        Some(CodexPatchProxyAction::DeleteFile)
    } else if name.ends_with("_update_file") {
        Some(CodexPatchProxyAction::UpdateFile)
    } else if name.ends_with("_replace_file") {
        Some(CodexPatchProxyAction::ReplaceFile)
    } else if name.ends_with("_batch") {
        Some(CodexPatchProxyAction::Batch)
    } else {
        None
    }
}

fn combine_namespace_description(namespace_description: &str, child_description: &str) -> String {
    let namespace_description = namespace_description.trim();
    let child_description = child_description.trim();
    match (
        namespace_description.is_empty(),
        child_description.is_empty(),
    ) {
        (true, true) => String::new(),
        (true, false) => child_description.to_string(),
        (false, true) => namespace_description.to_string(),
        (false, false) => format!("{namespace_description}\n\n{child_description}"),
    }
}

fn flatten_namespace_tool_name(namespace: &str, name: &str) -> String {
    if namespace.is_empty() {
        return name.to_string();
    }
    if name.is_empty() {
        return namespace.to_string();
    }
    if namespace.ends_with("__") || name.starts_with("__") {
        format!("{namespace}{name}")
    } else {
        format!("{namespace}__{name}")
    }
}

fn responses_tool_choice_to_chat(tool_choice: &Value, context: &CodexToolContext) -> Option<Value> {
    if let Some(choice_type) = tool_choice.get("type").and_then(Value::as_str) {
        if matches!(choice_type, "auto" | "none" | "required") {
            return Some(json!(choice_type));
        }
    }

    match tool_choice {
        Value::Object(object) if object.get("type").and_then(Value::as_str) == Some("function") => {
            if let Some(namespace) = object.get("namespace").and_then(Value::as_str) {
                let name = object.get("name").and_then(Value::as_str).unwrap_or("");
                return Some(json!({
                    "type": "function",
                    "function": {
                        "name": flatten_namespace_tool_name(namespace, name)
                    }
                }));
            }
            if let Some(function) = object.get("function").and_then(Value::as_object) {
                if let Some(namespace) = function.get("namespace").and_then(Value::as_str) {
                    let name = function.get("name").and_then(Value::as_str).unwrap_or("");
                    return Some(json!({
                        "type": "function",
                        "function": {
                            "name": flatten_namespace_tool_name(namespace, name)
                        }
                    }));
                }
            }
            Some(json!({
                "type": "function",
                "function": {
                    "name": object.get("name").and_then(Value::as_str).unwrap_or("")
                }
            }))
        }
        Value::Object(object) if object.get("type").and_then(Value::as_str) == Some("custom") => {
            let name = object.get("name").and_then(Value::as_str)?;
            let spec = context.custom_tools.get(name)?;
            let upstream_name = if spec.kind == CodexCustomToolKind::ApplyPatch {
                format!("{}_batch", spec.openai_name)
            } else {
                spec.openai_name.clone()
            };
            Some(json!({
                "type": "function",
                "function": { "name": upstream_name }
            }))
        }
        other => Some(other.clone()),
    }
}

fn chat_reasoning_to_response_output_item(message: &Value, response_id: &str) -> Option<Value> {
    let reasoning = chat_reasoning_text(message)?;
    if reasoning.is_empty() {
        return None;
    }
    Some(json!({
        "id": format!("rs_{response_id}"),
        "type": "reasoning",
        "reasoning_content": reasoning,
        "summary": [{ "type": "summary_text", "text": reasoning }]
    }))
}

fn chat_reasoning_text(message: &Value) -> Option<String> {
    if let Some(reasoning) = extract_reasoning_field_text(message) {
        return Some(reasoning);
    }

    if let Some(content) = message.get("content").and_then(Value::as_str) {
        if let Some((reasoning, _answer)) = split_leading_think_block(content) {
            if !reasoning.is_empty() {
                return Some(reasoning);
            }
        }
    }

    None
}

fn chat_message_to_response_output_item(message: &Value, response_id: &str) -> Option<Value> {
    let mut content = Vec::new();
    if let Some(text) = message.get("content").and_then(Value::as_str) {
        let text = split_leading_think_block(text)
            .map(|(_reasoning, answer)| answer)
            .unwrap_or_else(|| text.to_string());
        if !text.is_empty() {
            content.push(json!({ "type": "output_text", "text": text, "annotations": [] }));
        }
    } else if let Some(parts) = message.get("content").and_then(Value::as_array) {
        for part in parts {
            match part.get("type").and_then(Value::as_str).unwrap_or("") {
                "text" | "output_text" => {
                    if let Some(text) = part.get("text").and_then(Value::as_str) {
                        if !text.is_empty() {
                            content.push(
                                json!({ "type": "output_text", "text": text, "annotations": [] }),
                            );
                        }
                    }
                }
                "refusal" => {
                    if let Some(refusal) = part.get("refusal").and_then(Value::as_str) {
                        if !refusal.is_empty() {
                            content.push(json!({ "type": "refusal", "refusal": refusal }));
                        }
                    }
                }
                _ => {}
            }
        }
    }
    if let Some(refusal) = message.get("refusal").and_then(Value::as_str) {
        if !refusal.is_empty() {
            content.push(json!({ "type": "refusal", "refusal": refusal }));
        }
    }

    if content.is_empty() {
        return None;
    }

    Some(json!({
        "id": format!("{response_id}_msg"),
        "type": "message",
        "status": "completed",
        "role": "assistant",
        "content": content
    }))
}

fn chat_tool_calls_to_response_output_items(
    message: &Value,
    tool_context: &CodexToolContext,
) -> Vec<Value> {
    let mut output = Vec::new();
    if let Some(tool_calls) = message.get("tool_calls").and_then(Value::as_array) {
        for (index, tool_call) in tool_calls.iter().enumerate() {
            output.push(chat_tool_call_to_response_item(
                tool_call,
                index,
                tool_context,
            ));
        }
    } else if let Some(function_call) = message.get("function_call") {
        output.push(chat_legacy_function_call_to_response_item(
            function_call,
            tool_context,
        ));
    }
    output
}

fn chat_tool_call_to_response_item(
    tool_call: &Value,
    index: usize,
    tool_context: &CodexToolContext,
) -> Value {
    let call_id = tool_call
        .get("id")
        .and_then(Value::as_str)
        .filter(|value| !value.is_empty())
        .map(ToString::to_string)
        .unwrap_or_else(|| format!("call_{index}"));
    let function = tool_call.get("function").unwrap_or(&Value::Null);
    let name = function.get("name").and_then(Value::as_str).unwrap_or("");
    let arguments = responses_arguments_to_chat(function.get("arguments").unwrap_or(&json!({})));
    response_tool_call_item(&call_id, name, &arguments, tool_context)
}

fn chat_legacy_function_call_to_response_item(
    function_call: &Value,
    tool_context: &CodexToolContext,
) -> Value {
    let call_id = function_call
        .get("id")
        .and_then(Value::as_str)
        .filter(|value| !value.is_empty())
        .unwrap_or("call_0");
    let name = function_call
        .get("name")
        .and_then(Value::as_str)
        .unwrap_or("");
    let arguments =
        responses_arguments_to_chat(function_call.get("arguments").unwrap_or(&json!({})));
    response_tool_call_item(call_id, name, &arguments, tool_context)
}

fn tool_call_added_item(
    state: &ToolCallState,
    output_index: u32,
    tool_context: &CodexToolContext,
) -> Value {
    if tool_context.is_custom_tool_proxy(&state.name) {
        return json!({
            "type": "response.output_item.added",
            "output_index": output_index,
            "item": {
                "id": format!("ctc_{}", state.call_id),
                "type": "custom_tool_call",
                "status": "in_progress",
                "call_id": state.call_id,
                "name": tool_context.original_custom_tool_name(&state.name),
                "input": ""
            }
        });
    }
    let (display_name, namespace) = tool_context.openai_name_for_function_tool(&state.name);
    let mut item = json!({
        "type": "response.output_item.added",
        "output_index": output_index,
        "item": {
            "id": state.item_id,
            "type": "function_call",
            "status": "in_progress",
            "call_id": state.call_id,
            "name": display_name,
            "arguments": ""
        }
    });
    if !namespace.is_empty() {
        item["item"]["namespace"] = json!(namespace);
    }
    item
}

fn push_tool_call_delta_sse(
    output: &mut String,
    state: &ToolCallState,
    output_index: u32,
    delta: &str,
    tool_context: &CodexToolContext,
) {
    if tool_context.is_custom_tool_proxy(&state.name) {
        let _ = delta;
    } else {
        push_sse(
            output,
            "response.function_call_arguments.delta",
            json!({
                "type": "response.function_call_arguments.delta",
                "item_id": state.item_id,
                "output_index": output_index,
                "delta": delta
            }),
        );
    }
}

fn push_tool_call_done_sse(
    output: &mut String,
    state: &ToolCallState,
    output_index: u32,
    tool_context: &CodexToolContext,
) {
    if tool_context.is_custom_tool_proxy(&state.name) {
        push_sse(
            output,
            "response.custom_tool_call_input.delta",
            json!({
                "type": "response.custom_tool_call_input.delta",
                "item_id": format!("ctc_{}", state.call_id),
                "call_id": state.call_id,
                "output_index": output_index,
                "delta": reconstruct_custom_tool_call_input_with_context(
                    tool_context,
                    &state.name,
                    &state.arguments
                )
            }),
        );
        return;
    }
    push_sse(
        output,
        "response.function_call_arguments.done",
        json!({
            "type": "response.function_call_arguments.done",
            "item_id": state.item_id,
            "output_index": output_index,
            "arguments": state.arguments
        }),
    );
}

fn tool_call_done_item(state: &ToolCallState, tool_context: &CodexToolContext) -> Value {
    response_tool_call_item(&state.call_id, &state.name, &state.arguments, tool_context)
}

fn response_tool_call_item(
    call_id: &str,
    name: &str,
    arguments: &str,
    tool_context: &CodexToolContext,
) -> Value {
    if tool_context.is_custom_tool_proxy(name) {
        return json!({
            "id": format!("ctc_{call_id}"),
            "type": "custom_tool_call",
            "status": "completed",
            "call_id": call_id,
            "name": tool_context.original_custom_tool_name(name),
            "input": reconstruct_custom_tool_call_input_with_context(tool_context, name, arguments)
        });
    }
    let (display_name, namespace) = tool_context.openai_name_for_function_tool(name);
    let mut item = json!({
        "id": format!("fc_{call_id}"),
        "type": "function_call",
        "status": "completed",
        "call_id": call_id,
        "name": display_name,
        "arguments": arguments
    });
    if !namespace.is_empty() {
        item["namespace"] = json!(namespace);
    }
    item
}

fn split_leading_think_block(text: &str) -> Option<(String, String)> {
    let leading_ws_len = text.len() - text.trim_start().len();
    let after_ws = &text[leading_ws_len..];
    if !after_ws.starts_with(THINK_OPEN_TAG) {
        return None;
    }
    let body_start = leading_ws_len + THINK_OPEN_TAG.len();
    let close_relative = text[body_start..].find(THINK_CLOSE_TAG)?;
    let close_start = body_start + close_relative;
    let answer_start = close_start + THINK_CLOSE_TAG.len();
    Some((
        text[body_start..close_start].trim().to_string(),
        strip_think_answer_separator(&text[answer_start..]).to_string(),
    ))
}

fn strip_leading_think_open_tag(text: &str) -> Option<String> {
    let leading_ws_len = text.len() - text.trim_start().len();
    let after_ws = &text[leading_ws_len..];
    after_ws
        .strip_prefix(THINK_OPEN_TAG)
        .map(|value| value.trim().to_string())
}

fn strip_think_answer_separator(text: &str) -> &str {
    text.trim_start_matches(['\r', '\n', '\t', ' '])
}

fn extract_reasoning_field_text(value: &Value) -> Option<String> {
    for key in ["reasoning_content", "reasoning"] {
        if let Some(text) = value.get(key).and_then(Value::as_str) {
            if !text.is_empty() {
                return Some(text.to_string());
            }
        }
    }

    if let Some(reasoning) = value.get("reasoning") {
        for key in ["content", "text", "summary"] {
            if let Some(text) = reasoning.get(key).and_then(Value::as_str) {
                if !text.is_empty() {
                    return Some(text.to_string());
                }
            }
        }
    }

    value
        .get("reasoning_details")
        .and_then(extract_reasoning_details_text)
}

fn extract_reasoning_details_text(value: &Value) -> Option<String> {
    match value {
        Value::String(text) => (!text.is_empty()).then(|| text.to_string()),
        Value::Array(parts) => {
            let text = parts
                .iter()
                .filter_map(extract_reasoning_detail_part_text)
                .filter(|text| !text.is_empty())
                .collect::<Vec<_>>()
                .join("\n\n");
            (!text.is_empty()).then_some(text)
        }
        Value::Object(_) => extract_reasoning_detail_part_text(value),
        _ => None,
    }
}

fn extract_reasoning_detail_part_text(value: &Value) -> Option<String> {
    for key in ["text", "content", "summary"] {
        if let Some(text) = value.get(key).and_then(Value::as_str) {
            if !text.is_empty() {
                return Some(text.to_string());
            }
        }
    }

    if let Some(parts) = value.get("parts").and_then(Value::as_array) {
        let text = parts
            .iter()
            .filter_map(extract_reasoning_detail_part_text)
            .filter(|text| !text.is_empty())
            .collect::<Vec<_>>()
            .join("\n\n");
        return (!text.is_empty()).then_some(text);
    }

    None
}

fn extract_reasoning_summary_text(value: &Value) -> Option<String> {
    for key in ["reasoning_content", "content", "text"] {
        if let Some(text) = value.get(key).and_then(Value::as_str) {
            if !text.is_empty() {
                return Some(text.to_string());
            }
        }
    }

    let summary = value.get("summary")?;
    if let Some(text) = summary.as_str() {
        return (!text.is_empty()).then(|| text.to_string());
    }

    let parts = summary.as_array()?;
    let text = parts
        .iter()
        .filter_map(|part| {
            part.get("text")
                .and_then(Value::as_str)
                .or_else(|| part.get("content").and_then(Value::as_str))
                .or_else(|| part.as_str())
        })
        .filter(|text| !text.is_empty())
        .collect::<Vec<_>>()
        .join("\n\n");

    (!text.is_empty()).then_some(text)
}

fn default_responses_usage() -> Value {
    json!({ "input_tokens": 0, "output_tokens": 0, "total_tokens": 0 })
}

fn chat_usage_to_responses_usage(usage: Option<&Value>) -> Value {
    let Some(usage) = usage.filter(|value| value.is_object() && !value.is_null()) else {
        return default_responses_usage();
    };
    let mut input_tokens = usage
        .get("prompt_tokens")
        .or_else(|| usage.get("input_tokens"))
        .or_else(|| usage.get("promptTokenCount"))
        .and_then(Value::as_u64)
        .unwrap_or(0);
    let mut input_tokens_include_cache = usage.get("prompt_tokens").is_some();
    let output_tokens = usage
        .get("completion_tokens")
        .or_else(|| usage.get("output_tokens"))
        .or_else(|| usage.get("candidatesTokenCount"))
        .and_then(Value::as_u64)
        .unwrap_or(0);
    let mut cached_tokens = usage
        .pointer("/prompt_tokens_details/cached_tokens")
        .or_else(|| usage.pointer("/input_tokens_details/cached_tokens"))
        .or_else(|| usage.get("cachedContentTokenCount"))
        .and_then(Value::as_u64)
        .unwrap_or(0);
    let cache_creation = usage
        .get("cache_creation_input_tokens")
        .and_then(Value::as_u64)
        .unwrap_or(0);
    let cache_creation_5m = usage
        .get("cache_creation_5m_input_tokens")
        .and_then(Value::as_u64)
        .unwrap_or(0);
    let cache_creation_1h = usage
        .get("cache_creation_1h_input_tokens")
        .and_then(Value::as_u64)
        .unwrap_or(0);
    let has_claude_cache_fields = usage.get("cache_read_input_tokens").is_some()
        || usage.get("cache_creation_input_tokens").is_some()
        || usage.get("cache_creation_5m_input_tokens").is_some()
        || usage.get("cache_creation_1h_input_tokens").is_some();
    let has_cache_details = cached_tokens > 0
        || usage
            .pointer("/prompt_tokens_details/cached_tokens")
            .is_some()
        || usage
            .pointer("/input_tokens_details/cached_tokens")
            .is_some();

    if let Some(value) = usage.get("input_tokens").and_then(Value::as_u64) {
        input_tokens = value;
        input_tokens_include_cache = false;
    }
    if let Some(cache_read) = usage.get("cache_read_input_tokens").and_then(Value::as_u64) {
        cached_tokens = cache_read;
    }
    if let Some(prompt_tokens) = usage.get("promptTokenCount").and_then(Value::as_u64) {
        cached_tokens = usage
            .get("cachedContentTokenCount")
            .and_then(Value::as_u64)
            .unwrap_or(0);
        input_tokens = prompt_tokens.saturating_sub(cached_tokens);
        input_tokens_include_cache = false;
    }

    let usage_input_tokens = if input_tokens_include_cache {
        input_tokens.saturating_sub(
            cached_tokens
                + effective_cache_creation_tokens(
                    cache_creation,
                    cache_creation_5m,
                    cache_creation_1h,
                ),
        )
    } else {
        input_tokens
    };
    let should_recalculate_total = usage.get("total_tokens").is_none()
        || cached_tokens > 0
        || effective_cache_creation_tokens(cache_creation, cache_creation_5m, cache_creation_1h)
            > 0
        || usage.get("promptTokenCount").is_some();
    let total_tokens = if should_recalculate_total {
        usage_input_tokens
            + output_tokens
            + cached_tokens
            + effective_cache_creation_tokens(cache_creation, cache_creation_5m, cache_creation_1h)
    } else {
        usage
            .get("total_tokens")
            .and_then(Value::as_u64)
            .unwrap_or(usage_input_tokens + output_tokens)
    };
    let mut result = json!({
        "input_tokens": usage_input_tokens,
        "output_tokens": output_tokens,
        "total_tokens": total_tokens
    });

    if !has_claude_cache_fields && has_cache_details && cached_tokens > 0 {
        result["input_tokens_details"] = json!({ "cached_tokens": cached_tokens });
    }
    if let Some(details) = usage.get("completion_tokens_details") {
        result["output_tokens_details"] = details.clone();
    }
    if let Some(cache_read) = usage.get("cache_read_input_tokens") {
        result["cache_read_input_tokens"] = cache_read.clone();
    }
    if let Some(cache_creation) = usage.get("cache_creation_input_tokens") {
        result["cache_creation_input_tokens"] = cache_creation.clone();
    }
    if let Some(cache_creation) = usage.get("cache_creation_5m_input_tokens") {
        result["cache_creation_5m_input_tokens"] = cache_creation.clone();
    }
    if let Some(cache_creation) = usage.get("cache_creation_1h_input_tokens") {
        result["cache_creation_1h_input_tokens"] = cache_creation.clone();
    }
    let cache_ttl = match (cache_creation_5m > 0, cache_creation_1h > 0) {
        (true, true) => Some("mixed"),
        (true, false) => Some("5m"),
        (false, true) => Some("1h"),
        (false, false) => None,
    };
    if let Some(cache_ttl) = cache_ttl {
        result["cache_ttl"] = json!(cache_ttl);
    }
    result
}

fn effective_cache_creation_tokens(
    cache_creation: u64,
    cache_creation_5m: u64,
    cache_creation_1h: u64,
) -> u64 {
    if cache_creation > 0 {
        cache_creation
    } else {
        cache_creation_5m + cache_creation_1h
    }
}

fn response_status(finish_reason: Option<&str>) -> &'static str {
    match finish_reason {
        Some("length") | Some("content_filter") => "incomplete",
        _ => "completed",
    }
}

/// Map a Chat Completions `finish_reason` to the Responses
/// `incomplete_details.reason` value. `content_filter` matters here: without
/// it a filtered/truncated reply used to be reported as a normal
/// `completed` response and Codex silently lost the tail of the content.
fn incomplete_details_reason(finish_reason: Option<&str>) -> Option<&'static str> {
    match finish_reason {
        Some("length") => Some("max_output_tokens"),
        Some("content_filter") => Some("content_filter"),
        _ => None,
    }
}

fn response_output_text(value: &Value) -> String {
    match value {
        Value::String(text) => text.clone(),
        Value::Null => String::new(),
        other => canonical_json_string(other),
    }
}

fn build_custom_tool_call_history(name: &str, input: &Value) -> (String, String) {
    let input = response_output_text(input);
    if name == "apply_patch" || input.starts_with("*** Begin Patch") {
        let operations = parse_apply_patch_operations(&input);
        if operations.len() == 1 {
            let action = operations[0]
                .get("type")
                .and_then(Value::as_str)
                .and_then(single_apply_patch_action)
                .unwrap_or(CodexPatchProxyAction::Batch);
            return (
                format!("{name}_{}", action.suffix()),
                build_apply_patch_operation_arguments(&operations[0], action),
            );
        }
        return (
            format!("{name}_batch"),
            json!({ "operations": operations, "raw_patch": input }).to_string(),
        );
    }
    (name.to_string(), json!({ "input": input }).to_string())
}

fn reconstruct_custom_tool_call_input_with_context(
    tool_context: &CodexToolContext,
    upstream_name: &str,
    arguments: &str,
) -> String {
    if let Some(spec) = tool_context.custom_tools.get(upstream_name) {
        if spec.kind == CodexCustomToolKind::ApplyPatch {
            return reconstruct_apply_patch_input(spec.proxy_action, arguments);
        }
    }
    reconstruct_custom_tool_call_input(arguments)
}

fn reconstruct_custom_tool_call_input(arguments: &str) -> String {
    let Ok(value) = serde_json::from_str::<Value>(arguments) else {
        return arguments.to_string();
    };
    value
        .get("input")
        .map(response_output_text)
        .unwrap_or_else(|| arguments.to_string())
}

fn reconstruct_apply_patch_input(action: Option<CodexPatchProxyAction>, arguments: &str) -> String {
    let Ok(value) = serde_json::from_str::<Value>(arguments) else {
        return arguments.to_string();
    };
    if let Some(raw_patch) = value
        .get("raw_patch")
        .or_else(|| value.get("patch"))
        .or_else(|| value.get("input"))
        .and_then(Value::as_str)
        .filter(|value| !value.is_empty())
    {
        return raw_patch.to_string();
    }

    let operations = match action.unwrap_or(CodexPatchProxyAction::Batch) {
        CodexPatchProxyAction::AddFile => vec![json!({
            "type": "add_file",
            "path": value.get("path").and_then(Value::as_str).unwrap_or(""),
            "content": value.get("content").and_then(Value::as_str).unwrap_or("")
        })],
        CodexPatchProxyAction::DeleteFile => vec![json!({
            "type": "delete_file",
            "path": value.get("path").and_then(Value::as_str).unwrap_or("")
        })],
        CodexPatchProxyAction::UpdateFile => vec![json!({
            "type": "update_file",
            "path": value.get("path").and_then(Value::as_str).unwrap_or(""),
            "move_to": value.get("move_to").and_then(Value::as_str).unwrap_or(""),
            "hunks": value.get("hunks").cloned().unwrap_or_else(|| json!([]))
        })],
        CodexPatchProxyAction::ReplaceFile => vec![json!({
            "type": "replace_file",
            "path": value.get("path").and_then(Value::as_str).unwrap_or(""),
            "content": value.get("content").and_then(Value::as_str).unwrap_or("")
        })],
        CodexPatchProxyAction::Batch => value
            .get("operations")
            .and_then(Value::as_array)
            .cloned()
            .unwrap_or_default(),
    };

    build_apply_patch_text(&operations)
}

fn build_apply_patch_text(operations: &[Value]) -> String {
    let mut text = String::from("*** Begin Patch");
    for operation in operations {
        let op_type = operation.get("type").and_then(Value::as_str).unwrap_or("");
        let path = operation.get("path").and_then(Value::as_str).unwrap_or("");
        match op_type {
            "add_file" => {
                text.push_str(&format!("\n*** Add File: {path}"));
                for line in operation
                    .get("content")
                    .and_then(Value::as_str)
                    .unwrap_or("")
                    .lines()
                {
                    text.push_str("\n+");
                    text.push_str(line);
                }
            }
            "delete_file" => {
                text.push_str(&format!("\n*** Delete File: {path}"));
            }
            "update_file" => {
                text.push_str(&format!("\n*** Update File: {path}"));
                if let Some(move_to) = operation.get("move_to").and_then(Value::as_str) {
                    if !move_to.is_empty() {
                        text.push_str(&format!("\n*** Move to: {move_to}"));
                    }
                }
                if let Some(hunks) = operation.get("hunks").and_then(Value::as_array) {
                    for hunk in hunks {
                        let context = hunk.get("context").and_then(Value::as_str).unwrap_or("");
                        if context.is_empty() {
                            text.push_str("\n@@");
                        } else {
                            text.push_str(&format!("\n@@ {context}"));
                        }
                        if let Some(lines) = hunk.get("lines").and_then(Value::as_array) {
                            for line in lines {
                                text.push('\n');
                                text.push_str(line_op_prefix(
                                    line.get("op").and_then(Value::as_str).unwrap_or("context"),
                                ));
                                text.push_str(
                                    line.get("text").and_then(Value::as_str).unwrap_or(""),
                                );
                            }
                        }
                    }
                }
            }
            "replace_file" => {
                text.push_str(&format!("\n*** Delete File: {path}"));
                text.push_str(&format!("\n*** Add File: {path}"));
                for line in operation
                    .get("content")
                    .and_then(Value::as_str)
                    .unwrap_or("")
                    .lines()
                {
                    text.push_str("\n+");
                    text.push_str(line);
                }
            }
            _ => {}
        }
    }
    text.push_str("\n*** End Patch");
    text
}

fn line_op_prefix(op: &str) -> &'static str {
    match op {
        "add" => "+",
        "remove" | "delete" => "-",
        _ => " ",
    }
}

fn parse_apply_patch_operations(input: &str) -> Vec<Value> {
    let mut operations = Vec::new();
    let mut current: Option<serde_json::Map<String, Value>> = None;
    let mut content_lines: Vec<String> = Vec::new();
    let mut hunks: Vec<Value> = Vec::new();
    let mut current_hunk: Option<serde_json::Map<String, Value>> = None;
    let mut hunk_lines: Vec<Value> = Vec::new();

    let flush_hunk = |current_hunk: &mut Option<serde_json::Map<String, Value>>,
                      hunk_lines: &mut Vec<Value>,
                      hunks: &mut Vec<Value>| {
        if let Some(mut hunk) = current_hunk.take() {
            hunk.insert("lines".to_string(), json!(std::mem::take(hunk_lines)));
            hunks.push(Value::Object(hunk));
        }
    };
    let flush_operation = |current: &mut Option<serde_json::Map<String, Value>>,
                           content_lines: &mut Vec<String>,
                           hunks: &mut Vec<Value>,
                           operations: &mut Vec<Value>| {
        if let Some(mut operation) = current.take() {
            match operation.get("type").and_then(Value::as_str).unwrap_or("") {
                "add_file" | "replace_file" => {
                    operation.insert("content".to_string(), json!(content_lines.join("\n")));
                }
                "update_file" => {
                    operation.insert("hunks".to_string(), json!(std::mem::take(hunks)));
                }
                _ => {}
            }
            content_lines.clear();
            operations.push(Value::Object(operation));
        }
    };

    for raw_line in input.lines() {
        if raw_line == "*** Begin Patch" || raw_line == "*** End Patch" {
            continue;
        }
        if let Some(path) = raw_line.strip_prefix("*** Add File: ") {
            flush_hunk(&mut current_hunk, &mut hunk_lines, &mut hunks);
            flush_operation(
                &mut current,
                &mut content_lines,
                &mut hunks,
                &mut operations,
            );
            current = Some(serde_json::Map::from_iter([
                ("type".to_string(), json!("add_file")),
                ("path".to_string(), json!(path)),
            ]));
            continue;
        }
        if let Some(path) = raw_line.strip_prefix("*** Delete File: ") {
            flush_hunk(&mut current_hunk, &mut hunk_lines, &mut hunks);
            flush_operation(
                &mut current,
                &mut content_lines,
                &mut hunks,
                &mut operations,
            );
            current = Some(serde_json::Map::from_iter([
                ("type".to_string(), json!("delete_file")),
                ("path".to_string(), json!(path)),
            ]));
            continue;
        }
        if let Some(path) = raw_line.strip_prefix("*** Update File: ") {
            flush_hunk(&mut current_hunk, &mut hunk_lines, &mut hunks);
            flush_operation(
                &mut current,
                &mut content_lines,
                &mut hunks,
                &mut operations,
            );
            current = Some(serde_json::Map::from_iter([
                ("type".to_string(), json!("update_file")),
                ("path".to_string(), json!(path)),
            ]));
            continue;
        }
        if let Some(path) = raw_line.strip_prefix("*** Move to: ") {
            if let Some(operation) = current.as_mut() {
                operation.insert("move_to".to_string(), json!(path));
            }
            continue;
        }
        if raw_line.starts_with("@@") {
            flush_hunk(&mut current_hunk, &mut hunk_lines, &mut hunks);
            let context = raw_line.strip_prefix("@@").unwrap_or("").trim().to_string();
            current_hunk = Some(serde_json::Map::from_iter([(
                "context".to_string(),
                json!(context),
            )]));
            continue;
        }
        if let Some(operation) = current.as_ref() {
            match operation.get("type").and_then(Value::as_str).unwrap_or("") {
                "add_file" | "replace_file" => {
                    if let Some(line) = raw_line.strip_prefix('+') {
                        content_lines.push(line.to_string());
                    }
                }
                "update_file" => {
                    let (op, text) = match raw_line.chars().next() {
                        Some('+') => ("add", &raw_line[1..]),
                        Some('-') => ("remove", &raw_line[1..]),
                        Some(' ') => ("context", &raw_line[1..]),
                        _ => ("context", raw_line),
                    };
                    hunk_lines.push(json!({ "op": op, "text": text }));
                }
                _ => {}
            }
        }
    }

    flush_hunk(&mut current_hunk, &mut hunk_lines, &mut hunks);
    flush_operation(
        &mut current,
        &mut content_lines,
        &mut hunks,
        &mut operations,
    );
    operations
}

fn single_apply_patch_action(op_type: &str) -> Option<CodexPatchProxyAction> {
    match op_type {
        "add_file" => Some(CodexPatchProxyAction::AddFile),
        "delete_file" => Some(CodexPatchProxyAction::DeleteFile),
        "update_file" => Some(CodexPatchProxyAction::UpdateFile),
        "replace_file" => Some(CodexPatchProxyAction::ReplaceFile),
        _ => None,
    }
}

fn build_apply_patch_operation_arguments(
    operation: &Value,
    action: CodexPatchProxyAction,
) -> String {
    match action {
        CodexPatchProxyAction::AddFile | CodexPatchProxyAction::ReplaceFile => json!({
            "content": operation.get("content").and_then(Value::as_str).unwrap_or(""),
            "path": operation.get("path").and_then(Value::as_str).unwrap_or("")
        })
        .to_string(),
        CodexPatchProxyAction::DeleteFile => json!({
            "path": operation.get("path").and_then(Value::as_str).unwrap_or("")
        })
        .to_string(),
        CodexPatchProxyAction::UpdateFile => {
            let mut args = json!({
                "hunks": operation.get("hunks").cloned().unwrap_or_else(|| json!([])),
                "path": operation.get("path").and_then(Value::as_str).unwrap_or("")
            });
            if let Some(move_to) = operation.get("move_to").and_then(Value::as_str) {
                if !move_to.is_empty() {
                    args["move_to"] = json!(move_to);
                }
            }
            args.to_string()
        }
        CodexPatchProxyAction::Batch => json!({ "operations": [operation.clone()] }).to_string(),
    }
}

fn copy_response_request_fields(response: &mut Value, original_request: Option<&Value>) {
    let Some(original_request) = original_request else {
        return;
    };
    for key in [
        "instructions",
        "max_output_tokens",
        "parallel_tool_calls",
        "previous_response_id",
        "reasoning",
        "temperature",
        "tool_choice",
        "tools",
        "top_p",
        "metadata",
    ] {
        if let Some(value) = original_request.get(key) {
            response[key] = value.clone();
        }
    }
}

fn responses_arguments_to_chat(value: &Value) -> String {
    match value {
        Value::String(text) => normalize_chat_tool_arguments_string(text),
        Value::Object(_) => canonical_json_string(value),
        Value::Null => "{}".to_string(),
        other => canonical_json_string(&json!({ "input": other })),
    }
}

fn normalize_chat_tool_arguments_string(text: &str) -> String {
    let trimmed = text.trim();
    if trimmed.is_empty() {
        return "{}".to_string();
    }
    match serde_json::from_str::<Value>(trimmed) {
        Ok(Value::Object(_)) => trimmed.to_string(),
        Ok(value) => canonical_json_string(&json!({ "input": value })),
        Err(_) => canonical_json_string(&json!({ "input": text })),
    }
}

fn instruction_text(value: &Value) -> String {
    match value {
        Value::String(text) => text.clone(),
        Value::Array(parts) => parts
            .iter()
            .filter_map(|part| {
                part.get("text")
                    .and_then(Value::as_str)
                    .or_else(|| part.as_str())
            })
            .filter(|text| !text.is_empty())
            .collect::<Vec<_>>()
            .join("\n\n"),
        other => other.as_str().unwrap_or_default().to_string(),
    }
}

fn canonical_json_string(value: &Value) -> String {
    match value {
        Value::Null => "null".to_string(),
        Value::Bool(value) => value.to_string(),
        Value::Number(value) => value.to_string(),
        Value::String(value) => serde_json::to_string(value).unwrap_or_default(),
        Value::Array(values) => {
            let parts = values.iter().map(canonical_json_string).collect::<Vec<_>>();
            format!("[{}]", parts.join(","))
        }
        Value::Object(map) => {
            let mut entries = map.iter().collect::<Vec<_>>();
            entries.sort_by_key(|(key, _)| *key);
            let parts = entries
                .into_iter()
                .map(|(key, value)| {
                    let key = serde_json::to_string(key).unwrap_or_default();
                    format!("{key}:{}", canonical_json_string(value))
                })
                .collect::<Vec<_>>();
            format!("{{{}}}", parts.join(","))
        }
    }
}

fn apply_chat_reasoning_options(result: &mut Value, body: &Value, model: &str) {
    let Some(reasoning_enabled) = reasoning_requested(body) else {
        return;
    };
    let style = infer_chat_reasoning_style(model);

    match style {
        ChatReasoningStyle::Thinking => {
            result["thinking"] = json!({
                "type": if reasoning_enabled { "enabled" } else { "disabled" }
            });
        }
        ChatReasoningStyle::EnableThinking => {
            result["enable_thinking"] = json!(reasoning_enabled);
        }
        ChatReasoningStyle::ReasoningSplit => {
            result["reasoning_split"] = json!(reasoning_enabled);
        }
        ChatReasoningStyle::AnthropicThinking => {
            // Claude's own `thinking` object needs the raw effort string
            // (for the budget_tokens table), not just the on/off boolean
            // the other styles use above, and it must not fall through to
            // the generic `reasoning_effort` mapping below (Claude doesn't
            // accept that field — see `supports_reasoning_effort`).
            apply_anthropic_thinking(result, body);
            return;
        }
        _ => {}
    }

    if !reasoning_enabled {
        if style == ChatReasoningStyle::OpenRouter {
            result["reasoning"] = json!({ "effort": "none" });
        }
        return;
    }

    let Some(effort) = body.pointer("/reasoning/effort").and_then(Value::as_str) else {
        return;
    };
    let Some(mapped) = map_chat_reasoning_effort(effort, style) else {
        return;
    };

    match style {
        ChatReasoningStyle::OpenRouter => {
            result["reasoning"] = json!({ "effort": mapped });
        }
        ChatReasoningStyle::DeepSeek
        | ChatReasoningStyle::LowHigh
        | ChatReasoningStyle::Default
            if supports_reasoning_effort(model) =>
        {
            result["reasoning_effort"] = json!(mapped);
        }
        _ => {}
    }
}

/// Budget (in tokens) Anthropic's native `thinking.budget_tokens` uses per
/// Codex reasoning tier. Codex's effort enum
/// (none/minimal/low/medium/high/xhigh/max/ultra) has no official mapping to
/// Anthropic's token budget, so these are hand-picked, doubling from `low`
/// up to `xhigh` and continuing the curve for the gpt-5.6-sol-only
/// `max`/`ultra` tiers so picking them on a Claude model still yields a
/// larger budget than `xhigh` instead of silently no-op'ing.
fn anthropic_thinking_budget_tokens(effort: &str) -> Option<u64> {
    match effort {
        "low" => Some(2048),
        "medium" => Some(8192),
        "high" => Some(16384),
        "xhigh" => Some(32768),
        "max" => Some(49152),
        "ultra" => Some(65536),
        _ => None,
    }
}

/// Maps the Responses `reasoning.effort` string onto Anthropic's native
/// `thinking` object (`{"type":"enabled","budget_tokens":N}` or
/// `{"type":"disabled"}`) and applies the request-shape constraints
/// Anthropic enforces whenever thinking is enabled:
/// - `temperature`/`top_p` must be left at their API defaults (Anthropic
///   rejects a non-default `temperature` and rejects `top_p` outright while
///   thinking is on), so both are stripped rather than forwarded as
///   possibly-invalid values.
/// - `max_tokens` must be strictly greater than `budget_tokens`, so a
///   caller-supplied `max_tokens` at or under the budget gets bumped just
///   above it.
fn apply_anthropic_thinking(result: &mut Value, body: &Value) {
    let Some(effort) = body
        .pointer("/reasoning/effort")
        .and_then(Value::as_str)
        .map(|value| value.trim().to_ascii_lowercase())
    else {
        return;
    };

    let thinking = match effort.as_str() {
        "none" | "minimal" => json!({ "type": "disabled" }),
        _ => match anthropic_thinking_budget_tokens(&effort) {
            Some(budget_tokens) => json!({ "type": "enabled", "budget_tokens": budget_tokens }),
            // Unrecognized effort value: leave `thinking` untouched rather
            // than guess at a budget.
            None => return,
        },
    };
    let enabling = thinking["type"] == "enabled";
    result["thinking"] = thinking;

    if enabling {
        if let Some(obj) = result.as_object_mut() {
            obj.remove("temperature");
            obj.remove("top_p");
        }
        if let (Some(max_tokens), Some(budget_tokens)) = (
            result.get("max_tokens").and_then(Value::as_u64),
            result
                .pointer("/thinking/budget_tokens")
                .and_then(Value::as_u64),
        ) {
            if max_tokens <= budget_tokens {
                result["max_tokens"] = json!(budget_tokens + 1024);
            }
        }
    }
}

fn reasoning_requested(body: &Value) -> Option<bool> {
    if let Some(effort) = body.pointer("/reasoning/effort").and_then(Value::as_str) {
        return Some(!matches!(
            effort.trim().to_ascii_lowercase().as_str(),
            "none" | "off" | "disabled"
        ));
    }

    body.get("reasoning").map(|value| !value.is_null())
}

fn infer_chat_reasoning_style(model: &str) -> ChatReasoningStyle {
    let model = model.to_ascii_lowercase();
    if model.contains("openrouter") || model.starts_with("openrouter/") {
        return ChatReasoningStyle::OpenRouter;
    }
    if model.contains("deepseek") {
        return ChatReasoningStyle::DeepSeek;
    }
    if model.contains("qwen") || model.contains("dashscope") || model.contains("bailian") {
        return ChatReasoningStyle::EnableThinking;
    }
    if model.contains("kimi")
        || model.contains("moonshot")
        || model.contains("glm")
        || model.contains("zhipu")
        || model.contains("z.ai")
        || model.contains("mimo")
    {
        return ChatReasoningStyle::Thinking;
    }
    if model.contains("minimax") {
        return ChatReasoningStyle::ReasoningSplit;
    }
    if model.contains("siliconflow") {
        return ChatReasoningStyle::EnableThinking;
    }
    if model.contains("stepfun") || model.contains("step-3.5-flash-2603") {
        return ChatReasoningStyle::LowHigh;
    }
    if model.contains("claude") || model.contains("anthropic") {
        return ChatReasoningStyle::AnthropicThinking;
    }
    ChatReasoningStyle::Default
}

fn map_chat_reasoning_effort(effort: &str, style: ChatReasoningStyle) -> Option<&'static str> {
    let effort = effort.trim().to_ascii_lowercase();
    if matches!(effort.as_str(), "none" | "off" | "disabled") {
        return None;
    }

    match style {
        ChatReasoningStyle::DeepSeek => match effort.as_str() {
            "max" | "xhigh" => Some("max"),
            _ => Some("high"),
        },
        ChatReasoningStyle::LowHigh => match effort.as_str() {
            "minimal" | "low" => Some("low"),
            _ => Some("high"),
        },
        ChatReasoningStyle::OpenRouter => match effort.as_str() {
            "max" | "xhigh" => Some("xhigh"),
            "high" => Some("high"),
            "medium" => Some("medium"),
            "low" => Some("low"),
            "minimal" => Some("minimal"),
            _ => None,
        },
        _ => match effort.as_str() {
            "minimal" => Some("minimal"),
            "low" => Some("low"),
            "medium" => Some("medium"),
            "high" => Some("high"),
            "xhigh" => Some("xhigh"),
            "max" => Some("max"),
            // gpt-5.6-sol-only tier; falls through to here for any
            // non-Anthropic style that reaches this table (Claude never
            // does — it's fully handled by `apply_anthropic_thinking`).
            "ultra" => Some("ultra"),
            _ => None,
        },
    }
}

fn supports_reasoning_effort(model: &str) -> bool {
    is_openai_o_series(model)
        || model
            .to_lowercase()
            .strip_prefix("gpt-")
            .and_then(|rest| rest.chars().next())
            .is_some_and(|ch| ch.is_ascii_digit() && ch >= '5')
        || infer_chat_reasoning_style(model) == ChatReasoningStyle::DeepSeek
        || infer_chat_reasoning_style(model) == ChatReasoningStyle::LowHigh
}

fn is_openai_o_series(model: &str) -> bool {
    model.len() > 1
        && model.starts_with('o')
        && model
            .as_bytes()
            .get(1)
            .is_some_and(|byte| byte.is_ascii_digit())
}

/// GPT/o-series/embedding/image-gen models are served by OpenAI's own
/// backend, where Codex's native history/reasoning item shapes already work
/// unmodified. The Codex-internal item rewriting, stale-reasoning stripping
/// and context-budget trimming `upstream_request_parts` otherwise applies
/// exist only to keep third-party (CPA-translated) upstreams from rejecting
/// those shapes — running them against a native OpenAI request just adds
/// rewrite/retry churn for nothing.
pub(crate) fn is_native_openai_model(model: &str) -> bool {
    let model = model.trim().to_ascii_lowercase();
    model.starts_with("gpt")
        || model.starts_with("text-embedding")
        || model.starts_with("dall-e")
        || is_openai_o_series(&model)
}

// ===== Context Budget Management =====

const ESTIMATE_CHARS_PER_TOKEN: f64 = 3.5;
const ESTIMATE_CJK_CHARS_PER_TOKEN: f64 = 1.5;
const ESTIMATE_MESSAGE_OVERHEAD: u64 = 4;
const ESTIMATE_IMAGE_DEFAULT_TOKENS: u64 = 800;
const CONTEXT_BUDGET_MIN_RECENT_TURNS: usize = 3;

/// Estimate token count for a text string.
pub fn estimate_text_tokens(text: &str) -> u64 {
    if text.is_empty() {
        return 0;
    }
    let mut ascii_chars = 0u64;
    let mut other_chars = 0u64;
    for ch in text.chars() {
        if ch.is_ascii() {
            ascii_chars += 1;
        } else {
            other_chars += 1;
        }
    }
    let ascii_tokens = (ascii_chars as f64 / ESTIMATE_CHARS_PER_TOKEN).ceil() as u64;
    let other_tokens = (other_chars as f64 / ESTIMATE_CJK_CHARS_PER_TOKEN).ceil() as u64;
    ascii_tokens + other_tokens
}

/// Estimate token count for a single Chat Completions message.
pub fn estimate_message_tokens(message: &Value, image_estimate: u64) -> u64 {
    let mut tokens = ESTIMATE_MESSAGE_OVERHEAD;
    match message.get("content") {
        Some(Value::String(text)) => tokens += estimate_text_tokens(text),
        Some(Value::Array(parts)) => {
            for part in parts {
                if part.get("type").and_then(Value::as_str) == Some("image_url") {
                    tokens += image_estimate;
                } else {
                    let text = part.get("text").and_then(Value::as_str).unwrap_or("");
                    tokens += estimate_text_tokens(text);
                }
            }
        }
        _ => {}
    }
    if let Some(tool_calls) = message.get("tool_calls").and_then(Value::as_array) {
        for tc in tool_calls {
            tokens += estimate_text_tokens(&serde_json::to_string(tc).unwrap_or_default());
        }
    }
    if let Some(reasoning) = message.get("reasoning_content").and_then(Value::as_str) {
        tokens += estimate_text_tokens(reasoning);
    }
    tokens
}

/// Estimate total token count for a Chat Completions request body.
pub fn estimate_request_tokens(body: &Value, image_estimate: u64) -> u64 {
    let mut total = 0u64;
    if let Some(messages) = body.get("messages").and_then(Value::as_array) {
        for msg in messages {
            total += estimate_message_tokens(msg, image_estimate);
        }
    }
    if let Some(tools) = body.get("tools").and_then(Value::as_array) {
        total += estimate_text_tokens(&serde_json::to_string(tools).unwrap_or_default());
    }
    total
}

fn count_images_in_message(message: &Value) -> usize {
    match message.get("content") {
        Some(Value::Array(parts)) => parts
            .iter()
            .filter(|p| p.get("type").and_then(Value::as_str) == Some("image_url"))
            .count(),
        _ => 0,
    }
}

fn strip_images_from_message(message: &mut Value) -> usize {
    let content = message.get("content").cloned();
    let Some(Value::Array(parts)) = content else {
        return 0;
    };
    let image_count = parts
        .iter()
        .filter(|p| p.get("type").and_then(Value::as_str) == Some("image_url"))
        .count();
    if image_count == 0 {
        return 0;
    }

    let mut new_parts: Vec<Value> = parts
        .into_iter()
        .filter(|p| p.get("type").and_then(Value::as_str) != Some("image_url"))
        .collect();
    new_parts.push(json!({
        "type": "text",
        "text": format!("[{image_count} image(s) omitted to fit context budget]")
    }));

    let all_text = new_parts
        .iter()
        .all(|p| p.get("type").and_then(Value::as_str) == Some("text"));
    if all_text {
        let text = new_parts
            .iter()
            .filter_map(|p| p.get("text").and_then(Value::as_str))
            .collect::<Vec<_>>()
            .join("\n");
        message["content"] = json!(text);
    } else {
        message["content"] = json!(new_parts);
    }
    image_count
}

fn find_recent_turns_start(messages: &[Value], min_turns: usize) -> usize {
    let mut turn_count = 0usize;
    for i in (0..messages.len()).rev() {
        if messages[i].get("role").and_then(Value::as_str) == Some("user") {
            turn_count += 1;
            if turn_count >= min_turns {
                return i;
            }
        }
    }
    0
}

/// Apply context budget trimming to a Chat Completions request body.
///
/// Strategy:
/// 1. If under budget, pass through unchanged.
/// 2. Phase 1: Strip images from older messages (keep last user message images).
/// 3. Phase 2: Strip ALL images except the very last user message.
/// 4. Phase 3: Remove oldest non-system messages while keeping recent turns.
pub fn apply_context_budget(body: &mut Value, budget: &ContextBudgetConfig) -> ContextBudgetReport {
    if budget.is_unlimited() {
        return ContextBudgetReport::default();
    }
    let image_est = if budget.image_token_estimate > 0 {
        budget.image_token_estimate
    } else {
        ESTIMATE_IMAGE_DEFAULT_TOKENS
    };

    let original = estimate_request_tokens(body, image_est);
    if original <= budget.max_input_tokens {
        return ContextBudgetReport {
            original_estimated_tokens: original,
            final_estimated_tokens: original,
            ..Default::default()
        };
    }

    // Tool definitions are part of the upstream prompt too (Codex ships a
    // large tool schema). Every per-phase "are we under budget yet?" check
    // below must include them, or trimming stops early and the request is
    // still over the real budget even though the report claims otherwise.
    let tools_tokens = body
        .get("tools")
        .and_then(Value::as_array)
        .map(|tools| estimate_text_tokens(&serde_json::to_string(tools).unwrap_or_default()))
        .unwrap_or(0);

    let Some(messages) = body.get_mut("messages").and_then(Value::as_array_mut) else {
        return ContextBudgetReport {
            original_estimated_tokens: original,
            final_estimated_tokens: original,
            ..Default::default()
        };
    };

    let mut report = ContextBudgetReport {
        original_estimated_tokens: original,
        was_trimmed: true,
        ..Default::default()
    };

    // Phase 1: Strip images from all messages except the last 2 user messages.
    let mut recent_user_seen = 0usize;
    let mut phase1_boundary = 0usize;
    for i in (0..messages.len()).rev() {
        if messages[i].get("role").and_then(Value::as_str) == Some("user") {
            recent_user_seen += 1;
            if recent_user_seen >= 2 {
                phase1_boundary = i;
                break;
            }
        }
    }
    for i in 0..phase1_boundary {
        report.images_stripped += strip_images_from_message(&mut messages[i]);
    }

    let current = tools_tokens
        + messages
            .iter()
            .map(|m| estimate_message_tokens(m, image_est))
            .sum::<u64>();
    if current <= budget.max_input_tokens {
        report.final_estimated_tokens = current;
        return report;
    }

    // Phase 2: Strip ALL images except the very last user message.
    let last_user_idx = messages
        .iter()
        .rposition(|m| m.get("role").and_then(Value::as_str) == Some("user"));
    for i in 0..messages.len() {
        if Some(i) == last_user_idx {
            continue;
        }
        report.images_stripped += strip_images_from_message(&mut messages[i]);
    }

    let current = tools_tokens
        + messages
            .iter()
            .map(|m| estimate_message_tokens(m, image_est))
            .sum::<u64>();
    if current <= budget.max_input_tokens {
        report.final_estimated_tokens = current;
        return report;
    }

    // Phase 3: Remove oldest messages, keeping recent turns. The pinned
    // prefix covers leading system/developer messages plus Codex-injected
    // instruction user messages (permissions / AGENTS.md / environment
    // context), mirroring the Responses-path protection.
    let system_end = leading_instruction_items_end(messages);
    let recent_start = find_recent_turns_start(messages, CONTEXT_BUDGET_MIN_RECENT_TURNS);
    let soft_keep_from = recent_start.max(system_end);
    // Prefer keeping the configured number of recent user turns, but when
    // that soft guard still leaves the request over budget, continue
    // trimming up to (but never including) the latest user message. The
    // latest user message and everything after it form the active turn and
    // are the hard preservation boundary.
    let hard_keep_from = messages
        .iter()
        .rposition(|message| message.get("role").and_then(Value::as_str) == Some("user"))
        .unwrap_or(messages.len())
        .max(soft_keep_from);

    if hard_keep_from > system_end {
        let mut running = tools_tokens
            + messages
                .iter()
                .map(|m| estimate_message_tokens(m, image_est))
                .sum::<u64>();

        let mut remove_up_to = system_end;
        for i in system_end..hard_keep_from {
            let removed = i - system_end;
            let marker_tokens = if removed == 0 {
                0
            } else {
                estimate_message_tokens(
                    &json!({
                        "role": "system",
                        "content": format!(
                            "[Earlier conversation ({removed} messages) trimmed to fit context budget]"
                        )
                    }),
                    image_est,
                )
            };
            if running.saturating_add(marker_tokens) <= budget.max_input_tokens {
                break;
            }
            running -= estimate_message_tokens(&messages[i], image_est);
            remove_up_to = i + 1;
        }

        if remove_up_to > system_end {
            let removed = remove_up_to - system_end;
            report.messages_removed = removed;
            let marker = json!({
                "role": "system",
                "content": format!(
                    "[Earlier conversation ({removed} messages) trimmed to fit context budget]"
                )
            });
            messages.drain(system_end..remove_up_to);
            messages.insert(system_end, marker);
            remove_orphaned_tool_messages(messages, system_end + 1, &mut report);
        }
    }

    report.final_estimated_tokens = tools_tokens
        + messages
            .iter()
            .map(|m| estimate_message_tokens(m, image_est))
            .sum::<u64>();
    report
}

/// After Phase 3 drops a prefix of old messages, the cut can land in the
/// middle of a tool-call exchange: the assistant message carrying
/// `tool_calls` gets removed while its `role: "tool"` replies survive.
/// OpenAI-style upstreams reject such a request outright ("messages with
/// role 'tool' must be a response to a preceding message with 'tool_calls'"),
/// so drop any tool message whose `tool_call_id` no longer has a matching
/// preceding assistant `tool_calls` entry.
fn remove_orphaned_tool_messages(
    messages: &mut Vec<Value>,
    scan_from: usize,
    report: &mut ContextBudgetReport,
) {
    let mut known_call_ids: BTreeSet<String> = BTreeSet::new();
    let mut index = scan_from.min(messages.len());
    // Seed with any tool_calls that survive before the scan window (system
    // prefix cannot contain them, but stay defensive about the boundary).
    for message in messages.iter().take(index) {
        collect_tool_call_ids(message, &mut known_call_ids);
    }
    while index < messages.len() {
        let message = &messages[index];
        if message.get("role").and_then(Value::as_str) == Some("tool") {
            let call_id = message
                .get("tool_call_id")
                .and_then(Value::as_str)
                .unwrap_or("");
            if call_id.is_empty() || !known_call_ids.contains(call_id) {
                messages.remove(index);
                report.messages_removed += 1;
                continue;
            }
        } else {
            collect_tool_call_ids(message, &mut known_call_ids);
        }
        index += 1;
    }
}

fn collect_tool_call_ids(message: &Value, known_call_ids: &mut BTreeSet<String>) {
    if let Some(tool_calls) = message.get("tool_calls").and_then(Value::as_array) {
        for tool_call in tool_calls {
            if let Some(id) = tool_call.get("id").and_then(Value::as_str) {
                if !id.is_empty() {
                    known_call_ids.insert(id.to_string());
                }
            }
        }
    }
}

// ===== Responses Input Context Budget =====

fn estimate_responses_item_tokens(item: &Value, image_est: u64) -> u64 {
    let mut tokens = ESTIMATE_MESSAGE_OVERHEAD;
    match item.get("type").and_then(Value::as_str) {
        Some("function_call") | Some("custom_tool_call") => {
            if let Some(args) = item.get("arguments").or_else(|| item.get("input")) {
                tokens += estimate_text_tokens(&serde_json::to_string(args).unwrap_or_default());
            }
            if let Some(name) = item.get("name").and_then(Value::as_str) {
                tokens += estimate_text_tokens(name);
            }
        }
        Some("function_call_output") | Some("custom_tool_call_output") => {
            if let Some(output) = item.get("output") {
                tokens += estimate_text_tokens(&response_output_text(output));
            }
        }
        Some("reasoning") => {
            if let Some(text) = responses_reasoning_text(item) {
                tokens += estimate_text_tokens(&text);
            }
        }
        _ => {
            if let Some(content) = item.get("content") {
                tokens += estimate_responses_content_tokens(content, image_est);
            }
        }
    }
    tokens
}

fn estimate_responses_content_tokens(content: &Value, image_est: u64) -> u64 {
    match content {
        Value::String(text) => estimate_text_tokens(text),
        Value::Array(parts) => {
            let mut tokens = 0u64;
            for part in parts {
                match part.get("type").and_then(Value::as_str) {
                    Some("input_image") => tokens += image_est,
                    _ => {
                        let text = part.get("text").and_then(Value::as_str).unwrap_or("");
                        tokens += estimate_text_tokens(text);
                    }
                }
            }
            tokens
        }
        _ => estimate_text_tokens(&content.to_string()),
    }
}

fn strip_images_from_responses_item(item: &mut Value) -> usize {
    let Some(content) = item.get_mut("content") else {
        return 0;
    };
    let Some(parts) = content.as_array().cloned() else {
        return 0;
    };
    let image_count = parts
        .iter()
        .filter(|p| p.get("type").and_then(Value::as_str) == Some("input_image"))
        .count();
    if image_count == 0 {
        return 0;
    }
    let mut new_parts: Vec<Value> = parts
        .into_iter()
        .filter(|p| p.get("type").and_then(Value::as_str) != Some("input_image"))
        .collect();
    new_parts.push(json!({
        "type": "input_text",
        "text": format!("[{image_count} image(s) omitted to fit context budget]")
    }));
    *content = Value::Array(new_parts);
    image_count
}

fn find_responses_recent_boundary(items: &[Value], min_turns: usize) -> usize {
    let mut turn_count = 0usize;
    for i in (0..items.len()).rev() {
        let role = items[i].get("role").and_then(Value::as_str).unwrap_or("");
        if role == "user" {
            turn_count += 1;
            if turn_count >= min_turns {
                return i;
            }
        }
    }
    0
}

/// Markers identifying Codex-injected instruction messages at the head of a
/// request (permissions, AGENTS.md, environment context, collaboration mode,
/// app context). They carry standing behavioral rules for the whole thread —
/// e.g. the `update_plan` task-list requirement — so budget trimming must
/// never drop them, or models silently stop following those rules.
const LEADING_INSTRUCTION_MARKERS: [&str; 6] = [
    "<permissions instructions>",
    "<environment_context>",
    "<collaboration_mode>",
    "<user_instructions>",
    "<app-context>",
    "# AGENTS.md instructions",
];

fn item_is_instruction_message(item: &Value) -> bool {
    if let Some(item_type) = item.get("type").and_then(Value::as_str) {
        if item_type != "message" {
            return false;
        }
    }
    match item.get("role").and_then(Value::as_str) {
        Some("system") | Some("developer") => true,
        Some("user") => {
            let text = item
                .get("content")
                .map(instruction_text)
                .unwrap_or_default();
            let text = text.trim_start();
            LEADING_INSTRUCTION_MARKERS
                .iter()
                .any(|marker| text.starts_with(marker))
        }
        _ => false,
    }
}

/// Length of the leading contiguous run of instruction messages (works for
/// both Responses `input` items and Chat Completions `messages`). Identical
/// markers deeper in the history are treated as ordinary items.
fn leading_instruction_items_end(items: &[Value]) -> usize {
    items
        .iter()
        .take_while(|item| item_is_instruction_message(item))
        .count()
}

/// Strip `type: "reasoning"` items from a Responses API `input` array
/// before relaying upstream, covering two distinct ways Anthropic rejects
/// stale/malformed reasoning blocks:
///
/// 1. Reasoning items that belong to *completed* prior turns (anything
///    strictly before the last user message). Anthropic's Messages API
///    requires `thinking`/`redacted_thinking` blocks in the *latest*
///    assistant message to be replayed byte-for-byte exactly as originally
///    returned. Client-side context compaction (Codex's own history
///    summarization) or a mid-conversation model switch can rebuild/merge an
///    older assistant turn while it still carries a stale reasoning item,
///    which Anthropic then rejects with an HTTP 400
///    (`thinking blocks ... cannot be modified`).
/// 2. A dangling reasoning item at the very tail of the array (see
///    `strip_dangling_trailing_reasoning_item`), which Anthropic rejects
///    with a *different* HTTP 400
///    (`The final block in an assistant message cannot be thinking`).
///
/// Reasoning content from
/// completed turns isn't needed to continue the conversation — the model
/// re-derives its own reasoning for the *current* turn — so it is always
/// safe to drop it here, as long as the still-open tail of the conversation
/// (everything from the last user message onward, e.g. an in-flight
/// tool-call loop within the current turn) is left completely untouched.
fn strip_stale_reasoning_items(items: &mut Vec<Value>) -> usize {
    let mut boundary = items
        .iter()
        .rposition(|item| item.get("role").and_then(Value::as_str) == Some("user"))
        .map(|idx| idx + 1)
        .unwrap_or(0);
    let mut removed = 0usize;
    let mut i = 0usize;
    while i < boundary {
        if items[i].get("type").and_then(Value::as_str) == Some("reasoning") {
            items.remove(i);
            boundary -= 1;
            removed += 1;
        } else {
            i += 1;
        }
    }
    removed += strip_dangling_trailing_reasoning_item(items);
    removed
}

/// Drop the very last `input` item if it is a `type: "reasoning"` block with
/// nothing after it.
///
/// A `reasoning` item can only ever be *history* in the Responses `input`
/// array — the model hasn't produced this turn's reasoning yet when we're
/// building the request, so any reasoning item present is always a leftover
/// from an earlier, already-generated turn. When that earlier turn's stream
/// got interrupted (retry, model switch, client-side compaction) right after
/// the model started thinking but before it produced the accompanying
/// `function_call`/text, the trailing reasoning item ends up dangling with
/// nothing after it. Anthropic's Messages API rejects that outright with
/// `The final block in an assistant message cannot be thinking`, distinct
/// from (and not covered by) the "must replay byte-for-byte" rule that the
/// boundary-based stripping above already guards against. Unlike the
/// boundary-based stripping, this check is independent of where the last
/// user message is — a dangling trailing reasoning item is never valid to
/// resend regardless of position, and by definition it can't be part of a
/// still-open tool-call loop (an active loop would have a subsequent
/// `function_call`/`function_call_output` item after it).
fn strip_dangling_trailing_reasoning_item(items: &mut Vec<Value>) -> usize {
    let mut removed = 0usize;
    while items
        .last()
        .and_then(|item| item.get("type"))
        .and_then(Value::as_str)
        == Some("reasoning")
    {
        items.pop();
        removed += 1;
    }
    removed
}

/// Rewrite Codex-internal history items into standard Responses items
/// before sending to a third-party Responses upstream.
///
/// Codex sessions can contain item shapes that only Codex itself (and
/// OpenAI's native backend) understands; strict third-party Responses
/// implementations reject the whole request with an HTTP 422
/// (`data did not match any variant of untagged enum ModelInput`, observed
/// on grok's gateway) when any of them appears in `input`:
///
/// - `custom_tool_call` / `custom_tool_call_output` (freeform tools such as
///   `apply_patch`): converted to a `function_call` /
///   `function_call_output` pair with the raw input wrapped as a JSON
///   `{"input": ...}` arguments string — the universally accepted shape.
/// - `agent_message` (multi-agent collaboration traffic): converted to a
///   plain user `message`, keeping its text parts.
/// - `encrypted_content` content parts inside any message: dropped; they
///   are opaque to every upstream except the one that minted them.
///
/// These are always *history* items being replayed — converting them keeps
/// the information the model actually needs (what was called, what came
/// back, what was said) while restoring a schema every upstream accepts.
fn normalize_codex_internal_items_for_upstream(items: &mut Vec<Value>) -> usize {
    let mut normalized = 0usize;
    let mut index = 0usize;
    while index < items.len() {
        let item = &mut items[index];
        match item.get("type").and_then(Value::as_str) {
            Some("custom_tool_call") => {
                let name = item
                    .get("name")
                    .and_then(Value::as_str)
                    .unwrap_or("custom_tool")
                    .to_string();
                let input_text = response_output_text(
                    item.get("input")
                        .or_else(|| item.get("arguments"))
                        .unwrap_or(&Value::Null),
                );
                let call_id = item
                    .get("call_id")
                    .or_else(|| item.get("id"))
                    .and_then(Value::as_str)
                    .unwrap_or("")
                    .to_string();
                *item = json!({
                    "type": "function_call",
                    "name": name,
                    "arguments": json!({ "input": input_text }).to_string(),
                    "call_id": call_id,
                });
                normalized += 1;
            }
            Some("custom_tool_call_output") => {
                let call_id = item
                    .get("call_id")
                    .and_then(Value::as_str)
                    .unwrap_or("")
                    .to_string();
                let output = item.get("output").cloned().unwrap_or(Value::Null);
                *item = json!({
                    "type": "function_call_output",
                    "call_id": call_id,
                    "output": response_output_text(&output),
                });
                normalized += 1;
            }
            Some("agent_message") => {
                let parts: Vec<Value> = item
                    .get("content")
                    .and_then(Value::as_array)
                    .map(|parts| {
                        parts
                            .iter()
                            .filter(|part| {
                                part.get("type").and_then(Value::as_str)
                                    != Some("encrypted_content")
                            })
                            .cloned()
                            .collect()
                    })
                    .unwrap_or_default();
                if parts.is_empty() {
                    items.remove(index);
                    normalized += 1;
                    continue;
                }
                *item = json!({
                    "type": "message",
                    "role": "user",
                    "content": parts,
                });
                normalized += 1;
            }
            _ => {
                if let Some(parts) = item.get_mut("content").and_then(Value::as_array_mut) {
                    let before = parts.len();
                    parts.retain(|part| {
                        part.get("type").and_then(Value::as_str) != Some("encrypted_content")
                    });
                    if parts.len() != before {
                        normalized += 1;
                    }
                }
            }
        }
        index += 1;
    }
    normalized
}

/// Apply context budget trimming directly on a Responses API request body.
pub fn apply_responses_context_budget(
    body: &mut Value,
    budget: &ContextBudgetConfig,
) -> ContextBudgetReport {
    let mut report = ContextBudgetReport::default();
    if let Some(items) = body.get_mut("input").and_then(Value::as_array_mut) {
        report.stale_reasoning_items_stripped = strip_stale_reasoning_items(items);
    }

    if budget.is_unlimited() {
        return report;
    }
    let image_est = budget.image_token_estimate.max(1);

    let instructions_tokens = body
        .get("instructions")
        .map(|v| estimate_text_tokens(&instruction_text(v)))
        .unwrap_or(0);
    // Tool definitions count against the upstream context too; fold them
    // into the fixed overhead so trimming doesn't stop while the request is
    // still over the real budget.
    let tools_tokens = body
        .get("tools")
        .and_then(Value::as_array)
        .map(|tools| estimate_text_tokens(&serde_json::to_string(tools).unwrap_or_default()))
        .unwrap_or(0);
    let fixed_tokens = instructions_tokens + tools_tokens;

    let input_is_string = body.get("input").and_then(Value::as_str).is_some();
    if input_is_string {
        let text_tokens = body
            .get("input")
            .and_then(Value::as_str)
            .map(estimate_text_tokens)
            .unwrap_or(0);
        report.original_estimated_tokens = fixed_tokens + text_tokens;
        report.final_estimated_tokens = fixed_tokens + text_tokens;
        return report;
    }

    let Some(items) = body.get("input").and_then(Value::as_array) else {
        return report;
    };

    let item_tokens: Vec<u64> = items
        .iter()
        .map(|item| estimate_responses_item_tokens(item, image_est))
        .collect();
    let original = fixed_tokens + item_tokens.iter().sum::<u64>();

    if original <= budget.max_input_tokens {
        report.original_estimated_tokens = original;
        report.final_estimated_tokens = original;
        return report;
    }

    let items = body.get_mut("input").and_then(Value::as_array_mut).unwrap();
    report.original_estimated_tokens = original;
    report.was_trimmed = true;

    // Phase 1: Strip images from older items (keep last 2 user items' images).
    let mut recent_user_seen = 0usize;
    let mut phase1_boundary = 0usize;
    for i in (0..items.len()).rev() {
        if items[i].get("role").and_then(Value::as_str) == Some("user") {
            recent_user_seen += 1;
            if recent_user_seen >= 2 {
                phase1_boundary = i;
                break;
            }
        }
    }
    for i in 0..phase1_boundary {
        report.images_stripped += strip_images_from_responses_item(&mut items[i]);
    }

    let current: u64 = fixed_tokens
        + items
            .iter()
            .map(|i| estimate_responses_item_tokens(i, image_est))
            .sum::<u64>();
    if current <= budget.max_input_tokens {
        report.final_estimated_tokens = current;
        return report;
    }

    // Phase 2: Strip ALL images except the last user item.
    let last_user_idx = items
        .iter()
        .rposition(|i| i.get("role").and_then(Value::as_str) == Some("user"));
    for i in 0..items.len() {
        if Some(i) == last_user_idx {
            continue;
        }
        report.images_stripped += strip_images_from_responses_item(&mut items[i]);
    }

    let current: u64 = fixed_tokens
        + items
            .iter()
            .map(|i| estimate_responses_item_tokens(i, image_est))
            .sum::<u64>();
    if current <= budget.max_input_tokens {
        report.final_estimated_tokens = current;
        return report;
    }

    // Phase 3: Remove old items, keeping recent turns. The leading
    // instruction messages (permissions / AGENTS.md / environment context)
    // are pinned: dropping them silently strips standing rules such as the
    // `update_plan` task-list requirement.
    let pinned_end = leading_instruction_items_end(items);
    let soft_keep_from =
        find_responses_recent_boundary(items, CONTEXT_BUDGET_MIN_RECENT_TURNS).max(pinned_end);
    // The recent-turn count is a soft preference. If it cannot satisfy the
    // actual budget, continue dropping older completed turns until the
    // latest user message, which starts the active turn and is never
    // removed.
    let hard_keep_from = items
        .iter()
        .rposition(|item| item.get("role").and_then(Value::as_str) == Some("user"))
        .unwrap_or(items.len())
        .max(soft_keep_from);
    if hard_keep_from > pinned_end {
        let mut running = fixed_tokens
            + items
                .iter()
                .map(|i| estimate_responses_item_tokens(i, image_est))
                .sum::<u64>();
        let mut remove_up_to = pinned_end;
        for i in pinned_end..hard_keep_from {
            let removed = i - pinned_end;
            let marker_tokens = if removed == 0 {
                0
            } else {
                estimate_responses_item_tokens(
                    &json!({
                        "role": "user",
                        "content": format!(
                            "[Earlier conversation ({removed} items) trimmed to fit context budget]"
                        )
                    }),
                    image_est,
                )
            };
            if running.saturating_add(marker_tokens) <= budget.max_input_tokens {
                break;
            }
            running -= estimate_responses_item_tokens(&items[i], image_est);
            remove_up_to = i + 1;
        }
        if remove_up_to > pinned_end {
            let removed = remove_up_to - pinned_end;
            report.messages_removed = removed;
            let marker = json!({
                "role": "user",
                "content": format!(
                    "[Earlier conversation ({removed} items) trimmed to fit context budget]"
                )
            });
            items.drain(pinned_end..remove_up_to);
            items.insert(pinned_end, marker);
            remove_orphaned_responses_tool_outputs(items, &mut report);
        }
    }

    report.final_estimated_tokens = fixed_tokens
        + items
            .iter()
            .map(|i| estimate_responses_item_tokens(i, image_est))
            .sum::<u64>();
    report
}

/// After budget trimming drops a prefix of old items, the cut can land in
/// the middle of a tool exchange, leaving a `function_call_output` (or
/// `custom_tool_call_output`) whose originating `function_call` was removed.
/// Upstream providers reject such orphaned outputs, so drop any output item
/// whose `call_id` no longer has a matching call earlier in the array.
fn remove_orphaned_responses_tool_outputs(
    items: &mut Vec<Value>,
    report: &mut ContextBudgetReport,
) {
    let mut known_call_ids: BTreeSet<String> = BTreeSet::new();
    let mut index = 0usize;
    while index < items.len() {
        let item = &items[index];
        match item.get("type").and_then(Value::as_str) {
            Some("function_call") | Some("custom_tool_call") => {
                if let Some(id) = item
                    .get("call_id")
                    .or_else(|| item.get("id"))
                    .and_then(Value::as_str)
                {
                    if !id.is_empty() {
                        known_call_ids.insert(id.to_string());
                    }
                }
            }
            Some("function_call_output") | Some("custom_tool_call_output") => {
                let call_id = item.get("call_id").and_then(Value::as_str).unwrap_or("");
                if call_id.is_empty() || !known_call_ids.contains(call_id) {
                    items.remove(index);
                    report.messages_removed += 1;
                    continue;
                }
            }
            _ => {}
        }
        index += 1;
    }
}

// ===== Historical Input-Image Byte Budget =====

/// Byte-budget ceiling on how much historical `input_image` payload (bytes
/// of the `image_url` string, used as a stand-in for wire size) this proxy
/// will forward upstream in a single Responses `input` array.
///
/// This is independent of `ContextBudgetConfig`, which budgets by estimated
/// *token* count: Codex's own client-side context compaction never evicts
/// old images because it also reasons in tokens, and a flat per-image token
/// estimate is negligible next to the image's real multi-MB base64 size. So
/// once a screenshot (in a user message's `input_image` part) or a
/// `view_image` tool result (in a `function_call_output.output` array)
/// enters the conversation, it gets resent unchanged on every later turn,
/// forever.
///
/// Empirically: a real session carrying two 4.2MB base64 images (one of
/// each kind above) produced an 8.7MB request body and a
/// `400 invalid_request_error` from upstream; resending either image alone
/// (4.2MB) returned `200`. The true upstream body-size ceiling therefore
/// sits somewhere in (4.2MB, 8.7MB]; 4MB stays safely under the smallest
/// observed failure.
const INPUT_IMAGE_TOTAL_BUDGET_BYTES: usize = 4 * 1024 * 1024;

const INPUT_IMAGE_BUDGET_PLACEHOLDER_TEXT: &str =
    "[image removed by gateway: history image budget exceeded]";

/// Approximate an `input_image` part's wire size using its `image_url`
/// string length (a data URL for locally-attached images). The base64
/// payload dwarfs the rest of the part's JSON by orders of magnitude, so the
/// string length alone is good enough for budgeting purposes.
fn input_image_part_bytes(part: &Value) -> usize {
    let Some(image_url) = part.get("image_url") else {
        return 0;
    };
    if let Some(url) = image_url.as_str() {
        return url.len();
    }
    image_url
        .get("url")
        .and_then(Value::as_str)
        .map(str::len)
        .unwrap_or(0)
}

/// Cap the total byte size of historical `input_image` parts in a Responses
/// API request's `input` array to `INPUT_IMAGE_TOTAL_BUDGET_BYTES`, so a
/// conversation that has accumulated large images across many turns can't
/// blow the upstream request-body size limit (see that constant's doc for
/// the empirical basis). Returns the number of image parts replaced.
///
/// Scans two locations an `input_image` part can appear in:
/// - a message item's `content` array (e.g. a screenshot the user attached)
/// - a `function_call_output`/`custom_tool_call_output` item's `output`
///   array, when `output` is itself an array (e.g. the `view_image` tool)
///
/// Walks `input` newest-first (from the end of the array backwards),
/// keeping images while the cumulative byte budget allows it and replacing
/// every older image that doesn't fit with a small text placeholder. A
/// dropped image's bytes are never "refunded" to the budget, so one huge
/// image can't make room for itself by starving smaller, older images — if
/// nothing fits (including the single newest image), everything is
/// replaced. Only the image part itself is swapped for a placeholder; the
/// surrounding item, array shape, and any non-image part are left
/// untouched.
///
/// Runs unconditionally, ahead of (and independent of) the token-based
/// `apply_responses_context_budget`/`apply_context_budget`: those can leave
/// these images untouched entirely (a `max_input_tokens` of 0 means
/// unlimited, and even when enabled, a flat per-image token estimate rarely
/// reflects the real base64 size).
// (item_index, field the part's array lives under, part_index, bytes)
fn collect_input_image_slots(items: &[Value]) -> Vec<(usize, &'static str, usize, usize)> {
    let mut slots: Vec<(usize, &'static str, usize, usize)> = Vec::new();
    for (item_index, item) in items.iter().enumerate() {
        if let Some(parts) = item.get("content").and_then(Value::as_array) {
            for (part_index, part) in parts.iter().enumerate() {
                if part.get("type").and_then(Value::as_str) == Some("input_image") {
                    let bytes = input_image_part_bytes(part);
                    slots.push((item_index, "content", part_index, bytes));
                }
            }
        }
        let is_tool_output = matches!(
            item.get("type").and_then(Value::as_str),
            Some("function_call_output") | Some("custom_tool_call_output")
        );
        if is_tool_output {
            if let Some(parts) = item.get("output").and_then(Value::as_array) {
                for (part_index, part) in parts.iter().enumerate() {
                    if part.get("type").and_then(Value::as_str) == Some("input_image") {
                        let bytes = input_image_part_bytes(part);
                        slots.push((item_index, "output", part_index, bytes));
                    }
                }
            }
        }
    }
    slots
}

fn replace_image_slot_with_placeholder(
    items: &mut [Value],
    item_index: usize,
    field: &'static str,
    part_index: usize,
) {
    if let Some(target) = items[item_index]
        .get_mut(field)
        .and_then(Value::as_array_mut)
        .and_then(|parts| parts.get_mut(part_index))
    {
        *target = json!({
            "type": "input_text",
            "text": INPUT_IMAGE_BUDGET_PLACEHOLDER_TEXT
        });
    }
}

/// Replace every `input_image` part in the request with a text placeholder.
/// Last-resort fallback when the upstream rejects an image-bearing request
/// with HTTP 400 regardless of size (observed: an upstream content filter
/// 400s specific images while an equally-sized benign image sails through),
/// so the turn can still complete as text-only instead of dying on retry.
pub fn strip_all_input_images(body: &mut Value) -> usize {
    let Some(items) = body.get_mut("input").and_then(Value::as_array_mut) else {
        return 0;
    };
    let slots = collect_input_image_slots(items);
    for (item_index, field, part_index, _) in &slots {
        replace_image_slot_with_placeholder(items, *item_index, field, *part_index);
    }
    slots.len()
}

fn enforce_input_image_budget(body: &mut Value) -> usize {
    let Some(items) = body.get_mut("input").and_then(Value::as_array_mut) else {
        return 0;
    };

    let slots = collect_input_image_slots(items);
    if slots.is_empty() {
        return 0;
    }

    // Newest-first: keep images while they still fit the remaining budget,
    // queue every older one that doesn't for replacement.
    let mut budget_remaining = INPUT_IMAGE_TOTAL_BUDGET_BYTES;
    let mut to_replace: Vec<(usize, &'static str, usize)> = Vec::new();
    for (item_index, field, part_index, bytes) in slots.into_iter().rev() {
        if bytes <= budget_remaining {
            budget_remaining -= bytes;
        } else {
            to_replace.push((item_index, field, part_index));
        }
    }

    let replaced = to_replace.len();
    for (item_index, field, part_index) in to_replace {
        replace_image_slot_with_placeholder(items, item_index, field, part_index);
    }
    replaced
}

#[cfg(test)]
mod repeated_log_tests {
    use super::*;
    use std::time::Instant;

    #[test]
    fn render_log_line_wraps_error_body_in_red() {
        let rendered = render_log_line("[12:00:00] ", LogLevel::Error, "上游 500", true);
        assert!(rendered.contains("\x1b[31m上游 500\x1b[0m"));
        // 前缀本身单独用暗淡色包裹，和消息体的颜色互不干扰。
        assert!(rendered.starts_with("\x1b[2m[12:00:00] \x1b[0m"));
    }

    #[test]
    fn render_log_line_wraps_warn_body_in_yellow() {
        let rendered = render_log_line("[12:00:00] ", LogLevel::Warn, "重试中", true);
        assert!(rendered.contains("\x1b[33m重试中\x1b[0m"));
        assert!(rendered.starts_with("\x1b[2m[12:00:00] \x1b[0m"));
    }

    #[test]
    fn render_log_line_wraps_notice_body_in_cyan() {
        let rendered = render_log_line("[12:00:00] ", LogLevel::Notice, "已提醒模型更新 update_plan", true);
        assert!(rendered.contains("\x1b[36m已提醒模型更新 update_plan\x1b[0m"));
        assert!(rendered.starts_with("\x1b[2m[12:00:00] \x1b[0m"));
    }

    #[test]
    fn render_log_line_leaves_info_body_uncolored() {
        let rendered = render_log_line("[12:00:00] ", LogLevel::Info, "已裁剪", true);
        // Info 只有前缀带暗淡色，消息正文本身不额外包一层颜色码。
        assert!(rendered.starts_with("\x1b[2m[12:00:00] \x1b[0m"));
        assert!(rendered.ends_with("已裁剪\n\n"));
        assert!(!rendered.contains("\x1b[31m"));
        assert!(!rendered.contains("\x1b[33m"));
    }

    #[test]
    fn render_log_line_no_color_strips_every_ansi_escape() {
        let rendered = render_log_line("[12:00:00] ", LogLevel::Error, "上游 500", false);
        assert_eq!(rendered, "[12:00:00] 上游 500\n\n");
        assert!(!rendered.contains('\x1b'));
    }

    #[test]
    fn render_log_line_always_appends_blank_line_separator() {
        for (level, colors_enabled) in [
            (LogLevel::Error, true),
            (LogLevel::Warn, true),
            (LogLevel::Info, true),
            (LogLevel::Notice, true),
            (LogLevel::Error, false),
            (LogLevel::Warn, false),
            (LogLevel::Info, false),
            (LogLevel::Notice, false),
        ] {
            let rendered = render_log_line("[p] ", level, "msg", colors_enabled);
            assert!(
                rendered.ends_with("\n\n"),
                "level {level:?} (colors_enabled={colors_enabled}) should end with a blank-line \
                 separator so consecutive log lines have a blank line between them, got: {rendered:?}"
            );
        }
    }

    #[test]
    fn throttle_prints_first_occurrence_immediately() {
        let mut state: HashMap<&'static str, RepeatedLogState> = HashMap::new();
        let now = Instant::now();
        assert_eq!(
            throttle_repeated_log_line(
                &mut state,
                "key",
                "hello".to_string(),
                now,
                Duration::from_secs(3)
            ),
            Some("hello".to_string())
        );
    }

    #[test]
    fn throttle_suppresses_bursts_within_the_window_even_if_wording_changes() {
        // Simulates two interleaved conversations retrying against a
        // struggling upstream: the reasoning-strip counts differ (111 vs
        // 75) between attempts, but they're really the same underlying
        // "upstream is unhappy" situation and should collapse together.
        let mut state: HashMap<&'static str, RepeatedLogState> = HashMap::new();
        let throttle = Duration::from_secs(3);
        let t0 = Instant::now();

        assert!(
            throttle_repeated_log_line(
                &mut state,
                "key",
                "cleaned 111 items".to_string(),
                t0,
                throttle
            )
            .is_some()
        );
        assert_eq!(
            throttle_repeated_log_line(
                &mut state,
                "key",
                "cleaned 75 items".to_string(),
                t0 + Duration::from_millis(200),
                throttle
            ),
            None
        );
        assert_eq!(
            throttle_repeated_log_line(
                &mut state,
                "key",
                "cleaned 111 items".to_string(),
                t0 + Duration::from_millis(400),
                throttle
            ),
            None
        );
    }

    #[test]
    fn throttle_prints_again_after_window_elapses_with_suppressed_count() {
        let mut state: HashMap<&'static str, RepeatedLogState> = HashMap::new();
        let throttle = Duration::from_secs(3);
        let t0 = Instant::now();

        assert!(
            throttle_repeated_log_line(&mut state, "key", "msg".to_string(), t0, throttle)
                .is_some()
        );
        assert_eq!(
            throttle_repeated_log_line(
                &mut state,
                "key",
                "msg".to_string(),
                t0 + Duration::from_millis(500),
                throttle
            ),
            None
        );
        assert_eq!(
            throttle_repeated_log_line(
                &mut state,
                "key",
                "msg".to_string(),
                t0 + Duration::from_secs(1),
                throttle
            ),
            None
        );

        let after_window = throttle_repeated_log_line(
            &mut state,
            "key",
            "msg".to_string(),
            t0 + Duration::from_secs(4),
            throttle,
        );
        let line = after_window.expect("should print once the throttle window elapses");
        assert!(line.contains("又出现了 2 次"));
    }

    #[test]
    fn throttle_keeps_independent_windows_per_key() {
        let mut state: HashMap<&'static str, RepeatedLogState> = HashMap::new();
        let throttle = Duration::from_secs(3);
        let t0 = Instant::now();
        assert!(
            throttle_repeated_log_line(&mut state, "key_a", "msg".to_string(), t0, throttle)
                .is_some()
        );
        // A different key prints immediately too, independent of key_a's window.
        assert!(
            throttle_repeated_log_line(&mut state, "key_b", "msg".to_string(), t0, throttle)
                .is_some()
        );
    }

    #[test]
    fn upstream_final_retry_bypasses_proxy_only_when_proxy_configured() {
        // 无代理环境：任何一跳都不该切直连（本来就是直连）。
        assert!(!upstream_attempt_should_bypass_proxy(1, false));
        assert!(!upstream_attempt_should_bypass_proxy(3, false));
        assert_eq!(upstream_5xx_max_attempts(false), 3); // 1 首发 + 2 重试

        // 有代理环境：前几跳保持用户配置的代理路径，只有最后一跳换直连。
        assert!(!upstream_attempt_should_bypass_proxy(1, true));
        assert!(!upstream_attempt_should_bypass_proxy(2, true));
        assert!(!upstream_attempt_should_bypass_proxy(3, true));
        assert!(upstream_attempt_should_bypass_proxy(4, true));
        assert_eq!(upstream_5xx_max_attempts(true), 4); // 额外一次直连兜底
    }

    #[test]
    fn only_first_attempt_on_normal_path_uses_pooled_client() {
        // 首发且走正常代理路径：用池化连接省掉每轮握手。
        assert!(upstream_attempt_uses_pooled_client(1, false, false));
        // 5xx 重试：必须换全新连接（保持「换新连接自动重试」语义）。
        assert!(!upstream_attempt_uses_pooled_client(2, false, false));
        assert!(!upstream_attempt_uses_pooled_client(3, false, false));
        // 绕过代理的直连兜底跳：不能复用走代理建立的池化连接。
        assert!(!upstream_attempt_uses_pooled_client(4, true, false));
        // 池化发送已经失败过一次：本请求内不再回到池化路径。
        assert!(!upstream_attempt_uses_pooled_client(1, false, true));
    }

    #[test]
    fn pooled_send_retry_covers_connection_errors_but_not_header_timeouts() {
        // 响应头等待超时走 tokio::time::timeout，错误链里没有 reqwest 错误，
        // 不应触发池化重发（否则一次 120s 的等待会翻倍）。
        let timeout_error =
            anyhow::anyhow!("deadline has elapsed").context("上游请求超过 120 秒未返回响应头");
        assert!(!pooled_send_error_is_retryable(&timeout_error));
    }

    #[test]
    fn upstream_5xx_statuses_are_retryable_but_client_errors_are_not() {
        assert!(upstream_status_is_retryable(502));
        assert!(upstream_status_is_retryable(503));
        assert!(upstream_status_is_retryable(504));
        // 4xx（含限流 429）和 500/501 语义不同：origin 已经收到请求并给出
        // 明确答复，自动换连接重试没有意义，交回 Codex 侧处理。
        assert!(!upstream_status_is_retryable(400));
        assert!(!upstream_status_is_retryable(401));
        assert!(!upstream_status_is_retryable(429));
        assert!(!upstream_status_is_retryable(500));
        assert!(!upstream_status_is_retryable(200));
    }

    #[test]
    fn upstream_layer_label_identifies_cloudflare_edge() {
        assert_eq!(
            upstream_layer_label("cloudflare", "a19f58974c8e0a4d-AMS").as_deref(),
            Some("Cloudflare 边缘网关, cf-ray: a19f58974c8e0a4d-AMS")
        );
        assert_eq!(
            upstream_layer_label("cloudflare", "").as_deref(),
            Some("Cloudflare 边缘网关")
        );
        assert_eq!(
            upstream_layer_label("nginx/1.24", "").as_deref(),
            Some("server: nginx/1.24")
        );
        assert_eq!(upstream_layer_label("", ""), None);
    }

    #[test]
    fn upstream_error_attribution_explains_edge_502_and_retries() {
        let text = upstream_error_attribution("cloudflare", "ray-1", 3, 502);
        assert!(text.contains("Cloudflare 边缘网关"));
        assert!(text.contains("cf-ray: ray-1"));
        assert!(text.contains("自动重试 2 次"));
        assert!(text.contains("源站可能未收到这笔请求"));

        // 单次成功路径（非 5xx、无 server 头）不追加任何后缀。
        assert_eq!(upstream_error_attribution("", "", 1, 400), "");

        // 非 Cloudflare 的 502 不下“源站没收到”的判断，只报响应方与重试次数。
        let nginx = upstream_error_attribution("nginx", "", 2, 502);
        assert!(nginx.contains("server: nginx"));
        assert!(nginx.contains("自动重试 1 次"));
        assert!(!nginx.contains("源站可能未收到"));
    }
}

#[cfg(test)]
mod grok_review_fix_tests {
    use super::*;

    fn test_relay(
        protocol: RelayProtocol,
        context_budget: ContextBudgetConfig,
    ) -> ProtocolProxyUpstream {
        ProtocolProxyUpstream {
            id: "test".to_string(),
            name: "test".to_string(),
            base_url: "https://example.invalid".to_string(),
            api_key: "key".to_string(),
            protocol,
            user_agent: String::new(),
            context_budget,
            plan_hints: false,
            model_routes: BTreeMap::new(),
        }
    }

    #[test]
    fn responses_relay_normalizes_codex_internal_items_for_strict_upstreams() {
        // grok's Responses gateway 422s (`untagged enum ModelInput`) on any
        // Codex-internal history item. The Responses relay path must rewrite
        // them into standard items before sending upstream.
        let request_json = json!({
            "model": "grok-4.5",
            "input": [
                { "type": "message", "role": "user", "content": [
                    { "type": "input_text", "text": "hi" },
                    { "type": "encrypted_content", "data": "AAAA" }
                ] },
                { "type": "custom_tool_call", "name": "apply_patch",
                  "input": "*** Begin Patch\n*** End Patch", "call_id": "call_1" },
                { "type": "custom_tool_call_output", "call_id": "call_1", "output": "ok" },
                { "type": "agent_message", "author": "/root", "recipient": "/root/sub",
                  "content": [
                      { "type": "input_text", "text": "Message Type: NEW_TASK" },
                      { "type": "encrypted_content", "data": "BBBB" }
                  ] }
            ]
        });
        let relay = test_relay(RelayProtocol::Responses, ContextBudgetConfig::default());
        let (_, body, wire_api, _) = upstream_request_parts(&relay, request_json).unwrap();
        assert_eq!(wire_api, UpstreamWireApi::Responses);
        let items = body["input"].as_array().unwrap();
        let types: Vec<&str> = items
            .iter()
            .map(|item| item["type"].as_str().unwrap())
            .collect();
        assert_eq!(
            types,
            vec![
                "message",
                "function_call",
                "function_call_output",
                "message"
            ],
            "internal items must be rewritten to standard ones: {items:?}"
        );
        let serialized = serde_json::to_string(&body).unwrap();
        assert!(
            !serialized.contains("encrypted_content"),
            "encrypted_content parts must be stripped: {serialized}"
        );
        // Rewritten function_call keeps the tool linkage.
        assert_eq!(items[1]["name"], "apply_patch");
        assert_eq!(items[1]["call_id"], "call_1");
        assert_eq!(items[2]["call_id"], "call_1");
        // agent_message becomes a plain user message with its text kept.
        assert_eq!(items[3]["role"], "user");
        assert!(
            serde_json::to_string(&items[3])
                .unwrap()
                .contains("NEW_TASK")
        );
    }

    #[test]
    fn responses_relay_drops_agent_message_with_only_encrypted_content() {
        let request_json = json!({
            "model": "grok-4.5",
            "input": [
                { "type": "message", "role": "user", "content": [
                    { "type": "input_text", "text": "hi" }
                ] },
                { "type": "agent_message", "author": "/root", "recipient": "/root/sub",
                  "content": [ { "type": "encrypted_content", "data": "BBBB" } ] }
            ]
        });
        let relay = test_relay(RelayProtocol::Responses, ContextBudgetConfig::default());
        let (_, body, _, _) = upstream_request_parts(&relay, request_json).unwrap();
        let items = body["input"].as_array().unwrap();
        assert_eq!(
            items.len(),
            1,
            "empty agent_message must be dropped: {items:?}"
        );
        assert_eq!(items[0]["type"], "message");
    }

    #[test]
    fn chat_completions_relay_history_still_handles_custom_tool_call_after_normalize() {
        // The Chat Completions relay path already downgraded custom tool
        // calls during conversion; make sure the new Responses-path
        // normalization doesn't interfere with that behavior.
        let request_json = json!({
            "model": "test-model",
            "input": [
                { "role": "user", "content": "hi" },
                { "type": "custom_tool_call", "name": "apply_patch",
                  "input": "*** Begin Patch\n*** End Patch", "call_id": "call_7" },
                { "type": "custom_tool_call_output", "call_id": "call_7", "output": "Done" }
            ]
        });
        let relay = test_relay(
            RelayProtocol::ChatCompletions,
            ContextBudgetConfig::default(),
        );
        let (_, chat_body, wire_api, _) = upstream_request_parts(&relay, request_json).unwrap();
        assert_eq!(wire_api, UpstreamWireApi::ChatCompletions);
        let messages = chat_body["messages"].as_array().unwrap();
        let has_tool_call = messages.iter().any(|message| {
            message
                .get("tool_calls")
                .and_then(Value::as_array)
                .is_some_and(|calls| !calls.is_empty())
        });
        let has_tool_result = messages
            .iter()
            .any(|message| message.get("role").and_then(Value::as_str) == Some("tool"));
        assert!(
            has_tool_call && has_tool_result,
            "custom tool history must survive on the chat path: {messages:?}"
        );
    }

    #[test]
    fn chat_completions_relay_strips_stale_reasoning_items_too() {
        // Before the fix, `strip_stale_reasoning_items` only ran on the
        // Responses relay path; a stale `reasoning` item from a completed
        // turn would survive conversion into `reasoning_content` on a Chat
        // Completions message and get sent upstream, risking the same
        // "thinking blocks cannot be modified" rejection the Responses path
        // already guards against.
        let request_json = json!({
            "model": "test-model",
            "input": [
                { "role": "user", "content": "old question" },
                {
                    "type": "reasoning",
                    "id": "reasoning_stale",
                    "summary": [{ "type": "summary_text", "text": "stale thinking" }]
                },
                { "role": "assistant", "content": "old answer" },
                { "role": "user", "content": "new question after model switch" }
            ]
        });
        let relay = test_relay(
            RelayProtocol::ChatCompletions,
            ContextBudgetConfig::default(),
        );
        let (_, chat_body, wire_api, _) = upstream_request_parts(&relay, request_json).unwrap();
        assert_eq!(wire_api, UpstreamWireApi::ChatCompletions);
        let messages = chat_body["messages"].as_array().unwrap();
        let carries_stale_reasoning = messages.iter().any(|message| {
            message
                .get("reasoning_content")
                .and_then(Value::as_str)
                .map(|text| text.contains("stale thinking"))
                .unwrap_or(false)
        });
        assert!(
            !carries_stale_reasoning,
            "stale reasoning from a completed turn must not reach the Chat Completions body: {chat_body:?}"
        );
    }

    #[test]
    fn is_native_openai_model_matches_openai_native_prefixes_case_insensitively() {
        assert!(is_native_openai_model("gpt-5.1-codex"));
        assert!(is_native_openai_model("GPT-5.1-Codex"));
        assert!(is_native_openai_model("gpt-4o-mini"));
        assert!(is_native_openai_model("gpt-image-1"));
        assert!(is_native_openai_model("o3"));
        assert!(is_native_openai_model("o4-mini"));
        assert!(is_native_openai_model("text-embedding-3-large"));
        assert!(is_native_openai_model("dall-e-3"));
        assert!(!is_native_openai_model("grok-4.5"));
        assert!(!is_native_openai_model("claude-sonnet-5"));
        assert!(!is_native_openai_model("glm-4.7"));
        // Prefix match only — a namespaced third-party model that merely
        // contains "gpt" elsewhere in its id must not false-positive.
        assert!(!is_native_openai_model("openrouter/gpt-oss-120b"));
    }

    #[test]
    fn responses_relay_passes_native_openai_models_through_untouched() {
        // Native GPT traffic goes straight to OpenAI's own Responses
        // backend, which already understands Codex-internal item shapes and
        // never rejects stale reasoning items — normalizing/stripping them
        // anyway (as done for third-party CPA-translated upstreams below)
        // only adds needless rewrite/retry churn. See
        // `is_native_openai_model`.
        let request_json = json!({
            "model": "gpt-5.1-codex",
            "input": [
                { "role": "user", "content": "old question" },
                {
                    "type": "reasoning",
                    "id": "reasoning_stale",
                    "summary": [{ "type": "summary_text", "text": "stale thinking" }]
                },
                { "role": "assistant", "content": "old answer" },
                { "type": "message", "role": "user", "content": [
                    { "type": "input_text", "text": "hi" },
                    { "type": "encrypted_content", "data": "AAAA" }
                ] },
                { "type": "custom_tool_call", "name": "apply_patch",
                  "input": "*** Begin Patch\n*** End Patch", "call_id": "call_1" },
                { "type": "custom_tool_call_output", "call_id": "call_1", "output": "ok" }
            ]
        });
        let relay = test_relay(RelayProtocol::Responses, ContextBudgetConfig::default());
        let (_, body, wire_api, _) = upstream_request_parts(&relay, request_json.clone()).unwrap();
        assert_eq!(wire_api, UpstreamWireApi::Responses);
        assert_eq!(
            body, request_json,
            "a native OpenAI model's Responses body must pass through byte-for-byte: {body:?}"
        );
    }

    #[test]
    fn chat_completions_relay_skips_stale_reasoning_strip_for_native_openai_models() {
        // Same fixture as `chat_completions_relay_strips_stale_reasoning_items_too`
        // above, but for a native GPT model: the wire-format conversion to
        // Chat Completions still has to happen, just without the
        // third-party-upstream stale-reasoning cleanup.
        let request_json = json!({
            "model": "gpt-5.1-codex",
            "input": [
                { "role": "user", "content": "old question" },
                {
                    "type": "reasoning",
                    "id": "reasoning_stale",
                    "summary": [{ "type": "summary_text", "text": "stale thinking" }]
                },
                { "role": "assistant", "content": "old answer" },
                { "role": "user", "content": "new question after model switch" }
            ]
        });
        let relay = test_relay(
            RelayProtocol::ChatCompletions,
            ContextBudgetConfig::default(),
        );
        let (_, chat_body, wire_api, _) = upstream_request_parts(&relay, request_json).unwrap();
        assert_eq!(wire_api, UpstreamWireApi::ChatCompletions);
        let messages = chat_body["messages"].as_array().unwrap();
        let carries_stale_reasoning = messages.iter().any(|message| {
            message
                .get("reasoning_content")
                .and_then(Value::as_str)
                .map(|text| text.contains("stale thinking"))
                .unwrap_or(false)
        });
        assert!(
            carries_stale_reasoning,
            "native OpenAI models must skip stale-reasoning stripping while still converting the wire format: {chat_body:?}"
        );
    }

    #[test]
    fn native_openai_model_skips_input_image_budget_enforcement() {
        // Before the fix, `enforce_input_image_budget` ran unconditionally
        // ahead of the native-OpenAI early return in `upstream_request_parts`,
        // so an over-budget history image would still get silently rewritten
        // to a placeholder even for GPT models that don't need any of this
        // third-party-upstream handling.
        let big_image = format!("data:image/png;base64,{}", "A".repeat(5 * 1024 * 1024));
        let request_json = json!({
            "model": "gpt-5.1-codex",
            "input": [
                { "type": "message", "role": "user", "content": [
                    { "type": "input_image", "image_url": big_image }
                ] }
            ]
        });
        let relay = test_relay(RelayProtocol::Responses, ContextBudgetConfig::default());
        let (_, body, _, _) =
            upstream_request_parts(&relay, request_json.clone()).unwrap();
        assert_eq!(
            body, request_json,
            "native OpenAI model's over-budget image must survive the image-budget pass untouched: {body:?}"
        );
    }

    #[test]
    fn third_party_model_still_enforces_input_image_budget() {
        let big_image = format!("data:image/png;base64,{}", "A".repeat(5 * 1024 * 1024));
        let request_json = json!({
            "model": "grok-4.5",
            "input": [
                { "type": "message", "role": "user", "content": [
                    { "type": "input_image", "image_url": big_image.clone() }
                ] }
            ]
        });
        let relay = test_relay(RelayProtocol::Responses, ContextBudgetConfig::default());
        let (_, body, _, _) = upstream_request_parts(&relay, request_json).unwrap();
        let serialized = body.to_string();
        assert!(
            !serialized.contains(&big_image),
            "an over-budget image for a third-party model must still be replaced with a placeholder: {body:?}"
        );
    }

    #[test]
    fn chat_completions_direct_context_budget_skips_native_openai_but_trims_others() {
        // Covers `apply_chat_completions_direct_context_budget`, called from
        // `open_chat_completions_proxy_request` (the direct
        // `/v1/chat/completions` entry point for non-Codex callers) — a
        // separate code path from `upstream_request_parts` above that an
        // earlier native-OpenAI passthrough pass missed.
        fn body_with_three_user_turns(model: &str) -> Value {
            let image = json!({
                "type": "image_url",
                "image_url": { "url": "data:image/png;base64,AAAA" }
            });
            json!({
                "model": model,
                "messages": [
                    { "role": "user", "content": [image.clone()] },
                    { "role": "user", "content": [image] },
                    { "role": "user", "content": "final question" },
                ]
            })
        }
        let budget = ContextBudgetConfig::with_max_tokens(10);

        let mut native_body = body_with_three_user_turns("gpt-5.1-codex");
        let original_native = native_body.clone();
        apply_chat_completions_direct_context_budget(
            &mut native_body,
            Some("gpt-5.1-codex"),
            &budget,
        );
        assert_eq!(
            native_body, original_native,
            "native OpenAI model's Chat Completions direct body must pass through untouched: {native_body:?}"
        );

        let mut third_party_body = body_with_three_user_turns("grok-4.5");
        apply_chat_completions_direct_context_budget(
            &mut third_party_body,
            Some("grok-4.5"),
            &budget,
        );
        let serialized = third_party_body.to_string();
        assert!(
            !serialized.contains("data:image/png;base64,AAAA"),
            "third-party model must still get budget trimming: {third_party_body:?}"
        );
    }

    #[test]
    fn flush_tool_calls_merge_attaches_pending_reasoning_instead_of_dropping_it() {
        // Before the fix, merging tool_calls into an already-pushed
        // assistant message returned early without touching
        // `pending_reasoning`, so reasoning that arrived between an
        // assistant text turn and its follow-up tool call either got
        // attached to the wrong message or spilled into a spurious
        // trailing assistant message with empty content.
        let body = json!({
            "model": "test-model",
            "input": [
                {
                    "type": "message",
                    "role": "assistant",
                    "content": [{ "type": "output_text", "text": "earlier answer" }]
                },
                {
                    "type": "reasoning",
                    "summary": [{ "type": "summary_text", "text": "in-between thinking" }]
                },
                {
                    "type": "function_call",
                    "call_id": "call_1",
                    "name": "do_thing",
                    "arguments": "{}"
                },
                {
                    "type": "function_call_output",
                    "call_id": "call_1",
                    "output": "result"
                }
            ]
        });
        let chat = responses_to_chat_completions(body).expect("converts");
        let messages = chat["messages"].as_array().expect("messages array");
        let assistant_messages: Vec<&Value> = messages
            .iter()
            .filter(|m| m.get("role").and_then(Value::as_str) == Some("assistant"))
            .collect();
        assert_eq!(
            assistant_messages.len(),
            1,
            "reasoning must not spill into a second spurious assistant message: {messages:?}"
        );
        let assistant = assistant_messages[0];
        assert_eq!(
            assistant.get("reasoning_content").and_then(Value::as_str),
            Some("in-between thinking")
        );
        assert!(assistant.get("tool_calls").is_some());
    }

    #[test]
    fn responses_passthrough_failure_sse_emits_response_failed_event() {
        // Before the fix, an idle-timeout/interruption on the pure Responses
        // passthrough streaming path just broke the loop with no SSE frame
        // at all, leaving Codex to see the chunked stream end abruptly
        // instead of a legible terminal failure.
        let frame = responses_passthrough_failure_sse("boom", "upstream_timeout");
        let text = String::from_utf8(frame).expect("utf8");
        assert!(text.starts_with("event: response.failed\n"));
        let data_line = text
            .lines()
            .find(|line| line.starts_with("data: "))
            .expect("data line present");
        let payload: Value = serde_json::from_str(&data_line["data: ".len()..]).unwrap();
        assert_eq!(payload["type"], json!("response.failed"));
        assert_eq!(payload["response"]["status"], json!("failed"));
        assert_eq!(payload["response"]["error"]["message"], json!("boom"));
        assert_eq!(
            payload["response"]["error"]["type"],
            json!("upstream_timeout")
        );
    }

    #[test]
    fn warn_if_still_over_budget_is_noop_when_within_budget_or_unlimited() {
        let report = ContextBudgetReport {
            final_estimated_tokens: 100,
            ..Default::default()
        };
        // Should not panic; max_input_tokens == 0 means unlimited.
        warn_if_still_over_budget("test_event_unlimited", &report, 0);
        // Within budget: also should not panic (and has nothing to assert
        // externally since this only logs to stderr).
        warn_if_still_over_budget("test_event_within_budget", &report, 200);
    }

    #[test]
    fn chat_budget_accounts_for_trim_marker_tokens() {
        let mut body = json!({
            "model": "test-model",
            "messages": [
                { "role": "system", "content": "Keep these instructions." },
                { "role": "user", "content": "old user xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx" },
                { "role": "assistant", "content": "old answer yyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyy" },
                { "role": "user", "content": "recent user one" },
                { "role": "assistant", "content": "recent answer one" },
                { "role": "user", "content": "recent user two" },
                { "role": "assistant", "content": "recent answer two" },
                { "role": "user", "content": "recent user three" }
            ]
        });
        let messages = body["messages"].as_array().unwrap();
        let original = estimate_request_tokens(&body, ESTIMATE_IMAGE_DEFAULT_TOKENS);
        let first_removable = estimate_message_tokens(&messages[1], ESTIMATE_IMAGE_DEFAULT_TOKENS);
        let budget = ContextBudgetConfig {
            max_input_tokens: original - first_removable,
            image_token_estimate: ESTIMATE_IMAGE_DEFAULT_TOKENS,
        };

        let report = apply_context_budget(&mut body, &budget);

        assert!(report.was_trimmed);
        assert!(
            report.messages_removed >= 2,
            "the marker must force one more old message out: {body}"
        );
        assert!(
            report.final_estimated_tokens <= budget.max_input_tokens,
            "trim marker must be included in the final budget: {report:?}"
        );
    }

    #[test]
    fn responses_budget_accounts_for_trim_marker_tokens() {
        let mut body = json!({
            "model": "test-model",
            "input": [
                { "type": "message", "role": "system", "content": "Keep these instructions." },
                { "type": "message", "role": "user", "content": "old user xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx" },
                { "type": "message", "role": "assistant", "content": "old answer yyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyy" },
                { "type": "message", "role": "user", "content": "recent user one" },
                { "type": "message", "role": "assistant", "content": "recent answer one" },
                { "type": "message", "role": "user", "content": "recent user two" },
                { "type": "message", "role": "assistant", "content": "recent answer two" },
                { "type": "message", "role": "user", "content": "recent user three" }
            ]
        });
        let items = body["input"].as_array().unwrap();
        let original = items
            .iter()
            .map(|item| estimate_responses_item_tokens(item, ESTIMATE_IMAGE_DEFAULT_TOKENS))
            .sum::<u64>();
        let first_removable =
            estimate_responses_item_tokens(&items[1], ESTIMATE_IMAGE_DEFAULT_TOKENS);
        let budget = ContextBudgetConfig {
            max_input_tokens: original - first_removable,
            image_token_estimate: ESTIMATE_IMAGE_DEFAULT_TOKENS,
        };

        let report = apply_responses_context_budget(&mut body, &budget);

        assert!(report.was_trimmed);
        assert!(
            report.messages_removed >= 2,
            "the marker must force one more old item out: {body}"
        );
        assert!(
            report.final_estimated_tokens <= budget.max_input_tokens,
            "trim marker must be included in the final budget: {report:?}"
        );
    }

    #[test]
    fn chat_sse_ignores_chunks_after_done() {
        let mut converter = ChatSseToResponsesConverter::default();
        let before = converter.push_bytes(
            b"data: {\"id\":\"chatcmpl-1\",\"choices\":[{\"delta\":{\"content\":\"hi\"}}]}\n\ndata: [DONE]\n\n",
        );
        let before_text = String::from_utf8(before).unwrap();
        assert!(before_text.contains("response.completed"));
        assert!(before_text.contains("data: [DONE]"));

        // Some gateways keep sending usage/keep-alive chunks after [DONE];
        // nothing may be appended to the already-finalized stream.
        let after = converter.push_bytes(
            b"data: {\"id\":\"chatcmpl-1\",\"choices\":[{\"delta\":{\"content\":\"late\"}}],\"usage\":{\"prompt_tokens\":1}}\n\n",
        );
        assert!(
            after.is_empty(),
            "no events may follow response.completed / [DONE], got: {}",
            String::from_utf8_lossy(&after)
        );
        let finish = converter.finish();
        assert!(finish.is_empty());
    }

    #[test]
    fn content_filter_finish_reason_maps_to_incomplete_with_reason() {
        // Non-streaming path.
        let chat = serde_json::json!({
            "id": "chatcmpl-2",
            "model": "test",
            "choices": [{
                "finish_reason": "content_filter",
                "message": { "role": "assistant", "content": "partial" }
            }]
        });
        let response = chat_completion_to_response(chat).unwrap();
        assert_eq!(response["status"], serde_json::json!("incomplete"));
        assert_eq!(
            response["incomplete_details"]["reason"],
            serde_json::json!("content_filter")
        );

        // Streaming path.
        let sse = "data: {\"id\":\"chatcmpl-3\",\"choices\":[{\"delta\":{\"content\":\"x\"},\"finish_reason\":\"content_filter\"}]}\n\ndata: [DONE]\n\n";
        let converted = chat_sse_to_responses_sse(sse);
        let completed_data = converted
            .lines()
            .filter(|line| line.starts_with("data: ") && line.contains("response.completed"))
            .next_back()
            .expect("completed event present");
        let payload: Value = serde_json::from_str(&completed_data["data: ".len()..]).unwrap();
        assert_eq!(payload["response"]["status"], json!("incomplete"));
        assert_eq!(
            payload["response"]["incomplete_details"]["reason"],
            json!("content_filter")
        );
    }
}

#[cfg(test)]
mod input_image_budget_tests {
    use super::*;

    const ONE_MB: usize = 1024 * 1024;

    /// Build a data-url string of an exact byte length so budget math in
    /// these tests is precise regardless of the (irrelevant) fake payload
    /// contents.
    fn data_url(byte_len: usize) -> String {
        let prefix = "data:image/png;base64,";
        let filler_len = byte_len.saturating_sub(prefix.len());
        format!("{prefix}{}", "A".repeat(filler_len))
    }

    fn message_with_image(role: &str, image_url: &str) -> Value {
        json!({
            "type": "message",
            "role": role,
            "content": [{ "type": "input_image", "image_url": image_url }]
        })
    }

    fn function_call_output_with_image(call_id: &str, image_url: &str) -> Value {
        json!({
            "type": "function_call_output",
            "call_id": call_id,
            "output": [{ "type": "input_image", "image_url": image_url }]
        })
    }

    #[test]
    fn strip_all_input_images_replaces_every_image_regardless_of_budget() {
        let small = data_url(1024);
        let mut body = json!({
            "model": "test-model",
            "input": [
                message_with_image("user", &small),
                { "type": "message", "role": "user", "content": [{ "type": "input_text", "text": "keep me" }] },
                function_call_output_with_image("call_1", &small),
            ]
        });

        let stripped = strip_all_input_images(&mut body);

        assert_eq!(stripped, 2, "both images stripped even though under budget");
        let serialized = body.to_string();
        assert!(!serialized.contains("data:image"));
        assert!(serialized.contains("keep me"));
        assert_eq!(
            serialized
                .matches(INPUT_IMAGE_BUDGET_PLACEHOLDER_TEXT)
                .count(),
            2
        );
    }

    #[test]
    fn strip_all_input_images_is_noop_without_images() {
        let mut body = json!({
            "model": "test-model",
            "input": [
                { "type": "message", "role": "user", "content": [{ "type": "input_text", "text": "hi" }] }
            ]
        });
        let original = body.clone();
        assert_eq!(strip_all_input_images(&mut body), 0);
        assert_eq!(body, original);
    }

    #[test]
    fn within_budget_images_are_left_untouched() {
        let image_a = data_url(ONE_MB);
        let image_b = data_url(ONE_MB);
        let mut body = json!({
            "model": "test-model",
            "input": [
                message_with_image("user", &image_a),
                message_with_image("user", &image_b),
            ]
        });
        let original = body.clone();

        let replaced = enforce_input_image_budget(&mut body);

        assert_eq!(replaced, 0, "both 1MB images fit the 4MB budget together");
        assert_eq!(
            body, original,
            "body must be byte-for-byte unchanged when under budget"
        );
    }

    #[test]
    fn over_budget_replaces_oldest_image_keeps_newest_and_leaves_text_alone() {
        // 3MB (oldest) + 3MB (newest) = 6MB > 4MB budget. Walking
        // newest-first, the newest fits (remaining 4MB - 3MB = 1MB); the
        // oldest no longer fits in the remaining 1MB and must be replaced.
        let old_image = data_url(3 * ONE_MB);
        let new_image = data_url(3 * ONE_MB);
        let mut oldest = message_with_image("user", &old_image);
        oldest["content"].as_array_mut().unwrap().insert(
            0,
            json!({ "type": "input_text", "text": "look at this old screenshot" }),
        );
        let newest = message_with_image("user", &new_image);

        let mut body = json!({
            "model": "test-model",
            "input": [oldest, newest]
        });

        let replaced = enforce_input_image_budget(&mut body);

        assert_eq!(replaced, 1);
        let items = body["input"].as_array().unwrap();

        // Oldest message: text part untouched, image part replaced.
        let oldest_parts = items[0]["content"].as_array().unwrap();
        assert_eq!(oldest_parts[0]["type"], json!("input_text"));
        assert_eq!(
            oldest_parts[0]["text"],
            json!("look at this old screenshot")
        );
        assert_eq!(oldest_parts[1]["type"], json!("input_text"));
        assert_eq!(
            oldest_parts[1]["text"],
            json!(INPUT_IMAGE_BUDGET_PLACEHOLDER_TEXT)
        );

        // Newest message: image kept exactly as-is.
        let newest_parts = items[1]["content"].as_array().unwrap();
        assert_eq!(newest_parts[0]["type"], json!("input_image"));
        assert_eq!(newest_parts[0]["image_url"], json!(new_image));
    }

    #[test]
    fn mixed_message_and_function_call_output_images_are_budgeted_by_recency() {
        // Oldest -> newest: message A (2MB), function_call_output B (2MB),
        // function_call_output C (1MB), message D (2MB). Total 7MB > 4MB
        // budget. Walking newest-first: D fits (remaining 2MB), C fits
        // (remaining 1MB), B no longer fits (needs 2MB) so it's replaced,
        // and A doesn't fit either (dropped bytes are never refunded to the
        // budget) so it's replaced too — position in `input` (recency)
        // decides survival, not whether the image sits in a message or a
        // function_call_output.
        let image_a = data_url(2 * ONE_MB);
        let image_b = data_url(2 * ONE_MB);
        let image_c = data_url(ONE_MB);
        let image_d = data_url(2 * ONE_MB);

        let mut body = json!({
            "model": "test-model",
            "input": [
                message_with_image("user", &image_a),
                function_call_output_with_image("call_b", &image_b),
                function_call_output_with_image("call_c", &image_c),
                message_with_image("assistant", &image_d),
            ]
        });

        let replaced = enforce_input_image_budget(&mut body);

        assert_eq!(replaced, 2);
        let items = body["input"].as_array().unwrap();

        // A (message, oldest): replaced.
        assert_eq!(items[0]["content"][0]["type"], json!("input_text"));
        assert_eq!(
            items[0]["content"][0]["text"],
            json!(INPUT_IMAGE_BUDGET_PLACEHOLDER_TEXT)
        );
        // B (function_call_output): replaced.
        assert_eq!(items[1]["output"][0]["type"], json!("input_text"));
        assert_eq!(
            items[1]["output"][0]["text"],
            json!(INPUT_IMAGE_BUDGET_PLACEHOLDER_TEXT)
        );
        // C (function_call_output): kept.
        assert_eq!(items[2]["output"][0]["type"], json!("input_image"));
        assert_eq!(items[2]["output"][0]["image_url"], json!(image_c));
        // D (message, newest): kept.
        assert_eq!(items[3]["content"][0]["type"], json!("input_image"));
        assert_eq!(items[3]["content"][0]["image_url"], json!(image_d));
    }

    #[test]
    fn single_image_larger_than_budget_is_replaced_even_though_it_is_newest() {
        let huge_image = data_url(6 * ONE_MB);
        let mut item = message_with_image("user", &huge_image);
        item["content"].as_array_mut().unwrap().insert(
            0,
            json!({ "type": "input_text", "text": "here is a huge screenshot" }),
        );
        let mut body = json!({
            "model": "test-model",
            "input": [item]
        });

        let replaced = enforce_input_image_budget(&mut body);

        assert_eq!(
            replaced, 1,
            "a single image over budget must be replaced even with nothing older competing for the budget"
        );
        let parts = body["input"][0]["content"].as_array().unwrap();
        assert_eq!(parts[0]["type"], json!("input_text"));
        assert_eq!(parts[0]["text"], json!("here is a huge screenshot"));
        assert_eq!(parts[1]["type"], json!("input_text"));
        assert_eq!(parts[1]["text"], json!(INPUT_IMAGE_BUDGET_PLACEHOLDER_TEXT));
    }

    #[test]
    fn request_without_images_is_left_completely_unchanged() {
        let mut body = json!({
            "model": "test-model",
            "input": [
                { "role": "user", "content": "plain text question" },
                {
                    "type": "function_call",
                    "call_id": "call_1",
                    "name": "do_thing",
                    "arguments": "{}"
                },
                {
                    "type": "function_call_output",
                    "call_id": "call_1",
                    "output": "a plain string result, not an array"
                },
                {
                    "role": "assistant",
                    "content": [{ "type": "output_text", "text": "an answer" }]
                }
            ]
        });
        let original = body.clone();

        let replaced = enforce_input_image_budget(&mut body);

        assert_eq!(replaced, 0);
        assert_eq!(
            body, original,
            "a request with no input_image parts must be byte-for-byte unchanged"
        );
    }

    #[test]
    fn wired_into_both_responses_and_chat_completions_relay_paths() {
        // The whole point of cleaning at the top of `upstream_request_parts`
        // (before the protocol match) is that both wire formats derive from
        // the same Responses-shaped body, so one pass covers both — this
        // guards against a future refactor accidentally moving the call
        // into only one branch and re-opening the leak on the other.
        let huge_image = data_url(6 * ONE_MB);
        let request_json = json!({
            "model": "test-model",
            "input": [message_with_image("user", &huge_image)]
        });

        let responses_relay = ProtocolProxyUpstream {
            id: "test".to_string(),
            name: "test".to_string(),
            base_url: "https://example.invalid".to_string(),
            api_key: "key".to_string(),
            protocol: RelayProtocol::Responses,
            user_agent: String::new(),
            context_budget: ContextBudgetConfig::default(),
            plan_hints: false,
            model_routes: BTreeMap::new(),
        };
        let (_, responses_body, wire_api, _) =
            upstream_request_parts(&responses_relay, request_json.clone()).unwrap();
        assert_eq!(wire_api, UpstreamWireApi::Responses);
        assert_eq!(
            responses_body["input"][0]["content"][0]["type"],
            json!("input_text"),
            "Responses passthrough must replace the oversized image: {responses_body:?}"
        );

        let chat_relay = ProtocolProxyUpstream {
            protocol: RelayProtocol::ChatCompletions,
            ..responses_relay
        };
        let (_, chat_body, wire_api, _) =
            upstream_request_parts(&chat_relay, request_json).unwrap();
        assert_eq!(wire_api, UpstreamWireApi::ChatCompletions);
        let chat_json = serde_json::to_string(&chat_body).unwrap();
        assert!(
            !chat_json.contains("base64"),
            "converted Chat Completions body must not carry the oversized image's base64 payload: {chat_json}"
        );
    }
}

#[cfg(test)]
mod aggregate_routing_tests {
    use super::*;

    /// A default/active upstream plus two aggregate-mode routes claimed by
    /// two other (fictional) providers, for asserting that a request's
    /// `model` field picks the right one.
    fn relay_with_routes() -> ProtocolProxyUpstream {
        let mut model_routes = BTreeMap::new();
        model_routes.insert(
            "claude-sonnet-5".to_string(),
            ModelRoute {
                base_url: "https://claude.example/v1".to_string(),
                api_key: "sk-claude".to_string(),
            },
        );
        model_routes.insert(
            "gpt-5.6".to_string(),
            ModelRoute {
                base_url: "https://gpt.example/v1".to_string(),
                api_key: "sk-gpt".to_string(),
            },
        );
        ProtocolProxyUpstream {
            id: "default".to_string(),
            name: "default".to_string(),
            base_url: "https://default.example/v1".to_string(),
            api_key: "sk-default".to_string(),
            protocol: RelayProtocol::Responses,
            user_agent: String::new(),
            context_budget: ContextBudgetConfig::default(),
            plan_hints: false,
            model_routes,
        }
    }

    #[test]
    fn resolve_model_upstream_routes_matched_models_and_falls_back_for_others() {
        let relay = relay_with_routes();

        let (base_url, api_key) = resolve_model_upstream(&relay, Some("claude-sonnet-5"));
        assert_eq!(base_url, "https://claude.example/v1");
        assert_eq!(api_key, "sk-claude");

        let (base_url, api_key) = resolve_model_upstream(&relay, Some("gpt-5.6"));
        assert_eq!(base_url, "https://gpt.example/v1");
        assert_eq!(api_key, "sk-gpt");

        // Namespace-prefixed, suffixed, mixed-case model id still matches
        // the same route (see `crate::normalized_route_key`).
        let (base_url, api_key) = resolve_model_upstream(&relay, Some("openai/GPT-5.6[1m]"));
        assert_eq!(base_url, "https://gpt.example/v1");
        assert_eq!(api_key, "sk-gpt");

        // A model neither route claims falls back to the relay's own
        // default upstream.
        let (base_url, api_key) = resolve_model_upstream(&relay, Some("some-unclaimed-model"));
        assert_eq!(base_url, "https://default.example/v1");
        assert_eq!(api_key, "sk-default");

        // No model at all also falls back to the default.
        let (base_url, api_key) = resolve_model_upstream(&relay, None);
        assert_eq!(base_url, "https://default.example/v1");
        assert_eq!(api_key, "sk-default");
    }

    #[test]
    fn upstream_request_parts_routes_different_models_to_different_upstreams() {
        let relay = relay_with_routes();

        let claude_request = json!({ "model": "claude-sonnet-5", "input": [] });
        let (endpoint, _, _, api_key) = upstream_request_parts(&relay, claude_request).unwrap();
        assert_eq!(endpoint, responses_url("https://claude.example/v1"));
        assert_eq!(api_key, "sk-claude");

        let gpt_request = json!({ "model": "gpt-5.6", "input": [] });
        let (endpoint, _, _, api_key) = upstream_request_parts(&relay, gpt_request).unwrap();
        assert_eq!(endpoint, responses_url("https://gpt.example/v1"));
        assert_eq!(api_key, "sk-gpt");

        // A model neither route claims falls back to the relay's own
        // default upstream (the active/default provider).
        let other_request = json!({ "model": "some-other-model", "input": [] });
        let (endpoint, _, _, api_key) = upstream_request_parts(&relay, other_request).unwrap();
        assert_eq!(endpoint, responses_url("https://default.example/v1"));
        assert_eq!(api_key, "sk-default");
    }

    #[test]
    fn upstream_request_parts_routing_also_applies_to_chat_completions_wire_format() {
        // Same routing table, but `relay.protocol` is ChatCompletions this
        // time, so `upstream_request_parts` takes the other match arm — the
        // routed base_url/api_key must still apply there, since aggregate
        // routing is resolved once up front, ahead of the protocol match.
        let mut relay = relay_with_routes();
        relay.protocol = RelayProtocol::ChatCompletions;

        let request = json!({ "model": "gpt-5.6", "input": [] });
        let (endpoint, _, wire_api, api_key) = upstream_request_parts(&relay, request).unwrap();
        assert_eq!(wire_api, UpstreamWireApi::ChatCompletions);
        assert_eq!(endpoint, chat_completions_url("https://gpt.example/v1"));
        assert_eq!(api_key, "sk-gpt");
    }

    #[test]
    fn empty_model_routes_always_falls_back_to_default_upstream() {
        // Aggregate mode off: `model_routes` is empty, so every request
        // must keep using the relay's own base_url/api_key exactly as
        // before this feature existed.
        let relay = ProtocolProxyUpstream {
            id: "solo".to_string(),
            name: "solo".to_string(),
            base_url: "https://solo.example/v1".to_string(),
            api_key: "sk-solo".to_string(),
            protocol: RelayProtocol::Responses,
            user_agent: String::new(),
            context_budget: ContextBudgetConfig::default(),
            plan_hints: false,
            model_routes: BTreeMap::new(),
        };
        let (base_url, api_key) = resolve_model_upstream(&relay, Some("anything"));
        assert_eq!(base_url, "https://solo.example/v1");
        assert_eq!(api_key, "sk-solo");
    }
}

#[cfg(test)]
mod plan_nudge_tests {
    use super::*;

    fn function_call(name: &str) -> Value {
        json!({ "type": "function_call", "name": name, "arguments": "{}", "call_id": "call_1" })
    }

    fn update_plan_call() -> Value {
        function_call("update_plan")
    }

    fn plan_hints_relay(protocol: RelayProtocol) -> ProtocolProxyUpstream {
        ProtocolProxyUpstream {
            id: "test".to_string(),
            name: "test".to_string(),
            base_url: "https://example.invalid".to_string(),
            api_key: "key".to_string(),
            protocol,
            user_agent: String::new(),
            context_budget: ContextBudgetConfig::default(),
            plan_hints: true,
            model_routes: BTreeMap::new(),
        }
    }

    #[test]
    fn count_since_last_update_plan_counts_all_calls_when_never_called() {
        let items = vec![
            function_call("shell"),
            function_call("apply_patch"),
            function_call("shell"),
        ];
        assert_eq!(count_function_calls_since_last_update_plan(&items), 3);
    }

    #[test]
    fn count_since_last_update_plan_only_counts_calls_after_the_most_recent_one() {
        let items = vec![
            function_call("shell"),
            update_plan_call(),
            function_call("shell"),
            function_call("apply_patch"),
        ];
        assert_eq!(count_function_calls_since_last_update_plan(&items), 2);
    }

    #[test]
    fn count_since_last_update_plan_uses_the_latest_update_plan_not_the_first() {
        let items = vec![
            update_plan_call(),
            function_call("shell"),
            function_call("shell"),
            update_plan_call(),
            function_call("shell"),
        ];
        assert_eq!(count_function_calls_since_last_update_plan(&items), 1);
    }

    #[test]
    fn count_since_last_update_plan_is_zero_for_empty_or_missing_history() {
        assert_eq!(count_function_calls_since_last_update_plan(&[]), 0);
    }

    #[test]
    fn should_inject_plan_reminder_respects_plan_hints_toggle() {
        let mut items = vec![];
        for _ in 0..STALE_PLAN_REMINDER_THRESHOLD {
            items.push(function_call("shell"));
        }
        assert!(
            should_inject_plan_reminder(&items, true),
            "stale history with plan_hints on must trigger the nudge"
        );
        assert!(
            !should_inject_plan_reminder(&items, false),
            "plan_hints off must never trigger the nudge, however stale"
        );
    }

    #[test]
    fn should_inject_plan_reminder_only_fires_at_or_above_the_threshold() {
        let mut items = vec![];
        for _ in 0..(STALE_PLAN_REMINDER_THRESHOLD - 1) {
            items.push(function_call("shell"));
        }
        assert!(
            !should_inject_plan_reminder(&items, true),
            "one call under the threshold must not be a false positive"
        );
        items.push(function_call("shell"));
        assert!(
            should_inject_plan_reminder(&items, true),
            "hitting the threshold exactly must trigger the nudge"
        );
    }

    #[test]
    fn inject_plan_reminder_item_appends_a_labeled_user_role_item() {
        let mut items = vec![function_call("shell")];
        inject_plan_reminder_item(&mut items);
        assert_eq!(items.len(), 2);
        let injected = items.last().unwrap();
        assert_eq!(injected["role"], "user");
        let text = injected["content"].as_str().expect("content is a string");
        assert!(text.contains("update_plan"));
        assert!(text.starts_with("<plan_reminder>"));
    }

    #[test]
    fn upstream_request_parts_injects_reminder_for_responses_when_stale_and_enabled() {
        let mut input = vec![];
        for _ in 0..STALE_PLAN_REMINDER_THRESHOLD {
            input.push(function_call("shell"));
        }
        let request_json = json!({ "model": "test-model", "input": input });
        let relay = plan_hints_relay(RelayProtocol::Responses);

        let (_, body, wire_api, _) = upstream_request_parts(&relay, request_json).unwrap();
        assert_eq!(wire_api, UpstreamWireApi::Responses);
        let items = body["input"].as_array().unwrap();
        let last = items.last().unwrap();
        assert_eq!(last["role"], "user");
        assert!(
            last["content"]
                .as_str()
                .unwrap()
                .contains("update_plan"),
            "reminder must survive the Responses passthrough path unmodified: {items:?}"
        );
    }

    #[test]
    fn upstream_request_parts_injects_reminder_for_chat_completions_when_stale_and_enabled() {
        let mut input = vec![function_call("shell")];
        for _ in 0..STALE_PLAN_REMINDER_THRESHOLD {
            input.push(function_call("apply_patch"));
        }
        let request_json = json!({ "model": "test-model", "input": input });
        let relay = plan_hints_relay(RelayProtocol::ChatCompletions);

        let (_, body, wire_api, _) = upstream_request_parts(&relay, request_json).unwrap();
        assert_eq!(wire_api, UpstreamWireApi::ChatCompletions);
        let messages = body["messages"].as_array().unwrap();
        let has_reminder = messages.iter().any(|message| {
            message.get("role").and_then(Value::as_str) == Some("user")
                && message
                    .get("content")
                    .and_then(Value::as_str)
                    .is_some_and(|text| text.contains("update_plan"))
        });
        assert!(
            has_reminder,
            "reminder must survive translation into the Chat Completions body: {messages:?}"
        );
    }

    #[test]
    fn upstream_request_parts_skips_reminder_when_plan_hints_disabled() {
        let mut input = vec![];
        for _ in 0..(STALE_PLAN_REMINDER_THRESHOLD + 2) {
            input.push(function_call("shell"));
        }
        let request_json = json!({ "model": "test-model", "input": input.clone() });
        let mut relay = plan_hints_relay(RelayProtocol::Responses);
        relay.plan_hints = false;

        let (_, body, _, _) = upstream_request_parts(&relay, request_json).unwrap();
        let items = body["input"].as_array().unwrap();
        assert_eq!(
            items.len(),
            input.len(),
            "plan_hints off must never append the reminder, however stale: {items:?}"
        );
    }

    #[test]
    fn upstream_request_parts_skips_reminder_when_below_threshold() {
        let mut input = vec![];
        for _ in 0..(STALE_PLAN_REMINDER_THRESHOLD - 1) {
            input.push(function_call("shell"));
        }
        let request_json = json!({ "model": "test-model", "input": input.clone() });
        let relay = plan_hints_relay(RelayProtocol::Responses);

        let (_, body, _, _) = upstream_request_parts(&relay, request_json).unwrap();
        let items = body["input"].as_array().unwrap();
        assert_eq!(
            items.len(),
            input.len(),
            "a normal short multi-tool turn must not be a false positive: {items:?}"
        );
    }
}
