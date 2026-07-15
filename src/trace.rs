//! Lightweight request/response tracing for the Responses passthrough path.
//!
//! Exists to diagnose silent upstream failures after the fact — in
//! particular the "upstream returns HTTP 200, the stream ends normally, but
//! `response.output` only contains `reasoning` items" failure mode (see
//! `protocol_proxy::ResponsesPassthroughDiagnosis`), where nothing in
//! `agent.terminal.log` explains why Codex quietly ended the turn.
//!
//! Two tiers, both best-effort — a tracing failure must never affect the
//! actual proxied request, so every public function here only logs (via
//! `protocol_proxy::log_upstream_event_deduped`, so failures are deduped
//! like any other diagnostic) and returns `()`:
//!
//! - Always on: one JSON line per Responses request appended to
//!   `~/.codex-gateway-lite/trace/requests.jsonl`, rotated at 5MB into
//!   `requests.jsonl.1` — mirroring `agent.terminal.log`'s rotation in
//!   "Codex Gateway Lite.command" exactly (simple two-generation rotation).
//! - Opt-in via the `CODEX_GATEWAY_LITE_TRACE=full` environment variable:
//!   additionally dumps the full cleaned upstream request body to
//!   `trace/<seq>-request.json` and raw response SSE bytes to
//!   `trace/<seq>-response.sse` (each capped at 8MB), so a specific request
//!   can be replayed/inspected byte-for-byte. `cleanup_old_full_dumps` prunes
//!   these to the most recent 200 on every startup so full tracing can be
//!   left on without unbounded disk growth.

use std::fs::{self, File};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::SystemTime;

use serde_json::Value;

/// Rotation threshold for `requests.jsonl`, matching `agent.terminal.log`'s
/// 5MB threshold in "Codex Gateway Lite.command" for consistency.
const REQUESTS_LOG_MAX_BYTES: u64 = 5 * 1024 * 1024;
/// Per-file cap for full request/response dumps (`CODEX_GATEWAY_LITE_TRACE=full`
/// only). Generous enough for real request/response bodies while still
/// bounding worst-case disk usage per file.
const FULL_DUMP_MAX_BYTES: u64 = 8 * 1024 * 1024;
/// How many most-recent numbered dump files (`<seq>-request.json` /
/// `<seq>-response.sse`) `cleanup_old_full_dumps` keeps on startup.
const FULL_DUMP_RETAIN_COUNT: usize = 200;
/// Appended once when a full dump hits `FULL_DUMP_MAX_BYTES`, so it's
/// obvious from the file itself that content was cut off rather than the
/// response actually ending there.
const TRUNCATION_MARKER: &str = "\n...[截断：超过单文件大小上限，后续内容已省略]\n";

static TRACE_SEQ: AtomicU64 = AtomicU64::new(0);

/// Allocate the next process-local trace sequence number, used to name
/// `trace/<seq>-request.json` / `trace/<seq>-response.sse` and to correlate
/// a `trace/requests.jsonl` row back to them. Resets to 0 on every process
/// restart — acceptable because every `requests.jsonl` row also carries a
/// timestamp, so a restart never makes two rows genuinely ambiguous even if
/// their `seq` values happen to collide.
pub fn next_seq() -> u64 {
    TRACE_SEQ.fetch_add(1, Ordering::Relaxed)
}

/// Root trace directory: `~/.codex-gateway-lite/trace`. Reuses
/// `crate::user_home_dir()` rather than the `dirs` crate, which is not a
/// dependency of this project.
fn trace_dir() -> PathBuf {
    crate::user_home_dir()
        .unwrap_or_else(|| PathBuf::from("."))
        .join(".codex-gateway-lite")
        .join("trace")
}

fn requests_log_path(dir: &Path) -> PathBuf {
    dir.join("requests.jsonl")
}

fn request_dump_path(dir: &Path, seq: u64) -> PathBuf {
    dir.join(format!("{seq}-request.json"))
}

fn response_dump_path(dir: &Path, seq: u64) -> PathBuf {
    dir.join(format!("{seq}-response.sse"))
}

/// Whether `CODEX_GATEWAY_LITE_TRACE=full` is set, gating the heavier
/// per-request full dumps on top of the always-on `requests.jsonl` index.
fn full_trace_enabled() -> bool {
    full_trace_enabled_for(std::env::var("CODEX_GATEWAY_LITE_TRACE").ok().as_deref())
}

/// Pure decision logic behind `full_trace_enabled`, split out so tests can
/// exercise it without mutating the real (process-global) environment
/// variable — `std::env::set_var` requires `unsafe` on current Rust and
/// would be one more thing tests could flakily interfere with each other on.
fn full_trace_enabled_for(value: Option<&str>) -> bool {
    value == Some("full")
}

/// One `trace/requests.jsonl` row. Field names are deliberately plain
/// snake_case (no `rename_all`) since this is an internal diagnostic format
/// with no external schema to match.
#[derive(Debug, Clone, serde::Serialize)]
struct RequestTraceRecord {
    ts: String,
    seq: Option<u64>,
    model: String,
    session: String,
    endpoint: String,
    request_bytes: usize,
    stream: bool,
    status: u16,
    attempts: u32,
    response_bytes: u64,
    diagnosis: &'static str,
}

/// Append one metadata row to `trace/requests.jsonl` for a finished
/// Responses passthrough request — called from both the streaming
/// passthrough loop (`main.rs`) and the buffered fallback
/// (`protocol_proxy::handle_responses_upstream`, via
/// `protocol_proxy::record_responses_trace`). Always runs, independent of
/// `CODEX_GATEWAY_LITE_TRACE`; only the heavier per-request full dumps are
/// gated behind that.
///
/// Best-effort: any I/O failure is only logged (deduped) and never
/// propagated, since a trace write must never affect the actual proxied
/// response.
#[allow(clippy::too_many_arguments)]
pub fn record_responses_request(
    seq: Option<u64>,
    model: &str,
    session: &str,
    endpoint: &str,
    request_bytes: usize,
    stream: bool,
    status: u16,
    attempts: u32,
    response_bytes: u64,
    diagnosis: &'static str,
) {
    let record = RequestTraceRecord {
        ts: chrono::Local::now()
            .format("%Y-%m-%d %H:%M:%S%.3f")
            .to_string(),
        seq,
        model: model.to_string(),
        session: session.to_string(),
        endpoint: endpoint.to_string(),
        request_bytes,
        stream,
        status,
        attempts,
        response_bytes,
        diagnosis,
    };
    record_request_in_dir(&trace_dir(), &record);
}

fn record_request_in_dir(dir: &Path, record: &RequestTraceRecord) {
    let Ok(line) = serde_json::to_string(record) else {
        // A `RequestTraceRecord` is entirely plain data (strings/numbers/
        // bools) with no way to fail serialization; this is defensive only.
        return;
    };
    if let Err(error) = fs::create_dir_all(dir) {
        log_trace_io_failure(&format!(
            "创建 trace 目录失败：{}（{error}）",
            dir.display()
        ));
        return;
    }
    let path = requests_log_path(dir);
    rotate_if_too_large(&path, REQUESTS_LOG_MAX_BYTES);
    let result = fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(&path)
        .and_then(|mut file| writeln!(file, "{line}"));
    if let Err(error) = result {
        log_trace_io_failure(&format!("写入 trace/requests.jsonl 失败：{error}"));
    }
}

/// Two-generation rotation: if `path` is over `max_bytes`, move it to
/// `<path>.1` (overwriting any previous `.1`). Mirrors the
/// `agent.terminal.log` rotation in "Codex Gateway Lite.command" exactly
/// (`mv -f "$AGENT_LOG" "$AGENT_LOG.1"` past a 5MB threshold), so the two
/// log families behave the same way operationally.
fn rotate_if_too_large(path: &Path, max_bytes: u64) {
    let Ok(metadata) = fs::metadata(path) else {
        return;
    };
    if metadata.len() <= max_bytes {
        return;
    }
    let mut rotated = path.as_os_str().to_os_string();
    rotated.push(".1");
    let _ = fs::rename(path, PathBuf::from(rotated));
}

/// Write the full cleaned upstream request body to `trace/<seq>-request.json`.
/// No-ops unless `CODEX_GATEWAY_LITE_TRACE=full` is set. Best-effort and
/// capped at `FULL_DUMP_MAX_BYTES`; called from
/// `protocol_proxy::open_responses_proxy_request` right after the outgoing
/// body is finalized, so even a request that never gets a response
/// (connection failure, timeout) still leaves the outgoing body on disk.
pub fn write_full_request_dump(seq: u64, body: &Value) {
    if !full_trace_enabled() {
        return;
    }
    write_full_request_dump_in_dir(&trace_dir(), seq, body);
}

fn write_full_request_dump_in_dir(dir: &Path, seq: u64, body: &Value) {
    let Ok(text) = serde_json::to_vec_pretty(body) else {
        return;
    };
    if let Err(error) = fs::create_dir_all(dir) {
        log_trace_io_failure(&format!(
            "创建 trace 目录失败：{}（{error}）",
            dir.display()
        ));
        return;
    }
    let capped = cap_bytes(&text, FULL_DUMP_MAX_BYTES);
    if let Err(error) = fs::write(request_dump_path(dir, seq), &capped) {
        log_trace_io_failure(&format!("写入 trace 请求体 dump 失败：{error}"));
    }
}

/// Truncate `data` to `max_bytes`, appending `TRUNCATION_MARKER` in the
/// space made for it when truncation actually happens. Used for the
/// request dump, which is written in one shot (the whole body is already in
/// memory as a `Value`); the response dump instead truncates incrementally
/// as chunks stream in — see `ResponseDumpWriter::append`.
fn cap_bytes(data: &[u8], max_bytes: u64) -> Vec<u8> {
    let max_bytes = max_bytes as usize;
    if data.len() <= max_bytes {
        return data.to_vec();
    }
    let marker = TRUNCATION_MARKER.as_bytes();
    let keep = max_bytes.saturating_sub(marker.len()).min(data.len());
    let mut out = data[..keep].to_vec();
    out.extend_from_slice(marker);
    out
}

/// Incrementally writes raw response SSE bytes to `trace/<seq>-response.sse`
/// as they're relayed to Codex, for `CODEX_GATEWAY_LITE_TRACE=full`. Caps
/// total written bytes at `FULL_DUMP_MAX_BYTES`: once exceeded, a
/// truncation marker is appended once and further chunks are silently
/// dropped, so a huge or never-ending stream can't grow this file without
/// bound.
///
/// Constructing one is always safe and cheap even when full tracing is off
/// or `seq` is `None` (e.g. requests opened through `open_models_proxy_request`
/// / `open_chat_completions_proxy_request`, which don't participate in
/// tracing) — it just becomes inert, so call sites don't need to branch on
/// whether tracing is actually active.
pub struct ResponseDumpWriter {
    file: Option<File>,
    written: u64,
    truncated: bool,
}

impl ResponseDumpWriter {
    pub fn new(seq: Option<u64>) -> Self {
        let Some(seq) = seq.filter(|_| full_trace_enabled()) else {
            return Self::inert();
        };
        let dir = trace_dir();
        if let Err(error) = fs::create_dir_all(&dir) {
            log_trace_io_failure(&format!(
                "创建 trace 目录失败：{}（{error}）",
                dir.display()
            ));
            return Self::inert();
        }
        match open_dump_file(&response_dump_path(&dir, seq)) {
            Ok(file) => Self {
                file: Some(file),
                written: 0,
                truncated: false,
            },
            Err(error) => {
                log_trace_io_failure(&format!("创建 trace 响应体 dump 失败：{error}"));
                Self::inert()
            }
        }
    }

    fn inert() -> Self {
        Self {
            file: None,
            written: 0,
            truncated: false,
        }
    }

    /// Append one more raw chunk, exactly as relayed to Codex. No-ops once
    /// the byte cap has been hit, or if this writer was never enabled.
    pub fn append(&mut self, chunk: &[u8]) {
        if self.truncated || chunk.is_empty() {
            return;
        }
        let Some(file) = self.file.as_mut() else {
            return;
        };
        if self.written + chunk.len() as u64 > FULL_DUMP_MAX_BYTES {
            self.truncated = true;
            let _ = file.write_all(TRUNCATION_MARKER.as_bytes());
            return;
        }
        if file.write_all(chunk).is_ok() {
            self.written += chunk.len() as u64;
        }
    }
}

fn open_dump_file(path: &Path) -> std::io::Result<File> {
    fs::OpenOptions::new()
        .create(true)
        .write(true)
        .truncate(true)
        .open(path)
}

/// Whether `name` looks like a numbered full-dump file this module owns
/// (`<digits>-request.json` / `<digits>-response.sse`), as opposed to
/// `requests.jsonl`/`requests.jsonl.1` (which have their own rotation and
/// are never touched by `cleanup_old_full_dumps`) or anything unrelated a
/// user might have dropped into the same directory.
fn is_full_dump_file_name(name: &str) -> bool {
    let rest = name
        .strip_suffix("-request.json")
        .or_else(|| name.strip_suffix("-response.sse"));
    match rest {
        Some(rest) => !rest.is_empty() && rest.bytes().all(|byte| byte.is_ascii_digit()),
        None => false,
    }
}

/// Startup cleanup: keep only the most recent `FULL_DUMP_RETAIN_COUNT`
/// numbered dump files under `trace/`, deleting older ones. Runs
/// unconditionally (cheap no-op if the directory doesn't exist or has few
/// files) rather than only when full tracing is currently enabled, so dumps
/// left over from a *previous* run that had full tracing on still get
/// cleaned up even if this run has it off.
pub fn cleanup_old_full_dumps() {
    cleanup_old_full_dumps_in_dir(&trace_dir(), FULL_DUMP_RETAIN_COUNT);
}

/// Sorts by filesystem modification time rather than by the numeric `seq`
/// prefix, because `seq` resets to 0 on every process restart — a purely
/// numeric-filename sort would treat an old run's files and a new run's
/// files as if they were chronologically interleaved.
fn cleanup_old_full_dumps_in_dir(dir: &Path, retain_count: usize) {
    let Ok(entries) = fs::read_dir(dir) else {
        return;
    };
    let mut dumps: Vec<(PathBuf, SystemTime)> = entries
        .filter_map(Result::ok)
        .filter(|entry| {
            entry
                .file_name()
                .to_str()
                .is_some_and(is_full_dump_file_name)
        })
        .filter_map(|entry| {
            let modified = entry.metadata().and_then(|meta| meta.modified()).ok()?;
            Some((entry.path(), modified))
        })
        .collect();
    if dumps.len() <= retain_count {
        return;
    }
    // Newest first, so the tail past `retain_count` is what gets deleted.
    dumps.sort_by(|left, right| right.1.cmp(&left.1));
    for (path, _) in dumps.into_iter().skip(retain_count) {
        let _ = fs::remove_file(path);
    }
}

/// Report a tracing I/O failure exactly once per throttle window, via the
/// same deduped logger the rest of the proxy's diagnostics use — tracing
/// failures are just another upstream/environment diagnostic from the
/// user's point of view, not a special case.
fn log_trace_io_failure(message: &str) {
    crate::protocol_proxy::log_upstream_event_deduped(
        "trace_io_failure",
        crate::protocol_proxy::LogLevel::Warn,
        message.to_string(),
    );
}

#[cfg(test)]
mod trace_tests {
    use super::*;

    fn unique_temp_dir(label: &str) -> PathBuf {
        let unique = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .expect("clock is after unix epoch")
            .as_nanos();
        std::env::temp_dir().join(format!("codex-gateway-lite-trace-test-{label}-{unique}"))
    }

    #[test]
    fn full_trace_enabled_for_requires_exact_match() {
        assert!(!full_trace_enabled_for(None));
        assert!(full_trace_enabled_for(Some("full")));
        assert!(!full_trace_enabled_for(Some("")));
        assert!(!full_trace_enabled_for(Some("FULL")));
        assert!(!full_trace_enabled_for(Some("true")));
        assert!(!full_trace_enabled_for(Some("full ")));
    }

    #[test]
    fn is_full_dump_file_name_matches_numbered_dumps_only() {
        assert!(is_full_dump_file_name("42-request.json"));
        assert!(is_full_dump_file_name("42-response.sse"));
        assert!(is_full_dump_file_name("0-request.json"));
        assert!(!is_full_dump_file_name("abc-request.json"));
        assert!(!is_full_dump_file_name("-request.json"));
        assert!(!is_full_dump_file_name("42-request.txt"));
        assert!(!is_full_dump_file_name("requests.jsonl"));
        assert!(!is_full_dump_file_name("requests.jsonl.1"));
        assert!(!is_full_dump_file_name("42-response.ssex"));
    }

    #[test]
    fn cap_bytes_leaves_small_input_untouched() {
        let data = b"hello world";
        assert_eq!(cap_bytes(data, 1024), data.to_vec());
    }

    #[test]
    fn cap_bytes_truncates_and_appends_marker() {
        let data = vec![b'x'; 1000];
        let capped = cap_bytes(&data, 100);
        assert_eq!(capped.len(), 100);
        let text = String::from_utf8_lossy(&capped);
        assert!(text.ends_with(TRUNCATION_MARKER));
    }

    #[test]
    fn rotate_if_too_large_leaves_small_file_alone() {
        let dir = unique_temp_dir("rotate-small");
        fs::create_dir_all(&dir).expect("creates temp dir");
        let path = dir.join("requests.jsonl");
        fs::write(&path, b"short").expect("writes file");

        rotate_if_too_large(&path, 1024);

        assert!(path.exists());
        assert!(!dir.join("requests.jsonl.1").exists());
        assert_eq!(fs::read_to_string(&path).unwrap(), "short");
    }

    #[test]
    fn rotate_if_too_large_rotates_oversized_file() {
        let dir = unique_temp_dir("rotate-big");
        fs::create_dir_all(&dir).expect("creates temp dir");
        let path = dir.join("requests.jsonl");
        fs::write(&path, vec![b'a'; 200]).expect("writes file");

        rotate_if_too_large(&path, 100);

        assert!(!path.exists(), "original should have been renamed away");
        let mut rotated_name = path.as_os_str().to_os_string();
        rotated_name.push(".1");
        let rotated = PathBuf::from(rotated_name);
        assert!(rotated.exists(), "rotated .1 file should exist");
        assert_eq!(fs::read(&rotated).unwrap().len(), 200);
    }

    #[test]
    fn record_request_in_dir_writes_expected_json_shape() {
        let dir = unique_temp_dir("record");
        let record = RequestTraceRecord {
            ts: "2026-07-14 12:00:00.000".to_string(),
            seq: Some(7),
            model: "claude-fable-5".to_string(),
            session: "调试会话标题".to_string(),
            endpoint: "https://example.invalid/v1/responses".to_string(),
            request_bytes: 1234,
            stream: true,
            status: 200,
            attempts: 1,
            response_bytes: 5678,
            diagnosis: "reasoning_only",
        };

        record_request_in_dir(&dir, &record);

        let contents = fs::read_to_string(dir.join("requests.jsonl")).expect("reads jsonl");
        let line = contents.lines().next().expect("has one line");
        let parsed: Value = serde_json::from_str(line).expect("valid json line");
        assert_eq!(parsed["seq"], json_num(7));
        assert_eq!(parsed["model"], "claude-fable-5");
        assert_eq!(parsed["endpoint"], "https://example.invalid/v1/responses");
        assert_eq!(parsed["request_bytes"], json_num(1234));
        assert_eq!(parsed["stream"], true);
        assert_eq!(parsed["status"], json_num(200));
        assert_eq!(parsed["attempts"], json_num(1));
        assert_eq!(parsed["response_bytes"], json_num(5678));
        assert_eq!(parsed["diagnosis"], "reasoning_only");
        assert_eq!(parsed["ts"], "2026-07-14 12:00:00.000");
        assert_eq!(parsed["session"], "调试会话标题");
    }

    #[test]
    fn record_request_in_dir_rotates_before_appending() {
        let dir = unique_temp_dir("record-rotate");
        fs::create_dir_all(&dir).expect("creates temp dir");
        // Pre-seed an oversized existing log so the very next append should
        // trigger rotation before the new line lands.
        fs::write(
            dir.join("requests.jsonl"),
            vec![b'a'; REQUESTS_LOG_MAX_BYTES as usize + 1],
        )
        .expect("seeds oversized log");

        let record = RequestTraceRecord {
            ts: "2026-07-14 12:00:00.000".to_string(),
            seq: None,
            model: "grok".to_string(),
            session: "?".to_string(),
            endpoint: "https://example.invalid/v1/responses".to_string(),
            request_bytes: 1,
            stream: false,
            status: 200,
            attempts: 1,
            response_bytes: 2,
            diagnosis: "normal",
        };
        record_request_in_dir(&dir, &record);

        let rotated = dir.join("requests.jsonl.1");
        assert!(rotated.exists(), "oversized log should have been rotated");
        assert_eq!(
            fs::metadata(&rotated).unwrap().len() as usize,
            REQUESTS_LOG_MAX_BYTES as usize + 1
        );
        let fresh = fs::read_to_string(dir.join("requests.jsonl")).expect("reads fresh log");
        assert!(fresh.contains("\"model\":\"grok\""));
    }

    #[test]
    fn write_full_request_dump_in_dir_writes_pretty_json() {
        let dir = unique_temp_dir("request-dump");
        let body = serde_json::json!({ "model": "grok", "input": [] });

        write_full_request_dump_in_dir(&dir, 3, &body);

        let contents = fs::read_to_string(dir.join("3-request.json")).expect("reads dump");
        let parsed: Value = serde_json::from_str(&contents).expect("valid json");
        assert_eq!(parsed["model"], "grok");
    }

    #[test]
    fn response_dump_writer_inert_when_seq_is_none() {
        let mut writer = ResponseDumpWriter::new(None);
        writer.append(b"event: response.completed\n\n");
        // No directory should have been created since the writer never
        // activates without a seq.
        assert!(writer.file.is_none());
    }

    #[test]
    fn cleanup_old_full_dumps_in_dir_keeps_only_most_recent() {
        let dir = unique_temp_dir("cleanup");
        fs::create_dir_all(&dir).expect("creates temp dir");
        let now = SystemTime::now();
        // Five numbered dump pairs with strictly increasing mtimes, oldest
        // first, plus one unrelated file that must survive regardless of
        // age since it doesn't match the numbered-dump naming pattern.
        for index in 0..5u64 {
            let path = dir.join(format!("{index}-request.json"));
            fs::write(&path, b"{}").expect("writes dump");
            let mtime = now - std::time::Duration::from_secs(5 - index);
            File::open(&path)
                .expect("reopens dump")
                .set_modified(mtime)
                .expect("sets mtime");
        }
        let untouched = dir.join("requests.jsonl");
        fs::write(&untouched, b"{}\n").expect("writes untouched file");

        cleanup_old_full_dumps_in_dir(&dir, 3);

        for index in 0..2u64 {
            assert!(
                !dir.join(format!("{index}-request.json")).exists(),
                "oldest dumps should have been pruned"
            );
        }
        for index in 2..5u64 {
            assert!(
                dir.join(format!("{index}-request.json")).exists(),
                "most recent dumps should survive"
            );
        }
        assert!(untouched.exists(), "non-dump files must never be pruned");
    }

    fn json_num(value: u64) -> Value {
        serde_json::json!(value)
    }
}
