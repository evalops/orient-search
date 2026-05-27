use anyhow::Result;
use ignore::WalkBuilder;
use regex::Regex;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::HashMap;
use std::fs::File;
use std::io::{BufRead, BufReader};
use std::path::PathBuf;

#[derive(Debug, Clone)]
pub struct ScanOptions {
    pub roots: Vec<PathBuf>,
    pub max_files: Option<usize>,
    pub max_file_bytes: Option<u64>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, Ord, PartialOrd)]
#[serde(rename_all = "snake_case")]
pub enum ActionKind {
    SearchDiscovery,
    ReadFetch,
    WriteEdit,
    ShellOther,
    Web,
    Planning,
    Mcp,
    Other,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct KindMetrics {
    pub calls: usize,
    pub failures: usize,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct Metrics {
    pub files_scanned: usize,
    pub total_calls: usize,
    pub failed_calls: usize,
    pub by_kind: HashMap<ActionKind, KindMetrics>,
    pub by_tool: HashMap<String, KindMetrics>,
}

impl Metrics {
    pub fn failure_rate(&self) -> f64 {
        if self.total_calls == 0 {
            0.0
        } else {
            self.failed_calls as f64 / self.total_calls as f64
        }
    }

    pub fn orientation_share(&self) -> f64 {
        if self.total_calls == 0 {
            return 0.0;
        }
        let orientation = self
            .by_kind
            .get(&ActionKind::SearchDiscovery)
            .map(|m| m.calls)
            .unwrap_or_default()
            + self
                .by_kind
                .get(&ActionKind::ReadFetch)
                .map(|m| m.calls)
                .unwrap_or_default();
        orientation as f64 / self.total_calls as f64
    }
}

#[derive(Debug, Clone)]
struct ToolCall {
    call_id: String,
    tool_name: String,
    input: String,
}

pub fn scan_jsonl_roots(options: ScanOptions) -> Result<Metrics> {
    let files = discover_jsonl_files(&options)?;
    let mut metrics = Metrics {
        files_scanned: files.len(),
        ..Metrics::default()
    };
    let mut calls: HashMap<String, ToolCall> = HashMap::new();
    let mut failed: HashMap<String, bool> = HashMap::new();

    for path in files {
        let file = File::open(path)?;
        for line in BufReader::new(file).lines().map_while(Result::ok) {
            let trimmed = line.trim();
            if trimmed.is_empty() {
                continue;
            }
            let Ok(value) = serde_json::from_str::<Value>(trimmed) else {
                continue;
            };
            if let Some(call) = parse_tool_call(&value) {
                calls.insert(call.call_id.clone(), call);
            }
            if let Some((call_id, is_failed)) = parse_tool_output(&value) {
                failed.insert(call_id, is_failed);
            }
        }
    }

    for call in calls.values() {
        let is_failed = failed.get(&call.call_id).copied().unwrap_or(false);
        let kind = classify_action(&call.tool_name, &call.input);
        metrics.total_calls += 1;
        if is_failed {
            metrics.failed_calls += 1;
        }
        increment(&mut metrics.by_kind, kind, is_failed);
        increment(&mut metrics.by_tool, call.tool_name.clone(), is_failed);
    }

    Ok(metrics)
}

fn discover_jsonl_files(options: &ScanOptions) -> Result<Vec<PathBuf>> {
    let mut files = Vec::new();
    for root in &options.roots {
        if !root.exists() {
            continue;
        }
        for entry in WalkBuilder::new(root).hidden(false).build() {
            let entry = entry?;
            let path = entry.path();
            if !path.is_file() || path.extension().and_then(|ext| ext.to_str()) != Some("jsonl") {
                continue;
            }
            if let Some(max_bytes) = options.max_file_bytes {
                if entry.metadata()?.len() > max_bytes {
                    continue;
                }
            }
            files.push(path.to_path_buf());
        }
    }
    files.sort();
    if let Some(max_files) = options.max_files {
        files.truncate(max_files);
    }
    Ok(files)
}

fn parse_tool_call(value: &Value) -> Option<ToolCall> {
    let top_type = value.get("type")?.as_str()?;
    if top_type == "response_item" {
        let payload = value.get("payload")?;
        let item_type = payload.get("type")?.as_str()?;
        if !matches!(item_type, "function_call" | "custom_tool_call" | "web_search_call" | "tool_search_call") {
            return None;
        }
        let call_id = payload
            .get("call_id")
            .or_else(|| payload.get("id"))
            .and_then(Value::as_str)
            .unwrap_or("")
            .to_string();
        let tool_name = payload
            .get("name")
            .and_then(Value::as_str)
            .unwrap_or(item_type)
            .to_string();
        let input = compact_value(
            payload
                .get("arguments")
                .or_else(|| payload.get("input"))
                .or_else(|| payload.get("action")),
        );
        return Some(ToolCall {
            call_id,
            tool_name,
            input,
        });
    }

    if top_type == "assistant" {
        let content = value.get("message")?.get("content")?.as_array()?;
        for item in content {
            if item.get("type").and_then(Value::as_str) == Some("tool_use") {
                return Some(ToolCall {
                    call_id: item.get("id")?.as_str()?.to_string(),
                    tool_name: item.get("name")?.as_str()?.to_string(),
                    input: compact_value(item.get("input")),
                });
            }
        }
    }

    None
}

fn parse_tool_output(value: &Value) -> Option<(String, bool)> {
    let top_type = value.get("type")?.as_str()?;
    if top_type == "response_item" {
        let payload = value.get("payload")?;
        let item_type = payload.get("type")?.as_str()?;
        if !matches!(item_type, "function_call_output" | "custom_tool_call_output" | "tool_search_output") {
            return None;
        }
        let call_id = payload.get("call_id")?.as_str()?.to_string();
        let output = compact_value(payload.get("output"));
        return Some((call_id, output_failed(&output)));
    }
    if top_type == "user" {
        let content = value.get("message")?.get("content")?.as_array()?;
        for item in content {
            if item.get("type").and_then(Value::as_str) == Some("tool_result") {
                let call_id = item.get("tool_use_id")?.as_str()?.to_string();
                let explicit_error = item.get("is_error").and_then(Value::as_bool).unwrap_or(false);
                let output = compact_value(item.get("content"));
                return Some((call_id, explicit_error || output_failed(&output)));
            }
        }
    }
    None
}

fn output_failed(output: &str) -> bool {
    if let Some(captures) = Regex::new(r"(?i)(?:exit code|Process exited with code)\s+(-?\d+)")
        .unwrap()
        .captures(output)
    {
        return captures
            .get(1)
            .and_then(|m| m.as_str().parse::<i32>().ok())
            .map(|code| code != 0)
            .unwrap_or(false);
    }
    let lowered = output.to_lowercase();
    ["traceback", "permission denied", "command not found", "fatal:", "error:"]
        .iter()
        .any(|pattern| lowered.contains(pattern))
}

pub fn classify_action(tool_name: &str, input: &str) -> ActionKind {
    let tool = tool_name.to_lowercase();
    let input = input.to_lowercase();
    if contains_any(&tool, &["search", "grep", "rg", "glob", "find"])
        || Regex::new(r"\b(rg|grep|find|fd|ls)\b").unwrap().is_match(&input)
    {
        ActionKind::SearchDiscovery
    } else if contains_any(&tool, &["read", "fetch", "cat", "sed"])
        || Regex::new(r"\b(cat|sed|head|tail|nl)\b").unwrap().is_match(&input)
    {
        ActionKind::ReadFetch
    } else if contains_any(&tool, &["apply_patch", "edit", "write"]) {
        ActionKind::WriteEdit
    } else if contains_any(&tool, &["exec", "bash", "shell"]) {
        ActionKind::ShellOther
    } else if contains_any(&tool, &["web", "browser", "chrome"]) {
        ActionKind::Web
    } else if contains_any(&tool, &["todo", "plan"]) {
        ActionKind::Planning
    } else if tool.contains("mcp") {
        ActionKind::Mcp
    } else {
        ActionKind::Other
    }
}

fn contains_any(value: &str, needles: &[&str]) -> bool {
    needles.iter().any(|needle| value.contains(needle))
}

fn compact_value(value: Option<&Value>) -> String {
    match value {
        Some(Value::String(text)) => text.clone(),
        Some(other) => other.to_string(),
        None => String::new(),
    }
}

fn increment<K>(map: &mut HashMap<K, KindMetrics>, key: K, failed: bool)
where
    K: Eq + std::hash::Hash,
{
    let item = map.entry(key).or_default();
    item.calls += 1;
    if failed {
        item.failures += 1;
    }
}
