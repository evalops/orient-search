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
    failed: bool,
}

pub fn scan_jsonl_roots(options: ScanOptions) -> Result<Metrics> {
    let files = discover_jsonl_files(&options)?;
    let mut metrics = Metrics {
        files_scanned: files.len(),
        ..Metrics::default()
    };
    let mut calls: Vec<ToolCall> = Vec::new();

    for path in files {
        let file = File::open(path)?;
        let mut file_calls: Vec<ToolCall> = Vec::new();
        let mut file_failed: HashMap<String, bool> = HashMap::new();
        for line in BufReader::new(file).lines().map_while(Result::ok) {
            let trimmed = line.trim();
            if trimmed.is_empty() || !looks_like_tool_line(trimmed) {
                continue;
            }
            let Ok(value) = serde_json::from_str::<Value>(trimmed) else {
                continue;
            };
            file_calls.extend(parse_tool_calls(&value));
            for (call_id, is_failed) in parse_tool_outputs(&value) {
                file_failed.insert(call_id, is_failed);
            }
        }
        for mut call in file_calls {
            if let Some(is_failed) = file_failed.get(&call.call_id) {
                call.failed = *is_failed;
            }
            calls.push(call);
        }
    }

    for call in &calls {
        let is_failed = call.failed;
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
    files.sort_by(|left, right| {
        let left_modified = left
            .metadata()
            .and_then(|metadata| metadata.modified())
            .ok();
        let right_modified = right
            .metadata()
            .and_then(|metadata| metadata.modified())
            .ok();
        right_modified
            .cmp(&left_modified)
            .then_with(|| left.cmp(right))
    });
    if let Some(max_files) = options.max_files {
        files.truncate(max_files);
    }
    Ok(files)
}

fn looks_like_tool_line(line: &str) -> bool {
    [
        "function_call",
        "custom_tool_call",
        "web_search_call",
        "tool_search_call",
        "function_call_output",
        "custom_tool_call_output",
        "tool_search_output",
        "tool_use",
        "tool_result",
    ]
    .iter()
    .any(|needle| line.contains(needle))
}

fn parse_tool_calls(value: &Value) -> Vec<ToolCall> {
    let Some(top_type) = value.get("type").and_then(Value::as_str) else {
        return Vec::new();
    };
    if top_type == "response_item" {
        let Some(payload) = value.get("payload") else {
            return Vec::new();
        };
        let Some(item_type) = payload.get("type").and_then(Value::as_str) else {
            return Vec::new();
        };
        if !matches!(
            item_type,
            "function_call" | "custom_tool_call" | "web_search_call" | "tool_search_call"
        ) {
            return Vec::new();
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
        return vec![ToolCall {
            call_id,
            tool_name,
            input,
            failed: false,
        }];
    }

    if top_type == "assistant" {
        let Some(content) = value
            .get("message")
            .and_then(|message| message.get("content"))
            .and_then(Value::as_array)
        else {
            return Vec::new();
        };
        return content
            .iter()
            .filter(|item| item.get("type").and_then(Value::as_str) == Some("tool_use"))
            .filter_map(|item| {
                Some(ToolCall {
                    call_id: item.get("id")?.as_str()?.to_string(),
                    tool_name: item.get("name")?.as_str()?.to_string(),
                    input: compact_value(item.get("input")),
                    failed: false,
                })
            })
            .collect();
    }

    Vec::new()
}

fn parse_tool_outputs(value: &Value) -> Vec<(String, bool)> {
    let Some(top_type) = value.get("type").and_then(Value::as_str) else {
        return Vec::new();
    };
    if top_type == "response_item" {
        let Some(payload) = value.get("payload") else {
            return Vec::new();
        };
        let Some(item_type) = payload.get("type").and_then(Value::as_str) else {
            return Vec::new();
        };
        if !matches!(
            item_type,
            "function_call_output" | "custom_tool_call_output" | "tool_search_output"
        ) {
            return Vec::new();
        }
        let Some(call_id) = payload.get("call_id").and_then(Value::as_str) else {
            return Vec::new();
        };
        let output = compact_value(payload.get("output"));
        return vec![(call_id.to_string(), output_failed(&output))];
    }
    if top_type == "user" {
        let Some(content) = value
            .get("message")
            .and_then(|message| message.get("content"))
            .and_then(Value::as_array)
        else {
            return Vec::new();
        };
        return content
            .iter()
            .filter(|item| item.get("type").and_then(Value::as_str) == Some("tool_result"))
            .filter_map(|item| {
                let call_id = item.get("tool_use_id")?.as_str()?.to_string();
                let explicit_error = item
                    .get("is_error")
                    .and_then(Value::as_bool)
                    .unwrap_or(false);
                let output = compact_value(item.get("content"));
                Some((call_id, explicit_error || output_failed(&output)))
            })
            .collect();
    }
    Vec::new()
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
    [
        "traceback",
        "permission denied",
        "command not found",
        "fatal:",
        "error:",
    ]
    .iter()
    .any(|pattern| lowered.contains(pattern))
}

pub fn classify_action(tool_name: &str, input: &str) -> ActionKind {
    let tool = tool_name.to_lowercase();
    let input = input.to_lowercase();
    if contains_any(&tool, &["search", "grep", "rg", "glob", "find"])
        || Regex::new(r"\b(rg|grep|find|fd|ls)\b")
            .unwrap()
            .is_match(&input)
    {
        ActionKind::SearchDiscovery
    } else if contains_any(&tool, &["read", "fetch", "cat", "sed"])
        || Regex::new(r"\b(cat|sed|head|tail|nl)\b")
            .unwrap()
            .is_match(&input)
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
