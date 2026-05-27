use crate::repo_index::RepoIndexer;
use crate::session_metrics::{ScanOptions, scan_jsonl_roots};
use anyhow::{Result, anyhow};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::io::{BufRead, Write};
use std::path::PathBuf;

#[derive(Debug, Deserialize)]
pub struct ToolRequest {
    pub id: Value,
    pub tool: String,
    #[serde(default)]
    pub arguments: Value,
}

#[derive(Debug, Serialize)]
pub struct ToolResponse {
    pub id: Value,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub result: Option<Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

pub fn serve_jsonl(reader: impl BufRead, mut writer: impl Write) -> Result<()> {
    for line in reader.lines() {
        let line = line?;
        if line.trim().is_empty() {
            continue;
        }
        let response = match serde_json::from_str::<ToolRequest>(&line) {
            Ok(request) => dispatch(request),
            Err(error) => ToolResponse {
                id: Value::Null,
                result: None,
                error: Some(error.to_string()),
            },
        };
        writeln!(writer, "{}", serde_json::to_string(&response)?)?;
        writer.flush()?;
    }
    Ok(())
}

pub fn dispatch(request: ToolRequest) -> ToolResponse {
    match dispatch_result(&request) {
        Ok(result) => ToolResponse {
            id: request.id,
            result: Some(result),
            error: None,
        },
        Err(error) => ToolResponse {
            id: request.id,
            result: None,
            error: Some(error.to_string()),
        },
    }
}

fn dispatch_result(request: &ToolRequest) -> Result<Value> {
    match request.tool.as_str() {
        "repo_brief" => {
            let repo = path_arg(&request.arguments, "repo")?;
            let index = RepoIndexer::new(repo).build()?;
            Ok(serde_json::to_value(index.repo_brief())?)
        }
        "search_code" => {
            let repo = path_arg(&request.arguments, "repo")?;
            let query = string_arg(&request.arguments, "query")?;
            let limit = usize_arg(&request.arguments, "limit").unwrap_or(10);
            let index = RepoIndexer::new(repo).build()?;
            Ok(serde_json::to_value(index.search_code(&query, limit))?)
        }
        "find_symbol" => {
            let repo = path_arg(&request.arguments, "repo")?;
            let name = string_arg(&request.arguments, "name")?;
            let limit = usize_arg(&request.arguments, "limit").unwrap_or(10);
            let index = RepoIndexer::new(repo).build()?;
            Ok(serde_json::to_value(index.find_symbol(&name, limit))?)
        }
        "related_files" => {
            let repo = path_arg(&request.arguments, "repo")?;
            let path = string_arg(&request.arguments, "path")?;
            let limit = usize_arg(&request.arguments, "limit").unwrap_or(10);
            let index = RepoIndexer::new(repo).build()?;
            Ok(serde_json::to_value(index.related_files(&path, limit))?)
        }
        "metrics" => {
            let roots = request
                .arguments
                .get("roots")
                .and_then(Value::as_array)
                .map(|values| {
                    values
                        .iter()
                        .filter_map(Value::as_str)
                        .map(PathBuf::from)
                        .collect::<Vec<_>>()
                })
                .unwrap_or_else(|| {
                    vec![
                        path_arg(&request.arguments, "root").unwrap_or_else(|_| PathBuf::from(".")),
                    ]
                });
            let max_files = usize_arg(&request.arguments, "max_files");
            let max_file_bytes =
                usize_arg(&request.arguments, "max_file_mb").map(|mb| mb as u64 * 1024 * 1024);
            Ok(serde_json::to_value(scan_jsonl_roots(ScanOptions {
                roots,
                max_files,
                max_file_bytes,
            })?)?)
        }
        "list_tools" => Ok(json!([
            "repo_brief",
            "search_code",
            "find_symbol",
            "related_files",
            "metrics"
        ])),
        other => Err(anyhow!("unknown tool: {other}")),
    }
}

fn string_arg(arguments: &Value, name: &str) -> Result<String> {
    arguments
        .get(name)
        .and_then(Value::as_str)
        .map(String::from)
        .ok_or_else(|| anyhow!("missing string argument: {name}"))
}

fn path_arg(arguments: &Value, name: &str) -> Result<PathBuf> {
    Ok(PathBuf::from(string_arg(arguments, name)?))
}

fn usize_arg(arguments: &Value, name: &str) -> Option<usize> {
    arguments
        .get(name)
        .and_then(Value::as_u64)
        .map(|value| value as usize)
}
