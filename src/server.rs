use crate::fast_index::FastIndex;
use crate::repo_index::{
    RepoIndexer, SearchFilters, SnippetMode, read_file_range, search_repo_fast_filtered,
};
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
        "repo_map" => {
            let repo = path_arg(&request.arguments, "repo")?;
            let symbol_limit = usize_arg(&request.arguments, "symbols").unwrap_or(50);
            let test_limit = usize_arg(&request.arguments, "tests").unwrap_or(50);
            let index = RepoIndexer::new(repo).build()?;
            Ok(serde_json::to_value(
                index.repo_map(symbol_limit, test_limit),
            )?)
        }
        "read_range" => {
            let repo = path_arg(&request.arguments, "repo")?;
            let path = string_arg(&request.arguments, "path")?;
            let start = usize_arg(&request.arguments, "start").unwrap_or(1);
            let lines = usize_arg(&request.arguments, "lines").unwrap_or(80);
            Ok(serde_json::to_value(read_file_range(
                repo, &path, start, lines,
            )?)?)
        }
        "search_code" => {
            let repo = path_arg(&request.arguments, "repo")?;
            let query = string_arg(&request.arguments, "query")?;
            let limit = usize_arg(&request.arguments, "limit").unwrap_or(10);
            Ok(serde_json::to_value(search_repo_fast_filtered(
                repo,
                &query,
                limit,
                &search_filters(&request.arguments),
            )?)?)
        }
        "indexed_search_code" => {
            let index_path = path_arg(&request.arguments, "index")?;
            let query = string_arg(&request.arguments, "query")?;
            let limit = usize_arg(&request.arguments, "limit").unwrap_or(10);
            let index = FastIndex::load(index_path)?;
            Ok(serde_json::to_value(index.search_filtered(
                &query,
                limit,
                &search_filters(&request.arguments),
            )?)?)
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
        "list_tools" => Ok(json!([
            "repo_brief",
            "repo_map",
            "read_range",
            "search_code",
            "indexed_search_code",
            "find_symbol",
            "related_files"
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

fn optional_string_arg(arguments: &Value, name: &str) -> Option<String> {
    arguments
        .get(name)
        .and_then(Value::as_str)
        .map(String::from)
}

fn search_filters(arguments: &Value) -> SearchFilters {
    SearchFilters {
        path: optional_string_arg(arguments, "path"),
        language: optional_string_arg(arguments, "language"),
        extension: optional_string_arg(arguments, "extension"),
        symbol: optional_string_arg(arguments, "symbol"),
        file: optional_string_arg(arguments, "file"),
        repo: optional_string_arg(arguments, "repo_filter"),
        test: arguments.get("test").and_then(Value::as_bool),
        snippet: optional_string_arg(arguments, "snippet")
            .as_deref()
            .and_then(SnippetMode::parse)
            .unwrap_or_default(),
        require_all: arguments
            .get("require_all")
            .and_then(Value::as_bool)
            .unwrap_or(false),
        ..SearchFilters::default()
    }
}
