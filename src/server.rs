use crate::fast_index::FastIndex;
use crate::repo_index::{
    RepoIndexer, SearchFilters, SnippetMode, read_file_range, search_repo_fast_filtered,
};
use crate::shards::{
    build_shards, find_shard_symbol, read_shard_range, refresh_shards, search_shards,
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

pub fn tool_manifest() -> Value {
    json!([
        {
            "name": "list_tools",
            "description": "Return the available JSON-lines tool names.",
            "required": [],
            "optional": []
        },
        {
            "name": "tool_manifest",
            "description": "Return tool descriptions and argument metadata for agent wrappers.",
            "required": [],
            "optional": []
        },
        {
            "name": "repo_brief",
            "description": "Summarize a local repository with language counts, important files, and known commands.",
            "required": ["repo"],
            "optional": []
        },
        {
            "name": "repo_map",
            "description": "Return entrypoints, tests, top symbols, known commands, and important files for a local repository.",
            "required": ["repo"],
            "optional": ["symbols", "tests"]
        },
        {
            "name": "read_range",
            "description": "Read a bounded line range from a repository-relative path.",
            "required": ["repo", "path"],
            "optional": ["start", "lines"]
        },
        {
            "name": "search_code",
            "description": "Search a local repository with the fast fallback path and return ranked snippets.",
            "required": ["repo", "query"],
            "optional": SEARCH_OPTIONAL_ARGS
        },
        {
            "name": "indexed_search_code",
            "description": "Search a persistent single-repo index and return ranked snippets.",
            "required": ["index", "query"],
            "optional": SEARCH_INDEX_OPTIONAL_ARGS
        },
        {
            "name": "read_index_range",
            "description": "Read a bounded line range from a persistent index result path.",
            "required": ["index", "path"],
            "optional": ["start", "lines"]
        },
        {
            "name": "index_shards",
            "description": "Build a local multi-repo shard directory.",
            "required": ["repos", "output_dir"],
            "optional": []
        },
        {
            "name": "refresh_shards",
            "description": "Refresh every repo index in a local shard directory incrementally.",
            "required": ["index_dir"],
            "optional": []
        },
        {
            "name": "search_shards",
            "description": "Search a local multi-repo shard directory and return repo-prefixed ranked snippets.",
            "required": ["index_dir", "query"],
            "optional": SEARCH_INDEX_OPTIONAL_ARGS
        },
        {
            "name": "read_shard_range",
            "description": "Read a bounded line range from a repo-prefixed shard search result path.",
            "required": ["index_dir", "path"],
            "optional": ["start", "lines"]
        },
        {
            "name": "find_shard_symbol",
            "description": "Find symbol definitions across a local multi-repo shard directory.",
            "required": ["index_dir", "name"],
            "optional": ["limit", "repo", "repo_filter"]
        },
        {
            "name": "find_symbol",
            "description": "Find symbol definitions in a local repository.",
            "required": ["repo", "name"],
            "optional": ["limit"]
        },
        {
            "name": "find_index_symbol",
            "description": "Find symbol definitions directly from a persistent index.",
            "required": ["index", "name"],
            "optional": ["limit"]
        },
        {
            "name": "related_files",
            "description": "Find nearby source/test files related to a repository-relative path.",
            "required": ["repo", "path"],
            "optional": ["limit"]
        },
        {
            "name": "related_index_files",
            "description": "Find nearby source/test files related to an indexed result path.",
            "required": ["index", "path"],
            "optional": ["limit"]
        },
        {
            "name": "related_symbols",
            "description": "Find symbols related to a path and optional query.",
            "required": ["repo"],
            "optional": ["path", "query", "limit"]
        },
        {
            "name": "related_index_symbols",
            "description": "Find symbols related to an indexed path and optional query.",
            "required": ["index"],
            "optional": ["path", "query", "limit"]
        }
    ])
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
                &search_filters(&request.arguments, false),
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
                &search_filters(&request.arguments, true),
            )?)?)
        }
        "read_index_range" => {
            let index_path = path_arg(&request.arguments, "index")?;
            let path = string_arg(&request.arguments, "path")?;
            let start = usize_arg(&request.arguments, "start").unwrap_or(1);
            let lines = usize_arg(&request.arguments, "lines").unwrap_or(80);
            let index = FastIndex::load(index_path)?;
            Ok(serde_json::to_value(read_file_range(
                index.root, &path, start, lines,
            )?)?)
        }
        "index_shards" => {
            let repos = path_array_arg(&request.arguments, "repos")?;
            let output_dir = path_arg(&request.arguments, "output_dir")?;
            Ok(serde_json::to_value(build_shards(&repos, output_dir)?)?)
        }
        "refresh_shards" => {
            let index_dir = path_arg(&request.arguments, "index_dir")?;
            Ok(serde_json::to_value(refresh_shards(index_dir)?)?)
        }
        "search_shards" => {
            let index_dir = path_arg(&request.arguments, "index_dir")?;
            let query = string_arg(&request.arguments, "query")?;
            let limit = usize_arg(&request.arguments, "limit").unwrap_or(10);
            Ok(serde_json::to_value(search_shards(
                index_dir,
                &query,
                limit,
                &search_filters(&request.arguments, true),
            )?)?)
        }
        "read_shard_range" => {
            let index_dir = path_arg(&request.arguments, "index_dir")?;
            let path = string_arg(&request.arguments, "path")?;
            let start = usize_arg(&request.arguments, "start").unwrap_or(1);
            let lines = usize_arg(&request.arguments, "lines").unwrap_or(80);
            Ok(serde_json::to_value(read_shard_range(
                index_dir, &path, start, lines,
            )?)?)
        }
        "find_shard_symbol" => {
            let index_dir = path_arg(&request.arguments, "index_dir")?;
            let name = string_arg(&request.arguments, "name")?;
            let limit = usize_arg(&request.arguments, "limit").unwrap_or(10);
            Ok(serde_json::to_value(find_shard_symbol(
                index_dir,
                &name,
                limit,
                &search_filters(&request.arguments, true),
            )?)?)
        }
        "find_symbol" => {
            let repo = path_arg(&request.arguments, "repo")?;
            let name = string_arg(&request.arguments, "name")?;
            let limit = usize_arg(&request.arguments, "limit").unwrap_or(10);
            let index = RepoIndexer::new(repo).build()?;
            Ok(serde_json::to_value(index.find_symbol(&name, limit))?)
        }
        "find_index_symbol" => {
            let index_path = path_arg(&request.arguments, "index")?;
            let name = string_arg(&request.arguments, "name")?;
            let limit = usize_arg(&request.arguments, "limit").unwrap_or(10);
            let index = FastIndex::load(index_path)?;
            Ok(serde_json::to_value(index.find_symbol(&name, limit))?)
        }
        "related_files" => {
            let repo = path_arg(&request.arguments, "repo")?;
            let path = string_arg(&request.arguments, "path")?;
            let limit = usize_arg(&request.arguments, "limit").unwrap_or(10);
            let index = RepoIndexer::new(repo).build()?;
            Ok(serde_json::to_value(index.related_files(&path, limit))?)
        }
        "related_index_files" => {
            let index_path = path_arg(&request.arguments, "index")?;
            let path = string_arg(&request.arguments, "path")?;
            let limit = usize_arg(&request.arguments, "limit").unwrap_or(10);
            let fast_index = FastIndex::load(index_path)?;
            let index = RepoIndexer::new(fast_index.root).build()?;
            Ok(serde_json::to_value(index.related_files(&path, limit))?)
        }
        "related_symbols" => {
            let repo = path_arg(&request.arguments, "repo")?;
            let path = optional_string_arg(&request.arguments, "path");
            let query = optional_string_arg(&request.arguments, "query");
            let limit = usize_arg(&request.arguments, "limit").unwrap_or(10);
            let index = RepoIndexer::new(repo).build()?;
            Ok(serde_json::to_value(index.related_symbols(
                path.as_deref(),
                query.as_deref(),
                limit,
            ))?)
        }
        "related_index_symbols" => {
            let index_path = path_arg(&request.arguments, "index")?;
            let path = optional_string_arg(&request.arguments, "path");
            let query = optional_string_arg(&request.arguments, "query");
            let limit = usize_arg(&request.arguments, "limit").unwrap_or(10);
            let fast_index = FastIndex::load(index_path)?;
            let index = RepoIndexer::new(fast_index.root).build()?;
            Ok(serde_json::to_value(index.related_symbols(
                path.as_deref(),
                query.as_deref(),
                limit,
            ))?)
        }
        "tool_manifest" => Ok(tool_manifest()),
        "list_tools" => Ok(json!([
            "list_tools",
            "tool_manifest",
            "repo_brief",
            "repo_map",
            "read_range",
            "search_code",
            "indexed_search_code",
            "read_index_range",
            "index_shards",
            "refresh_shards",
            "search_shards",
            "read_shard_range",
            "find_shard_symbol",
            "find_symbol",
            "find_index_symbol",
            "related_files",
            "related_index_files",
            "related_symbols",
            "related_index_symbols"
        ])),
        other => Err(anyhow!("unknown tool: {other}")),
    }
}

const SEARCH_OPTIONAL_ARGS: &[&str] = &[
    "limit",
    "path",
    "language",
    "extension",
    "symbol",
    "file",
    "repo_filter",
    "test",
    "snippet",
    "explain",
    "require_all",
];

const SEARCH_INDEX_OPTIONAL_ARGS: &[&str] = &[
    "limit",
    "path",
    "language",
    "extension",
    "symbol",
    "file",
    "repo",
    "repo_filter",
    "test",
    "snippet",
    "explain",
    "require_all",
];

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

fn path_array_arg(arguments: &Value, name: &str) -> Result<Vec<PathBuf>> {
    let values = arguments
        .get(name)
        .and_then(Value::as_array)
        .ok_or_else(|| anyhow!("missing path array argument: {name}"))?;
    values
        .iter()
        .map(|value| {
            value
                .as_str()
                .map(PathBuf::from)
                .ok_or_else(|| anyhow!("path array argument {name} must contain only strings"))
        })
        .collect()
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

fn optional_string_arg_any(arguments: &Value, names: &[&str]) -> Option<String> {
    names
        .iter()
        .find_map(|name| optional_string_arg(arguments, name))
}

fn search_filters(arguments: &Value, allow_repo_alias: bool) -> SearchFilters {
    SearchFilters {
        path: optional_string_arg(arguments, "path"),
        language: optional_string_arg(arguments, "language"),
        extension: optional_string_arg(arguments, "extension"),
        symbol: optional_string_arg(arguments, "symbol"),
        file: optional_string_arg(arguments, "file"),
        repo: if allow_repo_alias {
            optional_string_arg_any(arguments, &["repo", "repo_filter"])
        } else {
            optional_string_arg(arguments, "repo_filter")
        },
        test: arguments.get("test").and_then(Value::as_bool),
        snippet: optional_string_arg(arguments, "snippet")
            .as_deref()
            .and_then(SnippetMode::parse)
            .unwrap_or_default(),
        explain: arguments
            .get("explain")
            .and_then(Value::as_bool)
            .unwrap_or(false),
        require_all: arguments
            .get("require_all")
            .and_then(Value::as_bool)
            .unwrap_or(false),
        ..SearchFilters::default()
    }
}
