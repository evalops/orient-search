use crate::discover::{DiscoverOptions, discover_repos};
use crate::fast_index::FastIndex;
use crate::query::{merge_filters, parse_query, query_text};
use crate::repo_index::{
    RepoIndexer, SearchFilters, SearchResult, SnippetMode, Symbol, attach_result_context,
    finalize_results, normalize_token, read_file_range, search_repo_fast_filtered,
};
use crate::shards::{
    ShardEntry, ShardManifest, ShardRepoMap, ShardSearchScope, build_shards, ensure_shards,
    filter_repo_map_by_prefix, filters_for_shard_scope, load_manifest, refresh_shards,
    resolve_shard_read_path, shard_query_plans, shard_search_scopes,
};
use anyhow::{Result, anyhow};
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value, json};
use std::collections::HashMap;
use std::io::{BufRead, Write};
use std::net::{TcpListener, TcpStream};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Condvar, Mutex};
use std::thread;

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
    let mut runtime = ToolRuntime::default();
    serve_jsonl_with_runtime(reader, &mut writer, &mut runtime)
}

pub fn serve_jsonl_with_runtime(
    reader: impl BufRead,
    mut writer: impl Write,
    runtime: &mut ToolRuntime,
) -> Result<()> {
    for line in reader.lines() {
        let line = line?;
        if line.trim().is_empty() {
            continue;
        }
        let response = runtime.dispatch_line(&line);
        writeln!(writer, "{}", serde_json::to_string(&response)?)?;
        writer.flush()?;
    }
    Ok(())
}

pub fn serve_tcp(listener: TcpListener, runtime: ToolRuntime) -> Result<()> {
    let runtime = Arc::new(runtime);
    for stream in listener.incoming() {
        let stream = stream?;
        let runtime = Arc::clone(&runtime);
        thread::spawn(move || {
            let _ = serve_tcp_stream(stream, runtime);
        });
    }
    Ok(())
}

fn serve_tcp_stream(stream: TcpStream, runtime: Arc<ToolRuntime>) -> Result<()> {
    let reader = std::io::BufReader::new(stream.try_clone()?);
    let mut writer = stream;
    for line in reader.lines() {
        let line = line?;
        if line.trim().is_empty() {
            continue;
        }
        let response = runtime.dispatch_line(&line);
        writeln!(writer, "{}", serde_json::to_string(&response)?)?;
        writer.flush()?;
    }
    Ok(())
}

pub fn dispatch(request: ToolRequest) -> ToolResponse {
    ToolRuntime::default().dispatch(request)
}

#[derive(Default)]
pub struct ToolRuntime {
    indexes: Mutex<HashMap<PathBuf, Arc<IndexCacheEntry>>>,
    shard_manifests: Mutex<HashMap<PathBuf, Arc<ShardManifest>>>,
}

struct IndexCacheEntry {
    state: Mutex<IndexCacheState>,
    ready: Condvar,
}

enum IndexCacheState {
    Loading,
    Ready(Arc<FastIndex>),
    Failed(String),
}

struct ShardSearchJob {
    shard: ShardEntry,
    scopes: Vec<ShardSearchScope>,
}

impl IndexCacheEntry {
    fn loading() -> Self {
        Self {
            state: Mutex::new(IndexCacheState::Loading),
            ready: Condvar::new(),
        }
    }

    fn is_ready(&self) -> bool {
        self.state
            .lock()
            .map(|state| matches!(*state, IndexCacheState::Ready(_)))
            .unwrap_or(false)
    }
}

impl ToolRuntime {
    pub fn warm_index(&self, index_path: PathBuf) -> Result<PathBuf> {
        let (key, _) = self.cached_index_with_key(index_path)?;
        Ok(key)
    }

    pub fn warm_shards(&self, index_dir: PathBuf) -> Result<usize> {
        let manifest = self.cached_shard_manifest(&index_dir)?;
        let mut warmed = 0usize;
        for shard in &manifest.shards {
            self.warm_index(index_dir.join(&shard.index))?;
            warmed += 1;
        }
        Ok(warmed)
    }

    pub fn cached_index_count(&self) -> usize {
        self.indexes
            .lock()
            .map(|indexes| indexes.values().filter(|entry| entry.is_ready()).count())
            .unwrap_or(0)
    }

    pub fn cached_shard_manifest_count(&self) -> usize {
        self.shard_manifests
            .lock()
            .map(|manifests| manifests.len())
            .unwrap_or(0)
    }

    pub fn daemon_status(&self) -> Value {
        json!({
            "cached_indexes": self.cached_index_count(),
            "cached_index_paths": self.cached_index_paths(),
            "cached_shard_manifests": self.cached_shard_manifest_count(),
            "cached_shard_manifest_paths": self.cached_shard_manifest_paths(),
            "cached_shard_manifest_details": self.cached_shard_manifest_details()
        })
    }

    pub fn dispatch_line(&self, line: &str) -> ToolResponse {
        match serde_json::from_str::<ToolRequest>(line) {
            Ok(request) => self.dispatch(request),
            Err(error) => ToolResponse {
                id: Value::Null,
                result: None,
                error: Some(error.to_string()),
            },
        }
    }

    pub fn dispatch(&self, request: ToolRequest) -> ToolResponse {
        match self.dispatch_result(&request) {
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
}

pub fn tool_manifest() -> Value {
    Value::Array(vec![
        tool_entry(
            "list_tools",
            "Return the available JSON-lines tool names.",
            &[],
            &[],
        ),
        tool_entry(
            "tool_manifest",
            "Return tool descriptions and argument metadata for agent wrappers.",
            &[],
            &[],
        ),
        tool_entry(
            "daemon_status",
            "Return local daemon runtime cache status for warm-index clients.",
            &[],
            &[],
        ),
        tool_entry(
            "warm_index",
            "Load a persistent single-repo index into the daemon cache before searches need it.",
            &["index"],
            &[],
        ),
        tool_entry(
            "warm_shards",
            "Load every shard index from a local shard directory into the daemon cache.",
            &["index_dir"],
            &[],
        ),
        tool_entry(
            "discover_repos",
            "Discover local repo roots under a broad workspace for shard setup.",
            &["root"],
            &["max_depth", "limit", "git_metadata", "tracked_files"],
        ),
        tool_entry(
            "repo_brief",
            "Summarize a local repository with language counts, important files, and known commands.",
            &["repo"],
            &[],
        ),
        tool_entry(
            "repo_map",
            "Return entrypoints, tests, top symbols, known commands, and important files for a local repository.",
            &["repo"],
            &["symbols", "tests"],
        ),
        tool_entry(
            "indexed_repo_map",
            "Return repo-map orientation from a persistent single-repo index.",
            &["index"],
            &["symbols", "tests"],
        ),
        tool_entry(
            "read_range",
            "Read a bounded line range from a repository-relative path.",
            &["repo", "path"],
            &["start", "lines"],
        ),
        tool_entry(
            "read_ranges",
            "Read several bounded line ranges from repository-relative paths in one request.",
            &["repo", "ranges"],
            &[],
        ),
        tool_entry(
            "search_code",
            "Search a local repository with the fast fallback path and return ranked snippets.",
            &["repo", "query"],
            SEARCH_OPTIONAL_ARGS,
        ),
        tool_entry(
            "indexed_search_code",
            "Search a persistent single-repo index and return ranked snippets.",
            &["index", "query"],
            SEARCH_INDEX_OPTIONAL_ARGS,
        ),
        tool_entry(
            "indexed_query_plan",
            "Return the indexed query plan, including missing postings, even when search has no hits.",
            &["index", "query"],
            PLAN_INDEX_OPTIONAL_ARGS,
        ),
        tool_entry(
            "read_index_range",
            "Read a bounded line range from a persistent index result path.",
            &["index", "path"],
            &["start", "lines"],
        ),
        tool_entry(
            "read_index_ranges",
            "Read several bounded line ranges from persistent index result paths in one request.",
            &["index", "ranges"],
            &[],
        ),
        tool_entry(
            "index_shards",
            "Build a local multi-repo shard directory from explicit repos or a discovered workspace root.",
            &["output_dir"],
            SHARD_BUILD_OPTIONAL_ARGS,
        ),
        tool_entry(
            "ensure_shards",
            "Build or refresh a local multi-repo shard directory, then warm its indexes in the daemon cache.",
            &["output_dir"],
            SHARD_BUILD_OPTIONAL_ARGS,
        ),
        tool_entry(
            "refresh_shards",
            "Refresh every repo index in a local shard directory incrementally.",
            &["index_dir"],
            &[],
        ),
        tool_entry(
            "search_shards",
            "Search a local multi-repo shard directory and return repo-prefixed ranked snippets.",
            &["index_dir", "query"],
            SEARCH_INDEX_OPTIONAL_ARGS,
        ),
        tool_entry(
            "shard_query_plan",
            "Return indexed query plans for every matching shard repo or alias.",
            &["index_dir", "query"],
            PLAN_INDEX_OPTIONAL_ARGS,
        ),
        tool_entry(
            "read_shard_range",
            "Read a bounded line range from a repo-prefixed shard search result path.",
            &["index_dir", "path"],
            &["start", "lines"],
        ),
        tool_entry(
            "read_shard_ranges",
            "Read several bounded line ranges from repo-prefixed shard result paths in one request.",
            &["index_dir", "ranges"],
            &[],
        ),
        tool_entry(
            "shard_repo_map",
            "Return repo-map orientation for every matching repo in a local shard directory.",
            &["index_dir"],
            &["symbols", "tests", "repo", "repo_filter"],
        ),
        tool_entry(
            "find_shard_symbol",
            "Find symbol definitions across a local multi-repo shard directory.",
            &["index_dir", "name"],
            &["limit", "repo", "repo_filter"],
        ),
        tool_entry(
            "find_symbol",
            "Find symbol definitions in a local repository.",
            &["repo", "name"],
            &["limit"],
        ),
        tool_entry(
            "find_index_symbol",
            "Find symbol definitions directly from a persistent index.",
            &["index", "name"],
            &["limit"],
        ),
        tool_entry(
            "related_files",
            "Find nearby source/test files related to a repository-relative path.",
            &["repo", "path"],
            &["limit"],
        ),
        tool_entry(
            "related_index_files",
            "Find nearby source/test files related to an indexed result path.",
            &["index", "path"],
            &["limit"],
        ),
        tool_entry(
            "related_shard_files",
            "Find nearby source/test files related to a shard result path.",
            &["index_dir", "path"],
            &["limit"],
        ),
        tool_entry(
            "related_symbols",
            "Find symbols related to a path and optional query.",
            &["repo"],
            &["path", "query", "limit"],
        ),
        tool_entry(
            "related_index_symbols",
            "Find symbols related to an indexed path and optional query.",
            &["index"],
            &["path", "query", "limit"],
        ),
        tool_entry(
            "related_shard_symbols",
            "Find symbols related to a shard result path and optional query.",
            &["index_dir", "path"],
            &["query", "limit"],
        ),
    ])
}

fn tool_entry(name: &str, description: &str, required: &[&str], optional: &[&str]) -> Value {
    json!({
        "name": name,
        "description": description,
        "required": required,
        "optional": optional,
        "arguments": argument_metadata(name, required, optional),
        "input_schema": input_schema(name, required, optional)
    })
}

fn argument_metadata(tool_name: &str, required: &[&str], optional: &[&str]) -> Vec<Value> {
    required
        .iter()
        .map(|name| argument_metadata_entry(tool_name, name, true))
        .chain(
            optional
                .iter()
                .map(|name| argument_metadata_entry(tool_name, name, false)),
        )
        .collect()
}

fn argument_metadata_entry(tool_name: &str, name: &str, required: bool) -> Value {
    let mut entry = Map::new();
    entry.insert("name".to_string(), json!(name));
    entry.insert("required".to_string(), json!(required));
    entry.insert("type".to_string(), json!(argument_type(name)));
    entry.insert("description".to_string(), json!(argument_description(name)));
    if let Some(default) = argument_default(tool_name, name) {
        entry.insert("default".to_string(), default);
    }
    if let Some(values) = argument_enum(name) {
        entry.insert("enum".to_string(), json!(values));
    }
    Value::Object(entry)
}

fn input_schema(tool_name: &str, required: &[&str], optional: &[&str]) -> Value {
    let mut properties = Map::new();
    for name in required.iter().chain(optional.iter()) {
        properties.insert((*name).to_string(), argument_schema(tool_name, name));
    }
    json!({
        "type": "object",
        "additionalProperties": false,
        "required": required,
        "properties": properties
    })
}

fn argument_schema(tool_name: &str, name: &str) -> Value {
    let mut schema = Map::new();
    match name {
        "exclude_file" | "exclude_path" | "exclude_language" | "exclude_extension"
        | "exclude_symbol" | "exclude_repo" => {
            schema.insert(
                "oneOf".to_string(),
                json!([
                    {"type": "string"},
                    {"type": "array", "items": {"type": "string"}}
                ]),
            );
        }
        "ranges" => {
            schema.insert("type".to_string(), json!("array"));
            schema.insert(
                "items".to_string(),
                json!({
                    "type": "object",
                    "required": ["path"],
                    "properties": {
                        "path": {"type": "string"},
                        "start": {"type": "integer", "minimum": 1, "default": 1},
                        "lines": {"type": "integer", "minimum": 1, "default": 80}
                    }
                }),
            );
        }
        "repos" | "discover_roots" => {
            schema.insert("type".to_string(), json!("array"));
            schema.insert("items".to_string(), json!({"type": "string"}));
        }
        "test" | "explain" | "require_all" | "git_metadata" | "tracked_files" => {
            schema.insert("type".to_string(), json!("boolean"));
        }
        "limit" | "max_depth" | "discover_limit" | "symbols" | "start" | "lines" | "tests"
        | "context_lines" => {
            schema.insert("type".to_string(), json!("integer"));
            schema.insert(
                "minimum".to_string(),
                json!(if name == "context_lines" { 0 } else { 1 }),
            );
        }
        _ => {
            schema.insert("type".to_string(), json!("string"));
        }
    }
    schema.insert("description".to_string(), json!(argument_description(name)));
    if let Some(default) = argument_default(tool_name, name) {
        schema.insert("default".to_string(), default);
    }
    if let Some(values) = argument_enum(name) {
        schema.insert("enum".to_string(), json!(values));
    }
    Value::Object(schema)
}

fn argument_type(name: &str) -> &'static str {
    match name {
        "limit" | "max_depth" | "discover_limit" | "symbols" | "start" | "lines" | "tests"
        | "context_lines" => "integer",
        "test" | "explain" | "require_all" | "git_metadata" | "tracked_files" => "boolean",
        "exclude_file" | "exclude_path" | "exclude_language" | "exclude_extension"
        | "exclude_symbol" | "exclude_repo" => "string|string[]",
        "ranges" => "range[]",
        "repos" | "discover_roots" => "string[]",
        _ => "string",
    }
}

fn argument_default(tool_name: &str, name: &str) -> Option<Value> {
    match (tool_name, name) {
        ("discover_repos", "limit") | ("index_shards" | "ensure_shards", "limit") => {
            Some(json!(500))
        }
        (_, "limit") => Some(json!(10)),
        (_, "max_depth") => Some(json!(4)),
        (_, "discover_limit") => Some(json!(500)),
        (_, "symbols" | "tests") => Some(json!(50)),
        (_, "start") => Some(json!(1)),
        (_, "lines") => Some(json!(80)),
        (_, "snippet") => Some(json!("medium")),
        (_, "context_lines") => Some(json!(0)),
        (_, "explain" | "require_all" | "git_metadata" | "tracked_files") => Some(json!(false)),
        _ => None,
    }
}

fn argument_enum(name: &str) -> Option<&'static [&'static str]> {
    match name {
        "snippet" => Some(&["short", "medium", "block", "symbol"]),
        _ => None,
    }
}

fn argument_description(name: &str) -> &'static str {
    match name {
        "repo" => "Local repository root or shard repo filter, depending on the tool.",
        "repo_filter" => "Repository name filter when repo is already used as a root path.",
        "index" => "Path to a persistent single-repo Orient index.",
        "index_dir" => "Path to a local multi-repo shard directory.",
        "output_dir" => "Directory where shard indexes and manifest.json should be written.",
        "query" => "Agent query string with filters, quoted phrases, and normal search terms.",
        "path" => "Repository-relative, index-relative, or shard-prefixed result path.",
        "ranges" => "Array of {path,start,lines} objects for batch range reads.",
        "limit" => "Maximum number of results to return.",
        "language" => "Detected language filter, such as rust, python, or typescript.",
        "extension" => "File extension filter with or without a leading dot.",
        "symbol" => "Symbol name to require or boost.",
        "file" => "File basename substring filter.",
        "test" => "When true, include only test paths; when false, exclude test paths.",
        "snippet" => "Snippet mode: short, medium, block, or symbol.",
        "explain" => "Include structured rank signals and indexed query plans.",
        "require_all" => "Require all normalized query tokens to appear in each result.",
        "context_lines" => "Attach this many bounded line-numbered context lines per result.",
        "exclude_file" => "File basename substring or list of substrings to exclude.",
        "exclude_path" => "Path substring or list of substrings to exclude.",
        "exclude_language" => "Language or list of languages to exclude.",
        "exclude_extension" => "Extension or list of extensions to exclude.",
        "exclude_symbol" => "Symbol name or list of symbols to exclude.",
        "exclude_repo" => "Repository name substring or list of substrings to exclude.",
        "root" | "discover_root" => "Workspace root to scan for repositories.",
        "discover_roots" => "Workspace roots to scan for repositories.",
        "repos" => "Explicit repository roots to add to a shard directory.",
        "max_depth" => "Maximum directory depth for repository discovery.",
        "discover_limit" => "Maximum discovered repositories to add when building shards.",
        "git_metadata" => {
            "Include git origin, branch, common git dir, clone/worktree kind, and repo-family groups in discovery results."
        }
        "tracked_files" => {
            "Include git tracked-file counts in discovery metadata and repo-family groups."
        }
        "symbols" => "Maximum top symbols to include in repo maps.",
        "tests" => "Maximum test files to include in repo maps.",
        "start" => "One-based start line for range reads.",
        "lines" => "Number of lines to read.",
        "name" => "Symbol name or search-like symbol query.",
        _ => "Tool argument.",
    }
}

impl ToolRuntime {
    fn dispatch_result(&self, request: &ToolRequest) -> Result<Value> {
        match request.tool.as_str() {
            "discover_repos" => {
                let root = path_arg(&request.arguments, "root")?;
                let max_depth = usize_arg(&request.arguments, "max_depth").unwrap_or(4);
                let limit = usize_arg(&request.arguments, "limit").unwrap_or(500);
                let git_metadata = request
                    .arguments
                    .get("git_metadata")
                    .and_then(Value::as_bool)
                    .unwrap_or(false);
                let tracked_files = request
                    .arguments
                    .get("tracked_files")
                    .and_then(Value::as_bool)
                    .unwrap_or(false);
                Ok(serde_json::to_value(discover_repos(
                    root,
                    &DiscoverOptions {
                        max_depth,
                        limit,
                        git_metadata,
                        tracked_files,
                    },
                )?)?)
            }
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
            "indexed_repo_map" => {
                let index_path = path_arg(&request.arguments, "index")?;
                let symbol_limit = usize_arg(&request.arguments, "symbols").unwrap_or(50);
                let test_limit = usize_arg(&request.arguments, "tests").unwrap_or(50);
                let index = self.cached_index(index_path)?;
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
            "read_ranges" => {
                let repo = path_arg(&request.arguments, "repo")?;
                let ranges = range_args(&request.arguments)?;
                let mut results = Vec::new();
                for range in ranges {
                    results.push(read_file_range(
                        &repo,
                        &range.path,
                        range.start,
                        range.lines,
                    )?);
                }
                Ok(serde_json::to_value(results)?)
            }
            "search_code" => {
                let repo = path_arg(&request.arguments, "repo")?;
                let query = string_arg(&request.arguments, "query")?;
                let limit = usize_arg(&request.arguments, "limit").unwrap_or(10);
                let context_lines = usize_arg(&request.arguments, "context_lines").unwrap_or(0);
                let mut results = search_repo_fast_filtered(
                    &repo,
                    &query,
                    limit,
                    &search_filters(&request.arguments, false)?,
                )?;
                attach_result_context(&mut results, context_lines, |path, start, lines| {
                    read_file_range(&repo, path, start, lines)
                })?;
                Ok(serde_json::to_value(results)?)
            }
            "indexed_search_code" => {
                let index_path = path_arg(&request.arguments, "index")?;
                let query = string_arg(&request.arguments, "query")?;
                let limit = usize_arg(&request.arguments, "limit").unwrap_or(10);
                let context_lines = usize_arg(&request.arguments, "context_lines").unwrap_or(0);
                let index = self.cached_index(index_path)?;
                let mut results = index.search_filtered(
                    &query,
                    limit,
                    &search_filters(&request.arguments, true)?,
                )?;
                attach_result_context(&mut results, context_lines, |path, start, lines| {
                    index.read_range(path, start, lines)
                })?;
                Ok(serde_json::to_value(results)?)
            }
            "indexed_query_plan" => {
                let index_path = path_arg(&request.arguments, "index")?;
                let query = string_arg(&request.arguments, "query")?;
                let index = self.cached_index(index_path)?;
                Ok(serde_json::to_value(index.query_plan(
                    &query,
                    &search_filters(&request.arguments, true)?,
                )?)?)
            }
            "read_index_range" => {
                let index_path = path_arg(&request.arguments, "index")?;
                let path = string_arg(&request.arguments, "path")?;
                let start = usize_arg(&request.arguments, "start").unwrap_or(1);
                let lines = usize_arg(&request.arguments, "lines").unwrap_or(80);
                let index = self.cached_index(index_path)?;
                Ok(serde_json::to_value(
                    index.read_range(&path, start, lines)?,
                )?)
            }
            "read_index_ranges" => {
                let index_path = path_arg(&request.arguments, "index")?;
                let ranges = range_args(&request.arguments)?;
                let index = self.cached_index(index_path)?;
                let mut results = Vec::new();
                for range in ranges {
                    results.push(index.read_range(&range.path, range.start, range.lines)?);
                }
                Ok(serde_json::to_value(results)?)
            }
            "index_shards" => {
                let repos = shard_repos_from_arguments_required(&request.arguments)?;
                let output_dir = path_arg(&request.arguments, "output_dir")?;
                let stats = build_shards(&repos, output_dir)?;
                self.clear_runtime_caches()?;
                Ok(serde_json::to_value(stats)?)
            }
            "ensure_shards" => {
                let repos = shard_repos_from_arguments(&request.arguments)?;
                let output_dir = path_arg(&request.arguments, "output_dir")?;
                let stats = ensure_shards(&repos, &output_dir)?;
                self.clear_runtime_caches()?;
                let warmed_indexes = self.warm_shards(output_dir)?;
                Ok(json!({
                    "stats": stats,
                    "warmed_indexes": warmed_indexes,
                    "cached_indexes": self.cached_index_count()
                }))
            }
            "refresh_shards" => {
                let index_dir = path_arg(&request.arguments, "index_dir")?;
                let stats = refresh_shards(index_dir)?;
                self.clear_runtime_caches()?;
                Ok(serde_json::to_value(stats)?)
            }
            "search_shards" => {
                let index_dir = path_arg(&request.arguments, "index_dir")?;
                let query = string_arg(&request.arguments, "query")?;
                let limit = usize_arg(&request.arguments, "limit").unwrap_or(10);
                let context_lines = usize_arg(&request.arguments, "context_lines").unwrap_or(0);
                Ok(serde_json::to_value(self.search_shards_cached(
                    &index_dir,
                    &query,
                    limit,
                    &search_filters(&request.arguments, true)?,
                    context_lines,
                )?)?)
            }
            "shard_query_plan" => {
                let index_dir = path_arg(&request.arguments, "index_dir")?;
                let query = string_arg(&request.arguments, "query")?;
                Ok(serde_json::to_value(shard_query_plans(
                    &index_dir,
                    &query,
                    &search_filters(&request.arguments, true)?,
                )?)?)
            }
            "read_shard_range" => {
                let index_dir = path_arg(&request.arguments, "index_dir")?;
                let path = string_arg(&request.arguments, "path")?;
                let start = usize_arg(&request.arguments, "start").unwrap_or(1);
                let lines = usize_arg(&request.arguments, "lines").unwrap_or(80);
                Ok(serde_json::to_value(self.read_shard_range_cached(
                    &index_dir, &path, start, lines,
                )?)?)
            }
            "read_shard_ranges" => {
                let index_dir = path_arg(&request.arguments, "index_dir")?;
                let ranges = range_args(&request.arguments)?;
                let mut results = Vec::new();
                for range in ranges {
                    results.push(self.read_shard_range_cached(
                        &index_dir,
                        &range.path,
                        range.start,
                        range.lines,
                    )?);
                }
                Ok(serde_json::to_value(results)?)
            }
            "shard_repo_map" => {
                let index_dir = path_arg(&request.arguments, "index_dir")?;
                let symbol_limit = usize_arg(&request.arguments, "symbols").unwrap_or(50);
                let test_limit = usize_arg(&request.arguments, "tests").unwrap_or(50);
                Ok(serde_json::to_value(self.shard_repo_maps_cached(
                    &index_dir,
                    symbol_limit,
                    test_limit,
                    &search_filters(&request.arguments, true)?,
                )?)?)
            }
            "find_shard_symbol" => {
                let index_dir = path_arg(&request.arguments, "index_dir")?;
                let name = string_arg(&request.arguments, "name")?;
                let limit = usize_arg(&request.arguments, "limit").unwrap_or(10);
                Ok(serde_json::to_value(self.find_shard_symbol_cached(
                    &index_dir,
                    &name,
                    limit,
                    &search_filters(&request.arguments, true)?,
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
                let index = self.cached_index(index_path)?;
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
                let index = self.cached_index(index_path)?;
                Ok(serde_json::to_value(index.related_files(&path, limit))?)
            }
            "related_shard_files" => {
                let index_dir = path_arg(&request.arguments, "index_dir")?;
                let path = string_arg(&request.arguments, "path")?;
                let limit = usize_arg(&request.arguments, "limit").unwrap_or(10);
                Ok(serde_json::to_value(
                    self.related_shard_files_cached(&index_dir, &path, limit)?,
                )?)
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
            "related_shard_symbols" => {
                let index_dir = path_arg(&request.arguments, "index_dir")?;
                let path = string_arg(&request.arguments, "path")?;
                let query = optional_string_arg(&request.arguments, "query");
                let limit = usize_arg(&request.arguments, "limit").unwrap_or(10);
                Ok(serde_json::to_value(self.related_shard_symbols_cached(
                    &index_dir,
                    &path,
                    query.as_deref(),
                    limit,
                )?)?)
            }
            "related_index_symbols" => {
                let index_path = path_arg(&request.arguments, "index")?;
                let path = optional_string_arg(&request.arguments, "path");
                let query = optional_string_arg(&request.arguments, "query");
                let limit = usize_arg(&request.arguments, "limit").unwrap_or(10);
                let index = self.cached_index(index_path)?;
                Ok(serde_json::to_value(index.related_symbols(
                    path.as_deref(),
                    query.as_deref(),
                    limit,
                ))?)
            }
            "warm_index" => {
                let index_path = path_arg(&request.arguments, "index")?;
                let key = self.warm_index(index_path)?;
                Ok(json!({
                    "cached_indexes": self.cached_index_count(),
                    "index": key
                }))
            }
            "warm_shards" => {
                let index_dir = path_arg(&request.arguments, "index_dir")?;
                let index_dir = canonical_cache_key(&index_dir);
                let warmed_indexes = self.warm_shards(index_dir.clone())?;
                Ok(json!({
                    "cached_indexes": self.cached_index_count(),
                    "warmed_indexes": warmed_indexes,
                    "warmed_shards": self.shard_manifest_detail(&index_dir)
                }))
            }
            "daemon_status" => Ok(self.daemon_status()),
            "tool_manifest" => Ok(tool_manifest()),
            "list_tools" => Ok(json!([
                "list_tools",
                "tool_manifest",
                "daemon_status",
                "warm_index",
                "warm_shards",
                "discover_repos",
                "repo_brief",
                "repo_map",
                "indexed_repo_map",
                "read_range",
                "read_ranges",
                "search_code",
                "indexed_search_code",
                "indexed_query_plan",
                "read_index_range",
                "read_index_ranges",
                "index_shards",
                "ensure_shards",
                "refresh_shards",
                "search_shards",
                "shard_query_plan",
                "read_shard_range",
                "read_shard_ranges",
                "shard_repo_map",
                "find_shard_symbol",
                "find_symbol",
                "find_index_symbol",
                "related_files",
                "related_index_files",
                "related_shard_files",
                "related_symbols",
                "related_index_symbols",
                "related_shard_symbols"
            ])),
            other => Err(anyhow!("unknown tool: {other}")),
        }
    }

    fn cached_index(&self, index_path: PathBuf) -> Result<Arc<FastIndex>> {
        Ok(self.cached_index_with_key(index_path)?.1)
    }

    fn cached_index_with_key(&self, index_path: PathBuf) -> Result<(PathBuf, Arc<FastIndex>)> {
        let key = canonical_cache_key(&index_path);
        let (entry, should_load) = {
            let mut indexes = self
                .indexes
                .lock()
                .map_err(|_| anyhow!("index cache lock poisoned"))?;
            if let Some(entry) = indexes.get(&key) {
                (Arc::clone(entry), false)
            } else {
                let entry = Arc::new(IndexCacheEntry::loading());
                indexes.insert(key.clone(), Arc::clone(&entry));
                (entry, true)
            }
        };

        if should_load {
            let loaded = FastIndex::load(&index_path).map(Arc::new);
            let result = match loaded {
                Ok(index) => {
                    *entry
                        .state
                        .lock()
                        .map_err(|_| anyhow!("index cache entry lock poisoned"))? =
                        IndexCacheState::Ready(Arc::clone(&index));
                    Ok((key.clone(), index))
                }
                Err(error) => {
                    let message = error.to_string();
                    *entry
                        .state
                        .lock()
                        .map_err(|_| anyhow!("index cache entry lock poisoned"))? =
                        IndexCacheState::Failed(message.clone());
                    Err(anyhow!(message))
                }
            };
            entry.ready.notify_all();
            if result.is_err() {
                let mut indexes = self
                    .indexes
                    .lock()
                    .map_err(|_| anyhow!("index cache lock poisoned"))?;
                if indexes
                    .get(&key)
                    .is_some_and(|cached| Arc::ptr_eq(cached, &entry))
                {
                    indexes.remove(&key);
                }
            }
            return result;
        }

        let mut state = entry
            .state
            .lock()
            .map_err(|_| anyhow!("index cache entry lock poisoned"))?;
        loop {
            match &*state {
                IndexCacheState::Ready(index) => return Ok((key, Arc::clone(index))),
                IndexCacheState::Failed(message) => return Err(anyhow!(message.clone())),
                IndexCacheState::Loading => {
                    state = entry
                        .ready
                        .wait(state)
                        .map_err(|_| anyhow!("index cache entry lock poisoned"))?;
                }
            }
        }
    }

    fn cached_index_paths(&self) -> Vec<String> {
        let mut paths = self
            .indexes
            .lock()
            .map(|indexes| {
                indexes
                    .iter()
                    .filter_map(|(path, entry)| {
                        entry.is_ready().then(|| path.to_string_lossy().to_string())
                    })
                    .collect::<Vec<_>>()
            })
            .unwrap_or_default();
        paths.sort();
        paths
    }

    fn cached_shard_manifest(&self, index_dir: &Path) -> Result<Arc<ShardManifest>> {
        let key = canonical_cache_key(index_dir);
        if let Some(manifest) = self
            .shard_manifests
            .lock()
            .map_err(|_| anyhow!("shard manifest cache lock poisoned"))?
            .get(&key)
            .cloned()
        {
            return Ok(manifest);
        }

        let manifest = Arc::new(load_manifest(index_dir)?);
        self.shard_manifests
            .lock()
            .map_err(|_| anyhow!("shard manifest cache lock poisoned"))?
            .entry(key)
            .or_insert_with(|| Arc::clone(&manifest));
        Ok(manifest)
    }

    fn cached_shard_manifest_paths(&self) -> Vec<String> {
        let mut paths = self
            .shard_manifests
            .lock()
            .map(|manifests| {
                manifests
                    .keys()
                    .map(|path| path.to_string_lossy().to_string())
                    .collect::<Vec<_>>()
            })
            .unwrap_or_default();
        paths.sort();
        paths
    }

    fn cached_shard_manifest_details(&self) -> Vec<Value> {
        let mut details = self
            .shard_manifests
            .lock()
            .map(|manifests| {
                manifests
                    .iter()
                    .map(|(path, manifest)| shard_manifest_detail(path, manifest))
                    .collect::<Vec<_>>()
            })
            .unwrap_or_default();
        details.sort_by(|left, right| {
            left.get("index_dir")
                .and_then(Value::as_str)
                .cmp(&right.get("index_dir").and_then(Value::as_str))
        });
        details
    }

    fn shard_manifest_detail(&self, index_dir: &Path) -> Value {
        let key = canonical_cache_key(index_dir);
        self.shard_manifests
            .lock()
            .ok()
            .and_then(|manifests| manifests.get(&key).cloned())
            .map(|manifest| shard_manifest_detail(&key, &manifest))
            .unwrap_or_else(|| {
                json!({
                    "index_dir": key.to_string_lossy(),
                    "shards": 0,
                    "repos": []
                })
            })
    }

    fn resolve_shard_path_cached(
        &self,
        index_dir: &Path,
        path: &str,
    ) -> Result<crate::shards::ResolvedShardRead> {
        let manifest = self.cached_shard_manifest(index_dir)?;
        let (prefix, relative_path) = path
            .split_once('/')
            .ok_or_else(|| anyhow!("shard path must be '<repo>/<path>'"))?;
        resolve_shard_read_path(&manifest, prefix, relative_path)
            .ok_or_else(|| anyhow!("unknown shard or alias: {prefix}"))
    }

    fn clear_runtime_caches(&self) -> Result<()> {
        self.indexes
            .lock()
            .map_err(|_| anyhow!("index cache lock poisoned"))?
            .clear();
        self.shard_manifests
            .lock()
            .map_err(|_| anyhow!("shard manifest cache lock poisoned"))?
            .clear();
        Ok(())
    }

    fn search_shards_cached(
        &self,
        index_dir: &std::path::Path,
        query: &str,
        limit: usize,
        filters: &SearchFilters,
        context_lines: usize,
    ) -> Result<Vec<SearchResult>> {
        let manifest = self.cached_shard_manifest(index_dir)?;
        let parsed = parse_query(query);
        let filters = merge_filters(filters.clone(), parsed.filters);
        let shard_query = query_text(&parsed.terms, &filters);
        let jobs = manifest
            .shards
            .iter()
            .cloned()
            .filter_map(|shard| {
                let scopes = shard_search_scopes(&shard, &filters);
                (!scopes.is_empty()).then_some(ShardSearchJob { shard, scopes })
            })
            .collect::<Vec<_>>();
        let results =
            self.search_shard_jobs_cached(index_dir, &shard_query, limit, &filters, jobs)?;
        let mut results = finalize_results(results, limit);
        attach_result_context(&mut results, context_lines, |path, start, lines| {
            self.read_shard_range_cached(index_dir, path, start, lines)
        })?;
        Ok(results)
    }

    fn search_shard_jobs_cached(
        &self,
        index_dir: &std::path::Path,
        query: &str,
        limit: usize,
        filters: &SearchFilters,
        jobs: Vec<ShardSearchJob>,
    ) -> Result<Vec<SearchResult>> {
        if jobs.is_empty() || limit == 0 {
            return Ok(Vec::new());
        }

        let workers = std::thread::available_parallelism()
            .map(|count| count.get())
            .unwrap_or(1)
            .min(jobs.len());
        if workers <= 1 {
            return self.search_shard_job_batch_cached(index_dir, query, limit, filters, &jobs);
        }

        let chunk_size = jobs.len().div_ceil(workers);
        let mut results = Vec::new();
        thread::scope(|scope| {
            let handles = jobs
                .chunks(chunk_size)
                .map(|chunk| {
                    scope.spawn(move || {
                        self.search_shard_job_batch_cached(index_dir, query, limit, filters, chunk)
                    })
                })
                .collect::<Vec<_>>();

            for handle in handles {
                let batch = handle
                    .join()
                    .map_err(|_| anyhow!("shard search worker panicked"))??;
                results.extend(batch);
            }
            Ok::<(), anyhow::Error>(())
        })?;
        Ok(results)
    }

    fn search_shard_job_batch_cached(
        &self,
        index_dir: &std::path::Path,
        query: &str,
        limit: usize,
        filters: &SearchFilters,
        jobs: &[ShardSearchJob],
    ) -> Result<Vec<SearchResult>> {
        let mut results = Vec::new();
        for job in jobs {
            let index = self.cached_index(index_dir.join(&job.shard.index))?;
            for scope in &job.scopes {
                let scoped_filters = filters_for_shard_scope(filters, scope.path_prefix.as_deref());
                for mut result in index.search_filtered(query, limit, &scoped_filters)? {
                    if let Some(prefix) = &scope.path_prefix {
                        if !result.path.starts_with(prefix) {
                            continue;
                        }
                    }
                    result.path = scoped_output_path(scope, &result.path);
                    result.reason = format!("shard:{}; {}", scope.output_prefix, result.reason);
                    results.push(result);
                }
            }
        }
        Ok(results)
    }

    fn read_shard_range_cached(
        &self,
        index_dir: &std::path::Path,
        path: &str,
        start: usize,
        lines: usize,
    ) -> Result<crate::repo_index::FileRange> {
        let resolved = self.resolve_shard_path_cached(index_dir, path)?;
        let index = self.cached_index(index_dir.join(&resolved.index))?;
        let mut range = index.read_range(&resolved.relative_path, start, lines)?;
        range.path = resolved.output_path(&range.path);
        Ok(range)
    }

    fn related_shard_files_cached(
        &self,
        index_dir: &std::path::Path,
        path: &str,
        limit: usize,
    ) -> Result<Vec<crate::repo_index::RelatedFile>> {
        let resolved = self.resolve_shard_path_cached(index_dir, path)?;
        let index = self.cached_index(index_dir.join(&resolved.index))?;
        let mut related =
            index.related_files(&resolved.relative_path, limit.saturating_mul(4).max(10));
        related.retain(|file| resolved.contains_actual_path(&file.path));
        for file in &mut related {
            file.path = resolved.output_path(&file.path);
        }
        related.truncate(limit);
        Ok(related)
    }

    fn related_shard_symbols_cached(
        &self,
        index_dir: &std::path::Path,
        path: &str,
        query: Option<&str>,
        limit: usize,
    ) -> Result<Vec<crate::repo_index::RelatedSymbol>> {
        let resolved = self.resolve_shard_path_cached(index_dir, path)?;
        let index = self.cached_index(index_dir.join(&resolved.index))?;
        let mut related = index.related_symbols(
            Some(&resolved.relative_path),
            query,
            limit.saturating_mul(4).max(10),
        );
        related.retain(|symbol| resolved.contains_actual_path(&symbol.symbol.path));
        for symbol in &mut related {
            symbol.symbol.path = resolved.output_path(&symbol.symbol.path);
        }
        related.truncate(limit);
        Ok(related)
    }

    fn shard_repo_maps_cached(
        &self,
        index_dir: &std::path::Path,
        symbol_limit: usize,
        test_limit: usize,
        filters: &SearchFilters,
    ) -> Result<Vec<ShardRepoMap>> {
        let manifest = self.cached_shard_manifest(index_dir)?;
        let mut maps = Vec::new();
        for shard in &manifest.shards {
            let scopes = shard_search_scopes(shard, filters);
            if scopes.is_empty() {
                continue;
            }
            let index = self.cached_index(index_dir.join(&shard.index))?;
            let scoped = scopes.iter().any(|scope| scope.path_prefix.is_some());
            let base_symbol_limit = if scoped { usize::MAX } else { symbol_limit };
            let base_test_limit = if scoped { usize::MAX } else { test_limit };
            for scope in scopes {
                let mut map = index.repo_map(base_symbol_limit, base_test_limit);
                if let Some(prefix) = scope.path_prefix.as_deref() {
                    filter_repo_map_by_prefix(&mut map, prefix);
                    map.test_files.truncate(test_limit);
                    map.top_symbols.truncate(symbol_limit);
                }
                prefix_repo_map_paths(&mut map, &scope);
                maps.push(ShardRepoMap {
                    aliases: shard
                        .aliases
                        .iter()
                        .map(|alias| alias.name.clone())
                        .collect(),
                    name: scope.output_prefix.clone(),
                    root: shard.root.clone(),
                    git: shard.git.clone(),
                    map,
                });
            }
        }
        maps.sort_by(|a, b| a.name.cmp(&b.name));
        Ok(maps)
    }

    fn find_shard_symbol_cached(
        &self,
        index_dir: &std::path::Path,
        name: &str,
        limit: usize,
        filters: &SearchFilters,
    ) -> Result<Vec<Symbol>> {
        let needle = normalize_token(name);
        if needle.is_empty() || limit == 0 {
            return Ok(Vec::new());
        }

        let manifest = self.cached_shard_manifest(index_dir)?;
        let mut symbols = Vec::new();
        for shard in &manifest.shards {
            let scopes = shard_search_scopes(shard, filters);
            if scopes.is_empty() {
                continue;
            }
            let index = self.cached_index(index_dir.join(&shard.index))?;
            for scope in scopes {
                for mut symbol in index.find_symbol(name, limit) {
                    if let Some(prefix) = &scope.path_prefix {
                        if !symbol.path.starts_with(prefix) {
                            continue;
                        }
                    }
                    symbol.path = scoped_output_path(&scope, &symbol.path);
                    symbols.push(symbol);
                }
            }
        }

        symbols.sort_by(|a, b| {
            symbol_match_score(b, name, &needle)
                .cmp(&symbol_match_score(a, name, &needle))
                .then_with(|| a.path.cmp(&b.path))
                .then_with(|| a.line.cmp(&b.line))
                .then_with(|| a.name.cmp(&b.name))
        });
        symbols.truncate(limit);
        Ok(symbols)
    }
}

fn shard_manifest_detail(index_dir: &Path, manifest: &ShardManifest) -> Value {
    let repos = manifest
        .shards
        .iter()
        .map(|shard| {
            json!({
                "name": shard.name,
                "root": shard.root,
                "index": shard.index,
                "aliases": shard
                    .aliases
                    .iter()
                    .map(|alias| alias.name.clone())
                    .collect::<Vec<_>>(),
                "git": shard.git
            })
        })
        .collect::<Vec<_>>();
    json!({
        "index_dir": index_dir.to_string_lossy().to_string(),
        "shards": manifest.shards.len(),
        "repos": repos
    })
}

fn canonical_cache_key(path: &Path) -> PathBuf {
    if let Ok(canonical) = path.canonicalize() {
        return canonical;
    }
    if let (Some(parent), Some(file_name)) = (path.parent(), path.file_name()) {
        if let Ok(canonical_parent) = parent.canonicalize() {
            return canonical_parent.join(file_name);
        }
    }
    path.to_path_buf()
}

fn symbol_match_score(symbol: &Symbol, name: &str, needle: &str) -> u8 {
    let normalized = normalize_token(&symbol.name);
    if symbol.name == name {
        100
    } else if normalized == needle {
        90
    } else if normalized.contains(needle) {
        60
    } else {
        0
    }
}

fn scoped_output_path(scope: &crate::shards::ShardSearchScope, path: &str) -> String {
    let trimmed = scope
        .path_prefix
        .as_deref()
        .and_then(|prefix| path.strip_prefix(prefix))
        .unwrap_or(path)
        .trim_start_matches('/');
    if trimmed.is_empty() {
        scope.output_prefix.clone()
    } else {
        format!("{}/{}", scope.output_prefix, trimmed)
    }
}

fn prefix_repo_map_paths(
    map: &mut crate::repo_index::RepoMap,
    scope: &crate::shards::ShardSearchScope,
) {
    for path in &mut map.brief.manifest_files {
        *path = scoped_output_path(scope, path);
    }
    for path in &mut map.brief.important_files {
        *path = scoped_output_path(scope, path);
    }
    for path in &mut map.entrypoints {
        *path = scoped_output_path(scope, path);
    }
    for path in &mut map.test_files {
        *path = scoped_output_path(scope, path);
    }
    for symbol in &mut map.top_symbols {
        symbol.path = scoped_output_path(scope, &symbol.path);
    }
    for related in &mut map.related_files {
        related.source_path = scoped_output_path(scope, &related.source_path);
        related.path = scoped_output_path(scope, &related.path);
    }
    for related in &mut map.related_symbols {
        related.source_path = scoped_output_path(scope, &related.source_path);
        related.symbol.path = scoped_output_path(scope, &related.symbol.path);
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
    "context_lines",
    "exclude_file",
    "exclude_path",
    "exclude_language",
    "exclude_extension",
    "exclude_symbol",
    "exclude_repo",
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
    "context_lines",
    "exclude_file",
    "exclude_path",
    "exclude_language",
    "exclude_extension",
    "exclude_symbol",
    "exclude_repo",
];

const PLAN_INDEX_OPTIONAL_ARGS: &[&str] = &[
    "path",
    "language",
    "extension",
    "symbol",
    "file",
    "repo",
    "repo_filter",
    "test",
    "require_all",
    "exclude_file",
    "exclude_path",
    "exclude_language",
    "exclude_extension",
    "exclude_symbol",
    "exclude_repo",
];

const SHARD_BUILD_OPTIONAL_ARGS: &[&str] = &[
    "repos",
    "discover_root",
    "discover_roots",
    "root",
    "max_depth",
    "discover_limit",
    "limit",
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

fn optional_path_array_arg(arguments: &Value, name: &str) -> Result<Vec<PathBuf>> {
    let Some(value) = arguments.get(name) else {
        return Ok(Vec::new());
    };
    let values = value
        .as_array()
        .ok_or_else(|| anyhow!("path array argument {name} must be an array"))?;
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

struct RangeArg {
    path: String,
    start: usize,
    lines: usize,
}

fn range_args(arguments: &Value) -> Result<Vec<RangeArg>> {
    let values = arguments
        .get("ranges")
        .and_then(Value::as_array)
        .ok_or_else(|| anyhow!("missing ranges array argument"))?;
    let mut ranges = Vec::with_capacity(values.len());
    for value in values {
        let path = value
            .get("path")
            .and_then(Value::as_str)
            .map(String::from)
            .ok_or_else(|| anyhow!("range entry must include string path"))?;
        let start = value.get("start").and_then(Value::as_u64).unwrap_or(1) as usize;
        let lines = value.get("lines").and_then(Value::as_u64).unwrap_or(80) as usize;
        ranges.push(RangeArg { path, start, lines });
    }
    Ok(ranges)
}

fn shard_repos_from_arguments(arguments: &Value) -> Result<Vec<PathBuf>> {
    let mut repos = optional_path_array_arg(arguments, "repos")?;
    let mut discover_roots = optional_path_array_arg(arguments, "discover_roots")?;
    if let Some(root) = optional_string_arg_any(arguments, &["discover_root", "root"]) {
        discover_roots.push(PathBuf::from(root));
    }
    if !discover_roots.is_empty() {
        let max_depth = usize_arg(arguments, "max_depth").unwrap_or(4);
        let limit = usize_arg(arguments, "discover_limit")
            .or_else(|| usize_arg(arguments, "limit"))
            .unwrap_or(500);
        for root in discover_roots {
            repos.extend(
                discover_repos(
                    root,
                    &DiscoverOptions {
                        max_depth,
                        limit,
                        ..DiscoverOptions::default()
                    },
                )?
                .repos
                .into_iter()
                .map(|repo| repo.path),
            );
        }
    }
    repos.sort();
    repos.dedup();
    Ok(repos)
}

fn shard_repos_from_arguments_required(arguments: &Value) -> Result<Vec<PathBuf>> {
    let repos = shard_repos_from_arguments(arguments)?;
    if repos.is_empty() {
        return Err(anyhow!("provide repos, discover_root, or discover_roots"));
    }
    Ok(repos)
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

fn optional_string_list_arg(arguments: &Value, name: &str) -> Result<Vec<String>> {
    let Some(value) = arguments.get(name) else {
        return Ok(Vec::new());
    };
    if let Some(value) = value.as_str() {
        return Ok(vec![value.to_string()]);
    }
    let values = value
        .as_array()
        .ok_or_else(|| anyhow!("string list argument {name} must be a string or array"))?;
    values
        .iter()
        .map(|value| {
            value
                .as_str()
                .map(String::from)
                .ok_or_else(|| anyhow!("string list argument {name} must contain only strings"))
        })
        .collect()
}

fn normalized_string_list_arg(arguments: &Value, name: &str) -> Result<Vec<String>> {
    Ok(optional_string_list_arg(arguments, name)?
        .into_iter()
        .map(|value| value.trim_start_matches('.').to_ascii_lowercase())
        .collect())
}

fn search_filters(arguments: &Value, allow_repo_alias: bool) -> Result<SearchFilters> {
    Ok(SearchFilters {
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
        exclude_file: optional_string_list_arg(arguments, "exclude_file")?,
        exclude_path: optional_string_list_arg(arguments, "exclude_path")?,
        exclude_language: normalized_string_list_arg(arguments, "exclude_language")?,
        exclude_extension: normalized_string_list_arg(arguments, "exclude_extension")?,
        exclude_symbol: optional_string_list_arg(arguments, "exclude_symbol")?,
        exclude_repo: optional_string_list_arg(arguments, "exclude_repo")?,
        ..SearchFilters::default()
    })
}
