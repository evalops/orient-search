use crate::discover::{
    DiscoverOptions, DiscoverySelectionSummary, discover_repos, discovery_selection_summary,
};
use crate::fast_index::{FastIndex, RefreshStats};
use crate::query::{merge_filters, parse_query, query_text};
use crate::repo_index::{
    MAX_ATTACHED_CONTEXT_LINES, MAX_READ_RANGE_LINES, MAX_SEARCH_RESULTS, QueryPlan, RepoIndexer,
    ResultToolRequest, SearchFilters, SearchResult, SnippetMode, Symbol, attach_result_context,
    attach_result_read_requests, attach_result_related_requests,
    attach_result_related_symbol_requests, finalize_results, normalize_token, read_file_range,
    search_repo_fast_filtered,
};
use crate::shards::{
    ShardEntry, ShardManifest, ShardQueryPlan, ShardRepoMap, ShardSearchScope, build_shards,
    ensure_shards, filter_repo_map_by_prefix, filters_for_shard_scope, load_manifest,
    refresh_shards, resolve_shard_path_from_manifest, shard_search_scopes, shard_status,
};
use ahash::AHashMap as HashMap;
use anyhow::{Result, anyhow};
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value, json};
use std::io::{BufRead, BufReader, Read, Write};
use std::net::TcpListener;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Condvar, Mutex};
use std::thread;

pub const MAX_BATCH_QUERIES: usize = 32;
pub const MAX_BATCH_RANGES: usize = 64;

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

#[derive(Debug, Serialize)]
struct SearchBatchResult {
    query: String,
    results: Vec<SearchResult>,
}

#[derive(Debug, Serialize)]
struct SearchAutoResult {
    query: String,
    surface: String,
    target: String,
    query_plan_request: ResultToolRequest,
    repo_map_request: ResultToolRequest,
    results: Vec<SearchResult>,
}

#[derive(Debug, Serialize)]
struct IndexedQueryPlanBatchResult {
    query: String,
    plan: QueryPlan,
}

#[derive(Debug, Serialize)]
struct QueryPlanBatchResult {
    query: String,
    plan: QueryPlan,
}

#[derive(Debug, Serialize)]
struct ShardQueryPlanBatchResult {
    query: String,
    plans: Vec<ShardQueryPlan>,
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
            let _ = serve_jsonl_stream(stream, runtime);
        });
    }
    Ok(())
}

pub fn serve_jsonl_stream(stream: impl Read + Write, runtime: Arc<ToolRuntime>) -> Result<()> {
    let mut reader = BufReader::new(stream);
    let mut line = String::new();
    loop {
        line.clear();
        if reader.read_line(&mut line)? == 0 {
            break;
        }
        if line.trim().is_empty() {
            continue;
        }
        let response = runtime.dispatch_line(&line);
        writeln!(reader.get_mut(), "{}", serde_json::to_string(&response)?)?;
        reader.get_mut().flush()?;
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

struct ShardJob {
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

    fn ready(index: Arc<FastIndex>) -> Self {
        Self {
            state: Mutex::new(IndexCacheState::Ready(index)),
            ready: Condvar::new(),
        }
    }

    fn is_ready(&self) -> bool {
        self.state
            .lock()
            .map(|state| matches!(*state, IndexCacheState::Ready(_)))
            .unwrap_or(false)
    }

    fn ready_index(&self) -> Option<Arc<FastIndex>> {
        self.state.lock().ok().and_then(|state| match &*state {
            IndexCacheState::Ready(index) => Some(Arc::clone(index)),
            IndexCacheState::Loading | IndexCacheState::Failed(_) => None,
        })
    }
}

impl ToolRuntime {
    pub fn warm_index(&self, index_path: PathBuf) -> Result<PathBuf> {
        let (key, _) = self.cached_index_with_key(index_path)?;
        Ok(key)
    }

    pub fn refresh_index(&self, repo: PathBuf, index_path: PathBuf) -> Result<RefreshStats> {
        let previous = if index_path.exists() {
            Some(self.cached_index(index_path.clone())?)
        } else {
            None
        };
        let outcome = FastIndex::refresh(repo, previous.as_deref())?;
        let stats = outcome.index.refresh_stats(&outcome);
        outcome.index.save(&index_path)?;
        self.replace_cached_index(index_path, Arc::new(outcome.index))?;
        Ok(stats)
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

    pub fn search_warm_shards(
        &self,
        index_dir: &Path,
        query: &str,
        limit: usize,
        filters: &SearchFilters,
    ) -> Result<Vec<SearchResult>> {
        self.search_shards_cached(index_dir, query, limit, filters, 0)
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
            "cached_index_details": self.cached_index_details(),
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
            "mcp_manifest",
            "Return MCP-shaped tool definitions with inputSchema for adapter wrappers.",
            &[],
            &[],
        ),
        tool_entry(
            "agent_guide",
            "Return a compact Orient workflow guide and request templates for local coding agents.",
            &[],
            &["repo", "index", "index_dir", "addr"],
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
            "ensure_index",
            "Build or refresh a persistent single-repo index from its live repository, then warm it in the daemon cache.",
            &["repo", "index"],
            &[],
        ),
        tool_entry(
            "refresh_index",
            "Refresh a persistent single-repo index from its live repository and replace the daemon cache entry.",
            &["repo", "index"],
            &[],
        ),
        tool_entry(
            "index_status",
            "Report whether a persistent single-repo index is stale versus its live repository.",
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
            &[
                "max_depth",
                "limit",
                "family_limit",
                "git_metadata",
                "tracked_files",
                "nested_manifests",
            ],
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
            "open_range",
            "Alias for read_range for agents that phrase context fetches as opening a file range.",
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
            "open_ranges",
            "Alias for read_ranges for agents that phrase context fetches as opening file ranges.",
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
            "search",
            "Alias for search_code for CLI-style JSON-lines clients.",
            &["repo", "query"],
            SEARCH_OPTIONAL_ARGS,
        ),
        tool_entry(
            "search_auto",
            "Search the best available local surface: explicit shard/index, single warmed daemon target, or a supplied live repo.",
            &["query"],
            SEARCH_AUTO_OPTIONAL_ARGS,
        ),
        tool_entry(
            "search_auto_batch",
            "Run several automatic searches against the best available local surface in one request.",
            &["queries"],
            SEARCH_AUTO_OPTIONAL_ARGS,
        ),
        tool_entry(
            "search_batch",
            "Run several fast fallback searches against one local repository in a single request.",
            &["repo", "queries"],
            SEARCH_OPTIONAL_ARGS,
        ),
        tool_entry(
            "search_query_plan",
            "Build a transient live-repo query plan with missing postings and repair hints.",
            &["repo", "query"],
            PLAN_OPTIONAL_ARGS,
        ),
        tool_entry(
            "search_plan",
            "Alias for search_query_plan for CLI-style JSON-lines clients.",
            &["repo", "query"],
            PLAN_OPTIONAL_ARGS,
        ),
        tool_entry(
            "search_query_plan_batch",
            "Build transient live-repo query plans for several searches in one request.",
            &["repo", "queries"],
            PLAN_OPTIONAL_ARGS,
        ),
        tool_entry(
            "search_plan_batch",
            "Alias for search_query_plan_batch for CLI-style JSON-lines clients.",
            &["repo", "queries"],
            PLAN_OPTIONAL_ARGS,
        ),
        tool_entry(
            "indexed_search_code",
            "Search a persistent single-repo index and return ranked snippets.",
            &["index", "query"],
            SEARCH_INDEX_OPTIONAL_ARGS,
        ),
        tool_entry(
            "indexed_search",
            "Alias for indexed_search_code for CLI-style JSON-lines clients.",
            &["index", "query"],
            SEARCH_INDEX_OPTIONAL_ARGS,
        ),
        tool_entry(
            "indexed_search_batch",
            "Run several searches against one persistent index in a single request.",
            &["index", "queries"],
            SEARCH_INDEX_OPTIONAL_ARGS,
        ),
        tool_entry(
            "indexed_query_plan",
            "Return the indexed query plan, including missing postings, even when search has no hits.",
            &["index", "query"],
            PLAN_INDEX_OPTIONAL_ARGS,
        ),
        tool_entry(
            "index_plan",
            "Alias for indexed_query_plan for CLI-style JSON-lines clients.",
            &["index", "query"],
            PLAN_INDEX_OPTIONAL_ARGS,
        ),
        tool_entry(
            "indexed_query_plan_batch",
            "Return query plans for several searches against one persistent index.",
            &["index", "queries"],
            PLAN_INDEX_OPTIONAL_ARGS,
        ),
        tool_entry(
            "read_index_range",
            "Read a bounded line range from a persistent index result path.",
            &["index", "path"],
            &["start", "lines"],
        ),
        tool_entry(
            "open_index_range",
            "Alias for read_index_range for agents that phrase context fetches as opening a file range.",
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
            "open_index_ranges",
            "Alias for read_index_ranges for agents that phrase context fetches as opening file ranges.",
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
            "shard_status",
            "Report stale shards and added, changed, or deleted files in a local shard directory.",
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
            "search_shards_batch",
            "Run several searches against one local multi-repo shard directory in a single request.",
            &["index_dir", "queries"],
            SEARCH_INDEX_OPTIONAL_ARGS,
        ),
        tool_entry(
            "shard_query_plan",
            "Return indexed query plans for every matching shard repo or alias.",
            &["index_dir", "query"],
            PLAN_INDEX_OPTIONAL_ARGS,
        ),
        tool_entry(
            "shard_plan",
            "Alias for shard_query_plan for CLI-style JSON-lines clients.",
            &["index_dir", "query"],
            PLAN_INDEX_OPTIONAL_ARGS,
        ),
        tool_entry(
            "shard_query_plan_batch",
            "Return shard query plans for several searches against one local multi-repo shard directory.",
            &["index_dir", "queries"],
            PLAN_INDEX_OPTIONAL_ARGS,
        ),
        tool_entry(
            "read_shard_range",
            "Read a bounded line range from a shard search result path or unique shard-relative path.",
            &["index_dir", "path"],
            &["start", "lines"],
        ),
        tool_entry(
            "open_shard_range",
            "Alias for read_shard_range for agents that phrase context fetches as opening a file range.",
            &["index_dir", "path"],
            &["start", "lines"],
        ),
        tool_entry(
            "read_shard_ranges",
            "Read several bounded line ranges from shard result paths or unique shard-relative paths in one request.",
            &["index_dir", "ranges"],
            &[],
        ),
        tool_entry(
            "open_shard_ranges",
            "Alias for read_shard_ranges for agents that phrase context fetches as opening file ranges.",
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
            "Find nearby source/test files related to a shard result path or unique shard-relative path.",
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
            "Find symbols related to a shard result path or unique shard-relative path and optional query.",
            &["index_dir", "path"],
            &["query", "limit"],
        ),
    ])
}

pub fn mcp_tool_manifest() -> Value {
    let tools = match tool_manifest() {
        Value::Array(tools) => tools
            .into_iter()
            .filter_map(|tool| {
                let name = tool.get("name")?.clone();
                let description = tool.get("description")?.clone();
                let input_schema = tool.get("input_schema")?.clone();
                Some(json!({
                    "name": name,
                    "description": description,
                    "inputSchema": input_schema,
                    "annotations": mcp_tool_annotations(tool.get("name")?.as_str()?)
                }))
            })
            .collect::<Vec<_>>(),
        _ => Vec::new(),
    };
    json!({
        "tools": tools
    })
}

pub fn agent_guide(
    repo: Option<&str>,
    index: Option<&str>,
    index_dir: Option<&str>,
    addr: Option<&str>,
) -> Value {
    let repo = repo.unwrap_or("/path/to/repo");
    let index = index.unwrap_or("/tmp/orient.index");
    let index_dir = index_dir.unwrap_or("/tmp/orient-shards");
    let addr = addr.unwrap_or("127.0.0.1:8796");
    json!({
        "name": "Orient Search",
        "purpose": "Fast local code search for coding agents; no session analytics.",
        "recommended_loop": [
            "Call tool_manifest or mcp_manifest once.",
            "Use repo_map, indexed_repo_map, or shard_repo_map before editing unfamiliar code.",
            "Search first, then use read_request, related_request, or related_symbols_request from results.",
            "Call a query-plan tool when results are empty, noisy, or overly broad."
        ],
        "preferred_surfaces": {
            "one_live_repo": "search_code",
            "one_persistent_repo": "indexed_search_code",
            "many_local_repos": "search_shards",
            "warmed_daemon_default": "search_auto"
        },
        "query_language": [
            "repo:platform",
            "path:src/auth or dir:src/auth",
            "file:auth.rs or file:*.rs",
            "lang:rust",
            "ext:rs",
            "symbol:SessionManager",
            "kind:function or type:function",
            "dep:serde",
            "import:crate::auth",
            "test:false, is:test, or is:source",
            "-path:docs",
            "\"quoted literal\"",
            "mode:any for exploratory searches"
        ],
        "transports": {
            "stdio": "orient serve-jsonl",
            "tcp_daemon": format!("orient serve-tcp --addr {addr} --index-dir {index_dir}"),
            "tcp_client": format!("orient client-jsonl --addr {addr}")
        },
        "setup_commands": {
            "single_repo": [
                format!("orient ensure-index --repo {repo} --index {index}"),
                format!("orient serve-tcp --addr {addr} --index {index}")
            ],
            "multi_repo_shards": [
                format!("orient ensure-shards --discover-root ~/Documents/Projects --output-dir {index_dir} --family-limit 2"),
                format!("orient serve-tcp --addr {addr} --index-dir {index_dir}")
            ]
        },
        "request_templates": {
            "manifest": {"id": "tools", "tool": "tool_manifest", "arguments": {}},
            "daemon_status": {"id": "status", "tool": "daemon_status", "arguments": {}},
            "live_repo_map": {
                "id": "map",
                "tool": "repo_map",
                "arguments": {"repo": repo, "symbols": 50, "tests": 50}
            },
            "live_search": {
                "id": "search",
                "tool": "search_code",
                "arguments": {"repo": repo, "query": "symbol:SessionManager token", "limit": 10, "explain": true}
            },
            "auto_search": {
                "id": "search",
                "tool": "search_auto",
                "arguments": {"query": "symbol:SessionManager token", "limit": 10, "explain": true}
            },
            "auto_search_batch": {
                "id": "searches",
                "tool": "search_auto_batch",
                "arguments": {"queries": ["symbol:SessionManager token", "path:src token"], "limit": 10, "explain": true}
            },
            "indexed_repo_map": {
                "id": "map",
                "tool": "indexed_repo_map",
                "arguments": {"index": index, "symbols": 50, "tests": 50}
            },
            "indexed_search": {
                "id": "search",
                "tool": "indexed_search_code",
                "arguments": {"index": index, "query": "path:src symbol:SessionManager token", "limit": 10, "refresh_if_stale": true}
            },
            "shard_repo_map": {
                "id": "map",
                "tool": "shard_repo_map",
                "arguments": {"index_dir": index_dir, "symbols": 50, "tests": 50}
            },
            "shard_search": {
                "id": "search",
                "tool": "search_shards",
                "arguments": {"index_dir": index_dir, "query": "repo:platform symbol:SessionManager token", "limit": 10, "explain": true, "refresh_if_stale": true}
            },
            "live_query_plan": {
                "id": "plan",
                "tool": "search_query_plan",
                "arguments": {"repo": repo, "query": "symbol:SessionManager token"}
            },
            "indexed_query_plan": {
                "id": "plan",
                "tool": "indexed_query_plan",
                "arguments": {"index": index, "query": "path:src symbol:SessionManager token"}
            },
            "shard_query_plan": {
                "id": "plan",
                "tool": "shard_query_plan",
                "arguments": {"index_dir": index_dir, "query": "repo:platform symbol:SessionManager token"}
            }
        },
        "result_followups": [
            "Use search_auto.query_plan_request or a search_auto_batch item query_plan_request when results are empty or noisy.",
            "Use search_auto.repo_map_request or a search_auto_batch item repo_map_request when the agent needs entrypoints, tests, commands, or top symbols for the chosen surface.",
            "Use result.read_request for one bounded file range.",
            "Batch several result.read_range objects with read_ranges, read_index_ranges, or read_shard_ranges.",
            "Use result.related_request for source/test siblings.",
            "Use result.related_symbols_request for nearby definitions and types."
        ],
        "hard_limits": {
            "max_results": MAX_SEARCH_RESULTS,
            "max_batch_queries": MAX_BATCH_QUERIES,
            "max_batch_ranges": MAX_BATCH_RANGES,
            "max_range_lines": MAX_READ_RANGE_LINES,
            "max_attached_context_lines": MAX_ATTACHED_CONTEXT_LINES
        }
    })
}

fn mcp_tool_annotations(name: &str) -> Value {
    let mutating = matches!(
        name,
        "warm_index"
            | "ensure_index"
            | "refresh_index"
            | "warm_shards"
            | "index_shards"
            | "ensure_shards"
            | "refresh_shards"
    );
    json!({
        "readOnlyHint": !mutating,
        "destructiveHint": false,
        "idempotentHint": !mutating,
        "openWorldHint": false
    })
}

fn tool_entry(name: &str, description: &str, required: &[&str], optional: &[&str]) -> Value {
    let mut entry = Map::new();
    entry.insert("name".to_string(), json!(name));
    entry.insert("description".to_string(), json!(description));
    entry.insert("required".to_string(), json!(required));
    entry.insert("optional".to_string(), json!(optional));
    entry.insert(
        "arguments".to_string(),
        json!(argument_metadata(name, required, optional)),
    );
    entry.insert(
        "input_schema".to_string(),
        input_schema(name, required, optional),
    );
    if let Some(default) = tool_daemon_default(name) {
        entry.insert("daemon_default".to_string(), default);
    }
    Value::Object(entry)
}

fn tool_names() -> Value {
    let names = match tool_manifest() {
        Value::Array(tools) => tools
            .into_iter()
            .filter_map(|tool| tool.get("name")?.as_str().map(str::to_string))
            .collect::<Vec<_>>(),
        _ => Vec::new(),
    };
    json!(names)
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
    entry.insert(
        "description".to_string(),
        json!(argument_description(tool_name, name)),
    );
    if let Some(default) = argument_default(tool_name, name) {
        entry.insert("default".to_string(), default);
    }
    if let Some(maximum) = argument_maximum(tool_name, name) {
        entry.insert("maximum".to_string(), json!(maximum));
    }
    if let Some(max_items) = argument_max_items(name) {
        entry.insert("max_items".to_string(), json!(max_items));
    }
    if let Some(values) = argument_enum(name) {
        entry.insert("enum".to_string(), json!(values));
    }
    if let Some(default) = argument_daemon_default(tool_name, name) {
        entry.insert("daemon_default".to_string(), default);
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
        "exclude_file"
        | "exclude_path"
        | "exclude_language"
        | "exclude_extension"
        | "exclude_symbol"
        | "exclude_symbol_kind"
        | "exclude_repo"
        | "exclude_dependency"
        | "exclude_import" => {
            schema.insert(
                "oneOf".to_string(),
                json!([
                    {"type": "string"},
                    {"type": "array", "items": {"type": "string"}}
                ]),
            );
        }
        "ranges" => {
            let path_description = range_path_description(tool_name);
            let range_schema = json!({
                "type": "object",
                "required": ["path"],
                "properties": {
                    "path": {"type": "string", "description": path_description},
                    "start": {"type": "integer", "minimum": 1, "default": 1},
                    "lines": {"type": "integer", "minimum": 1, "maximum": MAX_READ_RANGE_LINES, "default": 80}
                }
            });
            schema.insert(
                "oneOf".to_string(),
                json!([
                    range_schema.clone(),
                    {
                        "type": "array",
                        "minItems": 1,
                        "maxItems": MAX_BATCH_RANGES,
                        "items": range_schema
                    }
                ]),
            );
        }
        "queries" => {
            schema.insert("type".to_string(), json!("array"));
            schema.insert("minItems".to_string(), json!(1));
            schema.insert("maxItems".to_string(), json!(MAX_BATCH_QUERIES));
            schema.insert("items".to_string(), json!({"type": "string"}));
        }
        "repos" | "discover_roots" => {
            schema.insert("type".to_string(), json!("array"));
            schema.insert("items".to_string(), json!({"type": "string"}));
        }
        "test" | "explain" | "require_all" | "any_terms" | "refresh_if_stale" | "git_metadata"
        | "tracked_files" | "nested_manifests" => {
            schema.insert("type".to_string(), json!("boolean"));
        }
        "limit" | "max_depth" | "discover_limit" | "family_limit" | "symbols" | "start"
        | "lines" | "tests" | "context_lines" => {
            schema.insert("type".to_string(), json!("integer"));
            schema.insert(
                "minimum".to_string(),
                json!(if name == "context_lines" || name == "family_limit" {
                    0
                } else {
                    1
                }),
            );
            if let Some(maximum) = argument_maximum(tool_name, name) {
                schema.insert("maximum".to_string(), json!(maximum));
            }
        }
        _ => {
            schema.insert("type".to_string(), json!("string"));
        }
    }
    schema.insert(
        "description".to_string(),
        json!(argument_description(tool_name, name)),
    );
    if let Some(default) = argument_default(tool_name, name) {
        schema.insert("default".to_string(), default);
    }
    if let Some(values) = argument_enum(name) {
        schema.insert("enum".to_string(), json!(values));
    }
    if let Some(default) = argument_daemon_default(tool_name, name) {
        schema.insert("x-daemon-default".to_string(), default);
    }
    Value::Object(schema)
}

fn tool_daemon_default(tool_name: &str) -> Option<Value> {
    match daemon_default_kind(tool_name)? {
        DaemonDefaultKind::Index => Some(json!({
            "argument": "index",
            "source": "single_warmed_index",
            "when": "argument omitted and exactly one index is warmed in the daemon"
        })),
        DaemonDefaultKind::ShardDir => Some(json!({
            "argument": "index_dir",
            "source": "single_warmed_shard_dir",
            "when": "argument omitted and exactly one shard directory is warmed in the daemon"
        })),
    }
}

fn argument_daemon_default(tool_name: &str, name: &str) -> Option<Value> {
    match (daemon_default_kind(tool_name)?, name) {
        (DaemonDefaultKind::Index, "index") => Some(json!("single_warmed_index")),
        (DaemonDefaultKind::ShardDir, "index_dir") => Some(json!("single_warmed_shard_dir")),
        _ => None,
    }
}

#[derive(Debug, Clone, Copy)]
enum DaemonDefaultKind {
    Index,
    ShardDir,
}

fn daemon_default_kind(tool_name: &str) -> Option<DaemonDefaultKind> {
    match tool_name {
        "indexed_repo_map"
        | "indexed_search"
        | "indexed_search_code"
        | "indexed_search_batch"
        | "index_plan"
        | "indexed_query_plan"
        | "indexed_query_plan_batch"
        | "index_status"
        | "read_index_range"
        | "read_index_ranges"
        | "open_index_range"
        | "open_index_ranges"
        | "find_index_symbol"
        | "related_index_files"
        | "related_index_symbols" => Some(DaemonDefaultKind::Index),
        "refresh_shards"
        | "shard_status"
        | "search_shards"
        | "search_shards_batch"
        | "shard_plan"
        | "shard_query_plan"
        | "shard_query_plan_batch"
        | "read_shard_range"
        | "read_shard_ranges"
        | "open_shard_range"
        | "open_shard_ranges"
        | "shard_repo_map"
        | "find_shard_symbol"
        | "related_shard_files"
        | "related_shard_symbols" => Some(DaemonDefaultKind::ShardDir),
        _ => None,
    }
}

fn argument_type(name: &str) -> &'static str {
    match name {
        "limit" | "max_depth" | "discover_limit" | "family_limit" | "symbols" | "start"
        | "lines" | "tests" | "context_lines" => "integer",
        "test" | "explain" | "require_all" | "any_terms" | "refresh_if_stale" | "git_metadata"
        | "tracked_files" | "nested_manifests" => "boolean",
        "exclude_file"
        | "exclude_path"
        | "exclude_language"
        | "exclude_extension"
        | "exclude_symbol"
        | "exclude_symbol_kind"
        | "exclude_repo"
        | "exclude_dependency"
        | "exclude_import" => "string|string[]",
        "ranges" => "range|range[]",
        "repos" | "discover_roots" | "queries" => "string[]",
        _ => "string",
    }
}

fn argument_default(tool_name: &str, name: &str) -> Option<Value> {
    match (tool_name, name) {
        ("discover_repos", "limit") | ("index_shards" | "ensure_shards", "limit") => {
            Some(json!(500))
        }
        (_, "family_limit") => Some(json!(0)),
        (_, "limit") => Some(json!(10)),
        (_, "max_depth") => Some(json!(4)),
        (_, "discover_limit") => Some(json!(500)),
        (_, "symbols" | "tests") => Some(json!(50)),
        (_, "start") => Some(json!(1)),
        (_, "lines") => Some(json!(80)),
        (_, "snippet") => Some(json!("medium")),
        (_, "context_lines") => Some(json!(0)),
        ("agent_guide", "addr") => Some(json!("127.0.0.1:8796")),
        (
            _,
            "explain" | "require_all" | "any_terms" | "refresh_if_stale" | "git_metadata"
            | "tracked_files" | "nested_manifests",
        ) => Some(json!(false)),
        _ => None,
    }
}

fn argument_enum(name: &str) -> Option<&'static [&'static str]> {
    match name {
        "snippet" => Some(&["short", "medium", "block", "symbol"]),
        _ => None,
    }
}

fn argument_maximum(tool_name: &str, name: &str) -> Option<usize> {
    match name {
        "lines" => Some(MAX_READ_RANGE_LINES),
        "context_lines" => Some(MAX_ATTACHED_CONTEXT_LINES),
        "limit" if tool_has_result_limit(tool_name) => Some(MAX_SEARCH_RESULTS),
        _ => None,
    }
}

fn argument_max_items(name: &str) -> Option<usize> {
    match name {
        "queries" => Some(MAX_BATCH_QUERIES),
        "ranges" => Some(MAX_BATCH_RANGES),
        _ => None,
    }
}

fn tool_has_result_limit(tool_name: &str) -> bool {
    matches!(
        tool_name,
        "search_code"
            | "search"
            | "search_auto"
            | "search_auto_batch"
            | "indexed_search"
            | "search_batch"
            | "indexed_search_code"
            | "indexed_search_batch"
            | "search_shards"
            | "search_shards_batch"
    )
}

fn argument_description(tool_name: &str, name: &str) -> &'static str {
    match name {
        "repo" => "Local repository root or shard repo filter, depending on the tool.",
        "repo_filter" => "Repository name filter when repo is already used as a root path.",
        "index" => {
            "Path to a persistent single-repo Orient index. Daemon tools may omit this when exactly one index is warmed."
        }
        "index_dir" => {
            "Path to a local multi-repo shard directory. Daemon tools may omit this when exactly one shard directory is warmed."
        }
        "addr" => "Local TCP daemon address for generated setup and client commands.",
        "output_dir" => "Directory where shard indexes and manifest.json should be written.",
        "query" => "Agent query string with filters, quoted phrases, and normal search terms.",
        "queries" => "Agent query strings to run as one batch against the same search target.",
        "path" if is_shard_path_tool(tool_name) => {
            "Shard-prefixed result path, such as repo/src/lib.rs, or a unique unqualified shard-relative path, such as src/lib.rs."
        }
        "path" if is_index_path_tool(tool_name) => {
            "Index-relative result path, such as src/lib.rs."
        }
        "path" if is_live_path_tool(tool_name) => {
            "Repository-relative result path, such as src/lib.rs."
        }
        "path" => "Path substring filter or result path, depending on the tool.",
        "dir" => "Alias for path when filtering search results to a directory or path substring.",
        "ranges" if is_shard_range_tool(tool_name) => {
            "A {path,start,lines} object or array of them; path may be shard-prefixed or a unique unqualified shard-relative path."
        }
        "ranges" if is_index_range_tool(tool_name) => {
            "A {path,start,lines} object or array of them for index-relative batch range reads."
        }
        "ranges" => {
            "A {path,start,lines} object or array of them for repository-relative batch range reads."
        }
        "limit" => "Maximum number of results to return.",
        "language" => "Detected language filter, such as rust, python, or typescript.",
        "extension" => "File extension filter with or without a leading dot.",
        "symbol" => "Symbol name to require or boost.",
        "symbol_kind" => {
            "Symbol kind to require, such as function, class, struct, enum, or method."
        }
        "dependency" => "Dependency name substring used as a repo-level search filter.",
        "import" => "Imported module substring used as a file-level search filter.",
        "file" => "File basename substring filter.",
        "test" => "When true, include only test paths; when false, exclude test paths.",
        "snippet" => "Snippet mode: short, medium, block, or symbol.",
        "explain" => "Include structured rank signals and indexed query plans.",
        "require_all" => "Require all normalized query tokens to appear in each result.",
        "any_terms" => {
            "Match any normalized query token for exploratory orientation; query text can also use mode:any."
        }
        "context_lines" => "Attach this many bounded line-numbered context lines per result.",
        "refresh_if_stale" => {
            "When true, refresh a stale persistent index or shard directory before searching."
        }
        "exclude_file" => "File basename substring or list of substrings to exclude.",
        "exclude_path" => "Path substring or list of substrings to exclude.",
        "exclude_language" => "Language or list of languages to exclude.",
        "exclude_extension" => "Extension or list of extensions to exclude.",
        "exclude_symbol" => "Symbol name or list of symbols to exclude.",
        "exclude_symbol_kind" => "Symbol kind or list of kinds to exclude.",
        "exclude_repo" => "Repository name substring or list of substrings to exclude.",
        "exclude_dependency" => "Dependency name or list of dependency substrings to exclude.",
        "exclude_import" => "Imported module or list of module substrings to exclude.",
        "root" | "discover_root" => "Workspace root to scan for repositories.",
        "discover_roots" => "Workspace roots to scan for repositories.",
        "repos" => "Explicit repository roots to add to a shard directory.",
        "max_depth" => "Maximum directory depth for repository discovery.",
        "discover_limit" => "Maximum discovered repositories to add when building shards.",
        "family_limit" => {
            "Maximum selected repos per discovered git family; 0 means no per-family limit."
        }
        "git_metadata" => {
            "Include git origin, branch, common git dir, clone/worktree kind, and repo-family groups in discovery results."
        }
        "tracked_files" => {
            "Include git tracked-file counts in discovery metadata and repo-family groups."
        }
        "nested_manifests" => {
            "Also discover manifest-only projects nested inside a discovered git checkout."
        }
        "symbols" => "Maximum top symbols to include in repo maps.",
        "tests" => "Maximum test files to include in repo maps.",
        "start" => "One-based start line for range reads.",
        "lines" => "Number of lines to read, capped to the maximum bounded range size.",
        "name" => "Symbol name or search-like symbol query.",
        _ => "Tool argument.",
    }
}

fn range_path_description(tool_name: &str) -> &'static str {
    if is_shard_range_tool(tool_name) {
        "Shard-prefixed result path, such as repo/src/lib.rs, or a unique unqualified shard-relative path, such as src/lib.rs."
    } else if is_index_range_tool(tool_name) {
        "Index-relative result path, such as src/lib.rs."
    } else {
        "Repository-relative result path, such as src/lib.rs."
    }
}

fn is_shard_path_tool(tool_name: &str) -> bool {
    matches!(
        tool_name,
        "read_shard_range" | "open_shard_range" | "related_shard_files" | "related_shard_symbols"
    )
}

fn is_shard_range_tool(tool_name: &str) -> bool {
    matches!(tool_name, "read_shard_ranges" | "open_shard_ranges")
}

fn is_index_path_tool(tool_name: &str) -> bool {
    matches!(
        tool_name,
        "read_index_range" | "open_index_range" | "related_index_files" | "related_index_symbols"
    )
}

fn is_index_range_tool(tool_name: &str) -> bool {
    matches!(tool_name, "read_index_ranges" | "open_index_ranges")
}

fn is_live_path_tool(tool_name: &str) -> bool {
    matches!(
        tool_name,
        "read_range" | "open_range" | "related_files" | "related_symbols"
    )
}

fn read_request_args<T: Serialize>(name: &str, value: T) -> Map<String, Value> {
    let mut arguments = Map::new();
    arguments.insert(name.to_string(), json!(value));
    arguments
}

fn auto_query_plan_request<T: Serialize>(
    tool: &str,
    target_name: &str,
    target_value: T,
    source_arguments: &Value,
    query: &str,
) -> ResultToolRequest {
    let mut arguments = Map::new();
    if let Some(source) = source_arguments.as_object() {
        for (name, value) in source {
            if auto_query_plan_passthrough_arg(name, target_name) {
                arguments.insert(name.clone(), value.clone());
            }
        }
    }
    arguments.insert(target_name.to_string(), json!(target_value));
    arguments.insert("query".to_string(), json!(query));
    ResultToolRequest {
        tool: tool.to_string(),
        arguments: Value::Object(arguments),
    }
}

fn auto_query_plan_passthrough_arg(name: &str, target_name: &str) -> bool {
    if matches!(
        name,
        "query" | "queries" | "limit" | "context_lines" | "snippet" | "explain"
    ) {
        return false;
    }
    if name == target_name {
        return false;
    }
    if matches!(target_name, "index" | "index_dir") && matches!(name, "index" | "index_dir") {
        return false;
    }
    if target_name == "repo" && name == "repo" {
        return false;
    }
    true
}

fn auto_repo_map_request<T: Serialize>(
    tool: &str,
    target_name: &str,
    target_value: T,
    source_arguments: &Value,
) -> ResultToolRequest {
    let mut arguments = Map::new();
    arguments.insert(target_name.to_string(), json!(target_value));
    if tool == "shard_repo_map" {
        if let Some(source) = source_arguments.as_object() {
            if let Some(repo) = source.get("repo").or_else(|| source.get("repo_filter")) {
                arguments.insert("repo".to_string(), repo.clone());
            }
        }
    }
    ResultToolRequest {
        tool: tool.to_string(),
        arguments: Value::Object(arguments),
    }
}

impl ToolRuntime {
    fn dispatch_result(&self, request: &ToolRequest) -> Result<Value> {
        match request.tool.as_str() {
            "agent_guide" => Ok(agent_guide(
                optional_string_arg(&request.arguments, "repo").as_deref(),
                optional_string_arg(&request.arguments, "index").as_deref(),
                optional_string_arg(&request.arguments, "index_dir").as_deref(),
                optional_string_arg(&request.arguments, "addr").as_deref(),
            )),
            "discover_repos" => {
                let root = path_arg(&request.arguments, "root")?;
                let max_depth = positive_usize_arg(&request.arguments, "max_depth", 4)?;
                let limit = positive_usize_arg(&request.arguments, "limit", 500)?;
                let family_limit = optional_family_limit_arg(&request.arguments)?;
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
                let nested_manifests = request
                    .arguments
                    .get("nested_manifests")
                    .and_then(Value::as_bool)
                    .unwrap_or(false);
                Ok(serde_json::to_value(discover_repos(
                    root,
                    &DiscoverOptions {
                        max_depth,
                        limit,
                        family_limit,
                        git_metadata,
                        tracked_files,
                        nested_manifests,
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
                let symbol_limit = positive_usize_arg(&request.arguments, "symbols", 50)?;
                let test_limit = positive_usize_arg(&request.arguments, "tests", 50)?;
                let index = RepoIndexer::new(repo).build()?;
                Ok(serde_json::to_value(
                    index.repo_map(symbol_limit, test_limit),
                )?)
            }
            "indexed_repo_map" => {
                let index_path = self.index_path_arg_or_single_cached(&request.arguments)?;
                let symbol_limit = positive_usize_arg(&request.arguments, "symbols", 50)?;
                let test_limit = positive_usize_arg(&request.arguments, "tests", 50)?;
                let index = self.cached_index(index_path)?;
                Ok(serde_json::to_value(
                    index.repo_map(symbol_limit, test_limit),
                )?)
            }
            "read_range" | "open_range" => {
                let repo = path_arg(&request.arguments, "repo")?;
                let path = string_arg(&request.arguments, "path")?;
                let (start, lines) = read_window_args(&request.arguments)?;
                Ok(serde_json::to_value(read_file_range(
                    repo, &path, start, lines,
                )?)?)
            }
            "read_ranges" | "open_ranges" => {
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
            "search_code" | "search" => {
                let repo = path_arg(&request.arguments, "repo")?;
                let query = string_arg(&request.arguments, "query")?;
                let limit = search_limit_arg(&request.arguments)?;
                let context_lines = context_lines_arg(&request.arguments)?;
                let mut results = search_repo_fast_filtered(
                    &repo,
                    &query,
                    limit,
                    &search_filters(&request.arguments, false)?,
                )?;
                attach_result_context(&mut results, context_lines, |path, start, lines| {
                    read_file_range(&repo, path, start, lines)
                })?;
                attach_result_read_requests(
                    &mut results,
                    "read_range",
                    read_request_args("repo", &repo),
                );
                attach_result_related_requests(
                    &mut results,
                    "related_files",
                    read_request_args("repo", &repo),
                );
                attach_result_related_symbol_requests(
                    &mut results,
                    "related_symbols",
                    read_request_args("repo", &repo),
                );
                Ok(serde_json::to_value(results)?)
            }
            "search_auto" => {
                let query = string_arg(&request.arguments, "query")?;
                let limit = search_limit_arg(&request.arguments)?;
                let context_lines = context_lines_arg(&request.arguments)?;
                let refresh_if_stale = bool_arg(&request.arguments, "refresh_if_stale");
                let result = self.search_auto(
                    &request.arguments,
                    &query,
                    limit,
                    context_lines,
                    refresh_if_stale,
                )?;
                Ok(serde_json::to_value(result)?)
            }
            "search_auto_batch" => {
                let queries = string_array_arg(&request.arguments, "queries")?;
                let limit = search_limit_arg(&request.arguments)?;
                let context_lines = context_lines_arg(&request.arguments)?;
                let refresh_if_stale = bool_arg(&request.arguments, "refresh_if_stale");
                let mut batch = Vec::new();
                for query in queries {
                    batch.push(self.search_auto(
                        &request.arguments,
                        &query,
                        limit,
                        context_lines,
                        refresh_if_stale,
                    )?);
                }
                Ok(serde_json::to_value(batch)?)
            }
            "search_batch" => {
                let repo = path_arg(&request.arguments, "repo")?;
                let queries = string_array_arg(&request.arguments, "queries")?;
                let limit = search_limit_arg(&request.arguments)?;
                let context_lines = context_lines_arg(&request.arguments)?;
                let filters = search_filters(&request.arguments, false)?;
                let mut batch = Vec::new();
                for query in queries {
                    let mut results = search_repo_fast_filtered(&repo, &query, limit, &filters)?;
                    attach_result_context(&mut results, context_lines, |path, start, lines| {
                        read_file_range(&repo, path, start, lines)
                    })?;
                    attach_result_read_requests(
                        &mut results,
                        "read_range",
                        read_request_args("repo", &repo),
                    );
                    attach_result_related_requests(
                        &mut results,
                        "related_files",
                        read_request_args("repo", &repo),
                    );
                    attach_result_related_symbol_requests(
                        &mut results,
                        "related_symbols",
                        read_request_args("repo", &repo),
                    );
                    batch.push(SearchBatchResult { query, results });
                }
                Ok(serde_json::to_value(batch)?)
            }
            "search_query_plan" | "search_plan" => {
                let repo = path_arg(&request.arguments, "repo")?;
                let query = string_arg(&request.arguments, "query")?;
                let index = FastIndex::build(repo)?;
                Ok(serde_json::to_value(index.query_plan(
                    &query,
                    &search_filters(&request.arguments, false)?,
                )?)?)
            }
            "search_query_plan_batch" | "search_plan_batch" => {
                let repo = path_arg(&request.arguments, "repo")?;
                let queries = string_array_arg(&request.arguments, "queries")?;
                let index = FastIndex::build(repo)?;
                let filters = search_filters(&request.arguments, false)?;
                let mut batch = Vec::new();
                for query in queries {
                    let plan = index.query_plan(&query, &filters)?;
                    batch.push(QueryPlanBatchResult { query, plan });
                }
                Ok(serde_json::to_value(batch)?)
            }
            "indexed_search_code" | "indexed_search" => {
                let index_path = self.index_path_arg_or_single_cached(&request.arguments)?;
                let query = string_arg(&request.arguments, "query")?;
                let limit = search_limit_arg(&request.arguments)?;
                let context_lines = context_lines_arg(&request.arguments)?;
                let refresh_if_stale = bool_arg(&request.arguments, "refresh_if_stale");
                let index =
                    self.cached_index_maybe_refresh(index_path.clone(), refresh_if_stale)?;
                let mut results = index.search_filtered(
                    &query,
                    limit,
                    &search_filters(&request.arguments, true)?,
                )?;
                attach_result_context(&mut results, context_lines, |path, start, lines| {
                    index.read_range(path, start, lines)
                })?;
                attach_result_read_requests(
                    &mut results,
                    "read_index_range",
                    read_request_args("index", &index_path),
                );
                attach_result_related_requests(
                    &mut results,
                    "related_index_files",
                    read_request_args("index", &index_path),
                );
                attach_result_related_symbol_requests(
                    &mut results,
                    "related_index_symbols",
                    read_request_args("index", &index_path),
                );
                Ok(serde_json::to_value(results)?)
            }
            "indexed_search_batch" => {
                let index_path = self.index_path_arg_or_single_cached(&request.arguments)?;
                let queries = string_array_arg(&request.arguments, "queries")?;
                let limit = search_limit_arg(&request.arguments)?;
                let context_lines = context_lines_arg(&request.arguments)?;
                let refresh_if_stale = bool_arg(&request.arguments, "refresh_if_stale");
                let index =
                    self.cached_index_maybe_refresh(index_path.clone(), refresh_if_stale)?;
                let filters = search_filters(&request.arguments, true)?;
                let mut batch = Vec::new();
                for query in queries {
                    let mut results = index.search_filtered(&query, limit, &filters)?;
                    attach_result_context(&mut results, context_lines, |path, start, lines| {
                        index.read_range(path, start, lines)
                    })?;
                    attach_result_read_requests(
                        &mut results,
                        "read_index_range",
                        read_request_args("index", &index_path),
                    );
                    attach_result_related_requests(
                        &mut results,
                        "related_index_files",
                        read_request_args("index", &index_path),
                    );
                    attach_result_related_symbol_requests(
                        &mut results,
                        "related_index_symbols",
                        read_request_args("index", &index_path),
                    );
                    batch.push(SearchBatchResult { query, results });
                }
                Ok(serde_json::to_value(batch)?)
            }
            "indexed_query_plan" | "index_plan" => {
                let index_path = self.index_path_arg_or_single_cached(&request.arguments)?;
                let query = string_arg(&request.arguments, "query")?;
                let refresh_if_stale = bool_arg(&request.arguments, "refresh_if_stale");
                let index = self.cached_index_maybe_refresh(index_path, refresh_if_stale)?;
                Ok(serde_json::to_value(index.query_plan(
                    &query,
                    &search_filters(&request.arguments, true)?,
                )?)?)
            }
            "indexed_query_plan_batch" => {
                let index_path = self.index_path_arg_or_single_cached(&request.arguments)?;
                let queries = string_array_arg(&request.arguments, "queries")?;
                let refresh_if_stale = bool_arg(&request.arguments, "refresh_if_stale");
                let index = self.cached_index_maybe_refresh(index_path, refresh_if_stale)?;
                let filters = search_filters(&request.arguments, true)?;
                let mut batch = Vec::new();
                for query in queries {
                    let plan = index.query_plan(&query, &filters)?;
                    batch.push(IndexedQueryPlanBatchResult { query, plan });
                }
                Ok(serde_json::to_value(batch)?)
            }
            "index_status" => {
                let index_path = self.index_path_arg_or_single_cached(&request.arguments)?;
                let index = self.cached_index(index_path)?;
                Ok(serde_json::to_value(index.freshness()?)?)
            }
            "read_index_range" | "open_index_range" => {
                let index_path = self.index_path_arg_or_single_cached(&request.arguments)?;
                let path = string_arg(&request.arguments, "path")?;
                let (start, lines) = read_window_args(&request.arguments)?;
                let index = self.cached_index(index_path)?;
                Ok(serde_json::to_value(
                    index.read_range(&path, start, lines)?,
                )?)
            }
            "read_index_ranges" | "open_index_ranges" => {
                let index_path = self.index_path_arg_or_single_cached(&request.arguments)?;
                let ranges = range_args(&request.arguments)?;
                let index = self.cached_index(index_path)?;
                let mut results = Vec::new();
                for range in ranges {
                    results.push(index.read_range(&range.path, range.start, range.lines)?);
                }
                Ok(serde_json::to_value(results)?)
            }
            "ensure_index" | "refresh_index" => {
                let repo = path_arg(&request.arguments, "repo")?;
                let index_path = path_arg(&request.arguments, "index")?;
                Ok(serde_json::to_value(self.refresh_index(repo, index_path)?)?)
            }
            "index_shards" => {
                let selection = shard_repos_from_arguments_required(&request.arguments)?;
                let output_dir = path_arg(&request.arguments, "output_dir")?;
                let stats = build_shards(&selection.repos, output_dir)?;
                self.clear_runtime_caches()?;
                shard_bootstrap_output(stats, selection.discovery)
            }
            "ensure_shards" => {
                let selection = shard_repos_from_arguments(&request.arguments)?;
                let output_dir = path_arg(&request.arguments, "output_dir")?;
                let stats = ensure_shards(&selection.repos, &output_dir)?;
                self.clear_runtime_caches()?;
                let warmed_indexes = self.warm_shards(output_dir)?;
                Ok(json!({
                    "stats": shard_bootstrap_output(stats, selection.discovery)?,
                    "warmed_indexes": warmed_indexes,
                    "cached_indexes": self.cached_index_count()
                }))
            }
            "refresh_shards" => {
                let index_dir = self.shard_dir_arg_or_single_cached(&request.arguments)?;
                let stats = refresh_shards(index_dir)?;
                self.clear_runtime_caches()?;
                Ok(serde_json::to_value(stats)?)
            }
            "shard_status" => {
                let index_dir = self.shard_dir_arg_or_single_cached(&request.arguments)?;
                Ok(serde_json::to_value(shard_status(index_dir)?)?)
            }
            "search_shards" => {
                let index_dir = self.shard_dir_arg_or_single_cached(&request.arguments)?;
                let query = string_arg(&request.arguments, "query")?;
                let limit = search_limit_arg(&request.arguments)?;
                let context_lines = context_lines_arg(&request.arguments)?;
                if bool_arg(&request.arguments, "refresh_if_stale") {
                    self.refresh_shards_if_stale(&index_dir)?;
                }
                Ok(serde_json::to_value(self.search_shards_cached(
                    &index_dir,
                    &query,
                    limit,
                    &search_filters(&request.arguments, true)?,
                    context_lines,
                )?)?)
            }
            "search_shards_batch" => {
                let index_dir = self.shard_dir_arg_or_single_cached(&request.arguments)?;
                let queries = string_array_arg(&request.arguments, "queries")?;
                let limit = search_limit_arg(&request.arguments)?;
                let context_lines = context_lines_arg(&request.arguments)?;
                if bool_arg(&request.arguments, "refresh_if_stale") {
                    self.refresh_shards_if_stale(&index_dir)?;
                }
                let filters = search_filters(&request.arguments, true)?;
                let mut batch = Vec::new();
                for query in queries {
                    let results = self.search_shards_cached(
                        &index_dir,
                        &query,
                        limit,
                        &filters,
                        context_lines,
                    )?;
                    batch.push(SearchBatchResult { query, results });
                }
                Ok(serde_json::to_value(batch)?)
            }
            "shard_query_plan" | "shard_plan" => {
                let index_dir = self.shard_dir_arg_or_single_cached(&request.arguments)?;
                let query = string_arg(&request.arguments, "query")?;
                if bool_arg(&request.arguments, "refresh_if_stale") {
                    self.refresh_shards_if_stale(&index_dir)?;
                }
                Ok(serde_json::to_value(self.shard_query_plans_cached(
                    &index_dir,
                    &query,
                    &search_filters(&request.arguments, true)?,
                )?)?)
            }
            "shard_query_plan_batch" => {
                let index_dir = self.shard_dir_arg_or_single_cached(&request.arguments)?;
                let queries = string_array_arg(&request.arguments, "queries")?;
                if bool_arg(&request.arguments, "refresh_if_stale") {
                    self.refresh_shards_if_stale(&index_dir)?;
                }
                let filters = search_filters(&request.arguments, true)?;
                let mut batch = Vec::new();
                for query in queries {
                    let plans = self.shard_query_plans_cached(&index_dir, &query, &filters)?;
                    batch.push(ShardQueryPlanBatchResult { query, plans });
                }
                Ok(serde_json::to_value(batch)?)
            }
            "read_shard_range" | "open_shard_range" => {
                let index_dir = self.shard_dir_arg_or_single_cached(&request.arguments)?;
                let path = string_arg(&request.arguments, "path")?;
                let (start, lines) = read_window_args(&request.arguments)?;
                Ok(serde_json::to_value(self.read_shard_range_cached(
                    &index_dir, &path, start, lines,
                )?)?)
            }
            "read_shard_ranges" | "open_shard_ranges" => {
                let index_dir = self.shard_dir_arg_or_single_cached(&request.arguments)?;
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
                let index_dir = self.shard_dir_arg_or_single_cached(&request.arguments)?;
                let symbol_limit = positive_usize_arg(&request.arguments, "symbols", 50)?;
                let test_limit = positive_usize_arg(&request.arguments, "tests", 50)?;
                Ok(serde_json::to_value(self.shard_repo_maps_cached(
                    &index_dir,
                    symbol_limit,
                    test_limit,
                    &search_filters(&request.arguments, true)?,
                )?)?)
            }
            "find_shard_symbol" => {
                let index_dir = self.shard_dir_arg_or_single_cached(&request.arguments)?;
                let name = string_arg(&request.arguments, "name")?;
                let limit = positive_usize_arg(&request.arguments, "limit", 10)?;
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
                let limit = positive_usize_arg(&request.arguments, "limit", 10)?;
                let index = RepoIndexer::new(repo).build()?;
                Ok(serde_json::to_value(index.find_symbol(&name, limit))?)
            }
            "find_index_symbol" => {
                let index_path = self.index_path_arg_or_single_cached(&request.arguments)?;
                let name = string_arg(&request.arguments, "name")?;
                let limit = positive_usize_arg(&request.arguments, "limit", 10)?;
                let index = self.cached_index(index_path)?;
                Ok(serde_json::to_value(index.find_symbol(&name, limit))?)
            }
            "related_files" => {
                let repo = path_arg(&request.arguments, "repo")?;
                let path = string_arg(&request.arguments, "path")?;
                let limit = positive_usize_arg(&request.arguments, "limit", 10)?;
                let index = RepoIndexer::new(repo).build()?;
                Ok(serde_json::to_value(index.related_files(&path, limit))?)
            }
            "related_index_files" => {
                let index_path = self.index_path_arg_or_single_cached(&request.arguments)?;
                let path = string_arg(&request.arguments, "path")?;
                let limit = positive_usize_arg(&request.arguments, "limit", 10)?;
                let index = self.cached_index(index_path)?;
                Ok(serde_json::to_value(index.related_files(&path, limit))?)
            }
            "related_shard_files" => {
                let index_dir = self.shard_dir_arg_or_single_cached(&request.arguments)?;
                let path = string_arg(&request.arguments, "path")?;
                let limit = positive_usize_arg(&request.arguments, "limit", 10)?;
                Ok(serde_json::to_value(
                    self.related_shard_files_cached(&index_dir, &path, limit)?,
                )?)
            }
            "related_symbols" => {
                let repo = path_arg(&request.arguments, "repo")?;
                let path = optional_string_arg(&request.arguments, "path");
                let query = optional_string_arg(&request.arguments, "query");
                let limit = positive_usize_arg(&request.arguments, "limit", 10)?;
                let index = RepoIndexer::new(repo).build()?;
                Ok(serde_json::to_value(index.related_symbols(
                    path.as_deref(),
                    query.as_deref(),
                    limit,
                ))?)
            }
            "related_shard_symbols" => {
                let index_dir = self.shard_dir_arg_or_single_cached(&request.arguments)?;
                let path = string_arg(&request.arguments, "path")?;
                let query = optional_string_arg(&request.arguments, "query");
                let limit = positive_usize_arg(&request.arguments, "limit", 10)?;
                Ok(serde_json::to_value(self.related_shard_symbols_cached(
                    &index_dir,
                    &path,
                    query.as_deref(),
                    limit,
                )?)?)
            }
            "related_index_symbols" => {
                let index_path = self.index_path_arg_or_single_cached(&request.arguments)?;
                let path = optional_string_arg(&request.arguments, "path");
                let query = optional_string_arg(&request.arguments, "query");
                let limit = positive_usize_arg(&request.arguments, "limit", 10)?;
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
            "mcp_manifest" => Ok(mcp_tool_manifest()),
            "list_tools" => Ok(tool_names()),
            other => Err(anyhow!("unknown tool: {other}")),
        }
    }

    fn cached_index(&self, index_path: PathBuf) -> Result<Arc<FastIndex>> {
        Ok(self.cached_index_with_key(index_path)?.1)
    }

    fn index_path_arg_or_single_cached(&self, arguments: &Value) -> Result<PathBuf> {
        if arguments.get("index").is_some() {
            return path_arg(arguments, "index");
        }
        self.single_cached_index_path()
    }

    fn shard_dir_arg_or_single_cached(&self, arguments: &Value) -> Result<PathBuf> {
        if arguments.get("index_dir").is_some() {
            return path_arg(arguments, "index_dir");
        }
        self.single_cached_shard_manifest_path()
    }

    fn single_cached_index_path(&self) -> Result<PathBuf> {
        let mut paths = self
            .indexes
            .lock()
            .map_err(|_| anyhow!("index cache lock poisoned"))?
            .iter()
            .filter_map(|(path, entry)| entry.is_ready().then(|| path.clone()))
            .collect::<Vec<_>>();
        paths.sort();
        match paths.as_slice() {
            [path] => Ok(path.clone()),
            [] => Err(anyhow!(
                "index is required unless exactly one index is warmed in the daemon"
            )),
            _ => Err(anyhow!(
                "index is required because multiple indexes are warmed in the daemon: {}",
                join_paths_for_error(&paths)
            )),
        }
    }

    fn single_cached_shard_manifest_path(&self) -> Result<PathBuf> {
        let mut paths = self
            .shard_manifests
            .lock()
            .map_err(|_| anyhow!("shard manifest cache lock poisoned"))?
            .keys()
            .cloned()
            .collect::<Vec<_>>();
        paths.sort();
        match paths.as_slice() {
            [path] => Ok(path.clone()),
            [] => Err(anyhow!(
                "index_dir is required unless exactly one shard directory is warmed in the daemon"
            )),
            _ => Err(anyhow!(
                "index_dir is required because multiple shard directories are warmed in the daemon: {}",
                join_paths_for_error(&paths)
            )),
        }
    }

    fn search_auto(
        &self,
        arguments: &Value,
        query: &str,
        limit: usize,
        context_lines: usize,
        refresh_if_stale: bool,
    ) -> Result<SearchAutoResult> {
        if let Some(index_dir) = optional_string_arg(arguments, "index_dir").map(PathBuf::from) {
            return self.search_auto_shards(
                index_dir,
                arguments,
                query,
                limit,
                context_lines,
                refresh_if_stale,
            );
        }
        if let Some(index_path) = optional_string_arg(arguments, "index").map(PathBuf::from) {
            return self.search_auto_index(
                index_path,
                arguments,
                query,
                limit,
                context_lines,
                refresh_if_stale,
            );
        }
        if let Ok(index_dir) = self.single_cached_shard_manifest_path() {
            return self.search_auto_shards(
                index_dir,
                arguments,
                query,
                limit,
                context_lines,
                refresh_if_stale,
            );
        }
        if let Ok(index_path) = self.single_cached_index_path() {
            return self.search_auto_index(
                index_path,
                arguments,
                query,
                limit,
                context_lines,
                refresh_if_stale,
            );
        }
        if let Some(repo) = optional_string_arg(arguments, "repo").map(PathBuf::from) {
            let mut results =
                search_repo_fast_filtered(&repo, query, limit, &search_filters(arguments, false)?)?;
            attach_result_context(&mut results, context_lines, |path, start, lines| {
                read_file_range(&repo, path, start, lines)
            })?;
            attach_result_read_requests(
                &mut results,
                "read_range",
                read_request_args("repo", &repo),
            );
            attach_result_related_requests(
                &mut results,
                "related_files",
                read_request_args("repo", &repo),
            );
            attach_result_related_symbol_requests(
                &mut results,
                "related_symbols",
                read_request_args("repo", &repo),
            );
            return Ok(SearchAutoResult {
                query: query.to_string(),
                surface: "fallback".to_string(),
                target: repo.to_string_lossy().to_string(),
                query_plan_request: auto_query_plan_request(
                    "search_query_plan",
                    "repo",
                    &repo,
                    arguments,
                    query,
                ),
                repo_map_request: auto_repo_map_request("repo_map", "repo", &repo, arguments),
                results,
            });
        }
        Err(anyhow!(
            "search_auto needs index_dir, index, repo, or exactly one warmed shard/index target"
        ))
    }

    fn search_auto_shards(
        &self,
        index_dir: PathBuf,
        arguments: &Value,
        query: &str,
        limit: usize,
        context_lines: usize,
        refresh_if_stale: bool,
    ) -> Result<SearchAutoResult> {
        if refresh_if_stale {
            self.refresh_shards_if_stale(&index_dir)?;
        }
        let results = self.search_shards_cached(
            &index_dir,
            query,
            limit,
            &search_filters(arguments, true)?,
            context_lines,
        )?;
        Ok(SearchAutoResult {
            query: query.to_string(),
            surface: "shards".to_string(),
            target: index_dir.to_string_lossy().to_string(),
            query_plan_request: auto_query_plan_request(
                "shard_query_plan",
                "index_dir",
                &index_dir,
                arguments,
                query,
            ),
            repo_map_request: auto_repo_map_request(
                "shard_repo_map",
                "index_dir",
                &index_dir,
                arguments,
            ),
            results,
        })
    }

    fn search_auto_index(
        &self,
        index_path: PathBuf,
        arguments: &Value,
        query: &str,
        limit: usize,
        context_lines: usize,
        refresh_if_stale: bool,
    ) -> Result<SearchAutoResult> {
        let index = self.cached_index_maybe_refresh(index_path.clone(), refresh_if_stale)?;
        let mut results = index.search_filtered(query, limit, &search_filters(arguments, true)?)?;
        attach_result_context(&mut results, context_lines, |path, start, lines| {
            index.read_range(path, start, lines)
        })?;
        attach_result_read_requests(
            &mut results,
            "read_index_range",
            read_request_args("index", &index_path),
        );
        attach_result_related_requests(
            &mut results,
            "related_index_files",
            read_request_args("index", &index_path),
        );
        attach_result_related_symbol_requests(
            &mut results,
            "related_index_symbols",
            read_request_args("index", &index_path),
        );
        Ok(SearchAutoResult {
            query: query.to_string(),
            surface: "indexed".to_string(),
            target: index_path.to_string_lossy().to_string(),
            query_plan_request: auto_query_plan_request(
                "indexed_query_plan",
                "index",
                &index_path,
                arguments,
                query,
            ),
            repo_map_request: auto_repo_map_request(
                "indexed_repo_map",
                "index",
                &index_path,
                arguments,
            ),
            results,
        })
    }

    fn cached_index_maybe_refresh(
        &self,
        index_path: PathBuf,
        refresh_if_stale: bool,
    ) -> Result<Arc<FastIndex>> {
        let index = self.cached_index(index_path.clone())?;
        if !refresh_if_stale || !index.freshness()?.stale {
            return Ok(index);
        }
        let root = index.root.clone();
        drop(index);
        self.refresh_index(root, index_path.clone())?;
        self.cached_index(index_path)
    }

    fn refresh_shards_if_stale(&self, index_dir: &Path) -> Result<()> {
        if !shard_status(index_dir)?.stale {
            return Ok(());
        }
        refresh_shards(index_dir)?;
        self.clear_runtime_caches()
    }

    fn replace_cached_index(&self, index_path: PathBuf, index: Arc<FastIndex>) -> Result<PathBuf> {
        let key = canonical_cache_key(&index_path);
        self.indexes
            .lock()
            .map_err(|_| anyhow!("index cache lock poisoned"))?
            .insert(key.clone(), Arc::new(IndexCacheEntry::ready(index)));
        Ok(key)
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

    fn cached_index_details(&self) -> Vec<Value> {
        let mut details = self
            .indexes
            .lock()
            .map(|indexes| {
                indexes
                    .iter()
                    .filter_map(|(path, entry)| {
                        entry.ready_index().map(|index| {
                            let stats = index.stats();
                            json!({
                                "index": path.to_string_lossy(),
                                "root": stats.root.to_string_lossy(),
                                "version": stats.version,
                                "files": stats.files,
                                "source_bytes": stats.source_bytes,
                                "terms": stats.terms,
                                "path_terms": stats.path_terms,
                                "trigrams": stats.trigrams,
                                "posting_entries": stats.posting_entries,
                                "compressed_posting_bytes": stats.compressed_posting_bytes,
                                "symbols": stats.symbols
                            })
                        })
                    })
                    .collect::<Vec<_>>()
            })
            .unwrap_or_default();
        details.sort_by(|left, right| {
            left.get("index")
                .and_then(Value::as_str)
                .cmp(&right.get("index").and_then(Value::as_str))
        });
        details
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
        resolve_shard_path_from_manifest(&manifest, path)
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
                (!scopes.is_empty()).then_some(ShardJob { shard, scopes })
            })
            .collect::<Vec<_>>();
        let results =
            self.search_shard_jobs_cached(index_dir, &shard_query, limit, &filters, jobs)?;
        let mut results = finalize_results(results, limit);
        attach_result_context(&mut results, context_lines, |path, start, lines| {
            self.read_shard_range_cached(index_dir, path, start, lines)
        })?;
        attach_result_read_requests(
            &mut results,
            "read_shard_range",
            read_request_args("index_dir", index_dir),
        );
        attach_result_related_requests(
            &mut results,
            "related_shard_files",
            read_request_args("index_dir", index_dir),
        );
        attach_result_related_symbol_requests(
            &mut results,
            "related_shard_symbols",
            read_request_args("index_dir", index_dir),
        );
        Ok(results)
    }

    fn search_shard_jobs_cached(
        &self,
        index_dir: &std::path::Path,
        query: &str,
        limit: usize,
        filters: &SearchFilters,
        jobs: Vec<ShardJob>,
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

    fn shard_query_plans_cached(
        &self,
        index_dir: &std::path::Path,
        query: &str,
        filters: &SearchFilters,
    ) -> Result<Vec<ShardQueryPlan>> {
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
                (!scopes.is_empty()).then_some(ShardJob { shard, scopes })
            })
            .collect::<Vec<_>>();
        let mut plans =
            self.shard_query_plan_jobs_cached(index_dir, &shard_query, &filters, jobs)?;
        plans.sort_by(|left, right| left.name.cmp(&right.name));
        Ok(plans)
    }

    fn shard_query_plan_jobs_cached(
        &self,
        index_dir: &std::path::Path,
        query: &str,
        filters: &SearchFilters,
        jobs: Vec<ShardJob>,
    ) -> Result<Vec<ShardQueryPlan>> {
        if jobs.is_empty() {
            return Ok(Vec::new());
        }

        let workers = std::thread::available_parallelism()
            .map(|count| count.get())
            .unwrap_or(1)
            .min(jobs.len());
        if workers <= 1 {
            return self.shard_query_plan_job_batch_cached(index_dir, query, filters, &jobs);
        }

        let chunk_size = jobs.len().div_ceil(workers);
        let mut plans = Vec::new();
        thread::scope(|scope| {
            let handles = jobs
                .chunks(chunk_size)
                .map(|chunk| {
                    scope.spawn(move || {
                        self.shard_query_plan_job_batch_cached(index_dir, query, filters, chunk)
                    })
                })
                .collect::<Vec<_>>();

            for handle in handles {
                let batch = handle
                    .join()
                    .map_err(|_| anyhow!("shard query-plan worker panicked"))??;
                plans.extend(batch);
            }
            Ok::<(), anyhow::Error>(())
        })?;
        Ok(plans)
    }

    fn shard_query_plan_job_batch_cached(
        &self,
        index_dir: &std::path::Path,
        query: &str,
        filters: &SearchFilters,
        jobs: &[ShardJob],
    ) -> Result<Vec<ShardQueryPlan>> {
        let mut plans = Vec::new();
        for job in jobs {
            let index = self.cached_index(index_dir.join(&job.shard.index))?;
            for scope in &job.scopes {
                let scoped_filters = filters_for_shard_scope(filters, scope.path_prefix.as_deref());
                plans.push(ShardQueryPlan {
                    aliases: job
                        .shard
                        .aliases
                        .iter()
                        .map(|alias| alias.name.clone())
                        .collect(),
                    git: job.shard.git.clone(),
                    name: scope.output_prefix.clone(),
                    root: job.shard.root.clone(),
                    plan: index.query_plan(query, &scoped_filters)?,
                });
            }
        }
        Ok(plans)
    }

    fn search_shard_job_batch_cached(
        &self,
        index_dir: &std::path::Path,
        query: &str,
        limit: usize,
        filters: &SearchFilters,
        jobs: &[ShardJob],
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
                    prefix_search_result_paths(&mut result, scope);
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

fn prefix_search_result_paths(result: &mut SearchResult, scope: &crate::shards::ShardSearchScope) {
    result.path = scoped_output_path(scope, &result.path);
    if let Some(read_range) = &mut result.read_range {
        read_range.path = scoped_output_path(scope, &read_range.path);
    }
    if let Some(context) = &mut result.context {
        context.path = scoped_output_path(scope, &context.path);
    }
    if let Some(group) = &mut result.duplicate_group {
        for path in &mut group.duplicate_paths {
            *path = scoped_output_path(scope, path);
        }
        group.duplicate_paths.sort();
        group.duplicate_paths.dedup();
    }
}

fn prefix_repo_map_paths(
    map: &mut crate::repo_index::RepoMap,
    scope: &crate::shards::ShardSearchScope,
) {
    for hint in &mut map.brief.command_hints {
        hint.source = scoped_output_path(scope, &hint.source);
    }
    for hint in &mut map.brief.dependency_hints {
        hint.source = scoped_output_path(scope, &hint.source);
    }
    for hint in &mut map.brief.import_hints {
        hint.source = scoped_output_path(scope, &hint.source);
    }
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
    "dir",
    "language",
    "extension",
    "symbol",
    "symbol_kind",
    "dependency",
    "import",
    "file",
    "repo_filter",
    "test",
    "snippet",
    "explain",
    "require_all",
    "any_terms",
    "context_lines",
    "exclude_file",
    "exclude_path",
    "exclude_language",
    "exclude_extension",
    "exclude_symbol",
    "exclude_symbol_kind",
    "exclude_repo",
    "exclude_dependency",
    "exclude_import",
];

const SEARCH_AUTO_OPTIONAL_ARGS: &[&str] = &[
    "repo",
    "index",
    "index_dir",
    "limit",
    "path",
    "dir",
    "language",
    "extension",
    "symbol",
    "symbol_kind",
    "dependency",
    "import",
    "file",
    "repo_filter",
    "test",
    "snippet",
    "explain",
    "require_all",
    "any_terms",
    "context_lines",
    "refresh_if_stale",
    "exclude_file",
    "exclude_path",
    "exclude_language",
    "exclude_extension",
    "exclude_symbol",
    "exclude_symbol_kind",
    "exclude_repo",
    "exclude_dependency",
    "exclude_import",
];

const SEARCH_INDEX_OPTIONAL_ARGS: &[&str] = &[
    "limit",
    "path",
    "dir",
    "language",
    "extension",
    "symbol",
    "symbol_kind",
    "dependency",
    "import",
    "file",
    "repo",
    "repo_filter",
    "test",
    "snippet",
    "explain",
    "require_all",
    "any_terms",
    "context_lines",
    "refresh_if_stale",
    "exclude_file",
    "exclude_path",
    "exclude_language",
    "exclude_extension",
    "exclude_symbol",
    "exclude_symbol_kind",
    "exclude_repo",
    "exclude_dependency",
    "exclude_import",
];

const PLAN_OPTIONAL_ARGS: &[&str] = &[
    "path",
    "dir",
    "language",
    "extension",
    "symbol",
    "symbol_kind",
    "dependency",
    "import",
    "file",
    "repo_filter",
    "test",
    "require_all",
    "any_terms",
    "exclude_file",
    "exclude_path",
    "exclude_language",
    "exclude_extension",
    "exclude_symbol",
    "exclude_symbol_kind",
    "exclude_repo",
    "exclude_dependency",
    "exclude_import",
];

const PLAN_INDEX_OPTIONAL_ARGS: &[&str] = &[
    "path",
    "dir",
    "language",
    "extension",
    "symbol",
    "symbol_kind",
    "dependency",
    "import",
    "file",
    "repo",
    "repo_filter",
    "test",
    "require_all",
    "any_terms",
    "refresh_if_stale",
    "exclude_file",
    "exclude_path",
    "exclude_language",
    "exclude_extension",
    "exclude_symbol",
    "exclude_symbol_kind",
    "exclude_repo",
    "exclude_dependency",
    "exclude_import",
];

const SHARD_BUILD_OPTIONAL_ARGS: &[&str] = &[
    "repos",
    "discover_root",
    "discover_roots",
    "root",
    "max_depth",
    "discover_limit",
    "limit",
    "family_limit",
    "nested_manifests",
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

fn join_paths_for_error(paths: &[PathBuf]) -> String {
    paths
        .iter()
        .map(|path| path.to_string_lossy())
        .collect::<Vec<_>>()
        .join(", ")
}

fn bool_arg(arguments: &Value, name: &str) -> bool {
    arguments
        .get(name)
        .and_then(Value::as_bool)
        .unwrap_or(false)
}

fn read_window_args(arguments: &Value) -> Result<(usize, usize)> {
    let start = positive_usize_arg(arguments, "start", 1)?;
    let lines = bounded_usize_arg(arguments, "lines", 80, 1, Some(MAX_READ_RANGE_LINES))?;
    validate_read_window(start, lines)?;
    Ok((start, lines))
}

fn validate_read_window(start: usize, lines: usize) -> Result<()> {
    if start == 0 {
        return Err(anyhow!("range start must be a positive integer"));
    }
    if lines == 0 {
        return Err(anyhow!("range lines must be a positive integer"));
    }
    if lines > MAX_READ_RANGE_LINES {
        return Err(anyhow!(
            "range lines has {lines}, max {MAX_READ_RANGE_LINES}"
        ));
    }
    Ok(())
}

fn string_array_arg(arguments: &Value, name: &str) -> Result<Vec<String>> {
    let Some(value) = arguments.get(name) else {
        return Err(anyhow!("missing string array argument: {name}"));
    };
    let values = value
        .as_array()
        .ok_or_else(|| anyhow!("argument {name} must be an array of strings"))?;
    if values.is_empty() {
        return Err(anyhow!("argument {name} must not be empty"));
    }
    if values.len() > MAX_BATCH_QUERIES {
        return Err(anyhow!(
            "argument {name} has {} items, max {}",
            values.len(),
            MAX_BATCH_QUERIES
        ));
    }
    values
        .iter()
        .map(|value| {
            value
                .as_str()
                .map(String::from)
                .ok_or_else(|| anyhow!("argument {name} must be an array of strings"))
        })
        .collect()
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
    let value = arguments
        .get("ranges")
        .ok_or_else(|| anyhow!("missing ranges argument"))?;
    let owned_single;
    let values = if let Some(values) = value.as_array() {
        values
    } else if value.is_object() {
        owned_single = vec![value.clone()];
        &owned_single
    } else {
        return Err(anyhow!("argument ranges must be an object or array"));
    };
    if values.is_empty() {
        return Err(anyhow!("argument ranges must not be empty"));
    }
    if values.len() > MAX_BATCH_RANGES {
        return Err(anyhow!(
            "argument ranges has {} items, max {}",
            values.len(),
            MAX_BATCH_RANGES
        ));
    }
    let mut ranges = Vec::with_capacity(values.len());
    for value in values {
        let path = value
            .get("path")
            .and_then(Value::as_str)
            .map(String::from)
            .ok_or_else(|| anyhow!("range entry must include string path"))?;
        let start = bounded_usize_field(value, "start", 1, 1, None)?;
        let lines = bounded_usize_field(value, "lines", 80, 1, Some(MAX_READ_RANGE_LINES))?;
        validate_read_window(start, lines)?;
        ranges.push(RangeArg { path, start, lines });
    }
    Ok(ranges)
}

struct ShardRepoSelection {
    repos: Vec<PathBuf>,
    discovery: Vec<DiscoverySelectionSummary>,
}

fn shard_repos_from_arguments(arguments: &Value) -> Result<ShardRepoSelection> {
    let mut repos = optional_path_array_arg(arguments, "repos")?;
    let mut discover_roots = optional_path_array_arg(arguments, "discover_roots")?;
    if let Some(root) = optional_string_arg_any(arguments, &["discover_root", "root"]) {
        discover_roots.push(PathBuf::from(root));
    }
    if !discover_roots.is_empty() {
        let max_depth = positive_usize_arg(arguments, "max_depth", 4)?;
        let limit = optional_positive_usize_arg(arguments, "discover_limit")?
            .or(optional_positive_usize_arg(arguments, "limit")?)
            .unwrap_or(500);
        let family_limit = optional_family_limit_arg(arguments)?;
        let nested_manifests = arguments
            .get("nested_manifests")
            .and_then(Value::as_bool)
            .unwrap_or(false);
        let mut discovery = Vec::new();
        for root in discover_roots {
            let discovered = discover_repos(
                root,
                &DiscoverOptions {
                    max_depth,
                    limit,
                    family_limit,
                    nested_manifests,
                    ..DiscoverOptions::default()
                },
            )?;
            discovery.push(discovery_selection_summary(&discovered));
            repos.extend(discovered.repos.into_iter().map(|repo| repo.path));
        }
        repos.sort();
        repos.dedup();
        return Ok(ShardRepoSelection { repos, discovery });
    }
    repos.sort();
    repos.dedup();
    Ok(ShardRepoSelection {
        repos,
        discovery: Vec::new(),
    })
}

fn shard_repos_from_arguments_required(arguments: &Value) -> Result<ShardRepoSelection> {
    let selection = shard_repos_from_arguments(arguments)?;
    if selection.repos.is_empty() {
        return Err(anyhow!("provide repos, discover_root, or discover_roots"));
    }
    Ok(selection)
}

fn shard_bootstrap_output<T: Serialize>(
    stats: T,
    discovery: Vec<DiscoverySelectionSummary>,
) -> Result<Value> {
    let mut value = serde_json::to_value(stats)?;
    if !discovery.is_empty() {
        let object = value
            .as_object_mut()
            .ok_or_else(|| anyhow!("shard stats did not serialize to an object"))?;
        object.insert("discovery".to_string(), serde_json::to_value(discovery)?);
    }
    Ok(value)
}

fn search_limit_arg(arguments: &Value) -> Result<usize> {
    bounded_usize_arg(arguments, "limit", 10, 1, Some(MAX_SEARCH_RESULTS))
}

fn context_lines_arg(arguments: &Value) -> Result<usize> {
    bounded_usize_arg(
        arguments,
        "context_lines",
        0,
        0,
        Some(MAX_ATTACHED_CONTEXT_LINES),
    )
}

fn positive_usize_arg(arguments: &Value, name: &str, default: usize) -> Result<usize> {
    bounded_usize_arg(arguments, name, default, 1, None)
}

fn optional_positive_usize_arg(arguments: &Value, name: &str) -> Result<Option<usize>> {
    optional_bounded_usize_arg(arguments, name, 1, None)
}

fn optional_family_limit_arg(arguments: &Value) -> Result<Option<usize>> {
    Ok(optional_bounded_usize_arg(arguments, "family_limit", 0, None)?.filter(|limit| *limit > 0))
}

fn bounded_usize_arg(
    arguments: &Value,
    name: &str,
    default: usize,
    minimum: usize,
    maximum: Option<usize>,
) -> Result<usize> {
    Ok(optional_bounded_usize_arg(arguments, name, minimum, maximum)?.unwrap_or(default))
}

fn optional_bounded_usize_arg(
    arguments: &Value,
    name: &str,
    minimum: usize,
    maximum: Option<usize>,
) -> Result<Option<usize>> {
    bounded_usize_value(
        arguments.get(name),
        &format!("argument {name}"),
        minimum,
        maximum,
    )
}

fn bounded_usize_field(
    object: &Value,
    name: &str,
    default: usize,
    minimum: usize,
    maximum: Option<usize>,
) -> Result<usize> {
    Ok(
        bounded_usize_value(object.get(name), &format!("range {name}"), minimum, maximum)?
            .unwrap_or(default),
    )
}

fn bounded_usize_value(
    value: Option<&Value>,
    label: &str,
    minimum: usize,
    maximum: Option<usize>,
) -> Result<Option<usize>> {
    let Some(value) = value else {
        return Ok(None);
    };
    let value = value
        .as_u64()
        .ok_or_else(|| anyhow!("{label} must be a non-negative integer"))?;
    let value = usize::try_from(value).map_err(|_| anyhow!("{label} is too large"))?;
    if value < minimum {
        if minimum == 1 {
            return Err(anyhow!("{label} must be a positive integer"));
        }
        return Err(anyhow!("{label} must be at least {minimum}"));
    }
    if let Some(maximum) = maximum {
        if value > maximum {
            return Err(anyhow!("{label} has {value}, max {maximum}"));
        }
    }
    Ok(Some(value))
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
        path: optional_string_arg_any(arguments, &["path", "dir"]),
        language: optional_string_arg(arguments, "language"),
        extension: optional_string_arg(arguments, "extension"),
        symbol: optional_string_arg(arguments, "symbol"),
        symbol_kind: optional_string_arg(arguments, "symbol_kind")
            .map(|value| value.to_ascii_lowercase()),
        dependency: optional_string_arg(arguments, "dependency")
            .map(|value| value.to_ascii_lowercase()),
        import: optional_string_arg(arguments, "import").map(|value| value.to_ascii_lowercase()),
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
        require_all: bool_arg(arguments, "require_all") && !bool_arg(arguments, "any_terms"),
        match_any: bool_arg(arguments, "any_terms"),
        exclude_file: optional_string_list_arg(arguments, "exclude_file")?,
        exclude_path: optional_string_list_arg(arguments, "exclude_path")?,
        exclude_language: normalized_string_list_arg(arguments, "exclude_language")?,
        exclude_extension: normalized_string_list_arg(arguments, "exclude_extension")?,
        exclude_symbol: optional_string_list_arg(arguments, "exclude_symbol")?,
        exclude_symbol_kind: normalized_string_list_arg(arguments, "exclude_symbol_kind")?,
        exclude_repo: optional_string_list_arg(arguments, "exclude_repo")?,
        exclude_dependency: normalized_string_list_arg(arguments, "exclude_dependency")?,
        exclude_import: normalized_string_list_arg(arguments, "exclude_import")?,
        ..SearchFilters::default()
    })
}
