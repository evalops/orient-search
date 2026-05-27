use crate::fast_index::FastIndex;
use crate::query::{merge_filters, parse_query};
use crate::repo_index::{
    RepoIndexer, SearchFilters, SearchResult, SnippetMode, Symbol, finalize_results,
    normalize_token, read_file_range, repo_matches, search_repo_fast_filtered,
};
use crate::shards::{ShardRepoMap, build_shards, load_manifest, read_shard_range, refresh_shards};
use anyhow::{Result, anyhow};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::collections::HashMap;
use std::io::{BufRead, Write};
use std::net::{TcpListener, TcpStream};
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
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
    let runtime = Arc::new(Mutex::new(runtime));
    for stream in listener.incoming() {
        let stream = stream?;
        let runtime = Arc::clone(&runtime);
        thread::spawn(move || {
            let _ = serve_tcp_stream(stream, runtime);
        });
    }
    Ok(())
}

fn serve_tcp_stream(stream: TcpStream, runtime: Arc<Mutex<ToolRuntime>>) -> Result<()> {
    let reader = std::io::BufReader::new(stream.try_clone()?);
    let mut writer = stream;
    for line in reader.lines() {
        let line = line?;
        if line.trim().is_empty() {
            continue;
        }
        let response = {
            let mut runtime = runtime
                .lock()
                .map_err(|_| anyhow!("tool runtime lock poisoned"))?;
            runtime.dispatch_line(&line)
        };
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
    indexes: HashMap<PathBuf, FastIndex>,
}

impl ToolRuntime {
    pub fn warm_index(&mut self, index_path: PathBuf) -> Result<PathBuf> {
        let key = canonical_cache_key(&index_path);
        if !self.indexes.contains_key(&key) {
            let index = FastIndex::load(&index_path)?;
            self.indexes.insert(key.clone(), index);
        }
        Ok(key)
    }

    pub fn warm_shards(&mut self, index_dir: PathBuf) -> Result<usize> {
        let manifest = load_manifest(&index_dir)?;
        let mut warmed = 0usize;
        for shard in manifest.shards {
            self.warm_index(index_dir.join(&shard.index))?;
            warmed += 1;
        }
        Ok(warmed)
    }

    pub fn cached_index_count(&self) -> usize {
        self.indexes.len()
    }

    pub fn dispatch_line(&mut self, line: &str) -> ToolResponse {
        match serde_json::from_str::<ToolRequest>(line) {
            Ok(request) => self.dispatch(request),
            Err(error) => ToolResponse {
                id: Value::Null,
                result: None,
                error: Some(error.to_string()),
            },
        }
    }

    pub fn dispatch(&mut self, request: ToolRequest) -> ToolResponse {
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
            "name": "daemon_status",
            "description": "Return local daemon runtime cache status for warm-index clients.",
            "required": [],
            "optional": []
        },
        {
            "name": "warm_index",
            "description": "Load a persistent single-repo index into the daemon cache before searches need it.",
            "required": ["index"],
            "optional": []
        },
        {
            "name": "warm_shards",
            "description": "Load every shard index from a local shard directory into the daemon cache.",
            "required": ["index_dir"],
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
            "name": "indexed_repo_map",
            "description": "Return repo-map orientation from a persistent single-repo index.",
            "required": ["index"],
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
            "name": "shard_repo_map",
            "description": "Return repo-map orientation for every matching repo in a local shard directory.",
            "required": ["index_dir"],
            "optional": ["symbols", "tests", "repo", "repo_filter"]
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

impl ToolRuntime {
    fn dispatch_result(&mut self, request: &ToolRequest) -> Result<Value> {
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
                let index = self.cached_index(index_path)?;
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
                let index = self.cached_index(index_path)?;
                Ok(serde_json::to_value(read_file_range(
                    &index.root,
                    &path,
                    start,
                    lines,
                )?)?)
            }
            "index_shards" => {
                let repos = path_array_arg(&request.arguments, "repos")?;
                let output_dir = path_arg(&request.arguments, "output_dir")?;
                let stats = build_shards(&repos, output_dir)?;
                self.indexes.clear();
                Ok(serde_json::to_value(stats)?)
            }
            "refresh_shards" => {
                let index_dir = path_arg(&request.arguments, "index_dir")?;
                let stats = refresh_shards(index_dir)?;
                self.indexes.clear();
                Ok(serde_json::to_value(stats)?)
            }
            "search_shards" => {
                let index_dir = path_arg(&request.arguments, "index_dir")?;
                let query = string_arg(&request.arguments, "query")?;
                let limit = usize_arg(&request.arguments, "limit").unwrap_or(10);
                Ok(serde_json::to_value(self.search_shards_cached(
                    &index_dir,
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
            "shard_repo_map" => {
                let index_dir = path_arg(&request.arguments, "index_dir")?;
                let symbol_limit = usize_arg(&request.arguments, "symbols").unwrap_or(50);
                let test_limit = usize_arg(&request.arguments, "tests").unwrap_or(50);
                Ok(serde_json::to_value(self.shard_repo_maps_cached(
                    &index_dir,
                    symbol_limit,
                    test_limit,
                    &search_filters(&request.arguments, true),
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
                let fast_index = self.cached_index(index_path)?;
                let index = RepoIndexer::new(&fast_index.root).build()?;
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
                let fast_index = self.cached_index(index_path)?;
                let index = RepoIndexer::new(&fast_index.root).build()?;
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
                    "cached_indexes": self.indexes.len(),
                    "index": key
                }))
            }
            "warm_shards" => {
                let index_dir = path_arg(&request.arguments, "index_dir")?;
                let warmed_indexes = self.warm_shards(index_dir)?;
                Ok(json!({
                    "cached_indexes": self.indexes.len(),
                    "warmed_indexes": warmed_indexes
                }))
            }
            "daemon_status" => Ok(json!({
                "cached_indexes": self.indexes.len(),
                "cached_index_paths": self.cached_index_paths()
            })),
            "tool_manifest" => Ok(tool_manifest()),
            "list_tools" => Ok(json!([
                "list_tools",
                "tool_manifest",
                "daemon_status",
                "warm_index",
                "warm_shards",
                "repo_brief",
                "repo_map",
                "indexed_repo_map",
                "read_range",
                "search_code",
                "indexed_search_code",
                "read_index_range",
                "index_shards",
                "refresh_shards",
                "search_shards",
                "read_shard_range",
                "shard_repo_map",
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

    fn cached_index(&mut self, index_path: PathBuf) -> Result<&FastIndex> {
        let key = self.warm_index(index_path)?;
        Ok(self.indexes.get(&key).expect("cached index inserted"))
    }

    fn cached_index_paths(&self) -> Vec<String> {
        let mut paths = self
            .indexes
            .keys()
            .map(|path| path.to_string_lossy().to_string())
            .collect::<Vec<_>>();
        paths.sort();
        paths
    }

    fn search_shards_cached(
        &mut self,
        index_dir: &std::path::Path,
        query: &str,
        limit: usize,
        filters: &SearchFilters,
    ) -> Result<Vec<SearchResult>> {
        let manifest = load_manifest(index_dir)?;
        let parsed = parse_query(query);
        let filters = merge_filters(filters.clone(), parsed.filters);
        let mut results = Vec::new();
        for shard in manifest.shards {
            if !repo_matches(&shard.root, &filters) {
                continue;
            }
            let index = self.cached_index(index_dir.join(&shard.index))?;
            for mut result in index.search_filtered(query, limit, &filters)? {
                result.path = format!("{}/{}", shard.name, result.path);
                result.reason = format!("shard:{}; {}", shard.name, result.reason);
                results.push(result);
            }
        }
        Ok(finalize_results(results, limit))
    }

    fn shard_repo_maps_cached(
        &mut self,
        index_dir: &std::path::Path,
        symbol_limit: usize,
        test_limit: usize,
        filters: &SearchFilters,
    ) -> Result<Vec<ShardRepoMap>> {
        let manifest = load_manifest(index_dir)?;
        let mut maps = Vec::new();
        for shard in manifest.shards {
            if !repo_matches(&shard.root, filters) {
                continue;
            }
            let index = self.cached_index(index_dir.join(&shard.index))?;
            let mut map = index.repo_map(symbol_limit, test_limit);
            prefix_repo_map_paths(&mut map, &shard.name);
            maps.push(ShardRepoMap {
                name: shard.name,
                root: shard.root,
                map,
            });
        }
        maps.sort_by(|a, b| a.name.cmp(&b.name));
        Ok(maps)
    }

    fn find_shard_symbol_cached(
        &mut self,
        index_dir: &std::path::Path,
        name: &str,
        limit: usize,
        filters: &SearchFilters,
    ) -> Result<Vec<Symbol>> {
        let needle = normalize_token(name);
        if needle.is_empty() || limit == 0 {
            return Ok(Vec::new());
        }

        let manifest = load_manifest(index_dir)?;
        let mut symbols = Vec::new();
        for shard in manifest.shards {
            if !repo_matches(&shard.root, filters) {
                continue;
            }
            let index = self.cached_index(index_dir.join(&shard.index))?;
            for mut symbol in index.find_symbol(name, limit) {
                symbol.path = format!("{}/{}", shard.name, symbol.path);
                symbols.push(symbol);
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

fn canonical_cache_key(path: &std::path::Path) -> PathBuf {
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

fn prefix_repo_map_paths(map: &mut crate::repo_index::RepoMap, shard_name: &str) {
    for path in &mut map.brief.manifest_files {
        *path = format!("{shard_name}/{path}");
    }
    for path in &mut map.brief.important_files {
        *path = format!("{shard_name}/{path}");
    }
    for path in &mut map.entrypoints {
        *path = format!("{shard_name}/{path}");
    }
    for path in &mut map.test_files {
        *path = format!("{shard_name}/{path}");
    }
    for symbol in &mut map.top_symbols {
        symbol.path = format!("{shard_name}/{}", symbol.path);
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
