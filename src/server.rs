use crate::discover::{
    DiscoverOptions, DiscoverySelectionSummary, discover_repos, discovery_selection_summary,
    git_metadata_for_repo,
};
use crate::fast_index::{FastIndex, IndexFreshness, RefreshStats};
use crate::query::{merge_filters, normalize_symbol_kind, parse_query, query_text};
pub use crate::repo_index::MAX_BATCH_READ_LINES;
use crate::repo_index::{
    DEFAULT_REPO_MAP_READ_BATCH_RANGES, FileRange, MAX_ATTACHED_CONTEXT_LINES,
    MAX_READ_RANGE_LINES, MAX_RESULT_READ_BATCH_RANGES, MAX_SEARCH_RESULTS, QueryPlan,
    QueryPlanFilter, QueryPlanNextAction, QueryPlanSummary, RangeScope, RepoIndexer, RepoMapDetail,
    ResultToolRequest, SearchFilters, SearchResult, SnippetMode, Symbol, SymbolLookupResult,
    attach_repo_map_read_batch_request_with_limit, attach_result_context,
    attach_result_read_requests, attach_result_related_requests,
    attach_result_related_symbol_requests, finalize_results_for_filters,
    grouped_duplicate_count_from_results, grouped_duplicate_count_from_value, language_for,
    normalize_language_filter, normalize_token, query_plan_filter_field_present,
    read_batch_action_summary, read_file_range, read_file_range_scoped,
    related_file_lookup_results, related_symbol_lookup_results, result_read_batch_request,
    result_value_read_batch_request, search_repo_fast_filtered, symbol_lookup_read_batch_request,
    symbol_lookup_results,
};
use crate::shards::{
    ShardEntry, ShardFreshness, ShardManifest, ShardQueryPlan, ShardRepoMap, ShardSearchScope,
    append_shard_facet_repair_hints, bounded_shard_worker_count, build_shards_with_force,
    configured_max_shard_workers, ensure_shards, filter_repo_map_by_prefix,
    filters_for_shard_scope, load_manifest, refresh_shards, refresh_shards_by_root,
    related_query_without_shard_selectors, resolve_shard_path_from_manifest,
    shard_prefilter_query_impossible, shard_route_entries, shard_route_selection,
    shard_search_scopes, shard_selection_miss_plan, shard_sketch_may_diagnose_query,
    shard_sketch_may_match_query, shard_status, shard_status_by_root,
};
use ahash::{AHashMap as HashMap, AHashSet as HashSet};
use anyhow::{Context, Result, anyhow};
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value, json};
use std::fs;
use std::io::{BufRead, BufReader, Read, Write};
use std::net::TcpListener;
use std::path::{Path, PathBuf};
use std::sync::{
    Arc, Condvar, Mutex,
    atomic::{AtomicU64, Ordering as AtomicOrdering},
};
use std::thread;
use std::time::SystemTime;

pub const MAX_BATCH_QUERIES: usize = 32;
pub const MAX_BATCH_RANGES: usize = 64;
pub const DEFAULT_MAX_CACHED_INDEXES: usize = 64;
const DEFAULT_DAEMON_ADDR: &str = "127.0.0.1:8796";

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
    summary: SearchResultSummary,
    query_plan_request: ResultToolRequest,
    repo_map_request: ResultToolRequest,
    #[serde(skip_serializing_if = "Option::is_none")]
    read_batch_request: Option<ResultToolRequest>,
    #[serde(skip_serializing_if = "Option::is_none")]
    next_action: Option<Value>,
    results: Vec<SearchResult>,
}

#[derive(Debug, Serialize)]
struct SearchResultSummary {
    status: String,
    result_count: usize,
    #[serde(skip_serializing_if = "Option::is_none")]
    primary_retry_status: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    primary_retry_result_count: Option<usize>,
    #[serde(skip_serializing_if = "is_zero")]
    primary_retry_grouped_duplicate_count: usize,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    primary_retry_top_paths: Vec<String>,
    #[serde(skip_serializing_if = "is_zero")]
    grouped_duplicate_count: usize,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    top_paths: Vec<String>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    top_dirs: Vec<String>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    top_exts: Vec<String>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    top_langs: Vec<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    max_score: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    min_score: Option<f64>,
}

#[derive(Debug, Serialize)]
struct ReadRangesResponseSummary {
    status: &'static str,
    range_count: usize,
    total_lines: usize,
    path_count: usize,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    paths: Vec<String>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    top_dirs: Vec<String>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    top_exts: Vec<String>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    top_langs: Vec<String>,
}

fn search_result_summary(results: &[SearchResult]) -> SearchResultSummary {
    let result_count = results.len();
    SearchResultSummary {
        status: if result_count == 0 {
            "not_found".to_string()
        } else {
            "matched".to_string()
        },
        result_count,
        primary_retry_status: None,
        primary_retry_result_count: None,
        primary_retry_grouped_duplicate_count: 0,
        primary_retry_top_paths: Vec::new(),
        grouped_duplicate_count: grouped_duplicate_count_from_results(results),
        top_paths: search_summary_top_paths(results),
        top_dirs: search_summary_top_dirs(results),
        top_exts: search_summary_top_exts(results),
        top_langs: search_summary_top_langs(results),
        max_score: results.first().map(|result| result.score),
        min_score: results.last().map(|result| result.score),
    }
}

fn search_result_summary_with_primary_retry(
    results: &[SearchResult],
    primary_retry_result: &Option<Value>,
) -> SearchResultSummary {
    let mut summary = search_result_summary(results);
    let Some(retry_summary) = primary_retry_result
        .as_ref()
        .and_then(|result| result.get("summary"))
    else {
        return summary;
    };
    summary.primary_retry_status = retry_summary
        .get("status")
        .and_then(Value::as_str)
        .map(ToString::to_string);
    summary.primary_retry_result_count = retry_summary
        .get("result_count")
        .and_then(Value::as_u64)
        .and_then(|count| usize::try_from(count).ok());
    summary.primary_retry_grouped_duplicate_count = retry_summary
        .get("grouped_duplicate_count")
        .and_then(Value::as_u64)
        .and_then(|count| usize::try_from(count).ok())
        .unwrap_or(0);
    summary.primary_retry_top_paths = retry_summary
        .get("top_paths")
        .and_then(Value::as_array)
        .map(|paths| {
            paths
                .iter()
                .filter_map(Value::as_str)
                .map(ToString::to_string)
                .collect()
        })
        .unwrap_or_default();
    summary
}

fn search_summary_top_paths(results: &[SearchResult]) -> Vec<String> {
    let mut paths = Vec::new();
    for result in results {
        if !paths.iter().any(|path| path == &result.path) {
            paths.push(result.path.clone());
            if paths.len() == 5 {
                break;
            }
        }
    }
    paths
}

fn search_summary_top_dirs(results: &[SearchResult]) -> Vec<String> {
    let mut dirs = Vec::new();
    for result in results {
        let dir = search_summary_dir(&result.path);
        if !dirs.iter().any(|existing| existing == &dir) {
            dirs.push(dir);
            if dirs.len() == 5 {
                break;
            }
        }
    }
    dirs
}

fn search_summary_dir(path: &str) -> String {
    path.rsplit_once('/')
        .map(|(dir, _)| {
            if dir.is_empty() {
                ".".to_string()
            } else {
                dir.to_string()
            }
        })
        .unwrap_or_else(|| ".".to_string())
}

fn search_summary_top_exts(results: &[SearchResult]) -> Vec<String> {
    let mut exts = Vec::new();
    for result in results {
        let Some(ext) = search_summary_ext(&result.path) else {
            continue;
        };
        if !exts.iter().any(|existing| existing == &ext) {
            exts.push(ext);
            if exts.len() == 5 {
                break;
            }
        }
    }
    exts
}

fn search_summary_top_langs(results: &[SearchResult]) -> Vec<String> {
    let mut langs = Vec::new();
    for result in results {
        let Some(language) = language_for(Path::new(&result.path)) else {
            continue;
        };
        if !langs.iter().any(|existing| existing == &language) {
            langs.push(language);
            if langs.len() == 5 {
                break;
            }
        }
    }
    langs
}

fn search_summary_ext(path: &str) -> Option<String> {
    let ext = Path::new(path).extension()?.to_string_lossy();
    let ext = ext.trim();
    if ext.is_empty() {
        None
    } else {
        Some(ext.to_ascii_lowercase())
    }
}

fn search_batch_result(
    query: String,
    query_plan_request: ResultToolRequest,
    repo_map_request: ResultToolRequest,
    read_batch_request: Option<ResultToolRequest>,
    results: Vec<SearchResult>,
) -> SearchBatchResult {
    let next_action = search_batch_next_action(&read_batch_request, &query_plan_request);
    let summary = search_result_summary(&results);
    SearchBatchResult {
        query,
        summary,
        query_plan_request,
        repo_map_request,
        read_batch_request,
        next_action,
        results,
    }
}

fn search_batch_followups<T: Serialize + ?Sized>(
    query_plan_tool: &str,
    repo_map_tool: &str,
    target_name: &str,
    target_value: &T,
    source_arguments: &Value,
    query: &str,
    shard_scope_filters: Option<&SearchFilters>,
) -> (ResultToolRequest, ResultToolRequest) {
    (
        auto_query_plan_request(
            query_plan_tool,
            target_name,
            target_value,
            source_arguments,
            query,
        ),
        auto_repo_map_request(
            repo_map_tool,
            target_name,
            target_value,
            source_arguments,
            shard_scope_filters,
        ),
    )
}

fn search_batch_next_action(
    read_batch_request: &Option<ResultToolRequest>,
    query_plan_request: &ResultToolRequest,
) -> Option<Value> {
    read_batch_next_action(read_batch_request).or_else(|| {
        Some(json!({
            "kind": "query_plan",
            "source": "query_plan_request",
            "summary": "Plan a repaired or narrower query for this empty batch item.",
            "request": query_plan_request
        }))
    })
}

fn read_batch_next_action(read_batch_request: &Option<ResultToolRequest>) -> Option<Value> {
    read_batch_request.as_ref().map(|request| {
        let summary =
            read_batch_action_summary(request, "Read the batch item's top matching ranges.");
        json!({
            "kind": "read",
            "source": "read_batch_request",
            "summary": summary,
            "request": request
        })
    })
}

#[derive(Debug, Serialize)]
struct SearchAutoResult {
    query: String,
    summary: SearchResultSummary,
    surface: String,
    target: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    freshness: Option<SearchFreshness>,
    #[serde(skip_serializing_if = "Option::is_none")]
    refresh_request: Option<ResultToolRequest>,
    query_plan_request: ResultToolRequest,
    #[serde(skip_serializing_if = "Option::is_none")]
    query_plan_result: Option<Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    query_plan_summary: Option<Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    primary_diagnosis: Option<Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    primary_retry_request: Option<ResultToolRequest>,
    #[serde(skip_serializing_if = "Option::is_none")]
    primary_retry_result: Option<Value>,
    repo_map_request: ResultToolRequest,
    #[serde(skip_serializing_if = "Option::is_none")]
    read_batch_request: Option<ResultToolRequest>,
    #[serde(skip_serializing_if = "Option::is_none")]
    next_read_batch_request: Option<ResultToolRequest>,
    #[serde(skip_serializing_if = "Option::is_none")]
    next_action: Option<Value>,
    results: Vec<SearchResult>,
}

fn compact_optional_query_plan_result(summary: bool, result: Option<Value>) -> Option<Value> {
    if summary { None } else { result }
}

#[derive(Debug, Serialize)]
struct SearchFreshness {
    stale: bool,
    summary: String,
    #[serde(skip_serializing_if = "is_zero")]
    checked_files: usize,
    #[serde(skip_serializing_if = "is_zero")]
    changed_files: usize,
    #[serde(skip_serializing_if = "is_zero")]
    added_files: usize,
    #[serde(skip_serializing_if = "is_zero")]
    deleted_files: usize,
    #[serde(skip_serializing_if = "is_zero")]
    stale_shards: usize,
    #[serde(skip_serializing_if = "is_zero")]
    git_metadata_changed: usize,
    refresh_request: ResultToolRequest,
}

#[derive(Debug, Serialize)]
struct IndexedQueryPlanBatchResult {
    query: String,
    summary: QueryPlanSummary,
    #[serde(skip_serializing_if = "Option::is_none")]
    next_action: Option<QueryPlanNextAction>,
    plan: QueryPlan,
}

#[derive(Debug, Serialize)]
struct QueryPlanBatchResult {
    query: String,
    summary: QueryPlanSummary,
    #[serde(skip_serializing_if = "Option::is_none")]
    next_action: Option<QueryPlanNextAction>,
    plan: QueryPlan,
}

#[derive(Debug, Serialize)]
struct ShardQueryPlanBatchResult {
    query: String,
    summary: QueryPlanSummary,
    #[serde(skip_serializing_if = "Option::is_none")]
    next_action: Option<QueryPlanNextAction>,
    plans: Vec<ShardQueryPlan>,
}

fn indexed_query_plan_batch_result(query: String, plan: QueryPlan) -> IndexedQueryPlanBatchResult {
    let next_action = plan.next_action.clone();
    let summary = plan.compact_summary();
    IndexedQueryPlanBatchResult {
        query,
        summary,
        next_action,
        plan,
    }
}

fn query_plan_batch_result(query: String, plan: QueryPlan) -> QueryPlanBatchResult {
    let next_action = plan.next_action.clone();
    let summary = plan.compact_summary();
    QueryPlanBatchResult {
        query,
        summary,
        next_action,
        plan,
    }
}

fn shard_query_plan_batch_result(
    query: String,
    plans: Vec<ShardQueryPlan>,
) -> ShardQueryPlanBatchResult {
    let next_action = plans
        .iter()
        .find_map(|shard_plan| shard_plan.plan.next_action.clone());
    let summary = plans
        .iter()
        .find(|shard_plan| {
            shard_plan.plan.final_match_count > 0 || shard_plan.plan.next_action.is_some()
        })
        .or_else(|| plans.first())
        .map(|shard_plan| shard_plan.plan.compact_summary())
        .unwrap_or_else(|| QueryPlan::empty("no_shards", true).compact_summary());
    ShardQueryPlanBatchResult {
        query,
        summary,
        next_action,
        plans,
    }
}

fn shard_query_plan_summary_value(plans: &[ShardQueryPlan]) -> Value {
    Value::Array(
        plans
            .iter()
            .map(|shard_plan| {
                let mut item = json!({
                    "name": shard_plan.name,
                    "root": shard_plan.root,
                    "aliases": shard_plan.aliases,
                    "summary": shard_plan
                        .summary
                        .clone()
                        .unwrap_or_else(|| shard_plan.plan.compact_summary())
                });
                if let Some(next_action) = &shard_plan.next_action {
                    if let Some(object) = item.as_object_mut() {
                        object.insert("next_action".to_string(), json!(next_action));
                    }
                }
                item
            })
            .collect(),
    )
}

fn query_plan_response_value(plan: QueryPlan, summary_only: bool) -> Result<Value> {
    if summary_only {
        Ok(serde_json::to_value(plan.compact_summary())?)
    } else {
        Ok(serde_json::to_value(plan)?)
    }
}

fn query_plan_batch_response_value(
    query: String,
    plan: QueryPlan,
    summary_only: bool,
) -> Result<Value> {
    if summary_only {
        let next_action = plan.next_action.clone();
        let mut item = Map::new();
        item.insert("query".to_string(), Value::String(query));
        item.insert(
            "summary".to_string(),
            serde_json::to_value(plan.compact_summary())?,
        );
        if let Some(next_action) = next_action {
            item.insert(
                "next_action".to_string(),
                serde_json::to_value(next_action)?,
            );
        }
        Ok(Value::Object(item))
    } else {
        Ok(serde_json::to_value(query_plan_batch_result(query, plan))?)
    }
}

fn indexed_query_plan_batch_response_value(
    query: String,
    plan: QueryPlan,
    summary_only: bool,
) -> Result<Value> {
    if summary_only {
        query_plan_batch_response_value(query, plan, true)
    } else {
        Ok(serde_json::to_value(indexed_query_plan_batch_result(
            query, plan,
        ))?)
    }
}

fn shard_query_plan_response_value(plans: &[ShardQueryPlan], summary_only: bool) -> Result<Value> {
    if summary_only {
        Ok(shard_query_plan_summary_value(plans))
    } else {
        Ok(serde_json::to_value(plans)?)
    }
}

fn shard_query_plan_batch_response_value(
    query: String,
    plans: Vec<ShardQueryPlan>,
    summary_only: bool,
) -> Result<Value> {
    if summary_only {
        let next_action = plans
            .iter()
            .find_map(|shard_plan| shard_plan.plan.next_action.clone());
        let summary = plans
            .iter()
            .find(|shard_plan| {
                shard_plan.plan.final_match_count > 0 || shard_plan.plan.next_action.is_some()
            })
            .or_else(|| plans.first())
            .map(|shard_plan| shard_plan.plan.compact_summary())
            .unwrap_or_else(|| QueryPlan::empty("no_shards", true).compact_summary());
        let mut item = Map::new();
        item.insert("query".to_string(), Value::String(query));
        item.insert("summary".to_string(), serde_json::to_value(summary)?);
        if let Some(next_action) = next_action {
            item.insert(
                "next_action".to_string(),
                serde_json::to_value(next_action)?,
            );
        }
        item.insert("shards".to_string(), shard_query_plan_summary_value(&plans));
        Ok(Value::Object(item))
    } else {
        Ok(serde_json::to_value(shard_query_plan_batch_result(
            query, plans,
        ))?)
    }
}

#[derive(Debug, Serialize)]
struct SymbolBatchResult {
    name: String,
    summary: SymbolBatchSummary,
    #[serde(skip_serializing_if = "Option::is_none")]
    read_batch_request: Option<ResultToolRequest>,
    #[serde(skip_serializing_if = "Option::is_none")]
    next_action: Option<Value>,
    symbols: Vec<SymbolLookupResult>,
}

#[derive(Debug, Serialize)]
struct SymbolLookupResponse {
    summary: SymbolBatchSummary,
    results: Vec<SymbolLookupResult>,
    #[serde(skip_serializing_if = "Option::is_none")]
    read_batch_request: Option<ResultToolRequest>,
    #[serde(skip_serializing_if = "Option::is_none")]
    next_action: Option<Value>,
}

#[derive(Debug, Serialize)]
struct SymbolBatchSummary {
    status: String,
    symbol_count: usize,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    top_paths: Vec<String>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    top_dirs: Vec<String>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    top_exts: Vec<String>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    top_langs: Vec<String>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    kinds: Vec<String>,
}

fn symbol_batch_summary(symbols: &[SymbolLookupResult]) -> SymbolBatchSummary {
    let symbol_count = symbols.len();
    SymbolBatchSummary {
        status: if symbol_count == 0 {
            "not_found".to_string()
        } else {
            "matched".to_string()
        },
        symbol_count,
        top_paths: symbol_summary_top_paths(symbols),
        top_dirs: symbol_summary_top_dirs(symbols),
        top_exts: symbol_summary_top_exts(symbols),
        top_langs: symbol_summary_top_langs(symbols),
        kinds: symbol_summary_kinds(symbols),
    }
}

fn symbol_summary_top_paths(symbols: &[SymbolLookupResult]) -> Vec<String> {
    let mut paths = Vec::new();
    for symbol in symbols {
        if !paths.iter().any(|path| path == &symbol.symbol.path) {
            paths.push(symbol.symbol.path.clone());
            if paths.len() == 5 {
                break;
            }
        }
    }
    paths
}

fn symbol_summary_top_dirs(symbols: &[SymbolLookupResult]) -> Vec<String> {
    let mut dirs = Vec::new();
    for symbol in symbols {
        let dir = search_summary_dir(&symbol.symbol.path);
        if !dirs.iter().any(|existing| existing == &dir) {
            dirs.push(dir);
            if dirs.len() == 5 {
                break;
            }
        }
    }
    dirs
}

fn symbol_summary_top_exts(symbols: &[SymbolLookupResult]) -> Vec<String> {
    let mut exts = Vec::new();
    for symbol in symbols {
        let Some(ext) = search_summary_ext(&symbol.symbol.path) else {
            continue;
        };
        if !exts.iter().any(|existing| existing == &ext) {
            exts.push(ext);
            if exts.len() == 5 {
                break;
            }
        }
    }
    exts
}

fn symbol_summary_top_langs(symbols: &[SymbolLookupResult]) -> Vec<String> {
    let mut langs = Vec::new();
    for symbol in symbols {
        let Some(language) = language_for(Path::new(&symbol.symbol.path)) else {
            continue;
        };
        if !langs.iter().any(|existing| existing == &language) {
            langs.push(language);
            if langs.len() == 5 {
                break;
            }
        }
    }
    langs
}

fn symbol_summary_kinds(symbols: &[SymbolLookupResult]) -> Vec<String> {
    let mut kinds = Vec::new();
    for symbol in symbols {
        if !kinds.iter().any(|kind| kind == &symbol.symbol.kind) {
            kinds.push(symbol.symbol.kind.clone());
            if kinds.len() == 5 {
                break;
            }
        }
    }
    kinds
}

fn symbol_lookup_response(
    symbols: Vec<SymbolLookupResult>,
    include_read_batch: bool,
    batch_tool: &str,
    base_arguments: serde_json::Map<String, serde_json::Value>,
) -> Result<Value> {
    if !include_read_batch {
        return Ok(serde_json::to_value(symbols)?);
    }
    let read_batch_request = symbol_lookup_read_batch_request(&symbols, batch_tool, base_arguments);
    let next_action = read_batch_next_action(&read_batch_request);
    let summary = symbol_batch_summary(&symbols);
    Ok(serde_json::to_value(SymbolLookupResponse {
        summary,
        results: symbols,
        read_batch_request,
        next_action,
    })?)
}

fn symbol_batch_result(
    name: String,
    read_batch_request: Option<ResultToolRequest>,
    symbols: Vec<SymbolLookupResult>,
) -> SymbolBatchResult {
    let summary = symbol_batch_summary(&symbols);
    let next_action = read_batch_next_action(&read_batch_request);
    SymbolBatchResult {
        name,
        summary,
        read_batch_request,
        next_action,
        symbols,
    }
}

pub fn serve_jsonl(reader: impl BufRead, mut writer: impl Write) -> Result<()> {
    let mut runtime = ToolRuntime::default();
    serve_jsonl_with_runtime(reader, &mut writer, &mut runtime)
}

pub fn serve_mcp(reader: impl BufRead, mut writer: impl Write) -> Result<()> {
    let runtime = ToolRuntime::default();
    serve_mcp_with_runtime(reader, &mut writer, &runtime)
}

pub fn serve_mcp_with_runtime(
    reader: impl BufRead,
    mut writer: impl Write,
    runtime: &ToolRuntime,
) -> Result<()> {
    for line in reader.lines() {
        let line = line?;
        if line.trim().is_empty() {
            continue;
        }
        if let Some(response) = mcp_dispatch_line(runtime, &line) {
            writeln!(writer, "{}", serde_json::to_string(&response)?)?;
            writer.flush()?;
        }
    }
    Ok(())
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
        let client_command = stream
            .local_addr()
            .ok()
            .map(|addr| tcp_client_command(&addr.to_string()));
        let runtime = Arc::clone(&runtime);
        thread::spawn(move || {
            let _ = serve_jsonl_stream_with_client_command(stream, runtime, client_command);
        });
    }
    Ok(())
}

pub fn serve_jsonl_stream(stream: impl Read + Write, runtime: Arc<ToolRuntime>) -> Result<()> {
    serve_jsonl_stream_with_client_command(stream, runtime, None)
}

pub fn serve_jsonl_stream_with_client_command(
    stream: impl Read + Write,
    runtime: Arc<ToolRuntime>,
    client_command: Option<String>,
) -> Result<()> {
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
        let mut response = runtime.dispatch_line(&line);
        if let (Some(result), Some(client_command)) =
            (response.result.as_mut(), client_command.as_deref())
        {
            retarget_client_cli_commands(result, client_command);
        }
        writeln!(reader.get_mut(), "{}", serde_json::to_string(&response)?)?;
        reader.get_mut().flush()?;
    }
    Ok(())
}

pub fn dispatch(request: ToolRequest) -> ToolResponse {
    ToolRuntime::default().dispatch(request)
}

pub fn mcp_dispatch_line(runtime: &ToolRuntime, line: &str) -> Option<Value> {
    let request = match serde_json::from_str::<Value>(line) {
        Ok(request) => request,
        Err(error) => {
            return Some(mcp_error(Value::Null, -32700, error.to_string()));
        }
    };
    mcp_dispatch_value(runtime, &request)
}

pub fn mcp_dispatch_value(runtime: &ToolRuntime, request: &Value) -> Option<Value> {
    let id = request.get("id").cloned();
    let method = request.get("method").and_then(Value::as_str);
    if id.is_none() && method.is_some_and(|method| method.starts_with("notifications/")) {
        return None;
    }
    let id = id.unwrap_or(Value::Null);
    let Some(method) = method else {
        return Some(mcp_error(id, -32600, "missing JSON-RPC method"));
    };
    match method {
        "initialize" => Some(mcp_result(
            id,
            json!({
                "protocolVersion": "2024-11-05",
                "capabilities": {"tools": {}},
                "serverInfo": {
                    "name": "orient-search",
                    "version": env!("CARGO_PKG_VERSION")
                }
            }),
        )),
        "tools/list" => Some(mcp_result(id, mcp_tool_manifest())),
        "tools/call" => Some(mcp_tool_call(runtime, id, request)),
        _ => Some(mcp_error(
            id,
            -32601,
            format!("unknown MCP method: {method}"),
        )),
    }
}

fn mcp_tool_call(runtime: &ToolRuntime, id: Value, request: &Value) -> Value {
    let params = request.get("params").unwrap_or(&Value::Null);
    let Some(name) = params.get("name").and_then(Value::as_str) else {
        return mcp_error(id, -32602, "tools/call params.name is required");
    };
    let arguments = params
        .get("arguments")
        .cloned()
        .unwrap_or_else(|| json!({}));
    let response = runtime.dispatch(ToolRequest {
        id: id.clone(),
        tool: name.to_string(),
        arguments,
    });
    match response.result {
        Some(result) => mcp_result(id, mcp_tool_result(result, false)),
        None => mcp_result(
            id,
            mcp_tool_result(json!({"error": response.error.unwrap_or_default()}), true),
        ),
    }
}

fn mcp_tool_result(value: Value, is_error: bool) -> Value {
    let text = serde_json::to_string(&value).unwrap_or_else(|_| "null".to_string());
    json!({
        "content": [{"type": "text", "text": text}],
        "structuredContent": value,
        "isError": is_error
    })
}

fn mcp_result(id: Value, result: Value) -> Value {
    json!({
        "jsonrpc": "2.0",
        "id": id,
        "result": result
    })
}

fn mcp_error(id: Value, code: i64, message: impl Into<String>) -> Value {
    json!({
        "jsonrpc": "2.0",
        "id": id,
        "error": {
            "code": code,
            "message": message.into()
        }
    })
}

pub struct ToolRuntime {
    indexes: Mutex<HashMap<PathBuf, Arc<IndexCacheEntry>>>,
    shard_manifests: Mutex<HashMap<PathBuf, CachedShardManifest>>,
    next_index_access: AtomicU64,
    cache_policy: IndexCachePolicy,
    started_at: SystemTime,
}

#[derive(Clone, Copy)]
struct IndexCachePolicy {
    max_ready_indexes: Option<usize>,
}

impl Default for ToolRuntime {
    fn default() -> Self {
        Self {
            indexes: Mutex::new(HashMap::new()),
            shard_manifests: Mutex::new(HashMap::new()),
            next_index_access: AtomicU64::new(1),
            cache_policy: IndexCachePolicy::default(),
            started_at: SystemTime::now(),
        }
    }
}

impl Default for IndexCachePolicy {
    fn default() -> Self {
        Self {
            max_ready_indexes: Some(DEFAULT_MAX_CACHED_INDEXES),
        }
    }
}

struct IndexCacheEntry {
    state: Mutex<IndexCacheState>,
    ready: Condvar,
}

#[derive(Clone)]
struct CachedShardManifest {
    manifest: Arc<ShardManifest>,
    fingerprint: Option<CacheFileFingerprint>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct CacheFileFingerprint {
    len: u64,
    modified: Option<SystemTime>,
}

#[derive(Debug, Clone, Copy, Default)]
struct CachedIndexFootprint {
    content_snapshot_bytes: u64,
    line_offset_bytes: usize,
    fingerprint: Option<CacheFileFingerprint>,
}

struct CachedIndexSnapshot {
    index: Arc<FastIndex>,
    fingerprint: Option<CacheFileFingerprint>,
}

#[derive(Default)]
struct CacheDiskState {
    bytes: Option<u64>,
    missing: bool,
    changed: bool,
}

enum IndexCacheState {
    Loading,
    Ready {
        index: Arc<FastIndex>,
        fingerprint: Option<CacheFileFingerprint>,
        last_access: u64,
    },
    Failed(String),
}

#[derive(Clone)]
struct ShardJob {
    shard: ShardEntry,
    scopes: Vec<ShardSearchScope>,
}

fn shard_jobs_from_entries(
    shards: impl IntoIterator<Item = ShardEntry>,
    shard_query: &str,
    filters: &SearchFilters,
    check_sketch: bool,
) -> Vec<ShardJob> {
    shards
        .into_iter()
        .filter_map(|shard| {
            if check_sketch && !shard_sketch_may_match_query(&shard, shard_query, filters) {
                return None;
            }
            let scopes = shard_search_scopes(&shard, filters);
            (!scopes.is_empty()).then_some(ShardJob { shard, scopes })
        })
        .collect()
}

impl IndexCacheEntry {
    fn loading() -> Self {
        Self {
            state: Mutex::new(IndexCacheState::Loading),
            ready: Condvar::new(),
        }
    }

    fn ready(
        index: Arc<FastIndex>,
        fingerprint: Option<CacheFileFingerprint>,
        access: u64,
    ) -> Self {
        Self {
            state: Mutex::new(IndexCacheState::Ready {
                index,
                fingerprint,
                last_access: access,
            }),
            ready: Condvar::new(),
        }
    }

    fn is_ready(&self) -> bool {
        self.state
            .lock()
            .map(|state| matches!(*state, IndexCacheState::Ready { .. }))
            .unwrap_or(false)
    }

    fn ready_snapshot(&self) -> Option<CachedIndexSnapshot> {
        self.state.lock().ok().and_then(|state| match &*state {
            IndexCacheState::Ready {
                index, fingerprint, ..
            } => Some(CachedIndexSnapshot {
                index: Arc::clone(index),
                fingerprint: *fingerprint,
            }),
            IndexCacheState::Loading | IndexCacheState::Failed(_) => None,
        })
    }

    fn ready_is_stale(&self, current_fingerprint: Option<CacheFileFingerprint>) -> bool {
        let Some(current_fingerprint) = current_fingerprint else {
            return false;
        };
        self.state
            .lock()
            .map(|state| match &*state {
                IndexCacheState::Ready { fingerprint, .. } => {
                    *fingerprint != Some(current_fingerprint)
                }
                IndexCacheState::Loading | IndexCacheState::Failed(_) => false,
            })
            .unwrap_or(false)
    }

    fn last_access(&self) -> Option<u64> {
        self.state.lock().ok().and_then(|state| match &*state {
            IndexCacheState::Ready { last_access, .. } => Some(*last_access),
            IndexCacheState::Loading | IndexCacheState::Failed(_) => None,
        })
    }
}

impl ToolRuntime {
    pub fn with_max_cached_indexes(max_cached_indexes: usize) -> Self {
        Self {
            cache_policy: IndexCachePolicy {
                max_ready_indexes: Some(max_cached_indexes.max(1)),
            },
            ..Self::default()
        }
    }

    pub fn warm_index(&self, index_path: PathBuf) -> Result<PathBuf> {
        let (key, _) = self.cached_index_with_key(index_path)?;
        Ok(key)
    }

    pub fn refresh_index(&self, repo: PathBuf, index_path: PathBuf) -> Result<RefreshStats> {
        let previous = if index_path.exists() {
            FastIndex::load_reusable(&index_path)?.map(Arc::new)
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

    pub fn register_shards(&self, index_dir: PathBuf) -> Result<usize> {
        let manifest = self.cached_shard_manifest(&index_dir)?;
        Ok(manifest.shards.len())
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

    pub fn max_cached_indexes(&self) -> usize {
        self.cache_policy
            .max_ready_indexes
            .unwrap_or(DEFAULT_MAX_CACHED_INDEXES)
    }

    pub fn daemon_status(&self) -> Value {
        self.daemon_status_for_arguments(&json!({ "details": true }))
    }

    pub fn daemon_status_for_arguments(&self, arguments: &Value) -> Value {
        let search_auto_default = self.search_auto_default_status();
        let client_cwd = optional_string_arg(arguments, "cwd");
        let include_details = bool_arg(arguments, "details");
        let cached_index_details = self.cached_index_details();
        let cached_shard_manifest_details = self.cached_shard_manifest_details();
        let footprint =
            daemon_footprint_summary(&cached_index_details, &cached_shard_manifest_details);
        let repair_requests = self.daemon_repair_requests();
        let default_requests = client_cwd.as_deref().map_or_else(
            || daemon_default_requests(&search_auto_default),
            daemon_default_cwd_requests,
        );
        let mut status = json!({
            "daemon_version": env!("CARGO_PKG_VERSION"),
            "process_id": std::process::id(),
            "started_at_unix_secs": system_time_unix_secs(self.started_at),
            "uptime_secs": daemon_uptime_secs(self.started_at),
            "max_shard_workers": configured_max_shard_workers(),
            "search_auto_default": search_auto_default.clone(),
            "default_requests": default_requests,
            "max_cached_indexes": self.max_cached_indexes(),
            "cached_indexes": self.cached_index_count(),
            "cached_shard_manifests": self.cached_shard_manifest_count(),
            "footprint": footprint,
            "details_omitted": !include_details
        });
        if include_details {
            status["cached_index_paths"] = serde_json::to_value(self.cached_index_paths())
                .unwrap_or_else(|_| Value::Array(Vec::new()));
            status["cached_index_details"] = Value::Array(cached_index_details);
            status["cached_shard_manifest_paths"] =
                serde_json::to_value(self.cached_shard_manifest_paths())
                    .unwrap_or_else(|_| Value::Array(Vec::new()));
            status["cached_shard_manifest_details"] = Value::Array(cached_shard_manifest_details);
        } else {
            status["full_status_hint"] = json!(
                "rerun daemon_status with details:true for cached paths and per-target details"
            );
        }
        if !repair_requests.is_empty() {
            status["repair_requests"] = Value::Array(repair_requests);
        }
        if let Some(cwd) = client_cwd {
            status["client_scope"] = json!({
                "cwd": cwd,
                "default_requests_scoped": true
            });
        }
        status
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

fn daemon_uptime_secs(started_at: SystemTime) -> u64 {
    started_at.elapsed().unwrap_or_default().as_secs()
}

fn system_time_unix_secs(value: SystemTime) -> u64 {
    value
        .duration_since(SystemTime::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
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
            &["repo", "index", "index_dir", "addr", "socket", "profile"],
        ),
        tool_entry(
            "agent_instructions",
            "Return compact copyable local-agent instructions for using Orient first.",
            &[],
            &["repo", "index", "index_dir", "addr", "socket", "profile"],
        ),
        tool_entry(
            "daemon_status",
            "Return local daemon runtime cache status for registered shard and warm-index clients.",
            &[],
            &["cwd", "details"],
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
            "register_shards",
            "Load only the shard manifest into the daemon cache so searches can lazily load matching shard indexes.",
            &["index_dir"],
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
            &["detail"],
        ),
        tool_entry(
            "repo_map",
            "Return entrypoints, tests, top symbols, known commands, and important files for a live repo, persistent index, or shard directory.",
            &[],
            REPO_MAP_TARGET_OPTIONAL_ARGS,
        ),
        tool_entry(
            "indexed_repo_map",
            "Return repo-map orientation from a persistent single-repo index.",
            &["index"],
            &["symbols", "tests", "detail", "read_limit"],
        ),
        tool_entry(
            "read_range",
            "Read one bounded line range from a live repo, persistent index, or shard directory.",
            &[],
            READ_TARGET_OPTIONAL_ARGS,
        ),
        tool_entry(
            "open_range",
            "Alias for read_range for agents that phrase context fetches as opening a file range.",
            &[],
            READ_TARGET_OPTIONAL_ARGS,
        ),
        tool_entry(
            "read_ranges",
            "Read several bounded line ranges from a live repo, persistent index, or shard directory in one request.",
            &["ranges"],
            READ_BATCH_TARGET_OPTIONAL_ARGS,
        ),
        tool_entry(
            "open_ranges",
            "Alias for read_ranges for agents that phrase context fetches as opening file ranges.",
            &["ranges"],
            READ_BATCH_TARGET_OPTIONAL_ARGS,
        ),
        tool_entry(
            "search_code",
            "Search a local repository with the fast fallback path and return ranked snippets.",
            &["repo", "query"],
            SEARCH_OPTIONAL_ARGS,
        ),
        tool_entry(
            "search",
            "Search a live repo, persistent index, or shard directory using the same plain result shape.",
            &["query"],
            SEARCH_TARGET_OPTIONAL_ARGS,
        ),
        tool_entry(
            "search_auto",
            "Search the best available local surface: explicit shard/index, single registered daemon shard directory, single warmed index, or a supplied live repo.",
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
            "Run several searches against one live repo, persistent index, or shard directory in a single request.",
            &["queries"],
            SEARCH_TARGET_OPTIONAL_ARGS,
        ),
        tool_entry(
            "search_query_plan",
            "Build a transient live-repo query plan with missing postings and repair hints.",
            &["repo", "query"],
            PLAN_OPTIONAL_ARGS,
        ),
        tool_entry(
            "search_plan",
            "Return a query plan for a live repo, persistent index, or shard directory using the same target flags as search.",
            &["query"],
            PLAN_TARGET_OPTIONAL_ARGS,
        ),
        tool_entry(
            "search_query_plan_batch",
            "Build transient live-repo query plans for several searches in one request; each repaired item promotes next_action.",
            &["repo", "queries"],
            PLAN_OPTIONAL_ARGS,
        ),
        tool_entry(
            "search_plan_batch",
            "Return query plans for several searches against one live repo, persistent index, or shard directory; each repaired item promotes next_action.",
            &["queries"],
            PLAN_TARGET_OPTIONAL_ARGS,
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
            "Return query plans for several searches against one persistent index; each repaired item promotes next_action.",
            &["index", "queries"],
            PLAN_INDEX_OPTIONAL_ARGS,
        ),
        tool_entry(
            "read_index_range",
            "Read a bounded line range from a persistent index result path.",
            &["index"],
            READ_WINDOW_OPTIONAL_ARGS,
        ),
        tool_entry(
            "open_index_range",
            "Alias for read_index_range for agents that phrase context fetches as opening a file range.",
            &["index"],
            READ_WINDOW_OPTIONAL_ARGS,
        ),
        tool_entry(
            "read_index_ranges",
            "Read several bounded line ranges from persistent index result paths in one request.",
            &["index", "ranges"],
            READ_BATCH_INDEX_OPTIONAL_ARGS,
        ),
        tool_entry(
            "open_index_ranges",
            "Alias for read_index_ranges for agents that phrase context fetches as opening file ranges.",
            &["index", "ranges"],
            READ_BATCH_INDEX_OPTIONAL_ARGS,
        ),
        tool_entry(
            "index_shards",
            "Build a local multi-repo shard directory from explicit repos or a discovered workspace root.",
            &["output_dir"],
            INDEX_SHARD_BUILD_OPTIONAL_ARGS,
        ),
        tool_entry(
            "ensure_shards",
            "Build or refresh a local multi-repo shard directory, then register its manifest in the daemon cache.",
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
            &["cwd", "repo_filter"],
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
            "Return shard query plans for several searches against one local multi-repo shard directory; each repaired item promotes next_action.",
            &["index_dir", "queries"],
            PLAN_INDEX_OPTIONAL_ARGS,
        ),
        tool_entry(
            "read_shard_range",
            "Read a bounded line range from a shard search result path or unique shard-relative path.",
            &["index_dir"],
            READ_WINDOW_OPTIONAL_ARGS,
        ),
        tool_entry(
            "open_shard_range",
            "Alias for read_shard_range for agents that phrase context fetches as opening a file range.",
            &["index_dir"],
            READ_WINDOW_OPTIONAL_ARGS,
        ),
        tool_entry(
            "read_shard_ranges",
            "Read several bounded line ranges from shard result paths or unique shard-relative paths in one request.",
            &["index_dir", "ranges"],
            READ_BATCH_INDEX_OPTIONAL_ARGS,
        ),
        tool_entry(
            "open_shard_ranges",
            "Alias for read_shard_ranges for agents that phrase context fetches as opening file ranges.",
            &["index_dir", "ranges"],
            READ_BATCH_INDEX_OPTIONAL_ARGS,
        ),
        tool_entry(
            "shard_repo_map",
            "Return repo-map orientation for every matching repo in a local shard directory.",
            &["index_dir"],
            &[
                "symbols",
                "tests",
                "detail",
                "read_limit",
                "repo",
                "repo_filter",
                "cwd",
                "branch",
                "origin",
                "refresh_if_stale",
            ],
        ),
        tool_entry(
            "find_shard_symbol",
            "Find symbol definitions across a local multi-repo shard directory.",
            &["index_dir", "name"],
            SYMBOL_INDEX_OPTIONAL_ARGS,
        ),
        tool_entry(
            "find_shard_symbol_batch",
            "Find several symbol definitions across a local multi-repo shard directory in one request. Each item with hits includes read_batch_request and next_action.",
            &["index_dir", "names"],
            SYMBOL_INDEX_OPTIONAL_ARGS,
        ),
        tool_entry(
            "find_symbol",
            "Find symbol definitions in a live repo, persistent index, or shard directory.",
            &["name"],
            SYMBOL_TARGET_OPTIONAL_ARGS,
        ),
        tool_entry(
            "find_symbol_batch",
            "Find several symbol definitions in a live repo, persistent index, or shard directory in one request. Each item with hits includes read_batch_request and next_action.",
            &["names"],
            SYMBOL_TARGET_OPTIONAL_ARGS,
        ),
        tool_entry(
            "find_index_symbol",
            "Find symbol definitions directly from a persistent index.",
            &["index", "name"],
            SYMBOL_INDEX_OPTIONAL_ARGS,
        ),
        tool_entry(
            "find_index_symbol_batch",
            "Find several symbol definitions directly from a persistent index in one request. Each item with hits includes read_batch_request and next_action.",
            &["index", "names"],
            SYMBOL_INDEX_OPTIONAL_ARGS,
        ),
        tool_entry(
            "related_files",
            "Find nearby source/test files related to a path in a live repo, persistent index, or shard directory.",
            &["path"],
            RELATED_FILES_TARGET_OPTIONAL_ARGS,
        ),
        tool_entry(
            "related_index_files",
            "Find nearby source/test files related to an indexed result path.",
            &["index", "path"],
            RELATED_INDEX_FILES_OPTIONAL_ARGS,
        ),
        tool_entry(
            "related_shard_files",
            "Find nearby source/test files related to a shard result path or unique shard-relative path.",
            &["index_dir", "path"],
            RELATED_SHARD_FILES_OPTIONAL_ARGS,
        ),
        tool_entry(
            "related_symbols",
            "Find symbols related to a path and optional search-language query in a live repo, persistent index, or shard directory.",
            &[],
            RELATED_SYMBOLS_TARGET_OPTIONAL_ARGS,
        ),
        tool_entry(
            "related_index_symbols",
            "Find symbols related to an indexed path and optional search-language query.",
            &["index"],
            RELATED_INDEX_SYMBOLS_OPTIONAL_ARGS,
        ),
        tool_entry(
            "related_shard_symbols",
            "Find symbols related to a shard result path or unique shard-relative path and optional search-language query.",
            &["index_dir", "path"],
            RELATED_SHARD_SYMBOLS_OPTIONAL_ARGS,
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
    socket: Option<&str>,
    profile: Option<&str>,
) -> Value {
    let repo = repo.unwrap_or("/path/to/repo");
    let index = index.unwrap_or("/path/to/local/cache/orient.index");
    let index_dir = index_dir.unwrap_or("/path/to/local/cache/orient-shards");
    let addr = addr.unwrap_or(DEFAULT_DAEMON_ADDR);
    let profile = agent_profile(profile);
    let client_command = agent_client_command(addr, socket);
    let status_command = agent_status_command(addr, socket);
    let multi_repo_serve = agent_serve_command(addr, socket, "index-dir", index_dir);
    let single_repo_serve = agent_serve_command(addr, socket, "index", index);
    let instructions_command = agent_instructions_command(addr, socket, profile.name, index_dir);
    json!({
        "name": "Orient Search",
        "purpose": "Fast local code search for coding agents; no telemetry.",
        "profile": profile.name,
        "instruction_target": profile.instruction_target,
        "instruction_snippet": agent_instructions(Some(repo), Some(index), Some(index_dir), Some(addr), socket, Some(profile.name)),
        "quickstart": {
            "install": "cargo install --git https://github.com/evalops/orient-search",
            "multi_repo": [
                format!("orient ensure-shards --discover-root /path/to/workspaces --output-dir {index_dir} --family-limit 2"),
                multi_repo_serve
            ],
            "single_repo": [
                format!("orient ensure-index --repo {repo} --index {index}"),
                single_repo_serve
            ],
            "client": client_command,
            "status": status_command,
            "one_shot_search": "orient search-auto --retry-if-empty \"symbol:SessionManager token\"",
            "agent_instructions": instructions_command,
            "followup_request_hints": "Generated follow-up requests include jsonl, client_cli, and compact cli hints where available."
        },
        "recommended_loop": [
            "Call tool_manifest or mcp_manifest once.",
            "Call daemon_status when using a shared daemon; trust search_auto_default for no-target search_auto routing, run repair_requests when present, and use default_requests for copyable first calls.",
            "Use repo_map, indexed_repo_map, or shard_repo_map before editing unfamiliar code.",
            "Search first, then use read_request, related_request, or related_symbols_request from results.",
            "Call a query-plan tool when results are empty, noisy, or overly broad."
        ],
        "adapter_notes": [
            profile.adapter_note,
            "Keep cache paths local to the machine running the agents; do not copy machine-specific layouts into shared docs or reusable instructions.",
            "Orient shares code-search artifacts only and has no telemetry.",
            "Prefer JSON-lines/MCP tool calls and returned follow-up requests over repeated shell scans."
        ],
        "preferred_surfaces": {
            "one_live_repo": "search_code",
            "one_persistent_repo": "indexed_search_code",
            "many_local_repos": "search_shards",
            "warmed_daemon_default": "search_auto"
        },
        "query_language": [
            "repo:service",
            "path:src/auth or dir:src/auth",
            "file:auth.rs or file:*.rs",
            "lang:rust",
            "ext:rs",
            "symbol:SessionManager",
            "kind:function or type:function",
            "dep:serde",
            "import:crate::auth",
            "test:false, is:test, or is:source",
            "generated:false or is:generated",
            "-path:docs",
            "\"quoted literal\"",
            "mode:any for exploratory searches"
        ],
        "ranking_notes": [
            "Generated paths, including hashed JavaScript bundles under assets/ or static/, are demoted by default but still searchable.",
            "Use generated:true or is:generated when intentionally inspecting generated output."
        ],
        "transports": {
            "stdio": "orient serve-jsonl",
            "tcp_daemon": format!("orient serve-tcp --addr {addr} --index-dir {index_dir}"),
            "tcp_client": tcp_client_command(addr),
            "unix_daemon": socket.map(|socket| agent_serve_command(addr, Some(socket), "index-dir", index_dir)),
            "unix_client": socket.map(|socket| agent_client_command(addr, Some(socket)))
        },
        "setup_commands": {
            "single_repo": [
                format!("orient ensure-index --repo {repo} --index {index}"),
                agent_serve_command(addr, socket, "index", index)
            ],
            "multi_repo_shards": [
                format!("orient ensure-shards --discover-root /path/to/workspaces --output-dir {index_dir} --family-limit 2"),
                agent_serve_command(addr, socket, "index-dir", index_dir)
            ]
        },
        "request_templates": {
            "manifest": {"id": "tools", "tool": "tool_manifest", "arguments": {}},
            "daemon_status": {"id": "status", "tool": "daemon_status", "arguments": {}},
            "live_repo_map": {
                "id": "map",
                "tool": "repo_map",
                "arguments": {"repo": repo, "symbols": 50, "tests": 50, "detail": "compact", "read_limit": DEFAULT_REPO_MAP_READ_BATCH_RANGES}
            },
            "live_search": {
                "id": "search",
                "tool": "search_code",
                "arguments": {"repo": repo, "query": "symbol:SessionManager token", "limit": 10, "explain": true}
            },
            "auto_search": {
                "id": "search",
                "tool": "search_auto",
                "arguments": {"query": "symbol:SessionManager token", "limit": 10, "explain": true, "retry_if_empty": true, "summary": true}
            },
            "auto_search_batch": {
                "id": "searches",
                "tool": "search_auto_batch",
                "arguments": {"queries": ["symbol:SessionManager token", "path:src token"], "limit": 10, "explain": true, "retry_if_empty": true, "summary": true}
            },
            "indexed_repo_map": {
                "id": "map",
                "tool": "indexed_repo_map",
                "arguments": {"index": index, "symbols": 50, "tests": 50, "detail": "compact", "read_limit": DEFAULT_REPO_MAP_READ_BATCH_RANGES}
            },
            "indexed_search": {
                "id": "search",
                "tool": "indexed_search_code",
                "arguments": {"index": index, "query": "path:src symbol:SessionManager token", "limit": 10, "refresh_if_stale": true}
            },
            "shard_repo_map": {
                "id": "map",
                "tool": "shard_repo_map",
                "arguments": {"index_dir": index_dir, "symbols": 50, "tests": 50, "detail": "compact", "read_limit": DEFAULT_REPO_MAP_READ_BATCH_RANGES}
            },
            "shard_search": {
                "id": "search",
                "tool": "search_shards",
                "arguments": {"index_dir": index_dir, "query": "repo:service symbol:SessionManager token", "limit": 10, "explain": true, "refresh_if_stale": true}
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
                "arguments": {"index_dir": index_dir, "query": "repo:service symbol:SessionManager token"}
            }
        },
        "result_followups": [
            "Use compact query_plan_summary fields first; pass summary:true on search_auto/search_auto_batch to omit full query_plan_result details until diagnostics are needed.",
            "Use primary_retry_request, query_plan_summary, or query_plan_request first when an automatic search is empty; request full query_plan_result only when compact diagnostics are not enough.",
            "Set retry_if_empty:true on search_auto or search_auto_batch to run primary_retry_request once and receive primary_retry_result in the same call.",
            "Use search_auto.query_plan_request, a search_auto_batch item query_plan_request, or a search batch item query_plan_request when results are empty or noisy.",
            "Use search_auto.repo_map_request, a search_auto_batch item repo_map_request, or a search batch item repo_map_request when the agent needs entrypoints, tests, commands, or top symbols for the chosen surface.",
            "Use search_auto.next_read_batch_request or a search_auto_batch item next_read_batch_request as the preferred immediate read follow-up after automatic retries.",
            "Use search_auto.next_action or a search batch item next_action when the wrapper wants one prioritized follow-up request; empty search batch items point at query_plan_request.",
            "Use search_auto.read_batch_request, a search_auto_batch item read_batch_request, or a search batch item next_action/read_batch_request to read top ranges in one call.",
            "Use symbol batch item next_action/read_batch_request to read candidate definitions for one requested symbol name.",
            "Use read_batch_request.read_budget to keep batch reads under hard_limits.max_batch_read_lines; split large inspections instead of widening one call.",
            "Use result.read_request for one bounded file range.",
            "Batch several result.read_range objects with read_ranges, read_index_ranges, or read_shard_ranges.",
            "Use scope:symbol on manual read_range/read_ranges calls when opening from a line inside a function, class, or type definition; check summary.truncated before assuming the full definition was returned.",
            "Use result.related_request for source/test siblings.",
            "Use result.related_symbols_request for nearby definitions and types; search-generated requests include the original query."
        ],
        "hard_limits": {
            "max_results": MAX_SEARCH_RESULTS,
            "max_batch_queries": MAX_BATCH_QUERIES,
            "max_batch_ranges": MAX_BATCH_RANGES,
            "max_batch_read_lines": MAX_BATCH_READ_LINES,
            "max_range_lines": MAX_READ_RANGE_LINES,
            "max_attached_context_lines": MAX_ATTACHED_CONTEXT_LINES
        }
    })
}

pub fn agent_instructions(
    repo: Option<&str>,
    index: Option<&str>,
    index_dir: Option<&str>,
    addr: Option<&str>,
    socket: Option<&str>,
    profile: Option<&str>,
) -> String {
    let repo = repo.unwrap_or("/path/to/repo");
    let index = index.unwrap_or("/path/to/local/cache/orient.index");
    let index_dir = index_dir.unwrap_or("/path/to/local/cache/orient-shards");
    let addr = addr.unwrap_or(DEFAULT_DAEMON_ADDR);
    let profile = agent_profile(profile);
    let client_command = agent_client_command(addr, socket);
    let multi_repo_serve = agent_serve_command(addr, socket, "index-dir", index_dir);
    let single_repo_serve = agent_serve_command(addr, socket, "index", index);
    format!(
        "## Orient Search\n\
Use Orient for local code discovery and bounded file reads before `rg`, `find`, `ls`, `grep`, `cat`, or ad hoc filesystem scans.\n\
For terminal-native work, start with `orient search-auto --retry-if-empty \"<query>\"` and then run the returned `read_*`, `related_*`, or `query_plan_*` request before falling back to shell search.\n\
Prefer the shared daemon when it is running: `{client_command}`.\n\
Copy this snippet into {instruction_target}.\n\
Keep cache paths local to the machine running the agents; do not copy machine-specific layouts into shared docs or reusable instructions.\n\
Orient shares code-search artifacts only and has no telemetry.\n\
For many local repos, bootstrap it with `orient ensure-shards --discover-root /path/to/workspaces --output-dir {index_dir} --family-limit 2` and `{multi_repo_serve}`.\n\
For one repo, bootstrap it with `orient ensure-index --repo {repo} --index {index}` and `{single_repo_serve}`.\n\
At the start of a task, call `daemon_status` or `agent_guide`, then use `search_auto` with `retry_if_empty:true` and `summary:true` for normal lookup and `search_auto_batch` with `retry_if_empty:true` and `summary:true` for alternate query phrasings.\n\
Trust `daemon_status.search_auto_default` to see whether no-target `search_auto` will use a registered shard directory, warmed index, or the daemon current directory; run any `daemon_status.repair_requests`, then use `daemon_status.default_requests` for copyable first repo-map/search/query-plan calls.\n\
When calling `search`, `search_batch`, `search_auto`, `search_auto_batch`, `repo_map`, `search_plan`, `find_symbol`, `read_range`, `read_ranges`, `related_files`, or `related_symbols` through JSON-lines/MCP without an explicit target, pass `cwd` so shared shard daemons scope results to the current git checkout.\n\
Use query filters directly: `file:`, `path:`, `lang:`, `ext:`, `symbol:`, `type:`, `repo:`, `test:`, `generated:`, `code:`, `is:code`, `is:docs`, quoted literals, bare negative content terms like `-deprecated`, and negative filters like `-path:vendor` or `-is:generated`.\n\
Use `line:42` or `target_line:42` with `file:` or `path:` when the agent knows the relevant line and wants anchored snippets/read ranges.\n\
Generated paths, including hashed JavaScript bundles, are demoted by default; use `generated:true` or `is:generated` when intentionally inspecting generated output.\n\
After search, follow returned `next_action`, `next_read_batch_request`, `read_batch_request`, `read_request`, `related_request`, and `related_symbols_request`; each includes `jsonl` and `client_cli` for direct replay through `orient client-jsonl` when it wraps a tool request.\n\
If Orient returns a usable request, run that request instead of translating it into a shell search/read command.\n\
Use `read_batch_request.read_budget` to keep batch reads under the advertised hard limits; split large inspections into smaller calls instead of widening one huge request.\n\
For manual context reads from a line inside a definition, pass `scope:\"symbol\"` so `read_range` or `read_ranges` anchors at the nearest function, class, or type definition.\n\
Manual `read_range` and `read_ranges` calls accept pasted locations like `src/lib.rs:40-45` or `src/lib.rs#L40-L45`; use returned read requests when available.\n\
When results are empty, noisy, or suspicious, read `query_plan_summary` first, then use the returned `query_plan_request` or inline `query_plan_result` before broadening the search; pass `retry_if_empty:true` when you want Orient to execute the promoted retry once and return `primary_retry_result` immediately.\n\
Fall back to shell search only when Orient is unavailable or its query plan is not useful for the task.",
        instruction_target = profile.instruction_target
    )
}

#[derive(Debug, Clone, Copy)]
struct AgentProfile {
    name: &'static str,
    instruction_target: &'static str,
    adapter_note: &'static str,
}

fn agent_profile(profile: Option<&str>) -> AgentProfile {
    match profile
        .unwrap_or("generic")
        .trim()
        .to_ascii_lowercase()
        .replace(['_', '-'], "")
        .as_str()
    {
        "codex" => AgentProfile {
            name: "codex",
            instruction_target: "the local instruction file read by the selected coding agent",
            adapter_note: "Selected adapter profile; place the snippet in that agent's local instruction file for this repo.",
        },
        "claude" | "claudecode" => AgentProfile {
            name: "claude",
            instruction_target: "the local instruction file read by the selected coding agent",
            adapter_note: "Selected adapter profile; place the snippet in that agent's local instruction file for this repo.",
        },
        "amp" => AgentProfile {
            name: "amp",
            instruction_target: "the local instruction file read by the selected coding agent",
            adapter_note: "Selected adapter profile; place the snippet in that agent's local instruction file for this repo.",
        },
        _ => AgentProfile {
            name: "generic",
            instruction_target: "the local agent instruction file for this repo",
            adapter_note: "Selected profile: generic; place the snippet in the local instruction file your coding agent reads.",
        },
    }
}

fn non_empty_agent_socket(socket: Option<&str>) -> Option<&str> {
    socket.map(str::trim).filter(|socket| !socket.is_empty())
}

fn agent_client_command(addr: &str, socket: Option<&str>) -> String {
    if let Some(socket) = non_empty_agent_socket(socket) {
        unix_client_command(Path::new(socket))
    } else {
        tcp_client_command(addr)
    }
}

fn agent_status_command(addr: &str, socket: Option<&str>) -> String {
    if let Some(socket) = non_empty_agent_socket(socket) {
        format!(
            "orient daemon-status --socket {} --format json",
            shell_quote(socket)
        )
    } else {
        daemon_status_command(addr)
    }
}

fn agent_serve_command(
    addr: &str,
    socket: Option<&str>,
    target_flag: &str,
    target: &str,
) -> String {
    if let Some(socket) = non_empty_agent_socket(socket) {
        format!(
            "orient serve-unix --socket {} --{} {}",
            shell_quote(socket),
            target_flag,
            shell_quote(target)
        )
    } else {
        format!(
            "orient serve-tcp --addr {} --{} {}",
            shell_quote(addr),
            target_flag,
            shell_quote(target)
        )
    }
}

fn agent_instructions_command(
    addr: &str,
    socket: Option<&str>,
    profile: &str,
    index_dir: &str,
) -> String {
    let base = format!(
        "orient agent-instructions --profile {} --index-dir {}",
        shell_quote(profile),
        shell_quote(index_dir)
    );
    if let Some(socket) = non_empty_agent_socket(socket) {
        format!("{base} --socket {}", shell_quote(socket))
    } else if addr != DEFAULT_DAEMON_ADDR {
        format!("{base} --addr {}", shell_quote(addr))
    } else {
        base
    }
}

pub fn retarget_client_cli_commands(value: &mut Value, client_command: &str) {
    match value {
        Value::Object(object) => {
            let jsonl = object
                .get("jsonl")
                .and_then(Value::as_str)
                .map(str::to_string);
            if let Some(jsonl) = jsonl {
                object.insert(
                    "client_cli".to_string(),
                    Value::String(jsonl_client_cli(&jsonl, client_command)),
                );
            }
            for value in object.values_mut() {
                retarget_client_cli_commands(value, client_command);
            }
        }
        Value::Array(values) => {
            for value in values {
                retarget_client_cli_commands(value, client_command);
            }
        }
        Value::Null | Value::Bool(_) | Value::Number(_) | Value::String(_) => {}
    }
}

fn jsonl_client_cli(jsonl: &str, client_command: &str) -> String {
    format!("printf '%s\\n' {} | {client_command}", shell_quote(jsonl))
}

pub fn tcp_client_command(addr: &str) -> String {
    if addr == DEFAULT_DAEMON_ADDR {
        "orient client-jsonl --require-version".to_string()
    } else {
        format!(
            "orient client-jsonl --require-version --addr {}",
            shell_quote(addr)
        )
    }
}

pub fn unix_client_command(socket: &Path) -> String {
    format!(
        "orient client-jsonl --require-version --socket {}",
        shell_quote(&socket.to_string_lossy())
    )
}

fn daemon_status_command(addr: &str) -> String {
    if addr == DEFAULT_DAEMON_ADDR {
        "orient daemon-status --format json".to_string()
    } else {
        format!(
            "orient daemon-status --addr {} --format json",
            shell_quote(addr)
        )
    }
}

fn shell_quote(value: &str) -> String {
    if !value.is_empty()
        && value
            .bytes()
            .all(|byte| matches!(byte, b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'_' | b'-' | b'.' | b'/' | b':' | b'@' | b'%' | b'+' | b'=' | b','))
    {
        return value.to_string();
    }
    format!("'{}'", value.replace('\'', "'\\''"))
}

fn mcp_tool_annotations(name: &str) -> Value {
    let mutating = matches!(
        name,
        "warm_index"
            | "register_shards"
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
    if let Some(max_total_lines) = argument_max_total_lines(name) {
        entry.insert("max_total_lines".to_string(), json!(max_total_lines));
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
        for (alias, canonical) in argument_schema_aliases(name) {
            properties
                .entry(alias.clone())
                .or_insert_with(|| argument_alias_schema(tool_name, &alias, canonical));
        }
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
        name if string_list_argument(name) => {
            schema.insert(
                "oneOf".to_string(),
                json!([
                    {"type": "string"},
                    {"type": "array", "items": {"type": "string"}}
                ]),
            );
        }
        "range" | "ranges" => {
            let path_description = range_path_description(tool_name);
            let range_schema = json!({
                "type": "object",
                "required": ["path"],
                "properties": {
                    "path": {"type": "string", "description": path_description},
                    "start": {"type": "integer", "minimum": 1, "default": 1},
                    "start_line": {"type": "integer", "minimum": 1, "description": "Alias for start."},
                    "start-line": {"type": "integer", "minimum": 1, "description": "Alias for start."},
                    "line": {"type": "integer", "minimum": 1, "description": "Alias for start."},
                    "target_line": {"type": "integer", "minimum": 1, "description": "Alias for start."},
                    "target-line": {"type": "integer", "minimum": 1, "description": "Alias for start."},
                    "lines": {"type": "integer", "minimum": 1, "maximum": MAX_READ_RANGE_LINES, "default": 80},
                    "line_count": {"type": "integer", "minimum": 1, "maximum": MAX_READ_RANGE_LINES, "description": "Alias for lines."},
                    "line-count": {"type": "integer", "minimum": 1, "maximum": MAX_READ_RANGE_LINES, "description": "Alias for lines."},
                    "end_line": {"type": "integer", "minimum": 1, "description": "Inclusive end line; use instead of lines or line_count."},
                    "end-line": {"type": "integer", "minimum": 1, "description": "Alias for end_line."},
                    "end": {"type": "integer", "minimum": 1, "description": "Alias for end_line."},
                    "scope": {
                        "type": "string",
                        "enum": ["exact", "symbol"],
                        "default": "exact",
                        "description": "Use symbol to anchor this range around the nearest preceding symbol definition."
                    }
                }
            });
            let range_string_schema = json!({
                "type": "string",
                "description": "Compact PATH:START:LINES[:SCOPE] range or copied location such as path:line, path:start-end, path:line: text, path:start-end: text, path#Lstart-Lend, a Python traceback frame, a JavaScript stack frame, or a Go panic stack location."
            });
            if name == "range" {
                schema.insert(
                    "oneOf".to_string(),
                    json!([range_schema, range_string_schema]),
                );
            } else {
                schema.insert(
                    "oneOf".to_string(),
                    json!([
                        range_schema.clone(),
                        range_string_schema.clone(),
                        {
                            "type": "array",
                            "minItems": 1,
                            "maxItems": MAX_BATCH_RANGES,
                            "items": {
                                "oneOf": [range_schema, range_string_schema]
                            }
                        }
                    ]),
                );
                schema.insert("max_total_lines".to_string(), json!(MAX_BATCH_READ_LINES));
            }
            schema.insert(
                "description".to_string(),
                json!(argument_description(tool_name, name)),
            );
        }
        "queries" | "names" => {
            schema.insert("type".to_string(), json!("array"));
            schema.insert("minItems".to_string(), json!(1));
            schema.insert("maxItems".to_string(), json!(MAX_BATCH_QUERIES));
            schema.insert("items".to_string(), json!({"type": "string"}));
        }
        "repos" | "discover_roots" => {
            schema.insert("type".to_string(), json!("array"));
            schema.insert("items".to_string(), json!({"type": "string"}));
        }
        "test" | "generated" | "code" | "explain" | "require_all" | "any_terms" | "details"
        | "refresh_if_stale" | "diagnose" | "retry_if_empty" | "include_read_batch"
        | "include_summary" | "summary" | "git_metadata" | "tracked_files" | "nested_manifests"
        | "force" => {
            schema.insert("type".to_string(), json!("boolean"));
        }
        "limit" | "max_depth" | "discover_limit" | "family_limit" | "symbols" | "start"
        | "start_line" | "end_line" | "end" | "lines" | "line_count" | "tests"
        | "context_lines" | "read_limit" | "line" | "target_line" => {
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

fn argument_alias_schema(tool_name: &str, alias: &str, canonical: &str) -> Value {
    let mut schema = match argument_schema(tool_name, canonical) {
        Value::Object(schema) => schema,
        _ => Map::new(),
    };
    schema.insert(
        "description".to_string(),
        json!(format!("Alias for {canonical}.")),
    );
    if let Some(default) = argument_default(tool_name, alias) {
        schema.insert("default".to_string(), default);
    }
    if let Some(values) = argument_enum(alias) {
        schema.insert("enum".to_string(), json!(values));
    }
    Value::Object(schema)
}

fn argument_schema_aliases<'a>(name: &'a str) -> Vec<(String, &'a str)> {
    let mut aliases = Vec::new();
    if let Some(alias) = kebab_case_alias(name) {
        aliases.push((alias, name));
    }
    let extra: &[&str] = match name {
        "path" => &["dir", "directory", "folder"],
        "language" => &["lang"],
        "extension" => &["ext"],
        "file" => &["filename", "file-name", "file_name"],
        "symbol_kind" => &["kind", "type", "symbol-kind", "symbol_type", "symbol-type"],
        "dependency" => &["dep", "deps"],
        "import" => &["imports", "module", "modules", "use", "uses"],
        "branch" => &["git_branch", "git-branch"],
        "origin" => &["remote", "remote_origin", "remote-origin"],
        "repo_filter" => &["repo-filter"],
        "target_line" => &["target-line"],
        "end_line" => &["end"],
        "exclude_file" => &["exclude-filename", "exclude_file_name", "exclude-file-name"],
        "exclude_path" => &[
            "exclude_dir",
            "exclude-dir",
            "exclude_directory",
            "exclude-directory",
            "exclude_folder",
            "exclude-folder",
        ],
        "exclude_language" => &["exclude-lang"],
        "exclude_extension" => &["exclude-ext"],
        "exclude_symbol_kind" => &["exclude-kind", "exclude-type", "exclude-symbol-kind"],
        "exclude_origin" => &[
            "exclude_remote",
            "exclude-remote",
            "exclude_remote_origin",
            "exclude-remote-origin",
        ],
        "exclude_dependency" => &["exclude-dep", "exclude-deps"],
        "exclude_import" => &[
            "exclude-imports",
            "exclude-module",
            "exclude_modules",
            "exclude-modules",
            "exclude-use",
            "exclude_uses",
            "exclude-uses",
        ],
        _ => &[],
    };
    aliases.extend(extra.iter().map(|alias| ((*alias).to_string(), name)));
    aliases
}

fn kebab_case_alias(name: &str) -> Option<String> {
    name.contains('_').then(|| name.replace('_', "-"))
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
            "source": "single_registered_shard_dir",
            "when": "argument omitted and exactly one shard directory is registered in the daemon"
        })),
    }
}

fn argument_daemon_default(tool_name: &str, name: &str) -> Option<Value> {
    match (daemon_default_kind(tool_name)?, name) {
        (DaemonDefaultKind::Index, "index") => Some(json!("single_warmed_index")),
        (DaemonDefaultKind::ShardDir, "index_dir") => Some(json!("single_registered_shard_dir")),
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
        | "start_line" | "end_line" | "end" | "lines" | "line_count" | "tests"
        | "context_lines" | "read_limit" | "line" | "target_line" => "integer",
        "test" | "generated" | "code" | "explain" | "require_all" | "any_terms" | "details"
        | "refresh_if_stale" | "include_read_batch" | "include_summary" | "git_metadata"
        | "tracked_files" | "nested_manifests" | "summary" => "boolean",
        name if string_list_argument(name) => "string|string[]",
        "range" => "range|string",
        "ranges" => "range|string|range[]",
        "repos" | "discover_roots" | "queries" => "string[]",
        _ => "string",
    }
}

fn string_list_argument(name: &str) -> bool {
    matches!(
        name,
        "exclude_file"
            | "exclude_path"
            | "exclude_language"
            | "exclude_lang"
            | "exclude_extension"
            | "exclude_ext"
            | "exclude_symbol"
            | "exclude_symbol_kind"
            | "exclude_kind"
            | "exclude_type"
            | "exclude_repo"
            | "exclude_branch"
            | "exclude_git_branch"
            | "exclude_origin"
            | "exclude_remote"
            | "exclude_remote_origin"
            | "exclude_dependency"
            | "exclude_dep"
            | "exclude_deps"
            | "exclude_import"
            | "exclude_imports"
            | "exclude_module"
            | "exclude_modules"
            | "exclude_use"
            | "exclude_uses"
            | "exclude_content"
            | "exclude_text"
            | "exclude_term"
    )
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
        (_, "read_limit") => Some(json!(DEFAULT_REPO_MAP_READ_BATCH_RANGES)),
        (_, "start" | "start_line") => Some(json!(1)),
        (_, "lines" | "line_count") => Some(json!(80)),
        (_, "scope") => Some(json!("exact")),
        (_, "snippet" | "snippet_mode" | "snippet-mode") => Some(json!("medium")),
        (_, "detail") => Some(json!("compact")),
        ("agent_guide" | "agent_instructions", "profile") => Some(json!("generic")),
        (_, "context_lines") => Some(json!(0)),
        ("agent_guide" | "agent_instructions", "addr") => Some(json!("127.0.0.1:8796")),
        (
            _,
            "explain" | "require_all" | "any_terms" | "details" | "refresh_if_stale" | "diagnose"
            | "retry_if_empty" | "include_read_batch" | "include_summary" | "git_metadata"
            | "tracked_files" | "nested_manifests" | "force",
        ) => Some(json!(false)),
        _ => None,
    }
}

fn argument_enum(name: &str) -> Option<&'static [&'static str]> {
    match name {
        "snippet" | "snippet_mode" | "snippet-mode" => {
            Some(&["short", "medium", "block", "symbol"])
        }
        "scope" => Some(&["exact", "symbol"]),
        "detail" => Some(&["compact", "full"]),
        "profile" => Some(&["generic", "codex", "claude", "amp"]),
        _ => None,
    }
}

fn argument_maximum(tool_name: &str, name: &str) -> Option<usize> {
    match name {
        "lines" | "line_count" => Some(MAX_READ_RANGE_LINES),
        "context_lines" => Some(MAX_ATTACHED_CONTEXT_LINES),
        "read_limit" => Some(MAX_RESULT_READ_BATCH_RANGES),
        "limit" if tool_has_result_limit(tool_name) => Some(MAX_SEARCH_RESULTS),
        _ => None,
    }
}

fn argument_max_items(name: &str) -> Option<usize> {
    match name {
        "queries" | "names" => Some(MAX_BATCH_QUERIES),
        "ranges" => Some(MAX_BATCH_RANGES),
        _ => None,
    }
}

fn argument_max_total_lines(name: &str) -> Option<usize> {
    match name {
        "ranges" => Some(MAX_BATCH_READ_LINES),
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
            | "find_symbol"
            | "find_symbol_batch"
            | "find_index_symbol"
            | "find_index_symbol_batch"
            | "find_shard_symbol"
            | "find_shard_symbol_batch"
    )
}

fn argument_description(tool_name: &str, name: &str) -> &'static str {
    match name {
        "repo" => "Local repository root or shard repo filter, depending on the tool.",
        "repo_filter" => "Repository name filter when repo is already used as a root path.",
        "cwd" => {
            "Client working directory used by no-target daemon tools to scope registered shard searches and context reads to the current git checkout."
        }
        "branch" | "git_branch" => "Git branch substring filter for shard-aware agent searches.",
        "origin" | "remote" | "remote_origin" => {
            "Git remote origin substring filter for shard-aware agent searches."
        }
        "index" => {
            "Path to a persistent single-repo Orient index. Daemon tools may omit this when exactly one index is warmed."
        }
        "index_dir" => {
            "Path to a local multi-repo shard directory. Daemon tools may omit this when exactly one shard directory is registered."
        }
        "addr" => "Local TCP daemon address for generated setup and client commands.",
        "profile" => "Instruction profile for generated guidance: generic, codex, claude, or amp.",
        "output_dir" => "Directory where shard indexes and manifest.json should be written.",
        "query" => "Agent query string with filters, quoted phrases, and normal search terms.",
        "queries" => "Agent query strings to run as one batch against the same search target.",
        "name" => "Symbol name to look up.",
        "names" => "Symbol names to look up as one batch against the same target and filters.",
        "path" if is_target_context_tool(tool_name) => {
            "Result path for the selected target; use repo/index-relative paths for repo or index targets, and shard-prefixed or unique shard-relative paths for index_dir targets."
        }
        "path" if is_shard_path_tool(tool_name) => {
            "Shard-prefixed result path, unique unqualified shard-relative path, or copied location such as repo/src/lib.rs#L40-L45."
        }
        "path" if is_index_path_tool(tool_name) => {
            "Index-relative result path or copied location, such as src/lib.rs or src/lib.rs#L40-L45."
        }
        "path" if is_live_path_tool(tool_name) => {
            "Repository-relative result path, in-repo absolute path, or copied location, such as src/lib.rs, /repo/src/lib.rs, or src/lib.rs#L40-L45."
        }
        "path" => "Path substring filter or result path, depending on the tool.",
        "range" => {
            "Single range object or copied location for read_range/open_range; accepts the same shape as a search result read_range, plus strings like path:start-end, Python traceback frames, JavaScript stack frames, and Go panic stack locations."
        }
        "dir" | "directory" | "folder" => {
            "Alias for path when filtering search results to a directory or path substring."
        }
        "filename" | "file_name" => "Alias for file when filtering by basename.",
        "ranges" if is_shard_range_tool(tool_name) => {
            "A compact range string, copied path:start-end string, {path,start,lines} object, or array of them; path may be shard-prefixed or a unique unqualified shard-relative path."
        }
        "ranges" if is_index_range_tool(tool_name) => {
            "A compact range string, copied path:start-end string, {path,start,lines} object, or array of them for index-relative batch range reads."
        }
        "ranges" => {
            "A compact range string, copied path:start-end string, {path,start,lines} object, or array of them for repository-relative batch range reads."
        }
        "limit" => "Maximum number of results to return.",
        "language" => "Detected language filter, such as rust, python, or typescript.",
        "lang" => "Alias for language.",
        "extension" => "File extension filter with or without a leading dot.",
        "ext" => "Alias for extension.",
        "symbol" => "Symbol name to require or boost.",
        "symbol_kind" => {
            "Symbol kind to require, such as function, class, struct, enum, or method."
        }
        "kind" => "Alias for symbol_kind.",
        "type" => "Alias for symbol_kind using type-style names such as class, enum, or interface.",
        "dependency" => "Dependency name substring used as a repo-level search filter.",
        "dep" | "deps" => "Alias for dependency.",
        "import" => "Imported module substring used as a file-level search filter.",
        "module" | "modules" | "imports" | "use" | "uses" => "Alias for import.",
        "file" => "File basename substring filter.",
        "line" | "target_line" => {
            "Line number used to anchor snippets and read ranges for file/path-filter searches."
        }
        "test" => "When true, include only test paths; when false, exclude test paths.",
        "generated" => {
            "When true, include only generated-code paths; when false, exclude generated-code paths. Without this filter, generated paths are searchable but demoted in ranking."
        }
        "code" => {
            "When true, include only implementation source-code paths; when false, exclude implementation source-code paths."
        }
        "snippet" => "Snippet mode: short, medium, block, or symbol.",
        "snippet_mode" | "snippet-mode" => "Alias for snippet.",
        "detail" => {
            "Repo-map detail level: compact keeps first-orientation payloads small; full includes all available import hints."
        }
        "details" => {
            "When true for daemon_status, include cached paths and per-target runtime details. The default compact status omits them."
        }
        "explain" => "Include structured rank signals and indexed query plans.",
        "require_all" => "Require all normalized query tokens to appear in each result.",
        "any_terms" => {
            "Match any normalized query token for exploratory orientation; query text can also use mode:any."
        }
        "context_lines" => "Attach this many bounded line-numbered context lines per result.",
        "refresh_if_stale" => {
            "When true, refresh a stale persistent index or shard directory before searching."
        }
        "diagnose" => {
            "When true, include query_plan_result even when search_auto or search_auto_batch returns hits, saving a follow-up query-plan call for suspicious searches."
        }
        "retry_if_empty" => {
            "When true, search_auto runs the primary_retry_request once after an empty result and returns primary_retry_result."
        }
        "include_read_batch" => {
            "When true, related tools return {results, read_batch_request, next_action} instead of a bare result array."
        }
        "include_summary" => {
            "When true, batch read tools return {summary, ranges} instead of a bare range array."
        }
        "summary" => {
            "When true for query-plan and search_auto tools, return compact query-plan summaries, retry requests, and next_action instead of full nested plan payloads."
        }
        "force" => {
            "When true for index_shards, replace an existing shard directory even if the rebuild would remove existing shards."
        }
        "exclude_file" => "File basename substring or list of substrings to exclude.",
        "exclude_path" => "Path substring or list of substrings to exclude.",
        "exclude_language" => "Language or list of languages to exclude.",
        "exclude_lang" => "Alias for exclude_language.",
        "exclude_extension" => "Extension or list of extensions to exclude.",
        "exclude_ext" => "Alias for exclude_extension.",
        "exclude_symbol" => "Symbol name or list of symbols to exclude.",
        "exclude_symbol_kind" => "Symbol kind or list of kinds to exclude.",
        "exclude_kind" => "Alias for exclude_symbol_kind.",
        "exclude_type" => "Alias for exclude_symbol_kind using type-style names.",
        "exclude_repo" => "Repository name substring or list of substrings to exclude.",
        "exclude_branch" | "exclude_git_branch" => {
            "Git branch substring or list of substrings to exclude."
        }
        "exclude_origin" | "exclude_remote" | "exclude_remote_origin" => {
            "Git remote origin substring or list of substrings to exclude."
        }
        "exclude_dependency" => "Dependency name or list of dependency substrings to exclude.",
        "exclude_dep" | "exclude_deps" => "Alias for exclude_dependency.",
        "exclude_import" => "Imported module or list of module substrings to exclude.",
        "exclude_module" | "exclude_modules" | "exclude_imports" | "exclude_use"
        | "exclude_uses" => "Alias for exclude_import.",
        "exclude_content" => {
            "Content substring, quoted phrase, or list of content terms to exclude."
        }
        "exclude_text" | "exclude_term" => "Alias for exclude_content.",
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
        "read_limit" => {
            "Maximum ranges to include in a repo-map read_batch_request; raise it when the agent intentionally wants more files opened at once."
        }
        "start" => "One-based start line for range reads.",
        "start_line" => "Alias for start when passing line_range-shaped data.",
        "end_line" => "Inclusive end line for range reads; use instead of lines or line_count.",
        "end" => "Alias for end_line.",
        "lines" => "Number of lines to read, capped to the maximum bounded range size.",
        "line_count" => "Alias for lines when passing line_range-shaped data.",
        "scope" => {
            "Range read scope: exact reads the requested line window; symbol starts from the nearest preceding symbol definition and includes enough leading context to keep that symbol visible."
        }
        _ => "Tool argument.",
    }
}

fn range_path_description(tool_name: &str) -> &'static str {
    if is_target_context_tool(tool_name) {
        "Result path or copied location for the selected target; use repo/index-relative paths for repo or index targets, and shard-prefixed or unique shard-relative paths for index_dir targets."
    } else if is_shard_range_tool(tool_name) {
        "Shard-prefixed result path, unique unqualified shard-relative path, or copied location such as repo/src/lib.rs#L40-L45."
    } else if is_index_range_tool(tool_name) {
        "Index-relative result path or copied location, such as src/lib.rs or src/lib.rs#L40-L45."
    } else {
        "Repository-relative result path, in-repo absolute path, or copied location, such as src/lib.rs, /repo/src/lib.rs, or src/lib.rs#L40-L45."
    }
}

fn is_target_context_tool(tool_name: &str) -> bool {
    matches!(
        tool_name,
        "read_range"
            | "open_range"
            | "read_ranges"
            | "open_ranges"
            | "related_files"
            | "related_symbols"
    )
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
    ResultToolRequest::new(tool.to_string(), Value::Object(arguments))
}

fn auto_query_plan_passthrough_arg(name: &str, target_name: &str) -> bool {
    if matches!(
        name,
        "query"
            | "queries"
            | "cwd"
            | "limit"
            | "context_lines"
            | "snippet"
            | "snippet_mode"
            | "snippet-mode"
            | "explain"
            | "diagnose"
            | "retry_if_empty"
            | "summary"
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
    shard_scope_filters: Option<&SearchFilters>,
) -> ResultToolRequest {
    let mut arguments = Map::new();
    arguments.insert(target_name.to_string(), json!(target_value));
    arguments.insert("detail".to_string(), json!("compact"));
    arguments.insert(
        "read_limit".to_string(),
        json!(DEFAULT_REPO_MAP_READ_BATCH_RANGES),
    );
    if target_name == "index_dir" {
        if let Some(filters) = shard_scope_filters {
            add_shard_scope_filter_args(&mut arguments, filters);
        } else if let Some(source) = source_arguments.as_object() {
            if let Some(repo) = source.get("repo").or_else(|| source.get("repo_filter")) {
                arguments.insert("repo".to_string(), repo.clone());
            }
        }
    }
    ResultToolRequest::new(tool.to_string(), Value::Object(arguments))
}

fn search_auto_refresh_request<T: Serialize>(
    target_name: &str,
    target_value: T,
    source_arguments: &Value,
    query: &str,
) -> ResultToolRequest {
    let mut arguments = source_arguments.as_object().cloned().unwrap_or_default();
    arguments.remove("queries");
    arguments.insert(target_name.to_string(), json!(target_value));
    arguments.insert("query".to_string(), json!(query));
    arguments.insert("refresh_if_stale".to_string(), json!(true));
    ResultToolRequest::new("search_auto".to_string(), Value::Object(arguments))
}

fn index_search_freshness(
    status: &IndexFreshness,
    refresh_request: ResultToolRequest,
) -> SearchFreshness {
    SearchFreshness {
        stale: true,
        summary: freshness_summary(
            "Index",
            status.changed_files,
            status.added_files,
            status.deleted_files,
            0,
            0,
        ),
        checked_files: status.checked_files,
        changed_files: status.changed_files,
        added_files: status.added_files,
        deleted_files: status.deleted_files,
        stale_shards: 0,
        git_metadata_changed: 0,
        refresh_request,
    }
}

fn shard_search_freshness(
    status: &ShardFreshness,
    refresh_request: ResultToolRequest,
) -> SearchFreshness {
    SearchFreshness {
        stale: true,
        summary: freshness_summary(
            "Scoped shard index",
            status.changed_files,
            status.added_files,
            status.deleted_files,
            status.stale_shards,
            status.git_metadata_changed,
        ),
        checked_files: status
            .shards
            .iter()
            .map(|shard| shard.status.checked_files)
            .sum(),
        changed_files: status.changed_files,
        added_files: status.added_files,
        deleted_files: status.deleted_files,
        stale_shards: status.stale_shards,
        git_metadata_changed: status.git_metadata_changed,
        refresh_request,
    }
}

fn freshness_refresh_request(freshness: &Option<SearchFreshness>) -> Option<ResultToolRequest> {
    freshness
        .as_ref()
        .map(|freshness| freshness.refresh_request.clone())
}

fn freshness_summary(
    label: &str,
    changed_files: usize,
    added_files: usize,
    deleted_files: usize,
    stale_shards: usize,
    git_metadata_changed: usize,
) -> String {
    let mut parts = Vec::new();
    if stale_shards > 0 {
        parts.push(format!("{stale_shards} stale shard(s)"));
    }
    if git_metadata_changed > 0 {
        parts.push(format!("{git_metadata_changed} git metadata change(s)"));
    }
    if changed_files > 0 {
        parts.push(format!("{changed_files} changed file(s)"));
    }
    if added_files > 0 {
        parts.push(format!("{added_files} added file(s)"));
    }
    if deleted_files > 0 {
        parts.push(format!("{deleted_files} deleted file(s)"));
    }
    if parts.is_empty() {
        format!("{label} is stale; rerun the refresh_request before trusting empty results.")
    } else {
        format!(
            "{label} is stale: {}; rerun the refresh_request before trusting empty results.",
            parts.join(", ")
        )
    }
}

fn shard_freshness_roots_for_search(
    index_dir: &Path,
    arguments: &Value,
    filters: &SearchFilters,
) -> Result<Vec<PathBuf>> {
    let has_positive_selector =
        filters.repo.is_some() || filters.branch.is_some() || filters.origin.is_some();
    let mut roots = if has_positive_selector {
        Vec::new()
    } else {
        scoped_shard_status_roots(arguments)?
    };
    if roots.is_empty() && has_positive_selector {
        let manifest = load_manifest(index_dir)?;
        roots = manifest
            .shards
            .iter()
            .filter(|shard| !shard_search_scopes(shard, filters).is_empty())
            .map(|shard| shard.root.clone())
            .collect();
    }
    if has_positive_selector {
        if let Some(root) = live_cwd_shard_root_matching_filters(index_dir, arguments, filters)? {
            roots.push(root);
        }
    }
    roots.sort();
    roots.dedup();
    Ok(roots)
}

fn live_cwd_shard_root_matching_filters(
    index_dir: &Path,
    arguments: &Value,
    filters: &SearchFilters,
) -> Result<Option<PathBuf>> {
    let Some(repo_root) = git_root_from_client_cwd(arguments, "shard_refresh")? else {
        return Ok(None);
    };
    let repo_key = canonical_cache_key(&repo_root);
    let manifest = load_manifest(index_dir)?;
    let Some(shard) = manifest
        .shards
        .iter()
        .find(|shard| canonical_cache_key(&shard.root) == repo_key)
    else {
        return Ok(None);
    };
    let mut shard = shard.clone();
    let git = git_metadata_for_repo(&repo_root, false);
    shard.git = (git.origin.is_some() || git.git_common_dir.is_some()).then_some(git);
    if shard_search_scopes(&shard, filters).is_empty() {
        return Ok(None);
    }
    Ok(Some(repo_root))
}

enum ShardRefreshSelection {
    Roots(Vec<PathBuf>),
    All,
}

fn shard_refresh_selection_for_search(
    index_dir: &Path,
    arguments: &Value,
    filters: &SearchFilters,
) -> Result<ShardRefreshSelection> {
    let roots = shard_freshness_roots_for_search(index_dir, arguments, filters)?;
    if roots.is_empty()
        && (filters.repo.is_some() || filters.branch.is_some() || filters.origin.is_some())
    {
        let scoped_roots = scoped_shard_status_roots(arguments)?;
        if !scoped_roots.is_empty() {
            return Ok(ShardRefreshSelection::Roots(scoped_roots));
        }
        return Ok(ShardRefreshSelection::All);
    }
    if !roots.is_empty()
        || filters.repo.is_some()
        || filters.branch.is_some()
        || filters.origin.is_some()
    {
        return Ok(ShardRefreshSelection::Roots(roots));
    }
    Ok(ShardRefreshSelection::All)
}

fn add_shard_scope_filter_args(arguments: &mut Map<String, Value>, filters: &SearchFilters) {
    if let Some(repo) = &filters.repo {
        arguments.insert("repo".to_string(), json!(repo));
    }
    if let Some(branch) = &filters.branch {
        arguments.insert("branch".to_string(), json!(branch));
    }
    if let Some(origin) = &filters.origin {
        arguments.insert("origin".to_string(), json!(origin));
    }
    if !filters.exclude_repo.is_empty() {
        arguments.insert("exclude_repo".to_string(), json!(&filters.exclude_repo));
    }
    if !filters.exclude_branch.is_empty() {
        arguments.insert("exclude_branch".to_string(), json!(&filters.exclude_branch));
    }
    if !filters.exclude_origin.is_empty() {
        arguments.insert("exclude_origin".to_string(), json!(&filters.exclude_origin));
    }
}

fn attach_retry_requests<T: Serialize>(
    mut plan: QueryPlan,
    search_tool: &str,
    target_name: &str,
    target_value: T,
    source_arguments: &Value,
) -> QueryPlan {
    let retry_requests = retry_search_requests(
        &plan,
        search_tool,
        target_name,
        target_value,
        source_arguments,
    );
    plan.set_retry_requests(retry_requests);
    plan
}

fn attach_result_query_plan_retry_requests<T: Serialize>(
    results: &mut [SearchResult],
    search_tool: &str,
    target_name: &str,
    target_value: &T,
    source_arguments: &Value,
) {
    for result in results {
        let Some(plan) = result.query_plan.take() else {
            continue;
        };
        result.query_plan = Some(attach_retry_requests(
            plan,
            search_tool,
            target_name,
            target_value,
            source_arguments,
        ));
    }
}

fn primary_retry_request_from_plan(plan: &QueryPlan) -> Option<ResultToolRequest> {
    plan.primary_retry_request
        .clone()
        .or_else(|| plan.retry_requests.first().cloned())
}

fn primary_diagnosis_from_plan(plan: &QueryPlan) -> Option<Value> {
    plan.diagnosis
        .as_ref()
        .and_then(|diagnosis| serde_json::to_value(diagnosis).ok())
}

fn primary_retry_request_from_shard_plans(plans: &[ShardQueryPlan]) -> Option<ResultToolRequest> {
    plans
        .iter()
        .find_map(|shard_plan| primary_retry_request_from_plan(&shard_plan.plan))
}

fn primary_diagnosis_from_shard_plans(
    plans: &[ShardQueryPlan],
    results_empty: bool,
) -> Option<Value> {
    let diagnosis = plans
        .iter()
        .find_map(|shard_plan| primary_diagnosis_from_plan(&shard_plan.plan));
    if results_empty
        && diagnosis
            .as_ref()
            .and_then(|value| value.get("status"))
            .and_then(Value::as_str)
            .is_some_and(|status| matches!(status, "matched" | "candidate_cap_hit"))
    {
        return Some(json!({
            "status": "result_plan_mismatch",
            "summary": "Shard query plans reported matches, but shard search returned no final results after routing and finalization.",
            "next_action": "Run the query_plan_request, then retry with a narrower query or refresh/rebuild shards if the plan and results still disagree."
        }));
    }
    diagnosis
}

fn primary_retry_result_value(request: &ResultToolRequest, result: Value) -> Result<Value> {
    let read_batch_request = primary_retry_read_batch_request(request, &result);
    let summary = primary_retry_result_summary(&result);
    let mut value = json!({
        "request": request,
        "summary": summary,
        "results": result
    });
    if let Some(read_batch_request) = read_batch_request {
        value["read_batch_request"] = serde_json::to_value(read_batch_request)?;
    }
    Ok(value)
}

fn primary_retry_result_summary(result: &Value) -> Value {
    let results = result.as_array().map(Vec::as_slice).unwrap_or(&[]);
    let mut summary = json!({
        "status": if results.is_empty() { "not_found" } else { "matched" },
        "result_count": results.len()
    });
    let grouped_duplicate_count = grouped_duplicate_count_from_value(result);
    if grouped_duplicate_count > 0 {
        summary["grouped_duplicate_count"] = json!(grouped_duplicate_count);
    }
    let mut top_paths = Vec::new();
    for item in results {
        let Some(path) = item.get("path").and_then(Value::as_str) else {
            continue;
        };
        if !top_paths.iter().any(|existing| existing == path) {
            top_paths.push(path.to_string());
            if top_paths.len() == 5 {
                break;
            }
        }
    }
    if !top_paths.is_empty() {
        summary["top_paths"] = json!(top_paths);
    }
    let top_dirs = value_summary_top_dirs(results);
    if !top_dirs.is_empty() {
        summary["top_dirs"] = json!(top_dirs);
    }
    let top_exts = value_summary_top_exts(results);
    if !top_exts.is_empty() {
        summary["top_exts"] = json!(top_exts);
    }
    let top_langs = value_summary_top_langs(results);
    if !top_langs.is_empty() {
        summary["top_langs"] = json!(top_langs);
    }
    if let Some(score) = results
        .first()
        .and_then(|item| item.get("score"))
        .and_then(Value::as_f64)
    {
        summary["max_score"] = json!(score);
    }
    if let Some(score) = results
        .last()
        .and_then(|item| item.get("score"))
        .and_then(Value::as_f64)
    {
        summary["min_score"] = json!(score);
    }
    summary
}

fn value_summary_top_dirs(results: &[Value]) -> Vec<String> {
    let mut dirs = Vec::new();
    for item in results {
        let Some(path) = item.get("path").and_then(Value::as_str) else {
            continue;
        };
        let dir = search_summary_dir(path);
        if !dirs.iter().any(|existing| existing == &dir) {
            dirs.push(dir);
            if dirs.len() == 5 {
                break;
            }
        }
    }
    dirs
}

fn value_summary_top_exts(results: &[Value]) -> Vec<String> {
    let mut exts = Vec::new();
    for item in results {
        let Some(path) = item.get("path").and_then(Value::as_str) else {
            continue;
        };
        let Some(ext) = search_summary_ext(path) else {
            continue;
        };
        if !exts.iter().any(|existing| existing == &ext) {
            exts.push(ext);
            if exts.len() == 5 {
                break;
            }
        }
    }
    exts
}

fn value_summary_top_langs(results: &[Value]) -> Vec<String> {
    let mut langs = Vec::new();
    for item in results {
        let Some(path) = item.get("path").and_then(Value::as_str) else {
            continue;
        };
        let Some(language) = language_for(Path::new(path)) else {
            continue;
        };
        if !langs.iter().any(|existing| existing == &language) {
            langs.push(language);
            if langs.len() == 5 {
                break;
            }
        }
    }
    langs
}

fn primary_retry_read_batch_request(
    request: &ResultToolRequest,
    result: &Value,
) -> Option<ResultToolRequest> {
    let base_arguments = retry_read_base_arguments(request)?;
    result_value_read_batch_request(result, "read_ranges", base_arguments)
}

fn read_ranges_response_summary(ranges: &[FileRange]) -> ReadRangesResponseSummary {
    let mut seen_paths = HashSet::new();
    let mut paths = Vec::new();
    let mut top_dirs = Vec::new();
    let mut top_exts = Vec::new();
    let mut top_langs = Vec::new();
    let mut total_lines = 0;

    for range in ranges {
        total_lines += range.summary.line_count;
        let dir = search_summary_dir(&range.path);
        if top_dirs.len() < 5 && !top_dirs.iter().any(|existing| existing == &dir) {
            top_dirs.push(dir);
        }
        if let Some(ext) = search_summary_ext(&range.path)
            && top_exts.len() < 5
            && !top_exts.iter().any(|existing| existing == &ext)
        {
            top_exts.push(ext);
        }
        if let Some(language) = language_for(Path::new(&range.path))
            && top_langs.len() < 5
            && !top_langs.iter().any(|existing| existing == &language)
        {
            top_langs.push(language);
        }
        if seen_paths.insert(range.path.clone()) && paths.len() < 5 {
            paths.push(range.path.clone());
        }
    }

    ReadRangesResponseSummary {
        status: if ranges.is_empty() { "empty" } else { "read" },
        range_count: ranges.len(),
        total_lines,
        path_count: seen_paths.len(),
        paths,
        top_dirs,
        top_exts,
        top_langs,
    }
}

fn read_ranges_response_value(ranges: Vec<FileRange>, include_summary: bool) -> Result<Value> {
    if include_summary {
        Ok(json!({
            "summary": read_ranges_response_summary(&ranges),
            "ranges": ranges,
        }))
    } else {
        Ok(serde_json::to_value(ranges)?)
    }
}

fn related_lookup_response<T: Serialize>(
    results: Vec<T>,
    include_read_batch: bool,
    batch_tool: &str,
    base_arguments: Map<String, Value>,
    summary: &str,
) -> Result<Value> {
    let results = serde_json::to_value(results)?;
    if !include_read_batch {
        return Ok(results);
    }
    let read_batch_request = result_value_read_batch_request(&results, batch_tool, base_arguments);
    let next_action = read_batch_request.as_ref().map(|request| {
        json!({
            "kind": "read",
            "source": "read_batch_request",
            "summary": summary,
            "request": request
        })
    });
    Ok(json!({
        "summary": related_lookup_summary(&results),
        "results": results,
        "read_batch_request": read_batch_request,
        "next_action": next_action
    }))
}

fn related_lookup_summary(results: &Value) -> Value {
    let results = results.as_array().map(Vec::as_slice).unwrap_or(&[]);
    let result_count = results.len();
    let mut summary = json!({
        "status": if result_count == 0 { "not_found" } else { "matched" },
        "result_count": result_count
    });
    let mut top_paths = Vec::new();
    let mut top_dirs = Vec::new();
    let mut top_exts = Vec::new();
    let mut top_langs = Vec::new();
    let mut top_symbols = Vec::new();
    let mut symbol_kinds = Vec::new();
    for item in results {
        let path = related_lookup_item_path(item);
        if let Some(path) = path
            && top_paths.len() < 5
            && !top_paths.iter().any(|existing| existing == path)
        {
            top_paths.push(path.to_string());
        }
        if let Some(path) = path {
            let dir = search_summary_dir(path);
            if top_dirs.len() < 5 && !top_dirs.iter().any(|existing| existing == &dir) {
                top_dirs.push(dir);
            }
            if let Some(ext) = search_summary_ext(path)
                && top_exts.len() < 5
                && !top_exts.iter().any(|existing| existing == &ext)
            {
                top_exts.push(ext);
            }
            if let Some(language) = language_for(Path::new(path))
                && top_langs.len() < 5
                && !top_langs.iter().any(|existing| existing == &language)
            {
                top_langs.push(language);
            }
        }
        if let Some(symbol) = item.get("symbol") {
            if let Some(name) = symbol.get("name").and_then(Value::as_str)
                && top_symbols.len() < 5
                && !top_symbols.iter().any(|existing| existing == name)
            {
                top_symbols.push(name.to_string());
            }
            if let Some(kind) = symbol.get("kind").and_then(Value::as_str)
                && symbol_kinds.len() < 5
                && !symbol_kinds.iter().any(|existing| existing == kind)
            {
                symbol_kinds.push(kind.to_string());
            }
        }
    }
    if !top_paths.is_empty() {
        summary["top_paths"] = json!(top_paths);
    }
    if !top_dirs.is_empty() {
        summary["top_dirs"] = json!(top_dirs);
    }
    if !top_exts.is_empty() {
        summary["top_exts"] = json!(top_exts);
    }
    if !top_langs.is_empty() {
        summary["top_langs"] = json!(top_langs);
    }
    if !top_symbols.is_empty() {
        summary["top_symbols"] = json!(top_symbols);
    }
    if !symbol_kinds.is_empty() {
        summary["symbol_kinds"] = json!(symbol_kinds);
    }
    if let Some(score) = results
        .first()
        .and_then(|item| item.get("score"))
        .and_then(Value::as_f64)
    {
        summary["max_score"] = json!(score);
    }
    if let Some(score) = results
        .last()
        .and_then(|item| item.get("score"))
        .and_then(Value::as_f64)
    {
        summary["min_score"] = json!(score);
    }
    summary
}

fn related_lookup_item_path(item: &Value) -> Option<&str> {
    item.get("path")
        .or_else(|| item.get("symbol").and_then(|symbol| symbol.get("path")))
        .and_then(Value::as_str)
}

fn promoted_next_read_batch_request(
    read_batch_request: &Option<ResultToolRequest>,
    primary_retry_result: &Option<Value>,
) -> Option<ResultToolRequest> {
    read_batch_request.clone().or_else(|| {
        let value = primary_retry_result.as_ref()?.get("read_batch_request")?;
        serde_json::from_value(value.clone()).ok()
    })
}

fn search_auto_next_action(
    refresh_request: &Option<ResultToolRequest>,
    next_read_batch_request: &Option<ResultToolRequest>,
    primary_retry_request: &Option<ResultToolRequest>,
    repo_map_request: &ResultToolRequest,
    prefer_retry: bool,
) -> Option<Value> {
    if let Some(request) = refresh_request {
        return Some(json!({
            "kind": "refresh",
            "source": "refresh_request",
            "summary": "Refresh the stale scoped index, then repeat the search.",
            "request": request
        }));
    }
    if prefer_retry {
        if let Some(request) = primary_retry_request {
            return Some(json!({
                "kind": "retry",
                "source": "primary_retry_request",
                "summary": "Run the promoted repaired search.",
                "request": request
            }));
        }
    }
    if let Some(request) = next_read_batch_request {
        let summary = read_batch_action_summary(request, "Read the top available result ranges.");
        return Some(json!({
            "kind": "read",
            "source": "next_read_batch_request",
            "summary": summary,
            "request": request
        }));
    }
    if let Some(request) = primary_retry_request {
        return Some(json!({
            "kind": "retry",
            "source": "primary_retry_request",
            "summary": "Run the promoted repaired search.",
            "request": request
        }));
    }
    Some(json!({
        "kind": "map",
        "source": "repo_map_request",
        "summary": "Open a compact repo map before broadening manually.",
        "request": repo_map_request
    }))
}

fn should_prefer_retry_next_action(
    primary_diagnosis: &Option<Value>,
    primary_retry_request: &Option<ResultToolRequest>,
    primary_retry_result: &Option<Value>,
) -> bool {
    primary_retry_result.is_none()
        && primary_retry_request.is_some()
        && primary_diagnosis
            .as_ref()
            .and_then(|diagnosis| diagnosis.get("suggested_query"))
            .and_then(Value::as_str)
            .is_some()
}

fn retry_read_base_arguments(request: &ResultToolRequest) -> Option<Map<String, Value>> {
    let source = request.arguments.as_object()?;
    let mut arguments = Map::new();
    for name in ["index_dir", "index", "repo"] {
        if let Some(value) = source.get(name) {
            arguments.insert(name.to_string(), value.clone());
            return Some(arguments);
        }
    }
    None
}

fn arguments_scoped_to_client_cwd(arguments: &Value) -> Result<Value> {
    if optional_string_arg(arguments, "repo_filter").is_some() {
        return Ok(arguments.clone());
    }
    let Some(repo_root) = git_root_from_client_cwd(arguments, "cwd")? else {
        return Ok(arguments.clone());
    };
    let mut scoped = arguments.clone();
    let Some(object) = scoped.as_object_mut() else {
        return Ok(scoped);
    };
    object.insert(
        "repo_filter".to_string(),
        json!(repo_root.to_string_lossy().to_string()),
    );
    Ok(scoped)
}

fn arguments_scoped_to_client_cwd_for_query(arguments: &Value, query: &str) -> Result<Value> {
    let query_filters = parse_query(query).filters;
    if query_filters.repo.is_some()
        || query_filters.branch.is_some()
        || query_filters.origin.is_some()
    {
        return Ok(arguments.clone());
    }
    arguments_scoped_to_client_cwd(arguments)
}

fn live_repo_from_client_cwd(arguments: &Value, tool_name: &str) -> Result<PathBuf> {
    if let Some(repo_root) = git_root_from_client_cwd(arguments, tool_name)? {
        return Ok(repo_root);
    }
    if let Some(cwd) = optional_string_arg(arguments, "cwd") {
        return PathBuf::from(cwd)
            .canonicalize()
            .with_context(|| format!("canonicalize {tool_name} cwd"));
    }
    std::env::current_dir().with_context(|| format!("resolve current directory for {tool_name}"))
}

fn git_root_from_client_cwd(arguments: &Value, tool_name: &str) -> Result<Option<PathBuf>> {
    let Some(cwd) = optional_string_arg(arguments, "cwd") else {
        return Ok(None);
    };
    let cwd = PathBuf::from(cwd)
        .canonicalize()
        .with_context(|| format!("canonicalize {tool_name} cwd"))?;
    Ok(cwd
        .ancestors()
        .find(|ancestor| ancestor.join(".git").exists())
        .map(Path::to_path_buf))
}

fn index_matches_client_cwd(index: &FastIndex, arguments: &Value) -> Result<bool> {
    let Some(repo_root) = git_root_from_client_cwd(arguments, "cwd")? else {
        return Ok(false);
    };
    Ok(index
        .root
        .canonicalize()
        .unwrap_or_else(|_| index.root.clone())
        == repo_root)
}

fn scoped_shard_status_roots(arguments: &Value) -> Result<Vec<PathBuf>> {
    let repo_filter = optional_string_arg(arguments, "repo_filter");
    if let Some(repo_root) = git_root_from_client_cwd(arguments, "shard_status")? {
        let repo_root_text = repo_root.to_string_lossy().to_string();
        if repo_filter
            .as_deref()
            .is_none_or(|filter| filter == repo_root_text.as_str())
        {
            return Ok(vec![repo_root]);
        }
    }
    let Some(repo_filter) = repo_filter else {
        return Ok(Vec::new());
    };
    let path = PathBuf::from(&repo_filter);
    if path.is_absolute() && path.exists() {
        return path
            .canonicalize()
            .map(|root| vec![root])
            .with_context(|| format!("canonicalize shard_status repo_filter {repo_filter}"));
    }
    Ok(Vec::new())
}

fn retry_search_requests<T: Serialize>(
    plan: &QueryPlan,
    search_tool: &str,
    target_name: &str,
    target_value: T,
    source_arguments: &Value,
) -> Vec<ResultToolRequest> {
    let mut requests = Vec::new();
    let mut seen_queries = HashSet::new();
    let replaced_filter_fields = plan
        .repair_hints
        .iter()
        .filter_map(|hint| retry_replaced_filter_field(&hint.kind))
        .collect::<Vec<_>>();
    let repair_filter_fields = plan
        .repair_hints
        .iter()
        .filter_map(|hint| {
            retry_replaced_filter_field(&hint.kind)
                .or_else(|| retry_relaxed_filter_field(&hint.kind))
        })
        .collect::<Vec<_>>();
    for hint in &plan.repair_hints {
        let Some(query) = hint.suggested_query.as_ref() else {
            continue;
        };
        if !seen_queries.insert(query.clone()) {
            continue;
        }
        let mut arguments = Map::new();
        let replace_filter_field = retry_replaced_filter_field(&hint.kind);
        let relaxed_field = retry_relaxed_filter_field(&hint.kind);
        let suggested_filters = parse_query(query).filters;
        if hint.kind == "relax_filters" {
            if let Some(source) = source_arguments.as_object() {
                for name in ["refresh_if_stale", "require_all", "any_terms"] {
                    if let Some(value) = source.get(name) {
                        arguments.insert(name.to_string(), value.clone());
                    }
                }
            }
        } else if let Some(source) = source_arguments.as_object() {
            for (name, value) in source {
                if retry_source_arg_matches_filter(name, replace_filter_field.or(relaxed_field)) {
                    continue;
                }
                if retry_search_passthrough_arg(name, target_name) {
                    arguments.insert(name.clone(), value.clone());
                }
            }
        }
        if hint.kind != "relax_filters" {
            let skip_field = replace_filter_field.or(relaxed_field);
            add_plan_filter_args(&mut arguments, plan, target_name, skip_field);
            if skip_field.is_none() && retry_hint_drops_replaced_filters(&hint.action) {
                remove_repair_filter_args(
                    &mut arguments,
                    &replaced_filter_fields,
                    &suggested_filters,
                );
            } else if skip_field.is_none() && retry_hint_drops_repaired_filters(&hint.action) {
                remove_repair_filter_args(
                    &mut arguments,
                    &repair_filter_fields,
                    &suggested_filters,
                );
            }
        }
        arguments.insert(target_name.to_string(), json!(target_value));
        arguments.insert("query".to_string(), json!(query));
        arguments.insert("explain".to_string(), json!(true));
        requests.push(ResultToolRequest::new(
            search_tool.to_string(),
            Value::Object(arguments),
        ));
    }
    requests
}

fn retry_hint_drops_replaced_filters(action: &str) -> bool {
    action == "drop_terms"
}

fn retry_hint_drops_repaired_filters(action: &str) -> bool {
    matches!(action, "broaden_terms" | "relax_query" | "broaden_query")
}

fn retry_replaced_filter_field(kind: &str) -> Option<&'static str> {
    match kind {
        "replace_file_filter" => Some("file"),
        "replace_path_filter" => Some("path"),
        "replace_symbol_filter" => Some("symbol"),
        "replace_symbol_kind_filter" => Some("symbol_kind"),
        _ => None,
    }
}

fn retry_relaxed_filter_field(kind: &str) -> Option<&'static str> {
    match kind {
        "relax_file_filter" => Some("file"),
        "relax_path_filter" => Some("path"),
        "relax_language_filter" => Some("language"),
        "relax_extension_filter" => Some("extension"),
        "relax_test_filter" => Some("test"),
        "relax_generated_filter" => Some("generated"),
        "relax_code_filter" => Some("code"),
        "relax_symbol_kind_filter" => Some("symbol_kind"),
        "relax_repo_filter" => Some("repo"),
        "relax_branch_filter" => Some("branch"),
        "relax_origin_filter" => Some("origin"),
        "relax_dependency_filter" => Some("dependency"),
        "relax_import_filter" => Some("import"),
        _ => None,
    }
}

fn retry_source_arg_matches_filter(name: &str, field: Option<&str>) -> bool {
    matches!(
        (field, name),
        (Some("file"), "file" | "filename" | "file_name")
            | (Some("path"), "path" | "dir" | "directory" | "folder")
            | (Some("symbol"), "symbol")
            | (Some("language"), "language" | "lang")
            | (Some("extension"), "extension" | "ext")
            | (Some("test"), "test" | "tests")
            | (Some("generated"), "generated")
            | (Some("code"), "code")
            | (Some("symbol_kind"), "symbol_kind" | "kind" | "type")
            | (Some("repo"), "repo" | "repo_filter")
            | (Some("branch"), "branch" | "git_branch")
            | (Some("origin"), "origin" | "remote" | "remote_origin")
            | (Some("dependency"), "dependency" | "dep" | "deps")
            | (Some("import"), "import" | "module" | "use")
    )
}

fn retry_search_passthrough_arg(name: &str, target_name: &str) -> bool {
    if matches!(
        name,
        "query"
            | "queries"
            | "cwd"
            | "limit"
            | "context_lines"
            | "snippet"
            | "snippet_mode"
            | "snippet-mode"
            | "explain"
            | "diagnose"
            | "retry_if_empty"
            | "summary"
    ) {
        return false;
    }
    if name == target_name {
        return false;
    }
    if matches!(target_name, "index" | "index_dir") && matches!(name, "index" | "index_dir") {
        return false;
    }
    true
}

fn add_plan_filter_args(
    arguments: &mut Map<String, Value>,
    plan: &QueryPlan,
    target_name: &str,
    skip_field: Option<&str>,
) {
    let mut negated: HashMap<String, Vec<String>> = HashMap::default();
    for filter in &plan.active_filters {
        if skip_field == Some(filter.field.as_str()) {
            continue;
        }
        if !filter.negated {
            if filter.field == "repo" && target_name == "repo" {
                continue;
            }
            arguments.insert(filter.field.clone(), plan_filter_argument_value(filter));
            continue;
        }
        let key = format!("exclude_{}", filter.field);
        negated.entry(key).or_default().push(filter.value.clone());
    }
    for (name, values) in negated {
        arguments.insert(name, json!(values));
    }
}

fn remove_repair_filter_args(
    arguments: &mut Map<String, Value>,
    repair_filter_fields: &[&str],
    suggested_filters: &SearchFilters,
) {
    for field in repair_filter_fields {
        if query_plan_filter_field_present(field, suggested_filters) {
            continue;
        }
        arguments.remove(*field);
        arguments.remove(&format!("exclude_{field}"));
    }
}

fn plan_filter_argument_value(filter: &QueryPlanFilter) -> Value {
    match filter.field.as_str() {
        "test" | "generated" | "code" => json!(matches!(
            filter.value.to_ascii_lowercase().as_str(),
            "1" | "true" | "yes" | "y"
        )),
        _ => json!(filter.value),
    }
}

fn attach_shard_retry_requests(
    plans: &mut [ShardQueryPlan],
    index_dir: &Path,
    source_arguments: &Value,
) {
    attach_shard_retry_requests_with_tool(plans, "search_shards", index_dir, source_arguments);
}

fn attach_shard_retry_requests_with_tool(
    plans: &mut [ShardQueryPlan],
    search_tool: &str,
    index_dir: &Path,
    source_arguments: &Value,
) {
    for shard_plan in plans {
        shard_plan.plan = attach_retry_requests(
            shard_plan.plan.clone(),
            search_tool,
            "index_dir",
            index_dir,
            source_arguments,
        );
        shard_plan.refresh_summary();
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
                optional_string_arg(&request.arguments, "socket").as_deref(),
                optional_string_arg(&request.arguments, "profile").as_deref(),
            )),
            "agent_instructions" => Ok(json!({
                "instructions": agent_instructions(
                    optional_string_arg(&request.arguments, "repo").as_deref(),
                    optional_string_arg(&request.arguments, "index").as_deref(),
                    optional_string_arg(&request.arguments, "index_dir").as_deref(),
                    optional_string_arg(&request.arguments, "addr").as_deref(),
                    optional_string_arg(&request.arguments, "socket").as_deref(),
                    optional_string_arg(&request.arguments, "profile").as_deref(),
                )
            })),
            "discover_repos" => {
                let root = path_arg(&request.arguments, "root")?;
                let max_depth = positive_usize_arg(&request.arguments, "max_depth", 4)?;
                let limit = positive_usize_arg(&request.arguments, "limit", 500)?;
                let family_limit = optional_family_limit_arg(&request.arguments)?;
                let git_metadata = bool_arg(&request.arguments, "git_metadata");
                let tracked_files = bool_arg(&request.arguments, "tracked_files");
                let nested_manifests = bool_arg(&request.arguments, "nested_manifests");
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
                let detail = repo_map_detail_arg(&request.arguments)?;
                let index = RepoIndexer::new(repo).build()?;
                Ok(serde_json::to_value(index.repo_brief_with_detail(detail))?)
            }
            "repo_map" => {
                let symbol_limit = positive_usize_arg(&request.arguments, "symbols", 50)?;
                let test_limit = positive_usize_arg(&request.arguments, "tests", 50)?;
                let detail = repo_map_detail_arg(&request.arguments)?;
                let read_limit = repo_map_read_limit_arg(&request.arguments)?;
                if argument_value(&request.arguments, "index").is_some()
                    && argument_value(&request.arguments, "index_dir").is_some()
                {
                    return Err(anyhow!("repo_map accepts only one of index or index_dir"));
                }
                if let Some(index_dir) =
                    optional_string_arg(&request.arguments, "index_dir").map(PathBuf::from)
                {
                    let filters = search_filters(&request.arguments, true)?;
                    if bool_arg(&request.arguments, "refresh_if_stale") {
                        self.refresh_shards_for_arguments_if_stale(
                            &index_dir,
                            &request.arguments,
                            &filters,
                        )?;
                    }
                    return Ok(serde_json::to_value(self.shard_repo_maps_cached(
                        &index_dir,
                        symbol_limit,
                        test_limit,
                        detail,
                        read_limit,
                        &filters,
                        "read_ranges",
                    )?)?);
                }
                if let Some(index_path) =
                    optional_string_arg(&request.arguments, "index").map(PathBuf::from)
                {
                    let refresh_if_stale = bool_arg(&request.arguments, "refresh_if_stale");
                    let index =
                        self.cached_index_maybe_refresh(index_path.clone(), refresh_if_stale)?;
                    let mut map = index.repo_map_with_detail(symbol_limit, test_limit, detail);
                    attach_repo_map_read_batch_request_with_limit(
                        &mut map,
                        "read_ranges",
                        read_request_args("index", &index_path),
                        read_limit,
                    );
                    return Ok(serde_json::to_value(map)?);
                }
                if optional_string_arg(&request.arguments, "cwd").is_some() {
                    let scoped_arguments = arguments_scoped_to_client_cwd(&request.arguments)?;
                    if let Ok(index_dir) = self.single_cached_shard_manifest_path() {
                        let filters = search_filters(&scoped_arguments, true)?;
                        if bool_arg(&request.arguments, "refresh_if_stale") {
                            self.refresh_shards_for_arguments_if_stale(
                                &index_dir,
                                &scoped_arguments,
                                &filters,
                            )?;
                        }
                        return Ok(serde_json::to_value(self.shard_repo_maps_cached(
                            &index_dir,
                            symbol_limit,
                            test_limit,
                            detail,
                            read_limit,
                            &filters,
                            "read_ranges",
                        )?)?);
                    }
                    if let Ok(index_path) = self.single_cached_index_path() {
                        let refresh_if_stale = bool_arg(&request.arguments, "refresh_if_stale");
                        let index =
                            self.cached_index_maybe_refresh(index_path.clone(), refresh_if_stale)?;
                        if index_matches_client_cwd(&index, &request.arguments)? {
                            let mut map =
                                index.repo_map_with_detail(symbol_limit, test_limit, detail);
                            attach_repo_map_read_batch_request_with_limit(
                                &mut map,
                                "read_ranges",
                                read_request_args("index", &index_path),
                                read_limit,
                            );
                            return Ok(serde_json::to_value(map)?);
                        }
                    }
                }
                let repo = optional_string_arg(&request.arguments, "repo")
                    .map(PathBuf::from)
                    .map(Ok)
                    .unwrap_or_else(|| live_repo_from_client_cwd(&request.arguments, "repo_map"))?;
                let index = RepoIndexer::new(&repo).build()?;
                let mut map = index.repo_map_with_detail(symbol_limit, test_limit, detail);
                attach_repo_map_read_batch_request_with_limit(
                    &mut map,
                    "read_ranges",
                    read_request_args("repo", &repo),
                    read_limit,
                );
                Ok(serde_json::to_value(map)?)
            }
            "indexed_repo_map" => {
                let index_path = self.index_path_arg_or_single_cached(&request.arguments)?;
                let symbol_limit = positive_usize_arg(&request.arguments, "symbols", 50)?;
                let test_limit = positive_usize_arg(&request.arguments, "tests", 50)?;
                let detail = repo_map_detail_arg(&request.arguments)?;
                let read_limit = repo_map_read_limit_arg(&request.arguments)?;
                let index = self.cached_index(index_path.clone())?;
                let mut map = index.repo_map_with_detail(symbol_limit, test_limit, detail);
                attach_repo_map_read_batch_request_with_limit(
                    &mut map,
                    "read_index_ranges",
                    read_request_args("index", &index_path),
                    read_limit,
                );
                Ok(serde_json::to_value(map)?)
            }
            "read_range" | "open_range" => {
                let tool_name = request.tool.as_str();
                let range = single_range_arg(&request.arguments, tool_name)?;
                if argument_value(&request.arguments, "index").is_some()
                    && argument_value(&request.arguments, "index_dir").is_some()
                {
                    return Err(anyhow!(
                        "{tool_name} accepts only one of index or index_dir"
                    ));
                }
                if let Some(index_dir) =
                    optional_string_arg(&request.arguments, "index_dir").map(PathBuf::from)
                {
                    return Ok(serde_json::to_value(self.read_shard_range_cached_scoped(
                        &index_dir,
                        &range.path,
                        range.start,
                        range.lines,
                        range.scope,
                    )?)?);
                }
                if let Some(index_path) =
                    optional_string_arg(&request.arguments, "index").map(PathBuf::from)
                {
                    let index = self.cached_index(index_path)?;
                    return Ok(serde_json::to_value(index.read_range_scoped(
                        &range.path,
                        range.start,
                        range.lines,
                        range.scope,
                    )?)?);
                }
                if optional_string_arg(&request.arguments, "cwd").is_some() {
                    if let Ok(index_dir) = self.single_cached_shard_manifest_path() {
                        if let Some(range) = self.read_shard_range_for_client_cwd(
                            &index_dir,
                            &request.arguments,
                            &range.path,
                            range.start,
                            range.lines,
                            range.scope,
                            tool_name,
                        )? {
                            return Ok(serde_json::to_value(range)?);
                        }
                    }
                    if let Ok(index_path) = self.single_cached_index_path() {
                        let index = self.cached_index(index_path)?;
                        if index_matches_client_cwd(&index, &request.arguments)? {
                            return Ok(serde_json::to_value(index.read_range_scoped(
                                &range.path,
                                range.start,
                                range.lines,
                                range.scope,
                            )?)?);
                        }
                    }
                }
                let repo = optional_string_arg(&request.arguments, "repo")
                    .map(PathBuf::from)
                    .map(Ok)
                    .unwrap_or_else(|| live_repo_from_client_cwd(&request.arguments, tool_name))?;
                Ok(serde_json::to_value(read_file_range_scoped(
                    repo,
                    &range.path,
                    range.start,
                    range.lines,
                    range.scope,
                )?)?)
            }
            "read_ranges" | "open_ranges" => {
                let tool_name = request.tool.as_str();
                let ranges = range_args(&request.arguments, tool_name)?;
                let include_summary = bool_arg(&request.arguments, "include_summary");
                if argument_value(&request.arguments, "index").is_some()
                    && argument_value(&request.arguments, "index_dir").is_some()
                {
                    return Err(anyhow!(
                        "{tool_name} accepts only one of index or index_dir"
                    ));
                }
                if let Some(index_dir) =
                    optional_string_arg(&request.arguments, "index_dir").map(PathBuf::from)
                {
                    let mut results = Vec::new();
                    for range in ranges {
                        results.push(self.read_shard_range_cached_scoped(
                            &index_dir,
                            &range.path,
                            range.start,
                            range.lines,
                            range.scope,
                        )?);
                    }
                    return read_ranges_response_value(results, include_summary);
                }
                if let Some(index_path) =
                    optional_string_arg(&request.arguments, "index").map(PathBuf::from)
                {
                    let index = self.cached_index(index_path)?;
                    let mut results = Vec::new();
                    for range in ranges {
                        results.push(index.read_range_scoped(
                            &range.path,
                            range.start,
                            range.lines,
                            range.scope,
                        )?);
                    }
                    return read_ranges_response_value(results, include_summary);
                }
                if optional_string_arg(&request.arguments, "cwd").is_some() {
                    if let Ok(index_dir) = self.single_cached_shard_manifest_path() {
                        let mut results = Vec::new();
                        let mut matched = false;
                        for range in &ranges {
                            let Some(result) = self.read_shard_range_for_client_cwd(
                                &index_dir,
                                &request.arguments,
                                &range.path,
                                range.start,
                                range.lines,
                                range.scope,
                                tool_name,
                            )?
                            else {
                                break;
                            };
                            matched = true;
                            results.push(result);
                        }
                        if matched && results.len() == ranges.len() {
                            return read_ranges_response_value(results, include_summary);
                        }
                    }
                    if let Ok(index_path) = self.single_cached_index_path() {
                        let index = self.cached_index(index_path)?;
                        if index_matches_client_cwd(&index, &request.arguments)? {
                            let mut results = Vec::new();
                            for range in ranges {
                                results.push(index.read_range_scoped(
                                    &range.path,
                                    range.start,
                                    range.lines,
                                    range.scope,
                                )?);
                            }
                            return read_ranges_response_value(results, include_summary);
                        }
                    }
                }
                let repo = optional_string_arg(&request.arguments, "repo")
                    .map(PathBuf::from)
                    .map(Ok)
                    .unwrap_or_else(|| live_repo_from_client_cwd(&request.arguments, tool_name))?;
                let mut results = Vec::new();
                for range in ranges {
                    results.push(read_file_range_scoped(
                        &repo,
                        &range.path,
                        range.start,
                        range.lines,
                        range.scope,
                    )?);
                }
                read_ranges_response_value(results, include_summary)
            }
            "search_code" => {
                let repo = path_arg(&request.arguments, "repo")?;
                let query = string_arg(&request.arguments, "query")?;
                let limit = search_limit_arg(&request.arguments)?;
                let context_lines = context_lines_arg(&request.arguments)?;
                let filters = search_filters(&request.arguments, false)?;
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
                    Some(&filters),
                );
                attach_result_related_symbol_requests(
                    &mut results,
                    "related_symbols",
                    Some(&query),
                    read_request_args("repo", &repo),
                );
                Ok(serde_json::to_value(results)?)
            }
            "search" => {
                let query = string_arg(&request.arguments, "query")?;
                let limit = search_limit_arg(&request.arguments)?;
                let context_lines = context_lines_arg(&request.arguments)?;
                if argument_value(&request.arguments, "index").is_some()
                    && argument_value(&request.arguments, "index_dir").is_some()
                {
                    return Err(anyhow!("search accepts only one of index or index_dir"));
                }
                if let Some(index_dir) =
                    optional_string_arg(&request.arguments, "index_dir").map(PathBuf::from)
                {
                    let filters = search_filters(&request.arguments, true)?;
                    if bool_arg(&request.arguments, "refresh_if_stale") {
                        let refresh_filters =
                            merge_filters(filters.clone(), parse_query(&query).filters);
                        self.refresh_shards_for_arguments_if_stale(
                            &index_dir,
                            &request.arguments,
                            &refresh_filters,
                        )?;
                    }
                    let mut results = self.search_shards_cached(
                        &index_dir,
                        &query,
                        limit,
                        &filters,
                        context_lines,
                    )?;
                    attach_result_read_requests(
                        &mut results,
                        "read_range",
                        read_request_args("index_dir", &index_dir),
                    );
                    attach_result_related_requests(
                        &mut results,
                        "related_files",
                        read_request_args("index_dir", &index_dir),
                        Some(&filters),
                    );
                    attach_result_related_symbol_requests(
                        &mut results,
                        "related_symbols",
                        Some(&query),
                        read_request_args("index_dir", &index_dir),
                    );
                    return Ok(serde_json::to_value(results)?);
                }
                if let Some(index_path) =
                    optional_string_arg(&request.arguments, "index").map(PathBuf::from)
                {
                    let refresh_if_stale = bool_arg(&request.arguments, "refresh_if_stale");
                    let index =
                        self.cached_index_maybe_refresh(index_path.clone(), refresh_if_stale)?;
                    let filters = search_filters(&request.arguments, true)?;
                    let mut results = index.search_filtered(&query, limit, &filters)?;
                    attach_result_query_plan_retry_requests(
                        &mut results,
                        "indexed_search_code",
                        "index",
                        &index_path,
                        &request.arguments,
                    );
                    attach_result_context(&mut results, context_lines, |path, start, lines| {
                        index.read_range(path, start, lines)
                    })?;
                    attach_result_read_requests(
                        &mut results,
                        "read_range",
                        read_request_args("index", &index_path),
                    );
                    attach_result_related_requests(
                        &mut results,
                        "related_files",
                        read_request_args("index", &index_path),
                        Some(&filters),
                    );
                    attach_result_related_symbol_requests(
                        &mut results,
                        "related_symbols",
                        Some(&query),
                        read_request_args("index", &index_path),
                    );
                    return Ok(serde_json::to_value(results)?);
                }
                if optional_string_arg(&request.arguments, "cwd").is_some() {
                    let scoped_arguments =
                        arguments_scoped_to_client_cwd_for_query(&request.arguments, &query)?;
                    if let Ok(index_dir) = self.single_cached_shard_manifest_path() {
                        let filters = search_filters(&scoped_arguments, true)?;
                        if bool_arg(&request.arguments, "refresh_if_stale") {
                            let refresh_filters =
                                merge_filters(filters.clone(), parse_query(&query).filters);
                            self.refresh_shards_for_arguments_if_stale(
                                &index_dir,
                                &scoped_arguments,
                                &refresh_filters,
                            )?;
                        }
                        let mut results = self.search_shards_cached(
                            &index_dir,
                            &query,
                            limit,
                            &filters,
                            context_lines,
                        )?;
                        attach_result_read_requests(
                            &mut results,
                            "read_range",
                            read_request_args("index_dir", &index_dir),
                        );
                        attach_result_related_requests(
                            &mut results,
                            "related_files",
                            read_request_args("index_dir", &index_dir),
                            Some(&filters),
                        );
                        attach_result_related_symbol_requests(
                            &mut results,
                            "related_symbols",
                            Some(&query),
                            read_request_args("index_dir", &index_dir),
                        );
                        return Ok(serde_json::to_value(results)?);
                    }
                    if let Ok(index_path) = self.single_cached_index_path() {
                        let refresh_if_stale = bool_arg(&request.arguments, "refresh_if_stale");
                        let index =
                            self.cached_index_maybe_refresh(index_path.clone(), refresh_if_stale)?;
                        if index_matches_client_cwd(&index, &request.arguments)? {
                            let filters = search_filters(&scoped_arguments, true)?;
                            let mut results = index.search_filtered(&query, limit, &filters)?;
                            attach_result_query_plan_retry_requests(
                                &mut results,
                                "indexed_search_code",
                                "index",
                                &index_path,
                                &scoped_arguments,
                            );
                            attach_result_context(
                                &mut results,
                                context_lines,
                                |path, start, lines| index.read_range(path, start, lines),
                            )?;
                            attach_result_read_requests(
                                &mut results,
                                "read_range",
                                read_request_args("index", &index_path),
                            );
                            attach_result_related_requests(
                                &mut results,
                                "related_files",
                                read_request_args("index", &index_path),
                                Some(&filters),
                            );
                            attach_result_related_symbol_requests(
                                &mut results,
                                "related_symbols",
                                Some(&query),
                                read_request_args("index", &index_path),
                            );
                            return Ok(serde_json::to_value(results)?);
                        }
                    }
                }
                let repo = optional_string_arg(&request.arguments, "repo")
                    .map(PathBuf::from)
                    .map(Ok)
                    .unwrap_or_else(|| live_repo_from_client_cwd(&request.arguments, "search"))?;
                let filters = search_filters(&request.arguments, false)?;
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
                    Some(&filters),
                );
                attach_result_related_symbol_requests(
                    &mut results,
                    "related_symbols",
                    Some(&query),
                    read_request_args("repo", &repo),
                );
                Ok(serde_json::to_value(results)?)
            }
            "search_auto" => {
                let query = string_arg(&request.arguments, "query")?;
                let limit = search_limit_arg(&request.arguments)?;
                let context_lines = context_lines_arg(&request.arguments)?;
                let refresh_if_stale = bool_arg(&request.arguments, "refresh_if_stale");
                let diagnose = bool_arg(&request.arguments, "diagnose");
                let retry_if_empty = bool_arg(&request.arguments, "retry_if_empty");
                let summary = bool_arg(&request.arguments, "summary");
                let result = self.search_auto(
                    &request.arguments,
                    &query,
                    limit,
                    context_lines,
                    refresh_if_stale,
                    diagnose,
                    retry_if_empty,
                    summary,
                )?;
                Ok(serde_json::to_value(result)?)
            }
            "search_auto_batch" => {
                let queries = string_array_arg(&request.arguments, "queries")?;
                let limit = search_limit_arg(&request.arguments)?;
                let context_lines = context_lines_arg(&request.arguments)?;
                let mut refresh_if_stale = bool_arg(&request.arguments, "refresh_if_stale");
                let diagnose = bool_arg(&request.arguments, "diagnose");
                let retry_if_empty = bool_arg(&request.arguments, "retry_if_empty");
                let summary = bool_arg(&request.arguments, "summary");
                let refreshed_shard_dir = if refresh_if_stale {
                    self.refresh_search_auto_batch_shards_if_stale(&request.arguments, &queries)?
                } else {
                    None
                };
                if refreshed_shard_dir.is_some() {
                    refresh_if_stale = false;
                }
                let mut batch = Vec::new();
                if let Some(index_dir) = refreshed_shard_dir {
                    for query in queries {
                        let scoped_arguments = if argument_value(&request.arguments, "index_dir")
                            .is_some()
                        {
                            request.arguments.clone()
                        } else {
                            arguments_scoped_to_client_cwd_for_query(&request.arguments, &query)?
                        };
                        batch.push(self.search_auto_shards(
                            index_dir.clone(),
                            &scoped_arguments,
                            &query,
                            limit,
                            context_lines,
                            false,
                            diagnose,
                            retry_if_empty,
                            summary,
                        )?);
                    }
                } else {
                    for query in queries {
                        batch.push(self.search_auto(
                            &request.arguments,
                            &query,
                            limit,
                            context_lines,
                            refresh_if_stale,
                            diagnose,
                            retry_if_empty,
                            summary,
                        )?);
                    }
                }
                Ok(serde_json::to_value(batch)?)
            }
            "search_batch" => {
                let queries = string_array_arg(&request.arguments, "queries")?;
                let limit = search_limit_arg(&request.arguments)?;
                let context_lines = context_lines_arg(&request.arguments)?;
                if argument_value(&request.arguments, "index").is_some()
                    && argument_value(&request.arguments, "index_dir").is_some()
                {
                    return Err(anyhow!(
                        "search_batch accepts only one of index or index_dir"
                    ));
                }
                let mut batch = Vec::new();
                if let Some(index_dir) =
                    optional_string_arg(&request.arguments, "index_dir").map(PathBuf::from)
                {
                    let filters = search_filters(&request.arguments, true)?;
                    if bool_arg(&request.arguments, "refresh_if_stale") {
                        self.refresh_shards_for_query_batch_if_stale(
                            &index_dir,
                            &request.arguments,
                            &filters,
                            &queries,
                        )?;
                    }
                    for query in queries {
                        let mut results = self.search_shards_cached(
                            &index_dir,
                            &query,
                            limit,
                            &filters,
                            context_lines,
                        )?;
                        attach_result_read_requests(
                            &mut results,
                            "read_range",
                            read_request_args("index_dir", &index_dir),
                        );
                        attach_result_related_requests(
                            &mut results,
                            "related_files",
                            read_request_args("index_dir", &index_dir),
                            Some(&filters),
                        );
                        attach_result_related_symbol_requests(
                            &mut results,
                            "related_symbols",
                            Some(&query),
                            read_request_args("index_dir", &index_dir),
                        );
                        let read_batch_request = result_read_batch_request(
                            &results,
                            "read_ranges",
                            read_request_args("index_dir", &index_dir),
                        );
                        let shard_scope_filters =
                            merge_filters(filters.clone(), parse_query(&query).filters);
                        let (query_plan_request, repo_map_request) = search_batch_followups(
                            "search_plan",
                            "repo_map",
                            "index_dir",
                            &index_dir,
                            &request.arguments,
                            &query,
                            Some(&shard_scope_filters),
                        );
                        batch.push(search_batch_result(
                            query,
                            query_plan_request,
                            repo_map_request,
                            read_batch_request,
                            results,
                        ));
                    }
                    return Ok(serde_json::to_value(batch)?);
                }
                if let Some(index_path) =
                    optional_string_arg(&request.arguments, "index").map(PathBuf::from)
                {
                    let refresh_if_stale = bool_arg(&request.arguments, "refresh_if_stale");
                    let index =
                        self.cached_index_maybe_refresh(index_path.clone(), refresh_if_stale)?;
                    let filters = search_filters(&request.arguments, true)?;
                    for query in queries {
                        let mut results = index.search_filtered(&query, limit, &filters)?;
                        attach_result_context(
                            &mut results,
                            context_lines,
                            |path, start, lines| index.read_range(path, start, lines),
                        )?;
                        attach_result_read_requests(
                            &mut results,
                            "read_range",
                            read_request_args("index", &index_path),
                        );
                        attach_result_related_requests(
                            &mut results,
                            "related_files",
                            read_request_args("index", &index_path),
                            Some(&filters),
                        );
                        attach_result_related_symbol_requests(
                            &mut results,
                            "related_symbols",
                            Some(&query),
                            read_request_args("index", &index_path),
                        );
                        let read_batch_request = result_read_batch_request(
                            &results,
                            "read_ranges",
                            read_request_args("index", &index_path),
                        );
                        let (query_plan_request, repo_map_request) = search_batch_followups(
                            "search_plan",
                            "repo_map",
                            "index",
                            &index_path,
                            &request.arguments,
                            &query,
                            None,
                        );
                        batch.push(search_batch_result(
                            query,
                            query_plan_request,
                            repo_map_request,
                            read_batch_request,
                            results,
                        ));
                    }
                    return Ok(serde_json::to_value(batch)?);
                }
                if optional_string_arg(&request.arguments, "cwd").is_some() {
                    if let Ok(index_dir) = self.single_cached_shard_manifest_path() {
                        for query in &queries {
                            let scoped_arguments = arguments_scoped_to_client_cwd_for_query(
                                &request.arguments,
                                query,
                            )?;
                            let filters = search_filters(&scoped_arguments, true)?;
                            if bool_arg(&request.arguments, "refresh_if_stale") {
                                let refresh_filters =
                                    merge_filters(filters.clone(), parse_query(query).filters);
                                self.refresh_shards_for_arguments_if_stale(
                                    &index_dir,
                                    &scoped_arguments,
                                    &refresh_filters,
                                )?;
                            }
                            let mut results = self.search_shards_cached(
                                &index_dir,
                                query,
                                limit,
                                &filters,
                                context_lines,
                            )?;
                            attach_result_read_requests(
                                &mut results,
                                "read_range",
                                read_request_args("index_dir", &index_dir),
                            );
                            attach_result_related_requests(
                                &mut results,
                                "related_files",
                                read_request_args("index_dir", &index_dir),
                                Some(&filters),
                            );
                            attach_result_related_symbol_requests(
                                &mut results,
                                "related_symbols",
                                Some(query),
                                read_request_args("index_dir", &index_dir),
                            );
                            let read_batch_request = result_read_batch_request(
                                &results,
                                "read_ranges",
                                read_request_args("index_dir", &index_dir),
                            );
                            let shard_scope_filters =
                                merge_filters(filters.clone(), parse_query(query).filters);
                            let (query_plan_request, repo_map_request) = search_batch_followups(
                                "search_plan",
                                "repo_map",
                                "index_dir",
                                &index_dir,
                                &scoped_arguments,
                                query,
                                Some(&shard_scope_filters),
                            );
                            batch.push(search_batch_result(
                                query.clone(),
                                query_plan_request,
                                repo_map_request,
                                read_batch_request,
                                results,
                            ));
                        }
                        return Ok(serde_json::to_value(batch)?);
                    }
                    if let Ok(index_path) = self.single_cached_index_path() {
                        let refresh_if_stale = bool_arg(&request.arguments, "refresh_if_stale");
                        let index =
                            self.cached_index_maybe_refresh(index_path.clone(), refresh_if_stale)?;
                        if index_matches_client_cwd(&index, &request.arguments)? {
                            for query in &queries {
                                let scoped_arguments = arguments_scoped_to_client_cwd_for_query(
                                    &request.arguments,
                                    query,
                                )?;
                                let filters = search_filters(&scoped_arguments, true)?;
                                let mut results = index.search_filtered(query, limit, &filters)?;
                                attach_result_context(
                                    &mut results,
                                    context_lines,
                                    |path, start, lines| index.read_range(path, start, lines),
                                )?;
                                attach_result_read_requests(
                                    &mut results,
                                    "read_range",
                                    read_request_args("index", &index_path),
                                );
                                attach_result_related_requests(
                                    &mut results,
                                    "related_files",
                                    read_request_args("index", &index_path),
                                    Some(&filters),
                                );
                                attach_result_related_symbol_requests(
                                    &mut results,
                                    "related_symbols",
                                    Some(query),
                                    read_request_args("index", &index_path),
                                );
                                let read_batch_request = result_read_batch_request(
                                    &results,
                                    "read_ranges",
                                    read_request_args("index", &index_path),
                                );
                                let (query_plan_request, repo_map_request) = search_batch_followups(
                                    "search_plan",
                                    "repo_map",
                                    "index",
                                    &index_path,
                                    &scoped_arguments,
                                    query,
                                    None,
                                );
                                batch.push(search_batch_result(
                                    query.clone(),
                                    query_plan_request,
                                    repo_map_request,
                                    read_batch_request,
                                    results,
                                ));
                            }
                            return Ok(serde_json::to_value(batch)?);
                        }
                    }
                }
                let repo = optional_string_arg(&request.arguments, "repo")
                    .map(PathBuf::from)
                    .map(Ok)
                    .unwrap_or_else(|| {
                        live_repo_from_client_cwd(&request.arguments, "search_batch")
                    })?;
                let filters = search_filters(&request.arguments, false)?;
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
                        Some(&filters),
                    );
                    attach_result_related_symbol_requests(
                        &mut results,
                        "related_symbols",
                        Some(&query),
                        read_request_args("repo", &repo),
                    );
                    let read_batch_request = result_read_batch_request(
                        &results,
                        "read_ranges",
                        read_request_args("repo", &repo),
                    );
                    let (query_plan_request, repo_map_request) = search_batch_followups(
                        "search_plan",
                        "repo_map",
                        "repo",
                        &repo,
                        &request.arguments,
                        &query,
                        None,
                    );
                    batch.push(search_batch_result(
                        query,
                        query_plan_request,
                        repo_map_request,
                        read_batch_request,
                        results,
                    ));
                }
                Ok(serde_json::to_value(batch)?)
            }
            "search_query_plan" => {
                let repo = path_arg(&request.arguments, "repo")?;
                let query = string_arg(&request.arguments, "query")?;
                let summary_only = bool_arg(&request.arguments, "summary");
                let index = FastIndex::build(repo)?;
                let plan = index.query_plan(&query, &search_filters(&request.arguments, false)?)?;
                query_plan_response_value(
                    attach_retry_requests(
                        plan,
                        "search_code",
                        "repo",
                        &index.root,
                        &request.arguments,
                    ),
                    summary_only,
                )
            }
            "search_plan" => {
                let query = string_arg(&request.arguments, "query")?;
                let summary_only = bool_arg(&request.arguments, "summary");
                if argument_value(&request.arguments, "index").is_some()
                    && argument_value(&request.arguments, "index_dir").is_some()
                {
                    return Err(anyhow!(
                        "search_plan accepts only one of index or index_dir"
                    ));
                }
                if let Some(index_dir) =
                    optional_string_arg(&request.arguments, "index_dir").map(PathBuf::from)
                {
                    let filters = search_filters(&request.arguments, true)?;
                    if bool_arg(&request.arguments, "refresh_if_stale") {
                        let refresh_filters =
                            merge_filters(filters.clone(), parse_query(&query).filters);
                        self.refresh_shards_for_arguments_if_stale(
                            &index_dir,
                            &request.arguments,
                            &refresh_filters,
                        )?;
                    }
                    let mut plans = self.shard_query_plans_cached(&index_dir, &query, &filters)?;
                    attach_shard_retry_requests_with_tool(
                        &mut plans,
                        "search",
                        &index_dir,
                        &request.arguments,
                    );
                    return shard_query_plan_response_value(&plans, summary_only);
                }
                if let Some(index_path) =
                    optional_string_arg(&request.arguments, "index").map(PathBuf::from)
                {
                    let refresh_if_stale = bool_arg(&request.arguments, "refresh_if_stale");
                    let index =
                        self.cached_index_maybe_refresh(index_path.clone(), refresh_if_stale)?;
                    let plan =
                        index.query_plan(&query, &search_filters(&request.arguments, true)?)?;
                    return query_plan_response_value(
                        attach_retry_requests(
                            plan,
                            "search",
                            "index",
                            index_path,
                            &request.arguments,
                        ),
                        summary_only,
                    );
                }
                if optional_string_arg(&request.arguments, "cwd").is_some() {
                    let scoped_arguments =
                        arguments_scoped_to_client_cwd_for_query(&request.arguments, &query)?;
                    if let Ok(index_dir) = self.single_cached_shard_manifest_path() {
                        let filters = search_filters(&scoped_arguments, true)?;
                        if bool_arg(&request.arguments, "refresh_if_stale") {
                            let refresh_filters =
                                merge_filters(filters.clone(), parse_query(&query).filters);
                            self.refresh_shards_for_arguments_if_stale(
                                &index_dir,
                                &scoped_arguments,
                                &refresh_filters,
                            )?;
                        }
                        let mut plans =
                            self.shard_query_plans_cached(&index_dir, &query, &filters)?;
                        attach_shard_retry_requests_with_tool(
                            &mut plans,
                            "search",
                            &index_dir,
                            &scoped_arguments,
                        );
                        return shard_query_plan_response_value(&plans, summary_only);
                    }
                    if let Ok(index_path) = self.single_cached_index_path() {
                        let refresh_if_stale = bool_arg(&request.arguments, "refresh_if_stale");
                        let index =
                            self.cached_index_maybe_refresh(index_path.clone(), refresh_if_stale)?;
                        if index_matches_client_cwd(&index, &request.arguments)? {
                            let plan = index
                                .query_plan(&query, &search_filters(&scoped_arguments, true)?)?;
                            return query_plan_response_value(
                                attach_retry_requests(
                                    plan,
                                    "search",
                                    "index",
                                    index_path,
                                    &scoped_arguments,
                                ),
                                summary_only,
                            );
                        }
                    }
                }
                let repo = optional_string_arg(&request.arguments, "repo")
                    .map(PathBuf::from)
                    .map(Ok)
                    .unwrap_or_else(|| {
                        live_repo_from_client_cwd(&request.arguments, "search_plan")
                    })?;
                let index = FastIndex::build(repo)?;
                let plan = index.query_plan(&query, &search_filters(&request.arguments, false)?)?;
                query_plan_response_value(
                    attach_retry_requests(plan, "search", "repo", &index.root, &request.arguments),
                    summary_only,
                )
            }
            "search_query_plan_batch" => {
                let repo = path_arg(&request.arguments, "repo")?;
                let queries = string_array_arg(&request.arguments, "queries")?;
                let summary_only = bool_arg(&request.arguments, "summary");
                let index = FastIndex::build(repo)?;
                let filters = search_filters(&request.arguments, false)?;
                let mut batch = Vec::new();
                for query in queries {
                    let plan = attach_retry_requests(
                        index.query_plan(&query, &filters)?,
                        "search_code",
                        "repo",
                        &index.root,
                        &request.arguments,
                    );
                    batch.push(query_plan_batch_response_value(query, plan, summary_only)?);
                }
                Ok(serde_json::to_value(batch)?)
            }
            "search_plan_batch" => {
                let queries = string_array_arg(&request.arguments, "queries")?;
                let summary_only = bool_arg(&request.arguments, "summary");
                if argument_value(&request.arguments, "index").is_some()
                    && argument_value(&request.arguments, "index_dir").is_some()
                {
                    return Err(anyhow!(
                        "search_plan_batch accepts only one of index or index_dir"
                    ));
                }
                if let Some(index_dir) =
                    optional_string_arg(&request.arguments, "index_dir").map(PathBuf::from)
                {
                    let filters = search_filters(&request.arguments, true)?;
                    if bool_arg(&request.arguments, "refresh_if_stale") {
                        self.refresh_shards_for_query_batch_if_stale(
                            &index_dir,
                            &request.arguments,
                            &filters,
                            &queries,
                        )?;
                    }
                    let mut batch = Vec::new();
                    for query in queries {
                        let mut plans =
                            self.shard_query_plans_cached(&index_dir, &query, &filters)?;
                        attach_shard_retry_requests_with_tool(
                            &mut plans,
                            "search",
                            &index_dir,
                            &request.arguments,
                        );
                        batch.push(shard_query_plan_batch_response_value(
                            query,
                            plans,
                            summary_only,
                        )?);
                    }
                    return Ok(serde_json::to_value(batch)?);
                }
                if let Some(index_path) =
                    optional_string_arg(&request.arguments, "index").map(PathBuf::from)
                {
                    let refresh_if_stale = bool_arg(&request.arguments, "refresh_if_stale");
                    let index =
                        self.cached_index_maybe_refresh(index_path.clone(), refresh_if_stale)?;
                    let filters = search_filters(&request.arguments, true)?;
                    let mut batch = Vec::new();
                    for query in queries {
                        let plan = attach_retry_requests(
                            index.query_plan(&query, &filters)?,
                            "search",
                            "index",
                            &index_path,
                            &request.arguments,
                        );
                        batch.push(query_plan_batch_response_value(query, plan, summary_only)?);
                    }
                    return Ok(serde_json::to_value(batch)?);
                }
                if optional_string_arg(&request.arguments, "cwd").is_some() {
                    if let Ok(index_dir) = self.single_cached_shard_manifest_path() {
                        let mut batch = Vec::new();
                        for query in queries {
                            let scoped_arguments = arguments_scoped_to_client_cwd_for_query(
                                &request.arguments,
                                &query,
                            )?;
                            let filters = search_filters(&scoped_arguments, true)?;
                            if bool_arg(&request.arguments, "refresh_if_stale") {
                                let refresh_filters =
                                    merge_filters(filters.clone(), parse_query(&query).filters);
                                self.refresh_shards_for_arguments_if_stale(
                                    &index_dir,
                                    &scoped_arguments,
                                    &refresh_filters,
                                )?;
                            }
                            let mut plans =
                                self.shard_query_plans_cached(&index_dir, &query, &filters)?;
                            attach_shard_retry_requests_with_tool(
                                &mut plans,
                                "search",
                                &index_dir,
                                &scoped_arguments,
                            );
                            batch.push(shard_query_plan_batch_response_value(
                                query,
                                plans,
                                summary_only,
                            )?);
                        }
                        return Ok(serde_json::to_value(batch)?);
                    }
                    if let Ok(index_path) = self.single_cached_index_path() {
                        let refresh_if_stale = bool_arg(&request.arguments, "refresh_if_stale");
                        let index =
                            self.cached_index_maybe_refresh(index_path.clone(), refresh_if_stale)?;
                        if index_matches_client_cwd(&index, &request.arguments)? {
                            let mut batch = Vec::new();
                            for query in queries {
                                let scoped_arguments = arguments_scoped_to_client_cwd_for_query(
                                    &request.arguments,
                                    &query,
                                )?;
                                let filters = search_filters(&scoped_arguments, true)?;
                                let plan = attach_retry_requests(
                                    index.query_plan(&query, &filters)?,
                                    "search",
                                    "index",
                                    &index_path,
                                    &scoped_arguments,
                                );
                                batch.push(query_plan_batch_response_value(
                                    query,
                                    plan,
                                    summary_only,
                                )?);
                            }
                            return Ok(serde_json::to_value(batch)?);
                        }
                    }
                }
                let repo = optional_string_arg(&request.arguments, "repo")
                    .map(PathBuf::from)
                    .map(Ok)
                    .unwrap_or_else(|| {
                        live_repo_from_client_cwd(&request.arguments, "search_plan_batch")
                    })?;
                let index = FastIndex::build(repo)?;
                let filters = search_filters(&request.arguments, false)?;
                let mut batch = Vec::new();
                for query in queries {
                    let plan = attach_retry_requests(
                        index.query_plan(&query, &filters)?,
                        "search",
                        "repo",
                        &index.root,
                        &request.arguments,
                    );
                    batch.push(query_plan_batch_response_value(query, plan, summary_only)?);
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
                let filters = search_filters(&request.arguments, true)?;
                let mut results = index.search_filtered(&query, limit, &filters)?;
                attach_result_query_plan_retry_requests(
                    &mut results,
                    "indexed_search_code",
                    "index",
                    &index_path,
                    &request.arguments,
                );
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
                    Some(&filters),
                );
                attach_result_related_symbol_requests(
                    &mut results,
                    "related_index_symbols",
                    Some(&query),
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
                    attach_result_query_plan_retry_requests(
                        &mut results,
                        "indexed_search_code",
                        "index",
                        &index_path,
                        &request.arguments,
                    );
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
                        Some(&filters),
                    );
                    attach_result_related_symbol_requests(
                        &mut results,
                        "related_index_symbols",
                        Some(&query),
                        read_request_args("index", &index_path),
                    );
                    let read_batch_request = result_read_batch_request(
                        &results,
                        "read_index_ranges",
                        read_request_args("index", &index_path),
                    );
                    let (query_plan_request, repo_map_request) = search_batch_followups(
                        "indexed_query_plan",
                        "indexed_repo_map",
                        "index",
                        &index_path,
                        &request.arguments,
                        &query,
                        None,
                    );
                    batch.push(search_batch_result(
                        query,
                        query_plan_request,
                        repo_map_request,
                        read_batch_request,
                        results,
                    ));
                }
                Ok(serde_json::to_value(batch)?)
            }
            "indexed_query_plan" | "index_plan" => {
                let index_path = self.index_path_arg_or_single_cached(&request.arguments)?;
                let query = string_arg(&request.arguments, "query")?;
                let summary_only = bool_arg(&request.arguments, "summary");
                let refresh_if_stale = bool_arg(&request.arguments, "refresh_if_stale");
                let index =
                    self.cached_index_maybe_refresh(index_path.clone(), refresh_if_stale)?;
                let plan = index.query_plan(&query, &search_filters(&request.arguments, true)?)?;
                query_plan_response_value(
                    attach_retry_requests(
                        plan,
                        "indexed_search_code",
                        "index",
                        index_path,
                        &request.arguments,
                    ),
                    summary_only,
                )
            }
            "indexed_query_plan_batch" => {
                let index_path = self.index_path_arg_or_single_cached(&request.arguments)?;
                let queries = string_array_arg(&request.arguments, "queries")?;
                let summary_only = bool_arg(&request.arguments, "summary");
                let refresh_if_stale = bool_arg(&request.arguments, "refresh_if_stale");
                let index =
                    self.cached_index_maybe_refresh(index_path.clone(), refresh_if_stale)?;
                let filters = search_filters(&request.arguments, true)?;
                let mut batch = Vec::new();
                for query in queries {
                    let plan = attach_retry_requests(
                        index.query_plan(&query, &filters)?,
                        "indexed_search_code",
                        "index",
                        &index_path,
                        &request.arguments,
                    );
                    batch.push(indexed_query_plan_batch_response_value(
                        query,
                        plan,
                        summary_only,
                    )?);
                }
                Ok(serde_json::to_value(batch)?)
            }
            "index_status" => {
                let index_path = self.index_path_arg_or_single_cached(&request.arguments)?;
                let index = self.cached_index(index_path.clone())?;
                Ok(serde_json::to_value(index.freshness_at(index_path)?)?)
            }
            "read_index_range" | "open_index_range" => {
                let tool_name = request.tool.as_str();
                let index_path = self.index_path_arg_or_single_cached(&request.arguments)?;
                let range = single_range_arg(&request.arguments, tool_name)?;
                let index = self.cached_index(index_path)?;
                Ok(serde_json::to_value(index.read_range_scoped(
                    &range.path,
                    range.start,
                    range.lines,
                    range.scope,
                )?)?)
            }
            "read_index_ranges" | "open_index_ranges" => {
                let index_path = self.index_path_arg_or_single_cached(&request.arguments)?;
                let ranges = range_args(&request.arguments, request.tool.as_str())?;
                let include_summary = bool_arg(&request.arguments, "include_summary");
                let index = self.cached_index(index_path)?;
                let mut results = Vec::new();
                for range in ranges {
                    results.push(index.read_range_scoped(
                        &range.path,
                        range.start,
                        range.lines,
                        range.scope,
                    )?);
                }
                read_ranges_response_value(results, include_summary)
            }
            "ensure_index" | "refresh_index" => {
                let repo = path_arg(&request.arguments, "repo")?;
                let index_path = path_arg(&request.arguments, "index")?;
                Ok(serde_json::to_value(self.refresh_index(repo, index_path)?)?)
            }
            "index_shards" => {
                let selection = shard_repos_from_arguments_required(&request.arguments)?;
                let output_dir = path_arg(&request.arguments, "output_dir")?;
                let force = bool_arg(&request.arguments, "force");
                let stats = build_shards_with_force(&selection.repos, output_dir, force)?;
                self.clear_runtime_caches()?;
                shard_bootstrap_output(stats, selection.discovery)
            }
            "ensure_shards" => {
                let selection = shard_repos_from_arguments(&request.arguments)?;
                let output_dir = path_arg(&request.arguments, "output_dir")?;
                let stats = ensure_shards(&selection.repos, &output_dir)?;
                self.clear_runtime_caches()?;
                let registered_indexes = self.register_shards(output_dir)?;
                Ok(json!({
                    "stats": shard_bootstrap_output(stats, selection.discovery)?,
                    "registered_indexes": registered_indexes,
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
                let roots = scoped_shard_status_roots(&request.arguments)?;
                if roots.is_empty() {
                    Ok(serde_json::to_value(shard_status(index_dir)?)?)
                } else {
                    Ok(serde_json::to_value(shard_status_by_root(
                        index_dir, &roots,
                    )?)?)
                }
            }
            "search_shards" => {
                let index_dir = self.shard_dir_arg_or_single_cached(&request.arguments)?;
                let query = string_arg(&request.arguments, "query")?;
                let limit = search_limit_arg(&request.arguments)?;
                let context_lines = context_lines_arg(&request.arguments)?;
                let scoped_arguments =
                    arguments_scoped_to_client_cwd_for_query(&request.arguments, &query)?;
                let filters = search_filters(&scoped_arguments, true)?;
                if bool_arg(&request.arguments, "refresh_if_stale") {
                    let refresh_filters =
                        merge_filters(filters.clone(), parse_query(&query).filters);
                    self.refresh_shards_for_arguments_if_stale(
                        &index_dir,
                        &scoped_arguments,
                        &refresh_filters,
                    )?;
                }
                Ok(serde_json::to_value(self.search_shards_cached(
                    &index_dir,
                    &query,
                    limit,
                    &filters,
                    context_lines,
                )?)?)
            }
            "search_shards_batch" => {
                let index_dir = self.shard_dir_arg_or_single_cached(&request.arguments)?;
                let queries = string_array_arg(&request.arguments, "queries")?;
                let limit = search_limit_arg(&request.arguments)?;
                let context_lines = context_lines_arg(&request.arguments)?;
                let filters = search_filters(&request.arguments, true)?;
                if bool_arg(&request.arguments, "refresh_if_stale") {
                    self.refresh_shards_for_query_batch_if_stale(
                        &index_dir,
                        &request.arguments,
                        &filters,
                        &queries,
                    )?;
                }
                let mut batch = Vec::new();
                for query in queries {
                    let scoped_arguments =
                        arguments_scoped_to_client_cwd_for_query(&request.arguments, &query)?;
                    let filters = search_filters(&scoped_arguments, true)?;
                    let results = self.search_shards_cached(
                        &index_dir,
                        &query,
                        limit,
                        &filters,
                        context_lines,
                    )?;
                    let read_batch_request = result_read_batch_request(
                        &results,
                        "read_shard_ranges",
                        read_request_args("index_dir", &index_dir),
                    );
                    let shard_scope_filters =
                        merge_filters(filters.clone(), parse_query(&query).filters);
                    let (query_plan_request, repo_map_request) = search_batch_followups(
                        "shard_query_plan",
                        "shard_repo_map",
                        "index_dir",
                        &index_dir,
                        &scoped_arguments,
                        &query,
                        Some(&shard_scope_filters),
                    );
                    batch.push(search_batch_result(
                        query,
                        query_plan_request,
                        repo_map_request,
                        read_batch_request,
                        results,
                    ));
                }
                Ok(serde_json::to_value(batch)?)
            }
            "shard_query_plan" | "shard_plan" => {
                let index_dir = self.shard_dir_arg_or_single_cached(&request.arguments)?;
                let query = string_arg(&request.arguments, "query")?;
                let summary_only = bool_arg(&request.arguments, "summary");
                let scoped_arguments =
                    arguments_scoped_to_client_cwd_for_query(&request.arguments, &query)?;
                let filters = search_filters(&scoped_arguments, true)?;
                if bool_arg(&request.arguments, "refresh_if_stale") {
                    let refresh_filters =
                        merge_filters(filters.clone(), parse_query(&query).filters);
                    self.refresh_shards_for_arguments_if_stale(
                        &index_dir,
                        &scoped_arguments,
                        &refresh_filters,
                    )?;
                }
                let mut plans = self.shard_query_plans_cached(&index_dir, &query, &filters)?;
                attach_shard_retry_requests(&mut plans, &index_dir, &scoped_arguments);
                shard_query_plan_response_value(&plans, summary_only)
            }
            "shard_query_plan_batch" => {
                let index_dir = self.shard_dir_arg_or_single_cached(&request.arguments)?;
                let queries = string_array_arg(&request.arguments, "queries")?;
                let summary_only = bool_arg(&request.arguments, "summary");
                let mut batch = Vec::new();
                for query in queries {
                    let scoped_arguments =
                        arguments_scoped_to_client_cwd_for_query(&request.arguments, &query)?;
                    let filters = search_filters(&scoped_arguments, true)?;
                    if bool_arg(&request.arguments, "refresh_if_stale") {
                        let refresh_filters =
                            merge_filters(filters.clone(), parse_query(&query).filters);
                        self.refresh_shards_for_arguments_if_stale(
                            &index_dir,
                            &scoped_arguments,
                            &refresh_filters,
                        )?;
                    }
                    let mut plans = self.shard_query_plans_cached(&index_dir, &query, &filters)?;
                    attach_shard_retry_requests(&mut plans, &index_dir, &scoped_arguments);
                    batch.push(shard_query_plan_batch_response_value(
                        query,
                        plans,
                        summary_only,
                    )?);
                }
                Ok(serde_json::to_value(batch)?)
            }
            "read_shard_range" | "open_shard_range" => {
                let tool_name = request.tool.as_str();
                let index_dir = self.shard_dir_arg_or_single_cached(&request.arguments)?;
                let range = single_range_arg(&request.arguments, tool_name)?;
                Ok(serde_json::to_value(self.read_shard_range_cached_scoped(
                    &index_dir,
                    &range.path,
                    range.start,
                    range.lines,
                    range.scope,
                )?)?)
            }
            "read_shard_ranges" | "open_shard_ranges" => {
                let index_dir = self.shard_dir_arg_or_single_cached(&request.arguments)?;
                let ranges = range_args(&request.arguments, request.tool.as_str())?;
                let include_summary = bool_arg(&request.arguments, "include_summary");
                let mut results = Vec::new();
                for range in ranges {
                    results.push(self.read_shard_range_cached_scoped(
                        &index_dir,
                        &range.path,
                        range.start,
                        range.lines,
                        range.scope,
                    )?);
                }
                read_ranges_response_value(results, include_summary)
            }
            "shard_repo_map" => {
                let index_dir = self.shard_dir_arg_or_single_cached(&request.arguments)?;
                let symbol_limit = positive_usize_arg(&request.arguments, "symbols", 50)?;
                let test_limit = positive_usize_arg(&request.arguments, "tests", 50)?;
                let detail = repo_map_detail_arg(&request.arguments)?;
                let read_limit = repo_map_read_limit_arg(&request.arguments)?;
                let scoped_arguments = arguments_scoped_to_client_cwd(&request.arguments)?;
                let filters = search_filters(&scoped_arguments, true)?;
                if bool_arg(&request.arguments, "refresh_if_stale") {
                    self.refresh_shards_for_arguments_if_stale(
                        &index_dir,
                        &scoped_arguments,
                        &filters,
                    )?;
                }
                Ok(serde_json::to_value(self.shard_repo_maps_cached(
                    &index_dir,
                    symbol_limit,
                    test_limit,
                    detail,
                    read_limit,
                    &filters,
                    "read_shard_ranges",
                )?)?)
            }
            "find_shard_symbol" => {
                let index_dir = self.shard_dir_arg_or_single_cached(&request.arguments)?;
                let name = string_arg(&request.arguments, "name")?;
                let limit = positive_usize_arg(&request.arguments, "limit", 10)?;
                let include_read_batch = bool_arg(&request.arguments, "include_read_batch");
                let symbols = self.find_shard_symbol_cached(
                    &index_dir,
                    &name,
                    limit,
                    &search_filters(&request.arguments, true)?,
                )?;
                let base_args = read_request_args("index_dir", &index_dir);
                let symbols = symbol_lookup_results(symbols, "read_shard_range", base_args.clone());
                symbol_lookup_response(symbols, include_read_batch, "read_shard_ranges", base_args)
            }
            "find_shard_symbol_batch" => {
                let index_dir = self.shard_dir_arg_or_single_cached(&request.arguments)?;
                let names = string_array_arg(&request.arguments, "names")?;
                let limit = positive_usize_arg(&request.arguments, "limit", 10)?;
                let filters = search_filters(&request.arguments, true)?;
                let mut batch = Vec::new();
                for name in names {
                    let symbols =
                        self.find_shard_symbol_cached(&index_dir, &name, limit, &filters)?;
                    let symbols = symbol_lookup_results(
                        symbols,
                        "read_shard_range",
                        read_request_args("index_dir", &index_dir),
                    );
                    let read_batch_request = symbol_lookup_read_batch_request(
                        &symbols,
                        "read_shard_ranges",
                        read_request_args("index_dir", &index_dir),
                    );
                    batch.push(symbol_batch_result(name, read_batch_request, symbols));
                }
                Ok(serde_json::to_value(batch)?)
            }
            "find_symbol" => {
                let name = string_arg(&request.arguments, "name")?;
                let limit = positive_usize_arg(&request.arguments, "limit", 10)?;
                let include_read_batch = bool_arg(&request.arguments, "include_read_batch");
                if argument_value(&request.arguments, "index").is_some()
                    && argument_value(&request.arguments, "index_dir").is_some()
                {
                    return Err(anyhow!(
                        "find_symbol accepts only one of index or index_dir"
                    ));
                }
                if let Some(index_dir) =
                    optional_string_arg(&request.arguments, "index_dir").map(PathBuf::from)
                {
                    let symbols = self.find_shard_symbol_cached(
                        &index_dir,
                        &name,
                        limit,
                        &search_filters(&request.arguments, true)?,
                    )?;
                    let base_args = read_request_args("index_dir", &index_dir);
                    let symbols = symbol_lookup_results(symbols, "read_range", base_args.clone());
                    return symbol_lookup_response(
                        symbols,
                        include_read_batch,
                        "read_ranges",
                        base_args,
                    );
                }
                if let Some(index_path) =
                    optional_string_arg(&request.arguments, "index").map(PathBuf::from)
                {
                    let filters = search_filters(&request.arguments, true)?;
                    let index = self.cached_index(index_path.clone())?;
                    let symbols = index.find_symbol_filtered(&name, limit, &filters);
                    let base_args = read_request_args("index", &index_path);
                    let symbols = symbol_lookup_results(symbols, "read_range", base_args.clone());
                    return symbol_lookup_response(
                        symbols,
                        include_read_batch,
                        "read_ranges",
                        base_args,
                    );
                }
                if optional_string_arg(&request.arguments, "cwd").is_some() {
                    let scoped_arguments = arguments_scoped_to_client_cwd(&request.arguments)?;
                    if let Ok(index_dir) = self.single_cached_shard_manifest_path() {
                        let symbols = self.find_shard_symbol_cached(
                            &index_dir,
                            &name,
                            limit,
                            &search_filters(&scoped_arguments, true)?,
                        )?;
                        let base_args = read_request_args("index_dir", &index_dir);
                        let symbols =
                            symbol_lookup_results(symbols, "read_range", base_args.clone());
                        return symbol_lookup_response(
                            symbols,
                            include_read_batch,
                            "read_ranges",
                            base_args,
                        );
                    }
                    if let Ok(index_path) = self.single_cached_index_path() {
                        let index = self.cached_index(index_path.clone())?;
                        if index_matches_client_cwd(&index, &request.arguments)? {
                            let filters = search_filters(&scoped_arguments, true)?;
                            let symbols = index.find_symbol_filtered(&name, limit, &filters);
                            let base_args = read_request_args("index", &index_path);
                            let symbols =
                                symbol_lookup_results(symbols, "read_range", base_args.clone());
                            return symbol_lookup_response(
                                symbols,
                                include_read_batch,
                                "read_ranges",
                                base_args,
                            );
                        }
                    }
                }
                let repo = optional_string_arg(&request.arguments, "repo")
                    .map(PathBuf::from)
                    .map(Ok)
                    .unwrap_or_else(|| {
                        live_repo_from_client_cwd(&request.arguments, "find_symbol")
                    })?;
                let filters = search_filters(&request.arguments, false)?;
                let index = RepoIndexer::new(&repo).build()?;
                let symbols = index.find_symbol_filtered(&name, limit, &filters);
                let base_args = read_request_args("repo", &repo);
                let symbols = symbol_lookup_results(symbols, "read_range", base_args.clone());
                symbol_lookup_response(symbols, include_read_batch, "read_ranges", base_args)
            }
            "find_symbol_batch" => {
                let names = string_array_arg(&request.arguments, "names")?;
                let limit = positive_usize_arg(&request.arguments, "limit", 10)?;
                if argument_value(&request.arguments, "index").is_some()
                    && argument_value(&request.arguments, "index_dir").is_some()
                {
                    return Err(anyhow!(
                        "find_symbol_batch accepts only one of index or index_dir"
                    ));
                }
                if let Some(index_dir) =
                    optional_string_arg(&request.arguments, "index_dir").map(PathBuf::from)
                {
                    let filters = search_filters(&request.arguments, true)?;
                    let mut batch = Vec::new();
                    for name in names {
                        let symbols =
                            self.find_shard_symbol_cached(&index_dir, &name, limit, &filters)?;
                        let symbols = symbol_lookup_results(
                            symbols,
                            "read_range",
                            read_request_args("index_dir", &index_dir),
                        );
                        let read_batch_request = symbol_lookup_read_batch_request(
                            &symbols,
                            "read_ranges",
                            read_request_args("index_dir", &index_dir),
                        );
                        batch.push(symbol_batch_result(name, read_batch_request, symbols));
                    }
                    return Ok(serde_json::to_value(batch)?);
                }
                if let Some(index_path) =
                    optional_string_arg(&request.arguments, "index").map(PathBuf::from)
                {
                    let filters = search_filters(&request.arguments, true)?;
                    let index = self.cached_index(index_path.clone())?;
                    let batch = names
                        .into_iter()
                        .map(|name| {
                            let symbols = symbol_lookup_results(
                                index.find_symbol_filtered(&name, limit, &filters),
                                "read_range",
                                read_request_args("index", &index_path),
                            );
                            let read_batch_request = symbol_lookup_read_batch_request(
                                &symbols,
                                "read_ranges",
                                read_request_args("index", &index_path),
                            );
                            symbol_batch_result(name, read_batch_request, symbols)
                        })
                        .collect::<Vec<_>>();
                    return Ok(serde_json::to_value(batch)?);
                }
                if optional_string_arg(&request.arguments, "cwd").is_some() {
                    let scoped_arguments = arguments_scoped_to_client_cwd(&request.arguments)?;
                    if let Ok(index_dir) = self.single_cached_shard_manifest_path() {
                        let filters = search_filters(&scoped_arguments, true)?;
                        let mut batch = Vec::new();
                        for name in names {
                            let symbols =
                                self.find_shard_symbol_cached(&index_dir, &name, limit, &filters)?;
                            let symbols = symbol_lookup_results(
                                symbols,
                                "read_range",
                                read_request_args("index_dir", &index_dir),
                            );
                            let read_batch_request = symbol_lookup_read_batch_request(
                                &symbols,
                                "read_ranges",
                                read_request_args("index_dir", &index_dir),
                            );
                            batch.push(symbol_batch_result(name, read_batch_request, symbols));
                        }
                        return Ok(serde_json::to_value(batch)?);
                    }
                    if let Ok(index_path) = self.single_cached_index_path() {
                        let index = self.cached_index(index_path.clone())?;
                        if index_matches_client_cwd(&index, &request.arguments)? {
                            let filters = search_filters(&scoped_arguments, true)?;
                            let batch = names
                                .into_iter()
                                .map(|name| {
                                    let symbols = symbol_lookup_results(
                                        index.find_symbol_filtered(&name, limit, &filters),
                                        "read_range",
                                        read_request_args("index", &index_path),
                                    );
                                    let read_batch_request = symbol_lookup_read_batch_request(
                                        &symbols,
                                        "read_ranges",
                                        read_request_args("index", &index_path),
                                    );
                                    symbol_batch_result(name, read_batch_request, symbols)
                                })
                                .collect::<Vec<_>>();
                            return Ok(serde_json::to_value(batch)?);
                        }
                    }
                }
                let repo = optional_string_arg(&request.arguments, "repo")
                    .map(PathBuf::from)
                    .map(Ok)
                    .unwrap_or_else(|| {
                        live_repo_from_client_cwd(&request.arguments, "find_symbol_batch")
                    })?;
                let filters = search_filters(&request.arguments, false)?;
                let index = RepoIndexer::new(&repo).build()?;
                let batch = names
                    .into_iter()
                    .map(|name| {
                        let symbols = symbol_lookup_results(
                            index.find_symbol_filtered(&name, limit, &filters),
                            "read_range",
                            read_request_args("repo", &repo),
                        );
                        let read_batch_request = symbol_lookup_read_batch_request(
                            &symbols,
                            "read_ranges",
                            read_request_args("repo", &repo),
                        );
                        symbol_batch_result(name, read_batch_request, symbols)
                    })
                    .collect::<Vec<_>>();
                Ok(serde_json::to_value(batch)?)
            }
            "find_index_symbol" => {
                let index_path = self.index_path_arg_or_single_cached(&request.arguments)?;
                let name = string_arg(&request.arguments, "name")?;
                let limit = positive_usize_arg(&request.arguments, "limit", 10)?;
                let include_read_batch = bool_arg(&request.arguments, "include_read_batch");
                let filters = search_filters(&request.arguments, true)?;
                let index = self.cached_index(index_path.clone())?;
                let symbols = index.find_symbol_filtered(&name, limit, &filters);
                let base_args = read_request_args("index", &index_path);
                let symbols = symbol_lookup_results(symbols, "read_index_range", base_args.clone());
                symbol_lookup_response(symbols, include_read_batch, "read_index_ranges", base_args)
            }
            "find_index_symbol_batch" => {
                let index_path = self.index_path_arg_or_single_cached(&request.arguments)?;
                let names = string_array_arg(&request.arguments, "names")?;
                let limit = positive_usize_arg(&request.arguments, "limit", 10)?;
                let filters = search_filters(&request.arguments, true)?;
                let index = self.cached_index(index_path.clone())?;
                let batch = names
                    .into_iter()
                    .map(|name| {
                        let symbols = symbol_lookup_results(
                            index.find_symbol_filtered(&name, limit, &filters),
                            "read_index_range",
                            read_request_args("index", &index_path),
                        );
                        let read_batch_request = symbol_lookup_read_batch_request(
                            &symbols,
                            "read_index_ranges",
                            read_request_args("index", &index_path),
                        );
                        symbol_batch_result(name, read_batch_request, symbols)
                    })
                    .collect::<Vec<_>>();
                Ok(serde_json::to_value(batch)?)
            }
            "related_files" => {
                let path = string_arg(&request.arguments, "path")?;
                let limit = positive_usize_arg(&request.arguments, "limit", 10)?;
                let include_read_batch = bool_arg(&request.arguments, "include_read_batch");
                if argument_value(&request.arguments, "index").is_some()
                    && argument_value(&request.arguments, "index_dir").is_some()
                {
                    return Err(anyhow!(
                        "related_files accepts only one of index or index_dir"
                    ));
                }
                if let Some(index_dir) =
                    optional_string_arg(&request.arguments, "index_dir").map(PathBuf::from)
                {
                    let filters = related_file_filters(&request.arguments, true)?;
                    let related =
                        self.related_shard_files_cached(&index_dir, &path, limit, &filters)?;
                    let base_args = read_request_args("index_dir", &index_dir);
                    let results =
                        related_file_lookup_results(related, "read_range", base_args.clone());
                    return related_lookup_response(
                        results,
                        include_read_batch,
                        "read_ranges",
                        base_args,
                        "Read the related files in one bounded batch.",
                    );
                }
                if let Some(index_path) =
                    optional_string_arg(&request.arguments, "index").map(PathBuf::from)
                {
                    let filters = related_file_filters(&request.arguments, true)?;
                    let index = self.cached_index(index_path.clone())?;
                    let related = index.related_files_filtered(&path, limit, &filters);
                    let base_args = read_request_args("index", &index_path);
                    let results =
                        related_file_lookup_results(related, "read_range", base_args.clone());
                    return related_lookup_response(
                        results,
                        include_read_batch,
                        "read_ranges",
                        base_args,
                        "Read the related files in one bounded batch.",
                    );
                }
                if optional_string_arg(&request.arguments, "cwd").is_some() {
                    if let Ok(index_dir) = self.single_cached_shard_manifest_path() {
                        if let Some(scoped_path) = self.shard_output_path_for_client_cwd(
                            &index_dir,
                            &request.arguments,
                            &path,
                            "related_files",
                        )? {
                            let filters = related_file_filters(&request.arguments, true)?;
                            let related = self.related_shard_files_cached(
                                &index_dir,
                                &scoped_path,
                                limit,
                                &filters,
                            )?;
                            let base_args = read_request_args("index_dir", &index_dir);
                            let results = related_file_lookup_results(
                                related,
                                "read_range",
                                base_args.clone(),
                            );
                            return related_lookup_response(
                                results,
                                include_read_batch,
                                "read_ranges",
                                base_args,
                                "Read the related files in one bounded batch.",
                            );
                        }
                    }
                    if let Ok(index_path) = self.single_cached_index_path() {
                        let index = self.cached_index(index_path.clone())?;
                        if index_matches_client_cwd(&index, &request.arguments)? {
                            let filters = related_file_filters(&request.arguments, true)?;
                            let related = index.related_files_filtered(&path, limit, &filters);
                            let base_args = read_request_args("index", &index_path);
                            let results = related_file_lookup_results(
                                related,
                                "read_range",
                                base_args.clone(),
                            );
                            return related_lookup_response(
                                results,
                                include_read_batch,
                                "read_ranges",
                                base_args,
                                "Read the related files in one bounded batch.",
                            );
                        }
                    }
                }
                let repo = optional_string_arg(&request.arguments, "repo")
                    .map(PathBuf::from)
                    .map(Ok)
                    .unwrap_or_else(|| {
                        live_repo_from_client_cwd(&request.arguments, "related_files")
                    })?;
                let filters = related_file_filters(&request.arguments, false)?;
                let index = RepoIndexer::new(&repo).build()?;
                let related = index.related_files_filtered(&path, limit, &filters);
                let base_args = read_request_args("repo", &repo);
                let results = related_file_lookup_results(related, "read_range", base_args.clone());
                related_lookup_response(
                    results,
                    include_read_batch,
                    "read_ranges",
                    base_args,
                    "Read the related files in one bounded batch.",
                )
            }
            "related_index_files" => {
                let index_path = self.index_path_arg_or_single_cached(&request.arguments)?;
                let path = string_arg(&request.arguments, "path")?;
                let limit = positive_usize_arg(&request.arguments, "limit", 10)?;
                let include_read_batch = bool_arg(&request.arguments, "include_read_batch");
                let filters = related_file_filters(&request.arguments, true)?;
                let index = self.cached_index(index_path.clone())?;
                let related = index.related_files_filtered(&path, limit, &filters);
                let base_args = read_request_args("index", &index_path);
                let results =
                    related_file_lookup_results(related, "read_index_range", base_args.clone());
                related_lookup_response(
                    results,
                    include_read_batch,
                    "read_index_ranges",
                    base_args,
                    "Read the related files in one bounded batch.",
                )
            }
            "related_shard_files" => {
                let index_dir = self.shard_dir_arg_or_single_cached(&request.arguments)?;
                let path = string_arg(&request.arguments, "path")?;
                let limit = positive_usize_arg(&request.arguments, "limit", 10)?;
                let include_read_batch = bool_arg(&request.arguments, "include_read_batch");
                let filters = related_file_filters(&request.arguments, true)?;
                let related =
                    self.related_shard_files_cached(&index_dir, &path, limit, &filters)?;
                let base_args = read_request_args("index_dir", &index_dir);
                let results =
                    related_file_lookup_results(related, "read_shard_range", base_args.clone());
                related_lookup_response(
                    results,
                    include_read_batch,
                    "read_shard_ranges",
                    base_args,
                    "Read the related files in one bounded batch.",
                )
            }
            "related_symbols" => {
                let path = optional_string_arg(&request.arguments, "path");
                let query = optional_string_arg(&request.arguments, "query");
                let limit = positive_usize_arg(&request.arguments, "limit", 10)?;
                let include_read_batch = bool_arg(&request.arguments, "include_read_batch");
                if argument_value(&request.arguments, "index").is_some()
                    && argument_value(&request.arguments, "index_dir").is_some()
                {
                    return Err(anyhow!(
                        "related_symbols accepts only one of index or index_dir"
                    ));
                }
                if let Some(index_dir) =
                    optional_string_arg(&request.arguments, "index_dir").map(PathBuf::from)
                {
                    let path = path
                        .as_deref()
                        .ok_or_else(|| anyhow!("path is required for shard related_symbols"))?;
                    let filters = related_symbol_filters(&request.arguments, true)?;
                    let related = self.related_shard_symbols_cached(
                        &index_dir,
                        path,
                        query.as_deref(),
                        limit,
                        &filters,
                    )?;
                    let base_args = read_request_args("index_dir", &index_dir);
                    let results =
                        related_symbol_lookup_results(related, "read_range", base_args.clone());
                    return related_lookup_response(
                        results,
                        include_read_batch,
                        "read_ranges",
                        base_args,
                        "Read the related symbol definitions in one bounded batch.",
                    );
                }
                if let Some(index_path) =
                    optional_string_arg(&request.arguments, "index").map(PathBuf::from)
                {
                    let filters = related_symbol_filters(&request.arguments, true)?;
                    let index = self.cached_index(index_path.clone())?;
                    let related = index.related_symbols_filtered(
                        path.as_deref(),
                        query.as_deref(),
                        limit,
                        &filters,
                    );
                    let base_args = read_request_args("index", &index_path);
                    let results =
                        related_symbol_lookup_results(related, "read_range", base_args.clone());
                    return related_lookup_response(
                        results,
                        include_read_batch,
                        "read_ranges",
                        base_args,
                        "Read the related symbol definitions in one bounded batch.",
                    );
                }
                if optional_string_arg(&request.arguments, "cwd").is_some() {
                    if let Ok(index_dir) = self.single_cached_shard_manifest_path() {
                        let filters = related_symbol_filters(&request.arguments, true)?;
                        if let Some(related) = self.related_shard_symbols_for_client_cwd(
                            &index_dir,
                            &request.arguments,
                            path.as_deref(),
                            query.as_deref(),
                            limit,
                            &filters,
                            "related_symbols",
                        )? {
                            let base_args = read_request_args("index_dir", &index_dir);
                            let results = related_symbol_lookup_results(
                                related,
                                "read_range",
                                base_args.clone(),
                            );
                            return related_lookup_response(
                                results,
                                include_read_batch,
                                "read_ranges",
                                base_args,
                                "Read the related symbol definitions in one bounded batch.",
                            );
                        }
                    }
                    if let Ok(index_path) = self.single_cached_index_path() {
                        let index = self.cached_index(index_path.clone())?;
                        if index_matches_client_cwd(&index, &request.arguments)? {
                            let filters = related_symbol_filters(&request.arguments, true)?;
                            let related = index.related_symbols_filtered(
                                path.as_deref(),
                                query.as_deref(),
                                limit,
                                &filters,
                            );
                            let base_args = read_request_args("index", &index_path);
                            let results = related_symbol_lookup_results(
                                related,
                                "read_range",
                                base_args.clone(),
                            );
                            return related_lookup_response(
                                results,
                                include_read_batch,
                                "read_ranges",
                                base_args,
                                "Read the related symbol definitions in one bounded batch.",
                            );
                        }
                    }
                }
                let repo = optional_string_arg(&request.arguments, "repo")
                    .map(PathBuf::from)
                    .map(Ok)
                    .unwrap_or_else(|| {
                        live_repo_from_client_cwd(&request.arguments, "related_symbols")
                    })?;
                let filters = related_symbol_filters(&request.arguments, false)?;
                let index = RepoIndexer::new(&repo).build()?;
                let related = index.related_symbols_filtered(
                    path.as_deref(),
                    query.as_deref(),
                    limit,
                    &filters,
                );
                let base_args = read_request_args("repo", &repo);
                let results =
                    related_symbol_lookup_results(related, "read_range", base_args.clone());
                related_lookup_response(
                    results,
                    include_read_batch,
                    "read_ranges",
                    base_args,
                    "Read the related symbol definitions in one bounded batch.",
                )
            }
            "related_shard_symbols" => {
                let index_dir = self.shard_dir_arg_or_single_cached(&request.arguments)?;
                let path = string_arg(&request.arguments, "path")?;
                let query = optional_string_arg(&request.arguments, "query");
                let limit = positive_usize_arg(&request.arguments, "limit", 10)?;
                let include_read_batch = bool_arg(&request.arguments, "include_read_batch");
                let filters = related_symbol_filters(&request.arguments, true)?;
                let related = self.related_shard_symbols_cached(
                    &index_dir,
                    &path,
                    query.as_deref(),
                    limit,
                    &filters,
                )?;
                let base_args = read_request_args("index_dir", &index_dir);
                let results =
                    related_symbol_lookup_results(related, "read_shard_range", base_args.clone());
                related_lookup_response(
                    results,
                    include_read_batch,
                    "read_shard_ranges",
                    base_args,
                    "Read the related symbol definitions in one bounded batch.",
                )
            }
            "related_index_symbols" => {
                let index_path = self.index_path_arg_or_single_cached(&request.arguments)?;
                let path = optional_string_arg(&request.arguments, "path");
                let query = optional_string_arg(&request.arguments, "query");
                let limit = positive_usize_arg(&request.arguments, "limit", 10)?;
                let include_read_batch = bool_arg(&request.arguments, "include_read_batch");
                let filters = related_symbol_filters(&request.arguments, true)?;
                let index = self.cached_index(index_path.clone())?;
                let related = index.related_symbols_filtered(
                    path.as_deref(),
                    query.as_deref(),
                    limit,
                    &filters,
                );
                let base_args = read_request_args("index", &index_path);
                let results =
                    related_symbol_lookup_results(related, "read_index_range", base_args.clone());
                related_lookup_response(
                    results,
                    include_read_batch,
                    "read_index_ranges",
                    base_args,
                    "Read the related symbol definitions in one bounded batch.",
                )
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
            "register_shards" => {
                let index_dir = path_arg(&request.arguments, "index_dir")?;
                let index_dir = canonical_cache_key(&index_dir);
                let registered_indexes = self.register_shards(index_dir.clone())?;
                Ok(json!({
                    "cached_indexes": self.cached_index_count(),
                    "registered_indexes": registered_indexes,
                    "registered_shards": self.shard_manifest_detail(&index_dir)
                }))
            }
            "daemon_status" => Ok(self.daemon_status_for_arguments(&request.arguments)),
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
                "index_dir is required unless exactly one shard directory is registered in the daemon"
            )),
            _ => Err(anyhow!(
                "index_dir is required because multiple shard directories are registered in the daemon: {}",
                join_paths_for_error(&paths)
            )),
        }
    }

    fn search_auto_default_status(&self) -> Value {
        if let Ok(index_dir) = self.single_cached_shard_manifest_path() {
            return json!({
                "surface": "shards",
                "source": "single_registered_shard_dir",
                "target": index_dir.to_string_lossy()
            });
        }
        if let Some(index_dir) = self.single_unregistered_warmed_shard_dir() {
            return json!({
                "surface": "shards",
                "source": "single_warmed_shard_dir",
                "target": index_dir.to_string_lossy()
            });
        }
        if let Ok(index) = self.single_cached_index_path() {
            return json!({
                "surface": "indexed",
                "source": "single_warmed_index",
                "target": index.to_string_lossy()
            });
        }
        match std::env::current_dir() {
            Ok(repo) => json!({
                "surface": "fallback",
                "source": "process_current_dir",
                "target": repo.to_string_lossy()
            }),
            Err(error) => json!({
                "surface": "fallback",
                "source": "process_current_dir",
                "target": Value::Null,
                "error": error.to_string()
            }),
        }
    }

    fn single_unregistered_warmed_shard_dir(&self) -> Option<PathBuf> {
        let dirs = self.unregistered_warmed_shard_dirs();
        match dirs.as_slice() {
            [(index_dir, _)] => Some(index_dir.clone()),
            _ => None,
        }
    }

    fn unregistered_warmed_shard_dirs(&self) -> Vec<(PathBuf, PathBuf)> {
        let registered_dirs = self
            .shard_manifests
            .lock()
            .map(|manifests| manifests.keys().cloned().collect::<HashSet<_>>())
            .unwrap_or_default();
        let mut warmed_index_paths = self
            .indexes
            .lock()
            .map(|indexes| {
                indexes
                    .iter()
                    .filter_map(|(path, entry)| entry.is_ready().then(|| path.clone()))
                    .collect::<Vec<_>>()
            })
            .unwrap_or_default();
        warmed_index_paths.sort();

        let mut seen_dirs = HashSet::default();
        let mut dirs = Vec::new();
        for index_path in warmed_index_paths {
            let Some(parent) = index_path.parent() else {
                continue;
            };
            let index_dir = canonical_cache_key(parent);
            if registered_dirs.contains(&index_dir) || !seen_dirs.insert(index_dir.clone()) {
                continue;
            }
            if index_dir.join("manifest.json").is_file() {
                dirs.push((index_dir, index_path));
            }
        }
        dirs
    }

    fn daemon_repair_requests(&self) -> Vec<Value> {
        let mut requests = Vec::new();
        for (index_dir, index_path) in self.unregistered_warmed_shard_dirs() {
            requests.push(json!({
                "kind": "register_warmed_shard_dir",
                "summary": "A warmed index belongs to a shard directory whose manifest is not registered; run this request so no-target search_auto uses shard routing instead of a single warmed index.",
                "index_dir": index_dir.to_string_lossy(),
                "warmed_index": index_path.to_string_lossy(),
                "request": daemon_default_request(
                    "register-shards",
                    "register_shards",
                    json!({ "index_dir": index_dir })
                )
            }));
        }
        requests
    }

    fn search_auto(
        &self,
        arguments: &Value,
        query: &str,
        limit: usize,
        context_lines: usize,
        refresh_if_stale: bool,
        diagnose: bool,
        retry_if_empty: bool,
        summary: bool,
    ) -> Result<SearchAutoResult> {
        if let Some(index_dir) = optional_string_arg(arguments, "index_dir").map(PathBuf::from) {
            return self.search_auto_shards(
                index_dir,
                arguments,
                query,
                limit,
                context_lines,
                refresh_if_stale,
                diagnose,
                retry_if_empty,
                summary,
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
                diagnose,
                retry_if_empty,
                summary,
            );
        }
        if let Some(repo) = optional_string_arg(arguments, "repo").map(PathBuf::from) {
            return self.search_auto_live(
                repo,
                arguments,
                query,
                limit,
                context_lines,
                diagnose,
                retry_if_empty,
                summary,
            );
        }
        let scoped_arguments = arguments_scoped_to_client_cwd_for_query(arguments, query)?;
        if let Ok(index_dir) = self.single_cached_shard_manifest_path() {
            if self.search_auto_shards_match_client_scope(
                &index_dir,
                arguments,
                &scoped_arguments,
                query,
            )? {
                return self.search_auto_shards(
                    index_dir,
                    &scoped_arguments,
                    query,
                    limit,
                    context_lines,
                    refresh_if_stale,
                    diagnose,
                    retry_if_empty,
                    summary,
                );
            }
        }
        if let Some(index_dir) = self.single_unregistered_warmed_shard_dir() {
            if self.search_auto_shards_match_client_scope(
                &index_dir,
                arguments,
                &scoped_arguments,
                query,
            )? {
                return self.search_auto_shards(
                    index_dir,
                    &scoped_arguments,
                    query,
                    limit,
                    context_lines,
                    refresh_if_stale,
                    diagnose,
                    retry_if_empty,
                    summary,
                );
            }
        }
        if let Ok(index_path) = self.single_cached_index_path() {
            return self.search_auto_index(
                index_path,
                &scoped_arguments,
                query,
                limit,
                context_lines,
                refresh_if_stale,
                diagnose,
                retry_if_empty,
                summary,
            );
        }
        let repo = live_repo_from_client_cwd(arguments, "search_auto")?;
        self.search_auto_live(
            repo,
            &scoped_arguments,
            query,
            limit,
            context_lines,
            diagnose,
            retry_if_empty,
            summary,
        )
    }

    fn search_auto_shards_match_client_scope(
        &self,
        index_dir: &Path,
        arguments: &Value,
        scoped_arguments: &Value,
        query: &str,
    ) -> Result<bool> {
        if optional_string_arg(arguments, "cwd").is_none()
            || optional_string_arg(scoped_arguments, "repo_filter").is_none()
        {
            return Ok(true);
        }
        let filters = merge_filters(
            search_filters(scoped_arguments, true)?,
            parse_query(query).filters,
        );
        let manifest = self.cached_shard_manifest(index_dir)?;
        Ok(manifest
            .shards
            .iter()
            .any(|shard| !shard_search_scopes(shard, &filters).is_empty()))
    }

    fn refresh_search_auto_batch_shards_if_stale(
        &self,
        arguments: &Value,
        queries: &[String],
    ) -> Result<Option<PathBuf>> {
        if let Some(index_dir) = optional_string_arg(arguments, "index_dir").map(PathBuf::from) {
            let filters = search_filters(arguments, true)?;
            self.refresh_shards_for_query_batch_if_stale(&index_dir, arguments, &filters, queries)?;
            return Ok(Some(index_dir));
        }
        if optional_string_arg(arguments, "index").is_some()
            || optional_string_arg(arguments, "repo").is_some()
        {
            return Ok(None);
        }
        let index_dir = match self.single_cached_shard_manifest_path() {
            Ok(index_dir) => index_dir,
            Err(_) => {
                let Some(index_dir) = self.single_unregistered_warmed_shard_dir() else {
                    return Ok(None);
                };
                index_dir
            }
        };
        let filters = search_filters(arguments, true)?;
        self.refresh_shards_for_query_batch_if_stale(&index_dir, arguments, &filters, queries)?;
        Ok(Some(index_dir))
    }

    fn search_auto_primary_retry_result(
        &self,
        retry_if_empty: bool,
        original_results_empty: bool,
        request: Option<&ResultToolRequest>,
    ) -> Result<Option<Value>> {
        if !retry_if_empty || !original_results_empty {
            return Ok(None);
        }
        let Some(request) = request else {
            return Ok(None);
        };
        let response = self.dispatch(ToolRequest {
            id: json!("primary-retry"),
            tool: request.tool.clone(),
            arguments: request.arguments.clone(),
        });
        if let Some(error) = response.error {
            return Err(anyhow!("primary retry request failed: {error}"));
        }
        let result = response.result.unwrap_or(Value::Null);
        primary_retry_result_value(request, result).map(Some)
    }

    fn search_auto_live(
        &self,
        repo: PathBuf,
        arguments: &Value,
        query: &str,
        limit: usize,
        context_lines: usize,
        diagnose: bool,
        retry_if_empty: bool,
        summary: bool,
    ) -> Result<SearchAutoResult> {
        let filters = search_filters(arguments, false)?;
        let mut results = search_repo_fast_filtered(&repo, query, limit, &filters)?;
        attach_result_context(&mut results, context_lines, |path, start, lines| {
            read_file_range(&repo, path, start, lines)
        })?;
        attach_result_read_requests(&mut results, "read_range", read_request_args("repo", &repo));
        attach_result_related_requests(
            &mut results,
            "related_files",
            read_request_args("repo", &repo),
            Some(&filters),
        );
        attach_result_related_symbol_requests(
            &mut results,
            "related_symbols",
            Some(query),
            read_request_args("repo", &repo),
        );
        let (query_plan_result, query_plan_summary, primary_diagnosis, primary_retry_request) =
            if diagnose || results.is_empty() {
                let index = FastIndex::build(&repo)?;
                let plan = attach_retry_requests(
                    index.query_plan(query, &filters)?,
                    "search_code",
                    "repo",
                    &repo,
                    arguments,
                );
                (
                    Some(serde_json::to_value(&plan)?),
                    Some(serde_json::to_value(plan.compact_summary())?),
                    primary_diagnosis_from_plan(&plan),
                    primary_retry_request_from_plan(&plan),
                )
            } else {
                (None, None, None, None)
            };
        let primary_retry_result = self.search_auto_primary_retry_result(
            retry_if_empty,
            results.is_empty(),
            primary_retry_request.as_ref(),
        )?;
        let read_batch_request =
            result_read_batch_request(&results, "read_ranges", read_request_args("repo", &repo));
        let next_read_batch_request =
            promoted_next_read_batch_request(&read_batch_request, &primary_retry_result);
        let refresh_request = None;
        let repo_map_request = auto_repo_map_request("repo_map", "repo", &repo, arguments, None);
        let next_action = search_auto_next_action(
            &refresh_request,
            &next_read_batch_request,
            &primary_retry_request,
            &repo_map_request,
            should_prefer_retry_next_action(
                &primary_diagnosis,
                &primary_retry_request,
                &primary_retry_result,
            ),
        );
        Ok(SearchAutoResult {
            query: query.to_string(),
            summary: search_result_summary_with_primary_retry(&results, &primary_retry_result),
            surface: "fallback".to_string(),
            target: repo.to_string_lossy().to_string(),
            freshness: None,
            refresh_request,
            query_plan_request: auto_query_plan_request(
                "search_query_plan",
                "repo",
                &repo,
                arguments,
                query,
            ),
            query_plan_result: compact_optional_query_plan_result(summary, query_plan_result),
            query_plan_summary,
            primary_diagnosis,
            primary_retry_request,
            primary_retry_result,
            repo_map_request,
            read_batch_request,
            next_read_batch_request,
            next_action,
            results,
        })
    }

    fn search_auto_shards(
        &self,
        index_dir: PathBuf,
        arguments: &Value,
        query: &str,
        limit: usize,
        context_lines: usize,
        refresh_if_stale: bool,
        diagnose: bool,
        retry_if_empty: bool,
        summary: bool,
    ) -> Result<SearchAutoResult> {
        let filters = search_filters(arguments, true)?;
        let shard_scope_filters = merge_filters(filters.clone(), parse_query(query).filters);
        if refresh_if_stale {
            self.refresh_shards_for_arguments_if_stale(
                &index_dir,
                arguments,
                &shard_scope_filters,
            )?;
        }
        let mut results =
            self.search_shards_cached(&index_dir, query, limit, &filters, context_lines)?;
        attach_result_read_requests(
            &mut results,
            "read_range",
            read_request_args("index_dir", &index_dir),
        );
        attach_result_related_requests(
            &mut results,
            "related_files",
            read_request_args("index_dir", &index_dir),
            Some(&filters),
        );
        attach_result_related_symbol_requests(
            &mut results,
            "related_symbols",
            Some(query),
            read_request_args("index_dir", &index_dir),
        );
        let query_plan_result = if diagnose || results.is_empty() {
            let mut plans = self.shard_query_plans_cached(&index_dir, query, &filters)?;
            attach_shard_retry_requests(&mut plans, &index_dir, arguments);
            Some((
                serde_json::to_value(&plans)?,
                shard_query_plan_summary_value(&plans),
                primary_diagnosis_from_shard_plans(&plans, results.is_empty()),
                primary_retry_request_from_shard_plans(&plans),
            ))
        } else {
            None
        };
        let (query_plan_result, query_plan_summary, primary_diagnosis, primary_retry_request) =
            query_plan_result
                .map(|(result, summary, diagnosis, primary)| {
                    (Some(result), Some(summary), diagnosis, primary)
                })
                .unwrap_or((None, None, None, None));
        let primary_retry_result = self.search_auto_primary_retry_result(
            retry_if_empty,
            results.is_empty(),
            primary_retry_request.as_ref(),
        )?;
        let freshness = self.search_auto_shard_freshness(
            !refresh_if_stale && (diagnose || results.is_empty()),
            &index_dir,
            arguments,
            &shard_scope_filters,
            query,
        )?;
        let read_batch_request = result_read_batch_request(
            &results,
            "read_ranges",
            read_request_args("index_dir", &index_dir),
        );
        let next_read_batch_request =
            promoted_next_read_batch_request(&read_batch_request, &primary_retry_result);
        let refresh_request = freshness_refresh_request(&freshness);
        let repo_map_request = auto_repo_map_request(
            "repo_map",
            "index_dir",
            &index_dir,
            arguments,
            Some(&shard_scope_filters),
        );
        let next_action = search_auto_next_action(
            &refresh_request,
            &next_read_batch_request,
            &primary_retry_request,
            &repo_map_request,
            should_prefer_retry_next_action(
                &primary_diagnosis,
                &primary_retry_request,
                &primary_retry_result,
            ),
        );
        Ok(SearchAutoResult {
            query: query.to_string(),
            summary: search_result_summary_with_primary_retry(&results, &primary_retry_result),
            surface: "shards".to_string(),
            target: index_dir.to_string_lossy().to_string(),
            refresh_request,
            freshness,
            query_plan_request: auto_query_plan_request(
                "shard_query_plan",
                "index_dir",
                &index_dir,
                arguments,
                query,
            ),
            query_plan_result: compact_optional_query_plan_result(summary, query_plan_result),
            query_plan_summary,
            primary_diagnosis,
            primary_retry_request,
            primary_retry_result,
            repo_map_request,
            read_batch_request,
            next_read_batch_request,
            next_action,
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
        diagnose: bool,
        retry_if_empty: bool,
        summary: bool,
    ) -> Result<SearchAutoResult> {
        let index = self.cached_index_maybe_refresh(index_path.clone(), refresh_if_stale)?;
        let filters = search_filters(arguments, true)?;
        let mut results = index.search_filtered(query, limit, &filters)?;
        attach_result_query_plan_retry_requests(
            &mut results,
            "indexed_search_code",
            "index",
            &index_path,
            arguments,
        );
        attach_result_context(&mut results, context_lines, |path, start, lines| {
            index.read_range(path, start, lines)
        })?;
        attach_result_read_requests(
            &mut results,
            "read_range",
            read_request_args("index", &index_path),
        );
        attach_result_related_requests(
            &mut results,
            "related_files",
            read_request_args("index", &index_path),
            Some(&filters),
        );
        attach_result_related_symbol_requests(
            &mut results,
            "related_symbols",
            Some(query),
            read_request_args("index", &index_path),
        );
        let (query_plan_result, query_plan_summary, primary_diagnosis, primary_retry_request) =
            if diagnose || results.is_empty() {
                let plan = attach_retry_requests(
                    index.query_plan(query, &filters)?,
                    "indexed_search_code",
                    "index",
                    &index_path,
                    arguments,
                );
                (
                    Some(serde_json::to_value(&plan)?),
                    Some(serde_json::to_value(plan.compact_summary())?),
                    primary_diagnosis_from_plan(&plan),
                    primary_retry_request_from_plan(&plan),
                )
            } else {
                (None, None, None, None)
            };
        let primary_retry_result = self.search_auto_primary_retry_result(
            retry_if_empty,
            results.is_empty(),
            primary_retry_request.as_ref(),
        )?;
        let freshness = self.search_auto_index_freshness(
            !refresh_if_stale && (diagnose || results.is_empty()),
            &index,
            &index_path,
            arguments,
            query,
        )?;
        let read_batch_request = result_read_batch_request(
            &results,
            "read_ranges",
            read_request_args("index", &index_path),
        );
        let next_read_batch_request =
            promoted_next_read_batch_request(&read_batch_request, &primary_retry_result);
        let refresh_request = freshness_refresh_request(&freshness);
        let repo_map_request =
            auto_repo_map_request("repo_map", "index", &index_path, arguments, None);
        let next_action = search_auto_next_action(
            &refresh_request,
            &next_read_batch_request,
            &primary_retry_request,
            &repo_map_request,
            should_prefer_retry_next_action(
                &primary_diagnosis,
                &primary_retry_request,
                &primary_retry_result,
            ),
        );
        Ok(SearchAutoResult {
            query: query.to_string(),
            summary: search_result_summary_with_primary_retry(&results, &primary_retry_result),
            surface: "indexed".to_string(),
            target: index_path.to_string_lossy().to_string(),
            refresh_request,
            freshness,
            query_plan_request: auto_query_plan_request(
                "indexed_query_plan",
                "index",
                &index_path,
                arguments,
                query,
            ),
            query_plan_result: compact_optional_query_plan_result(summary, query_plan_result),
            query_plan_summary,
            primary_diagnosis,
            primary_retry_request,
            primary_retry_result,
            repo_map_request,
            read_batch_request,
            next_read_batch_request,
            next_action,
            results,
        })
    }

    fn search_auto_index_freshness(
        &self,
        should_check: bool,
        index: &FastIndex,
        index_path: &Path,
        arguments: &Value,
        query: &str,
    ) -> Result<Option<SearchFreshness>> {
        if !should_check {
            return Ok(None);
        }
        let status = index.freshness_at(index_path)?;
        if !status.stale {
            return Ok(None);
        }
        Ok(Some(index_search_freshness(
            &status,
            search_auto_refresh_request("index", index_path, arguments, query),
        )))
    }

    fn search_auto_shard_freshness(
        &self,
        should_check: bool,
        index_dir: &Path,
        arguments: &Value,
        filters: &SearchFilters,
        query: &str,
    ) -> Result<Option<SearchFreshness>> {
        if !should_check {
            return Ok(None);
        }
        let roots = shard_freshness_roots_for_search(index_dir, arguments, filters)?;
        if roots.is_empty() {
            return Ok(None);
        }
        let status = shard_status_by_root(index_dir, &roots)?;
        if !status.stale {
            return Ok(None);
        }
        Ok(Some(shard_search_freshness(
            &status,
            search_auto_refresh_request("index_dir", index_dir, arguments, query),
        )))
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

    fn refresh_shards_for_arguments_if_stale(
        &self,
        index_dir: &Path,
        arguments: &Value,
        filters: &SearchFilters,
    ) -> Result<()> {
        self.refresh_shards_for_filter_set_if_stale(index_dir, arguments, [filters])
    }

    fn refresh_shards_for_filter_set_if_stale<'a>(
        &self,
        index_dir: &Path,
        arguments: &Value,
        filters: impl IntoIterator<Item = &'a SearchFilters>,
    ) -> Result<()> {
        let mut roots = Vec::new();
        let mut refresh_all = false;
        for filters in filters {
            match shard_refresh_selection_for_search(index_dir, arguments, filters)? {
                ShardRefreshSelection::Roots(selected_roots) => roots.extend(selected_roots),
                ShardRefreshSelection::All => refresh_all = true,
            }
        }
        if refresh_all {
            return self.refresh_shards_if_stale(index_dir);
        }
        roots.sort();
        roots.dedup();
        if roots.is_empty() || !shard_status_by_root(index_dir, &roots)?.stale {
            return Ok(());
        }
        refresh_shards_by_root(index_dir, &roots)?;
        self.clear_runtime_caches()
    }

    fn refresh_shards_for_query_batch_if_stale(
        &self,
        index_dir: &Path,
        arguments: &Value,
        base_filters: &SearchFilters,
        queries: &[String],
    ) -> Result<()> {
        let refresh_filters = queries
            .iter()
            .map(|query| merge_filters(base_filters.clone(), parse_query(query).filters))
            .collect::<Vec<_>>();
        self.refresh_shards_for_filter_set_if_stale(index_dir, arguments, &refresh_filters)
    }

    fn next_index_access(&self) -> u64 {
        self.next_index_access.fetch_add(1, AtomicOrdering::Relaxed)
    }

    fn replace_cached_index(&self, index_path: PathBuf, index: Arc<FastIndex>) -> Result<PathBuf> {
        let key = canonical_cache_key(&index_path);
        let fingerprint = index_file_fingerprint(&key);
        let access = self.next_index_access();
        {
            self.indexes
                .lock()
                .map_err(|_| anyhow!("index cache lock poisoned"))?
                .insert(
                    key.clone(),
                    Arc::new(IndexCacheEntry::ready(index, fingerprint, access)),
                );
        }
        self.evict_cached_indexes_if_needed(&key)?;
        Ok(key)
    }

    fn cached_index_with_key(&self, index_path: PathBuf) -> Result<(PathBuf, Arc<FastIndex>)> {
        let key = canonical_cache_key(&index_path);
        loop {
            let current_fingerprint = index_file_fingerprint(&key);
            let (entry, should_load) = {
                let mut indexes = self
                    .indexes
                    .lock()
                    .map_err(|_| anyhow!("index cache lock poisoned"))?;
                if let Some(entry) = indexes.get(&key) {
                    if entry.ready_is_stale(current_fingerprint) {
                        let entry = Arc::new(IndexCacheEntry::loading());
                        indexes.insert(key.clone(), Arc::clone(&entry));
                        (entry, true)
                    } else {
                        (Arc::clone(entry), false)
                    }
                } else {
                    let entry = Arc::new(IndexCacheEntry::loading());
                    indexes.insert(key.clone(), Arc::clone(&entry));
                    (entry, true)
                }
            };

            if should_load {
                let loaded = FastIndex::load(&key).map(Arc::new);
                let result = match loaded {
                    Ok(index) => {
                        let fingerprint = index_file_fingerprint(&key);
                        let access = self.next_index_access();
                        *entry
                            .state
                            .lock()
                            .map_err(|_| anyhow!("index cache entry lock poisoned"))? =
                            IndexCacheState::Ready {
                                index: Arc::clone(&index),
                                fingerprint,
                                last_access: access,
                            };
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
                if result.is_ok() {
                    self.evict_cached_indexes_if_needed(&key)?;
                } else {
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
                match &mut *state {
                    IndexCacheState::Ready {
                        index,
                        fingerprint,
                        last_access,
                    } => {
                        let current_fingerprint = index_file_fingerprint(&key);
                        if current_fingerprint.is_some() && *fingerprint != current_fingerprint {
                            drop(state);
                            break;
                        }
                        *last_access = self.next_index_access();
                        return Ok((key, Arc::clone(index)));
                    }
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
                        entry.ready_snapshot().map(|snapshot| {
                            let stats = snapshot.index.stats();
                            let disk = cache_disk_state(path, snapshot.fingerprint);
                            json!({
                                "index": path.to_string_lossy(),
                                "root": stats.root.to_string_lossy(),
                                "version": stats.version,
                                "files": stats.files,
                                "index_bytes": disk.bytes,
                                "disk_missing": disk.missing,
                                "disk_changed": disk.changed,
                                "source_bytes": stats.source_bytes,
                                "content_snapshot_bytes": stats.content_snapshot_bytes,
                                "line_offset_bytes": stats.line_offset_bytes,
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
        let current_fingerprint = shard_manifest_fingerprint(&key);
        let should_reload = {
            let manifests = self
                .shard_manifests
                .lock()
                .map_err(|_| anyhow!("shard manifest cache lock poisoned"))?;
            if let Some(entry) = manifests.get(&key) {
                if current_fingerprint.is_none() || entry.fingerprint == current_fingerprint {
                    return Ok(Arc::clone(&entry.manifest));
                }
                true
            } else {
                false
            }
        };

        let manifest = Arc::new(load_manifest(&key)?);
        let fingerprint = shard_manifest_fingerprint(&key);
        if should_reload {
            self.evict_cached_indexes_in_dir(&key)?;
        }
        self.shard_manifests
            .lock()
            .map_err(|_| anyhow!("shard manifest cache lock poisoned"))?
            .insert(
                key.clone(),
                CachedShardManifest {
                    manifest: Arc::clone(&manifest),
                    fingerprint,
                },
            );
        Ok(manifest)
    }

    fn cached_shard_manifest_if_fresh(
        &self,
        index_dir: &Path,
    ) -> Result<Option<Arc<ShardManifest>>> {
        let key = canonical_cache_key(index_dir);
        let current_fingerprint = shard_manifest_fingerprint(&key);
        let manifests = self
            .shard_manifests
            .lock()
            .map_err(|_| anyhow!("shard manifest cache lock poisoned"))?;
        Ok(manifests.get(&key).and_then(|entry| {
            (current_fingerprint.is_none() || entry.fingerprint == current_fingerprint)
                .then(|| Arc::clone(&entry.manifest))
        }))
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
        let footprints = self.cached_index_footprints();
        let mut details = self
            .shard_manifests
            .lock()
            .map(|manifests| {
                manifests
                    .iter()
                    .map(|(path, entry)| {
                        shard_manifest_detail(path, &entry.manifest, entry.fingerprint, &footprints)
                    })
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
        let footprints = self.cached_index_footprints();
        self.shard_manifests
            .lock()
            .ok()
            .and_then(|manifests| manifests.get(&key).cloned())
            .map(|entry| {
                shard_manifest_detail(&key, &entry.manifest, entry.fingerprint, &footprints)
            })
            .unwrap_or_else(|| {
                json!({
                    "index_dir": key.to_string_lossy(),
                    "shards": 0,
                    "repos": []
                })
            })
    }

    fn cached_index_footprints(&self) -> HashMap<PathBuf, CachedIndexFootprint> {
        self.indexes
            .lock()
            .map(|indexes| {
                indexes
                    .iter()
                    .filter_map(|(path, entry)| {
                        entry.ready_snapshot().map(|snapshot| {
                            let stats = snapshot.index.stats();
                            (
                                path.clone(),
                                CachedIndexFootprint {
                                    content_snapshot_bytes: stats.content_snapshot_bytes,
                                    line_offset_bytes: stats.line_offset_bytes,
                                    fingerprint: snapshot.fingerprint,
                                },
                            )
                        })
                    })
                    .collect()
            })
            .unwrap_or_default()
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

    fn evict_cached_indexes_in_dir(&self, index_dir: &Path) -> Result<()> {
        let index_dir = canonical_cache_key(index_dir);
        self.indexes
            .lock()
            .map_err(|_| anyhow!("index cache lock poisoned"))?
            .retain(|path, _| !path.starts_with(&index_dir));
        Ok(())
    }

    fn evict_cached_indexes_if_needed(&self, protected: &Path) -> Result<()> {
        let Some(max_ready_indexes) = self.cache_policy.max_ready_indexes else {
            return Ok(());
        };
        let mut indexes = self
            .indexes
            .lock()
            .map_err(|_| anyhow!("index cache lock poisoned"))?;
        loop {
            let ready_count = indexes.values().filter(|entry| entry.is_ready()).count();
            if ready_count <= max_ready_indexes {
                break;
            }
            let victim = indexes
                .iter()
                .filter(|(path, entry)| path.as_path() != protected && entry.is_ready())
                .filter_map(|(path, entry)| {
                    entry.last_access().map(|access| (path.clone(), access))
                })
                .min_by_key(|(_, access)| *access)
                .map(|(path, _)| path);
            let Some(victim) = victim else {
                break;
            };
            indexes.remove(&victim);
        }
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
        let parsed = parse_query(query);
        let filters = merge_filters(filters.clone(), parsed.filters);
        let shard_query = query_text(&parsed.terms, &filters);
        if shard_prefilter_query_impossible(index_dir, &shard_query, &filters)? {
            return Ok(Vec::new());
        }
        let jobs = if let Some(shards) = shard_route_entries(index_dir, &shard_query, &filters)? {
            shard_jobs_from_entries(shards, &shard_query, &filters, true)
        } else if let Some(manifest) = self.cached_shard_manifest_if_fresh(index_dir)? {
            shard_jobs_from_entries(
                manifest.shards.iter().cloned(),
                &shard_query,
                &filters,
                true,
            )
        } else {
            let manifest = self.cached_shard_manifest(index_dir)?;
            shard_jobs_from_entries(
                manifest.shards.iter().cloned(),
                &shard_query,
                &filters,
                true,
            )
        };
        let results =
            self.search_shard_jobs_cached(index_dir, &shard_query, limit, &filters, jobs)?;
        let mut results = finalize_results_for_filters(results, limit, &filters);
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
            Some(&filters),
        );
        attach_result_related_symbol_requests(
            &mut results,
            "related_shard_symbols",
            Some(query),
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

        let workers = bounded_shard_worker_count(jobs.len());
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
        let parsed = parse_query(query);
        let filters = merge_filters(filters.clone(), parsed.filters);
        let shard_query = query_text(&parsed.terms, &filters);
        let route_selection = shard_route_selection(index_dir, &shard_query, &filters)?;
        let (jobs, shard_count, shard_names) = if let Some(selection) = route_selection {
            (
                shard_jobs_from_entries(selection.shards, &shard_query, &filters, false),
                selection.shard_count,
                selection.shard_names,
            )
        } else {
            let manifest = self.cached_shard_manifest(index_dir)?;
            let shard_count = manifest.shards.len();
            let shard_names = manifest
                .shards
                .iter()
                .map(|shard| shard.name.clone())
                .collect::<Vec<_>>();
            let jobs = manifest
                .shards
                .iter()
                .cloned()
                .filter_map(|shard| {
                    let scopes = shard_search_scopes(&shard, &filters);
                    (!scopes.is_empty()).then_some(ShardJob { shard, scopes })
                })
                .collect::<Vec<_>>();
            (jobs, shard_count, shard_names)
        };
        let jobs = self.shard_diagnostic_jobs(jobs, &shard_query);
        if jobs.is_empty() {
            return Ok(vec![shard_selection_miss_plan(
                index_dir,
                &shard_query,
                &filters,
                shard_count,
                shard_names,
            )]);
        }
        let mut plans =
            self.shard_query_plan_jobs_cached(index_dir, &shard_query, &filters, jobs)?;
        plans.sort_by(|left, right| left.name.cmp(&right.name));
        append_shard_facet_repair_hints(&mut plans, &parsed.terms, &filters);
        Ok(plans)
    }

    fn shard_diagnostic_jobs(&self, jobs: Vec<ShardJob>, shard_query: &str) -> Vec<ShardJob> {
        let filtered = jobs
            .iter()
            .filter(|job| shard_sketch_may_diagnose_query(&job.shard, shard_query))
            .cloned()
            .collect::<Vec<_>>();
        if filtered.is_empty() { jobs } else { filtered }
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

        let workers = bounded_shard_worker_count(jobs.len());
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
                    summary: None,
                    next_action: None,
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
        self.read_shard_range_cached_scoped(index_dir, path, start, lines, RangeScope::Exact)
    }

    fn read_shard_range_cached_scoped(
        &self,
        index_dir: &std::path::Path,
        path: &str,
        start: usize,
        lines: usize,
        scope: RangeScope,
    ) -> Result<crate::repo_index::FileRange> {
        let resolved = self.resolve_shard_path_cached(index_dir, path)?;
        let index = self.cached_index(index_dir.join(&resolved.index))?;
        let mut range = index.read_range_scoped(&resolved.relative_path, start, lines, scope)?;
        range.path = resolved.output_path(&range.path);
        if let Some(symbol) = &mut range.symbol {
            symbol.path = resolved.output_path(&symbol.path);
        }
        Ok(range)
    }

    fn read_shard_range_for_client_cwd(
        &self,
        index_dir: &Path,
        arguments: &Value,
        path: &str,
        start: usize,
        lines: usize,
        scope: RangeScope,
        tool_name: &str,
    ) -> Result<Option<crate::repo_index::FileRange>> {
        let Some(repo_root) = git_root_from_client_cwd(arguments, tool_name)? else {
            return Ok(None);
        };
        let manifest = self.cached_shard_manifest(index_dir)?;
        let Some(shard) = manifest
            .shards
            .iter()
            .find(|shard| canonical_cache_key(&shard.root) == repo_root)
        else {
            return Ok(None);
        };
        let resolved = resolved_shard_read_for_client_cwd(shard, path);
        let index = self.cached_index(index_dir.join(&resolved.index))?;
        let mut range = index.read_range_scoped(&resolved.relative_path, start, lines, scope)?;
        range.path = resolved.output_path(&range.path);
        if let Some(symbol) = &mut range.symbol {
            symbol.path = resolved.output_path(&symbol.path);
        }
        Ok(Some(range))
    }

    fn shard_output_path_for_client_cwd(
        &self,
        index_dir: &Path,
        arguments: &Value,
        path: &str,
        tool_name: &str,
    ) -> Result<Option<String>> {
        let Some(repo_root) = git_root_from_client_cwd(arguments, tool_name)? else {
            return Ok(None);
        };
        let manifest = self.cached_shard_manifest(index_dir)?;
        let Some(shard) = manifest
            .shards
            .iter()
            .find(|shard| canonical_cache_key(&shard.root) == repo_root)
        else {
            return Ok(None);
        };
        let resolved = resolved_shard_read_for_client_cwd(shard, path);
        Ok(Some(resolved.output_path(&resolved.relative_path)))
    }

    fn related_shard_symbols_for_client_cwd(
        &self,
        index_dir: &Path,
        arguments: &Value,
        path: Option<&str>,
        query: Option<&str>,
        limit: usize,
        filters: &SearchFilters,
        tool_name: &str,
    ) -> Result<Option<Vec<crate::repo_index::RelatedSymbol>>> {
        let Some(repo_root) = git_root_from_client_cwd(arguments, tool_name)? else {
            return Ok(None);
        };
        let manifest = self.cached_shard_manifest(index_dir)?;
        let Some(shard) = manifest
            .shards
            .iter()
            .find(|shard| canonical_cache_key(&shard.root) == repo_root)
        else {
            return Ok(None);
        };
        let resolved = path.map(|path| resolved_shard_read_for_client_cwd(shard, path));
        let index = self.cached_index(index_dir.join(&shard.index))?;
        let query = related_query_without_shard_selectors(query);
        let mut filters = filters.clone();
        filters.repo = None;
        filters.branch = None;
        filters.origin = None;
        filters.exclude_repo.clear();
        filters.exclude_branch.clear();
        filters.exclude_origin.clear();
        let anchor_path = resolved
            .as_ref()
            .map(|resolved| resolved.relative_path.as_str());
        let mut related = index.related_symbols_filtered(
            anchor_path,
            query.as_deref(),
            limit.saturating_mul(4).max(10),
            &filters,
        );
        if let Some(resolved) = &resolved {
            related.retain(|symbol| resolved.contains_actual_path(&symbol.symbol.path));
            for symbol in &mut related {
                symbol.symbol.path = resolved.output_path(&symbol.symbol.path);
            }
        } else {
            for symbol in &mut related {
                symbol.symbol.path = format!("{}/{}", shard.name, symbol.symbol.path);
            }
        }
        related.truncate(limit);
        Ok(Some(related))
    }

    fn related_shard_files_cached(
        &self,
        index_dir: &std::path::Path,
        path: &str,
        limit: usize,
        filters: &SearchFilters,
    ) -> Result<Vec<crate::repo_index::RelatedFile>> {
        let resolved = self.resolve_shard_path_cached(index_dir, path)?;
        let index = self.cached_index(index_dir.join(&resolved.index))?;
        let mut filters = filters.clone();
        filters.repo = None;
        filters.branch = None;
        filters.origin = None;
        filters.exclude_repo.clear();
        filters.exclude_branch.clear();
        filters.exclude_origin.clear();
        let mut related = index.related_files_filtered(
            &resolved.relative_path,
            limit.saturating_mul(4).max(10),
            &filters,
        );
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
        filters: &SearchFilters,
    ) -> Result<Vec<crate::repo_index::RelatedSymbol>> {
        let resolved = self.resolve_shard_path_cached(index_dir, path)?;
        let index = self.cached_index(index_dir.join(&resolved.index))?;
        let query = related_query_without_shard_selectors(query);
        let mut filters = filters.clone();
        filters.repo = None;
        filters.branch = None;
        filters.origin = None;
        filters.exclude_repo.clear();
        filters.exclude_branch.clear();
        filters.exclude_origin.clear();
        let mut related = index.related_symbols_filtered(
            Some(&resolved.relative_path),
            query.as_deref(),
            limit.saturating_mul(4).max(10),
            &filters,
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
        detail: RepoMapDetail,
        read_limit: usize,
        filters: &SearchFilters,
        read_tool: &str,
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
                let mut map =
                    index.repo_map_with_detail(base_symbol_limit, base_test_limit, detail);
                if let Some(prefix) = scope.path_prefix.as_deref() {
                    filter_repo_map_by_prefix(&mut map, prefix);
                    map.test_files.truncate(test_limit);
                    map.top_symbols.truncate(symbol_limit);
                    map.refresh_summary();
                }
                prefix_repo_map_paths(&mut map, &scope);
                attach_repo_map_read_batch_request_with_limit(
                    &mut map,
                    read_tool,
                    read_request_args("index_dir", index_dir),
                    read_limit,
                );
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
                let scoped_filters = filters_for_shard_scope(filters, scope.path_prefix.as_deref());
                for mut symbol in index.find_symbol_filtered(name, limit, &scoped_filters) {
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

fn shard_manifest_detail(
    index_dir: &Path,
    manifest: &ShardManifest,
    manifest_fingerprint: Option<CacheFileFingerprint>,
    footprints: &HashMap<PathBuf, CachedIndexFootprint>,
) -> Value {
    let mut total_index_bytes = 0u64;
    let mut total_content_snapshot_bytes = 0u64;
    let mut total_line_offset_bytes = 0usize;
    let manifest_disk = cache_disk_state(&index_dir.join("manifest.json"), manifest_fingerprint);
    let repos = manifest
        .shards
        .iter()
        .map(|shard| {
            let index_path = index_dir.join(&shard.index);
            let footprint = footprints
                .get(&canonical_cache_key(&index_path))
                .copied()
                .unwrap_or_default();
            let index_disk = cache_disk_state(&index_path, footprint.fingerprint);
            if let Some(index_bytes) = index_disk.bytes {
                total_index_bytes += index_bytes;
            }
            total_content_snapshot_bytes += footprint.content_snapshot_bytes;
            total_line_offset_bytes += footprint.line_offset_bytes;
            json!({
                "name": shard.name,
                "root": shard.root,
                "index": shard.index,
                "index_bytes": index_disk.bytes,
                "index_disk_missing": index_disk.missing,
                "index_disk_changed": index_disk.changed,
                "content_snapshot_bytes": footprint.content_snapshot_bytes,
                "line_offset_bytes": footprint.line_offset_bytes,
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
        "manifest_bytes": manifest_disk.bytes,
        "manifest_disk_missing": manifest_disk.missing,
        "manifest_disk_changed": manifest_disk.changed,
        "index_bytes": total_index_bytes,
        "content_snapshot_bytes": total_content_snapshot_bytes,
        "line_offset_bytes": total_line_offset_bytes,
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

fn index_file_fingerprint(index_path: &Path) -> Option<CacheFileFingerprint> {
    file_fingerprint(index_path)
}

fn shard_manifest_fingerprint(index_dir: &Path) -> Option<CacheFileFingerprint> {
    file_fingerprint(&index_dir.join("manifest.json"))
}

fn file_fingerprint(path: &Path) -> Option<CacheFileFingerprint> {
    let metadata = fs::metadata(path).ok()?;
    Some(CacheFileFingerprint {
        len: metadata.len(),
        modified: metadata.modified().ok(),
    })
}

fn cache_disk_state(
    path: &Path,
    cached_fingerprint: Option<CacheFileFingerprint>,
) -> CacheDiskState {
    let current = file_fingerprint(path);
    CacheDiskState {
        bytes: current.map(|fingerprint| fingerprint.len),
        missing: current.is_none(),
        changed: current.is_some_and(|fingerprint| cached_fingerprint != Some(fingerprint)),
    }
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

fn resolved_shard_read_for_client_cwd(
    shard: &ShardEntry,
    path: &str,
) -> crate::shards::ResolvedShardRead {
    let normalized = path.trim().replace('\\', "/");
    if let Some((prefix, relative_path)) = normalized.split_once('/') {
        if prefix == shard.name {
            return crate::shards::ResolvedShardRead {
                index: shard.index.clone(),
                relative_path: relative_path.to_string(),
                output_prefix: shard.name.clone(),
                path_prefix: None,
            };
        }
        if let Some(alias) = shard.aliases.iter().find(|alias| alias.name == prefix) {
            let relative_path = alias
                .path_prefix
                .as_deref()
                .map(|path_prefix| {
                    if relative_path.is_empty() {
                        path_prefix.trim_end_matches('/').to_string()
                    } else {
                        format!(
                            "{}/{}",
                            path_prefix.trim_end_matches('/'),
                            relative_path.trim_start_matches('/')
                        )
                    }
                })
                .unwrap_or_else(|| relative_path.to_string());
            return crate::shards::ResolvedShardRead {
                index: shard.index.clone(),
                relative_path,
                output_prefix: alias.name.clone(),
                path_prefix: alias.path_prefix.clone(),
            };
        }
    }

    crate::shards::ResolvedShardRead {
        index: shard.index.clone(),
        relative_path: normalized,
        output_prefix: shard.name.clone(),
        path_prefix: None,
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
    for path in &mut map.manifest_files {
        *path = scoped_output_path(scope, path);
    }
    for path in &mut map.important_files {
        *path = scoped_output_path(scope, path);
    }
    for hint in &mut map.command_hints {
        hint.source = scoped_output_path(scope, &hint.source);
    }
    for hint in &mut map.dependency_hints {
        hint.source = scoped_output_path(scope, &hint.source);
    }
    for hint in &mut map.import_hints {
        hint.source = scoped_output_path(scope, &hint.source);
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
    "lang",
    "extension",
    "ext",
    "symbol",
    "symbol_kind",
    "kind",
    "type",
    "dependency",
    "dep",
    "deps",
    "import",
    "imports",
    "module",
    "modules",
    "use",
    "uses",
    "file",
    "line",
    "target_line",
    "repo_filter",
    "branch",
    "origin",
    "test",
    "generated",
    "code",
    "snippet",
    "snippet_mode",
    "snippet-mode",
    "explain",
    "require_all",
    "any_terms",
    "context_lines",
    "exclude_file",
    "exclude_path",
    "exclude_language",
    "exclude_lang",
    "exclude_extension",
    "exclude_ext",
    "exclude_symbol",
    "exclude_symbol_kind",
    "exclude_kind",
    "exclude_type",
    "exclude_repo",
    "exclude_branch",
    "exclude_origin",
    "exclude_dependency",
    "exclude_dep",
    "exclude_deps",
    "exclude_import",
    "exclude_imports",
    "exclude_module",
    "exclude_modules",
    "exclude_use",
    "exclude_uses",
    "exclude_content",
    "exclude_text",
    "exclude_term",
];

const SEARCH_TARGET_OPTIONAL_ARGS: &[&str] = &[
    "repo",
    "index",
    "index_dir",
    "cwd",
    "limit",
    "path",
    "dir",
    "language",
    "lang",
    "extension",
    "ext",
    "symbol",
    "symbol_kind",
    "kind",
    "type",
    "dependency",
    "dep",
    "deps",
    "import",
    "imports",
    "module",
    "modules",
    "use",
    "uses",
    "file",
    "line",
    "target_line",
    "repo_filter",
    "branch",
    "origin",
    "test",
    "generated",
    "code",
    "snippet",
    "snippet_mode",
    "snippet-mode",
    "explain",
    "require_all",
    "any_terms",
    "context_lines",
    "refresh_if_stale",
    "exclude_file",
    "exclude_path",
    "exclude_language",
    "exclude_lang",
    "exclude_extension",
    "exclude_ext",
    "exclude_symbol",
    "exclude_symbol_kind",
    "exclude_kind",
    "exclude_type",
    "exclude_repo",
    "exclude_branch",
    "exclude_origin",
    "exclude_dependency",
    "exclude_dep",
    "exclude_deps",
    "exclude_import",
    "exclude_imports",
    "exclude_module",
    "exclude_modules",
    "exclude_use",
    "exclude_uses",
    "exclude_content",
    "exclude_text",
    "exclude_term",
];

const READ_TARGET_OPTIONAL_ARGS: &[&str] = &[
    "repo",
    "index",
    "index_dir",
    "cwd",
    "path",
    "range",
    "start",
    "start_line",
    "line",
    "target_line",
    "lines",
    "line_count",
    "end_line",
    "end",
    "scope",
];

const READ_BATCH_TARGET_OPTIONAL_ARGS: &[&str] = &[
    "repo",
    "index",
    "index_dir",
    "cwd",
    "scope",
    "include_summary",
];
const READ_BATCH_INDEX_OPTIONAL_ARGS: &[&str] = &["scope", "include_summary"];
const READ_WINDOW_OPTIONAL_ARGS: &[&str] = &[
    "path",
    "range",
    "start",
    "start_line",
    "line",
    "target_line",
    "lines",
    "line_count",
    "end_line",
    "end",
    "scope",
];

const REPO_MAP_TARGET_OPTIONAL_ARGS: &[&str] = &[
    "repo",
    "index",
    "index_dir",
    "cwd",
    "symbols",
    "tests",
    "detail",
    "read_limit",
    "repo_filter",
    "branch",
    "origin",
    "refresh_if_stale",
];

const RELATED_FILES_TARGET_OPTIONAL_ARGS: &[&str] = &[
    "repo",
    "index",
    "index_dir",
    "cwd",
    "limit",
    "include_read_batch",
    "language",
    "lang",
    "extension",
    "ext",
    "symbol",
    "symbol_kind",
    "kind",
    "type",
    "dependency",
    "dep",
    "deps",
    "import",
    "imports",
    "module",
    "modules",
    "use",
    "uses",
    "file",
    "repo_filter",
    "branch",
    "origin",
    "test",
    "generated",
    "code",
    "exclude_file",
    "exclude_path",
    "exclude_folder",
    "exclude_language",
    "exclude_lang",
    "exclude_extension",
    "exclude_ext",
    "exclude_symbol",
    "exclude_symbol_kind",
    "exclude_kind",
    "exclude_type",
    "exclude_repo",
    "exclude_branch",
    "exclude_origin",
    "exclude_dependency",
    "exclude_dep",
    "exclude_deps",
    "exclude_import",
    "exclude_imports",
    "exclude_module",
    "exclude_modules",
    "exclude_use",
    "exclude_uses",
    "exclude_content",
    "exclude_text",
    "exclude_term",
];

const RELATED_INDEX_FILES_OPTIONAL_ARGS: &[&str] = &[
    "limit",
    "include_read_batch",
    "language",
    "lang",
    "extension",
    "ext",
    "symbol",
    "symbol_kind",
    "kind",
    "type",
    "dependency",
    "dep",
    "deps",
    "import",
    "imports",
    "module",
    "modules",
    "use",
    "uses",
    "file",
    "repo",
    "repo_filter",
    "branch",
    "origin",
    "test",
    "generated",
    "code",
    "exclude_file",
    "exclude_path",
    "exclude_folder",
    "exclude_language",
    "exclude_lang",
    "exclude_extension",
    "exclude_ext",
    "exclude_symbol",
    "exclude_symbol_kind",
    "exclude_kind",
    "exclude_type",
    "exclude_repo",
    "exclude_branch",
    "exclude_origin",
    "exclude_dependency",
    "exclude_dep",
    "exclude_deps",
    "exclude_import",
    "exclude_imports",
    "exclude_module",
    "exclude_modules",
    "exclude_use",
    "exclude_uses",
    "exclude_content",
    "exclude_text",
    "exclude_term",
];

const RELATED_SHARD_FILES_OPTIONAL_ARGS: &[&str] = RELATED_INDEX_FILES_OPTIONAL_ARGS;

const RELATED_SYMBOLS_TARGET_OPTIONAL_ARGS: &[&str] = &[
    "repo",
    "index",
    "index_dir",
    "cwd",
    "path",
    "query",
    "limit",
    "include_read_batch",
    "language",
    "lang",
    "extension",
    "ext",
    "symbol",
    "symbol_kind",
    "kind",
    "type",
    "dependency",
    "dep",
    "deps",
    "import",
    "imports",
    "module",
    "modules",
    "use",
    "uses",
    "file",
    "line",
    "target_line",
    "repo_filter",
    "branch",
    "origin",
    "test",
    "generated",
    "code",
    "exclude_file",
    "exclude_language",
    "exclude_lang",
    "exclude_extension",
    "exclude_ext",
    "exclude_symbol",
    "exclude_symbol_kind",
    "exclude_kind",
    "exclude_type",
    "exclude_repo",
    "exclude_branch",
    "exclude_origin",
    "exclude_dependency",
    "exclude_dep",
    "exclude_deps",
    "exclude_import",
    "exclude_imports",
    "exclude_module",
    "exclude_modules",
    "exclude_use",
    "exclude_uses",
    "exclude_content",
    "exclude_text",
    "exclude_term",
];

const RELATED_INDEX_SYMBOLS_OPTIONAL_ARGS: &[&str] = &[
    "path",
    "query",
    "limit",
    "include_read_batch",
    "language",
    "lang",
    "extension",
    "ext",
    "symbol",
    "symbol_kind",
    "kind",
    "type",
    "dependency",
    "dep",
    "deps",
    "import",
    "imports",
    "module",
    "modules",
    "use",
    "uses",
    "file",
    "line",
    "target_line",
    "repo",
    "repo_filter",
    "branch",
    "origin",
    "test",
    "generated",
    "code",
    "exclude_file",
    "exclude_language",
    "exclude_lang",
    "exclude_extension",
    "exclude_ext",
    "exclude_symbol",
    "exclude_symbol_kind",
    "exclude_kind",
    "exclude_type",
    "exclude_repo",
    "exclude_branch",
    "exclude_origin",
    "exclude_dependency",
    "exclude_dep",
    "exclude_deps",
    "exclude_import",
    "exclude_imports",
    "exclude_module",
    "exclude_modules",
    "exclude_use",
    "exclude_uses",
    "exclude_content",
    "exclude_text",
    "exclude_term",
];

const RELATED_SHARD_SYMBOLS_OPTIONAL_ARGS: &[&str] = &[
    "query",
    "limit",
    "include_read_batch",
    "language",
    "lang",
    "extension",
    "ext",
    "symbol",
    "symbol_kind",
    "kind",
    "type",
    "dependency",
    "dep",
    "deps",
    "import",
    "imports",
    "module",
    "modules",
    "use",
    "uses",
    "file",
    "line",
    "target_line",
    "repo",
    "repo_filter",
    "branch",
    "origin",
    "test",
    "generated",
    "code",
    "exclude_file",
    "exclude_language",
    "exclude_lang",
    "exclude_extension",
    "exclude_ext",
    "exclude_symbol",
    "exclude_symbol_kind",
    "exclude_kind",
    "exclude_type",
    "exclude_repo",
    "exclude_branch",
    "exclude_origin",
    "exclude_dependency",
    "exclude_dep",
    "exclude_deps",
    "exclude_import",
    "exclude_imports",
    "exclude_module",
    "exclude_modules",
    "exclude_use",
    "exclude_uses",
    "exclude_content",
    "exclude_text",
    "exclude_term",
];

const SYMBOL_TARGET_OPTIONAL_ARGS: &[&str] = &[
    "repo",
    "index",
    "index_dir",
    "cwd",
    "limit",
    "include_read_batch",
    "path",
    "dir",
    "language",
    "lang",
    "extension",
    "ext",
    "symbol",
    "symbol_kind",
    "kind",
    "type",
    "dependency",
    "dep",
    "deps",
    "import",
    "imports",
    "module",
    "modules",
    "use",
    "uses",
    "file",
    "line",
    "target_line",
    "repo_filter",
    "branch",
    "origin",
    "test",
    "generated",
    "code",
    "exclude_file",
    "exclude_path",
    "exclude_language",
    "exclude_lang",
    "exclude_extension",
    "exclude_ext",
    "exclude_symbol",
    "exclude_symbol_kind",
    "exclude_kind",
    "exclude_type",
    "exclude_repo",
    "exclude_branch",
    "exclude_origin",
    "exclude_dependency",
    "exclude_dep",
    "exclude_deps",
    "exclude_import",
    "exclude_imports",
    "exclude_module",
    "exclude_modules",
    "exclude_use",
    "exclude_uses",
    "exclude_content",
    "exclude_text",
    "exclude_term",
];

const SYMBOL_INDEX_OPTIONAL_ARGS: &[&str] = &[
    "limit",
    "include_read_batch",
    "path",
    "dir",
    "language",
    "lang",
    "extension",
    "ext",
    "symbol",
    "symbol_kind",
    "kind",
    "type",
    "dependency",
    "dep",
    "deps",
    "import",
    "imports",
    "module",
    "modules",
    "use",
    "uses",
    "file",
    "repo",
    "repo_filter",
    "branch",
    "origin",
    "test",
    "generated",
    "code",
    "exclude_file",
    "exclude_path",
    "exclude_language",
    "exclude_lang",
    "exclude_extension",
    "exclude_ext",
    "exclude_symbol",
    "exclude_symbol_kind",
    "exclude_kind",
    "exclude_type",
    "exclude_repo",
    "exclude_branch",
    "exclude_origin",
    "exclude_dependency",
    "exclude_dep",
    "exclude_deps",
    "exclude_import",
    "exclude_imports",
    "exclude_module",
    "exclude_modules",
    "exclude_use",
    "exclude_uses",
    "exclude_content",
    "exclude_text",
    "exclude_term",
];

const SEARCH_AUTO_OPTIONAL_ARGS: &[&str] = &[
    "repo",
    "index",
    "index_dir",
    "cwd",
    "limit",
    "path",
    "dir",
    "language",
    "lang",
    "extension",
    "ext",
    "symbol",
    "symbol_kind",
    "kind",
    "type",
    "dependency",
    "dep",
    "deps",
    "import",
    "imports",
    "module",
    "modules",
    "use",
    "uses",
    "file",
    "line",
    "target_line",
    "repo_filter",
    "branch",
    "origin",
    "test",
    "generated",
    "code",
    "snippet",
    "snippet_mode",
    "snippet-mode",
    "explain",
    "require_all",
    "any_terms",
    "context_lines",
    "refresh_if_stale",
    "diagnose",
    "retry_if_empty",
    "summary",
    "exclude_file",
    "exclude_path",
    "exclude_language",
    "exclude_lang",
    "exclude_extension",
    "exclude_ext",
    "exclude_symbol",
    "exclude_symbol_kind",
    "exclude_kind",
    "exclude_type",
    "exclude_repo",
    "exclude_branch",
    "exclude_origin",
    "exclude_dependency",
    "exclude_dep",
    "exclude_deps",
    "exclude_import",
    "exclude_imports",
    "exclude_module",
    "exclude_modules",
    "exclude_use",
    "exclude_uses",
    "exclude_content",
    "exclude_text",
    "exclude_term",
];

const SEARCH_INDEX_OPTIONAL_ARGS: &[&str] = &[
    "limit",
    "path",
    "dir",
    "language",
    "lang",
    "extension",
    "ext",
    "symbol",
    "symbol_kind",
    "kind",
    "type",
    "dependency",
    "dep",
    "deps",
    "import",
    "imports",
    "module",
    "modules",
    "use",
    "uses",
    "file",
    "line",
    "target_line",
    "repo",
    "repo_filter",
    "branch",
    "origin",
    "test",
    "generated",
    "code",
    "snippet",
    "snippet_mode",
    "snippet-mode",
    "explain",
    "require_all",
    "any_terms",
    "context_lines",
    "refresh_if_stale",
    "exclude_file",
    "exclude_path",
    "exclude_language",
    "exclude_lang",
    "exclude_extension",
    "exclude_ext",
    "exclude_symbol",
    "exclude_symbol_kind",
    "exclude_kind",
    "exclude_type",
    "exclude_repo",
    "exclude_branch",
    "exclude_origin",
    "exclude_dependency",
    "exclude_dep",
    "exclude_deps",
    "exclude_import",
    "exclude_imports",
    "exclude_module",
    "exclude_modules",
    "exclude_use",
    "exclude_uses",
    "exclude_content",
    "exclude_text",
    "exclude_term",
];

const PLAN_OPTIONAL_ARGS: &[&str] = &[
    "path",
    "dir",
    "language",
    "lang",
    "extension",
    "ext",
    "symbol",
    "symbol_kind",
    "kind",
    "type",
    "dependency",
    "dep",
    "deps",
    "import",
    "imports",
    "module",
    "modules",
    "use",
    "uses",
    "file",
    "line",
    "target_line",
    "repo_filter",
    "branch",
    "origin",
    "test",
    "generated",
    "code",
    "require_all",
    "any_terms",
    "summary",
    "exclude_file",
    "exclude_path",
    "exclude_language",
    "exclude_lang",
    "exclude_extension",
    "exclude_ext",
    "exclude_symbol",
    "exclude_symbol_kind",
    "exclude_kind",
    "exclude_type",
    "exclude_repo",
    "exclude_branch",
    "exclude_origin",
    "exclude_dependency",
    "exclude_dep",
    "exclude_deps",
    "exclude_import",
    "exclude_imports",
    "exclude_module",
    "exclude_modules",
    "exclude_use",
    "exclude_uses",
    "exclude_content",
    "exclude_text",
    "exclude_term",
];

const PLAN_TARGET_OPTIONAL_ARGS: &[&str] = &[
    "repo",
    "index",
    "index_dir",
    "cwd",
    "path",
    "dir",
    "language",
    "lang",
    "extension",
    "ext",
    "symbol",
    "symbol_kind",
    "kind",
    "type",
    "dependency",
    "dep",
    "deps",
    "import",
    "imports",
    "module",
    "modules",
    "use",
    "uses",
    "file",
    "repo_filter",
    "branch",
    "origin",
    "test",
    "generated",
    "code",
    "require_all",
    "any_terms",
    "refresh_if_stale",
    "summary",
    "exclude_file",
    "exclude_path",
    "exclude_language",
    "exclude_lang",
    "exclude_extension",
    "exclude_ext",
    "exclude_symbol",
    "exclude_symbol_kind",
    "exclude_kind",
    "exclude_type",
    "exclude_repo",
    "exclude_branch",
    "exclude_origin",
    "exclude_dependency",
    "exclude_dep",
    "exclude_deps",
    "exclude_import",
    "exclude_imports",
    "exclude_module",
    "exclude_modules",
    "exclude_use",
    "exclude_uses",
    "exclude_content",
    "exclude_text",
    "exclude_term",
];

const PLAN_INDEX_OPTIONAL_ARGS: &[&str] = &[
    "path",
    "dir",
    "language",
    "lang",
    "extension",
    "ext",
    "symbol",
    "symbol_kind",
    "kind",
    "type",
    "dependency",
    "dep",
    "deps",
    "import",
    "imports",
    "module",
    "modules",
    "use",
    "uses",
    "file",
    "repo",
    "repo_filter",
    "branch",
    "origin",
    "test",
    "generated",
    "code",
    "require_all",
    "any_terms",
    "refresh_if_stale",
    "summary",
    "exclude_file",
    "exclude_path",
    "exclude_language",
    "exclude_lang",
    "exclude_extension",
    "exclude_ext",
    "exclude_symbol",
    "exclude_symbol_kind",
    "exclude_kind",
    "exclude_type",
    "exclude_repo",
    "exclude_branch",
    "exclude_origin",
    "exclude_dependency",
    "exclude_dep",
    "exclude_deps",
    "exclude_import",
    "exclude_imports",
    "exclude_module",
    "exclude_modules",
    "exclude_use",
    "exclude_uses",
    "exclude_content",
    "exclude_text",
    "exclude_term",
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

const INDEX_SHARD_BUILD_OPTIONAL_ARGS: &[&str] = &[
    "repos",
    "discover_root",
    "discover_roots",
    "root",
    "max_depth",
    "discover_limit",
    "limit",
    "family_limit",
    "nested_manifests",
    "force",
];

fn string_arg(arguments: &Value, name: &str) -> Result<String> {
    argument_value(arguments, name)
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
    argument_value(arguments, name)
        .and_then(Value::as_bool)
        .unwrap_or(false)
}

fn repo_map_detail_arg(arguments: &Value) -> Result<RepoMapDetail> {
    match argument_value(arguments, "detail")
        .and_then(Value::as_str)
        .unwrap_or("compact")
    {
        "compact" => Ok(RepoMapDetail::Compact),
        "full" => Ok(RepoMapDetail::Full),
        value => Err(anyhow!(
            "invalid repo map detail {value:?}; expected compact or full"
        )),
    }
}

#[derive(Debug, Clone, Copy)]
struct ReadWindowArg {
    start: usize,
    lines: usize,
    explicit_start: bool,
    explicit_lines: bool,
}

fn read_window_arg(arguments: &Value) -> Result<ReadWindowArg> {
    read_window_fields(arguments, "argument")
}

fn read_window_fields(value: &Value, label: &str) -> Result<ReadWindowArg> {
    let start = optional_bounded_usize_field_any(
        value,
        &["start", "start_line", "line", "target_line"],
        label,
        1,
        None,
    )?;
    let lines = optional_bounded_usize_field_any(
        value,
        &["lines", "line_count"],
        label,
        1,
        Some(MAX_READ_RANGE_LINES),
    )?;
    let end_line = optional_bounded_usize_field_any(value, &["end_line", "end"], label, 1, None)?;
    if lines.is_some() && end_line.is_some() {
        return Err(anyhow!(
            "{label} accepts only one of lines/line_count or end_line/end"
        ));
    }
    let start = start.unwrap_or(1);
    let (lines, explicit_lines) = if let Some(lines) = lines {
        (lines, true)
    } else if let Some(end_line) = end_line {
        if end_line < start {
            return Err(anyhow!(
                "{label} end_line must be greater than or equal to start"
            ));
        }
        (end_line.saturating_sub(start).saturating_add(1), true)
    } else {
        (80, false)
    };
    validate_read_window(start, lines)?;
    Ok(ReadWindowArg {
        start,
        lines,
        explicit_start: has_any_field(value, &["start", "start_line", "line", "target_line"]),
        explicit_lines,
    })
}

fn read_scope_arg(arguments: &Value) -> Result<RangeScope> {
    optional_read_scope_arg(arguments, "scope")?.map_or(Ok(RangeScope::Exact), Ok)
}

fn optional_read_scope_arg(value: &Value, name: &str) -> Result<Option<RangeScope>> {
    let Some(scope) = argument_value(value, name) else {
        return Ok(None);
    };
    let Some(scope) = scope.as_str() else {
        return Err(anyhow!("{name} must be a string"));
    };
    RangeScope::parse(scope)
        .map(Some)
        .ok_or_else(|| anyhow!("invalid {name} {scope:?}; expected exact or symbol"))
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
    let Some(value) = argument_value(arguments, name) else {
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
    let Some(value) = argument_value(arguments, name) else {
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
    scope: RangeScope,
}

fn range_args(arguments: &Value, tool_name: &str) -> Result<Vec<RangeArg>> {
    let value =
        argument_value(arguments, "ranges").ok_or_else(|| anyhow!("missing ranges argument"))?;
    let owned_single;
    let values = if let Some(values) = value.as_array() {
        values
    } else if value.is_object() || value.is_string() {
        owned_single = vec![value.clone()];
        &owned_single
    } else {
        return Err(anyhow!(
            "argument ranges must be a string, object, or array"
        ));
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
    let default_scope = read_scope_arg(arguments)?;
    let mut ranges = Vec::with_capacity(values.len());
    for value in values {
        ranges.push(range_arg(value, default_scope)?);
    }
    let ranges = compact_range_args(ranges);
    validate_batch_read_line_budget(&ranges, tool_name)?;
    Ok(ranges)
}

fn single_range_arg(arguments: &Value, tool_name: &str) -> Result<RangeArg> {
    let has_path = argument_value(arguments, "path").is_some();
    let has_range = argument_value(arguments, "range").is_some();
    let has_ranges = argument_value(arguments, "ranges").is_some();
    if (has_path && (has_range || has_ranges)) || (has_range && has_ranges) {
        return Err(anyhow!(
            "{tool_name} accepts one of path/start/lines, range, or ranges"
        ));
    }
    let default_scope = read_scope_arg(arguments)?;
    if let Some(value) = argument_value(arguments, "range") {
        return range_arg(value, default_scope);
    }
    if let Some(value) = argument_value(arguments, "ranges") {
        let value = if let Some(values) = value.as_array() {
            if values.len() != 1 {
                return Err(anyhow!(
                    "{tool_name} accepts exactly one range; use {tool_name}s for batches"
                ));
            }
            &values[0]
        } else {
            value
        };
        return range_arg(value, default_scope);
    }
    let path = string_arg(arguments, "path")?;
    let window = read_window_arg(arguments)?;
    normalize_read_range_arg(
        path,
        window.start,
        window.lines,
        default_scope,
        window.explicit_start,
        window.explicit_lines,
    )
}

fn compact_range_args(ranges: Vec<RangeArg>) -> Vec<RangeArg> {
    let mut compacted = Vec::with_capacity(ranges.len());
    for range in ranges {
        if try_dedupe_or_merge_range_arg(&mut compacted, &range) {
            continue;
        }
        compacted.push(range);
    }
    compacted
}

fn try_dedupe_or_merge_range_arg(ranges: &mut [RangeArg], range: &RangeArg) -> bool {
    if ranges
        .iter()
        .any(|existing| same_range_arg(existing, range))
    {
        return true;
    }
    if range.scope != RangeScope::Exact {
        return false;
    }
    if let Some(existing) = ranges
        .iter_mut()
        .find(|existing| can_merge_range_args(existing, range))
    {
        let start = existing.start.min(range.start);
        let end = range_arg_end(existing).max(range_arg_end(range));
        existing.start = start;
        existing.lines = end.saturating_sub(start).saturating_add(1);
        true
    } else {
        false
    }
}

fn same_range_arg(left: &RangeArg, right: &RangeArg) -> bool {
    left.path == right.path
        && left.start == right.start
        && left.lines == right.lines
        && left.scope == right.scope
}

fn can_merge_range_args(left: &RangeArg, right: &RangeArg) -> bool {
    if left.scope != RangeScope::Exact
        || right.scope != RangeScope::Exact
        || left.path != right.path
    {
        return false;
    }
    let start = left.start.min(right.start);
    let end = range_arg_end(left).max(range_arg_end(right));
    if end.saturating_sub(start).saturating_add(1) > MAX_READ_RANGE_LINES {
        return false;
    }
    left.start <= range_arg_end(right).saturating_add(1)
        && right.start <= range_arg_end(left).saturating_add(1)
}

fn range_arg_end(range: &RangeArg) -> usize {
    range.start.saturating_add(range.lines.saturating_sub(1))
}

fn validate_batch_read_line_budget(ranges: &[RangeArg], tool_name: &str) -> Result<()> {
    let total = ranges
        .iter()
        .try_fold(0usize, |total, range| total.checked_add(range.lines))
        .ok_or_else(|| anyhow!("batch read line count overflowed"))?;
    if total > MAX_BATCH_READ_LINES {
        return Err(anyhow!(
            "argument ranges requests {total} total lines, max {MAX_BATCH_READ_LINES}; split into smaller {tool_name} calls or lower lines per range"
        ));
    }
    Ok(())
}

fn range_arg(value: &Value, default_scope: RangeScope) -> Result<RangeArg> {
    if let Some(value) = value.as_str() {
        return range_arg_from_string(value, default_scope);
    }
    let path = value
        .get("path")
        .and_then(Value::as_str)
        .map(String::from)
        .ok_or_else(|| anyhow!("range entry must include string path"))?;
    let window = read_window_fields(value, "range")?;
    let scope = optional_read_scope_arg(value, "scope")?.unwrap_or(default_scope);
    normalize_read_range_arg(
        path,
        window.start,
        window.lines,
        scope,
        window.explicit_start,
        window.explicit_lines,
    )
}

fn range_arg_from_string(value: &str, default_scope: RangeScope) -> Result<RangeArg> {
    let (value, scope) = split_range_scope(value);
    let scope = scope.unwrap_or(default_scope);
    if let Some(range) = parse_compact_range_arg(value, scope)? {
        return Ok(range);
    }
    parse_copied_location_range(value, 1, 80, scope, false, false).ok_or_else(|| {
        anyhow!(
            "range string must be PATH:START:LINES[:SCOPE] or a copied PATH:LINE/PATH:START-END location"
        )
    })
}

fn normalize_read_range_arg(
    path: String,
    start: usize,
    lines: usize,
    scope: RangeScope,
    explicit_start: bool,
    explicit_lines: bool,
) -> Result<RangeArg> {
    validate_read_window(start, lines)?;
    if let Some(range) =
        parse_copied_location_range(&path, start, lines, scope, explicit_start, explicit_lines)
    {
        validate_read_window(range.start, range.lines)?;
        return Ok(range);
    }
    Ok(RangeArg {
        path,
        start,
        lines,
        scope,
    })
}

fn split_range_scope(value: &str) -> (&str, Option<RangeScope>) {
    let Some((base, scope_text)) = value.rsplit_once(':') else {
        return (value, None);
    };
    let Some(scope) = RangeScope::parse(scope_text) else {
        return (value, None);
    };
    (base, Some(scope))
}

fn parse_compact_range_arg(value: &str, scope: RangeScope) -> Result<Option<RangeArg>> {
    let mut parts = value.rsplitn(3, ':');
    let Some(lines) = parts.next().and_then(|value| value.parse::<usize>().ok()) else {
        return Ok(None);
    };
    let Some(start) = parts.next().and_then(|value| value.parse::<usize>().ok()) else {
        return Ok(None);
    };
    let Some(path) = parts.next().filter(|path| !path.is_empty()) else {
        return Ok(None);
    };
    if path_has_embedded_location_prefix(path) {
        return Ok(None);
    }
    validate_read_window(start, lines)?;
    Ok(Some(RangeArg {
        path: path.to_string(),
        start,
        lines,
        scope,
    }))
}

fn path_has_embedded_location_prefix(path: &str) -> bool {
    let lower = path.to_ascii_lowercase();
    path.contains("-->")
        || path.contains('\n')
        || lower.trim_start().starts_with("at ")
        || lower.contains(" at ")
}

fn parse_copied_location_range(
    value: &str,
    fallback_start: usize,
    fallback_lines: usize,
    scope: RangeScope,
    explicit_start: bool,
    explicit_lines: bool,
) -> Option<RangeArg> {
    let parsed = parse_query(value);
    let target_line = parsed.filters.target_line?;
    let path = parsed
        .filters
        .path
        .or(parsed.filters.file)
        .filter(|path| !path.is_empty())?;
    Some(RangeArg {
        path,
        start: if explicit_start {
            fallback_start
        } else {
            target_line
        },
        lines: if explicit_lines {
            fallback_lines
        } else {
            copied_location_lines(value).unwrap_or(80)
        },
        scope,
    })
}

fn copied_location_lines(value: &str) -> Option<usize> {
    copied_hash_anchor_lines(value)
        .or_else(|| copied_bitbucket_lines_anchor_lines(value))
        .or_else(|| copied_azure_devops_lines(value))
        .or_else(|| copied_colon_range_lines(value))
}

fn copied_hash_anchor_lines(value: &str) -> Option<usize> {
    let lower = value.to_ascii_lowercase();
    let marker = lower.find("#l")?;
    let after_marker = &value[marker + 2..];
    let (start, after_start) = split_leading_digits(after_marker)?;
    let after_start = strip_hash_anchor_column(after_start.trim_start()).trim_start();
    let after_dash = after_start.strip_prefix('-')?.trim_start();
    let after_optional_l = after_dash
        .strip_prefix('L')
        .or_else(|| after_dash.strip_prefix('l'))
        .unwrap_or(after_dash);
    let (end, _) = split_leading_digits(after_optional_l)?;
    (end >= start).then_some(end - start + 1)
}

fn copied_bitbucket_lines_anchor_lines(value: &str) -> Option<usize> {
    let lower = value.to_ascii_lowercase();
    let marker = lower.find("#lines-")?;
    let after_marker = &value[marker + "#lines-".len()..];
    let (start, after_start) = split_leading_digits(after_marker)?;
    let after_colon = after_start.strip_prefix(':')?;
    let (end, _) = split_leading_digits(after_colon)?;
    (end >= start).then_some(end - start + 1)
}

fn copied_azure_devops_lines(value: &str) -> Option<usize> {
    let lower = value.to_ascii_lowercase();
    if !(lower.contains("dev.azure.com/") || lower.contains(".visualstudio.com/"))
        || !lower.contains("/_git/")
    {
        return None;
    }
    let query_start = value.find('?')?;
    let query_end = value.find('#').unwrap_or(value.len());
    let query = &value[query_start + 1..query_end];
    let start = query_value(query, "line")
        .or_else(|| query_value(query, "lineStart"))
        .and_then(|value| value.parse::<usize>().ok())?;
    let end = query_value(query, "lineEnd").and_then(|value| value.parse::<usize>().ok())?;
    (end >= start).then_some(end - start + 1)
}

fn query_value<'a>(query: &'a str, name: &str) -> Option<&'a str> {
    query.split('&').find_map(|part| {
        let part = part.trim_start_matches(|ch| matches!(ch, '?' | '&'));
        let (key, value) = part.split_once('=')?;
        key.eq_ignore_ascii_case(name).then_some(value)
    })
}

fn strip_hash_anchor_column(value: &str) -> &str {
    let Some(rest) = value.strip_prefix('C').or_else(|| value.strip_prefix('c')) else {
        return value;
    };
    split_leading_digits(rest)
        .map(|(_, after_column)| after_column)
        .unwrap_or(value)
}

fn copied_colon_range_lines(value: &str) -> Option<usize> {
    for (colon_index, _) in value.match_indices(':') {
        let after_colon = &value[colon_index + 1..];
        let Some((start, after_start)) = split_leading_digits(after_colon) else {
            continue;
        };
        let Some(after_dash) = after_start.trim_start().strip_prefix('-') else {
            continue;
        };
        let Some((end, after_end)) = split_leading_digits(after_dash.trim_start()) else {
            continue;
        };
        if end >= start && colon_range_tail_is_structural(after_end) {
            return Some(end - start + 1);
        }
    }
    None
}

fn colon_range_tail_is_structural(value: &str) -> bool {
    let value = value.trim_start();
    value.is_empty()
        || value
            .chars()
            .next()
            .is_some_and(|ch| matches!(ch, ':' | ')' | ']' | '}' | '>' | ',' | ';'))
}

fn split_leading_digits(value: &str) -> Option<(usize, &str)> {
    let digit_end = value
        .char_indices()
        .take_while(|(_, ch)| ch.is_ascii_digit())
        .map(|(index, ch)| index + ch.len_utf8())
        .last()?;
    let number = value[..digit_end].parse::<usize>().ok()?;
    Some((number, &value[digit_end..]))
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
        let nested_manifests = bool_arg(arguments, "nested_manifests");
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

fn snippet_mode_arg(arguments: &Value) -> Result<SnippetMode> {
    let Some(value) =
        optional_string_arg_any(arguments, &["snippet", "snippet_mode", "snippet-mode"])
    else {
        return Ok(SnippetMode::default());
    };
    SnippetMode::parse(&value)
        .ok_or_else(|| anyhow!("snippet mode must be one of: short, medium, block, symbol"))
}

fn is_zero(value: &usize) -> bool {
    *value == 0
}

fn repo_map_read_limit_arg(arguments: &Value) -> Result<usize> {
    bounded_usize_arg(
        arguments,
        "read_limit",
        DEFAULT_REPO_MAP_READ_BATCH_RANGES,
        1,
        Some(MAX_RESULT_READ_BATCH_RANGES),
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
        argument_value(arguments, name),
        &format!("argument {name}"),
        minimum,
        maximum,
    )
}

fn optional_bounded_usize_field_any(
    object: &Value,
    names: &[&str],
    label: &str,
    minimum: usize,
    maximum: Option<usize>,
) -> Result<Option<usize>> {
    for name in names {
        if let Some(value) = bounded_usize_value(
            argument_value(object, name),
            &format!("{label} {name}"),
            minimum,
            maximum,
        )? {
            return Ok(Some(value));
        }
    }
    Ok(None)
}

fn has_any_field(value: &Value, names: &[&str]) -> bool {
    names
        .iter()
        .any(|name| argument_value(value, name).is_some())
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
    argument_value(arguments, name)
        .and_then(Value::as_str)
        .map(String::from)
}

fn argument_value<'a>(arguments: &'a Value, name: &str) -> Option<&'a Value> {
    arguments
        .get(name)
        .or_else(|| kebab_case_alias(name).and_then(|alias| arguments.get(alias)))
}

fn daemon_footprint_summary(index_details: &[Value], shard_manifest_details: &[Value]) -> Value {
    json!({
        "loaded_indexes": index_details.len(),
        "loaded_files": sum_u64_field(index_details, "files"),
        "loaded_index_bytes": sum_u64_field(index_details, "index_bytes"),
        "loaded_source_bytes": sum_u64_field(index_details, "source_bytes"),
        "loaded_content_snapshot_bytes": sum_u64_field(index_details, "content_snapshot_bytes"),
        "loaded_line_offset_bytes": sum_u64_field(index_details, "line_offset_bytes"),
        "loaded_symbols": sum_u64_field(index_details, "symbols"),
        "loaded_posting_entries": sum_u64_field(index_details, "posting_entries"),
        "loaded_compressed_posting_bytes": sum_u64_field(index_details, "compressed_posting_bytes"),
        "disk_missing_indexes": count_bool_field(index_details, "disk_missing"),
        "disk_changed_indexes": count_bool_field(index_details, "disk_changed"),
        "cached_shard_manifests": shard_manifest_details.len(),
        "known_shard_repos": sum_u64_field(shard_manifest_details, "shards"),
        "known_shard_index_bytes": sum_u64_field(shard_manifest_details, "index_bytes"),
        "known_shard_content_snapshot_bytes": sum_u64_field(
            shard_manifest_details,
            "content_snapshot_bytes",
        ),
        "known_shard_line_offset_bytes": sum_u64_field(shard_manifest_details, "line_offset_bytes"),
        "manifest_disk_missing": count_bool_field(shard_manifest_details, "manifest_disk_missing"),
        "manifest_disk_changed": count_bool_field(shard_manifest_details, "manifest_disk_changed"),
    })
}

fn sum_u64_field(items: &[Value], field: &str) -> u64 {
    items
        .iter()
        .filter_map(|item| item.get(field).and_then(Value::as_u64))
        .sum()
}

fn count_bool_field(items: &[Value], field: &str) -> usize {
    items
        .iter()
        .filter(|item| item.get(field).and_then(Value::as_bool).unwrap_or(false))
        .count()
}

fn daemon_default_requests(search_auto_default: &Value) -> Value {
    let target = search_auto_default.get("target").and_then(Value::as_str);
    json!({
        "manifest": daemon_default_request("tools", "tool_manifest", json!({})),
        "agent_guide": daemon_default_request("guide", "agent_guide", json!({})),
        "repo_map": daemon_default_repo_map_request(search_auto_default, target),
        "search": daemon_default_request(
            "search",
            "search_auto",
            json!({
                "query": "symbol:SessionManager token",
                "limit": 10,
                "explain": true
            }),
        ),
        "search_batch": daemon_default_request(
            "searches",
            "search_auto_batch",
            json!({
                "queries": [
                    "symbol:SessionManager token",
                    "path:src token"
                ],
                "limit": 10,
                "explain": true
            }),
        ),
        "query_plan": daemon_default_query_plan_request(search_auto_default, target),
        "note": "Use search_auto without an explicit target when search_auto_default is trusted; use the targeted repo_map and query_plan requests when orienting or diagnosing. Each default request includes jsonl and client_cli for direct terminal use."
    })
}

fn daemon_default_cwd_requests(cwd: &str) -> Value {
    json!({
        "manifest": daemon_default_request("tools", "tool_manifest", json!({})),
        "agent_guide": daemon_default_request("guide", "agent_guide", json!({})),
        "repo_map": daemon_default_request(
            "map",
            "repo_map",
            json!({
                "cwd": cwd,
                "detail": "compact",
                "read_limit": DEFAULT_REPO_MAP_READ_BATCH_RANGES,
                "refresh_if_stale": true
            }),
        ),
        "search": daemon_default_request(
            "search",
            "search_auto",
            json!({
                "cwd": cwd,
                "query": "symbol:SessionManager token",
                "limit": 10,
                "explain": true,
                "refresh_if_stale": true
            }),
        ),
        "search_batch": daemon_default_request(
            "searches",
            "search_auto_batch",
            json!({
                "cwd": cwd,
                "queries": [
                    "symbol:SessionManager token",
                    "path:src token"
                ],
                "limit": 10,
                "explain": true,
                "refresh_if_stale": true
            }),
        ),
        "query_plan": daemon_default_request(
            "plan",
            "search_plan",
            json!({
                "cwd": cwd,
                "query": "symbol:SessionManager missingterm",
                "require_all": true,
                "summary": true,
                "refresh_if_stale": true
            }),
        ),
        "note": "These default requests include cwd and refresh_if_stale so a shared daemon scopes no-target map, search, and query-plan calls to the active checkout and refreshes that shard before use. Each default request includes jsonl and client_cli for direct terminal use."
    })
}

fn daemon_default_repo_map_request(search_auto_default: &Value, target: Option<&str>) -> Value {
    let mut arguments = Map::new();
    arguments.insert("detail".to_string(), json!("compact"));
    arguments.insert(
        "read_limit".to_string(),
        json!(DEFAULT_REPO_MAP_READ_BATCH_RANGES),
    );
    let tool = match (
        search_auto_default.get("surface").and_then(Value::as_str),
        target,
    ) {
        (Some("shards"), Some(index_dir)) => {
            arguments.insert("index_dir".to_string(), json!(index_dir));
            "shard_repo_map"
        }
        (Some("indexed"), Some(index)) => {
            arguments.insert("index".to_string(), json!(index));
            "indexed_repo_map"
        }
        (_, Some(repo)) => {
            arguments.insert("repo".to_string(), json!(repo));
            "repo_map"
        }
        _ => "repo_map",
    };
    daemon_default_request("map", tool, Value::Object(arguments))
}

fn daemon_default_query_plan_request(search_auto_default: &Value, target: Option<&str>) -> Value {
    let mut arguments = Map::new();
    arguments.insert(
        "query".to_string(),
        json!("symbol:SessionManager missingterm"),
    );
    arguments.insert("require_all".to_string(), json!(true));
    arguments.insert("summary".to_string(), json!(true));
    let tool = match (
        search_auto_default.get("surface").and_then(Value::as_str),
        target,
    ) {
        (Some("shards"), Some(index_dir)) => {
            arguments.insert("index_dir".to_string(), json!(index_dir));
            "shard_query_plan"
        }
        (Some("indexed"), Some(index)) => {
            arguments.insert("index".to_string(), json!(index));
            "indexed_query_plan"
        }
        (_, Some(repo)) => {
            arguments.insert("repo".to_string(), json!(repo));
            "search_query_plan"
        }
        _ => "search_query_plan",
    };
    daemon_default_request("plan", tool, Value::Object(arguments))
}

fn daemon_default_request(id: &str, tool: &str, arguments: Value) -> Value {
    serde_json::to_value(ResultToolRequest::with_id(id, tool, arguments))
        .expect("serialize daemon default request")
}

fn optional_string_arg_any(arguments: &Value, names: &[&str]) -> Option<String> {
    names
        .iter()
        .find_map(|name| optional_string_arg(arguments, name))
}

fn optional_positive_usize_arg_any(arguments: &Value, names: &[&str]) -> Result<Option<usize>> {
    for name in names {
        if let Some(value) = optional_positive_usize_arg(arguments, name)? {
            return Ok(Some(value));
        }
    }
    Ok(None)
}

fn optional_string_list_arg(arguments: &Value, name: &str) -> Result<Vec<String>> {
    let Some(value) = argument_value(arguments, name) else {
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

fn optional_string_list_arg_any(arguments: &Value, names: &[&str]) -> Result<Vec<String>> {
    let mut values = Vec::new();
    for name in names {
        values.extend(optional_string_list_arg(arguments, name)?);
    }
    Ok(values)
}

fn normalized_string_list_arg_any(arguments: &Value, names: &[&str]) -> Result<Vec<String>> {
    Ok(optional_string_list_arg_any(arguments, names)?
        .into_iter()
        .map(|value| value.trim_start_matches('.').to_ascii_lowercase())
        .collect())
}

fn symbol_kind_arg_any(arguments: &Value, names: &[&str]) -> Option<String> {
    optional_string_arg_any(arguments, names).map(|value| normalize_symbol_kind(&value))
}

fn symbol_kind_list_arg_any(arguments: &Value, names: &[&str]) -> Result<Vec<String>> {
    Ok(optional_string_list_arg_any(arguments, names)?
        .into_iter()
        .map(|value| normalize_symbol_kind(&value))
        .collect())
}

fn language_list_arg_any(arguments: &Value, names: &[&str]) -> Result<Vec<String>> {
    Ok(optional_string_list_arg_any(arguments, names)?
        .into_iter()
        .map(|value| normalize_language_filter(&value))
        .collect())
}

fn search_filters(arguments: &Value, allow_repo_alias: bool) -> Result<SearchFilters> {
    Ok(SearchFilters {
        path: optional_string_arg_any(arguments, &["path", "dir", "directory", "folder"]),
        language: optional_string_arg_any(arguments, &["language", "lang"])
            .map(|value| normalize_language_filter(&value)),
        extension: optional_string_arg_any(arguments, &["extension", "ext"]),
        symbol: optional_string_arg(arguments, "symbol"),
        symbol_kind: symbol_kind_arg_any(arguments, &["symbol_kind", "kind", "type"]),
        branch: optional_string_arg_any(arguments, &["branch", "git_branch"]),
        origin: optional_string_arg_any(arguments, &["origin", "remote", "remote_origin"]),
        dependency: optional_string_arg_any(arguments, &["dependency", "dep", "deps"])
            .map(|value| value.to_ascii_lowercase()),
        import: optional_string_arg_any(
            arguments,
            &["import", "imports", "module", "modules", "use", "uses"],
        )
        .map(|value| value.to_ascii_lowercase()),
        file: optional_string_arg_any(arguments, &["file", "filename", "file_name"]),
        repo: if allow_repo_alias {
            optional_string_arg_any(arguments, &["repo", "repo_filter"])
        } else {
            optional_string_arg(arguments, "repo_filter")
        },
        test: arguments.get("test").and_then(Value::as_bool),
        generated: arguments.get("generated").and_then(Value::as_bool),
        code: arguments.get("code").and_then(Value::as_bool),
        target_line: optional_positive_usize_arg_any(arguments, &["line", "target_line"])?,
        snippet: snippet_mode_arg(arguments)?,
        explain: arguments
            .get("explain")
            .and_then(Value::as_bool)
            .unwrap_or(false),
        require_all: bool_arg(arguments, "require_all") && !bool_arg(arguments, "any_terms"),
        match_any: bool_arg(arguments, "any_terms"),
        exclude_file: optional_string_list_arg_any(
            arguments,
            &["exclude_file", "exclude_filename", "exclude_file_name"],
        )?,
        exclude_path: optional_string_list_arg_any(
            arguments,
            &[
                "exclude_path",
                "exclude_dir",
                "exclude_directory",
                "exclude_folder",
            ],
        )?,
        exclude_language: language_list_arg_any(arguments, &["exclude_language", "exclude_lang"])?,
        exclude_extension: normalized_string_list_arg_any(
            arguments,
            &["exclude_extension", "exclude_ext"],
        )?,
        exclude_symbol: optional_string_list_arg(arguments, "exclude_symbol")?,
        exclude_symbol_kind: symbol_kind_list_arg_any(
            arguments,
            &["exclude_symbol_kind", "exclude_kind", "exclude_type"],
        )?,
        exclude_repo: optional_string_list_arg(arguments, "exclude_repo")?,
        exclude_branch: optional_string_list_arg_any(
            arguments,
            &["exclude_branch", "exclude_git_branch"],
        )?,
        exclude_origin: optional_string_list_arg_any(
            arguments,
            &["exclude_origin", "exclude_remote", "exclude_remote_origin"],
        )?,
        exclude_dependency: normalized_string_list_arg_any(
            arguments,
            &["exclude_dependency", "exclude_dep", "exclude_deps"],
        )?,
        exclude_import: normalized_string_list_arg_any(
            arguments,
            &[
                "exclude_import",
                "exclude_imports",
                "exclude_module",
                "exclude_modules",
                "exclude_use",
                "exclude_uses",
            ],
        )?,
        exclude_content: optional_string_list_arg_any(
            arguments,
            &["exclude_content", "exclude_text", "exclude_term"],
        )?,
        ..SearchFilters::default()
    })
}

fn related_symbol_filters(arguments: &Value, allow_repo_alias: bool) -> Result<SearchFilters> {
    let mut filters = search_filters(arguments, allow_repo_alias)?;
    // In related-symbol tools, `path` names the anchor file. Keep directory scoping in
    // the query string (for example `query:"dir:src kind:struct"`) to avoid ambiguity.
    filters.path = None;
    Ok(filters)
}

fn related_file_filters(arguments: &Value, allow_repo_alias: bool) -> Result<SearchFilters> {
    let mut filters = search_filters(arguments, allow_repo_alias)?;
    // In related-file tools, `path` names the anchor file; use exclude_path or other
    // structured filters to scope which related files can be returned.
    filters.path = None;
    Ok(filters)
}
