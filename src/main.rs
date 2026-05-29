use ahash::AHashSet as HashSet;
use anyhow::{Result, bail};
use clap::{Args, Parser, Subcommand, ValueEnum};
use orient::discover::{
    DiscoverOptions, DiscoverySelectionSummary, discover_repos, discovery_selection_summary,
};
use orient::fast_index::{FastIndex, RefreshStats};
use orient::query::{merge_filters, normalize_symbol_kind, parse_query};
use orient::repo_index::{
    DEFAULT_REPO_MAP_READ_BATCH_RANGES, MAX_READ_RANGE_LINES, MAX_RESULT_READ_BATCH_RANGES,
    QueryPlan, QueryPlanFilter, RangeScope, RepoIndexer, RepoMapDetail, ResultToolRequest,
    SearchFilters, SearchResult, SnippetMode, SymbolLookupResult,
    attach_repo_map_read_batch_request_with_limit, attach_result_context,
    attach_result_read_requests, attach_result_related_requests,
    attach_result_related_symbol_requests, normalize_language_filter,
    query_plan_filter_field_present, read_file_range, read_file_range_scoped,
    related_file_lookup_results, related_symbol_lookup_results, result_read_batch_request,
    result_value_read_batch_request, search_repo_fast_filtered, symbol_lookup_read_batch_request,
    symbol_lookup_results,
};
use orient::server::{
    DEFAULT_MAX_CACHED_INDEXES, MAX_BATCH_QUERIES, MAX_BATCH_RANGES, MAX_BATCH_READ_LINES,
    ToolRequest, ToolRuntime, agent_guide, agent_instructions, mcp_tool_manifest,
    retarget_client_cli_commands, serve_jsonl, serve_jsonl_stream_with_client_command, serve_mcp,
    serve_tcp, tcp_client_command, tool_manifest, unix_client_command,
};
use orient::shards::{
    ShardFreshness, ShardQueryPlan, build_shards_with_force, ensure_shards, find_shard_symbol,
    read_shard_range, read_shard_range_scoped, refresh_shards, related_shard_files_filtered,
    related_shard_symbols_filtered, search_shards, shard_query_plans, shard_repo_maps,
    shard_status,
};
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};
use std::any::Any;
use std::env;
use std::fs;
use std::io::{self, BufRead, BufReader, Read, Write};
use std::net::{TcpListener, TcpStream};
#[cfg(unix)]
use std::os::unix::fs::FileTypeExt;
#[cfg(unix)]
use std::os::unix::net::{UnixListener, UnixStream};
use std::path::{Path, PathBuf};
use std::str::FromStr;
use std::sync::Arc;
use std::time::Instant;

const DEFAULT_DAEMON_ADDR: &str = "127.0.0.1:8796";
const DEFAULT_CLI_READ_RANGE_LINES: usize = 80;

#[derive(Debug, Parser)]
#[command(name = "orient")]
#[command(about = "Fast local code search for coding agents")]
struct Cli {
    #[arg(long, global = true, hide = true)]
    _json: bool,
    #[command(subcommand)]
    command: Commands,
}

#[derive(Debug, Subcommand)]
enum Commands {
    DiscoverRepos {
        #[arg(long, default_value = ".")]
        root: PathBuf,
        #[arg(long, default_value_t = 4)]
        max_depth: usize,
        #[arg(long, default_value_t = 500)]
        limit: usize,
        #[arg(long)]
        family_limit: Option<usize>,
        #[arg(long)]
        git_metadata: bool,
        #[arg(long)]
        tracked_files: bool,
        #[arg(long)]
        nested_manifests: bool,
    },
    Index {
        #[arg(long, default_value = ".")]
        repo: PathBuf,
        #[arg(long)]
        output: PathBuf,
    },
    RefreshIndex {
        #[arg(long, default_value = ".")]
        repo: PathBuf,
        #[arg(long)]
        index: PathBuf,
    },
    IndexStatus {
        #[arg(long)]
        index: PathBuf,
    },
    EnsureIndex {
        #[arg(long, default_value = ".")]
        repo: PathBuf,
        #[arg(long)]
        index: PathBuf,
    },
    IndexShards {
        #[arg(long = "repo")]
        repos: Vec<PathBuf>,
        #[arg(long = "discover-root")]
        discover_roots: Vec<PathBuf>,
        #[arg(long, default_value_t = 4)]
        max_depth: usize,
        #[arg(long, default_value_t = 500)]
        discover_limit: usize,
        #[arg(long)]
        family_limit: Option<usize>,
        #[arg(long)]
        nested_manifests: bool,
        #[arg(long)]
        force: bool,
        #[arg(long)]
        output_dir: PathBuf,
    },
    RefreshShards {
        #[arg(long)]
        index_dir: PathBuf,
    },
    ShardStatus {
        #[arg(long)]
        index_dir: PathBuf,
        #[arg(long)]
        summary: bool,
    },
    EnsureShards {
        #[arg(long = "repo")]
        repos: Vec<PathBuf>,
        #[arg(long = "discover-root")]
        discover_roots: Vec<PathBuf>,
        #[arg(long, default_value_t = 4)]
        max_depth: usize,
        #[arg(long, default_value_t = 500)]
        discover_limit: usize,
        #[arg(long)]
        family_limit: Option<usize>,
        #[arg(long)]
        nested_manifests: bool,
        #[arg(long)]
        output_dir: PathBuf,
    },
    SearchShards {
        #[arg(long)]
        index_dir: PathBuf,
        #[arg(allow_hyphen_values = true, required_unless_present = "query_arg")]
        query: Option<String>,
        #[arg(long = "query", value_name = "QUERY", conflicts_with = "query")]
        query_arg: Option<String>,
        #[arg(long, default_value_t = 10)]
        limit: usize,
        #[arg(long = "repo")]
        repo: Option<String>,
        #[command(flatten)]
        filters: CommonSearchArgs,
        #[arg(long, default_value_t = 0)]
        context_lines: usize,
        #[arg(long)]
        refresh_if_stale: bool,
    },
    SearchShardsBatch {
        #[arg(long)]
        index_dir: PathBuf,
        #[arg(required = true, allow_hyphen_values = true)]
        queries: Vec<String>,
        #[arg(long, default_value_t = 10)]
        limit: usize,
        #[arg(long = "repo")]
        repo: Option<String>,
        #[command(flatten)]
        filters: CommonSearchArgs,
        #[arg(long, default_value_t = 0)]
        context_lines: usize,
        #[arg(long)]
        refresh_if_stale: bool,
    },
    ShardPlan {
        #[arg(long)]
        index_dir: PathBuf,
        #[arg(allow_hyphen_values = true, required_unless_present = "query_arg")]
        query: Option<String>,
        #[arg(long = "query", value_name = "QUERY", conflicts_with = "query")]
        query_arg: Option<String>,
        #[arg(long = "repo")]
        repo: Option<String>,
        #[command(flatten)]
        filters: CommonSearchArgs,
        #[arg(long)]
        refresh_if_stale: bool,
    },
    ShardPlanBatch {
        #[arg(long)]
        index_dir: PathBuf,
        #[arg(required = true, allow_hyphen_values = true)]
        queries: Vec<String>,
        #[arg(long = "repo")]
        repo: Option<String>,
        #[command(flatten)]
        filters: CommonSearchArgs,
        #[arg(long)]
        refresh_if_stale: bool,
    },
    #[command(alias = "open-shard-range")]
    ReadShardRange {
        #[arg(long)]
        index_dir: PathBuf,
        #[arg(value_name = "PATH", required_unless_present = "path_arg")]
        path: Option<String>,
        #[arg(long = "path", value_name = "PATH", conflicts_with = "path")]
        path_arg: Option<String>,
        #[arg(long, default_value_t = 1)]
        start: usize,
        #[arg(long, default_value_t = DEFAULT_CLI_READ_RANGE_LINES)]
        lines: usize,
        #[arg(long, value_enum, default_value_t = ReadScopeArg::Exact)]
        scope: ReadScopeArg,
    },
    #[command(alias = "open-shard-ranges")]
    ReadShardRanges {
        #[arg(long)]
        index_dir: PathBuf,
        #[arg(long = "range", value_name = "PATH:START:LINES[:SCOPE]")]
        ranges: Vec<CliRangeSpec>,
        paths: Vec<String>,
        #[arg(long, default_value_t = 1)]
        start: usize,
        #[arg(long, default_value_t = DEFAULT_CLI_READ_RANGE_LINES)]
        lines: usize,
        #[arg(long, value_enum, default_value_t = ReadScopeArg::Exact)]
        scope: ReadScopeArg,
    },
    ShardSymbol {
        #[arg(long)]
        index_dir: PathBuf,
        name: String,
        #[arg(long, default_value_t = 10)]
        limit: usize,
        #[arg(long = "repo")]
        repo: Option<String>,
        #[command(flatten)]
        filters: CommonSearchArgs,
    },
    ShardSymbolBatch {
        #[arg(long)]
        index_dir: PathBuf,
        #[arg(required = true)]
        names: Vec<String>,
        #[arg(long, default_value_t = 10)]
        limit: usize,
        #[arg(long = "repo")]
        repo: Option<String>,
        #[command(flatten)]
        filters: CommonSearchArgs,
    },
    ShardMap {
        #[arg(long)]
        index_dir: PathBuf,
        #[arg(long, default_value_t = 50)]
        symbols: usize,
        #[arg(long, default_value_t = 50)]
        tests: usize,
        #[arg(long = "repo")]
        repo: Option<String>,
        #[arg(long, alias = "git-branch", alias = "git_branch")]
        branch: Option<String>,
        #[arg(
            long,
            alias = "remote",
            alias = "remote-origin",
            alias = "remote_origin"
        )]
        origin: Option<String>,
        #[arg(long, default_value = "compact", value_parser = ["compact", "full"])]
        detail: String,
        #[arg(long = "read-limit", default_value_t = DEFAULT_REPO_MAP_READ_BATCH_RANGES)]
        read_limit: usize,
        #[arg(long, default_value = "json", value_parser = ["json"])]
        format: String,
    },
    Brief {
        #[arg(long, default_value = ".")]
        repo: PathBuf,
        #[arg(long, default_value = "compact", value_parser = ["compact", "full"])]
        detail: String,
    },
    RepoMap {
        #[arg(long, default_value = ".")]
        repo: PathBuf,
        #[arg(long, conflicts_with = "index_dir")]
        index: Option<PathBuf>,
        #[arg(long, conflicts_with = "index")]
        index_dir: Option<PathBuf>,
        #[arg(long, default_value_t = 50)]
        symbols: usize,
        #[arg(long, default_value_t = 50)]
        tests: usize,
        #[arg(long = "repo-filter")]
        repo_filter: Option<String>,
        #[arg(long, alias = "git-branch", alias = "git_branch")]
        branch: Option<String>,
        #[arg(
            long,
            alias = "remote",
            alias = "remote-origin",
            alias = "remote_origin"
        )]
        origin: Option<String>,
        #[arg(long, default_value = "compact", value_parser = ["compact", "full"])]
        detail: String,
        #[arg(long = "read-limit", default_value_t = DEFAULT_REPO_MAP_READ_BATCH_RANGES)]
        read_limit: usize,
        #[arg(long, default_value = "json", value_parser = ["json"])]
        format: String,
    },
    IndexMap {
        #[arg(long)]
        index: PathBuf,
        #[arg(long, default_value_t = 50)]
        symbols: usize,
        #[arg(long, default_value_t = 50)]
        tests: usize,
        #[arg(long, default_value = "compact", value_parser = ["compact", "full"])]
        detail: String,
        #[arg(long = "read-limit", default_value_t = DEFAULT_REPO_MAP_READ_BATCH_RANGES)]
        read_limit: usize,
        #[arg(long, default_value = "json", value_parser = ["json"])]
        format: String,
    },
    IndexPlan {
        #[arg(long)]
        index: PathBuf,
        #[arg(allow_hyphen_values = true, required_unless_present = "query_arg")]
        query: Option<String>,
        #[arg(long = "query", value_name = "QUERY", conflicts_with = "query")]
        query_arg: Option<String>,
        #[arg(long = "repo-filter")]
        repo_filter: Option<String>,
        #[command(flatten)]
        filters: CommonSearchArgs,
        #[arg(long)]
        refresh_if_stale: bool,
    },
    IndexPlanBatch {
        #[arg(long)]
        index: PathBuf,
        #[arg(required = true, allow_hyphen_values = true)]
        queries: Vec<String>,
        #[arg(long = "repo-filter")]
        repo_filter: Option<String>,
        #[command(flatten)]
        filters: CommonSearchArgs,
        #[arg(long)]
        refresh_if_stale: bool,
    },
    SearchPlan {
        #[arg(long, default_value = ".")]
        repo: PathBuf,
        #[arg(long, conflicts_with = "index_dir")]
        index: Option<PathBuf>,
        #[arg(long, conflicts_with = "index")]
        index_dir: Option<PathBuf>,
        #[arg(allow_hyphen_values = true, required_unless_present = "query_arg")]
        query: Option<String>,
        #[arg(long = "query", value_name = "QUERY", conflicts_with = "query")]
        query_arg: Option<String>,
        #[arg(long = "repo-filter")]
        repo_filter: Option<String>,
        #[command(flatten)]
        filters: CommonSearchArgs,
        #[arg(long)]
        refresh_if_stale: bool,
    },
    SearchPlanBatch {
        #[arg(long, default_value = ".")]
        repo: PathBuf,
        #[arg(long, conflicts_with = "index_dir")]
        index: Option<PathBuf>,
        #[arg(long, conflicts_with = "index")]
        index_dir: Option<PathBuf>,
        #[arg(required = true, allow_hyphen_values = true)]
        queries: Vec<String>,
        #[arg(long = "repo-filter")]
        repo_filter: Option<String>,
        #[command(flatten)]
        filters: CommonSearchArgs,
        #[arg(long)]
        refresh_if_stale: bool,
    },
    #[command(alias = "open-range")]
    ReadRange {
        #[arg(long, default_value = ".")]
        repo: PathBuf,
        #[arg(long, conflicts_with = "index_dir")]
        index: Option<PathBuf>,
        #[arg(long, conflicts_with = "index")]
        index_dir: Option<PathBuf>,
        #[arg(value_name = "PATH", required_unless_present = "path_arg")]
        path: Option<String>,
        #[arg(long = "path", value_name = "PATH", conflicts_with = "path")]
        path_arg: Option<String>,
        #[arg(long, default_value_t = 1)]
        start: usize,
        #[arg(long, default_value_t = 80)]
        lines: usize,
        #[arg(long, value_enum, default_value_t = ReadScopeArg::Exact)]
        scope: ReadScopeArg,
    },
    #[command(alias = "open-ranges")]
    ReadRanges {
        #[arg(long, default_value = ".")]
        repo: PathBuf,
        #[arg(long, conflicts_with = "index_dir")]
        index: Option<PathBuf>,
        #[arg(long, conflicts_with = "index")]
        index_dir: Option<PathBuf>,
        #[arg(long = "range", value_name = "PATH:START:LINES[:SCOPE]")]
        ranges: Vec<CliRangeSpec>,
        paths: Vec<String>,
        #[arg(long, default_value_t = 1)]
        start: usize,
        #[arg(long, default_value_t = 80)]
        lines: usize,
        #[arg(long, value_enum, default_value_t = ReadScopeArg::Exact)]
        scope: ReadScopeArg,
    },
    Search {
        #[arg(long, default_value = ".")]
        repo: PathBuf,
        #[arg(long, conflicts_with = "index_dir")]
        index: Option<PathBuf>,
        #[arg(long, conflicts_with = "index")]
        index_dir: Option<PathBuf>,
        #[arg(allow_hyphen_values = true, required_unless_present = "query_arg")]
        query: Option<String>,
        #[arg(long = "query", value_name = "QUERY", conflicts_with = "query")]
        query_arg: Option<String>,
        #[arg(long, default_value_t = 10)]
        limit: usize,
        #[arg(long = "repo-filter")]
        repo_filter: Option<String>,
        #[command(flatten)]
        filters: CommonSearchArgs,
        #[arg(long, default_value_t = 0)]
        context_lines: usize,
        #[arg(long)]
        refresh_if_stale: bool,
    },
    SearchAuto {
        #[arg(allow_hyphen_values = true, required_unless_present = "query_arg")]
        query: Option<String>,
        #[arg(long = "query", value_name = "QUERY", conflicts_with = "query")]
        query_arg: Option<String>,
        #[arg(long)]
        repo: Option<PathBuf>,
        #[arg(long)]
        index: Option<PathBuf>,
        #[arg(long)]
        index_dir: Option<PathBuf>,
        #[arg(long, default_value_t = 10)]
        limit: usize,
        #[arg(long = "repo-filter")]
        repo_filter: Option<String>,
        #[command(flatten)]
        filters: CommonSearchArgs,
        #[arg(long, default_value_t = 0)]
        context_lines: usize,
        #[arg(long)]
        refresh_if_stale: bool,
        #[arg(long)]
        diagnose: bool,
        #[arg(long)]
        retry_if_empty: bool,
        #[arg(long = "daemon-addr", visible_alias = "addr", default_value = DEFAULT_DAEMON_ADDR)]
        daemon_addr: String,
        #[arg(long)]
        no_daemon: bool,
    },
    SearchAutoBatch {
        #[arg(required = true, allow_hyphen_values = true)]
        queries: Vec<String>,
        #[arg(long)]
        repo: Option<PathBuf>,
        #[arg(long)]
        index: Option<PathBuf>,
        #[arg(long)]
        index_dir: Option<PathBuf>,
        #[arg(long, default_value_t = 10)]
        limit: usize,
        #[arg(long = "repo-filter")]
        repo_filter: Option<String>,
        #[command(flatten)]
        filters: CommonSearchArgs,
        #[arg(long, default_value_t = 0)]
        context_lines: usize,
        #[arg(long)]
        refresh_if_stale: bool,
        #[arg(long)]
        diagnose: bool,
        #[arg(long)]
        retry_if_empty: bool,
        #[arg(long = "daemon-addr", visible_alias = "addr", default_value = DEFAULT_DAEMON_ADDR)]
        daemon_addr: String,
        #[arg(long)]
        no_daemon: bool,
    },
    SearchBatch {
        #[arg(long, default_value = ".")]
        repo: PathBuf,
        #[arg(long, conflicts_with = "index_dir")]
        index: Option<PathBuf>,
        #[arg(long, conflicts_with = "index")]
        index_dir: Option<PathBuf>,
        #[arg(required = true, allow_hyphen_values = true)]
        queries: Vec<String>,
        #[arg(long, default_value_t = 10)]
        limit: usize,
        #[arg(long = "repo-filter")]
        repo_filter: Option<String>,
        #[command(flatten)]
        filters: CommonSearchArgs,
        #[arg(long, default_value_t = 0)]
        context_lines: usize,
        #[arg(long)]
        refresh_if_stale: bool,
    },
    IndexedSearch {
        #[arg(long)]
        index: PathBuf,
        #[arg(allow_hyphen_values = true, required_unless_present = "query_arg")]
        query: Option<String>,
        #[arg(long = "query", value_name = "QUERY", conflicts_with = "query")]
        query_arg: Option<String>,
        #[arg(long, default_value_t = 10)]
        limit: usize,
        #[arg(long = "repo-filter")]
        repo_filter: Option<String>,
        #[command(flatten)]
        filters: CommonSearchArgs,
        #[arg(long, default_value_t = 0)]
        context_lines: usize,
        #[arg(long)]
        refresh_if_stale: bool,
    },
    IndexedSearchBatch {
        #[arg(long)]
        index: PathBuf,
        #[arg(required = true, allow_hyphen_values = true)]
        queries: Vec<String>,
        #[arg(long, default_value_t = 10)]
        limit: usize,
        #[arg(long = "repo-filter")]
        repo_filter: Option<String>,
        #[command(flatten)]
        filters: CommonSearchArgs,
        #[arg(long, default_value_t = 0)]
        context_lines: usize,
        #[arg(long)]
        refresh_if_stale: bool,
    },
    #[command(alias = "open-index-range")]
    ReadIndexRange {
        #[arg(long)]
        index: PathBuf,
        #[arg(value_name = "PATH", required_unless_present = "path_arg")]
        path: Option<String>,
        #[arg(long = "path", value_name = "PATH", conflicts_with = "path")]
        path_arg: Option<String>,
        #[arg(long, default_value_t = 1)]
        start: usize,
        #[arg(long, default_value_t = 80)]
        lines: usize,
        #[arg(long, value_enum, default_value_t = ReadScopeArg::Exact)]
        scope: ReadScopeArg,
    },
    #[command(alias = "open-index-ranges")]
    ReadIndexRanges {
        #[arg(long)]
        index: PathBuf,
        #[arg(long = "range", value_name = "PATH:START:LINES[:SCOPE]")]
        ranges: Vec<CliRangeSpec>,
        paths: Vec<String>,
        #[arg(long, default_value_t = 1)]
        start: usize,
        #[arg(long, default_value_t = 80)]
        lines: usize,
        #[arg(long, value_enum, default_value_t = ReadScopeArg::Exact)]
        scope: ReadScopeArg,
    },
    Symbol {
        #[arg(long, default_value = ".")]
        repo: PathBuf,
        #[arg(long, conflicts_with = "index_dir")]
        index: Option<PathBuf>,
        #[arg(long, conflicts_with = "index")]
        index_dir: Option<PathBuf>,
        name: String,
        #[arg(long, default_value_t = 10)]
        limit: usize,
        #[arg(long = "repo-filter")]
        repo_filter: Option<String>,
        #[command(flatten)]
        filters: CommonSearchArgs,
    },
    SymbolBatch {
        #[arg(long, default_value = ".")]
        repo: PathBuf,
        #[arg(long, conflicts_with = "index_dir")]
        index: Option<PathBuf>,
        #[arg(long, conflicts_with = "index")]
        index_dir: Option<PathBuf>,
        #[arg(required = true)]
        names: Vec<String>,
        #[arg(long, default_value_t = 10)]
        limit: usize,
        #[arg(long = "repo-filter")]
        repo_filter: Option<String>,
        #[command(flatten)]
        filters: CommonSearchArgs,
    },
    IndexSymbol {
        #[arg(long)]
        index: PathBuf,
        name: String,
        #[arg(long, default_value_t = 10)]
        limit: usize,
        #[arg(long = "repo")]
        repo_filter: Option<String>,
        #[command(flatten)]
        filters: CommonSearchArgs,
    },
    IndexSymbolBatch {
        #[arg(long)]
        index: PathBuf,
        #[arg(required = true)]
        names: Vec<String>,
        #[arg(long, default_value_t = 10)]
        limit: usize,
        #[arg(long = "repo")]
        repo_filter: Option<String>,
        #[command(flatten)]
        filters: CommonSearchArgs,
    },
    Related {
        #[arg(long, default_value = ".")]
        repo: PathBuf,
        #[arg(long, conflicts_with = "index_dir")]
        index: Option<PathBuf>,
        #[arg(long, conflicts_with = "index")]
        index_dir: Option<PathBuf>,
        #[arg(value_name = "PATH", required_unless_present = "path_arg")]
        path: Option<String>,
        #[arg(long = "path", value_name = "PATH", conflicts_with = "path")]
        path_arg: Option<String>,
        #[arg(long, default_value_t = 10)]
        limit: usize,
        #[arg(long)]
        include_read_batch: bool,
        #[command(flatten)]
        filters: RelatedSymbolFilterArgs,
    },
    RelatedIndex {
        #[arg(long)]
        index: PathBuf,
        #[arg(value_name = "PATH", required_unless_present = "path_arg")]
        path: Option<String>,
        #[arg(long = "path", value_name = "PATH", conflicts_with = "path")]
        path_arg: Option<String>,
        #[arg(long, default_value_t = 10)]
        limit: usize,
        #[arg(long)]
        include_read_batch: bool,
        #[command(flatten)]
        filters: RelatedSymbolFilterArgs,
    },
    RelatedShard {
        #[arg(long)]
        index_dir: PathBuf,
        #[arg(value_name = "PATH", required_unless_present = "path_arg")]
        path: Option<String>,
        #[arg(long = "path", value_name = "PATH", conflicts_with = "path")]
        path_arg: Option<String>,
        #[arg(long, default_value_t = 10)]
        limit: usize,
        #[arg(long)]
        include_read_batch: bool,
        #[command(flatten)]
        filters: RelatedSymbolFilterArgs,
    },
    RelatedSymbols {
        #[arg(long, default_value = ".")]
        repo: PathBuf,
        #[arg(long, conflicts_with = "index_dir")]
        index: Option<PathBuf>,
        #[arg(long, conflicts_with = "index")]
        index_dir: Option<PathBuf>,
        #[arg(long)]
        path: Option<String>,
        #[arg(long)]
        query: Option<String>,
        #[arg(long, default_value_t = 10)]
        limit: usize,
        #[arg(long)]
        include_read_batch: bool,
        #[command(flatten)]
        filters: RelatedSymbolFilterArgs,
    },
    RelatedShardSymbols {
        #[arg(long)]
        index_dir: PathBuf,
        #[arg(value_name = "PATH", required_unless_present = "path_arg")]
        path: Option<String>,
        #[arg(long = "path", value_name = "PATH", conflicts_with = "path")]
        path_arg: Option<String>,
        #[arg(long)]
        query: Option<String>,
        #[arg(long, default_value_t = 10)]
        limit: usize,
        #[arg(long)]
        include_read_batch: bool,
        #[command(flatten)]
        filters: RelatedSymbolFilterArgs,
    },
    RelatedIndexSymbols {
        #[arg(long)]
        index: PathBuf,
        #[arg(long)]
        path: Option<String>,
        #[arg(long)]
        query: Option<String>,
        #[arg(long, default_value_t = 10)]
        limit: usize,
        #[arg(long)]
        include_read_batch: bool,
        #[command(flatten)]
        filters: RelatedSymbolFilterArgs,
    },
    BenchSearch {
        #[arg(long, default_value = ".")]
        repo: PathBuf,
        #[arg(long)]
        index: Option<PathBuf>,
        #[arg(long, value_enum, default_value = "auto")]
        mode: BenchSearchMode,
        #[arg(long, default_value_t = 10)]
        runs: usize,
        #[arg(long, default_value_t = 3)]
        warmup: usize,
        #[arg(long, default_value_t = 10)]
        limit: usize,
        #[arg(long = "repo-filter")]
        repo_filter: Option<String>,
        #[command(flatten)]
        filters: CommonSearchArgs,
        #[arg(long)]
        fail_p95_ms: Option<f64>,
        #[arg(long)]
        baseline: Option<PathBuf>,
        #[arg(long)]
        allow_baseline_mode_mismatch: bool,
        #[arg(long)]
        require_faster_than_baseline: bool,
        #[arg(long)]
        write_baseline: Option<PathBuf>,
        #[arg(long, default_value_t = 0.25)]
        max_p95_regression: f64,
        #[arg(long = "query", value_name = "QUERY")]
        query_args: Vec<String>,
        #[arg(required_unless_present = "query_args", allow_hyphen_values = true)]
        queries: Vec<String>,
    },
    BenchShards {
        #[arg(long)]
        index_dir: PathBuf,
        #[arg(long, conflicts_with = "cold")]
        cached: bool,
        #[arg(long)]
        cold: bool,
        #[arg(long, default_value_t = 10)]
        runs: usize,
        #[arg(long, default_value_t = 3)]
        warmup: usize,
        #[arg(long, default_value_t = 10)]
        limit: usize,
        #[arg(long = "repo")]
        repo: Option<String>,
        #[command(flatten)]
        filters: CommonSearchArgs,
        #[arg(long)]
        fail_p95_ms: Option<f64>,
        #[arg(long)]
        baseline: Option<PathBuf>,
        #[arg(long)]
        write_baseline: Option<PathBuf>,
        #[arg(long, default_value_t = 0.25)]
        max_p95_regression: f64,
        #[arg(long = "query", value_name = "QUERY")]
        query_args: Vec<String>,
        #[arg(required_unless_present = "query_args", allow_hyphen_values = true)]
        queries: Vec<String>,
    },
    ToolManifest,
    McpManifest,
    AgentGuide {
        #[arg(long)]
        repo: Option<String>,
        #[arg(long)]
        index: Option<String>,
        #[arg(long)]
        index_dir: Option<String>,
        #[arg(long, default_value = "127.0.0.1:8796")]
        addr: String,
        #[arg(long, value_enum, default_value_t = AgentProfileArg::Generic)]
        profile: AgentProfileArg,
    },
    AgentInstructions {
        #[arg(long)]
        repo: Option<String>,
        #[arg(long)]
        index: Option<String>,
        #[arg(long)]
        index_dir: Option<String>,
        #[arg(long, default_value = "127.0.0.1:8796")]
        addr: String,
        #[arg(long, value_enum, default_value_t = AgentProfileArg::Generic)]
        profile: AgentProfileArg,
    },
    DaemonStatus {
        #[arg(long)]
        socket: Option<PathBuf>,
        #[arg(long, default_value = DEFAULT_DAEMON_ADDR)]
        addr: Option<String>,
        #[arg(long, default_value = "summary", value_parser = ["summary", "json"])]
        format: String,
    },
    Doctor {
        #[arg(long, default_value = ".")]
        repo: PathBuf,
        #[arg(long)]
        index: Option<PathBuf>,
        #[arg(long = "index-dir")]
        index_dir: Option<PathBuf>,
        #[arg(long)]
        socket: Option<PathBuf>,
        #[arg(long, default_value = DEFAULT_DAEMON_ADDR)]
        addr: Option<String>,
        #[arg(long, default_value = "text", value_parser = ["text", "json"])]
        format: String,
        #[arg(long)]
        strict: bool,
    },
    ServeJsonl,
    ServeMcp,
    ServeTcp {
        #[arg(long, default_value = "127.0.0.1:8796")]
        addr: String,
        #[arg(long = "index")]
        indexes: Vec<PathBuf>,
        #[arg(long = "index-dir")]
        index_dirs: Vec<PathBuf>,
        #[arg(long = "warm-index-dir")]
        warm_index_dirs: Vec<PathBuf>,
        #[arg(long, default_value_t = DEFAULT_MAX_CACHED_INDEXES)]
        max_cached_indexes: usize,
        #[arg(long = "ensure-shards-dir")]
        ensure_shard_dirs: Vec<PathBuf>,
        #[arg(long = "repo")]
        repos: Vec<PathBuf>,
        #[arg(long = "discover-root")]
        discover_roots: Vec<PathBuf>,
        #[arg(long, default_value_t = 4)]
        max_depth: usize,
        #[arg(long, default_value_t = 500)]
        discover_limit: usize,
        #[arg(long)]
        family_limit: Option<usize>,
        #[arg(long)]
        nested_manifests: bool,
    },
    #[cfg(unix)]
    ServeUnix {
        #[arg(long)]
        socket: PathBuf,
        #[arg(long = "index")]
        indexes: Vec<PathBuf>,
        #[arg(long = "index-dir")]
        index_dirs: Vec<PathBuf>,
        #[arg(long = "warm-index-dir")]
        warm_index_dirs: Vec<PathBuf>,
        #[arg(long, default_value_t = DEFAULT_MAX_CACHED_INDEXES)]
        max_cached_indexes: usize,
        #[arg(long = "ensure-shards-dir")]
        ensure_shard_dirs: Vec<PathBuf>,
        #[arg(long = "repo")]
        repos: Vec<PathBuf>,
        #[arg(long = "discover-root")]
        discover_roots: Vec<PathBuf>,
        #[arg(long, default_value_t = 4)]
        max_depth: usize,
        #[arg(long, default_value_t = 500)]
        discover_limit: usize,
        #[arg(long)]
        family_limit: Option<usize>,
        #[arg(long)]
        nested_manifests: bool,
    },
    ClientJsonl {
        #[arg(long)]
        socket: Option<PathBuf>,
        #[arg(long, default_value = DEFAULT_DAEMON_ADDR)]
        addr: Option<String>,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
enum BenchSearchMode {
    Auto,
    Fallback,
    Indexed,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
enum ReadScopeArg {
    Exact,
    Symbol,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
enum AgentProfileArg {
    Generic,
    Codex,
    Claude,
    Amp,
}

impl AgentProfileArg {
    fn as_str(self) -> &'static str {
        match self {
            AgentProfileArg::Generic => "generic",
            AgentProfileArg::Codex => "codex",
            AgentProfileArg::Claude => "claude",
            AgentProfileArg::Amp => "amp",
        }
    }
}

impl From<ReadScopeArg> for RangeScope {
    fn from(value: ReadScopeArg) -> Self {
        match value {
            ReadScopeArg::Exact => RangeScope::Exact,
            ReadScopeArg::Symbol => RangeScope::Symbol,
        }
    }
}

#[derive(Debug, Clone, Args)]
struct CommonSearchArgs {
    #[arg(long, alias = "dir", alias = "directory", alias = "folder")]
    path: Option<String>,
    #[arg(long, alias = "lang")]
    language: Option<String>,
    #[arg(long, alias = "ext")]
    extension: Option<String>,
    #[arg(long, alias = "filename", alias = "file-name", alias = "file_name")]
    file: Option<String>,
    #[arg(long, alias = "target-line", alias = "target_line")]
    line: Option<usize>,
    #[arg(long)]
    symbol: Option<String>,
    #[arg(
        long = "kind",
        alias = "type",
        alias = "symbol-kind",
        alias = "symbol_kind"
    )]
    symbol_kind: Option<String>,
    #[arg(long, alias = "git-branch", alias = "git_branch")]
    branch: Option<String>,
    #[arg(
        long,
        alias = "remote",
        alias = "remote-origin",
        alias = "remote_origin"
    )]
    origin: Option<String>,
    #[arg(long, alias = "dep", alias = "deps")]
    dependency: Option<String>,
    #[arg(
        long,
        alias = "imports",
        alias = "module",
        alias = "modules",
        alias = "use",
        alias = "uses"
    )]
    import: Option<String>,
    #[arg(long)]
    test: Option<bool>,
    #[arg(long)]
    generated: Option<bool>,
    #[arg(long)]
    code: Option<bool>,
    #[arg(long)]
    require_all: bool,
    #[arg(long, conflicts_with = "require_all")]
    any_terms: bool,
    #[arg(long, default_value = "medium")]
    snippet: String,
    #[arg(long)]
    explain: bool,
    #[arg(
        long = "exclude-file",
        alias = "exclude-filename",
        alias = "exclude-file-name",
        alias = "exclude_file_name"
    )]
    exclude_file: Vec<String>,
    #[arg(
        long = "exclude-path",
        alias = "exclude-dir",
        alias = "exclude-directory",
        alias = "exclude-folder"
    )]
    exclude_path: Vec<String>,
    #[arg(
        long = "exclude-language",
        alias = "exclude-lang",
        alias = "exclude_language"
    )]
    exclude_language: Vec<String>,
    #[arg(
        long = "exclude-extension",
        alias = "exclude-ext",
        alias = "exclude_extension"
    )]
    exclude_extension: Vec<String>,
    #[arg(long = "exclude-symbol")]
    exclude_symbol: Vec<String>,
    #[arg(
        long = "exclude-kind",
        alias = "exclude-type",
        alias = "exclude-symbol-kind",
        alias = "exclude_symbol_kind"
    )]
    exclude_symbol_kind: Vec<String>,
    #[arg(long = "exclude-repo")]
    exclude_repo: Vec<String>,
    #[arg(
        long = "exclude-branch",
        alias = "exclude-git-branch",
        alias = "exclude_branch",
        alias = "exclude_git_branch"
    )]
    exclude_branch: Vec<String>,
    #[arg(
        long = "exclude-origin",
        alias = "exclude-remote",
        alias = "exclude-remote-origin",
        alias = "exclude_origin",
        alias = "exclude_remote_origin"
    )]
    exclude_origin: Vec<String>,
    #[arg(
        long = "exclude-dependency",
        alias = "exclude-dep",
        alias = "exclude-deps"
    )]
    exclude_dependency: Vec<String>,
    #[arg(
        long = "exclude-import",
        alias = "exclude-imports",
        alias = "exclude-module",
        alias = "exclude-modules",
        alias = "exclude-use",
        alias = "exclude-uses"
    )]
    exclude_import: Vec<String>,
    #[arg(
        long = "exclude-content",
        alias = "exclude-text",
        alias = "exclude-term"
    )]
    exclude_content: Vec<String>,
}

#[derive(Debug, Clone, Args)]
struct RelatedSymbolFilterArgs {
    #[arg(long, alias = "lang")]
    language: Option<String>,
    #[arg(long, alias = "ext")]
    extension: Option<String>,
    #[arg(long, alias = "filename", alias = "file-name", alias = "file_name")]
    file: Option<String>,
    #[arg(long, alias = "target-line", alias = "target_line")]
    line: Option<usize>,
    #[arg(long)]
    symbol: Option<String>,
    #[arg(
        long = "kind",
        alias = "type",
        alias = "symbol-kind",
        alias = "symbol_kind"
    )]
    symbol_kind: Option<String>,
    #[arg(long, alias = "git-branch", alias = "git_branch")]
    branch: Option<String>,
    #[arg(
        long,
        alias = "remote",
        alias = "remote-origin",
        alias = "remote_origin"
    )]
    origin: Option<String>,
    #[arg(long = "repo-filter")]
    repo_filter: Option<String>,
    #[arg(long, alias = "dep", alias = "deps")]
    dependency: Option<String>,
    #[arg(
        long,
        alias = "imports",
        alias = "module",
        alias = "modules",
        alias = "use",
        alias = "uses"
    )]
    import: Option<String>,
    #[arg(long)]
    test: Option<bool>,
    #[arg(long)]
    generated: Option<bool>,
    #[arg(long)]
    code: Option<bool>,
    #[arg(
        long = "exclude-file",
        alias = "exclude-filename",
        alias = "exclude-file-name",
        alias = "exclude_file_name"
    )]
    exclude_file: Vec<String>,
    #[arg(
        long = "exclude-path",
        alias = "exclude-dir",
        alias = "exclude-directory",
        alias = "exclude-folder"
    )]
    exclude_path: Vec<String>,
    #[arg(
        long = "exclude-language",
        alias = "exclude-lang",
        alias = "exclude_language"
    )]
    exclude_language: Vec<String>,
    #[arg(
        long = "exclude-extension",
        alias = "exclude-ext",
        alias = "exclude_extension"
    )]
    exclude_extension: Vec<String>,
    #[arg(long = "exclude-symbol")]
    exclude_symbol: Vec<String>,
    #[arg(
        long = "exclude-kind",
        alias = "exclude-type",
        alias = "exclude-symbol-kind",
        alias = "exclude_symbol_kind"
    )]
    exclude_symbol_kind: Vec<String>,
    #[arg(long = "exclude-repo")]
    exclude_repo: Vec<String>,
    #[arg(
        long = "exclude-branch",
        alias = "exclude-git-branch",
        alias = "exclude_branch",
        alias = "exclude_git_branch"
    )]
    exclude_branch: Vec<String>,
    #[arg(
        long = "exclude-origin",
        alias = "exclude-remote",
        alias = "exclude-remote-origin",
        alias = "exclude_origin",
        alias = "exclude_remote_origin"
    )]
    exclude_origin: Vec<String>,
    #[arg(
        long = "exclude-dependency",
        alias = "exclude-dep",
        alias = "exclude-deps"
    )]
    exclude_dependency: Vec<String>,
    #[arg(
        long = "exclude-import",
        alias = "exclude-imports",
        alias = "exclude-module",
        alias = "exclude-modules",
        alias = "exclude-use",
        alias = "exclude-uses"
    )]
    exclude_import: Vec<String>,
    #[arg(
        long = "exclude-content",
        alias = "exclude-text",
        alias = "exclude-term"
    )]
    exclude_content: Vec<String>,
}

fn search_filters_from_args(
    args: &CommonSearchArgs,
    repo: Option<String>,
) -> Result<SearchFilters> {
    if args.line == Some(0) {
        bail!("--line must be a positive integer");
    }
    Ok(SearchFilters {
        file: args.file.clone(),
        path: args.path.clone(),
        language: args
            .language
            .as_ref()
            .map(|value| normalize_language_filter(value)),
        extension: args
            .extension
            .as_ref()
            .map(|value| normalize_extension(value)),
        symbol: args.symbol.clone(),
        symbol_kind: args
            .symbol_kind
            .as_ref()
            .map(|value| normalize_symbol_kind(value)),
        repo,
        branch: args.branch.clone(),
        origin: args.origin.clone(),
        dependency: args
            .dependency
            .as_ref()
            .map(|value| normalize_filter(value)),
        import: args.import.as_ref().map(|value| normalize_filter(value)),
        test: args.test,
        generated: args.generated,
        code: args.code,
        target_line: args.line,
        require_all: args.require_all && !args.any_terms,
        match_any: args.any_terms,
        snippet: snippet_mode_arg(&args.snippet)?,
        explain: args.explain,
        exclude_file: args.exclude_file.clone(),
        exclude_path: args.exclude_path.clone(),
        exclude_language: args
            .exclude_language
            .iter()
            .map(|value| normalize_language_filter(value))
            .collect(),
        exclude_extension: args
            .exclude_extension
            .iter()
            .map(|value| normalize_extension(value))
            .collect(),
        exclude_symbol: args.exclude_symbol.clone(),
        exclude_symbol_kind: args
            .exclude_symbol_kind
            .iter()
            .map(|value| normalize_symbol_kind(value))
            .collect(),
        exclude_repo: args.exclude_repo.clone(),
        exclude_branch: args.exclude_branch.clone(),
        exclude_origin: args.exclude_origin.clone(),
        exclude_dependency: args
            .exclude_dependency
            .iter()
            .map(|value| normalize_filter(value))
            .collect(),
        exclude_import: args
            .exclude_import
            .iter()
            .map(|value| normalize_filter(value))
            .collect(),
        exclude_content: args.exclude_content.clone(),
    })
}

fn related_symbol_filters_from_args(
    args: &RelatedSymbolFilterArgs,
    repo: Option<String>,
) -> SearchFilters {
    SearchFilters {
        file: args.file.clone(),
        path: None,
        language: args
            .language
            .as_ref()
            .map(|value| normalize_language_filter(value)),
        extension: args
            .extension
            .as_ref()
            .map(|value| normalize_extension(value)),
        symbol: args.symbol.clone(),
        symbol_kind: args
            .symbol_kind
            .as_ref()
            .map(|value| normalize_symbol_kind(value)),
        repo: repo.or_else(|| args.repo_filter.clone()),
        branch: args.branch.clone(),
        origin: args.origin.clone(),
        dependency: args
            .dependency
            .as_ref()
            .map(|value| normalize_filter(value)),
        import: args.import.as_ref().map(|value| normalize_filter(value)),
        test: args.test,
        generated: args.generated,
        code: args.code,
        target_line: args.line,
        exclude_file: args.exclude_file.clone(),
        exclude_path: args.exclude_path.clone(),
        exclude_language: args
            .exclude_language
            .iter()
            .map(|value| normalize_language_filter(value))
            .collect(),
        exclude_extension: args
            .exclude_extension
            .iter()
            .map(|value| normalize_extension(value))
            .collect(),
        exclude_symbol: args.exclude_symbol.clone(),
        exclude_symbol_kind: args
            .exclude_symbol_kind
            .iter()
            .map(|value| normalize_symbol_kind(value))
            .collect(),
        exclude_repo: args.exclude_repo.clone(),
        exclude_branch: args.exclude_branch.clone(),
        exclude_origin: args.exclude_origin.clone(),
        exclude_dependency: args
            .exclude_dependency
            .iter()
            .map(|value| normalize_filter(value))
            .collect(),
        exclude_import: args
            .exclude_import
            .iter()
            .map(|value| normalize_filter(value))
            .collect(),
        exclude_content: args.exclude_content.clone(),
        ..SearchFilters::default()
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct BenchReport {
    mode: String,
    runs: usize,
    warmup: usize,
    limit: usize,
    queries: Vec<QueryBench>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct QueryBench {
    query: String,
    result_count: usize,
    min_ms: f64,
    p50_ms: f64,
    p95_ms: f64,
    #[serde(default)]
    p99_ms: f64,
    max_ms: f64,
    samples_ms: Vec<f64>,
}

#[derive(Debug, Clone, Serialize)]
struct SearchBatchResult {
    query: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    read_batch_request: Option<ResultToolRequest>,
    #[serde(skip_serializing_if = "Option::is_none")]
    next_action: Option<Value>,
    results: Vec<SearchResult>,
}

fn search_batch_result(
    query: String,
    read_batch_request: Option<ResultToolRequest>,
    results: Vec<SearchResult>,
) -> SearchBatchResult {
    let next_action = read_batch_next_action(&read_batch_request);
    SearchBatchResult {
        query,
        read_batch_request,
        next_action,
        results,
    }
}

fn read_batch_next_action(read_batch_request: &Option<ResultToolRequest>) -> Option<Value> {
    read_batch_request.as_ref().map(|request| {
        serde_json::json!({
            "kind": "read",
            "source": "read_batch_request",
            "summary": "Read the batch item's top matching ranges.",
            "request": request
        })
    })
}

#[derive(Debug, Clone, Serialize)]
struct IndexedQueryPlanBatchResult {
    query: String,
    plan: QueryPlan,
}

#[derive(Debug, Clone, Serialize)]
struct QueryPlanBatchResult {
    query: String,
    plan: QueryPlan,
}

#[derive(Debug, Clone, Serialize)]
struct ShardQueryPlanBatchResult {
    query: String,
    plans: Vec<ShardQueryPlan>,
}

#[derive(Debug, Clone, Serialize)]
struct SymbolBatchResult {
    name: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    read_batch_request: Option<ResultToolRequest>,
    #[serde(skip_serializing_if = "Option::is_none")]
    next_action: Option<Value>,
    symbols: Vec<SymbolLookupResult>,
}

fn symbol_batch_result(
    name: String,
    read_batch_request: Option<ResultToolRequest>,
    symbols: Vec<SymbolLookupResult>,
) -> SymbolBatchResult {
    let next_action = read_batch_next_action(&read_batch_request);
    SymbolBatchResult {
        name,
        read_batch_request,
        next_action,
        symbols,
    }
}

fn main() {
    let default_hook = std::panic::take_hook();
    std::panic::set_hook(Box::new(move |info| {
        if !panic_payload_is_broken_pipe(info.payload()) {
            default_hook(info);
        }
    }));

    match std::panic::catch_unwind(run) {
        Ok(Ok(())) => {}
        Ok(Err(error)) => {
            eprintln!("Error: {error:?}");
            std::process::exit(1);
        }
        Err(payload) if panic_payload_is_broken_pipe(payload.as_ref()) => {
            std::process::exit(0);
        }
        Err(payload) => std::panic::resume_unwind(payload),
    }
}

fn panic_payload_is_broken_pipe(payload: &(dyn Any + Send)) -> bool {
    let Some(message) = panic_payload_message(payload) else {
        return false;
    };
    message.contains("failed printing to stdout") && message.contains("Broken pipe")
}

fn panic_payload_message(payload: &(dyn Any + Send)) -> Option<&str> {
    if let Some(message) = payload.downcast_ref::<&str>() {
        Some(*message)
    } else {
        payload.downcast_ref::<String>().map(String::as_str)
    }
}

fn read_request_args<T: Serialize>(name: &str, value: T) -> Map<String, Value> {
    let mut arguments = Map::new();
    arguments.insert(name.to_string(), serde_json::json!(value));
    arguments
}

fn print_related_response<T: Serialize>(
    results: Vec<T>,
    include_read_batch: bool,
    batch_tool: &str,
    base_arguments: Map<String, Value>,
    summary: &str,
) -> Result<()> {
    let results = serde_json::to_value(results)?;
    if !include_read_batch {
        println!("{}", serde_json::to_string(&results)?);
        return Ok(());
    }
    let read_batch_request = result_value_read_batch_request(&results, batch_tool, base_arguments);
    let next_action = read_batch_request.as_ref().map(|request| {
        serde_json::json!({
            "kind": "read",
            "source": "read_batch_request",
            "summary": summary,
            "request": request
        })
    });
    println!(
        "{}",
        serde_json::to_string(&serde_json::json!({
            "results": results,
            "read_batch_request": read_batch_request,
            "next_action": next_action
        }))?
    );
    Ok(())
}

fn repo_map_detail_from_cli(value: &str) -> Result<RepoMapDetail> {
    match value {
        "compact" => Ok(RepoMapDetail::Compact),
        "full" => Ok(RepoMapDetail::Full),
        _ => bail!("repo map detail must be compact or full"),
    }
}

fn repo_map_read_limit_from_cli(value: usize) -> Result<usize> {
    if value == 0 || value > MAX_RESULT_READ_BATCH_RANGES {
        bail!("repo map read limit must be between 1 and {MAX_RESULT_READ_BATCH_RANGES}");
    }
    Ok(value)
}

fn attach_cli_retry_requests<T: Serialize>(
    mut plan: QueryPlan,
    search_tool: &str,
    target_name: &str,
    target_value: T,
    filters: &SearchFilters,
) -> QueryPlan {
    let retry_requests = cli_retry_requests(&plan, search_tool, target_name, target_value, filters);
    plan.set_retry_requests(retry_requests);
    plan
}

fn attach_cli_result_query_plan_retry_requests<T: Serialize>(
    results: &mut [SearchResult],
    search_tool: &str,
    target_name: &str,
    target_value: &T,
    filters: &SearchFilters,
) {
    for result in results {
        let Some(plan) = result.query_plan.take() else {
            continue;
        };
        result.query_plan = Some(attach_cli_retry_requests(
            plan,
            search_tool,
            target_name,
            target_value,
            filters,
        ));
    }
}

fn cli_retry_requests<T: Serialize>(
    plan: &QueryPlan,
    search_tool: &str,
    target_name: &str,
    target_value: T,
    filters: &SearchFilters,
) -> Vec<ResultToolRequest> {
    let mut requests = Vec::new();
    let mut seen_queries = HashSet::new();
    let replaced_filter_fields = plan
        .repair_hints
        .iter()
        .filter_map(|hint| cli_replaced_filter_field(&hint.kind))
        .collect::<Vec<_>>();
    let repair_filter_fields = plan
        .repair_hints
        .iter()
        .filter_map(|hint| {
            cli_replaced_filter_field(&hint.kind).or_else(|| cli_relaxed_filter_field(&hint.kind))
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
        if hint.kind != "relax_filters" {
            let skip_field = cli_replaced_filter_field(&hint.kind)
                .or_else(|| cli_relaxed_filter_field(&hint.kind));
            let suggested_filters = parse_query(query).filters;
            add_filter_retry_args(&mut arguments, filters, target_name, skip_field);
            add_plan_filter_retry_args(&mut arguments, plan, target_name, skip_field);
            if skip_field.is_none() && retry_hint_drops_replaced_filters(&hint.action) {
                remove_repair_filter_retry_args(
                    &mut arguments,
                    &replaced_filter_fields,
                    &suggested_filters,
                );
            } else if skip_field.is_none() && retry_hint_drops_repaired_filters(&hint.action) {
                remove_repair_filter_retry_args(
                    &mut arguments,
                    &repair_filter_fields,
                    &suggested_filters,
                );
            }
        }
        arguments.insert(target_name.to_string(), serde_json::json!(target_value));
        arguments.insert("query".to_string(), serde_json::json!(query));
        arguments.insert("explain".to_string(), serde_json::json!(true));
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

fn remove_repair_filter_retry_args(
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

fn cli_replaced_filter_field(kind: &str) -> Option<&'static str> {
    match kind {
        "replace_file_filter" => Some("file"),
        "replace_path_filter" => Some("path"),
        "replace_symbol_filter" => Some("symbol"),
        "replace_symbol_kind_filter" => Some("symbol_kind"),
        _ => None,
    }
}

fn cli_relaxed_filter_field(kind: &str) -> Option<&'static str> {
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

fn add_filter_retry_args(
    arguments: &mut Map<String, Value>,
    filters: &SearchFilters,
    target_name: &str,
    skip_field: Option<&str>,
) {
    if skip_field != Some("file") {
        insert_string_arg(arguments, "file", filters.file.as_ref());
    }
    if skip_field != Some("path") {
        insert_string_arg(arguments, "path", filters.path.as_ref());
    }
    if skip_field != Some("language") {
        insert_string_arg(arguments, "language", filters.language.as_ref());
    }
    if skip_field != Some("extension") {
        insert_string_arg(arguments, "extension", filters.extension.as_ref());
    }
    if skip_field != Some("symbol") {
        insert_string_arg(arguments, "symbol", filters.symbol.as_ref());
    }
    if skip_field != Some("symbol_kind") {
        insert_string_arg(arguments, "symbol_kind", filters.symbol_kind.as_ref());
    }
    if target_name != "repo" && skip_field != Some("repo") {
        insert_string_arg(arguments, "repo", filters.repo.as_ref());
    }
    if skip_field != Some("branch") {
        insert_string_arg(arguments, "branch", filters.branch.as_ref());
    }
    if skip_field != Some("origin") {
        insert_string_arg(arguments, "origin", filters.origin.as_ref());
    }
    if skip_field != Some("dependency") {
        insert_string_arg(arguments, "dependency", filters.dependency.as_ref());
    }
    if skip_field != Some("import") {
        insert_string_arg(arguments, "import", filters.import.as_ref());
    }
    if skip_field != Some("test")
        && let Some(test) = filters.test
    {
        arguments.insert("test".to_string(), serde_json::json!(test));
    }
    if skip_field != Some("generated")
        && let Some(generated) = filters.generated
    {
        arguments.insert("generated".to_string(), serde_json::json!(generated));
    }
    if skip_field != Some("code")
        && let Some(code) = filters.code
    {
        arguments.insert("code".to_string(), serde_json::json!(code));
    }
    if let Some(line) = filters.target_line {
        arguments.insert("line".to_string(), serde_json::json!(line));
    }
    insert_string_array_arg(arguments, "exclude_file", &filters.exclude_file);
    insert_string_array_arg(arguments, "exclude_path", &filters.exclude_path);
    insert_string_array_arg(arguments, "exclude_language", &filters.exclude_language);
    insert_string_array_arg(arguments, "exclude_extension", &filters.exclude_extension);
    insert_string_array_arg(arguments, "exclude_symbol", &filters.exclude_symbol);
    insert_string_array_arg(
        arguments,
        "exclude_symbol_kind",
        &filters.exclude_symbol_kind,
    );
    if skip_field != Some("repo") {
        insert_string_array_arg(arguments, "exclude_repo", &filters.exclude_repo);
    }
    if skip_field != Some("branch") {
        insert_string_array_arg(arguments, "exclude_branch", &filters.exclude_branch);
    }
    if skip_field != Some("origin") {
        insert_string_array_arg(arguments, "exclude_origin", &filters.exclude_origin);
    }
    insert_string_array_arg(arguments, "exclude_dependency", &filters.exclude_dependency);
    insert_string_array_arg(arguments, "exclude_import", &filters.exclude_import);
    insert_string_array_arg(arguments, "exclude_content", &filters.exclude_content);
}

fn insert_string_arg(arguments: &mut Map<String, Value>, name: &str, value: Option<&String>) {
    if let Some(value) = value {
        arguments.insert(name.to_string(), serde_json::json!(value));
    }
}

fn insert_string_array_arg(arguments: &mut Map<String, Value>, name: &str, values: &[String]) {
    if !values.is_empty() {
        arguments.insert(name.to_string(), serde_json::json!(values));
    }
}

fn shard_scope_filters_for_query(filters: &SearchFilters, query: &str) -> SearchFilters {
    merge_filters(filters.clone(), parse_query(query).filters)
}

fn shard_repo_map_request(index_dir: &Path, filters: &SearchFilters) -> Value {
    let mut arguments = Map::new();
    arguments.insert("index_dir".to_string(), serde_json::json!(index_dir));
    arguments.insert("detail".to_string(), serde_json::json!("compact"));
    arguments.insert(
        "read_limit".to_string(),
        serde_json::json!(DEFAULT_REPO_MAP_READ_BATCH_RANGES),
    );
    add_shard_scope_filter_args(&mut arguments, filters);
    serde_json::json!({
        "tool": "shard_repo_map",
        "arguments": arguments
    })
}

fn add_shard_scope_filter_args(arguments: &mut Map<String, Value>, filters: &SearchFilters) {
    insert_string_arg(arguments, "repo", filters.repo.as_ref());
    insert_string_arg(arguments, "branch", filters.branch.as_ref());
    insert_string_arg(arguments, "origin", filters.origin.as_ref());
    insert_string_array_arg(arguments, "exclude_repo", &filters.exclude_repo);
    insert_string_array_arg(arguments, "exclude_branch", &filters.exclude_branch);
    insert_string_array_arg(arguments, "exclude_origin", &filters.exclude_origin);
}

fn add_plan_filter_retry_args(
    arguments: &mut Map<String, Value>,
    plan: &QueryPlan,
    target_name: &str,
    skip_field: Option<&str>,
) {
    let mut negated: Map<String, Value> = Map::new();
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
        let entry = negated
            .entry(key)
            .or_insert_with(|| Value::Array(Vec::new()));
        if let Value::Array(values) = entry {
            values.push(serde_json::json!(filter.value));
        }
    }
    arguments.extend(negated);
}

fn plan_filter_argument_value(filter: &QueryPlanFilter) -> Value {
    match filter.field.as_str() {
        "test" | "generated" | "code" => serde_json::json!(matches!(
            filter.value.to_ascii_lowercase().as_str(),
            "1" | "true" | "yes" | "y"
        )),
        "line" => filter
            .value
            .parse::<usize>()
            .map(Value::from)
            .unwrap_or_else(|_| serde_json::json!(filter.value)),
        _ => serde_json::json!(filter.value),
    }
}

fn attach_cli_shard_retry_requests(
    plans: &mut [ShardQueryPlan],
    index_dir: &Path,
    filters: &SearchFilters,
) {
    attach_cli_shard_retry_requests_with_tool(plans, "search_shards", index_dir, filters);
}

fn attach_cli_shard_retry_requests_with_tool(
    plans: &mut [ShardQueryPlan],
    search_tool: &str,
    index_dir: &Path,
    filters: &SearchFilters,
) {
    for shard_plan in plans {
        shard_plan.plan = attach_cli_retry_requests(
            shard_plan.plan.clone(),
            search_tool,
            "index_dir",
            index_dir,
            filters,
        );
    }
}

fn primary_cli_retry_request_from_plan(plan: &QueryPlan) -> Option<Value> {
    plan.primary_retry_request
        .clone()
        .or_else(|| plan.retry_requests.first().cloned())
        .and_then(|request| serde_json::to_value(request).ok())
}

fn primary_cli_diagnosis_from_plan(plan: &QueryPlan) -> Option<Value> {
    plan.diagnosis
        .as_ref()
        .and_then(|diagnosis| serde_json::to_value(diagnosis).ok())
}

fn primary_cli_retry_request_from_shard_plans(plans: &[ShardQueryPlan]) -> Option<Value> {
    plans
        .iter()
        .find_map(|shard_plan| primary_cli_retry_request_from_plan(&shard_plan.plan))
}

fn primary_cli_diagnosis_from_shard_plans(plans: &[ShardQueryPlan]) -> Option<Value> {
    plans
        .iter()
        .find_map(|shard_plan| primary_cli_diagnosis_from_plan(&shard_plan.plan))
}

fn primary_cli_retry_result(
    retry_if_empty: bool,
    original_results_empty: bool,
    request: Option<&Value>,
) -> Result<Option<Value>> {
    if !retry_if_empty || !original_results_empty {
        return Ok(None);
    }
    let Some(request) = request else {
        return Ok(None);
    };
    let Some(tool) = request.get("tool").and_then(Value::as_str) else {
        return Ok(None);
    };
    let arguments = request
        .get("arguments")
        .cloned()
        .unwrap_or_else(|| serde_json::json!({}));
    let response = ToolRuntime::default().dispatch(ToolRequest {
        id: serde_json::json!("primary-retry"),
        tool: tool.to_string(),
        arguments,
    });
    if let Some(error) = response.error {
        bail!("primary retry request failed: {error}");
    }
    let result = response.result.unwrap_or(Value::Null);
    let read_batch_request = primary_cli_retry_read_batch_request(request, &result);
    let mut value = serde_json::json!({
        "request": request,
        "results": result
    });
    if let Some(read_batch_request) = read_batch_request {
        value["read_batch_request"] = serde_json::to_value(read_batch_request)?;
    }
    Ok(Some(value))
}

fn primary_cli_retry_read_batch_request(
    request: &Value,
    result: &Value,
) -> Option<ResultToolRequest> {
    let source = request.get("arguments")?.as_object()?;
    let mut arguments = serde_json::Map::new();
    for name in ["index_dir", "index", "repo"] {
        if let Some(value) = source.get(name) {
            arguments.insert(name.to_string(), value.clone());
            return result_value_read_batch_request(result, "read_ranges", arguments);
        }
    }
    None
}

fn insert_optional_json_field(object: &mut Value, name: &str, value: Option<Value>) {
    if let (Value::Object(object), Some(value)) = (object, value) {
        object.insert(name.to_string(), value);
    }
}

fn attach_cli_next_action(object: &mut Value) {
    let Some(fields) = object.as_object_mut() else {
        return;
    };
    let prefer_retry = fields
        .get("primary_retry_result")
        .is_none_or(Value::is_null)
        && fields
            .get("primary_retry_request")
            .is_some_and(|value| !value.is_null())
        && fields
            .get("primary_diagnosis")
            .and_then(|diagnosis| diagnosis.get("suggested_query"))
            .and_then(Value::as_str)
            .is_some();
    let ordered_actions = if prefer_retry {
        [
            (
                "primary_retry_request",
                "retry",
                "Run the promoted repaired search.",
            ),
            (
                "next_read_batch_request",
                "read",
                "Read the top available result ranges.",
            ),
            (
                "repo_map_request",
                "map",
                "Open a compact repo map before broadening manually.",
            ),
        ]
    } else {
        [
            (
                "next_read_batch_request",
                "read",
                "Read the top available result ranges.",
            ),
            (
                "primary_retry_request",
                "retry",
                "Run the promoted repaired search.",
            ),
            (
                "repo_map_request",
                "map",
                "Open a compact repo map before broadening manually.",
            ),
        ]
    };
    for (source, kind, summary) in ordered_actions {
        let Some(request) = fields.get(source).filter(|value| !value.is_null()).cloned() else {
            continue;
        };
        fields.insert(
            "next_action".to_string(),
            serde_json::json!({
                "kind": kind,
                "source": source,
                "summary": summary,
                "request": request
            }),
        );
        break;
    }
}

fn promoted_cli_next_read_batch_request(
    read_batch_request: &Option<ResultToolRequest>,
    primary_retry_result: &Option<Value>,
) -> Option<Value> {
    read_batch_request
        .as_ref()
        .and_then(|request| serde_json::to_value(request).ok())
        .or_else(|| {
            primary_retry_result
                .as_ref()?
                .get("read_batch_request")
                .cloned()
        })
}

fn daemon_search_auto_arguments(
    query: &str,
    limit: usize,
    filters: &SearchFilters,
    context_lines: usize,
    refresh_if_stale: bool,
    diagnose: bool,
    retry_if_empty: bool,
) -> Value {
    let mut arguments = search_filter_arguments(filters);
    arguments.insert("query".to_string(), Value::String(query.to_string()));
    arguments.insert("limit".to_string(), serde_json::json!(limit));
    arguments.insert(
        "context_lines".to_string(),
        serde_json::json!(context_lines),
    );
    arguments.insert(
        "refresh_if_stale".to_string(),
        serde_json::json!(refresh_if_stale),
    );
    arguments.insert("diagnose".to_string(), serde_json::json!(diagnose));
    arguments.insert(
        "retry_if_empty".to_string(),
        serde_json::json!(retry_if_empty),
    );
    Value::Object(arguments)
}

fn daemon_search_auto_batch_arguments(
    queries: &[String],
    limit: usize,
    filters: &SearchFilters,
    context_lines: usize,
    refresh_if_stale: bool,
    diagnose: bool,
    retry_if_empty: bool,
) -> Value {
    let mut arguments = search_filter_arguments(filters);
    arguments.insert("queries".to_string(), serde_json::json!(queries));
    arguments.insert("limit".to_string(), serde_json::json!(limit));
    arguments.insert(
        "context_lines".to_string(),
        serde_json::json!(context_lines),
    );
    arguments.insert(
        "refresh_if_stale".to_string(),
        serde_json::json!(refresh_if_stale),
    );
    arguments.insert("diagnose".to_string(), serde_json::json!(diagnose));
    arguments.insert(
        "retry_if_empty".to_string(),
        serde_json::json!(retry_if_empty),
    );
    Value::Object(arguments)
}

fn search_filter_arguments(filters: &SearchFilters) -> Map<String, Value> {
    let mut arguments = Map::new();
    insert_optional_string(&mut arguments, "file", &filters.file);
    insert_optional_string(&mut arguments, "path", &filters.path);
    insert_optional_string(&mut arguments, "language", &filters.language);
    insert_optional_string(&mut arguments, "extension", &filters.extension);
    insert_optional_string(&mut arguments, "symbol", &filters.symbol);
    insert_optional_string(&mut arguments, "symbol_kind", &filters.symbol_kind);
    insert_optional_string(&mut arguments, "repo_filter", &filters.repo);
    insert_optional_string(&mut arguments, "branch", &filters.branch);
    insert_optional_string(&mut arguments, "origin", &filters.origin);
    insert_optional_string(&mut arguments, "dependency", &filters.dependency);
    insert_optional_string(&mut arguments, "import", &filters.import);
    insert_optional_bool(&mut arguments, "test", filters.test);
    insert_optional_bool(&mut arguments, "generated", filters.generated);
    insert_optional_bool(&mut arguments, "code", filters.code);
    insert_optional_usize(&mut arguments, "line", filters.target_line);
    arguments.insert(
        "require_all".to_string(),
        serde_json::json!(filters.require_all),
    );
    arguments.insert(
        "any_terms".to_string(),
        serde_json::json!(filters.match_any),
    );
    arguments.insert(
        "snippet".to_string(),
        Value::String(snippet_mode_name(filters.snippet).to_string()),
    );
    arguments.insert("explain".to_string(), serde_json::json!(filters.explain));
    insert_string_vec(&mut arguments, "exclude_file", &filters.exclude_file);
    insert_string_vec(&mut arguments, "exclude_path", &filters.exclude_path);
    insert_string_vec(
        &mut arguments,
        "exclude_language",
        &filters.exclude_language,
    );
    insert_string_vec(
        &mut arguments,
        "exclude_extension",
        &filters.exclude_extension,
    );
    insert_string_vec(&mut arguments, "exclude_symbol", &filters.exclude_symbol);
    insert_string_vec(
        &mut arguments,
        "exclude_symbol_kind",
        &filters.exclude_symbol_kind,
    );
    insert_string_vec(&mut arguments, "exclude_repo", &filters.exclude_repo);
    insert_string_vec(&mut arguments, "exclude_branch", &filters.exclude_branch);
    insert_string_vec(&mut arguments, "exclude_origin", &filters.exclude_origin);
    insert_string_vec(
        &mut arguments,
        "exclude_dependency",
        &filters.exclude_dependency,
    );
    insert_string_vec(&mut arguments, "exclude_import", &filters.exclude_import);
    insert_string_vec(&mut arguments, "exclude_content", &filters.exclude_content);
    arguments
}

fn insert_optional_string(arguments: &mut Map<String, Value>, name: &str, value: &Option<String>) {
    if let Some(value) = value {
        arguments.insert(name.to_string(), Value::String(value.clone()));
    }
}

fn insert_optional_bool(arguments: &mut Map<String, Value>, name: &str, value: Option<bool>) {
    if let Some(value) = value {
        arguments.insert(name.to_string(), serde_json::json!(value));
    }
}

fn insert_optional_usize(arguments: &mut Map<String, Value>, name: &str, value: Option<usize>) {
    if let Some(value) = value {
        arguments.insert(name.to_string(), serde_json::json!(value));
    }
}

fn insert_string_vec(arguments: &mut Map<String, Value>, name: &str, values: &[String]) {
    if !values.is_empty() {
        arguments.insert(name.to_string(), serde_json::json!(values));
    }
}

fn snippet_mode_name(mode: SnippetMode) -> &'static str {
    match mode {
        SnippetMode::Short => "short",
        SnippetMode::Medium => "medium",
        SnippetMode::Block => "block",
        SnippetMode::Symbol => "symbol",
    }
}

fn run() -> Result<()> {
    let cli = Cli::parse();
    match cli.command {
        Commands::DiscoverRepos {
            root,
            max_depth,
            limit,
            family_limit,
            git_metadata,
            tracked_files,
            nested_manifests,
        } => {
            println!(
                "{}",
                serde_json::to_string(&discover_repos(
                    root,
                    &DiscoverOptions {
                        max_depth,
                        limit,
                        family_limit: normalize_family_limit(family_limit),
                        git_metadata,
                        tracked_files,
                        nested_manifests,
                    },
                )?)?
            );
        }
        Commands::Index { repo, output } => {
            let index = FastIndex::build(repo)?;
            index.save(&output)?;
            println!("{}", serde_json::to_string(&index.stats())?);
        }
        Commands::RefreshIndex { repo, index } | Commands::EnsureIndex { repo, index } => {
            println!(
                "{}",
                serde_json::to_string(&refresh_or_build_index(repo, index)?)?
            );
        }
        Commands::IndexStatus { index } => {
            let index_path = index;
            let index = FastIndex::load(&index_path)?;
            println!(
                "{}",
                serde_json::to_string(&index.freshness_at(index_path)?)?
            );
        }
        Commands::IndexShards {
            repos,
            discover_roots,
            max_depth,
            discover_limit,
            family_limit,
            nested_manifests,
            force,
            output_dir,
        } => {
            let selection = shard_repos_from_args_required(
                repos,
                discover_roots,
                max_depth,
                discover_limit,
                normalize_family_limit(family_limit),
                nested_manifests,
            )?;
            let stats = build_shards_with_force(&selection.repos, output_dir, force)?;
            println!(
                "{}",
                serde_json::to_string(&shard_bootstrap_output(stats, selection.discovery)?)?
            );
        }
        Commands::RefreshShards { index_dir } => {
            println!("{}", serde_json::to_string(&refresh_shards(index_dir)?)?);
        }
        Commands::ShardStatus { index_dir, summary } => {
            let status = shard_status(index_dir)?;
            let output = if summary {
                shard_status_summary(&status)
            } else {
                serde_json::to_value(status)?
            };
            println!("{}", serde_json::to_string(&output)?);
        }
        Commands::EnsureShards {
            repos,
            discover_roots,
            max_depth,
            discover_limit,
            family_limit,
            nested_manifests,
            output_dir,
        } => {
            let selection = shard_repos_from_args(
                repos,
                discover_roots,
                max_depth,
                discover_limit,
                normalize_family_limit(family_limit),
                nested_manifests,
            )?;
            let stats = ensure_shards(&selection.repos, output_dir)?;
            println!(
                "{}",
                serde_json::to_string(&shard_bootstrap_output(stats, selection.discovery)?)?
            );
        }
        Commands::SearchShards {
            index_dir,
            query,
            query_arg,
            limit,
            repo,
            filters,
            context_lines,
            refresh_if_stale,
        } => {
            if refresh_if_stale && shard_status(&index_dir)?.stale {
                refresh_shards(&index_dir)?;
            }
            let filters = search_filters_from_args(&filters, repo)?;
            let query = cli_single_query_for_filters(query, query_arg, &filters)?;
            let mut results = search_shards(&index_dir, &query, limit, &filters)?;
            attach_result_context(&mut results, context_lines, |path, start, lines| {
                read_shard_range(&index_dir, path, start, lines)
            })?;
            attach_result_read_requests(
                &mut results,
                "read_shard_range",
                read_request_args("index_dir", &index_dir),
            );
            attach_result_related_requests(
                &mut results,
                "related_shard_files",
                read_request_args("index_dir", &index_dir),
                Some(&filters),
            );
            attach_result_related_symbol_requests(
                &mut results,
                "related_shard_symbols",
                Some(&query),
                read_request_args("index_dir", &index_dir),
            );
            println!("{}", serde_json::to_string(&results)?);
        }
        Commands::SearchShardsBatch {
            index_dir,
            queries,
            limit,
            repo,
            filters,
            context_lines,
            refresh_if_stale,
        } => {
            let queries = cli_batch_queries(queries)?;
            if refresh_if_stale && shard_status(&index_dir)?.stale {
                refresh_shards(&index_dir)?;
            }
            let filters = search_filters_from_args(&filters, repo)?;
            let mut batch = Vec::new();
            for query in queries {
                let mut results = search_shards(&index_dir, &query, limit, &filters)?;
                attach_result_context(&mut results, context_lines, |path, start, lines| {
                    read_shard_range(&index_dir, path, start, lines)
                })?;
                attach_result_read_requests(
                    &mut results,
                    "read_shard_range",
                    read_request_args("index_dir", &index_dir),
                );
                attach_result_related_requests(
                    &mut results,
                    "related_shard_files",
                    read_request_args("index_dir", &index_dir),
                    Some(&filters),
                );
                attach_result_related_symbol_requests(
                    &mut results,
                    "related_shard_symbols",
                    Some(&query),
                    read_request_args("index_dir", &index_dir),
                );
                let read_batch_request = result_read_batch_request(
                    &results,
                    "read_shard_ranges",
                    read_request_args("index_dir", &index_dir),
                );
                batch.push(search_batch_result(query, read_batch_request, results));
            }
            println!("{}", serde_json::to_string(&batch)?);
        }
        Commands::ShardPlan {
            index_dir,
            query,
            query_arg,
            repo,
            filters,
            refresh_if_stale,
        } => {
            if refresh_if_stale && shard_status(&index_dir)?.stale {
                refresh_shards(&index_dir)?;
            }
            let filters = search_filters_from_args(&filters, repo)?;
            let query = cli_single_query_for_filters(query, query_arg, &filters)?;
            let mut plans = shard_query_plans(&index_dir, &query, &filters)?;
            attach_cli_shard_retry_requests(&mut plans, &index_dir, &filters);
            println!("{}", serde_json::to_string(&plans)?);
        }
        Commands::ShardPlanBatch {
            index_dir,
            queries,
            repo,
            filters,
            refresh_if_stale,
        } => {
            let queries = cli_batch_queries(queries)?;
            if refresh_if_stale && shard_status(&index_dir)?.stale {
                refresh_shards(&index_dir)?;
            }
            let filters = search_filters_from_args(&filters, repo)?;
            let mut batch = Vec::new();
            for query in queries {
                let mut plans = shard_query_plans(&index_dir, &query, &filters)?;
                attach_cli_shard_retry_requests(&mut plans, &index_dir, &filters);
                batch.push(ShardQueryPlanBatchResult { query, plans });
            }
            println!("{}", serde_json::to_string(&batch)?);
        }
        Commands::ReadShardRange {
            index_dir,
            path,
            path_arg,
            start,
            lines,
            scope,
        } => {
            let range = cli_single_range(path, path_arg, start, lines)?;
            println!(
                "{}",
                serde_json::to_string(&read_shard_range_scoped(
                    index_dir,
                    &range.path,
                    range.start,
                    range.lines,
                    range.scope.unwrap_or_else(|| RangeScope::from(scope)),
                )?)?
            );
        }
        Commands::ReadShardRanges {
            index_dir,
            ranges,
            paths,
            start,
            lines,
            scope,
        } => {
            let mut results = Vec::new();
            let scope = RangeScope::from(scope);
            for range in cli_ranges(paths, ranges, start, lines)? {
                results.push(read_shard_range_scoped(
                    &index_dir,
                    &range.path,
                    range.start,
                    range.lines,
                    range.scope.unwrap_or(scope),
                )?);
            }
            println!("{}", serde_json::to_string(&results)?);
        }
        Commands::ShardSymbol {
            index_dir,
            name,
            limit,
            repo,
            filters,
        } => {
            let filters = search_filters_from_args(&filters, repo)?;
            let symbols = find_shard_symbol(&index_dir, &name, limit, &filters)?;
            println!(
                "{}",
                serde_json::to_string(&symbol_lookup_results(
                    symbols,
                    "read_shard_range",
                    read_request_args("index_dir", &index_dir)
                ))?
            );
        }
        Commands::ShardSymbolBatch {
            index_dir,
            names,
            limit,
            repo,
            filters,
        } => {
            let filters = search_filters_from_args(&filters, repo)?;
            let mut batch = Vec::new();
            for name in cli_batch_queries(names)? {
                let symbols = find_shard_symbol(&index_dir, &name, limit, &filters)?;
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
            println!("{}", serde_json::to_string(&batch)?);
        }
        Commands::ShardMap {
            index_dir,
            symbols,
            tests,
            repo,
            branch,
            origin,
            detail,
            read_limit,
            format: _format,
        } => {
            let detail = repo_map_detail_from_cli(&detail)?;
            let read_limit = repo_map_read_limit_from_cli(read_limit)?;
            let mut maps = shard_repo_maps(
                &index_dir,
                symbols,
                tests,
                detail,
                &SearchFilters {
                    repo,
                    branch,
                    origin,
                    ..SearchFilters::default()
                },
            )?;
            for shard_map in &mut maps {
                attach_repo_map_read_batch_request_with_limit(
                    &mut shard_map.map,
                    "read_shard_ranges",
                    read_request_args("index_dir", &index_dir),
                    read_limit,
                );
            }
            println!("{}", serde_json::to_string(&maps)?);
        }
        Commands::Brief { repo, detail } => {
            let detail = repo_map_detail_from_cli(&detail)?;
            let index = RepoIndexer::new(repo).build()?;
            println!(
                "{}",
                serde_json::to_string(&index.repo_brief_with_detail(detail))?
            );
        }
        Commands::RepoMap {
            repo,
            index,
            index_dir,
            symbols,
            tests,
            repo_filter,
            branch,
            origin,
            detail,
            read_limit,
            format: _format,
        } => {
            let detail = repo_map_detail_from_cli(&detail)?;
            let read_limit = repo_map_read_limit_from_cli(read_limit)?;
            if let Some(index_dir) = index_dir {
                let mut maps = shard_repo_maps(
                    &index_dir,
                    symbols,
                    tests,
                    detail,
                    &SearchFilters {
                        repo: repo_filter,
                        branch,
                        origin,
                        ..SearchFilters::default()
                    },
                )?;
                for shard_map in &mut maps {
                    attach_repo_map_read_batch_request_with_limit(
                        &mut shard_map.map,
                        "read_ranges",
                        read_request_args("index_dir", &index_dir),
                        read_limit,
                    );
                }
                println!("{}", serde_json::to_string(&maps)?);
            } else if let Some(index_path) = index {
                let index = FastIndex::load(&index_path)?;
                let mut map = index.repo_map_with_detail(symbols, tests, detail);
                attach_repo_map_read_batch_request_with_limit(
                    &mut map,
                    "read_ranges",
                    read_request_args("index", &index_path),
                    read_limit,
                );
                println!("{}", serde_json::to_string(&map)?);
            } else {
                let index = RepoIndexer::new(&repo).build()?;
                let mut map = index.repo_map_with_detail(symbols, tests, detail);
                attach_repo_map_read_batch_request_with_limit(
                    &mut map,
                    "read_ranges",
                    read_request_args("repo", &repo),
                    read_limit,
                );
                println!("{}", serde_json::to_string(&map)?);
            }
        }
        Commands::IndexMap {
            index,
            symbols,
            tests,
            detail,
            read_limit,
            format: _format,
        } => {
            let detail = repo_map_detail_from_cli(&detail)?;
            let read_limit = repo_map_read_limit_from_cli(read_limit)?;
            let index_path = index;
            let index = FastIndex::load(&index_path)?;
            let mut map = index.repo_map_with_detail(symbols, tests, detail);
            attach_repo_map_read_batch_request_with_limit(
                &mut map,
                "read_index_ranges",
                read_request_args("index", &index_path),
                read_limit,
            );
            println!("{}", serde_json::to_string(&map)?);
        }
        Commands::IndexPlan {
            index,
            query,
            query_arg,
            repo_filter,
            filters,
            refresh_if_stale,
        } => {
            let index_path = index;
            let index = load_index_for_search(index_path.clone(), refresh_if_stale)?;
            let filters = search_filters_from_args(&filters, repo_filter)?;
            let query = cli_single_query_for_filters(query, query_arg, &filters)?;
            let plan = attach_cli_retry_requests(
                index.query_plan(&query, &filters)?,
                "indexed_search_code",
                "index",
                &index_path,
                &filters,
            );
            println!("{}", serde_json::to_string(&plan)?);
        }
        Commands::IndexPlanBatch {
            index,
            queries,
            repo_filter,
            filters,
            refresh_if_stale,
        } => {
            let queries = cli_batch_queries(queries)?;
            let index_path = index;
            let index = load_index_for_search(index_path.clone(), refresh_if_stale)?;
            let filters = search_filters_from_args(&filters, repo_filter)?;
            let mut batch = Vec::new();
            for query in queries {
                let plan = attach_cli_retry_requests(
                    index.query_plan(&query, &filters)?,
                    "indexed_search_code",
                    "index",
                    &index_path,
                    &filters,
                );
                batch.push(IndexedQueryPlanBatchResult { query, plan });
            }
            println!("{}", serde_json::to_string(&batch)?);
        }
        Commands::SearchPlan {
            repo,
            index,
            index_dir,
            query,
            query_arg,
            repo_filter,
            filters,
            refresh_if_stale,
        } => {
            let filters = search_filters_from_args(&filters, repo_filter)?;
            let query = cli_single_query_for_filters(query, query_arg, &filters)?;
            if let Some(index_dir) = index_dir {
                if refresh_if_stale && shard_status(&index_dir)?.stale {
                    refresh_shards(&index_dir)?;
                }
                let mut plans = shard_query_plans(&index_dir, &query, &filters)?;
                attach_cli_shard_retry_requests_with_tool(
                    &mut plans, "search", &index_dir, &filters,
                );
                println!("{}", serde_json::to_string(&plans)?);
            } else if let Some(index_path) = index {
                let index = load_index_for_search(index_path.clone(), refresh_if_stale)?;
                let plan = attach_cli_retry_requests(
                    index.query_plan(&query, &filters)?,
                    "search",
                    "index",
                    &index_path,
                    &filters,
                );
                println!("{}", serde_json::to_string(&plan)?);
            } else {
                let index = FastIndex::build(repo)?;
                let plan = attach_cli_retry_requests(
                    index.query_plan(&query, &filters)?,
                    "search",
                    "repo",
                    &index.root,
                    &filters,
                );
                println!("{}", serde_json::to_string(&plan)?);
            }
        }
        Commands::SearchPlanBatch {
            repo,
            index,
            index_dir,
            queries,
            repo_filter,
            filters,
            refresh_if_stale,
        } => {
            let queries = cli_batch_queries(queries)?;
            let filters = search_filters_from_args(&filters, repo_filter)?;
            if let Some(index_dir) = index_dir {
                if refresh_if_stale && shard_status(&index_dir)?.stale {
                    refresh_shards(&index_dir)?;
                }
                let mut batch = Vec::new();
                for query in queries {
                    let mut plans = shard_query_plans(&index_dir, &query, &filters)?;
                    attach_cli_shard_retry_requests_with_tool(
                        &mut plans, "search", &index_dir, &filters,
                    );
                    batch.push(ShardQueryPlanBatchResult { query, plans });
                }
                println!("{}", serde_json::to_string(&batch)?);
            } else if let Some(index_path) = index {
                let index = load_index_for_search(index_path.clone(), refresh_if_stale)?;
                let mut batch = Vec::new();
                for query in queries {
                    let plan = attach_cli_retry_requests(
                        index.query_plan(&query, &filters)?,
                        "search",
                        "index",
                        &index_path,
                        &filters,
                    );
                    batch.push(QueryPlanBatchResult { query, plan });
                }
                println!("{}", serde_json::to_string(&batch)?);
            } else {
                let index = FastIndex::build(repo)?;
                let mut batch = Vec::new();
                for query in queries {
                    let plan = attach_cli_retry_requests(
                        index.query_plan(&query, &filters)?,
                        "search",
                        "repo",
                        &index.root,
                        &filters,
                    );
                    batch.push(QueryPlanBatchResult { query, plan });
                }
                println!("{}", serde_json::to_string(&batch)?);
            }
        }
        Commands::ReadRange {
            repo,
            index,
            index_dir,
            path,
            path_arg,
            start,
            lines,
            scope,
        } => {
            let range_spec = cli_single_range(path, path_arg, start, lines)?;
            let scope = RangeScope::from(scope);
            let scope = range_spec.scope.unwrap_or(scope);
            let range = if let Some(index_dir) = index_dir {
                read_shard_range_scoped(
                    &index_dir,
                    &range_spec.path,
                    range_spec.start,
                    range_spec.lines,
                    scope,
                )?
            } else if let Some(index_path) = index {
                FastIndex::load(index_path)?.read_range_scoped(
                    &range_spec.path,
                    range_spec.start,
                    range_spec.lines,
                    scope,
                )?
            } else {
                read_file_range_scoped(
                    repo,
                    &range_spec.path,
                    range_spec.start,
                    range_spec.lines,
                    scope,
                )?
            };
            println!("{}", serde_json::to_string(&range)?);
        }
        Commands::ReadRanges {
            repo,
            index,
            index_dir,
            ranges,
            paths,
            start,
            lines,
            scope,
        } => {
            let mut results = Vec::new();
            let scope = RangeScope::from(scope);
            if let Some(index_dir) = index_dir {
                for range in cli_ranges(paths, ranges, start, lines)? {
                    results.push(read_shard_range_scoped(
                        &index_dir,
                        &range.path,
                        range.start,
                        range.lines,
                        range.scope.unwrap_or(scope),
                    )?);
                }
            } else if let Some(index_path) = index {
                let index = FastIndex::load(index_path)?;
                for range in cli_ranges(paths, ranges, start, lines)? {
                    results.push(index.read_range_scoped(
                        &range.path,
                        range.start,
                        range.lines,
                        range.scope.unwrap_or(scope),
                    )?);
                }
            } else {
                for range in cli_ranges(paths, ranges, start, lines)? {
                    results.push(read_file_range_scoped(
                        &repo,
                        &range.path,
                        range.start,
                        range.lines,
                        range.scope.unwrap_or(scope),
                    )?);
                }
            }
            println!("{}", serde_json::to_string(&results)?);
        }
        Commands::Search {
            repo,
            index,
            index_dir,
            query,
            query_arg,
            limit,
            repo_filter,
            filters,
            context_lines,
            refresh_if_stale,
        } => {
            let filters = search_filters_from_args(&filters, repo_filter)?;
            let query = cli_single_query_for_filters(query, query_arg, &filters)?;
            let results = if let Some(index_dir) = index_dir {
                if refresh_if_stale && shard_status(&index_dir)?.stale {
                    refresh_shards(&index_dir)?;
                }
                let mut results = search_shards(&index_dir, &query, limit, &filters)?;
                attach_result_context(&mut results, context_lines, |path, start, lines| {
                    read_shard_range(&index_dir, path, start, lines)
                })?;
                attach_result_read_requests(
                    &mut results,
                    "read_shard_range",
                    read_request_args("index_dir", &index_dir),
                );
                attach_result_related_requests(
                    &mut results,
                    "related_shard_files",
                    read_request_args("index_dir", &index_dir),
                    Some(&filters),
                );
                attach_result_related_symbol_requests(
                    &mut results,
                    "related_shard_symbols",
                    Some(&query),
                    read_request_args("index_dir", &index_dir),
                );
                results
            } else if let Some(index_path) = index {
                let index = load_index_for_search(index_path.clone(), refresh_if_stale)?;
                let mut results = index.search_filtered(&query, limit, &filters)?;
                attach_cli_result_query_plan_retry_requests(
                    &mut results,
                    "indexed_search_code",
                    "index",
                    &index_path,
                    &filters,
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
                results
            } else {
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
                results
            };
            println!("{}", serde_json::to_string(&results)?);
        }
        Commands::SearchAuto {
            query,
            query_arg,
            repo,
            index,
            index_dir,
            limit,
            repo_filter,
            filters,
            context_lines,
            refresh_if_stale,
            diagnose,
            retry_if_empty,
            daemon_addr,
            no_daemon,
        } => {
            let filters = search_filters_from_args(&filters, repo_filter.clone())?;
            let query = cli_single_query_for_filters(query, query_arg, &filters)?;
            if repo.is_none() && index.is_none() && index_dir.is_none() && !no_daemon {
                let mut filters = filters.clone();
                infer_current_repo_filter_if_missing(&mut filters);
                let arguments = daemon_search_auto_arguments(
                    &query,
                    limit,
                    &filters,
                    context_lines,
                    refresh_if_stale,
                    diagnose,
                    retry_if_empty,
                );
                if let Some(mut result) =
                    try_daemon_tool_request_tcp(&daemon_addr, "search_auto", arguments)?
                {
                    retarget_client_cli_commands(&mut result, &tcp_client_command(&daemon_addr));
                    println!("{}", serde_json::to_string(&result)?);
                    return Ok(());
                }
            }
            if let Some(index_dir) = index_dir {
                if refresh_if_stale && shard_status(&index_dir)?.stale {
                    refresh_shards(&index_dir)?;
                }
                let shard_scope_filters = shard_scope_filters_for_query(&filters, &query);
                let mut results = search_shards(&index_dir, &query, limit, &filters)?;
                attach_result_context(&mut results, context_lines, |path, start, lines| {
                    read_shard_range(&index_dir, path, start, lines)
                })?;
                attach_result_read_requests(
                    &mut results,
                    "read_shard_range",
                    read_request_args("index_dir", &index_dir),
                );
                attach_result_related_requests(
                    &mut results,
                    "related_shard_files",
                    read_request_args("index_dir", &index_dir),
                    Some(&filters),
                );
                attach_result_related_symbol_requests(
                    &mut results,
                    "related_shard_symbols",
                    Some(&query),
                    read_request_args("index_dir", &index_dir),
                );
                let (query_plan_result, primary_diagnosis, primary_retry_request) =
                    if diagnose || results.is_empty() {
                        let mut plans = shard_query_plans(&index_dir, &query, &filters)?;
                        attach_cli_shard_retry_requests(&mut plans, &index_dir, &filters);
                        (
                            Some(serde_json::to_value(&plans)?),
                            primary_cli_diagnosis_from_shard_plans(&plans),
                            primary_cli_retry_request_from_shard_plans(&plans),
                        )
                    } else {
                        (None, None, None)
                    };
                let primary_retry_result = primary_cli_retry_result(
                    retry_if_empty,
                    results.is_empty(),
                    primary_retry_request.as_ref(),
                )?;
                let read_batch_request = result_read_batch_request(
                    &results,
                    "read_ranges",
                    read_request_args("index_dir", &index_dir),
                );
                let next_read_batch_request = promoted_cli_next_read_batch_request(
                    &read_batch_request,
                    &primary_retry_result,
                );
                let mut output = serde_json::json!({
                    "query": query,
                    "surface": "shards",
                    "target": index_dir,
                    "query_plan_request": {
                        "tool": "shard_query_plan",
                        "arguments": {"index_dir": index_dir, "query": query}
                    },
                    "repo_map_request": shard_repo_map_request(&index_dir, &shard_scope_filters),
                    "read_batch_request": read_batch_request,
                    "results": results
                });
                insert_optional_json_field(&mut output, "query_plan_result", query_plan_result);
                insert_optional_json_field(&mut output, "primary_diagnosis", primary_diagnosis);
                insert_optional_json_field(
                    &mut output,
                    "primary_retry_request",
                    primary_retry_request,
                );
                insert_optional_json_field(
                    &mut output,
                    "primary_retry_result",
                    primary_retry_result,
                );
                insert_optional_json_field(
                    &mut output,
                    "next_read_batch_request",
                    next_read_batch_request,
                );
                attach_cli_next_action(&mut output);
                println!("{}", serde_json::to_string(&output)?);
            } else if let Some(index_path) = index {
                let index = load_index_for_search(index_path.clone(), refresh_if_stale)?;
                let mut results = index.search_filtered(&query, limit, &filters)?;
                attach_cli_result_query_plan_retry_requests(
                    &mut results,
                    "indexed_search_code",
                    "index",
                    &index_path,
                    &filters,
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
                let (query_plan_result, primary_diagnosis, primary_retry_request) =
                    if diagnose || results.is_empty() {
                        let plan = attach_cli_retry_requests(
                            index.query_plan(&query, &filters)?,
                            "indexed_search_code",
                            "index",
                            &index_path,
                            &filters,
                        );
                        (
                            Some(serde_json::to_value(&plan)?),
                            primary_cli_diagnosis_from_plan(&plan),
                            primary_cli_retry_request_from_plan(&plan),
                        )
                    } else {
                        (None, None, None)
                    };
                let primary_retry_result = primary_cli_retry_result(
                    retry_if_empty,
                    results.is_empty(),
                    primary_retry_request.as_ref(),
                )?;
                let read_batch_request = result_read_batch_request(
                    &results,
                    "read_ranges",
                    read_request_args("index", &index_path),
                );
                let next_read_batch_request = promoted_cli_next_read_batch_request(
                    &read_batch_request,
                    &primary_retry_result,
                );
                let mut output = serde_json::json!({
                    "query": query,
                    "surface": "indexed",
                    "target": index_path,
                    "query_plan_request": {
                        "tool": "indexed_query_plan",
                        "arguments": {"index": index_path, "query": query}
                    },
                    "repo_map_request": {
                        "tool": "indexed_repo_map",
                        "arguments": {"index": index_path, "detail": "compact", "read_limit": DEFAULT_REPO_MAP_READ_BATCH_RANGES}
                    },
                    "read_batch_request": read_batch_request,
                    "results": results
                });
                insert_optional_json_field(&mut output, "query_plan_result", query_plan_result);
                insert_optional_json_field(&mut output, "primary_diagnosis", primary_diagnosis);
                insert_optional_json_field(
                    &mut output,
                    "primary_retry_request",
                    primary_retry_request,
                );
                insert_optional_json_field(
                    &mut output,
                    "primary_retry_result",
                    primary_retry_result,
                );
                insert_optional_json_field(
                    &mut output,
                    "next_read_batch_request",
                    next_read_batch_request,
                );
                attach_cli_next_action(&mut output);
                println!("{}", serde_json::to_string(&output)?);
            } else {
                let repo = repo.unwrap_or_else(|| PathBuf::from("."));
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
                let (query_plan_result, primary_diagnosis, primary_retry_request) =
                    if diagnose || results.is_empty() {
                        let index = FastIndex::build(&repo)?;
                        let plan = attach_cli_retry_requests(
                            index.query_plan(&query, &filters)?,
                            "search_code",
                            "repo",
                            &repo,
                            &filters,
                        );
                        (
                            Some(serde_json::to_value(&plan)?),
                            primary_cli_diagnosis_from_plan(&plan),
                            primary_cli_retry_request_from_plan(&plan),
                        )
                    } else {
                        (None, None, None)
                    };
                let primary_retry_result = primary_cli_retry_result(
                    retry_if_empty,
                    results.is_empty(),
                    primary_retry_request.as_ref(),
                )?;
                let read_batch_request = result_read_batch_request(
                    &results,
                    "read_ranges",
                    read_request_args("repo", &repo),
                );
                let next_read_batch_request = promoted_cli_next_read_batch_request(
                    &read_batch_request,
                    &primary_retry_result,
                );
                let mut output = serde_json::json!({
                    "query": query,
                    "surface": "fallback",
                    "target": repo,
                    "query_plan_request": {
                        "tool": "search_query_plan",
                        "arguments": {"repo": repo, "query": query}
                    },
                    "repo_map_request": {
                        "tool": "repo_map",
                        "arguments": {"repo": repo, "detail": "compact", "read_limit": DEFAULT_REPO_MAP_READ_BATCH_RANGES}
                    },
                    "read_batch_request": read_batch_request,
                    "results": results
                });
                insert_optional_json_field(&mut output, "query_plan_result", query_plan_result);
                insert_optional_json_field(&mut output, "primary_diagnosis", primary_diagnosis);
                insert_optional_json_field(
                    &mut output,
                    "primary_retry_request",
                    primary_retry_request,
                );
                insert_optional_json_field(
                    &mut output,
                    "primary_retry_result",
                    primary_retry_result,
                );
                insert_optional_json_field(
                    &mut output,
                    "next_read_batch_request",
                    next_read_batch_request,
                );
                attach_cli_next_action(&mut output);
                println!("{}", serde_json::to_string(&output)?);
            }
        }
        Commands::SearchAutoBatch {
            queries,
            repo,
            index,
            index_dir,
            limit,
            repo_filter,
            filters,
            context_lines,
            refresh_if_stale,
            diagnose,
            retry_if_empty,
            daemon_addr,
            no_daemon,
        } => {
            let queries = cli_batch_queries(queries)?;
            if repo.is_none() && index.is_none() && index_dir.is_none() && !no_daemon {
                let mut filters = search_filters_from_args(&filters, repo_filter.clone())?;
                infer_current_repo_filter_if_missing(&mut filters);
                let arguments = daemon_search_auto_batch_arguments(
                    &queries,
                    limit,
                    &filters,
                    context_lines,
                    refresh_if_stale,
                    diagnose,
                    retry_if_empty,
                );
                if let Some(mut result) =
                    try_daemon_tool_request_tcp(&daemon_addr, "search_auto_batch", arguments)?
                {
                    retarget_client_cli_commands(&mut result, &tcp_client_command(&daemon_addr));
                    println!("{}", serde_json::to_string(&result)?);
                    return Ok(());
                }
            }
            let mut batch = Vec::new();
            if let Some(index_dir) = index_dir {
                if refresh_if_stale && shard_status(&index_dir)?.stale {
                    refresh_shards(&index_dir)?;
                }
                let filters = search_filters_from_args(&filters, repo_filter)?;
                for query in queries {
                    let shard_scope_filters = shard_scope_filters_for_query(&filters, &query);
                    let mut results = search_shards(&index_dir, &query, limit, &filters)?;
                    attach_result_context(&mut results, context_lines, |path, start, lines| {
                        read_shard_range(&index_dir, path, start, lines)
                    })?;
                    attach_result_read_requests(
                        &mut results,
                        "read_shard_range",
                        read_request_args("index_dir", &index_dir),
                    );
                    attach_result_related_requests(
                        &mut results,
                        "related_shard_files",
                        read_request_args("index_dir", &index_dir),
                        Some(&filters),
                    );
                    attach_result_related_symbol_requests(
                        &mut results,
                        "related_shard_symbols",
                        Some(&query),
                        read_request_args("index_dir", &index_dir),
                    );
                    let (query_plan_result, primary_diagnosis, primary_retry_request) =
                        if diagnose || results.is_empty() {
                            let mut plans = shard_query_plans(&index_dir, &query, &filters)?;
                            attach_cli_shard_retry_requests(&mut plans, &index_dir, &filters);
                            (
                                Some(serde_json::to_value(&plans)?),
                                primary_cli_diagnosis_from_shard_plans(&plans),
                                primary_cli_retry_request_from_shard_plans(&plans),
                            )
                        } else {
                            (None, None, None)
                        };
                    let primary_retry_result = primary_cli_retry_result(
                        retry_if_empty,
                        results.is_empty(),
                        primary_retry_request.as_ref(),
                    )?;
                    let read_batch_request = result_read_batch_request(
                        &results,
                        "read_ranges",
                        read_request_args("index_dir", &index_dir),
                    );
                    let next_read_batch_request = promoted_cli_next_read_batch_request(
                        &read_batch_request,
                        &primary_retry_result,
                    );
                    let mut item = serde_json::json!({
                        "query": query,
                        "surface": "shards",
                        "target": index_dir,
                        "query_plan_request": {
                            "tool": "shard_query_plan",
                            "arguments": {"index_dir": index_dir, "query": query}
                        },
                        "repo_map_request": shard_repo_map_request(&index_dir, &shard_scope_filters),
                        "read_batch_request": read_batch_request,
                        "results": results
                    });
                    insert_optional_json_field(&mut item, "query_plan_result", query_plan_result);
                    insert_optional_json_field(&mut item, "primary_diagnosis", primary_diagnosis);
                    insert_optional_json_field(
                        &mut item,
                        "primary_retry_request",
                        primary_retry_request,
                    );
                    insert_optional_json_field(
                        &mut item,
                        "primary_retry_result",
                        primary_retry_result,
                    );
                    insert_optional_json_field(
                        &mut item,
                        "next_read_batch_request",
                        next_read_batch_request,
                    );
                    attach_cli_next_action(&mut item);
                    batch.push(item);
                }
            } else if let Some(index_path) = index {
                let index = load_index_for_search(index_path.clone(), refresh_if_stale)?;
                let filters = search_filters_from_args(&filters, repo_filter)?;
                for query in queries {
                    let mut results = index.search_filtered(&query, limit, &filters)?;
                    attach_cli_result_query_plan_retry_requests(
                        &mut results,
                        "indexed_search_code",
                        "index",
                        &index_path,
                        &filters,
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
                    let (query_plan_result, primary_diagnosis, primary_retry_request) =
                        if diagnose || results.is_empty() {
                            let plan = attach_cli_retry_requests(
                                index.query_plan(&query, &filters)?,
                                "indexed_search_code",
                                "index",
                                &index_path,
                                &filters,
                            );
                            (
                                Some(serde_json::to_value(&plan)?),
                                primary_cli_diagnosis_from_plan(&plan),
                                primary_cli_retry_request_from_plan(&plan),
                            )
                        } else {
                            (None, None, None)
                        };
                    let primary_retry_result = primary_cli_retry_result(
                        retry_if_empty,
                        results.is_empty(),
                        primary_retry_request.as_ref(),
                    )?;
                    let read_batch_request = result_read_batch_request(
                        &results,
                        "read_ranges",
                        read_request_args("index", &index_path),
                    );
                    let next_read_batch_request = promoted_cli_next_read_batch_request(
                        &read_batch_request,
                        &primary_retry_result,
                    );
                    let mut item = serde_json::json!({
                        "query": query,
                        "surface": "indexed",
                        "target": index_path,
                        "query_plan_request": {
                            "tool": "indexed_query_plan",
                            "arguments": {"index": index_path, "query": query}
                        },
                        "repo_map_request": {
                            "tool": "indexed_repo_map",
                            "arguments": {"index": index_path, "detail": "compact", "read_limit": DEFAULT_REPO_MAP_READ_BATCH_RANGES}
                        },
                        "read_batch_request": read_batch_request,
                        "results": results
                    });
                    insert_optional_json_field(&mut item, "query_plan_result", query_plan_result);
                    insert_optional_json_field(&mut item, "primary_diagnosis", primary_diagnosis);
                    insert_optional_json_field(
                        &mut item,
                        "primary_retry_request",
                        primary_retry_request,
                    );
                    insert_optional_json_field(
                        &mut item,
                        "primary_retry_result",
                        primary_retry_result,
                    );
                    insert_optional_json_field(
                        &mut item,
                        "next_read_batch_request",
                        next_read_batch_request,
                    );
                    attach_cli_next_action(&mut item);
                    batch.push(item);
                }
            } else {
                let repo = repo.unwrap_or_else(|| PathBuf::from("."));
                let filters = search_filters_from_args(&filters, repo_filter)?;
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
                    let (query_plan_result, primary_diagnosis, primary_retry_request) =
                        if diagnose || results.is_empty() {
                            let index = FastIndex::build(&repo)?;
                            let plan = attach_cli_retry_requests(
                                index.query_plan(&query, &filters)?,
                                "search_code",
                                "repo",
                                &repo,
                                &filters,
                            );
                            (
                                Some(serde_json::to_value(&plan)?),
                                primary_cli_diagnosis_from_plan(&plan),
                                primary_cli_retry_request_from_plan(&plan),
                            )
                        } else {
                            (None, None, None)
                        };
                    let primary_retry_result = primary_cli_retry_result(
                        retry_if_empty,
                        results.is_empty(),
                        primary_retry_request.as_ref(),
                    )?;
                    let read_batch_request = result_read_batch_request(
                        &results,
                        "read_ranges",
                        read_request_args("repo", &repo),
                    );
                    let next_read_batch_request = promoted_cli_next_read_batch_request(
                        &read_batch_request,
                        &primary_retry_result,
                    );
                    let mut item = serde_json::json!({
                        "query": query,
                        "surface": "fallback",
                        "target": repo,
                        "query_plan_request": {
                            "tool": "search_query_plan",
                            "arguments": {"repo": repo, "query": query}
                        },
                        "repo_map_request": {
                            "tool": "repo_map",
                            "arguments": {"repo": repo, "detail": "compact", "read_limit": DEFAULT_REPO_MAP_READ_BATCH_RANGES}
                        },
                        "read_batch_request": read_batch_request,
                        "results": results
                    });
                    insert_optional_json_field(&mut item, "query_plan_result", query_plan_result);
                    insert_optional_json_field(&mut item, "primary_diagnosis", primary_diagnosis);
                    insert_optional_json_field(
                        &mut item,
                        "primary_retry_request",
                        primary_retry_request,
                    );
                    insert_optional_json_field(
                        &mut item,
                        "primary_retry_result",
                        primary_retry_result,
                    );
                    insert_optional_json_field(
                        &mut item,
                        "next_read_batch_request",
                        next_read_batch_request,
                    );
                    attach_cli_next_action(&mut item);
                    batch.push(item);
                }
            }
            println!("{}", serde_json::to_string(&batch)?);
        }
        Commands::SearchBatch {
            repo,
            index,
            index_dir,
            queries,
            limit,
            repo_filter,
            filters,
            context_lines,
            refresh_if_stale,
        } => {
            let queries = cli_batch_queries(queries)?;
            let filters = search_filters_from_args(&filters, repo_filter)?;
            let mut batch = Vec::new();
            if let Some(index_dir) = index_dir {
                if refresh_if_stale && shard_status(&index_dir)?.stale {
                    refresh_shards(&index_dir)?;
                }
                for query in queries {
                    let mut results = search_shards(&index_dir, &query, limit, &filters)?;
                    attach_result_context(&mut results, context_lines, |path, start, lines| {
                        read_shard_range(&index_dir, path, start, lines)
                    })?;
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
                    batch.push(search_batch_result(query, read_batch_request, results));
                }
            } else if let Some(index_path) = index {
                let index = load_index_for_search(index_path.clone(), refresh_if_stale)?;
                for query in queries {
                    let mut results = index.search_filtered(&query, limit, &filters)?;
                    attach_cli_result_query_plan_retry_requests(
                        &mut results,
                        "indexed_search_code",
                        "index",
                        &index_path,
                        &filters,
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
                    let read_batch_request = result_read_batch_request(
                        &results,
                        "read_ranges",
                        read_request_args("index", &index_path),
                    );
                    batch.push(search_batch_result(query, read_batch_request, results));
                }
            } else {
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
                    batch.push(search_batch_result(query, read_batch_request, results));
                }
            }
            println!("{}", serde_json::to_string(&batch)?);
        }
        Commands::IndexedSearch {
            index,
            query,
            query_arg,
            limit,
            repo_filter,
            filters,
            context_lines,
            refresh_if_stale,
        } => {
            let index_path = index;
            let index = load_index_for_search(index_path.clone(), refresh_if_stale)?;
            let filters = search_filters_from_args(&filters, repo_filter)?;
            let query = cli_single_query_for_filters(query, query_arg, &filters)?;
            let mut results = index.search_filtered(&query, limit, &filters)?;
            attach_cli_result_query_plan_retry_requests(
                &mut results,
                "indexed_search_code",
                "index",
                &index_path,
                &filters,
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
            println!("{}", serde_json::to_string(&results)?);
        }
        Commands::IndexedSearchBatch {
            index,
            queries,
            limit,
            repo_filter,
            filters,
            context_lines,
            refresh_if_stale,
        } => {
            let queries = cli_batch_queries(queries)?;
            let index_path = index;
            let index = load_index_for_search(index_path.clone(), refresh_if_stale)?;
            let filters = search_filters_from_args(&filters, repo_filter)?;
            let mut batch = Vec::new();
            for query in queries {
                let mut results = index.search_filtered(&query, limit, &filters)?;
                attach_cli_result_query_plan_retry_requests(
                    &mut results,
                    "indexed_search_code",
                    "index",
                    &index_path,
                    &filters,
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
                batch.push(search_batch_result(query, read_batch_request, results));
            }
            println!("{}", serde_json::to_string(&batch)?);
        }
        Commands::ReadIndexRange {
            index,
            path,
            path_arg,
            start,
            lines,
            scope,
        } => {
            let range = cli_single_range(path, path_arg, start, lines)?;
            let index = FastIndex::load(index)?;
            println!(
                "{}",
                serde_json::to_string(&index.read_range_scoped(
                    &range.path,
                    range.start,
                    range.lines,
                    range.scope.unwrap_or_else(|| RangeScope::from(scope)),
                )?)?
            );
        }
        Commands::ReadIndexRanges {
            index,
            ranges,
            paths,
            start,
            lines,
            scope,
        } => {
            let index = FastIndex::load(index)?;
            let mut results = Vec::new();
            let scope = RangeScope::from(scope);
            for range in cli_ranges(paths, ranges, start, lines)? {
                results.push(index.read_range_scoped(
                    &range.path,
                    range.start,
                    range.lines,
                    range.scope.unwrap_or(scope),
                )?);
            }
            println!("{}", serde_json::to_string(&results)?);
        }
        Commands::Symbol {
            repo,
            index,
            index_dir,
            name,
            limit,
            repo_filter,
            filters,
        } => {
            let filters = search_filters_from_args(&filters, repo_filter)?;
            let (symbols, base_args) = if let Some(index_dir) = index_dir {
                (
                    find_shard_symbol(&index_dir, &name, limit, &filters)?,
                    read_request_args("index_dir", &index_dir),
                )
            } else if let Some(index_path) = index {
                let index = FastIndex::load(&index_path)?;
                (
                    index.find_symbol_filtered(&name, limit, &filters),
                    read_request_args("index", &index_path),
                )
            } else {
                let index = RepoIndexer::new(&repo).build()?;
                (
                    index.find_symbol_filtered(&name, limit, &filters),
                    read_request_args("repo", &repo),
                )
            };
            println!(
                "{}",
                serde_json::to_string(&symbol_lookup_results(symbols, "read_range", base_args))?
            );
        }
        Commands::SymbolBatch {
            repo,
            index,
            index_dir,
            names,
            limit,
            repo_filter,
            filters,
        } => {
            let filters = search_filters_from_args(&filters, repo_filter)?;
            let names = cli_batch_queries(names)?;
            let batch = if let Some(index_dir) = index_dir {
                let mut batch = Vec::new();
                for name in names {
                    let symbols = find_shard_symbol(&index_dir, &name, limit, &filters)?;
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
                batch
            } else if let Some(index_path) = index {
                let index = FastIndex::load(&index_path)?;
                names
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
                    .collect::<Vec<_>>()
            } else {
                let index = RepoIndexer::new(&repo).build()?;
                names
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
                    .collect::<Vec<_>>()
            };
            println!("{}", serde_json::to_string(&batch)?);
        }
        Commands::IndexSymbol {
            index,
            name,
            limit,
            repo_filter,
            filters,
        } => {
            let filters = search_filters_from_args(&filters, repo_filter)?;
            let index_path = index;
            let index = FastIndex::load(&index_path)?;
            let symbols = index.find_symbol_filtered(&name, limit, &filters);
            println!(
                "{}",
                serde_json::to_string(&symbol_lookup_results(
                    symbols,
                    "read_index_range",
                    read_request_args("index", &index_path)
                ))?
            );
        }
        Commands::IndexSymbolBatch {
            index,
            names,
            limit,
            repo_filter,
            filters,
        } => {
            let filters = search_filters_from_args(&filters, repo_filter)?;
            let index_path = index;
            let index = FastIndex::load(&index_path)?;
            let batch = cli_batch_queries(names)?
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
            println!("{}", serde_json::to_string(&batch)?);
        }
        Commands::Related {
            repo,
            index,
            index_dir,
            path,
            path_arg,
            limit,
            include_read_batch,
            filters,
        } => {
            let path = cli_single_path(path, path_arg)?;
            let filters = related_symbol_filters_from_args(&filters, None);
            let (related, base_args) = if let Some(index_dir) = index_dir {
                (
                    related_shard_files_filtered(&index_dir, &path, limit, &filters)?,
                    read_request_args("index_dir", &index_dir),
                )
            } else if let Some(index_path) = index {
                let index = FastIndex::load(&index_path)?;
                (
                    index.related_files_filtered(&path, limit, &filters),
                    read_request_args("index", &index_path),
                )
            } else {
                let index = RepoIndexer::new(&repo).build()?;
                (
                    index.related_files_filtered(&path, limit, &filters),
                    read_request_args("repo", &repo),
                )
            };
            print_related_response(
                related_file_lookup_results(related, "read_range", base_args.clone()),
                include_read_batch,
                "read_ranges",
                base_args,
                "Read the related files in one bounded batch.",
            )?;
        }
        Commands::RelatedIndex {
            index,
            path,
            path_arg,
            limit,
            include_read_batch,
            filters,
        } => {
            let path = cli_single_path(path, path_arg)?;
            let index_path = index;
            let filters = related_symbol_filters_from_args(&filters, None);
            let index = FastIndex::load(&index_path)?;
            let related = index.related_files_filtered(&path, limit, &filters);
            let base_args = read_request_args("index", &index_path);
            print_related_response(
                related_file_lookup_results(related, "read_index_range", base_args.clone()),
                include_read_batch,
                "read_index_ranges",
                base_args,
                "Read the related files in one bounded batch.",
            )?;
        }
        Commands::RelatedShard {
            index_dir,
            path,
            path_arg,
            limit,
            include_read_batch,
            filters,
        } => {
            let path = cli_single_path(path, path_arg)?;
            let filters = related_symbol_filters_from_args(&filters, None);
            let related = related_shard_files_filtered(&index_dir, &path, limit, &filters)?;
            let base_args = read_request_args("index_dir", &index_dir);
            print_related_response(
                related_file_lookup_results(related, "read_shard_range", base_args.clone()),
                include_read_batch,
                "read_shard_ranges",
                base_args,
                "Read the related files in one bounded batch.",
            )?;
        }
        Commands::RelatedSymbols {
            repo,
            index,
            index_dir,
            path,
            query,
            limit,
            include_read_batch,
            filters,
        } => {
            let filters = related_symbol_filters_from_args(&filters, None);
            let (related, base_args) = if let Some(index_dir) = index_dir {
                let path = path
                    .as_deref()
                    .filter(|path| !path.is_empty())
                    .ok_or_else(|| {
                        anyhow::anyhow!("provide --path PATH for shard related-symbols")
                    })?;
                (
                    related_shard_symbols_filtered(
                        &index_dir,
                        path,
                        query.as_deref(),
                        limit,
                        &filters,
                    )?,
                    read_request_args("index_dir", &index_dir),
                )
            } else if let Some(index_path) = index {
                let index = FastIndex::load(&index_path)?;
                (
                    index.related_symbols_filtered(
                        path.as_deref(),
                        query.as_deref(),
                        limit,
                        &filters,
                    ),
                    read_request_args("index", &index_path),
                )
            } else {
                let index = RepoIndexer::new(&repo).build()?;
                (
                    index.related_symbols_filtered(
                        path.as_deref(),
                        query.as_deref(),
                        limit,
                        &filters,
                    ),
                    read_request_args("repo", &repo),
                )
            };
            print_related_response(
                related_symbol_lookup_results(related, "read_range", base_args.clone()),
                include_read_batch,
                "read_ranges",
                base_args,
                "Read the related symbol definitions in one bounded batch.",
            )?;
        }
        Commands::RelatedIndexSymbols {
            index,
            path,
            query,
            limit,
            include_read_batch,
            filters,
        } => {
            let index_path = index;
            let filters = related_symbol_filters_from_args(&filters, None);
            let index = FastIndex::load(&index_path)?;
            let related =
                index.related_symbols_filtered(path.as_deref(), query.as_deref(), limit, &filters);
            let base_args = read_request_args("index", &index_path);
            print_related_response(
                related_symbol_lookup_results(related, "read_index_range", base_args.clone()),
                include_read_batch,
                "read_index_ranges",
                base_args,
                "Read the related symbol definitions in one bounded batch.",
            )?;
        }
        Commands::RelatedShardSymbols {
            index_dir,
            path,
            path_arg,
            query,
            limit,
            include_read_batch,
            filters,
        } => {
            let path = cli_single_path(path, path_arg)?;
            let filters = related_symbol_filters_from_args(&filters, None);
            let related = related_shard_symbols_filtered(
                &index_dir,
                &path,
                query.as_deref(),
                limit,
                &filters,
            )?;
            let base_args = read_request_args("index_dir", &index_dir);
            print_related_response(
                related_symbol_lookup_results(related, "read_shard_range", base_args.clone()),
                include_read_batch,
                "read_shard_ranges",
                base_args,
                "Read the related symbol definitions in one bounded batch.",
            )?;
        }
        Commands::BenchSearch {
            repo,
            index,
            mode,
            runs,
            warmup,
            limit,
            repo_filter,
            filters,
            fail_p95_ms,
            baseline,
            allow_baseline_mode_mismatch,
            require_faster_than_baseline,
            write_baseline,
            max_p95_regression,
            query_args,
            queries,
        } => {
            let queries = cli_benchmark_queries(query_args, queries)?;
            let filters = search_filters_from_args(&filters, repo_filter)?;
            let report = bench_search(BenchConfig {
                repo,
                index,
                mode,
                runs,
                warmup,
                limit,
                filters,
                queries,
            })?;
            println!("{}", serde_json::to_string(&report)?);
            if let Some(path) = write_baseline {
                write_bench_baseline(&path, &report)?;
            }
            if let Some(path) = baseline {
                compare_bench_baseline(
                    &path,
                    &report,
                    max_p95_regression,
                    allow_baseline_mode_mismatch,
                    require_faster_than_baseline,
                )?;
            }
            if let Some(threshold) = fail_p95_ms {
                fail_slow_bench_queries(&report, threshold)?;
            }
        }
        Commands::BenchShards {
            index_dir,
            cached,
            cold,
            runs,
            warmup,
            limit,
            repo,
            filters,
            fail_p95_ms,
            baseline,
            write_baseline,
            max_p95_regression,
            query_args,
            queries,
        } => {
            let queries = cli_benchmark_queries(query_args, queries)?;
            let filters = search_filters_from_args(&filters, repo)?;
            let report = bench_shards(ShardBenchConfig {
                index_dir,
                cached: cached || !cold,
                runs,
                warmup,
                limit,
                filters,
                queries,
            })?;
            println!("{}", serde_json::to_string(&report)?);
            if let Some(path) = write_baseline {
                write_bench_baseline(&path, &report)?;
            }
            if let Some(path) = baseline {
                compare_bench_baseline(&path, &report, max_p95_regression, false, false)?;
            }
            if let Some(threshold) = fail_p95_ms {
                fail_slow_bench_queries(&report, threshold)?;
            }
        }
        Commands::ToolManifest => {
            println!("{}", serde_json::to_string(&tool_manifest())?);
        }
        Commands::McpManifest => {
            println!("{}", serde_json::to_string(&mcp_tool_manifest())?);
        }
        Commands::AgentGuide {
            repo,
            index,
            index_dir,
            addr,
            profile,
        } => {
            println!(
                "{}",
                serde_json::to_string(&agent_guide(
                    repo.as_deref(),
                    index.as_deref(),
                    index_dir.as_deref(),
                    Some(&addr),
                    Some(profile.as_str()),
                ))?
            );
        }
        Commands::AgentInstructions {
            repo,
            index,
            index_dir,
            addr,
            profile,
        } => {
            println!(
                "{}",
                agent_instructions(
                    repo.as_deref(),
                    index.as_deref(),
                    index_dir.as_deref(),
                    Some(&addr),
                    Some(profile.as_str()),
                )
            );
        }
        Commands::DaemonStatus {
            socket,
            addr,
            format,
        } => {
            let (mut status, client_command) = if let Some(socket) = socket {
                (
                    daemon_status_unix(&socket)?,
                    unix_client_command(socket.as_path()),
                )
            } else {
                let addr = addr.as_deref().unwrap_or(DEFAULT_DAEMON_ADDR);
                (daemon_status_tcp(addr)?, tcp_client_command(addr))
            };
            retarget_client_cli_commands(&mut status, &client_command);
            let output = if format == "json" {
                status
            } else {
                daemon_status_summary(&status)
            };
            println!("{}", serde_json::to_string(&output)?);
        }
        Commands::Doctor {
            repo,
            index,
            index_dir,
            socket,
            addr,
            format,
            strict,
        } => {
            let report = doctor_report(DoctorConfig {
                repo,
                index,
                index_dir,
                socket,
                addr: addr.unwrap_or_else(|| DEFAULT_DAEMON_ADDR.to_string()),
            });
            match format.as_str() {
                "json" => println!("{}", serde_json::to_string(&report)?),
                _ => print_doctor_report(&report)?,
            }
            if strict && !report.ok {
                bail!("doctor found unhealthy checks");
            }
        }
        Commands::ServeJsonl => {
            let stdin = io::stdin();
            let stdout = io::stdout();
            serve_jsonl(stdin.lock(), stdout.lock())?;
        }
        Commands::ServeMcp => {
            let stdin = io::stdin();
            let stdout = io::stdout();
            serve_mcp(stdin.lock(), stdout.lock())?;
        }
        Commands::ServeTcp {
            addr,
            indexes,
            index_dirs,
            warm_index_dirs,
            max_cached_indexes,
            ensure_shard_dirs,
            repos,
            discover_roots,
            max_depth,
            discover_limit,
            family_limit,
            nested_manifests,
        } => {
            let listener = TcpListener::bind(&addr)?;
            let (runtime, ensured_shards) = bootstrap_runtime(
                indexes,
                index_dirs,
                warm_index_dirs,
                max_cached_indexes,
                ensure_shard_dirs,
                repos,
                discover_roots,
                max_depth,
                discover_limit,
                family_limit,
                nested_manifests,
            )?;
            let addr = listener.local_addr()?.to_string();
            let mut startup = serde_json::json!({
                "addr": addr.clone(),
                "transport": "tcp",
                "max_cached_indexes": runtime.max_cached_indexes(),
                "cached_indexes": runtime.cached_index_count(),
                "ensured_shards": ensured_shards,
                "daemon_status": runtime.daemon_status_for_arguments(&serde_json::json!({}))
            });
            retarget_client_cli_commands(&mut startup, &tcp_client_command(&addr));
            println!("{}", serde_json::to_string(&startup)?);
            io::stdout().flush()?;
            serve_tcp(listener, runtime)?;
        }
        #[cfg(unix)]
        Commands::ServeUnix {
            socket,
            indexes,
            index_dirs,
            warm_index_dirs,
            max_cached_indexes,
            ensure_shard_dirs,
            repos,
            discover_roots,
            max_depth,
            discover_limit,
            family_limit,
            nested_manifests,
        } => {
            prepare_unix_socket_path(&socket)?;
            if let Some(parent) = socket.parent() {
                fs::create_dir_all(parent)?;
            }
            let listener = UnixListener::bind(&socket)?;
            let (runtime, ensured_shards) = bootstrap_runtime(
                indexes,
                index_dirs,
                warm_index_dirs,
                max_cached_indexes,
                ensure_shard_dirs,
                repos,
                discover_roots,
                max_depth,
                discover_limit,
                family_limit,
                nested_manifests,
            )?;
            let mut startup = serde_json::json!({
                "socket": socket.clone(),
                "transport": "unix",
                "max_cached_indexes": runtime.max_cached_indexes(),
                "cached_indexes": runtime.cached_index_count(),
                "ensured_shards": ensured_shards,
                "daemon_status": runtime.daemon_status_for_arguments(&serde_json::json!({}))
            });
            retarget_client_cli_commands(&mut startup, &unix_client_command(&socket));
            println!("{}", serde_json::to_string(&startup)?);
            io::stdout().flush()?;
            serve_unix(listener, socket, runtime)?;
        }
        Commands::ClientJsonl { socket, addr } => {
            if let Some(socket) = socket {
                client_jsonl_unix(&socket)?;
            } else {
                client_jsonl_tcp(addr.as_deref().unwrap_or(DEFAULT_DAEMON_ADDR))?;
            }
        }
    }
    Ok(())
}

fn bootstrap_runtime(
    indexes: Vec<PathBuf>,
    index_dirs: Vec<PathBuf>,
    warm_index_dirs: Vec<PathBuf>,
    max_cached_indexes: usize,
    ensure_shard_dirs: Vec<PathBuf>,
    repos: Vec<PathBuf>,
    discover_roots: Vec<PathBuf>,
    max_depth: usize,
    discover_limit: usize,
    family_limit: Option<usize>,
    nested_manifests: bool,
) -> Result<(ToolRuntime, Vec<Value>)> {
    let runtime = ToolRuntime::with_max_cached_indexes(max_cached_indexes);
    for index in indexes {
        runtime.warm_index(index)?;
    }
    for index_dir in index_dirs {
        runtime.register_shards(index_dir)?;
    }
    for index_dir in warm_index_dirs {
        runtime.warm_shards(index_dir)?;
    }
    let mut ensured_shards = Vec::new();
    if !ensure_shard_dirs.is_empty() {
        let selection = shard_repos_from_args(
            repos,
            discover_roots,
            max_depth,
            discover_limit,
            normalize_family_limit(family_limit),
            nested_manifests,
        )?;
        for index_dir in ensure_shard_dirs {
            let stats = ensure_shards(&selection.repos, &index_dir)?;
            runtime.register_shards(index_dir)?;
            ensured_shards.push(shard_bootstrap_output(stats, selection.discovery.clone())?);
        }
    }
    Ok((runtime, ensured_shards))
}

fn client_jsonl_tcp(addr: &str) -> Result<()> {
    client_jsonl_stream(TcpStream::connect(addr)?)
}

#[cfg(unix)]
fn client_jsonl_unix(socket: &Path) -> Result<()> {
    client_jsonl_stream(UnixStream::connect(socket)?)
}

fn daemon_status_tcp(addr: &str) -> Result<Value> {
    daemon_status_stream(TcpStream::connect(addr)?)
}

fn try_daemon_tool_request_tcp(addr: &str, tool: &str, arguments: Value) -> Result<Option<Value>> {
    let stream = match TcpStream::connect(addr) {
        Ok(stream) => stream,
        Err(_) => return Ok(None),
    };
    daemon_tool_request_stream(stream, tool, arguments).map(Some)
}

fn daemon_tool_request_stream(
    stream: impl Read + Write,
    tool: &str,
    arguments: Value,
) -> Result<Value> {
    let mut reader = BufReader::new(stream);
    let request = serde_json::json!({
        "id": "cli",
        "tool": tool,
        "arguments": arguments
    });
    writeln!(reader.get_mut(), "{request}")?;
    reader.get_mut().flush()?;

    let mut response = String::new();
    reader.read_line(&mut response)?;
    if response.is_empty() {
        bail!("daemon closed connection without a response");
    }
    let response: Value = serde_json::from_str(&response)?;
    if let Some(error) = response.get("error").and_then(Value::as_str) {
        bail!("{error}");
    }
    response
        .get("result")
        .cloned()
        .ok_or_else(|| anyhow::anyhow!("daemon response did not include result"))
}

#[cfg(unix)]
fn daemon_status_unix(socket: &Path) -> Result<Value> {
    daemon_status_stream(UnixStream::connect(socket)?)
}

#[cfg(not(unix))]
fn daemon_status_unix(_socket: &Path) -> Result<Value> {
    bail!("unix sockets are not supported on this platform")
}

fn daemon_status_stream(stream: impl Read + Write) -> Result<Value> {
    let mut reader = BufReader::new(stream);
    let request = serde_json::json!({
        "id": "status",
        "tool": "daemon_status",
        "arguments": {"details": true}
    });
    writeln!(reader.get_mut(), "{request}")?;
    reader.get_mut().flush()?;

    let mut response = String::new();
    reader.read_line(&mut response)?;
    if response.is_empty() {
        bail!("daemon closed connection without a response");
    }
    let response: Value = serde_json::from_str(&response)?;
    if let Some(error) = response.get("error").and_then(Value::as_str) {
        bail!("{error}");
    }
    response
        .get("result")
        .cloned()
        .ok_or_else(|| anyhow::anyhow!("daemon response did not include result"))
}

fn daemon_status_summary(status: &Value) -> Value {
    let search_default = status
        .get("search_auto_default")
        .cloned()
        .unwrap_or_else(|| serde_json::json!({}));
    let target_present = search_default
        .get("target")
        .and_then(Value::as_str)
        .is_some_and(|target| !target.is_empty());
    serde_json::json!({
        "search_auto_default": {
            "surface": search_default.get("surface").cloned().unwrap_or(Value::Null),
            "source": search_default.get("source").cloned().unwrap_or(Value::Null),
            "target_present": target_present
        },
        "cached_indexes": status
            .get("cached_indexes")
            .cloned()
            .unwrap_or_else(|| serde_json::json!(0)),
        "max_cached_indexes": status
            .get("max_cached_indexes")
            .cloned()
            .unwrap_or_else(|| serde_json::json!(DEFAULT_MAX_CACHED_INDEXES)),
        "cached_shard_manifests": status
            .get("cached_shard_manifests")
            .cloned()
            .unwrap_or_else(|| serde_json::json!(0)),
        "footprint": status
            .get("footprint")
            .cloned()
            .unwrap_or_else(|| serde_json::json!({})),
        "default_requests_available": status.get("default_requests").is_some(),
        "details_omitted": true,
        "full_status_hint": "rerun with --format json for cached paths and copyable default requests"
    })
}

#[derive(Debug, Serialize)]
struct DoctorReport {
    ok: bool,
    checks: Vec<DoctorCheck>,
    recommendations: Vec<String>,
    commands: Vec<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    daemon_status: Option<Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    index_status: Option<Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    shard_status: Option<Value>,
}

#[derive(Debug, Serialize)]
struct DoctorCheck {
    name: String,
    status: DoctorCheckStatus,
    message: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    details: Option<Value>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
enum DoctorCheckStatus {
    Ok,
    Warn,
    Error,
}

struct DoctorConfig {
    repo: PathBuf,
    index: Option<PathBuf>,
    index_dir: Option<PathBuf>,
    socket: Option<PathBuf>,
    addr: String,
}

fn doctor_report(config: DoctorConfig) -> DoctorReport {
    let mut checks = Vec::new();
    let mut recommendations = Vec::new();
    let mut commands = Vec::new();
    let mut daemon_status = None;
    let mut index_status = None;
    let mut shard_status_value = None;

    let repo = config.repo;
    match repo.canonicalize() {
        Ok(canonical) => {
            if repo_has_git_metadata(&canonical) {
                checks.push(doctor_check(
                    "repo",
                    DoctorCheckStatus::Ok,
                    format!("repo exists: {}", canonical.display()),
                    None,
                ));
            } else {
                checks.push(doctor_check(
                    "repo",
                    DoctorCheckStatus::Warn,
                    format!(
                        "repo path exists but no .git metadata was found: {}",
                        canonical.display()
                    ),
                    None,
                ));
            }
        }
        Err(error) => checks.push(doctor_check(
            "repo",
            DoctorCheckStatus::Error,
            format!("repo path is not readable: {} ({error})", repo.display()),
            None,
        )),
    }

    for tool in ["orient", "rg", "fd"] {
        let found = command_in_path(tool);
        let status = if found {
            DoctorCheckStatus::Ok
        } else if tool == "orient" {
            DoctorCheckStatus::Warn
        } else {
            DoctorCheckStatus::Warn
        };
        let message = if found {
            format!("{tool} is on PATH")
        } else if tool == "orient" {
            "orient is not on PATH for new shell-native agents".to_string()
        } else {
            format!("{tool} is not on PATH; live fallback search may be less capable")
        };
        checks.push(doctor_check(format!("tool:{tool}"), status, message, None));
    }

    if let Some(index) = config.index.as_ref() {
        match FastIndex::load(index) {
            Ok(index_data) => match index_data.freshness() {
                Ok(freshness) => {
                    let stale = freshness.stale;
                    let status_value = serde_json::to_value(&freshness).ok();
                    checks.push(doctor_check(
                        "index",
                        if stale {
                            DoctorCheckStatus::Warn
                        } else {
                            DoctorCheckStatus::Ok
                        },
                        if stale {
                            format!("index is stale: {}", index.display())
                        } else {
                            format!("index is fresh: {}", index.display())
                        },
                        status_value.clone(),
                    ));
                    if stale {
                        commands.push(format!(
                            "orient refresh-index --repo {} --index {}",
                            shell_quote_path(&index_data.root),
                            shell_quote_path(index)
                        ));
                    }
                    index_status = status_value;
                }
                Err(error) => checks.push(doctor_check(
                    "index",
                    DoctorCheckStatus::Error,
                    format!(
                        "index freshness check failed for {}: {error}",
                        index.display()
                    ),
                    None,
                )),
            },
            Err(error) => checks.push(doctor_check(
                "index",
                DoctorCheckStatus::Error,
                format!("index could not be loaded: {} ({error})", index.display()),
                None,
            )),
        }
    }

    if let Some(index_dir) = config.index_dir.as_ref() {
        match shard_status(index_dir) {
            Ok(status) => {
                let stale = status.stale;
                let status_value = serde_json::to_value(&status).ok();
                checks.push(doctor_check(
                    "shards",
                    if stale {
                        DoctorCheckStatus::Warn
                    } else {
                        DoctorCheckStatus::Ok
                    },
                    if stale {
                        format!("shard directory is stale: {}", index_dir.display())
                    } else {
                        format!("shard directory is fresh: {}", index_dir.display())
                    },
                    status_value.clone(),
                ));
                if stale {
                    commands.push(format!(
                        "orient refresh-shards --index-dir {}",
                        shell_quote_path(index_dir)
                    ));
                }
                shard_status_value = status_value;
            }
            Err(error) => checks.push(doctor_check(
                "shards",
                DoctorCheckStatus::Error,
                format!(
                    "shard directory could not be inspected: {} ({error})",
                    index_dir.display()
                ),
                None,
            )),
        }
    }

    let daemon_result = if let Some(socket) = config.socket.as_ref() {
        daemon_status_unix(socket).map(|status| (status, unix_client_command(socket)))
    } else {
        daemon_status_tcp(&config.addr).map(|status| (status, tcp_client_command(&config.addr)))
    };
    match daemon_result {
        Ok((mut status, client_command)) => {
            retarget_client_cli_commands(&mut status, &client_command);
            let default_surface = status
                .get("search_auto_default")
                .and_then(|value| value.get("surface"))
                .and_then(Value::as_str)
                .unwrap_or("unknown");
            checks.push(doctor_check(
                "daemon",
                DoctorCheckStatus::Ok,
                format!("daemon reachable; search_auto default surface is {default_surface}"),
                Some(status.clone()),
            ));
            daemon_status = Some(status);
        }
        Err(error) => {
            checks.push(doctor_check(
                "daemon",
                DoctorCheckStatus::Warn,
                format!("daemon is not reachable: {error}"),
                None,
            ));
            if let Some(socket) = config.socket.as_ref() {
                if let Some(index_dir) = config.index_dir.as_ref() {
                    commands.push(format!(
                        "orient serve-unix --socket {} --index-dir {}",
                        shell_quote_path(socket),
                        shell_quote_path(index_dir)
                    ));
                } else if let Some(index) = config.index.as_ref() {
                    commands.push(format!(
                        "orient serve-unix --socket {} --index {}",
                        shell_quote_path(socket),
                        shell_quote_path(index)
                    ));
                }
            } else if let Some(index_dir) = config.index_dir.as_ref() {
                commands.push(format!(
                    "orient serve-tcp --addr {} --index-dir {}",
                    shell_quote(&config.addr),
                    shell_quote_path(index_dir)
                ));
            } else if let Some(index) = config.index.as_ref() {
                commands.push(format!(
                    "orient serve-tcp --addr {} --index {}",
                    shell_quote(&config.addr),
                    shell_quote_path(index)
                ));
            }
        }
    }

    if config.index.is_none() && config.index_dir.is_none() {
        recommendations.push(
            "pass --index or --index-dir to verify the exact shared search target".to_string(),
        );
        commands.push(
            "orient ensure-shards --discover-root /path/to/workspaces --output-dir /tmp/orient-shards --family-limit 2".to_string(),
        );
        commands.push(format!(
            "orient serve-tcp --addr {} --index-dir /tmp/orient-shards",
            shell_quote(&config.addr)
        ));
    }
    if daemon_status.is_some() {
        commands.push(if config.socket.is_some() {
            "orient client-jsonl --socket <socket>".to_string()
        } else {
            format!("orient client-jsonl --addr {}", shell_quote(&config.addr))
        });
    }

    recommendations.push(
        "agents should call daemon_status or search_auto before falling back to scattered shell search"
            .to_string(),
    );

    let ok = !checks
        .iter()
        .any(|check| check.status == DoctorCheckStatus::Error);
    commands.sort();
    commands.dedup();
    DoctorReport {
        ok,
        checks,
        recommendations,
        commands,
        daemon_status,
        index_status,
        shard_status: shard_status_value,
    }
}

fn doctor_check(
    name: impl Into<String>,
    status: DoctorCheckStatus,
    message: impl Into<String>,
    details: Option<Value>,
) -> DoctorCheck {
    DoctorCheck {
        name: name.into(),
        status,
        message: message.into(),
        details,
    }
}

fn print_doctor_report(report: &DoctorReport) -> Result<()> {
    println!(
        "Orient doctor: {}",
        if report.ok {
            "healthy"
        } else {
            "needs attention"
        }
    );
    for check in &report.checks {
        let label = match check.status {
            DoctorCheckStatus::Ok => "ok",
            DoctorCheckStatus::Warn => "warn",
            DoctorCheckStatus::Error => "error",
        };
        println!("[{label}] {}: {}", check.name, check.message);
    }
    if !report.recommendations.is_empty() {
        println!();
        println!("Recommendations:");
        for recommendation in &report.recommendations {
            println!("- {recommendation}");
        }
    }
    if !report.commands.is_empty() {
        println!();
        println!("Commands:");
        for command in &report.commands {
            println!("- {command}");
        }
    }
    Ok(())
}

fn repo_has_git_metadata(path: &Path) -> bool {
    path.ancestors()
        .any(|ancestor| ancestor.join(".git").exists())
}

fn infer_current_repo_filter_if_missing(filters: &mut SearchFilters) {
    if filters.repo.is_some() {
        return;
    }
    let Some(root) = current_git_repo_root() else {
        return;
    };
    filters.repo = Some(root.to_string_lossy().to_string());
}

fn current_git_repo_root() -> Option<PathBuf> {
    let cwd = env::current_dir().ok()?;
    let cwd = cwd.canonicalize().unwrap_or(cwd);
    cwd.ancestors()
        .find(|ancestor| ancestor.join(".git").exists())
        .map(Path::to_path_buf)
}

fn command_in_path(name: &str) -> bool {
    let Some(path) = env::var_os("PATH") else {
        return false;
    };
    env::split_paths(&path).any(|dir| {
        let candidate = dir.join(name);
        if candidate.is_file() {
            return true;
        }
        #[cfg(windows)]
        {
            dir.join(format!("{name}.exe")).is_file()
        }
        #[cfg(not(windows))]
        {
            false
        }
    })
}

fn shell_quote_path(path: &Path) -> String {
    shell_quote(&path.to_string_lossy())
}

fn shell_quote(value: &str) -> String {
    format!("'{}'", value.replace('\'', "'\\''"))
}

fn client_jsonl_stream(stream: impl Read + Write) -> Result<()> {
    let cwd = env::current_dir().ok();
    client_jsonl_stream_with_cwd(stream, cwd.as_deref())
}

fn client_jsonl_stream_with_cwd(stream: impl Read + Write, cwd: Option<&Path>) -> Result<()> {
    let mut reader = BufReader::new(stream);
    let stdin = io::stdin();
    let mut stdout = io::stdout();
    let mut response = String::new();

    for line in stdin.lock().lines() {
        let line = line?;
        if line.trim().is_empty() {
            continue;
        }
        let line = client_jsonl_request_with_default_cwd(&line, cwd)?;
        writeln!(reader.get_mut(), "{line}")?;
        reader.get_mut().flush()?;
        response.clear();
        reader.read_line(&mut response)?;
        if response.is_empty() {
            bail!("daemon closed connection without a response");
        }
        write!(stdout, "{response}")?;
        stdout.flush()?;
    }

    Ok(())
}

fn client_jsonl_request_with_default_cwd(line: &str, cwd: Option<&Path>) -> Result<String> {
    let Some(cwd) = cwd else {
        return Ok(line.to_string());
    };
    let mut request = match serde_json::from_str::<Value>(line) {
        Ok(request) => request,
        Err(_) => return Ok(line.to_string()),
    };
    let tool = request
        .get("tool")
        .and_then(Value::as_str)
        .unwrap_or_default();
    if !client_jsonl_tool_accepts_default_cwd(tool) {
        return Ok(line.to_string());
    }
    let Some(request_object) = request.as_object_mut() else {
        return Ok(line.to_string());
    };
    let arguments = request_object
        .entry("arguments")
        .or_insert_with(|| Value::Object(Map::new()));
    if arguments.is_null() {
        *arguments = Value::Object(Map::new());
    }
    let Some(arguments) = arguments.as_object_mut() else {
        return Ok(line.to_string());
    };
    if client_jsonl_arguments_have_target(arguments) {
        return Ok(line.to_string());
    }
    arguments.insert(
        "cwd".to_string(),
        Value::String(cwd.to_string_lossy().to_string()),
    );
    serde_json::to_string(&request).map_err(Into::into)
}

fn client_jsonl_tool_accepts_default_cwd(tool: &str) -> bool {
    matches!(
        tool,
        "search"
            | "search_code"
            | "search_batch"
            | "search_auto"
            | "search_auto_batch"
            | "repo_map"
            | "search_plan"
            | "search_plan_batch"
            | "find_symbol"
            | "find_symbol_batch"
            | "read_range"
            | "open_range"
            | "read_ranges"
            | "open_ranges"
            | "related_files"
            | "related_symbols"
    )
}

fn client_jsonl_arguments_have_target(arguments: &Map<String, Value>) -> bool {
    ["repo", "index", "index_dir", "cwd", "repo_filter"]
        .iter()
        .any(|key| arguments.get(*key).is_some_and(|value| !value.is_null()))
}

#[cfg(unix)]
fn serve_unix(listener: UnixListener, socket: PathBuf, runtime: ToolRuntime) -> Result<()> {
    let runtime = Arc::new(runtime);
    let client_command = unix_client_command(&socket);
    for stream in listener.incoming() {
        let stream = stream?;
        let runtime = Arc::clone(&runtime);
        let client_command = client_command.clone();
        std::thread::spawn(move || {
            let _ = serve_jsonl_stream_with_client_command(stream, runtime, Some(client_command));
        });
    }
    Ok(())
}

#[cfg(unix)]
fn prepare_unix_socket_path(socket: &Path) -> Result<()> {
    if !socket.exists() {
        return Ok(());
    }
    if !fs::symlink_metadata(socket)?.file_type().is_socket() {
        bail!("refusing to remove non-socket path: {}", socket.display());
    }
    if UnixStream::connect(socket).is_ok() {
        bail!(
            "refusing to replace active unix socket: {}",
            socket.display()
        );
    }
    fs::remove_file(socket)?;
    Ok(())
}

struct ShardRepoSelection {
    repos: Vec<PathBuf>,
    discovery: Vec<DiscoverySelectionSummary>,
}

fn shard_repos_from_args(
    mut repos: Vec<PathBuf>,
    discover_roots: Vec<PathBuf>,
    max_depth: usize,
    discover_limit: usize,
    family_limit: Option<usize>,
    nested_manifests: bool,
) -> Result<ShardRepoSelection> {
    let mut discovery = Vec::new();
    for root in discover_roots {
        let discovered = discover_repos(
            root,
            &DiscoverOptions {
                max_depth,
                limit: discover_limit,
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
    Ok(ShardRepoSelection { repos, discovery })
}

fn shard_repos_from_args_required(
    repos: Vec<PathBuf>,
    discover_roots: Vec<PathBuf>,
    max_depth: usize,
    discover_limit: usize,
    family_limit: Option<usize>,
    nested_manifests: bool,
) -> Result<ShardRepoSelection> {
    let selection = shard_repos_from_args(
        repos,
        discover_roots,
        max_depth,
        discover_limit,
        family_limit,
        nested_manifests,
    )?;
    if selection.repos.is_empty() {
        bail!("provide at least one --repo or --discover-root");
    }
    Ok(selection)
}

fn normalize_family_limit(value: Option<usize>) -> Option<usize> {
    value.filter(|limit| *limit > 0)
}

fn refresh_or_build_index(repo: PathBuf, index: PathBuf) -> Result<RefreshStats> {
    let previous = if index.exists() {
        FastIndex::load_reusable(&index)?
    } else {
        None
    };
    let outcome = FastIndex::refresh(repo, previous.as_ref())?;
    outcome.index.save(&index)?;
    Ok(outcome.index.refresh_stats(&outcome))
}

fn load_index_for_search(index_path: PathBuf, refresh_if_stale: bool) -> Result<FastIndex> {
    let index = FastIndex::load(&index_path)?;
    if !refresh_if_stale || !index.freshness()?.stale {
        return Ok(index);
    }
    refresh_or_build_index(index.root.clone(), index_path.clone())?;
    FastIndex::load(index_path)
}

#[derive(Debug, Clone)]
struct CliRangeSpec {
    path: String,
    start: usize,
    lines: usize,
    scope: Option<RangeScope>,
}

impl FromStr for CliRangeSpec {
    type Err = String;

    fn from_str(value: &str) -> std::result::Result<Self, Self::Err> {
        let (value, scope) = split_cli_range_scope(value);
        if let Some(range) = parse_compact_cli_range(value, scope)? {
            return Ok(range);
        }
        if let Some(range) = parse_copied_location_cli_range(value, scope) {
            return Ok(range);
        }
        Err("range must be PATH:START:LINES[:SCOPE] or a copied PATH:LINE location".to_string())
    }
}

fn split_cli_range_scope(value: &str) -> (&str, Option<RangeScope>) {
    let Some((base, scope_text)) = value.rsplit_once(':') else {
        return (value, None);
    };
    let Some(scope) = RangeScope::parse(scope_text) else {
        return (value, None);
    };
    (base, Some(scope))
}

fn parse_compact_cli_range(
    value: &str,
    scope: Option<RangeScope>,
) -> std::result::Result<Option<CliRangeSpec>, String> {
    let mut parts = value.rsplitn(3, ':');
    let lines = parts
        .next()
        .ok_or_else(|| "range must be PATH:START:LINES".to_string())?
        .parse::<usize>()
        .ok();
    let Some(lines) = lines else {
        return Ok(None);
    };
    let start = parts
        .next()
        .ok_or_else(|| "range must be PATH:START:LINES".to_string())?
        .parse::<usize>()
        .ok();
    let Some(start) = start else {
        return Ok(None);
    };
    let path = parts
        .next()
        .filter(|path| !path.is_empty())
        .ok_or_else(|| "range must be PATH:START:LINES".to_string())?
        .to_string();
    if start == 0 || lines == 0 {
        return Err("range start and lines must be positive integers".to_string());
    }
    Ok(Some(CliRangeSpec {
        path,
        start,
        lines,
        scope,
    }))
}

fn parse_copied_location_cli_range(value: &str, scope: Option<RangeScope>) -> Option<CliRangeSpec> {
    let parsed = parse_query(value);
    let start = parsed.filters.target_line?;
    let path = parsed
        .filters
        .path
        .or(parsed.filters.file)
        .filter(|path| !path.is_empty())?;
    Some(CliRangeSpec {
        path,
        start,
        lines: copied_hash_anchor_lines(value).unwrap_or(DEFAULT_CLI_READ_RANGE_LINES),
        scope,
    })
}

fn copied_hash_anchor_lines(value: &str) -> Option<usize> {
    let lower = value.to_ascii_lowercase();
    let marker = lower.find("#l")?;
    let after_marker = &value[marker + 2..];
    let (start, after_start) = split_leading_digits(after_marker)?;
    let after_start = after_start.trim_start();
    let after_dash = after_start.strip_prefix('-')?.trim_start();
    let after_optional_l = after_dash
        .strip_prefix('L')
        .or_else(|| after_dash.strip_prefix('l'))
        .unwrap_or(after_dash);
    let (end, _) = split_leading_digits(after_optional_l)?;
    (end >= start).then_some(end - start + 1)
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

fn cli_ranges(
    paths: Vec<String>,
    mut ranges: Vec<CliRangeSpec>,
    start: usize,
    lines: usize,
) -> Result<Vec<CliRangeSpec>> {
    validate_cli_range_bounds(start, lines)?;
    ranges.extend(paths.into_iter().map(|path| {
        CliRangeSpec::from_str(&path).unwrap_or(CliRangeSpec {
            path,
            start,
            lines,
            scope: None,
        })
    }));
    if ranges.is_empty() {
        bail!("provide at least one path or --range PATH:START:LINES[:SCOPE]");
    }
    for range in &ranges {
        validate_cli_range_spec(range)?;
    }
    if ranges.len() > MAX_BATCH_RANGES {
        bail!(
            "ranges has {} items, max {}",
            ranges.len(),
            MAX_BATCH_RANGES
        );
    }
    validate_cli_batch_read_line_budget(&ranges)?;
    Ok(ranges)
}

fn validate_cli_batch_read_line_budget(ranges: &[CliRangeSpec]) -> Result<()> {
    let total = ranges
        .iter()
        .try_fold(0usize, |total, range| total.checked_add(range.lines))
        .ok_or_else(|| anyhow::anyhow!("batch read line count overflowed"))?;
    if total > MAX_BATCH_READ_LINES {
        bail!(
            "ranges request {total} total lines, max {MAX_BATCH_READ_LINES}; split into smaller read-ranges calls or lower lines per range"
        );
    }
    Ok(())
}

fn cli_single_range(
    path: Option<String>,
    path_arg: Option<String>,
    start: usize,
    lines: usize,
) -> Result<CliRangeSpec> {
    validate_cli_range_bounds(start, lines)?;
    if let Some(path) = path {
        let range = CliRangeSpec::from_str(&path).unwrap_or(CliRangeSpec {
            path,
            start,
            lines,
            scope: None,
        });
        validate_cli_range_spec(&range)?;
        return Ok(range);
    }
    if let Some(path) = path_arg {
        let range = CliRangeSpec::from_str(&path).unwrap_or(CliRangeSpec {
            path,
            start,
            lines,
            scope: None,
        });
        validate_cli_range_spec(&range)?;
        return Ok(range);
    }
    bail!("provide a path, PATH:START:LINES[:SCOPE], or --path PATH")
}

fn cli_single_path(path: Option<String>, path_arg: Option<String>) -> Result<String> {
    path.or(path_arg)
        .filter(|path| !path.is_empty())
        .ok_or_else(|| anyhow::anyhow!("provide a path or --path PATH"))
}

fn cli_single_query_for_filters(
    query: Option<String>,
    query_arg: Option<String>,
    filters: &SearchFilters,
) -> Result<String> {
    let query = query.or(query_arg).unwrap_or_default();
    if query.is_empty() && !cli_filter_only_query(filters) {
        bail!("provide a query or --query QUERY");
    }
    Ok(query)
}

fn cli_filter_only_query(filters: &SearchFilters) -> bool {
    filters.file.is_some()
        || filters.path.is_some()
        || filters.language.is_some()
        || filters.extension.is_some()
        || filters.symbol_kind.is_some()
        || filters.repo.is_some()
        || filters.branch.is_some()
        || filters.origin.is_some()
        || filters.dependency.is_some()
        || filters.import.is_some()
        || filters.test.is_some()
        || filters.generated.is_some()
        || filters.code.is_some()
}

fn validate_cli_range_bounds(start: usize, lines: usize) -> Result<()> {
    if start == 0 {
        bail!("range start must be a positive integer");
    }
    if lines == 0 {
        bail!("range lines must be a positive integer");
    }
    if lines > MAX_READ_RANGE_LINES {
        bail!("range lines has {lines}, max {MAX_READ_RANGE_LINES}");
    }
    Ok(())
}

fn validate_cli_range_spec(range: &CliRangeSpec) -> Result<()> {
    validate_cli_range_bounds(range.start, range.lines)
}

fn cli_batch_queries(queries: Vec<String>) -> Result<Vec<String>> {
    if queries.len() > MAX_BATCH_QUERIES {
        bail!(
            "queries has {} items, max {}",
            queries.len(),
            MAX_BATCH_QUERIES
        );
    }
    Ok(queries)
}

fn cli_benchmark_queries(query_args: Vec<String>, queries: Vec<String>) -> Result<Vec<String>> {
    let queries = query_args
        .into_iter()
        .chain(queries)
        .filter(|query| !query.is_empty())
        .collect::<Vec<_>>();
    if queries.is_empty() {
        bail!("provide at least one query or --query QUERY");
    }
    cli_batch_queries(queries)
}

fn shard_bootstrap_output<T: Serialize>(
    stats: T,
    discovery: Vec<DiscoverySelectionSummary>,
) -> Result<Value> {
    let mut value = serde_json::to_value(stats)?;
    if !discovery.is_empty() {
        let object = value
            .as_object_mut()
            .ok_or_else(|| anyhow::anyhow!("shard stats did not serialize to an object"))?;
        object.insert("discovery".to_string(), serde_json::to_value(discovery)?);
    }
    Ok(value)
}

fn shard_status_summary(status: &ShardFreshness) -> Value {
    let mut largest = status
        .shards
        .iter()
        .map(|shard| {
            (
                shard.status.index_bytes,
                serde_json::json!({
                    "name": shard.name,
                    "root": shard.root,
                    "index": shard.index,
                    "index_bytes": shard.status.index_bytes,
                    "source_bytes": shard.status.source_bytes,
                    "content_snapshot_bytes": shard.status.content_snapshot_bytes,
                    "line_offset_bytes": shard.status.line_offset_bytes,
                    "compressed_posting_bytes": shard.status.compressed_posting_bytes,
                    "files": shard.status.indexed_files,
                    "symbols": shard.status.symbols,
                    "stale": shard.status.stale || shard.git_metadata_stale,
                    "git_metadata_stale": shard.git_metadata_stale
                }),
            )
        })
        .collect::<Vec<_>>();
    largest.sort_by(|left, right| right.0.cmp(&left.0));
    largest.truncate(10);

    let stale_shards = status
        .shards
        .iter()
        .filter(|shard| shard.status.stale || shard.git_metadata_stale)
        .take(20)
        .map(|shard| {
            serde_json::json!({
                "name": shard.name,
                "root": shard.root,
                "changed_files": shard.status.changed_files,
                "deleted_files": shard.status.deleted_files,
                "added_files": shard.status.added_files,
                "git_metadata_stale": shard.git_metadata_stale
            })
        })
        .collect::<Vec<_>>();

    serde_json::json!({
        "version": status.version,
        "index_dir": status.index_dir,
        "shard_count": status.shard_count,
        "manifest_bytes": status.manifest_bytes,
        "manifest_sidecar_bytes": status.manifest_sidecar_bytes,
        "manifest_prefilter_bytes": status.manifest_prefilter_bytes,
        "manifest_route_bytes": status.manifest_route_bytes,
        "manifest_route_exact_terms": status.manifest_route_exact_terms,
        "manifest_route_trigram_terms": status.manifest_route_trigram_terms,
        "manifest_route_substring_filter_shards": status.manifest_route_substring_filter_shards,
        "manifest_route_omitted_exact_terms": status.manifest_route_omitted_exact_terms,
        "manifest_route_omitted_trigram_terms": status.manifest_route_omitted_trigram_terms,
        "stale": status.stale,
        "stale_shards": status.stale_shards,
        "index_bytes": status.index_bytes,
        "source_bytes": status.source_bytes,
        "content_snapshot_bytes": status.content_snapshot_bytes,
        "line_offset_bytes": status.line_offset_bytes,
        "terms": status.terms,
        "path_terms": status.path_terms,
        "trigrams": status.trigrams,
        "posting_entries": status.posting_entries,
        "compressed_posting_bytes": status.compressed_posting_bytes,
        "symbols": status.symbols,
        "changed_files": status.changed_files,
        "deleted_files": status.deleted_files,
        "added_files": status.added_files,
        "git_metadata_changed": status.git_metadata_changed,
        "largest_shards": largest.into_iter().map(|(_, value)| value).collect::<Vec<_>>(),
        "stale_shard_examples": stale_shards
    })
}

struct BenchConfig {
    repo: PathBuf,
    index: Option<PathBuf>,
    mode: BenchSearchMode,
    runs: usize,
    warmup: usize,
    limit: usize,
    filters: SearchFilters,
    queries: Vec<String>,
}

struct ShardBenchConfig {
    index_dir: PathBuf,
    cached: bool,
    runs: usize,
    warmup: usize,
    limit: usize,
    filters: SearchFilters,
    queries: Vec<String>,
}

fn bench_search(config: BenchConfig) -> Result<BenchReport> {
    let runs = config.runs.max(1);
    let indexed = match (config.mode, config.index.as_ref()) {
        (BenchSearchMode::Auto, Some(index)) | (BenchSearchMode::Indexed, Some(index)) => {
            Some(FastIndex::load(index)?)
        }
        (BenchSearchMode::Auto | BenchSearchMode::Fallback, None) => None,
        (BenchSearchMode::Fallback, Some(_)) => {
            bail!("--mode fallback cannot be combined with --index")
        }
        (BenchSearchMode::Indexed, None) => Some(FastIndex::build(&config.repo)?),
    };
    let mode = if indexed.is_some() {
        "indexed".to_string()
    } else {
        "fallback".to_string()
    };
    let mut query_reports = Vec::new();

    for query in &config.queries {
        for _ in 0..config.warmup {
            let _ = run_search_once(
                &config.repo,
                indexed.as_ref(),
                query,
                config.limit,
                &config.filters,
            )?;
        }

        let mut samples_ms = Vec::with_capacity(runs);
        let mut result_count = 0usize;
        for _ in 0..runs {
            let started = Instant::now();
            let results = run_search_once(
                &config.repo,
                indexed.as_ref(),
                query,
                config.limit,
                &config.filters,
            )?;
            samples_ms.push(started.elapsed().as_secs_f64() * 1_000.0);
            result_count = results.len();
        }
        query_reports.push(summarize_query(query, result_count, samples_ms));
    }

    Ok(BenchReport {
        mode,
        runs,
        warmup: config.warmup,
        limit: config.limit,
        queries: query_reports,
    })
}

fn bench_shards(config: ShardBenchConfig) -> Result<BenchReport> {
    let runs = config.runs.max(1);
    let runtime = config.cached.then(ToolRuntime::default);
    if let Some(runtime) = &runtime {
        runtime.warm_shards(config.index_dir.clone())?;
    }
    let mut query_reports = Vec::new();

    for query in &config.queries {
        for _ in 0..config.warmup {
            let _ = run_shard_search_once(
                &config.index_dir,
                runtime.as_ref(),
                query,
                config.limit,
                &config.filters,
            )?;
        }

        let mut samples_ms = Vec::with_capacity(runs);
        let mut result_count = 0usize;
        for _ in 0..runs {
            let started = Instant::now();
            let results = run_shard_search_once(
                &config.index_dir,
                runtime.as_ref(),
                query,
                config.limit,
                &config.filters,
            )?;
            samples_ms.push(started.elapsed().as_secs_f64() * 1_000.0);
            result_count = results.len();
        }
        query_reports.push(summarize_query(query, result_count, samples_ms));
    }

    Ok(BenchReport {
        mode: if config.cached {
            "shards_cached".to_string()
        } else {
            "shards".to_string()
        },
        runs,
        warmup: config.warmup,
        limit: config.limit,
        queries: query_reports,
    })
}

fn run_shard_search_once(
    index_dir: &Path,
    runtime: Option<&ToolRuntime>,
    query: &str,
    limit: usize,
    filters: &SearchFilters,
) -> Result<Vec<orient::repo_index::SearchResult>> {
    if let Some(runtime) = runtime {
        runtime.search_warm_shards(index_dir, query, limit, filters)
    } else {
        search_shards(index_dir, query, limit, filters)
    }
}

fn run_search_once(
    repo: &PathBuf,
    index: Option<&FastIndex>,
    query: &str,
    limit: usize,
    filters: &SearchFilters,
) -> Result<Vec<orient::repo_index::SearchResult>> {
    if let Some(index) = index {
        index.search_filtered(query, limit, filters)
    } else {
        search_repo_fast_filtered(repo, query, limit, filters)
    }
}

fn summarize_query(query: &str, result_count: usize, mut samples_ms: Vec<f64>) -> QueryBench {
    samples_ms.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
    let min_ms = *samples_ms.first().unwrap_or(&0.0);
    let max_ms = *samples_ms.last().unwrap_or(&0.0);
    let p50_ms = percentile(&samples_ms, 0.50);
    let p95_ms = percentile(&samples_ms, 0.95);
    let p99_ms = percentile(&samples_ms, 0.99);
    QueryBench {
        query: query.to_string(),
        result_count,
        min_ms: round_ms(min_ms),
        p50_ms: round_ms(p50_ms),
        p95_ms: round_ms(p95_ms),
        p99_ms: round_ms(p99_ms),
        max_ms: round_ms(max_ms),
        samples_ms: samples_ms.into_iter().map(round_ms).collect(),
    }
}

fn percentile(sorted: &[f64], quantile: f64) -> f64 {
    if sorted.is_empty() {
        return 0.0;
    }
    let index = ((sorted.len() as f64 * quantile).ceil() as usize)
        .saturating_sub(1)
        .min(sorted.len() - 1);
    sorted[index]
}

fn round_ms(value: f64) -> f64 {
    (value * 1_000.0).round() / 1_000.0
}

fn write_bench_baseline(path: &PathBuf, report: &BenchReport) -> Result<()> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }
    fs::write(path, serde_json::to_vec_pretty(report)?)?;
    Ok(())
}

fn compare_bench_baseline(
    path: &PathBuf,
    current: &BenchReport,
    max_regression: f64,
    allow_mode_mismatch: bool,
    require_faster: bool,
) -> Result<()> {
    let baseline = serde_json::from_slice::<BenchReport>(&fs::read(path)?)?;
    if !allow_mode_mismatch && baseline.mode != current.mode {
        bail!(
            "benchmark mode {:?} does not match baseline mode {:?}",
            current.mode,
            baseline.mode
        );
    }

    for query in &current.queries {
        let Some(previous) = baseline
            .queries
            .iter()
            .find(|previous| previous.query == query.query)
        else {
            bail!("query {:?} is missing from benchmark baseline", query.query);
        };
        if require_faster && query.p95_ms >= previous.p95_ms {
            bail!(
                "p95 {:.3}ms for query {:?} was not faster than baseline {:.3}ms",
                query.p95_ms,
                query.query,
                previous.p95_ms
            );
        }
        let allowed = previous.p95_ms * (1.0 + max_regression.max(0.0));
        if query.p95_ms > allowed {
            bail!(
                "p95 {:.3}ms for query {:?} exceeded baseline {:.3}ms by more than {:.1}%",
                query.p95_ms,
                query.query,
                previous.p95_ms,
                max_regression.max(0.0) * 100.0
            );
        }
    }

    Ok(())
}

fn fail_slow_bench_queries(report: &BenchReport, threshold: f64) -> Result<()> {
    if let Some(slowest) = report
        .queries
        .iter()
        .filter(|query| query.p95_ms > threshold)
        .max_by(|left, right| left.p95_ms.total_cmp(&right.p95_ms))
    {
        bail!(
            "p95 {:.3}ms for query {:?} exceeded threshold {:.3}ms",
            slowest.p95_ms,
            slowest.query,
            threshold
        );
    }
    Ok(())
}

fn snippet_mode_arg(value: &str) -> Result<SnippetMode> {
    SnippetMode::parse(value)
        .ok_or_else(|| anyhow::anyhow!("snippet must be one of: short, medium, block, symbol"))
}

fn normalize_filter(value: &str) -> String {
    value.to_ascii_lowercase()
}

fn normalize_extension(value: &str) -> String {
    value.trim_start_matches('.').to_ascii_lowercase()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cli_range_spec_parses_paths_with_colons_from_the_right() {
        let range = CliRangeSpec::from_str("src/auth:token.rs:12:4").unwrap();

        assert_eq!(range.path, "src/auth:token.rs");
        assert_eq!(range.start, 12);
        assert_eq!(range.lines, 4);
        assert_eq!(range.scope, None);
    }

    #[test]
    fn cli_range_spec_accepts_optional_trailing_scope() {
        let range = CliRangeSpec::from_str("src/auth:token.rs:12:4:symbol").unwrap();

        assert_eq!(range.path, "src/auth:token.rs");
        assert_eq!(range.start, 12);
        assert_eq!(range.lines, 4);
        assert_eq!(range.scope, Some(RangeScope::Symbol));

        let exact = CliRangeSpec::from_str("src/auth.rs:12:4:exact").unwrap();
        assert_eq!(exact.scope, Some(RangeScope::Exact));
    }

    #[test]
    fn cli_range_spec_accepts_copied_locations() {
        let line = CliRangeSpec::from_str("src/auth.rs:12").unwrap();
        assert_eq!(line.path, "src/auth.rs");
        assert_eq!(line.start, 12);
        assert_eq!(line.lines, DEFAULT_CLI_READ_RANGE_LINES);
        assert_eq!(line.scope, None);

        let copied_line = CliRangeSpec::from_str("src/auth.rs:12: pub fn issue_token").unwrap();
        assert_eq!(copied_line.path, "src/auth.rs");
        assert_eq!(copied_line.start, 12);
        assert_eq!(copied_line.lines, DEFAULT_CLI_READ_RANGE_LINES);

        let hash = CliRangeSpec::from_str("src/auth.rs#L12-L15").unwrap();
        assert_eq!(hash.path, "src/auth.rs");
        assert_eq!(hash.start, 12);
        assert_eq!(hash.lines, 4);

        let markdown =
            CliRangeSpec::from_str("[src/auth.rs#L12-L15](src/auth.rs#L12-L15)").unwrap();
        assert_eq!(markdown.path, "src/auth.rs");
        assert_eq!(markdown.start, 12);
        assert_eq!(markdown.lines, 4);

        let hosted = CliRangeSpec::from_str(
            "https://github.com/evalops/orient-search/blob/main/src/auth.rs#L12-L15",
        )
        .unwrap();
        assert_eq!(hosted.path, "src/auth.rs");
        assert_eq!(hosted.start, 12);
        assert_eq!(hosted.lines, 4);

        let hosted_query = CliRangeSpec::from_str(
            "https://github.com/evalops/orient-search/blob/main/src/auth.rs?plain=1#L12-L15",
        )
        .unwrap();
        assert_eq!(hosted_query.path, "src/auth.rs");
        assert_eq!(hosted_query.start, 12);
        assert_eq!(hosted_query.lines, 4);

        let sourcegraph = CliRangeSpec::from_str(
            "https://sourcegraph.com/github.com/evalops/orient-search/-/blob/src/auth.rs?L12:7",
        )
        .unwrap();
        assert_eq!(sourcegraph.path, "src/auth.rs");
        assert_eq!(sourcegraph.start, 12);
        assert_eq!(sourcegraph.lines, DEFAULT_CLI_READ_RANGE_LINES);

        let wrapped =
            CliRangeSpec::from_str("at Object.handle (src/auth.rs#L12-L15):symbol").unwrap();
        assert_eq!(wrapped.path, "src/auth.rs");
        assert_eq!(wrapped.start, 12);
        assert_eq!(wrapped.lines, 4);
        assert_eq!(wrapped.scope, Some(RangeScope::Symbol));
    }

    #[test]
    fn cli_range_spec_rejects_zero_start_or_lines() {
        assert!(CliRangeSpec::from_str("src/auth.rs:0:1").is_err());
        assert!(CliRangeSpec::from_str("src/auth.rs:1:0").is_err());
    }

    #[test]
    fn client_jsonl_adds_cwd_to_target_aware_requests() {
        let request = r#"{"id":"search","tool":"search_auto","arguments":{"query":"token"}}"#;
        let updated =
            client_jsonl_request_with_default_cwd(request, Some(Path::new("/tmp/current-repo")))
                .unwrap();
        let updated: Value = serde_json::from_str(&updated).unwrap();

        assert_eq!(
            updated["arguments"]["cwd"],
            serde_json::json!("/tmp/current-repo")
        );
        assert_eq!(updated["arguments"]["query"], serde_json::json!("token"));
    }

    #[test]
    fn client_jsonl_preserves_explicit_targets_and_non_target_tools() {
        let explicit = r#"{"id":"search","tool":"search_auto","arguments":{"repo":"/tmp/repo","query":"token"}}"#;
        assert_eq!(
            client_jsonl_request_with_default_cwd(explicit, Some(Path::new("/tmp/current-repo")))
                .unwrap(),
            explicit
        );

        let repo_filter = r#"{"id":"search","tool":"search_auto","arguments":{"repo_filter":"api","query":"token"}}"#;
        assert_eq!(
            client_jsonl_request_with_default_cwd(
                repo_filter,
                Some(Path::new("/tmp/current-repo"))
            )
            .unwrap(),
            repo_filter
        );

        let status = r#"{"id":"status","tool":"daemon_status","arguments":{}}"#;
        assert_eq!(
            client_jsonl_request_with_default_cwd(status, Some(Path::new("/tmp/current-repo")))
                .unwrap(),
            status
        );
    }
}
