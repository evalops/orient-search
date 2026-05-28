use anyhow::{Result, bail};
use clap::{Args, Parser, Subcommand};
use orient::discover::{
    DiscoverOptions, DiscoverySelectionSummary, discover_repos, discovery_selection_summary,
};
use orient::fast_index::{FastIndex, RefreshStats};
use orient::repo_index::{
    QueryPlan, RepoIndexer, SearchFilters, SearchResult, SnippetMode, attach_result_context,
    read_file_range, search_repo_fast_filtered,
};
use orient::server::{
    MAX_BATCH_QUERIES, MAX_BATCH_RANGES, ToolRuntime, mcp_tool_manifest, serve_jsonl, serve_tcp,
    tool_manifest,
};
use orient::shards::{
    ShardQueryPlan, build_shards, ensure_shards, find_shard_symbol, read_shard_range,
    refresh_shards, related_shard_files, related_shard_symbols, search_shards, shard_query_plans,
    shard_repo_maps, shard_status,
};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::fs;
use std::io::{self, BufRead, BufReader, Write};
use std::net::{TcpListener, TcpStream};
use std::path::{Path, PathBuf};
use std::str::FromStr;
use std::time::Instant;

#[derive(Debug, Parser)]
#[command(name = "orient")]
#[command(about = "Fast local code search for coding agents")]
struct Cli {
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
        output_dir: PathBuf,
    },
    RefreshShards {
        #[arg(long)]
        index_dir: PathBuf,
    },
    ShardStatus {
        #[arg(long)]
        index_dir: PathBuf,
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
        query: String,
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
        #[arg(required = true)]
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
        query: String,
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
        #[arg(required = true)]
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
        path: String,
        #[arg(long, default_value_t = 1)]
        start: usize,
        #[arg(long, default_value_t = 80)]
        lines: usize,
    },
    #[command(alias = "open-shard-ranges")]
    ReadShardRanges {
        #[arg(long)]
        index_dir: PathBuf,
        #[arg(long = "range", value_name = "PATH:START:LINES")]
        ranges: Vec<CliRangeSpec>,
        paths: Vec<String>,
        #[arg(long, default_value_t = 1)]
        start: usize,
        #[arg(long, default_value_t = 80)]
        lines: usize,
    },
    ShardSymbol {
        #[arg(long)]
        index_dir: PathBuf,
        name: String,
        #[arg(long, default_value_t = 10)]
        limit: usize,
        #[arg(long = "repo")]
        repo: Option<String>,
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
    },
    Brief {
        #[arg(long, default_value = ".")]
        repo: PathBuf,
    },
    RepoMap {
        #[arg(long, default_value = ".")]
        repo: PathBuf,
        #[arg(long, default_value_t = 50)]
        symbols: usize,
        #[arg(long, default_value_t = 50)]
        tests: usize,
    },
    IndexMap {
        #[arg(long)]
        index: PathBuf,
        #[arg(long, default_value_t = 50)]
        symbols: usize,
        #[arg(long, default_value_t = 50)]
        tests: usize,
    },
    IndexPlan {
        #[arg(long)]
        index: PathBuf,
        query: String,
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
        #[arg(required = true)]
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
        path: String,
        #[arg(long, default_value_t = 1)]
        start: usize,
        #[arg(long, default_value_t = 80)]
        lines: usize,
    },
    #[command(alias = "open-ranges")]
    ReadRanges {
        #[arg(long, default_value = ".")]
        repo: PathBuf,
        #[arg(long = "range", value_name = "PATH:START:LINES")]
        ranges: Vec<CliRangeSpec>,
        paths: Vec<String>,
        #[arg(long, default_value_t = 1)]
        start: usize,
        #[arg(long, default_value_t = 80)]
        lines: usize,
    },
    Search {
        #[arg(long, default_value = ".")]
        repo: PathBuf,
        query: String,
        #[arg(long, default_value_t = 10)]
        limit: usize,
        #[arg(long = "repo-filter")]
        repo_filter: Option<String>,
        #[command(flatten)]
        filters: CommonSearchArgs,
        #[arg(long, default_value_t = 0)]
        context_lines: usize,
    },
    SearchBatch {
        #[arg(long, default_value = ".")]
        repo: PathBuf,
        #[arg(required = true)]
        queries: Vec<String>,
        #[arg(long, default_value_t = 10)]
        limit: usize,
        #[arg(long = "repo-filter")]
        repo_filter: Option<String>,
        #[command(flatten)]
        filters: CommonSearchArgs,
        #[arg(long, default_value_t = 0)]
        context_lines: usize,
    },
    IndexedSearch {
        #[arg(long)]
        index: PathBuf,
        query: String,
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
        #[arg(required = true)]
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
        path: String,
        #[arg(long, default_value_t = 1)]
        start: usize,
        #[arg(long, default_value_t = 80)]
        lines: usize,
    },
    #[command(alias = "open-index-ranges")]
    ReadIndexRanges {
        #[arg(long)]
        index: PathBuf,
        #[arg(long = "range", value_name = "PATH:START:LINES")]
        ranges: Vec<CliRangeSpec>,
        paths: Vec<String>,
        #[arg(long, default_value_t = 1)]
        start: usize,
        #[arg(long, default_value_t = 80)]
        lines: usize,
    },
    Symbol {
        #[arg(long, default_value = ".")]
        repo: PathBuf,
        name: String,
        #[arg(long, default_value_t = 10)]
        limit: usize,
    },
    IndexSymbol {
        #[arg(long)]
        index: PathBuf,
        name: String,
        #[arg(long, default_value_t = 10)]
        limit: usize,
    },
    Related {
        #[arg(long, default_value = ".")]
        repo: PathBuf,
        path: String,
        #[arg(long, default_value_t = 10)]
        limit: usize,
    },
    RelatedIndex {
        #[arg(long)]
        index: PathBuf,
        path: String,
        #[arg(long, default_value_t = 10)]
        limit: usize,
    },
    RelatedShard {
        #[arg(long)]
        index_dir: PathBuf,
        path: String,
        #[arg(long, default_value_t = 10)]
        limit: usize,
    },
    RelatedSymbols {
        #[arg(long, default_value = ".")]
        repo: PathBuf,
        #[arg(long)]
        path: Option<String>,
        #[arg(long)]
        query: Option<String>,
        #[arg(long, default_value_t = 10)]
        limit: usize,
    },
    RelatedShardSymbols {
        #[arg(long)]
        index_dir: PathBuf,
        path: String,
        #[arg(long)]
        query: Option<String>,
        #[arg(long, default_value_t = 10)]
        limit: usize,
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
    },
    BenchSearch {
        #[arg(long, default_value = ".")]
        repo: PathBuf,
        #[arg(long)]
        index: Option<PathBuf>,
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
        write_baseline: Option<PathBuf>,
        #[arg(long, default_value_t = 0.25)]
        max_p95_regression: f64,
        #[arg(required = true)]
        queries: Vec<String>,
    },
    BenchShards {
        #[arg(long)]
        index_dir: PathBuf,
        #[arg(long)]
        cached: bool,
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
        #[arg(required = true)]
        queries: Vec<String>,
    },
    ToolManifest,
    McpManifest,
    ServeJsonl,
    ServeTcp {
        #[arg(long, default_value = "127.0.0.1:8796")]
        addr: String,
        #[arg(long = "index")]
        indexes: Vec<PathBuf>,
        #[arg(long = "index-dir")]
        index_dirs: Vec<PathBuf>,
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
        #[arg(long, default_value = "127.0.0.1:8796")]
        addr: String,
    },
}

#[derive(Debug, Clone, Args)]
struct CommonSearchArgs {
    #[arg(long, alias = "dir")]
    path: Option<String>,
    #[arg(long)]
    language: Option<String>,
    #[arg(long)]
    extension: Option<String>,
    #[arg(long)]
    file: Option<String>,
    #[arg(long)]
    symbol: Option<String>,
    #[arg(long)]
    test: Option<bool>,
    #[arg(long)]
    require_all: bool,
    #[arg(long, default_value = "medium")]
    snippet: String,
    #[arg(long)]
    explain: bool,
    #[arg(long = "exclude-file")]
    exclude_file: Vec<String>,
    #[arg(long = "exclude-path")]
    exclude_path: Vec<String>,
    #[arg(long = "exclude-language")]
    exclude_language: Vec<String>,
    #[arg(long = "exclude-extension")]
    exclude_extension: Vec<String>,
    #[arg(long = "exclude-symbol")]
    exclude_symbol: Vec<String>,
    #[arg(long = "exclude-repo")]
    exclude_repo: Vec<String>,
}

fn search_filters_from_args(
    args: &CommonSearchArgs,
    repo: Option<String>,
) -> Result<SearchFilters> {
    Ok(SearchFilters {
        file: args.file.clone(),
        path: args.path.clone(),
        language: args.language.as_ref().map(|value| normalize_filter(value)),
        extension: args
            .extension
            .as_ref()
            .map(|value| normalize_extension(value)),
        symbol: args.symbol.clone(),
        repo,
        test: args.test,
        require_all: args.require_all,
        snippet: snippet_mode_arg(&args.snippet)?,
        explain: args.explain,
        exclude_file: args.exclude_file.clone(),
        exclude_path: args.exclude_path.clone(),
        exclude_language: args
            .exclude_language
            .iter()
            .map(|value| normalize_filter(value))
            .collect(),
        exclude_extension: args
            .exclude_extension
            .iter()
            .map(|value| normalize_extension(value))
            .collect(),
        exclude_symbol: args.exclude_symbol.clone(),
        exclude_repo: args.exclude_repo.clone(),
    })
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
    max_ms: f64,
    samples_ms: Vec<f64>,
}

#[derive(Debug, Clone, Serialize)]
struct SearchBatchResult {
    query: String,
    results: Vec<SearchResult>,
}

#[derive(Debug, Clone, Serialize)]
struct IndexedQueryPlanBatchResult {
    query: String,
    plan: QueryPlan,
}

#[derive(Debug, Clone, Serialize)]
struct ShardQueryPlanBatchResult {
    query: String,
    plans: Vec<ShardQueryPlan>,
}

fn main() -> Result<()> {
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
            let index = FastIndex::load(index)?;
            println!("{}", serde_json::to_string(&index.freshness()?)?);
        }
        Commands::IndexShards {
            repos,
            discover_roots,
            max_depth,
            discover_limit,
            family_limit,
            nested_manifests,
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
            let stats = build_shards(&selection.repos, output_dir)?;
            println!(
                "{}",
                serde_json::to_string(&shard_bootstrap_output(stats, selection.discovery)?)?
            );
        }
        Commands::RefreshShards { index_dir } => {
            println!("{}", serde_json::to_string(&refresh_shards(index_dir)?)?);
        }
        Commands::ShardStatus { index_dir } => {
            println!("{}", serde_json::to_string(&shard_status(index_dir)?)?);
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
            let mut results = search_shards(&index_dir, &query, limit, &filters)?;
            attach_result_context(&mut results, context_lines, |path, start, lines| {
                read_shard_range(&index_dir, path, start, lines)
            })?;
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
                batch.push(SearchBatchResult { query, results });
            }
            println!("{}", serde_json::to_string(&batch)?);
        }
        Commands::ShardPlan {
            index_dir,
            query,
            repo,
            filters,
            refresh_if_stale,
        } => {
            if refresh_if_stale && shard_status(&index_dir)?.stale {
                refresh_shards(&index_dir)?;
            }
            println!(
                "{}",
                serde_json::to_string(&shard_query_plans(
                    &index_dir,
                    &query,
                    &search_filters_from_args(&filters, repo)?,
                )?)?
            );
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
                let plans = shard_query_plans(&index_dir, &query, &filters)?;
                batch.push(ShardQueryPlanBatchResult { query, plans });
            }
            println!("{}", serde_json::to_string(&batch)?);
        }
        Commands::ReadShardRange {
            index_dir,
            path,
            start,
            lines,
        } => {
            println!(
                "{}",
                serde_json::to_string(&read_shard_range(index_dir, &path, start, lines)?)?
            );
        }
        Commands::ReadShardRanges {
            index_dir,
            ranges,
            paths,
            start,
            lines,
        } => {
            let mut results = Vec::new();
            for range in cli_ranges(paths, ranges, start, lines)? {
                results.push(read_shard_range(
                    &index_dir,
                    &range.path,
                    range.start,
                    range.lines,
                )?);
            }
            println!("{}", serde_json::to_string(&results)?);
        }
        Commands::ShardSymbol {
            index_dir,
            name,
            limit,
            repo,
        } => {
            println!(
                "{}",
                serde_json::to_string(&find_shard_symbol(
                    index_dir,
                    &name,
                    limit,
                    &SearchFilters {
                        repo,
                        ..SearchFilters::default()
                    },
                )?)?
            );
        }
        Commands::ShardMap {
            index_dir,
            symbols,
            tests,
            repo,
        } => {
            println!(
                "{}",
                serde_json::to_string(&shard_repo_maps(
                    index_dir,
                    symbols,
                    tests,
                    &SearchFilters {
                        repo,
                        ..SearchFilters::default()
                    },
                )?)?
            );
        }
        Commands::Brief { repo } => {
            let index = RepoIndexer::new(repo).build()?;
            println!("{}", serde_json::to_string(&index.repo_brief())?);
        }
        Commands::RepoMap {
            repo,
            symbols,
            tests,
        } => {
            let index = RepoIndexer::new(repo).build()?;
            println!(
                "{}",
                serde_json::to_string(&index.repo_map(symbols, tests))?
            );
        }
        Commands::IndexMap {
            index,
            symbols,
            tests,
        } => {
            let index = FastIndex::load(index)?;
            println!(
                "{}",
                serde_json::to_string(&index.repo_map(symbols, tests))?
            );
        }
        Commands::IndexPlan {
            index,
            query,
            repo_filter,
            filters,
            refresh_if_stale,
        } => {
            let index = load_index_for_search(index, refresh_if_stale)?;
            println!(
                "{}",
                serde_json::to_string(
                    &index.query_plan(&query, &search_filters_from_args(&filters, repo_filter)?,)?
                )?
            );
        }
        Commands::IndexPlanBatch {
            index,
            queries,
            repo_filter,
            filters,
            refresh_if_stale,
        } => {
            let queries = cli_batch_queries(queries)?;
            let index = load_index_for_search(index, refresh_if_stale)?;
            let filters = search_filters_from_args(&filters, repo_filter)?;
            let mut batch = Vec::new();
            for query in queries {
                let plan = index.query_plan(&query, &filters)?;
                batch.push(IndexedQueryPlanBatchResult { query, plan });
            }
            println!("{}", serde_json::to_string(&batch)?);
        }
        Commands::ReadRange {
            repo,
            path,
            start,
            lines,
        } => {
            println!(
                "{}",
                serde_json::to_string(&read_file_range(repo, &path, start, lines)?)?
            );
        }
        Commands::ReadRanges {
            repo,
            ranges,
            paths,
            start,
            lines,
        } => {
            let mut results = Vec::new();
            for range in cli_ranges(paths, ranges, start, lines)? {
                results.push(read_file_range(
                    &repo,
                    &range.path,
                    range.start,
                    range.lines,
                )?);
            }
            println!("{}", serde_json::to_string(&results)?);
        }
        Commands::Search {
            repo,
            query,
            limit,
            repo_filter,
            filters,
            context_lines,
        } => {
            let filters = search_filters_from_args(&filters, repo_filter)?;
            let mut results = search_repo_fast_filtered(&repo, &query, limit, &filters)?;
            attach_result_context(&mut results, context_lines, |path, start, lines| {
                read_file_range(&repo, path, start, lines)
            })?;
            println!("{}", serde_json::to_string(&results)?);
        }
        Commands::SearchBatch {
            repo,
            queries,
            limit,
            repo_filter,
            filters,
            context_lines,
        } => {
            let queries = cli_batch_queries(queries)?;
            let filters = search_filters_from_args(&filters, repo_filter)?;
            let mut batch = Vec::new();
            for query in queries {
                let mut results = search_repo_fast_filtered(&repo, &query, limit, &filters)?;
                attach_result_context(&mut results, context_lines, |path, start, lines| {
                    read_file_range(&repo, path, start, lines)
                })?;
                batch.push(SearchBatchResult { query, results });
            }
            println!("{}", serde_json::to_string(&batch)?);
        }
        Commands::IndexedSearch {
            index,
            query,
            limit,
            repo_filter,
            filters,
            context_lines,
            refresh_if_stale,
        } => {
            let index = load_index_for_search(index, refresh_if_stale)?;
            let filters = search_filters_from_args(&filters, repo_filter)?;
            let mut results = index.search_filtered(&query, limit, &filters)?;
            attach_result_context(&mut results, context_lines, |path, start, lines| {
                index.read_range(path, start, lines)
            })?;
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
            let index = load_index_for_search(index, refresh_if_stale)?;
            let filters = search_filters_from_args(&filters, repo_filter)?;
            let mut batch = Vec::new();
            for query in queries {
                let mut results = index.search_filtered(&query, limit, &filters)?;
                attach_result_context(&mut results, context_lines, |path, start, lines| {
                    index.read_range(path, start, lines)
                })?;
                batch.push(SearchBatchResult { query, results });
            }
            println!("{}", serde_json::to_string(&batch)?);
        }
        Commands::ReadIndexRange {
            index,
            path,
            start,
            lines,
        } => {
            let index = FastIndex::load(index)?;
            println!(
                "{}",
                serde_json::to_string(&index.read_range(&path, start, lines)?)?
            );
        }
        Commands::ReadIndexRanges {
            index,
            ranges,
            paths,
            start,
            lines,
        } => {
            let index = FastIndex::load(index)?;
            let mut results = Vec::new();
            for range in cli_ranges(paths, ranges, start, lines)? {
                results.push(index.read_range(&range.path, range.start, range.lines)?);
            }
            println!("{}", serde_json::to_string(&results)?);
        }
        Commands::Symbol { repo, name, limit } => {
            let index = RepoIndexer::new(repo).build()?;
            println!(
                "{}",
                serde_json::to_string(&index.find_symbol(&name, limit))?
            );
        }
        Commands::IndexSymbol { index, name, limit } => {
            let index = FastIndex::load(index)?;
            println!(
                "{}",
                serde_json::to_string(&index.find_symbol(&name, limit))?
            );
        }
        Commands::Related { repo, path, limit } => {
            let index = RepoIndexer::new(repo).build()?;
            println!(
                "{}",
                serde_json::to_string(&index.related_files(&path, limit))?
            );
        }
        Commands::RelatedIndex { index, path, limit } => {
            let index = FastIndex::load(index)?;
            println!(
                "{}",
                serde_json::to_string(&index.related_files(&path, limit))?
            );
        }
        Commands::RelatedShard {
            index_dir,
            path,
            limit,
        } => {
            println!(
                "{}",
                serde_json::to_string(&related_shard_files(index_dir, &path, limit)?)?
            );
        }
        Commands::RelatedSymbols {
            repo,
            path,
            query,
            limit,
        } => {
            let index = RepoIndexer::new(repo).build()?;
            println!(
                "{}",
                serde_json::to_string(&index.related_symbols(
                    path.as_deref(),
                    query.as_deref(),
                    limit,
                ))?
            );
        }
        Commands::RelatedIndexSymbols {
            index,
            path,
            query,
            limit,
        } => {
            let index = FastIndex::load(index)?;
            println!(
                "{}",
                serde_json::to_string(&index.related_symbols(
                    path.as_deref(),
                    query.as_deref(),
                    limit,
                ))?
            );
        }
        Commands::RelatedShardSymbols {
            index_dir,
            path,
            query,
            limit,
        } => {
            println!(
                "{}",
                serde_json::to_string(&related_shard_symbols(
                    index_dir,
                    &path,
                    query.as_deref(),
                    limit,
                )?)?
            );
        }
        Commands::BenchSearch {
            repo,
            index,
            runs,
            warmup,
            limit,
            repo_filter,
            filters,
            fail_p95_ms,
            baseline,
            write_baseline,
            max_p95_regression,
            queries,
        } => {
            let filters = search_filters_from_args(&filters, repo_filter)?;
            let report = bench_search(BenchConfig {
                repo,
                index,
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
                compare_bench_baseline(&path, &report, max_p95_regression)?;
            }
            if let Some(threshold) = fail_p95_ms {
                fail_slow_bench_queries(&report, threshold)?;
            }
        }
        Commands::BenchShards {
            index_dir,
            cached,
            runs,
            warmup,
            limit,
            repo,
            filters,
            fail_p95_ms,
            baseline,
            write_baseline,
            max_p95_regression,
            queries,
        } => {
            let filters = search_filters_from_args(&filters, repo)?;
            let report = bench_shards(ShardBenchConfig {
                index_dir,
                cached,
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
                compare_bench_baseline(&path, &report, max_p95_regression)?;
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
        Commands::ServeJsonl => {
            let stdin = io::stdin();
            let stdout = io::stdout();
            serve_jsonl(stdin.lock(), stdout.lock())?;
        }
        Commands::ServeTcp {
            addr,
            indexes,
            index_dirs,
            ensure_shard_dirs,
            repos,
            discover_roots,
            max_depth,
            discover_limit,
            family_limit,
            nested_manifests,
        } => {
            let listener = TcpListener::bind(&addr)?;
            let runtime = ToolRuntime::default();
            for index in indexes {
                runtime.warm_index(index)?;
            }
            for index_dir in index_dirs {
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
                    runtime.warm_shards(index_dir)?;
                    ensured_shards
                        .push(shard_bootstrap_output(stats, selection.discovery.clone())?);
                }
            }
            println!(
                "{}",
                serde_json::to_string(&serde_json::json!({
                    "addr": listener.local_addr()?.to_string(),
                    "cached_indexes": runtime.cached_index_count(),
                    "ensured_shards": ensured_shards,
                    "daemon_status": runtime.daemon_status()
                }))?
            );
            io::stdout().flush()?;
            serve_tcp(listener, runtime)?;
        }
        Commands::ClientJsonl { addr } => {
            client_jsonl(&addr)?;
        }
    }
    Ok(())
}

fn client_jsonl(addr: &str) -> Result<()> {
    let mut writer = TcpStream::connect(addr)?;
    let mut reader = BufReader::new(writer.try_clone()?);
    let stdin = io::stdin();
    let mut stdout = io::stdout();
    let mut response = String::new();

    for line in stdin.lock().lines() {
        let line = line?;
        if line.trim().is_empty() {
            continue;
        }
        writeln!(writer, "{line}")?;
        writer.flush()?;
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
        Some(FastIndex::load(&index)?)
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
}

impl FromStr for CliRangeSpec {
    type Err = String;

    fn from_str(value: &str) -> std::result::Result<Self, Self::Err> {
        let mut parts = value.rsplitn(3, ':');
        let lines = parts
            .next()
            .ok_or_else(|| "range must be PATH:START:LINES".to_string())?
            .parse::<usize>()
            .map_err(|_| "range lines must be a positive integer".to_string())?;
        let start = parts
            .next()
            .ok_or_else(|| "range must be PATH:START:LINES".to_string())?
            .parse::<usize>()
            .map_err(|_| "range start must be a positive integer".to_string())?;
        let path = parts
            .next()
            .filter(|path| !path.is_empty())
            .ok_or_else(|| "range must be PATH:START:LINES".to_string())?
            .to_string();
        if start == 0 || lines == 0 {
            return Err("range start and lines must be positive integers".to_string());
        }
        Ok(Self { path, start, lines })
    }
}

fn cli_ranges(
    paths: Vec<String>,
    mut ranges: Vec<CliRangeSpec>,
    start: usize,
    lines: usize,
) -> Result<Vec<CliRangeSpec>> {
    ranges.extend(
        paths
            .into_iter()
            .map(|path| CliRangeSpec { path, start, lines }),
    );
    if ranges.is_empty() {
        bail!("provide at least one path or --range PATH:START:LINES");
    }
    if ranges.len() > MAX_BATCH_RANGES {
        bail!(
            "ranges has {} items, max {}",
            ranges.len(),
            MAX_BATCH_RANGES
        );
    }
    Ok(ranges)
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

struct BenchConfig {
    repo: PathBuf,
    index: Option<PathBuf>,
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
    let indexed = config.index.as_ref().map(FastIndex::load).transpose()?;
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
    QueryBench {
        query: query.to_string(),
        result_count,
        min_ms: round_ms(min_ms),
        p50_ms: round_ms(p50_ms),
        p95_ms: round_ms(p95_ms),
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
) -> Result<()> {
    let baseline = serde_json::from_slice::<BenchReport>(&fs::read(path)?)?;
    if baseline.mode != current.mode {
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
