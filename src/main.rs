use anyhow::{Result, bail};
use clap::{Parser, Subcommand};
use orient::discover::{DiscoverOptions, discover_repos};
use orient::fast_index::FastIndex;
use orient::repo_index::{
    RepoIndexer, SearchFilters, SnippetMode, attach_result_context, read_file_range,
    search_repo_fast_filtered,
};
use orient::server::{ToolRuntime, serve_jsonl, serve_tcp, tool_manifest};
use orient::shards::{
    build_shards, ensure_shards, find_shard_symbol, read_shard_range, refresh_shards,
    related_shard_files, related_shard_symbols, search_shards, shard_repo_maps,
};
use serde::{Deserialize, Serialize};
use std::fs;
use std::io::{self, BufRead, BufReader, Write};
use std::net::{TcpListener, TcpStream};
use std::path::PathBuf;
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
        output_dir: PathBuf,
    },
    RefreshShards {
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
        output_dir: PathBuf,
    },
    SearchShards {
        #[arg(long)]
        index_dir: PathBuf,
        query: String,
        #[arg(long, default_value_t = 10)]
        limit: usize,
        #[arg(long)]
        path: Option<String>,
        #[arg(long)]
        language: Option<String>,
        #[arg(long)]
        extension: Option<String>,
        #[arg(long = "repo")]
        repo: Option<String>,
        #[arg(long)]
        require_all: bool,
        #[arg(long, default_value = "medium")]
        snippet: String,
        #[arg(long)]
        explain: bool,
        #[arg(long, default_value_t = 0)]
        context_lines: usize,
    },
    ReadShardRange {
        #[arg(long)]
        index_dir: PathBuf,
        path: String,
        #[arg(long, default_value_t = 1)]
        start: usize,
        #[arg(long, default_value_t = 80)]
        lines: usize,
    },
    ReadShardRanges {
        #[arg(long)]
        index_dir: PathBuf,
        #[arg(required = true)]
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
    ReadRange {
        #[arg(long, default_value = ".")]
        repo: PathBuf,
        path: String,
        #[arg(long, default_value_t = 1)]
        start: usize,
        #[arg(long, default_value_t = 80)]
        lines: usize,
    },
    ReadRanges {
        #[arg(long, default_value = ".")]
        repo: PathBuf,
        #[arg(required = true)]
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
        #[arg(long)]
        path: Option<String>,
        #[arg(long)]
        language: Option<String>,
        #[arg(long)]
        extension: Option<String>,
        #[arg(long)]
        require_all: bool,
        #[arg(long, default_value = "medium")]
        snippet: String,
        #[arg(long)]
        explain: bool,
        #[arg(long, default_value_t = 0)]
        context_lines: usize,
    },
    IndexedSearch {
        #[arg(long)]
        index: PathBuf,
        query: String,
        #[arg(long, default_value_t = 10)]
        limit: usize,
        #[arg(long)]
        path: Option<String>,
        #[arg(long)]
        language: Option<String>,
        #[arg(long)]
        extension: Option<String>,
        #[arg(long)]
        require_all: bool,
        #[arg(long, default_value = "medium")]
        snippet: String,
        #[arg(long)]
        explain: bool,
        #[arg(long, default_value_t = 0)]
        context_lines: usize,
    },
    ReadIndexRange {
        #[arg(long)]
        index: PathBuf,
        path: String,
        #[arg(long, default_value_t = 1)]
        start: usize,
        #[arg(long, default_value_t = 80)]
        lines: usize,
    },
    ReadIndexRanges {
        #[arg(long)]
        index: PathBuf,
        #[arg(required = true)]
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
        #[arg(long)]
        path: Option<String>,
        #[arg(long)]
        language: Option<String>,
        #[arg(long)]
        extension: Option<String>,
        #[arg(long)]
        require_all: bool,
        #[arg(long, default_value = "medium")]
        snippet: String,
        #[arg(long)]
        explain: bool,
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
    ServeJsonl,
    ServeTcp {
        #[arg(long, default_value = "127.0.0.1:8796")]
        addr: String,
        #[arg(long = "index")]
        indexes: Vec<PathBuf>,
        #[arg(long = "index-dir")]
        index_dirs: Vec<PathBuf>,
    },
    ClientJsonl {
        #[arg(long, default_value = "127.0.0.1:8796")]
        addr: String,
    },
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

fn main() -> Result<()> {
    let cli = Cli::parse();
    match cli.command {
        Commands::DiscoverRepos {
            root,
            max_depth,
            limit,
        } => {
            println!(
                "{}",
                serde_json::to_string(&discover_repos(
                    root,
                    &DiscoverOptions { max_depth, limit },
                )?)?
            );
        }
        Commands::Index { repo, output } => {
            let index = FastIndex::build(repo)?;
            index.save(&output)?;
            println!("{}", serde_json::to_string(&index.stats())?);
        }
        Commands::RefreshIndex { repo, index } => {
            let previous = if index.exists() {
                Some(FastIndex::load(&index)?)
            } else {
                None
            };
            let outcome = FastIndex::refresh(repo, previous.as_ref())?;
            outcome.index.save(&index)?;
            println!(
                "{}",
                serde_json::to_string(&outcome.index.refresh_stats(&outcome))?
            );
        }
        Commands::IndexShards {
            repos,
            discover_roots,
            max_depth,
            discover_limit,
            output_dir,
        } => {
            let repos =
                shard_repos_from_args_required(repos, discover_roots, max_depth, discover_limit)?;
            println!(
                "{}",
                serde_json::to_string(&build_shards(&repos, output_dir)?)?
            );
        }
        Commands::RefreshShards { index_dir } => {
            println!("{}", serde_json::to_string(&refresh_shards(index_dir)?)?);
        }
        Commands::EnsureShards {
            repos,
            discover_roots,
            max_depth,
            discover_limit,
            output_dir,
        } => {
            let repos = shard_repos_from_args(repos, discover_roots, max_depth, discover_limit)?;
            println!(
                "{}",
                serde_json::to_string(&ensure_shards(&repos, output_dir)?)?
            );
        }
        Commands::SearchShards {
            index_dir,
            query,
            limit,
            path,
            language,
            extension,
            repo,
            require_all,
            snippet,
            explain,
            context_lines,
        } => {
            let snippet = snippet_mode_arg(&snippet)?;
            let mut results = search_shards(
                &index_dir,
                &query,
                limit,
                &SearchFilters {
                    path,
                    language,
                    extension,
                    repo,
                    require_all,
                    snippet,
                    explain,
                    ..SearchFilters::default()
                },
            )?;
            attach_result_context(&mut results, context_lines, |path, start, lines| {
                read_shard_range(&index_dir, path, start, lines)
            })?;
            println!("{}", serde_json::to_string(&results)?);
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
            paths,
            start,
            lines,
        } => {
            let mut ranges = Vec::new();
            for path in paths {
                ranges.push(read_shard_range(&index_dir, &path, start, lines)?);
            }
            println!("{}", serde_json::to_string(&ranges)?);
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
            paths,
            start,
            lines,
        } => {
            let mut ranges = Vec::new();
            for path in paths {
                ranges.push(read_file_range(&repo, &path, start, lines)?);
            }
            println!("{}", serde_json::to_string(&ranges)?);
        }
        Commands::Search {
            repo,
            query,
            limit,
            path,
            language,
            extension,
            require_all,
            snippet,
            explain,
            context_lines,
        } => {
            let snippet = snippet_mode_arg(&snippet)?;
            let mut results = search_repo_fast_filtered(
                &repo,
                &query,
                limit,
                &SearchFilters {
                    path,
                    language,
                    extension,
                    require_all,
                    snippet,
                    explain,
                    ..SearchFilters::default()
                },
            )?;
            attach_result_context(&mut results, context_lines, |path, start, lines| {
                read_file_range(&repo, path, start, lines)
            })?;
            println!("{}", serde_json::to_string(&results)?);
        }
        Commands::IndexedSearch {
            index,
            query,
            limit,
            path,
            language,
            extension,
            require_all,
            snippet,
            explain,
            context_lines,
        } => {
            let snippet = snippet_mode_arg(&snippet)?;
            let index = FastIndex::load(index)?;
            let mut results = index.search_filtered(
                &query,
                limit,
                &SearchFilters {
                    path,
                    language,
                    extension,
                    require_all,
                    snippet,
                    explain,
                    ..SearchFilters::default()
                },
            )?;
            attach_result_context(&mut results, context_lines, |path, start, lines| {
                index.read_range(path, start, lines)
            })?;
            println!("{}", serde_json::to_string(&results)?);
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
            paths,
            start,
            lines,
        } => {
            let index = FastIndex::load(index)?;
            let mut ranges = Vec::new();
            for path in paths {
                ranges.push(index.read_range(&path, start, lines)?);
            }
            println!("{}", serde_json::to_string(&ranges)?);
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
            path,
            language,
            extension,
            require_all,
            snippet,
            explain,
            fail_p95_ms,
            baseline,
            write_baseline,
            max_p95_regression,
            queries,
        } => {
            let snippet = snippet_mode_arg(&snippet)?;
            let filters = SearchFilters {
                path,
                language,
                extension,
                require_all,
                snippet,
                explain,
                ..SearchFilters::default()
            };
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
            }
        }
        Commands::ToolManifest => {
            println!("{}", serde_json::to_string(&tool_manifest())?);
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
        } => {
            let listener = TcpListener::bind(&addr)?;
            let runtime = ToolRuntime::default();
            for index in indexes {
                runtime.warm_index(index)?;
            }
            for index_dir in index_dirs {
                runtime.warm_shards(index_dir)?;
            }
            println!(
                "{}",
                serde_json::to_string(&serde_json::json!({
                    "addr": listener.local_addr()?.to_string(),
                    "cached_indexes": runtime.cached_index_count()
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

fn shard_repos_from_args(
    mut repos: Vec<PathBuf>,
    discover_roots: Vec<PathBuf>,
    max_depth: usize,
    discover_limit: usize,
) -> Result<Vec<PathBuf>> {
    for root in discover_roots {
        let discovered = discover_repos(
            root,
            &DiscoverOptions {
                max_depth,
                limit: discover_limit,
            },
        )?;
        repos.extend(discovered.repos.into_iter().map(|repo| repo.path));
    }
    repos.sort();
    repos.dedup();
    Ok(repos)
}

fn shard_repos_from_args_required(
    repos: Vec<PathBuf>,
    discover_roots: Vec<PathBuf>,
    max_depth: usize,
    discover_limit: usize,
) -> Result<Vec<PathBuf>> {
    let repos = shard_repos_from_args(repos, discover_roots, max_depth, discover_limit)?;
    if repos.is_empty() {
        bail!("provide at least one --repo or --discover-root");
    }
    Ok(repos)
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

fn snippet_mode_arg(value: &str) -> Result<SnippetMode> {
    SnippetMode::parse(value)
        .ok_or_else(|| anyhow::anyhow!("snippet must be one of: short, medium, block, symbol"))
}
