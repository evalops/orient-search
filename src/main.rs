use anyhow::{Result, bail};
use clap::{Parser, Subcommand};
use orient::fast_index::FastIndex;
use orient::repo_index::{
    RepoIndexer, SearchFilters, SnippetMode, read_file_range, search_repo_fast_filtered,
};
use orient::server::serve_jsonl;
use serde::Serialize;
use std::io;
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
    ReadRange {
        #[arg(long, default_value = ".")]
        repo: PathBuf,
        path: String,
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
    },
    Symbol {
        #[arg(long, default_value = ".")]
        repo: PathBuf,
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
        #[arg(required = true)]
        queries: Vec<String>,
    },
    ServeJsonl,
}

#[derive(Debug, Serialize)]
struct BenchReport {
    mode: String,
    runs: usize,
    warmup: usize,
    limit: usize,
    queries: Vec<QueryBench>,
}

#[derive(Debug, Serialize)]
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
        } => {
            let snippet = snippet_mode_arg(&snippet)?;
            println!(
                "{}",
                serde_json::to_string(&search_repo_fast_filtered(
                    repo,
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
                )?)?
            );
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
        } => {
            let snippet = snippet_mode_arg(&snippet)?;
            let index = FastIndex::load(index)?;
            println!(
                "{}",
                serde_json::to_string(&index.search_filtered(
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
                )?)?
            );
        }
        Commands::Symbol { repo, name, limit } => {
            let index = RepoIndexer::new(repo).build()?;
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
        Commands::ServeJsonl => {
            let stdin = io::stdin();
            let stdout = io::stdout();
            serve_jsonl(stdin.lock(), stdout.lock())?;
        }
    }
    Ok(())
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

fn snippet_mode_arg(value: &str) -> Result<SnippetMode> {
    SnippetMode::parse(value)
        .ok_or_else(|| anyhow::anyhow!("snippet must be one of: short, medium, block, symbol"))
}
