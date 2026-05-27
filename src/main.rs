use anyhow::Result;
use clap::{Parser, Subcommand};
use orient::fast_index::FastIndex;
use orient::repo_index::{RepoIndexer, SearchFilters, search_repo_fast_filtered};
use orient::server::serve_jsonl;
use orient::session_metrics::{ScanOptions, scan_jsonl_roots};
use std::io;
use std::path::PathBuf;

#[derive(Debug, Parser)]
#[command(name = "orient")]
#[command(about = "Local repo and session orientation layer for coding agents")]
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
    Brief {
        #[arg(long, default_value = ".")]
        repo: PathBuf,
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
    Metrics {
        #[arg(long = "root", required = true)]
        roots: Vec<PathBuf>,
        #[arg(long)]
        max_files: Option<usize>,
        #[arg(long)]
        max_file_mb: Option<u64>,
    },
    ServeJsonl,
}

fn main() -> Result<()> {
    let cli = Cli::parse();
    match cli.command {
        Commands::Index { repo, output } => {
            let index = FastIndex::build(repo)?;
            index.save(&output)?;
            println!("{}", serde_json::to_string(&index.stats())?);
        }
        Commands::Brief { repo } => {
            let index = RepoIndexer::new(repo).build()?;
            println!("{}", serde_json::to_string(&index.repo_brief())?);
        }
        Commands::Search {
            repo,
            query,
            limit,
            path,
            language,
        } => {
            println!(
                "{}",
                serde_json::to_string(&search_repo_fast_filtered(
                    repo,
                    &query,
                    limit,
                    &SearchFilters { path, language },
                )?)?
            );
        }
        Commands::IndexedSearch {
            index,
            query,
            limit,
            path,
            language,
        } => {
            let index = FastIndex::load(index)?;
            println!(
                "{}",
                serde_json::to_string(&index.search_filtered(
                    &query,
                    limit,
                    &SearchFilters { path, language },
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
        Commands::Metrics {
            roots,
            max_files,
            max_file_mb,
        } => {
            let metrics = scan_jsonl_roots(ScanOptions {
                roots,
                max_files,
                max_file_bytes: max_file_mb.map(|mb| mb * 1024 * 1024),
            })?;
            println!("{}", serde_json::to_string(&metrics)?);
        }
        Commands::ServeJsonl => {
            let stdin = io::stdin();
            let stdout = io::stdout();
            serve_jsonl(stdin.lock(), stdout.lock())?;
        }
    }
    Ok(())
}
