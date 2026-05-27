use anyhow::Result;
use clap::{Parser, Subcommand};
use orient::fast_index::FastIndex;
use orient::repo_index::{RepoIndexer, SearchFilters, search_repo_fast_filtered};
use orient::server::serve_jsonl;
use std::io;
use std::path::PathBuf;

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
        Commands::Search {
            repo,
            query,
            limit,
            path,
            language,
            extension,
            require_all,
        } => {
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
        } => {
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
        Commands::ServeJsonl => {
            let stdin = io::stdin();
            let stdout = io::stdout();
            serve_jsonl(stdin.lock(), stdout.lock())?;
        }
    }
    Ok(())
}
