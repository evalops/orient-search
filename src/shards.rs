//! Multi-repo shard manifests for local indexed search.

use crate::discover::{RepoGitMetadata, git_metadata_for_repo};
use crate::fast_index::{FastIndex, IndexFreshness, IndexStats};
use crate::query::{merge_filters, parse_query, query_text};
use crate::repo_index::{
    CommandHint, FileRange, QueryPlan, RelatedFile, RelatedSymbol, RepoMap, SearchFilters,
    SearchResult, Symbol, finalize_results, is_manifest_file, language_for, normalize_token,
};
use ahash::{AHashMap as HashMap, AHashSet as HashSet};
use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use std::fs;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::process;
use std::thread;
use std::time::{SystemTime, UNIX_EPOCH};

const SHARD_MANIFEST_VERSION: u32 = 1;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ShardManifest {
    pub version: u32,
    pub shards: Vec<ShardEntry>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ShardEntry {
    pub name: String,
    pub root: PathBuf,
    pub index: String,
    #[serde(default)]
    pub aliases: Vec<ShardAlias>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub git: Option<RepoGitMetadata>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ShardAlias {
    pub name: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub path_prefix: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ShardBuildStats {
    pub version: u32,
    pub output_dir: PathBuf,
    pub shards: usize,
    pub files: usize,
    pub source_bytes: u64,
    pub terms: usize,
    pub path_terms: usize,
    pub trigrams: usize,
    pub posting_entries: usize,
    pub compressed_posting_bytes: usize,
    pub symbols: usize,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ShardRefreshStats {
    pub version: u32,
    pub output_dir: PathBuf,
    pub shards: usize,
    pub files: usize,
    pub source_bytes: u64,
    pub terms: usize,
    pub path_terms: usize,
    pub trigrams: usize,
    pub posting_entries: usize,
    pub compressed_posting_bytes: usize,
    pub symbols: usize,
    pub removed_shards: usize,
    pub reused_files: usize,
    pub renamed_files: usize,
    pub refreshed_files: usize,
    pub deleted_files: usize,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ShardEnsureStats {
    pub version: u32,
    pub output_dir: PathBuf,
    pub action: String,
    pub shards: usize,
    pub added_shards: usize,
    pub removed_shards: usize,
    pub files: usize,
    pub source_bytes: u64,
    pub terms: usize,
    pub path_terms: usize,
    pub trigrams: usize,
    pub posting_entries: usize,
    pub compressed_posting_bytes: usize,
    pub symbols: usize,
    pub reused_files: usize,
    pub renamed_files: usize,
    pub refreshed_files: usize,
    pub deleted_files: usize,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ShardRepoMap {
    pub name: String,
    pub root: PathBuf,
    pub aliases: Vec<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub git: Option<RepoGitMetadata>,
    pub map: RepoMap,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ShardQueryPlan {
    pub name: String,
    pub root: PathBuf,
    pub aliases: Vec<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub git: Option<RepoGitMetadata>,
    pub plan: QueryPlan,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ShardFreshness {
    pub version: u32,
    pub index_dir: PathBuf,
    pub shard_count: usize,
    pub stale: bool,
    pub stale_shards: usize,
    pub source_bytes: u64,
    pub terms: usize,
    pub path_terms: usize,
    pub trigrams: usize,
    pub posting_entries: usize,
    pub compressed_posting_bytes: usize,
    pub symbols: usize,
    pub changed_files: usize,
    pub deleted_files: usize,
    pub added_files: usize,
    pub shards: Vec<ShardIndexFreshness>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ShardIndexFreshness {
    pub name: String,
    pub root: PathBuf,
    pub aliases: Vec<String>,
    pub index: String,
    pub status: IndexFreshness,
}

pub fn build_shards(repos: &[PathBuf], output_dir: impl AsRef<Path>) -> Result<ShardBuildStats> {
    let output_dir = output_dir.as_ref();
    fs::create_dir_all(output_dir)?;
    let mut manifest = ShardManifest {
        version: SHARD_MANIFEST_VERSION,
        shards: Vec::new(),
    };
    let mut total = ShardBuildStats {
        version: SHARD_MANIFEST_VERSION,
        output_dir: output_dir.to_path_buf(),
        shards: 0,
        files: 0,
        source_bytes: 0,
        terms: 0,
        path_terms: 0,
        trigrams: 0,
        posting_entries: 0,
        compressed_posting_bytes: 0,
        symbols: 0,
    };

    let mut names = HashSet::new();
    for repo in repos {
        let root = repo.canonicalize()?;
        let base_name = root
            .file_name()
            .map(|value| value.to_string_lossy().to_string())
            .unwrap_or_else(|| "repo".to_string());
        let hash = stable_hash(&root);
        let name = unique_shard_name(&base_name, &hash, &mut names);
        let index_name = format!("{}-{}.orient", sanitize_name(&name), stable_hash(&root));
        let index_path = output_dir.join(&index_name);
        let index = FastIndex::build(&root)?;
        index.save(&index_path)?;
        let stats = index.stats();
        add_stats(&mut total, &stats);
        manifest.shards.push(ShardEntry {
            aliases: shard_aliases(&root, &base_name)?,
            git: shard_git_metadata(&root),
            name,
            root,
            index: index_name,
        });
    }

    total.shards = manifest.shards.len();
    save_manifest(output_dir, &manifest)?;
    Ok(total)
}

pub fn ensure_shards(repos: &[PathBuf], output_dir: impl AsRef<Path>) -> Result<ShardEnsureStats> {
    let output_dir = output_dir.as_ref();
    if output_dir.join("manifest.json").exists() {
        let stats = refresh_shards(output_dir)?;
        let mut total = ShardEnsureStats {
            version: stats.version,
            output_dir: stats.output_dir,
            action: ensure_action(stats.removed_shards, 0),
            shards: stats.shards,
            added_shards: 0,
            removed_shards: stats.removed_shards,
            files: stats.files,
            source_bytes: stats.source_bytes,
            terms: stats.terms,
            path_terms: stats.path_terms,
            trigrams: stats.trigrams,
            posting_entries: stats.posting_entries,
            compressed_posting_bytes: stats.compressed_posting_bytes,
            symbols: stats.symbols,
            reused_files: stats.reused_files,
            renamed_files: stats.renamed_files,
            refreshed_files: stats.refreshed_files,
            deleted_files: stats.deleted_files,
        };
        let added = add_missing_shards(repos, output_dir, &mut total)?;
        total.action = ensure_action(total.removed_shards, added);
        return Ok(total);
    }

    anyhow::ensure!(
        !repos.is_empty(),
        "provide repos or discover roots when building a new shard directory"
    );
    let stats = build_shards(repos, output_dir)?;
    Ok(ShardEnsureStats {
        version: stats.version,
        output_dir: stats.output_dir,
        action: "build".to_string(),
        shards: stats.shards,
        added_shards: stats.shards,
        removed_shards: 0,
        files: stats.files,
        source_bytes: stats.source_bytes,
        terms: stats.terms,
        path_terms: stats.path_terms,
        trigrams: stats.trigrams,
        posting_entries: stats.posting_entries,
        compressed_posting_bytes: stats.compressed_posting_bytes,
        symbols: stats.symbols,
        reused_files: 0,
        renamed_files: 0,
        refreshed_files: stats.files,
        deleted_files: 0,
    })
}

pub fn refresh_shards(index_dir: impl AsRef<Path>) -> Result<ShardRefreshStats> {
    let index_dir = index_dir.as_ref();
    let mut manifest = load_manifest(index_dir)?;
    let mut total = ShardRefreshStats {
        version: SHARD_MANIFEST_VERSION,
        output_dir: index_dir.to_path_buf(),
        shards: manifest.shards.len(),
        files: 0,
        source_bytes: 0,
        terms: 0,
        path_terms: 0,
        trigrams: 0,
        posting_entries: 0,
        compressed_posting_bytes: 0,
        symbols: 0,
        removed_shards: 0,
        reused_files: 0,
        renamed_files: 0,
        refreshed_files: 0,
        deleted_files: 0,
    };

    let mut kept_shards = Vec::with_capacity(manifest.shards.len());
    for mut shard in manifest.shards {
        if !shard.root.exists() {
            let _ = fs::remove_file(index_dir.join(&shard.index));
            total.removed_shards += 1;
            continue;
        }
        let index_path = index_dir.join(&shard.index);
        let previous = if index_path.exists() {
            Some(
                FastIndex::load(&index_path)
                    .with_context(|| format!("load shard {}", shard.index))?,
            )
        } else {
            None
        };
        let outcome = FastIndex::refresh(&shard.root, previous.as_ref())
            .with_context(|| format!("refresh shard {}", shard.name))?;
        outcome.index.save(&index_path)?;
        let stats = outcome.index.stats();
        add_index_stats(&mut total, &stats);
        total.reused_files += outcome.reused_files;
        total.renamed_files += outcome.renamed_files;
        total.refreshed_files += outcome.refreshed_files;
        total.deleted_files += outcome.deleted_files;
        let base_name = shard
            .root
            .file_name()
            .map(|value| value.to_string_lossy().to_string())
            .unwrap_or_else(|| shard.name.clone());
        shard.aliases = shard_aliases(&shard.root, &base_name)?;
        shard.git = shard_git_metadata(&shard.root);
        kept_shards.push(shard);
    }

    manifest.shards = kept_shards;
    total.shards = manifest.shards.len();
    save_manifest(index_dir, &manifest)?;
    Ok(total)
}

pub fn shard_status(index_dir: impl AsRef<Path>) -> Result<ShardFreshness> {
    let index_dir = index_dir.as_ref();
    let manifest = load_manifest(index_dir)?;
    let shards = shard_status_jobs(index_dir, &manifest.shards)?;
    let mut stale_shards = 0usize;
    let mut changed_files = 0usize;
    let mut deleted_files = 0usize;
    let mut added_files = 0usize;
    let mut source_bytes = 0u64;
    let mut terms = 0usize;
    let mut path_terms = 0usize;
    let mut trigrams = 0usize;
    let mut posting_entries = 0usize;
    let mut compressed_posting_bytes = 0usize;
    let mut symbols = 0usize;

    for shard in &shards {
        let status = &shard.status;
        if status.stale {
            stale_shards += 1;
        }
        changed_files += status.changed_files;
        deleted_files += status.deleted_files;
        added_files += status.added_files;
        source_bytes += status.source_bytes;
        terms += status.terms;
        path_terms += status.path_terms;
        trigrams += status.trigrams;
        posting_entries += status.posting_entries;
        compressed_posting_bytes += status.compressed_posting_bytes;
        symbols += status.symbols;
    }

    Ok(ShardFreshness {
        version: SHARD_MANIFEST_VERSION,
        index_dir: index_dir.to_path_buf(),
        shard_count: manifest.shards.len(),
        stale: stale_shards > 0,
        stale_shards,
        source_bytes,
        terms,
        path_terms,
        trigrams,
        posting_entries,
        compressed_posting_bytes,
        symbols,
        changed_files,
        deleted_files,
        added_files,
        shards,
    })
}

fn shard_status_jobs(index_dir: &Path, shards: &[ShardEntry]) -> Result<Vec<ShardIndexFreshness>> {
    if shards.is_empty() {
        return Ok(Vec::new());
    }

    let workers = std::thread::available_parallelism()
        .map(|count| count.get())
        .unwrap_or(1)
        .min(shards.len());
    if workers <= 1 {
        return shard_status_job_batch(index_dir, 0, shards)
            .map(|items| ordered_status_items(items, shards.len()));
    }

    let chunk_size = shards.len().div_ceil(workers);
    let mut items = Vec::with_capacity(shards.len());
    thread::scope(|scope| {
        let handles = shards
            .chunks(chunk_size)
            .enumerate()
            .map(|(chunk_index, chunk)| {
                let offset = chunk_index * chunk_size;
                scope.spawn(move || shard_status_job_batch(index_dir, offset, chunk))
            })
            .collect::<Vec<_>>();

        for handle in handles {
            let batch = handle
                .join()
                .map_err(|_| anyhow::anyhow!("shard status worker panicked"))??;
            items.extend(batch);
        }
        Ok::<(), anyhow::Error>(())
    })?;
    Ok(ordered_status_items(items, shards.len()))
}

fn shard_status_job_batch(
    index_dir: &Path,
    offset: usize,
    shards: &[ShardEntry],
) -> Result<Vec<(usize, ShardIndexFreshness)>> {
    let mut statuses = Vec::with_capacity(shards.len());
    for (index, shard) in shards.iter().enumerate() {
        let loaded = FastIndex::load(index_dir.join(&shard.index))
            .with_context(|| format!("load shard {}", shard.index))?;
        let status = loaded
            .freshness()
            .with_context(|| format!("check shard freshness {}", shard.name))?;
        statuses.push((
            offset + index,
            ShardIndexFreshness {
                name: shard.name.clone(),
                root: shard.root.clone(),
                aliases: shard
                    .aliases
                    .iter()
                    .map(|alias| alias.name.clone())
                    .collect(),
                index: shard.index.clone(),
                status,
            },
        ));
    }
    Ok(statuses)
}

fn ordered_status_items(
    mut items: Vec<(usize, ShardIndexFreshness)>,
    expected_len: usize,
) -> Vec<ShardIndexFreshness> {
    items.sort_by_key(|(index, _)| *index);
    items
        .into_iter()
        .take(expected_len)
        .map(|(_, status)| status)
        .collect()
}

fn ensure_action(removed: usize, added: usize) -> String {
    match (removed > 0, added > 0) {
        (true, true) => "refresh+prune+add",
        (true, false) => "refresh+prune",
        (false, true) => "refresh+add",
        (false, false) => "refresh",
    }
    .to_string()
}

fn add_missing_shards(
    repos: &[PathBuf],
    output_dir: &Path,
    total: &mut ShardEnsureStats,
) -> Result<usize> {
    if repos.is_empty() {
        return Ok(0);
    }

    let mut manifest = load_manifest(output_dir)?;
    let mut existing_roots = manifest
        .shards
        .iter()
        .map(|shard| canonical_or_self(&shard.root))
        .collect::<HashSet<_>>();
    let mut names = manifest
        .shards
        .iter()
        .map(|shard| shard.name.clone())
        .collect::<HashSet<_>>();
    let mut added = 0usize;

    for repo in repos {
        let root = repo.canonicalize()?;
        if !existing_roots.insert(root.clone()) {
            continue;
        }
        let base_name = root
            .file_name()
            .map(|value| value.to_string_lossy().to_string())
            .unwrap_or_else(|| "repo".to_string());
        let hash = stable_hash(&root);
        let name = unique_shard_name(&base_name, &hash, &mut names);
        let index_name = format!("{}-{}.orient", sanitize_name(&name), stable_hash(&root));
        let index_path = output_dir.join(&index_name);
        let index = FastIndex::build(&root)?;
        index.save(&index_path)?;
        add_ensure_stats(total, &index.stats());
        manifest.shards.push(ShardEntry {
            aliases: shard_aliases(&root, &base_name)?,
            git: shard_git_metadata(&root),
            name,
            root,
            index: index_name,
        });
        added += 1;
    }

    if added > 0 {
        total.added_shards += added;
        total.shards = manifest.shards.len();
        save_manifest(output_dir, &manifest)?;
    }

    Ok(added)
}

pub fn search_shards(
    index_dir: impl AsRef<Path>,
    query: &str,
    limit: usize,
    filters: &SearchFilters,
) -> Result<Vec<SearchResult>> {
    let index_dir = index_dir.as_ref();
    let manifest = load_manifest(index_dir)?;
    let parsed = parse_query(query);
    let filters = merge_filters(filters.clone(), parsed.filters);
    let shard_query = query_text(&parsed.terms, &filters);
    let jobs = manifest
        .shards
        .into_iter()
        .filter_map(|shard| {
            let scopes = shard_search_scopes(&shard, &filters);
            (!scopes.is_empty()).then_some(ShardJob { shard, scopes })
        })
        .collect::<Vec<_>>();
    let results = search_shard_jobs(index_dir, &shard_query, limit, &filters, jobs)?;
    Ok(finalize_results(results, limit))
}

#[derive(Debug, Clone)]
struct ShardJob {
    shard: ShardEntry,
    scopes: Vec<ShardSearchScope>,
}

fn search_shard_jobs(
    index_dir: &Path,
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
        return search_shard_job_batch(index_dir, query, limit, filters, &jobs);
    }

    let chunk_size = jobs.len().div_ceil(workers);
    let mut results = Vec::new();
    thread::scope(|scope| {
        let handles = jobs
            .chunks(chunk_size)
            .map(|chunk| {
                scope.spawn(move || search_shard_job_batch(index_dir, query, limit, filters, chunk))
            })
            .collect::<Vec<_>>();

        for handle in handles {
            let batch = handle
                .join()
                .map_err(|_| anyhow::anyhow!("shard search worker panicked"))??;
            results.extend(batch);
        }
        Ok::<(), anyhow::Error>(())
    })?;
    Ok(results)
}

fn search_shard_job_batch(
    index_dir: &Path,
    query: &str,
    limit: usize,
    filters: &SearchFilters,
    jobs: &[ShardJob],
) -> Result<Vec<SearchResult>> {
    let mut results = Vec::new();
    for job in jobs {
        let index = FastIndex::load(index_dir.join(&job.shard.index))
            .with_context(|| format!("load shard {}", job.shard.index))?;
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

pub fn shard_query_plans(
    index_dir: impl AsRef<Path>,
    query: &str,
    filters: &SearchFilters,
) -> Result<Vec<ShardQueryPlan>> {
    let index_dir = index_dir.as_ref();
    let manifest = load_manifest(index_dir)?;
    let parsed = parse_query(query);
    let filters = merge_filters(filters.clone(), parsed.filters);
    let shard_query = query_text(&parsed.terms, &filters);
    let jobs = manifest
        .shards
        .into_iter()
        .filter_map(|shard| {
            let scopes = shard_search_scopes(&shard, &filters);
            (!scopes.is_empty()).then_some(ShardJob { shard, scopes })
        })
        .collect::<Vec<_>>();
    let mut plans = shard_query_plan_jobs(index_dir, &shard_query, &filters, jobs)?;
    plans.sort_by(|left, right| left.name.cmp(&right.name));
    Ok(plans)
}

fn shard_query_plan_jobs(
    index_dir: &Path,
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
        return shard_query_plan_job_batch(index_dir, query, filters, &jobs);
    }

    let chunk_size = jobs.len().div_ceil(workers);
    let mut plans = Vec::new();
    thread::scope(|scope| {
        let handles = jobs
            .chunks(chunk_size)
            .map(|chunk| {
                scope.spawn(move || shard_query_plan_job_batch(index_dir, query, filters, chunk))
            })
            .collect::<Vec<_>>();

        for handle in handles {
            let batch = handle
                .join()
                .map_err(|_| anyhow::anyhow!("shard query-plan worker panicked"))??;
            plans.extend(batch);
        }
        Ok::<(), anyhow::Error>(())
    })?;
    Ok(plans)
}

fn shard_query_plan_job_batch(
    index_dir: &Path,
    query: &str,
    filters: &SearchFilters,
    jobs: &[ShardJob],
) -> Result<Vec<ShardQueryPlan>> {
    let mut plans = Vec::new();
    for job in jobs {
        let index = FastIndex::load(index_dir.join(&job.shard.index))
            .with_context(|| format!("load shard {}", job.shard.index))?;
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

pub fn find_shard_symbol(
    index_dir: impl AsRef<Path>,
    name: &str,
    limit: usize,
    filters: &SearchFilters,
) -> Result<Vec<Symbol>> {
    let needle = normalize_token(name);
    if needle.is_empty() || limit == 0 {
        return Ok(Vec::new());
    }

    let index_dir = index_dir.as_ref();
    let manifest = load_manifest(index_dir)?;
    let mut symbols = Vec::new();
    for shard in manifest.shards {
        let scopes = shard_search_scopes(&shard, filters);
        if scopes.is_empty() {
            continue;
        }
        let index = FastIndex::load(index_dir.join(&shard.index))
            .with_context(|| format!("load shard {}", shard.index))?;
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

pub fn shard_repo_maps(
    index_dir: impl AsRef<Path>,
    symbol_limit: usize,
    test_limit: usize,
    filters: &SearchFilters,
) -> Result<Vec<ShardRepoMap>> {
    let index_dir = index_dir.as_ref();
    let manifest = load_manifest(index_dir)?;
    let mut maps = Vec::new();
    for shard in manifest.shards {
        let scopes = shard_search_scopes(&shard, filters);
        if scopes.is_empty() {
            continue;
        }
        let index = FastIndex::load(index_dir.join(&shard.index))
            .with_context(|| format!("load shard {}", shard.index))?;
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
                git: shard.git.clone(),
                name: scope.output_prefix.clone(),
                root: shard.root.clone(),
                map,
            });
        }
    }
    maps.sort_by(|a, b| a.name.cmp(&b.name));
    Ok(maps)
}

pub fn read_shard_range(
    index_dir: impl AsRef<Path>,
    shard_path: &str,
    start: usize,
    lines: usize,
) -> Result<FileRange> {
    let manifest = load_manifest(index_dir.as_ref())?;
    let (prefix, relative_path) = shard_path
        .split_once('/')
        .ok_or_else(|| anyhow::anyhow!("shard path must be '<repo>/<path>'"))?;
    let resolved = resolve_shard_read_path(&manifest, prefix, relative_path)
        .ok_or_else(|| anyhow::anyhow!("unknown shard or alias: {prefix}"))?;
    let index = FastIndex::load(index_dir.as_ref().join(&resolved.index))
        .with_context(|| format!("load shard {}", resolved.index))?;
    let mut range = index.read_range(&resolved.relative_path, start, lines)?;
    range.path = resolved.output_path(&range.path);
    Ok(range)
}

pub fn related_shard_files(
    index_dir: impl AsRef<Path>,
    shard_path: &str,
    limit: usize,
) -> Result<Vec<RelatedFile>> {
    let resolved = resolve_shard_path(index_dir.as_ref(), shard_path)?;
    let index = FastIndex::load(index_dir.as_ref().join(&resolved.index))
        .with_context(|| format!("load shard {}", resolved.index))?;
    let mut related = index.related_files(&resolved.relative_path, limit.saturating_mul(4).max(10));
    related.retain(|file| resolved.contains_actual_path(&file.path));
    for file in &mut related {
        file.path = resolved.output_path(&file.path);
    }
    related.truncate(limit);
    Ok(related)
}

pub fn related_shard_symbols(
    index_dir: impl AsRef<Path>,
    shard_path: &str,
    query: Option<&str>,
    limit: usize,
) -> Result<Vec<RelatedSymbol>> {
    let resolved = resolve_shard_path(index_dir.as_ref(), shard_path)?;
    let index = FastIndex::load(index_dir.as_ref().join(&resolved.index))
        .with_context(|| format!("load shard {}", resolved.index))?;
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

pub(crate) struct ResolvedShardRead {
    pub(crate) index: String,
    pub(crate) relative_path: String,
    pub(crate) output_prefix: String,
    pub(crate) path_prefix: Option<String>,
}

impl ResolvedShardRead {
    pub(crate) fn contains_actual_path(&self, path: &str) -> bool {
        self.path_prefix
            .as_deref()
            .map(|prefix| path == prefix.trim_end_matches('/') || path.starts_with(prefix))
            .unwrap_or(true)
    }

    pub(crate) fn output_path(&self, path: &str) -> String {
        let trimmed = self
            .path_prefix
            .as_deref()
            .and_then(|prefix| path.strip_prefix(prefix))
            .unwrap_or(path)
            .trim_start_matches('/');
        if trimmed.is_empty() {
            self.output_prefix.clone()
        } else {
            format!("{}/{}", self.output_prefix, trimmed)
        }
    }
}

pub(crate) fn resolve_shard_path(index_dir: &Path, shard_path: &str) -> Result<ResolvedShardRead> {
    let manifest = load_manifest(index_dir)?;
    let (prefix, relative_path) = shard_path
        .split_once('/')
        .ok_or_else(|| anyhow::anyhow!("shard path must be '<repo>/<path>'"))?;
    resolve_shard_read_path(&manifest, prefix, relative_path)
        .ok_or_else(|| anyhow::anyhow!("unknown shard or alias: {prefix}"))
}

pub(crate) fn resolve_shard_read_path(
    manifest: &ShardManifest,
    prefix: &str,
    relative_path: &str,
) -> Option<ResolvedShardRead> {
    if let Some(shard) = manifest.shards.iter().find(|shard| shard.name == prefix) {
        return Some(ResolvedShardRead {
            index: shard.index.clone(),
            relative_path: relative_path.to_string(),
            output_prefix: shard.name.clone(),
            path_prefix: None,
        });
    }

    for shard in &manifest.shards {
        let Some(alias) = shard.aliases.iter().find(|alias| alias.name == prefix) else {
            continue;
        };
        let relative_path = alias
            .path_prefix
            .as_ref()
            .map(|path_prefix| format!("{}{}", path_prefix, relative_path))
            .unwrap_or_else(|| relative_path.to_string());
        return Some(ResolvedShardRead {
            index: shard.index.clone(),
            relative_path,
            output_prefix: alias.name.clone(),
            path_prefix: alias.path_prefix.clone(),
        });
    }

    None
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

fn scoped_output_path(scope: &ShardSearchScope, path: &str) -> String {
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

fn prefix_search_result_paths(result: &mut SearchResult, scope: &ShardSearchScope) {
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

fn prefix_repo_map_paths(map: &mut RepoMap, scope: &ShardSearchScope) {
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

pub(crate) fn filter_repo_map_by_prefix(map: &mut RepoMap, path_prefix: &str) {
    let prefix = path_prefix.trim_end_matches('/');
    let matches_prefix = |path: &str| path == prefix || path.starts_with(&format!("{prefix}/"));

    map.brief.manifest_files.retain(|path| matches_prefix(path));
    map.brief
        .important_files
        .retain(|path| matches_prefix(path));
    map.brief
        .dependency_hints
        .retain(|hint| matches_prefix(&hint.source));
    map.brief
        .import_hints
        .retain(|hint| matches_prefix(&hint.source));
    map.entrypoints.retain(|path| matches_prefix(path));
    map.test_files.retain(|path| matches_prefix(path));
    map.top_symbols
        .retain(|symbol| matches_prefix(&symbol.path));
    map.related_files
        .retain(|related| matches_prefix(&related.source_path) && matches_prefix(&related.path));
    map.related_symbols.retain(|related| {
        matches_prefix(&related.source_path) && matches_prefix(&related.symbol.path)
    });

    let retained_paths = map
        .brief
        .manifest_files
        .iter()
        .chain(map.brief.important_files.iter())
        .chain(map.entrypoints.iter())
        .chain(map.test_files.iter())
        .chain(map.top_symbols.iter().map(|symbol| &symbol.path))
        .chain(map.related_files.iter().map(|related| &related.source_path))
        .chain(map.related_files.iter().map(|related| &related.path))
        .chain(
            map.related_symbols
                .iter()
                .map(|related| &related.source_path),
        )
        .chain(
            map.related_symbols
                .iter()
                .map(|related| &related.symbol.path),
        )
        .collect::<HashSet<_>>()
        .into_iter()
        .cloned()
        .collect::<Vec<_>>();

    map.brief.file_count = retained_paths.len();
    map.brief.language_counts = language_counts_for_paths(&retained_paths);
    map.brief.known_commands = known_commands_for_manifest_paths(&map.brief.manifest_files);
    map.brief.command_hints = command_hints_for_manifest_paths(&map.brief.manifest_files);
}

fn language_counts_for_paths(paths: &[String]) -> HashMap<String, usize> {
    let mut counts = HashMap::new();
    for path in paths {
        if let Some(language) = language_for(Path::new(path)) {
            *counts.entry(language).or_insert(0) += 1;
        }
    }
    counts
}

fn known_commands_for_manifest_paths(paths: &[String]) -> Vec<String> {
    let mut commands = command_hints_for_manifest_paths(paths)
        .into_iter()
        .map(|hint| hint.command)
        .collect::<Vec<_>>();
    commands.sort();
    commands.dedup();
    commands
}

fn command_hints_for_manifest_paths(paths: &[String]) -> Vec<CommandHint> {
    let has_manifest = |name: &str| {
        paths
            .iter()
            .any(|path| Path::new(path).file_name().and_then(|value| value.to_str()) == Some(name))
    };
    let manifest_path = |name: &str| {
        paths
            .iter()
            .find(|path| Path::new(path).file_name().and_then(|value| value.to_str()) == Some(name))
            .cloned()
    };
    let mut hints = Vec::new();
    if let Some(source) = manifest_path("Cargo.toml") {
        hints.push(command_hint("cargo test", "test", source));
    }
    if let Some(source) = manifest_path("pyproject.toml") {
        hints.push(command_hint("pytest", "test", source));
    }
    if let Some(source) = manifest_path("package.json") {
        let package_manager = if has_manifest("pnpm-lock.yaml") {
            "pnpm"
        } else if has_manifest("yarn.lock") {
            "yarn"
        } else if has_manifest("bun.lock") || has_manifest("bun.lockb") {
            "bun"
        } else {
            "npm"
        };
        hints.push(command_hint(
            format!("{package_manager} test"),
            "test",
            source,
        ));
    }
    if let Some(source) = manifest_path("go.mod") {
        hints.push(command_hint("go test ./...", "test", source));
    }
    if let Some(source) = manifest_path("Package.swift") {
        hints.push(command_hint("swift test", "test", source));
    }
    hints.sort_by(|left, right| {
        left.command
            .cmp(&right.command)
            .then_with(|| left.source.cmp(&right.source))
    });
    hints.dedup_by(|left, right| left.command == right.command && left.source == right.source);
    hints
}

fn command_hint(
    command: impl Into<String>,
    kind: impl Into<String>,
    source: impl Into<String>,
) -> CommandHint {
    CommandHint {
        command: command.into(),
        kind: kind.into(),
        source: source.into(),
    }
}

pub(crate) fn load_manifest(index_dir: &Path) -> Result<ShardManifest> {
    let bytes = fs::read(index_dir.join("manifest.json"))
        .with_context(|| format!("read shard manifest {}", index_dir.display()))?;
    let manifest = serde_json::from_slice::<ShardManifest>(&bytes)?;
    anyhow::ensure!(
        manifest.version == SHARD_MANIFEST_VERSION,
        "unsupported shard manifest version {}",
        manifest.version
    );
    validate_manifest(&manifest)?;
    Ok(manifest)
}

fn validate_manifest(manifest: &ShardManifest) -> Result<()> {
    let mut shard_names = HashSet::new();
    let mut shard_indexes = HashSet::new();
    for shard in &manifest.shards {
        anyhow::ensure!(!shard.name.trim().is_empty(), "shard name cannot be empty");
        anyhow::ensure!(
            !shard.root.as_os_str().is_empty(),
            "shard root cannot be empty for {}",
            shard.name
        );
        anyhow::ensure!(
            shard_names.insert(shard.name.clone()),
            "duplicate shard name {}",
            shard.name
        );
        anyhow::ensure!(
            valid_manifest_file_name(&shard.index),
            "invalid shard manifest index path {}",
            shard.index
        );
        anyhow::ensure!(
            shard_indexes.insert(shard.index.clone()),
            "duplicate shard index {}",
            shard.index
        );

        let mut alias_names = HashSet::new();
        for alias in &shard.aliases {
            anyhow::ensure!(
                !alias.name.trim().is_empty(),
                "alias name cannot be empty for shard {}",
                shard.name
            );
            anyhow::ensure!(
                alias_names.insert(alias.name.clone()),
                "duplicate alias {} in shard {}",
                alias.name,
                shard.name
            );
            if let Some(prefix) = &alias.path_prefix {
                anyhow::ensure!(
                    valid_manifest_relative_prefix(prefix),
                    "invalid alias path prefix {} in shard {}",
                    prefix,
                    shard.name
                );
            }
        }
    }
    Ok(())
}

fn valid_manifest_file_name(value: &str) -> bool {
    let path = Path::new(value);
    !value.trim().is_empty()
        && path.is_relative()
        && path
            .components()
            .all(|component| matches!(component, std::path::Component::Normal(_)))
}

fn valid_manifest_relative_prefix(value: &str) -> bool {
    let trimmed = value.trim_end_matches('/');
    if trimmed.is_empty() {
        return true;
    }
    let path = Path::new(trimmed);
    path.is_relative()
        && path.components().all(|component| {
            matches!(
                component,
                std::path::Component::Normal(_) | std::path::Component::CurDir
            )
        })
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) struct ShardSearchScope {
    pub(crate) path_prefix: Option<String>,
    pub(crate) output_prefix: String,
}

pub(crate) fn shard_search_scopes(
    shard: &ShardEntry,
    filters: &SearchFilters,
) -> Vec<ShardSearchScope> {
    if filters.exclude_repo.iter().any(|filter| {
        shard_identity_matches(shard, filter)
            || shard
                .aliases
                .iter()
                .any(|alias| alias_matches(alias, filter))
    }) {
        return Vec::new();
    }

    let Some(filter) = &filters.repo else {
        return vec![ShardSearchScope {
            path_prefix: None,
            output_prefix: shard.name.clone(),
        }];
    };

    let mut scopes = Vec::<ShardSearchScope>::new();
    for alias in &shard.aliases {
        if alias_matches(alias, filter) {
            scopes.push(ShardSearchScope {
                path_prefix: alias.path_prefix.clone(),
                output_prefix: alias.name.clone(),
            });
        }
    }
    if scopes.is_empty() && shard_identity_matches(shard, filter) {
        scopes.push(ShardSearchScope {
            path_prefix: None,
            output_prefix: shard.name.clone(),
        });
    }
    scopes.sort();
    scopes.dedup();
    scopes
}

pub(crate) fn filters_for_shard_scope(
    filters: &SearchFilters,
    path_prefix: Option<&str>,
) -> SearchFilters {
    let mut filters = filters.clone();
    filters.repo = None;
    filters.exclude_repo.clear();
    if let Some(prefix) = path_prefix {
        if filters.path.is_none() {
            filters.path = Some(prefix.trim_end_matches('/').to_string());
        }
    }
    filters
}

fn save_manifest(index_dir: &Path, manifest: &ShardManifest) -> Result<()> {
    let manifest_path = index_dir.join("manifest.json");
    let bytes = serde_json::to_vec_pretty(manifest)?;
    atomic_write(&manifest_path, &bytes)
        .with_context(|| format!("write shard manifest {}", index_dir.display()))
}

fn atomic_write(path: &Path, bytes: &[u8]) -> Result<()> {
    let tmp_path = temporary_manifest_path(path);
    let result = (|| -> Result<()> {
        let mut file = fs::File::create(&tmp_path)
            .with_context(|| format!("create temp manifest {}", tmp_path.display()))?;
        file.write_all(bytes)
            .with_context(|| format!("write temp manifest {}", tmp_path.display()))?;
        file.sync_all()
            .with_context(|| format!("sync temp manifest {}", tmp_path.display()))?;
        drop(file);
        fs::rename(&tmp_path, path)
            .with_context(|| format!("replace manifest {}", path.display()))?;
        if let Some(parent) = path.parent() {
            if let Ok(parent_dir) = fs::File::open(parent) {
                let _ = parent_dir.sync_all();
            }
        }
        Ok(())
    })();

    if result.is_err() {
        let _ = fs::remove_file(&tmp_path);
    }
    result
}

fn temporary_manifest_path(path: &Path) -> PathBuf {
    let parent = path.parent().unwrap_or_else(|| Path::new("."));
    let file_name = path
        .file_name()
        .map(|value| value.to_string_lossy())
        .unwrap_or_else(|| "manifest.json".into());
    parent.join(format!(
        ".{file_name}.tmp-{}-{}",
        process::id(),
        current_nanos()
    ))
}

fn current_nanos() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_nanos())
        .unwrap_or_default()
}

fn shard_aliases(root: &Path, base_name: &str) -> Result<Vec<ShardAlias>> {
    let mut aliases = Vec::new();
    let mut seen = HashSet::new();
    push_alias(&mut aliases, &mut seen, base_name, None);

    for entry in fs::read_dir(root)? {
        let entry = entry?;
        let file_type = entry.file_type()?;
        if !file_type.is_dir() {
            continue;
        }
        let path = entry.path();
        if !directory_has_manifest(&path)? {
            continue;
        }
        let name = entry.file_name().to_string_lossy().to_string();
        push_alias(&mut aliases, &mut seen, &name, Some(format!("{name}/")));
    }

    aliases.sort_by(|left, right| {
        left.name
            .cmp(&right.name)
            .then_with(|| left.path_prefix.cmp(&right.path_prefix))
    });
    Ok(aliases)
}

fn shard_git_metadata(root: &Path) -> Option<RepoGitMetadata> {
    let metadata = git_metadata_for_repo(root, false);
    (metadata.origin.is_some() || metadata.git_common_dir.is_some()).then_some(metadata)
}

fn directory_has_manifest(path: &Path) -> Result<bool> {
    for entry in fs::read_dir(path)? {
        let entry = entry?;
        if !entry.file_type()?.is_file() {
            continue;
        }
        let file_name = entry.file_name().to_string_lossy().to_string();
        if is_manifest_file(&file_name) {
            return Ok(true);
        }
    }
    Ok(false)
}

fn push_alias(
    aliases: &mut Vec<ShardAlias>,
    seen: &mut HashSet<(String, Option<String>)>,
    name: &str,
    path_prefix: Option<String>,
) {
    let name = name.trim();
    if name.is_empty() {
        return;
    }
    let key = (name.to_ascii_lowercase(), path_prefix.clone());
    if seen.insert(key) {
        aliases.push(ShardAlias {
            name: name.to_string(),
            path_prefix,
        });
    }
}

fn shard_identity_matches(shard: &ShardEntry, filter: &str) -> bool {
    let filter = filter.to_ascii_lowercase();
    shard.name.to_ascii_lowercase().contains(&filter)
        || shard
            .root
            .file_name()
            .map(|value| {
                value
                    .to_string_lossy()
                    .to_ascii_lowercase()
                    .contains(&filter)
            })
            .unwrap_or(false)
        || shard
            .root
            .to_string_lossy()
            .to_ascii_lowercase()
            .contains(&filter)
        || shard
            .git
            .as_ref()
            .map(|git| git_metadata_matches(git, &filter))
            .unwrap_or(false)
}

fn alias_matches(alias: &ShardAlias, filter: &str) -> bool {
    alias
        .name
        .to_ascii_lowercase()
        .contains(&filter.to_ascii_lowercase())
}

fn git_metadata_matches(git: &RepoGitMetadata, filter: &str) -> bool {
    git.origin
        .as_deref()
        .is_some_and(|value| value.to_ascii_lowercase().contains(filter))
        || git
            .branch
            .as_deref()
            .is_some_and(|value| value.to_ascii_lowercase().contains(filter))
        || git
            .git_kind
            .as_deref()
            .is_some_and(|value| value.to_ascii_lowercase().contains(filter))
        || git.git_common_dir.as_ref().is_some_and(|value| {
            value
                .to_string_lossy()
                .to_ascii_lowercase()
                .contains(filter)
        })
}

fn add_stats(total: &mut ShardBuildStats, stats: &IndexStats) {
    total.files += stats.files;
    total.source_bytes += stats.source_bytes;
    total.terms += stats.terms;
    total.path_terms += stats.path_terms;
    total.trigrams += stats.trigrams;
    total.posting_entries += stats.posting_entries;
    total.compressed_posting_bytes += stats.compressed_posting_bytes;
    total.symbols += stats.symbols;
}

fn add_index_stats(total: &mut ShardRefreshStats, stats: &IndexStats) {
    total.files += stats.files;
    total.source_bytes += stats.source_bytes;
    total.terms += stats.terms;
    total.path_terms += stats.path_terms;
    total.trigrams += stats.trigrams;
    total.posting_entries += stats.posting_entries;
    total.compressed_posting_bytes += stats.compressed_posting_bytes;
    total.symbols += stats.symbols;
}

fn add_ensure_stats(total: &mut ShardEnsureStats, stats: &IndexStats) {
    total.files += stats.files;
    total.source_bytes += stats.source_bytes;
    total.terms += stats.terms;
    total.path_terms += stats.path_terms;
    total.trigrams += stats.trigrams;
    total.posting_entries += stats.posting_entries;
    total.compressed_posting_bytes += stats.compressed_posting_bytes;
    total.symbols += stats.symbols;
    total.refreshed_files += stats.files;
}

fn sanitize_name(name: &str) -> String {
    let value = name
        .chars()
        .map(|ch| {
            if ch.is_ascii_alphanumeric() || ch == '-' || ch == '_' {
                ch
            } else {
                '-'
            }
        })
        .collect::<String>();
    value.trim_matches('-').to_string()
}

fn canonical_or_self(path: &Path) -> PathBuf {
    path.canonicalize().unwrap_or_else(|_| path.to_path_buf())
}

fn unique_shard_name(base_name: &str, hash: &str, names: &mut HashSet<String>) -> String {
    let mut name = base_name.to_string();
    if names.insert(name.clone()) {
        return name;
    }

    name = format!("{base_name}-{}", &hash[..8]);
    if names.insert(name.clone()) {
        return name;
    }

    let mut counter = 2usize;
    loop {
        let candidate = format!("{base_name}-{}-{counter}", &hash[..8]);
        if names.insert(candidate.clone()) {
            return candidate;
        }
        counter += 1;
    }
}

fn stable_hash(path: &Path) -> String {
    let mut hash = 0xcbf29ce484222325u64;
    for byte in path.to_string_lossy().as_bytes() {
        hash ^= *byte as u64;
        hash = hash.wrapping_mul(0x100000001b3);
    }
    format!("{hash:016x}")
}
