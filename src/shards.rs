//! Multi-repo shard manifests for local indexed search.

use crate::fast_index::{FastIndex, IndexStats};
use crate::query::{merge_filters, parse_query};
use crate::repo_index::{
    FileRange, SearchFilters, SearchResult, Symbol, finalize_results, normalize_token,
    read_file_range, repo_matches,
};
use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use std::collections::HashSet;
use std::fs;
use std::path::{Path, PathBuf};

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
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ShardBuildStats {
    pub version: u32,
    pub output_dir: PathBuf,
    pub shards: usize,
    pub files: usize,
    pub terms: usize,
    pub path_terms: usize,
    pub trigrams: usize,
    pub symbols: usize,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ShardRefreshStats {
    pub version: u32,
    pub output_dir: PathBuf,
    pub shards: usize,
    pub files: usize,
    pub terms: usize,
    pub path_terms: usize,
    pub trigrams: usize,
    pub symbols: usize,
    pub reused_files: usize,
    pub refreshed_files: usize,
    pub deleted_files: usize,
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
        terms: 0,
        path_terms: 0,
        trigrams: 0,
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
            name,
            root,
            index: index_name,
        });
    }

    total.shards = manifest.shards.len();
    save_manifest(output_dir, &manifest)?;
    Ok(total)
}

pub fn refresh_shards(index_dir: impl AsRef<Path>) -> Result<ShardRefreshStats> {
    let index_dir = index_dir.as_ref();
    let manifest = load_manifest(index_dir)?;
    let mut total = ShardRefreshStats {
        version: SHARD_MANIFEST_VERSION,
        output_dir: index_dir.to_path_buf(),
        shards: manifest.shards.len(),
        files: 0,
        terms: 0,
        path_terms: 0,
        trigrams: 0,
        symbols: 0,
        reused_files: 0,
        refreshed_files: 0,
        deleted_files: 0,
    };

    for shard in &manifest.shards {
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
        total.refreshed_files += outcome.refreshed_files;
        total.deleted_files += outcome.deleted_files;
    }

    save_manifest(index_dir, &manifest)?;
    Ok(total)
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
    let mut results = Vec::new();
    for shard in manifest.shards {
        if !repo_matches(&shard.root, &filters) {
            continue;
        }
        let index = FastIndex::load(index_dir.join(&shard.index))
            .with_context(|| format!("load shard {}", shard.index))?;
        for mut result in index.search_filtered(query, limit, &filters)? {
            result.path = format!("{}/{}", shard.name, result.path);
            result.reason = format!("shard:{}; {}", shard.name, result.reason);
            results.push(result);
        }
    }
    Ok(finalize_results(results, limit))
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
        if !repo_matches(&shard.root, filters) {
            continue;
        }
        let index = FastIndex::load(index_dir.join(&shard.index))
            .with_context(|| format!("load shard {}", shard.index))?;
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

pub fn read_shard_range(
    index_dir: impl AsRef<Path>,
    shard_path: &str,
    start: usize,
    lines: usize,
) -> Result<FileRange> {
    let manifest = load_manifest(index_dir.as_ref())?;
    let (shard_name, relative_path) = shard_path
        .split_once('/')
        .ok_or_else(|| anyhow::anyhow!("shard path must be '<repo>/<path>'"))?;
    let shard = manifest
        .shards
        .iter()
        .find(|shard| shard.name == shard_name)
        .ok_or_else(|| anyhow::anyhow!("unknown shard: {shard_name}"))?;
    let mut range = read_file_range(&shard.root, relative_path, start, lines)?;
    range.path = format!("{}/{}", shard.name, range.path);
    Ok(range)
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

fn load_manifest(index_dir: &Path) -> Result<ShardManifest> {
    let bytes = fs::read(index_dir.join("manifest.json"))
        .with_context(|| format!("read shard manifest {}", index_dir.display()))?;
    let manifest = serde_json::from_slice::<ShardManifest>(&bytes)?;
    anyhow::ensure!(
        manifest.version == SHARD_MANIFEST_VERSION,
        "unsupported shard manifest version {}",
        manifest.version
    );
    Ok(manifest)
}

fn save_manifest(index_dir: &Path, manifest: &ShardManifest) -> Result<()> {
    fs::write(
        index_dir.join("manifest.json"),
        serde_json::to_vec_pretty(manifest)?,
    )
    .with_context(|| format!("write shard manifest {}", index_dir.display()))
}

fn add_stats(total: &mut ShardBuildStats, stats: &IndexStats) {
    total.files += stats.files;
    total.terms += stats.terms;
    total.path_terms += stats.path_terms;
    total.trigrams += stats.trigrams;
    total.symbols += stats.symbols;
}

fn add_index_stats(total: &mut ShardRefreshStats, stats: &IndexStats) {
    total.files += stats.files;
    total.terms += stats.terms;
    total.path_terms += stats.path_terms;
    total.trigrams += stats.trigrams;
    total.symbols += stats.symbols;
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
