//! Multi-repo shard manifests for local indexed search.

use crate::discover::{RepoGitMetadata, git_metadata_for_repo};
use crate::fast_index::{FastIndex, IndexFreshness, IndexStats};
use crate::query::{merge_filters, parse_query, query_text, query_with_filters_text};
use crate::repo_index::{
    CommandHint, FileRange, QueryPlan, QueryPlanFilter, QueryPlanRepairHint, RangeScope,
    RelatedFile, RelatedSymbol, RepoMap, RepoMapDetail, SearchFilters, SearchResult, Symbol,
    finalize_results_for_filters, is_manifest_file, language_for, normalize_token,
    unique_query_tokens,
};
use ahash::{AHashMap as HashMap, AHashSet as HashSet};
use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use std::fs::{self, OpenOptions};
use std::io::ErrorKind;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::process;
use std::thread;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

const SHARD_MANIFEST_VERSION: u32 = 1;
const SHARD_MANIFEST_SIDECAR_VERSION: u32 = 2;
const SHARD_MANIFEST_PREFILTER_VERSION: u32 = 1;
const SHARD_MANIFEST_ROUTE_VERSION: u32 = 6;
const SHARD_MANIFEST_FILE: &str = "manifest.json";
const SHARD_MANIFEST_SIDECAR_FILE: &str = "manifest.bin";
const SHARD_MANIFEST_PREFILTER_FILE: &str = "manifest.prefilter.bin";
const SHARD_MANIFEST_ROUTE_FILE: &str = "manifest.route.bin";
const SHARD_ROUTE_MAX_POSTING_SHARDS: usize = 64;
const SHARD_WRITE_LOCK_FILE: &str = ".orient-shards.lock";
const SHARD_WRITE_LOCK_TIMEOUT: Duration = Duration::from_secs(120);
const SHARD_WRITE_LOCK_STALE_AFTER: Duration = Duration::from_secs(30 * 60);
const SHARD_WRITE_LOCK_RETRY: Duration = Duration::from_millis(25);
const SHARD_TRIGRAM_SKETCH_WORDS: usize = 512;
const SHARD_SUBSTRING_SKETCH_WORDS: usize = 4096;
const SHARD_KIND_SKETCH_WORDS: usize = 16;
const SHARD_FILTER_SKETCH_WORDS: usize = 64;
const SHARD_SUBSTRING_PREFILTER_MAX_TOKEN_CHARS: usize = 20;
const SHARD_ROUTE_SUBSTRING_GRAM_CHARS: usize = 6;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ShardManifest {
    pub version: u32,
    pub shards: Vec<ShardEntry>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct ShardManifestSidecar {
    version: u32,
    json_fingerprint: ManifestFileFingerprint,
    manifest: ShardManifestSidecarData,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct ShardManifestSidecarData {
    version: u32,
    shards: Vec<ShardManifestSidecarEntry>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct ShardManifestSidecarEntry {
    name: String,
    root: PathBuf,
    index: String,
    aliases: Vec<ShardRouteAlias>,
    git: Option<ShardRouteGitMetadata>,
    sketch: Option<ShardQuerySketch>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct ShardManifestPrefilter {
    version: u32,
    json_fingerprint: ManifestFileFingerprint,
    exact_hashes: Vec<u32>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct ShardManifestRoute {
    version: u32,
    json_fingerprint: ManifestFileFingerprint,
    shards: Vec<ShardRouteEntry>,
    exact_terms: Vec<ShardRouteTerm>,
    trigram_terms: Vec<ShardRouteTerm>,
    omitted_hashes: Vec<u32>,
    omitted_trigram_hashes: Vec<u32>,
    shard_ids: Vec<u8>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct ShardRouteEntry {
    name: String,
    root: PathBuf,
    index: String,
    aliases: Vec<ShardRouteAlias>,
    git: Option<ShardRouteGitMetadata>,
    substring_bits: Vec<u64>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct ShardRouteAlias {
    name: String,
    path_prefix: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct ShardRouteGitMetadata {
    git_kind: Option<String>,
    branch: Option<String>,
    origin: Option<String>,
    git_common_dir: Option<PathBuf>,
    tracked_files: Option<usize>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
struct ShardRouteTerm {
    hash: u32,
    start: u32,
    len: u16,
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum ShardRouteLookup {
    Candidates(Vec<u16>),
    MissingHash,
    Omitted,
    Corrupt,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
struct ShardRouteRequirements {
    exact_hashes: Vec<u32>,
    trigram_hashes: Vec<u32>,
    substring_grams: Vec<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
struct ManifestFileFingerprint {
    len: u64,
    modified_secs: u64,
    modified_nanos: u32,
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
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sketch: Option<ShardQuerySketch>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ShardQuerySketch {
    #[serde(default)]
    pub exact_hashes: Vec<u32>,
    #[serde(default)]
    pub trigram_hashes: Vec<u32>,
    #[serde(default)]
    pub exact_bits: Vec<u64>,
    #[serde(default)]
    pub trigram_bits: Vec<u64>,
    #[serde(default)]
    pub substring_bits: Vec<u64>,
    #[serde(default)]
    pub symbol_kind_bits: Vec<u64>,
    #[serde(default)]
    pub filter_bits: Vec<u64>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ShardAlias {
    pub name: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub path_prefix: Option<String>,
}

impl ShardRouteEntry {
    fn from_shard(shard: &ShardEntry) -> Self {
        Self {
            name: shard.name.clone(),
            root: shard.root.clone(),
            index: shard.index.clone(),
            aliases: shard
                .aliases
                .iter()
                .map(|alias| ShardRouteAlias {
                    name: alias.name.clone(),
                    path_prefix: alias.path_prefix.clone(),
                })
                .collect(),
            git: shard.git.as_ref().map(ShardRouteGitMetadata::from_git),
            substring_bits: shard
                .sketch
                .as_ref()
                .map(|sketch| sketch.substring_bits.clone())
                .unwrap_or_default(),
        }
    }

    fn into_shard(self) -> ShardEntry {
        ShardEntry {
            name: self.name,
            root: self.root,
            index: self.index,
            aliases: self
                .aliases
                .into_iter()
                .map(|alias| ShardAlias {
                    name: alias.name,
                    path_prefix: alias.path_prefix,
                })
                .collect(),
            git: self.git.map(ShardRouteGitMetadata::into_git),
            sketch: None,
        }
    }
}

impl ShardManifestSidecarData {
    fn from_manifest(manifest: &ShardManifest) -> Self {
        Self {
            version: manifest.version,
            shards: manifest
                .shards
                .iter()
                .map(ShardManifestSidecarEntry::from_shard)
                .collect(),
        }
    }

    fn into_manifest(self) -> ShardManifest {
        ShardManifest {
            version: self.version,
            shards: self
                .shards
                .into_iter()
                .map(ShardManifestSidecarEntry::into_shard)
                .collect(),
        }
    }
}

impl ShardManifestSidecarEntry {
    fn from_shard(shard: &ShardEntry) -> Self {
        Self {
            name: shard.name.clone(),
            root: shard.root.clone(),
            index: shard.index.clone(),
            aliases: shard
                .aliases
                .iter()
                .map(|alias| ShardRouteAlias {
                    name: alias.name.clone(),
                    path_prefix: alias.path_prefix.clone(),
                })
                .collect(),
            git: shard.git.as_ref().map(ShardRouteGitMetadata::from_git),
            sketch: shard.sketch.clone(),
        }
    }

    fn into_shard(self) -> ShardEntry {
        ShardEntry {
            name: self.name,
            root: self.root,
            index: self.index,
            aliases: self
                .aliases
                .into_iter()
                .map(|alias| ShardAlias {
                    name: alias.name,
                    path_prefix: alias.path_prefix,
                })
                .collect(),
            git: self.git.map(ShardRouteGitMetadata::into_git),
            sketch: self.sketch,
        }
    }
}

impl ShardRouteGitMetadata {
    fn from_git(git: &RepoGitMetadata) -> Self {
        Self {
            git_kind: git.git_kind.clone(),
            branch: git.branch.clone(),
            origin: git.origin.clone(),
            git_common_dir: git.git_common_dir.clone(),
            tracked_files: git.tracked_files,
        }
    }

    fn into_git(self) -> RepoGitMetadata {
        RepoGitMetadata {
            git_kind: self.git_kind,
            branch: self.branch,
            origin: self.origin,
            git_common_dir: self.git_common_dir,
            tracked_files: self.tracked_files,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ShardBuildStats {
    pub version: u32,
    pub output_dir: PathBuf,
    pub shards: usize,
    pub files: usize,
    pub source_bytes: u64,
    pub content_snapshot_bytes: u64,
    pub line_offset_bytes: usize,
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
    pub content_snapshot_bytes: u64,
    pub line_offset_bytes: usize,
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
    pub content_snapshot_bytes: u64,
    pub line_offset_bytes: usize,
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
    pub manifest_bytes: u64,
    pub manifest_sidecar_bytes: u64,
    pub manifest_prefilter_bytes: u64,
    pub manifest_route_bytes: u64,
    pub manifest_route_exact_terms: usize,
    pub manifest_route_trigram_terms: usize,
    pub manifest_route_substring_filter_shards: usize,
    pub manifest_route_omitted_exact_terms: usize,
    pub manifest_route_omitted_trigram_terms: usize,
    pub stale: bool,
    pub stale_shards: usize,
    pub index_bytes: u64,
    pub source_bytes: u64,
    pub content_snapshot_bytes: u64,
    pub line_offset_bytes: usize,
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
    build_shards_with_force(repos, output_dir, false)
}

pub fn build_shards_with_force(
    repos: &[PathBuf],
    output_dir: impl AsRef<Path>,
    force: bool,
) -> Result<ShardBuildStats> {
    let output_dir = output_dir.as_ref();
    let _lock = ShardWriteLock::acquire(output_dir)?;
    guard_rebuild_against_shrink(repos, output_dir, force)?;
    build_shards_unlocked(
        repos,
        output_dir,
        if force {
            ShardManifestWriteMode::AllowShrink
        } else {
            ShardManifestWriteMode::PreserveExisting
        },
    )
}

fn build_shards_unlocked(
    repos: &[PathBuf],
    output_dir: &Path,
    write_mode: ShardManifestWriteMode,
) -> Result<ShardBuildStats> {
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
        content_snapshot_bytes: 0,
        line_offset_bytes: 0,
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
            sketch: Some(shard_query_sketch(&index)),
            name,
            root,
            index: index_name,
        });
    }

    total.shards = manifest.shards.len();
    save_manifest_with_mode(output_dir, &manifest, write_mode)?;
    Ok(total)
}

fn guard_rebuild_against_shrink(repos: &[PathBuf], output_dir: &Path, force: bool) -> Result<()> {
    if force || !output_dir.join("manifest.json").exists() {
        return Ok(());
    }
    let manifest = load_manifest(output_dir)?;
    if manifest.shards.is_empty() {
        return Ok(());
    }
    let requested_roots = repos
        .iter()
        .map(|repo| repo.canonicalize())
        .collect::<std::result::Result<HashSet<_>, _>>()?;
    let omitted = manifest
        .shards
        .iter()
        .filter(|shard| !requested_roots.contains(&canonical_or_self(&shard.root)))
        .count();
    anyhow::ensure!(
        omitted == 0,
        "refusing to overwrite shard directory {} because the existing manifest has {} shard(s) and the requested rebuild would remove {} shard(s); use ensure-shards to add or refresh repos, refresh-shards to prune missing roots, or index-shards --force to replace the shard directory",
        output_dir.display(),
        manifest.shards.len(),
        omitted,
    );
    Ok(())
}

pub fn ensure_shards(repos: &[PathBuf], output_dir: impl AsRef<Path>) -> Result<ShardEnsureStats> {
    let output_dir = output_dir.as_ref();
    let _lock = ShardWriteLock::acquire(output_dir)?;
    if output_dir.join("manifest.json").exists() {
        let stats = refresh_shards_unlocked(output_dir)?;
        let mut total = ShardEnsureStats {
            version: stats.version,
            output_dir: stats.output_dir,
            action: ensure_action(stats.removed_shards, 0),
            shards: stats.shards,
            added_shards: 0,
            removed_shards: stats.removed_shards,
            files: stats.files,
            source_bytes: stats.source_bytes,
            content_snapshot_bytes: stats.content_snapshot_bytes,
            line_offset_bytes: stats.line_offset_bytes,
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
    let stats = build_shards_unlocked(repos, output_dir, ShardManifestWriteMode::PreserveExisting)?;
    Ok(ShardEnsureStats {
        version: stats.version,
        output_dir: stats.output_dir,
        action: "build".to_string(),
        shards: stats.shards,
        added_shards: stats.shards,
        removed_shards: 0,
        files: stats.files,
        source_bytes: stats.source_bytes,
        content_snapshot_bytes: stats.content_snapshot_bytes,
        line_offset_bytes: stats.line_offset_bytes,
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
    let _lock = ShardWriteLock::acquire(index_dir)?;
    refresh_shards_unlocked(index_dir)
}

pub fn refresh_shards_by_root(
    index_dir: impl AsRef<Path>,
    roots: &[PathBuf],
) -> Result<ShardRefreshStats> {
    let index_dir = index_dir.as_ref();
    let _lock = ShardWriteLock::acquire(index_dir)?;
    refresh_shards_by_root_unlocked(index_dir, roots)
}

fn refresh_shards_unlocked(index_dir: &Path) -> Result<ShardRefreshStats> {
    let mut manifest = load_manifest(index_dir)?;
    let mut total = empty_shard_refresh_stats(index_dir, manifest.shards.len());

    let mut kept_shards = Vec::with_capacity(manifest.shards.len());
    for mut shard in manifest.shards {
        if !shard.root.exists() {
            let _ = fs::remove_file(index_dir.join(&shard.index));
            total.removed_shards += 1;
            continue;
        }
        refresh_existing_shard(index_dir, &mut shard, &mut total)?;
        kept_shards.push(shard);
    }

    manifest.shards = kept_shards;
    total.shards = manifest.shards.len();
    save_manifest_with_mode(index_dir, &manifest, ShardManifestWriteMode::AllowShrink)?;
    Ok(total)
}

fn refresh_shards_by_root_unlocked(
    index_dir: &Path,
    roots: &[PathBuf],
) -> Result<ShardRefreshStats> {
    let mut manifest = load_manifest(index_dir)?;
    let roots = roots
        .iter()
        .map(|root| canonical_or_self(root))
        .collect::<HashSet<_>>();
    let mut total = empty_shard_refresh_stats(index_dir, manifest.shards.len());
    let mut kept_shards = Vec::with_capacity(manifest.shards.len());

    for mut shard in manifest.shards {
        if !roots.contains(&canonical_or_self(&shard.root)) {
            kept_shards.push(shard);
            continue;
        }
        if !shard.root.exists() {
            let _ = fs::remove_file(index_dir.join(&shard.index));
            total.removed_shards += 1;
            continue;
        }
        refresh_existing_shard(index_dir, &mut shard, &mut total)?;
        kept_shards.push(shard);
    }

    manifest.shards = kept_shards;
    total.shards = manifest.shards.len();
    save_manifest_with_mode(index_dir, &manifest, ShardManifestWriteMode::AllowShrink)?;
    Ok(total)
}

fn empty_shard_refresh_stats(index_dir: &Path, shards: usize) -> ShardRefreshStats {
    ShardRefreshStats {
        version: SHARD_MANIFEST_VERSION,
        output_dir: index_dir.to_path_buf(),
        shards,
        files: 0,
        source_bytes: 0,
        content_snapshot_bytes: 0,
        line_offset_bytes: 0,
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
    }
}

fn refresh_existing_shard(
    index_dir: &Path,
    shard: &mut ShardEntry,
    total: &mut ShardRefreshStats,
) -> Result<()> {
    let index_path = index_dir.join(&shard.index);
    let previous = if index_path.exists() {
        FastIndex::load_reusable(&index_path)
            .with_context(|| format!("load shard {}", shard.index))?
    } else {
        None
    };
    let outcome = FastIndex::refresh(&shard.root, previous.as_ref())
        .with_context(|| format!("refresh shard {}", shard.name))?;
    outcome.index.save(&index_path)?;
    let stats = outcome.index.stats();
    add_index_stats(total, &stats);
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
    shard.sketch = Some(shard_query_sketch(&outcome.index));
    Ok(())
}

pub fn shard_status(index_dir: impl AsRef<Path>) -> Result<ShardFreshness> {
    let index_dir = index_dir.as_ref();
    let manifest = load_manifest(index_dir)?;
    let route = load_manifest_route(index_dir)?;
    let shards = shard_status_jobs(index_dir, &manifest.shards)?;
    Ok(shard_freshness_from_statuses(
        index_dir,
        manifest.shards.len(),
        route.as_ref(),
        shards,
    ))
}

pub fn shard_status_by_root(
    index_dir: impl AsRef<Path>,
    roots: &[PathBuf],
) -> Result<ShardFreshness> {
    let index_dir = index_dir.as_ref();
    let manifest = load_manifest(index_dir)?;
    let route = load_manifest_route(index_dir)?;
    let roots = roots
        .iter()
        .map(|root| canonical_or_self(root))
        .collect::<HashSet<_>>();
    let selected = manifest
        .shards
        .iter()
        .filter(|shard| roots.contains(&canonical_or_self(&shard.root)))
        .cloned()
        .collect::<Vec<_>>();
    let shard_count = selected.len();
    let shards = shard_status_jobs(index_dir, &selected)?;
    Ok(shard_freshness_from_statuses(
        index_dir,
        shard_count,
        route.as_ref(),
        shards,
    ))
}

fn shard_freshness_from_statuses(
    index_dir: &Path,
    shard_count: usize,
    route: Option<&ShardManifestRoute>,
    shards: Vec<ShardIndexFreshness>,
) -> ShardFreshness {
    let mut stale_shards = 0usize;
    let mut changed_files = 0usize;
    let mut deleted_files = 0usize;
    let mut added_files = 0usize;
    let mut index_bytes = 0u64;
    let mut source_bytes = 0u64;
    let mut content_snapshot_bytes = 0u64;
    let mut line_offset_bytes = 0usize;
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
        index_bytes += status.index_bytes;
        source_bytes += status.source_bytes;
        content_snapshot_bytes += status.content_snapshot_bytes;
        line_offset_bytes += status.line_offset_bytes;
        terms += status.terms;
        path_terms += status.path_terms;
        trigrams += status.trigrams;
        posting_entries += status.posting_entries;
        compressed_posting_bytes += status.compressed_posting_bytes;
        symbols += status.symbols;
    }

    ShardFreshness {
        version: SHARD_MANIFEST_VERSION,
        index_dir: index_dir.to_path_buf(),
        shard_count,
        manifest_bytes: file_len(index_dir.join(SHARD_MANIFEST_FILE)),
        manifest_sidecar_bytes: file_len(index_dir.join(SHARD_MANIFEST_SIDECAR_FILE)),
        manifest_prefilter_bytes: file_len(index_dir.join(SHARD_MANIFEST_PREFILTER_FILE)),
        manifest_route_bytes: file_len(index_dir.join(SHARD_MANIFEST_ROUTE_FILE)),
        manifest_route_exact_terms: route
            .map(|route| route.exact_terms.len())
            .unwrap_or_default(),
        manifest_route_trigram_terms: route
            .map(|route| route.trigram_terms.len())
            .unwrap_or_default(),
        manifest_route_substring_filter_shards: route
            .map(|route| {
                route
                    .shards
                    .iter()
                    .filter(|shard| !shard.substring_bits.is_empty())
                    .count()
            })
            .unwrap_or_default(),
        manifest_route_omitted_exact_terms: route
            .map(|route| route.omitted_hashes.len())
            .unwrap_or_default(),
        manifest_route_omitted_trigram_terms: route
            .map(|route| route.omitted_trigram_hashes.len())
            .unwrap_or_default(),
        stale: stale_shards > 0,
        stale_shards,
        index_bytes,
        source_bytes,
        content_snapshot_bytes,
        line_offset_bytes,
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
    }
}

fn file_len(path: impl AsRef<Path>) -> u64 {
    fs::metadata(path)
        .map(|metadata| metadata.len())
        .unwrap_or(0)
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
        let index_path = index_dir.join(&shard.index);
        let loaded =
            FastIndex::load(&index_path).with_context(|| format!("load shard {}", shard.index))?;
        let status = loaded
            .freshness_at(&index_path)
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

struct ShardWriteLock {
    path: PathBuf,
}

impl ShardWriteLock {
    fn acquire(index_dir: &Path) -> Result<Self> {
        fs::create_dir_all(index_dir)
            .with_context(|| format!("create shard directory {}", index_dir.display()))?;
        let path = index_dir.join(SHARD_WRITE_LOCK_FILE);
        let started = Instant::now();
        loop {
            match OpenOptions::new().write(true).create_new(true).open(&path) {
                Ok(mut file) => {
                    writeln!(file, "pid={}", process::id())
                        .with_context(|| format!("write shard lock {}", path.display()))?;
                    writeln!(file, "created_nanos={}", current_nanos())
                        .with_context(|| format!("write shard lock {}", path.display()))?;
                    file.sync_all()
                        .with_context(|| format!("sync shard lock {}", path.display()))?;
                    return Ok(Self { path });
                }
                Err(error) if error.kind() == ErrorKind::AlreadyExists => {
                    if shard_lock_is_stale(&path)? {
                        let _ = fs::remove_file(&path);
                        continue;
                    }
                    if started.elapsed() >= SHARD_WRITE_LOCK_TIMEOUT {
                        anyhow::bail!(
                            "timed out waiting for shard writer lock {} after {:?}",
                            path.display(),
                            SHARD_WRITE_LOCK_TIMEOUT
                        );
                    }
                    thread::sleep(SHARD_WRITE_LOCK_RETRY);
                }
                Err(error) => {
                    return Err(error)
                        .with_context(|| format!("create shard lock {}", path.display()));
                }
            }
        }
    }
}

impl Drop for ShardWriteLock {
    fn drop(&mut self) {
        let _ = fs::remove_file(&self.path);
    }
}

fn shard_lock_is_stale(path: &Path) -> Result<bool> {
    let Ok(metadata) = fs::metadata(path) else {
        return Ok(false);
    };
    let modified = metadata
        .modified()
        .with_context(|| format!("stat shard lock {}", path.display()))?;
    Ok(modified
        .elapsed()
        .map(|elapsed| elapsed >= SHARD_WRITE_LOCK_STALE_AFTER)
        .unwrap_or(false))
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
            sketch: Some(shard_query_sketch(&index)),
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
    let parsed = parse_query(query);
    let filters = merge_filters(filters.clone(), parsed.filters);
    let shard_query = query_text(&parsed.terms, &filters);
    if shard_prefilter_query_impossible(index_dir, &shard_query, &filters)? {
        return Ok(Vec::new());
    }
    let jobs = if let Some(shards) = shard_route_entries(index_dir, &shard_query, &filters)? {
        shard_entries_to_jobs(shards, &filters)
    } else {
        let manifest = load_manifest(index_dir)?;
        manifest
            .shards
            .into_iter()
            .filter_map(|shard| {
                if !shard_sketch_may_match_query(&shard, &shard_query, &filters) {
                    return None;
                }
                let scopes = shard_search_scopes(&shard, &filters);
                (!scopes.is_empty()).then_some(ShardJob { shard, scopes })
            })
            .collect::<Vec<_>>()
    };
    let results = search_shard_jobs(index_dir, &shard_query, limit, &filters, jobs)?;
    Ok(finalize_results_for_filters(results, limit, &filters))
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
            if !index.query_may_match(query, &scoped_filters) {
                continue;
            }
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
    let shard_count = manifest.shards.len();
    let shard_names = manifest
        .shards
        .iter()
        .map(|shard| shard.name.clone())
        .collect::<Vec<_>>();
    let jobs = manifest
        .shards
        .into_iter()
        .filter_map(|shard| {
            let scopes = shard_search_scopes(&shard, &filters);
            (!scopes.is_empty()).then_some(ShardJob { shard, scopes })
        })
        .collect::<Vec<_>>();
    let filtered_jobs = shard_diagnostic_jobs(jobs, &shard_query);
    let jobs = filtered_jobs;
    if jobs.is_empty() {
        return Ok(vec![shard_selection_miss_plan(
            index_dir,
            &shard_query,
            &filters,
            shard_count,
            shard_names,
        )]);
    }
    let mut plans = shard_query_plan_jobs(index_dir, &shard_query, &filters, jobs)?;
    plans.sort_by(|left, right| left.name.cmp(&right.name));
    append_shard_facet_repair_hints(&mut plans, &parsed.terms, &filters);
    Ok(plans)
}

fn shard_diagnostic_jobs(jobs: Vec<ShardJob>, shard_query: &str) -> Vec<ShardJob> {
    let query_tokens = unique_query_tokens(shard_query);
    if query_tokens.is_empty() {
        return jobs;
    }
    let filtered = jobs
        .iter()
        .filter(|job| shard_sketch_may_diagnose(&job.shard, &query_tokens))
        .cloned()
        .collect::<Vec<_>>();
    if filtered.is_empty() { jobs } else { filtered }
}

pub(crate) fn shard_selection_miss_plan(
    index_dir: &Path,
    query: &str,
    filters: &SearchFilters,
    shard_count: usize,
    shard_names: Vec<String>,
) -> ShardQueryPlan {
    let message = if shard_count == 0 {
        "The shard manifest has no searchable shards. Build or refresh the shard directory before searching."
    } else {
        "No shard matched the repo/branch/origin filters. Relax shard filters or inspect shard_repo_map for available names, branches, and origins."
    };
    ShardQueryPlan {
        name: "__shard_selection__".to_string(),
        root: index_dir.to_path_buf(),
        aliases: shard_names,
        git: None,
        plan: QueryPlan {
            strategy: "shard_filter_mismatch".to_string(),
            require_all: filters.require_all,
            query_tokens: Vec::new(),
            query_phrases: Vec::new(),
            query_trigrams: Vec::new(),
            active_filters: shard_scope_plan_filters(filters),
            planned_postings: Vec::new(),
            missing_terms: Vec::new(),
            missing_trigrams: Vec::new(),
            candidate_count: shard_count,
            candidate_cap: shard_count,
            candidate_cap_hit: false,
            filtered_candidate_count: 0,
            scored_candidate_count: 0,
            final_match_count: 0,
            repair_hints: vec![QueryPlanRepairHint {
                kind: "relax_filters".to_string(),
                message: message.to_string(),
                suggested_query: (!query.trim().is_empty()).then(|| query.to_string()),
            }],
            retry_requests: Vec::new(),
        },
    }
}

fn shard_scope_plan_filters(filters: &SearchFilters) -> Vec<QueryPlanFilter> {
    let mut active = Vec::new();
    if let Some(value) = &filters.repo {
        active.push(shard_scope_plan_filter("repo", value, false));
    }
    if let Some(value) = &filters.branch {
        active.push(shard_scope_plan_filter("branch", value, false));
    }
    if let Some(value) = &filters.origin {
        active.push(shard_scope_plan_filter("origin", value, false));
    }
    for value in &filters.exclude_repo {
        active.push(shard_scope_plan_filter("repo", value, true));
    }
    for value in &filters.exclude_branch {
        active.push(shard_scope_plan_filter("branch", value, true));
    }
    for value in &filters.exclude_origin {
        active.push(shard_scope_plan_filter("origin", value, true));
    }
    active
}

fn shard_scope_plan_filter(field: &str, value: &str, negated: bool) -> QueryPlanFilter {
    QueryPlanFilter {
        field: field.to_string(),
        value: value.to_string(),
        negated,
        candidate_matches: None,
        candidate_rejections: None,
    }
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

pub(crate) fn append_shard_facet_repair_hints(
    plans: &mut [ShardQueryPlan],
    query_terms: &[String],
    filters: &SearchFilters,
) {
    if plans.is_empty() {
        return;
    }
    let total = plans.iter().map(shard_plan_weight).sum::<usize>();
    if total < 16 {
        return;
    }

    let mut hints = Vec::new();
    if filters.repo.is_none() {
        if let Some((repo, count)) = top_meaningful_weighted_facet(
            plans
                .iter()
                .map(|plan| (plan.name.clone(), shard_plan_weight(plan))),
            total,
        ) {
            hints.push(shard_facet_hint(
                "narrow_by_repo",
                "repo",
                &repo,
                count,
                total,
                query_terms,
                filters,
            ));
        }
    }
    if filters.branch.is_none() {
        if let Some((branch, count)) = top_meaningful_weighted_facet(
            plans.iter().filter_map(|plan| {
                plan.git
                    .as_ref()
                    .and_then(|git| git.branch.clone())
                    .map(|branch| (branch, shard_plan_weight(plan)))
            }),
            total,
        ) {
            hints.push(shard_facet_hint(
                "narrow_by_branch",
                "branch",
                &branch,
                count,
                total,
                query_terms,
                filters,
            ));
        }
    }
    if filters.origin.is_none() {
        if let Some((origin, count)) = top_meaningful_weighted_facet(
            plans.iter().filter_map(|plan| {
                plan.git
                    .as_ref()
                    .and_then(|git| git.origin.clone())
                    .map(|origin| (origin, shard_plan_weight(plan)))
            }),
            total,
        ) {
            hints.push(shard_facet_hint(
                "narrow_by_origin",
                "origin",
                &origin,
                count,
                total,
                query_terms,
                filters,
            ));
        }
    }

    if hints.is_empty() {
        return;
    }
    hints.truncate(3);
    if let Some(plan) = plans.iter_mut().find(|plan| shard_plan_weight(plan) > 0) {
        for hint in hints {
            if !plan
                .plan
                .repair_hints
                .iter()
                .any(|existing| existing.kind == hint.kind)
            {
                plan.plan.repair_hints.push(hint);
            }
        }
    }
}

fn shard_plan_weight(plan: &ShardQueryPlan) -> usize {
    plan.plan
        .final_match_count
        .max(plan.plan.scored_candidate_count)
        .max(plan.plan.filtered_candidate_count)
        .max(plan.plan.candidate_count)
}

fn top_meaningful_weighted_facet(
    values: impl Iterator<Item = (String, usize)>,
    total: usize,
) -> Option<(String, usize)> {
    let mut counts = HashMap::<String, usize>::new();
    for (value, count) in values {
        if value.trim().is_empty() || count == 0 {
            continue;
        }
        *counts.entry(value).or_default() += count;
    }
    let mut counts = counts.into_iter().collect::<Vec<_>>();
    counts.sort_by(|left, right| right.1.cmp(&left.1).then_with(|| left.0.cmp(&right.0)));
    counts
        .into_iter()
        .find(|(_, count)| facet_count_is_meaningful(*count, total))
}

fn facet_count_is_meaningful(count: usize, total: usize) -> bool {
    count >= 2 && count < total && count.saturating_mul(5) <= total.saturating_mul(4)
}

fn shard_facet_hint(
    kind: &str,
    field: &str,
    value: &str,
    count: usize,
    total: usize,
    query_terms: &[String],
    filters: &SearchFilters,
) -> QueryPlanRepairHint {
    let mut narrowed = filters.clone();
    match field {
        "repo" => narrowed.repo = Some(value.to_string()),
        "branch" => narrowed.branch = Some(value.to_string()),
        "origin" => narrowed.origin = Some(value.to_string()),
        _ => {}
    }
    let suggested_query = query_with_filters_text(query_terms, &narrowed);
    QueryPlanRepairHint {
        kind: kind.to_string(),
        message: format!(
            "Filter `{field}:{value}` narrows the shard candidate set from {total} files to {count}."
        ),
        suggested_query: (!suggested_query.trim().is_empty()).then_some(suggested_query),
    }
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

pub fn shard_repo_maps(
    index_dir: impl AsRef<Path>,
    symbol_limit: usize,
    test_limit: usize,
    detail: RepoMapDetail,
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
            let mut map = index.repo_map_with_detail(base_symbol_limit, base_test_limit, detail);
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
    read_shard_range_scoped(index_dir, shard_path, start, lines, RangeScope::Exact)
}

pub fn read_shard_range_scoped(
    index_dir: impl AsRef<Path>,
    shard_path: &str,
    start: usize,
    lines: usize,
    scope: RangeScope,
) -> Result<FileRange> {
    let resolved = resolve_shard_path(index_dir.as_ref(), shard_path)?;
    let index = FastIndex::load(index_dir.as_ref().join(&resolved.index))
        .with_context(|| format!("load shard {}", resolved.index))?;
    let mut range = index.read_range_scoped(&resolved.relative_path, start, lines, scope)?;
    range.path = resolved.output_path(&range.path);
    if let Some(symbol) = &mut range.symbol {
        symbol.path = resolved.output_path(&symbol.path);
    }
    Ok(range)
}

pub fn related_shard_files(
    index_dir: impl AsRef<Path>,
    shard_path: &str,
    limit: usize,
) -> Result<Vec<RelatedFile>> {
    related_shard_files_filtered(index_dir, shard_path, limit, &SearchFilters::default())
}

pub fn related_shard_files_filtered(
    index_dir: impl AsRef<Path>,
    shard_path: &str,
    limit: usize,
    filters: &SearchFilters,
) -> Result<Vec<RelatedFile>> {
    let resolved = resolve_shard_path(index_dir.as_ref(), shard_path)?;
    let index = FastIndex::load(index_dir.as_ref().join(&resolved.index))
        .with_context(|| format!("load shard {}", resolved.index))?;
    let filters = related_filters_without_shard_selectors(filters);
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

pub fn related_shard_symbols(
    index_dir: impl AsRef<Path>,
    shard_path: &str,
    query: Option<&str>,
    limit: usize,
) -> Result<Vec<RelatedSymbol>> {
    related_shard_symbols_filtered(
        index_dir,
        shard_path,
        query,
        limit,
        &SearchFilters::default(),
    )
}

pub fn related_shard_symbols_filtered(
    index_dir: impl AsRef<Path>,
    shard_path: &str,
    query: Option<&str>,
    limit: usize,
    filters: &SearchFilters,
) -> Result<Vec<RelatedSymbol>> {
    let resolved = resolve_shard_path(index_dir.as_ref(), shard_path)?;
    let index = FastIndex::load(index_dir.as_ref().join(&resolved.index))
        .with_context(|| format!("load shard {}", resolved.index))?;
    let query = related_query_without_shard_selectors(query);
    let filters = related_filters_without_shard_selectors(filters);
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

fn related_filters_without_shard_selectors(filters: &SearchFilters) -> SearchFilters {
    let mut filters = filters.clone();
    filters.repo = None;
    filters.branch = None;
    filters.origin = None;
    filters.exclude_repo.clear();
    filters.exclude_branch.clear();
    filters.exclude_origin.clear();
    filters
}

pub(crate) fn related_query_without_shard_selectors(query: Option<&str>) -> Option<String> {
    let query = query?;
    let parsed = parse_query(query);
    let mut filters = parsed.filters;
    filters.repo = None;
    filters.branch = None;
    filters.origin = None;
    filters.exclude_repo.clear();
    filters.exclude_branch.clear();
    filters.exclude_origin.clear();
    let query = query_with_filters_text(&parsed.terms, &filters);
    (!query.trim().is_empty()).then_some(query)
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
    resolve_shard_path_from_manifest(&manifest, shard_path)
}

pub(crate) fn resolve_shard_path_from_manifest(
    manifest: &ShardManifest,
    shard_path: &str,
) -> Result<ResolvedShardRead> {
    if let Some((prefix, relative_path)) = shard_path.split_once('/') {
        if let Some(resolved) = resolve_shard_read_path(manifest, prefix, relative_path) {
            return Ok(resolved);
        }
    }

    let candidates = unqualified_shard_path_candidates(manifest, shard_path);
    match candidates.len() {
        1 => Ok(candidates.into_iter().next().expect("candidate exists")),
        0 => {
            if let Some((prefix, _)) = shard_path.split_once('/') {
                anyhow::bail!(
                    "unknown shard or alias: {prefix}; use '<repo>/<path>' or a unique shard-relative path"
                );
            }
            anyhow::bail!("shard path must be '<repo>/<path>' or a unique shard-relative path");
        }
        _ => {
            let mut names = candidates
                .iter()
                .map(|candidate| candidate.output_prefix.clone())
                .collect::<Vec<_>>();
            names.sort();
            names.dedup();
            anyhow::bail!(
                "ambiguous shard path {shard_path:?}; matched {}; use '<repo>/<path>'",
                names.join(", ")
            );
        }
    }
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

fn unqualified_shard_path_candidates(
    manifest: &ShardManifest,
    shard_path: &str,
) -> Vec<ResolvedShardRead> {
    if !valid_unqualified_shard_path(shard_path) {
        return Vec::new();
    }

    if manifest.shards.len() == 1 {
        let shard = &manifest.shards[0];
        return vec![ResolvedShardRead {
            index: shard.index.clone(),
            relative_path: shard_path.to_string(),
            output_prefix: shard.name.clone(),
            path_prefix: None,
        }];
    }

    let mut candidates = Vec::new();
    for shard in &manifest.shards {
        if shard.root.join(shard_path).is_file() {
            candidates.push(ResolvedShardRead {
                index: shard.index.clone(),
                relative_path: shard_path.to_string(),
                output_prefix: shard.name.clone(),
                path_prefix: None,
            });
        }
    }
    candidates
}

fn valid_unqualified_shard_path(value: &str) -> bool {
    let path = Path::new(value);
    !value.trim().is_empty()
        && path.is_relative()
        && path.components().all(|component| {
            matches!(
                component,
                std::path::Component::Normal(_) | std::path::Component::CurDir
            )
        })
        && path.components().any(|component| {
            matches!(
                component,
                std::path::Component::Normal(_) | std::path::Component::CurDir
            )
        })
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

fn shard_query_sketch(index: &FastIndex) -> ShardQuerySketch {
    let mut exact_hashes = Vec::with_capacity(
        index.postings.len() + index.path_postings.len() + index.symbol_postings.len(),
    );
    let mut trigram_bits = vec![0; SHARD_TRIGRAM_SKETCH_WORDS];
    let mut substring_bits = vec![0; SHARD_SUBSTRING_SKETCH_WORDS];
    let mut symbol_kind_bits = vec![0; SHARD_KIND_SKETCH_WORDS];
    let mut filter_bits = vec![0; SHARD_FILTER_SKETCH_WORDS];

    for key in index
        .postings
        .keys()
        .chain(index.path_postings.keys())
        .chain(index.symbol_postings.keys())
    {
        exact_hashes.push(sketch_fingerprint(key));
    }
    for file in &index.files {
        push_content_identifier_hashes(&file.content, &mut exact_hashes);
        push_content_substring_grams(&file.content, &mut substring_bits);
    }
    exact_hashes.sort_unstable();
    exact_hashes.dedup();
    for key in index.trigram_postings.keys() {
        sketch_insert(&mut trigram_bits, key);
    }
    let mut trigram_hashes = index
        .trigram_postings
        .keys()
        .map(|key| sketch_fingerprint(key))
        .collect::<Vec<_>>();
    trigram_hashes.sort_unstable();
    trigram_hashes.dedup();
    for key in index.symbol_kind_postings.keys() {
        sketch_insert(&mut symbol_kind_bits, key);
    }
    for key in index.attribute_postings.keys() {
        sketch_insert(&mut filter_bits, key);
    }

    ShardQuerySketch {
        exact_hashes,
        trigram_hashes,
        exact_bits: Vec::new(),
        trigram_bits,
        substring_bits,
        symbol_kind_bits,
        filter_bits,
    }
}

pub(crate) fn shard_sketch_may_match(
    shard: &ShardEntry,
    query_tokens: &[String],
    query_identifier: Option<&str>,
    filters: &SearchFilters,
) -> bool {
    let Some(sketch) = &shard.sketch else {
        return true;
    };

    if !shard_sketch_filters_may_match(sketch, filters) {
        return false;
    }
    if query_tokens.is_empty() {
        return true;
    }
    if let Some(identifier) = query_identifier {
        if !shard_sketch_exact_may_contain(sketch, identifier) {
            return false;
        }
    }

    let require_all = filters.require_all || (query_tokens.len() > 1 && !filters.match_any);
    let allow_trigram_fallback = filters.match_any
        || (query_tokens.len() == 1 && shard_allows_substring_prefilter(&query_tokens[0]));
    if require_all {
        query_tokens.iter().all(|token| {
            shard_sketch_token_may_match(sketch, token, filters, allow_trigram_fallback)
        })
    } else {
        query_tokens.iter().any(|token| {
            shard_sketch_token_may_match(sketch, token, filters, allow_trigram_fallback)
        })
    }
}

pub(crate) fn shard_sketch_may_match_query(
    shard: &ShardEntry,
    shard_query: &str,
    filters: &SearchFilters,
) -> bool {
    let query_tokens = unique_query_tokens(shard_query);
    let query_identifier = shard_query_identifier_prefilter(shard_query, &query_tokens, filters);
    shard_sketch_may_match(shard, &query_tokens, query_identifier.as_deref(), filters)
}

pub(crate) fn shard_sketch_may_diagnose_query(shard: &ShardEntry, shard_query: &str) -> bool {
    let query_tokens = unique_query_tokens(shard_query);
    query_tokens.is_empty() || shard_sketch_may_diagnose(shard, &query_tokens)
}

fn shard_sketch_may_diagnose(shard: &ShardEntry, query_tokens: &[String]) -> bool {
    let Some(sketch) = &shard.sketch else {
        return true;
    };
    query_tokens
        .iter()
        .any(|token| shard_sketch_token_may_diagnose(sketch, token))
}

fn shard_query_identifier_prefilter(
    shard_query: &str,
    query_tokens: &[String],
    filters: &SearchFilters,
) -> Option<String> {
    if filters.match_any || query_tokens.len() <= 1 || !shard_query.contains('_') {
        return None;
    }
    let normalized = normalize_token(shard_query);
    (normalized.chars().count() > SHARD_SUBSTRING_PREFILTER_MAX_TOKEN_CHARS).then_some(normalized)
}

fn shard_sketch_token_may_match(
    sketch: &ShardQuerySketch,
    token: &str,
    filters: &SearchFilters,
    allow_trigram_fallback: bool,
) -> bool {
    let exact = shard_sketch_exact_may_contain(sketch, token);
    if exact {
        return true;
    }
    if filters.symbol_kind.is_some() || !allow_trigram_fallback {
        return false;
    }
    let trigrams = shard_query_trigrams(token);
    !trigrams.is_empty()
        && trigrams
            .iter()
            .all(|trigram| shard_sketch_trigram_may_contain(sketch, trigram))
}

fn shard_sketch_token_may_diagnose(sketch: &ShardQuerySketch, token: &str) -> bool {
    if shard_sketch_exact_may_contain(sketch, token) {
        return true;
    }
    if !shard_allows_substring_prefilter(token) {
        return false;
    }
    let trigrams = shard_query_trigrams(token);
    !trigrams.is_empty()
        && trigrams
            .iter()
            .all(|trigram| shard_sketch_trigram_may_contain(sketch, trigram))
}

fn shard_allows_substring_prefilter(token: &str) -> bool {
    token.chars().count() <= SHARD_SUBSTRING_PREFILTER_MAX_TOKEN_CHARS && !token.contains('_')
}

fn shard_query_substring_grams(query: &str) -> Vec<String> {
    let chars = query
        .chars()
        .flat_map(char::to_lowercase)
        .filter(|ch| ch.is_ascii_alphanumeric() || *ch == '_')
        .collect::<Vec<_>>();
    if chars.len() < SHARD_ROUTE_SUBSTRING_GRAM_CHARS {
        return Vec::new();
    }
    let mut grams = chars
        .windows(SHARD_ROUTE_SUBSTRING_GRAM_CHARS)
        .map(|window| window.iter().collect::<String>())
        .collect::<Vec<_>>();
    grams.sort();
    grams.dedup();
    grams
}

fn push_content_substring_grams(content: &str, substring_bits: &mut [u64]) {
    let mut segment = String::new();
    for ch in content.chars() {
        if ch.is_ascii_alphanumeric() || ch == '_' {
            segment.extend(ch.to_lowercase());
            continue;
        }
        push_segment_substring_grams(&mut segment, substring_bits);
    }
    push_segment_substring_grams(&mut segment, substring_bits);
}

fn push_segment_substring_grams(segment: &mut String, substring_bits: &mut [u64]) {
    if segment.chars().count() >= SHARD_ROUTE_SUBSTRING_GRAM_CHARS {
        for gram in shard_query_substring_grams(segment) {
            sketch_insert(substring_bits, &gram);
        }
    }
    segment.clear();
}

fn push_content_identifier_hashes(content: &str, exact_hashes: &mut Vec<u32>) {
    let mut identifier = String::new();
    for ch in content.chars() {
        if ch.is_ascii_alphanumeric() || ch == '_' {
            identifier.push(ch);
            continue;
        }
        push_identifier_hash(&mut identifier, exact_hashes);
    }
    push_identifier_hash(&mut identifier, exact_hashes);
}

fn push_identifier_hash(identifier: &mut String, exact_hashes: &mut Vec<u32>) {
    if identifier.len() > 1
        && identifier.contains('_')
        && identifier
            .bytes()
            .next()
            .is_some_and(|byte| byte.is_ascii_alphabetic())
    {
        let normalized = normalize_token(identifier);
        if normalized.chars().count() > SHARD_SUBSTRING_PREFILTER_MAX_TOKEN_CHARS {
            exact_hashes.push(sketch_fingerprint(&normalized));
        }
    }
    identifier.clear();
}

fn shard_sketch_exact_may_contain(sketch: &ShardQuerySketch, token: &str) -> bool {
    if !sketch.exact_hashes.is_empty() {
        return sketch
            .exact_hashes
            .binary_search(&sketch_fingerprint(token))
            .is_ok();
    }
    sketch_contains(&sketch.exact_bits, token)
}

fn shard_sketch_trigram_may_contain(sketch: &ShardQuerySketch, trigram: &str) -> bool {
    if !sketch.trigram_hashes.is_empty() {
        return sketch
            .trigram_hashes
            .binary_search(&sketch_fingerprint(trigram))
            .is_ok();
    }
    sketch_contains(&sketch.trigram_bits, trigram)
}

fn shard_sketch_filters_may_match(sketch: &ShardQuerySketch, filters: &SearchFilters) -> bool {
    if let Some(kind) = &filters.symbol_kind {
        if !sketch_contains(&sketch.symbol_kind_bits, kind) {
            return false;
        }
    }
    for (field, value) in [
        ("language", filters.language.as_deref()),
        ("extension", filters.extension.as_deref()),
    ] {
        if let Some(value) = value {
            if !sketch_contains(&sketch.filter_bits, &format!("{field}:{value}")) {
                return false;
            }
        }
    }
    for (field, value) in [
        ("test", filters.test),
        ("generated", filters.generated),
        ("code", filters.code),
    ] {
        if let Some(value) = value {
            if !sketch_contains(&sketch.filter_bits, &format!("{field}:{value}")) {
                return false;
            }
        }
    }
    true
}

fn sketch_insert(words: &mut [u64], value: &str) {
    for salt in 0..3 {
        let bit = sketch_bit(words.len(), value, salt);
        words[bit / 64] |= 1u64 << (bit % 64);
    }
}

fn sketch_contains(words: &[u64], value: &str) -> bool {
    !words.is_empty()
        && (0..3).all(|salt| {
            let bit = sketch_bit(words.len(), value, salt);
            (words[bit / 64] & (1u64 << (bit % 64))) != 0
        })
}

fn sketch_bit(words: usize, value: &str, salt: u64) -> usize {
    (sketch_hash(value, salt) as usize) % (words * 64)
}

fn sketch_fingerprint(value: &str) -> u32 {
    let hash = sketch_hash(value, 0x517c_c1b7);
    ((hash >> 32) as u32) ^ (hash as u32)
}

fn sketch_hash(value: &str, salt: u64) -> u64 {
    let mut hash = 0xcbf29ce484222325u64 ^ salt.wrapping_mul(0x9e3779b97f4a7c15);
    for byte in value.as_bytes() {
        hash ^= *byte as u64;
        hash = hash.wrapping_mul(0x100000001b3);
    }
    hash
}

fn shard_query_trigrams(query: &str) -> Vec<String> {
    let mut trigrams = query
        .chars()
        .flat_map(char::to_lowercase)
        .filter(|ch| ch.is_ascii_alphanumeric() || *ch == '_')
        .collect::<Vec<_>>()
        .windows(3)
        .map(|window| window.iter().collect::<String>())
        .collect::<Vec<_>>();
    trigrams.sort();
    trigrams.dedup();
    trigrams
}

pub(crate) fn load_manifest(index_dir: &Path) -> Result<ShardManifest> {
    let manifest_path = index_dir.join(SHARD_MANIFEST_FILE);
    let fingerprint = manifest_file_fingerprint(&manifest_path).ok();
    if let Some(manifest) = load_manifest_sidecar(index_dir, fingerprint)? {
        return Ok(manifest);
    }

    let bytes = fs::read(&manifest_path)
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

pub(crate) fn shard_prefilter_query_impossible(
    index_dir: &Path,
    shard_query: &str,
    filters: &SearchFilters,
) -> Result<bool> {
    let required_hashes = shard_prefilter_required_exact_hashes(shard_query, filters);
    if required_hashes.is_empty() {
        return Ok(false);
    }
    let Some(prefilter) = load_manifest_prefilter(index_dir)? else {
        return Ok(false);
    };
    Ok(required_hashes
        .iter()
        .any(|hash| prefilter.exact_hashes.binary_search(hash).is_err()))
}

pub(crate) fn shard_route_entries(
    index_dir: &Path,
    shard_query: &str,
    filters: &SearchFilters,
) -> Result<Option<Vec<ShardEntry>>> {
    let requirements = shard_route_requirements(shard_query, filters);
    if requirements.exact_hashes.is_empty() && requirements.trigram_hashes.is_empty() {
        return Ok(None);
    }
    let Some(route) = load_manifest_route(index_dir)? else {
        return Ok(None);
    };
    let candidate_ids = match shard_route_candidate_ids(&route, &requirements) {
        ShardRouteLookup::Candidates(candidate_ids) => candidate_ids,
        ShardRouteLookup::MissingHash => return Ok(Some(Vec::new())),
        ShardRouteLookup::Omitted => return Ok(None),
        ShardRouteLookup::Corrupt => return Ok(None),
    };
    let shards = candidate_ids
        .into_iter()
        .filter_map(|id| route.shards.get(id as usize).cloned())
        .map(ShardRouteEntry::into_shard)
        .collect::<Vec<_>>();
    Ok(Some(shards))
}

fn shard_entries_to_jobs(shards: Vec<ShardEntry>, filters: &SearchFilters) -> Vec<ShardJob> {
    shards
        .into_iter()
        .filter_map(|shard| {
            let scopes = shard_search_scopes(&shard, filters);
            (!scopes.is_empty()).then_some(ShardJob { shard, scopes })
        })
        .collect()
}

fn shard_route_candidate_ids(
    route: &ShardManifestRoute,
    requirements: &ShardRouteRequirements,
) -> ShardRouteLookup {
    let mut candidate_ids: Option<Vec<u16>> = None;
    let mut saw_omitted = false;
    for (terms, omitted_hashes, required_hashes) in [
        (
            route.exact_terms.as_slice(),
            route.omitted_hashes.as_slice(),
            requirements.exact_hashes.as_slice(),
        ),
        (
            route.trigram_terms.as_slice(),
            route.omitted_trigram_hashes.as_slice(),
            requirements.trigram_hashes.as_slice(),
        ),
    ] {
        for hash in required_hashes {
            let postings = match shard_route_postings(route, terms, *hash) {
                Ok(Some(postings)) => postings,
                Ok(None) if omitted_hashes.binary_search(hash).is_ok() => {
                    saw_omitted = true;
                    continue;
                }
                Ok(None) => return ShardRouteLookup::MissingHash,
                Err(()) => return ShardRouteLookup::Corrupt,
            };
            candidate_ids = Some(match candidate_ids {
                Some(existing) => intersect_u16_sorted(&existing, &postings),
                None => postings,
            });
            if candidate_ids.as_ref().is_some_and(Vec::is_empty) {
                break;
            }
        }
        if candidate_ids.as_ref().is_some_and(Vec::is_empty) {
            break;
        }
    }
    if !requirements.substring_grams.is_empty() {
        let ids = candidate_ids
            .take()
            .unwrap_or_else(|| (0..route.shards.len()).map(|id| id as u16).collect());
        candidate_ids = Some(
            ids.into_iter()
                .filter(|id| {
                    route
                        .shards
                        .get(*id as usize)
                        .is_some_and(|shard| shard_route_substrings_may_match(shard, requirements))
                })
                .collect(),
        );
    }
    match candidate_ids {
        Some(candidate_ids) => ShardRouteLookup::Candidates(candidate_ids),
        None if saw_omitted => ShardRouteLookup::Omitted,
        None => ShardRouteLookup::Candidates(Vec::new()),
    }
}

fn shard_route_substrings_may_match(
    shard: &ShardRouteEntry,
    requirements: &ShardRouteRequirements,
) -> bool {
    !shard.substring_bits.is_empty()
        && requirements
            .substring_grams
            .iter()
            .all(|gram| sketch_contains(&shard.substring_bits, gram))
}

fn shard_route_postings(
    route: &ShardManifestRoute,
    terms: &[ShardRouteTerm],
    hash: u32,
) -> Result<Option<Vec<u16>>, ()> {
    let index = terms.binary_search_by_key(&hash, |term| term.hash).ok();
    let Some(index) = index else {
        return Ok(None);
    };
    let term = terms[index];
    let start = term.start as usize;
    if start > route.shard_ids.len() {
        return Err(());
    }
    decode_route_shard_ids(&route.shard_ids[start..], term.len as usize)
        .map(Some)
        .ok_or(())
}

fn shard_route_requirements(shard_query: &str, filters: &SearchFilters) -> ShardRouteRequirements {
    let exact_hashes = shard_prefilter_required_exact_hashes(shard_query, filters);
    let query_tokens = unique_query_tokens(shard_query);
    let mut trigram_hashes = Vec::new();
    if filters.symbol_kind.is_none()
        && query_tokens.len() == 1
        && shard_allows_substring_prefilter(&query_tokens[0])
    {
        trigram_hashes.extend(
            shard_query_trigrams(&query_tokens[0])
                .into_iter()
                .map(|trigram| sketch_fingerprint(&trigram)),
        );
    }
    let substring_grams = if filters.symbol_kind.is_none()
        && query_tokens.len() == 1
        && shard_allows_substring_prefilter(&query_tokens[0])
    {
        shard_query_substring_grams(&query_tokens[0])
    } else {
        Vec::new()
    };
    trigram_hashes.sort_unstable();
    trigram_hashes.dedup();
    ShardRouteRequirements {
        exact_hashes,
        trigram_hashes,
        substring_grams,
    }
}

fn intersect_u16_sorted(left: &[u16], right: &[u16]) -> Vec<u16> {
    let mut out = Vec::new();
    let mut left_index = 0usize;
    let mut right_index = 0usize;
    while let (Some(left_value), Some(right_value)) = (left.get(left_index), right.get(right_index))
    {
        match left_value.cmp(right_value) {
            std::cmp::Ordering::Less => left_index += 1,
            std::cmp::Ordering::Greater => right_index += 1,
            std::cmp::Ordering::Equal => {
                out.push(*left_value);
                left_index += 1;
                right_index += 1;
            }
        }
    }
    out
}

fn encode_route_shard_ids(ids: &[u16], bytes: &mut Vec<u8>) {
    let mut previous = 0u16;
    for (index, id) in ids.iter().copied().enumerate() {
        let delta = if index == 0 {
            id
        } else {
            id.saturating_sub(previous)
        };
        encode_var_u32(delta as u32, bytes);
        previous = id;
    }
}

fn decode_route_shard_ids(bytes: &[u8], len: usize) -> Option<Vec<u16>> {
    let mut ids = Vec::with_capacity(len);
    let mut offset = 0usize;
    let mut previous = 0u16;
    for index in 0..len {
        let delta = decode_var_u32(bytes, &mut offset)?;
        let value = if index == 0 {
            u16::try_from(delta).ok()?
        } else {
            let delta = u16::try_from(delta).ok()?;
            previous.checked_add(delta)?
        };
        if index > 0 && value <= previous {
            return None;
        }
        ids.push(value);
        previous = value;
    }
    Some(ids)
}

fn encode_var_u32(mut value: u32, bytes: &mut Vec<u8>) {
    while value >= 0x80 {
        bytes.push((value as u8) | 0x80);
        value >>= 7;
    }
    bytes.push(value as u8);
}

fn decode_var_u32(bytes: &[u8], offset: &mut usize) -> Option<u32> {
    let mut value = 0u32;
    let mut shift = 0u32;
    loop {
        let byte = *bytes.get(*offset)?;
        *offset += 1;
        value |= ((byte & 0x7f) as u32) << shift;
        if byte & 0x80 == 0 {
            return Some(value);
        }
        shift += 7;
        if shift >= 32 {
            return None;
        }
    }
}

fn shard_prefilter_required_exact_hashes(shard_query: &str, filters: &SearchFilters) -> Vec<u32> {
    let query_tokens = unique_query_tokens(shard_query);
    let mut hashes = Vec::new();
    if let Some(identifier) = shard_query_identifier_prefilter(shard_query, &query_tokens, filters)
    {
        hashes.push(sketch_fingerprint(&identifier));
    }

    let require_all = filters.require_all || (query_tokens.len() > 1 && !filters.match_any);
    if require_all || filters.symbol_kind.is_some() || query_tokens.len() == 1 {
        hashes.extend(
            query_tokens
                .iter()
                .filter(|token| {
                    filters.symbol_kind.is_some() || !shard_allows_substring_prefilter(token)
                })
                .map(|token| sketch_fingerprint(token)),
        );
    }
    hashes.sort_unstable();
    hashes.dedup();
    hashes
}

fn load_manifest_prefilter(index_dir: &Path) -> Result<Option<ShardManifestPrefilter>> {
    let json_fingerprint = manifest_file_fingerprint(&index_dir.join(SHARD_MANIFEST_FILE)).ok();
    let Some(json_fingerprint) = json_fingerprint else {
        return Ok(None);
    };
    let prefilter_path = index_dir.join(SHARD_MANIFEST_PREFILTER_FILE);
    let bytes = match fs::read(&prefilter_path) {
        Ok(bytes) => bytes,
        Err(error) if error.kind() == ErrorKind::NotFound => return Ok(None),
        Err(_) => return Ok(None),
    };
    let prefilter = match bincode::deserialize::<ShardManifestPrefilter>(&bytes) {
        Ok(prefilter) => prefilter,
        Err(_) => return Ok(None),
    };
    if prefilter.version != SHARD_MANIFEST_PREFILTER_VERSION
        || prefilter.json_fingerprint != json_fingerprint
    {
        return Ok(None);
    }
    Ok(Some(prefilter))
}

fn load_manifest_route(index_dir: &Path) -> Result<Option<ShardManifestRoute>> {
    let json_fingerprint = manifest_file_fingerprint(&index_dir.join(SHARD_MANIFEST_FILE)).ok();
    let Some(json_fingerprint) = json_fingerprint else {
        return Ok(None);
    };
    let route_path = index_dir.join(SHARD_MANIFEST_ROUTE_FILE);
    let bytes = match fs::read(&route_path) {
        Ok(bytes) => bytes,
        Err(error) if error.kind() == ErrorKind::NotFound => return Ok(None),
        Err(_) => return Ok(None),
    };
    let route = match bincode::deserialize::<ShardManifestRoute>(&bytes) {
        Ok(route) => route,
        Err(_) => return Ok(None),
    };
    if route.version != SHARD_MANIFEST_ROUTE_VERSION || route.json_fingerprint != json_fingerprint {
        return Ok(None);
    }
    Ok(Some(route))
}

fn load_manifest_sidecar(
    index_dir: &Path,
    json_fingerprint: Option<ManifestFileFingerprint>,
) -> Result<Option<ShardManifest>> {
    let Some(json_fingerprint) = json_fingerprint else {
        return Ok(None);
    };
    let sidecar_path = index_dir.join(SHARD_MANIFEST_SIDECAR_FILE);
    let bytes = match fs::read(&sidecar_path) {
        Ok(bytes) => bytes,
        Err(error) if error.kind() == ErrorKind::NotFound => return Ok(None),
        Err(_) => return Ok(None),
    };
    let sidecar = match bincode::deserialize::<ShardManifestSidecar>(&bytes) {
        Ok(sidecar) => sidecar,
        Err(_) => return Ok(None),
    };
    if sidecar.version != SHARD_MANIFEST_SIDECAR_VERSION
        || sidecar.json_fingerprint != json_fingerprint
    {
        return Ok(None);
    }
    let manifest = sidecar.manifest.into_manifest();
    if manifest.version != SHARD_MANIFEST_VERSION || validate_manifest(&manifest).is_err() {
        return Ok(None);
    }
    Ok(Some(manifest))
}

fn manifest_file_fingerprint(path: &Path) -> Result<ManifestFileFingerprint> {
    let metadata =
        fs::metadata(path).with_context(|| format!("stat shard manifest {}", path.display()))?;
    let modified = metadata
        .modified()
        .unwrap_or(SystemTime::UNIX_EPOCH)
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default();
    Ok(ManifestFileFingerprint {
        len: metadata.len(),
        modified_secs: modified.as_secs(),
        modified_nanos: modified.subsec_nanos(),
    })
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
    }) || filters
        .exclude_branch
        .iter()
        .any(|filter| shard_git_branch_matches(shard, filter))
        || filters
            .exclude_origin
            .iter()
            .any(|filter| shard_git_origin_matches(shard, filter))
    {
        return Vec::new();
    }

    if filters
        .branch
        .as_deref()
        .is_some_and(|filter| !shard_git_branch_matches(shard, filter))
        || filters
            .origin
            .as_deref()
            .is_some_and(|filter| !shard_git_origin_matches(shard, filter))
    {
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
    filters.branch = None;
    filters.origin = None;
    filters.exclude_repo.clear();
    filters.exclude_branch.clear();
    filters.exclude_origin.clear();
    if let Some(prefix) = path_prefix {
        if filters.path.is_none() {
            filters.path = Some(prefix.trim_end_matches('/').to_string());
        }
    }
    filters
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ShardManifestWriteMode {
    PreserveExisting,
    AllowShrink,
}

fn save_manifest(index_dir: &Path, manifest: &ShardManifest) -> Result<()> {
    save_manifest_with_mode(
        index_dir,
        manifest,
        ShardManifestWriteMode::PreserveExisting,
    )
}

fn save_manifest_with_mode(
    index_dir: &Path,
    manifest: &ShardManifest,
    mode: ShardManifestWriteMode,
) -> Result<()> {
    guard_manifest_write_preserves_existing_roots(index_dir, manifest, mode)?;
    let manifest_path = index_dir.join(SHARD_MANIFEST_FILE);
    let json_manifest = slim_manifest_for_json(manifest);
    let bytes = serde_json::to_vec_pretty(&json_manifest)?;
    atomic_write(&manifest_path, &bytes)
        .with_context(|| format!("write shard manifest {}", index_dir.display()))?;
    save_manifest_sidecar(index_dir, manifest)?;
    save_manifest_prefilter(index_dir, manifest)?;
    save_manifest_route(index_dir, manifest)
}

fn slim_manifest_for_json(manifest: &ShardManifest) -> ShardManifest {
    let mut manifest = manifest.clone();
    for shard in &mut manifest.shards {
        shard.sketch = None;
    }
    manifest
}

fn save_manifest_sidecar(index_dir: &Path, manifest: &ShardManifest) -> Result<()> {
    let manifest_path = index_dir.join(SHARD_MANIFEST_FILE);
    let sidecar = ShardManifestSidecar {
        version: SHARD_MANIFEST_SIDECAR_VERSION,
        json_fingerprint: manifest_file_fingerprint(&manifest_path)?,
        manifest: ShardManifestSidecarData::from_manifest(manifest),
    };
    let bytes = bincode::serialize(&sidecar)?;
    atomic_write(&index_dir.join(SHARD_MANIFEST_SIDECAR_FILE), &bytes)
        .with_context(|| format!("write shard manifest sidecar {}", index_dir.display()))
}

fn save_manifest_prefilter(index_dir: &Path, manifest: &ShardManifest) -> Result<()> {
    let manifest_path = index_dir.join(SHARD_MANIFEST_FILE);
    let mut exact_hashes = manifest
        .shards
        .iter()
        .filter_map(|shard| shard.sketch.as_ref())
        .flat_map(|sketch| sketch.exact_hashes.iter().copied())
        .collect::<Vec<_>>();
    exact_hashes.sort_unstable();
    exact_hashes.dedup();
    let prefilter = ShardManifestPrefilter {
        version: SHARD_MANIFEST_PREFILTER_VERSION,
        json_fingerprint: manifest_file_fingerprint(&manifest_path)?,
        exact_hashes,
    };
    let bytes = bincode::serialize(&prefilter)?;
    atomic_write(&index_dir.join(SHARD_MANIFEST_PREFILTER_FILE), &bytes)
        .with_context(|| format!("write shard manifest prefilter {}", index_dir.display()))
}

fn save_manifest_route(index_dir: &Path, manifest: &ShardManifest) -> Result<()> {
    anyhow::ensure!(
        manifest.shards.len() <= u16::MAX as usize,
        "shard route supports at most {} shards",
        u16::MAX
    );
    let manifest_path = index_dir.join(SHARD_MANIFEST_FILE);
    let mut postings = HashMap::<u32, Vec<u16>>::new();
    let mut trigram_postings = HashMap::<u32, Vec<u16>>::new();
    let shards = manifest
        .shards
        .iter()
        .enumerate()
        .map(|(shard_id, shard)| {
            if let Some(sketch) = &shard.sketch {
                for hash in &sketch.exact_hashes {
                    postings.entry(*hash).or_default().push(shard_id as u16);
                }
                for hash in &sketch.trigram_hashes {
                    trigram_postings
                        .entry(*hash)
                        .or_default()
                        .push(shard_id as u16);
                }
            }
            ShardRouteEntry::from_shard(shard)
        })
        .collect::<Vec<_>>();

    let mut terms = postings.into_iter().collect::<Vec<_>>();
    terms.sort_unstable_by_key(|(hash, _)| *hash);
    let mut exact_terms = Vec::with_capacity(terms.len());
    let mut omitted_hashes = Vec::new();
    let mut shard_ids = Vec::new();
    encode_route_terms(terms, &mut exact_terms, &mut omitted_hashes, &mut shard_ids);

    let mut trigram_terms_input = trigram_postings.into_iter().collect::<Vec<_>>();
    trigram_terms_input.sort_unstable_by_key(|(hash, _)| *hash);
    let mut trigram_terms = Vec::with_capacity(trigram_terms_input.len());
    let mut omitted_trigram_hashes = Vec::new();
    encode_route_terms(
        trigram_terms_input,
        &mut trigram_terms,
        &mut omitted_trigram_hashes,
        &mut shard_ids,
    );

    let route = ShardManifestRoute {
        version: SHARD_MANIFEST_ROUTE_VERSION,
        json_fingerprint: manifest_file_fingerprint(&manifest_path)?,
        shards,
        exact_terms,
        trigram_terms,
        omitted_hashes,
        omitted_trigram_hashes,
        shard_ids,
    };
    let bytes = bincode::serialize(&route)?;
    atomic_write(&index_dir.join(SHARD_MANIFEST_ROUTE_FILE), &bytes)
        .with_context(|| format!("write shard manifest route {}", index_dir.display()))
}

fn encode_route_terms(
    terms: Vec<(u32, Vec<u16>)>,
    route_terms: &mut Vec<ShardRouteTerm>,
    omitted_hashes: &mut Vec<u32>,
    shard_ids: &mut Vec<u8>,
) {
    for (hash, mut ids) in terms {
        ids.sort_unstable();
        ids.dedup();
        if ids.len() > SHARD_ROUTE_MAX_POSTING_SHARDS {
            omitted_hashes.push(hash);
            continue;
        }
        let start = shard_ids.len() as u32;
        let len = ids.len() as u16;
        encode_route_shard_ids(&ids, shard_ids);
        route_terms.push(ShardRouteTerm { hash, start, len });
    }
    omitted_hashes.sort_unstable();
}

fn guard_manifest_write_preserves_existing_roots(
    index_dir: &Path,
    next: &ShardManifest,
    mode: ShardManifestWriteMode,
) -> Result<()> {
    if mode == ShardManifestWriteMode::AllowShrink {
        return Ok(());
    }
    let manifest_path = index_dir.join(SHARD_MANIFEST_FILE);
    if !manifest_path.exists() {
        return Ok(());
    }
    let previous = load_manifest(index_dir)?;
    let next_roots = next
        .shards
        .iter()
        .map(|shard| canonical_or_self(&shard.root))
        .collect::<HashSet<_>>();
    let omitted = previous
        .shards
        .iter()
        .filter(|shard| !next_roots.contains(&canonical_or_self(&shard.root)))
        .count();
    anyhow::ensure!(
        omitted == 0,
        "refusing to write shard manifest {} because it would remove {} existing shard root(s); use refresh-shards to prune missing roots or index-shards --force to replace the shard directory",
        manifest_path.display(),
        omitted,
    );
    Ok(())
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

fn shard_git_branch_matches(shard: &ShardEntry, filter: &str) -> bool {
    shard.git.as_ref().is_some_and(|git| {
        git.branch
            .as_deref()
            .is_some_and(|value| metadata_filter_matches(value, filter))
    })
}

fn shard_git_origin_matches(shard: &ShardEntry, filter: &str) -> bool {
    shard.git.as_ref().is_some_and(|git| {
        git.origin
            .as_deref()
            .is_some_and(|value| metadata_filter_matches(value, filter))
    })
}

fn metadata_filter_matches(value: &str, filter: &str) -> bool {
    value
        .to_ascii_lowercase()
        .contains(&filter.to_ascii_lowercase())
}

fn add_stats(total: &mut ShardBuildStats, stats: &IndexStats) {
    total.files += stats.files;
    total.source_bytes += stats.source_bytes;
    total.content_snapshot_bytes += stats.content_snapshot_bytes;
    total.line_offset_bytes += stats.line_offset_bytes;
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
    total.content_snapshot_bytes += stats.content_snapshot_bytes;
    total.line_offset_bytes += stats.line_offset_bytes;
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
    total.content_snapshot_bytes += stats.content_snapshot_bytes;
    total.line_offset_bytes += stats.line_offset_bytes;
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

#[cfg(test)]
mod tests {
    use super::*;

    fn test_manifest(root_a: &Path, root_b: Option<&Path>) -> ShardManifest {
        let mut shards = vec![ShardEntry {
            name: "auth".to_string(),
            root: root_a.to_path_buf(),
            index: "auth.orient".to_string(),
            aliases: Vec::new(),
            git: None,
            sketch: None,
        }];
        if let Some(root_b) = root_b {
            shards.push(ShardEntry {
                name: "billing".to_string(),
                root: root_b.to_path_buf(),
                index: "billing.orient".to_string(),
                aliases: Vec::new(),
                git: None,
                sketch: None,
            });
        }
        ShardManifest {
            version: SHARD_MANIFEST_VERSION,
            shards,
        }
    }

    fn exact_route_requirements(hashes: &[u32]) -> ShardRouteRequirements {
        ShardRouteRequirements {
            exact_hashes: hashes.to_vec(),
            trigram_hashes: Vec::new(),
            substring_grams: Vec::new(),
        }
    }

    fn trigram_route_requirements(hashes: &[u32]) -> ShardRouteRequirements {
        ShardRouteRequirements {
            exact_hashes: Vec::new(),
            trigram_hashes: hashes.to_vec(),
            substring_grams: Vec::new(),
        }
    }

    fn substring_route_requirements(value: &str) -> ShardRouteRequirements {
        ShardRouteRequirements {
            exact_hashes: Vec::new(),
            trigram_hashes: Vec::new(),
            substring_grams: shard_query_substring_grams(value),
        }
    }

    fn substring_bits_for(value: &str) -> Vec<u64> {
        let mut bits = vec![0; SHARD_SUBSTRING_SKETCH_WORDS];
        push_content_substring_grams(value, &mut bits);
        bits
    }

    #[test]
    fn manifest_writer_refuses_unexpected_shrink_without_explicit_mode() {
        let dir = tempfile::tempdir().unwrap();
        let auth = dir.path().join("auth");
        let billing = dir.path().join("billing");
        fs::create_dir_all(&auth).unwrap();
        fs::create_dir_all(&billing).unwrap();

        let full = test_manifest(&auth, Some(&billing));
        save_manifest(dir.path(), &full).unwrap();

        let shrink = test_manifest(&auth, None);
        let error = save_manifest(dir.path(), &shrink).unwrap_err().to_string();
        assert!(
            error.contains("refusing to write shard manifest"),
            "{error}"
        );
        assert_eq!(load_manifest(dir.path()).unwrap().shards.len(), 2);

        save_manifest_with_mode(dir.path(), &shrink, ShardManifestWriteMode::AllowShrink).unwrap();
        assert_eq!(load_manifest(dir.path()).unwrap().shards.len(), 1);
    }

    #[test]
    fn manifest_sidecar_loads_when_current_and_falls_back_when_invalid() {
        let dir = tempfile::tempdir().unwrap();
        let auth = dir.path().join("auth");
        fs::create_dir_all(&auth).unwrap();

        let manifest = test_manifest(&auth, None);
        save_manifest(dir.path(), &manifest).unwrap();
        let sidecar_path = dir.path().join(SHARD_MANIFEST_SIDECAR_FILE);
        assert!(sidecar_path.exists());
        assert_eq!(load_manifest(dir.path()).unwrap(), manifest);

        fs::write(&sidecar_path, b"not bincode").unwrap();
        assert_eq!(load_manifest(dir.path()).unwrap(), manifest);
    }

    #[test]
    fn manifest_json_omits_heavy_sketches_but_sidecar_preserves_them() {
        let dir = tempfile::tempdir().unwrap();
        let auth = dir.path().join("auth");
        fs::create_dir_all(&auth).unwrap();

        let mut manifest = test_manifest(&auth, None);
        manifest.shards[0].sketch = Some(ShardQuerySketch {
            exact_hashes: vec![sketch_fingerprint("routeprobe")],
            trigram_hashes: vec![sketch_fingerprint("rou")],
            exact_bits: Vec::new(),
            trigram_bits: Vec::new(),
            substring_bits: substring_bits_for("routeprobe"),
            symbol_kind_bits: vec![1],
            filter_bits: vec![2],
        });
        save_manifest(dir.path(), &manifest).unwrap();

        let manifest_json = fs::read_to_string(dir.path().join(SHARD_MANIFEST_FILE)).unwrap();
        assert!(!manifest_json.contains("\"sketch\""), "{manifest_json}");
        let sidecar = bincode::deserialize::<ShardManifestSidecar>(
            &fs::read(dir.path().join(SHARD_MANIFEST_SIDECAR_FILE)).unwrap(),
        )
        .unwrap();
        assert_eq!(sidecar.manifest.clone().into_manifest(), manifest);
        assert_eq!(
            sidecar.json_fingerprint,
            manifest_file_fingerprint(&dir.path().join(SHARD_MANIFEST_FILE)).unwrap()
        );
        assert_eq!(load_manifest(dir.path()).unwrap(), manifest);

        fs::write(dir.path().join(SHARD_MANIFEST_SIDECAR_FILE), b"not bincode").unwrap();
        let fallback = load_manifest(dir.path()).unwrap();
        assert_eq!(fallback, slim_manifest_for_json(&manifest));
        assert!(fallback.shards[0].sketch.is_none());
    }

    #[test]
    fn manifest_sidecar_is_ignored_when_json_fingerprint_changes() {
        let dir = tempfile::tempdir().unwrap();
        let auth = dir.path().join("auth");
        let billing = dir.path().join("billing");
        fs::create_dir_all(&auth).unwrap();
        fs::create_dir_all(&billing).unwrap();

        let full = test_manifest(&auth, Some(&billing));
        save_manifest(dir.path(), &full).unwrap();
        let stale_sidecar = fs::read(dir.path().join(SHARD_MANIFEST_SIDECAR_FILE)).unwrap();

        let shrink = test_manifest(&auth, None);
        save_manifest_with_mode(dir.path(), &shrink, ShardManifestWriteMode::AllowShrink).unwrap();
        fs::write(dir.path().join(SHARD_MANIFEST_SIDECAR_FILE), stale_sidecar).unwrap();

        assert_eq!(load_manifest(dir.path()).unwrap(), shrink);
    }

    #[test]
    fn manifest_sidecar_is_ignored_when_current_but_invalid() {
        let dir = tempfile::tempdir().unwrap();
        let auth = dir.path().join("auth");
        fs::create_dir_all(&auth).unwrap();

        let manifest = test_manifest(&auth, None);
        save_manifest(dir.path(), &manifest).unwrap();
        let mut invalid_manifest = manifest.clone();
        invalid_manifest.version = SHARD_MANIFEST_VERSION + 1;
        let sidecar = ShardManifestSidecar {
            version: SHARD_MANIFEST_SIDECAR_VERSION,
            json_fingerprint: manifest_file_fingerprint(&dir.path().join(SHARD_MANIFEST_FILE))
                .unwrap(),
            manifest: ShardManifestSidecarData::from_manifest(&invalid_manifest),
        };
        fs::write(
            dir.path().join(SHARD_MANIFEST_SIDECAR_FILE),
            bincode::serialize(&sidecar).unwrap(),
        )
        .unwrap();

        assert_eq!(load_manifest(dir.path()).unwrap(), manifest);
    }

    #[test]
    fn manifest_route_round_trips() {
        let dir = tempfile::tempdir().unwrap();
        let auth = dir.path().join("auth");
        let billing = dir.path().join("billing");
        fs::create_dir_all(&auth).unwrap();
        fs::create_dir_all(&billing).unwrap();

        let mut manifest = test_manifest(&auth, Some(&billing));
        manifest.shards[0].sketch = Some(ShardQuerySketch {
            exact_hashes: vec![
                sketch_fingerprint("routeprobe"),
                sketch_fingerprint("sharedrouteprobe"),
            ],
            trigram_hashes: Vec::new(),
            exact_bits: Vec::new(),
            trigram_bits: Vec::new(),
            substring_bits: Vec::new(),
            symbol_kind_bits: Vec::new(),
            filter_bits: Vec::new(),
        });
        manifest.shards[1].sketch = Some(ShardQuerySketch {
            exact_hashes: vec![sketch_fingerprint("sharedrouteprobe")],
            trigram_hashes: Vec::new(),
            exact_bits: Vec::new(),
            trigram_bits: Vec::new(),
            substring_bits: Vec::new(),
            symbol_kind_bits: Vec::new(),
            filter_bits: Vec::new(),
        });
        save_manifest(dir.path(), &manifest).unwrap();

        let route = load_manifest_route(dir.path()).unwrap().unwrap();
        assert_eq!(route.shards.len(), 2);
        assert_eq!(
            shard_route_candidate_ids(
                &route,
                &exact_route_requirements(&[sketch_fingerprint("routeprobe")])
            ),
            ShardRouteLookup::Candidates(vec![0])
        );
        assert_eq!(
            shard_route_candidate_ids(
                &route,
                &exact_route_requirements(&[sketch_fingerprint("sharedrouteprobe")])
            ),
            ShardRouteLookup::Candidates(vec![0, 1])
        );
        assert!(route.shard_ids.len() < 4);

        let missing = shard_route_candidate_ids(
            &route,
            &exact_route_requirements(&[sketch_fingerprint("missingrouteprobe")]),
        );
        assert_eq!(missing, ShardRouteLookup::MissingHash);

        let mut corrupt = route.clone();
        let routeprobe_index = corrupt
            .exact_terms
            .binary_search_by_key(&sketch_fingerprint("routeprobe"), |term| term.hash)
            .unwrap();
        corrupt.exact_terms[routeprobe_index].start = corrupt.shard_ids.len() as u32 + 1;
        assert_eq!(
            shard_route_candidate_ids(
                &corrupt,
                &exact_route_requirements(&[sketch_fingerprint("routeprobe")])
            ),
            ShardRouteLookup::Corrupt
        );
    }

    #[test]
    fn manifest_route_omits_broad_terms_for_manifest_fallback() {
        let dir = tempfile::tempdir().unwrap();
        let broad_hash = sketch_fingerprint("broadrouteprobe");
        let narrow_hash = sketch_fingerprint("narrowrouteprobe");
        let shards = (0..=SHARD_ROUTE_MAX_POSTING_SHARDS)
            .map(|index| ShardEntry {
                name: format!("repo-{index}"),
                root: dir.path().join(format!("repo-{index}")),
                index: format!("repo-{index}.orient"),
                aliases: Vec::new(),
                git: None,
                sketch: Some(ShardQuerySketch {
                    exact_hashes: if index == 0 {
                        vec![broad_hash, narrow_hash]
                    } else {
                        vec![broad_hash]
                    },
                    trigram_hashes: Vec::new(),
                    exact_bits: Vec::new(),
                    trigram_bits: Vec::new(),
                    substring_bits: Vec::new(),
                    symbol_kind_bits: Vec::new(),
                    filter_bits: Vec::new(),
                }),
            })
            .collect();
        let manifest = ShardManifest {
            version: SHARD_MANIFEST_VERSION,
            shards,
        };
        save_manifest(dir.path(), &manifest).unwrap();

        let route = load_manifest_route(dir.path()).unwrap().unwrap();
        assert!(route.omitted_hashes.binary_search(&broad_hash).is_ok());
        assert!(
            route
                .exact_terms
                .binary_search_by_key(&broad_hash, |term| term.hash)
                .is_err()
        );
        assert_eq!(
            shard_route_candidate_ids(&route, &exact_route_requirements(&[broad_hash])),
            ShardRouteLookup::Omitted
        );
        assert_eq!(
            shard_route_candidate_ids(
                &route,
                &exact_route_requirements(&[broad_hash, narrow_hash])
            ),
            ShardRouteLookup::Candidates(vec![0])
        );
    }

    #[test]
    fn manifest_route_uses_trigrams_for_substring_queries() {
        let dir = tempfile::tempdir().unwrap();
        let mut needle_hashes = shard_query_trigrams("needle")
            .into_iter()
            .map(|trigram| sketch_fingerprint(&trigram))
            .collect::<Vec<_>>();
        needle_hashes.sort_unstable();
        let mut needle_hashes = needle_hashes;
        needle_hashes.sort_unstable();
        let other_hashes = shard_query_trigrams("other")
            .into_iter()
            .map(|trigram| sketch_fingerprint(&trigram))
            .collect::<Vec<_>>();
        let mut manifest =
            test_manifest(&dir.path().join("needle"), Some(&dir.path().join("other")));
        manifest.shards[0].sketch = Some(ShardQuerySketch {
            exact_hashes: Vec::new(),
            trigram_hashes: needle_hashes.clone(),
            exact_bits: Vec::new(),
            trigram_bits: Vec::new(),
            substring_bits: Vec::new(),
            symbol_kind_bits: Vec::new(),
            filter_bits: Vec::new(),
        });
        manifest.shards[1].sketch = Some(ShardQuerySketch {
            exact_hashes: Vec::new(),
            trigram_hashes: other_hashes,
            exact_bits: Vec::new(),
            trigram_bits: Vec::new(),
            substring_bits: Vec::new(),
            symbol_kind_bits: Vec::new(),
            filter_bits: Vec::new(),
        });
        save_manifest(dir.path(), &manifest).unwrap();

        let route = load_manifest_route(dir.path()).unwrap().unwrap();
        assert!(!route.trigram_terms.is_empty());
        assert_eq!(
            shard_route_requirements("needle", &SearchFilters::default()).trigram_hashes,
            needle_hashes
        );
        assert_eq!(
            shard_route_candidate_ids(&route, &trigram_route_requirements(&needle_hashes)),
            ShardRouteLookup::Candidates(vec![0])
        );
    }

    #[test]
    fn manifest_route_uses_long_substring_bits_to_prune_trigram_false_positives() {
        let dir = tempfile::tempdir().unwrap();
        let mut manifest = test_manifest(&dir.path().join("hit"), Some(&dir.path().join("miss")));
        let shared_trigrams = shard_query_trigrams("trigramprobe")
            .into_iter()
            .map(|trigram| sketch_fingerprint(&trigram))
            .collect::<Vec<_>>();
        manifest.shards[0].sketch = Some(ShardQuerySketch {
            exact_hashes: Vec::new(),
            trigram_hashes: shared_trigrams.clone(),
            exact_bits: Vec::new(),
            trigram_bits: Vec::new(),
            substring_bits: substring_bits_for("prefix_trigramprobesuffix"),
            symbol_kind_bits: Vec::new(),
            filter_bits: Vec::new(),
        });
        manifest.shards[1].sketch = Some(ShardQuerySketch {
            exact_hashes: Vec::new(),
            trigram_hashes: shared_trigrams.clone(),
            exact_bits: Vec::new(),
            trigram_bits: Vec::new(),
            substring_bits: substring_bits_for("trigram and probe appear apart"),
            symbol_kind_bits: Vec::new(),
            filter_bits: Vec::new(),
        });
        save_manifest(dir.path(), &manifest).unwrap();

        let route = load_manifest_route(dir.path()).unwrap().unwrap();
        assert!(!route.shards[0].substring_bits.is_empty());
        assert_eq!(
            shard_route_candidate_ids(&route, &trigram_route_requirements(&shared_trigrams)),
            ShardRouteLookup::Candidates(vec![0, 1])
        );
        assert_eq!(
            shard_route_candidate_ids(&route, &substring_route_requirements("trigramprobe")),
            ShardRouteLookup::Candidates(vec![0])
        );
    }

    #[test]
    fn route_shard_id_varints_round_trip_sparse_ids() {
        let ids = vec![0, 1, 127, 128, 255, 16_384, u16::MAX];
        let mut bytes = Vec::new();

        encode_route_shard_ids(&ids, &mut bytes);

        assert_eq!(decode_route_shard_ids(&bytes, ids.len()), Some(ids));
        assert!(bytes.len() < 2 * 7);

        let mut truncated = bytes;
        truncated.pop();
        assert_eq!(decode_route_shard_ids(&truncated, 7), None);
        assert_eq!(decode_route_shard_ids(&[1, 0], 2), None);
    }

    #[test]
    fn malformed_route_varints_mark_lookup_corrupt() {
        let route = ShardManifestRoute {
            version: SHARD_MANIFEST_ROUTE_VERSION,
            json_fingerprint: ManifestFileFingerprint {
                len: 0,
                modified_secs: 0,
                modified_nanos: 0,
            },
            shards: Vec::new(),
            exact_terms: vec![ShardRouteTerm {
                hash: 42,
                start: 0,
                len: 1,
            }],
            omitted_hashes: Vec::new(),
            trigram_terms: Vec::new(),
            omitted_trigram_hashes: Vec::new(),
            shard_ids: vec![0x80],
        };

        assert_eq!(
            shard_route_candidate_ids(&route, &exact_route_requirements(&[42])),
            ShardRouteLookup::Corrupt
        );
    }
}
