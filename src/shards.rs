//! Multi-repo shard manifests for local indexed search.

use crate::fast_index::{FastIndex, IndexStats};
use crate::query::{merge_filters, parse_query, query_text};
use crate::repo_index::{
    FileRange, RelatedFile, RelatedSymbol, RepoMap, SearchFilters, SearchResult, Symbol,
    finalize_results, is_manifest_file, language_for, normalize_token,
};
use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
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
    #[serde(default)]
    pub aliases: Vec<ShardAlias>,
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
    pub files: usize,
    pub terms: usize,
    pub path_terms: usize,
    pub trigrams: usize,
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
    pub map: RepoMap,
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
            aliases: shard_aliases(&root, &base_name)?,
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
        return Ok(ShardEnsureStats {
            version: stats.version,
            output_dir: stats.output_dir,
            action: "refresh".to_string(),
            shards: stats.shards,
            files: stats.files,
            terms: stats.terms,
            path_terms: stats.path_terms,
            trigrams: stats.trigrams,
            symbols: stats.symbols,
            reused_files: stats.reused_files,
            renamed_files: stats.renamed_files,
            refreshed_files: stats.refreshed_files,
            deleted_files: stats.deleted_files,
        });
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
        files: stats.files,
        terms: stats.terms,
        path_terms: stats.path_terms,
        trigrams: stats.trigrams,
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
        terms: 0,
        path_terms: 0,
        trigrams: 0,
        symbols: 0,
        reused_files: 0,
        renamed_files: 0,
        refreshed_files: 0,
        deleted_files: 0,
    };

    for shard in &mut manifest.shards {
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
    let shard_query = query_text(&parsed.terms, &filters);
    let mut results = Vec::new();
    for shard in manifest.shards {
        let scopes = shard_search_scopes(&shard, &filters);
        if scopes.is_empty() {
            continue;
        }
        let index = FastIndex::load(index_dir.join(&shard.index))
            .with_context(|| format!("load shard {}", shard.index))?;
        for scope in scopes {
            let scoped_filters = filters_for_shard_scope(&filters, scope.path_prefix.as_deref());
            for mut result in index.search_filtered(&shard_query, limit, &scoped_filters)? {
                if let Some(prefix) = &scope.path_prefix {
                    if !result.path.starts_with(prefix) {
                        continue;
                    }
                }
                result.path = scoped_output_path(&scope, &result.path);
                result.reason = format!("shard:{}; {}", scope.output_prefix, result.reason);
                results.push(result);
            }
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
    range.path = format!("{}/{}", resolved.output_prefix, relative_path);
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

fn prefix_repo_map_paths(map: &mut RepoMap, scope: &ShardSearchScope) {
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
}

pub(crate) fn filter_repo_map_by_prefix(map: &mut RepoMap, path_prefix: &str) {
    let prefix = path_prefix.trim_end_matches('/');
    let matches_prefix = |path: &str| path == prefix || path.starts_with(&format!("{prefix}/"));

    map.brief.manifest_files.retain(|path| matches_prefix(path));
    map.brief
        .important_files
        .retain(|path| matches_prefix(path));
    map.entrypoints.retain(|path| matches_prefix(path));
    map.test_files.retain(|path| matches_prefix(path));
    map.top_symbols
        .retain(|symbol| matches_prefix(&symbol.path));

    let retained_paths = map
        .brief
        .manifest_files
        .iter()
        .chain(map.brief.important_files.iter())
        .chain(map.entrypoints.iter())
        .chain(map.test_files.iter())
        .chain(map.top_symbols.iter().map(|symbol| &symbol.path))
        .collect::<HashSet<_>>()
        .into_iter()
        .cloned()
        .collect::<Vec<_>>();

    map.brief.file_count = retained_paths.len();
    map.brief.language_counts = language_counts_for_paths(&retained_paths);
    map.brief.known_commands = known_commands_for_manifest_paths(&map.brief.manifest_files);
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
    let has_manifest = |name: &str| {
        paths
            .iter()
            .any(|path| Path::new(path).file_name().and_then(|value| value.to_str()) == Some(name))
    };
    let mut commands = Vec::new();
    if has_manifest("Cargo.toml") {
        commands.push("cargo test".to_string());
    }
    if has_manifest("pyproject.toml") {
        commands.push("pytest".to_string());
    }
    if has_manifest("package.json") {
        commands.push("npm test".to_string());
    }
    if has_manifest("go.mod") {
        commands.push("go test ./...".to_string());
    }
    commands
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
    Ok(manifest)
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
    fs::write(
        index_dir.join("manifest.json"),
        serde_json::to_vec_pretty(manifest)?,
    )
    .with_context(|| format!("write shard manifest {}", index_dir.display()))
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
}

fn alias_matches(alias: &ShardAlias, filter: &str) -> bool {
    alias
        .name
        .to_ascii_lowercase()
        .contains(&filter.to_ascii_lowercase())
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
