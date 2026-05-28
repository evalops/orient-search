//! Persistent local search index for agent-oriented code retrieval.

use crate::query::{merge_filters, normalize_phrase_text, parse_query, query_phrases, query_text};
use crate::repo_index::{
    FileRange, MAX_READ_RANGE_LINES, QueryPlan, QueryPlanFilter, QueryPlanPosting,
    QueryPlanRepairHint, RankSignal, RelatedFile, RelatedSymbol, RepoBrief, RepoMap, SearchFilters,
    SearchResult, SnippetMode, Symbol, apply_phrase_matches, best_snippet_for_path_with_phrases,
    capped_search_limit, command_hints_from_manifest_texts, dependency_filters_match,
    dependency_hints_from_manifest_texts, extract_symbols, filter_only_query,
    filter_only_search_result, filter_value_matches, finalize_results,
    import_hints_from_source_texts, is_entrypoint_path, is_ignored, is_important_file,
    is_manifest_file, is_test_path, known_commands_from_hints, language_for,
    matches_filters_with_path_metadata, normalize_token, regular_file_metadata, related_stem_terms,
    repo_map_seed_paths, repo_matches, result_matches_all_tokens, result_matches_symbol_filters,
    round4, score_filter_only_path, source_import_filters_match, symbol_exact_phrase_bonus,
    symbol_kind_rank, symbol_query_match_score, token_counts, tokenize, unique_query_tokens,
};
use ahash::{AHashMap as HashMap, AHashSet as HashSet};
use anyhow::{Context, Result};
use ignore::WalkBuilder;
use memmap2::Mmap;
use serde::{Deserialize, Serialize};
use std::cmp::Ordering;
use std::fs;
use std::io::Write;
use std::path::{Component, Path, PathBuf};
use std::process;
use std::time::{SystemTime, UNIX_EPOCH};

const INDEX_VERSION: u32 = 12;
const PREVIOUS_DISK_INDEX_VERSION: u32 = 11;
const OLDER_DISK_INDEX_VERSION: u32 = 10;
const LEGACY_RAW_INDEX_VERSION: u32 = 9;
const INDEX_MAGIC: &[u8] = b"ORIENTIDX\0";
const INDEX_HEADER_LEN: usize = INDEX_MAGIC.len() + std::mem::size_of::<u32>();
const MAX_FILE_BYTES: u64 = 512_000;
const MAX_TERM_LINES_PER_TERM: usize = 64;
const MAX_INDEX_CANDIDATES_TO_SCORE: usize = 8_192;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct FastIndex {
    pub version: u32,
    pub root: PathBuf,
    pub files: Vec<IndexedPath>,
    pub postings: HashMap<String, Vec<Posting>>,
    pub path_postings: HashMap<String, Vec<Posting>>,
    pub trigram_postings: HashMap<String, Vec<Posting>>,
    #[serde(default)]
    pub symbol_postings: HashMap<String, Vec<Posting>>,
    #[serde(default)]
    pub symbol_kind_postings: HashMap<String, Vec<Posting>>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
struct FastIndexDisk {
    version: u32,
    root: PathBuf,
    files: Vec<IndexedPath>,
    postings: HashMap<String, CompressedPostingList>,
    path_postings: HashMap<String, CompressedPostingList>,
    trigram_postings: HashMap<String, CompressedPostingList>,
    #[serde(default)]
    symbol_postings: HashMap<String, CompressedPostingList>,
    #[serde(default)]
    symbol_kind_postings: HashMap<String, CompressedPostingList>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct CompressedPostingList {
    postings: u32,
    bytes: Vec<u8>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct IndexedPath {
    pub path: String,
    #[serde(default)]
    pub path_lower: String,
    #[serde(skip)]
    pub file_name_lower: String,
    #[serde(skip)]
    pub extension_lower: Option<String>,
    pub language: String,
    pub size: u64,
    pub content_hash: u64,
    pub modified_secs: u64,
    pub modified_nanos: u32,
    pub terms: Vec<TermCount>,
    pub path_terms: Vec<TermCount>,
    pub trigrams: Vec<TermCount>,
    pub symbols: Vec<IndexedSymbol>,
    pub line_offsets: Vec<u32>,
    #[serde(default)]
    pub term_lines: Vec<IndexedTermLines>,
    #[serde(default)]
    pub content: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct IndexedSymbol {
    pub name: String,
    pub kind: String,
    pub line: usize,
    pub normalized: String,
    pub tokens: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TermCount {
    pub term: String,
    pub count: u16,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct IndexedTermLines {
    pub term: String,
    pub lines: Vec<u32>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Posting {
    pub file_id: u32,
    pub count: u16,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct IndexStats {
    pub version: u32,
    pub root: PathBuf,
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
pub struct RefreshStats {
    pub version: u32,
    pub root: PathBuf,
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

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct IndexFreshness {
    pub version: u32,
    pub root: PathBuf,
    pub root_exists: bool,
    pub stale: bool,
    pub indexed_files: usize,
    pub source_bytes: u64,
    pub terms: usize,
    pub path_terms: usize,
    pub trigrams: usize,
    pub posting_entries: usize,
    pub compressed_posting_bytes: usize,
    pub symbols: usize,
    pub checked_files: usize,
    pub changed_files: usize,
    pub deleted_files: usize,
    pub added_files: usize,
    pub changed_paths: Vec<String>,
    pub deleted_paths: Vec<String>,
    pub added_paths: Vec<String>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct RefreshOutcome {
    pub index: FastIndex,
    pub reused_files: usize,
    pub renamed_files: usize,
    pub refreshed_files: usize,
    pub deleted_files: usize,
}

#[derive(Debug, Clone)]
struct RefreshCandidate {
    rel: String,
    language: String,
    size: u64,
    modified_secs: u64,
    modified_nanos: u32,
}

impl FastIndex {
    pub fn build(root: impl AsRef<Path>) -> Result<Self> {
        Ok(Self::refresh(root, None)?.index)
    }

    pub fn refresh(root: impl AsRef<Path>, previous: Option<&FastIndex>) -> Result<RefreshOutcome> {
        let root = root.as_ref().canonicalize()?;
        let previous_files = previous
            .filter(|index| index.root == root)
            .map(|index| {
                index
                    .files
                    .iter()
                    .map(|file| (file.path.clone(), file.clone()))
                    .collect::<HashMap<_, _>>()
            })
            .unwrap_or_default();
        let mut candidates = Vec::new();
        let mut current_paths = HashSet::new();
        for entry in WalkBuilder::new(&root)
            .hidden(false)
            .filter_entry(|entry| !is_ignored(entry.path()))
            .build()
        {
            let entry = entry?;
            let path = entry.path();
            let Some(metadata) = regular_file_metadata(path) else {
                continue;
            };
            if metadata.len() > MAX_FILE_BYTES {
                continue;
            }
            let Some(language) = language_for(path) else {
                continue;
            };
            let rel = path
                .strip_prefix(&root)?
                .to_string_lossy()
                .replace('\\', "/");
            let (modified_secs, modified_nanos) = modified_parts(metadata.modified().ok());
            current_paths.insert(rel.clone());
            candidates.push(RefreshCandidate {
                rel,
                language,
                size: metadata.len(),
                modified_secs,
                modified_nanos,
            });
        }

        let mut rename_candidates = previous_files
            .values()
            .filter(|file| !current_paths.contains(&file.path))
            .fold(
                HashMap::<(String, u64, u64), Vec<IndexedPath>>::new(),
                |mut grouped, file| {
                    grouped
                        .entry((file.language.clone(), file.size, file.content_hash))
                        .or_default()
                        .push(file.clone());
                    grouped
                },
            );
        let mut files = Vec::new();
        let mut reused_files = 0usize;
        let mut renamed_files = 0usize;
        let mut refreshed_files = 0usize;

        for candidate in candidates {
            if let Some(previous) = previous_files.get(&candidate.rel) {
                if previous.size == candidate.size
                    && previous.modified_secs == candidate.modified_secs
                    && previous.modified_nanos == candidate.modified_nanos
                    && previous.language == candidate.language
                {
                    files.push(previous.clone());
                    reused_files += 1;
                    continue;
                }
            }
            let text = fs::read_to_string(root.join(&candidate.rel)).unwrap_or_default();
            let content_hash = content_hash(text.as_bytes());
            let rename_key = (candidate.language.clone(), candidate.size, content_hash);
            if !text.contains('\0') {
                if let Some(previous) = rename_candidates
                    .get_mut(&rename_key)
                    .and_then(|files| files.pop())
                {
                    files.push(retarget_indexed_file(previous, &candidate, &text));
                    reused_files += 1;
                    renamed_files += 1;
                    continue;
                }
            }
            let Some(file) = index_file(
                &root,
                &candidate.rel,
                candidate.language,
                candidate.size,
                candidate.modified_secs,
                candidate.modified_nanos,
            ) else {
                continue;
            };
            files.push(file);
            refreshed_files += 1;
        }

        let missing_previous_files = previous_files
            .keys()
            .filter(|path| !current_paths.contains(*path))
            .count();
        let deleted_files = missing_previous_files.saturating_sub(renamed_files);
        let postings = rebuild_postings(&files, |file| &file.terms);
        let path_postings = rebuild_postings(&files, |file| &file.path_terms);
        let trigram_postings = rebuild_postings(&files, |file| &file.trigrams);
        let symbol_postings = rebuild_symbol_postings(&files);
        let symbol_kind_postings = rebuild_symbol_kind_postings(&files);
        Ok(Self {
            version: INDEX_VERSION,
            root,
            files,
            postings,
            path_postings,
            trigram_postings,
            symbol_postings,
            symbol_kind_postings,
        })
        .map(|index| RefreshOutcome {
            index,
            reused_files,
            renamed_files,
            refreshed_files,
            deleted_files,
        })
    }

    pub fn load(path: impl AsRef<Path>) -> Result<Self> {
        let path = path.as_ref();
        let file =
            fs::File::open(path).with_context(|| format!("open index {}", path.display()))?;
        let metadata = file
            .metadata()
            .with_context(|| format!("stat index {}", path.display()))?;
        if metadata.len() == 0 {
            return Self::load_index_bytes(&[], path);
        }
        let bytes = unsafe { Mmap::map(&file) }
            .with_context(|| format!("mmap index {}", path.display()))?;
        Self::load_index_bytes(&bytes, path)
    }

    fn load_index_bytes(bytes: &[u8], path: &Path) -> Result<Self> {
        let (payload, header_version) = index_payload(bytes)
            .with_context(|| format!("parse index header {}", path.display()))?;
        let mut index = match header_version {
            Some(INDEX_VERSION | PREVIOUS_DISK_INDEX_VERSION | OLDER_DISK_INDEX_VERSION) => {
                bincode::deserialize::<FastIndexDisk>(payload)
                    .with_context(|| format!("parse index {}", path.display()))?
                    .into_index()
                    .with_context(|| format!("decode index {}", path.display()))?
            }
            Some(LEGACY_RAW_INDEX_VERSION) | None => load_raw_index(payload)
                .with_context(|| format!("parse index {}", path.display()))?,
            Some(version) => anyhow::bail!("unsupported index version {}", version),
        };
        index.normalize_loaded();
        Ok(index)
    }

    pub fn save(&self, path: impl AsRef<Path>) -> Result<()> {
        if let Some(parent) = path.as_ref().parent() {
            fs::create_dir_all(parent)?;
        }
        let payload = bincode::serialize(&FastIndexDisk::from_index(self))?;
        let mut bytes = Vec::with_capacity(INDEX_HEADER_LEN + payload.len());
        bytes.extend_from_slice(INDEX_MAGIC);
        bytes.extend_from_slice(&INDEX_VERSION.to_le_bytes());
        bytes.extend_from_slice(&payload);
        atomic_write(path.as_ref(), &bytes)
            .with_context(|| format!("write index {}", path.as_ref().display()))
    }

    pub fn stats(&self) -> IndexStats {
        IndexStats {
            version: self.version,
            root: self.root.clone(),
            files: self.files.len(),
            source_bytes: indexed_source_bytes(&self.files),
            terms: self.postings.len(),
            path_terms: self.path_postings.len(),
            trigrams: self.trigram_postings.len(),
            posting_entries: total_posting_entries(&self.postings)
                + total_posting_entries(&self.path_postings)
                + total_posting_entries(&self.trigram_postings)
                + total_posting_entries(&self.symbol_postings)
                + total_posting_entries(&self.symbol_kind_postings),
            compressed_posting_bytes: total_compressed_posting_bytes(&self.postings)
                + total_compressed_posting_bytes(&self.path_postings)
                + total_compressed_posting_bytes(&self.trigram_postings)
                + total_compressed_posting_bytes(&self.symbol_postings)
                + total_compressed_posting_bytes(&self.symbol_kind_postings),
            symbols: self.files.iter().map(|file| file.symbols.len()).sum(),
        }
    }

    fn normalize_loaded(&mut self) {
        for file in &mut self.files {
            refresh_indexed_path_metadata(file);
        }
        normalize_posting_map(&mut self.postings);
        normalize_posting_map(&mut self.path_postings);
        normalize_posting_map(&mut self.trigram_postings);
        if self.symbol_postings.is_empty() {
            self.symbol_postings = rebuild_symbol_postings(&self.files);
        } else {
            normalize_posting_map(&mut self.symbol_postings);
        }
        if self.symbol_kind_postings.is_empty() {
            self.symbol_kind_postings = rebuild_symbol_kind_postings(&self.files);
        } else {
            normalize_posting_map(&mut self.symbol_kind_postings);
        }
    }

    pub fn refresh_stats(&self, outcome: &RefreshOutcome) -> RefreshStats {
        RefreshStats {
            version: self.version,
            root: self.root.clone(),
            files: self.files.len(),
            source_bytes: indexed_source_bytes(&self.files),
            terms: self.postings.len(),
            path_terms: self.path_postings.len(),
            trigrams: self.trigram_postings.len(),
            posting_entries: total_posting_entries(&self.postings)
                + total_posting_entries(&self.path_postings)
                + total_posting_entries(&self.trigram_postings)
                + total_posting_entries(&self.symbol_postings)
                + total_posting_entries(&self.symbol_kind_postings),
            compressed_posting_bytes: total_compressed_posting_bytes(&self.postings)
                + total_compressed_posting_bytes(&self.path_postings)
                + total_compressed_posting_bytes(&self.trigram_postings)
                + total_compressed_posting_bytes(&self.symbol_postings)
                + total_compressed_posting_bytes(&self.symbol_kind_postings),
            symbols: self.files.iter().map(|file| file.symbols.len()).sum(),
            reused_files: outcome.reused_files,
            renamed_files: outcome.renamed_files,
            refreshed_files: outcome.refreshed_files,
            deleted_files: outcome.deleted_files,
        }
    }

    pub fn freshness(&self) -> Result<IndexFreshness> {
        let stats = self.stats();
        if !self.root.exists() {
            let mut deleted_paths = self
                .files
                .iter()
                .map(|file| file.path.clone())
                .collect::<Vec<_>>();
            deleted_paths.sort();
            return Ok(IndexFreshness {
                version: self.version,
                root: self.root.clone(),
                root_exists: false,
                stale: !deleted_paths.is_empty(),
                indexed_files: self.files.len(),
                source_bytes: stats.source_bytes,
                terms: stats.terms,
                path_terms: stats.path_terms,
                trigrams: stats.trigrams,
                posting_entries: stats.posting_entries,
                compressed_posting_bytes: stats.compressed_posting_bytes,
                symbols: stats.symbols,
                checked_files: 0,
                changed_files: 0,
                deleted_files: deleted_paths.len(),
                added_files: 0,
                changed_paths: Vec::new(),
                deleted_paths,
                added_paths: Vec::new(),
            });
        }

        let indexed = self
            .files
            .iter()
            .map(|file| (file.path.clone(), file))
            .collect::<HashMap<_, _>>();
        let mut current_paths = HashSet::new();
        let mut changed_paths = Vec::new();
        let mut added_paths = Vec::new();
        let mut checked_files = 0usize;

        for entry in WalkBuilder::new(&self.root)
            .hidden(false)
            .filter_entry(|entry| !is_ignored(entry.path()))
            .build()
        {
            let entry = entry?;
            let path = entry.path();
            let Some(metadata) = regular_file_metadata(path) else {
                continue;
            };
            if metadata.len() > MAX_FILE_BYTES {
                continue;
            }
            let Some(language) = language_for(path) else {
                continue;
            };
            let rel = path
                .strip_prefix(&self.root)?
                .to_string_lossy()
                .replace('\\', "/");
            checked_files += 1;
            current_paths.insert(rel.clone());
            match indexed.get(&rel) {
                Some(previous)
                    if previous.size == metadata.len()
                        && previous.language == language
                        && modified_matches(previous, metadata.modified().ok()) => {}
                Some(_) => changed_paths.push(rel),
                None => added_paths.push(rel),
            }
        }

        let mut deleted_paths = indexed
            .keys()
            .filter(|path| !current_paths.contains(*path))
            .cloned()
            .collect::<Vec<_>>();
        changed_paths.sort();
        deleted_paths.sort();
        added_paths.sort();
        Ok(IndexFreshness {
            version: self.version,
            root: self.root.clone(),
            root_exists: true,
            stale: !(changed_paths.is_empty()
                && deleted_paths.is_empty()
                && added_paths.is_empty()),
            indexed_files: self.files.len(),
            source_bytes: stats.source_bytes,
            terms: stats.terms,
            path_terms: stats.path_terms,
            trigrams: stats.trigrams,
            posting_entries: stats.posting_entries,
            compressed_posting_bytes: stats.compressed_posting_bytes,
            symbols: stats.symbols,
            checked_files,
            changed_files: changed_paths.len(),
            deleted_files: deleted_paths.len(),
            added_files: added_paths.len(),
            changed_paths,
            deleted_paths,
            added_paths,
        })
    }

    pub fn search(&self, query: &str, limit: usize) -> Result<Vec<SearchResult>> {
        self.search_filtered(query, limit, &SearchFilters::default())
    }

    pub fn repo_map(&self, symbol_limit: usize, test_limit: usize) -> RepoMap {
        let mut language_counts = HashMap::new();
        for file in &self.files {
            *language_counts.entry(file.language.clone()).or_insert(0) += 1;
        }

        let mut manifest_files = self
            .files
            .iter()
            .filter(|file| is_manifest_file(&file.path))
            .map(|file| file.path.clone())
            .collect::<Vec<_>>();
        manifest_files.sort();

        let mut important_files = self
            .files
            .iter()
            .filter(|file| is_important_file(&file.path))
            .map(|file| file.path.clone())
            .collect::<Vec<_>>();
        important_files.sort();

        let mut entrypoints = self
            .files
            .iter()
            .filter(|file| is_entrypoint_path(&file.path))
            .map(|file| file.path.clone())
            .collect::<Vec<_>>();
        entrypoints.sort();

        let mut test_files = self
            .files
            .iter()
            .filter(|file| is_test_path(&file.path_lower))
            .map(|file| file.path.clone())
            .collect::<Vec<_>>();
        test_files.sort();
        test_files.truncate(test_limit);

        let mut top_symbols = self
            .files
            .iter()
            .flat_map(|file| {
                file.symbols.iter().map(|symbol| Symbol {
                    name: symbol.name.clone(),
                    kind: symbol.kind.clone(),
                    path: file.path.clone(),
                    line: symbol.line,
                })
            })
            .collect::<Vec<_>>();
        top_symbols.sort_by(|a, b| {
            symbol_kind_rank(&a.kind)
                .cmp(&symbol_kind_rank(&b.kind))
                .then_with(|| a.path.cmp(&b.path))
                .then_with(|| a.line.cmp(&b.line))
                .then_with(|| a.name.cmp(&b.name))
        });
        top_symbols.truncate(symbol_limit);

        let mut related_file_seeds = important_files.clone();
        related_file_seeds.extend(top_symbols.iter().map(|symbol| symbol.path.clone()));
        let related_files =
            self.repo_map_related_files(&entrypoints, &test_files, &related_file_seeds, 12);
        let related_symbols =
            self.repo_map_related_symbols(&entrypoints, &test_files, &top_symbols, 12);

        let command_hints = command_hints_from_indexed_files(&self.files);
        let known_commands = known_commands_from_hints(&command_hints);
        let dependency_hints = dependency_hints_from_indexed_files(&self.files);
        let import_hints = import_hints_from_indexed_files(&self.files);

        RepoMap {
            brief: RepoBrief {
                root_name: self
                    .root
                    .file_name()
                    .map(|value| value.to_string_lossy().to_string())
                    .unwrap_or_else(|| self.root.display().to_string()),
                file_count: self.files.len(),
                language_counts,
                known_commands,
                command_hints,
                dependency_hints,
                import_hints,
                manifest_files,
                important_files,
            },
            entrypoints,
            test_files,
            top_symbols,
            related_files,
            related_symbols,
        }
    }

    fn repo_map_related_files(
        &self,
        entrypoints: &[String],
        test_files: &[String],
        important_files: &[String],
        limit: usize,
    ) -> Vec<crate::repo_index::RepoMapRelatedFile> {
        let mut seen = HashSet::new();
        let mut related = Vec::new();
        for source_path in repo_map_seed_paths(entrypoints, test_files, important_files) {
            for item in self.related_files(&source_path, 3) {
                if seen.insert((source_path.clone(), item.path.clone())) {
                    related.push(crate::repo_index::RepoMapRelatedFile {
                        source_path: source_path.clone(),
                        path: item.path,
                        reason: item.reason,
                        score: item.score,
                    });
                }
            }
        }
        related.truncate(limit);
        related
    }

    fn repo_map_related_symbols(
        &self,
        entrypoints: &[String],
        test_files: &[String],
        top_symbols: &[Symbol],
        limit: usize,
    ) -> Vec<crate::repo_index::RepoMapRelatedSymbol> {
        let symbol_paths = top_symbols
            .iter()
            .map(|symbol| symbol.path.clone())
            .collect::<Vec<_>>();
        let mut seen = HashSet::new();
        let mut related = Vec::new();
        for source_path in repo_map_seed_paths(entrypoints, test_files, &symbol_paths) {
            for item in self.related_symbols(Some(&source_path), None, 3) {
                let key = (
                    source_path.clone(),
                    item.symbol.path.clone(),
                    item.symbol.line,
                    item.symbol.name.clone(),
                );
                if seen.insert(key) {
                    related.push(crate::repo_index::RepoMapRelatedSymbol {
                        source_path: source_path.clone(),
                        symbol: item.symbol,
                        reason: item.reason,
                        score: item.score,
                    });
                }
            }
        }
        related.truncate(limit);
        related
    }

    fn matches_dependency_filters(&self, filters: &SearchFilters) -> bool {
        if filters.dependency.is_none() && filters.exclude_dependency.is_empty() {
            return true;
        }
        dependency_filters_match(&dependency_hints_from_indexed_files(&self.files), filters)
    }

    pub fn find_symbol(&self, name: &str, limit: usize) -> Vec<Symbol> {
        let needle = normalize_token(name);
        if needle.is_empty() || limit == 0 {
            return Vec::new();
        }

        let mut scored = Vec::new();

        for file in &self.files {
            for symbol in &file.symbols {
                let score = if symbol.name == name {
                    100
                } else if symbol.normalized == needle {
                    90
                } else if symbol.normalized.contains(&needle) {
                    60
                } else {
                    0
                };
                if score > 0 {
                    scored.push((
                        score,
                        Symbol {
                            name: symbol.name.clone(),
                            kind: symbol.kind.clone(),
                            path: file.path.clone(),
                            line: symbol.line,
                        },
                    ));
                }
            }
        }

        scored.sort_by(|a, b| {
            b.0.cmp(&a.0)
                .then_with(|| a.1.path.cmp(&b.1.path))
                .then_with(|| a.1.line.cmp(&b.1.line))
                .then_with(|| a.1.name.cmp(&b.1.name))
        });
        scored
            .into_iter()
            .take(limit)
            .map(|(_, symbol)| symbol)
            .collect()
    }

    pub fn read_range(
        &self,
        path: &str,
        start_line: usize,
        line_count: usize,
    ) -> Result<FileRange> {
        let normalized = normalize_index_relative_path(path)?;
        let file = self
            .files
            .iter()
            .find(|file| file.path == normalized)
            .ok_or_else(|| anyhow::anyhow!("path is not present in index: {normalized}"))?;
        Ok(indexed_file_range(file, start_line, line_count))
    }

    pub fn related_files(&self, path: &str, limit: usize) -> Vec<RelatedFile> {
        if limit == 0 {
            return Vec::new();
        }

        let normalized = path.trim_start_matches('/').to_string();
        let stem = Path::new(&normalized)
            .file_stem()
            .map(|value| value.to_string_lossy().to_string())
            .unwrap_or_default();
        let stem_lower = stem.to_ascii_lowercase();
        let stem_terms = related_stem_terms(&stem_lower);
        let directory = Path::new(&normalized)
            .parent()
            .map(|value| value.to_string_lossy().to_string())
            .unwrap_or_default();
        let normalized_lower = normalized.to_ascii_lowercase();
        let source_is_test = is_test_path(&normalized_lower);
        let source_symbols = self
            .files
            .iter()
            .find(|file| file.path == normalized)
            .map(|file| {
                file.symbols
                    .iter()
                    .map(|symbol| symbol.name.clone())
                    .collect::<Vec<_>>()
            })
            .unwrap_or_default();
        let mut related = Vec::new();

        for file in &self.files {
            if file.path == normalized {
                continue;
            }
            let lower = &file.path_lower;
            let mut score = 0.0;
            let mut reasons = Vec::new();

            if !stem_lower.is_empty() && lower.contains(&stem_lower) {
                score += 4.0;
                reasons.push(format!("shares stem {stem}"));
            }
            if stem_terms.iter().any(|term| lower.contains(term)) {
                score += 3.0;
                reasons.push("shares normalized stem".to_string());
            }
            if source_is_test != is_test_path(&lower)
                && stem_terms.iter().any(|term| lower.contains(term))
            {
                score += 5.0;
                reasons.push("source/test counterpart".to_string());
            }

            let file_dir = Path::new(&file.path)
                .parent()
                .map(|value| value.to_string_lossy().to_string())
                .unwrap_or_default();
            if !directory.is_empty() && file_dir == directory {
                score += 1.0;
                reasons.push("same directory".to_string());
            }
            let content_lower = file.content.to_ascii_lowercase();
            for symbol in &source_symbols {
                if content_lower.contains(&symbol.to_ascii_lowercase()) {
                    score += 6.0;
                    reasons.push(format!("references symbol {symbol}"));
                    break;
                }
            }

            if score > 0.0 {
                related.push(RelatedFile {
                    path: file.path.clone(),
                    reason: reasons.join("; "),
                    score,
                });
            }
        }

        related.sort_by(|a, b| {
            b.score
                .partial_cmp(&a.score)
                .unwrap_or(Ordering::Equal)
                .then_with(|| a.path.cmp(&b.path))
        });
        related.truncate(limit);
        related
    }

    pub fn related_symbols(
        &self,
        path: Option<&str>,
        query: Option<&str>,
        limit: usize,
    ) -> Vec<RelatedSymbol> {
        if limit == 0 {
            return Vec::new();
        }

        let normalized_path = path.map(|value| value.trim_start_matches('/').to_string());
        if let Some(path) = &normalized_path {
            if !self.files.iter().any(|file| &file.path == path) {
                return Vec::new();
            }
        }
        let query_tokens = query
            .map(tokenize)
            .unwrap_or_default()
            .into_iter()
            .collect::<HashSet<_>>();
        let query_symbol = query.map(normalize_token).unwrap_or_default();
        let path_stem = normalized_path
            .as_deref()
            .and_then(|path| Path::new(path).file_stem())
            .map(|value| value.to_string_lossy().to_ascii_lowercase())
            .unwrap_or_default();
        let path_stem_terms = related_stem_terms(&path_stem);
        let path_dir = normalized_path
            .as_deref()
            .and_then(|path| Path::new(path).parent())
            .map(|value| value.to_string_lossy().to_string())
            .unwrap_or_default();
        let mut related = Vec::new();

        for file in &self.files {
            for indexed_symbol in &file.symbols {
                let symbol = Symbol {
                    name: indexed_symbol.name.clone(),
                    kind: indexed_symbol.kind.clone(),
                    path: file.path.clone(),
                    line: indexed_symbol.line,
                };
                let mut score = 0.0;
                let mut reasons = Vec::new();

                if let Some(path) = &normalized_path {
                    if &symbol.path == path {
                        score += 20.0;
                        reasons.push("same file".to_string());
                    }
                    if !path_dir.is_empty()
                        && Path::new(&symbol.path)
                            .parent()
                            .map(|value| value.to_string_lossy() == path_dir)
                            .unwrap_or(false)
                    {
                        score += 4.0;
                        reasons.push("same directory".to_string());
                    }
                    let symbol_path_lower = symbol.path.to_ascii_lowercase();
                    if !path_stem.is_empty()
                        && (symbol.name.to_ascii_lowercase().contains(&path_stem)
                            || symbol_path_lower.contains(&path_stem))
                    {
                        score += 3.0;
                        reasons.push(format!("shares stem {path_stem}"));
                    }
                    if path_stem_terms.iter().any(|term| {
                        symbol.name.to_ascii_lowercase().contains(term)
                            || symbol_path_lower.contains(term)
                    }) {
                        score += 3.0;
                        reasons.push("shares normalized stem".to_string());
                    }
                }

                if !query_tokens.is_empty() {
                    let symbol_tokens = indexed_symbol
                        .tokens
                        .iter()
                        .cloned()
                        .chain(tokenize(&symbol.path))
                        .collect::<HashSet<_>>();
                    let overlap = query_tokens
                        .iter()
                        .filter(|token| symbol_tokens.contains(*token))
                        .count();
                    if overlap > 0 {
                        score += 5.0 * overlap as f64;
                        reasons.push(format!("query overlap {overlap}"));
                    }
                    if !query_symbol.is_empty() && indexed_symbol.normalized == query_symbol {
                        score += 15.0;
                        reasons.push("exact query symbol".to_string());
                    }
                }

                if score > 0.0 {
                    score += match symbol.kind.as_str() {
                        "class" | "struct" | "enum" | "interface" => 2.0,
                        _ => 0.0,
                    };
                    related.push(RelatedSymbol {
                        symbol,
                        reason: reasons.join("; "),
                        score: round4(score),
                    });
                }
            }
        }

        related.sort_by(|a, b| {
            b.score
                .partial_cmp(&a.score)
                .unwrap_or(Ordering::Equal)
                .then_with(|| a.symbol.path.cmp(&b.symbol.path))
                .then_with(|| a.symbol.line.cmp(&b.symbol.line))
                .then_with(|| a.symbol.name.cmp(&b.symbol.name))
        });
        related.truncate(limit);
        related
    }

    pub fn search_filtered(
        &self,
        query: &str,
        limit: usize,
        filters: &SearchFilters,
    ) -> Result<Vec<SearchResult>> {
        let limit = capped_search_limit(limit);
        let parsed = parse_query(query);
        let query_phrases = query_phrases(&parsed.terms);
        let mut filters = merge_filters(filters.clone(), parsed.filters);
        if !repo_matches(&self.root, &filters) {
            return Ok(Vec::new());
        }
        if !self.matches_dependency_filters(&filters) {
            return Ok(Vec::new());
        }
        let query = query_text(&parsed.terms, &filters);
        let query_tokens = unique_query_tokens(&query);
        let query_trigrams = query_trigrams(&query);
        let symbol_kind_postings =
            symbol_kind_postings_for_filters(&self.symbol_kind_postings, &filters);
        if limit == 0 {
            return Ok(Vec::new());
        }
        if query_tokens.is_empty() && query_trigrams.is_empty() {
            return if filter_only_query(&filters) {
                Ok(self.search_filter_only(limit, &filters, &symbol_kind_postings))
            } else {
                Ok(Vec::new())
            };
        }
        if query_tokens.len() > 1 && !filters.match_any {
            filters.require_all = true;
        }
        let exact_symbol_query = exact_symbol_query_name(&parsed.terms, filters.symbol.as_deref());

        let mut token_postings = query_tokens
            .iter()
            .filter_map(|token| self.postings.get(token).map(|postings| (token, postings)))
            .collect::<Vec<_>>();
        let symbol_postings = exact_symbol_query
            .as_ref()
            .and_then(|symbol| {
                self.symbol_postings
                    .get(symbol)
                    .map(|postings| (symbol, postings))
            })
            .into_iter()
            .collect::<Vec<_>>();
        let mut path_postings = query_tokens
            .iter()
            .filter_map(|token| {
                self.path_postings
                    .get(token)
                    .map(|postings| (token, postings))
            })
            .collect::<Vec<_>>();
        let use_trigrams = !query_trigrams.is_empty()
            && (token_postings.len() < query_tokens.len()
                || (query_tokens.len() == 1 && query_tokens[0].len() >= 5));
        let mut trigram_postings = if use_trigrams {
            query_trigrams
                .iter()
                .filter_map(|trigram| {
                    self.trigram_postings
                        .get(trigram)
                        .map(|postings| (trigram, postings))
                })
                .collect::<Vec<_>>()
        } else {
            Vec::new()
        };
        let missing_terms = missing_query_terms(
            &query_tokens,
            &token_postings,
            &symbol_postings,
            &path_postings,
        );
        let missing_trigrams = if use_trigrams {
            missing_query_trigrams(&query_trigrams, &trigram_postings)
        } else {
            Vec::new()
        };
        if token_postings.is_empty()
            && symbol_postings.is_empty()
            && path_postings.is_empty()
            && trigram_postings.is_empty()
        {
            return Ok(Vec::new());
        }
        token_postings.sort_by_key(|(_, postings)| postings.len());
        path_postings.sort_by_key(|(_, postings)| postings.len());
        trigram_postings.sort_by_key(|(_, postings)| postings.len());
        let content_tokens = token_postings
            .iter()
            .map(|(token, _)| (*token).as_str())
            .collect::<HashSet<_>>();
        let path_plan_postings = path_postings
            .iter()
            .filter(|(token, _)| filters.match_any || !content_tokens.contains(token.as_str()))
            .collect::<Vec<_>>();

        let mut planned_postings = token_postings
            .iter()
            .map(|(_, postings)| *postings)
            .chain(symbol_postings.iter().map(|(_, postings)| *postings))
            .chain(symbol_kind_postings.iter().map(|(_, postings)| *postings))
            .chain(path_plan_postings.iter().map(|(_, postings)| *postings))
            .chain(
                trigram_postings
                    .iter()
                    .take(8)
                    .map(|(_, postings)| *postings),
            )
            .collect::<Vec<_>>();
        planned_postings.sort_by_key(|postings| postings.len());
        let candidate_ids =
            if filters.require_all && has_unsatisfied_missing_terms(&missing_terms, &filters) {
                Vec::new()
            } else if use_trigrams
                && (!token_postings.is_empty() || !path_postings.is_empty())
                && query_tokens.len() == 1
            {
                let token_only = token_postings
                    .iter()
                    .map(|(_, postings)| *postings)
                    .chain(path_plan_postings.iter().map(|(_, postings)| *postings))
                    .collect::<Vec<_>>();
                let trigram_only = trigram_postings
                    .iter()
                    .take(8)
                    .map(|(_, postings)| *postings)
                    .collect::<Vec<_>>();
                union_candidates(
                    intersect_planned_postings(&token_only, false),
                    intersect_planned_postings(&trigram_only, true),
                )
            } else {
                intersect_planned_postings(&planned_postings, filters.require_all)
            };
        let candidate_count = candidate_ids.len();

        let posting_lists = token_postings
            .iter()
            .map(|(token, postings)| (token.as_str(), postings.as_slice()))
            .collect::<Vec<_>>();
        let path_lists = path_postings
            .iter()
            .map(|(token, postings)| (token.as_str(), postings.as_slice()))
            .collect::<Vec<_>>();
        let trigram_lists = trigram_postings
            .iter()
            .take(16)
            .map(|(trigram, postings)| (trigram.as_str(), postings.as_slice()))
            .collect::<Vec<_>>();
        let query_name = query_tokens.join("");
        let candidate_cap = indexed_candidate_cap(limit);
        let (candidate_ids, candidate_cap_hit) = cap_candidate_ids(
            candidate_ids,
            candidate_cap,
            &self.files,
            &query_name,
            &query_tokens,
            &posting_lists,
            &path_lists,
            &trigram_lists,
        );
        let active_filters =
            query_plan_filters_for_candidates(&filters, &self.files, &candidate_ids);
        let filtered_candidate_ids = candidate_ids
            .into_iter()
            .filter(|file_id| {
                self.files
                    .get(*file_id as usize)
                    .is_some_and(|file| indexed_file_matches_filters(file, &filters))
            })
            .collect::<Vec<_>>();
        let filtered_candidate_count = filtered_candidate_ids.len();
        let results = filtered_candidate_ids
            .into_iter()
            .filter_map(|file_id| {
                self.score_file(
                    file_id,
                    &query_tokens,
                    &query_name,
                    &query_phrases,
                    &posting_lists,
                    &path_lists,
                    &trigram_lists,
                    filters.snippet,
                    filters.explain,
                    filters.symbol.as_deref(),
                    None,
                )
            })
            .collect::<Vec<_>>();

        let mut results = results;
        let scored_candidate_count = results.len();
        if filters.require_all {
            results.retain(|result| result_matches_all_tokens(result, &query_tokens));
        }
        results.retain(|result| result_matches_symbol_filters(result, &filters));
        let final_match_count = results.len();
        if filters.explain {
            let query_plan = indexed_query_plan(
                &query_tokens,
                &query_phrases,
                &query_trigrams,
                &token_postings,
                &symbol_postings,
                &symbol_kind_postings,
                &path_postings,
                &trigram_postings,
                &missing_terms,
                &missing_trigrams,
                use_trigrams,
                active_filters,
                filters.require_all,
                candidate_count,
                candidate_cap,
                candidate_cap_hit,
                filtered_candidate_count,
                scored_candidate_count,
                final_match_count,
            );
            for result in &mut results {
                result.query_plan = Some(query_plan.clone());
            }
        }
        Ok(finalize_results(results, limit))
    }

    pub fn query_plan(&self, query: &str, filters: &SearchFilters) -> Result<QueryPlan> {
        let parsed = parse_query(query);
        let query_phrases = query_phrases(&parsed.terms);
        let mut filters = merge_filters(filters.clone(), parsed.filters);
        if !repo_matches(&self.root, &filters) {
            return Ok(QueryPlan {
                strategy: "repo_filter_mismatch".to_string(),
                require_all: filters.require_all,
                query_tokens: Vec::new(),
                query_phrases,
                query_trigrams: Vec::new(),
                active_filters: query_plan_filters(&filters),
                planned_postings: Vec::new(),
                missing_terms: Vec::new(),
                missing_trigrams: Vec::new(),
                candidate_count: 0,
                candidate_cap: MAX_INDEX_CANDIDATES_TO_SCORE,
                candidate_cap_hit: false,
                filtered_candidate_count: 0,
                scored_candidate_count: 0,
                final_match_count: 0,
                repair_hints: vec![repair_hint(
                    "repo_filter_mismatch",
                    "The repo: filter does not match this index root. Relax repo: or choose a matching shard/index.",
                    None,
                )],
                retry_requests: Vec::new(),
            });
        }
        if !self.matches_dependency_filters(&filters) {
            return Ok(QueryPlan {
                strategy: "dependency_filter_mismatch".to_string(),
                require_all: filters.require_all,
                query_tokens: Vec::new(),
                query_phrases,
                query_trigrams: Vec::new(),
                active_filters: query_plan_filters(&filters),
                planned_postings: Vec::new(),
                missing_terms: Vec::new(),
                missing_trigrams: Vec::new(),
                candidate_count: 0,
                candidate_cap: MAX_INDEX_CANDIDATES_TO_SCORE,
                candidate_cap_hit: false,
                filtered_candidate_count: 0,
                scored_candidate_count: 0,
                final_match_count: 0,
                repair_hints: vec![repair_hint(
                    "dependency_filter_mismatch",
                    "The dependency filter does not match this index. Relax dep: or choose a matching shard/index.",
                    None,
                )],
                retry_requests: Vec::new(),
            });
        }
        let query = query_text(&parsed.terms, &filters);
        let query_tokens = unique_query_tokens(&query);
        let query_trigrams = query_trigrams(&query);
        let symbol_kind_postings =
            symbol_kind_postings_for_filters(&self.symbol_kind_postings, &filters);
        if query_tokens.len() > 1 && !filters.match_any {
            filters.require_all = true;
        }
        let exact_symbol_query = exact_symbol_query_name(&parsed.terms, filters.symbol.as_deref());
        if query_tokens.is_empty() && query_trigrams.is_empty() {
            if filter_only_query(&filters) {
                let candidate_ids = filter_only_candidate_ids(&symbol_kind_postings);
                let final_match_count = match &candidate_ids {
                    Some(candidate_ids) => candidate_ids
                        .iter()
                        .filter_map(|file_id| self.files.get(*file_id as usize))
                        .filter(|file| {
                            indexed_file_matches_filters(file, &filters)
                                && score_filter_only_path(&file.path, &filters, false).is_some()
                        })
                        .count(),
                    None => self
                        .files
                        .iter()
                        .filter(|file| {
                            indexed_file_matches_filters(file, &filters)
                                && score_filter_only_path(&file.path, &filters, false).is_some()
                        })
                        .count(),
                };
                let candidate_count = candidate_ids
                    .as_ref()
                    .map(Vec::len)
                    .unwrap_or(final_match_count);
                return Ok(QueryPlan {
                    strategy: if symbol_kind_postings.is_empty() {
                        "filter_scan".to_string()
                    } else {
                        "symbol_kind_filter_postings".to_string()
                    },
                    require_all: filters.require_all,
                    query_tokens,
                    query_phrases,
                    query_trigrams,
                    active_filters: query_plan_filters(&filters),
                    planned_postings: symbol_kind_postings
                        .iter()
                        .map(|(kind, postings)| plan_posting("symbol_kind", kind, postings))
                        .collect(),
                    missing_terms: Vec::new(),
                    missing_trigrams: Vec::new(),
                    candidate_count,
                    candidate_cap: MAX_INDEX_CANDIDATES_TO_SCORE,
                    candidate_cap_hit: false,
                    filtered_candidate_count: final_match_count,
                    scored_candidate_count: final_match_count,
                    final_match_count,
                    repair_hints: filter_scan_repair_hints(
                        &filters,
                        &self.symbol_kind_postings,
                        final_match_count,
                    ),
                    retry_requests: Vec::new(),
                });
            }
            return Ok(QueryPlan {
                strategy: "empty_query".to_string(),
                require_all: filters.require_all,
                query_tokens,
                query_phrases,
                query_trigrams,
                active_filters: query_plan_filters(&filters),
                planned_postings: Vec::new(),
                missing_terms: Vec::new(),
                missing_trigrams: Vec::new(),
                candidate_count: 0,
                candidate_cap: MAX_INDEX_CANDIDATES_TO_SCORE,
                candidate_cap_hit: false,
                filtered_candidate_count: 0,
                scored_candidate_count: 0,
                final_match_count: 0,
                repair_hints: vec![repair_hint(
                    "empty_query",
                    "Add a content term, quoted literal, symbol:, or positive file/path/lang/ext/test filter.",
                    None,
                )],
                retry_requests: Vec::new(),
            });
        }

        let mut token_postings = query_tokens
            .iter()
            .filter_map(|token| self.postings.get(token).map(|postings| (token, postings)))
            .collect::<Vec<_>>();
        let symbol_postings = exact_symbol_query
            .as_ref()
            .and_then(|symbol| {
                self.symbol_postings
                    .get(symbol)
                    .map(|postings| (symbol, postings))
            })
            .into_iter()
            .collect::<Vec<_>>();
        let mut path_postings = query_tokens
            .iter()
            .filter_map(|token| {
                self.path_postings
                    .get(token)
                    .map(|postings| (token, postings))
            })
            .collect::<Vec<_>>();
        let use_trigrams = !query_trigrams.is_empty()
            && (token_postings.len() < query_tokens.len()
                || (query_tokens.len() == 1 && query_tokens[0].len() >= 5));
        let mut trigram_postings = if use_trigrams {
            query_trigrams
                .iter()
                .filter_map(|trigram| {
                    self.trigram_postings
                        .get(trigram)
                        .map(|postings| (trigram, postings))
                })
                .collect::<Vec<_>>()
        } else {
            Vec::new()
        };
        let missing_terms = missing_query_terms(
            &query_tokens,
            &token_postings,
            &symbol_postings,
            &path_postings,
        );
        let missing_trigrams = if use_trigrams {
            missing_query_trigrams(&query_trigrams, &trigram_postings)
        } else {
            Vec::new()
        };
        token_postings.sort_by_key(|(_, postings)| postings.len());
        path_postings.sort_by_key(|(_, postings)| postings.len());
        trigram_postings.sort_by_key(|(_, postings)| postings.len());
        let content_tokens = token_postings
            .iter()
            .map(|(token, _)| (*token).as_str())
            .collect::<HashSet<_>>();
        let path_plan_postings = path_postings
            .iter()
            .filter(|(token, _)| filters.match_any || !content_tokens.contains(token.as_str()))
            .collect::<Vec<_>>();
        let mut planned_postings = token_postings
            .iter()
            .map(|(_, postings)| *postings)
            .chain(symbol_postings.iter().map(|(_, postings)| *postings))
            .chain(symbol_kind_postings.iter().map(|(_, postings)| *postings))
            .chain(path_plan_postings.iter().map(|(_, postings)| *postings))
            .chain(
                trigram_postings
                    .iter()
                    .take(8)
                    .map(|(_, postings)| *postings),
            )
            .collect::<Vec<_>>();
        planned_postings.sort_by_key(|postings| postings.len());
        let candidate_ids =
            if filters.require_all && has_unsatisfied_missing_terms(&missing_terms, &filters) {
                Vec::new()
            } else if use_trigrams
                && (!token_postings.is_empty() || !path_postings.is_empty())
                && query_tokens.len() == 1
            {
                let token_only = token_postings
                    .iter()
                    .map(|(_, postings)| *postings)
                    .chain(path_plan_postings.iter().map(|(_, postings)| *postings))
                    .collect::<Vec<_>>();
                let trigram_only = trigram_postings
                    .iter()
                    .take(8)
                    .map(|(_, postings)| *postings)
                    .collect::<Vec<_>>();
                union_candidates(
                    intersect_planned_postings(&token_only, false),
                    intersect_planned_postings(&trigram_only, true),
                )
            } else {
                intersect_planned_postings(&planned_postings, filters.require_all)
            };

        let candidate_count = candidate_ids.len();
        let posting_lists = token_postings
            .iter()
            .map(|(token, postings)| (token.as_str(), postings.as_slice()))
            .collect::<Vec<_>>();
        let path_lists = path_postings
            .iter()
            .map(|(token, postings)| (token.as_str(), postings.as_slice()))
            .collect::<Vec<_>>();
        let trigram_lists = trigram_postings
            .iter()
            .take(16)
            .map(|(trigram, postings)| (trigram.as_str(), postings.as_slice()))
            .collect::<Vec<_>>();
        let query_name = query_tokens.join("");
        let candidate_cap = MAX_INDEX_CANDIDATES_TO_SCORE;
        let (candidate_ids, candidate_cap_hit) = cap_candidate_ids(
            candidate_ids,
            candidate_cap,
            &self.files,
            &query_name,
            &query_tokens,
            &posting_lists,
            &path_lists,
            &trigram_lists,
        );
        let active_filters =
            query_plan_filters_for_candidates(&filters, &self.files, &candidate_ids);
        let filtered_candidate_ids = candidate_ids
            .into_iter()
            .filter(|file_id| {
                self.files
                    .get(*file_id as usize)
                    .is_some_and(|file| indexed_file_matches_filters(file, &filters))
            })
            .collect::<Vec<_>>();
        let filtered_candidate_count = filtered_candidate_ids.len();
        let mut results = filtered_candidate_ids
            .into_iter()
            .filter_map(|file_id| {
                self.score_file(
                    file_id,
                    &query_tokens,
                    &query_name,
                    &query_phrases,
                    &posting_lists,
                    &path_lists,
                    &trigram_lists,
                    filters.snippet,
                    false,
                    filters.symbol.as_deref(),
                    None,
                )
            })
            .collect::<Vec<_>>();
        let scored_candidate_count = results.len();
        if filters.require_all {
            results.retain(|result| result_matches_all_tokens(result, &query_tokens));
        }
        results.retain(|result| result_matches_symbol_filters(result, &filters));
        let final_match_count = results.len();

        Ok(indexed_query_plan(
            &query_tokens,
            &query_phrases,
            &query_trigrams,
            &token_postings,
            &symbol_postings,
            &symbol_kind_postings,
            &path_postings,
            &trigram_postings,
            &missing_terms,
            &missing_trigrams,
            use_trigrams,
            active_filters,
            filters.require_all,
            candidate_count,
            candidate_cap,
            candidate_cap_hit,
            filtered_candidate_count,
            scored_candidate_count,
            final_match_count,
        ))
    }

    fn search_filter_only(
        &self,
        limit: usize,
        filters: &SearchFilters,
        symbol_kind_postings: &[(&String, &Vec<Posting>)],
    ) -> Vec<SearchResult> {
        let candidate_ids = filter_only_candidate_ids(symbol_kind_postings);
        let mut results = match &candidate_ids {
            Some(candidate_ids) => candidate_ids
                .iter()
                .filter_map(|file_id| self.files.get(*file_id as usize))
                .filter_map(|file| indexed_filter_only_result(&self.root, file, filters))
                .collect::<Vec<_>>(),
            None => self
                .files
                .iter()
                .filter_map(|file| indexed_filter_only_result(&self.root, file, filters))
                .collect::<Vec<_>>(),
        };
        let final_match_count = results.len();
        if filters.explain {
            let candidate_count = candidate_ids
                .as_ref()
                .map(Vec::len)
                .unwrap_or(final_match_count);
            let query_plan = QueryPlan {
                strategy: if symbol_kind_postings.is_empty() {
                    "filter_scan".to_string()
                } else {
                    "symbol_kind_filter_postings".to_string()
                },
                require_all: filters.require_all,
                query_tokens: Vec::new(),
                query_phrases: Vec::new(),
                query_trigrams: Vec::new(),
                active_filters: query_plan_filters(filters),
                planned_postings: symbol_kind_postings
                    .iter()
                    .map(|(kind, postings)| plan_posting("symbol_kind", kind, postings))
                    .collect(),
                missing_terms: Vec::new(),
                missing_trigrams: Vec::new(),
                candidate_count,
                candidate_cap: MAX_INDEX_CANDIDATES_TO_SCORE,
                candidate_cap_hit: false,
                filtered_candidate_count: final_match_count,
                scored_candidate_count: final_match_count,
                final_match_count,
                repair_hints: filter_scan_repair_hints(
                    filters,
                    &self.symbol_kind_postings,
                    final_match_count,
                ),
                retry_requests: Vec::new(),
            };
            for result in &mut results {
                result.query_plan = Some(query_plan.clone());
            }
        }
        finalize_results(results, limit)
    }

    fn score_file(
        &self,
        file_id: u32,
        query_tokens: &[String],
        query_name: &str,
        query_phrases: &[String],
        posting_lists: &[(&str, &[Posting])],
        path_lists: &[(&str, &[Posting])],
        trigram_lists: &[(&str, &[Posting])],
        snippet_mode: SnippetMode,
        explain: bool,
        symbol_filter: Option<&str>,
        query_plan: Option<&QueryPlan>,
    ) -> Option<SearchResult> {
        let file = self.files.get(file_id as usize)?;
        let path_lower = &file.path_lower;
        let mut score = 0.0;
        let mut reasons = Vec::new();
        let mut signals = Vec::new();
        if !apply_phrase_matches(
            &file.path,
            &file.content,
            query_phrases,
            "content_phrase",
            16.0,
            &mut score,
            &mut reasons,
            &mut signals,
        ) {
            return None;
        }

        for &(token, postings) in posting_lists {
            let count = posting_count(postings, file_id);
            let mut token_score = 0.0;
            if count > 0 {
                let amount = 1.0 + (count as f64).ln();
                token_score += amount;
                signals.push(rank_signal("term_frequency", token, amount));
            }
            if path_lower.contains(token) {
                token_score += 8.0;
                signals.push(rank_signal("path_match", token, 8.0));
            }
            if token_score > 0.0 {
                score += token_score;
                reasons.push(token.to_string());
            }
        }
        for &(token, postings) in path_lists {
            let count = posting_count(postings, file_id);
            if count > 0 {
                let amount = 8.0 + (count as f64).ln();
                score += amount;
                signals.push(rank_signal("path_term", token, amount));
                if !reasons.iter().any(|reason| reason == token) {
                    reasons.push(token.to_string());
                }
            }
        }
        let mut trigram_score = 0.0;
        let mut trigram_hits = 0usize;
        for &(trigram, postings) in trigram_lists {
            let count = posting_count(postings, file_id);
            if count > 0 {
                trigram_score += 0.2 + (count as f64).ln() * 0.05;
                trigram_hits += 1;
                if explain {
                    signals.push(rank_signal("trigram_match", trigram, 0.2));
                }
            }
        }
        if trigram_hits > 0 {
            score += trigram_score;
            reasons.push(format!("trigrams:{trigram_hits}"));
        }
        for symbol in &file.symbols {
            if let Some((kind, amount)) = symbol_query_match_score(
                &symbol.normalized,
                &symbol.tokens,
                query_tokens,
                &query_name,
            ) {
                score += amount;
                reasons.push(format!("symbol:{}", symbol.name));
                signals.push(rank_signal(kind, &symbol.name, amount));
            }
        }
        if score == 0.0 {
            return None;
        }

        let symbol_line = symbol_filter.and_then(|wanted| indexed_symbol_filter_line(file, wanted));
        let snippet = symbol_line
            .and_then(|line| indexed_symbol_filter_snippet(&self.root, file, line, snippet_mode))
            .unwrap_or_else(|| {
                indexed_snippet(&self.root, file, query_tokens, query_phrases, snippet_mode)
            });
        let mut match_lines = indexed_match_lines(file, query_tokens, query_phrases, 16);
        if let Some(line) = symbol_line {
            match_lines.retain(|match_line| *match_line != line);
            match_lines.insert(0, line);
        }

        Some(SearchResult {
            path: file.path.clone(),
            score: round4(score),
            reason: format!("indexed match {}", reasons.join(", ")),
            snippet,
            line_range: None,
            match_lines,
            explanation: explain.then_some(signals),
            query_plan: query_plan.cloned(),
            duplicate_group: None,
            context: None,
            read_range: None,
            read_request: None,
            related_request: None,
            related_symbols_request: None,
        })
    }
}

impl FastIndexDisk {
    fn from_index(index: &FastIndex) -> Self {
        Self {
            version: INDEX_VERSION,
            root: index.root.clone(),
            files: index.files.clone(),
            postings: compress_posting_map(&index.postings),
            path_postings: compress_posting_map(&index.path_postings),
            trigram_postings: compress_posting_map(&index.trigram_postings),
            symbol_postings: compress_posting_map(&index.symbol_postings),
            symbol_kind_postings: compress_posting_map(&index.symbol_kind_postings),
        }
    }

    fn into_index(self) -> Result<FastIndex> {
        anyhow::ensure!(
            self.version == INDEX_VERSION
                || self.version == PREVIOUS_DISK_INDEX_VERSION
                || self.version == OLDER_DISK_INDEX_VERSION,
            "unsupported index version {}",
            self.version
        );
        Ok(FastIndex {
            version: INDEX_VERSION,
            root: self.root,
            files: self.files,
            postings: decompress_posting_map(self.postings)?,
            path_postings: decompress_posting_map(self.path_postings)?,
            trigram_postings: decompress_posting_map(self.trigram_postings)?,
            symbol_postings: decompress_posting_map(self.symbol_postings)?,
            symbol_kind_postings: decompress_posting_map(self.symbol_kind_postings)?,
        })
    }
}

fn load_raw_index(payload: &[u8]) -> Result<FastIndex> {
    let mut index = bincode::deserialize::<FastIndex>(payload)?;
    anyhow::ensure!(
        index.version == INDEX_VERSION || index.version == LEGACY_RAW_INDEX_VERSION,
        "unsupported index version {}",
        index.version
    );
    index.version = INDEX_VERSION;
    if index.symbol_postings.is_empty() {
        index.symbol_postings = rebuild_symbol_postings(&index.files);
    }
    if index.symbol_kind_postings.is_empty() {
        index.symbol_kind_postings = rebuild_symbol_kind_postings(&index.files);
    }
    Ok(index)
}

fn compress_posting_map(
    postings: &HashMap<String, Vec<Posting>>,
) -> HashMap<String, CompressedPostingList> {
    postings
        .iter()
        .map(|(term, postings)| (term.clone(), compress_postings(postings)))
        .collect()
}

fn indexed_source_bytes(files: &[IndexedPath]) -> u64 {
    files.iter().map(|file| file.size).sum()
}

fn total_posting_entries(postings: &HashMap<String, Vec<Posting>>) -> usize {
    postings.values().map(Vec::len).sum()
}

fn total_compressed_posting_bytes(postings: &HashMap<String, Vec<Posting>>) -> usize {
    postings
        .values()
        .map(|postings| compress_postings(postings).bytes.len())
        .sum()
}

fn decompress_posting_map(
    postings: HashMap<String, CompressedPostingList>,
) -> Result<HashMap<String, Vec<Posting>>> {
    postings
        .into_iter()
        .map(|(term, postings)| Ok((term, postings.decompress()?)))
        .collect()
}

fn compress_postings(postings: &[Posting]) -> CompressedPostingList {
    let mut bytes = Vec::with_capacity(postings.len() * 3);
    let mut previous_file_id = 0u32;
    for (index, posting) in postings.iter().enumerate() {
        let delta = if index == 0 {
            posting.file_id
        } else {
            posting.file_id.saturating_sub(previous_file_id)
        };
        encode_var_u32(delta, &mut bytes);
        encode_var_u32(posting.count as u32, &mut bytes);
        previous_file_id = posting.file_id;
    }
    CompressedPostingList {
        postings: postings.len().min(u32::MAX as usize) as u32,
        bytes,
    }
}

impl CompressedPostingList {
    fn decompress(self) -> Result<Vec<Posting>> {
        let mut postings = Vec::with_capacity(self.postings as usize);
        let mut offset = 0usize;
        let mut previous_file_id = 0u32;
        for index in 0..self.postings {
            let delta = decode_var_u32(&self.bytes, &mut offset)?;
            let count = decode_var_u32(&self.bytes, &mut offset)?;
            anyhow::ensure!(count <= u16::MAX as u32, "posting count is too large");
            anyhow::ensure!(
                index == 0 || delta > 0,
                "compressed posting list is not strictly increasing"
            );
            let file_id = if index == 0 {
                delta
            } else {
                previous_file_id
                    .checked_add(delta)
                    .ok_or_else(|| anyhow::anyhow!("posting file id overflow"))?
            };
            postings.push(Posting {
                file_id,
                count: count as u16,
            });
            previous_file_id = file_id;
        }
        anyhow::ensure!(
            offset == self.bytes.len(),
            "compressed posting list has trailing bytes"
        );
        Ok(postings)
    }
}

fn encode_var_u32(mut value: u32, bytes: &mut Vec<u8>) {
    while value >= 0x80 {
        bytes.push((value as u8 & 0x7f) | 0x80);
        value >>= 7;
    }
    bytes.push(value as u8);
}

fn decode_var_u32(bytes: &[u8], offset: &mut usize) -> Result<u32> {
    let mut value = 0u32;
    let mut shift = 0u32;
    loop {
        let Some(byte) = bytes.get(*offset).copied() else {
            anyhow::bail!("truncated varint");
        };
        *offset += 1;
        value |= ((byte & 0x7f) as u32) << shift;
        if byte & 0x80 == 0 {
            return Ok(value);
        }
        shift += 7;
        anyhow::ensure!(shift < 32, "varint is too large");
    }
}

fn normalize_posting_map(postings: &mut HashMap<String, Vec<Posting>>) {
    for values in postings.values_mut() {
        values.sort_unstable_by_key(|posting| posting.file_id);
        let mut normalized: Vec<Posting> = Vec::with_capacity(values.len());
        for posting in values.drain(..) {
            if let Some(previous) = normalized.last_mut() {
                if previous.file_id == posting.file_id {
                    previous.count = previous.count.max(posting.count);
                    continue;
                }
            }
            normalized.push(posting);
        }
        *values = normalized;
    }
}

#[cfg(test)]
mod compressed_posting_tests {
    use super::*;

    #[test]
    fn compressed_postings_round_trip_delta_varints() {
        let postings = vec![
            Posting {
                file_id: 3,
                count: 1,
            },
            Posting {
                file_id: 130,
                count: 7,
            },
            Posting {
                file_id: 16_384,
                count: 42,
            },
        ];

        assert_eq!(compress_postings(&postings).decompress().unwrap(), postings);
    }

    #[test]
    fn compressed_postings_reject_truncated_varints() {
        let list = CompressedPostingList {
            postings: 1,
            bytes: vec![0x80],
        };

        let error = list.decompress().unwrap_err().to_string();
        assert!(error.contains("truncated varint"), "{error}");
    }

    #[test]
    fn compressed_postings_reject_trailing_bytes() {
        let list = CompressedPostingList {
            postings: 1,
            bytes: vec![1, 1, 0],
        };

        let error = list.decompress().unwrap_err().to_string();
        assert!(error.contains("trailing bytes"), "{error}");
    }

    #[test]
    fn compressed_postings_reject_non_increasing_file_ids() {
        let list = CompressedPostingList {
            postings: 2,
            bytes: vec![1, 1, 0, 1],
        };

        let error = list.decompress().unwrap_err().to_string();
        assert!(error.contains("strictly increasing"), "{error}");
    }
}

fn normalize_index_relative_path(path: &str) -> Result<String> {
    let normalized_separators = path.replace('\\', "/");
    let requested = Path::new(&normalized_separators);
    anyhow::ensure!(requested.is_relative(), "path must be index-relative");

    let mut parts = Vec::new();
    for component in requested.components() {
        match component {
            Component::Normal(part) => {
                parts.push(part.to_string_lossy().to_string());
            }
            Component::CurDir => {}
            Component::ParentDir | Component::RootDir | Component::Prefix(_) => {
                anyhow::bail!("path must be index-relative");
            }
        }
    }
    let normalized = parts.join("/");
    anyhow::ensure!(!normalized.is_empty(), "path must be index-relative");
    Ok(normalized)
}

fn index_payload(bytes: &[u8]) -> Result<(&[u8], Option<u32>)> {
    if !bytes.starts_with(INDEX_MAGIC) {
        return Ok((bytes, None));
    }
    anyhow::ensure!(bytes.len() >= INDEX_HEADER_LEN, "index header is truncated");
    let version_offset = INDEX_MAGIC.len();
    let version = u32::from_le_bytes(
        bytes[version_offset..version_offset + std::mem::size_of::<u32>()]
            .try_into()
            .expect("header version length is fixed"),
    );
    Ok((&bytes[INDEX_HEADER_LEN..], Some(version)))
}

fn atomic_write(path: &Path, bytes: &[u8]) -> Result<()> {
    let tmp_path = temporary_index_path(path);
    let result = (|| -> Result<()> {
        let mut file = fs::File::create(&tmp_path)
            .with_context(|| format!("create temp index {}", tmp_path.display()))?;
        file.write_all(bytes)
            .with_context(|| format!("write temp index {}", tmp_path.display()))?;
        file.sync_all()
            .with_context(|| format!("sync temp index {}", tmp_path.display()))?;
        drop(file);
        fs::rename(&tmp_path, path).with_context(|| format!("replace index {}", path.display()))?;
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

fn temporary_index_path(path: &Path) -> PathBuf {
    let parent = path.parent().unwrap_or_else(|| Path::new("."));
    let file_name = path
        .file_name()
        .map(|value| value.to_string_lossy())
        .unwrap_or_else(|| "index".into());
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

fn rank_signal(kind: &str, value: &str, score: f64) -> RankSignal {
    RankSignal {
        kind: kind.to_string(),
        value: value.to_string(),
        score: round4(score),
    }
}

fn query_plan_filters(filters: &SearchFilters) -> Vec<QueryPlanFilter> {
    let mut active = Vec::new();
    if let Some(value) = &filters.file {
        active.push(plan_filter("file", value, false));
    }
    if let Some(value) = &filters.path {
        active.push(plan_filter("path", value, false));
    }
    if let Some(value) = &filters.language {
        active.push(plan_filter("language", value, false));
    }
    if let Some(value) = &filters.extension {
        active.push(plan_filter("extension", value, false));
    }
    if let Some(value) = &filters.symbol {
        active.push(plan_filter("symbol", value, false));
    }
    if let Some(value) = &filters.symbol_kind {
        active.push(plan_filter("symbol_kind", value, false));
    }
    if let Some(value) = &filters.repo {
        active.push(plan_filter("repo", value, false));
    }
    if let Some(value) = &filters.dependency {
        active.push(plan_filter("dependency", value, false));
    }
    if let Some(value) = &filters.import {
        active.push(plan_filter("import", value, false));
    }
    if let Some(value) = filters.test {
        active.push(plan_filter("test", &value.to_string(), false));
    }
    for value in &filters.exclude_file {
        active.push(plan_filter("file", value, true));
    }
    for value in &filters.exclude_path {
        active.push(plan_filter("path", value, true));
    }
    for value in &filters.exclude_language {
        active.push(plan_filter("language", value, true));
    }
    for value in &filters.exclude_extension {
        active.push(plan_filter("extension", value, true));
    }
    for value in &filters.exclude_symbol {
        active.push(plan_filter("symbol", value, true));
    }
    for value in &filters.exclude_symbol_kind {
        active.push(plan_filter("symbol_kind", value, true));
    }
    for value in &filters.exclude_repo {
        active.push(plan_filter("repo", value, true));
    }
    for value in &filters.exclude_dependency {
        active.push(plan_filter("dependency", value, true));
    }
    for value in &filters.exclude_import {
        active.push(plan_filter("import", value, true));
    }
    active
}

fn plan_filter(field: &str, value: &str, negated: bool) -> QueryPlanFilter {
    QueryPlanFilter {
        field: field.to_string(),
        value: value.to_string(),
        negated,
        candidate_matches: None,
        candidate_rejections: None,
    }
}

fn query_plan_filters_for_candidates(
    filters: &SearchFilters,
    files: &[IndexedPath],
    candidate_ids: &[u32],
) -> Vec<QueryPlanFilter> {
    let total = candidate_ids.len();
    query_plan_filters(filters)
        .into_iter()
        .map(|mut filter| {
            if !matches!(filter.field.as_str(), "repo" | "dependency") {
                let matched = candidate_ids
                    .iter()
                    .filter_map(|file_id| files.get(*file_id as usize))
                    .filter(|file| indexed_path_matches_plan_filter(file, &filter))
                    .count();
                filter.candidate_matches = Some(matched);
                filter.candidate_rejections = Some(total.saturating_sub(matched));
            }
            filter
        })
        .collect()
}

fn indexed_path_matches_plan_filter(file: &IndexedPath, filter: &QueryPlanFilter) -> bool {
    let matches = match filter.field.as_str() {
        "file" => filter_value_matches(&file.file_name_lower, &filter.value),
        "path" => filter_value_matches(&file.path_lower, &filter.value),
        "language" => file.language == filter.value.trim().to_ascii_lowercase(),
        "extension" => file.extension_lower.as_deref().is_some_and(|extension| {
            extension
                == filter
                    .value
                    .trim()
                    .trim_start_matches('.')
                    .to_ascii_lowercase()
        }),
        "symbol" => indexed_path_matches_symbol_filter(file, &filter.value),
        "symbol_kind" => indexed_path_matches_symbol_kind_filter(file, &filter.value),
        "import" => indexed_path_matches_import_filter(file, &filter.value),
        "test" => {
            let wanted = matches!(
                filter.value.to_ascii_lowercase().as_str(),
                "1" | "true" | "yes" | "y"
            );
            is_test_path(&file.path_lower) == wanted
        }
        _ => true,
    };
    matches != filter.negated
}

fn indexed_file_matches_filters(file: &IndexedPath, filters: &SearchFilters) -> bool {
    matches_filters_with_path_metadata(
        &file.path_lower,
        &file.file_name_lower,
        file.extension_lower.as_deref(),
        Some(&file.language),
        filters,
    ) && indexed_path_matches_symbol_kind_filters(file, filters)
        && source_import_filters_match(&file.path, &file.content, filters)
}

fn indexed_filter_only_result(
    root: &Path,
    file: &IndexedPath,
    filters: &SearchFilters,
) -> Option<SearchResult> {
    if !indexed_file_matches_filters(file, filters) {
        return None;
    }
    let matched = score_filter_only_path(&file.path, filters, filters.explain)?;
    let mut result = filter_only_search_result(
        &file.path,
        &file.content,
        matched,
        filters.snippet,
        filters.explain,
    );
    if let Some(symbol) = filters
        .symbol_kind
        .as_deref()
        .and_then(|kind| indexed_symbol_kind_filter_symbol(file, kind))
    {
        let line = symbol.line;
        if let Some(snippet) = indexed_symbol_filter_snippet(root, file, line, filters.snippet) {
            result.snippet = snippet;
            result.match_lines = vec![line];
        }
        result.reason.push_str(&format!(", symbol:{}", symbol.name));
    }
    Some(result)
}

fn indexed_path_matches_symbol_filter(file: &IndexedPath, wanted: &str) -> bool {
    let wanted = normalize_token(wanted);
    !wanted.is_empty()
        && file
            .symbols
            .iter()
            .any(|symbol| symbol.normalized == wanted || normalize_token(&symbol.name) == wanted)
}

fn indexed_path_matches_import_filter(file: &IndexedPath, wanted: &str) -> bool {
    source_import_filters_match(
        &file.path,
        &file.content,
        &SearchFilters {
            import: Some(wanted.to_string()),
            ..SearchFilters::default()
        },
    )
}

fn indexed_path_matches_symbol_kind_filters(file: &IndexedPath, filters: &SearchFilters) -> bool {
    if filters.symbol_kind.is_none() && filters.exclude_symbol_kind.is_empty() {
        return true;
    }
    if let Some(wanted) = &filters.symbol_kind {
        if !file
            .symbols
            .iter()
            .any(|symbol| symbol.kind.eq_ignore_ascii_case(wanted))
        {
            return false;
        }
    }
    !filters.exclude_symbol_kind.iter().any(|excluded| {
        file.symbols
            .iter()
            .any(|symbol| symbol.kind.eq_ignore_ascii_case(excluded))
    })
}

fn indexed_path_matches_symbol_kind_filter(file: &IndexedPath, wanted: &str) -> bool {
    indexed_path_matches_symbol_kind_filters(
        file,
        &SearchFilters {
            symbol_kind: Some(wanted.to_ascii_lowercase()),
            ..SearchFilters::default()
        },
    )
}

fn indexed_query_plan(
    query_tokens: &[String],
    query_phrases: &[String],
    query_trigrams: &[String],
    token_postings: &[(&String, &Vec<Posting>)],
    symbol_postings: &[(&String, &Vec<Posting>)],
    symbol_kind_postings: &[(&String, &Vec<Posting>)],
    path_postings: &[(&String, &Vec<Posting>)],
    trigram_postings: &[(&String, &Vec<Posting>)],
    missing_terms: &[String],
    missing_trigrams: &[String],
    use_trigrams: bool,
    active_filters: Vec<QueryPlanFilter>,
    require_all: bool,
    candidate_count: usize,
    candidate_cap: usize,
    candidate_cap_hit: bool,
    filtered_candidate_count: usize,
    scored_candidate_count: usize,
    final_match_count: usize,
) -> QueryPlan {
    let mut planned_postings = token_postings
        .iter()
        .map(|(token, postings)| plan_posting("content", token, postings))
        .chain(
            symbol_postings
                .iter()
                .map(|(symbol, postings)| plan_posting("symbol", symbol, postings)),
        )
        .chain(
            symbol_kind_postings
                .iter()
                .map(|(kind, postings)| plan_posting("symbol_kind", kind, postings)),
        )
        .chain(
            path_postings
                .iter()
                .map(|(token, postings)| plan_posting("path", token, postings)),
        )
        .chain(
            trigram_postings
                .iter()
                .take(8)
                .map(|(trigram, postings)| plan_posting("trigram", trigram, postings)),
        )
        .collect::<Vec<_>>();
    planned_postings.sort_by(|left, right| {
        left.postings
            .cmp(&right.postings)
            .then_with(|| left.kind.cmp(&right.kind))
            .then_with(|| left.value.cmp(&right.value))
    });
    planned_postings.truncate(16);

    QueryPlan {
        strategy: if use_trigrams && query_tokens.len() == 1 {
            "token_or_trigram_union".to_string()
        } else if require_all {
            "posting_intersection".to_string()
        } else {
            "posting_union".to_string()
        },
        require_all,
        query_tokens: query_tokens.to_vec(),
        query_phrases: query_phrases.to_vec(),
        query_trigrams: if use_trigrams {
            query_trigrams.to_vec()
        } else {
            Vec::new()
        },
        active_filters,
        planned_postings,
        missing_terms: missing_terms.to_vec(),
        missing_trigrams: missing_trigrams.to_vec(),
        candidate_count,
        candidate_cap,
        candidate_cap_hit,
        filtered_candidate_count,
        scored_candidate_count,
        final_match_count,
        repair_hints: query_plan_repair_hints(
            query_tokens,
            query_phrases,
            missing_terms,
            missing_trigrams,
            require_all,
            candidate_count,
            candidate_cap,
            candidate_cap_hit,
            filtered_candidate_count,
            scored_candidate_count,
            final_match_count,
        ),
        retry_requests: Vec::new(),
    }
}

fn query_plan_repair_hints(
    query_tokens: &[String],
    query_phrases: &[String],
    missing_terms: &[String],
    missing_trigrams: &[String],
    require_all: bool,
    candidate_count: usize,
    candidate_cap: usize,
    candidate_cap_hit: bool,
    filtered_candidate_count: usize,
    scored_candidate_count: usize,
    final_match_count: usize,
) -> Vec<QueryPlanRepairHint> {
    let mut hints = Vec::new();
    if candidate_cap_hit {
        hints.push(repair_hint(
            "narrow_query",
            format!(
                "The indexed planner found {candidate_count} candidates and capped scoring at {candidate_cap}. Add a rarer term or file/path/lang/ext/symbol filter for more complete results."
            ),
            suggested_token_query(query_tokens),
        ));
    }
    if final_match_count > 0 {
        return hints;
    }

    if !missing_terms.is_empty() {
        hints.push(repair_hint(
            "drop_missing_terms",
            format!(
                "Required terms have no content or path postings: {}. Drop or replace them before retrying.",
                missing_terms.join(", ")
            ),
            relaxed_query_without_terms(query_tokens, missing_terms),
        ));
    }
    if missing_terms.is_empty() && candidate_count == 0 && !missing_trigrams.is_empty() {
        hints.push(repair_hint(
            "shorten_substring",
            "The literal's trigrams are not present. Try a shorter substring, a symbol: filter, or separate identifier tokens.",
            None,
        ));
    }
    if require_all && query_tokens.len() > 1 && candidate_count == 0 && missing_terms.is_empty() {
        hints.push(repair_hint(
            "try_any_terms",
            "Each term has postings, but no file contains all terms. Retry with mode:any for broad orientation, then refine with file/path/symbol filters.",
            suggested_any_terms_query(query_tokens),
        ));
    }
    if candidate_count > 0 && filtered_candidate_count == 0 {
        hints.push(repair_hint(
            "relax_filters",
            "Posting candidates exist, but file/path/language/extension/test filters rejected all of them.",
            suggested_token_query(query_tokens),
        ));
    }
    if filtered_candidate_count > 0 && scored_candidate_count == 0 && !query_phrases.is_empty() {
        hints.push(repair_hint(
            "relax_phrase",
            "Filtered candidates exist, but exact quoted phrase verification rejected them. Try the phrase as separate tokens.",
            suggested_token_query(query_tokens),
        ));
    }
    if scored_candidate_count > 0 && final_match_count == 0 && require_all {
        hints.push(repair_hint(
            "relax_and",
            "Candidates scored, but final AND or symbol checks rejected them. Retry with mode:any or fewer terms.",
            suggested_any_terms_query(query_tokens).or_else(|| suggested_token_query(query_tokens)),
        ));
    }
    if hints.is_empty() {
        hints.push(repair_hint(
            "broaden_query",
            "No final matches were produced. Try fewer terms, looser filters, or inspect planned_postings for the rarest surviving term.",
            if require_all && query_tokens.len() > 1 {
                suggested_any_terms_query(query_tokens)
            } else {
                suggested_token_query(query_tokens)
            },
        ));
    }
    hints
}

fn filter_scan_repair_hints(
    filters: &SearchFilters,
    symbol_kind_postings: &HashMap<String, Vec<Posting>>,
    candidate_count: usize,
) -> Vec<QueryPlanRepairHint> {
    if candidate_count != 0 {
        return Vec::new();
    }
    if let Some(kind) = &filters.symbol_kind {
        if !symbol_kind_postings.contains_key(kind) {
            let suggested_query = suggested_symbol_kind_query(kind, symbol_kind_postings);
            let available = available_symbol_kinds(symbol_kind_postings);
            let message = if available.is_empty() {
                format!("No indexed symbols use kind `{kind}`.")
            } else {
                format!(
                    "No indexed symbols use kind `{kind}`. Available kinds: {}.",
                    available.join(", ")
                )
            };
            return vec![repair_hint(
                "replace_symbol_kind_filter",
                message,
                suggested_query,
            )];
        }
    }
    vec![repair_hint(
        "relax_filters",
        "No files matched the filter-only query. Relax file/path/language/extension/test filters.",
        None,
    )]
}

fn available_symbol_kinds(symbol_kind_postings: &HashMap<String, Vec<Posting>>) -> Vec<String> {
    let mut kinds = symbol_kind_postings.keys().cloned().collect::<Vec<_>>();
    kinds.sort();
    kinds.truncate(8);
    kinds
}

fn suggested_symbol_kind_query(
    wanted: &str,
    symbol_kind_postings: &HashMap<String, Vec<Posting>>,
) -> Option<String> {
    let wanted = wanted.trim().to_ascii_lowercase();
    if wanted.is_empty() {
        return None;
    }
    let mut best = symbol_kind_postings
        .keys()
        .map(|kind| (kind.as_str(), edit_distance_at_most(&wanted, kind, 3)))
        .filter_map(|(kind, distance)| distance.map(|distance| (kind, distance)))
        .collect::<Vec<_>>();
    best.sort_by(|left, right| {
        left.1
            .cmp(&right.1)
            .then_with(|| left.0.len().cmp(&right.0.len()))
            .then_with(|| left.0.cmp(right.0))
    });
    best.first()
        .filter(|(_, distance)| *distance <= 2)
        .map(|(kind, _)| format!("kind:{kind}"))
}

fn edit_distance_at_most(left: &str, right: &str, max_distance: usize) -> Option<usize> {
    if left.len().abs_diff(right.len()) > max_distance {
        return None;
    }
    let mut previous = (0..=right.len()).collect::<Vec<_>>();
    let mut current = vec![0; right.len() + 1];
    for (left_index, left_byte) in left.bytes().enumerate() {
        current[0] = left_index + 1;
        let mut row_min = current[0];
        for (right_index, right_byte) in right.bytes().enumerate() {
            let substitution = usize::from(left_byte != right_byte);
            current[right_index + 1] = (previous[right_index + 1] + 1)
                .min(current[right_index] + 1)
                .min(previous[right_index] + substitution);
            row_min = row_min.min(current[right_index + 1]);
        }
        if row_min > max_distance {
            return None;
        }
        std::mem::swap(&mut previous, &mut current);
    }
    (previous[right.len()] <= max_distance).then_some(previous[right.len()])
}

fn repair_hint(
    kind: impl Into<String>,
    message: impl Into<String>,
    suggested_query: Option<String>,
) -> QueryPlanRepairHint {
    QueryPlanRepairHint {
        kind: kind.into(),
        message: message.into(),
        suggested_query,
    }
}

fn relaxed_query_without_terms(
    query_tokens: &[String],
    missing_terms: &[String],
) -> Option<String> {
    let missing = missing_terms
        .iter()
        .map(String::as_str)
        .collect::<HashSet<_>>();
    let remaining = query_tokens
        .iter()
        .filter(|token| !missing.contains(token.as_str()))
        .cloned()
        .collect::<Vec<_>>();
    suggested_query_from_tokens(&remaining)
}

fn suggested_token_query(query_tokens: &[String]) -> Option<String> {
    suggested_query_from_tokens(query_tokens)
}

fn suggested_any_terms_query(query_tokens: &[String]) -> Option<String> {
    suggested_query_from_tokens(query_tokens).map(|query| format!("mode:any {query}"))
}

fn suggested_query_from_tokens(query_tokens: &[String]) -> Option<String> {
    (!query_tokens.is_empty()).then(|| query_tokens.join(" "))
}

fn missing_query_terms(
    query_tokens: &[String],
    token_postings: &[(&String, &Vec<Posting>)],
    symbol_postings: &[(&String, &Vec<Posting>)],
    path_postings: &[(&String, &Vec<Posting>)],
) -> Vec<String> {
    query_tokens
        .iter()
        .filter(|token| {
            !token_postings
                .iter()
                .any(|(posted, _)| posted.as_str() == token.as_str())
                && !symbol_postings
                    .iter()
                    .any(|(posted, _)| posted.as_str() == normalize_token(token).as_str())
                && !path_postings
                    .iter()
                    .any(|(posted, _)| posted.as_str() == token.as_str())
        })
        .cloned()
        .collect()
}

fn missing_query_trigrams(
    query_trigrams: &[String],
    trigram_postings: &[(&String, &Vec<Posting>)],
) -> Vec<String> {
    query_trigrams
        .iter()
        .filter(|trigram| {
            !trigram_postings
                .iter()
                .any(|(posted, _)| posted.as_str() == trigram.as_str())
        })
        .cloned()
        .collect()
}

fn exact_symbol_query_name(
    terms: &[String],
    explicit_symbol_filter: Option<&str>,
) -> Option<String> {
    if let Some(symbol) = explicit_symbol_filter {
        let normalized = normalize_token(symbol);
        return (!normalized.is_empty()).then_some(normalized);
    }
    if terms.len() != 1 {
        return None;
    }
    let term = terms.first()?.trim();
    if !identifier_shaped_query_term(term) {
        return None;
    }
    let normalized = normalize_token(term);
    (normalized.len() >= 3).then_some(normalized)
}

fn identifier_shaped_query_term(term: &str) -> bool {
    let mut previous = None;
    for ch in term.chars() {
        if matches!(ch, '_' | ':' | '.' | '#' | '-' | '/') {
            return true;
        }
        if previous.is_some_and(|prev: char| prev.is_ascii_lowercase() && ch.is_ascii_uppercase()) {
            return true;
        }
        previous = Some(ch);
    }
    false
}

fn symbol_kind_postings_for_filters<'a>(
    postings: &'a HashMap<String, Vec<Posting>>,
    filters: &SearchFilters,
) -> Vec<(&'a String, &'a Vec<Posting>)> {
    filters
        .symbol_kind
        .as_ref()
        .and_then(|kind| postings.get_key_value(kind))
        .into_iter()
        .collect()
}

fn filter_only_candidate_ids(
    symbol_kind_postings: &[(&String, &Vec<Posting>)],
) -> Option<Vec<u32>> {
    if symbol_kind_postings.is_empty() {
        return None;
    }
    Some(intersect_planned_postings(
        &symbol_kind_postings
            .iter()
            .map(|(_, postings)| *postings)
            .collect::<Vec<_>>(),
        true,
    ))
}

fn has_unsatisfied_missing_terms(missing_terms: &[String], filters: &SearchFilters) -> bool {
    let symbol = filters
        .symbol
        .as_ref()
        .map(|symbol| normalize_token(symbol));
    missing_terms.iter().any(|term| {
        symbol
            .as_ref()
            .is_none_or(|symbol| symbol.as_str() != term.as_str())
    })
}

fn plan_posting(kind: &str, value: &str, postings: &[Posting]) -> QueryPlanPosting {
    QueryPlanPosting {
        kind: kind.to_string(),
        value: value.to_string(),
        postings: postings.len(),
    }
}

fn index_file(
    root: &Path,
    rel: &str,
    language: String,
    size: u64,
    modified_secs: u64,
    modified_nanos: u32,
) -> Option<IndexedPath> {
    let text = fs::read_to_string(root.join(rel)).unwrap_or_default();
    if text.contains('\0') {
        return None;
    }
    let content_hash = content_hash(text.as_bytes());
    let line_offsets = line_offsets(&text);
    let term_lines = indexed_term_lines(&text);
    let mut terms = token_counts(&text)
        .into_iter()
        .map(|(term, count)| TermCount {
            term,
            count: count.min(u16::MAX as usize) as u16,
        })
        .collect::<Vec<_>>();
    terms.sort_by(|a, b| a.term.cmp(&b.term));
    let mut path_terms = token_counts(rel)
        .into_iter()
        .map(|(term, count)| TermCount {
            term,
            count: count.min(u16::MAX as usize) as u16,
        })
        .collect::<Vec<_>>();
    path_terms.sort_by(|a, b| a.term.cmp(&b.term));
    let mut trigrams = trigram_counts(&format!("{rel}\n{text}"))
        .into_iter()
        .map(|(term, count)| TermCount {
            term,
            count: count.min(u16::MAX as usize) as u16,
        })
        .collect::<Vec<_>>();
    trigrams.sort_by(|a, b| a.term.cmp(&b.term));
    let symbols = extract_symbols(rel, &text, &language)
        .into_iter()
        .map(|symbol| IndexedSymbol {
            normalized: normalize_token(&symbol.name),
            tokens: tokenize(&symbol.name),
            name: symbol.name,
            kind: symbol.kind,
            line: symbol.line,
        })
        .collect();

    Some(IndexedPath {
        path: rel.to_string(),
        path_lower: rel.to_ascii_lowercase(),
        file_name_lower: indexed_file_name_lower(rel),
        extension_lower: indexed_extension_lower(rel),
        language,
        size,
        content_hash,
        modified_secs,
        modified_nanos,
        terms,
        path_terms,
        trigrams,
        symbols,
        line_offsets,
        term_lines,
        content: text,
    })
}

fn retarget_indexed_file(
    mut previous: IndexedPath,
    candidate: &RefreshCandidate,
    text: &str,
) -> IndexedPath {
    previous.path = candidate.rel.clone();
    previous.path_lower = candidate.rel.to_ascii_lowercase();
    previous.file_name_lower = indexed_file_name_lower(&candidate.rel);
    previous.extension_lower = indexed_extension_lower(&candidate.rel);
    previous.language = candidate.language.clone();
    previous.size = candidate.size;
    previous.content_hash = content_hash(text.as_bytes());
    previous.modified_secs = candidate.modified_secs;
    previous.modified_nanos = candidate.modified_nanos;
    previous.path_terms = counted_terms(&token_counts(&candidate.rel));
    previous.trigrams = counted_terms(&trigram_counts(&format!("{}\n{text}", candidate.rel)));
    previous.term_lines = indexed_term_lines(text);
    previous.content = text.to_string();
    previous
}

fn refresh_indexed_path_metadata(file: &mut IndexedPath) {
    file.path_lower = file.path.to_ascii_lowercase();
    file.file_name_lower = indexed_file_name_lower(&file.path);
    file.extension_lower = indexed_extension_lower(&file.path);
}

fn indexed_file_name_lower(path: &str) -> String {
    Path::new(path)
        .file_name()
        .map(|value| value.to_string_lossy().to_ascii_lowercase())
        .unwrap_or_default()
}

fn indexed_extension_lower(path: &str) -> Option<String> {
    Path::new(path)
        .extension()
        .map(|value| value.to_string_lossy().to_ascii_lowercase())
}

fn indexed_term_lines(text: &str) -> Vec<IndexedTermLines> {
    let mut lines_by_term = HashMap::<String, Vec<u32>>::new();
    for (line_index, line) in text.lines().enumerate() {
        let line_number = (line_index + 1).min(u32::MAX as usize) as u32;
        for token in tokenize(line) {
            let lines = lines_by_term.entry(token).or_default();
            if lines.last().copied() != Some(line_number) && lines.len() < MAX_TERM_LINES_PER_TERM {
                lines.push(line_number);
            }
        }
    }
    let mut term_lines = lines_by_term
        .into_iter()
        .map(|(term, lines)| IndexedTermLines { term, lines })
        .collect::<Vec<_>>();
    term_lines.sort_by(|left, right| left.term.cmp(&right.term));
    term_lines
}

fn indexed_match_lines(
    file: &IndexedPath,
    query_tokens: &[String],
    query_phrases: &[String],
    limit: usize,
) -> Vec<usize> {
    if (query_tokens.is_empty() && query_phrases.is_empty()) || limit == 0 {
        return Vec::new();
    }
    let mut lines = indexed_line_scores(
        file,
        file.content.as_bytes(),
        &file.line_offsets,
        query_tokens,
        query_phrases,
    )
    .into_iter()
    .collect::<Vec<_>>();
    lines.sort_by_key(|(line, score)| (std::cmp::Reverse(*score), *line));
    let mut lines = lines.into_iter().map(|(line, _)| line).collect::<Vec<_>>();
    lines.truncate(limit);
    lines
}

fn counted_terms(counts: &HashMap<String, usize>) -> Vec<TermCount> {
    let mut terms = counts
        .iter()
        .map(|(term, count)| TermCount {
            term: term.clone(),
            count: (*count).min(u16::MAX as usize) as u16,
        })
        .collect::<Vec<_>>();
    terms.sort_by(|a, b| a.term.cmp(&b.term));
    terms
}

fn command_hints_from_indexed_files(files: &[IndexedPath]) -> Vec<crate::repo_index::CommandHint> {
    command_hints_from_manifest_texts(
        files
            .iter()
            .map(|file| (file.path.as_str(), file.content.as_str())),
    )
}

fn dependency_hints_from_indexed_files(
    files: &[IndexedPath],
) -> Vec<crate::repo_index::DependencyHint> {
    dependency_hints_from_manifest_texts(
        files
            .iter()
            .map(|file| (file.path.as_str(), file.content.as_str())),
    )
}

fn import_hints_from_indexed_files(files: &[IndexedPath]) -> Vec<crate::repo_index::ImportHint> {
    import_hints_from_source_texts(
        files
            .iter()
            .map(|file| (file.path.as_str(), file.content.as_str())),
    )
}

fn line_offsets(text: &str) -> Vec<u32> {
    let mut offsets = vec![0];
    for (index, byte) in text.bytes().enumerate() {
        if byte == b'\n' && index + 1 < text.len() {
            offsets.push((index + 1).min(u32::MAX as usize) as u32);
        }
    }
    offsets
}

fn content_hash(bytes: &[u8]) -> u64 {
    let mut hash = 0xcbf29ce484222325u64;
    for byte in bytes {
        hash ^= *byte as u64;
        hash = hash.wrapping_mul(0x100000001b3);
    }
    hash
}

fn indexed_snippet(
    root: &Path,
    file: &IndexedPath,
    query_tokens: &[String],
    query_phrases: &[String],
    mode: SnippetMode,
) -> String {
    let live_bytes = fs::read(root.join(&file.path)).ok().filter(|bytes| {
        bytes.len() as u64 == file.size && content_hash(bytes) == file.content_hash
    });
    let bytes = live_bytes
        .as_deref()
        .unwrap_or_else(|| file.content.as_bytes());
    if bytes.is_empty() || file.line_offsets.is_empty() {
        return String::new();
    }

    if matches!(mode, SnippetMode::Symbol) {
        let query_name = query_tokens.join("");
        if let Some(line) = file
            .symbols
            .iter()
            .find(|symbol| {
                symbol.normalized == query_name
                    || symbol
                        .tokens
                        .iter()
                        .any(|token| query_tokens.contains(token))
            })
            .map(|symbol| symbol.line)
        {
            return render_indexed_window(&bytes, &file.line_offsets, line, mode);
        }
    }

    if let Some(line) = best_matching_line(
        file,
        &bytes,
        &file.line_offsets,
        query_tokens,
        query_phrases,
    ) {
        return render_indexed_window(&bytes, &file.line_offsets, line, mode);
    }

    let text = String::from_utf8_lossy(&bytes);
    best_snippet_for_path_with_phrases(&file.path, &text, query_tokens, query_phrases, mode)
}

fn indexed_symbol_filter_line(file: &IndexedPath, wanted: &str) -> Option<usize> {
    let wanted = normalize_token(wanted);
    if wanted.is_empty() {
        return None;
    }
    file.symbols
        .iter()
        .find(|symbol| symbol.normalized == wanted)
        .map(|symbol| symbol.line)
}

fn indexed_symbol_kind_filter_symbol<'a>(
    file: &'a IndexedPath,
    wanted: &str,
) -> Option<&'a IndexedSymbol> {
    file.symbols
        .iter()
        .find(|symbol| symbol.kind.eq_ignore_ascii_case(wanted))
}

fn indexed_symbol_filter_snippet(
    root: &Path,
    file: &IndexedPath,
    line: usize,
    mode: SnippetMode,
) -> Option<String> {
    if file.line_offsets.is_empty() {
        return None;
    }
    let live_bytes = fs::read(root.join(&file.path)).ok().filter(|bytes| {
        bytes.len() as u64 == file.size && content_hash(bytes) == file.content_hash
    });
    let bytes = live_bytes
        .as_deref()
        .unwrap_or_else(|| file.content.as_bytes());
    Some(render_indexed_window(bytes, &file.line_offsets, line, mode))
}

fn best_matching_line(
    file: &IndexedPath,
    bytes: &[u8],
    offsets: &[u32],
    query_tokens: &[String],
    query_phrases: &[String],
) -> Option<usize> {
    indexed_line_scores(file, bytes, offsets, query_tokens, query_phrases)
        .into_iter()
        .max_by_key(|(line, score)| (*score, std::cmp::Reverse(*line)))
        .map(|(line, _)| line)
}

fn indexed_line_scores(
    file: &IndexedPath,
    bytes: &[u8],
    offsets: &[u32],
    query_tokens: &[String],
    query_phrases: &[String],
) -> HashMap<usize, usize> {
    let mut scores = HashMap::<usize, usize>::new();
    for token in query_tokens {
        if let Ok(index) = file
            .term_lines
            .binary_search_by(|entry| entry.term.as_str().cmp(token.as_str()))
        {
            for line in &file.term_lines[index].lines {
                *scores.entry(*line as usize).or_insert(0) += 1;
            }
        }
    }

    for phrase in query_phrases
        .iter()
        .filter(|phrase| phrase.split_whitespace().nth(1).is_some())
    {
        for (index, offset) in offsets.iter().enumerate() {
            let start = *offset as usize;
            let end = line_end(bytes, offsets, index);
            let line = String::from_utf8_lossy(&bytes[start..end]);
            let phrase_text = normalize_phrase_text(&line);
            if phrase_text.contains(phrase) {
                *scores.entry(index + 1).or_insert(0) += 100;
            }
        }
    }
    let query_name = query_tokens.join("");
    for symbol in &file.symbols {
        if let Some((kind, amount)) = symbol_query_match_score(
            &symbol.normalized,
            &symbol.tokens,
            query_tokens,
            &query_name,
        ) {
            let bonus = if kind == "symbol_exact" { 250 } else { 150 };
            let exact_phrase_bonus =
                symbol_exact_phrase_bonus(&symbol.name, query_phrases).unwrap_or(0);
            *scores.entry(symbol.line).or_insert(0) += bonus + exact_phrase_bonus + amount as usize;
        }
    }
    scores
}

fn render_indexed_window(
    bytes: &[u8],
    offsets: &[u32],
    center_line: usize,
    mode: SnippetMode,
) -> String {
    let (before, after) = mode.window();
    let line_count = offsets.len();
    let center = center_line.max(1).min(line_count);
    let start_line = center.saturating_sub(before).max(1);
    let end_line = (center + after).min(line_count);
    let mut rendered = Vec::new();

    for line in start_line..=end_line {
        let index = line - 1;
        let start = offsets[index] as usize;
        let end = line_end(bytes, offsets, index);
        let text = String::from_utf8_lossy(&bytes[start..end]);
        rendered.push(format!("{line}: {}", text.trim_end_matches(['\r', '\n'])));
    }

    rendered.join("\n").chars().take(mode.max_chars()).collect()
}

fn indexed_file_range(file: &IndexedPath, start_line: usize, line_count: usize) -> FileRange {
    let bytes = file.content.as_bytes();
    if bytes.is_empty() || file.line_offsets.is_empty() {
        return FileRange {
            path: file.path.clone(),
            start_line: 1,
            end_line: 0,
            total_lines: 0,
            text: String::new(),
        };
    }

    let total_lines = file.line_offsets.len();
    let start = start_line.max(1).min(total_lines.max(1));
    let count = line_count.max(1).min(MAX_READ_RANGE_LINES);
    let end_line = (start + count - 1).min(total_lines);
    let mut rendered = Vec::with_capacity(end_line.saturating_sub(start) + 1);

    for line in start..=end_line {
        let index = line - 1;
        let start_byte = file.line_offsets[index] as usize;
        let end_byte = line_end(bytes, &file.line_offsets, index);
        let text = String::from_utf8_lossy(&bytes[start_byte..end_byte]);
        rendered.push(format!("{line}: {}", text.trim_end_matches(['\r', '\n'])));
    }

    FileRange {
        path: file.path.clone(),
        start_line: start,
        end_line,
        total_lines,
        text: rendered.join("\n"),
    }
}

fn line_end(bytes: &[u8], offsets: &[u32], index: usize) -> usize {
    offsets
        .get(index + 1)
        .map(|offset| *offset as usize)
        .unwrap_or(bytes.len())
}

fn rebuild_postings(
    files: &[IndexedPath],
    terms_for: impl Fn(&IndexedPath) -> &[TermCount],
) -> HashMap<String, Vec<Posting>> {
    let mut postings: HashMap<String, Vec<Posting>> = HashMap::new();
    for (file_id, file) in files.iter().enumerate() {
        for term in terms_for(file) {
            postings
                .entry(term.term.clone())
                .or_default()
                .push(Posting {
                    file_id: file_id as u32,
                    count: term.count,
                });
        }
    }
    for values in postings.values_mut() {
        values.sort_unstable_by_key(|posting| posting.file_id);
    }
    postings
}

fn rebuild_symbol_postings(files: &[IndexedPath]) -> HashMap<String, Vec<Posting>> {
    let mut postings: HashMap<String, Vec<Posting>> = HashMap::new();
    for (file_id, file) in files.iter().enumerate() {
        let mut counts: HashMap<String, u16> = HashMap::new();
        for symbol in &file.symbols {
            if symbol.normalized.is_empty() {
                continue;
            }
            let count = counts.entry(symbol.normalized.clone()).or_default();
            *count = count.saturating_add(1);
        }
        for (symbol, count) in counts {
            postings.entry(symbol).or_default().push(Posting {
                file_id: file_id as u32,
                count,
            });
        }
    }
    for values in postings.values_mut() {
        values.sort_unstable_by_key(|posting| posting.file_id);
    }
    postings
}

fn rebuild_symbol_kind_postings(files: &[IndexedPath]) -> HashMap<String, Vec<Posting>> {
    let mut postings: HashMap<String, Vec<Posting>> = HashMap::new();
    for (file_id, file) in files.iter().enumerate() {
        let mut counts: HashMap<String, u16> = HashMap::new();
        for symbol in &file.symbols {
            let kind = symbol.kind.trim().to_ascii_lowercase();
            if kind.is_empty() {
                continue;
            }
            let count = counts.entry(kind).or_default();
            *count = count.saturating_add(1);
        }
        for (kind, count) in counts {
            postings.entry(kind).or_default().push(Posting {
                file_id: file_id as u32,
                count,
            });
        }
    }
    for values in postings.values_mut() {
        values.sort_unstable_by_key(|posting| posting.file_id);
    }
    postings
}

fn indexed_candidate_cap(limit: usize) -> usize {
    (limit.max(1) * 512).clamp(1_024, MAX_INDEX_CANDIDATES_TO_SCORE)
}

fn cap_candidate_ids(
    candidate_ids: Vec<u32>,
    candidate_cap: usize,
    files: &[IndexedPath],
    query_name: &str,
    query_tokens: &[String],
    posting_lists: &[(&str, &[Posting])],
    path_lists: &[(&str, &[Posting])],
    trigram_lists: &[(&str, &[Posting])],
) -> (Vec<u32>, bool) {
    let cap_hit = candidate_ids.len() > candidate_cap;
    if !cap_hit {
        return (candidate_ids, false);
    }

    let mut ranked = candidate_ids
        .into_iter()
        .map(|file_id| CandidateCapRank {
            file_id,
            score: candidate_rank_score(
                file_id,
                files,
                query_name,
                query_tokens,
                posting_lists,
                path_lists,
                trigram_lists,
            ),
        })
        .collect::<Vec<_>>();
    ranked.sort_by(|left, right| {
        right
            .score
            .partial_cmp(&left.score)
            .unwrap_or(Ordering::Equal)
            .then_with(|| {
                candidate_path(files, left.file_id).cmp(candidate_path(files, right.file_id))
            })
            .then_with(|| left.file_id.cmp(&right.file_id))
    });
    ranked.truncate(candidate_cap);
    let ids = ranked.into_iter().map(|rank| rank.file_id).collect();
    (ids, cap_hit)
}

#[derive(Debug, Clone, Copy)]
struct CandidateCapRank {
    file_id: u32,
    score: f64,
}

fn candidate_rank_score(
    file_id: u32,
    files: &[IndexedPath],
    query_name: &str,
    query_tokens: &[String],
    posting_lists: &[(&str, &[Posting])],
    path_lists: &[(&str, &[Posting])],
    trigram_lists: &[(&str, &[Posting])],
) -> f64 {
    let Some(file) = files.get(file_id as usize) else {
        return 0.0;
    };
    let path_lower = &file.path_lower;
    let mut score = 0.0;
    for &(token, postings) in posting_lists {
        let count = posting_count(postings, file_id);
        if count > 0 {
            score += 1.0 + (count as f64).ln();
        }
        if path_lower.contains(token) {
            score += 8.0;
        }
    }
    for &(_, postings) in path_lists {
        let count = posting_count(postings, file_id);
        if count > 0 {
            score += 8.0 + (count as f64).ln();
        }
    }
    for &(_, postings) in trigram_lists {
        let count = posting_count(postings, file_id);
        if count > 0 {
            score += 0.2 + (count as f64).ln() * 0.05;
        }
    }
    for symbol in &file.symbols {
        if let Some((_, amount)) =
            symbol_query_match_score(&symbol.normalized, &symbol.tokens, query_tokens, query_name)
        {
            score += amount;
        }
    }
    score
}

fn posting_count(postings: &[Posting], file_id: u32) -> u16 {
    postings
        .binary_search_by_key(&file_id, |posting| posting.file_id)
        .map(|index| postings[index].count)
        .unwrap_or_default()
}

fn candidate_path(files: &[IndexedPath], file_id: u32) -> &str {
    files
        .get(file_id as usize)
        .map(|file| file.path.as_str())
        .unwrap_or("")
}

fn intersect_planned_postings(planned: &[&Vec<Posting>], require_all: bool) -> Vec<u32> {
    let Some(first) = planned.first() else {
        return Vec::new();
    };
    if !require_all {
        let mut candidate_ids = Vec::new();
        for postings in planned {
            candidate_ids = union_candidates(candidate_ids, posting_file_ids(postings));
        }
        return candidate_ids;
    }
    let mut candidate_ids = posting_file_ids(first);
    for postings in planned.iter().skip(1) {
        candidate_ids = intersect_sorted_ids_with_postings(&candidate_ids, postings);
        if candidate_ids.is_empty() {
            break;
        }
    }
    candidate_ids
}

fn posting_file_ids(postings: &[Posting]) -> Vec<u32> {
    postings.iter().map(|posting| posting.file_id).collect()
}

fn intersect_sorted_ids_with_postings(left: &[u32], right: &[Posting]) -> Vec<u32> {
    let mut intersection = Vec::with_capacity(left.len().min(right.len()));
    let mut left_index = 0usize;
    let mut right_index = 0usize;
    while let (Some(left_id), Some(right_posting)) = (left.get(left_index), right.get(right_index))
    {
        match left_id.cmp(&right_posting.file_id) {
            Ordering::Less => left_index += 1,
            Ordering::Greater => right_index += 1,
            Ordering::Equal => {
                intersection.push(*left_id);
                left_index += 1;
                right_index += 1;
            }
        }
    }
    intersection
}

fn union_candidates(left: Vec<u32>, right: Vec<u32>) -> Vec<u32> {
    let mut merged = Vec::with_capacity(left.len() + right.len());
    let mut left_index = 0usize;
    let mut right_index = 0usize;
    while left_index < left.len() && right_index < right.len() {
        match left[left_index].cmp(&right[right_index]) {
            Ordering::Less => {
                merged.push(left[left_index]);
                left_index += 1;
            }
            Ordering::Greater => {
                merged.push(right[right_index]);
                right_index += 1;
            }
            Ordering::Equal => {
                merged.push(left[left_index]);
                left_index += 1;
                right_index += 1;
            }
        }
    }
    merged.extend_from_slice(&left[left_index..]);
    merged.extend_from_slice(&right[right_index..]);
    merged
}

fn query_trigrams(query: &str) -> Vec<String> {
    let mut trigrams = trigram_counts(query).into_keys().collect::<Vec<_>>();
    trigrams.sort();
    trigrams
}

fn trigram_counts(text: &str) -> HashMap<String, usize> {
    let mut counts = HashMap::new();
    let mut current = String::new();
    for ch in text.chars().flat_map(char::to_lowercase) {
        if ch.is_ascii_alphanumeric() || ch == '_' {
            current.push(ch);
            continue;
        }
        count_segment_trigrams(&current, &mut counts);
        current.clear();
    }
    count_segment_trigrams(&current, &mut counts);
    counts
}

fn count_segment_trigrams(segment: &str, counts: &mut HashMap<String, usize>) {
    let chars = segment.chars().collect::<Vec<_>>();
    if chars.len() < 3 {
        return;
    }
    for window in chars.windows(3) {
        let trigram = window.iter().collect::<String>();
        *counts.entry(trigram).or_default() += 1;
    }
}

fn modified_parts(modified: Option<SystemTime>) -> (u64, u32) {
    let duration = modified
        .and_then(|time| time.duration_since(UNIX_EPOCH).ok())
        .unwrap_or_default();
    (duration.as_secs(), duration.subsec_nanos())
}

fn modified_matches(file: &IndexedPath, modified: Option<SystemTime>) -> bool {
    let (secs, nanos) = modified_parts(modified);
    file.modified_secs == secs && file.modified_nanos == nanos
}
