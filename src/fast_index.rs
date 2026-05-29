//! Persistent local search index for agent-oriented code retrieval.

use crate::query::{merge_filters, normalize_phrase_text, parse_query, query_phrases, query_text};
use crate::repo_index::{
    FileRange, GENERATED_PATH_SCORE_MULTIPLIER, MAX_READ_RANGE_LINES, PathFilterMatcher, QueryPlan,
    QueryPlanFilter, QueryPlanPosting, QueryPlanRepairHint, RangeScope, RankSignal, RelatedFile,
    RelatedSymbol, RepoBrief, RepoMap, RepoMapDetail, SearchFilters, SearchResult, SnippetMode,
    Symbol, best_snippet_for_path_with_phrases, capped_search_limit,
    command_hints_from_manifest_texts, dependency_filters_match,
    dependency_hints_from_manifest_texts, extract_symbols, filter_only_query, filter_value_matches,
    finalize_results, import_hints_from_source_texts, is_entrypoint_path, is_generated_path,
    is_ignored, is_important_file, is_manifest_file, is_source_code_language, is_test_path,
    known_commands_from_hints, language_for, matches_filters_with_compiled_path_metadata,
    normalize_language_filter, normalize_token, referenced_symbol_name, regular_file_metadata,
    related_query_terms_symbol_and_filters, related_stem_terms, repo_map_seed_paths, repo_matches,
    result_matches_all_tokens, result_matches_symbol_filters, round4, score_filter_only_path_match,
    select_repo_brief_import_hints, select_repo_map_top_symbols,
    source_excluded_content_filters_match, source_import_filters_match, symbol_exact_phrase_bonus,
    symbol_for_anchor, symbol_matches_related_filters, symbol_query_match_score,
    symbol_scoped_window, token_counts, tokenize, unique_query_tokens,
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

const INDEX_VERSION: u32 = 13;
const PREVIOUS_DISK_INDEX_VERSION: u32 = 12;
const OLDER_DISK_INDEX_VERSION: u32 = 11;
const OLDEST_DISK_INDEX_VERSION: u32 = 10;
const LEGACY_RAW_INDEX_VERSION: u32 = 9;
const INDEX_MAGIC: &[u8] = b"ORIENTIDX\0";
const INDEX_HEADER_LEN: usize = INDEX_MAGIC.len() + std::mem::size_of::<u32>();
const MAX_FILE_BYTES: u64 = 512_000;
const MAX_TERM_LINES_PER_TERM: usize = 64;
const MAX_INDEX_CANDIDATES_TO_SCORE: usize = 8_192;
const DEFAULT_INDEXED_SYMBOL_READ_CONTEXT_BEFORE: usize = 20;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct FastIndex {
    pub version: u32,
    pub root: PathBuf,
    pub files: Vec<IndexedPath>,
    pub postings: HashMap<String, Vec<Posting>>,
    pub path_postings: HashMap<String, Vec<Posting>>,
    pub trigram_postings: HashMap<String, Vec<Posting>>,
    #[serde(skip)]
    path_trigram_postings: HashMap<String, Vec<Posting>>,
    #[serde(default)]
    pub symbol_postings: HashMap<String, Vec<Posting>>,
    #[serde(default)]
    pub symbol_kind_postings: HashMap<String, Vec<Posting>>,
    #[serde(default)]
    pub attribute_postings: HashMap<String, Vec<Posting>>,
    #[serde(skip)]
    path_ids: HashMap<String, u32>,
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
    #[serde(default)]
    attribute_postings: HashMap<String, CompressedPostingList>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
struct FastIndexDiskV12 {
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
    #[serde(skip)]
    pub name_lower: String,
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
pub struct RefreshStats {
    pub version: u32,
    pub root: PathBuf,
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

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct IndexFreshness {
    pub version: u32,
    pub root: PathBuf,
    pub root_exists: bool,
    pub stale: bool,
    pub indexed_files: usize,
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
        let path_trigram_postings = rebuild_path_trigram_postings(&files);
        let symbol_postings = rebuild_symbol_postings(&files);
        let symbol_kind_postings = rebuild_symbol_kind_postings(&files);
        let attribute_postings = rebuild_attribute_postings(&files);
        let path_ids = rebuild_path_ids(&files);
        Ok(Self {
            version: INDEX_VERSION,
            root,
            files,
            postings,
            path_postings,
            trigram_postings,
            path_trigram_postings,
            symbol_postings,
            symbol_kind_postings,
            attribute_postings,
            path_ids,
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

    pub fn load_reusable(path: impl AsRef<Path>) -> Result<Option<Self>> {
        match Self::load(path) {
            Ok(index) => Ok(Some(index)),
            Err(error) if rebuildable_load_error(&error) => Ok(None),
            Err(error) => Err(error),
        }
    }

    fn load_index_bytes(bytes: &[u8], path: &Path) -> Result<Self> {
        let (payload, header_version) = index_payload(bytes)
            .with_context(|| format!("parse index header {}", path.display()))?;
        let mut index = match header_version {
            Some(INDEX_VERSION) => bincode::deserialize::<FastIndexDisk>(payload)
                .with_context(|| format!("parse index {}", path.display()))?
                .into_index()
                .with_context(|| format!("decode index {}", path.display()))?,
            Some(
                PREVIOUS_DISK_INDEX_VERSION | OLDER_DISK_INDEX_VERSION | OLDEST_DISK_INDEX_VERSION,
            ) => bincode::deserialize::<FastIndexDiskV12>(payload)
                .with_context(|| format!("parse index {}", path.display()))?
                .into_index()
                .with_context(|| format!("decode index {}", path.display()))?,
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
            content_snapshot_bytes: indexed_content_snapshot_bytes(&self.files),
            line_offset_bytes: indexed_line_offset_bytes(&self.files),
            terms: self.postings.len(),
            path_terms: self.path_postings.len(),
            trigrams: self.trigram_postings.len(),
            posting_entries: total_posting_entries(&self.postings)
                + total_posting_entries(&self.path_postings)
                + total_posting_entries(&self.trigram_postings)
                + total_posting_entries(&self.symbol_postings)
                + total_posting_entries(&self.symbol_kind_postings)
                + total_posting_entries(&self.attribute_postings),
            compressed_posting_bytes: total_compressed_posting_bytes(&self.postings)
                + total_compressed_posting_bytes(&self.path_postings)
                + total_compressed_posting_bytes(&self.trigram_postings)
                + total_compressed_posting_bytes(&self.symbol_postings)
                + total_compressed_posting_bytes(&self.symbol_kind_postings)
                + total_compressed_posting_bytes(&self.attribute_postings),
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
        self.attribute_postings = rebuild_attribute_postings(&self.files);
        self.path_trigram_postings = rebuild_path_trigram_postings(&self.files);
        self.path_ids = rebuild_path_ids(&self.files);
    }

    pub fn refresh_stats(&self, outcome: &RefreshOutcome) -> RefreshStats {
        RefreshStats {
            version: self.version,
            root: self.root.clone(),
            files: self.files.len(),
            source_bytes: indexed_source_bytes(&self.files),
            content_snapshot_bytes: indexed_content_snapshot_bytes(&self.files),
            line_offset_bytes: indexed_line_offset_bytes(&self.files),
            terms: self.postings.len(),
            path_terms: self.path_postings.len(),
            trigrams: self.trigram_postings.len(),
            posting_entries: total_posting_entries(&self.postings)
                + total_posting_entries(&self.path_postings)
                + total_posting_entries(&self.trigram_postings)
                + total_posting_entries(&self.symbol_postings)
                + total_posting_entries(&self.symbol_kind_postings)
                + total_posting_entries(&self.attribute_postings),
            compressed_posting_bytes: total_compressed_posting_bytes(&self.postings)
                + total_compressed_posting_bytes(&self.path_postings)
                + total_compressed_posting_bytes(&self.trigram_postings)
                + total_compressed_posting_bytes(&self.symbol_postings)
                + total_compressed_posting_bytes(&self.symbol_kind_postings)
                + total_compressed_posting_bytes(&self.attribute_postings),
            symbols: self.files.iter().map(|file| file.symbols.len()).sum(),
            reused_files: outcome.reused_files,
            renamed_files: outcome.renamed_files,
            refreshed_files: outcome.refreshed_files,
            deleted_files: outcome.deleted_files,
        }
    }

    pub fn freshness(&self) -> Result<IndexFreshness> {
        self.freshness_with_index_bytes(0)
    }

    pub fn freshness_at(&self, index_path: impl AsRef<Path>) -> Result<IndexFreshness> {
        let index_bytes = fs::metadata(index_path.as_ref())
            .with_context(|| format!("stat index {}", index_path.as_ref().display()))?
            .len();
        self.freshness_with_index_bytes(index_bytes)
    }

    fn freshness_with_index_bytes(&self, index_bytes: u64) -> Result<IndexFreshness> {
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
                index_bytes,
                source_bytes: stats.source_bytes,
                content_snapshot_bytes: stats.content_snapshot_bytes,
                line_offset_bytes: stats.line_offset_bytes,
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
            index_bytes,
            source_bytes: stats.source_bytes,
            content_snapshot_bytes: stats.content_snapshot_bytes,
            line_offset_bytes: stats.line_offset_bytes,
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
        self.repo_map_with_detail(symbol_limit, test_limit, RepoMapDetail::Compact)
    }

    pub fn repo_map_with_detail(
        &self,
        symbol_limit: usize,
        test_limit: usize,
        detail: RepoMapDetail,
    ) -> RepoMap {
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

        let top_symbols = self
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
        let top_symbols = select_repo_map_top_symbols(top_symbols, symbol_limit);

        let mut related_file_seeds = important_files.clone();
        related_file_seeds.extend(top_symbols.iter().map(|symbol| symbol.path.clone()));
        let related_files =
            self.repo_map_related_files(&entrypoints, &test_files, &related_file_seeds, 12);
        let related_symbols =
            self.repo_map_related_symbols(&entrypoints, &test_files, &top_symbols, 12);

        let command_hints = command_hints_from_indexed_files(&self.files);
        let known_commands = known_commands_from_hints(&command_hints);
        let dependency_hints = dependency_hints_from_indexed_files(&self.files);
        let import_hints = match detail {
            RepoMapDetail::Compact => {
                select_repo_brief_import_hints(import_hints_from_indexed_files(&self.files))
            }
            RepoMapDetail::Full => import_hints_from_indexed_files(&self.files),
        };

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
            read_batch_request: None,
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

    fn indexed_file(&self, path: &str) -> Option<&IndexedPath> {
        self.path_ids
            .get(path)
            .and_then(|file_id| self.files.get(*file_id as usize))
            .filter(|file| file.path == path)
    }

    pub fn find_symbol(&self, name: &str, limit: usize) -> Vec<Symbol> {
        self.find_symbol_filtered(name, limit, &SearchFilters::default())
    }

    pub fn find_symbol_filtered(
        &self,
        name: &str,
        limit: usize,
        filters: &SearchFilters,
    ) -> Vec<Symbol> {
        let needle = normalize_token(name);
        if needle.is_empty()
            || limit == 0
            || !repo_matches(&self.root, filters)
            || !self.matches_dependency_filters(filters)
        {
            return Vec::new();
        }

        let mut scored = Vec::new();
        let mut seen = HashSet::new();
        let path_filters = PathFilterMatcher::from_filters(filters);

        if let Some(postings) = self.symbol_postings.get(&needle) {
            for posting in postings {
                let Some(file) = self.files.get(posting.file_id as usize) else {
                    continue;
                };
                if !indexed_file_matches_related_symbol_filters_compiled(
                    file,
                    filters,
                    &path_filters,
                ) {
                    continue;
                }
                for symbol in &file.symbols {
                    if symbol.normalized != needle {
                        continue;
                    }
                    if !indexed_symbol_matches_related_filters(symbol, filters) {
                        continue;
                    }
                    push_symbol_match(name, file, symbol, 90, &mut scored, &mut seen);
                }
            }
        }

        if scored.len() < limit {
            for file in &self.files {
                if !indexed_file_matches_related_symbol_filters_compiled(
                    file,
                    filters,
                    &path_filters,
                ) {
                    continue;
                }
                for symbol in &file.symbols {
                    if !indexed_symbol_matches_related_filters(symbol, filters) {
                        continue;
                    }
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
                        push_symbol_match(name, file, symbol, score, &mut scored, &mut seen);
                    }
                }
            }
        }

        scored_symbols(scored, limit)
    }

    pub fn read_range(
        &self,
        path: &str,
        start_line: usize,
        line_count: usize,
    ) -> Result<FileRange> {
        self.read_range_scoped(path, start_line, line_count, RangeScope::Exact)
    }

    pub fn read_range_scoped(
        &self,
        path: &str,
        start_line: usize,
        line_count: usize,
        scope: RangeScope,
    ) -> Result<FileRange> {
        let normalized = normalize_index_relative_path(path)?;
        let file = self
            .indexed_file(&normalized)
            .ok_or_else(|| anyhow::anyhow!("path is not present in index: {normalized}"))?;
        Ok(indexed_file_range_scoped(
            file, start_line, line_count, scope,
        ))
    }

    pub fn related_files(&self, path: &str, limit: usize) -> Vec<RelatedFile> {
        self.related_files_filtered(path, limit, &SearchFilters::default())
    }

    pub fn related_files_filtered(
        &self,
        path: &str,
        limit: usize,
        filters: &SearchFilters,
    ) -> Vec<RelatedFile> {
        if limit == 0 {
            return Vec::new();
        }

        let normalized = path.trim_start_matches('/').to_string();
        let Some(source_file) = self.indexed_file(&normalized) else {
            return Vec::new();
        };
        if !repo_matches(&self.root, filters) || !self.matches_dependency_filters(filters) {
            return Vec::new();
        }
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
        let source_symbols = source_file
            .symbols
            .iter()
            .map(|symbol| (symbol.name.clone(), symbol.name_lower.clone()))
            .collect::<Vec<_>>();
        let mut related = Vec::new();
        let path_filters = PathFilterMatcher::from_filters(filters);

        for file in &self.files {
            if file.path == normalized {
                continue;
            }
            if !indexed_file_matches_related_file_filters_compiled(file, filters, &path_filters) {
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
            if let Some(symbol) = referenced_symbol_name(&file.content, &source_symbols) {
                score += 6.0;
                reasons.push(format!("references symbol {symbol}"));
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
        self.related_symbols_filtered(path, query, limit, &SearchFilters::default())
    }

    pub fn related_symbols_filtered(
        &self,
        path: Option<&str>,
        query: Option<&str>,
        limit: usize,
        filters: &SearchFilters,
    ) -> Vec<RelatedSymbol> {
        if limit == 0 {
            return Vec::new();
        }

        let normalized_path = path.map(|value| value.trim_start_matches('/').to_string());
        if let Some(path) = &normalized_path {
            if self.indexed_file(path).is_none() {
                return Vec::new();
            }
        }
        let (query_terms, query_symbol, query_filters) =
            related_query_terms_symbol_and_filters(query);
        let filters = merge_filters(filters.clone(), query_filters);
        if !repo_matches(&self.root, &filters) || !self.matches_dependency_filters(&filters) {
            return Vec::new();
        }
        let query_tokens = query_terms.into_iter().collect::<HashSet<_>>();
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
        let mut seen = HashSet::new();
        let path_filters = PathFilterMatcher::from_filters(&filters);

        if normalized_path.is_none() && !query_symbol.is_empty() {
            if let Some(postings) = self.symbol_postings.get(&query_symbol) {
                for posting in postings {
                    let Some(file) = self.files.get(posting.file_id as usize) else {
                        continue;
                    };
                    if !indexed_file_matches_related_symbol_filters_compiled(
                        file,
                        &filters,
                        &path_filters,
                    ) {
                        continue;
                    }
                    for indexed_symbol in &file.symbols {
                        if indexed_symbol.normalized != query_symbol {
                            continue;
                        }
                        if !indexed_symbol_matches_related_filters(indexed_symbol, &filters) {
                            continue;
                        }
                        let symbol = Symbol {
                            name: indexed_symbol.name.clone(),
                            kind: indexed_symbol.kind.clone(),
                            path: file.path.clone(),
                            line: indexed_symbol.line,
                        };
                        let overlap = indexed_query_token_overlap(
                            &query_tokens,
                            &indexed_symbol.tokens,
                            &file.path_terms,
                        );
                        let mut score = 15.0 + 5.0 * overlap as f64;
                        let mut reasons = vec!["exact query symbol".to_string()];
                        if overlap > 0 {
                            reasons.push(format!("query overlap {overlap}"));
                        }
                        score += match symbol.kind.as_str() {
                            "class" | "struct" | "enum" | "interface" => 2.0,
                            _ => 0.0,
                        };
                        let key = (symbol.path.clone(), symbol.line, symbol.name.clone());
                        if seen.insert(key) {
                            related.push(RelatedSymbol {
                                symbol,
                                reason: reasons.join("; "),
                                score: round4(score),
                            });
                        }
                    }
                }
            }
        }

        if related.len() < limit {
            for file in &self.files {
                if !indexed_file_matches_related_symbol_filters_compiled(
                    file,
                    &filters,
                    &path_filters,
                ) {
                    continue;
                }
                for indexed_symbol in &file.symbols {
                    if !indexed_symbol_matches_related_filters(indexed_symbol, &filters) {
                        continue;
                    }
                    if seen.contains(&(
                        file.path.clone(),
                        indexed_symbol.line,
                        indexed_symbol.name.clone(),
                    )) {
                        continue;
                    }
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
                        if !path_stem.is_empty()
                            && (indexed_symbol.name_lower.contains(&path_stem)
                                || file.path_lower.contains(&path_stem))
                        {
                            score += 3.0;
                            reasons.push(format!("shares stem {path_stem}"));
                        }
                        if path_stem_terms.iter().any(|term| {
                            indexed_symbol.name_lower.contains(term)
                                || file.path_lower.contains(term)
                        }) {
                            score += 3.0;
                            reasons.push("shares normalized stem".to_string());
                        }
                    }

                    if !query_tokens.is_empty() {
                        let overlap = indexed_query_token_overlap(
                            &query_tokens,
                            &indexed_symbol.tokens,
                            &file.path_terms,
                        );
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
                        let key = (symbol.path.clone(), symbol.line, symbol.name.clone());
                        if seen.insert(key) {
                            related.push(RelatedSymbol {
                                symbol,
                                reason: reasons.join("; "),
                                score: round4(score),
                            });
                        }
                    }
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
        let attribute_postings = attribute_postings_for_filters(&self.attribute_postings, &filters);
        let path_filter_postings =
            path_filter_trigram_postings_for_filters(&self.path_trigram_postings, &filters);
        if path_filter_postings.is_impossible() {
            return Ok(Vec::new());
        }
        if limit == 0 {
            return Ok(Vec::new());
        }
        if query_tokens.is_empty() && query_trigrams.is_empty() {
            return if filter_only_query(&filters) {
                Ok(self.search_filter_only(
                    limit,
                    &filters,
                    &symbol_kind_postings,
                    &attribute_postings,
                    &path_filter_postings,
                ))
            } else {
                Ok(Vec::new())
            };
        }
        if query_tokens.len() > 1 && !filters.match_any {
            filters.require_all = true;
        }
        let allow_implicit_symbol_score =
            !parsed.explicit_content_terms || filters.symbol.is_some();
        let exact_symbol_query =
            planned_symbol_query_name(&parsed.terms, &filters, parsed.explicit_content_terms);

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
        if suppress_symbol_kind_trigram_fallback(
            &filters,
            &query_tokens,
            token_postings.len(),
            symbol_postings.len(),
            path_postings.len(),
        ) {
            trigram_postings.clear();
        }
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
        let candidate_trigram_postings = candidate_trigram_postings(
            &trigram_postings,
            &missing_trigrams,
            &query_tokens,
            use_trigrams,
        );

        let mut planned_postings = token_postings
            .iter()
            .map(|(_, postings)| *postings)
            .chain(symbol_postings.iter().map(|(_, postings)| *postings))
            .chain(path_plan_postings.iter().map(|(_, postings)| *postings))
            .chain(candidate_trigram_postings.iter().copied())
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
                union_candidates(
                    intersect_planned_postings(&token_only, false),
                    intersect_planned_postings(&candidate_trigram_postings, true),
                )
            } else {
                intersect_planned_postings(&planned_postings, filters.require_all)
            };
        let candidate_ids =
            intersect_symbol_kind_postings(candidate_ids, &symbol_kind_postings, &filters);
        let candidate_ids = intersect_attribute_postings(candidate_ids, &attribute_postings);
        let candidate_ids =
            intersect_path_filter_trigram_postings(candidate_ids, &path_filter_postings);
        let candidate_ids = filter_single_literal_trigram_candidates(
            candidate_ids,
            &self.files,
            &query_tokens,
            use_trigrams,
        );
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
        let active_filters =
            query_plan_filters_for_candidates(&filters, &self.files, &candidate_ids);
        let filtered_candidate_ids =
            indexed_filter_candidate_ids(&self.files, candidate_ids, &filters);
        let filtered_candidate_count = filtered_candidate_ids.len();
        let facet_candidate_ids = filtered_candidate_ids.clone();
        let candidate_cap = indexed_candidate_cap(limit);
        let (filtered_candidate_ids, candidate_cap_hit) = cap_candidate_ids(
            filtered_candidate_ids,
            candidate_cap,
            &self.files,
            &query_name,
            &query_tokens,
            &posting_lists,
            &path_lists,
            &trigram_lists,
        );
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
                    allow_implicit_symbol_score,
                    filters.generated.is_none(),
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
                &attribute_postings,
                &path_postings,
                &trigram_postings,
                &missing_terms,
                &missing_trigrams,
                use_trigrams,
                active_filters,
                &filters,
                &self.files,
                &facet_candidate_ids,
                &self.symbol_postings,
                &self.symbol_kind_postings,
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

    pub fn query_may_match(&self, query: &str, filters: &SearchFilters) -> bool {
        let parsed = parse_query(query);
        let mut filters = merge_filters(filters.clone(), parsed.filters);
        if !repo_matches(&self.root, &filters) || !self.matches_dependency_filters(&filters) {
            return false;
        }

        let query = query_text(&parsed.terms, &filters);
        let query_tokens = unique_query_tokens(&query);
        let query_trigrams = query_trigrams(&query);
        let symbol_kind_postings =
            symbol_kind_postings_for_filters(&self.symbol_kind_postings, &filters);
        if filters.symbol_kind.is_some() && symbol_kind_postings.is_empty() {
            return false;
        }
        let attribute_postings = attribute_postings_for_filters(&self.attribute_postings, &filters);
        if attribute_postings.is_impossible() {
            return false;
        }
        let path_filter_postings =
            path_filter_trigram_postings_for_filters(&self.path_trigram_postings, &filters);
        if path_filter_postings.is_impossible() {
            return false;
        }
        if query_tokens.is_empty() && query_trigrams.is_empty() {
            return filter_only_query(&filters);
        }
        if query_tokens.len() > 1 && !filters.match_any {
            filters.require_all = true;
        }
        let exact_symbol_query =
            planned_symbol_query_name(&parsed.terms, &filters, parsed.explicit_content_terms);

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
        if suppress_symbol_kind_trigram_fallback(
            &filters,
            &query_tokens,
            token_postings.len(),
            symbol_postings.len(),
            path_postings.len(),
        ) {
            trigram_postings.clear();
        }
        if token_postings.is_empty()
            && symbol_postings.is_empty()
            && path_postings.is_empty()
            && trigram_postings.is_empty()
        {
            return false;
        }
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
        if filters.require_all && has_unsatisfied_missing_terms(&missing_terms, &filters) {
            return false;
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
        let candidate_trigram_postings = candidate_trigram_postings(
            &trigram_postings,
            &missing_trigrams,
            &query_tokens,
            use_trigrams,
        );
        let mut planned_postings = token_postings
            .iter()
            .map(|(_, postings)| *postings)
            .chain(symbol_postings.iter().map(|(_, postings)| *postings))
            .chain(path_plan_postings.iter().map(|(_, postings)| *postings))
            .chain(candidate_trigram_postings.iter().copied())
            .collect::<Vec<_>>();
        planned_postings.sort_by_key(|postings| postings.len());

        let candidate_ids = if use_trigrams
            && (!token_postings.is_empty() || !path_postings.is_empty())
            && query_tokens.len() == 1
        {
            let token_only = token_postings
                .iter()
                .map(|(_, postings)| *postings)
                .chain(path_plan_postings.iter().map(|(_, postings)| *postings))
                .collect::<Vec<_>>();
            union_candidates(
                intersect_planned_postings(&token_only, false),
                intersect_planned_postings(&candidate_trigram_postings, true),
            )
        } else {
            intersect_planned_postings(&planned_postings, filters.require_all)
        };
        let candidate_ids =
            intersect_symbol_kind_postings(candidate_ids, &symbol_kind_postings, &filters);
        let candidate_ids = intersect_attribute_postings(candidate_ids, &attribute_postings);
        let candidate_ids =
            intersect_path_filter_trigram_postings(candidate_ids, &path_filter_postings);
        let candidate_ids = filter_single_literal_trigram_candidates(
            candidate_ids,
            &self.files,
            &query_tokens,
            use_trigrams,
        );
        !indexed_filter_candidate_ids(&self.files, candidate_ids, &filters).is_empty()
    }

    pub fn query_plan(&self, query: &str, filters: &SearchFilters) -> Result<QueryPlan> {
        let parsed = parse_query(query);
        let query_phrases = query_phrases(&parsed.terms);
        let mut filters = merge_filters(filters.clone(), parsed.filters);
        if !repo_matches(&self.root, &filters) {
            let retry_query = query_text(&parsed.terms, &filters);
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
                repair_hints: repo_scope_mismatch_repair_hints(&filters, &retry_query),
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
        let attribute_postings = attribute_postings_for_filters(&self.attribute_postings, &filters);
        let path_filter_postings =
            path_filter_trigram_postings_for_filters(&self.path_trigram_postings, &filters);
        if query_tokens.len() > 1 && !filters.match_any {
            filters.require_all = true;
        }
        let allow_implicit_symbol_score =
            !parsed.explicit_content_terms || filters.symbol.is_some();
        let exact_symbol_query =
            planned_symbol_query_name(&parsed.terms, &filters, parsed.explicit_content_terms);
        if query_tokens.is_empty() && query_trigrams.is_empty() {
            if filter_only_query(&filters) {
                let candidate_ids = filter_only_candidate_ids(
                    &symbol_kind_postings,
                    &attribute_postings,
                    &path_filter_postings,
                    &filters,
                );
                let path_filters = PathFilterMatcher::from_filters(&filters);
                let final_match_count = match &candidate_ids {
                    Some(candidate_ids) => candidate_ids
                        .iter()
                        .filter_map(|file_id| self.files.get(*file_id as usize))
                        .filter(|file| {
                            indexed_file_matches_filters_compiled(file, &filters, &path_filters)
                        })
                        .count(),
                    None => self
                        .files
                        .iter()
                        .filter(|file| {
                            indexed_file_matches_filters_compiled(file, &filters, &path_filters)
                        })
                        .count(),
                };
                let candidate_count = candidate_ids
                    .as_ref()
                    .map(Vec::len)
                    .unwrap_or(final_match_count);
                return Ok(QueryPlan {
                    strategy: filter_only_strategy(&filters, &path_filter_postings).to_string(),
                    require_all: filters.require_all,
                    query_tokens,
                    query_phrases,
                    query_trigrams,
                    active_filters: query_plan_filters(&filters),
                    planned_postings: symbol_kind_postings
                        .iter()
                        .map(|(kind, postings)| plan_posting("symbol_kind", kind, postings))
                        .chain(attribute_postings.plan_postings())
                        .chain(path_filter_postings.plan_postings())
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
                        &self.files,
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
        if suppress_symbol_kind_trigram_fallback(
            &filters,
            &query_tokens,
            token_postings.len(),
            symbol_postings.len(),
            path_postings.len(),
        ) {
            trigram_postings.clear();
        }
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
        let candidate_trigram_postings = candidate_trigram_postings(
            &trigram_postings,
            &missing_trigrams,
            &query_tokens,
            use_trigrams,
        );
        let mut planned_postings = token_postings
            .iter()
            .map(|(_, postings)| *postings)
            .chain(symbol_postings.iter().map(|(_, postings)| *postings))
            .chain(path_plan_postings.iter().map(|(_, postings)| *postings))
            .chain(candidate_trigram_postings.iter().copied())
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
                union_candidates(
                    intersect_planned_postings(&token_only, false),
                    intersect_planned_postings(&candidate_trigram_postings, true),
                )
            } else {
                intersect_planned_postings(&planned_postings, filters.require_all)
            };
        let candidate_ids =
            intersect_symbol_kind_postings(candidate_ids, &symbol_kind_postings, &filters);
        let candidate_ids = intersect_attribute_postings(candidate_ids, &attribute_postings);
        let candidate_ids =
            intersect_path_filter_trigram_postings(candidate_ids, &path_filter_postings);
        let candidate_ids = filter_single_literal_trigram_candidates(
            candidate_ids,
            &self.files,
            &query_tokens,
            use_trigrams,
        );

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
        let active_filters =
            query_plan_filters_for_candidates(&filters, &self.files, &candidate_ids);
        let filtered_candidate_ids =
            indexed_filter_candidate_ids(&self.files, candidate_ids, &filters);
        let filtered_candidate_count = filtered_candidate_ids.len();
        let facet_candidate_ids = filtered_candidate_ids.clone();
        let candidate_cap = MAX_INDEX_CANDIDATES_TO_SCORE;
        let (filtered_candidate_ids, candidate_cap_hit) = cap_candidate_ids(
            filtered_candidate_ids,
            candidate_cap,
            &self.files,
            &query_name,
            &query_tokens,
            &posting_lists,
            &path_lists,
            &trigram_lists,
        );
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
                    allow_implicit_symbol_score,
                    filters.generated.is_none(),
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
            &attribute_postings,
            &path_postings,
            &trigram_postings,
            &missing_terms,
            &missing_trigrams,
            use_trigrams,
            active_filters,
            &filters,
            &self.files,
            &facet_candidate_ids,
            &self.symbol_postings,
            &self.symbol_kind_postings,
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
        attribute_postings: &AttributeFilterPostings<'_>,
        path_filter_postings: &PathFilterTrigramPostings<'_>,
    ) -> Vec<SearchResult> {
        let candidate_ids = filter_only_candidate_ids(
            symbol_kind_postings,
            attribute_postings,
            path_filter_postings,
            filters,
        );
        let mut results = match &candidate_ids {
            Some(candidate_ids) => candidate_ids
                .iter()
                .filter_map(|file_id| self.files.get(*file_id as usize))
                .filter_map(|file| indexed_filter_only_result(file, filters))
                .collect::<Vec<_>>(),
            None => self
                .files
                .iter()
                .filter_map(|file| indexed_filter_only_result(file, filters))
                .collect::<Vec<_>>(),
        };
        let final_match_count = results.len();
        if filters.explain {
            let candidate_count = candidate_ids
                .as_ref()
                .map(Vec::len)
                .unwrap_or(final_match_count);
            let query_plan = QueryPlan {
                strategy: filter_only_strategy(filters, path_filter_postings).to_string(),
                require_all: filters.require_all,
                query_tokens: Vec::new(),
                query_phrases: Vec::new(),
                query_trigrams: Vec::new(),
                active_filters: query_plan_filters(filters),
                planned_postings: symbol_kind_postings
                    .iter()
                    .map(|(kind, postings)| plan_posting("symbol_kind", kind, postings))
                    .chain(attribute_postings.plan_postings())
                    .chain(path_filter_postings.plan_postings())
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
                    &self.files,
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
        allow_implicit_symbol_score: bool,
        demote_generated: bool,
        query_plan: Option<&QueryPlan>,
    ) -> Option<SearchResult> {
        let file = self.files.get(file_id as usize)?;
        let path_lower = &file.path_lower;
        let mut score = 0.0;
        let mut reasons = Vec::new();
        let mut signals = Vec::new();
        if !indexed_apply_phrase_matches(
            file,
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
        if allow_implicit_symbol_score {
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
        }
        if score == 0.0 {
            return None;
        }
        if demote_generated && is_generated_path(path_lower) {
            score *= GENERATED_PATH_SCORE_MULTIPLIER;
            if explain {
                signals.push(rank_signal(
                    "generated_path_penalty",
                    &file.path,
                    -1.0 + GENERATED_PATH_SCORE_MULTIPLIER,
                ));
            }
        }

        let symbol_line = symbol_filter.and_then(|wanted| indexed_symbol_filter_line(file, wanted));
        let snippet = symbol_line
            .and_then(|line| indexed_symbol_filter_snippet(file, line, snippet_mode))
            .unwrap_or_else(|| indexed_snippet(file, query_tokens, query_phrases, snippet_mode));
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
            attribute_postings: compress_posting_map(&index.attribute_postings),
        }
    }

    fn into_index(self) -> Result<FastIndex> {
        anyhow::ensure!(
            self.version == INDEX_VERSION
                || self.version == PREVIOUS_DISK_INDEX_VERSION
                || self.version == OLDER_DISK_INDEX_VERSION
                || self.version == OLDEST_DISK_INDEX_VERSION,
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
            path_trigram_postings: HashMap::new(),
            symbol_postings: decompress_posting_map(self.symbol_postings)?,
            symbol_kind_postings: decompress_posting_map(self.symbol_kind_postings)?,
            attribute_postings: decompress_posting_map(self.attribute_postings)?,
            path_ids: HashMap::new(),
        })
    }
}

impl FastIndexDiskV12 {
    #[cfg(test)]
    fn from_index_with_version(index: &FastIndex, version: u32) -> Self {
        Self {
            version,
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
            self.version == PREVIOUS_DISK_INDEX_VERSION
                || self.version == OLDER_DISK_INDEX_VERSION
                || self.version == OLDEST_DISK_INDEX_VERSION,
            "unsupported index version {}",
            self.version
        );
        let files = self.files;
        let attribute_postings = rebuild_attribute_postings(&files);
        Ok(FastIndex {
            version: INDEX_VERSION,
            root: self.root,
            postings: decompress_posting_map(self.postings)?,
            path_postings: decompress_posting_map(self.path_postings)?,
            trigram_postings: decompress_posting_map(self.trigram_postings)?,
            path_trigram_postings: HashMap::new(),
            symbol_postings: decompress_posting_map(self.symbol_postings)?,
            symbol_kind_postings: decompress_posting_map(self.symbol_kind_postings)?,
            attribute_postings,
            files,
            path_ids: HashMap::new(),
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
    if index.attribute_postings.is_empty() && !index.files.is_empty() {
        index.attribute_postings = rebuild_attribute_postings(&index.files);
    }
    Ok(index)
}

fn rebuildable_load_error(error: &anyhow::Error) -> bool {
    error.chain().any(|cause| {
        let message = cause.to_string();
        message.starts_with("parse index ")
            || message.starts_with("parse index header ")
            || message.starts_with("decode index ")
            || message.starts_with("unsupported index version ")
    })
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

fn indexed_content_snapshot_bytes(files: &[IndexedPath]) -> u64 {
    files.iter().map(|file| file.content.len() as u64).sum()
}

fn indexed_line_offset_bytes(files: &[IndexedPath]) -> usize {
    files
        .iter()
        .map(|file| file.line_offsets.len() * std::mem::size_of::<u32>())
        .sum()
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

    #[test]
    fn version_12_disk_indexes_rebuild_attribute_postings_on_load() {
        let repo = tempfile::tempdir().unwrap();
        let source = repo.path().join("src/lib.rs");
        std::fs::create_dir_all(source.parent().unwrap()).unwrap();
        std::fs::write(&source, "pub fn sharedneedle() {}\n").unwrap();
        let docs = repo.path().join("README.md");
        std::fs::write(&docs, "sharedneedle docs\n").unwrap();

        let index = FastIndex::build(repo.path()).unwrap();
        let old_disk =
            FastIndexDiskV12::from_index_with_version(&index, PREVIOUS_DISK_INDEX_VERSION);
        let mut bytes = Vec::new();
        bytes.extend_from_slice(INDEX_MAGIC);
        bytes.extend_from_slice(&PREVIOUS_DISK_INDEX_VERSION.to_le_bytes());
        bytes.extend_from_slice(&bincode::serialize(&old_disk).unwrap());
        let path = repo.path().join(".orient/v12.index");
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        std::fs::write(&path, bytes).unwrap();

        let loaded = FastIndex::load(&path).unwrap();
        assert_eq!(loaded.version, INDEX_VERSION);
        assert!(loaded.attribute_postings.contains_key("code:true"));
        assert!(loaded.attribute_postings.contains_key("code:false"));
        assert_eq!(
            loaded
                .query_plan("sharedneedle code:false", &SearchFilters::default())
                .unwrap()
                .candidate_count,
            1
        );
    }

    #[test]
    fn current_disk_indexes_rebuild_stale_attribute_postings_on_load() {
        let repo = tempfile::tempdir().unwrap();
        let source = repo.path().join("src/lib.rs");
        std::fs::create_dir_all(source.parent().unwrap()).unwrap();
        std::fs::write(&source, "pub fn sharedneedle() {}\n").unwrap();
        let bundle = repo
            .path()
            .join("webview/assets/chunk-OIYGIGL5-CJrBIAxA.js");
        std::fs::create_dir_all(bundle.parent().unwrap()).unwrap();
        std::fs::write(&bundle, "function sharedneedle() {}\n").unwrap();

        let index = FastIndex::build(repo.path()).unwrap();
        let mut disk = FastIndexDisk::from_index(&index);
        disk.attribute_postings
            .retain(|key, _| !key.starts_with("generated:"));
        let mut bytes = Vec::new();
        bytes.extend_from_slice(INDEX_MAGIC);
        bytes.extend_from_slice(&INDEX_VERSION.to_le_bytes());
        bytes.extend_from_slice(&bincode::serialize(&disk).unwrap());
        let path = repo.path().join(".orient/current.index");
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        std::fs::write(&path, bytes).unwrap();

        let loaded = FastIndex::load(&path).unwrap();
        assert!(loaded.attribute_postings.contains_key("generated:true"));
        assert_eq!(
            loaded
                .query_plan("sharedneedle is:generated", &SearchFilters::default())
                .unwrap()
                .candidate_count,
            1
        );
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

fn push_symbol_match(
    query: &str,
    file: &IndexedPath,
    symbol: &IndexedSymbol,
    score: i32,
    scored: &mut Vec<(i32, Symbol)>,
    seen: &mut HashSet<(String, usize, String)>,
) {
    if score <= 0 || !seen.insert((file.path.clone(), symbol.line, symbol.name.clone())) {
        return;
    }
    scored.push((
        if symbol.name == query { 100 } else { score },
        Symbol {
            name: symbol.name.clone(),
            kind: symbol.kind.clone(),
            path: file.path.clone(),
            line: symbol.line,
        },
    ));
}

fn scored_symbols(mut scored: Vec<(i32, Symbol)>, limit: usize) -> Vec<Symbol> {
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
    if let Some(value) = &filters.branch {
        active.push(plan_filter("branch", value, false));
    }
    if let Some(value) = &filters.origin {
        active.push(plan_filter("origin", value, false));
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
    if let Some(value) = filters.generated {
        active.push(plan_filter("generated", &value.to_string(), false));
    }
    if let Some(value) = filters.code {
        active.push(plan_filter("code", &value.to_string(), false));
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
    for value in &filters.exclude_branch {
        active.push(plan_filter("branch", value, true));
    }
    for value in &filters.exclude_origin {
        active.push(plan_filter("origin", value, true));
    }
    for value in &filters.exclude_dependency {
        active.push(plan_filter("dependency", value, true));
    }
    for value in &filters.exclude_import {
        active.push(plan_filter("import", value, true));
    }
    for value in &filters.exclude_content {
        active.push(plan_filter("content", value, true));
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
            if !matches!(
                filter.field.as_str(),
                "repo" | "branch" | "origin" | "dependency"
            ) {
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
        "language" => file.language == normalize_language_filter(&filter.value),
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
        "content" => {
            !filter.value.trim().is_empty()
                && !source_excluded_content_filters_match(
                    &file.content,
                    &SearchFilters {
                        exclude_content: vec![filter.value.clone()],
                        ..SearchFilters::default()
                    },
                )
        }
        "test" => {
            let wanted = matches!(
                filter.value.to_ascii_lowercase().as_str(),
                "1" | "true" | "yes" | "y"
            );
            is_test_path(&file.path_lower) == wanted
        }
        "generated" => {
            let wanted = matches!(
                filter.value.to_ascii_lowercase().as_str(),
                "1" | "true" | "yes" | "y"
            );
            is_generated_path(&file.path_lower) == wanted
        }
        "code" => {
            let wanted = matches!(
                filter.value.to_ascii_lowercase().as_str(),
                "1" | "true" | "yes" | "y"
            );
            crate::repo_index::is_source_code_language(&file.language) == wanted
        }
        _ => true,
    };
    matches != filter.negated
}

fn indexed_filter_candidate_ids(
    files: &[IndexedPath],
    candidate_ids: Vec<u32>,
    filters: &SearchFilters,
) -> Vec<u32> {
    let path_filters = PathFilterMatcher::from_filters(filters);
    candidate_ids
        .into_iter()
        .filter(|file_id| {
            files.get(*file_id as usize).is_some_and(|file| {
                indexed_file_matches_filters_compiled(file, filters, &path_filters)
            })
        })
        .collect()
}

fn indexed_apply_phrase_matches(
    file: &IndexedPath,
    query_phrases: &[String],
    content_signal_kind: &str,
    content_score: f64,
    score: &mut f64,
    reasons: &mut Vec<String>,
    signals: &mut Vec<RankSignal>,
) -> bool {
    if query_phrases.is_empty() {
        return true;
    }

    let path_phrase_text = normalize_phrase_text(&file.path_lower);
    let mut matches = Vec::with_capacity(query_phrases.len());
    for phrase in query_phrases {
        let path_match = path_phrase_text.contains(phrase);
        let content_match = indexed_content_contains_phrase(file, phrase);
        if !path_match && !content_match {
            return false;
        }
        matches.push((phrase, path_match, content_match));
    }

    for (phrase, path_match, content_match) in matches {
        let reason = format!("phrase:{phrase}");
        if !reasons.contains(&reason) {
            reasons.push(reason);
        }
        if path_match {
            *score += 10.0;
            signals.push(rank_signal("path_phrase", phrase, 10.0));
        }
        if content_match {
            *score += content_score;
            signals.push(rank_signal(content_signal_kind, phrase, content_score));
        }
    }
    true
}

fn indexed_content_contains_phrase(file: &IndexedPath, phrase: &str) -> bool {
    if file.term_lines.is_empty() || file.line_offsets.is_empty() {
        return normalize_phrase_text(&file.content).contains(phrase);
    }
    let (lines, capped) = indexed_phrase_candidate_lines(file, phrase);
    if lines.is_empty() {
        return false;
    }
    if !indexed_phrase_match_lines_from_candidates(
        file.content.as_bytes(),
        &file.line_offsets,
        phrase,
        &lines,
    )
    .is_empty()
    {
        return true;
    }
    capped && normalize_phrase_text(&file.content).contains(phrase)
}

fn indexed_file_matches_filters(file: &IndexedPath, filters: &SearchFilters) -> bool {
    let path_filters = PathFilterMatcher::from_filters(filters);
    indexed_file_matches_filters_compiled(file, filters, &path_filters)
}

fn indexed_file_matches_filters_compiled(
    file: &IndexedPath,
    filters: &SearchFilters,
    path_filters: &PathFilterMatcher,
) -> bool {
    matches_filters_with_compiled_path_metadata(
        &file.path_lower,
        &file.file_name_lower,
        file.extension_lower.as_deref(),
        Some(&file.language),
        path_filters,
    ) && indexed_path_matches_symbol_kind_filters(file, filters)
        && source_import_filters_match(&file.path, &file.content, filters)
        && source_excluded_content_filters_match(&file.content, filters)
}

fn indexed_filter_only_result(file: &IndexedPath, filters: &SearchFilters) -> Option<SearchResult> {
    if !indexed_file_matches_filters(file, filters) {
        return None;
    }
    let matched = score_filter_only_path_match(&file.path, filters, filters.explain);
    let mut result = SearchResult {
        path: file.path.clone(),
        score: matched.score,
        reason: format!("filter match {}", matched.reasons.join(", ")),
        snippet: indexed_filter_only_snippet(file, filters.snippet),
        line_range: None,
        match_lines: Vec::new(),
        explanation: filters.explain.then_some(matched.signals),
        query_plan: None,
        duplicate_group: None,
        context: None,
        read_range: None,
        read_request: None,
        related_request: None,
        related_symbols_request: None,
    };
    if let Some(symbol) = filters
        .symbol_kind
        .as_deref()
        .and_then(|kind| indexed_symbol_kind_filter_symbol(file, kind))
    {
        let line = symbol.line;
        if let Some(snippet) = indexed_symbol_filter_snippet(file, line, filters.snippet) {
            result.snippet = snippet;
            result.match_lines = vec![line];
        }
        result.reason.push_str(&format!(", symbol:{}", symbol.name));
    }
    Some(result)
}

fn indexed_filter_only_snippet(file: &IndexedPath, mode: SnippetMode) -> String {
    if file.content.is_empty() || file.line_offsets.is_empty() {
        return String::new();
    }
    render_indexed_window(file.content.as_bytes(), &file.line_offsets, 1, mode)
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

fn indexed_file_matches_related_symbol_filters_compiled(
    file: &IndexedPath,
    filters: &SearchFilters,
    path_filters: &PathFilterMatcher,
) -> bool {
    matches_filters_with_compiled_path_metadata(
        &file.path_lower,
        &file.file_name_lower,
        file.extension_lower.as_deref(),
        Some(&file.language),
        path_filters,
    ) && source_import_filters_match(&file.path, &file.content, filters)
        && source_excluded_content_filters_match(&file.content, filters)
}

fn indexed_query_token_overlap(
    query_tokens: &HashSet<String>,
    symbol_tokens: &[String],
    path_terms: &[TermCount],
) -> usize {
    query_tokens
        .iter()
        .filter(|token| {
            symbol_tokens.iter().any(|candidate| candidate == *token)
                || path_terms.iter().any(|term| term.term == token.as_str())
        })
        .count()
}

fn indexed_file_matches_related_file_filters_compiled(
    file: &IndexedPath,
    filters: &SearchFilters,
    path_filters: &PathFilterMatcher,
) -> bool {
    indexed_file_matches_related_symbol_filters_compiled(file, filters, path_filters)
        && indexed_path_matches_symbol_filter_fields(file, filters)
}

fn indexed_path_matches_symbol_filter_fields(file: &IndexedPath, filters: &SearchFilters) -> bool {
    if filters.symbol.is_none() && filters.exclude_symbol.is_empty() {
        return true;
    }
    if filters
        .exclude_symbol
        .iter()
        .any(|symbol| indexed_path_matches_symbol_filter(file, symbol))
    {
        return false;
    }
    filters
        .symbol
        .as_deref()
        .is_none_or(|symbol| indexed_path_matches_symbol_filter(file, symbol))
}

fn indexed_symbol_matches_related_filters(symbol: &IndexedSymbol, filters: &SearchFilters) -> bool {
    symbol_matches_related_filters(&symbol.name, &symbol.kind, filters)
}

fn indexed_query_plan(
    query_tokens: &[String],
    query_phrases: &[String],
    query_trigrams: &[String],
    token_postings: &[(&String, &Vec<Posting>)],
    symbol_postings: &[(&String, &Vec<Posting>)],
    symbol_kind_postings: &[(&String, &Vec<Posting>)],
    attribute_postings: &AttributeFilterPostings<'_>,
    path_postings: &[(&String, &Vec<Posting>)],
    trigram_postings: &[(&String, &Vec<Posting>)],
    missing_terms: &[String],
    missing_trigrams: &[String],
    use_trigrams: bool,
    active_filters: Vec<QueryPlanFilter>,
    filters: &SearchFilters,
    files: &[IndexedPath],
    facet_candidate_ids: &[u32],
    all_symbol_postings: &HashMap<String, Vec<Posting>>,
    all_symbol_kind_postings: &HashMap<String, Vec<Posting>>,
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
        .chain(attribute_postings.plan_postings())
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

    let repair_hints = query_plan_repair_hints(
        query_tokens,
        query_phrases,
        missing_terms,
        missing_trigrams,
        filters,
        files,
        facet_candidate_ids,
        all_symbol_postings,
        all_symbol_kind_postings,
        &active_filters,
        require_all,
        candidate_count,
        candidate_cap,
        candidate_cap_hit,
        filtered_candidate_count,
        scored_candidate_count,
        final_match_count,
    );

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
        repair_hints,
        retry_requests: Vec::new(),
    }
}

fn query_plan_repair_hints(
    query_tokens: &[String],
    query_phrases: &[String],
    missing_terms: &[String],
    missing_trigrams: &[String],
    filters: &SearchFilters,
    files: &[IndexedPath],
    facet_candidate_ids: &[u32],
    all_symbol_postings: &HashMap<String, Vec<Posting>>,
    all_symbol_kind_postings: &HashMap<String, Vec<Posting>>,
    active_filters: &[QueryPlanFilter],
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
                "The indexed planner found {candidate_count} candidates and capped scoring at {candidate_cap}. Add a rarer term or file/path/lang/ext/symbol/generated filter for more complete results."
            ),
            None,
        ));
        hints.extend(candidate_facet_repair_hints(
            query_tokens,
            filters,
            files,
            facet_candidate_ids,
        ));
    }
    if final_match_count > 0 {
        if !candidate_cap_hit {
            hints.extend(candidate_facet_repair_hints(
                query_tokens,
                filters,
                files,
                facet_candidate_ids,
            ));
        }
        return hints;
    }

    if let Some(symbol) = filters.symbol.as_ref() {
        let normalized = normalize_token(symbol);
        if !normalized.is_empty() && !all_symbol_postings.contains_key(&normalized) {
            let suggested_query = suggested_symbol_query(symbol, files);
            let message = match suggested_query.as_deref() {
                Some(query) => format!(
                    "No indexed symbol exactly matches `{symbol}`. Retry with `{query}` or relax the symbol filter."
                ),
                None => format!(
                    "No indexed symbol exactly matches `{symbol}`. Relax symbol: or search for content terms instead."
                ),
            };
            hints.push(repair_hint(
                "replace_symbol_filter",
                message,
                suggested_query,
            ));
        }
    }
    if let Some(kind) = filters.symbol_kind.as_ref() {
        if !all_symbol_kind_postings.contains_key(kind) {
            let suggested_query = suggested_symbol_kind_replacement_query(
                kind,
                all_symbol_kind_postings,
                query_tokens,
            );
            let available = available_symbol_kinds(all_symbol_kind_postings);
            let message = if available.is_empty() {
                format!("No indexed symbols use kind `{kind}`.")
            } else {
                format!(
                    "No indexed symbols use kind `{kind}`. Available kinds: {}.",
                    available.join(", ")
                )
            };
            hints.push(repair_hint(
                "replace_symbol_kind_filter",
                message,
                suggested_query,
            ));
        }
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
    if candidate_count == 0 {
        hints.extend(filter_specific_repair_hints(active_filters, query_tokens));
    }
    if require_all && query_tokens.len() > 1 && candidate_count == 0 && missing_terms.is_empty() {
        hints.push(repair_hint(
            "try_any_terms",
            "Each term has postings, but no file contains all terms. Retry with mode:any for broad orientation, then refine with file/path/symbol filters.",
            suggested_any_terms_query(query_tokens),
        ));
    }
    if candidate_count > 0 && filtered_candidate_count == 0 {
        hints.extend(filter_specific_repair_hints(active_filters, query_tokens));
        hints.push(repair_hint(
            "relax_filters",
            "Posting candidates exist, but file/path/language/extension/test/generated/code filters rejected all of them.",
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

fn filter_specific_repair_hints(
    active_filters: &[QueryPlanFilter],
    query_tokens: &[String],
) -> Vec<QueryPlanRepairHint> {
    active_filters
        .iter()
        .filter(|filter| {
            !filter.negated
                && filter.candidate_matches == Some(0)
                && matches!(
                    filter.field.as_str(),
                    "file"
                        | "path"
                        | "language"
                        | "extension"
                        | "test"
                        | "generated"
                        | "symbol_kind"
                        | "import"
                )
        })
        .map(|filter| {
            let field_label = match filter.field.as_str() {
                "language" => "lang",
                "extension" => "ext",
                "symbol_kind" => "kind",
                other => other,
            };
            repair_hint(
                format!("relax_{}_filter", filter.field),
                format!(
                    "The {field_label}:{} filter rejected every posting candidate. Retry without just that filter before dropping the rest of the scope.",
                    filter.value
                ),
                suggested_token_query(query_tokens),
            )
        })
        .collect()
}

fn candidate_facet_repair_hints(
    query_tokens: &[String],
    filters: &SearchFilters,
    files: &[IndexedPath],
    candidate_ids: &[u32],
) -> Vec<QueryPlanRepairHint> {
    let total = candidate_ids.len();
    if total < 16 {
        return Vec::new();
    }

    let mut hints = Vec::new();
    if filters.path.is_none() {
        if let Some((prefix, count)) = top_meaningful_string_facet(
            candidate_ids
                .iter()
                .filter_map(|file_id| files.get(*file_id as usize))
                .filter_map(|file| path_prefix_facet(&file.path)),
            total,
        ) {
            hints.push(facet_hint(
                "narrow_by_path",
                "path",
                &prefix,
                count,
                total,
                query_tokens,
            ));
        }
    }
    if filters.extension.is_none() {
        if let Some((extension, count)) = top_meaningful_string_facet(
            candidate_ids
                .iter()
                .filter_map(|file_id| files.get(*file_id as usize))
                .filter_map(|file| file.extension_lower.clone()),
            total,
        ) {
            hints.push(facet_hint(
                "narrow_by_extension",
                "ext",
                &extension,
                count,
                total,
                query_tokens,
            ));
        }
    }
    if filters.language.is_none() {
        if let Some((language, count)) = top_meaningful_string_facet(
            candidate_ids
                .iter()
                .filter_map(|file_id| files.get(*file_id as usize))
                .map(|file| file.language.to_ascii_lowercase())
                .filter(|language| !language.is_empty()),
            total,
        ) {
            hints.push(facet_hint(
                "narrow_by_language",
                "lang",
                &language,
                count,
                total,
                query_tokens,
            ));
        }
    }
    if filters.symbol_kind.is_none() {
        if let Some((kind, count)) = top_meaningful_string_facet(
            candidate_ids
                .iter()
                .filter_map(|file_id| files.get(*file_id as usize))
                .flat_map(symbol_kind_facets_for_file),
            total,
        ) {
            hints.push(facet_hint(
                "narrow_by_symbol_kind",
                "kind",
                &kind,
                count,
                total,
                query_tokens,
            ));
        }
    }
    if filters.test.is_none() {
        if let Some((value, count)) = meaningful_bool_facet(
            candidate_ids
                .iter()
                .filter_map(|file_id| files.get(*file_id as usize))
                .map(|file| is_test_path(&file.path_lower)),
            total,
        ) {
            hints.push(facet_hint(
                "narrow_by_test",
                "test",
                if value { "true" } else { "false" },
                count,
                total,
                query_tokens,
            ));
        }
    }
    if filters.generated.is_none() {
        if let Some((value, count)) = meaningful_bool_facet(
            candidate_ids
                .iter()
                .filter_map(|file_id| files.get(*file_id as usize))
                .map(|file| is_generated_path(&file.path_lower)),
            total,
        ) {
            hints.push(facet_hint(
                "narrow_by_generated",
                "generated",
                if value { "true" } else { "false" },
                count,
                total,
                query_tokens,
            ));
        }
    }
    if filters.code.is_none() {
        if let Some((value, count)) = meaningful_bool_facet(
            candidate_ids
                .iter()
                .filter_map(|file_id| files.get(*file_id as usize))
                .map(|file| is_source_code_language(&file.language)),
            total,
        ) {
            hints.push(facet_hint(
                "narrow_by_code",
                "code",
                if value { "true" } else { "false" },
                count,
                total,
                query_tokens,
            ));
        }
    }
    hints.truncate(5);
    hints
}

fn symbol_kind_facets_for_file(file: &IndexedPath) -> Vec<String> {
    let mut seen = HashSet::new();
    let mut kinds = file
        .symbols
        .iter()
        .filter_map(|symbol| {
            let kind = symbol.kind.to_ascii_lowercase();
            (!kind.is_empty() && seen.insert(kind.clone())).then_some(kind)
        })
        .collect::<Vec<_>>();
    kinds.sort();
    kinds
}

fn top_meaningful_string_facet(
    values: impl Iterator<Item = String>,
    total: usize,
) -> Option<(String, usize)> {
    let mut counts = HashMap::<String, usize>::new();
    for value in values {
        if value.trim().is_empty() {
            continue;
        }
        *counts.entry(value).or_default() += 1;
    }
    let mut counts = counts.into_iter().collect::<Vec<_>>();
    counts.sort_by(|left, right| right.1.cmp(&left.1).then_with(|| left.0.cmp(&right.0)));
    counts
        .into_iter()
        .find(|(_, count)| facet_count_is_meaningful(*count, total))
}

fn meaningful_bool_facet(
    values: impl Iterator<Item = bool>,
    total: usize,
) -> Option<(bool, usize)> {
    let mut true_count = 0usize;
    let mut false_count = 0usize;
    for value in values {
        if value {
            true_count += 1;
        } else {
            false_count += 1;
        }
    }
    [(true, true_count), (false, false_count)]
        .into_iter()
        .filter(|(_, count)| facet_count_is_meaningful(*count, total))
        .min_by(|left, right| left.1.cmp(&right.1).then_with(|| right.0.cmp(&left.0)))
}

fn facet_count_is_meaningful(count: usize, total: usize) -> bool {
    count >= 2 && count < total && count.saturating_mul(5) <= total.saturating_mul(4)
}

fn facet_hint(
    kind: &str,
    field: &str,
    value: &str,
    count: usize,
    total: usize,
    query_tokens: &[String],
) -> QueryPlanRepairHint {
    repair_hint(
        kind,
        format!(
            "Filter `{field}:{value}` narrows the current candidate set from {total} files to {count}."
        ),
        suggested_faceted_query(field, value, query_tokens),
    )
}

fn suggested_faceted_query(field: &str, value: &str, query_tokens: &[String]) -> Option<String> {
    let facet = format!("{field}:{value}");
    let query = suggested_token_query(query_tokens)?;
    Some(format!("{facet} {query}"))
}

fn path_prefix_facet(path: &str) -> Option<String> {
    let mut parts = path.split('/').filter(|part| !part.is_empty());
    let first = parts.next()?;
    if ["src", "tests", "test", "docs", "examples", "benches"].contains(&first) {
        return Some(first.to_string());
    }
    let second = parts.next()?;
    Some(format!("{first}/{second}"))
}

fn repo_scope_mismatch_repair_hints(
    filters: &SearchFilters,
    suggested_query: &str,
) -> Vec<QueryPlanRepairHint> {
    let suggested_query = Some(suggested_query.to_string());
    let mut hints = Vec::new();
    if filters.repo.is_some() || !filters.exclude_repo.is_empty() {
        hints.push(repair_hint(
            "relax_repo_filter",
            "The repo: filter does not match this index root. Retry without that repo filter or choose a matching shard/index.",
            suggested_query.clone(),
        ));
    }
    if filters.branch.is_some() || !filters.exclude_branch.is_empty() {
        hints.push(repair_hint(
            "relax_branch_filter",
            "The branch: filter does not match this index root. Retry without that branch filter or choose a matching checkout.",
            suggested_query.clone(),
        ));
    }
    if filters.origin.is_some() || !filters.exclude_origin.is_empty() {
        hints.push(repair_hint(
            "relax_origin_filter",
            "The origin: filter does not match this index root. Retry without that origin filter or choose a matching checkout.",
            suggested_query.clone(),
        ));
    }
    if hints.is_empty() {
        hints.push(repair_hint(
            "repo_filter_mismatch",
            "The repo/git filters do not match this index root. Relax scope filters or choose a matching shard/index.",
            suggested_query,
        ));
    }
    hints
}

fn filter_scan_repair_hints(
    filters: &SearchFilters,
    symbol_kind_postings: &HashMap<String, Vec<Posting>>,
    files: &[IndexedPath],
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
    if let Some(file) = filters.file.as_ref() {
        if let Some(query) = suggested_file_filter_query(file, files) {
            return vec![repair_hint(
                "replace_file_filter",
                format!("No indexed file name matches `file:{file}`. Retry with `{query}`."),
                Some(query),
            )];
        }
    }
    if let Some(path) = filters.path.as_ref() {
        if let Some(query) = suggested_path_filter_query(path, files) {
            return vec![repair_hint(
                "replace_path_filter",
                format!("No indexed path matches `path:{path}`. Retry with `{query}`."),
                Some(query),
            )];
        }
    }
    let mut hints = filter_scan_specific_repair_hints(filters);
    hints.push(repair_hint(
        "relax_filters",
        "No files matched the filter-only query. Relax file/path/language/extension/test/generated/code filters.",
        None,
    ));
    hints
}

fn filter_scan_specific_repair_hints(filters: &SearchFilters) -> Vec<QueryPlanRepairHint> {
    let mut active = Vec::<(&str, &str, String)>::new();
    if let Some(value) = filters
        .file
        .as_ref()
        .filter(|value| !value.trim().is_empty())
    {
        active.push(("file", "file", value.clone()));
    }
    if let Some(value) = filters
        .path
        .as_ref()
        .filter(|value| !value.trim().is_empty())
    {
        active.push(("path", "path", value.clone()));
    }
    if let Some(value) = filters
        .language
        .as_ref()
        .filter(|value| !value.trim().is_empty())
    {
        active.push(("language", "lang", value.clone()));
    }
    if let Some(value) = filters
        .extension
        .as_ref()
        .filter(|value| !value.trim().is_empty())
    {
        active.push(("extension", "ext", value.clone()));
    }
    if let Some(value) = filters
        .dependency
        .as_ref()
        .filter(|value| !value.trim().is_empty())
    {
        active.push(("dependency", "dep", value.clone()));
    }
    if let Some(value) = filters
        .import
        .as_ref()
        .filter(|value| !value.trim().is_empty())
    {
        active.push(("import", "import", value.clone()));
    }
    if let Some(test) = filters.test {
        active.push(("test", "test", test.to_string()));
    }
    if let Some(generated) = filters.generated {
        active.push(("generated", "generated", generated.to_string()));
    }
    if let Some(code) = filters.code {
        active.push(("code", "code", code.to_string()));
    }

    active
        .iter()
        .map(|(field, label, value)| {
            let remaining_scope = active
                .iter()
                .any(|(other_field, _, _)| other_field != field);
            repair_hint(
                format!("relax_{}_filter", field),
                format!(
                    "No files matched the {label}:{value} filter-only query. Retry without just that filter before dropping the rest of the scope."
                ),
                remaining_scope.then(String::new),
            )
        })
        .collect()
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

fn suggested_symbol_kind_replacement_query(
    wanted: &str,
    symbol_kind_postings: &HashMap<String, Vec<Posting>>,
    query_tokens: &[String],
) -> Option<String> {
    let replacement = suggested_symbol_kind_query(wanted, symbol_kind_postings)?;
    Some(match suggested_query_from_tokens(query_tokens) {
        Some(query) => format!("{replacement} {query}"),
        None => replacement,
    })
}

fn suggested_symbol_query(wanted: &str, files: &[IndexedPath]) -> Option<String> {
    let wanted = normalize_token(wanted);
    if wanted.is_empty() {
        return None;
    }
    let mut best = files
        .iter()
        .flat_map(|file| file.symbols.iter())
        .filter(|symbol| !symbol.normalized.is_empty())
        .map(|symbol| {
            (
                symbol.name.as_str(),
                symbol.normalized.as_str(),
                edit_distance_at_most(&wanted, &symbol.normalized, 4),
            )
        })
        .filter_map(|(name, normalized, distance)| {
            distance.map(|distance| (name, normalized, distance))
        })
        .collect::<Vec<_>>();
    best.sort_by(|left, right| {
        left.2
            .cmp(&right.2)
            .then_with(|| left.1.len().cmp(&right.1.len()))
            .then_with(|| left.1.cmp(right.1))
            .then_with(|| left.0.cmp(right.0))
    });
    best.first()
        .filter(|(_, _, distance)| *distance <= 3)
        .map(|(name, _, _)| format!("symbol:{name}"))
}

fn suggested_file_filter_query(wanted: &str, files: &[IndexedPath]) -> Option<String> {
    if filter_suggestion_is_glob(wanted) {
        return None;
    }
    let wanted_path = normalize_filter_path_for_suggestion(wanted);
    if wanted_path.contains('/') {
        if let Some(query) = suggested_path_filter_query(wanted, files) {
            return Some(query);
        }
    }
    let wanted = wanted_path.rsplit('/').next().unwrap_or("").to_string();
    if wanted.is_empty() {
        return None;
    }
    let mut best = files
        .iter()
        .map(|file| {
            (
                indexed_file_name(&file.path),
                file.file_name_lower.as_str(),
                edit_distance_at_most(&wanted, &file.file_name_lower, 4),
            )
        })
        .filter_map(|(file_name, file_name_lower, distance)| {
            distance.map(|distance| (file_name, file_name_lower, distance))
        })
        .collect::<Vec<_>>();
    best.sort_by(|left, right| {
        left.2
            .cmp(&right.2)
            .then_with(|| left.1.len().cmp(&right.1.len()))
            .then_with(|| left.1.cmp(right.1))
    });
    best.first()
        .filter(|(_, _, distance)| *distance <= 3)
        .map(|(file_name, _, _)| format!("file:{file_name}"))
}

fn suggested_path_filter_query(wanted: &str, files: &[IndexedPath]) -> Option<String> {
    if filter_suggestion_is_glob(wanted) {
        return None;
    }
    let wanted = normalize_filter_path_for_suggestion(wanted);
    if wanted.is_empty() {
        return None;
    }
    let mut best = files
        .iter()
        .map(|file| {
            (
                file.path.as_str(),
                file.path_lower.as_str(),
                edit_distance_at_most(&wanted, &file.path_lower, 6),
            )
        })
        .filter_map(|(path, path_lower, distance)| {
            distance.map(|distance| (path, path_lower, distance))
        })
        .collect::<Vec<_>>();
    best.sort_by(|left, right| {
        left.2
            .cmp(&right.2)
            .then_with(|| left.1.len().cmp(&right.1.len()))
            .then_with(|| left.1.cmp(right.1))
    });
    best.first()
        .filter(|(_, _, distance)| *distance <= 4)
        .map(|(path, _, _)| format!("path:{path}"))
}

fn filter_suggestion_is_glob(value: &str) -> bool {
    value.contains('*') || value.contains('?')
}

fn normalize_filter_path_for_suggestion(path: &str) -> String {
    path.trim()
        .trim_start_matches("./")
        .trim_start_matches('/')
        .replace('\\', "/")
        .to_ascii_lowercase()
}

fn indexed_file_name(path: &str) -> String {
    Path::new(path)
        .file_name()
        .map(|value| value.to_string_lossy().into_owned())
        .unwrap_or_default()
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

fn planned_symbol_query_name(
    terms: &[String],
    filters: &SearchFilters,
    explicit_content_terms: bool,
) -> Option<String> {
    if filters.symbol.is_none() && explicit_content_terms {
        return None;
    }
    if filters.symbol.is_none() && !scope_allows_required_implicit_symbol_posting(filters) {
        return None;
    }
    exact_symbol_query_name(terms, filters.symbol.as_deref())
}

fn scope_allows_required_implicit_symbol_posting(filters: &SearchFilters) -> bool {
    if filters.test.is_some()
        || !filters.exclude_file.is_empty()
        || !filters.exclude_path.is_empty()
        || !filters.exclude_language.is_empty()
        || !filters.exclude_extension.is_empty()
        || !filters.exclude_symbol.is_empty()
        || !filters.exclude_symbol_kind.is_empty()
        || !filters.exclude_repo.is_empty()
        || !filters.exclude_dependency.is_empty()
        || !filters.exclude_import.is_empty()
        || !filters.exclude_content.is_empty()
    {
        return false;
    }
    positive_scope_can_contain_symbols(filters)
}

fn positive_scope_can_contain_symbols(filters: &SearchFilters) -> bool {
    filters
        .language
        .as_deref()
        .is_none_or(language_can_contain_symbols)
        && filters
            .extension
            .as_deref()
            .and_then(language_from_filter_extension)
            .is_none_or(|language| language_can_contain_symbols(&language))
        && filters
            .file
            .as_deref()
            .and_then(language_from_filter_path)
            .is_none_or(|language| language_can_contain_symbols(&language))
        && filters
            .path
            .as_deref()
            .and_then(language_from_filter_path)
            .is_none_or(|language| language_can_contain_symbols(&language))
}

fn language_can_contain_symbols(language: &str) -> bool {
    let language = normalize_language_filter(language);
    matches!(
        language.as_str(),
        "python"
            | "rust"
            | "javascript"
            | "typescript"
            | "go"
            | "ruby"
            | "java"
            | "kotlin"
            | "swift"
    )
}

fn language_from_filter_extension(extension: &str) -> Option<String> {
    let extension = extension.trim().trim_start_matches('.');
    if extension.is_empty() {
        return None;
    }
    let probe = format!("file.{extension}");
    language_for(Path::new(&probe))
}

fn language_from_filter_path(path: &str) -> Option<String> {
    let path = path.trim().replace('\\', "/");
    if path.is_empty() {
        return None;
    }
    let last = path.rsplit('/').next().unwrap_or(path.as_str());
    let extension = last.rsplit_once('.')?.1;
    if extension.is_empty()
        || extension.contains('*')
        || extension.contains('?')
        || extension.contains('[')
        || extension.contains(']')
    {
        return None;
    }
    language_from_filter_extension(extension)
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

#[derive(Debug)]
struct AttributeFilterPostings<'a> {
    postings: Vec<(String, &'a Vec<Posting>)>,
    missing: Vec<String>,
}

impl AttributeFilterPostings<'_> {
    fn is_impossible(&self) -> bool {
        !self.missing.is_empty()
    }

    fn is_empty(&self) -> bool {
        self.postings.is_empty() && self.missing.is_empty()
    }

    fn plan_postings(&self) -> Vec<QueryPlanPosting> {
        self.postings
            .iter()
            .map(|(key, postings)| plan_posting("filter", key, postings))
            .collect()
    }
}

#[derive(Debug)]
struct PathFilterTrigramPostings<'a> {
    postings: Vec<(String, &'a Vec<Posting>)>,
    missing: Vec<String>,
}

impl PathFilterTrigramPostings<'_> {
    fn is_impossible(&self) -> bool {
        !self.missing.is_empty()
    }

    fn is_empty(&self) -> bool {
        self.postings.is_empty() && self.missing.is_empty()
    }

    fn plan_postings(&self) -> Vec<QueryPlanPosting> {
        self.postings
            .iter()
            .map(|(key, postings)| plan_posting("path_filter_trigram", key, postings))
            .collect()
    }
}

fn attribute_postings_for_filters<'a>(
    postings: &'a HashMap<String, Vec<Posting>>,
    filters: &SearchFilters,
) -> AttributeFilterPostings<'a> {
    let keys = attribute_filter_keys(filters);
    let mut planned = Vec::with_capacity(keys.len());
    let mut missing = Vec::new();
    for key in keys {
        match postings.get(&key) {
            Some(values) => planned.push((key, values)),
            None => missing.push(key),
        }
    }
    AttributeFilterPostings {
        postings: planned,
        missing,
    }
}

fn attribute_filter_keys(filters: &SearchFilters) -> Vec<String> {
    let mut keys = Vec::new();
    if let Some(language) = filters.language.as_deref() {
        let language = normalize_language_filter(language);
        if !language.is_empty() {
            keys.push(attribute_posting_key("language", &language));
        }
    }
    if let Some(extension) = filters.extension.as_deref() {
        let extension = extension
            .trim()
            .trim_start_matches('.')
            .to_ascii_lowercase();
        if !extension.is_empty() {
            keys.push(attribute_posting_key("extension", &extension));
        }
    }
    if let Some(test) = filters.test {
        keys.push(attribute_posting_key("test", bool_attribute_value(test)));
    }
    if let Some(generated) = filters.generated {
        keys.push(attribute_posting_key(
            "generated",
            bool_attribute_value(generated),
        ));
    }
    if let Some(code) = filters.code {
        keys.push(attribute_posting_key("code", bool_attribute_value(code)));
    }
    keys
}

fn path_filter_trigram_postings_for_filters<'a>(
    postings: &'a HashMap<String, Vec<Posting>>,
    filters: &SearchFilters,
) -> PathFilterTrigramPostings<'a> {
    let keys = path_filter_trigram_keys(filters);
    let mut planned = Vec::with_capacity(keys.len());
    let mut missing = Vec::new();
    for key in keys {
        match postings.get(&key) {
            Some(values) => planned.push((key, values)),
            None => missing.push(key),
        }
    }
    PathFilterTrigramPostings {
        postings: planned,
        missing,
    }
}

fn path_filter_trigram_keys(filters: &SearchFilters) -> Vec<String> {
    let mut keys = Vec::new();
    if let Some(file) = filters.file.as_deref() {
        push_filter_trigram_keys(file, &mut keys);
    }
    if let Some(path) = filters.path.as_deref() {
        push_filter_trigram_keys(path, &mut keys);
    }
    keys.sort();
    keys.dedup();
    keys
}

fn push_filter_trigram_keys(filter: &str, keys: &mut Vec<String>) {
    let value = filter.trim().replace('\\', "/").to_ascii_lowercase();
    if value.contains('*') || value.contains('?') {
        return;
    }
    keys.extend(query_trigrams(&value));
}

fn filter_only_candidate_ids(
    symbol_kind_postings: &[(&String, &Vec<Posting>)],
    attribute_postings: &AttributeFilterPostings<'_>,
    path_filter_postings: &PathFilterTrigramPostings<'_>,
    filters: &SearchFilters,
) -> Option<Vec<u32>> {
    if filters.symbol_kind.is_some() && symbol_kind_postings.is_empty() {
        return Some(Vec::new());
    }
    if attribute_postings.is_impossible() || path_filter_postings.is_impossible() {
        return Some(Vec::new());
    }
    if symbol_kind_postings.is_empty()
        && attribute_postings.is_empty()
        && path_filter_postings.is_empty()
    {
        return None;
    }
    let planned_postings = symbol_kind_postings
        .iter()
        .map(|(_, postings)| *postings)
        .chain(
            attribute_postings
                .postings
                .iter()
                .map(|(_, postings)| *postings),
        )
        .chain(
            path_filter_postings
                .postings
                .iter()
                .map(|(_, postings)| *postings),
        )
        .collect::<Vec<_>>();
    Some(intersect_planned_postings(&planned_postings, true))
}

fn filter_only_strategy(
    filters: &SearchFilters,
    path_filter_postings: &PathFilterTrigramPostings<'_>,
) -> &'static str {
    if filters.symbol_kind.is_some() {
        "symbol_kind_filter_postings"
    } else if filters.language.is_some()
        || filters.extension.is_some()
        || filters.test.is_some()
        || filters.generated.is_some()
        || filters.code.is_some()
    {
        "attribute_filter_postings"
    } else if !path_filter_postings.is_empty() {
        "path_filter_trigram_postings"
    } else {
        "filter_scan"
    }
}

fn intersect_symbol_kind_postings(
    candidate_ids: Vec<u32>,
    symbol_kind_postings: &[(&String, &Vec<Posting>)],
    filters: &SearchFilters,
) -> Vec<u32> {
    if filters.symbol_kind.is_some() && symbol_kind_postings.is_empty() {
        return Vec::new();
    }
    if candidate_ids.is_empty() || symbol_kind_postings.is_empty() {
        return candidate_ids;
    }
    symbol_kind_postings
        .iter()
        .fold(candidate_ids, |candidate_ids, (_, postings)| {
            intersect_sorted_ids_with_postings(&candidate_ids, postings)
        })
}

fn candidate_trigram_postings<'a>(
    trigram_postings: &'a [(&'a String, &'a Vec<Posting>)],
    missing_trigrams: &[String],
    query_tokens: &[String],
    use_trigrams: bool,
) -> Vec<&'a Vec<Posting>> {
    if !use_trigrams {
        return Vec::new();
    }
    if query_tokens.len() == 1 {
        if !missing_trigrams.is_empty() {
            return Vec::new();
        }
        return trigram_postings
            .iter()
            .map(|(_, postings)| *postings)
            .collect();
    }
    trigram_postings
        .iter()
        .take(8)
        .map(|(_, postings)| *postings)
        .collect()
}

fn filter_single_literal_trigram_candidates(
    candidate_ids: Vec<u32>,
    files: &[IndexedPath],
    query_tokens: &[String],
    use_trigrams: bool,
) -> Vec<u32> {
    if !use_trigrams || query_tokens.len() != 1 {
        return candidate_ids;
    }
    let token = &query_tokens[0];
    candidate_ids
        .into_iter()
        .filter(|file_id| {
            files
                .get(*file_id as usize)
                .is_some_and(|file| indexed_file_has_verified_query_token(file, token))
        })
        .collect()
}

fn suppress_symbol_kind_trigram_fallback(
    filters: &SearchFilters,
    query_tokens: &[String],
    token_postings: usize,
    symbol_postings: usize,
    path_postings: usize,
) -> bool {
    filters.symbol_kind.is_some()
        && query_tokens.len() == 1
        && token_postings == 0
        && symbol_postings == 0
        && path_postings == 0
}

fn indexed_file_has_verified_query_token(file: &IndexedPath, token: &str) -> bool {
    file.path_lower.contains(token)
        || file.terms.iter().any(|term| term.term == token)
        || file.path_terms.iter().any(|term| term.term == token)
        || file.symbols.iter().any(|symbol| {
            symbol.normalized.contains(token) || symbol.tokens.iter().any(|part| part == token)
        })
        || contains_ascii_case_insensitive(&file.content, token)
}

fn contains_ascii_case_insensitive(haystack: &str, needle: &str) -> bool {
    let needle = needle.as_bytes();
    if needle.is_empty() {
        return true;
    }
    haystack
        .as_bytes()
        .windows(needle.len())
        .any(|window| window.eq_ignore_ascii_case(needle))
}

fn intersect_attribute_postings(
    candidate_ids: Vec<u32>,
    attribute_postings: &AttributeFilterPostings<'_>,
) -> Vec<u32> {
    if candidate_ids.is_empty() || attribute_postings.is_empty() {
        return candidate_ids;
    }
    if attribute_postings.is_impossible() {
        return Vec::new();
    }
    attribute_postings
        .postings
        .iter()
        .fold(candidate_ids, |candidate_ids, (_, postings)| {
            intersect_sorted_ids_with_postings(&candidate_ids, postings)
        })
}

fn intersect_path_filter_trigram_postings(
    candidate_ids: Vec<u32>,
    path_filter_postings: &PathFilterTrigramPostings<'_>,
) -> Vec<u32> {
    if candidate_ids.is_empty() || path_filter_postings.is_empty() {
        return candidate_ids;
    }
    if path_filter_postings.is_impossible() {
        return Vec::new();
    }
    path_filter_postings
        .postings
        .iter()
        .fold(candidate_ids, |candidate_ids, (_, postings)| {
            intersect_sorted_ids_with_postings(&candidate_ids, postings)
        })
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
            name_lower: symbol.name.to_ascii_lowercase(),
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
    for symbol in &mut file.symbols {
        if symbol.name_lower.is_empty() {
            symbol.name_lower = symbol.name.to_ascii_lowercase();
        }
    }
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
    file: &IndexedPath,
    query_tokens: &[String],
    query_phrases: &[String],
    mode: SnippetMode,
) -> String {
    let bytes = file.content.as_bytes();
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
    file: &IndexedPath,
    line: usize,
    mode: SnippetMode,
) -> Option<String> {
    if file.line_offsets.is_empty() {
        return None;
    }
    Some(render_indexed_window(
        file.content.as_bytes(),
        &file.line_offsets,
        line,
        mode,
    ))
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

    for phrase in query_phrases {
        let mut phrase_lines = if file.term_lines.is_empty() {
            indexed_phrase_match_lines_by_scan(bytes, offsets, phrase)
        } else {
            let (candidate_lines, capped) = indexed_phrase_candidate_lines(file, phrase);
            let mut phrase_lines = indexed_phrase_match_lines_from_candidates(
                bytes,
                offsets,
                phrase,
                &candidate_lines,
            );
            if phrase_lines.is_empty() && capped {
                phrase_lines = indexed_phrase_match_lines_by_scan(bytes, offsets, phrase);
            }
            phrase_lines
        };
        phrase_lines.truncate(MAX_TERM_LINES_PER_TERM);
        for line in phrase_lines {
            *scores.entry(line).or_insert(0) += 100;
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

fn indexed_phrase_candidate_lines(file: &IndexedPath, phrase: &str) -> (Vec<u32>, bool) {
    let mut best_lines: Option<&[u32]> = None;
    let mut capped = false;
    for token in tokenize(phrase) {
        let Ok(index) = file
            .term_lines
            .binary_search_by(|entry| entry.term.as_str().cmp(token.as_str()))
        else {
            return (Vec::new(), false);
        };
        let lines = file.term_lines[index].lines.as_slice();
        capped |= lines.len() >= MAX_TERM_LINES_PER_TERM;
        if best_lines.is_none_or(|best| lines.len() < best.len()) {
            best_lines = Some(lines);
        }
    }
    (best_lines.unwrap_or_default().to_vec(), capped)
}

fn indexed_phrase_match_lines_from_candidates(
    bytes: &[u8],
    offsets: &[u32],
    phrase: &str,
    candidate_lines: &[u32],
) -> Vec<usize> {
    let mut matched = Vec::new();
    for line in candidate_lines {
        let Some(index) = (*line as usize).checked_sub(1) else {
            continue;
        };
        if index >= offsets.len() {
            continue;
        }
        let phrase_text = indexed_phrase_window_text(bytes, offsets, index);
        if normalize_phrase_text(&phrase_text).contains(phrase) {
            matched.push(index + 1);
        }
    }
    matched.sort_unstable();
    matched.dedup();
    matched
}

fn indexed_phrase_match_lines_by_scan(bytes: &[u8], offsets: &[u32], phrase: &str) -> Vec<usize> {
    let mut matched = Vec::new();
    for index in 0..offsets.len() {
        let phrase_text = indexed_phrase_window_text(bytes, offsets, index);
        if normalize_phrase_text(&phrase_text).contains(phrase) {
            matched.push(index + 1);
        }
    }
    matched
}

fn indexed_phrase_window_text<'a>(
    bytes: &'a [u8],
    offsets: &[u32],
    center_index: usize,
) -> std::borrow::Cow<'a, str> {
    let start_index = center_index.saturating_sub(1);
    let end_index = (center_index + 1).min(offsets.len().saturating_sub(1));
    let start = offsets[start_index] as usize;
    let end = line_end(bytes, offsets, end_index);
    String::from_utf8_lossy(&bytes[start..end])
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
    indexed_file_range_with_symbol(file, start_line, line_count, None)
}

fn indexed_file_range_scoped(
    file: &IndexedPath,
    start_line: usize,
    line_count: usize,
    scope: RangeScope,
) -> FileRange {
    if scope == RangeScope::Symbol {
        let symbols = file
            .symbols
            .iter()
            .map(|symbol| Symbol {
                name: symbol.name.clone(),
                kind: symbol.kind.clone(),
                path: file.path.clone(),
                line: symbol.line,
            })
            .collect::<Vec<_>>();
        if let Some(symbol) = symbol_for_anchor(&symbols, start_line) {
            let (symbol_start, symbol_lines) = symbol_scoped_window(
                symbol.line,
                line_count,
                DEFAULT_INDEXED_SYMBOL_READ_CONTEXT_BEFORE,
            );
            return indexed_file_range_with_symbol(
                file,
                symbol_start,
                symbol_lines,
                Some(symbol.clone()),
            );
        }
    }
    indexed_file_range(file, start_line, line_count)
}

fn indexed_file_range_with_symbol(
    file: &IndexedPath,
    start_line: usize,
    line_count: usize,
    symbol: Option<Symbol>,
) -> FileRange {
    let bytes = file.content.as_bytes();
    if bytes.is_empty() || file.line_offsets.is_empty() {
        return FileRange {
            path: file.path.clone(),
            start_line: 1,
            end_line: 0,
            total_lines: 0,
            text: String::new(),
            symbol,
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
        symbol,
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

fn rebuild_path_ids(files: &[IndexedPath]) -> HashMap<String, u32> {
    files
        .iter()
        .enumerate()
        .map(|(file_id, file)| (file.path.clone(), file_id as u32))
        .collect()
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

fn rebuild_attribute_postings(files: &[IndexedPath]) -> HashMap<String, Vec<Posting>> {
    let mut postings: HashMap<String, Vec<Posting>> = HashMap::new();
    for (file_id, file) in files.iter().enumerate() {
        for key in indexed_attribute_keys(file) {
            postings.entry(key).or_default().push(Posting {
                file_id: file_id as u32,
                count: 1,
            });
        }
    }
    for values in postings.values_mut() {
        values.sort_unstable_by_key(|posting| posting.file_id);
    }
    postings
}

fn rebuild_path_trigram_postings(files: &[IndexedPath]) -> HashMap<String, Vec<Posting>> {
    let mut postings: HashMap<String, Vec<Posting>> = HashMap::new();
    for (file_id, file) in files.iter().enumerate() {
        for term in counted_terms(&trigram_counts(&file.path_lower)) {
            postings.entry(term.term).or_default().push(Posting {
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

fn indexed_attribute_keys(file: &IndexedPath) -> Vec<String> {
    let mut keys = Vec::with_capacity(5);
    let language = normalize_language_filter(&file.language);
    if !language.is_empty() {
        keys.push(attribute_posting_key("language", &language));
    }
    if let Some(extension) = &file.extension_lower {
        if !extension.is_empty() {
            keys.push(attribute_posting_key("extension", extension));
        }
    }
    keys.push(attribute_posting_key(
        "test",
        bool_attribute_value(is_test_path(&file.path_lower)),
    ));
    keys.push(attribute_posting_key(
        "generated",
        bool_attribute_value(is_generated_path(&file.path_lower)),
    ));
    keys.push(attribute_posting_key(
        "code",
        bool_attribute_value(is_source_code_language(&file.language)),
    ));
    keys
}

fn bool_attribute_value(value: bool) -> &'static str {
    if value { "true" } else { "false" }
}

fn attribute_posting_key(field: &str, value: &str) -> String {
    format!("{field}:{value}")
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
    ranked.select_nth_unstable_by(candidate_cap, |left, right| {
        compare_candidate_cap_rank(left, right, files)
    });
    ranked.truncate(candidate_cap);
    ranked.sort_by(|left, right| compare_candidate_cap_rank(left, right, files));
    let ids = ranked.into_iter().map(|rank| rank.file_id).collect();
    (ids, cap_hit)
}

#[derive(Debug, Clone, Copy)]
struct CandidateCapRank {
    file_id: u32,
    score: f64,
}

fn compare_candidate_cap_rank(
    left: &CandidateCapRank,
    right: &CandidateCapRank,
    files: &[IndexedPath],
) -> Ordering {
    right
        .score
        .partial_cmp(&left.score)
        .unwrap_or(Ordering::Equal)
        .then_with(|| candidate_path(files, left.file_id).cmp(candidate_path(files, right.file_id)))
        .then_with(|| left.file_id.cmp(&right.file_id))
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
