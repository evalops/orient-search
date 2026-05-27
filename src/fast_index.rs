//! Persistent local search index for agent-oriented code retrieval.

use crate::query::{merge_filters, normalize_phrase_text, parse_query, query_phrases, query_text};
use crate::repo_index::{
    FileRange, QueryPlan, QueryPlanPosting, RankSignal, RelatedFile, RelatedSymbol, RepoBrief,
    RepoMap, SearchFilters, SearchResult, SnippetMode, Symbol, apply_phrase_matches,
    best_snippet_for_path, extract_symbols, file_range_from_text, finalize_results,
    is_entrypoint_path, is_ignored, is_important_file, is_manifest_file, is_test_path,
    known_commands_from_manifest_texts, language_for, matches_filters, normalize_token,
    regular_file_metadata, repo_matches, result_matches_all_tokens, result_matches_symbol_filters,
    round4, symbol_kind_rank, token_counts, tokenize,
};
use anyhow::{Context, Result};
use ignore::WalkBuilder;
use serde::{Deserialize, Serialize};
use std::cmp::Ordering;
use std::collections::{HashMap, HashSet};
use std::fs;
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

const INDEX_VERSION: u32 = 9;
const MAX_FILE_BYTES: u64 = 512_000;
const MAX_TERM_LINES_PER_TERM: usize = 64;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct FastIndex {
    pub version: u32,
    pub root: PathBuf,
    pub files: Vec<IndexedPath>,
    pub postings: HashMap<String, Vec<Posting>>,
    pub path_postings: HashMap<String, Vec<Posting>>,
    pub trigram_postings: HashMap<String, Vec<Posting>>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct IndexedPath {
    pub path: String,
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
    pub terms: usize,
    pub path_terms: usize,
    pub trigrams: usize,
    pub symbols: usize,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RefreshStats {
    pub version: u32,
    pub root: PathBuf,
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
        Ok(Self {
            version: INDEX_VERSION,
            root,
            files,
            postings,
            path_postings,
            trigram_postings,
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
        let bytes = fs::read(path.as_ref())
            .with_context(|| format!("read index {}", path.as_ref().display()))?;
        let index = bincode::deserialize::<Self>(&bytes)
            .with_context(|| format!("parse index {}", path.as_ref().display()))?;
        anyhow::ensure!(
            index.version == INDEX_VERSION,
            "unsupported index version {}",
            index.version
        );
        Ok(index)
    }

    pub fn save(&self, path: impl AsRef<Path>) -> Result<()> {
        if let Some(parent) = path.as_ref().parent() {
            fs::create_dir_all(parent)?;
        }
        fs::write(path.as_ref(), bincode::serialize(self)?)
            .with_context(|| format!("write index {}", path.as_ref().display()))
    }

    pub fn stats(&self) -> IndexStats {
        IndexStats {
            version: self.version,
            root: self.root.clone(),
            files: self.files.len(),
            terms: self.postings.len(),
            path_terms: self.path_postings.len(),
            trigrams: self.trigram_postings.len(),
            symbols: self.files.iter().map(|file| file.symbols.len()).sum(),
        }
    }

    pub fn refresh_stats(&self, outcome: &RefreshOutcome) -> RefreshStats {
        RefreshStats {
            version: self.version,
            root: self.root.clone(),
            files: self.files.len(),
            terms: self.postings.len(),
            path_terms: self.path_postings.len(),
            trigrams: self.trigram_postings.len(),
            symbols: self.files.iter().map(|file| file.symbols.len()).sum(),
            reused_files: outcome.reused_files,
            renamed_files: outcome.renamed_files,
            refreshed_files: outcome.refreshed_files,
            deleted_files: outcome.deleted_files,
        }
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
            .filter(|file| is_test_path(&file.path.to_ascii_lowercase()))
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

        RepoMap {
            brief: RepoBrief {
                root_name: self
                    .root
                    .file_name()
                    .map(|value| value.to_string_lossy().to_string())
                    .unwrap_or_else(|| self.root.display().to_string()),
                file_count: self.files.len(),
                language_counts,
                known_commands: known_commands_from_indexed_files(&self.files),
                manifest_files,
                important_files,
            },
            entrypoints,
            test_files,
            top_symbols,
        }
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
        let requested = Path::new(path);
        anyhow::ensure!(
            requested.is_relative()
                && !requested
                    .components()
                    .any(|component| matches!(component, std::path::Component::ParentDir)),
            "path must be index-relative"
        );
        let normalized = requested.to_string_lossy().replace('\\', "/");
        anyhow::ensure!(
            !normalized.starts_with('/') && !normalized.split('/').any(|part| part == ".."),
            "path must be index-relative"
        );
        let file = self
            .files
            .iter()
            .find(|file| file.path == normalized)
            .ok_or_else(|| anyhow::anyhow!("path is not present in index: {normalized}"))?;
        Ok(file_range_from_text(
            file.path.clone(),
            &file.content,
            start_line,
            line_count,
        ))
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
        let stem_without_test = stem_lower.strip_prefix("test_").unwrap_or(&stem_lower);
        let directory = Path::new(&normalized)
            .parent()
            .map(|value| value.to_string_lossy().to_string())
            .unwrap_or_default();
        let mut related = Vec::new();

        for file in &self.files {
            if file.path == normalized {
                continue;
            }
            let lower = file.path.to_ascii_lowercase();
            let mut score = 0.0;
            let mut reasons = Vec::new();

            if !stem_lower.is_empty() && lower.contains(&stem_lower) {
                score += 4.0;
                reasons.push(format!("shares stem {stem}"));
            }
            if is_test_path(&lower)
                && !stem_without_test.is_empty()
                && lower.contains(stem_without_test)
            {
                score += 5.0;
                reasons.push("test coverage candidate".to_string());
            }

            let file_dir = Path::new(&file.path)
                .parent()
                .map(|value| value.to_string_lossy().to_string())
                .unwrap_or_default();
            if !directory.is_empty() && file_dir == directory {
                score += 1.0;
                reasons.push("same directory".to_string());
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
        let parsed = parse_query(query);
        let query_phrases = query_phrases(&parsed.terms);
        let mut filters = merge_filters(filters.clone(), parsed.filters);
        if !repo_matches(&self.root, &filters) {
            return Ok(Vec::new());
        }
        let query = query_text(&parsed.terms, &filters);
        let query_tokens = tokenize(&query);
        let query_trigrams = query_trigrams(&query);
        if (query_tokens.is_empty() && query_trigrams.is_empty()) || limit == 0 {
            return Ok(Vec::new());
        }
        if query_tokens.len() > 1 {
            filters.require_all = true;
        }

        let mut token_postings = query_tokens
            .iter()
            .filter_map(|token| self.postings.get(token).map(|postings| (token, postings)))
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
        if token_postings.is_empty() && path_postings.is_empty() && trigram_postings.is_empty() {
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
            .filter(|(token, _)| !content_tokens.contains(token.as_str()))
            .collect::<Vec<_>>();

        let mut planned_postings = token_postings
            .iter()
            .map(|(_, postings)| *postings)
            .chain(path_plan_postings.iter().map(|(_, postings)| *postings))
            .chain(
                trigram_postings
                    .iter()
                    .take(8)
                    .map(|(_, postings)| *postings),
            )
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
        let query_plan = filters.explain.then(|| {
            indexed_query_plan(
                &query_tokens,
                &query_phrases,
                &query_trigrams,
                &token_postings,
                &path_postings,
                &trigram_postings,
                use_trigrams,
                filters.require_all,
                candidate_ids.len(),
            )
        });

        let posting_maps = token_postings
            .iter()
            .map(|(token, postings)| {
                (
                    (*token).clone(),
                    postings
                        .iter()
                        .map(|posting| (posting.file_id, posting.count))
                        .collect::<HashMap<_, _>>(),
                )
            })
            .collect::<Vec<_>>();
        let path_maps = path_postings
            .iter()
            .map(|(token, postings)| {
                (
                    (*token).clone(),
                    postings
                        .iter()
                        .map(|posting| (posting.file_id, posting.count))
                        .collect::<HashMap<_, _>>(),
                )
            })
            .collect::<Vec<_>>();
        let trigram_maps = trigram_postings
            .iter()
            .take(16)
            .map(|(trigram, postings)| {
                (
                    (*trigram).clone(),
                    postings
                        .iter()
                        .map(|posting| (posting.file_id, posting.count))
                        .collect::<HashMap<_, _>>(),
                )
            })
            .collect::<Vec<_>>();
        let results = candidate_ids
            .into_iter()
            .filter(|file_id| {
                self.files
                    .get(*file_id as usize)
                    .is_some_and(|file| matches_filters(&file.path, &filters))
            })
            .filter_map(|file_id| {
                self.score_file(
                    file_id,
                    &query_tokens,
                    &query_phrases,
                    &posting_maps,
                    &path_maps,
                    &trigram_maps,
                    filters.snippet,
                    filters.explain,
                    query_plan.as_ref(),
                )
            })
            .collect::<Vec<_>>();

        let mut results = results;
        if filters.require_all {
            results.retain(|result| result_matches_all_tokens(result, &query_tokens));
        }
        results.retain(|result| result_matches_symbol_filters(result, &filters));
        Ok(finalize_results(results, limit))
    }

    fn score_file(
        &self,
        file_id: u32,
        query_tokens: &[String],
        query_phrases: &[String],
        posting_maps: &[(String, HashMap<u32, u16>)],
        path_maps: &[(String, HashMap<u32, u16>)],
        trigram_maps: &[(String, HashMap<u32, u16>)],
        snippet_mode: SnippetMode,
        explain: bool,
        query_plan: Option<&QueryPlan>,
    ) -> Option<SearchResult> {
        let file = self.files.get(file_id as usize)?;
        let path_lower = file.path.to_lowercase();
        let query_name = query_tokens.join("");
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

        for (token, postings) in posting_maps {
            let count = postings.get(&file_id).copied().unwrap_or_default();
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
                reasons.push(token.clone());
            }
        }
        for (token, postings) in path_maps {
            let count = postings.get(&file_id).copied().unwrap_or_default();
            if count > 0 {
                let amount = 8.0 + (count as f64).ln();
                score += amount;
                signals.push(rank_signal("path_term", token, amount));
                if !reasons.contains(token) {
                    reasons.push(token.clone());
                }
            }
        }
        let mut trigram_score = 0.0;
        let mut trigram_hits = 0usize;
        for (trigram, postings) in trigram_maps {
            let count = postings.get(&file_id).copied().unwrap_or_default();
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
            if symbol.normalized == query_name || query_tokens.contains(&symbol.normalized) {
                score += 25.0;
                reasons.push(format!("symbol:{}", symbol.name));
                signals.push(rank_signal("symbol_exact", &symbol.name, 25.0));
            } else {
                let overlap = symbol
                    .tokens
                    .iter()
                    .filter(|token| query_tokens.contains(token))
                    .count();
                if overlap > 0 {
                    let amount = 4.0 * overlap as f64;
                    score += amount;
                    reasons.push(format!("symbol:{}", symbol.name));
                    signals.push(rank_signal("symbol_overlap", &symbol.name, amount));
                }
            }
        }
        if score == 0.0 {
            return None;
        }

        let snippet = indexed_snippet(&self.root, file, query_tokens, query_phrases, snippet_mode);
        let match_lines = indexed_match_lines(file, query_tokens, query_phrases, 16);

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
        })
    }
}

fn rank_signal(kind: &str, value: &str, score: f64) -> RankSignal {
    RankSignal {
        kind: kind.to_string(),
        value: value.to_string(),
        score: round4(score),
    }
}

fn indexed_query_plan(
    query_tokens: &[String],
    query_phrases: &[String],
    query_trigrams: &[String],
    token_postings: &[(&String, &Vec<Posting>)],
    path_postings: &[(&String, &Vec<Posting>)],
    trigram_postings: &[(&String, &Vec<Posting>)],
    use_trigrams: bool,
    require_all: bool,
    candidate_count: usize,
) -> QueryPlan {
    let mut planned_postings = token_postings
        .iter()
        .map(|(token, postings)| plan_posting("content", token, postings))
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
        query_trigrams: query_trigrams.to_vec(),
        planned_postings,
        candidate_count,
    }
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
    let mut lines = query_tokens
        .iter()
        .filter_map(|token| {
            file.term_lines
                .binary_search_by(|entry| entry.term.as_str().cmp(token.as_str()))
                .ok()
                .map(|index| file.term_lines[index].lines.as_slice())
        })
        .flat_map(|lines| lines.iter().copied())
        .map(|line| line as usize)
        .collect::<Vec<_>>();
    if !query_phrases.is_empty() {
        for (index, line) in file.content.lines().enumerate() {
            let line_lower = normalize_phrase_text(line);
            if query_phrases
                .iter()
                .any(|phrase| line_lower.contains(phrase))
            {
                lines.push(index + 1);
            }
        }
    }
    lines.sort_unstable();
    lines.dedup();
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

fn known_commands_from_indexed_files(files: &[IndexedPath]) -> Vec<String> {
    known_commands_from_manifest_texts(
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

    if let Some(line) = first_matching_line(&bytes, &file.line_offsets, query_tokens, query_phrases)
    {
        return render_indexed_window(&bytes, &file.line_offsets, line, mode);
    }

    let text = String::from_utf8_lossy(&bytes);
    best_snippet_for_path(&file.path, &text, query_tokens, mode)
}

fn first_matching_line(
    bytes: &[u8],
    offsets: &[u32],
    query_tokens: &[String],
    query_phrases: &[String],
) -> Option<usize> {
    offsets.iter().enumerate().find_map(|(index, offset)| {
        let start = *offset as usize;
        let end = line_end(bytes, offsets, index);
        let line = String::from_utf8_lossy(&bytes[start..end]);
        let lowered = line.to_lowercase();
        let phrase_text = normalize_phrase_text(&line);
        (query_phrases
            .iter()
            .any(|phrase| phrase_text.contains(phrase))
            || query_tokens.iter().any(|token| lowered.contains(token)))
        .then_some(index + 1)
    })
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
    postings
}

fn intersect_planned_postings(planned: &[&Vec<Posting>], require_all: bool) -> HashSet<u32> {
    let Some(first) = planned.first() else {
        return HashSet::new();
    };
    let mut candidate_ids = first
        .iter()
        .map(|posting| posting.file_id)
        .collect::<HashSet<_>>();
    for postings in planned.iter().skip(1) {
        let ids = postings
            .iter()
            .map(|posting| posting.file_id)
            .collect::<HashSet<_>>();
        candidate_ids.retain(|id| ids.contains(id));
        if candidate_ids.is_empty() {
            break;
        }
    }
    if candidate_ids.is_empty() && !require_all {
        return first.iter().map(|posting| posting.file_id).collect();
    }
    candidate_ids
}

fn union_candidates(left: HashSet<u32>, right: HashSet<u32>) -> HashSet<u32> {
    left.into_iter().chain(right).collect()
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
