//! Persistent local search index for agent-oriented code retrieval.

use crate::query::{merge_filters, parse_query, query_text};
use crate::repo_index::{
    RankSignal, SearchFilters, SearchResult, SnippetMode, best_snippet_for_path, extract_symbols,
    finalize_results, is_ignored, language_for, matches_filters, normalize_token, repo_matches,
    result_matches_all_tokens, result_matches_symbol_filters, round4, token_counts, tokenize,
};
use anyhow::{Context, Result};
use ignore::WalkBuilder;
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::fs;
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

const INDEX_VERSION: u32 = 6;
const MAX_FILE_BYTES: u64 = 512_000;

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
    pub modified_secs: u64,
    pub modified_nanos: u32,
    pub terms: Vec<TermCount>,
    pub path_terms: Vec<TermCount>,
    pub trigrams: Vec<TermCount>,
    pub symbols: Vec<IndexedSymbol>,
    pub line_offsets: Vec<u32>,
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
    pub refreshed_files: usize,
    pub deleted_files: usize,
}

#[derive(Debug, Clone, PartialEq)]
pub struct RefreshOutcome {
    pub index: FastIndex,
    pub reused_files: usize,
    pub refreshed_files: usize,
    pub deleted_files: usize,
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
        let mut seen = HashSet::new();
        let mut files = Vec::new();
        let mut reused_files = 0usize;
        let mut refreshed_files = 0usize;

        for entry in WalkBuilder::new(&root)
            .hidden(false)
            .filter_entry(|entry| !is_ignored(entry.path()))
            .build()
        {
            let entry = entry?;
            let path = entry.path();
            if !path.is_file() {
                continue;
            }
            let metadata = entry.metadata()?;
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
            seen.insert(rel.clone());
            if let Some(previous) = previous_files.get(&rel) {
                if previous.size == metadata.len()
                    && previous.modified_secs == modified_secs
                    && previous.modified_nanos == modified_nanos
                    && previous.language == language
                {
                    files.push(previous.clone());
                    reused_files += 1;
                    continue;
                }
            }
            let Some(file) = index_file(
                &root,
                &rel,
                language,
                metadata.len(),
                modified_secs,
                modified_nanos,
            ) else {
                continue;
            };
            files.push(file);
            refreshed_files += 1;
        }

        let deleted_files = previous_files
            .keys()
            .filter(|path| !seen.contains(*path))
            .count();
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
            refreshed_files: outcome.refreshed_files,
            deleted_files: outcome.deleted_files,
        }
    }

    pub fn search(&self, query: &str, limit: usize) -> Result<Vec<SearchResult>> {
        self.search_filtered(query, limit, &SearchFilters::default())
    }

    pub fn search_filtered(
        &self,
        query: &str,
        limit: usize,
        filters: &SearchFilters,
    ) -> Result<Vec<SearchResult>> {
        let parsed = parse_query(query);
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
                    &posting_maps,
                    &path_maps,
                    &trigram_maps,
                    filters.snippet,
                    filters.explain,
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
        posting_maps: &[(String, HashMap<u32, u16>)],
        path_maps: &[(String, HashMap<u32, u16>)],
        trigram_maps: &[(String, HashMap<u32, u16>)],
        snippet_mode: SnippetMode,
        explain: bool,
    ) -> Option<SearchResult> {
        let file = self.files.get(file_id as usize)?;
        let path_lower = file.path.to_lowercase();
        let query_name = query_tokens.join("");
        let mut score = 0.0;
        let mut reasons = Vec::new();
        let mut signals = Vec::new();

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
            if symbol.normalized == query_name {
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

        let snippet = indexed_snippet(&self.root, file, query_tokens, snippet_mode);

        Some(SearchResult {
            path: file.path.clone(),
            score: round4(score),
            reason: format!("indexed match {}", reasons.join(", ")),
            snippet,
            explanation: explain.then_some(signals),
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
    let line_offsets = line_offsets(&text);
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
        modified_secs,
        modified_nanos,
        terms,
        path_terms,
        trigrams,
        symbols,
        line_offsets,
    })
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

fn indexed_snippet(
    root: &Path,
    file: &IndexedPath,
    query_tokens: &[String],
    mode: SnippetMode,
) -> String {
    let path = root.join(&file.path);
    let Ok(bytes) = fs::read(&path) else {
        return String::new();
    };
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

    if let Some(line) = first_matching_line(&bytes, &file.line_offsets, query_tokens) {
        return render_indexed_window(&bytes, &file.line_offsets, line, mode);
    }

    let text = String::from_utf8_lossy(&bytes);
    best_snippet_for_path(&file.path, &text, query_tokens, mode)
}

fn first_matching_line(bytes: &[u8], offsets: &[u32], query_tokens: &[String]) -> Option<usize> {
    offsets.iter().enumerate().find_map(|(index, offset)| {
        let start = *offset as usize;
        let end = line_end(bytes, offsets, index);
        let lowered = String::from_utf8_lossy(&bytes[start..end]).to_lowercase();
        query_tokens
            .iter()
            .any(|token| lowered.contains(token))
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
