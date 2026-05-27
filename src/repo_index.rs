//! Repo orientation index.

use crate::query::{merge_filters, normalize_phrase_text, parse_query, query_phrases, query_text};
use anyhow::Result;
use ignore::WalkBuilder;
use regex::Regex;
use serde::{Deserialize, Serialize};
use std::cmp::Ordering;
use std::collections::{HashMap, HashSet};
use std::fs;
use std::io::{BufRead, BufReader};
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::sync::{LazyLock, mpsc};
use std::thread;
use std::time::{Duration, Instant};

const MAX_FILE_BYTES: u64 = 512_000;
const MAX_ATTACHED_CONTEXT_LINES: usize = 500;
const RIPGREP_TIMEOUT: Duration = Duration::from_millis(250);
const RIPGREP_POLL_INTERVAL: Duration = Duration::from_millis(5);
static TOKEN_RE: LazyLock<Regex> = LazyLock::new(|| Regex::new(r"[A-Za-z][A-Za-z0-9_]*").unwrap());
static SYMBOL_RE: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(r"\b(?:pub\s+)?(?:async\s+)?(?:fn|function|class|interface|struct|enum|const|let|var)\s+([A-Za-z_$][A-Za-z0-9_$]*)").unwrap()
});
static PYTHON_SYMBOL_RE: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(r"^\s*(class|def|async\s+def)\s+([A-Za-z_][A-Za-z0-9_]*)").unwrap()
});

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Symbol {
    pub name: String,
    pub kind: String,
    pub path: String,
    pub line: usize,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SearchResult {
    pub path: String,
    pub score: f64,
    pub reason: String,
    pub snippet: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub line_range: Option<ResultLineRange>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub match_lines: Vec<usize>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub explanation: Option<Vec<RankSignal>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub query_plan: Option<QueryPlan>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub duplicate_group: Option<DuplicateGroup>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub context: Option<FileRange>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RankSignal {
    pub kind: String,
    pub value: String,
    pub score: f64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ResultLineRange {
    pub start_line: usize,
    pub end_line: usize,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct QueryPlan {
    pub strategy: String,
    pub require_all: bool,
    pub query_tokens: Vec<String>,
    pub query_phrases: Vec<String>,
    pub query_trigrams: Vec<String>,
    pub planned_postings: Vec<QueryPlanPosting>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub missing_terms: Vec<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub missing_trigrams: Vec<String>,
    pub candidate_count: usize,
    #[serde(default)]
    pub candidate_cap: usize,
    #[serde(default)]
    pub candidate_cap_hit: bool,
    #[serde(default)]
    pub filtered_candidate_count: usize,
    #[serde(default)]
    pub scored_candidate_count: usize,
    #[serde(default)]
    pub final_match_count: usize,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub repair_hints: Vec<QueryPlanRepairHint>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct QueryPlanPosting {
    pub kind: String,
    pub value: String,
    pub postings: usize,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct QueryPlanRepairHint {
    pub kind: String,
    pub message: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub suggested_query: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DuplicateGroup {
    pub canonical_path: String,
    pub duplicate_count: usize,
    pub duplicate_paths: Vec<String>,
}

#[derive(Debug, Copy, Clone, Default, PartialEq, Eq)]
pub enum SnippetMode {
    Short,
    #[default]
    Medium,
    Block,
    Symbol,
}

impl SnippetMode {
    pub fn parse(value: &str) -> Option<Self> {
        match value.to_ascii_lowercase().as_str() {
            "short" => Some(Self::Short),
            "medium" => Some(Self::Medium),
            "block" => Some(Self::Block),
            "symbol" => Some(Self::Symbol),
            _ => None,
        }
    }

    pub(crate) fn window(self) -> (usize, usize) {
        match self {
            Self::Short => (0, 0),
            Self::Medium | Self::Symbol => (1, 2),
            Self::Block => (3, 8),
        }
    }

    pub(crate) fn max_chars(self) -> usize {
        match self {
            Self::Short => 240,
            Self::Medium | Self::Symbol => 700,
            Self::Block => 2_000,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SearchFilters {
    pub file: Option<String>,
    pub path: Option<String>,
    pub language: Option<String>,
    pub extension: Option<String>,
    pub symbol: Option<String>,
    pub repo: Option<String>,
    pub test: Option<bool>,
    pub require_all: bool,
    pub snippet: SnippetMode,
    pub explain: bool,
    pub exclude_file: Vec<String>,
    pub exclude_path: Vec<String>,
    pub exclude_language: Vec<String>,
    pub exclude_extension: Vec<String>,
    pub exclude_symbol: Vec<String>,
    pub exclude_repo: Vec<String>,
}

#[derive(Debug, Clone)]
pub(crate) struct FilterOnlyMatch {
    pub score: f64,
    pub reasons: Vec<String>,
    pub signals: Vec<RankSignal>,
}

impl Default for SearchFilters {
    fn default() -> Self {
        Self {
            file: None,
            path: None,
            language: None,
            extension: None,
            symbol: None,
            repo: None,
            test: None,
            require_all: false,
            snippet: SnippetMode::Medium,
            explain: false,
            exclude_file: Vec::new(),
            exclude_path: Vec::new(),
            exclude_language: Vec::new(),
            exclude_extension: Vec::new(),
            exclude_symbol: Vec::new(),
            exclude_repo: Vec::new(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RelatedFile {
    pub path: String,
    pub reason: String,
    pub score: f64,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RelatedSymbol {
    pub symbol: Symbol,
    pub reason: String,
    pub score: f64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RepoBrief {
    pub root_name: String,
    pub file_count: usize,
    pub language_counts: HashMap<String, usize>,
    pub known_commands: Vec<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub command_hints: Vec<CommandHint>,
    pub manifest_files: Vec<String>,
    pub important_files: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CommandHint {
    pub command: String,
    pub kind: String,
    pub source: String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RepoMap {
    pub brief: RepoBrief,
    pub entrypoints: Vec<String>,
    pub test_files: Vec<String>,
    pub top_symbols: Vec<Symbol>,
    pub related_files: Vec<RepoMapRelatedFile>,
    pub related_symbols: Vec<RepoMapRelatedSymbol>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RepoMapRelatedFile {
    pub source_path: String,
    pub path: String,
    pub reason: String,
    pub score: f64,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RepoMapRelatedSymbol {
    pub source_path: String,
    pub symbol: Symbol,
    pub reason: String,
    pub score: f64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FileRange {
    pub path: String,
    pub start_line: usize,
    pub end_line: usize,
    pub total_lines: usize,
    pub text: String,
}

#[derive(Debug, Clone)]
struct IndexedFile {
    path: String,
    language: String,
    text: String,
    tokens: HashMap<String, usize>,
    symbols: Vec<Symbol>,
}

#[derive(Debug, Clone)]
pub struct RepoIndex {
    root: PathBuf,
    files: HashMap<String, IndexedFile>,
    symbols: Vec<Symbol>,
    doc_freq: HashMap<String, usize>,
}

#[derive(Debug, Clone)]
pub struct RepoIndexer {
    root: PathBuf,
}

pub fn search_repo_fast(
    root: impl AsRef<Path>,
    query: &str,
    limit: usize,
) -> Result<Vec<SearchResult>> {
    search_repo_fast_filtered(root, query, limit, &SearchFilters::default())
}

pub fn search_repo_fast_filtered(
    root: impl AsRef<Path>,
    query: &str,
    limit: usize,
    filters: &SearchFilters,
) -> Result<Vec<SearchResult>> {
    search_repo_fast_filtered_with_timeout(root, query, limit, filters, RIPGREP_TIMEOUT)
}

pub fn search_repo_fast_filtered_with_timeout(
    root: impl AsRef<Path>,
    query: &str,
    limit: usize,
    filters: &SearchFilters,
    timeout: Duration,
) -> Result<Vec<SearchResult>> {
    let root = root.as_ref().canonicalize()?;
    let parsed = parse_query(query);
    let query_phrases = query_phrases(&parsed.terms);
    let mut filters = merge_filters(filters.clone(), parsed.filters);
    if !repo_matches(&root, &filters) {
        return Ok(Vec::new());
    }
    let query = query_text(&parsed.terms, &filters);
    let query_tokens = tokenize(&query);
    if limit == 0 {
        return Ok(Vec::new());
    }
    if query_tokens.is_empty() && query_phrases.is_empty() {
        return if filter_only_query(&filters) {
            search_repo_filter_only(&root, limit, &filters)
        } else {
            Ok(Vec::new())
        };
    }
    if query_tokens.len() > 1 {
        filters.require_all = true;
    }

    if let Some(results) = search_repo_ripgrep(
        &root,
        &query_tokens,
        &query_phrases,
        limit,
        &filters,
        timeout,
    )? {
        return Ok(results);
    }

    search_repo_streaming(&root, &query_tokens, &query_phrases, limit, &filters)
}

fn search_repo_filter_only(
    root: &Path,
    limit: usize,
    filters: &SearchFilters,
) -> Result<Vec<SearchResult>> {
    let mut candidates = Vec::new();
    let candidate_cap = (limit.max(1) * 100).clamp(100, 5_000);

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
        if metadata.len() > MAX_FILE_BYTES || language_for(path).is_none() {
            continue;
        }
        let rel = path
            .strip_prefix(root)?
            .to_string_lossy()
            .replace('\\', "/");
        let Some(matched) = score_filter_only_path(&rel, filters, filters.explain) else {
            continue;
        };
        candidates.push((rel, matched));
        if candidates.len() >= candidate_cap {
            break;
        }
    }

    candidates.sort_by(|(left_path, left), (right_path, right)| {
        right
            .score
            .partial_cmp(&left.score)
            .unwrap_or(Ordering::Equal)
            .then_with(|| left_path.cmp(right_path))
    });
    candidates.truncate(limit.max(1) * 20);

    let mut results = Vec::new();
    for (path, matched) in candidates {
        let text = fs::read_to_string(root.join(&path)).unwrap_or_default();
        if text.contains('\0') {
            continue;
        }
        results.push(filter_only_search_result(
            &path,
            &text,
            matched,
            filters.snippet,
            filters.explain,
        ));
    }

    Ok(finalize_results(results, limit))
}

fn search_repo_ripgrep(
    root: &Path,
    query_tokens: &[String],
    query_phrases: &[String],
    limit: usize,
    filters: &SearchFilters,
    timeout: Duration,
) -> Result<Option<Vec<SearchResult>>> {
    let mut command = Command::new("rg");
    command
        .current_dir(root)
        .arg("--json")
        .arg("--hidden")
        .arg("--ignore-case")
        .arg("--fixed-strings")
        .arg("--line-number")
        .arg("--max-count")
        .arg("12")
        .arg("--max-filesize")
        .arg(format!("{MAX_FILE_BYTES}"))
        .arg("--glob")
        .arg("!.git/**")
        .arg("--glob")
        .arg("!.venv/**")
        .arg("--glob")
        .arg("!__pycache__/**")
        .arg("--glob")
        .arg("!.pytest_cache/**")
        .arg("--glob")
        .arg("!.orient/**")
        .arg("--glob")
        .arg("!node_modules/**")
        .arg("--glob")
        .arg("!dist/**")
        .arg("--glob")
        .arg("!build/**")
        .arg("--glob")
        .arg("!.next/**")
        .arg("--glob")
        .arg("!coverage/**")
        .arg("--glob")
        .arg("!target/**");

    for token in query_tokens {
        command.arg("-e").arg(token);
    }
    for phrase in query_phrases {
        command.arg("-e").arg(phrase);
    }
    command.arg(".");

    let Ok(mut child) = command
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .spawn()
    else {
        return Ok(None);
    };
    let Some(stdout) = child.stdout.take() else {
        return Ok(None);
    };
    let (lines_tx, lines_rx) = mpsc::channel();
    thread::spawn(move || {
        for line in BufReader::new(stdout).lines() {
            if lines_tx.send(line).is_err() {
                break;
            }
        }
    });

    let mut scored: HashMap<String, SearchResult> = HashMap::new();
    let max_matches = (limit.max(1) * 300).clamp(1_000, 8_000);
    let mut match_count = 0usize;
    let deadline = Instant::now() + timeout;

    loop {
        let now = Instant::now();
        if now >= deadline {
            let _ = child.kill();
            break;
        }
        let wait_for = (deadline - now).min(RIPGREP_POLL_INTERVAL);
        let line = match lines_rx.recv_timeout(wait_for) {
            Ok(line) => line?,
            Err(mpsc::RecvTimeoutError::Timeout) => {
                if child.try_wait()?.is_some() {
                    break;
                }
                continue;
            }
            Err(mpsc::RecvTimeoutError::Disconnected) => break,
        };
        let Ok(value) = serde_json::from_str::<serde_json::Value>(&line) else {
            continue;
        };
        if value.get("type").and_then(|value| value.as_str()) != Some("match") {
            continue;
        }
        let Some(data) = value.get("data") else {
            continue;
        };
        let Some(raw_path) = data
            .get("path")
            .and_then(|path| path.get("text"))
            .and_then(|text| text.as_str())
        else {
            continue;
        };
        let path = raw_path
            .trim_start_matches("./")
            .trim_start_matches('/')
            .to_string();
        if language_for(Path::new(&path)).is_none()
            || is_ignored(Path::new(&path))
            || !matches_filters(&path, filters)
        {
            continue;
        }
        let Some(text) = data
            .get("lines")
            .and_then(|lines| lines.get("text"))
            .and_then(|text| text.as_str())
        else {
            continue;
        };
        let line_number = data
            .get("line_number")
            .and_then(|line| line.as_u64())
            .unwrap_or_default();
        merge_match_result(
            &mut scored,
            &root,
            &path,
            text,
            line_number,
            query_tokens,
            query_phrases,
            true,
            filters.snippet,
            filters.explain,
        );
        match_count += 1;
        if match_count >= max_matches {
            let _ = child.kill();
            break;
        }
    }

    let _ = child.wait();
    let mut results = scored.into_values().collect::<Vec<_>>();
    if filters.require_all {
        results.retain(|result| result_matches_all_tokens(result, query_tokens));
    }
    if !query_phrases.is_empty() {
        results.retain(|result| result_or_file_matches_phrases(root, result, query_phrases));
    }
    results.retain(|result| result_matches_symbol_filters(result, filters));
    Ok(Some(finalize_results(results, limit)))
}

fn search_repo_streaming(
    root: &Path,
    query_tokens: &[String],
    query_phrases: &[String],
    limit: usize,
    filters: &SearchFilters,
) -> Result<Vec<SearchResult>> {
    let mut results = Vec::new();

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
        if metadata.len() > MAX_FILE_BYTES || language_for(path).is_none() {
            continue;
        }
        let text = fs::read_to_string(path).unwrap_or_default();
        if text.contains('\0') {
            continue;
        }
        let rel = path
            .strip_prefix(&root)?
            .to_string_lossy()
            .replace('\\', "/");
        if !matches_filters(&rel, filters) {
            continue;
        }
        if let Some(result) = score_text_file(
            &rel,
            &text,
            &query_tokens,
            query_phrases,
            true,
            filters.snippet,
            filters.explain,
        ) {
            results.push(result);
        }
    }

    if filters.require_all {
        results.retain(|result| result_matches_all_tokens(result, query_tokens));
    }
    results.retain(|result| result_matches_symbol_filters(result, filters));
    Ok(finalize_results(results, limit))
}

fn merge_match_result(
    scored: &mut HashMap<String, SearchResult>,
    root: &Path,
    path: &str,
    line: &str,
    line_number: u64,
    query_tokens: &[String],
    query_phrases: &[String],
    parse_symbols: bool,
    snippet_mode: SnippetMode,
    explain: bool,
) {
    let path_lower = path.to_lowercase();
    let line_lower = line.to_lowercase();
    let query_name = query_tokens.join("");
    let mut score = 0.0;
    let mut reasons = Vec::new();
    let mut signals = Vec::new();
    let _ = apply_phrase_matches(
        path,
        line,
        query_phrases,
        "line_phrase",
        12.0,
        &mut score,
        &mut reasons,
        &mut signals,
    );
    let match_lines = if line_number > 0
        && (query_tokens.iter().any(|token| line_lower.contains(token))
            || query_phrases
                .iter()
                .any(|phrase| line_lower.contains(phrase)))
    {
        vec![line_number as usize]
    } else {
        Vec::new()
    };

    for token in query_tokens {
        let mut token_score = 0.0;
        if path_lower.contains(token) {
            token_score += 6.0;
            signals.push(rank_signal("path_match", token, 6.0));
        }
        if line_lower.contains(token) {
            token_score += 2.0;
            signals.push(rank_signal("line_match", token, 2.0));
        }
        if token_score > 0.0 {
            score += token_score;
            reasons.push(token.clone());
        }
    }

    if parse_symbols {
        apply_symbol_boost(
            path,
            line,
            query_tokens,
            &query_name,
            &mut score,
            &mut reasons,
            &mut signals,
        );
    }

    if score == 0.0 {
        return;
    }

    let snippet_line = line.trim_end();
    let snippet = if matches!(snippet_mode, SnippetMode::Block | SnippetMode::Symbol) {
        fs::read_to_string(root.join(path))
            .ok()
            .map(|text| best_snippet_for_path(path, &text, query_tokens, snippet_mode))
            .filter(|snippet| !snippet.is_empty())
            .unwrap_or_else(|| {
                if line_number > 0 {
                    format!("{line_number}: {snippet_line}")
                } else {
                    snippet_line.to_string()
                }
            })
    } else if line_number > 0 {
        format!("{line_number}: {snippet_line}")
    } else {
        snippet_line.to_string()
    }
    .chars()
    .take(snippet_mode.max_chars())
    .collect::<String>();

    scored
        .entry(path.to_string())
        .and_modify(|result| {
            result.score = round4(result.score + score);
            if explain {
                result
                    .explanation
                    .get_or_insert_with(Vec::new)
                    .extend(signals.clone());
            }
            result.match_lines.extend(match_lines.iter().copied());
            if !matches!(snippet_mode, SnippetMode::Block | SnippetMode::Symbol)
                && result.snippet.len() < snippet_mode.max_chars()
                && !result.snippet.contains(snippet_line)
            {
                result.snippet.push('\n');
                result
                    .snippet
                    .push_str(&snippet.chars().take(240).collect::<String>());
            }
            let mut merged = result
                .reason
                .trim_start_matches("matched ")
                .split(", ")
                .filter(|value| !value.is_empty())
                .map(String::from)
                .collect::<HashSet<_>>();
            for reason in &reasons {
                merged.insert(reason.clone());
            }
            let mut merged = merged.into_iter().collect::<Vec<_>>();
            merged.sort();
            result.reason = format!("matched {}", merged.join(", "));
        })
        .or_insert_with(|| SearchResult {
            path: path.to_string(),
            score: round4(score),
            reason: format!("matched {}", reasons.join(", ")),
            snippet,
            line_range: None,
            match_lines,
            explanation: explain.then_some(signals),
            query_plan: None,
            duplicate_group: None,
            context: None,
        });
}

impl RepoIndexer {
    pub fn new(root: impl AsRef<Path>) -> Self {
        Self {
            root: root.as_ref().to_path_buf(),
        }
    }

    pub fn build(&self) -> Result<RepoIndex> {
        let root = self.root.canonicalize()?;
        let mut files = HashMap::new();

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
            let text = fs::read_to_string(path).unwrap_or_default();
            if text.contains('\0') {
                continue;
            }
            let rel = path
                .strip_prefix(&root)?
                .to_string_lossy()
                .replace('\\', "/");
            let symbols = extract_symbols(&rel, &text, &language);
            let tokens = token_counts(&format!("{rel}\n{text}"));
            files.insert(
                rel.clone(),
                IndexedFile {
                    path: rel,
                    language,
                    text,
                    tokens,
                    symbols,
                },
            );
        }

        let symbols = files
            .values()
            .flat_map(|file| file.symbols.clone())
            .collect::<Vec<_>>();
        let doc_freq = build_doc_freq(&files);

        Ok(RepoIndex {
            root,
            files,
            symbols,
            doc_freq,
        })
    }
}

impl RepoIndex {
    pub fn find_symbol(&self, name: &str, limit: usize) -> Vec<Symbol> {
        let needle = normalize_token(name);
        if needle.is_empty() || limit == 0 {
            return Vec::new();
        }

        let mut scored = Vec::new();
        for symbol in &self.symbols {
            let symbol_token = normalize_token(&symbol.name);
            let score = if symbol.name == name {
                100
            } else if symbol_token == needle {
                90
            } else if symbol_token.contains(&needle) {
                60
            } else {
                0
            };
            if score > 0 {
                scored.push((score, symbol.clone()));
            }
        }
        scored.sort_by(|a, b| b.0.cmp(&a.0).then_with(|| a.1.path.cmp(&b.1.path)));
        scored
            .into_iter()
            .take(limit)
            .map(|(_, symbol)| symbol)
            .collect()
    }

    pub fn search_code(&self, query: &str, limit: usize) -> Vec<SearchResult> {
        let query_tokens = tokenize(query);
        if query_tokens.is_empty() {
            return Vec::new();
        }
        let query_set = query_tokens.iter().cloned().collect::<HashSet<_>>();
        let total_docs = self.files.len().max(1) as f64;
        let mut results = Vec::new();

        for file in self.files.values() {
            let mut score = 0.0;
            let mut reasons = HashSet::new();
            for token in &query_tokens {
                let Some(tf) = file.tokens.get(token) else {
                    continue;
                };
                let df = *self.doc_freq.get(token).unwrap_or(&0) as f64;
                let idf = ((total_docs + 1.0) / (df + 1.0)).ln() + 1.0;
                score += (1.0 + (*tf as f64).ln()) * idf;
                reasons.insert(token.clone());
            }
            for symbol in &file.symbols {
                let overlap = tokenize(&symbol.name)
                    .into_iter()
                    .filter(|token| query_set.contains(token))
                    .collect::<Vec<_>>();
                if !overlap.is_empty() {
                    score += 2.0 * overlap.len() as f64;
                    for token in overlap {
                        reasons.insert(token);
                    }
                }
            }
            if score > 0.0 {
                let mut reasons = reasons.into_iter().collect::<Vec<_>>();
                reasons.sort();
                results.push(SearchResult {
                    path: file.path.clone(),
                    score: round4(score),
                    reason: format!("matched {}", reasons.join(", ")),
                    snippet: best_snippet(&file.text, &query_tokens),
                    line_range: None,
                    match_lines: match_lines_from_text(&file.text, &query_tokens, &[], 16),
                    explanation: None,
                    query_plan: None,
                    duplicate_group: None,
                    context: None,
                });
            }
        }

        finalize_results(results, limit)
    }

    pub fn related_files(&self, path: &str, limit: usize) -> Vec<RelatedFile> {
        let normalized = path.trim_start_matches('/').to_string();
        let stem = Path::new(&normalized)
            .file_stem()
            .map(|value| value.to_string_lossy().to_string())
            .unwrap_or_default();
        let directory = Path::new(&normalized)
            .parent()
            .map(|value| value.to_string_lossy().to_string())
            .unwrap_or_default();
        let source_symbols = self
            .files
            .get(&normalized)
            .map(|file| {
                file.symbols
                    .iter()
                    .map(|symbol| symbol.name.clone())
                    .collect::<Vec<_>>()
            })
            .unwrap_or_default();
        let mut related = Vec::new();
        for (file_path, file) in &self.files {
            if file_path == &normalized {
                continue;
            }
            let lower = file_path.to_lowercase();
            let mut score = 0.0;
            let mut reasons = Vec::new();
            if !stem.is_empty() && lower.contains(&stem.to_lowercase()) {
                score += 4.0;
                reasons.push(format!("shares stem {stem}"));
            }
            if is_test_path(&lower)
                && !stem.is_empty()
                && lower.contains(&stem.replace("test_", "").to_lowercase())
            {
                score += 5.0;
                reasons.push("test coverage candidate".to_string());
            }
            let file_dir = Path::new(file_path)
                .parent()
                .map(|value| value.to_string_lossy().to_string())
                .unwrap_or_default();
            if !directory.is_empty() && file_dir == directory {
                score += 1.0;
                reasons.push("same directory".to_string());
            }
            let text_lower = file.text.to_ascii_lowercase();
            for symbol in &source_symbols {
                if text_lower.contains(&symbol.to_ascii_lowercase()) {
                    score += 6.0;
                    reasons.push(format!("references symbol {symbol}"));
                    break;
                }
            }
            if score > 0.0 {
                related.push(RelatedFile {
                    path: file_path.clone(),
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

        for symbol in &self.symbols {
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
                let symbol_tokens = tokenize(&symbol.name)
                    .into_iter()
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
                if !query_symbol.is_empty() && normalize_token(&symbol.name) == query_symbol {
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
                    symbol: symbol.clone(),
                    reason: reasons.join("; "),
                    score: round4(score),
                });
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

    pub fn repo_brief(&self) -> RepoBrief {
        let mut language_counts = HashMap::new();
        for file in self.files.values() {
            *language_counts.entry(file.language.clone()).or_insert(0) += 1;
        }
        let mut manifest_files = self
            .files
            .keys()
            .filter(|path| is_manifest_file(path))
            .cloned()
            .collect::<Vec<_>>();
        manifest_files.sort();

        let mut important_files = self
            .files
            .keys()
            .filter(|path| is_important_file(path))
            .cloned()
            .collect::<Vec<_>>();
        important_files.sort();

        let command_hints = self.command_hints();
        let known_commands = known_commands_from_hints(&command_hints);

        RepoBrief {
            root_name: self
                .root
                .file_name()
                .map(|value| value.to_string_lossy().to_string())
                .unwrap_or_else(|| self.root.display().to_string()),
            file_count: self.files.len(),
            language_counts,
            known_commands,
            command_hints,
            manifest_files,
            important_files,
        }
    }

    pub fn repo_map(&self, symbol_limit: usize, test_limit: usize) -> RepoMap {
        let mut entrypoints = self
            .files
            .keys()
            .filter(|path| is_entrypoint_path(path))
            .cloned()
            .collect::<Vec<_>>();
        entrypoints.sort();

        let mut test_files = self
            .files
            .keys()
            .filter(|path| is_test_path(&path.to_ascii_lowercase()))
            .cloned()
            .collect::<Vec<_>>();
        test_files.sort();
        test_files.truncate(test_limit);

        let mut top_symbols = self.symbols.clone();
        top_symbols.sort_by(|a, b| {
            symbol_kind_rank(&a.kind)
                .cmp(&symbol_kind_rank(&b.kind))
                .then_with(|| a.path.cmp(&b.path))
                .then_with(|| a.line.cmp(&b.line))
                .then_with(|| a.name.cmp(&b.name))
        });
        top_symbols.truncate(symbol_limit);

        let brief = self.repo_brief();
        let mut related_file_seeds = brief.important_files.clone();
        related_file_seeds.extend(top_symbols.iter().map(|symbol| symbol.path.clone()));
        let related_files =
            self.repo_map_related_files(&entrypoints, &test_files, &related_file_seeds, 12);
        let related_symbols =
            self.repo_map_related_symbols(&entrypoints, &test_files, &top_symbols, 12);

        RepoMap {
            brief,
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
    ) -> Vec<RepoMapRelatedFile> {
        let mut seen = HashSet::new();
        let mut related = Vec::new();
        for source_path in repo_map_seed_paths(entrypoints, test_files, important_files) {
            for item in self.related_files(&source_path, 3) {
                if seen.insert((source_path.clone(), item.path.clone())) {
                    related.push(RepoMapRelatedFile {
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
    ) -> Vec<RepoMapRelatedSymbol> {
        let important_files = top_symbols
            .iter()
            .map(|symbol| symbol.path.clone())
            .collect::<Vec<_>>();
        let mut seen = HashSet::new();
        let mut related = Vec::new();
        for source_path in repo_map_seed_paths(entrypoints, test_files, &important_files) {
            for item in self.related_symbols(Some(&source_path), None, 3) {
                let key = (
                    source_path.clone(),
                    item.symbol.path.clone(),
                    item.symbol.line,
                    item.symbol.name.clone(),
                );
                if seen.insert(key) {
                    related.push(RepoMapRelatedSymbol {
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

    fn command_hints(&self) -> Vec<CommandHint> {
        command_hints_from_manifest_texts(
            self.files
                .iter()
                .map(|(path, file)| (path.as_str(), file.text.as_str())),
        )
    }
}

pub(crate) fn repo_map_seed_paths(
    entrypoints: &[String],
    test_files: &[String],
    important_files: &[String],
) -> Vec<String> {
    let mut seeds = Vec::new();
    for path in entrypoints
        .iter()
        .chain(test_files.iter())
        .chain(important_files.iter())
    {
        if !seeds.contains(path) {
            seeds.push(path.clone());
        }
        if seeds.len() >= 12 {
            break;
        }
    }
    seeds
}

pub fn read_file_range(
    root: impl AsRef<Path>,
    path: &str,
    start_line: usize,
    line_count: usize,
) -> Result<FileRange> {
    let root = root.as_ref().canonicalize()?;
    let requested = Path::new(path);
    anyhow::ensure!(
        requested.is_relative()
            && !requested
                .components()
                .any(|component| matches!(component, std::path::Component::ParentDir)),
        "path must be repo-relative"
    );
    let absolute = root.join(requested).canonicalize()?;
    anyhow::ensure!(
        absolute.starts_with(&root),
        "path must stay inside repository"
    );
    anyhow::ensure!(absolute.is_file(), "path is not a file");
    let metadata = absolute.metadata()?;
    anyhow::ensure!(
        metadata.len() <= MAX_FILE_BYTES,
        "file exceeds max readable size"
    );
    let text = fs::read_to_string(&absolute)?;
    anyhow::ensure!(!text.contains('\0'), "file appears to be binary");

    let rel = absolute
        .strip_prefix(&root)?
        .to_string_lossy()
        .replace('\\', "/");

    Ok(file_range_from_text(rel, &text, start_line, line_count))
}

pub(crate) fn file_range_from_text(
    path: impl Into<String>,
    text: &str,
    start_line: usize,
    line_count: usize,
) -> FileRange {
    let lines = text.lines().collect::<Vec<_>>();
    let total_lines = lines.len();
    let start = start_line.max(1).min(total_lines.max(1));
    let count = line_count.max(1);
    let end = (start + count - 1).min(total_lines);
    let range_text = if total_lines == 0 {
        String::new()
    } else {
        format_numbered_lines(&lines, start - 1, end)
    };

    FileRange {
        path: path.into(),
        start_line: start,
        end_line: end,
        total_lines,
        text: range_text,
    }
}

pub(crate) fn is_ignored(path: &Path) -> bool {
    path.components().any(|component| {
        let part = component.as_os_str().to_string_lossy();
        matches!(
            part.as_ref(),
            ".git"
                | ".venv"
                | "__pycache__"
                | ".pytest_cache"
                | ".orient"
                | "node_modules"
                | "dist"
                | "build"
                | ".next"
                | "coverage"
                | "target"
        )
    })
}

pub(crate) fn regular_file_metadata(path: &Path) -> Option<fs::Metadata> {
    let metadata = fs::symlink_metadata(path).ok()?;
    metadata.file_type().is_file().then_some(metadata)
}

pub(crate) fn command_hints_from_manifest_texts<'a>(
    files: impl IntoIterator<Item = (&'a str, &'a str)>,
) -> Vec<CommandHint> {
    let mut files = files.into_iter().collect::<Vec<_>>();
    files.sort_by(|left, right| left.0.cmp(right.0));
    let manifest_path = |name: &str| manifest_path_in_files(&files, name);
    let has_file = |name: &str| manifest_path(name).is_some();

    let mut hints = Vec::new();
    if let Some(source) = manifest_path("Cargo.toml") {
        hints.push(command_hint("cargo test", "test", source));
    }
    if let Some(source) = manifest_path("pyproject.toml") {
        hints.push(command_hint("pytest", "test", source));
    }
    for (path, package_json) in files.iter().filter(|(path, _)| {
        Path::new(path).file_name().and_then(|value| value.to_str()) == Some("package.json")
    }) {
        hints.extend(package_json_command_hints(
            package_json,
            package_manager_command(&has_file),
            path,
        ));
    }
    if let Some(source) = manifest_path("go.mod") {
        hints.push(command_hint("go test ./...", "test", source));
    }
    if let Some(source) = manifest_path("Package.swift") {
        hints.push(command_hint("swift test", "test", source));
    }
    if let Some(source) = manifest_path("Makefile") {
        hints.push(command_hint("make test", "test", source));
    }
    hints.sort_by(|left, right| {
        left.command
            .cmp(&right.command)
            .then_with(|| left.source.cmp(&right.source))
            .then_with(|| left.kind.cmp(&right.kind))
    });
    hints.dedup_by(|left, right| left.command == right.command && left.source == right.source);
    hints
}

pub(crate) fn known_commands_from_hints(hints: &[CommandHint]) -> Vec<String> {
    let mut commands = hints
        .iter()
        .map(|hint| hint.command.clone())
        .collect::<Vec<_>>();
    commands.sort();
    commands.dedup();
    commands
}

fn manifest_path_in_files(files: &[(&str, &str)], name: &str) -> Option<String> {
    files
        .iter()
        .find(|(path, _)| {
            Path::new(path).file_name().and_then(|value| value.to_str()) == Some(name)
        })
        .map(|(path, _)| (*path).to_string())
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

fn package_manager_command(has_file: &impl Fn(&str) -> bool) -> &'static str {
    if has_file("pnpm-lock.yaml") {
        "pnpm"
    } else if has_file("yarn.lock") {
        "yarn"
    } else if has_file("bun.lock") || has_file("bun.lockb") {
        "bun"
    } else {
        "npm"
    }
}

fn package_json_command_hints(
    package_json: &str,
    package_manager: &str,
    source: &str,
) -> Vec<CommandHint> {
    let Ok(value) = serde_json::from_str::<serde_json::Value>(package_json) else {
        return vec![command_hint(
            format!("{package_manager} test"),
            "test",
            source,
        )];
    };
    let Some(scripts) = value.get("scripts").and_then(|value| value.as_object()) else {
        return vec![command_hint(
            format!("{package_manager} test"),
            "test",
            source,
        )];
    };

    ["test", "lint", "typecheck", "check", "build"]
        .into_iter()
        .filter(|script| scripts.contains_key(*script))
        .map(|script| {
            let command = if script == "test" {
                format!("{package_manager} test")
            } else {
                format!("{package_manager} run {script}")
            };
            command_hint(command, script, source)
        })
        .collect()
}

pub(crate) fn language_for(path: &Path) -> Option<String> {
    let file_name = path.file_name()?.to_string_lossy();
    if matches!(
        file_name.as_ref(),
        "README"
            | "README.md"
            | "AGENTS.md"
            | "CLAUDE.md"
            | "Makefile"
            | "yarn.lock"
            | "bun.lock"
            | "bun.lockb"
    ) {
        return Some("text".to_string());
    }
    let ext = path.extension()?.to_string_lossy().to_lowercase();
    let language = match ext.as_str() {
        "py" => "python",
        "rs" => "rust",
        "js" | "jsx" => "javascript",
        "ts" | "tsx" => "typescript",
        "go" => "go",
        "rb" => "ruby",
        "java" => "java",
        "kt" => "kotlin",
        "swift" => "swift",
        "md" => "markdown",
        "toml" => "toml",
        "json" => "json",
        "yaml" | "yml" => "yaml",
        _ => return None,
    };
    Some(language.to_string())
}

pub(crate) fn extract_symbols(path: &str, text: &str, language: &str) -> Vec<Symbol> {
    if language == "python" {
        return extract_python_symbols(path, text);
    }
    text.lines()
        .enumerate()
        .filter_map(|(index, line)| {
            let capture = SYMBOL_RE.captures(line)?;
            let kind = if line.contains("class ") {
                "class"
            } else if line.contains("struct ") {
                "struct"
            } else if line.contains("enum ") {
                "enum"
            } else {
                "function"
            };
            Some(Symbol {
                name: capture.get(1)?.as_str().to_string(),
                kind: kind.to_string(),
                path: path.to_string(),
                line: index + 1,
            })
        })
        .collect()
}

fn extract_python_symbols(path: &str, text: &str) -> Vec<Symbol> {
    text.lines()
        .enumerate()
        .filter_map(|(index, line)| {
            let capture = PYTHON_SYMBOL_RE.captures(line)?;
            let raw_kind = capture.get(1)?.as_str();
            Some(Symbol {
                name: capture.get(2)?.as_str().to_string(),
                kind: if raw_kind == "class" {
                    "class"
                } else {
                    "function"
                }
                .to_string(),
                path: path.to_string(),
                line: index + 1,
            })
        })
        .collect()
}

fn build_doc_freq(files: &HashMap<String, IndexedFile>) -> HashMap<String, usize> {
    let mut doc_freq = HashMap::new();
    for file in files.values() {
        for token in file.tokens.keys() {
            *doc_freq.entry(token.clone()).or_insert(0) += 1;
        }
    }
    doc_freq
}

pub(crate) fn token_counts(text: &str) -> HashMap<String, usize> {
    let mut counts = HashMap::new();
    for token in tokenize(text) {
        *counts.entry(token).or_insert(0) += 1;
    }
    counts
}

fn score_text_file(
    path: &str,
    text: &str,
    query_tokens: &[String],
    query_phrases: &[String],
    parse_symbols: bool,
    snippet_mode: SnippetMode,
    explain: bool,
) -> Option<SearchResult> {
    let path_lower = path.to_lowercase();
    let text_lower = text.to_lowercase();
    let query_name = query_tokens.join("");
    let mut score = 0.0;
    let mut reasons = Vec::new();
    let mut signals = Vec::new();
    if !apply_phrase_matches(
        path,
        text,
        query_phrases,
        "content_phrase",
        16.0,
        &mut score,
        &mut reasons,
        &mut signals,
    ) {
        return None;
    }

    for token in query_tokens {
        let mut token_score = 0.0;
        if path_lower.contains(token) {
            token_score += 6.0;
            signals.push(rank_signal("path_match", token, 6.0));
        }
        let occurrences = text_lower.matches(token).take(12).count();
        if occurrences > 0 {
            let amount = 1.0 + (occurrences as f64).ln();
            token_score += amount;
            signals.push(rank_signal("content_match", token, amount));
        }
        if token_score > 0.0 {
            score += token_score;
            reasons.push(token.clone());
        }
    }

    if parse_symbols {
        let language = language_for(Path::new(path)).unwrap_or_else(|| "text".to_string());
        for symbol in extract_symbols(path, text, &language) {
            apply_symbol_match(
                &symbol.name,
                query_tokens,
                &query_name,
                &mut score,
                &mut reasons,
                &mut signals,
            );
        }
    }

    if score == 0.0 {
        return None;
    }

    Some(SearchResult {
        path: path.to_string(),
        score: round4(score),
        reason: format!("matched {}", reasons.join(", ")),
        snippet: best_snippet_for_path(path, text, query_tokens, snippet_mode),
        line_range: None,
        match_lines: match_lines_from_text(text, query_tokens, query_phrases, 16),
        explanation: explain.then_some(signals),
        query_plan: None,
        duplicate_group: None,
        context: None,
    })
}

pub fn attach_result_context(
    results: &mut [SearchResult],
    line_count: usize,
    mut read_range: impl FnMut(&str, usize, usize) -> Result<FileRange>,
) -> Result<()> {
    let Some(line_count) = attached_context_line_count(line_count) else {
        return Ok(());
    };
    for result in results {
        let start = context_start_line(result, line_count);
        result.context = Some(read_range(&result.path, start, line_count)?);
    }
    Ok(())
}

fn attached_context_line_count(line_count: usize) -> Option<usize> {
    (line_count > 0).then(|| line_count.min(MAX_ATTACHED_CONTEXT_LINES))
}

fn context_start_line(result: &SearchResult, line_count: usize) -> usize {
    let anchor = result
        .match_lines
        .first()
        .copied()
        .or_else(|| result.line_range.as_ref().map(|range| range.start_line))
        .unwrap_or(1);
    anchor.saturating_sub(line_count / 3).max(1)
}

fn apply_symbol_boost(
    path: &str,
    line: &str,
    query_tokens: &[String],
    query_name: &str,
    score: &mut f64,
    reasons: &mut Vec<String>,
    signals: &mut Vec<RankSignal>,
) {
    let Some(language) = language_for(Path::new(path)) else {
        return;
    };
    for symbol in extract_symbols(path, line, &language) {
        apply_symbol_match(
            &symbol.name,
            query_tokens,
            query_name,
            score,
            reasons,
            signals,
        );
    }
}

pub(crate) fn apply_phrase_matches(
    path_lower: &str,
    content_lower: &str,
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
    let path_phrase_text = normalize_phrase_text(path_lower);
    let content_phrase_text = normalize_phrase_text(content_lower);
    let matches = query_phrases
        .iter()
        .map(|phrase| {
            (
                phrase,
                path_phrase_text.contains(phrase),
                content_phrase_text.contains(phrase),
            )
        })
        .collect::<Vec<_>>();
    if matches
        .iter()
        .any(|(_, path_match, content_match)| !path_match && !content_match)
    {
        return false;
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

fn result_or_file_matches_phrases(
    root: &Path,
    result: &SearchResult,
    query_phrases: &[String],
) -> bool {
    let result_text = normalize_phrase_text(&format!("{}\n{}", result.path, result.snippet));
    if query_phrases
        .iter()
        .all(|phrase| result_text.contains(phrase))
    {
        return true;
    }
    fs::read_to_string(root.join(&result.path))
        .ok()
        .map(|text| {
            let text = normalize_phrase_text(&text);
            query_phrases.iter().all(|phrase| text.contains(phrase))
        })
        .unwrap_or(false)
}

fn apply_symbol_match(
    symbol_name: &str,
    query_tokens: &[String],
    query_name: &str,
    score: &mut f64,
    reasons: &mut Vec<String>,
    signals: &mut Vec<RankSignal>,
) {
    let normalized = normalize_token(symbol_name);
    if normalized == query_name || query_tokens.contains(&normalized) {
        *score += 25.0;
        reasons.push(format!("symbol:{symbol_name}"));
        signals.push(rank_signal("symbol_exact", symbol_name, 25.0));
        return;
    }
    let overlap = tokenize(symbol_name)
        .into_iter()
        .filter(|token| query_tokens.contains(token))
        .count();
    if overlap > 0 {
        let amount = 4.0 * overlap as f64;
        *score += amount;
        reasons.push(format!("symbol:{symbol_name}"));
        signals.push(rank_signal("symbol_overlap", symbol_name, amount));
    }
}

fn rank_signal(kind: &str, value: &str, score: f64) -> RankSignal {
    RankSignal {
        kind: kind.to_string(),
        value: value.to_string(),
        score: round4(score),
    }
}

pub(crate) fn tokenize(text: &str) -> Vec<String> {
    let split = identifier_boundary_text(text);
    TOKEN_RE
        .find_iter(&split)
        .map(|m| m.as_str().to_lowercase())
        .filter(|token| token.len() > 1)
        .collect()
}

pub(crate) fn normalize_token(text: &str) -> String {
    tokenize(text).join("")
}

pub(crate) fn identifier_boundary_text(text: &str) -> String {
    let chars = text.chars().collect::<Vec<_>>();
    let mut split = String::with_capacity(text.len());
    for (index, ch) in chars.iter().copied().enumerate() {
        if should_split_identifier_boundary(&chars, index) {
            split.push(' ');
        }
        split.push(ch);
    }
    split.replace('_', " ")
}

fn should_split_identifier_boundary(chars: &[char], index: usize) -> bool {
    let ch = chars[index];
    if !ch.is_uppercase() {
        return false;
    }
    let Some(previous) = index
        .checked_sub(1)
        .and_then(|previous| chars.get(previous))
    else {
        return false;
    };
    if previous.is_lowercase() || previous.is_ascii_digit() {
        return true;
    }
    previous.is_uppercase() && chars.get(index + 1).is_some_and(|next| next.is_lowercase())
}

pub(crate) fn best_snippet(text: &str, query_tokens: &[String]) -> String {
    best_snippet_for_path("", text, query_tokens, SnippetMode::Medium)
}

pub(crate) fn best_snippet_for_path(
    path: &str,
    text: &str,
    query_tokens: &[String],
    mode: SnippetMode,
) -> String {
    let lines = text.lines().collect::<Vec<_>>();
    if matches!(mode, SnippetMode::Symbol) {
        let language = language_for(Path::new(path)).unwrap_or_else(|| "text".to_string());
        if let Some(symbol_line) = extract_symbols(path, text, &language)
            .into_iter()
            .find(|symbol| {
                let normalized = normalize_token(&symbol.name);
                normalized == query_tokens.join("")
                    || tokenize(&symbol.name)
                        .into_iter()
                        .any(|token| query_tokens.contains(&token))
            })
            .map(|symbol| symbol.line)
        {
            return format_snippet_window(&lines, symbol_line.saturating_sub(1), mode);
        }
    }
    for (idx, line) in lines.iter().enumerate() {
        let lowered = line.to_lowercase();
        if query_tokens.iter().any(|token| lowered.contains(token)) {
            return format_snippet_window(&lines, idx, mode);
        }
    }
    let (_, after) = mode.window();
    format_numbered_lines(&lines, 0, lines.len().min(after + 1))
        .chars()
        .take(mode.max_chars())
        .collect()
}

fn format_snippet_window(lines: &[&str], center: usize, mode: SnippetMode) -> String {
    let (before, after) = mode.window();
    let start = center.saturating_sub(before);
    let end = (center + after + 1).min(lines.len());
    format_numbered_lines(lines, start, end)
        .chars()
        .take(mode.max_chars())
        .collect()
}

pub(crate) fn is_test_path(path: &str) -> bool {
    path.starts_with("test")
        || path.contains("/test")
        || path.ends_with("_test.py")
        || path.ends_with(".test.ts")
        || path.ends_with(".test.tsx")
}

pub(crate) fn is_entrypoint_path(path: &str) -> bool {
    matches!(
        path,
        "src/main.rs"
            | "src/lib.rs"
            | "main.py"
            | "app.py"
            | "server.py"
            | "index.js"
            | "index.ts"
            | "src/index.js"
            | "src/index.ts"
            | "cmd/main.go"
            | "main.go"
            | "Package.swift"
            | "Cargo.toml"
            | "package.json"
            | "pyproject.toml"
    ) || path.starts_with("cmd/")
}

pub(crate) fn is_manifest_file(path: &str) -> bool {
    let file_name = Path::new(path)
        .file_name()
        .and_then(|value| value.to_str())
        .unwrap_or(path);
    matches!(
        file_name,
        "Cargo.toml"
            | "pyproject.toml"
            | "package.json"
            | "package-lock.json"
            | "pnpm-lock.yaml"
            | "yarn.lock"
            | "go.mod"
            | "go.sum"
            | "Gemfile"
            | "Package.swift"
            | "pom.xml"
            | "build.gradle"
            | "settings.gradle"
            | "deno.json"
            | "composer.json"
    )
}

pub(crate) fn is_important_file(path: &str) -> bool {
    matches!(path, "AGENTS.md" | "CLAUDE.md" | "README.md" | "Makefile") || is_manifest_file(path)
}

pub(crate) fn symbol_kind_rank(kind: &str) -> usize {
    match kind {
        "class" | "struct" | "enum" | "interface" => 0,
        "function" => 1,
        _ => 2,
    }
}

pub(crate) fn round4(value: f64) -> f64 {
    (value * 10_000.0).round() / 10_000.0
}

pub(crate) fn finalize_results(mut results: Vec<SearchResult>, limit: usize) -> Vec<SearchResult> {
    for result in &mut results {
        if let Some(signals) = result.explanation.take() {
            result.explanation = Some(compact_rank_signals(signals));
        }
        if result.line_range.is_none() {
            result.line_range = line_range_from_snippet(&result.snippet);
        }
        compact_match_lines(&mut result.match_lines);
    }

    results.sort_by(|a, b| {
        b.score
            .partial_cmp(&a.score)
            .unwrap_or(Ordering::Equal)
            .then_with(|| a.path.cmp(&b.path))
    });

    let mut seen = HashMap::<String, usize>::new();
    let mut deduped = Vec::new();
    for result in results {
        let signature = result_signature(&result);
        if let Some(existing) = seen.get(&signature).copied() {
            record_duplicate(&mut deduped[existing], result.path);
        } else if deduped.len() < limit {
            seen.insert(signature, deduped.len());
            deduped.push(result);
        }
    }
    deduped
}

pub(crate) fn match_lines_from_text(
    text: &str,
    query_tokens: &[String],
    query_phrases: &[String],
    limit: usize,
) -> Vec<usize> {
    if (query_tokens.is_empty() && query_phrases.is_empty()) || limit == 0 {
        return Vec::new();
    }
    let mut lines = Vec::new();
    for (index, line) in text.lines().enumerate() {
        let line_lower = line.to_lowercase();
        let phrase_line = normalize_phrase_text(line);
        if query_tokens.iter().any(|token| line_lower.contains(token))
            || query_phrases
                .iter()
                .any(|phrase| phrase_line.contains(phrase))
        {
            lines.push(index + 1);
            if lines.len() >= limit {
                break;
            }
        }
    }
    lines
}

fn compact_match_lines(lines: &mut Vec<usize>) {
    lines.sort_unstable();
    lines.dedup();
    lines.truncate(16);
}

fn record_duplicate(result: &mut SearchResult, path: String) {
    let canonical_path = normalized_result_path(&result.path);
    let group = result
        .duplicate_group
        .get_or_insert_with(|| DuplicateGroup {
            canonical_path,
            duplicate_count: 0,
            duplicate_paths: Vec::new(),
        });
    group.duplicate_count += 1;
    if group.duplicate_paths.len() < 8 && !group.duplicate_paths.contains(&path) {
        group.duplicate_paths.push(path);
    }
}

fn compact_rank_signals(signals: Vec<RankSignal>) -> Vec<RankSignal> {
    let mut grouped = HashMap::<(String, String), f64>::new();
    for signal in signals {
        *grouped.entry((signal.kind, signal.value)).or_default() += signal.score;
    }
    let mut signals = grouped
        .into_iter()
        .map(|((kind, value), score)| RankSignal {
            kind,
            value,
            score: round4(score),
        })
        .collect::<Vec<_>>();
    signals.sort_by(|left, right| {
        right
            .score
            .partial_cmp(&left.score)
            .unwrap_or(Ordering::Equal)
            .then_with(|| left.kind.cmp(&right.kind))
            .then_with(|| left.value.cmp(&right.value))
    });
    signals.truncate(16);
    signals
}

fn line_range_from_snippet(snippet: &str) -> Option<ResultLineRange> {
    let mut start_line = usize::MAX;
    let mut end_line = 0usize;
    for line in snippet.lines() {
        let Some(number) = line
            .split_once(':')
            .and_then(|(prefix, _)| prefix.trim().parse::<usize>().ok())
        else {
            continue;
        };
        start_line = start_line.min(number);
        end_line = end_line.max(number);
    }
    (end_line > 0).then_some(ResultLineRange {
        start_line,
        end_line,
    })
}

pub(crate) fn matches_filters(path: &str, filters: &SearchFilters) -> bool {
    let path_lower = path.to_ascii_lowercase();
    if let Some(file_filter) = &filters.file {
        let Some(file_name) = Path::new(path)
            .file_name()
            .map(|value| value.to_string_lossy().to_ascii_lowercase())
        else {
            return false;
        };
        if !file_name.contains(&file_filter.to_ascii_lowercase()) {
            return false;
        }
    }
    if let Some(path_filter) = &filters.path {
        if !path_lower.contains(&path_filter.to_ascii_lowercase()) {
            return false;
        }
    }
    if let Some(language_filter) = &filters.language {
        let Some(language) = language_for(Path::new(path)) else {
            return false;
        };
        if language != language_filter.trim().to_lowercase() {
            return false;
        }
    }
    if let Some(extension_filter) = &filters.extension {
        let wanted = extension_filter
            .trim()
            .trim_start_matches('.')
            .to_lowercase();
        let Some(extension) = Path::new(path)
            .extension()
            .map(|value| value.to_string_lossy().to_lowercase())
        else {
            return false;
        };
        if extension != wanted {
            return false;
        }
    }
    if let Some(test) = filters.test {
        if is_test_path(&path_lower) != test {
            return false;
        }
    }
    let file_name = Path::new(path)
        .file_name()
        .map(|value| value.to_string_lossy().to_ascii_lowercase())
        .unwrap_or_default();
    if filters
        .exclude_file
        .iter()
        .any(|filter| file_name.contains(&filter.to_ascii_lowercase()))
    {
        return false;
    }
    if filters
        .exclude_path
        .iter()
        .any(|filter| path_lower.contains(&filter.to_ascii_lowercase()))
    {
        return false;
    }
    if let Some(language) = language_for(Path::new(path)) {
        if filters
            .exclude_language
            .iter()
            .any(|filter| &language == filter)
        {
            return false;
        }
    }
    if let Some(extension) = Path::new(path)
        .extension()
        .map(|value| value.to_string_lossy().to_lowercase())
    {
        if filters
            .exclude_extension
            .iter()
            .any(|filter| &extension == filter)
        {
            return false;
        }
    }
    true
}

pub(crate) fn filter_only_query(filters: &SearchFilters) -> bool {
    filters.file.is_some()
        || filters.path.is_some()
        || filters.language.is_some()
        || filters.extension.is_some()
        || filters.repo.is_some()
        || filters.test.is_some()
}

pub(crate) fn score_filter_only_path(
    path: &str,
    filters: &SearchFilters,
    explain: bool,
) -> Option<FilterOnlyMatch> {
    if !filter_only_query(filters) || !matches_filters(path, filters) {
        return None;
    }

    let mut score = 0.0;
    let mut reasons = Vec::new();
    let mut signals = Vec::new();

    if let Some(file) = &filters.file {
        add_filter_signal(
            "file_filter",
            file,
            14.0,
            explain,
            &mut score,
            &mut reasons,
            &mut signals,
        );
    }
    if let Some(path_filter) = &filters.path {
        add_filter_signal(
            "path_filter",
            path_filter,
            10.0,
            explain,
            &mut score,
            &mut reasons,
            &mut signals,
        );
    }
    if let Some(language) = &filters.language {
        add_filter_signal(
            "language_filter",
            language,
            6.0,
            explain,
            &mut score,
            &mut reasons,
            &mut signals,
        );
    }
    if let Some(extension) = &filters.extension {
        add_filter_signal(
            "extension_filter",
            extension,
            6.0,
            explain,
            &mut score,
            &mut reasons,
            &mut signals,
        );
    }
    if let Some(test) = filters.test {
        add_filter_signal(
            "test_filter",
            if test { "true" } else { "false" },
            5.0,
            explain,
            &mut score,
            &mut reasons,
            &mut signals,
        );
    }
    if let Some(repo) = &filters.repo {
        add_filter_signal(
            "repo_filter",
            repo,
            2.0,
            explain,
            &mut score,
            &mut reasons,
            &mut signals,
        );
    }
    if is_important_file(path) {
        score += 1.5;
        reasons.push("important_file".to_string());
        if explain {
            signals.push(rank_signal("important_file", path, 1.5));
        }
    }
    if is_entrypoint_path(path) {
        score += 1.0;
        reasons.push("entrypoint".to_string());
        if explain {
            signals.push(rank_signal("entrypoint", path, 1.0));
        }
    }

    Some(FilterOnlyMatch {
        score: round4(score),
        reasons,
        signals,
    })
}

pub(crate) fn filter_only_search_result(
    path: &str,
    text: &str,
    matched: FilterOnlyMatch,
    snippet_mode: SnippetMode,
    explain: bool,
) -> SearchResult {
    SearchResult {
        path: path.to_string(),
        score: matched.score,
        reason: format!("filter match {}", matched.reasons.join(", ")),
        snippet: best_snippet_for_path(path, text, &[], snippet_mode),
        line_range: None,
        match_lines: Vec::new(),
        explanation: explain.then_some(matched.signals),
        query_plan: None,
        duplicate_group: None,
        context: None,
    }
}

fn add_filter_signal(
    kind: &str,
    value: &str,
    amount: f64,
    explain: bool,
    score: &mut f64,
    reasons: &mut Vec<String>,
    signals: &mut Vec<RankSignal>,
) {
    *score += amount;
    reasons.push(format!("{kind}:{value}"));
    if explain {
        signals.push(rank_signal(kind, value, amount));
    }
}

pub(crate) fn repo_matches(root: &Path, filters: &SearchFilters) -> bool {
    let repo = root
        .file_name()
        .map(|value| value.to_string_lossy().to_ascii_lowercase())
        .unwrap_or_else(|| root.display().to_string());
    if let Some(filter) = &filters.repo {
        if !repo.contains(&filter.to_ascii_lowercase()) {
            return false;
        }
    }
    !filters
        .exclude_repo
        .iter()
        .any(|filter| repo.contains(&filter.to_ascii_lowercase()))
}

pub(crate) fn result_matches_all_tokens(result: &SearchResult, query_tokens: &[String]) -> bool {
    let haystack = format!("{}\n{}\n{}", result.path, result.reason, result.snippet).to_lowercase();
    query_tokens.iter().all(|token| haystack.contains(token))
}

pub(crate) fn result_matches_symbol_filters(
    result: &SearchResult,
    filters: &SearchFilters,
) -> bool {
    if let Some(symbol) = &filters.symbol {
        if !reason_contains_symbol(&result.reason, symbol) {
            return false;
        }
    }
    !filters
        .exclude_symbol
        .iter()
        .any(|symbol| reason_contains_symbol(&result.reason, symbol))
}

fn reason_contains_symbol(reason: &str, wanted: &str) -> bool {
    let wanted = normalize_token(wanted);
    if wanted.is_empty() {
        return false;
    }
    reason
        .trim_start_matches("matched ")
        .split(", ")
        .filter_map(|part| part.strip_prefix("symbol:"))
        .any(|symbol| normalize_token(symbol) == wanted)
}

fn format_numbered_lines(lines: &[&str], start: usize, end: usize) -> String {
    lines[start..end]
        .iter()
        .enumerate()
        .map(|(offset, line)| format!("{}: {}", start + offset + 1, line))
        .collect::<Vec<_>>()
        .join("\n")
}

fn result_signature(result: &SearchResult) -> String {
    let comparable_path = normalized_result_path(&result.path);
    let snippet = normalized_snippet_signature(&result.snippet);
    format!("{comparable_path}\n{snippet}")
}

fn normalized_result_path(path: &str) -> String {
    let path = path.trim_start_matches("./").trim_start_matches('/');
    if let Some(manifest) = Path::new(path).file_name().and_then(|value| value.to_str()) {
        if matches!(
            manifest,
            "Cargo.toml"
                | "package.json"
                | "pyproject.toml"
                | "go.mod"
                | "Package.swift"
                | "Makefile"
        ) {
            return manifest.to_string();
        }
    }

    ["/src/", "/tests/", "/test/", "/pkg/", "/cmd/", "/internal/"]
        .iter()
        .find_map(|marker| path.find(marker).map(|index| path[index + 1..].to_string()))
        .unwrap_or_else(|| path.to_string())
}

fn normalized_snippet_signature(snippet: &str) -> String {
    snippet
        .lines()
        .map(|line| {
            line.trim()
                .trim_start_matches(|ch: char| {
                    ch.is_ascii_digit() || ch == ':' || ch.is_whitespace()
                })
                .split_whitespace()
                .collect::<Vec<_>>()
                .join(" ")
        })
        .filter(|line| !line.is_empty())
        .collect::<Vec<_>>()
        .join("\n")
        .chars()
        .take(320)
        .collect()
}
