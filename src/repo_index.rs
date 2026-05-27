//! Repo orientation index.

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
const RIPGREP_TIMEOUT: Duration = Duration::from_millis(250);
const RIPGREP_POLL_INTERVAL: Duration = Duration::from_millis(5);
static CAMEL_RE: LazyLock<Regex> = LazyLock::new(|| Regex::new(r"([a-z0-9])([A-Z])").unwrap());
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
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct SearchFilters {
    pub path: Option<String>,
    pub language: Option<String>,
    pub extension: Option<String>,
    pub require_all: bool,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RelatedFile {
    pub path: String,
    pub reason: String,
    pub score: f64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RepoBrief {
    pub root_name: String,
    pub file_count: usize,
    pub language_counts: HashMap<String, usize>,
    pub known_commands: Vec<String>,
    pub important_files: Vec<String>,
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
    let query_tokens = tokenize(query);
    if query_tokens.is_empty() || limit == 0 {
        return Ok(Vec::new());
    }

    if let Some(results) = search_repo_ripgrep(&root, &query_tokens, limit, filters, timeout)? {
        return Ok(results);
    }

    search_repo_streaming(&root, &query_tokens, limit, filters)
}

fn search_repo_ripgrep(
    root: &Path,
    query_tokens: &[String],
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
    command.arg(".");

    let Ok(mut child) = command.stdout(Stdio::piped()).stderr(Stdio::null()).spawn() else {
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
        merge_match_result(&mut scored, &path, text, line_number, query_tokens);
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
    Ok(Some(finalize_results(results, limit)))
}

fn search_repo_streaming(
    root: &Path,
    query_tokens: &[String],
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
        if !path.is_file() {
            continue;
        }
        let metadata = entry.metadata()?;
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
        if let Some(result) = score_text_file(&rel, &text, &query_tokens) {
            results.push(result);
        }
    }

    if filters.require_all {
        results.retain(|result| result_matches_all_tokens(result, query_tokens));
    }
    Ok(finalize_results(results, limit))
}

fn merge_match_result(
    scored: &mut HashMap<String, SearchResult>,
    path: &str,
    line: &str,
    line_number: u64,
    query_tokens: &[String],
) {
    let path_lower = path.to_lowercase();
    let line_lower = line.to_lowercase();
    let mut score = 0.0;
    let mut reasons = Vec::new();

    for token in query_tokens {
        let mut token_score = 0.0;
        if path_lower.contains(token) {
            token_score += 6.0;
        }
        if line_lower.contains(token) {
            token_score += 2.0;
        }
        if token_score > 0.0 {
            score += token_score;
            reasons.push(token.clone());
        }
    }

    if score == 0.0 {
        return;
    }

    let snippet_line = line.trim_end();
    let snippet = if line_number > 0 {
        format!("{line_number}: {snippet_line}")
    } else {
        snippet_line.to_string()
    };

    scored
        .entry(path.to_string())
        .and_modify(|result| {
            result.score = round4(result.score + score);
            if result.snippet.len() < 700 && !result.snippet.contains(snippet_line) {
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
        let mut related = Vec::new();
        for file_path in self.files.keys() {
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

    pub fn repo_brief(&self) -> RepoBrief {
        let mut language_counts = HashMap::new();
        for file in self.files.values() {
            *language_counts.entry(file.language.clone()).or_insert(0) += 1;
        }
        let important_files = [
            "AGENTS.md",
            "CLAUDE.md",
            "README.md",
            "pyproject.toml",
            "package.json",
            "Cargo.toml",
            "Makefile",
        ]
        .into_iter()
        .filter(|name| self.files.contains_key(*name))
        .map(String::from)
        .collect();

        RepoBrief {
            root_name: self
                .root
                .file_name()
                .map(|value| value.to_string_lossy().to_string())
                .unwrap_or_else(|| self.root.display().to_string()),
            file_count: self.files.len(),
            language_counts,
            known_commands: self.known_commands(),
            important_files,
        }
    }

    fn known_commands(&self) -> Vec<String> {
        let mut commands = Vec::new();
        if self.files.contains_key("pyproject.toml") {
            commands.push("pytest".to_string());
        }
        if self.files.contains_key("Cargo.toml") {
            commands.push("cargo test".to_string());
        }
        if self.files.contains_key("package.json") {
            commands.push("npm test".to_string());
            commands.push("npm run lint".to_string());
        }
        if self.files.contains_key("Makefile") {
            commands.push("make test".to_string());
        }
        commands
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

pub(crate) fn language_for(path: &Path) -> Option<String> {
    let file_name = path.file_name()?.to_string_lossy();
    if matches!(
        file_name.as_ref(),
        "README" | "README.md" | "AGENTS.md" | "CLAUDE.md" | "Makefile"
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

fn score_text_file(path: &str, text: &str, query_tokens: &[String]) -> Option<SearchResult> {
    let path_lower = path.to_lowercase();
    let text_lower = text.to_lowercase();
    let mut score = 0.0;
    let mut reasons = Vec::new();

    for token in query_tokens {
        let mut token_score = 0.0;
        if path_lower.contains(token) {
            token_score += 6.0;
        }
        let occurrences = text_lower.matches(token).take(12).count();
        if occurrences > 0 {
            token_score += 1.0 + (occurrences as f64).ln();
        }
        if token_score > 0.0 {
            score += token_score;
            reasons.push(token.clone());
        }
    }

    if score == 0.0 {
        return None;
    }

    Some(SearchResult {
        path: path.to_string(),
        score: round4(score),
        reason: format!("matched {}", reasons.join(", ")),
        snippet: best_snippet(text, query_tokens),
    })
}

pub(crate) fn tokenize(text: &str) -> Vec<String> {
    let split = CAMEL_RE.replace_all(text, "$1 $2").replace('_', " ");
    TOKEN_RE
        .find_iter(&split)
        .map(|m| m.as_str().to_lowercase())
        .filter(|token| token.len() > 1)
        .collect()
}

pub(crate) fn normalize_token(text: &str) -> String {
    tokenize(text).join("")
}

pub(crate) fn best_snippet(text: &str, query_tokens: &[String]) -> String {
    let lines = text.lines().collect::<Vec<_>>();
    for (idx, line) in lines.iter().enumerate() {
        let lowered = line.to_lowercase();
        if query_tokens.iter().any(|token| lowered.contains(token)) {
            let start = idx.saturating_sub(1);
            let end = (idx + 3).min(lines.len());
            return format_numbered_lines(&lines, start, end)
                .chars()
                .take(700)
                .collect();
        }
    }
    format_numbered_lines(&lines, 0, lines.len().min(6))
}

fn is_test_path(path: &str) -> bool {
    path.starts_with("test")
        || path.contains("/test")
        || path.ends_with("_test.py")
        || path.ends_with(".test.ts")
        || path.ends_with(".test.tsx")
}

pub(crate) fn round4(value: f64) -> f64 {
    (value * 10_000.0).round() / 10_000.0
}

pub(crate) fn finalize_results(mut results: Vec<SearchResult>, limit: usize) -> Vec<SearchResult> {
    results.sort_by(|a, b| {
        b.score
            .partial_cmp(&a.score)
            .unwrap_or(Ordering::Equal)
            .then_with(|| a.path.cmp(&b.path))
    });

    let mut seen = HashSet::new();
    let mut deduped = Vec::new();
    for result in results {
        if seen.insert(result_signature(&result)) {
            deduped.push(result);
        }
        if deduped.len() >= limit {
            break;
        }
    }
    deduped
}

pub(crate) fn matches_filters(path: &str, filters: &SearchFilters) -> bool {
    if let Some(path_filter) = &filters.path {
        if !path.contains(path_filter) {
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
    true
}

pub(crate) fn result_matches_all_tokens(result: &SearchResult, query_tokens: &[String]) -> bool {
    let haystack = format!("{}\n{}\n{}", result.path, result.reason, result.snippet).to_lowercase();
    query_tokens.iter().all(|token| haystack.contains(token))
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
    let comparable_path = ["/src/", "/tests/", "/test/", "/pkg/", "/cmd/", "/internal/"]
        .iter()
        .find_map(|marker| {
            result
                .path
                .find(marker)
                .map(|index| result.path[index + 1..].to_string())
        })
        .unwrap_or_else(|| result.path.clone());
    let snippet = result
        .snippet
        .lines()
        .next()
        .unwrap_or_default()
        .trim()
        .trim_start_matches(|ch: char| ch.is_ascii_digit() || ch == ':' || ch.is_whitespace())
        .chars()
        .take(160)
        .collect::<String>();
    format!("{comparable_path}\n{snippet}")
}
