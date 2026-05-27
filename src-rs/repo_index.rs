//! Repo orientation index.

use anyhow::Result;
use ignore::WalkBuilder;
use regex::Regex;
use serde::{Deserialize, Serialize};
use std::cmp::Ordering;
use std::collections::{HashMap, HashSet};
use std::fs;
use std::path::{Path, PathBuf};

const MAX_FILE_BYTES: u64 = 512_000;

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
            let rel = path.strip_prefix(&root)?.to_string_lossy().replace('\\', "/");
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

        results.sort_by(|a, b| {
            b.score
                .partial_cmp(&a.score)
                .unwrap_or(Ordering::Equal)
                .then_with(|| a.path.cmp(&b.path))
        });
        results.truncate(limit);
        results
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
            if is_test_path(&lower) && !stem.is_empty() && lower.contains(&stem.replace("test_", "").to_lowercase()) {
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

fn is_ignored(path: &Path) -> bool {
    path.components().any(|component| {
        let part = component.as_os_str().to_string_lossy();
        matches!(
            part.as_ref(),
            ".git"
                | ".venv"
                | "__pycache__"
                | ".pytest_cache"
                | "node_modules"
                | "dist"
                | "build"
                | ".next"
                | "coverage"
                | "target"
        )
    })
}

fn language_for(path: &Path) -> Option<String> {
    let file_name = path.file_name()?.to_string_lossy();
    if matches!(file_name.as_ref(), "README" | "README.md" | "AGENTS.md" | "CLAUDE.md" | "Makefile") {
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

fn extract_symbols(path: &str, text: &str, language: &str) -> Vec<Symbol> {
    if language == "python" {
        return extract_python_symbols(path, text);
    }
    let re = Regex::new(r"\b(?:pub\s+)?(?:async\s+)?(?:fn|function|class|interface|struct|enum|const|let|var)\s+([A-Za-z_$][A-Za-z0-9_$]*)").unwrap();
    text.lines()
        .enumerate()
        .filter_map(|(index, line)| {
            let capture = re.captures(line)?;
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
    let re = Regex::new(r"^\s*(class|def|async\s+def)\s+([A-Za-z_][A-Za-z0-9_]*)").unwrap();
    text.lines()
        .enumerate()
        .filter_map(|(index, line)| {
            let capture = re.captures(line)?;
            let raw_kind = capture.get(1)?.as_str();
            Some(Symbol {
                name: capture.get(2)?.as_str().to_string(),
                kind: if raw_kind == "class" { "class" } else { "function" }.to_string(),
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

fn token_counts(text: &str) -> HashMap<String, usize> {
    let mut counts = HashMap::new();
    for token in tokenize(text) {
        *counts.entry(token).or_insert(0) += 1;
    }
    counts
}

fn tokenize(text: &str) -> Vec<String> {
    let camel = Regex::new(r"([a-z0-9])([A-Z])").unwrap();
    let split = camel.replace_all(text, "$1 $2");
    let re = Regex::new(r"[A-Za-z][A-Za-z0-9_]*").unwrap();
    re.find_iter(&split)
        .map(|m| m.as_str().to_lowercase())
        .filter(|token| token.len() > 1)
        .collect()
}

fn normalize_token(text: &str) -> String {
    tokenize(text).join("")
}

fn best_snippet(text: &str, query_tokens: &[String]) -> String {
    let lines = text.lines().collect::<Vec<_>>();
    for (idx, line) in lines.iter().enumerate() {
        let lowered = line.to_lowercase();
        if query_tokens.iter().any(|token| lowered.contains(token)) {
            let start = idx.saturating_sub(1);
            let end = (idx + 3).min(lines.len());
            return lines[start..end].join("\n").chars().take(700).collect();
        }
    }
    lines.into_iter().take(6).collect::<Vec<_>>().join("\n")
}

fn is_test_path(path: &str) -> bool {
    path.starts_with("test")
        || path.contains("/test")
        || path.ends_with("_test.py")
        || path.ends_with(".test.ts")
        || path.ends_with(".test.tsx")
}

fn round4(value: f64) -> f64 {
    (value * 10_000.0).round() / 10_000.0
}
