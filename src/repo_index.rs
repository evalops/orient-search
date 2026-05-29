//! Repo orientation index.

use crate::discover::git_metadata_for_repo;
use crate::query::{merge_filters, normalize_phrase_text, parse_query, query_phrases, query_text};
use ahash::{AHashMap as HashMap, AHashSet as HashSet};
use anyhow::Result;
use ignore::WalkBuilder;
use regex::Regex;
use serde::{Deserialize, Serialize};
use std::cmp::Ordering;
use std::fs;
use std::io::{BufRead, BufReader};
use std::path::{Component, Path, PathBuf};
use std::process::{Command, Stdio};
use std::sync::{LazyLock, mpsc};
use std::thread;
use std::time::{Duration, Instant};

const MAX_FILE_BYTES: u64 = 512_000;
pub const MAX_ATTACHED_CONTEXT_LINES: usize = 500;
pub const MAX_READ_RANGE_LINES: usize = 1_000;
pub const MAX_SEARCH_RESULTS: usize = 100;
pub const MAX_RESULT_READ_BATCH_RANGES: usize = 64;
pub const DEFAULT_REPO_MAP_READ_BATCH_RANGES: usize = 16;
const MAX_REPO_BRIEF_IMPORT_HINTS: usize = 32;
const DEFAULT_RESULT_READ_LINES: usize = 80;
const DEFAULT_RELATED_FILE_READ_LINES: usize = 80;
pub(crate) const DEFAULT_SYMBOL_READ_CONTEXT_BEFORE: usize = 20;
const DEFAULT_SYMBOL_READ_LINES: usize = 80;
const RIPGREP_TIMEOUT: Duration = Duration::from_millis(250);
const RIPGREP_POLL_INTERVAL: Duration = Duration::from_millis(5);
const SYMBOL_REFERENCE_LOWERCASE_THRESHOLD: usize = 16;
pub(crate) const GENERATED_PATH_SCORE_MULTIPLIER: f64 = 0.2;
const GENERATED_DIRECTORY_SEGMENTS: &[&str] = &[
    "generated",
    "__generated__",
    "gen",
    "gensrc",
    "codegen",
    "autogen",
    "auto-generated",
];
const GENERATED_FILE_STEM_PREFIXES: &[&str] = &["generated_", "generated-"];
const GENERATED_FILE_STEM_SUFFIXES: &[&str] = &[
    "_generated",
    "-generated",
    ".generated",
    ".gen",
    "_gen",
    "-gen",
];
const GENERATED_FILE_NAME_SUFFIXES: &[&str] =
    &[".pb.go", ".pb.rs", ".g.dart", ".min.js", ".bundle.js"];
const GENERATED_FILE_GLOBS: &[&str] = &[
    "generated.*",
    "generated_*",
    "generated-*",
    "*_generated.*",
    "*-generated.*",
    "*.generated.*",
    "*.gen.*",
    "*_gen.*",
    "*-gen.*",
    "*.pb.go",
    "*.pb.rs",
    "*.g.dart",
    "*.min.js",
    "*.bundle.js",
    "chunk-*.js",
    "index-*.js",
    "main-*.js",
    "runtime-*.js",
    "vendor-*.js",
    "preload-helper-*.js",
];
const GENERATED_BUNDLE_DIR_SEGMENTS: &[&str] = &["assets", "static"];
const CODE_FILE_GLOBS: &[&str] = &[
    "**/*.py",
    "**/*.rs",
    "**/*.js",
    "**/*.jsx",
    "**/*.ts",
    "**/*.tsx",
    "**/*.go",
    "**/*.rb",
    "**/*.java",
    "**/*.kt",
    "**/*.swift",
];
const PROSE_FILE_GLOBS: &[&str] = &["**/*.md", "**/*.toml", "**/*.json", "**/*.yaml", "**/*.yml"];
const PROSE_FILE_NAME_GLOBS: &[&str] = &[
    "**/README",
    "**/Makefile",
    "**/yarn.lock",
    "**/bun.lock",
    "**/bun.lockb",
];
const FD_CODE_FILE_PATTERN: &str = r"\.(py|rs|js|jsx|ts|tsx|go|rb|java|kt|swift)$";
const FD_PROSE_FILE_PATTERN: &str =
    r"^(README|Makefile|yarn\.lock|bun\.lock|bun\.lockb)$|\.(md|toml|json|ya?ml)$";
static TOKEN_RE: LazyLock<Regex> = LazyLock::new(|| Regex::new(r"[A-Za-z][A-Za-z0-9_]*").unwrap());
static SYMBOL_RE: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(r"\b(?:(?:pub(?:\([^)]*\))?|export|default|declare|async)\s+)*(fn|function|class|interface|struct|enum|trait|type|const|let|var)\s+([A-Za-z_$][A-Za-z0-9_$]*)").unwrap()
});
static PYTHON_SYMBOL_RE: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(r"^\s*(class|def|async\s+def)\s+([A-Za-z_][A-Za-z0-9_]*)").unwrap()
});
static GO_FUNC_SYMBOL_RE: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(r"^\s*func\s+(?:\([^)]*\)\s*)?([A-Za-z_][A-Za-z0-9_]*)\s*\(").unwrap()
});
static GO_TYPE_SYMBOL_RE: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(r"^\s*type\s+([A-Za-z_][A-Za-z0-9_]*)\s+(struct|interface)\b").unwrap()
});
static RUBY_SYMBOL_RE: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"^\s*(class|module|def)\s+([A-Za-z_][A-Za-z0-9_!?=]*)").unwrap());
static KOTLIN_FUNC_SYMBOL_RE: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(r"\bfun\s+(?:[A-Za-z_][A-Za-z0-9_<>?.]*\.)?([A-Za-z_][A-Za-z0-9_]*)\s*\(").unwrap()
});
static KOTLIN_TYPE_SYMBOL_RE: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(r"\b(?:(data|sealed|enum)\s+)?(class|interface|object)\s+([A-Za-z_][A-Za-z0-9_]*)")
        .unwrap()
});
static SWIFT_FUNC_SYMBOL_RE: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"\bfunc\s+([A-Za-z_][A-Za-z0-9_]*)\s*\(").unwrap());
static SWIFT_TYPE_SYMBOL_RE: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(r"\b(class|struct|enum|protocol)\s+([A-Za-z_][A-Za-z0-9_]*)").unwrap()
});
static JAVA_METHOD_SYMBOL_RE: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(r"^\s*(?:public|private|protected|static|final|abstract|synchronized|native|default|\s)+[A-Za-z_][A-Za-z0-9_<>\[\], ?]*\s+([A-Za-z_][A-Za-z0-9_]*)\s*\(").unwrap()
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
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub read_range: Option<ResultReadRange>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub read_request: Option<ResultToolRequest>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub related_request: Option<ResultToolRequest>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub related_symbols_request: Option<ResultToolRequest>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ResultReadRange {
    pub path: String,
    pub start: usize,
    pub lines: usize,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub scope: Option<RangeScope>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ResultToolRequest {
    pub id: String,
    pub tool: String,
    pub arguments: serde_json::Value,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cli: Option<String>,
    pub jsonl: String,
    pub client_cli: String,
}

pub type ResultReadRequest = ResultToolRequest;

impl ResultToolRequest {
    pub fn new(tool: impl Into<String>, arguments: serde_json::Value) -> Self {
        let tool = tool.into();
        Self::with_id(default_tool_request_id(&tool), tool, arguments)
    }

    pub fn with_id(
        id: impl Into<String>,
        tool: impl Into<String>,
        arguments: serde_json::Value,
    ) -> Self {
        let id = id.into();
        let tool = tool.into();
        let cli = cli_command_for_request(&tool, &arguments);
        let jsonl = serde_json::json!({
            "id": id.clone(),
            "tool": tool.clone(),
            "arguments": arguments.clone()
        })
        .to_string();
        let client_cli = format!(
            "printf '%s\\n' {} | orient client-jsonl",
            shell_quote(&jsonl)
        );
        Self {
            id,
            tool,
            arguments,
            cli,
            jsonl,
            client_cli,
        }
    }
}

fn default_tool_request_id(tool: &str) -> &'static str {
    match tool {
        "repo_map" | "indexed_repo_map" | "shard_repo_map" => "map",
        "search_query_plan" | "search_plan" | "indexed_query_plan" | "index_plan"
        | "shard_query_plan" | "shard_plan" => "plan",
        "search" | "search_code" | "indexed_search" | "indexed_search_code" | "search_shards" => {
            "search"
        }
        "related_files"
        | "related_index_files"
        | "related_shard_files"
        | "related_symbols"
        | "related_index_symbols"
        | "related_shard_symbols" => "related",
        "find_symbol"
        | "find_symbol_batch"
        | "find_index_symbol"
        | "find_index_symbol_batch"
        | "find_shard_symbol"
        | "find_shard_symbol_batch" => "symbol",
        "read_range" | "open_range" | "read_ranges" | "open_ranges" | "read_index_range"
        | "open_index_range" | "read_index_ranges" | "open_index_ranges" | "read_shard_range"
        | "open_shard_range" | "read_shard_ranges" | "open_shard_ranges" => "read",
        _ => "request",
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RelatedFileLookupResult {
    pub path: String,
    pub reason: String,
    pub score: f64,
    pub read_range: ResultReadRange,
    pub read_request: ResultToolRequest,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SymbolLookupResult {
    #[serde(flatten)]
    pub symbol: Symbol,
    pub read_range: ResultReadRange,
    pub read_request: ResultToolRequest,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RelatedSymbolLookupResult {
    pub symbol: Symbol,
    pub reason: String,
    pub score: f64,
    pub read_range: ResultReadRange,
    pub read_request: ResultToolRequest,
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

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct QueryPlan {
    pub strategy: String,
    pub require_all: bool,
    pub query_tokens: Vec<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub query_phrases: Vec<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub query_trigrams: Vec<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub active_filters: Vec<QueryPlanFilter>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
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
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub diagnosis: Option<QueryPlanDiagnosis>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub repair_hints: Vec<QueryPlanRepairHint>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub retry_requests: Vec<ResultToolRequest>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct QueryPlanDiagnosis {
    pub status: String,
    pub summary: String,
    pub next_action: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub primary_hint_kind: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub suggested_query: Option<String>,
}

impl QueryPlan {
    pub fn with_diagnosis(mut self) -> Self {
        self.diagnosis = Some(QueryPlanDiagnosis::from_plan(&self));
        self
    }
}

impl QueryPlanDiagnosis {
    fn from_plan(plan: &QueryPlan) -> Self {
        let primary_hint = plan.repair_hints.first();
        let suggested_query = plan
            .repair_hints
            .iter()
            .find_map(|hint| hint.suggested_query.clone());
        let primary_hint_kind = primary_hint.map(|hint| hint.kind.clone());
        let next_action = if let Some(query) = suggested_query.as_ref() {
            format!("Run the first retry request or retry with query `{query}`.")
        } else if plan.final_match_count > 0 && plan.candidate_cap_hit {
            "Narrow with a rarer term or a facet hint before trusting the top results.".to_string()
        } else if plan.final_match_count > 0 {
            "Use the top results and their read_request/read_batch_request for bounded context."
                .to_string()
        } else {
            "Inspect repair_hints and planned_postings, then relax the narrowest missing term or filter."
                .to_string()
        };

        let primary_hint_kind_ref = primary_hint.map(|hint| hint.kind.as_str());
        let (status, summary) = if plan.final_match_count > 0 && plan.candidate_cap_hit {
            (
                "candidate_cap_hit",
                format!(
                    "Found {} final matches after scoring a capped candidate set of {} from {} candidates.",
                    plan.final_match_count, plan.candidate_cap, plan.candidate_count
                ),
            )
        } else if plan.final_match_count > 0 {
            (
                "matched",
                format!(
                    "Found {} final matches from {} candidates.",
                    plan.final_match_count, plan.candidate_count
                ),
            )
        } else if plan.strategy == "empty_query" {
            (
                "empty_query",
                "No positive query term or searchable positive filter was provided.".to_string(),
            )
        } else if plan.strategy.ends_with("_mismatch") {
            (
                "scope_mismatch",
                "The selected repo, shard, branch, origin, or dependency scope rejected the query."
                    .to_string(),
            )
        } else if primary_hint_kind_ref.is_some_and(|kind| {
            kind.starts_with("replace_")
                || kind.starts_with("relax_")
                || kind == "dependency_filter_mismatch"
        }) {
            (
                "filters_rejected",
                primary_hint
                    .map(|hint| hint.message.clone())
                    .unwrap_or_else(|| "Active filters rejected the query.".to_string()),
            )
        } else if !plan.missing_terms.is_empty() {
            (
                "missing_terms",
                format!(
                    "Required terms have no content or path postings: {}.",
                    plan.missing_terms.join(", ")
                ),
            )
        } else if !plan.missing_trigrams.is_empty() {
            (
                "missing_trigrams",
                "The literal substring trigrams are absent from the index.".to_string(),
            )
        } else if plan.candidate_count == 0 {
            (
                "no_candidates",
                "Each required posting/filter scope produced no shared candidate files."
                    .to_string(),
            )
        } else if plan.filtered_candidate_count == 0 {
            (
                "filters_rejected",
                format!(
                    "Postings found {} candidates, but active filters rejected all of them.",
                    plan.candidate_count
                ),
            )
        } else if plan.scored_candidate_count == 0 && !plan.query_phrases.is_empty() {
            (
                "phrase_rejected",
                format!(
                    "{} filtered candidates survived, but quoted phrase verification rejected them.",
                    plan.filtered_candidate_count
                ),
            )
        } else if plan.scored_candidate_count > 0 && plan.require_all {
            (
                "and_rejected",
                format!(
                    "{} candidates scored, but final AND or symbol checks rejected them.",
                    plan.scored_candidate_count
                ),
            )
        } else {
            (
                "no_final_matches",
                "The plan produced no final matches after candidate selection and scoring."
                    .to_string(),
            )
        };

        Self {
            status: status.to_string(),
            summary,
            next_action,
            primary_hint_kind,
            suggested_query,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct QueryPlanPosting {
    pub kind: String,
    pub value: String,
    pub postings: usize,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct QueryPlanFilter {
    pub field: String,
    pub value: String,
    pub negated: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub candidate_matches: Option<usize>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub candidate_rejections: Option<usize>,
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
    pub symbol_kind: Option<String>,
    pub repo: Option<String>,
    pub branch: Option<String>,
    pub origin: Option<String>,
    pub dependency: Option<String>,
    pub import: Option<String>,
    pub test: Option<bool>,
    pub generated: Option<bool>,
    pub code: Option<bool>,
    pub target_line: Option<usize>,
    pub require_all: bool,
    pub match_any: bool,
    pub snippet: SnippetMode,
    pub explain: bool,
    pub exclude_file: Vec<String>,
    pub exclude_path: Vec<String>,
    pub exclude_language: Vec<String>,
    pub exclude_extension: Vec<String>,
    pub exclude_symbol: Vec<String>,
    pub exclude_symbol_kind: Vec<String>,
    pub exclude_repo: Vec<String>,
    pub exclude_branch: Vec<String>,
    pub exclude_origin: Vec<String>,
    pub exclude_dependency: Vec<String>,
    pub exclude_import: Vec<String>,
    pub exclude_content: Vec<String>,
}

pub(crate) fn normalize_search_filters_for_root(filters: &mut SearchFilters, root: &Path) {
    if let Some(path) = filters
        .path
        .as_deref()
        .and_then(|path| root_relative_path_filter(root, path))
    {
        filters.path = Some(path);
    }
    if let Some(path) = filters
        .file
        .as_deref()
        .and_then(|file| root_relative_path_filter(root, file))
    {
        filters.path = Some(path);
        filters.file = None;
    }
    filters.exclude_path = filters
        .exclude_path
        .iter()
        .map(|path| root_relative_path_filter(root, path).unwrap_or_else(|| path.clone()))
        .collect();
    let mut exclude_path_from_file = Vec::new();
    filters.exclude_file.retain(|file| {
        if let Some(path) = root_relative_path_filter(root, file) {
            exclude_path_from_file.push(path);
            false
        } else {
            true
        }
    });
    filters.exclude_path.extend(exclude_path_from_file);
}

fn root_relative_path_filter(root: &Path, value: &str) -> Option<String> {
    let value = value.trim().replace('\\', "/");
    if value.is_empty()
        || value.contains('*')
        || value.contains('?')
        || value.contains('\0')
        || !Path::new(&value).is_absolute()
    {
        return None;
    }
    let requested = Path::new(&value);
    let root = root.canonicalize().unwrap_or_else(|_| root.to_path_buf());
    let requested = requested
        .canonicalize()
        .unwrap_or_else(|_| requested.to_path_buf());
    let rel = requested.strip_prefix(&root).ok()?;
    let mut parts = Vec::new();
    for component in rel.components() {
        match component {
            Component::Normal(part) => parts.push(part.to_string_lossy().to_string()),
            Component::CurDir => {}
            Component::ParentDir | Component::RootDir | Component::Prefix(_) => return None,
        }
    }
    let rel = parts.join("/");
    (!rel.is_empty()).then_some(rel)
}

#[derive(Debug, Clone)]
pub(crate) struct FilterOnlyMatch {
    pub score: f64,
    pub reasons: Vec<String>,
    pub signals: Vec<RankSignal>,
}

#[derive(Debug, Clone)]
pub(crate) struct PathFilterMatcher {
    file: Option<FilterPattern>,
    path: Option<FilterPattern>,
    language: Option<String>,
    extension: Option<String>,
    exclude_file: Vec<FilterPattern>,
    exclude_path: Vec<FilterPattern>,
    exclude_language: Vec<String>,
    exclude_extension: Vec<String>,
    test: Option<bool>,
    generated: Option<bool>,
    code: Option<bool>,
}

#[derive(Debug, Clone)]
struct FilterPattern {
    value: String,
    wildcard: bool,
}

impl FilterPattern {
    fn new(filter: &str) -> Self {
        let value = normalize_path_filter(filter);
        let wildcard = value.contains('*') || value.contains('?');
        Self { value, wildcard }
    }

    fn matches(&self, haystack_lower: &str) -> bool {
        if self.wildcard {
            wildcard_matches(&self.value, haystack_lower)
        } else {
            haystack_lower.contains(&self.value)
        }
    }
}

impl PathFilterMatcher {
    pub(crate) fn from_filters(filters: &SearchFilters) -> Self {
        Self {
            file: filters.file.as_deref().map(FilterPattern::new),
            path: filters.path.as_deref().map(FilterPattern::new),
            language: filters.language.as_deref().map(normalize_language_filter),
            extension: filters.extension.as_deref().map(normalize_extension_filter),
            exclude_file: filters
                .exclude_file
                .iter()
                .map(|filter| FilterPattern::new(filter))
                .collect(),
            exclude_path: filters
                .exclude_path
                .iter()
                .map(|filter| FilterPattern::new(filter))
                .collect(),
            exclude_language: filters
                .exclude_language
                .iter()
                .map(|filter| normalize_language_filter(filter))
                .collect(),
            exclude_extension: filters
                .exclude_extension
                .iter()
                .map(|filter| normalize_extension_filter(filter))
                .collect(),
            test: filters.test,
            generated: filters.generated,
            code: filters.code,
        }
    }

    fn matches(
        &self,
        path_lower: &str,
        file_name_lower: &str,
        extension_lower: Option<&str>,
        language: Option<&str>,
    ) -> bool {
        if self
            .file
            .as_ref()
            .is_some_and(|filter| !filter.matches(file_name_lower))
        {
            return false;
        }
        if self
            .path
            .as_ref()
            .is_some_and(|filter| !filter.matches(path_lower))
        {
            return false;
        }
        if let Some(language_filter) = &self.language {
            let Some(language) = language else {
                return false;
            };
            if language != language_filter {
                return false;
            }
        }
        if let Some(extension_filter) = &self.extension {
            let Some(extension) = extension_lower else {
                return false;
            };
            if extension != extension_filter {
                return false;
            }
        }
        if self
            .test
            .is_some_and(|test| is_test_path(path_lower) != test)
        {
            return false;
        }
        if self
            .generated
            .is_some_and(|generated| is_generated_path(path_lower) != generated)
        {
            return false;
        }
        if self
            .code
            .is_some_and(|code| language.map(is_source_code_language).unwrap_or(false) != code)
        {
            return false;
        }
        if self
            .exclude_file
            .iter()
            .any(|filter| filter.matches(file_name_lower))
        {
            return false;
        }
        if self
            .exclude_path
            .iter()
            .any(|filter| filter.matches(path_lower))
        {
            return false;
        }
        if let Some(language) = language {
            if self
                .exclude_language
                .iter()
                .any(|filter| language == filter)
            {
                return false;
            }
        }
        if let Some(extension) = extension_lower {
            if self
                .exclude_extension
                .iter()
                .any(|filter| extension == filter)
            {
                return false;
            }
        }
        true
    }
}

impl Default for SearchFilters {
    fn default() -> Self {
        Self {
            file: None,
            path: None,
            language: None,
            extension: None,
            symbol: None,
            symbol_kind: None,
            repo: None,
            branch: None,
            origin: None,
            dependency: None,
            import: None,
            test: None,
            generated: None,
            code: None,
            target_line: None,
            require_all: false,
            match_any: false,
            snippet: SnippetMode::Medium,
            explain: false,
            exclude_file: Vec::new(),
            exclude_path: Vec::new(),
            exclude_language: Vec::new(),
            exclude_extension: Vec::new(),
            exclude_symbol: Vec::new(),
            exclude_symbol_kind: Vec::new(),
            exclude_repo: Vec::new(),
            exclude_branch: Vec::new(),
            exclude_origin: Vec::new(),
            exclude_dependency: Vec::new(),
            exclude_import: Vec::new(),
            exclude_content: Vec::new(),
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
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub dependency_hints: Vec<DependencyHint>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub import_hints: Vec<ImportHint>,
    pub manifest_files: Vec<String>,
    pub important_files: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CommandHint {
    pub command: String,
    pub kind: String,
    pub source: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DependencyHint {
    pub name: String,
    pub kind: String,
    pub source: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ImportHint {
    pub module: String,
    pub kind: String,
    pub source: String,
    pub line: usize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RepoMapDetail {
    Compact,
    Full,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RepoMap {
    pub brief: RepoBrief,
    pub entrypoints: Vec<String>,
    pub test_files: Vec<String>,
    pub top_symbols: Vec<Symbol>,
    pub related_files: Vec<RepoMapRelatedFile>,
    pub related_symbols: Vec<RepoMapRelatedSymbol>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub read_batch_request: Option<ResultToolRequest>,
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
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub symbol: Option<Symbol>,
}

#[derive(Debug, Copy, Clone, Default, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum RangeScope {
    #[default]
    Exact,
    Symbol,
}

impl RangeScope {
    pub fn parse(value: &str) -> Option<Self> {
        match value.to_ascii_lowercase().as_str() {
            "exact" | "range" | "line" | "lines" => Some(Self::Exact),
            "symbol" | "definition" | "def" => Some(Self::Symbol),
            _ => None,
        }
    }
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
    let limit = capped_search_limit(limit);
    let root = root.as_ref().canonicalize()?;
    let deadline = Instant::now() + timeout;
    let parsed = parse_query(query);
    let query_phrases = query_phrases(&parsed.terms);
    let mut filters = merge_filters(filters.clone(), parsed.filters);
    normalize_search_filters_for_root(&mut filters, &root);
    if !repo_matches(&root, &filters) {
        return Ok(Vec::new());
    }
    if !repo_dependency_filters_match(&root, &filters)? {
        return Ok(Vec::new());
    }
    let query = query_text(&parsed.terms, &filters);
    let query_tokens = unique_query_tokens(&query);
    if limit == 0 {
        return Ok(Vec::new());
    }
    if query_tokens.is_empty() && query_phrases.is_empty() {
        return if filter_only_query(&filters) {
            search_repo_filter_only(&root, limit, &filters, timeout)
        } else {
            Ok(Vec::new())
        };
    }
    if query_tokens.len() > 1 && !filters.match_any {
        filters.require_all = true;
    }

    if let Some(results) = search_repo_ripgrep(
        &root,
        &parsed.terms,
        &query_tokens,
        &query_phrases,
        limit,
        &filters,
        timeout,
    )? {
        if !results.is_empty()
            || !strict_fallback_rescue_needed(&query_tokens, &query_phrases, &filters)
        {
            return Ok(results);
        }
        let rescued = search_repo_streaming_until(
            &root,
            &query_tokens,
            &query_phrases,
            limit,
            &filters,
            deadline,
        )?;
        if !rescued.is_empty() {
            return Ok(rescued);
        }
        return Ok(results);
    }

    search_repo_streaming_until(
        &root,
        &query_tokens,
        &query_phrases,
        limit,
        &filters,
        deadline,
    )
}

fn strict_fallback_rescue_needed(
    query_tokens: &[String],
    query_phrases: &[String],
    filters: &SearchFilters,
) -> bool {
    filters.require_all
        || (!filters.match_any && query_tokens.len() > 1)
        || !query_phrases.is_empty()
}

fn search_repo_filter_only(
    root: &Path,
    limit: usize,
    filters: &SearchFilters,
    timeout: Duration,
) -> Result<Vec<SearchResult>> {
    let candidate_cap = filter_only_candidate_cap(limit, filters);
    let deadline = Instant::now() + timeout;
    let mut candidates = if let Some(candidates) =
        filter_only_candidates_from_direct_location(root, filters, deadline)?
    {
        candidates
    } else {
        let mut candidates =
            filter_only_candidates_from_fd_files(root, filters, candidate_cap, deadline)?
                .unwrap_or_default();
        if candidates.is_empty() && Instant::now() < deadline {
            candidates =
                filter_only_candidates_from_rg_files(root, filters, candidate_cap, deadline)?
                    .unwrap_or_default();
        }
        if candidates.is_empty() && Instant::now() < deadline {
            candidates = filter_only_candidates_from_walk(root, filters, candidate_cap, deadline)?;
        }
        candidates
    };

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
        if Instant::now() >= deadline {
            break;
        }
        let text = fs::read_to_string(root.join(&path)).unwrap_or_default();
        if text.contains('\0') {
            continue;
        }
        if !source_content_filters_match(&path, &text, filters) {
            continue;
        }
        results.push(filter_only_search_result(&path, &text, matched, filters));
    }

    Ok(finalize_results_for_filters(results, limit, filters))
}

fn filter_only_candidates_from_direct_location(
    root: &Path,
    filters: &SearchFilters,
    deadline: Instant,
) -> Result<Option<Vec<(String, FilterOnlyMatch)>>> {
    let Some((path, authoritative)) = direct_location_filter_path(filters) else {
        return Ok(None);
    };
    if Instant::now() >= deadline {
        return Ok(Some(Vec::new()));
    }
    let Some(path) = normalize_direct_repo_relative_path(&path) else {
        return Ok(Some(Vec::new()));
    };
    let Ok(root) = root.canonicalize() else {
        return Ok(Some(Vec::new()));
    };
    let Ok(absolute) = root.join(&path).canonicalize() else {
        return if authoritative {
            Ok(Some(Vec::new()))
        } else {
            Ok(None)
        };
    };
    if !absolute.starts_with(&root) || !absolute.is_file() {
        return if authoritative {
            Ok(Some(Vec::new()))
        } else {
            Ok(None)
        };
    }
    let rel = absolute
        .strip_prefix(&root)?
        .to_string_lossy()
        .replace('\\', "/");
    let mut candidates = Vec::new();
    let _ = push_filter_only_candidate(&root, filters, &rel, &mut candidates)?;
    Ok(Some(candidates))
}

fn direct_location_filter_path(filters: &SearchFilters) -> Option<(String, bool)> {
    if let Some(path) = filters
        .path
        .as_deref()
        .filter(|path| exact_direct_path_filter(path))
    {
        return Some((path.to_string(), filters.target_line.is_some()));
    }
    filters.target_line?;
    filters
        .file
        .as_deref()
        .filter(|file| exact_direct_path_filter(file))
        .map(|file| (file.to_string(), false))
}

fn exact_direct_path_filter(path: &str) -> bool {
    let path = strip_leading_current_dir_segments(path.trim().replace('\\', "/"));
    !path.is_empty()
        && !path.contains('*')
        && !path.contains('?')
        && !path.contains('\0')
        && Path::new(&path).is_relative()
}

fn normalize_direct_repo_relative_path(path: &str) -> Option<String> {
    let normalized = strip_leading_current_dir_segments(path.trim().replace('\\', "/"));
    let requested = Path::new(&normalized);
    if !requested.is_relative()
        || requested.components().any(|component| {
            matches!(
                component,
                Component::ParentDir | Component::RootDir | Component::Prefix(_)
            )
        })
    {
        return None;
    }
    Some(normalized.trim_start_matches('/').to_string())
}

fn filter_only_candidates_from_fd_files(
    root: &Path,
    filters: &SearchFilters,
    candidate_cap: usize,
    deadline: Instant,
) -> Result<Option<Vec<(String, FilterOnlyMatch)>>> {
    let Some(pattern) = fd_positive_file_pattern(filters) else {
        return Ok(None);
    };
    let mut command = Command::new("fd");
    command
        .current_dir(root)
        .arg("-H")
        .arg("-I")
        .arg("-i")
        .arg("-t")
        .arg("f")
        .arg("--color")
        .arg("never")
        .arg("--exclude")
        .arg(".git")
        .arg("--exclude")
        .arg(".venv")
        .arg("--exclude")
        .arg("__pycache__")
        .arg("--exclude")
        .arg(".pytest_cache")
        .arg("--exclude")
        .arg(".orient")
        .arg("--exclude")
        .arg("node_modules")
        .arg("--exclude")
        .arg("dist")
        .arg("--exclude")
        .arg("build")
        .arg("--exclude")
        .arg(".next")
        .arg("--exclude")
        .arg("coverage")
        .arg("--exclude")
        .arg("target")
        .arg("--max-results")
        .arg(fd_max_results(filters, candidate_cap))
        .arg(pattern);
    add_generated_filter_fd_excludes(&mut command, filters);

    collect_filter_only_candidates_from_path_command(
        command,
        root,
        filters,
        candidate_cap,
        deadline,
    )
}

fn filter_only_candidates_from_rg_files(
    root: &Path,
    filters: &SearchFilters,
    candidate_cap: usize,
    deadline: Instant,
) -> Result<Option<Vec<(String, FilterOnlyMatch)>>> {
    let mut command = Command::new("rg");
    command
        .current_dir(root)
        .arg("--files")
        .arg("--hidden")
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
    add_scope_filter_ripgrep_globs(&mut command, filters);
    command.arg(".");

    collect_filter_only_candidates_from_path_command(
        command,
        root,
        filters,
        candidate_cap,
        deadline,
    )
}

fn collect_filter_only_candidates_from_path_command(
    mut command: Command,
    root: &Path,
    filters: &SearchFilters,
    candidate_cap: usize,
    deadline: Instant,
) -> Result<Option<Vec<(String, FilterOnlyMatch)>>> {
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

    let mut candidates = Vec::new();
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
                let _ = child.try_wait()?;
                continue;
            }
            Err(mpsc::RecvTimeoutError::Disconnected) => break,
        };
        let rel = line
            .trim_start_matches("./")
            .trim_start_matches('/')
            .replace('\\', "/");
        if push_filter_only_candidate(root, filters, &rel, &mut candidates)? {
            if candidates.len() >= candidate_cap {
                let _ = child.kill();
                break;
            }
        }
    }

    let _ = child.wait();
    Ok(Some(candidates))
}

fn filter_only_candidates_from_walk(
    root: &Path,
    filters: &SearchFilters,
    candidate_cap: usize,
    deadline: Instant,
) -> Result<Vec<(String, FilterOnlyMatch)>> {
    let mut candidates = Vec::new();
    for entry in WalkBuilder::new(&root)
        .hidden(false)
        .filter_entry(|entry| !is_ignored(entry.path()))
        .build()
    {
        if Instant::now() >= deadline {
            break;
        }
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
        if push_filter_only_candidate(root, filters, &rel, &mut candidates)? {
            if candidates.len() >= candidate_cap {
                break;
            }
        }
    }
    Ok(candidates)
}

fn push_filter_only_candidate(
    root: &Path,
    filters: &SearchFilters,
    rel: &str,
    candidates: &mut Vec<(String, FilterOnlyMatch)>,
) -> Result<bool> {
    let path = root.join(rel);
    let Some(metadata) = regular_file_metadata(&path) else {
        return Ok(false);
    };
    if metadata.len() > MAX_FILE_BYTES || language_for(Path::new(rel)).is_none() {
        return Ok(false);
    }
    if content_filters_active(filters) {
        let text = fs::read_to_string(&path).unwrap_or_default();
        if text.contains('\0') || !source_content_filters_match(rel, &text, filters) {
            return Ok(false);
        }
    }
    let rel_lower = rel.to_ascii_lowercase();
    let Some(matched) =
        score_filter_only_path_with_lower(rel, &rel_lower, filters, filters.explain)
    else {
        return Ok(false);
    };
    candidates.push((rel.to_string(), matched));
    Ok(true)
}

fn fd_positive_file_pattern(filters: &SearchFilters) -> Option<String> {
    if prefer_rg_files_for_positive_structural_scope(filters) {
        return None;
    }
    if let Some(file) = filters.file.as_deref() {
        let file = file.trim().replace('\\', "/");
        if file.is_empty() || file.contains('/') || file.contains('*') || file.contains('?') {
            return None;
        }
        return Some(regex::escape(&file));
    }
    code_fd_file_pattern(filters).map(ToString::to_string)
}

fn fd_max_results(filters: &SearchFilters, candidate_cap: usize) -> String {
    if fd_can_self_limit(filters) {
        candidate_cap.to_string()
    } else {
        "0".to_string()
    }
}

fn fd_can_self_limit(filters: &SearchFilters) -> bool {
    filters.file.as_deref().is_some_and(simple_fd_file_pattern)
        && filters.path.is_none()
        && filters.language.is_none()
        && filters.extension.is_none()
        && filters.symbol.is_none()
        && filters.symbol_kind.is_none()
        && filters.repo.is_none()
        && filters.branch.is_none()
        && filters.origin.is_none()
        && filters.dependency.is_none()
        && filters.import.is_none()
        && filters.test.is_none()
        && filters.generated.is_none()
        && filters.code.is_none()
        && filters.exclude_file.is_empty()
        && filters.exclude_path.is_empty()
        && filters.exclude_language.is_empty()
        && filters.exclude_extension.is_empty()
        && filters.exclude_symbol.is_empty()
        && filters.exclude_symbol_kind.is_empty()
        && filters.exclude_repo.is_empty()
        && filters.exclude_branch.is_empty()
        && filters.exclude_origin.is_empty()
        && filters.exclude_dependency.is_empty()
        && filters.exclude_import.is_empty()
        && filters.exclude_content.is_empty()
}

fn simple_fd_file_pattern(file: &str) -> bool {
    let file = file.trim();
    !file.is_empty()
        && !file.contains('/')
        && !file.contains('\\')
        && !file.contains('*')
        && !file.contains('?')
}

fn prefer_rg_files_for_positive_structural_scope(filters: &SearchFilters) -> bool {
    filters.test == Some(true) || filters.generated == Some(true)
}

fn code_fd_file_pattern(filters: &SearchFilters) -> Option<&'static str> {
    match filters.code {
        Some(true) => Some(FD_CODE_FILE_PATTERN),
        Some(false) => Some(FD_PROSE_FILE_PATTERN),
        None => None,
    }
}

fn rg_positive_file_glob(filters: &SearchFilters) -> Option<String> {
    rg_file_glob(filters.file.as_deref()?)
}

fn rg_file_glob(value: &str) -> Option<String> {
    let file = value.trim().replace('\\', "/");
    if file.is_empty() {
        return None;
    }
    if file.contains('*') || file.contains('?') {
        if file.contains('/') {
            Some(file)
        } else {
            Some(format!("**/{file}"))
        }
    } else if file.contains('/') {
        Some(format!("*{file}*"))
    } else {
        Some(format!("**/*{file}*"))
    }
}

fn rg_positive_path_globs(filters: &SearchFilters) -> Vec<String> {
    filters
        .path
        .as_deref()
        .map(rg_path_globs)
        .unwrap_or_default()
}

fn rg_path_globs(value: &str) -> Vec<String> {
    let path = value.trim().replace('\\', "/");
    if path.is_empty() {
        return Vec::new();
    }
    if path.contains('*') || path.contains('?') {
        if path.contains('/') {
            vec![path]
        } else {
            vec![format!("**/{path}"), format!("**/{path}/**")]
        }
    } else if safe_literal_glob_fragment(&path) {
        vec![format!("**/*{path}*"), format!("**/*{path}*/**")]
    } else {
        Vec::new()
    }
}

fn rg_extension_glob(extension: &str) -> Option<String> {
    let extension = extension
        .trim()
        .trim_start_matches('.')
        .to_ascii_lowercase();
    if extension.is_empty() || !safe_literal_glob_fragment(&extension) {
        return None;
    }
    Some(format!("**/*.{extension}"))
}

fn rg_language_globs(language: &str) -> &'static [&'static str] {
    match normalize_language_filter(language).as_str() {
        "python" => &["**/*.py"],
        "rust" => &["**/*.rs"],
        "javascript" => &["**/*.js", "**/*.jsx"],
        "typescript" => &["**/*.ts", "**/*.tsx"],
        "go" => &["**/*.go"],
        "ruby" => &["**/*.rb"],
        "java" => &["**/*.java"],
        "kotlin" => &["**/*.kt"],
        "swift" => &["**/*.swift"],
        "markdown" => &["**/*.md"],
        "toml" => &["**/*.toml"],
        "json" => &["**/*.json"],
        "yaml" => &["**/*.yaml", "**/*.yml"],
        "xml" => &["**/*.xml"],
        "gradle" => &["**/*.gradle"],
        "dockerfile" => &["**/Dockerfile"],
        "justfile" => &["**/Justfile"],
        "go-mod" => &["**/go.mod", "**/go.sum"],
        _ => &[],
    }
}

fn safe_literal_glob_fragment(value: &str) -> bool {
    !value
        .chars()
        .any(|ch| matches!(ch, '[' | ']' | '{' | '}' | '\0'))
}

fn add_scope_filter_ripgrep_globs(command: &mut Command, filters: &SearchFilters) {
    if let Some(glob) = rg_positive_file_glob(filters) {
        command.arg("--iglob").arg(glob);
    }
    for glob in rg_positive_path_globs(filters) {
        command.arg("--iglob").arg(glob);
    }
    for glob in rg_negative_file_globs(filters) {
        command.arg("--iglob").arg(format!("!{glob}"));
    }
    for glob in rg_negative_path_globs(filters) {
        command.arg("--iglob").arg(format!("!{glob}"));
    }
    if let Some(extension) = filters.extension.as_deref().and_then(rg_extension_glob) {
        command.arg("--iglob").arg(extension);
    }
    if let Some(language) = filters.language.as_deref() {
        for glob in rg_language_globs(language) {
            command.arg("--iglob").arg(glob);
        }
    }
    for extension in filters
        .exclude_extension
        .iter()
        .filter_map(|extension| rg_extension_glob(extension).map(|glob| format!("!{glob}")))
    {
        command.arg("--iglob").arg(extension);
    }
    for language in &filters.exclude_language {
        for glob in rg_language_globs(language) {
            command.arg("--iglob").arg(format!("!{glob}"));
        }
    }
    add_test_filter_ripgrep_globs(command, filters.test);
    add_generated_filter_ripgrep_globs(command, filters);
    add_code_filter_ripgrep_globs(command, filters);
}

fn add_generated_filter_fd_excludes(command: &mut Command, filters: &SearchFilters) {
    if filters.generated != Some(false) {
        return;
    }
    for segment in GENERATED_DIRECTORY_SEGMENTS {
        command.arg("--exclude").arg(segment);
    }
    for glob in GENERATED_FILE_GLOBS {
        command.arg("--exclude").arg(glob);
    }
}

fn add_generated_filter_ripgrep_globs(command: &mut Command, filters: &SearchFilters) {
    for glob in generated_ripgrep_globs(filters) {
        command.arg("--iglob").arg(glob);
    }
}

fn generated_ripgrep_globs(filters: &SearchFilters) -> Vec<String> {
    let Some(generated) = filters.generated else {
        return Vec::new();
    };
    let prefix = if generated { "" } else { "!" };
    GENERATED_DIRECTORY_SEGMENTS
        .iter()
        .map(|segment| format!("{prefix}**/{segment}/**"))
        .chain(
            GENERATED_FILE_GLOBS
                .iter()
                .map(|glob| format!("{prefix}**/{glob}")),
        )
        .collect()
}

fn add_code_filter_ripgrep_globs(command: &mut Command, filters: &SearchFilters) {
    for glob in code_ripgrep_globs(filters) {
        command.arg("--iglob").arg(glob);
    }
}

fn code_ripgrep_globs(filters: &SearchFilters) -> Vec<String> {
    let Some(code) = filters.code else {
        return Vec::new();
    };
    let has_positive_scope = positive_ripgrep_scope_active(filters);
    match (code, has_positive_scope) {
        (true, false) => CODE_FILE_GLOBS
            .iter()
            .map(|glob| (*glob).to_string())
            .collect(),
        (true, true) => PROSE_FILE_GLOBS
            .iter()
            .chain(PROSE_FILE_NAME_GLOBS.iter())
            .map(|glob| format!("!{glob}"))
            .collect(),
        (false, false) => PROSE_FILE_GLOBS
            .iter()
            .chain(PROSE_FILE_NAME_GLOBS.iter())
            .map(|glob| (*glob).to_string())
            .collect(),
        (false, true) => CODE_FILE_GLOBS
            .iter()
            .map(|glob| format!("!{glob}"))
            .collect(),
    }
}

fn positive_ripgrep_scope_active(filters: &SearchFilters) -> bool {
    filters
        .file
        .as_deref()
        .is_some_and(|file| !file.trim().is_empty())
        || filters
            .path
            .as_deref()
            .is_some_and(|path| !path.trim().is_empty())
        || filters
            .extension
            .as_deref()
            .is_some_and(|extension| !extension.trim().is_empty())
        || filters
            .language
            .as_deref()
            .is_some_and(|language| !language.trim().is_empty())
        || filters.test == Some(true)
        || filters.generated == Some(true)
}

fn rg_negative_file_globs(filters: &SearchFilters) -> Vec<String> {
    filters
        .exclude_file
        .iter()
        .filter(|file| !file.contains('/') && !file.contains('\\'))
        .filter_map(|file| rg_file_glob(file))
        .collect()
}

fn rg_negative_path_globs(filters: &SearchFilters) -> Vec<String> {
    filters
        .exclude_path
        .iter()
        .flat_map(|path| rg_path_globs(path))
        .collect()
}

fn filter_only_candidate_cap(limit: usize, filters: &SearchFilters) -> usize {
    if rg_positive_file_glob(filters).is_some() {
        limit.max(1)
    } else {
        (limit.max(1) * 100).clamp(100, 5_000)
    }
}

fn ripgrep_patterns(
    raw_terms: &[String],
    query_tokens: &[String],
    query_phrases: &[String],
    filters: &SearchFilters,
) -> Vec<String> {
    let mut patterns = Vec::new();
    if let Some(symbol) = &filters.symbol {
        let symbol = symbol.trim();
        if !symbol.is_empty() {
            patterns.push(symbol.to_string());
        }
    }

    let token_source = if filters.symbol.is_some() && !raw_terms.is_empty() {
        unique_query_tokens(&raw_terms.join(" "))
    } else if filters.symbol.is_some() {
        Vec::new()
    } else {
        query_tokens.to_vec()
    };
    patterns.extend(token_source);
    patterns.extend(query_phrases.iter().cloned());
    patterns.sort();
    patterns.dedup();
    patterns
}

fn search_repo_ripgrep(
    root: &Path,
    raw_terms: &[String],
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

    add_scope_filter_ripgrep_globs(&mut command, filters);

    for pattern in ripgrep_patterns(raw_terms, query_tokens, query_phrases, filters) {
        command.arg("-e").arg(pattern);
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
    let mut path_filter_cache = HashMap::<String, Option<String>>::new();
    let mut content_filter_cache = HashMap::<String, bool>::new();
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
                let _ = child.try_wait()?;
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
        let path_lower = path_filter_cache
            .entry(path.clone())
            .or_insert_with(|| {
                let path_lower = path.to_ascii_lowercase();
                if language_for(Path::new(&path)).is_none()
                    || is_ignored(Path::new(&path))
                    || !matches_filters_with_path_lower(&path, &path_lower, filters)
                {
                    None
                } else {
                    Some(path_lower)
                }
            })
            .as_deref();
        let Some(path_lower) = path_lower else {
            continue;
        };
        if !source_content_filters_match_cached(root, &path, filters, &mut content_filter_cache) {
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
            path_lower,
            text,
            line_number,
            query_tokens,
            query_phrases,
            true,
            filters.snippet,
            filters.explain,
            filters.generated.is_none(),
        );
        match_count += 1;
        if match_count >= max_matches {
            let _ = child.kill();
            break;
        }
    }

    let _ = child.wait();
    let mut results = scored.into_values().collect::<Vec<_>>();
    retain_results_matching_file_symbol_filters(root, &mut results, filters);
    if filters.require_all {
        results.retain(|result| result_matches_all_tokens(result, query_tokens));
    }
    if !query_phrases.is_empty() {
        results.retain(|result| result_or_file_matches_phrases(root, result, query_phrases));
    }
    if strict_fallback_rescue_needed(query_tokens, query_phrases, filters) {
        refresh_result_snippets_from_files(
            root,
            &mut results,
            query_tokens,
            query_phrases,
            filters.snippet,
            filters.explain,
        );
    }
    Ok(Some(finalize_results_for_filters(results, limit, filters)))
}

const TEST_RIPGREP_GLOBS: &[&str] = &[
    "test/**",
    "tests/**",
    "spec/**",
    "specs/**",
    "**/test/**",
    "**/tests/**",
    "**/__tests__/**",
    "**/spec/**",
    "**/specs/**",
    "**/test_*",
    "**/tests_*",
    "**/test-*",
    "**/tests-*",
    "**/spec_*",
    "**/specs_*",
    "**/spec-*",
    "**/specs-*",
    "**/*_test.*",
    "**/*_tests.*",
    "**/*_spec.*",
    "**/*_specs.*",
    "**/*.test.*",
    "**/*.tests.*",
    "**/*.spec.*",
    "**/*.specs.*",
    "**/*-test.*",
    "**/*-tests.*",
    "**/*-spec.*",
    "**/*-specs.*",
];

fn add_test_filter_ripgrep_globs(command: &mut Command, test: Option<bool>) {
    match test {
        Some(true) => {
            for glob in TEST_RIPGREP_GLOBS {
                command.arg("--iglob").arg(glob);
            }
        }
        Some(false) => {
            for glob in TEST_RIPGREP_GLOBS {
                command.arg("--iglob").arg(format!("!{glob}"));
            }
        }
        None => {}
    }
}

fn refresh_result_snippets_from_files(
    root: &Path,
    results: &mut [SearchResult],
    query_tokens: &[String],
    query_phrases: &[String],
    snippet_mode: SnippetMode,
    explain: bool,
) {
    for result in results {
        let text = fs::read_to_string(root.join(&result.path)).unwrap_or_default();
        if text.contains('\0') {
            continue;
        }
        refresh_result_symbol_scores_from_text(result, &text, query_tokens, explain);
        let snippet = best_snippet_for_path_with_phrases(
            &result.path,
            &text,
            query_tokens,
            query_phrases,
            snippet_mode,
        );
        if !snippet.is_empty() {
            result.snippet = snippet;
            result.line_range = None;
            result.match_lines =
                ranked_match_lines_from_text(&result.path, &text, query_tokens, query_phrases, 16);
        }
    }
}

fn refresh_result_symbol_scores_from_text(
    result: &mut SearchResult,
    text: &str,
    query_tokens: &[String],
    explain: bool,
) {
    if query_tokens.is_empty() {
        return;
    }
    let Some(language) = language_for(Path::new(&result.path)) else {
        return;
    };
    let query_name = query_tokens.join("");
    let mut reasons = result
        .reason
        .trim_start_matches("matched ")
        .split(", ")
        .filter(|value| !value.is_empty())
        .map(String::from)
        .collect::<Vec<_>>();
    let mut score = 0.0;
    let mut signals = Vec::new();
    for symbol in extract_symbols(&result.path, text, &language) {
        apply_symbol_match(
            &symbol.name,
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
    result.score = round4(result.score + score);
    reasons.sort();
    result.reason = format!("matched {}", reasons.join(", "));
    if explain {
        result
            .explanation
            .get_or_insert_with(Vec::new)
            .extend(signals);
    }
}

fn search_repo_streaming_until(
    root: &Path,
    query_tokens: &[String],
    query_phrases: &[String],
    limit: usize,
    filters: &SearchFilters,
    deadline: Instant,
) -> Result<Vec<SearchResult>> {
    let mut results = Vec::new();

    for entry in WalkBuilder::new(&root)
        .hidden(false)
        .filter_entry(|entry| !is_ignored(entry.path()))
        .build()
    {
        if Instant::now() >= deadline {
            break;
        }
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
        if !matches_filters(&rel, filters) || !source_content_filters_match(&rel, &text, filters) {
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
            filters.generated.is_none(),
        ) {
            results.push(result);
        }
    }

    retain_results_matching_file_symbol_filters(root, &mut results, filters);
    if filters.require_all {
        results.retain(|result| result_matches_all_tokens(result, query_tokens));
    }
    Ok(finalize_results_for_filters(results, limit, filters))
}

fn retain_results_matching_file_symbol_filters(
    root: &Path,
    results: &mut Vec<SearchResult>,
    filters: &SearchFilters,
) {
    if filters.symbol.is_none() && filters.exclude_symbol.is_empty() {
        return;
    }
    results.retain_mut(|result| result_matches_file_symbol_filters(root, result, filters));
}

fn result_matches_file_symbol_filters(
    root: &Path,
    result: &mut SearchResult,
    filters: &SearchFilters,
) -> bool {
    let needs_file_check = filters.symbol.is_some() || !filters.exclude_symbol.is_empty();
    if !needs_file_check {
        return true;
    }

    let text = fs::read_to_string(root.join(&result.path)).unwrap_or_default();
    if text.contains('\0') {
        return false;
    }
    let Some(language) = language_for(Path::new(&result.path)) else {
        return false;
    };
    let symbols = extract_symbols(&result.path, &text, &language);
    if filters.exclude_symbol.iter().any(|wanted| {
        symbols
            .iter()
            .any(|symbol| symbol_name_matches(symbol, wanted))
    }) {
        return false;
    }
    if let Some(wanted) = &filters.symbol {
        let Some(symbol) = symbols
            .iter()
            .find(|symbol| symbol_name_matches(symbol, wanted))
        else {
            return false;
        };
        append_symbol_reason(result, &symbol.name);
        anchor_result_on_symbol(result, &text, symbol, filters.snippet);
    }
    true
}

fn symbol_name_matches(symbol: &Symbol, wanted: &str) -> bool {
    let wanted = normalize_token(wanted);
    !wanted.is_empty() && normalize_token(&symbol.name) == wanted
}

fn append_symbol_reason(result: &mut SearchResult, symbol_name: &str) {
    if reason_contains_symbol(&result.reason, symbol_name) {
        return;
    }
    if result.reason.trim().is_empty() {
        result.reason = format!("matched symbol:{symbol_name}");
    } else {
        result.reason.push_str(", symbol:");
        result.reason.push_str(symbol_name);
    }
    result.score = round4(result.score + 20.0);
}

fn anchor_result_on_symbol(
    result: &mut SearchResult,
    text: &str,
    symbol: &Symbol,
    snippet_mode: SnippetMode,
) {
    let (before, after) = snippet_mode.window();
    let start_line = symbol.line.saturating_sub(before).max(1);
    let line_count = before + after + 1;
    let range = file_range_from_text(&result.path, text, start_line, line_count);
    result.snippet = range.text.chars().take(snippet_mode.max_chars()).collect();
    result.match_lines.retain(|line| *line != symbol.line);
    result.match_lines.insert(0, symbol.line);
}

fn merge_match_result(
    scored: &mut HashMap<String, SearchResult>,
    root: &Path,
    path: &str,
    path_lower: &str,
    line: &str,
    line_number: u64,
    query_tokens: &[String],
    query_phrases: &[String],
    parse_symbols: bool,
    snippet_mode: SnippetMode,
    explain: bool,
    demote_generated: bool,
) {
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
    if demote_generated && is_generated_path(path_lower) {
        score *= GENERATED_PATH_SCORE_MULTIPLIER;
        if explain {
            signals.push(rank_signal(
                "generated_path_penalty",
                path,
                -1.0 + GENERATED_PATH_SCORE_MULTIPLIER,
            ));
        }
    }

    let snippet_line = line.trim_end();
    let snippet = if matches!(snippet_mode, SnippetMode::Block | SnippetMode::Symbol) {
        fs::read_to_string(root.join(path))
            .ok()
            .map(|text| {
                best_snippet_for_path_with_phrases(
                    path,
                    &text,
                    query_tokens,
                    query_phrases,
                    snippet_mode,
                )
            })
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
            read_range: None,
            read_request: None,
            related_request: None,
            related_symbols_request: None,
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
            || !dependency_filters_match(&self.dependency_hints(), filters)
        {
            return Vec::new();
        }

        let mut scored = Vec::new();
        for symbol in &self.symbols {
            if !self.related_symbol_matches_filters(symbol, filters) {
                continue;
            }
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
        let query_tokens = unique_query_tokens(query);
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
                    read_range: None,
                    read_request: None,
                    related_request: None,
                    related_symbols_request: None,
                });
            }
        }

        finalize_results(results, limit)
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
        if !self.files.contains_key(&normalized) {
            return Vec::new();
        }
        if !repo_matches(&self.root, filters)
            || !dependency_filters_match(&self.dependency_hints(), filters)
        {
            return Vec::new();
        }
        let stem = Path::new(&normalized)
            .file_stem()
            .map(|value| value.to_string_lossy().to_string())
            .unwrap_or_default();
        let stem_lower = stem.to_ascii_lowercase();
        let stem_terms = related_stem_terms(&stem);
        let directory = Path::new(&normalized)
            .parent()
            .map(|value| value.to_string_lossy().to_string())
            .unwrap_or_default();
        let source_is_test = is_test_path(&normalized.to_ascii_lowercase());
        let source_symbols = self
            .files
            .get(&normalized)
            .map(|file| {
                file.symbols
                    .iter()
                    .map(|symbol| (symbol.name.clone(), symbol.name.to_ascii_lowercase()))
                    .collect::<Vec<_>>()
            })
            .unwrap_or_default();
        let mut related = Vec::new();
        for (file_path, file) in &self.files {
            if file_path == &normalized {
                continue;
            }
            if !Self::related_file_matches_filters(file_path, file, filters) {
                continue;
            }
            let lower = file_path.to_ascii_lowercase();
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
            let file_dir = Path::new(file_path)
                .parent()
                .map(|value| value.to_string_lossy().to_string())
                .unwrap_or_default();
            if !directory.is_empty() && file_dir == directory {
                score += 1.0;
                reasons.push("same directory".to_string());
            }
            if let Some(symbol) = referenced_symbol_name(&file.text, &source_symbols) {
                score += 6.0;
                reasons.push(format!("references symbol {symbol}"));
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
        self.related_symbols_filtered(path, query, limit, &SearchFilters::default())
    }

    pub fn related_symbols_filtered(
        &self,
        path: Option<&str>,
        query: Option<&str>,
        limit: usize,
        filters: &SearchFilters,
    ) -> Vec<RelatedSymbol> {
        let normalized_path = path.map(|value| value.trim_start_matches('/').to_string());
        if let Some(path) = &normalized_path {
            if !self.files.contains_key(path) {
                return Vec::new();
            }
        }
        let (query_terms, query_symbol, query_filters) =
            related_query_terms_symbol_and_filters(query);
        let filters = merge_filters(filters.clone(), query_filters);
        if !repo_matches(&self.root, &filters)
            || !dependency_filters_match(&self.dependency_hints(), &filters)
        {
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

        for symbol in &self.symbols {
            if !self.related_symbol_matches_filters(symbol, &filters) {
                continue;
            }
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
                let symbol_name_lower = symbol.name.to_ascii_lowercase();
                if !path_stem.is_empty()
                    && (symbol_name_lower.contains(&path_stem)
                        || symbol_path_lower.contains(&path_stem))
                {
                    score += 3.0;
                    reasons.push(format!("shares stem {path_stem}"));
                }
                if path_stem_terms.iter().any(|term| {
                    symbol_name_lower.contains(term) || symbol_path_lower.contains(term)
                }) {
                    score += 3.0;
                    reasons.push("shares normalized stem".to_string());
                }
            }

            if !query_tokens.is_empty() {
                let symbol_name_tokens = tokenize(&symbol.name);
                let symbol_path_tokens = tokenize(&symbol.path);
                let overlap =
                    query_token_overlap(&query_tokens, &symbol_name_tokens, &symbol_path_tokens);
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

    fn related_symbol_matches_filters(&self, symbol: &Symbol, filters: &SearchFilters) -> bool {
        let Some(file) = self.files.get(&symbol.path) else {
            return false;
        };
        let path_lower = symbol.path.to_ascii_lowercase();
        let file_name_lower = Path::new(&symbol.path)
            .file_name()
            .map(|value| value.to_string_lossy().to_ascii_lowercase())
            .unwrap_or_default();
        let extension_lower = Path::new(&symbol.path)
            .extension()
            .map(|value| value.to_string_lossy().to_lowercase());
        matches_filters_with_path_metadata(
            &path_lower,
            &file_name_lower,
            extension_lower.as_deref(),
            Some(&file.language),
            filters,
        ) && source_import_filters_match(&symbol.path, &file.text, filters)
            && source_excluded_content_filters_match(&file.text, filters)
            && symbol_matches_related_filters(&symbol.name, &symbol.kind, filters)
    }

    fn related_file_matches_filters(
        path: &str,
        file: &IndexedFile,
        filters: &SearchFilters,
    ) -> bool {
        let path_lower = path.to_ascii_lowercase();
        let file_name_lower = Path::new(path)
            .file_name()
            .map(|value| value.to_string_lossy().to_ascii_lowercase())
            .unwrap_or_default();
        let extension_lower = Path::new(path)
            .extension()
            .map(|value| value.to_string_lossy().to_lowercase());
        matches_filters_with_path_metadata(
            &path_lower,
            &file_name_lower,
            extension_lower.as_deref(),
            Some(&file.language),
            filters,
        ) && source_import_filters_match(path, &file.text, filters)
            && source_excluded_content_filters_match(&file.text, filters)
            && symbol_kind_filters_match(&file.symbols, filters)
            && file_symbol_filters_match(&file.symbols, filters)
    }

    pub fn repo_brief(&self) -> RepoBrief {
        self.repo_brief_with_detail(RepoMapDetail::Compact)
    }

    pub fn repo_brief_with_detail(&self, detail: RepoMapDetail) -> RepoBrief {
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
        let dependency_hints = self.dependency_hints();
        let import_hints = match detail {
            RepoMapDetail::Compact => select_repo_brief_import_hints(self.import_hints()),
            RepoMapDetail::Full => self.import_hints(),
        };

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
            dependency_hints,
            import_hints,
            manifest_files,
            important_files,
        }
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

        let top_symbols = select_repo_map_top_symbols(self.symbols.clone(), symbol_limit);

        let brief = self.repo_brief_with_detail(detail);
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
            read_batch_request: None,
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

    fn dependency_hints(&self) -> Vec<DependencyHint> {
        dependency_hints_from_manifest_texts(
            self.files
                .iter()
                .map(|(path, file)| (path.as_str(), file.text.as_str())),
        )
    }

    fn import_hints(&self) -> Vec<ImportHint> {
        import_hints_from_source_texts(
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
    read_file_range_scoped(root, path, start_line, line_count, RangeScope::Exact)
}

pub fn read_file_range_scoped(
    root: impl AsRef<Path>,
    path: &str,
    start_line: usize,
    line_count: usize,
    scope: RangeScope,
) -> Result<FileRange> {
    let root = root.as_ref().canonicalize()?;
    let normalized_separators = path.replace('\\', "/");
    let requested = Path::new(&normalized_separators);
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

    Ok(file_range_from_text_scoped(
        rel, &text, start_line, line_count, scope,
    ))
}

pub(crate) fn file_range_from_text(
    path: impl Into<String>,
    text: &str,
    start_line: usize,
    line_count: usize,
) -> FileRange {
    file_range_from_text_with_symbol(path, text, start_line, line_count, None)
}

pub(crate) fn file_range_from_text_scoped(
    path: impl Into<String>,
    text: &str,
    start_line: usize,
    line_count: usize,
    scope: RangeScope,
) -> FileRange {
    let path = path.into();
    if scope == RangeScope::Symbol {
        let language = language_for(Path::new(&path)).unwrap_or_else(|| "text".to_string());
        let symbols = extract_symbols(&path, text, &language);
        if let Some(symbol) = symbol_for_anchor(&symbols, start_line) {
            let (symbol_start, symbol_lines) =
                symbol_scoped_window(symbol.line, line_count, DEFAULT_SYMBOL_READ_CONTEXT_BEFORE);
            return file_range_from_text_with_symbol(
                path,
                text,
                symbol_start,
                symbol_lines,
                Some(symbol.clone()),
            );
        }
    }
    file_range_from_text_with_symbol(path, text, start_line, line_count, None)
}

pub(crate) fn symbol_scoped_window(
    symbol_line: usize,
    requested_lines: usize,
    context_before: usize,
) -> (usize, usize) {
    let start = symbol_line.saturating_sub(context_before).max(1);
    let prefix_lines = symbol_line.saturating_sub(start);
    let lines = requested_lines
        .max(1)
        .saturating_add(prefix_lines)
        .min(MAX_READ_RANGE_LINES);
    (start, lines)
}

pub(crate) fn symbol_for_anchor(symbols: &[Symbol], anchor_line: usize) -> Option<&Symbol> {
    symbols
        .iter()
        .filter(|symbol| is_context_anchor_symbol(&symbol.kind))
        .filter(|symbol| symbol.line <= anchor_line)
        .max_by_key(|symbol| symbol.line)
        .or_else(|| {
            symbols
                .iter()
                .filter(|symbol| is_context_anchor_symbol(&symbol.kind))
                .next()
        })
        .or_else(|| {
            symbols
                .iter()
                .filter(|symbol| symbol.line <= anchor_line)
                .max_by_key(|symbol| symbol.line)
        })
        .or_else(|| symbols.first())
}

fn is_context_anchor_symbol(kind: &str) -> bool {
    !matches!(kind, "const" | "let" | "var")
}

fn file_range_from_text_with_symbol(
    path: impl Into<String>,
    text: &str,
    start_line: usize,
    line_count: usize,
    symbol: Option<Symbol>,
) -> FileRange {
    let lines = text.lines().collect::<Vec<_>>();
    let total_lines = lines.len();
    let start = start_line.max(1).min(total_lines.max(1));
    let count = line_count.max(1).min(MAX_READ_RANGE_LINES);
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
        symbol,
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

pub(crate) fn dependency_hints_from_manifest_texts<'a>(
    files: impl IntoIterator<Item = (&'a str, &'a str)>,
) -> Vec<DependencyHint> {
    let mut hints = Vec::new();
    for (path, text) in files {
        let file_name = Path::new(path)
            .file_name()
            .and_then(|value| value.to_str())
            .unwrap_or(path);
        match file_name {
            "Cargo.toml" => hints.extend(cargo_dependency_hints(text, path)),
            "package.json" => hints.extend(package_json_dependency_hints(text, path)),
            "pyproject.toml" => hints.extend(pyproject_dependency_hints(text, path)),
            "go.mod" => hints.extend(go_mod_dependency_hints(text, path)),
            _ => {}
        }
    }
    hints.sort_by(|left, right| {
        dependency_kind_rank(&left.kind)
            .cmp(&dependency_kind_rank(&right.kind))
            .then_with(|| left.name.cmp(&right.name))
            .then_with(|| left.source.cmp(&right.source))
    });
    hints.dedup_by(|left, right| {
        left.name == right.name && left.kind == right.kind && left.source == right.source
    });
    hints.truncate(40);
    hints
}

pub(crate) fn dependency_filters_match(hints: &[DependencyHint], filters: &SearchFilters) -> bool {
    let names = hints
        .iter()
        .map(|hint| hint.name.to_ascii_lowercase())
        .collect::<Vec<_>>();
    if let Some(wanted) = &filters.dependency {
        let wanted = wanted.to_ascii_lowercase();
        if !names.iter().any(|name| name.contains(&wanted)) {
            return false;
        }
    }
    !filters.exclude_dependency.iter().any(|excluded| {
        let excluded = excluded.to_ascii_lowercase();
        names.iter().any(|name| name.contains(&excluded))
    })
}

pub(crate) fn import_hints_from_source_texts<'a>(
    files: impl IntoIterator<Item = (&'a str, &'a str)>,
) -> Vec<ImportHint> {
    let mut hints = Vec::new();
    for (path, text) in files {
        let Some(language) = language_for(Path::new(path)) else {
            continue;
        };
        for (line_index, line) in text.lines().enumerate() {
            let line_number = line_index + 1;
            match language.as_str() {
                "rust" => hints.extend(rust_import_hints(line, path, line_number)),
                "python" => hints.extend(python_import_hints(line, path, line_number)),
                "javascript" | "typescript" => {
                    hints.extend(js_import_hints(line, path, line_number))
                }
                "go" => hints.extend(go_import_hints(line, path, line_number)),
                _ => {}
            }
        }
    }
    hints.sort_by(|left, right| {
        left.source
            .cmp(&right.source)
            .then_with(|| left.line.cmp(&right.line))
            .then_with(|| left.kind.cmp(&right.kind))
            .then_with(|| left.module.cmp(&right.module))
    });
    hints.dedup_by(|left, right| {
        left.module == right.module
            && left.kind == right.kind
            && left.source == right.source
            && left.line == right.line
    });
    hints.truncate(80);
    hints
}

pub(crate) fn select_repo_brief_import_hints(mut hints: Vec<ImportHint>) -> Vec<ImportHint> {
    if hints.len() <= MAX_REPO_BRIEF_IMPORT_HINTS {
        return hints;
    }
    hints.sort_by(|left, right| {
        left.source
            .cmp(&right.source)
            .then_with(|| left.line.cmp(&right.line))
            .then_with(|| left.kind.cmp(&right.kind))
            .then_with(|| left.module.cmp(&right.module))
    });

    let per_source_cap = (MAX_REPO_BRIEF_IMPORT_HINTS / 4).max(4);
    let mut selected = Vec::with_capacity(MAX_REPO_BRIEF_IMPORT_HINTS);
    let mut source_counts = HashMap::<String, usize>::new();
    for hint in &hints {
        let count = source_counts.entry(hint.source.clone()).or_default();
        if *count < per_source_cap {
            selected.push(hint.clone());
            *count += 1;
        }
        if selected.len() == MAX_REPO_BRIEF_IMPORT_HINTS {
            return selected;
        }
    }

    for hint in hints {
        if selected.iter().any(|selected| {
            selected.source == hint.source
                && selected.line == hint.line
                && selected.kind == hint.kind
                && selected.module == hint.module
        }) {
            continue;
        }
        selected.push(hint);
        if selected.len() == MAX_REPO_BRIEF_IMPORT_HINTS {
            break;
        }
    }
    selected
}

pub(crate) fn import_filters_active(filters: &SearchFilters) -> bool {
    filters.import.is_some() || !filters.exclude_import.is_empty()
}

pub(crate) fn symbol_kind_filters_active(filters: &SearchFilters) -> bool {
    filters.symbol_kind.is_some() || !filters.exclude_symbol_kind.is_empty()
}

pub(crate) fn content_filters_active(filters: &SearchFilters) -> bool {
    import_filters_active(filters)
        || symbol_kind_filters_active(filters)
        || !filters.exclude_content.is_empty()
}

pub(crate) fn source_content_filters_match(
    path: &str,
    text: &str,
    filters: &SearchFilters,
) -> bool {
    source_import_filters_match(path, text, filters)
        && source_symbol_kind_filters_match(path, text, filters)
        && source_excluded_content_filters_match(text, filters)
}

fn source_content_filters_match_cached(
    root: &Path,
    path: &str,
    filters: &SearchFilters,
    cache: &mut HashMap<String, bool>,
) -> bool {
    if !content_filters_active(filters) {
        return true;
    }
    *cache.entry(path.to_string()).or_insert_with(|| {
        let text = fs::read_to_string(root.join(path)).unwrap_or_default();
        !text.contains('\0') && source_content_filters_match(path, &text, filters)
    })
}

pub(crate) fn source_import_filters_match(path: &str, text: &str, filters: &SearchFilters) -> bool {
    if !import_filters_active(filters) {
        return true;
    }
    let hints = import_hints_from_source_texts([(path, text)]);
    import_filters_match(&hints, filters)
}

pub(crate) fn source_excluded_content_filters_match(text: &str, filters: &SearchFilters) -> bool {
    if filters.exclude_content.is_empty() {
        return true;
    }
    let text_lower = text.to_ascii_lowercase();
    let mut normalized_text = None;
    for excluded in filters.exclude_content.iter().map(|value| value.trim()) {
        if excluded.is_empty() {
            continue;
        }
        if text_lower.contains(&excluded.to_ascii_lowercase()) {
            return false;
        }
        let normalized = normalize_phrase_text(excluded);
        if normalized.is_empty() {
            continue;
        }
        let normalized_text = normalized_text.get_or_insert_with(|| normalize_phrase_text(text));
        if normalized_text.contains(&normalized) {
            return false;
        }
    }
    true
}

pub(crate) fn source_symbol_kind_filters_match(
    path: &str,
    text: &str,
    filters: &SearchFilters,
) -> bool {
    if !symbol_kind_filters_active(filters) {
        return true;
    }
    let Some(language) = language_for(Path::new(path)) else {
        return false;
    };
    symbol_kind_filters_match(&extract_symbols(path, text, &language), filters)
}

pub(crate) fn symbol_kind_filters_match(symbols: &[Symbol], filters: &SearchFilters) -> bool {
    let kinds = symbols
        .iter()
        .map(|symbol| symbol.kind.to_ascii_lowercase())
        .collect::<Vec<_>>();
    if let Some(wanted) = &filters.symbol_kind {
        let wanted = wanted.to_ascii_lowercase();
        if !kinds.iter().any(|kind| kind == &wanted) {
            return false;
        }
    }
    !filters.exclude_symbol_kind.iter().any(|excluded| {
        let excluded = excluded.to_ascii_lowercase();
        kinds.iter().any(|kind| kind == &excluded)
    })
}

pub(crate) fn file_symbol_filters_match(symbols: &[Symbol], filters: &SearchFilters) -> bool {
    if filters.symbol.is_none() && filters.exclude_symbol.is_empty() {
        return true;
    }
    if filters.exclude_symbol.iter().any(|wanted| {
        symbols
            .iter()
            .any(|symbol| symbol_name_matches(symbol, wanted))
    }) {
        return false;
    }
    filters.symbol.as_ref().is_none_or(|wanted| {
        symbols
            .iter()
            .any(|symbol| symbol_name_matches(symbol, wanted))
    })
}

fn import_filters_match(hints: &[ImportHint], filters: &SearchFilters) -> bool {
    let modules = hints
        .iter()
        .map(|hint| hint.module.to_ascii_lowercase())
        .collect::<Vec<_>>();
    if let Some(wanted) = &filters.import {
        let wanted = wanted.to_ascii_lowercase();
        if !modules.iter().any(|module| module.contains(&wanted)) {
            return false;
        }
    }
    !filters.exclude_import.iter().any(|excluded| {
        let excluded = excluded.to_ascii_lowercase();
        modules.iter().any(|module| module.contains(&excluded))
    })
}

fn import_hint(
    module: impl Into<String>,
    kind: impl Into<String>,
    source: impl Into<String>,
    line: usize,
) -> Option<ImportHint> {
    let module = normalize_import_module(&module.into())?;
    Some(ImportHint {
        module,
        kind: kind.into(),
        source: source.into(),
        line,
    })
}

fn rust_import_hints(line: &str, source: &str, line_number: usize) -> Vec<ImportHint> {
    let line = line.trim();
    if let Some(module) = line.strip_prefix("use ") {
        return import_hint(rust_use_module(module), "use", source, line_number)
            .into_iter()
            .collect();
    }
    if let Some(module) = line
        .strip_prefix("pub mod ")
        .or_else(|| line.strip_prefix("mod "))
    {
        return import_hint(module.trim_end_matches(';'), "mod", source, line_number)
            .into_iter()
            .collect();
    }
    Vec::new()
}

fn rust_use_module(module: &str) -> String {
    module
        .trim_end_matches(';')
        .split("::{")
        .next()
        .unwrap_or(module)
        .trim()
        .to_string()
}

fn python_import_hints(line: &str, source: &str, line_number: usize) -> Vec<ImportHint> {
    let line = line.trim();
    if let Some(modules) = line.strip_prefix("import ") {
        return modules
            .split(',')
            .filter_map(|module| {
                let module = module.split_whitespace().next().unwrap_or_default().trim();
                import_hint(module, "import", source, line_number)
            })
            .collect();
    }
    if let Some(rest) = line.strip_prefix("from ") {
        if let Some((module, _)) = rest.split_once(" import ") {
            return import_hint(module, "from", source, line_number)
                .into_iter()
                .collect();
        }
    }
    Vec::new()
}

fn js_import_hints(line: &str, source: &str, line_number: usize) -> Vec<ImportHint> {
    static JS_FROM_RE: LazyLock<Regex> = LazyLock::new(|| {
        Regex::new(r#"\b(?:import|export)\b.+?\bfrom\s+["']([^"']+)["']"#).unwrap()
    });
    static JS_SIDE_EFFECT_RE: LazyLock<Regex> =
        LazyLock::new(|| Regex::new(r#"\bimport\s+["']([^"']+)["']"#).unwrap());
    static JS_REQUIRE_RE: LazyLock<Regex> =
        LazyLock::new(|| Regex::new(r#"\brequire\(\s*["']([^"']+)["']\s*\)"#).unwrap());

    let mut hints = Vec::new();
    if let Some(module) = JS_FROM_RE
        .captures(line)
        .and_then(|capture| capture.get(1))
        .and_then(|module| import_hint(module.as_str(), "import", source, line_number))
    {
        hints.push(module);
    }
    if let Some(module) = JS_SIDE_EFFECT_RE
        .captures(line)
        .and_then(|capture| capture.get(1))
        .and_then(|module| import_hint(module.as_str(), "import", source, line_number))
    {
        hints.push(module);
    }
    if let Some(module) = JS_REQUIRE_RE
        .captures(line)
        .and_then(|capture| capture.get(1))
        .and_then(|module| import_hint(module.as_str(), "require", source, line_number))
    {
        hints.push(module);
    }
    hints
}

fn go_import_hints(line: &str, source: &str, line_number: usize) -> Vec<ImportHint> {
    static GO_IMPORT_RE: LazyLock<Regex> = LazyLock::new(|| Regex::new(r#""([^"]+)""#).unwrap());
    let line = line.trim();
    if line == "import (" || line == ")" || line.starts_with("//") {
        return Vec::new();
    }
    if !line.starts_with("import ") && !line.starts_with('"') {
        return Vec::new();
    }
    GO_IMPORT_RE
        .captures_iter(line)
        .filter_map(|capture| {
            capture
                .get(1)
                .and_then(|module| import_hint(module.as_str(), "import", source, line_number))
        })
        .collect()
}

fn normalize_import_module(module: &str) -> Option<String> {
    let module = module
        .trim()
        .trim_matches(|ch: char| ch == '"' || ch == '\'' || ch == ';' || ch == ',' || ch == '{')
        .trim();
    if module.is_empty()
        || module.starts_with("//")
        || module.starts_with('#')
        || !module.chars().any(|ch| ch.is_alphanumeric() || ch == '.')
    {
        return None;
    }
    Some(module.chars().take(160).collect())
}

fn repo_dependency_filters_match(root: &Path, filters: &SearchFilters) -> Result<bool> {
    if filters.dependency.is_none() && filters.exclude_dependency.is_empty() {
        return Ok(true);
    }
    Ok(dependency_filters_match(
        &dependency_hints_from_live_repo(root)?,
        filters,
    ))
}

fn dependency_hints_from_live_repo(root: &Path) -> Result<Vec<DependencyHint>> {
    let mut manifests = Vec::new();
    for entry in WalkBuilder::new(root)
        .hidden(false)
        .filter_entry(|entry| !is_ignored(entry.path()))
        .build()
    {
        let entry = entry?;
        let path = entry.path();
        let Some(metadata) = regular_file_metadata(path) else {
            continue;
        };
        if metadata.len() > MAX_FILE_BYTES || !is_dependency_manifest_path(path) {
            continue;
        }
        let rel = path
            .strip_prefix(root)?
            .to_string_lossy()
            .replace('\\', "/");
        let text = fs::read_to_string(path).unwrap_or_default();
        if !text.contains('\0') {
            manifests.push((rel, text));
        }
    }
    Ok(dependency_hints_from_manifest_texts(
        manifests
            .iter()
            .map(|(path, text)| (path.as_str(), text.as_str())),
    ))
}

fn is_dependency_manifest_path(path: &Path) -> bool {
    matches!(
        path.file_name().and_then(|value| value.to_str()),
        Some("Cargo.toml" | "package.json" | "pyproject.toml" | "go.mod")
    )
}

fn dependency_hint(
    name: impl Into<String>,
    kind: impl Into<String>,
    source: impl Into<String>,
) -> DependencyHint {
    DependencyHint {
        name: name.into(),
        kind: kind.into(),
        source: source.into(),
    }
}

fn dependency_kind_rank(kind: &str) -> usize {
    match kind {
        "dependency" => 0,
        "dev_dependency" => 1,
        "build_dependency" => 2,
        _ => 3,
    }
}

fn cargo_dependency_hints(manifest: &str, source: &str) -> Vec<DependencyHint> {
    let mut hints = Vec::new();
    let mut section = "";
    for line in manifest.lines() {
        let line = line.trim();
        if line.starts_with('[') && line.ends_with(']') {
            section = line.trim_matches(&['[', ']'][..]);
            continue;
        }
        let kind = match section {
            "dependencies" | "workspace.dependencies" => "dependency",
            "dev-dependencies" | "workspace.dev-dependencies" => "dev_dependency",
            "build-dependencies" | "workspace.build-dependencies" => "build_dependency",
            _ => continue,
        };
        let Some((name, _)) = line.split_once('=') else {
            continue;
        };
        let name = name.trim().trim_matches('"').trim_matches('\'');
        if is_dependency_name(name) {
            hints.push(dependency_hint(name, kind, source));
        }
    }
    hints
}

fn package_json_dependency_hints(package_json: &str, source: &str) -> Vec<DependencyHint> {
    let Ok(value) = serde_json::from_str::<serde_json::Value>(package_json) else {
        return Vec::new();
    };
    let mut hints = Vec::new();
    for (field, kind) in [
        ("dependencies", "dependency"),
        ("devDependencies", "dev_dependency"),
        ("peerDependencies", "peer_dependency"),
    ] {
        if let Some(dependencies) = value.get(field).and_then(|value| value.as_object()) {
            hints.extend(
                dependencies
                    .keys()
                    .filter(|name| is_dependency_name(name))
                    .map(|name| dependency_hint(name, kind, source)),
            );
        }
    }
    hints
}

fn pyproject_dependency_hints(manifest: &str, source: &str) -> Vec<DependencyHint> {
    let mut hints = Vec::new();
    let mut section = "";
    let mut in_dependencies_array = false;
    for line in manifest.lines() {
        let line = line.trim();
        if line.starts_with('[') && line.ends_with(']') {
            section = line.trim_matches(&['[', ']'][..]);
            in_dependencies_array = false;
            continue;
        }
        if section == "project" && line.starts_with("dependencies") {
            in_dependencies_array = line.contains('[') && !line.contains(']');
            hints.extend(quoted_dependency_names(line).map(|name| {
                dependency_hint(normalize_python_requirement(&name), "dependency", source)
            }));
            continue;
        }
        if in_dependencies_array {
            hints.extend(quoted_dependency_names(line).map(|name| {
                dependency_hint(normalize_python_requirement(&name), "dependency", source)
            }));
            if line.contains(']') {
                in_dependencies_array = false;
            }
            continue;
        }
        if section == "tool.poetry.dependencies" || section == "tool.poetry.group.dev.dependencies"
        {
            let kind = if section.contains(".dev.") {
                "dev_dependency"
            } else {
                "dependency"
            };
            let Some((name, _)) = line.split_once('=') else {
                continue;
            };
            let name = name.trim().trim_matches('"').trim_matches('\'');
            if is_dependency_name(name) && name != "python" {
                hints.push(dependency_hint(name, kind, source));
            }
        }
    }
    hints
}

fn quoted_dependency_names(line: &str) -> impl Iterator<Item = String> + '_ {
    static QUOTED_RE: LazyLock<Regex> =
        LazyLock::new(|| Regex::new(r#""([^"]+)"|'([^']+)'"#).unwrap());
    QUOTED_RE.captures_iter(line).filter_map(|capture| {
        capture
            .get(1)
            .or_else(|| capture.get(2))
            .map(|value| value.as_str().to_string())
    })
}

fn normalize_python_requirement(requirement: &str) -> String {
    requirement
        .split(|ch: char| {
            ch == '<'
                || ch == '>'
                || ch == '='
                || ch == '~'
                || ch == '!'
                || ch == '['
                || ch.is_whitespace()
        })
        .next()
        .unwrap_or(requirement)
        .to_string()
}

fn go_mod_dependency_hints(manifest: &str, source: &str) -> Vec<DependencyHint> {
    let mut hints = Vec::new();
    let mut in_require_block = false;
    for line in manifest.lines() {
        let line = line.trim();
        if line.starts_with("require (") {
            in_require_block = true;
            continue;
        }
        if in_require_block && line == ")" {
            in_require_block = false;
            continue;
        }
        let dependency = if in_require_block {
            line.split_whitespace().next()
        } else {
            line.strip_prefix("require ")
                .and_then(|line| line.split_whitespace().next())
        };
        if let Some(name) = dependency.filter(|name| is_dependency_name(name)) {
            hints.push(dependency_hint(name, "dependency", source));
        }
    }
    hints
}

fn is_dependency_name(name: &str) -> bool {
    !name.is_empty()
        && !name.starts_with('#')
        && !name.starts_with("//")
        && name.chars().any(|ch| ch.is_alphanumeric())
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
    if let Some(language) = special_file_language(&file_name) {
        return Some(language.to_string());
    }
    if matches!(file_name.as_ref(), "README" | "Makefile") {
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
        "xml" => "xml",
        "gradle" => "gradle",
        _ => return None,
    };
    Some(language.to_string())
}

fn special_file_language(file_name: &str) -> Option<&'static str> {
    match file_name {
        "Cargo.lock" => Some("toml"),
        "Dockerfile" => Some("dockerfile"),
        "Gemfile" => Some("ruby"),
        "Justfile" => Some("justfile"),
        "go.mod" | "go.sum" => Some("go-mod"),
        "pom.xml" => Some("xml"),
        "build.gradle" | "settings.gradle" => Some("gradle"),
        "yarn.lock" | "bun.lock" | "bun.lockb" => Some("text"),
        _ => None,
    }
}

pub(crate) fn is_source_code_language(language: &str) -> bool {
    matches!(
        normalize_language_filter(language).as_str(),
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

pub fn normalize_language_filter(value: &str) -> String {
    match value.trim().to_ascii_lowercase().as_str() {
        "py" | "python" => "python".to_string(),
        "rs" | "rust" => "rust".to_string(),
        "js" | "jsx" | "javascript" => "javascript".to_string(),
        "ts" | "tsx" | "typescript" => "typescript".to_string(),
        "go" | "golang" => "go".to_string(),
        "rb" | "ruby" => "ruby".to_string(),
        "kt" | "kotlin" => "kotlin".to_string(),
        "md" | "markdown" => "markdown".to_string(),
        "yml" | "yaml" => "yaml".to_string(),
        "xml" => "xml".to_string(),
        "gradle" => "gradle".to_string(),
        "docker" | "dockerfile" => "dockerfile".to_string(),
        "just" | "justfile" => "justfile".to_string(),
        "gomod" | "go-mod" | "go.mod" => "go-mod".to_string(),
        "txt" | "text" => "text".to_string(),
        other => other.to_string(),
    }
}

pub(crate) fn extract_symbols(path: &str, text: &str, language: &str) -> Vec<Symbol> {
    if language == "python" {
        return extract_python_symbols(path, text);
    }
    if language == "go" {
        return extract_go_symbols(path, text);
    }
    if language == "ruby" {
        return extract_ruby_symbols(path, text);
    }
    if language == "kotlin" {
        return extract_kotlin_symbols(path, text);
    }
    if language == "swift" {
        return extract_swift_symbols(path, text);
    }
    if language == "java" {
        return extract_java_symbols(path, text);
    }
    if !language_supports_generic_symbols(language) {
        return Vec::new();
    }
    text.lines()
        .enumerate()
        .filter_map(|(index, line)| {
            let capture = SYMBOL_RE.captures(line)?;
            Some(Symbol {
                name: capture.get(2)?.as_str().to_string(),
                kind: generic_symbol_kind(capture.get(1)?.as_str(), line).to_string(),
                path: path.to_string(),
                line: index + 1,
            })
        })
        .collect()
}

fn language_supports_generic_symbols(language: &str) -> bool {
    matches!(language, "rust" | "javascript" | "typescript")
}

fn generic_symbol_kind(keyword: &str, line: &str) -> &'static str {
    match keyword {
        "class" => "class",
        "interface" => "interface",
        "struct" => "struct",
        "enum" => "enum",
        "trait" => "trait",
        "type" => "type",
        "const" | "let" | "var" if line.contains("=>") || line.contains("function") => "function",
        "const" => "const",
        "let" => "let",
        "var" => "var",
        _ => "function",
    }
}

fn extract_ruby_symbols(path: &str, text: &str) -> Vec<Symbol> {
    text.lines()
        .enumerate()
        .filter_map(|(index, line)| {
            let capture = RUBY_SYMBOL_RE.captures(line)?;
            let raw_kind = capture.get(1)?.as_str();
            Some(Symbol {
                name: capture.get(2)?.as_str().to_string(),
                kind: if raw_kind == "def" {
                    "function"
                } else {
                    "class"
                }
                .to_string(),
                path: path.to_string(),
                line: index + 1,
            })
        })
        .collect()
}

fn extract_kotlin_symbols(path: &str, text: &str) -> Vec<Symbol> {
    text.lines()
        .enumerate()
        .filter_map(|(index, line)| {
            if let Some(capture) = KOTLIN_FUNC_SYMBOL_RE.captures(line) {
                return Some(Symbol {
                    name: capture.get(1)?.as_str().to_string(),
                    kind: "function".to_string(),
                    path: path.to_string(),
                    line: index + 1,
                });
            }
            let capture = KOTLIN_TYPE_SYMBOL_RE.captures(line)?;
            Some(Symbol {
                name: capture.get(3)?.as_str().to_string(),
                kind: kotlin_type_kind(
                    capture.get(1).map(|value| value.as_str()),
                    capture.get(2)?.as_str(),
                )
                .to_string(),
                path: path.to_string(),
                line: index + 1,
            })
        })
        .collect()
}

fn kotlin_type_kind(prefix: Option<&str>, kind: &str) -> &'static str {
    match (prefix, kind) {
        (Some("enum"), "class") => "enum",
        (_, "interface") => "interface",
        (_, "object") => "class",
        _ => "class",
    }
}

fn extract_swift_symbols(path: &str, text: &str) -> Vec<Symbol> {
    text.lines()
        .enumerate()
        .filter_map(|(index, line)| {
            if let Some(capture) = SWIFT_FUNC_SYMBOL_RE.captures(line) {
                return Some(Symbol {
                    name: capture.get(1)?.as_str().to_string(),
                    kind: "function".to_string(),
                    path: path.to_string(),
                    line: index + 1,
                });
            }
            let capture = SWIFT_TYPE_SYMBOL_RE.captures(line)?;
            Some(Symbol {
                name: capture.get(2)?.as_str().to_string(),
                kind: swift_type_kind(capture.get(1)?.as_str()).to_string(),
                path: path.to_string(),
                line: index + 1,
            })
        })
        .collect()
}

fn swift_type_kind(kind: &str) -> &'static str {
    match kind {
        "protocol" => "interface",
        "class" => "class",
        "struct" => "struct",
        "enum" => "enum",
        _ => "class",
    }
}

fn extract_java_symbols(path: &str, text: &str) -> Vec<Symbol> {
    let mut symbols = Vec::new();
    for (index, line) in text.lines().enumerate() {
        if let Some(capture) = SYMBOL_RE.captures(line) {
            symbols.push(Symbol {
                name: capture
                    .get(2)
                    .map(|value| value.as_str())
                    .unwrap_or_default()
                    .to_string(),
                kind: generic_symbol_kind(
                    capture
                        .get(1)
                        .map(|value| value.as_str())
                        .unwrap_or_default(),
                    line,
                )
                .to_string(),
                path: path.to_string(),
                line: index + 1,
            });
            continue;
        }
        if let Some(capture) = JAVA_METHOD_SYMBOL_RE.captures(line) {
            let Some(name) = capture.get(1).map(|value| value.as_str()) else {
                continue;
            };
            if !matches!(name, "if" | "for" | "while" | "switch" | "catch") {
                symbols.push(Symbol {
                    name: name.to_string(),
                    kind: "function".to_string(),
                    path: path.to_string(),
                    line: index + 1,
                });
            }
        }
    }
    symbols
}

fn extract_go_symbols(path: &str, text: &str) -> Vec<Symbol> {
    text.lines()
        .enumerate()
        .filter_map(|(index, line)| {
            if let Some(capture) = GO_FUNC_SYMBOL_RE.captures(line) {
                return Some(Symbol {
                    name: capture.get(1)?.as_str().to_string(),
                    kind: "function".to_string(),
                    path: path.to_string(),
                    line: index + 1,
                });
            }
            let capture = GO_TYPE_SYMBOL_RE.captures(line)?;
            Some(Symbol {
                name: capture.get(1)?.as_str().to_string(),
                kind: capture.get(2)?.as_str().to_string(),
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
    demote_generated: bool,
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
    if demote_generated && is_generated_path(&path_lower) {
        score *= GENERATED_PATH_SCORE_MULTIPLIER;
        if explain {
            signals.push(rank_signal(
                "generated_path_penalty",
                path,
                -1.0 + GENERATED_PATH_SCORE_MULTIPLIER,
            ));
        }
    }

    Some(SearchResult {
        path: path.to_string(),
        score: round4(score),
        reason: format!("matched {}", reasons.join(", ")),
        snippet: best_snippet_for_path_with_phrases(
            path,
            text,
            query_tokens,
            query_phrases,
            snippet_mode,
        ),
        line_range: None,
        match_lines: ranked_match_lines_from_text(path, text, query_tokens, query_phrases, 16),
        explanation: explain.then_some(signals),
        query_plan: None,
        duplicate_group: None,
        context: None,
        read_range: None,
        read_request: None,
        related_request: None,
        related_symbols_request: None,
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
    let line_range = result.line_range.as_ref();
    let anchor = result
        .match_lines
        .iter()
        .copied()
        .find(|line| {
            line_range.is_none_or(|range| *line >= range.start_line && *line <= range.end_line)
        })
        .or_else(|| line_range.map(|range| range.start_line))
        .or_else(|| result.match_lines.first().copied())
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
    if reasons.iter().any(|reason| {
        reason
            .strip_prefix("symbol:")
            .is_some_and(|existing| normalize_token(existing) == normalized)
    }) {
        return;
    }
    let symbol_tokens = tokenize(symbol_name);
    let Some((kind, amount)) =
        symbol_query_match_score(&normalized, &symbol_tokens, query_tokens, query_name)
    else {
        return;
    };
    *score += amount;
    reasons.push(format!("symbol:{symbol_name}"));
    signals.push(rank_signal(kind, symbol_name, amount));
}

pub(crate) fn symbol_query_match_score(
    symbol_normalized: &str,
    symbol_tokens: &[String],
    query_tokens: &[String],
    query_name: &str,
) -> Option<(&'static str, f64)> {
    if symbol_normalized == query_name
        || (query_tokens.len() == 1 && query_tokens[0] == symbol_normalized)
        || (symbol_tokens.len() > 1 && query_tokens.iter().any(|token| token == symbol_normalized))
    {
        return Some(("symbol_exact", 44.0));
    }
    if query_tokens.len() > 1
        && (symbol_normalized.starts_with(query_name) || symbol_normalized.ends_with(query_name))
    {
        return Some(("symbol_boundary_contains", 36.0));
    }
    if query_tokens.len() > 1 && symbol_normalized.contains(query_name) {
        return Some(("symbol_contains", 12.0));
    }
    let overlap = symbol_tokens
        .iter()
        .filter(|token| query_tokens.contains(token))
        .count();
    let min_overlap = if query_tokens.len() > 1 { 2 } else { 1 };
    (overlap >= min_overlap).then_some(("symbol_overlap", 4.0 * overlap as f64))
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

pub(crate) fn unique_query_tokens(text: &str) -> Vec<String> {
    let mut seen = HashSet::new();
    tokenize(text)
        .into_iter()
        .filter(|token| seen.insert(token.clone()))
        .collect()
}

pub(crate) fn normalize_token(text: &str) -> String {
    tokenize(text).join("")
}

pub(crate) fn related_query_terms_symbol_and_filters(
    query: Option<&str>,
) -> (Vec<String>, String, SearchFilters) {
    let Some(query) = query else {
        return (Vec::new(), String::new(), SearchFilters::default());
    };
    let parsed = parse_query(query);
    let text = query_text(&parsed.terms, &parsed.filters);
    let mut tokens = tokenize(&text);
    tokens.sort();
    tokens.dedup();
    let symbol = normalize_token(&text);
    (tokens, symbol, parsed.filters)
}

pub(crate) fn symbol_matches_related_filters(
    name: &str,
    kind: &str,
    filters: &SearchFilters,
) -> bool {
    let normalized_name = normalize_token(name);
    if let Some(wanted) = &filters.symbol {
        let wanted = normalize_token(wanted);
        if wanted.is_empty() || normalized_name != wanted {
            return false;
        }
    }
    if filters
        .exclude_symbol
        .iter()
        .any(|excluded| normalize_token(excluded) == normalized_name)
    {
        return false;
    }
    if let Some(wanted) = &filters.symbol_kind {
        if !kind.eq_ignore_ascii_case(wanted) {
            return false;
        }
    }
    !filters
        .exclude_symbol_kind
        .iter()
        .any(|excluded| kind.eq_ignore_ascii_case(excluded))
}

pub(crate) fn contains_ascii_case_insensitive(haystack: &str, needle: &str) -> bool {
    let needle = needle.as_bytes();
    if needle.is_empty() || needle.len() > haystack.len() {
        return false;
    }
    haystack
        .as_bytes()
        .windows(needle.len())
        .any(|window| window.eq_ignore_ascii_case(needle))
}

pub(crate) fn referenced_symbol_name<'a>(
    text: &str,
    symbols: &'a [(String, String)],
) -> Option<&'a str> {
    if symbols.len() > SYMBOL_REFERENCE_LOWERCASE_THRESHOLD {
        let text_lower = text.to_ascii_lowercase();
        return symbols
            .iter()
            .find(|(_, symbol_lower)| text_lower.contains(symbol_lower))
            .map(|(symbol, _)| symbol.as_str());
    }
    symbols
        .iter()
        .find(|(symbol, _)| contains_ascii_case_insensitive(text, symbol))
        .map(|(symbol, _)| symbol.as_str())
}

pub(crate) fn query_token_overlap(
    query_tokens: &ahash::AHashSet<String>,
    symbol_tokens: &[String],
    path_tokens: &[String],
) -> usize {
    query_tokens
        .iter()
        .filter(|token| {
            symbol_tokens.iter().any(|candidate| candidate == *token)
                || path_tokens.iter().any(|candidate| candidate == *token)
        })
        .count()
}

pub(crate) fn related_stem_terms(stem: &str) -> Vec<String> {
    let lower = stem.to_ascii_lowercase();
    let mut terms = Vec::new();
    push_related_stem_term(&mut terms, lower.as_str());

    for prefix in [
        "test_", "tests_", "spec_", "specs_", "test.", "tests.", "spec.", "specs.", "test-",
        "tests-", "spec-", "specs-",
    ] {
        if let Some(stripped) = lower.strip_prefix(prefix) {
            push_related_stem_term(&mut terms, stripped);
        }
    }
    for suffix in [
        "_test", "_tests", "_spec", "_specs", ".test", ".tests", ".spec", ".specs", "-test",
        "-tests", "-spec", "-specs",
    ] {
        if let Some(stripped) = lower.strip_suffix(suffix) {
            push_related_stem_term(&mut terms, stripped);
        }
    }

    terms
}

fn push_related_stem_term(terms: &mut Vec<String>, value: &str) {
    let value = value.trim_matches(|ch: char| !ch.is_ascii_alphanumeric());
    if value.len() > 1 && !matches!(value, "test" | "tests" | "spec" | "specs") {
        let value = value.to_string();
        if !terms.contains(&value) {
            terms.push(value);
        }
    }
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
    best_snippet_for_path_with_phrases(path, text, query_tokens, &[], mode)
}

pub(crate) fn best_snippet_for_path_with_phrases(
    path: &str,
    text: &str,
    query_tokens: &[String],
    query_phrases: &[String],
    mode: SnippetMode,
) -> String {
    let lines = text.lines().collect::<Vec<_>>();
    if let Some((line, _)) = line_scores_for_path(path, text, &lines, query_tokens, query_phrases)
        .into_iter()
        .max_by_key(|(line, score)| (*score, std::cmp::Reverse(*line)))
    {
        return format_snippet_window(&lines, line.saturating_sub(1), mode);
    }
    let (_, after) = mode.window();
    format_numbered_lines(&lines, 0, lines.len().min(after + 1))
        .chars()
        .take(mode.max_chars())
        .collect()
}

pub(crate) fn best_snippet_at_line(text: &str, line: usize, mode: SnippetMode) -> String {
    let lines = text.lines().collect::<Vec<_>>();
    if lines.is_empty() {
        return String::new();
    }
    let center = line.saturating_sub(1).min(lines.len().saturating_sub(1));
    format_snippet_window(&lines, center, mode)
}

fn line_scores_for_path(
    path: &str,
    text: &str,
    lines: &[&str],
    query_tokens: &[String],
    query_phrases: &[String],
) -> HashMap<usize, usize> {
    let mut scores = HashMap::<usize, usize>::new();
    for (idx, line) in lines.iter().enumerate() {
        let line_lower = line.to_lowercase();
        let phrase_line = normalize_phrase_text(line);
        let mut score = 0usize;
        for token in query_tokens {
            if line_lower.contains(token) {
                score += 1;
            }
        }
        for phrase in query_phrases {
            if phrase_line.contains(phrase) {
                score += 100;
            }
        }
        if score > 0 {
            scores.insert(idx + 1, score);
        }
    }

    let language = language_for(Path::new(path)).unwrap_or_else(|| "text".to_string());
    let query_name = query_tokens.join("");
    for symbol in extract_symbols(path, text, &language) {
        let normalized = normalize_token(&symbol.name);
        let tokens = tokenize(&symbol.name);
        if let Some((kind, amount)) =
            symbol_query_match_score(&normalized, &tokens, query_tokens, &query_name)
        {
            let bonus = if kind == "symbol_exact" { 250 } else { 150 };
            let exact_phrase_bonus =
                symbol_exact_phrase_bonus(&symbol.name, query_phrases).unwrap_or(0);
            *scores.entry(symbol.line).or_insert(0) += bonus + exact_phrase_bonus + amount as usize;
        }
    }

    scores
}

pub(crate) fn symbol_exact_phrase_bonus(
    symbol_name: &str,
    query_phrases: &[String],
) -> Option<usize> {
    let symbol_phrase = normalize_phrase_text(symbol_name);
    query_phrases
        .iter()
        .any(|phrase| phrase == &symbol_phrase)
        .then_some(200)
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
    let normalized_path;
    let path = if path
        .bytes()
        .any(|byte| byte == b'\\' || byte.is_ascii_uppercase())
    {
        normalized_path = path.replace('\\', "/").to_ascii_lowercase();
        normalized_path.as_str()
    } else {
        path
    };

    let mut file_name = path;
    for part in path.split('/').filter(|part| !part.is_empty()) {
        if matches!(part, "test" | "tests" | "__tests__" | "spec" | "specs") {
            return true;
        }
        file_name = part;
    }

    if file_name.starts_with("test_")
        || file_name.starts_with("tests_")
        || file_name.starts_with("test-")
        || file_name.starts_with("tests-")
        || file_name.starts_with("spec_")
        || file_name.starts_with("specs_")
        || file_name.starts_with("spec-")
        || file_name.starts_with("specs-")
    {
        return true;
    }

    let stem = file_name
        .rsplit_once('.')
        .map(|(stem, _)| stem)
        .unwrap_or(file_name);
    [
        "_test", "_tests", "_spec", "_specs", ".test", ".tests", ".spec", ".specs", "-test",
        "-tests", "-spec", "-specs",
    ]
    .iter()
    .any(|suffix| stem.ends_with(suffix))
}

pub(crate) fn is_generated_path(path: &str) -> bool {
    let normalized_path;
    let path = if path
        .bytes()
        .any(|byte| byte == b'\\' || byte.is_ascii_uppercase())
    {
        normalized_path = path.replace('\\', "/").to_ascii_lowercase();
        normalized_path.as_str()
    } else {
        path
    };

    let mut file_name = path;
    let mut in_bundle_dir = false;
    for part in path.split('/').filter(|part| !part.is_empty()) {
        if is_generated_directory_segment(part) {
            return true;
        }
        if GENERATED_BUNDLE_DIR_SEGMENTS.contains(&part) {
            in_bundle_dir = true;
        }
        file_name = part;
    }

    is_generated_file_name(file_name) || (in_bundle_dir && is_generated_bundle_asset(file_name))
}

fn is_generated_directory_segment(segment: &str) -> bool {
    GENERATED_DIRECTORY_SEGMENTS.contains(&segment)
}

fn is_generated_file_name(file_name: &str) -> bool {
    let stem = file_name
        .rsplit_once('.')
        .map(|(stem, _)| stem)
        .unwrap_or(file_name);
    stem == "generated"
        || GENERATED_FILE_STEM_PREFIXES
            .iter()
            .any(|prefix| stem.starts_with(prefix))
        || GENERATED_FILE_STEM_SUFFIXES
            .iter()
            .any(|suffix| stem.ends_with(suffix))
        || GENERATED_FILE_NAME_SUFFIXES
            .iter()
            .any(|suffix| file_name.ends_with(suffix))
}

fn is_generated_bundle_asset(file_name: &str) -> bool {
    let Some(stem) = file_name.strip_suffix(".js") else {
        return false;
    };
    let parts = stem.split('-').collect::<Vec<_>>();
    if parts.len() < 2 {
        return false;
    };

    for split_at in 1..parts.len() {
        let hash_parts = &parts[split_at..];
        if hash_parts.iter().all(|part| is_bundle_hash_segment(part))
            && hash_parts
                .iter()
                .any(|part| part.bytes().any(|byte| byte.is_ascii_digit()))
        {
            return true;
        }
    }
    false
}

fn is_bundle_hash_segment(value: &str) -> bool {
    value.len() >= 6
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || byte == b'_')
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
            | "Cargo.lock"
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
    matches!(
        path,
        "AGENTS.md" | "CLAUDE.md" | "README.md" | "Dockerfile" | "Justfile" | "Makefile"
    ) || is_manifest_file(path)
}

pub(crate) fn symbol_kind_rank(kind: &str) -> usize {
    match kind {
        "class" | "struct" | "enum" | "interface" | "trait" | "type" => 0,
        "function" => 1,
        _ => 2,
    }
}

pub(crate) fn select_repo_map_top_symbols(mut symbols: Vec<Symbol>, limit: usize) -> Vec<Symbol> {
    if limit == 0 {
        return Vec::new();
    }
    symbols.sort_by(|a, b| {
        symbol_kind_rank(&a.kind)
            .cmp(&symbol_kind_rank(&b.kind))
            .then_with(|| a.path.cmp(&b.path))
            .then_with(|| a.line.cmp(&b.line))
            .then_with(|| a.name.cmp(&b.name))
    });

    let per_path_cap = if limit <= 4 {
        limit
    } else {
        limit.saturating_add(3) / 4
    };
    let mut selected = Vec::with_capacity(limit.min(symbols.len()));
    let mut path_counts = HashMap::<String, usize>::new();
    for symbol in &symbols {
        let count = path_counts.entry(symbol.path.clone()).or_default();
        if *count < per_path_cap {
            selected.push(symbol.clone());
            *count += 1;
        }
        if selected.len() == limit {
            return selected;
        }
    }

    for symbol in symbols {
        if selected.iter().any(|selected| {
            selected.path == symbol.path
                && selected.line == symbol.line
                && selected.name == symbol.name
        }) {
            continue;
        }
        selected.push(symbol);
        if selected.len() == limit {
            break;
        }
    }
    selected
}

pub(crate) fn round4(value: f64) -> f64 {
    (value * 10_000.0).round() / 10_000.0
}

pub fn capped_search_limit(limit: usize) -> usize {
    limit.min(MAX_SEARCH_RESULTS)
}

pub fn attach_result_read_requests(
    results: &mut [SearchResult],
    tool: &str,
    base_arguments: serde_json::Map<String, serde_json::Value>,
) {
    for result in results {
        let Some(read_range) = &result.read_range else {
            continue;
        };
        result.read_request = Some(read_request_from_range(tool, &base_arguments, read_range));
    }
}

pub fn symbol_lookup_results(
    symbols: Vec<Symbol>,
    tool: &str,
    base_arguments: serde_json::Map<String, serde_json::Value>,
) -> Vec<SymbolLookupResult> {
    symbols
        .into_iter()
        .map(|symbol| {
            let read_range = symbol_read_range(&symbol);
            let read_request = read_request_from_range(tool, &base_arguments, &read_range);
            SymbolLookupResult {
                symbol,
                read_range,
                read_request,
            }
        })
        .collect()
}

pub fn symbol_lookup_read_batch_request(
    symbols: &[SymbolLookupResult],
    tool: &str,
    base_arguments: serde_json::Map<String, serde_json::Value>,
) -> Option<ResultToolRequest> {
    let ranges = symbols
        .iter()
        .map(|symbol| symbol.read_range.clone())
        .collect::<Vec<_>>();
    read_batch_request_from_ranges(ranges, tool, base_arguments)
}

pub fn related_file_lookup_results(
    related: Vec<RelatedFile>,
    tool: &str,
    base_arguments: serde_json::Map<String, serde_json::Value>,
) -> Vec<RelatedFileLookupResult> {
    related
        .into_iter()
        .map(|related| {
            let read_range = related_file_read_range(&related);
            let read_request = read_request_from_range(tool, &base_arguments, &read_range);
            RelatedFileLookupResult {
                path: related.path,
                reason: related.reason,
                score: related.score,
                read_range,
                read_request,
            }
        })
        .collect()
}

pub fn related_symbol_lookup_results(
    related: Vec<RelatedSymbol>,
    tool: &str,
    base_arguments: serde_json::Map<String, serde_json::Value>,
) -> Vec<RelatedSymbolLookupResult> {
    related
        .into_iter()
        .map(|related| {
            let read_range = symbol_read_range(&related.symbol);
            let read_request = read_request_from_range(tool, &base_arguments, &read_range);
            RelatedSymbolLookupResult {
                symbol: related.symbol,
                reason: related.reason,
                score: related.score,
                read_range,
                read_request,
            }
        })
        .collect()
}

pub fn attach_repo_map_read_batch_request(
    map: &mut RepoMap,
    tool: &str,
    base_arguments: serde_json::Map<String, serde_json::Value>,
) {
    attach_repo_map_read_batch_request_with_limit(
        map,
        tool,
        base_arguments,
        DEFAULT_REPO_MAP_READ_BATCH_RANGES,
    );
}

pub fn attach_repo_map_read_batch_request_with_limit(
    map: &mut RepoMap,
    tool: &str,
    base_arguments: serde_json::Map<String, serde_json::Value>,
    read_limit: usize,
) {
    map.read_batch_request = read_batch_request_from_ranges_with_limit(
        repo_map_read_ranges(map),
        tool,
        base_arguments,
        read_limit,
    );
}

fn read_request_from_range(
    tool: &str,
    base_arguments: &serde_json::Map<String, serde_json::Value>,
    read_range: &ResultReadRange,
) -> ResultToolRequest {
    let mut arguments = base_arguments.clone();
    arguments.insert(
        "path".to_string(),
        serde_json::json!(read_range.path.clone()),
    );
    arguments.insert("start".to_string(), serde_json::json!(read_range.start));
    arguments.insert("lines".to_string(), serde_json::json!(read_range.lines));
    if let Some(scope) = read_range.scope {
        arguments.insert("scope".to_string(), serde_json::json!(scope));
    }
    ResultReadRequest::new(tool.to_string(), serde_json::Value::Object(arguments))
}

fn read_batch_request_from_ranges(
    ranges: Vec<ResultReadRange>,
    tool: &str,
    base_arguments: serde_json::Map<String, serde_json::Value>,
) -> Option<ResultToolRequest> {
    read_batch_request_from_ranges_with_limit(
        ranges,
        tool,
        base_arguments,
        MAX_RESULT_READ_BATCH_RANGES,
    )
}

fn read_batch_request_from_ranges_with_limit(
    read_ranges: Vec<ResultReadRange>,
    tool: &str,
    mut base_arguments: serde_json::Map<String, serde_json::Value>,
    read_limit: usize,
) -> Option<ResultToolRequest> {
    let limit = read_limit.min(MAX_RESULT_READ_BATCH_RANGES);
    let mut seen = HashSet::new();
    let mut ranges = Vec::new();
    for read_range in read_ranges {
        if ranges.len() >= limit {
            break;
        }
        let key = (
            read_range.path.clone(),
            read_range.start,
            read_range.lines,
            read_range.scope,
        );
        if !seen.insert(key) {
            continue;
        }
        ranges.push(read_range);
    }
    if ranges.is_empty() {
        return None;
    }
    let common_scope = common_read_scope(&ranges);
    if let Some(scope) = common_scope {
        base_arguments.insert("scope".to_string(), serde_json::json!(scope));
    }
    let range_values = ranges
        .into_iter()
        .map(|read_range| {
            let mut value = serde_json::json!({
                "path": read_range.path,
                "start": read_range.start,
                "lines": read_range.lines
            });
            if common_scope.is_none()
                && let Some(scope) = read_range.scope
            {
                value["scope"] = serde_json::json!(scope);
            }
            value
        })
        .collect::<Vec<_>>();
    base_arguments.insert("ranges".to_string(), serde_json::Value::Array(range_values));
    Some(ResultToolRequest::new(
        tool.to_string(),
        serde_json::Value::Object(base_arguments),
    ))
}

fn common_read_scope(ranges: &[ResultReadRange]) -> Option<RangeScope> {
    let first = ranges.first()?.scope?;
    ranges
        .iter()
        .all(|range| range.scope == Some(first))
        .then_some(first)
}

pub fn result_read_batch_request(
    results: &[SearchResult],
    tool: &str,
    base_arguments: serde_json::Map<String, serde_json::Value>,
) -> Option<ResultToolRequest> {
    let ranges = results
        .iter()
        .filter_map(|result| result.read_range.as_ref())
        .cloned()
        .collect::<Vec<_>>();
    read_batch_request_from_ranges(ranges, tool, base_arguments)
}

pub fn attach_result_related_requests(
    results: &mut [SearchResult],
    tool: &str,
    base_arguments: serde_json::Map<String, serde_json::Value>,
    filters: Option<&SearchFilters>,
) {
    for result in results {
        let mut arguments = base_arguments.clone();
        if let Some(filters) = filters {
            append_related_filter_arguments(&mut arguments, filters);
        }
        arguments.insert("path".to_string(), serde_json::json!(result.path.clone()));
        result.related_request = Some(ResultToolRequest::new(
            tool.to_string(),
            serde_json::Value::Object(arguments),
        ));
    }
}

fn append_related_filter_arguments(
    arguments: &mut serde_json::Map<String, serde_json::Value>,
    filters: &SearchFilters,
) {
    insert_optional_string(arguments, "file", filters.file.as_deref());
    insert_optional_string(arguments, "language", filters.language.as_deref());
    insert_optional_string(arguments, "extension", filters.extension.as_deref());
    insert_optional_string(arguments, "symbol", filters.symbol.as_deref());
    insert_optional_string(arguments, "symbol_kind", filters.symbol_kind.as_deref());
    insert_optional_string(arguments, "repo_filter", filters.repo.as_deref());
    insert_optional_string(arguments, "branch", filters.branch.as_deref());
    insert_optional_string(arguments, "origin", filters.origin.as_deref());
    insert_optional_string(arguments, "dependency", filters.dependency.as_deref());
    insert_optional_string(arguments, "import", filters.import.as_deref());
    insert_optional_bool(arguments, "test", filters.test);
    insert_optional_bool(arguments, "generated", filters.generated);
    insert_optional_bool(arguments, "code", filters.code);
    insert_string_array(arguments, "exclude_file", &filters.exclude_file);
    insert_string_array(arguments, "exclude_path", &filters.exclude_path);
    insert_string_array(arguments, "exclude_language", &filters.exclude_language);
    insert_string_array(arguments, "exclude_extension", &filters.exclude_extension);
    insert_string_array(arguments, "exclude_symbol", &filters.exclude_symbol);
    insert_string_array(
        arguments,
        "exclude_symbol_kind",
        &filters.exclude_symbol_kind,
    );
    insert_string_array(arguments, "exclude_repo", &filters.exclude_repo);
    insert_string_array(arguments, "exclude_branch", &filters.exclude_branch);
    insert_string_array(arguments, "exclude_origin", &filters.exclude_origin);
    insert_string_array(arguments, "exclude_dependency", &filters.exclude_dependency);
    insert_string_array(arguments, "exclude_import", &filters.exclude_import);
    insert_string_array(arguments, "exclude_content", &filters.exclude_content);
}

fn insert_optional_string(
    arguments: &mut serde_json::Map<String, serde_json::Value>,
    key: &str,
    value: Option<&str>,
) {
    if let Some(value) = value.filter(|value| !value.trim().is_empty()) {
        arguments.insert(key.to_string(), serde_json::json!(value));
    }
}

fn insert_optional_bool(
    arguments: &mut serde_json::Map<String, serde_json::Value>,
    key: &str,
    value: Option<bool>,
) {
    if let Some(value) = value {
        arguments.insert(key.to_string(), serde_json::json!(value));
    }
}

fn insert_string_array(
    arguments: &mut serde_json::Map<String, serde_json::Value>,
    key: &str,
    values: &[String],
) {
    let values = values
        .iter()
        .filter(|value| !value.trim().is_empty())
        .map(|value| serde_json::json!(value))
        .collect::<Vec<_>>();
    if !values.is_empty() {
        arguments.insert(key.to_string(), serde_json::Value::Array(values));
    }
}

pub fn attach_result_related_symbol_requests(
    results: &mut [SearchResult],
    tool: &str,
    query: Option<&str>,
    base_arguments: serde_json::Map<String, serde_json::Value>,
) {
    for result in results {
        let mut arguments = base_arguments.clone();
        if let Some(query) = query.filter(|query| !query.trim().is_empty()) {
            arguments.insert("query".to_string(), serde_json::json!(query));
        }
        arguments.insert("path".to_string(), serde_json::json!(result.path.clone()));
        result.related_symbols_request = Some(ResultToolRequest::new(
            tool.to_string(),
            serde_json::Value::Object(arguments),
        ));
    }
}

fn cli_command_for_request(tool: &str, arguments: &serde_json::Value) -> Option<String> {
    let args = arguments.as_object()?;
    let read_subcommand = match tool {
        "read_range" | "open_range" => "read-range",
        "read_ranges" | "open_ranges" => "read-ranges",
        "read_index_range" | "open_index_range" => "read-index-range",
        "read_index_ranges" | "open_index_ranges" => "read-index-ranges",
        "read_shard_range" | "open_shard_range" => "read-shard-range",
        "read_shard_ranges" | "open_shard_ranges" => "read-shard-ranges",
        _ => {
            return related_cli_command_for_request(tool, args)
                .or_else(|| repo_map_cli_command_for_request(tool, args))
                .or_else(|| query_plan_cli_command_for_request(tool, args))
                .or_else(|| search_cli_command_for_request(tool, args));
        }
    };
    let mut parts = vec!["orient".to_string(), read_subcommand.to_string()];
    append_target_cli_args(&mut parts, args);
    append_string_cli_arg(&mut parts, args, "scope", "--scope");
    if let Some(ranges) = args.get("ranges").and_then(|value| value.as_array()) {
        for range in ranges {
            let range = range.as_object()?;
            parts.push(compact_range_arg(range)?);
        }
    } else {
        parts.push(compact_range_arg(args)?);
    }
    Some(parts.join(" "))
}

fn repo_map_cli_command_for_request(
    tool: &str,
    args: &serde_json::Map<String, serde_json::Value>,
) -> Option<String> {
    if !matches!(tool, "repo_map" | "indexed_repo_map" | "shard_repo_map") {
        return None;
    }
    let mut parts = vec!["orient".to_string(), "repo-map".to_string()];
    append_target_cli_args(&mut parts, args);
    append_repo_filter_cli_arg(&mut parts, args);
    append_scalar_cli_arg(&mut parts, args, "symbols", "--symbols");
    append_scalar_cli_arg(&mut parts, args, "tests", "--tests");
    append_string_cli_arg(&mut parts, args, "detail", "--detail");
    append_scalar_cli_arg(&mut parts, args, "read_limit", "--read-limit");
    Some(parts.join(" "))
}

fn query_plan_cli_command_for_request(
    tool: &str,
    args: &serde_json::Map<String, serde_json::Value>,
) -> Option<String> {
    if !matches!(
        tool,
        "search_query_plan"
            | "search_plan"
            | "indexed_query_plan"
            | "index_plan"
            | "shard_query_plan"
            | "shard_plan"
    ) {
        return None;
    }
    let query = args.get("query")?.as_str()?;
    let mut parts = vec!["orient".to_string(), "search-plan".to_string()];
    append_target_cli_args(&mut parts, args);
    append_search_filter_cli_args(&mut parts, args);
    parts.push("--query".to_string());
    parts.push(shell_quote(query));
    Some(parts.join(" "))
}

fn search_cli_command_for_request(
    tool: &str,
    args: &serde_json::Map<String, serde_json::Value>,
) -> Option<String> {
    if !matches!(
        tool,
        "search" | "search_code" | "indexed_search" | "indexed_search_code" | "search_shards"
    ) {
        return None;
    }
    let query = args.get("query")?.as_str()?;
    let mut parts = vec!["orient".to_string(), "search".to_string()];
    append_target_cli_args(&mut parts, args);
    append_scalar_cli_arg(&mut parts, args, "limit", "--limit");
    append_search_filter_cli_args(&mut parts, args);
    append_scalar_cli_arg(&mut parts, args, "context_lines", "--context-lines");
    append_bool_cli_arg(&mut parts, args, "diagnose", "--diagnose");
    parts.push("--query".to_string());
    parts.push(shell_quote(query));
    Some(parts.join(" "))
}

fn related_cli_command_for_request(
    tool: &str,
    args: &serde_json::Map<String, serde_json::Value>,
) -> Option<String> {
    let subcommand = match tool {
        "related_files" => "related",
        "related_index_files" => "related-index",
        "related_shard_files" => "related-shard",
        "related_symbols" => "related-symbols",
        "related_index_symbols" => "related-index-symbols",
        "related_shard_symbols" => "related-shard-symbols",
        _ => return None,
    };
    let mut parts = vec!["orient".to_string(), subcommand.to_string()];
    append_target_cli_args(&mut parts, args);
    if let Some(path) = args.get("path").and_then(|value| value.as_str()) {
        parts.push("--path".to_string());
        parts.push(shell_quote(path));
    }
    if let Some(query) = args.get("query").and_then(|value| value.as_str()) {
        parts.push("--query".to_string());
        parts.push(shell_quote(query));
    }
    if let Some(limit) = args.get("limit").and_then(|value| value.as_u64()) {
        parts.push("--limit".to_string());
        parts.push(limit.to_string());
    }
    append_related_filter_cli_args(&mut parts, args);
    Some(parts.join(" "))
}

fn append_related_filter_cli_args(
    parts: &mut Vec<String>,
    args: &serde_json::Map<String, serde_json::Value>,
) {
    append_repo_filter_cli_arg(parts, args);
    append_repeated_string_cli_arg(parts, args, "language", "--language");
    append_repeated_string_cli_arg(parts, args, "extension", "--extension");
    append_repeated_string_cli_arg(parts, args, "file", "--file");
    append_repeated_string_cli_arg(parts, args, "symbol", "--symbol");
    append_repeated_string_cli_arg(parts, args, "symbol_kind", "--kind");
    append_repeated_string_cli_arg(parts, args, "branch", "--branch");
    append_repeated_string_cli_arg(parts, args, "origin", "--origin");
    append_repeated_string_cli_arg(parts, args, "dependency", "--dependency");
    append_repeated_string_cli_arg(parts, args, "import", "--import");
    append_bool_value_cli_arg(parts, args, "test", "--test");
    append_bool_value_cli_arg(parts, args, "generated", "--generated");
    append_bool_value_cli_arg(parts, args, "code", "--code");
    append_repeated_string_cli_arg(parts, args, "exclude_file", "--exclude-file");
    append_repeated_string_cli_arg(parts, args, "exclude_path", "--exclude-path");
    append_repeated_string_cli_arg(parts, args, "exclude_language", "--exclude-language");
    append_repeated_string_cli_arg(parts, args, "exclude_extension", "--exclude-extension");
    append_repeated_string_cli_arg(parts, args, "exclude_symbol", "--exclude-symbol");
    append_repeated_string_cli_arg(parts, args, "exclude_symbol_kind", "--exclude-kind");
    append_repeated_string_cli_arg(parts, args, "exclude_repo", "--exclude-repo");
    append_repeated_string_cli_arg(parts, args, "exclude_branch", "--exclude-branch");
    append_repeated_string_cli_arg(parts, args, "exclude_origin", "--exclude-origin");
    append_repeated_string_cli_arg(parts, args, "exclude_dependency", "--exclude-dependency");
    append_repeated_string_cli_arg(parts, args, "exclude_import", "--exclude-import");
    append_repeated_string_cli_arg(parts, args, "exclude_content", "--exclude-content");
}

fn append_search_filter_cli_args(
    parts: &mut Vec<String>,
    args: &serde_json::Map<String, serde_json::Value>,
) {
    append_repo_filter_cli_arg(parts, args);
    append_repeated_string_cli_arg(parts, args, "path", "--path");
    append_repeated_string_cli_arg(parts, args, "language", "--language");
    append_repeated_string_cli_arg(parts, args, "extension", "--extension");
    append_repeated_string_cli_arg(parts, args, "file", "--file");
    append_repeated_string_cli_arg(parts, args, "symbol", "--symbol");
    append_repeated_string_cli_arg(parts, args, "symbol_kind", "--kind");
    append_repeated_string_cli_arg(parts, args, "branch", "--branch");
    append_repeated_string_cli_arg(parts, args, "origin", "--origin");
    append_repeated_string_cli_arg(parts, args, "dependency", "--dependency");
    append_repeated_string_cli_arg(parts, args, "import", "--import");
    append_bool_value_cli_arg(parts, args, "test", "--test");
    append_bool_value_cli_arg(parts, args, "generated", "--generated");
    append_bool_value_cli_arg(parts, args, "code", "--code");
    append_line_cli_arg(parts, args);
    append_bool_cli_arg(parts, args, "require_all", "--require-all");
    append_bool_cli_arg(parts, args, "any_terms", "--any-terms");
    append_string_cli_arg(parts, args, "snippet", "--snippet");
    append_bool_cli_arg(parts, args, "explain", "--explain");
    append_repeated_string_cli_arg(parts, args, "exclude_file", "--exclude-file");
    append_repeated_string_cli_arg(parts, args, "exclude_path", "--exclude-path");
    append_repeated_string_cli_arg(parts, args, "exclude_language", "--exclude-language");
    append_repeated_string_cli_arg(parts, args, "exclude_extension", "--exclude-extension");
    append_repeated_string_cli_arg(parts, args, "exclude_symbol", "--exclude-symbol");
    append_repeated_string_cli_arg(parts, args, "exclude_symbol_kind", "--exclude-kind");
    append_repeated_string_cli_arg(parts, args, "exclude_repo", "--exclude-repo");
    append_repeated_string_cli_arg(parts, args, "exclude_branch", "--exclude-branch");
    append_repeated_string_cli_arg(parts, args, "exclude_origin", "--exclude-origin");
    append_repeated_string_cli_arg(parts, args, "exclude_dependency", "--exclude-dependency");
    append_repeated_string_cli_arg(parts, args, "exclude_import", "--exclude-import");
    append_repeated_string_cli_arg(parts, args, "exclude_content", "--exclude-content");
    append_bool_cli_arg(parts, args, "refresh_if_stale", "--refresh-if-stale");
}

fn append_target_cli_args(
    parts: &mut Vec<String>,
    args: &serde_json::Map<String, serde_json::Value>,
) {
    if let Some(value) = args.get("index_dir").and_then(|value| value.as_str()) {
        parts.push("--index-dir".to_string());
        parts.push(shell_quote(value));
    } else if let Some(value) = args.get("index").and_then(|value| value.as_str()) {
        parts.push("--index".to_string());
        parts.push(shell_quote(value));
    } else if let Some(value) = args.get("repo").and_then(|value| value.as_str()) {
        parts.push("--repo".to_string());
        parts.push(shell_quote(value));
    }
}

fn append_repo_filter_cli_arg(
    parts: &mut Vec<String>,
    args: &serde_json::Map<String, serde_json::Value>,
) {
    if args.get("index_dir").is_some() {
        if let Some(value) = args
            .get("repo_filter")
            .or_else(|| args.get("repo"))
            .and_then(|value| value.as_str())
        {
            parts.push("--repo-filter".to_string());
            parts.push(shell_quote(value));
        }
    } else {
        append_string_cli_arg(parts, args, "repo_filter", "--repo-filter");
    }
}

fn append_scalar_cli_arg(
    parts: &mut Vec<String>,
    args: &serde_json::Map<String, serde_json::Value>,
    key: &str,
    flag: &str,
) {
    let Some(value) = args.get(key).and_then(scalar_cli_arg_value) else {
        return;
    };
    parts.push(flag.to_string());
    parts.push(shell_quote(&value));
}

fn append_line_cli_arg(parts: &mut Vec<String>, args: &serde_json::Map<String, serde_json::Value>) {
    if args.get("line").and_then(scalar_cli_arg_value).is_some() {
        append_scalar_cli_arg(parts, args, "line", "--line");
    } else {
        append_scalar_cli_arg(parts, args, "target_line", "--line");
    }
}

fn append_string_cli_arg(
    parts: &mut Vec<String>,
    args: &serde_json::Map<String, serde_json::Value>,
    key: &str,
    flag: &str,
) {
    let Some(value) = args.get(key).and_then(|value| value.as_str()) else {
        return;
    };
    parts.push(flag.to_string());
    parts.push(shell_quote(value));
}

fn append_repeated_string_cli_arg(
    parts: &mut Vec<String>,
    args: &serde_json::Map<String, serde_json::Value>,
    key: &str,
    flag: &str,
) {
    let Some(value) = args.get(key) else {
        return;
    };
    if value.is_null() {
        return;
    }
    if let Some(values) = value.as_array() {
        for value in values.iter().filter_map(|value| value.as_str()) {
            parts.push(flag.to_string());
            parts.push(shell_quote(value));
        }
    } else if let Some(value) = value.as_str() {
        parts.push(flag.to_string());
        parts.push(shell_quote(value));
    }
}

fn append_bool_cli_arg(
    parts: &mut Vec<String>,
    args: &serde_json::Map<String, serde_json::Value>,
    key: &str,
    flag: &str,
) {
    if args.get(key).and_then(|value| value.as_bool()) == Some(true) {
        parts.push(flag.to_string());
    }
}

fn append_bool_value_cli_arg(
    parts: &mut Vec<String>,
    args: &serde_json::Map<String, serde_json::Value>,
    key: &str,
    flag: &str,
) {
    let Some(value) = args.get(key).and_then(|value| value.as_bool()) else {
        return;
    };
    parts.push(flag.to_string());
    parts.push(value.to_string());
}

fn scalar_cli_arg_value(value: &serde_json::Value) -> Option<String> {
    if value.is_null() {
        return None;
    }
    value
        .as_str()
        .map(str::to_string)
        .or_else(|| value.as_u64().map(|value| value.to_string()))
        .or_else(|| value.as_i64().map(|value| value.to_string()))
        .or_else(|| value.as_f64().map(|value| value.to_string()))
}

fn compact_range_arg(args: &serde_json::Map<String, serde_json::Value>) -> Option<String> {
    let path = args.get("path")?.as_str()?;
    let start = args.get("start")?.as_u64()?;
    let lines = args.get("lines")?.as_u64()?;
    let scope = args.get("scope").and_then(|value| value.as_str());
    let range = if let Some(scope) = scope {
        format!("{path}:{start}:{lines}:{scope}")
    } else {
        format!("{path}:{start}:{lines}")
    };
    Some(shell_quote(&range))
}

fn shell_quote(value: &str) -> String {
    if !value.is_empty()
        && value
            .bytes()
            .all(|byte| matches!(byte, b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'_' | b'-' | b'.' | b'/' | b':' | b'@' | b'%' | b'+' | b'=' | b','))
    {
        return value.to_string();
    }
    format!("'{}'", value.replace('\'', "'\\''"))
}

pub(crate) fn finalize_results(results: Vec<SearchResult>, limit: usize) -> Vec<SearchResult> {
    finalize_results_with_read_scope(results, limit, None)
}

pub(crate) fn finalize_results_for_filters(
    results: Vec<SearchResult>,
    limit: usize,
    filters: &SearchFilters,
) -> Vec<SearchResult> {
    finalize_results_with_read_scope(results, limit, read_scope_for_filters(filters))
}

fn read_scope_for_filters(filters: &SearchFilters) -> Option<RangeScope> {
    if matches!(filters.snippet, SnippetMode::Symbol)
        || filters.symbol.is_some()
        || filters.symbol_kind.is_some()
    {
        Some(RangeScope::Symbol)
    } else {
        None
    }
}

fn finalize_results_with_read_scope(
    mut results: Vec<SearchResult>,
    limit: usize,
    read_scope: Option<RangeScope>,
) -> Vec<SearchResult> {
    let limit = capped_search_limit(limit);
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
    for result in &mut deduped {
        finalize_result_metadata(result, read_scope);
    }
    deduped
}

fn finalize_result_metadata(result: &mut SearchResult, read_scope: Option<RangeScope>) {
    if let Some(signals) = result.explanation.take() {
        result.explanation = Some(compact_rank_signals(signals));
    }
    if result.line_range.is_none() {
        result.line_range = line_range_from_snippet(&result.snippet);
    }
    compact_match_lines(&mut result.match_lines);
    result.read_range = Some(result_read_range(result, read_scope));
}

fn result_read_range(result: &SearchResult, read_scope: Option<RangeScope>) -> ResultReadRange {
    let start = if read_scope == Some(RangeScope::Symbol) {
        symbol_scope_anchor_line(result)
    } else {
        context_start_line(result, DEFAULT_RESULT_READ_LINES)
    };
    ResultReadRange {
        path: result.path.clone(),
        start,
        lines: DEFAULT_RESULT_READ_LINES,
        scope: read_scope,
    }
}

fn symbol_scope_anchor_line(result: &SearchResult) -> usize {
    result
        .match_lines
        .first()
        .copied()
        .or_else(|| result.line_range.as_ref().map(|range| range.start_line))
        .unwrap_or(1)
}

fn related_file_read_range(related: &RelatedFile) -> ResultReadRange {
    ResultReadRange {
        path: related.path.clone(),
        start: 1,
        lines: DEFAULT_RELATED_FILE_READ_LINES,
        scope: None,
    }
}

fn repo_map_read_ranges(map: &RepoMap) -> Vec<ResultReadRange> {
    let mut ranges = Vec::new();
    let mut seen = HashSet::new();

    for path in map
        .brief
        .important_files
        .iter()
        .chain(map.brief.manifest_files.iter())
        .chain(map.entrypoints.iter())
        .chain(map.test_files.iter())
        .chain(map.related_files.iter().map(|related| &related.source_path))
        .chain(map.related_files.iter().map(|related| &related.path))
        .chain(
            map.related_symbols
                .iter()
                .map(|related| &related.source_path),
        )
    {
        push_repo_map_range(
            &mut ranges,
            &mut seen,
            ResultReadRange {
                path: path.clone(),
                start: 1,
                lines: DEFAULT_RELATED_FILE_READ_LINES,
                scope: None,
            },
        );
    }

    for symbol in map
        .top_symbols
        .iter()
        .chain(map.related_symbols.iter().map(|related| &related.symbol))
    {
        push_repo_map_range(&mut ranges, &mut seen, symbol_read_range(symbol));
    }

    ranges
}

fn push_repo_map_range(
    ranges: &mut Vec<ResultReadRange>,
    seen: &mut HashSet<(String, usize, usize, Option<RangeScope>)>,
    range: ResultReadRange,
) {
    if seen.insert((range.path.clone(), range.start, range.lines, range.scope)) {
        ranges.push(range);
    }
}

fn symbol_read_range(symbol: &Symbol) -> ResultReadRange {
    ResultReadRange {
        path: symbol.path.clone(),
        start: symbol.line,
        lines: DEFAULT_SYMBOL_READ_LINES,
        scope: Some(RangeScope::Symbol),
    }
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

pub(crate) fn ranked_match_lines_from_text(
    path: &str,
    text: &str,
    query_tokens: &[String],
    query_phrases: &[String],
    limit: usize,
) -> Vec<usize> {
    if (query_tokens.is_empty() && query_phrases.is_empty()) || limit == 0 {
        return Vec::new();
    }
    let lines = text.lines().collect::<Vec<_>>();
    let mut lines = line_scores_for_path(path, text, &lines, query_tokens, query_phrases)
        .into_iter()
        .collect::<Vec<_>>();
    lines.sort_by_key(|(line, score)| (std::cmp::Reverse(*score), *line));
    let mut lines = lines.into_iter().map(|(line, _)| line).collect::<Vec<_>>();
    lines.truncate(limit);
    lines
}

fn compact_match_lines(lines: &mut Vec<usize>) {
    let mut seen = HashSet::new();
    lines.retain(|line| seen.insert(*line));
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
    let mut start_line = None;
    let mut end_line = None;
    for line in snippet.lines() {
        let Some(number) = line
            .split_once(':')
            .and_then(|(prefix, _)| prefix.trim().parse::<usize>().ok())
        else {
            continue;
        };
        match (start_line, end_line) {
            (None, _) => {
                start_line = Some(number);
                end_line = Some(number);
            }
            (Some(_), Some(end)) if number == end + 1 => {
                end_line = Some(number);
            }
            (Some(_), Some(_)) => break,
            _ => {}
        }
    }
    Some(ResultLineRange {
        start_line: start_line?,
        end_line: end_line?,
    })
}

pub(crate) fn matches_filters(path: &str, filters: &SearchFilters) -> bool {
    let path_lower = path.to_ascii_lowercase();
    matches_filters_with_path_lower(path, &path_lower, filters)
}

pub(crate) fn matches_filters_with_path_lower(
    path: &str,
    path_lower: &str,
    filters: &SearchFilters,
) -> bool {
    let file_name_lower = Path::new(path)
        .file_name()
        .map(|value| value.to_string_lossy().to_ascii_lowercase())
        .unwrap_or_default();
    let extension_lower = Path::new(path)
        .extension()
        .map(|value| value.to_string_lossy().to_lowercase());
    let language = language_for(Path::new(path));
    matches_filters_with_path_metadata(
        path_lower,
        &file_name_lower,
        extension_lower.as_deref(),
        language.as_deref(),
        filters,
    )
}

pub(crate) fn matches_filters_with_path_metadata(
    path_lower: &str,
    file_name_lower: &str,
    extension_lower: Option<&str>,
    language: Option<&str>,
    filters: &SearchFilters,
) -> bool {
    let matcher = PathFilterMatcher::from_filters(filters);
    matches_filters_with_compiled_path_metadata(
        path_lower,
        file_name_lower,
        extension_lower,
        language,
        &matcher,
    )
}

pub(crate) fn matches_filters_with_compiled_path_metadata(
    path_lower: &str,
    file_name_lower: &str,
    extension_lower: Option<&str>,
    language: Option<&str>,
    matcher: &PathFilterMatcher,
) -> bool {
    matcher.matches(path_lower, file_name_lower, extension_lower, language)
}

pub(crate) fn filter_value_matches(haystack_lower: &str, filter: &str) -> bool {
    FilterPattern::new(filter).matches(haystack_lower)
}

fn normalize_path_filter(filter: &str) -> String {
    strip_leading_current_dir_segments(filter.trim().replace('\\', "/")).to_ascii_lowercase()
}

fn strip_leading_current_dir_segments(mut value: String) -> String {
    while let Some(stripped) = value.strip_prefix("./") {
        value = stripped.to_string();
    }
    value
}

fn normalize_extension_filter(filter: &str) -> String {
    filter.trim().trim_start_matches('.').to_ascii_lowercase()
}

fn wildcard_matches(pattern: &str, haystack: &str) -> bool {
    let pattern = pattern.as_bytes();
    let haystack = haystack.as_bytes();
    let (mut pattern_index, mut haystack_index) = (0, 0);
    let mut star_index = None;
    let mut star_match_index = 0;

    while haystack_index < haystack.len() {
        if pattern_index < pattern.len()
            && (pattern[pattern_index] == b'?'
                || pattern[pattern_index] == haystack[haystack_index])
        {
            pattern_index += 1;
            haystack_index += 1;
        } else if pattern_index < pattern.len() && pattern[pattern_index] == b'*' {
            star_index = Some(pattern_index);
            pattern_index += 1;
            star_match_index = haystack_index;
        } else if let Some(star) = star_index {
            pattern_index = star + 1;
            star_match_index += 1;
            haystack_index = star_match_index;
        } else {
            return false;
        }
    }

    while pattern_index < pattern.len() && pattern[pattern_index] == b'*' {
        pattern_index += 1;
    }

    pattern_index == pattern.len()
}

pub(crate) fn filter_only_query(filters: &SearchFilters) -> bool {
    filters.file.is_some()
        || filters.path.is_some()
        || filters.language.is_some()
        || filters.extension.is_some()
        || filters.symbol_kind.is_some()
        || filters.repo.is_some()
        || filters.branch.is_some()
        || filters.origin.is_some()
        || filters.dependency.is_some()
        || filters.import.is_some()
        || filters.test.is_some()
        || filters.generated.is_some()
        || filters.code.is_some()
}

fn score_filter_only_path_with_lower(
    path: &str,
    path_lower: &str,
    filters: &SearchFilters,
    explain: bool,
) -> Option<FilterOnlyMatch> {
    if !filter_only_query(filters) || !matches_filters_with_path_lower(path, path_lower, filters) {
        return None;
    }

    Some(score_filter_only_path_match(path, filters, explain))
}

pub(crate) fn score_filter_only_path_match(
    path: &str,
    filters: &SearchFilters,
    explain: bool,
) -> FilterOnlyMatch {
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
    if let Some(generated) = filters.generated {
        add_filter_signal(
            "generated_filter",
            if generated { "true" } else { "false" },
            4.0,
            explain,
            &mut score,
            &mut reasons,
            &mut signals,
        );
    }
    if let Some(code) = filters.code {
        add_filter_signal(
            "code_filter",
            if code { "true" } else { "false" },
            4.0,
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
    if let Some(branch) = &filters.branch {
        add_filter_signal(
            "branch_filter",
            branch,
            2.0,
            explain,
            &mut score,
            &mut reasons,
            &mut signals,
        );
    }
    if let Some(origin) = &filters.origin {
        add_filter_signal(
            "origin_filter",
            origin,
            2.0,
            explain,
            &mut score,
            &mut reasons,
            &mut signals,
        );
    }
    if let Some(dependency) = &filters.dependency {
        add_filter_signal(
            "dependency_filter",
            dependency,
            2.0,
            explain,
            &mut score,
            &mut reasons,
            &mut signals,
        );
    }
    if let Some(kind) = &filters.symbol_kind {
        add_filter_signal(
            "symbol_kind_filter",
            kind,
            3.0,
            explain,
            &mut score,
            &mut reasons,
            &mut signals,
        );
    }
    if let Some(import) = &filters.import {
        add_filter_signal(
            "import_filter",
            import,
            2.0,
            explain,
            &mut score,
            &mut reasons,
            &mut signals,
        );
    }
    if let Some(line) = filters.target_line {
        let value = line.to_string();
        add_filter_signal(
            "line_filter",
            &value,
            0.5,
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

    FilterOnlyMatch {
        score: round4(score),
        reasons,
        signals,
    }
}

pub(crate) fn filter_only_search_result(
    path: &str,
    text: &str,
    matched: FilterOnlyMatch,
    filters: &SearchFilters,
) -> SearchResult {
    let snippet_mode = filters.snippet;
    let snippet = filters
        .target_line
        .map(|line| best_snippet_at_line(text, line, snippet_mode))
        .filter(|snippet| !snippet.is_empty())
        .unwrap_or_else(|| best_snippet_for_path(path, text, &[], snippet_mode));
    let match_lines = filters
        .target_line
        .map(|line| vec![line])
        .unwrap_or_default();

    SearchResult {
        path: path.to_string(),
        score: matched.score,
        reason: format!("filter match {}", matched.reasons.join(", ")),
        snippet,
        line_range: None,
        match_lines,
        explanation: filters.explain.then_some(matched.signals),
        query_plan: None,
        duplicate_group: None,
        context: None,
        read_range: None,
        read_request: None,
        related_request: None,
        related_symbols_request: None,
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
    let repo_name = root
        .file_name()
        .map(|value| value.to_string_lossy().to_ascii_lowercase())
        .unwrap_or_else(|| root.display().to_string());
    let repo_root = root.to_string_lossy().to_ascii_lowercase();
    if let Some(filter) = &filters.repo {
        let filter = filter.to_ascii_lowercase();
        if !repo_name.contains(&filter) && !repo_root.contains(&filter) {
            return false;
        }
    }
    if filters.exclude_repo.iter().any(|filter| {
        let filter = filter.to_ascii_lowercase();
        repo_name.contains(&filter) || repo_root.contains(&filter)
    }) {
        return false;
    }

    if filters.branch.is_none()
        && filters.origin.is_none()
        && filters.exclude_branch.is_empty()
        && filters.exclude_origin.is_empty()
    {
        return true;
    }

    let git = git_metadata_for_repo(root, false);
    if let Some(filter) = &filters.branch {
        if !metadata_value_matches(git.branch.as_deref(), filter) {
            return false;
        }
    }
    if let Some(filter) = &filters.origin {
        if !metadata_value_matches(git.origin.as_deref(), filter) {
            return false;
        }
    }
    if filters
        .exclude_branch
        .iter()
        .any(|filter| metadata_value_matches(git.branch.as_deref(), filter))
        || filters
            .exclude_origin
            .iter()
            .any(|filter| metadata_value_matches(git.origin.as_deref(), filter))
    {
        return false;
    }
    true
}

fn metadata_value_matches(value: Option<&str>, filter: &str) -> bool {
    value
        .map(|value| value.to_ascii_lowercase())
        .is_some_and(|value| value.contains(&filter.to_ascii_lowercase()))
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

#[cfg(test)]
mod tests {
    use super::*;

    fn test_result(path: &str, score: f64, snippet: &str) -> SearchResult {
        SearchResult {
            path: path.to_string(),
            score,
            reason: "test".to_string(),
            snippet: snippet.to_string(),
            line_range: None,
            match_lines: vec![2, 2, 1],
            explanation: Some(vec![
                RankSignal {
                    kind: "term".to_string(),
                    value: "alpha".to_string(),
                    score: 1.0,
                },
                RankSignal {
                    kind: "term".to_string(),
                    value: "alpha".to_string(),
                    score: 2.0,
                },
            ]),
            query_plan: None,
            duplicate_group: None,
            context: None,
            read_range: None,
            read_request: None,
            related_request: None,
            related_symbols_request: None,
        }
    }

    #[test]
    fn finalize_results_dedupes_before_metadata_population() {
        let results = vec![
            test_result("one/src/auth.rs", 10.0, "2: pub fn issue_token() {}"),
            test_result("two/src/auth.rs", 9.0, "9: pub fn issue_token() {}"),
            test_result("src/other.rs", 1.0, "1: pub fn other() {}"),
        ];

        let finalized = finalize_results(results, 1);

        assert_eq!(finalized.len(), 1);
        assert_eq!(finalized[0].path, "one/src/auth.rs");
        assert_eq!(
            finalized[0]
                .duplicate_group
                .as_ref()
                .unwrap()
                .duplicate_paths,
            vec!["two/src/auth.rs"]
        );
        assert_eq!(finalized[0].line_range.as_ref().unwrap().start_line, 2);
        assert_eq!(finalized[0].match_lines, vec![2, 1]);
        assert_eq!(finalized[0].read_range.as_ref().unwrap().start, 1);
        let signals = finalized[0].explanation.as_ref().unwrap();
        assert_eq!(signals.len(), 1);
        assert_eq!(signals[0].score, 3.0);
    }

    #[test]
    fn compiled_path_filter_matcher_normalizes_once_and_preserves_semantics() {
        let filters = SearchFilters {
            file: Some("AUTH*.RS".to_string()),
            path: Some("SRC\\AUTH".to_string()),
            language: Some("Rust".to_string()),
            extension: Some(".RS".to_string()),
            exclude_path: vec!["generated".to_string()],
            exclude_extension: vec![".md".to_string()],
            test: Some(false),
            generated: Some(false),
            ..SearchFilters::default()
        };
        let matcher = PathFilterMatcher::from_filters(&filters);

        assert!(matches_filters_with_compiled_path_metadata(
            "src/auth.rs",
            "auth.rs",
            Some("rs"),
            Some("rust"),
            &matcher
        ));
        assert!(!matches_filters_with_compiled_path_metadata(
            "src/generated/auth.rs",
            "auth.rs",
            Some("rs"),
            Some("rust"),
            &matcher
        ));
        assert!(!matches_filters_with_compiled_path_metadata(
            "src/auth.md",
            "auth.md",
            Some("md"),
            Some("markdown"),
            &matcher
        ));
    }

    #[test]
    fn code_filter_splits_implementation_from_docs_and_config() {
        let code_filters = SearchFilters {
            code: Some(true),
            ..SearchFilters::default()
        };
        let code_matcher = PathFilterMatcher::from_filters(&code_filters);
        assert!(matches_filters_with_compiled_path_metadata(
            "src/auth.rs",
            "auth.rs",
            Some("rs"),
            Some("rust"),
            &code_matcher
        ));
        assert!(!matches_filters_with_compiled_path_metadata(
            "docs/auth.md",
            "auth.md",
            Some("md"),
            Some("markdown"),
            &code_matcher
        ));
        assert!(!matches_filters_with_compiled_path_metadata(
            "Cargo.toml",
            "cargo.toml",
            Some("toml"),
            Some("toml"),
            &code_matcher
        ));

        let prose_filters = SearchFilters {
            code: Some(false),
            ..SearchFilters::default()
        };
        let prose_matcher = PathFilterMatcher::from_filters(&prose_filters);
        assert!(matches_filters_with_compiled_path_metadata(
            "docs/auth.md",
            "auth.md",
            Some("md"),
            Some("markdown"),
            &prose_matcher
        ));
        assert!(matches_filters_with_compiled_path_metadata(
            "config/app.yaml",
            "app.yaml",
            Some("yaml"),
            Some("yaml"),
            &prose_matcher
        ));
        assert!(!matches_filters_with_compiled_path_metadata(
            "src/auth.ts",
            "auth.ts",
            Some("ts"),
            Some("typescript"),
            &prose_matcher
        ));
    }

    #[test]
    fn read_tool_requests_include_shell_safe_cli_hints() {
        let request = ResultToolRequest::new(
            "read_range",
            serde_json::json!({
                "repo": "/tmp/my repo",
                "path": "src/it'll work.rs",
                "start": 3,
                "lines": 4
            }),
        );

        assert_eq!(
            request.cli.as_deref(),
            Some("orient read-range --repo '/tmp/my repo' 'src/it'\\''ll work.rs:3:4'")
        );
        assert_eq!(request.id, "read");
        assert!(
            request.client_cli.contains("| orient client-jsonl"),
            "{request:?}"
        );
        let jsonl: serde_json::Value = serde_json::from_str(&request.jsonl).unwrap();
        assert_eq!(jsonl["id"], serde_json::json!("read"));
        assert_eq!(jsonl["tool"], serde_json::json!("read_range"));
        assert_eq!(
            jsonl["arguments"]["path"],
            serde_json::json!("src/it'll work.rs")
        );
    }

    #[test]
    fn search_tool_requests_include_query_flag_cli_hints() {
        let request = ResultToolRequest::new(
            "search_code",
            serde_json::json!({
                "repo": "/tmp/my repo",
                "query": "issue token",
                "limit": 3
            }),
        );

        assert_eq!(
            request.cli.as_deref(),
            Some("orient search --repo '/tmp/my repo' --limit 3 --query 'issue token'")
        );

        let plan_request = ResultToolRequest::new(
            "indexed_query_plan",
            serde_json::json!({
                "index": "/tmp/orient.index",
                "query": "SessionManager definitely_missing",
                "path": "src"
            }),
        );

        assert_eq!(
            plan_request.cli.as_deref(),
            Some(
                "orient search-plan --index /tmp/orient.index --path src --query 'SessionManager definitely_missing'"
            )
        );
    }

    #[test]
    fn batch_read_tool_requests_use_compact_ranges() {
        let request = ResultToolRequest::new(
            "read_index_ranges",
            serde_json::json!({
                "index": "/tmp/orient.index",
                "ranges": [
                    {"path": "src/lib.rs", "start": 1, "lines": 80},
                    {"path": "tests/auth test.rs", "start": 3, "lines": 4}
                ]
            }),
        );

        assert_eq!(
            request.cli.as_deref(),
            Some(
                "orient read-index-ranges --index /tmp/orient.index src/lib.rs:1:80 'tests/auth test.rs:3:4'"
            )
        );
    }

    #[test]
    fn batch_read_tool_request_cli_preserves_per_range_scope() {
        let request = ResultToolRequest::new(
            "read_ranges",
            serde_json::json!({
                "repo": "/tmp/my repo",
                "ranges": [
                    {"path": "src/lib.rs", "start": 1, "lines": 80},
                    {"path": "src/auth.rs", "start": 26, "lines": 80, "scope": "symbol"}
                ]
            }),
        );

        assert_eq!(
            request.cli.as_deref(),
            Some(
                "orient read-ranges --repo '/tmp/my repo' src/lib.rs:1:80 src/auth.rs:26:80:symbol"
            )
        );
    }

    #[test]
    fn batch_read_tool_requests_dedupe_ranges_before_limit() {
        let mut base_arguments = serde_json::Map::new();
        base_arguments.insert("index".to_string(), serde_json::json!("/tmp/orient.index"));
        let request = read_batch_request_from_ranges_with_limit(
            vec![
                ResultReadRange {
                    path: "src/lib.rs".to_string(),
                    start: 1,
                    lines: 80,
                    scope: None,
                },
                ResultReadRange {
                    path: "src/lib.rs".to_string(),
                    start: 1,
                    lines: 80,
                    scope: None,
                },
                ResultReadRange {
                    path: "tests/auth.rs".to_string(),
                    start: 5,
                    lines: 40,
                    scope: None,
                },
            ],
            "read_index_ranges",
            base_arguments,
            2,
        )
        .unwrap();

        assert_eq!(request.arguments["ranges"].as_array().unwrap().len(), 2);
        assert_eq!(
            request.arguments["ranges"][0]["path"],
            serde_json::json!("src/lib.rs")
        );
        assert_eq!(
            request.arguments["ranges"][1]["path"],
            serde_json::json!("tests/auth.rs")
        );
    }

    #[test]
    fn symbol_snippet_results_generate_symbol_scoped_read_requests() {
        let results = vec![SearchResult {
            path: "src/auth.rs".to_string(),
            score: 10.0,
            reason: "matched symbol:issue_token".to_string(),
            snippet: "26: pub fn issue_token() {\n27:     let token = 42;\n28: }".to_string(),
            line_range: None,
            match_lines: vec![26],
            explanation: None,
            query_plan: None,
            duplicate_group: None,
            context: None,
            read_range: None,
            read_request: None,
            related_request: None,
            related_symbols_request: None,
        }];
        let filters = SearchFilters {
            snippet: SnippetMode::Symbol,
            ..SearchFilters::default()
        };
        let mut finalized = finalize_results_for_filters(results, 1, &filters);
        attach_result_read_requests(&mut finalized, "read_range", serde_json::Map::new());

        let read_range = finalized[0].read_range.as_ref().unwrap();
        assert_eq!(read_range.scope, Some(RangeScope::Symbol));
        assert_eq!(read_range.start, 26);
        assert_eq!(
            finalized[0].read_request.as_ref().unwrap().arguments["scope"],
            serde_json::json!("symbol")
        );
    }

    #[test]
    fn symbol_lookup_read_requests_are_symbol_scoped() {
        let symbols = vec![Symbol {
            name: "issue_token".to_string(),
            kind: "function".to_string(),
            path: "src/auth.rs".to_string(),
            line: 26,
        }];
        let results = symbol_lookup_results(symbols, "read_range", serde_json::Map::new());

        assert_eq!(results[0].read_range.scope, Some(RangeScope::Symbol));
        assert_eq!(results[0].read_range.start, 26);
        assert_eq!(
            results[0].read_request.arguments["scope"],
            serde_json::json!("symbol")
        );

        let batch =
            symbol_lookup_read_batch_request(&results, "read_ranges", serde_json::Map::new())
                .unwrap();
        assert_eq!(batch.arguments["scope"], serde_json::json!("symbol"));
    }

    #[test]
    fn related_tool_requests_include_cli_hints() {
        let request = ResultToolRequest::new(
            "related_symbols",
            serde_json::json!({
                "repo": "/tmp/my repo",
                "path": "src/auth.rs",
                "query": "symbol:SessionManager issue token"
            }),
        );

        assert_eq!(
            request.cli.as_deref(),
            Some(
                "orient related-symbols --repo '/tmp/my repo' --path src/auth.rs --query 'symbol:SessionManager issue token'"
            )
        );
    }

    #[test]
    fn map_plan_and_search_requests_include_cli_hints() {
        let map = ResultToolRequest::new(
            "repo_map",
            serde_json::json!({
                "index_dir": "/tmp/orient shards",
                "repo": "platform",
                "detail": "compact",
                "read_limit": 8
            }),
        );
        assert_eq!(
            map.cli.as_deref(),
            Some(
                "orient repo-map --index-dir '/tmp/orient shards' --repo-filter platform --detail compact --read-limit 8"
            )
        );

        let plan = ResultToolRequest::new(
            "search_query_plan",
            serde_json::json!({
                "repo": "/tmp/my repo",
                "query": "symbol:SessionManager issue token",
                "path": "src/auth",
                "language": "rust",
                "require_all": true
            }),
        );
        assert_eq!(
            plan.cli.as_deref(),
            Some(
                "orient search-plan --repo '/tmp/my repo' --path src/auth --language rust --require-all --query 'symbol:SessionManager issue token'"
            )
        );

        let retry = ResultToolRequest::new(
            "search",
            serde_json::json!({
                "repo": "/tmp/my repo",
                "query": "mode:any issue token",
                "path": null,
                "language": "rust",
                "explain": true,
                "limit": 5
            }),
        );
        assert_eq!(
            retry.cli.as_deref(),
            Some(
                "orient search --repo '/tmp/my repo' --limit 5 --language rust --explain --query 'mode:any issue token'"
            )
        );
    }

    #[test]
    fn generated_filters_push_conservative_ripgrep_globs() {
        let filters = SearchFilters {
            generated: Some(false),
            ..SearchFilters::default()
        };
        let globs = generated_ripgrep_globs(&filters);

        assert!(globs.contains(&"!**/generated/**".to_string()));
        assert!(globs.contains(&"!**/__generated__/**".to_string()));
        assert!(globs.contains(&"!**/codegen/**".to_string()));
        assert!(globs.contains(&"!**/*.pb.go".to_string()));
        assert!(globs.contains(&"!**/*.g.dart".to_string()));
        assert!(globs.contains(&"!**/*_gen.*".to_string()));
        assert!(globs.contains(&"!**/chunk-*.js".to_string()));
        assert!(globs.contains(&"!**/preload-helper-*.js".to_string()));
        assert!(globs.iter().all(|glob| glob.starts_with("!**/")));

        let positive = generated_ripgrep_globs(&SearchFilters {
            generated: Some(true),
            ..SearchFilters::default()
        });
        assert!(positive.contains(&"**/generated/**".to_string()));
        assert!(positive.contains(&"**/*.pb.rs".to_string()));
        assert!(positive.contains(&"**/*.g.dart".to_string()));
        assert!(positive.contains(&"**/chunk-*.js".to_string()));
        assert!(positive.iter().all(|glob| glob.starts_with("**/")));
        assert_eq!(
            generated_ripgrep_globs(&SearchFilters::default()),
            Vec::<String>::new()
        );
    }

    #[test]
    fn code_filters_push_safe_ripgrep_globs() {
        let code_only = code_ripgrep_globs(&SearchFilters {
            code: Some(true),
            ..SearchFilters::default()
        });
        assert!(code_only.contains(&"**/*.rs".to_string()));
        assert!(code_only.contains(&"**/*.ts".to_string()));
        assert!(code_only.iter().all(|glob| !glob.starts_with('!')));

        let prose_only = code_ripgrep_globs(&SearchFilters {
            code: Some(false),
            ..SearchFilters::default()
        });
        assert!(prose_only.contains(&"**/*.md".to_string()));
        assert!(prose_only.contains(&"**/README".to_string()));
        assert!(prose_only.contains(&"**/bun.lockb".to_string()));
        assert!(prose_only.iter().all(|glob| !glob.starts_with('!')));

        let scoped_code = code_ripgrep_globs(&SearchFilters {
            code: Some(true),
            path: Some("src".to_string()),
            ..SearchFilters::default()
        });
        assert!(scoped_code.contains(&"!**/*.md".to_string()));
        assert!(scoped_code.contains(&"!**/README".to_string()));
        assert!(scoped_code.iter().all(|glob| glob.starts_with('!')));

        let scoped_prose = code_ripgrep_globs(&SearchFilters {
            code: Some(false),
            file: Some("auth".to_string()),
            ..SearchFilters::default()
        });
        assert!(scoped_prose.contains(&"!**/*.rs".to_string()));
        assert!(scoped_prose.contains(&"!**/*.tsx".to_string()));
        assert!(scoped_prose.iter().all(|glob| glob.starts_with('!')));
    }

    #[test]
    fn generated_path_detection_covers_directory_and_suffix_patterns() {
        let generated_paths = [
            "src/generated/cache.rs",
            "src/__generated__/cache.ts",
            "gen/schema.rs",
            "codegen/client.ts",
            "src/session.generated.rs",
            "src/schema.pb.go",
            "src/schema.pb.rs",
            "src/models.g.dart",
            "src/generated_client.rs",
            "src/auth_gen.rs",
            "webview/assets/chunk-OIYGIGL5-CJrBIAxA.js",
            "webview/assets/react-BE0_fAZJ.js",
            "webview/assets/preload-helper-Chd9yIcd.js",
            "public/static/js/main-a1b2c3d4.js",
            "src/vendor.min.js",
            "src/client.bundle.js",
            r"Src\AUTO-GENERATED\Client.ts",
        ];
        for path in generated_paths {
            assert!(is_generated_path(path), "{path} should be generated");
        }

        let handwritten_paths = [
            "src/general.rs",
            "src/generator.rs",
            "src/code_generation.rs",
            "src/auth.test.rs",
            "src/generation/auth.rs",
            "src/progenitor/client.rs",
            "src/assets/chunk_loader.js",
            "src/assets/date-helpers.js",
            "src/static/react-wrapper.js",
        ];
        for path in handwritten_paths {
            assert!(!is_generated_path(path), "{path} should be handwritten");
        }
    }

    #[test]
    fn source_content_filter_cache_reuses_per_path_decisions() {
        let repo = tempfile::tempdir().unwrap();
        let source = repo.path().join("src/lib.rs");
        fs::create_dir_all(source.parent().unwrap()).unwrap();
        fs::write(
            &source,
            "use serde::Serialize;\npub fn issue_token() { let token = \"serde\"; }\n",
        )
        .unwrap();
        let filters = SearchFilters {
            import: Some("serde".to_string()),
            symbol_kind: Some("function".to_string()),
            ..SearchFilters::default()
        };
        let mut cache = HashMap::new();

        assert!(source_content_filters_match_cached(
            repo.path(),
            "src/lib.rs",
            &filters,
            &mut cache
        ));
        assert_eq!(cache.len(), 1);

        fs::write(&source, "pub struct NotAFunction;\n").unwrap();
        assert!(source_content_filters_match_cached(
            repo.path(),
            "src/lib.rs",
            &filters,
            &mut cache
        ));
        assert_eq!(cache.len(), 1);

        cache.insert("src/lib.rs".to_string(), false);
        assert!(!source_content_filters_match_cached(
            repo.path(),
            "src/lib.rs",
            &filters,
            &mut cache
        ));
    }

    #[test]
    fn fd_file_prefilter_defers_positive_structural_scopes_to_rg() {
        assert_eq!(
            fd_positive_file_pattern(&SearchFilters {
                file: Some("auth.rs".to_string()),
                ..SearchFilters::default()
            })
            .as_deref(),
            Some("auth\\.rs")
        );
        assert_eq!(
            fd_positive_file_pattern(&SearchFilters {
                file: Some("auth.rs".to_string()),
                generated: Some(false),
                ..SearchFilters::default()
            })
            .as_deref(),
            Some("auth\\.rs")
        );
        assert!(
            fd_positive_file_pattern(&SearchFilters {
                file: Some("auth.rs".to_string()),
                generated: Some(true),
                ..SearchFilters::default()
            })
            .is_none()
        );
        assert!(
            fd_positive_file_pattern(&SearchFilters {
                file: Some("auth.rs".to_string()),
                test: Some(true),
                ..SearchFilters::default()
            })
            .is_none()
        );
        assert_eq!(
            fd_positive_file_pattern(&SearchFilters {
                code: Some(true),
                ..SearchFilters::default()
            })
            .as_deref(),
            Some(FD_CODE_FILE_PATTERN)
        );
        assert_eq!(
            fd_positive_file_pattern(&SearchFilters {
                code: Some(false),
                ..SearchFilters::default()
            })
            .as_deref(),
            Some(FD_PROSE_FILE_PATTERN)
        );
        assert!(
            fd_positive_file_pattern(&SearchFilters {
                code: Some(true),
                test: Some(true),
                ..SearchFilters::default()
            })
            .is_none()
        );
    }

    #[test]
    fn fd_self_limit_only_for_plain_file_filters() {
        assert!(fd_can_self_limit(&SearchFilters {
            file: Some("Cargo.toml".to_string()),
            ..SearchFilters::default()
        }));
        assert!(!fd_can_self_limit(&SearchFilters {
            file: Some("Cargo.toml".to_string()),
            path: Some("crates".to_string()),
            ..SearchFilters::default()
        }));
        assert!(!fd_can_self_limit(&SearchFilters {
            file: Some("Cargo.toml".to_string()),
            exclude_content: vec!["skipme".to_string()],
            ..SearchFilters::default()
        }));
        assert!(!fd_can_self_limit(&SearchFilters {
            file: Some("Cargo.toml".to_string()),
            extension: Some("toml".to_string()),
            ..SearchFilters::default()
        }));
        assert!(!fd_can_self_limit(&SearchFilters {
            file: Some("Cargo.*".to_string()),
            ..SearchFilters::default()
        }));
        assert_eq!(
            fd_max_results(
                &SearchFilters {
                    file: Some("Cargo.toml".to_string()),
                    ..SearchFilters::default()
                },
                10
            ),
            "10"
        );
        assert_eq!(
            fd_max_results(
                &SearchFilters {
                    file: Some("Cargo.toml".to_string()),
                    exclude_path: vec!["vendor".to_string()],
                    ..SearchFilters::default()
                },
                10
            ),
            "0"
        );
    }
}
