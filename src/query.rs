use crate::repo_index::{
    SearchFilters, identifier_boundary_text, normalize_language_filter, tokenize,
};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ParsedQuery {
    pub terms: Vec<String>,
    pub filters: SearchFilters,
    pub explicit_content_terms: bool,
}

pub fn parse_query(input: &str) -> ParsedQuery {
    let mut terms = Vec::new();
    let mut filters = SearchFilters::default();
    let mut explicit_content_terms = false;

    for token in split_query(input) {
        let (negated, token) = split_negated_token(token);
        if let Some(term) = content_term(&token) {
            if negated {
                filters.exclude_content.push(term);
            } else {
                terms.push(term);
                explicit_content_terms = true;
            }
            continue;
        }
        if apply_match_mode(&mut filters, &token, negated)
            || apply_filter(&mut filters, &token, negated)
        {
            continue;
        }
        if negated {
            let token = token.trim();
            if !token.is_empty() {
                filters.exclude_content.push(token.to_string());
            }
        } else if !token.trim().is_empty() {
            terms.push(token);
        }
    }

    infer_leading_location_term(&mut terms, &mut filters, explicit_content_terms);
    infer_pytest_command_node_id_term(&mut terms, &mut filters, explicit_content_terms);
    infer_leading_pytest_node_id_term(&mut terms, &mut filters, explicit_content_terms);
    infer_cargo_test_command_symbol_term(&mut terms, &mut filters, explicit_content_terms);
    if terms.len() > 1 && !filters.match_any {
        filters.require_all = true;
    }
    infer_bazel_label_term(&mut terms, &mut filters, explicit_content_terms);
    infer_path_like_single_term(&mut terms, &mut filters, explicit_content_terms);

    ParsedQuery {
        terms,
        filters,
        explicit_content_terms,
    }
}

fn split_negated_token(token: String) -> (bool, String) {
    if token == "-->" {
        return (false, token);
    }
    if let Some(value) = token.strip_prefix('-').or_else(|| token.strip_prefix('!'))
        && !value.is_empty()
        && !value.starts_with('=')
    {
        return (true, value.to_string());
    }
    (false, token)
}

fn content_term(token: &str) -> Option<String> {
    let (key, value) = token.split_once(':')?;
    if !matches!(
        key.to_ascii_lowercase().as_str(),
        "content" | "text" | "term"
    ) {
        return None;
    }
    let value = value.trim();
    if value.is_empty() {
        return None;
    }
    Some(value.to_string())
}

fn path_scope_key(key: &str) -> bool {
    matches!(
        key,
        "path" | "dir" | "directory" | "folder" | "in" | "inside" | "under" | "within"
    )
}

fn apply_match_mode(filters: &mut SearchFilters, token: &str, negated: bool) -> bool {
    if negated {
        return false;
    }
    let Some((key, value)) = token.split_once(':') else {
        return false;
    };
    let key = key.to_ascii_lowercase();
    let value = value.trim().to_ascii_lowercase();
    match (key.as_str(), value.as_str()) {
        ("mode" | "match" | "terms", "any" | "or" | "some")
        | ("all" | "require_all" | "require-all", "false" | "0" | "no")
        | ("any_terms" | "any-terms", "true" | "1" | "yes") => {
            filters.match_any = true;
            filters.require_all = false;
            true
        }
        ("mode" | "match" | "terms", "all" | "and")
        | ("all" | "require_all" | "require-all", "true" | "1" | "yes")
        | ("any_terms" | "any-terms", "false" | "0" | "no") => {
            filters.match_any = false;
            filters.require_all = true;
            true
        }
        _ => false,
    }
}

fn apply_filter(filters: &mut SearchFilters, token: &str, negated: bool) -> bool {
    let Some((key, value)) = token.split_once(':') else {
        return apply_bare_filter(filters, token, negated);
    };
    let key = key.to_ascii_lowercase();
    let value = value.trim().to_string();
    if value.is_empty() {
        return false;
    }

    if !negated && matches!(key.as_str(), "not" | "without" | "exclude") {
        return apply_negative_alias(filters, &value);
    }

    if !negated && let Some(kind) = symbol_kind_from_shorthand_key(&key) {
        filters.symbol_kind = Some(kind);
        filters.symbol = Some(value);
        return true;
    }

    match (negated, key.as_str()) {
        (false, "file" | "filename" | "file_name" | "file-name" | "basename") => {
            let (value, target_line) = strip_location_suffix(&value);
            let value = strip_leading_current_dir_segments(value);
            if target_line.is_some() && value.contains('/') {
                filters.path = Some(value);
            } else {
                filters.file = Some(value);
            }
            filters.target_line = target_line.or(filters.target_line);
        }
        (false, key) if path_scope_key(key) => {
            let (value, target_line) = strip_location_suffix(&value);
            let value = strip_leading_current_dir_segments(value);
            filters.path = Some(value);
            filters.target_line = target_line.or(filters.target_line);
        }
        (false, "lang" | "language") => filters.language = Some(normalize_language_filter(&value)),
        (false, "ext" | "extension") => {
            filters.extension = Some(value.trim_start_matches('.').to_ascii_lowercase())
        }
        (false, "symbol") => filters.symbol = Some(value),
        (false, "kind" | "symbol_kind" | "symbol-kind") => {
            filters.symbol_kind = Some(normalize_symbol_kind(&value))
        }
        (false, "type" | "symbol_type" | "symbol-type") => {
            match symbol_kind_from_type_value(&value) {
                Some(kind) => filters.symbol_kind = Some(kind),
                None => return false,
            }
        }
        (false, "repo" | "repo_filter" | "repo-filter") => filters.repo = Some(value),
        (false, "branch" | "git_branch" | "git-branch") => filters.branch = Some(value),
        (false, "origin" | "remote" | "remote_origin" | "remote-origin") => {
            filters.origin = Some(value)
        }
        (false, "dep" | "deps" | "dependency" | "dependencies") => {
            filters.dependency = Some(value.to_ascii_lowercase())
        }
        (false, "import" | "imports" | "module" | "modules" | "use" | "uses") => {
            filters.import = Some(value.to_ascii_lowercase())
        }
        (false, "test" | "tests") => filters.test = Some(parse_boolish(&value).unwrap_or(true)),
        (false, "is") => match test_filter_from_is_value(&value) {
            Some(IsFilter::Test(value)) => filters.test = Some(value),
            Some(IsFilter::Generated(value)) => filters.generated = Some(value),
            Some(IsFilter::Code(value)) => filters.code = Some(value),
            None => return false,
        },
        (false, "generated" | "generated_code" | "generated-code") => {
            filters.generated = Some(parse_boolish(&value).unwrap_or(true))
        }
        (false, "code" | "source_code" | "source-code") => {
            filters.code = Some(parse_boolish(&value).unwrap_or(true))
        }
        (false, "line" | "line_number" | "line-number" | "target_line" | "target-line") => {
            let Some(line) = parse_positive_usize(&value) else {
                return false;
            };
            filters.target_line = Some(line);
        }
        (
            false,
            "exclude_file" | "exclude-file" | "exclude_filename" | "exclude-filename"
            | "exclude_file_name" | "exclude-file-name",
        ) => filters.exclude_file.push(value),
        (
            false,
            "exclude_path" | "exclude-path" | "exclude_dir" | "exclude-dir" | "exclude_directory"
            | "exclude-directory" | "exclude_folder" | "exclude-folder",
        ) => filters.exclude_path.push(value),
        (false, "exclude_language" | "exclude-language" | "exclude_lang" | "exclude-lang") => {
            filters
                .exclude_language
                .push(normalize_language_filter(&value))
        }
        (false, "exclude_extension" | "exclude-extension" | "exclude_ext" | "exclude-ext") => {
            filters
                .exclude_extension
                .push(value.trim_start_matches('.').to_ascii_lowercase())
        }
        (false, "exclude_symbol" | "exclude-symbol") => filters.exclude_symbol.push(value),
        (
            false,
            "exclude_symbol_kind"
            | "exclude-symbol-kind"
            | "exclude_kind"
            | "exclude-kind"
            | "exclude_type"
            | "exclude-type",
        ) => filters
            .exclude_symbol_kind
            .push(normalize_symbol_kind(&value)),
        (false, "exclude_repo" | "exclude-repo") => filters.exclude_repo.push(value),
        (
            false,
            "exclude_branch" | "exclude-branch" | "exclude_git_branch" | "exclude-git-branch",
        ) => filters.exclude_branch.push(value),
        (
            false,
            "exclude_origin"
            | "exclude-origin"
            | "exclude_remote"
            | "exclude-remote"
            | "exclude_remote_origin"
            | "exclude-remote-origin",
        ) => filters.exclude_origin.push(value),
        (
            false,
            "exclude_dependency" | "exclude-dependency" | "exclude_dep" | "exclude-dep"
            | "exclude_deps" | "exclude-deps",
        ) => filters.exclude_dependency.push(value.to_ascii_lowercase()),
        (
            false,
            "exclude_import" | "exclude-import" | "exclude_imports" | "exclude-imports"
            | "exclude_module" | "exclude-module" | "exclude_modules" | "exclude-modules"
            | "exclude_use" | "exclude-use" | "exclude_uses" | "exclude-uses",
        ) => filters.exclude_import.push(value.to_ascii_lowercase()),
        (
            false,
            "exclude_content" | "exclude_text" | "exclude_term" | "exclude-content"
            | "exclude-text" | "exclude-term",
        ) => filters.exclude_content.push(value),
        (true, "file" | "filename" | "file_name" | "file-name" | "basename") => {
            filters.exclude_file.push(value)
        }
        (true, key) if path_scope_key(key) => filters.exclude_path.push(value),
        (true, "lang" | "language") => filters
            .exclude_language
            .push(normalize_language_filter(&value)),
        (true, "ext" | "extension") => filters
            .exclude_extension
            .push(value.trim_start_matches('.').to_ascii_lowercase()),
        (true, "symbol") => filters.exclude_symbol.push(value),
        (true, "kind" | "symbol_kind" | "symbol-kind") => filters
            .exclude_symbol_kind
            .push(normalize_symbol_kind(&value)),
        (true, "type" | "symbol_type" | "symbol-type") => match symbol_kind_from_type_value(&value)
        {
            Some(kind) => filters.exclude_symbol_kind.push(kind),
            None => return false,
        },
        (true, "repo" | "repo_filter" | "repo-filter") => filters.exclude_repo.push(value),
        (true, "branch" | "git_branch" | "git-branch") => filters.exclude_branch.push(value),
        (true, "origin" | "remote" | "remote_origin" | "remote-origin") => {
            filters.exclude_origin.push(value)
        }
        (true, "dep" | "deps" | "dependency" | "dependencies") => {
            filters.exclude_dependency.push(value.to_ascii_lowercase())
        }
        (true, "import" | "imports" | "module" | "modules" | "use" | "uses") => {
            filters.exclude_import.push(value.to_ascii_lowercase())
        }
        (true, "test" | "tests") => filters.test = Some(false),
        (true, "is") => match test_filter_from_is_value(&value) {
            Some(IsFilter::Test(value)) => filters.test = Some(!value),
            Some(IsFilter::Generated(value)) => filters.generated = Some(!value),
            Some(IsFilter::Code(value)) => filters.code = Some(!value),
            None => return false,
        },
        (true, "generated" | "generated_code" | "generated-code") => {
            filters.generated = Some(false)
        }
        (true, "code" | "source_code" | "source-code") => filters.code = Some(false),
        _ => return false,
    }
    true
}

fn apply_negative_alias(filters: &mut SearchFilters, value: &str) -> bool {
    if let Some(term) = content_term(value) {
        filters.exclude_content.push(term);
        return true;
    }
    if apply_filter(filters, value, true) {
        return true;
    }
    filters.exclude_content.push(value.to_string());
    true
}

fn apply_bare_filter(filters: &mut SearchFilters, token: &str, negated: bool) -> bool {
    let token = token.to_ascii_lowercase();
    match (negated, token.as_str()) {
        (true, "test" | "tests") => filters.test = Some(false),
        (true, "generated" | "gen" | "codegen" | "autogen") => filters.generated = Some(false),
        (true, "docs" | "documentation" | "prose" | "config" | "configuration") => {
            filters.code = Some(true)
        }
        (true, path) => match bare_path_exclusion(path) {
            Some(path) => filters.exclude_path.push(path.to_string()),
            None => return false,
        },
        _ => return false,
    }
    true
}

fn bare_path_exclusion(token: &str) -> Option<&'static str> {
    match token {
        "vendor" | "vendors" => Some("vendor"),
        "node_modules" | "node-modules" => Some("node_modules"),
        "third_party" | "third-party" => Some("third_party"),
        "external" | "externals" => Some("external"),
        "dist" | "distribution" => Some("dist"),
        "build" | "builds" => Some("build"),
        "target" | "targets" => Some("target"),
        "coverage" => Some("coverage"),
        ".next" | "nextjs" => Some(".next"),
        _ => None,
    }
}

enum IsFilter {
    Test(bool),
    Generated(bool),
    Code(bool),
}

fn test_filter_from_is_value(value: &str) -> Option<IsFilter> {
    match value.to_ascii_lowercase().as_str() {
        "test" | "tests" | "spec" | "specs" => Some(IsFilter::Test(true)),
        "source" | "src" | "prod" | "production" => Some(IsFilter::Test(false)),
        "generated" | "gen" | "codegen" | "autogen" => Some(IsFilter::Generated(true)),
        "authored" | "manual" | "handwritten" => Some(IsFilter::Generated(false)),
        "code" | "source-code" | "source_code" | "implementation" | "impl" => {
            Some(IsFilter::Code(true))
        }
        "prose" | "docs" | "documentation" | "config" | "configuration" => {
            Some(IsFilter::Code(false))
        }
        _ => None,
    }
}

fn infer_path_like_single_term(
    terms: &mut Vec<String>,
    filters: &mut SearchFilters,
    explicit_content_terms: bool,
) {
    if explicit_content_terms
        || terms.len() != 1
        || filters.file.is_some()
        || filters.path.is_some()
        || filters.symbol.is_some()
    {
        return;
    }

    let (term, target_line) = strip_location_suffix(&terms[0]);
    let term = term.trim().replace('\\', "/");
    if term.is_empty() || term.chars().any(char::is_whitespace) || term == "-" {
        return;
    }
    let term = strip_leading_current_dir_segments(term);
    if term.is_empty() || term.starts_with('.') {
        return;
    }

    if term.contains('/') {
        filters.path = Some(term);
        filters.target_line = target_line;
        terms.clear();
        filters.require_all = false;
        return;
    }

    if looks_like_file_name_query(&term) {
        filters.file = Some(term);
        filters.target_line = target_line;
        terms.clear();
        filters.require_all = false;
    }
}

fn infer_bazel_label_term(
    terms: &mut Vec<String>,
    filters: &mut SearchFilters,
    explicit_content_terms: bool,
) {
    if explicit_content_terms
        || terms.is_empty()
        || filters.file.is_some()
        || filters.path.is_some()
        || filters.symbol.is_some()
    {
        return;
    }

    let label_terms: Vec<_> = terms
        .iter()
        .enumerate()
        .filter_map(|(index, term)| bazel_label_parts(term).map(|parts| (index, parts)))
        .collect();
    let [(label_index, (package, target))] = label_terms.as_slice() else {
        return;
    };
    if terms.len() > 1 && !is_bazel_command_context(terms, *label_index) {
        return;
    }
    if let Some(package) = package {
        filters.path = Some(package.clone());
    }
    filters.symbol = Some(target.clone());
    filters.symbol_kind = Some("target".to_string());
    terms.clear();
    filters.require_all = false;
}

fn is_bazel_command_context(terms: &[String], label_index: usize) -> bool {
    let mut saw_bazel_binary = false;
    for (index, term) in terms.iter().enumerate() {
        if index == label_index {
            continue;
        }
        let term = trim_location_token_wrappers(term).to_ascii_lowercase();
        if matches!(term.as_str(), "bazel" | "bazelisk") {
            saw_bazel_binary = true;
            continue;
        }
        if is_bazel_command_word(&term) {
            continue;
        }
        return false;
    }
    saw_bazel_binary
}

fn is_bazel_command_word(value: &str) -> bool {
    matches!(
        value,
        "build"
            | "test"
            | "run"
            | "query"
            | "cquery"
            | "aquery"
            | "coverage"
            | "clean"
            | "fetch"
            | "sync"
            | "mobile-install"
    )
}

fn bazel_label_parts(value: &str) -> Option<(Option<String>, String)> {
    let value = trim_location_token_wrappers(value);
    if value.chars().any(char::is_whitespace) {
        return None;
    }

    let (package, target) = if let Some(target) = value.strip_prefix(':') {
        (None, target)
    } else if let Some(rest) = value.strip_prefix("//") {
        let (package, target) = rest.split_once(':')?;
        let package = package.trim_matches('/');
        let package = (!package.is_empty()).then(|| package.to_string());
        (package, target)
    } else {
        return None;
    };

    let target = target.trim();
    if !is_bazel_label_target(target) {
        return None;
    }
    Some((package, target.to_string()))
}

fn is_bazel_label_target(value: &str) -> bool {
    !value.is_empty()
        && !value.contains('/')
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'-' | b'.'))
}

fn infer_pytest_command_node_id_term(
    terms: &mut Vec<String>,
    filters: &mut SearchFilters,
    explicit_content_terms: bool,
) {
    if explicit_content_terms
        || terms.len() < 2
        || filters.file.is_some()
        || filters.path.is_some()
        || filters.symbol.is_some()
    {
        return;
    }

    let node_terms: Vec<_> = terms
        .iter()
        .enumerate()
        .filter_map(|(index, term)| pytest_node_id_path(term).map(|path| (index, path)))
        .collect();
    let [(node_index, path)] = node_terms.as_slice() else {
        return;
    };
    if !is_pytest_command_context(terms, *node_index) {
        return;
    }

    let path = strip_leading_current_dir_segments(path.clone());
    let applied = if path.contains('/') {
        filters.path = Some(path);
        true
    } else if looks_like_file_name_query(&path) {
        filters.file = Some(path);
        true
    } else {
        false
    };
    if !applied {
        return;
    }

    filters
        .exclude_content
        .retain(|term| !is_pytest_command_flag_term(term));
    terms.clear();
    filters.require_all = false;
}

fn is_pytest_command_context(terms: &[String], node_index: usize) -> bool {
    let mut saw_pytest_runner = false;
    for (index, term) in terms.iter().enumerate() {
        if index == node_index {
            continue;
        }
        let term = trim_location_token_wrappers(term).to_ascii_lowercase();
        if matches!(term.as_str(), "pytest" | "py.test") {
            saw_pytest_runner = true;
            continue;
        }
        if is_pytest_command_word(&term) {
            continue;
        }
        return false;
    }
    saw_pytest_runner
}

fn is_pytest_command_word(value: &str) -> bool {
    matches!(value, "python" | "python3" | "uv" | "poetry" | "run")
        || value
            .strip_prefix("python")
            .is_some_and(|version| version.chars().all(|ch| ch.is_ascii_digit() || ch == '.'))
}

fn is_pytest_command_flag_term(value: &str) -> bool {
    matches!(value, "m" | "q" | "s" | "v" | "vv")
}

fn infer_cargo_test_command_symbol_term(
    terms: &mut Vec<String>,
    filters: &mut SearchFilters,
    explicit_content_terms: bool,
) {
    if explicit_content_terms
        || terms.len() < 3
        || filters.file.is_some()
        || filters.path.is_some()
        || filters.symbol.is_some()
    {
        return;
    }

    let mut saw_cargo = false;
    let mut saw_test = false;
    let mut targets = Vec::new();
    for term in terms.iter() {
        let term = trim_location_token_wrappers(term);
        let lower = term.to_ascii_lowercase();
        if lower == "cargo" {
            saw_cargo = true;
            continue;
        }
        if lower == "test" {
            saw_test = true;
            continue;
        }
        let Some(target) = cargo_test_symbol_target(term) else {
            return;
        };
        targets.push(target);
    }

    let [target] = targets.as_slice() else {
        return;
    };
    if !saw_cargo || !saw_test {
        return;
    }

    filters.symbol = Some(target.clone());
    filters.symbol_kind = Some("function".to_string());
    terms.clear();
    filters.require_all = false;
}

fn cargo_test_symbol_target(value: &str) -> Option<String> {
    let target = value.rsplit("::").next().unwrap_or(value).trim();
    if !is_rust_test_symbol_name(target) {
        return None;
    }
    Some(target.to_string())
}

fn is_rust_test_symbol_name(value: &str) -> bool {
    let mut chars = value.chars();
    let Some(first) = chars.next() else {
        return false;
    };
    (first == '_' || first.is_ascii_alphabetic())
        && chars.all(|ch| ch == '_' || ch.is_ascii_alphanumeric())
}

fn infer_leading_pytest_node_id_term(
    terms: &mut Vec<String>,
    filters: &mut SearchFilters,
    explicit_content_terms: bool,
) {
    if explicit_content_terms
        || terms.is_empty()
        || filters.file.is_some()
        || filters.path.is_some()
        || filters.symbol.is_some()
    {
        return;
    }

    for index in 0..terms.len().min(4) {
        let Some(path) = pytest_node_id_path(&terms[index]) else {
            continue;
        };
        let path = strip_leading_current_dir_segments(path);
        let applied = if path.contains('/') {
            filters.path = Some(path);
            true
        } else if looks_like_file_name_query(&path) {
            filters.file = Some(path);
            true
        } else {
            false
        };
        if !applied {
            return;
        }
        filters.require_all = false;
        let status_prefixed =
            index > 0 && terms[..index].iter().all(|term| test_status_prefix(term));
        if index == 0 || status_prefixed {
            terms.drain(0..=index);
            if status_prefixed || diagnostic_message_prefix_terms(terms) {
                terms.clear();
            }
        } else {
            terms.remove(index);
        }
        return;
    }
}

fn infer_leading_location_term(
    terms: &mut Vec<String>,
    filters: &mut SearchFilters,
    explicit_content_terms: bool,
) {
    if explicit_content_terms
        || terms.is_empty()
        || filters.file.is_some()
        || filters.path.is_some()
        || filters.symbol.is_some()
    {
        return;
    }

    if let Some((path, target_line)) = github_actions_annotation_location_term(terms)
        .or_else(|| stack_block_location_term(terms))
        .or_else(|| diagnostic_block_location_term(terms))
    {
        apply_location_filter(path, target_line, terms, filters);
        return;
    }

    let diagnostic_prefix = terms
        .first()
        .map(|term| diagnostic_arrow_prefix(term))
        .unwrap_or(false);
    let Some((index, path, target_line, trailing_term)) = leading_location_term(terms) else {
        return;
    };
    let discard_remaining_diagnostic_terms = diagnostic_prefix && index > 0;
    let Some(path) = location_filter_path(path) else {
        return;
    };
    if apply_location_filter_path(path, target_line, filters) {
        let trailing_is_diagnostic = trailing_term
            .as_deref()
            .is_some_and(diagnostic_message_prefix_term);
        terms.drain(0..=index);
        if discard_remaining_diagnostic_terms
            || trailing_is_diagnostic
            || diagnostic_message_prefix_terms(terms)
        {
            terms.clear();
        } else if let Some(trailing_term) = trailing_term {
            terms.insert(0, trailing_term);
        }
    }
}

fn apply_location_filter(
    path: String,
    target_line: usize,
    terms: &mut Vec<String>,
    filters: &mut SearchFilters,
) {
    let Some(path) = location_filter_path(path) else {
        return;
    };
    if apply_location_filter_path(path, target_line, filters) {
        terms.clear();
    }
}

fn location_filter_path(path: String) -> Option<String> {
    let path = strip_leading_current_dir_segments(path);
    if path.is_empty() || path.starts_with('.') {
        return None;
    }
    Some(path)
}

fn apply_location_filter_path(
    path: String,
    target_line: usize,
    filters: &mut SearchFilters,
) -> bool {
    if path.contains('/') {
        filters.path = Some(path);
    } else if looks_like_file_name_query(&path) {
        filters.file = Some(path);
    } else {
        return false;
    }
    filters.target_line = Some(target_line);
    filters.require_all = false;
    true
}

fn diagnostic_block_location_term(terms: &[String]) -> Option<(String, usize)> {
    for (index, term) in terms.iter().take(32).enumerate() {
        if !diagnostic_arrow_prefix(term) {
            continue;
        }
        for location in terms[index + 1..].iter().take(3) {
            if let Some((path, line, _)) = split_leading_location_token(location) {
                return Some((path, line));
            }
        }
    }
    None
}

fn stack_block_location_term(terms: &[String]) -> Option<(String, usize)> {
    for start in 1..terms.len().min(64) {
        if let Some((_, path, line, _)) = python_file_location_terms(&terms[start..]) {
            return Some((path, line));
        }
        if go_stack_location_context(&terms[..start])
            && let Some((path, line, _)) = split_leading_location_token(&terms[start])
        {
            return Some((path, line));
        }
        if !stack_frame_prefix(&terms[start]) {
            continue;
        }
        for location in terms[start + 1..].iter().take(4) {
            if let Some((path, line, _)) = split_leading_location_token(location) {
                return Some((path, line));
            }
        }
    }
    None
}

fn github_actions_annotation_location_term(terms: &[String]) -> Option<(String, usize)> {
    for start in 0..terms.len().min(64) {
        if !trim_location_token_wrappers(&terms[start]).starts_with("::") {
            continue;
        }
        let end = (start + 8).min(terms.len());
        let text = terms[start..end].join(" ");
        if let Some(location) = parse_github_actions_annotation_location(&text) {
            return Some(location);
        }
    }
    None
}

fn parse_github_actions_annotation_location(value: &str) -> Option<(String, usize)> {
    let value = trim_location_token_wrappers(value);
    let rest = value.strip_prefix("::")?;
    let command_end = rest.find("::")?;
    let header = rest[..command_end].trim();
    let props_start = header
        .char_indices()
        .find_map(|(index, ch)| ch.is_whitespace().then_some(index))?;
    let command = &header[..props_start];
    if !matches!(
        command.to_ascii_lowercase().as_str(),
        "error" | "warning" | "notice" | "debug"
    ) {
        return None;
    }
    let props = header[props_start..].trim();
    let file = decode_action_property_value(action_property_value(props, "file")?)?;
    let path = normalize_location_token(&file)?;
    if !looks_like_location_path(&path) {
        return None;
    }
    let line = action_property_value(props, "line")
        .or_else(|| action_property_value(props, "startLine"))
        .and_then(parse_positive_usize)?;
    Some((path, line))
}

fn action_property_value<'a>(props: &'a str, name: &str) -> Option<&'a str> {
    props.split(',').find_map(|part| {
        let (key, value) = part.split_once('=')?;
        key.trim()
            .eq_ignore_ascii_case(name)
            .then_some(value.trim())
    })
}

fn decode_action_property_value(value: &str) -> Option<String> {
    decode_query_component(value).or_else(|| (!value.trim().is_empty()).then(|| value.to_string()))
}

fn go_stack_location_context(terms: &[String]) -> bool {
    terms.iter().rev().take(6).any(|term| {
        let term = trim_location_token_wrappers(term);
        term.eq_ignore_ascii_case("panic")
            || term.to_ascii_lowercase().starts_with("panic:")
            || term.ends_with("()")
    })
}

fn leading_location_term(terms: &[String]) -> Option<(usize, String, usize, Option<String>)> {
    if let Some(location) = python_file_location_terms(terms) {
        return Some(location);
    }
    for (index, term) in terms.iter().take(4).enumerate() {
        if index > 0 && !stack_location_prefix(&terms[..index]) {
            return None;
        }
        let Some((path, line, trailing)) = split_leading_location_token(term) else {
            continue;
        };
        return Some((index, path, line, trailing));
    }
    None
}

fn python_file_location_terms(terms: &[String]) -> Option<(usize, String, usize, Option<String>)> {
    if terms.len() < 4 || !term_eq_ignore_ascii_punctuation(&terms[0], "file") {
        return None;
    }
    let path = normalize_location_token(&terms[1])?;
    if !looks_like_location_path(&path) || !term_eq_ignore_ascii_punctuation(&terms[2], "line") {
        return None;
    }
    let (line, _) = split_leading_positive_number(trim_location_token_wrappers(&terms[3]))?;
    let (consume_index, trailing) =
        match terms.get(4).map(|term| trim_location_token_wrappers(term)) {
            Some(value) if value.eq_ignore_ascii_case("in") => {
                let trailing = terms.get(5).cloned();
                (trailing.as_ref().map(|_| 5).unwrap_or(4), trailing)
            }
            Some(value) if !value.is_empty() => (4, Some(value.to_string())),
            _ => (3, None),
        };
    Some((consume_index, path, line, trailing))
}

fn stack_location_prefix(terms: &[String]) -> bool {
    let Some(first) = terms.first() else {
        return true;
    };
    let first = trim_location_token_wrappers(first).to_ascii_lowercase();
    (stack_frame_prefix(&first) || diagnostic_arrow_prefix(&first))
        && terms.len() <= 3
        && terms[1..].iter().all(|term| {
            let term = trim_location_token_wrappers(term);
            !term.is_empty() && !term.contains(':') && !looks_like_location_path(term)
        })
}

fn stack_frame_prefix(term: &str) -> bool {
    matches!(
        trim_location_token_wrappers(term)
            .to_ascii_lowercase()
            .as_str(),
        "at" | "from" | "file"
    )
}

fn diagnostic_arrow_prefix(term: &str) -> bool {
    matches!(trim_location_token_wrappers(term), "-->" | "--")
}

fn diagnostic_message_prefix_terms(terms: &[String]) -> bool {
    terms
        .first()
        .is_some_and(|term| diagnostic_message_prefix_term(term))
}

fn diagnostic_message_prefix_term(term: &str) -> bool {
    let term = trim_location_token_wrappers(term)
        .trim_end_matches(':')
        .to_ascii_lowercase();
    matches!(
        term.as_str(),
        "error" | "warning" | "warn" | "notice" | "note" | "info" | "fatal"
    )
}

fn test_status_prefix(term: &str) -> bool {
    let term = trim_location_token_wrappers(term)
        .trim_end_matches(':')
        .to_ascii_lowercase();
    matches!(
        term.as_str(),
        "failed" | "failure" | "fail" | "error" | "errors" | "passed" | "pass" | "skipped"
    )
}

fn split_leading_location_token(token: &str) -> Option<(String, usize, Option<String>)> {
    let paren_normalized = normalize_parenthesized_location_token(token)?;
    if !paren_normalized.is_empty()
        && !paren_normalized.contains("://")
        && let Some(location) = split_parenthesized_line_location(&paren_normalized)
    {
        return Some(location);
    }
    if !paren_normalized.is_empty()
        && !paren_normalized.contains("://")
        && let Some(location) = split_embedded_parenthesized_location(&paren_normalized)
    {
        return Some(location);
    }

    let normalized = normalize_location_token(token)?;
    if normalized.is_empty() || normalized.contains("://") {
        return None;
    }
    if let Some(location) = split_hash_line_anchor(&normalized) {
        return Some(location);
    }
    if let Some(location) = split_parenthesized_line_location(&normalized) {
        return Some(location);
    }

    for (path_end, _) in normalized.match_indices(':') {
        let path = &normalized[..path_end];
        if !looks_like_location_path(path) {
            continue;
        }
        let rest = &normalized[path_end + 1..];
        let Some((line, after_line)) = split_leading_positive_number(rest) else {
            continue;
        };
        if after_line.is_empty() {
            return Some((path.to_string(), line, None));
        }
        if let Some(after_range) = strip_colon_line_range(after_line) {
            return Some((path.to_string(), line, non_empty_location_tail(after_range)));
        }
        let Some(after_line_colon) = after_line.strip_prefix(':') else {
            continue;
        };
        if after_line_colon.is_empty() {
            return Some((path.to_string(), line, None));
        }
        if let Some((_, after_column)) = split_leading_positive_number(after_line_colon) {
            if after_column.is_empty() {
                return Some((path.to_string(), line, None));
            }
            if let Some(text) = after_column.strip_prefix(':') {
                return Some((path.to_string(), line, non_empty_location_tail(text)));
            }
        }
        return Some((
            path.to_string(),
            line,
            non_empty_location_tail(after_line_colon),
        ));
    }
    None
}

fn split_hash_line_anchor(value: &str) -> Option<(String, usize, Option<String>)> {
    let lower = value.to_ascii_lowercase();
    let anchor_start = lower.find("#l")?;
    let path = &value[..anchor_start];
    if !looks_like_location_path(path) {
        return None;
    }
    let (line, rest) = split_leading_positive_number(&value[anchor_start + 2..])?;
    let rest = strip_hash_line_range(rest);
    let trailing = rest
        .strip_prefix(':')
        .and_then(non_empty_location_tail)
        .or_else(|| non_empty_location_tail(rest));
    Some((path.to_string(), line, trailing))
}

fn strip_hash_line_range(value: &str) -> &str {
    let value = strip_hash_line_column(value);
    let Some(rest) = value.strip_prefix('-') else {
        return value;
    };
    let rest = rest
        .strip_prefix('L')
        .or_else(|| rest.strip_prefix('l'))
        .unwrap_or(rest);
    split_leading_positive_number(rest)
        .map(|(_, after_range)| strip_hash_line_column(after_range))
        .unwrap_or(value)
}

fn strip_hash_line_column(value: &str) -> &str {
    let Some(rest) = value.strip_prefix('C').or_else(|| value.strip_prefix('c')) else {
        return value;
    };
    split_leading_positive_number(rest)
        .map(|(_, after_column)| after_column)
        .unwrap_or(value)
}

fn normalize_location_token(token: &str) -> Option<String> {
    let token = markdown_link_target(token).unwrap_or(token);
    let normalized = trim_location_token_wrappers(token).replace('\\', "/");
    let normalized = code_hosted_location_path(&normalized).unwrap_or(normalized);
    (!normalized.is_empty()).then_some(normalized)
}

fn normalize_parenthesized_location_token(token: &str) -> Option<String> {
    let token = markdown_link_target(token).unwrap_or(token);
    let normalized = trim_outer_location_wrappers(token)
        .trim_end_matches(|ch| matches!(ch, ',' | ';' | '"' | '\''))
        .replace('\\', "/");
    let normalized = code_hosted_location_path(&normalized).unwrap_or(normalized);
    (!normalized.is_empty()).then_some(normalized)
}

fn trim_outer_location_wrappers(mut token: &str) -> &str {
    token = token.trim();
    while let Some(stripped) = strip_balanced_outer_wrapper(token) {
        token = stripped.trim();
    }
    token
        .trim_start_matches(|ch| matches!(ch, '[' | '{' | '<' | '"' | '\''))
        .trim_end_matches(|ch| matches!(ch, ']' | '}' | '>' | '"' | '\''))
}

fn strip_balanced_outer_wrapper(token: &str) -> Option<&str> {
    let (open, close) = match token.as_bytes().first().copied()? {
        b'(' => ('(', ')'),
        b'[' => ('[', ']'),
        b'{' => ('{', '}'),
        b'<' => ('<', '>'),
        _ => return None,
    };
    if !token.ends_with(close) {
        return None;
    }
    let mut depth = 0usize;
    for (index, ch) in token.char_indices() {
        if ch == open {
            depth += 1;
        } else if ch == close {
            depth = depth.checked_sub(1)?;
            if depth == 0 && index + ch.len_utf8() != token.len() {
                return None;
            }
        }
    }
    (depth == 0).then_some(&token[open.len_utf8()..token.len() - close.len_utf8()])
}

fn markdown_link_target(token: &str) -> Option<&str> {
    let token = token.trim();
    let marker = token.rfind("](")?;
    let after_marker = &token[marker + 2..];
    let end = after_marker.find(')')?;
    let target = after_marker[..end].trim();
    (!target.is_empty()).then_some(target)
}

fn code_hosted_location_path(value: &str) -> Option<String> {
    if !value.contains("://") {
        return None;
    }
    if let Some(path) = azure_devops_location_path(value) {
        return Some(path);
    }
    let query_start = value.find('?').unwrap_or(value.len());
    let anchor_start = value.find('#').unwrap_or(value.len());
    let suffix_start = query_start.min(anchor_start);
    let (base, suffix) = value.split_at(suffix_start);
    let line_anchor = hosted_line_anchor_suffix(suffix);
    let lower_base = base.to_ascii_lowercase();
    if let Some(path) = raw_github_location_path(base, &lower_base) {
        return Some(format!("{path}{line_anchor}"));
    }
    if let Some(path) = bitbucket_location_path(base, &lower_base) {
        return Some(format!("{path}{line_anchor}"));
    }
    for marker in [
        "/-/blob/", "/blob/", "/-/raw/", "/raw/", "/-/tree/", "/tree/",
    ] {
        let Some(marker_start) = lower_base.find(marker) else {
            continue;
        };
        let after_marker = &base[marker_start + marker.len()..];
        let path = if lower_base.contains("sourcegraph.com/") && marker.starts_with("/-/") {
            after_marker
        } else {
            let Some(path) = hosted_repo_path_after_ref(after_marker) else {
                continue;
            };
            path
        };
        if looks_like_location_path(path) {
            return Some(format!("{path}{line_anchor}"));
        }
    }
    None
}

fn azure_devops_location_path(value: &str) -> Option<String> {
    let lower = value.to_ascii_lowercase();
    if !(lower.contains("dev.azure.com/") || lower.contains(".visualstudio.com/"))
        || !lower.contains("/_git/")
    {
        return None;
    }
    let query_start = value.find('?')?;
    let query_end = value.find('#').unwrap_or(value.len());
    let query = &value[query_start + 1..query_end];
    let path = decode_query_component(query_value(query, "path")?)?;
    let path = path.trim_start_matches('/').to_string();
    if !looks_like_location_path(&path) {
        return None;
    }
    Some(format!("{path}{}", azure_devops_line_anchor_suffix(query)))
}

fn azure_devops_line_anchor_suffix(query: &str) -> String {
    let Some(line) = query_value(query, "line")
        .or_else(|| query_value(query, "lineStart"))
        .and_then(|value| value.parse::<usize>().ok())
    else {
        return String::new();
    };
    if let Some(end) = query_value(query, "lineEnd").and_then(|value| value.parse::<usize>().ok())
        && end >= line
    {
        return format!("#L{line}-L{end}");
    }
    format!("#L{line}")
}

fn query_value<'a>(query: &'a str, name: &str) -> Option<&'a str> {
    query.split('&').find_map(|part| {
        let part = part.trim_start_matches(|ch| matches!(ch, '?' | '&'));
        let (key, value) = part.split_once('=')?;
        key.eq_ignore_ascii_case(name).then_some(value)
    })
}

fn decode_query_component(value: &str) -> Option<String> {
    let mut decoded = String::with_capacity(value.len());
    let mut chars = value.as_bytes().iter().copied().peekable();
    while let Some(byte) = chars.next() {
        match byte {
            b'+' => decoded.push(' '),
            b'%' => {
                let hi = chars.next()?;
                let lo = chars.next()?;
                let value = hex_value(hi)? << 4 | hex_value(lo)?;
                decoded.push(char::from(value));
            }
            _ => decoded.push(char::from(byte)),
        }
    }
    Some(decoded)
}

fn hex_value(byte: u8) -> Option<u8> {
    match byte {
        b'0'..=b'9' => Some(byte - b'0'),
        b'a'..=b'f' => Some(byte - b'a' + 10),
        b'A'..=b'F' => Some(byte - b'A' + 10),
        _ => None,
    }
}

fn raw_github_location_path<'a>(base: &'a str, lower_base: &str) -> Option<&'a str> {
    let marker = "raw.githubusercontent.com/";
    let marker_start = lower_base.find(marker)?;
    let after_host = &base[marker_start + marker.len()..];
    let (_, after_owner) = after_host.split_once('/')?;
    let (_, after_repo) = after_owner.split_once('/')?;
    hosted_repo_path_after_ref(after_repo)
}

fn bitbucket_location_path<'a>(base: &'a str, lower_base: &str) -> Option<&'a str> {
    let marker = "bitbucket.org/";
    let marker_start = lower_base.find(marker)?;
    let after_host = &base[marker_start + marker.len()..];
    let (_, after_owner) = after_host.split_once('/')?;
    let (_, after_repo) = after_owner.split_once('/')?;
    let after_src = after_repo.strip_prefix("src/")?;
    hosted_repo_path_after_ref(after_src)
}

fn hosted_repo_path_after_ref(after_marker: &str) -> Option<&str> {
    let (ref_prefix, default_path) = after_marker.split_once('/')?;
    if !looks_like_location_path(default_path) {
        return None;
    }
    if !hosted_slashy_branch_namespace(ref_prefix) {
        return Some(default_path);
    }

    let mut best: Option<(&str, usize)> = None;
    for (slash_index, _) in after_marker.match_indices('/') {
        let candidate = &after_marker[slash_index + 1..];
        if !looks_like_location_path(candidate) {
            continue;
        }
        let score = hosted_repo_path_score(candidate);
        let replace = best
            .map(|(best_path, best_score)| {
                score > best_score || (score == best_score && candidate.len() > best_path.len())
            })
            .unwrap_or(true);
        if replace {
            best = Some((candidate, score));
        }
    }
    best.map(|(path, _)| path).or(Some(default_path))
}

fn hosted_slashy_branch_namespace(segment: &str) -> bool {
    matches!(
        segment.to_ascii_lowercase().as_str(),
        "bug"
            | "bugfix"
            | "chore"
            | "dependabot"
            | "dev"
            | "feature"
            | "feat"
            | "fix"
            | "hotfix"
            | "renovate"
            | "release"
            | "revert"
            | "topic"
            | "user"
            | "users"
    )
}

fn hosted_repo_path_score(path: &str) -> usize {
    let lower = path.to_ascii_lowercase();
    if hosted_manifest_path(&lower) {
        return 120;
    }
    let root = lower.split('/').next().unwrap_or_default();
    if hosted_repo_root_segment(root) {
        return 100;
    }
    1
}

fn hosted_manifest_path(path: &str) -> bool {
    matches!(
        path,
        "agents.md"
            | "build.bazel"
            | "cargo.lock"
            | "cargo.toml"
            | "dockerfile"
            | "go.mod"
            | "go.sum"
            | "justfile"
            | "makefile"
            | "module.bazel"
            | "package.json"
            | "pnpm-lock.yaml"
            | "pom.xml"
            | "pyproject.toml"
            | "requirements.txt"
            | "yarn.lock"
    ) || path.starts_with("readme.")
}

fn hosted_repo_root_segment(segment: &str) -> bool {
    matches!(
        segment,
        ".github"
            | "app"
            | "apps"
            | "bin"
            | "cmd"
            | "config"
            | "crates"
            | "docs"
            | "examples"
            | "fixtures"
            | "include"
            | "internal"
            | "lib"
            | "packages"
            | "pkg"
            | "scripts"
            | "services"
            | "src"
            | "test"
            | "tests"
            | "tools"
    )
}

fn hosted_line_anchor_suffix(suffix: &str) -> String {
    if suffix.is_empty() {
        return String::new();
    }
    if let Some(anchor_start) = suffix.find('#') {
        let anchor = &suffix[anchor_start..];
        if anchor
            .strip_prefix("#L")
            .or_else(|| anchor.strip_prefix("#l"))
            .and_then(split_leading_positive_number)
            .is_some()
        {
            return anchor.to_string();
        }
        if let Some(line_anchor) = bitbucket_lines_anchor_suffix(anchor) {
            return line_anchor;
        }
    }
    let Some(query_start) = suffix.find('?') else {
        return String::new();
    };
    let query = &suffix[query_start + 1..suffix.find('#').unwrap_or(suffix.len())];
    for part in query.split('&') {
        let part = part.trim_start_matches(|ch| matches!(ch, '?' | '&'));
        let Some(line_spec) = part.strip_prefix('L').or_else(|| part.strip_prefix('l')) else {
            continue;
        };
        let Some((line, rest)) = split_leading_positive_number(line_spec) else {
            continue;
        };
        let rest = rest.trim_start_matches(':');
        if let Some(range) = rest.strip_prefix('-') {
            let range = range
                .strip_prefix('L')
                .or_else(|| range.strip_prefix('l'))
                .unwrap_or(range);
            if let Some((end, _)) = split_leading_positive_number(range) {
                return format!("#L{line}-L{end}");
            }
        }
        return format!("#L{line}");
    }
    String::new()
}

fn bitbucket_lines_anchor_suffix(anchor: &str) -> Option<String> {
    let line_spec = anchor
        .strip_prefix("#lines-")
        .or_else(|| anchor.strip_prefix("#LINES-"))?;
    let (line, rest) = split_leading_positive_number(line_spec)?;
    if let Some(range) = rest.strip_prefix(':')
        && let Some((end, _)) = split_leading_positive_number(range)
    {
        return Some(format!("#L{line}-L{end}"));
    }
    Some(format!("#L{line}"))
}

fn trim_location_token_wrappers(token: &str) -> &str {
    token
        .trim()
        .trim_start_matches(|ch| matches!(ch, '(' | '[' | '{' | '<' | '"' | '\''))
        .trim_end_matches(|ch| matches!(ch, ')' | ']' | '}' | '>' | '"' | '\'' | ',' | ';'))
}

fn term_eq_ignore_ascii_punctuation(term: &str, expected: &str) -> bool {
    trim_location_token_wrappers(term).eq_ignore_ascii_case(expected)
}

fn strip_location_suffix(value: &str) -> (String, Option<usize>) {
    if let Some(path) = pytest_node_id_path(value) {
        return (path, None);
    }
    if let Some((path, line, _)) = normalize_parenthesized_location_token(value).and_then(|value| {
        (!value.contains("://"))
            .then(|| split_parenthesized_line_location(&value))
            .flatten()
    }) {
        return (path, Some(line));
    }
    let normalized =
        normalize_location_token(value).unwrap_or_else(|| value.trim().replace('\\', "/"));
    if let Some((path, line, _)) = split_hash_line_anchor(&normalized) {
        return (path, Some(line));
    }
    if let Some((path, line, _)) = split_colon_line_range_location(&normalized) {
        return (path, Some(line));
    }
    if let Some((path, line, _)) = split_parenthesized_line_location(&normalized) {
        return (path, Some(line));
    }
    let Some((prefix, column_or_line)) = split_numeric_suffix(&normalized) else {
        return (normalized, None);
    };
    let (path, line) = split_numeric_suffix(prefix).unwrap_or((prefix, column_or_line));
    if looks_like_location_path(path) {
        (path.to_string(), Some(line))
    } else {
        (normalized, None)
    }
}

fn pytest_node_id_path(value: &str) -> Option<String> {
    let token = trim_outer_location_wrappers(value).replace('\\', "/");
    let (path, node) = token.split_once("::")?;
    if node.is_empty() || node.starts_with(':') {
        return None;
    }
    let path = path.trim();
    if !looks_like_location_path(path) {
        return None;
    }
    Some(path.to_string())
}

fn strip_leading_current_dir_segments(mut value: String) -> String {
    while let Some(stripped) = value.strip_prefix("./") {
        value = stripped.to_string();
    }
    value
}

fn split_numeric_suffix(value: &str) -> Option<(&str, usize)> {
    let (prefix, suffix) = value.rsplit_once(':')?;
    if suffix.is_empty() || !suffix.bytes().all(|byte| byte.is_ascii_digit()) {
        return None;
    }
    let number = suffix.parse::<usize>().ok()?;
    (number > 0).then_some((prefix, number))
}

fn split_colon_line_range_location(value: &str) -> Option<(String, usize, Option<String>)> {
    for (path_end, _) in value.match_indices(':') {
        let path = &value[..path_end];
        if !looks_like_location_path(path) {
            continue;
        }
        let rest = &value[path_end + 1..];
        let Some((line, after_line)) = split_leading_positive_number(rest) else {
            continue;
        };
        let Some(after_range) = strip_colon_line_range(after_line) else {
            continue;
        };
        return Some((path.to_string(), line, non_empty_location_tail(after_range)));
    }
    None
}

fn split_parenthesized_line_location(value: &str) -> Option<(String, usize, Option<String>)> {
    let close = value.find(')')?;
    let before_close = &value[..close];
    let open = before_close.rfind('(')?;
    let path = &before_close[..open];
    if !looks_like_location_path(path) {
        return None;
    }
    let (line, after_line) = split_leading_positive_number(before_close[open + 1..].trim())?;
    let after_line = after_line.trim_start();
    if !after_line.is_empty() {
        let after_column_prefix = after_line.strip_prefix(',')?.trim_start();
        let (_, after_column) = split_leading_positive_number(after_column_prefix)?;
        if !after_column.trim().is_empty() {
            return None;
        }
    }
    let trailing = match value[close + 1..].trim_start() {
        "" => None,
        rest => non_empty_location_tail(rest.strip_prefix(':').unwrap_or(rest)),
    };
    Some((path.to_string(), line, trailing))
}

fn split_embedded_parenthesized_location(value: &str) -> Option<(String, usize, Option<String>)> {
    let close = value.rfind(')')?;
    let before_close = &value[..close];
    let open = before_close.rfind('(')?;
    let prefix = before_close[..open].trim();
    if prefix.is_empty()
        || prefix.chars().any(char::is_whitespace)
        || looks_like_location_path(prefix)
    {
        return None;
    }
    let inner = before_close[open + 1..].trim();
    if inner.is_empty() || inner.contains("://") {
        return None;
    }
    let (path, line, _) = split_location_fragment(inner)?;
    let trailing = match value[close + 1..].trim_start() {
        "" => None,
        rest => non_empty_location_tail(rest.strip_prefix(':').unwrap_or(rest)),
    };
    Some((path, line, trailing))
}

fn split_location_fragment(value: &str) -> Option<(String, usize, Option<String>)> {
    if let Some(location) = split_hash_line_anchor(value) {
        return Some(location);
    }
    if let Some(location) = split_parenthesized_line_location(value) {
        return Some(location);
    }
    for (path_end, _) in value.match_indices(':') {
        let path = &value[..path_end];
        if !looks_like_location_path(path) {
            continue;
        }
        let rest = &value[path_end + 1..];
        let (line, after_line) = split_leading_positive_number(rest)?;
        if after_line.is_empty() {
            return Some((path.to_string(), line, None));
        }
        if let Some(after_range) = strip_colon_line_range(after_line) {
            return Some((path.to_string(), line, non_empty_location_tail(after_range)));
        }
        let Some(after_line_colon) = after_line.strip_prefix(':') else {
            continue;
        };
        if after_line_colon.is_empty() {
            return Some((path.to_string(), line, None));
        }
        if let Some((_, after_column)) = split_leading_positive_number(after_line_colon)
            && after_column.is_empty()
        {
            return Some((path.to_string(), line, None));
        }
    }
    None
}

fn strip_colon_line_range(value: &str) -> Option<&str> {
    let rest = value.strip_prefix('-')?;
    let (_, after_range) = split_leading_positive_number(rest)?;
    if after_range.is_empty() {
        return Some("");
    }
    after_range.strip_prefix(':')
}

fn split_leading_positive_number(value: &str) -> Option<(usize, &str)> {
    let digit_count = value
        .bytes()
        .take_while(|byte| byte.is_ascii_digit())
        .count();
    if digit_count == 0 {
        return None;
    }
    let number = value[..digit_count].parse::<usize>().ok()?;
    (number > 0).then_some((number, &value[digit_count..]))
}

fn non_empty_location_tail(value: &str) -> Option<String> {
    let value = value.trim();
    (!value.is_empty()).then(|| value.to_string())
}

fn looks_like_location_path(path: &str) -> bool {
    !path.is_empty()
        && !path.contains("://")
        && !path.ends_with(':')
        && (path.contains('/') || looks_like_file_name_query(path))
}

fn looks_like_file_name_query(term: &str) -> bool {
    let lower = term.to_ascii_lowercase();
    matches!(
        lower.as_str(),
        "readme"
            | "makefile"
            | "dockerfile"
            | "justfile"
            | "gemfile"
            | "cargo.lock"
            | "go.mod"
            | "go.sum"
            | "pom.xml"
            | "build.gradle"
            | "build.gradle.kts"
            | "settings.gradle"
            | "settings.gradle.kts"
            | "yarn.lock"
            | "bun.lock"
            | "bun.lockb"
            | "agents.md"
    ) || lower.starts_with("readme.")
        || lower.starts_with("license.")
        || lower.starts_with("changelog.")
        || lower.starts_with("contributing.")
        || lower
            .rsplit_once('.')
            .is_some_and(|(_, extension)| agent_path_query_extension(extension))
}

fn agent_path_query_extension(extension: &str) -> bool {
    matches!(
        extension,
        "rs" | "toml"
            | "json"
            | "yaml"
            | "yml"
            | "xml"
            | "gradle"
            | "md"
            | "py"
            | "js"
            | "jsx"
            | "ts"
            | "tsx"
            | "go"
            | "java"
            | "kt"
            | "kts"
            | "swift"
            | "rb"
    )
}

pub fn normalize_symbol_kind(value: &str) -> String {
    match value.trim().to_ascii_lowercase().as_str() {
        "fn" | "func" | "function" | "functions" | "method" | "methods" | "def" => {
            "function".to_string()
        }
        "consts" | "constant" | "constants" => "const".to_string(),
        "vars" | "variable" | "variables" => "var".to_string(),
        "classes" => "class".to_string(),
        "structs" => "struct".to_string(),
        "enums" => "enum".to_string(),
        "interfaces" => "interface".to_string(),
        "traits" => "trait".to_string(),
        "types" => "type".to_string(),
        "targets" | "recipe" | "recipes" | "task" | "tasks" => "target".to_string(),
        "packages" => "package".to_string(),
        "services" | "compose-service" | "compose-services" => "service".to_string(),
        "stages" | "docker-stage" | "docker-stages" => "stage".to_string(),
        "binary" | "binaries" | "bins" => "bin".to_string(),
        "examples" => "example".to_string(),
        "benches" | "benchmark" | "benchmarks" => "bench".to_string(),
        "scripts" | "npm-script" | "npm-scripts" | "package-script" | "package-scripts" => {
            "script".to_string()
        }
        other => other.to_string(),
    }
}

fn symbol_kind_from_shorthand_key(key: &str) -> Option<String> {
    let kind = normalize_symbol_kind(key);
    matches!(
        kind.as_str(),
        "function"
            | "class"
            | "interface"
            | "struct"
            | "enum"
            | "trait"
            | "const"
            | "var"
            | "target"
            | "script"
            | "package"
            | "service"
            | "stage"
            | "bin"
            | "example"
            | "bench"
    )
    .then_some(kind)
}

fn symbol_kind_from_type_value(value: &str) -> Option<String> {
    let kind = normalize_symbol_kind(value);
    matches!(
        kind.as_str(),
        "function"
            | "class"
            | "interface"
            | "struct"
            | "enum"
            | "trait"
            | "type"
            | "const"
            | "let"
            | "var"
            | "target"
            | "script"
            | "package"
            | "bin"
            | "example"
            | "bench"
    )
    .then_some(kind)
}

fn parse_boolish(value: &str) -> Option<bool> {
    match value.to_ascii_lowercase().as_str() {
        "1" | "true" | "yes" | "y" => Some(true),
        "0" | "false" | "no" | "n" => Some(false),
        _ => None,
    }
}

fn parse_positive_usize(value: &str) -> Option<usize> {
    value
        .trim()
        .parse::<usize>()
        .ok()
        .filter(|value| *value > 0)
}

fn split_query(input: &str) -> Vec<String> {
    let mut tokens = Vec::new();
    let mut current = String::new();
    let mut chars = input.chars().peekable();
    let mut in_quote = false;
    let mut quote_char = '\0';

    while let Some(ch) = chars.next() {
        if in_quote {
            if ch == '\\' {
                if let Some(next) = chars.next() {
                    current.push(next);
                }
            } else if ch == quote_char {
                in_quote = false;
            } else {
                current.push(ch);
            }
            continue;
        }

        match ch {
            '"' | '\'' => {
                in_quote = true;
                quote_char = ch;
            }
            ch if ch.is_whitespace() => {
                if !current.is_empty() {
                    tokens.push(std::mem::take(&mut current));
                }
            }
            _ => current.push(ch),
        }
    }

    if !current.is_empty() {
        tokens.push(current);
    }
    tokens
}

pub fn query_text(terms: &[String], filters: &SearchFilters) -> String {
    let mut pieces = terms.to_vec();
    if let Some(symbol) = &filters.symbol {
        pieces.push(symbol.clone());
    }
    pieces.join(" ")
}

pub fn query_with_filters_text(terms: &[String], filters: &SearchFilters) -> String {
    let mut pieces = Vec::new();
    if filters.match_any {
        pieces.push("mode:any".to_string());
    }
    pieces.extend(
        terms
            .iter()
            .map(|term| query_token_text(term))
            .collect::<Vec<_>>(),
    );
    push_query_filter(&mut pieces, "file", filters.file.as_deref(), false);
    push_query_filter(&mut pieces, "path", filters.path.as_deref(), false);
    push_query_filter(&mut pieces, "lang", filters.language.as_deref(), false);
    push_query_filter(&mut pieces, "ext", filters.extension.as_deref(), false);
    push_query_filter(&mut pieces, "symbol", filters.symbol.as_deref(), false);
    push_query_filter(&mut pieces, "kind", filters.symbol_kind.as_deref(), false);
    push_query_filter(&mut pieces, "repo", filters.repo.as_deref(), false);
    push_query_filter(&mut pieces, "branch", filters.branch.as_deref(), false);
    push_query_filter(&mut pieces, "origin", filters.origin.as_deref(), false);
    push_query_filter(&mut pieces, "dep", filters.dependency.as_deref(), false);
    push_query_filter(&mut pieces, "import", filters.import.as_deref(), false);
    if let Some(test) = filters.test {
        pieces.push(format!("test:{test}"));
    }
    if let Some(generated) = filters.generated {
        pieces.push(format!("generated:{generated}"));
    }
    if let Some(code) = filters.code {
        pieces.push(format!("code:{code}"));
    }
    if let Some(line) = filters.target_line {
        pieces.push(format!("line:{line}"));
    }
    for value in &filters.exclude_file {
        push_query_filter(&mut pieces, "file", Some(value), true);
    }
    for value in &filters.exclude_path {
        push_query_filter(&mut pieces, "path", Some(value), true);
    }
    for value in &filters.exclude_language {
        push_query_filter(&mut pieces, "lang", Some(value), true);
    }
    for value in &filters.exclude_extension {
        push_query_filter(&mut pieces, "ext", Some(value), true);
    }
    for value in &filters.exclude_symbol {
        push_query_filter(&mut pieces, "symbol", Some(value), true);
    }
    for value in &filters.exclude_symbol_kind {
        push_query_filter(&mut pieces, "kind", Some(value), true);
    }
    for value in &filters.exclude_repo {
        push_query_filter(&mut pieces, "repo", Some(value), true);
    }
    for value in &filters.exclude_branch {
        push_query_filter(&mut pieces, "branch", Some(value), true);
    }
    for value in &filters.exclude_origin {
        push_query_filter(&mut pieces, "origin", Some(value), true);
    }
    for value in &filters.exclude_dependency {
        push_query_filter(&mut pieces, "dep", Some(value), true);
    }
    for value in &filters.exclude_import {
        push_query_filter(&mut pieces, "import", Some(value), true);
    }
    for value in &filters.exclude_content {
        push_query_filter(&mut pieces, "content", Some(value), true);
    }
    pieces.join(" ")
}

fn push_query_filter(pieces: &mut Vec<String>, key: &str, value: Option<&str>, negated: bool) {
    let Some(value) = value else {
        return;
    };
    if value.trim().is_empty() {
        return;
    }
    let prefix = if negated { "-" } else { "" };
    pieces.push(format!("{prefix}{key}:{}", query_token_text(value)));
}

fn query_token_text(value: &str) -> String {
    if value
        .chars()
        .any(|ch| ch.is_whitespace() || matches!(ch, '"' | '\'' | '\\'))
    {
        let escaped = value.replace('\\', "\\\\").replace('"', "\\\"");
        format!("\"{escaped}\"")
    } else {
        value.to_string()
    }
}

pub fn query_phrases(terms: &[String]) -> Vec<String> {
    let mut phrases = terms
        .iter()
        .map(|term| normalize_phrase_text(term))
        .filter(|term| term.chars().any(char::is_whitespace) && tokenize(term).len() > 1)
        .collect::<Vec<_>>();
    phrases.sort();
    phrases.dedup();
    phrases
}

pub(crate) fn normalize_phrase_text(input: &str) -> String {
    let mut normalized = String::new();
    let mut last_was_space = true;
    for ch in identifier_boundary_text(input).chars() {
        if ch.is_alphanumeric() {
            for lower in ch.to_lowercase() {
                normalized.push(lower);
            }
            last_was_space = false;
        } else if !last_was_space {
            normalized.push(' ');
            last_was_space = true;
        }
    }
    if normalized.ends_with(' ') {
        normalized.pop();
    }
    normalized
}

pub fn merge_filters(mut base: SearchFilters, parsed: SearchFilters) -> SearchFilters {
    if parsed.file.is_some() {
        base.file = parsed.file;
    }
    if parsed.path.is_some() {
        base.path = parsed.path;
    }
    if parsed.language.is_some() {
        base.language = parsed.language;
    }
    if parsed.extension.is_some() {
        base.extension = parsed.extension;
    }
    if parsed.symbol.is_some() {
        base.symbol = parsed.symbol;
    }
    if parsed.symbol_kind.is_some() {
        base.symbol_kind = parsed.symbol_kind;
    }
    if parsed.repo.is_some() {
        base.repo = parsed.repo;
    }
    if parsed.branch.is_some() {
        base.branch = parsed.branch;
    }
    if parsed.origin.is_some() {
        base.origin = parsed.origin;
    }
    if parsed.dependency.is_some() {
        base.dependency = parsed.dependency;
    }
    if parsed.import.is_some() {
        base.import = parsed.import;
    }
    if parsed.test.is_some() {
        base.test = parsed.test;
    }
    if parsed.generated.is_some() {
        base.generated = parsed.generated;
    }
    if parsed.code.is_some() {
        base.code = parsed.code;
    }
    if parsed.target_line.is_some() {
        base.target_line = parsed.target_line;
    }
    if base.match_any || parsed.match_any {
        base.match_any |= parsed.match_any;
        base.require_all = false;
    } else {
        base.require_all |= parsed.require_all;
    }
    base.exclude_file.extend(parsed.exclude_file);
    base.exclude_path.extend(parsed.exclude_path);
    base.exclude_language.extend(parsed.exclude_language);
    base.exclude_extension.extend(parsed.exclude_extension);
    base.exclude_symbol.extend(parsed.exclude_symbol);
    base.exclude_symbol_kind.extend(parsed.exclude_symbol_kind);
    base.exclude_repo.extend(parsed.exclude_repo);
    base.exclude_branch.extend(parsed.exclude_branch);
    base.exclude_origin.extend(parsed.exclude_origin);
    base.exclude_dependency.extend(parsed.exclude_dependency);
    base.exclude_import.extend(parsed.exclude_import);
    base.exclude_content.extend(parsed.exclude_content);
    base
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_terms_quotes_filters_and_negatives() {
        let parsed = parse_query(
            r#"symbol:SessionManager lang:rust -dir:docs "issue token" -deprecated test:false"#,
        );

        assert_eq!(parsed.terms, vec!["issue token"]);
        assert_eq!(parsed.filters.symbol.as_deref(), Some("SessionManager"));
        assert_eq!(parsed.filters.language.as_deref(), Some("rust"));
        assert_eq!(parsed.filters.exclude_path, vec!["docs"]);
        assert_eq!(parsed.filters.exclude_content, vec!["deprecated"]);
        assert_eq!(parsed.filters.test, Some(false));
        assert_eq!(query_phrases(&parsed.terms), vec!["issue token"]);

        let phrase = parse_query(r#"SessionManager -"legacy token""#);
        assert_eq!(phrase.terms, vec!["SessionManager"]);
        assert_eq!(phrase.filters.exclude_content, vec!["legacy token"]);
    }

    #[test]
    fn parses_aliases_booleans_escapes_and_negatives() {
        let parsed = parse_query(
            r#"file:'auth service.rs' language:Rust extension:.RS repo:orient branch:main origin:example test:true !ext:md !repo:old -branch:wip -origin:legacy "quoted \"token\"""#,
        );

        assert_eq!(parsed.terms, vec![r#"quoted "token""#]);
        assert_eq!(parsed.filters.file.as_deref(), Some("auth service.rs"));
        assert_eq!(parsed.filters.language.as_deref(), Some("rust"));
        assert_eq!(parsed.filters.extension.as_deref(), Some("rs"));
        assert_eq!(parsed.filters.repo.as_deref(), Some("orient"));
        assert_eq!(parsed.filters.branch.as_deref(), Some("main"));
        assert_eq!(parsed.filters.origin.as_deref(), Some("example"));
        assert_eq!(parsed.filters.test, Some(true));
        assert_eq!(parsed.filters.exclude_extension, vec!["md"]);
        assert_eq!(parsed.filters.exclude_repo, vec!["old"]);
        assert_eq!(parsed.filters.exclude_branch, vec!["wip"]);
        assert_eq!(parsed.filters.exclude_origin, vec!["legacy"]);

        let bang = parse_query("!test !generated !vendor !deprecated != operator");
        assert_eq!(bang.terms, vec!["!=", "operator"]);
        assert_eq!(bang.filters.test, Some(false));
        assert_eq!(bang.filters.generated, Some(false));
        assert_eq!(bang.filters.exclude_content, vec!["deprecated"]);
        assert_eq!(bang.filters.exclude_path, vec!["vendor"]);

        let positive_test = parse_query("test parser support");
        assert_eq!(positive_test.terms, vec!["test", "parser", "support"]);
        assert_eq!(positive_test.filters.test, None);

        let path_negated = parse_query("!path:vendor SessionManager");
        assert_eq!(path_negated.terms, vec!["SessionManager"]);
        assert_eq!(path_negated.filters.exclude_path, vec!["vendor"]);

        let docs = parse_query("!Docs SessionManager");
        assert_eq!(docs.terms, vec!["SessionManager"]);
        assert_eq!(docs.filters.code, Some(true));

        let noisy_dirs = parse_query("!node-modules !third_party !target SessionManager");
        assert_eq!(noisy_dirs.terms, vec!["SessionManager"]);
        assert_eq!(
            noisy_dirs.filters.exclude_path,
            vec!["node_modules", "third_party", "target"]
        );

        let negative_aliases = parse_query(
            "not:docs without:path:vendor exclude:generated exclude:deprecated not:content:legacy",
        );
        assert!(negative_aliases.terms.is_empty());
        assert_eq!(negative_aliases.filters.code, Some(true));
        assert_eq!(negative_aliases.filters.generated, Some(false));
        assert_eq!(negative_aliases.filters.exclude_path, vec!["vendor"]);
        assert_eq!(
            negative_aliases.filters.exclude_content,
            vec!["deprecated", "legacy"]
        );

        let positive_docs = parse_query("docs SessionManager");
        assert_eq!(positive_docs.terms, vec!["docs", "SessionManager"]);
        assert_eq!(positive_docs.filters.code, None);

        let positive_vendor = parse_query("vendor SessionManager");
        assert_eq!(positive_vendor.terms, vec!["vendor", "SessionManager"]);
        assert!(positive_vendor.filters.exclude_path.is_empty());
    }

    #[test]
    fn parses_dependency_filters() {
        let parsed = parse_query(
            "dep:serde dependency:tokio import:crate::server kind:func -type:classes -module:legacy symbol:Runtime",
        );

        assert_eq!(parsed.terms, Vec::<String>::new());
        assert_eq!(parsed.filters.dependency.as_deref(), Some("tokio"));
        assert_eq!(parsed.filters.import.as_deref(), Some("crate::server"));
        assert_eq!(parsed.filters.symbol_kind.as_deref(), Some("function"));
        assert_eq!(parsed.filters.exclude_symbol_kind, vec!["class"]);
        assert_eq!(parsed.filters.exclude_import, vec!["legacy"]);
        assert_eq!(parsed.filters.symbol.as_deref(), Some("Runtime"));
    }

    #[test]
    fn serializes_query_text_with_filters_for_followups() {
        let parsed = parse_query(
            r#"mode:any path:'src auth' line:42 lang:Rust symbol:SessionManager code:true -branch:wip -origin:legacy "issue token""#,
        );

        let text = query_with_filters_text(&parsed.terms, &parsed.filters);
        let reparsed = parse_query(&text);
        assert!(reparsed.filters.match_any);
        assert_eq!(reparsed.terms, vec!["issue token"]);
        assert_eq!(reparsed.filters.path.as_deref(), Some("src auth"));
        assert_eq!(reparsed.filters.target_line, Some(42));
        assert_eq!(reparsed.filters.language.as_deref(), Some("rust"));
        assert_eq!(reparsed.filters.symbol.as_deref(), Some("SessionManager"));
        assert_eq!(reparsed.filters.code, Some(true));
        assert_eq!(reparsed.filters.exclude_branch, vec!["wip"]);
        assert_eq!(reparsed.filters.exclude_origin, vec!["legacy"]);
    }

    #[test]
    fn parses_agent_friendly_file_and_path_aliases() {
        let parsed = parse_query(
            "folder:src directory:services in:packages/auth filename:auth.rs lang:ts -lang:md -file_name:generated.rs -folder:vendor !under:third_party without:within:fixtures token",
        );

        assert_eq!(parsed.terms, vec!["token"]);
        assert_eq!(parsed.filters.path.as_deref(), Some("packages/auth"));
        assert_eq!(parsed.filters.file.as_deref(), Some("auth.rs"));
        assert_eq!(parsed.filters.language.as_deref(), Some("typescript"));
        assert_eq!(parsed.filters.exclude_language, vec!["markdown"]);
        assert_eq!(parsed.filters.exclude_file, vec!["generated.rs"]);
        assert_eq!(
            parsed.filters.exclude_path,
            vec!["vendor", "third_party", "fixtures"]
        );

        let cli_style = parse_query(
            "file-name:auth.rs repo-filter:service target-line:12 require-all:true exclude-path:vendor exclude-file:generated.rs exclude-language:md exclude-extension:txt exclude-symbol:LegacyToken exclude-symbol-kind:class exclude-repo:old exclude-branch:wip exclude-origin:legacy exclude-dependency:serde exclude-import:legacy exclude-content:deprecated token auth",
        );
        assert_eq!(cli_style.terms, vec!["token", "auth"]);
        assert_eq!(cli_style.filters.file.as_deref(), Some("auth.rs"));
        assert_eq!(cli_style.filters.repo.as_deref(), Some("service"));
        assert_eq!(cli_style.filters.target_line, Some(12));
        assert!(cli_style.filters.require_all);
        assert_eq!(cli_style.filters.exclude_path, vec!["vendor"]);
        assert_eq!(cli_style.filters.exclude_file, vec!["generated.rs"]);
        assert_eq!(cli_style.filters.exclude_language, vec!["markdown"]);
        assert_eq!(cli_style.filters.exclude_extension, vec!["txt"]);
        assert_eq!(cli_style.filters.exclude_symbol, vec!["LegacyToken"]);
        assert_eq!(cli_style.filters.exclude_symbol_kind, vec!["class"]);
        assert_eq!(cli_style.filters.exclude_repo, vec!["old"]);
        assert_eq!(cli_style.filters.exclude_branch, vec!["wip"]);
        assert_eq!(cli_style.filters.exclude_origin, vec!["legacy"]);
        assert_eq!(cli_style.filters.exclude_dependency, vec!["serde"]);
        assert_eq!(cli_style.filters.exclude_import, vec!["legacy"]);
        assert_eq!(cli_style.filters.exclude_content, vec!["deprecated"]);

        let broad = parse_query("any-terms:true session token");
        assert!(broad.filters.match_any);
        assert!(!broad.filters.require_all);
    }

    #[test]
    fn parses_explicit_line_filters_for_query_anchors() {
        let parsed = parse_query("path:src/auth.rs line:42 issue token");

        assert_eq!(parsed.terms, vec!["issue", "token"]);
        assert_eq!(parsed.filters.path.as_deref(), Some("src/auth.rs"));
        assert_eq!(parsed.filters.target_line, Some(42));
        assert!(parsed.filters.require_all);

        let target_line = parse_query("file:auth.rs target_line:7 token");
        assert_eq!(target_line.terms, vec!["token"]);
        assert_eq!(target_line.filters.file.as_deref(), Some("auth.rs"));
        assert_eq!(target_line.filters.target_line, Some(7));

        let invalid = parse_query("path:src/auth.rs line:0 token");
        assert_eq!(invalid.terms, vec!["line:0", "token"]);
        assert_eq!(invalid.filters.path.as_deref(), Some("src/auth.rs"));
        assert_eq!(invalid.filters.target_line, None);
    }

    #[test]
    fn parses_location_suffixes_on_explicit_file_and_path_filters() {
        let path = parse_query("path:src/server.rs:42:9");
        assert!(path.terms.is_empty());
        assert_eq!(path.filters.path.as_deref(), Some("src/server.rs"));
        assert_eq!(path.filters.target_line, Some(42));

        let file = parse_query("file:Cargo.toml:12");
        assert!(file.terms.is_empty());
        assert_eq!(file.filters.file.as_deref(), Some("Cargo.toml"));
        assert_eq!(file.filters.target_line, Some(12));

        let file_range = parse_query("file:Cargo.toml:12-18");
        assert!(file_range.terms.is_empty());
        assert_eq!(file_range.filters.file.as_deref(), Some("Cargo.toml"));
        assert_eq!(file_range.filters.target_line, Some(12));

        let paren_path = parse_query("path:src/server.rs(42,9)");
        assert!(paren_path.terms.is_empty());
        assert_eq!(paren_path.filters.path.as_deref(), Some("src/server.rs"));
        assert_eq!(paren_path.filters.target_line, Some(42));

        let paren_file = parse_query("file:Cargo.toml(12)");
        assert!(paren_file.terms.is_empty());
        assert_eq!(paren_file.filters.file.as_deref(), Some("Cargo.toml"));
        assert_eq!(paren_file.filters.target_line, Some(12));

        let accidental_path = parse_query("file:src/server.rs:42");
        assert!(accidental_path.terms.is_empty());
        assert_eq!(
            accidental_path.filters.path.as_deref(),
            Some("src/server.rs")
        );
        assert_eq!(accidental_path.filters.target_line, Some(42));
    }

    #[test]
    fn infers_bare_filename_and_path_queries_as_fast_filters() {
        let manifest = parse_query("Cargo.toml");
        assert!(manifest.terms.is_empty());
        assert_eq!(manifest.filters.file.as_deref(), Some("Cargo.toml"));
        assert!(!manifest.filters.require_all);

        let source_path = parse_query("src/server.rs");
        assert!(source_path.terms.is_empty());
        assert_eq!(source_path.filters.path.as_deref(), Some("src/server.rs"));
        assert_eq!(source_path.filters.target_line, None);

        let pytest_node_id = parse_query("tests/test_auth.py::test_login");
        assert!(pytest_node_id.terms.is_empty());
        assert_eq!(
            pytest_node_id.filters.path.as_deref(),
            Some("tests/test_auth.py")
        );
        assert_eq!(pytest_node_id.filters.target_line, None);

        let pytest_command = parse_query("pytest tests/test_auth.py::test_login -q");
        assert!(pytest_command.terms.is_empty());
        assert_eq!(
            pytest_command.filters.path.as_deref(),
            Some("tests/test_auth.py")
        );
        assert_eq!(pytest_command.filters.target_line, None);

        let cargo_test_command = parse_query("cargo test parser_accepts_locations");
        assert!(cargo_test_command.terms.is_empty());
        assert_eq!(
            cargo_test_command.filters.symbol.as_deref(),
            Some("parser_accepts_locations")
        );
        assert_eq!(
            cargo_test_command.filters.symbol_kind.as_deref(),
            Some("function")
        );

        let cargo_test_module_path =
            parse_query("cargo test query::tests::parser_accepts_locations");
        assert!(cargo_test_module_path.terms.is_empty());
        assert_eq!(
            cargo_test_module_path.filters.symbol.as_deref(),
            Some("parser_accepts_locations")
        );
        assert_eq!(
            cargo_test_module_path.filters.symbol_kind.as_deref(),
            Some("function")
        );

        let pytest_failure_line = parse_query("FAILED tests/test_auth.py::test_login - failed");
        assert!(pytest_failure_line.terms.is_empty());
        assert_eq!(
            pytest_failure_line.filters.path.as_deref(),
            Some("tests/test_auth.py")
        );
        assert_eq!(pytest_failure_line.filters.target_line, None);

        let dot_source_path = parse_query("./src/server.rs");
        assert!(dot_source_path.terms.is_empty());
        assert_eq!(
            dot_source_path.filters.path.as_deref(),
            Some("src/server.rs")
        );

        let source_location = parse_query("src/server.rs:42:9");
        assert!(source_location.terms.is_empty());
        assert_eq!(
            source_location.filters.path.as_deref(),
            Some("src/server.rs")
        );
        assert_eq!(source_location.filters.target_line, Some(42));

        let bazel_package_label = parse_query("//tools/search:orient_cli");
        assert!(bazel_package_label.terms.is_empty());
        assert_eq!(
            bazel_package_label.filters.path.as_deref(),
            Some("tools/search")
        );
        assert_eq!(
            bazel_package_label.filters.symbol.as_deref(),
            Some("orient_cli")
        );
        assert_eq!(
            bazel_package_label.filters.symbol_kind.as_deref(),
            Some("target")
        );

        let bazel_relative_label = parse_query(":agent_smoke_test");
        assert!(bazel_relative_label.terms.is_empty());
        assert_eq!(
            bazel_relative_label.filters.symbol.as_deref(),
            Some("agent_smoke_test")
        );
        assert_eq!(
            bazel_relative_label.filters.symbol_kind.as_deref(),
            Some("target")
        );

        let bazel_command_label = parse_query("bazel test //tools/search:orient_cli");
        assert!(bazel_command_label.terms.is_empty());
        assert_eq!(
            bazel_command_label.filters.path.as_deref(),
            Some("tools/search")
        );
        assert_eq!(
            bazel_command_label.filters.symbol.as_deref(),
            Some("orient_cli")
        );
        assert_eq!(
            bazel_command_label.filters.symbol_kind.as_deref(),
            Some("target")
        );

        let rust_diagnostic_location = parse_query("--> src/server.rs:42:9: borrowed value");
        assert_eq!(rust_diagnostic_location.terms, Vec::<String>::new());
        assert_eq!(
            rust_diagnostic_location.filters.path.as_deref(),
            Some("src/server.rs")
        );
        assert_eq!(rust_diagnostic_location.filters.target_line, Some(42));

        let rust_diagnostic_block = parse_query(
            "error[E0505]: borrowed value does not live long enough\n  --> src/server.rs:42:9\n   |\n42 |     handle_request();",
        );
        assert_eq!(rust_diagnostic_block.terms, Vec::<String>::new());
        assert_eq!(
            rust_diagnostic_block.filters.path.as_deref(),
            Some("src/server.rs")
        );
        assert_eq!(rust_diagnostic_block.filters.target_line, Some(42));

        let github_actions_annotation =
            parse_query("::error file=src/server.rs,line=42,col=9::borrowed value");
        assert_eq!(github_actions_annotation.terms, Vec::<String>::new());
        assert_eq!(
            github_actions_annotation.filters.path.as_deref(),
            Some("src/server.rs")
        );
        assert_eq!(github_actions_annotation.filters.target_line, Some(42));

        let github_actions_block = parse_query(
            "cargo test failed\n::warning file=src/server.rs,line=42,endLine=45::borrowed value",
        );
        assert_eq!(github_actions_block.terms, Vec::<String>::new());
        assert_eq!(
            github_actions_block.filters.path.as_deref(),
            Some("src/server.rs")
        );
        assert_eq!(github_actions_block.filters.target_line, Some(42));

        let python_traceback_block = parse_query(
            "Traceback (most recent call last):\n  File \"src/server.rs\", line 42, in handle_request\n    issue_token()",
        );
        assert_eq!(python_traceback_block.terms, Vec::<String>::new());
        assert_eq!(
            python_traceback_block.filters.path.as_deref(),
            Some("src/server.rs")
        );
        assert_eq!(python_traceback_block.filters.target_line, Some(42));

        let js_stack_block = parse_query(
            "Error: boom\n    at handleRequest (src/server.rs:42:9)\n    at main (src/main.rs:7:1)",
        );
        assert_eq!(js_stack_block.terms, Vec::<String>::new());
        assert_eq!(
            js_stack_block.filters.path.as_deref(),
            Some("src/server.rs")
        );
        assert_eq!(js_stack_block.filters.target_line, Some(42));

        let java_stack_block = parse_query(
            "java.lang.IllegalStateException: boom\n    at com.example.Service.handle(Service.java:42)\n    at com.example.Main.main(Main.java:7)",
        );
        assert_eq!(java_stack_block.terms, Vec::<String>::new());
        assert_eq!(
            java_stack_block.filters.file.as_deref(),
            Some("Service.java")
        );
        assert_eq!(java_stack_block.filters.target_line, Some(42));

        let kotlin_stack_block = parse_query(
            "Exception in thread \"main\"\n    at com.example.ServiceKt.handle(Service.kt:42:9)",
        );
        assert_eq!(kotlin_stack_block.terms, Vec::<String>::new());
        assert_eq!(
            kotlin_stack_block.filters.file.as_deref(),
            Some("Service.kt")
        );
        assert_eq!(kotlin_stack_block.filters.target_line, Some(42));

        let go_panic_block =
            parse_query("panic: boom\n\nmain.handleRequest()\n\t/tmp/work/src/server.rs:42 +0x20");
        assert_eq!(go_panic_block.terms, Vec::<String>::new());
        assert_eq!(
            go_panic_block.filters.path.as_deref(),
            Some("/tmp/work/src/server.rs")
        );
        assert_eq!(go_panic_block.filters.target_line, Some(42));

        let dot_source_location = parse_query("./src/server.rs:42:9");
        assert!(dot_source_location.terms.is_empty());
        assert_eq!(
            dot_source_location.filters.path.as_deref(),
            Some("src/server.rs")
        );
        assert_eq!(dot_source_location.filters.target_line, Some(42));

        let hash_source_location = parse_query("src/server.rs#L42");
        assert!(hash_source_location.terms.is_empty());
        assert_eq!(
            hash_source_location.filters.path.as_deref(),
            Some("src/server.rs")
        );
        assert_eq!(hash_source_location.filters.target_line, Some(42));

        let hash_range_source_location = parse_query("src/server.rs#L42-L45");
        assert!(hash_range_source_location.terms.is_empty());
        assert_eq!(
            hash_range_source_location.filters.path.as_deref(),
            Some("src/server.rs")
        );
        assert_eq!(hash_range_source_location.filters.target_line, Some(42));

        let hash_column_source_location = parse_query("src/server.rs#L42C5-L45C9");
        assert!(hash_column_source_location.terms.is_empty());
        assert_eq!(
            hash_column_source_location.filters.path.as_deref(),
            Some("src/server.rs")
        );
        assert_eq!(hash_column_source_location.filters.target_line, Some(42));

        let colon_range_source_location = parse_query("src/server.rs:42-45");
        assert!(colon_range_source_location.terms.is_empty());
        assert_eq!(
            colon_range_source_location.filters.path.as_deref(),
            Some("src/server.rs")
        );
        assert_eq!(colon_range_source_location.filters.target_line, Some(42));

        let paren_source_location = parse_query("src/server.rs(42,9): borrowed value");
        assert_eq!(paren_source_location.terms, vec!["borrowed", "value"]);
        assert_eq!(
            paren_source_location.filters.path.as_deref(),
            Some("src/server.rs")
        );
        assert_eq!(paren_source_location.filters.target_line, Some(42));

        let msbuild_source_location =
            parse_query("src/server.rs(42,9): error CS1002: missing semicolon");
        assert_eq!(msbuild_source_location.terms, Vec::<String>::new());
        assert_eq!(
            msbuild_source_location.filters.path.as_deref(),
            Some("src/server.rs")
        );
        assert_eq!(msbuild_source_location.filters.target_line, Some(42));

        let markdown_source_location =
            parse_query("[src/server.rs#L42-L45](src/server.rs#L42-L45)");
        assert!(markdown_source_location.terms.is_empty());
        assert_eq!(
            markdown_source_location.filters.path.as_deref(),
            Some("src/server.rs")
        );
        assert_eq!(markdown_source_location.filters.target_line, Some(42));

        let hosted_source_location = parse_query(
            "[src/server.rs:42](https://github.com/evalops/orient-search/blob/main/src/server.rs#L42)",
        );
        assert!(hosted_source_location.terms.is_empty());
        assert_eq!(
            hosted_source_location.filters.path.as_deref(),
            Some("src/server.rs")
        );
        assert_eq!(hosted_source_location.filters.target_line, Some(42));

        let hosted_query_source_location = parse_query(
            "https://github.com/evalops/orient-search/blob/main/src/server.rs?plain=1#L42-L45",
        );
        assert!(hosted_query_source_location.terms.is_empty());
        assert_eq!(
            hosted_query_source_location.filters.path.as_deref(),
            Some("src/server.rs")
        );
        assert_eq!(hosted_query_source_location.filters.target_line, Some(42));

        let hosted_column_source_location = parse_query(
            "https://github.com/evalops/orient-search/blob/main/src/server.rs#L42C5-L45C9",
        );
        assert!(hosted_column_source_location.terms.is_empty());
        assert_eq!(
            hosted_column_source_location.filters.path.as_deref(),
            Some("src/server.rs")
        );
        assert_eq!(hosted_column_source_location.filters.target_line, Some(42));

        let hosted_slashy_branch_source_location = parse_query(
            "https://github.com/evalops/orient-search/blob/feature/search/src/server.rs#L42-L45",
        );
        assert!(hosted_slashy_branch_source_location.terms.is_empty());
        assert_eq!(
            hosted_slashy_branch_source_location.filters.path.as_deref(),
            Some("src/server.rs")
        );
        assert_eq!(
            hosted_slashy_branch_source_location.filters.target_line,
            Some(42)
        );

        let hosted_main_branch_nested_source_location = parse_query(
            "https://github.com/evalops/orient-search/blob/main/search/src/server.rs#L42-L45",
        );
        assert!(hosted_main_branch_nested_source_location.terms.is_empty());
        assert_eq!(
            hosted_main_branch_nested_source_location
                .filters
                .path
                .as_deref(),
            Some("search/src/server.rs")
        );
        assert_eq!(
            hosted_main_branch_nested_source_location
                .filters
                .target_line,
            Some(42)
        );

        let raw_github_source_location = parse_query(
            "https://raw.githubusercontent.com/evalops/orient-search/main/src/server.rs",
        );
        assert!(raw_github_source_location.terms.is_empty());
        assert_eq!(
            raw_github_source_location.filters.path.as_deref(),
            Some("src/server.rs")
        );
        assert_eq!(raw_github_source_location.filters.target_line, None);

        let raw_github_slashy_branch_source_location = parse_query(
            "https://raw.githubusercontent.com/evalops/orient-search/feature/search/src/server.rs#L42-L45",
        );
        assert!(raw_github_slashy_branch_source_location.terms.is_empty());
        assert_eq!(
            raw_github_slashy_branch_source_location
                .filters
                .path
                .as_deref(),
            Some("src/server.rs")
        );
        assert_eq!(
            raw_github_slashy_branch_source_location.filters.target_line,
            Some(42)
        );

        let raw_github_nested_path_source_location = parse_query(
            "https://raw.githubusercontent.com/evalops/orient-search/main/search/src/server.rs",
        );
        assert!(raw_github_nested_path_source_location.terms.is_empty());
        assert_eq!(
            raw_github_nested_path_source_location
                .filters
                .path
                .as_deref(),
            Some("search/src/server.rs")
        );
        assert_eq!(
            raw_github_nested_path_source_location.filters.target_line,
            None
        );

        let bitbucket_source_location = parse_query(
            "https://bitbucket.org/evalops/orient-search/src/main/src/server.rs#lines-42:45",
        );
        assert!(bitbucket_source_location.terms.is_empty());
        assert_eq!(
            bitbucket_source_location.filters.path.as_deref(),
            Some("src/server.rs")
        );
        assert_eq!(bitbucket_source_location.filters.target_line, Some(42));

        let bitbucket_nested_path_source_location = parse_query(
            "https://bitbucket.org/evalops/orient-search/src/main/search/src/server.rs",
        );
        assert!(bitbucket_nested_path_source_location.terms.is_empty());
        assert_eq!(
            bitbucket_nested_path_source_location
                .filters
                .path
                .as_deref(),
            Some("search/src/server.rs")
        );
        assert_eq!(
            bitbucket_nested_path_source_location.filters.target_line,
            None
        );

        let gitlab_source_location = parse_query(
            "https://gitlab.com/evalops/orient-search/-/blob/main/src/server.rs#L42-45",
        );
        assert!(gitlab_source_location.terms.is_empty());
        assert_eq!(
            gitlab_source_location.filters.path.as_deref(),
            Some("src/server.rs")
        );
        assert_eq!(gitlab_source_location.filters.target_line, Some(42));

        let gitlab_slashy_branch_source_location = parse_query(
            "https://gitlab.com/evalops/orient-search/-/blob/feature/search/src/server.rs#L42-L45",
        );
        assert!(gitlab_slashy_branch_source_location.terms.is_empty());
        assert_eq!(
            gitlab_slashy_branch_source_location.filters.path.as_deref(),
            Some("src/server.rs")
        );
        assert_eq!(
            gitlab_slashy_branch_source_location.filters.target_line,
            Some(42)
        );

        let gitlab_raw_source_location =
            parse_query("https://gitlab.com/evalops/orient-search/-/raw/main/src/server.rs#L42-45");
        assert!(gitlab_raw_source_location.terms.is_empty());
        assert_eq!(
            gitlab_raw_source_location.filters.path.as_deref(),
            Some("src/server.rs")
        );
        assert_eq!(gitlab_raw_source_location.filters.target_line, Some(42));

        let azure_devops_source_location = parse_query(
            "https://dev.azure.com/evalops/platform/_git/orient-search?path=/src/server.rs&version=GBmain&line=42&lineEnd=45",
        );
        assert!(azure_devops_source_location.terms.is_empty());
        assert_eq!(
            azure_devops_source_location.filters.path.as_deref(),
            Some("src/server.rs")
        );
        assert_eq!(azure_devops_source_location.filters.target_line, Some(42));

        let visual_studio_source_location = parse_query(
            "https://evalops.visualstudio.com/platform/_git/orient-search?path=%2Fsrc%2Fserver.rs&line=42&lineEnd=45",
        );
        assert!(visual_studio_source_location.terms.is_empty());
        assert_eq!(
            visual_studio_source_location.filters.path.as_deref(),
            Some("src/server.rs")
        );
        assert_eq!(visual_studio_source_location.filters.target_line, Some(42));

        let sourcegraph_source_location = parse_query(
            "https://sourcegraph.com/github.com/evalops/orient-search/-/blob/src/server.rs?L42:9",
        );
        assert!(sourcegraph_source_location.terms.is_empty());
        assert_eq!(
            sourcegraph_source_location.filters.path.as_deref(),
            Some("src/server.rs")
        );
        assert_eq!(sourcegraph_source_location.filters.target_line, Some(42));

        let copied_source_line = parse_query("src/server.rs:42: pub fn handle_request");
        assert_eq!(
            copied_source_line.terms,
            vec!["pub", "fn", "handle_request"]
        );
        assert_eq!(
            copied_source_line.filters.path.as_deref(),
            Some("src/server.rs")
        );
        assert_eq!(copied_source_line.filters.target_line, Some(42));
        assert!(copied_source_line.filters.require_all);

        let copied_source_column_line = parse_query("src/server.rs:42:9:handle_request");
        assert_eq!(copied_source_column_line.terms, vec!["handle_request"]);
        assert_eq!(
            copied_source_column_line.filters.path.as_deref(),
            Some("src/server.rs")
        );
        assert_eq!(copied_source_column_line.filters.target_line, Some(42));

        let copied_range_source_line = parse_query("src/server.rs:42-45: handle_request");
        assert_eq!(copied_range_source_line.terms, vec!["handle_request"]);
        assert_eq!(
            copied_range_source_line.filters.path.as_deref(),
            Some("src/server.rs")
        );
        assert_eq!(copied_range_source_line.filters.target_line, Some(42));

        let wrapped_source_location = parse_query("(src/server.rs:42:9)");
        assert!(wrapped_source_location.terms.is_empty());
        assert_eq!(
            wrapped_source_location.filters.path.as_deref(),
            Some("src/server.rs")
        );
        assert_eq!(wrapped_source_location.filters.target_line, Some(42));

        let wrapped_hash_source_location = parse_query("(src/server.rs#L42-L45)");
        assert!(wrapped_hash_source_location.terms.is_empty());
        assert_eq!(
            wrapped_hash_source_location.filters.path.as_deref(),
            Some("src/server.rs")
        );
        assert_eq!(wrapped_hash_source_location.filters.target_line, Some(42));

        let stack_source_location = parse_query("at Object.handle (src/server.rs:42:9)");
        assert!(stack_source_location.terms.is_empty());
        assert_eq!(
            stack_source_location.filters.path.as_deref(),
            Some("src/server.rs")
        );
        assert_eq!(stack_source_location.filters.target_line, Some(42));

        let stack_hash_source_location = parse_query("at Object.handle (src/server.rs#L42)");
        assert!(stack_hash_source_location.terms.is_empty());
        assert_eq!(
            stack_hash_source_location.filters.path.as_deref(),
            Some("src/server.rs")
        );
        assert_eq!(stack_hash_source_location.filters.target_line, Some(42));

        let python_source_location =
            parse_query(r#"File "src/server.rs", line 42, in handle_request"#);
        assert_eq!(python_source_location.terms, vec!["handle_request"]);
        assert_eq!(
            python_source_location.filters.path.as_deref(),
            Some("src/server.rs")
        );
        assert_eq!(python_source_location.filters.target_line, Some(42));

        let copied_manifest_line = parse_query("Cargo.toml:12:name");
        assert_eq!(copied_manifest_line.terms, vec!["name"]);
        assert_eq!(
            copied_manifest_line.filters.file.as_deref(),
            Some("Cargo.toml")
        );
        assert_eq!(copied_manifest_line.filters.target_line, Some(12));

        let go_mod = parse_query("go.mod");
        assert!(go_mod.terms.is_empty());
        assert_eq!(go_mod.filters.file.as_deref(), Some("go.mod"));

        let manifest_location = parse_query("Cargo.toml:12");
        assert!(manifest_location.terms.is_empty());
        assert_eq!(
            manifest_location.filters.file.as_deref(),
            Some("Cargo.toml")
        );
        assert_eq!(manifest_location.filters.target_line, Some(12));

        let dockerfile = parse_query("Dockerfile");
        assert!(dockerfile.terms.is_empty());
        assert_eq!(dockerfile.filters.file.as_deref(), Some("Dockerfile"));

        let explicit_content = parse_query("content:Cargo.toml");
        assert_eq!(explicit_content.terms, vec!["Cargo.toml"]);
        assert!(explicit_content.filters.file.is_none());

        let symbolish = parse_query("SessionManager");
        assert_eq!(symbolish.terms, vec!["SessionManager"]);
        assert!(symbolish.filters.file.is_none());
        assert!(symbolish.filters.path.is_none());

        let common_word = parse_query("build");
        assert_eq!(common_word.terms, vec!["build"]);
        assert!(common_word.filters.file.is_none());
        assert!(common_word.filters.path.is_none());
    }

    #[test]
    fn parses_type_aliases_for_known_symbol_kinds() {
        let parsed = parse_query("type:function route request");
        assert_eq!(parsed.terms, vec!["route", "request"]);
        assert_eq!(parsed.filters.symbol_kind.as_deref(), Some("function"));

        let plural = parse_query("type:functions route request");
        assert_eq!(plural.filters.symbol_kind.as_deref(), Some("function"));

        let alias = parse_query("symbol_type:struct SessionManager");
        assert_eq!(alias.filters.symbol_kind.as_deref(), Some("struct"));

        let shorthand = parse_query("fn:issue_token");
        assert!(shorthand.terms.is_empty());
        assert_eq!(shorthand.filters.symbol.as_deref(), Some("issue_token"));
        assert_eq!(shorthand.filters.symbol_kind.as_deref(), Some("function"));

        let class_shorthand = parse_query("class:SessionManager auth");
        assert_eq!(class_shorthand.terms, vec!["auth"]);
        assert_eq!(
            class_shorthand.filters.symbol.as_deref(),
            Some("SessionManager")
        );
        assert_eq!(
            class_shorthand.filters.symbol_kind.as_deref(),
            Some("class")
        );

        let excluded = parse_query("-symbol-type:interfaces gateway");
        assert_eq!(excluded.filters.exclude_symbol_kind, vec!["interface"]);

        let target = parse_query("kind:target deploy");
        assert_eq!(target.terms, vec!["deploy"]);
        assert_eq!(target.filters.symbol_kind.as_deref(), Some("target"));

        let recipe_shorthand = parse_query("recipe:release");
        assert!(recipe_shorthand.terms.is_empty());
        assert_eq!(recipe_shorthand.filters.symbol.as_deref(), Some("release"));
        assert_eq!(
            recipe_shorthand.filters.symbol_kind.as_deref(),
            Some("target")
        );

        let script_shorthand = parse_query("script:typecheck");
        assert!(script_shorthand.terms.is_empty());
        assert_eq!(
            script_shorthand.filters.symbol.as_deref(),
            Some("typecheck")
        );
        assert_eq!(
            script_shorthand.filters.symbol_kind.as_deref(),
            Some("script")
        );

        let package_shorthand = parse_query("package:auth-api");
        assert!(package_shorthand.terms.is_empty());
        assert_eq!(
            package_shorthand.filters.symbol.as_deref(),
            Some("auth-api")
        );
        assert_eq!(
            package_shorthand.filters.symbol_kind.as_deref(),
            Some("package")
        );

        let service_shorthand = parse_query("service:api");
        assert!(service_shorthand.terms.is_empty());
        assert_eq!(service_shorthand.filters.symbol.as_deref(), Some("api"));
        assert_eq!(
            service_shorthand.filters.symbol_kind.as_deref(),
            Some("service")
        );

        let stage_shorthand = parse_query("stage:builder");
        assert!(stage_shorthand.terms.is_empty());
        assert_eq!(stage_shorthand.filters.symbol.as_deref(), Some("builder"));
        assert_eq!(
            stage_shorthand.filters.symbol_kind.as_deref(),
            Some("stage")
        );

        let bin_shorthand = parse_query("bin:auth-worker");
        assert!(bin_shorthand.terms.is_empty());
        assert_eq!(bin_shorthand.filters.symbol.as_deref(), Some("auth-worker"));
        assert_eq!(bin_shorthand.filters.symbol_kind.as_deref(), Some("bin"));

        let unknown = parse_query("type:file gateway");
        assert_eq!(unknown.terms, vec!["type:file", "gateway"]);
        assert_eq!(unknown.filters.symbol_kind, None);
    }

    #[test]
    fn parses_is_test_and_is_source_aliases() {
        let tests = parse_query("is:test issue token");
        assert_eq!(tests.terms, vec!["issue", "token"]);
        assert_eq!(tests.filters.test, Some(true));

        let source = parse_query("is:source issue token");
        assert_eq!(source.terms, vec!["issue", "token"]);
        assert_eq!(source.filters.test, Some(false));
        assert_eq!(source.filters.code, None);

        let code = parse_query("is:code issue token");
        assert_eq!(code.terms, vec!["issue", "token"]);
        assert_eq!(code.filters.code, Some(true));

        let prose = parse_query("is:docs issue token");
        assert_eq!(prose.terms, vec!["issue", "token"]);
        assert_eq!(prose.filters.code, Some(false));

        let negated = parse_query("-is:test issue token");
        assert_eq!(negated.filters.test, Some(false));

        let generated = parse_query("is:generated issue token");
        assert_eq!(generated.terms, vec!["issue", "token"]);
        assert_eq!(generated.filters.generated, Some(true));

        let authored = parse_query("-is:generated issue token");
        assert_eq!(authored.terms, vec!["issue", "token"]);
        assert_eq!(authored.filters.generated, Some(false));

        let generated_bool = parse_query("generated:false issue token");
        assert_eq!(generated_bool.filters.generated, Some(false));

        let unknown = parse_query("is:vendored issue token");
        assert_eq!(unknown.terms, vec!["is:vendored", "issue", "token"]);
        assert_eq!(unknown.filters.test, None);
        assert_eq!(unknown.filters.generated, None);
    }

    #[test]
    fn parses_content_text_and_term_aliases_as_query_terms() {
        let parsed = parse_query(
            r#"content:"database connection refused" text:gateway term:SessionManager"#,
        );
        assert_eq!(
            parsed.terms,
            vec!["database connection refused", "gateway", "SessionManager"]
        );
        assert!(parsed.explicit_content_terms);
        assert!(parsed.filters.file.is_none());

        let mode = parse_query("terms:any content:roadmap compression");
        assert!(mode.filters.match_any);
        assert!(mode.explicit_content_terms);
        assert_eq!(mode.terms, vec!["roadmap", "compression"]);

        let negated = parse_query("-content:generated issue token");
        assert!(!negated.explicit_content_terms);
        assert_eq!(negated.terms, vec!["issue", "token"]);
        assert_eq!(negated.filters.exclude_content, vec!["generated"]);
    }

    #[test]
    fn parses_explicit_term_match_modes() {
        let relaxed = parse_query("mode:any roadmap mmap compression");
        assert_eq!(relaxed.terms, vec!["roadmap", "mmap", "compression"]);
        assert!(relaxed.filters.match_any);
        assert!(!relaxed.filters.require_all);

        let strict = parse_query("match:all roadmap mmap");
        assert!(!strict.filters.match_any);
        assert!(strict.filters.require_all);
    }

    #[test]
    fn normalizes_quoted_phrases_across_identifier_boundaries() {
        assert_eq!(normalize_phrase_text("issue_token"), "issue token");
        assert_eq!(normalize_phrase_text("issue-token"), "issue token");
        assert_eq!(normalize_phrase_text("issueToken"), "issue token");
        assert_eq!(normalize_phrase_text("HTTPServer"), "http server");
        assert_eq!(normalize_phrase_text("XMLHTTPServer"), "xmlhttp server");
        assert_eq!(tokenize("HTTPServer"), vec!["http", "server"]);
        assert_eq!(
            query_phrases(&["issueToken".to_string()]),
            vec!["issue token"]
        );
    }

    #[test]
    fn parser_tolerates_adversarial_inputs_without_panics() {
        let cases = [
            "",
            "-",
            "::::",
            "path:",
            "-path:",
            "\"unterminated quote",
            "'unterminated single",
            r#"path:"src/auth space" -file:'generated.rs' token"#,
            r#"symbol:SessionManager\ test:true test:false"#,
            "repo:old -repo:new -test random words",
            "emoji:😀 \"multi word\" ext:.tsx",
            "a\tb\nc\r\n-path:target",
        ];

        for input in cases {
            let parsed = parse_query(input);
            assert!(!parsed.terms.iter().any(|term| term.trim().is_empty()));
        }
    }

    #[test]
    fn merge_filters_keeps_base_and_extends_negatives() {
        let base = SearchFilters {
            path: Some("src/".to_string()),
            code: Some(false),
            exclude_path: vec!["target".to_string()],
            ..SearchFilters::default()
        };
        let parsed = parse_query(r#"lang:rust code:true -path:fixtures token auth"#);
        let merged = merge_filters(base, parsed.filters);

        assert_eq!(merged.path.as_deref(), Some("src/"));
        assert_eq!(merged.language.as_deref(), Some("rust"));
        assert_eq!(merged.code, Some(true));
        assert_eq!(merged.exclude_path, vec!["target", "fixtures"]);
        assert!(merged.require_all);
    }

    #[test]
    fn merge_filters_lets_any_terms_override_default_and() {
        let base = SearchFilters {
            match_any: true,
            ..SearchFilters::default()
        };
        let parsed = parse_query("token auth");
        let merged = merge_filters(base, parsed.filters);

        assert!(merged.match_any);
        assert!(!merged.require_all);
    }
}
