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
        let (negated, token) = token
            .strip_prefix('-')
            .map(|value| (true, value.to_string()))
            .unwrap_or((false, token));
        if let Some(term) = content_term(&token, negated) {
            terms.push(term);
            explicit_content_terms = true;
            continue;
        }
        if apply_match_mode(&mut filters, &token, negated)
            || apply_filter(&mut filters, &token, negated)
        {
            continue;
        }
        if !negated && !token.trim().is_empty() {
            terms.push(token);
        }
    }

    if terms.len() > 1 && !filters.match_any {
        filters.require_all = true;
    }

    ParsedQuery {
        terms,
        filters,
        explicit_content_terms,
    }
}

fn content_term(token: &str, negated: bool) -> Option<String> {
    if negated {
        return None;
    }
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
        ("mode" | "match" | "terms", "any" | "or" | "some") | ("all", "false" | "0" | "no") => {
            filters.match_any = true;
            filters.require_all = false;
            true
        }
        ("mode" | "match" | "terms", "all" | "and") | ("all", "true" | "1" | "yes") => {
            filters.match_any = false;
            filters.require_all = true;
            true
        }
        _ => false,
    }
}

fn apply_filter(filters: &mut SearchFilters, token: &str, negated: bool) -> bool {
    let Some((key, value)) = token.split_once(':') else {
        if token == "test" || token == "tests" {
            filters.test = Some(!negated);
            return true;
        }
        return false;
    };
    let key = key.to_ascii_lowercase();
    let value = value.trim().to_string();
    if value.is_empty() {
        return false;
    }

    match (negated, key.as_str()) {
        (false, "file" | "filename" | "file_name" | "basename") => filters.file = Some(value),
        (false, "path" | "dir" | "directory" | "folder") => filters.path = Some(value),
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
        (false, "repo") => filters.repo = Some(value),
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
            None => return false,
        },
        (false, "generated" | "generated_code" | "generated-code") => {
            filters.generated = Some(parse_boolish(&value).unwrap_or(true))
        }
        (true, "file" | "filename" | "file_name" | "basename") => filters.exclude_file.push(value),
        (true, "path" | "dir" | "directory" | "folder") => filters.exclude_path.push(value),
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
        (true, "repo") => filters.exclude_repo.push(value),
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
            None => return false,
        },
        (true, "generated" | "generated_code" | "generated-code") => {
            filters.generated = Some(false)
        }
        _ => return false,
    }
    true
}

enum IsFilter {
    Test(bool),
    Generated(bool),
}

fn test_filter_from_is_value(value: &str) -> Option<IsFilter> {
    match value.to_ascii_lowercase().as_str() {
        "test" | "tests" | "spec" | "specs" => Some(IsFilter::Test(true)),
        "source" | "src" | "code" | "prod" | "production" => Some(IsFilter::Test(false)),
        "generated" | "gen" | "codegen" | "autogen" => Some(IsFilter::Generated(true)),
        "authored" | "manual" | "handwritten" => Some(IsFilter::Generated(false)),
        _ => None,
    }
}

pub fn normalize_symbol_kind(value: &str) -> String {
    match value.trim().to_ascii_lowercase().as_str() {
        "func" | "function" | "functions" | "method" | "methods" => "function".to_string(),
        "consts" | "constant" | "constants" => "const".to_string(),
        "vars" | "variable" | "variables" => "var".to_string(),
        "classes" => "class".to_string(),
        "structs" => "struct".to_string(),
        "enums" => "enum".to_string(),
        "interfaces" => "interface".to_string(),
        "traits" => "trait".to_string(),
        "types" => "type".to_string(),
        other => other.to_string(),
    }
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
    let mut pieces = terms
        .iter()
        .map(|term| query_token_text(term))
        .collect::<Vec<_>>();
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
    base
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_terms_quotes_filters_and_negatives() {
        let parsed =
            parse_query(r#"symbol:SessionManager lang:rust -dir:docs "issue token" test:false"#);

        assert_eq!(parsed.terms, vec!["issue token"]);
        assert_eq!(parsed.filters.symbol.as_deref(), Some("SessionManager"));
        assert_eq!(parsed.filters.language.as_deref(), Some("rust"));
        assert_eq!(parsed.filters.exclude_path, vec!["docs"]);
        assert_eq!(parsed.filters.test, Some(false));
        assert_eq!(query_phrases(&parsed.terms), vec!["issue token"]);
    }

    #[test]
    fn parses_aliases_booleans_escapes_and_negatives() {
        let parsed = parse_query(
            r#"file:'auth service.rs' language:Rust extension:.RS repo:orient branch:main origin:evalops tests -ext:md -repo:old -branch:wip -origin:legacy "quoted \"token\"""#,
        );

        assert_eq!(parsed.terms, vec![r#"quoted "token""#]);
        assert_eq!(parsed.filters.file.as_deref(), Some("auth service.rs"));
        assert_eq!(parsed.filters.language.as_deref(), Some("rust"));
        assert_eq!(parsed.filters.extension.as_deref(), Some("rs"));
        assert_eq!(parsed.filters.repo.as_deref(), Some("orient"));
        assert_eq!(parsed.filters.branch.as_deref(), Some("main"));
        assert_eq!(parsed.filters.origin.as_deref(), Some("evalops"));
        assert_eq!(parsed.filters.test, Some(true));
        assert_eq!(parsed.filters.exclude_extension, vec!["md"]);
        assert_eq!(parsed.filters.exclude_repo, vec!["old"]);
        assert_eq!(parsed.filters.exclude_branch, vec!["wip"]);
        assert_eq!(parsed.filters.exclude_origin, vec!["legacy"]);
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
            r#"path:'src auth' lang:Rust symbol:SessionManager -branch:wip -origin:legacy "issue token""#,
        );

        let text = query_with_filters_text(&parsed.terms, &parsed.filters);
        let reparsed = parse_query(&text);
        assert_eq!(reparsed.terms, vec!["issue token"]);
        assert_eq!(reparsed.filters.path.as_deref(), Some("src auth"));
        assert_eq!(reparsed.filters.language.as_deref(), Some("rust"));
        assert_eq!(reparsed.filters.symbol.as_deref(), Some("SessionManager"));
        assert_eq!(reparsed.filters.exclude_branch, vec!["wip"]);
        assert_eq!(reparsed.filters.exclude_origin, vec!["legacy"]);
    }

    #[test]
    fn parses_agent_friendly_file_and_path_aliases() {
        let parsed = parse_query(
            "folder:src directory:services filename:auth.rs lang:ts -lang:md -file_name:generated.rs -folder:vendor token",
        );

        assert_eq!(parsed.terms, vec!["token"]);
        assert_eq!(parsed.filters.path.as_deref(), Some("services"));
        assert_eq!(parsed.filters.file.as_deref(), Some("auth.rs"));
        assert_eq!(parsed.filters.language.as_deref(), Some("typescript"));
        assert_eq!(parsed.filters.exclude_language, vec!["markdown"]);
        assert_eq!(parsed.filters.exclude_file, vec!["generated.rs"]);
        assert_eq!(parsed.filters.exclude_path, vec!["vendor"]);
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

        let excluded = parse_query("-symbol-type:interfaces gateway");
        assert_eq!(excluded.filters.exclude_symbol_kind, vec!["interface"]);

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
            exclude_path: vec!["target".to_string()],
            ..SearchFilters::default()
        };
        let parsed = parse_query(r#"lang:rust -path:fixtures token auth"#);
        let merged = merge_filters(base, parsed.filters);

        assert_eq!(merged.path.as_deref(), Some("src/"));
        assert_eq!(merged.language.as_deref(), Some("rust"));
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
