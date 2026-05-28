use crate::repo_index::{SearchFilters, identifier_boundary_text, tokenize};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ParsedQuery {
    pub terms: Vec<String>,
    pub filters: SearchFilters,
}

pub fn parse_query(input: &str) -> ParsedQuery {
    let mut terms = Vec::new();
    let mut filters = SearchFilters::default();

    for token in split_query(input) {
        let (negated, token) = token
            .strip_prefix('-')
            .map(|value| (true, value.to_string()))
            .unwrap_or((false, token));
        if apply_filter(&mut filters, &token, negated) {
            continue;
        }
        if !negated && !token.trim().is_empty() {
            terms.push(token);
        }
    }

    if terms.len() > 1 {
        filters.require_all = true;
    }

    ParsedQuery { terms, filters }
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
        (false, "file") => filters.file = Some(value),
        (false, "path" | "dir" | "directory") => filters.path = Some(value),
        (false, "lang" | "language") => filters.language = Some(value.to_ascii_lowercase()),
        (false, "ext" | "extension") => {
            filters.extension = Some(value.trim_start_matches('.').to_ascii_lowercase())
        }
        (false, "symbol") => filters.symbol = Some(value),
        (false, "repo") => filters.repo = Some(value),
        (false, "dep" | "deps" | "dependency" | "dependencies") => {
            filters.dependency = Some(value.to_ascii_lowercase())
        }
        (false, "test" | "tests") => filters.test = Some(parse_boolish(&value).unwrap_or(true)),
        (true, "file") => filters.exclude_file.push(value),
        (true, "path" | "dir" | "directory") => filters.exclude_path.push(value),
        (true, "lang" | "language") => filters.exclude_language.push(value.to_ascii_lowercase()),
        (true, "ext" | "extension") => filters
            .exclude_extension
            .push(value.trim_start_matches('.').to_ascii_lowercase()),
        (true, "symbol") => filters.exclude_symbol.push(value),
        (true, "repo") => filters.exclude_repo.push(value),
        (true, "dep" | "deps" | "dependency" | "dependencies") => {
            filters.exclude_dependency.push(value.to_ascii_lowercase())
        }
        (true, "test" | "tests") => filters.test = Some(false),
        _ => return false,
    }
    true
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
    if parsed.repo.is_some() {
        base.repo = parsed.repo;
    }
    if parsed.dependency.is_some() {
        base.dependency = parsed.dependency;
    }
    if parsed.test.is_some() {
        base.test = parsed.test;
    }
    base.require_all |= parsed.require_all;
    base.exclude_file.extend(parsed.exclude_file);
    base.exclude_path.extend(parsed.exclude_path);
    base.exclude_language.extend(parsed.exclude_language);
    base.exclude_extension.extend(parsed.exclude_extension);
    base.exclude_symbol.extend(parsed.exclude_symbol);
    base.exclude_repo.extend(parsed.exclude_repo);
    base.exclude_dependency.extend(parsed.exclude_dependency);
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
            r#"file:'auth service.rs' language:Rust extension:.RS repo:orient tests -ext:md -repo:old "quoted \"token\"""#,
        );

        assert_eq!(parsed.terms, vec![r#"quoted "token""#]);
        assert_eq!(parsed.filters.file.as_deref(), Some("auth service.rs"));
        assert_eq!(parsed.filters.language.as_deref(), Some("rust"));
        assert_eq!(parsed.filters.extension.as_deref(), Some("rs"));
        assert_eq!(parsed.filters.repo.as_deref(), Some("orient"));
        assert_eq!(parsed.filters.test, Some(true));
        assert_eq!(parsed.filters.exclude_extension, vec!["md"]);
        assert_eq!(parsed.filters.exclude_repo, vec!["old"]);
    }

    #[test]
    fn parses_dependency_filters() {
        let parsed = parse_query("dep:serde dependency:tokio -deps:react symbol:Runtime");

        assert_eq!(parsed.terms, Vec::<String>::new());
        assert_eq!(parsed.filters.dependency.as_deref(), Some("tokio"));
        assert_eq!(parsed.filters.exclude_dependency, vec!["react"]);
        assert_eq!(parsed.filters.symbol.as_deref(), Some("Runtime"));
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
}
