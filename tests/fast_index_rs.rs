use std::fs;
use std::path::Path;
use std::process::Command;
use std::time::{Duration, Instant};

use orient::fast_index::FastIndex;
use orient::repo_index::{
    SearchFilters, SnippetMode, attach_result_context, search_repo_fast_filtered,
    search_repo_fast_filtered_with_timeout,
};

fn write(path: &Path, text: &str) {
    fs::create_dir_all(path.parent().unwrap()).unwrap();
    fs::write(path, text).unwrap();
}

#[test]
fn refresh_reuses_unchanged_files_and_picks_up_changed_terms() {
    let repo = tempfile::tempdir().unwrap();
    write(
        &repo.path().join("src/auth.rs"),
        "pub struct SessionManager;\npub fn issue_token() {}\n",
    );
    write(
        &repo.path().join("src/billing.rs"),
        "pub fn invoice_total() {}\n",
    );

    let first = FastIndex::build(repo.path()).unwrap();
    assert_eq!(first.search("rotating secret", 10).unwrap(), Vec::new());

    write(
        &repo.path().join("src/auth.rs"),
        "pub struct SessionManager;\npub fn rotating_secret() {}\n",
    );
    let outcome = FastIndex::refresh(repo.path(), Some(&first)).unwrap();
    assert!(outcome.reused_files >= 1);
    assert!(outcome.refreshed_files >= 1);

    let refreshed = outcome.index;
    let results = refreshed.search("rotating secret", 10).unwrap();
    assert_eq!(results[0].path, "src/auth.rs");
}

#[test]
fn refresh_reuses_renamed_files_by_content_fingerprint() {
    let repo = tempfile::tempdir().unwrap();
    write(
        &repo.path().join("src/auth.rs"),
        "pub struct SessionManager;\npub fn issue_token() {}\n",
    );

    let first = FastIndex::build(repo.path()).unwrap();
    fs::rename(
        repo.path().join("src/auth.rs"),
        repo.path().join("src/session.rs"),
    )
    .unwrap();

    let outcome = FastIndex::refresh(repo.path(), Some(&first)).unwrap();
    assert_eq!(outcome.renamed_files, 1);
    assert_eq!(outcome.deleted_files, 0);
    assert_eq!(outcome.refreshed_files, 0);
    assert_eq!(outcome.reused_files, 1);

    let refreshed = outcome.index;
    let results = refreshed.search("SessionManager", 10).unwrap();
    assert_eq!(results[0].path, "src/session.rs");
    assert!(refreshed.search("auth", 10).unwrap().is_empty());
    assert_eq!(
        refreshed
            .files
            .iter()
            .find(|file| file.path == "src/session.rs")
            .unwrap()
            .content_hash,
        first.files[0].content_hash
    );
}

#[test]
fn indexed_search_and_read_range_use_persisted_snapshot_text() {
    let repo = tempfile::tempdir().unwrap();
    write(
        &repo.path().join("src/auth.rs"),
        "pub struct SessionManager;\nimpl SessionManager {\n    pub fn issue_token(&self) -> String {\n        \"token\".to_string()\n    }\n}\n",
    );
    let index_path = repo.path().join(".orient/auth.index");
    let index = FastIndex::build(repo.path()).unwrap();
    index.save(&index_path).unwrap();
    fs::remove_file(repo.path().join("src/auth.rs")).unwrap();

    let loaded = FastIndex::load(&index_path).unwrap();
    let mut results = loaded
        .search_filtered(
            "issue_token",
            10,
            &SearchFilters {
                snippet: SnippetMode::Block,
                ..SearchFilters::default()
            },
        )
        .unwrap();
    assert_eq!(results[0].path, "src/auth.rs");
    assert!(results[0].snippet.contains("3:     pub fn issue_token"));
    assert_eq!(results[0].line_range.as_ref().unwrap().start_line, 1);
    attach_result_context(&mut results, 3, |path, start, lines| {
        loaded.read_range(path, start, lines)
    })
    .unwrap();
    let context = results[0].context.as_ref().unwrap();
    assert_eq!(context.start_line, 2);
    assert!(context.text.contains("3:     pub fn issue_token"));

    let range = loaded.read_range("src/auth.rs", 2, 3).unwrap();
    assert_eq!(range.start_line, 2);
    assert_eq!(range.end_line, 4);
    assert!(range.text.contains("2: impl SessionManager"));
    assert!(range.text.contains("3:     pub fn issue_token"));
    assert!(loaded.read_range("../src/auth.rs", 1, 1).is_err());
    assert!(loaded.read_range("src/../auth.rs", 1, 1).is_err());
    assert!(loaded.read_range("src\\..\\auth.rs", 1, 1).is_err());
}

#[test]
fn indexed_search_supports_filters_require_all_and_symbol_boosting() {
    let repo = tempfile::tempdir().unwrap();
    write(
        &repo.path().join("src/auth.rs"),
        "pub struct SessionManager;\npub fn issue_token() {}\n",
    );
    write(
        &repo.path().join("docs/session.md"),
        "Session manager notes mention session manager many times.\n",
    );
    write(
        &repo.path().join("src/session.ts"),
        "export function issueToken() { return 'token' }\n",
    );

    let index = FastIndex::build(repo.path()).unwrap();
    let results = index
        .search_filtered(
            "SessionManager",
            10,
            &SearchFilters {
                path: Some("src/".to_string()),
                language: Some("rust".to_string()),
                extension: Some("rs".to_string()),
                require_all: true,
                ..SearchFilters::default()
            },
        )
        .unwrap();

    assert_eq!(results.len(), 1);
    assert_eq!(results[0].path, "src/auth.rs");
    assert!(results[0].reason.contains("symbol:SessionManager"));
    assert!(results[0].snippet.contains("1:"));
}

#[test]
fn indexed_symbol_lookup_returns_definition_paths() {
    let repo = tempfile::tempdir().unwrap();
    write(
        &repo.path().join("src/auth.rs"),
        "pub struct SessionManager;\npub fn issue_token() {}\n",
    );
    write(
        &repo.path().join("src/session.rs"),
        "pub fn session_manager_helper() {}\n",
    );

    let index = FastIndex::build(repo.path()).unwrap();
    let symbols = index.find_symbol("SessionManager", 10);

    assert_eq!(symbols[0].name, "SessionManager");
    assert_eq!(symbols[0].kind, "struct");
    assert_eq!(symbols[0].path, "src/auth.rs");
    assert_eq!(symbols[0].line, 1);

    let normalized = index.find_symbol("issue token", 10);
    assert_eq!(normalized[0].name, "issue_token");
    assert!(index.find_symbol("", 10).is_empty());
    assert!(index.find_symbol("SessionManager", 0).is_empty());
}

#[test]
fn indexed_repo_map_returns_orientation_from_persisted_metadata() {
    let repo = tempfile::tempdir().unwrap();
    write(
        &repo.path().join("src/lib.rs"),
        "pub struct SessionManager;\npub fn issue_token() {}\n",
    );
    write(
        &repo.path().join("tests/auth_test.rs"),
        "use sample::SessionManager;\n#[test]\nfn issue_token_round_trip() {}\n",
    );
    write(
        &repo.path().join("Cargo.toml"),
        "[package]\nname='sample'\nversion='0.1.0'\nedition='2024'\n",
    );
    write(
        &repo.path().join("package.json"),
        r#"{"scripts":{"test":"vitest run","lint":"eslint .","build":"vite build"}}"#,
    );
    write(&repo.path().join("yarn.lock"), "# yarn lockfile\n");

    let map = FastIndex::build(repo.path()).unwrap().repo_map(5, 5);

    assert!(map.entrypoints.contains(&"src/lib.rs".to_string()));
    assert!(map.test_files.contains(&"tests/auth_test.rs".to_string()));
    assert!(map.brief.manifest_files.contains(&"Cargo.toml".to_string()));
    assert!(
        map.brief
            .important_files
            .contains(&"Cargo.toml".to_string())
    );
    assert!(map.brief.known_commands.contains(&"cargo test".to_string()));
    assert!(map.brief.known_commands.contains(&"yarn test".to_string()));
    assert!(
        map.brief
            .known_commands
            .contains(&"yarn run lint".to_string())
    );
    assert!(
        map.brief
            .known_commands
            .contains(&"yarn run build".to_string())
    );
    assert_eq!(map.top_symbols[0].name, "SessionManager");
}

#[test]
fn indexed_related_context_uses_persisted_metadata() {
    let repo = tempfile::tempdir().unwrap();
    write(
        &repo.path().join("src/auth.rs"),
        "pub struct SessionManager;\npub fn issue_token() {}\n",
    );
    write(
        &repo.path().join("tests/auth_test.rs"),
        "use sample::SessionManager;\n#[test]\nfn issue_token_round_trip() {}\n",
    );

    let index_path = repo.path().join("orient.index");
    let index = FastIndex::build(repo.path()).unwrap();
    index.save(&index_path).unwrap();
    fs::remove_file(repo.path().join("tests/auth_test.rs")).unwrap();
    let loaded = FastIndex::load(&index_path).unwrap();

    let related_files = loaded.related_files("src/auth.rs", 10);
    assert!(
        related_files
            .iter()
            .any(|file| file.path == "tests/auth_test.rs"),
        "{related_files:?}"
    );

    let related_symbols = loaded.related_symbols(Some("src/auth.rs"), Some("SessionManager"), 10);
    assert!(
        related_symbols.iter().any(|symbol| {
            symbol.symbol.name == "SessionManager"
                && symbol.symbol.path == "src/auth.rs"
                && symbol.reason.contains("same file")
        }),
        "{related_symbols:?}"
    );
}

#[test]
fn fallback_search_boosts_exact_symbols() {
    let repo = tempfile::tempdir().unwrap();
    write(
        &repo.path().join("docs/session_manager.md"),
        "SessionManager SessionManager SessionManager notes.\n",
    );
    write(
        &repo.path().join("src/auth.rs"),
        "pub struct SessionManager;\npub fn issue_token() {}\n",
    );

    let results =
        search_repo_fast_filtered(repo.path(), "SessionManager", 10, &SearchFilters::default())
            .unwrap();

    assert_eq!(results[0].path, "src/auth.rs");
    assert!(results[0].reason.contains("symbol:SessionManager"));
}

#[test]
fn query_language_filters_fallback_and_indexed_search() {
    let repo = tempfile::tempdir().unwrap();
    write(
        &repo.path().join("src/auth.rs"),
        "pub struct SessionManager;\npub fn issue_token() {}\n",
    );
    write(
        &repo.path().join("src/session.ts"),
        "export function SessionManager() { return 'doc-ish' }\n",
    );
    write(
        &repo.path().join("tests/auth_test.rs"),
        "pub fn issue_token_test() {}\n",
    );
    write(
        &repo.path().join("docs/auth.md"),
        "SessionManager issue token docs.\n",
    );

    let query = r#"symbol:sessionmanager lang:Rust ext:.RS path:SRC -path:DOCS "issue token""#;
    let fallback =
        search_repo_fast_filtered(repo.path(), query, 10, &SearchFilters::default()).unwrap();
    assert_eq!(fallback.len(), 1);
    assert_eq!(fallback[0].path, "src/auth.rs");

    let indexed = FastIndex::build(repo.path()).unwrap();
    let indexed_results = indexed
        .search_filtered(query, 10, &SearchFilters::default())
        .unwrap();
    assert_eq!(indexed_results.len(), 1);
    assert_eq!(indexed_results[0].path, "src/auth.rs");

    let file_filtered = search_repo_fast_filtered(
        repo.path(),
        r#"file:AUTH.RS issue token"#,
        10,
        &SearchFilters::default(),
    )
    .unwrap();
    assert_eq!(file_filtered.len(), 1);
    assert_eq!(file_filtered[0].path, "src/auth.rs");

    let test_results = search_repo_fast_filtered(
        repo.path(),
        r#"test:true issue token -path:src"#,
        10,
        &SearchFilters::default(),
    )
    .unwrap();
    assert_eq!(test_results.len(), 1);
    assert_eq!(test_results[0].path, "tests/auth_test.rs");
}

#[test]
fn quoted_phrases_require_exact_matches_and_explain_phrase_signals() {
    let repo = tempfile::tempdir().unwrap();
    write(
        &repo.path().join("src/exact.rs"),
        "pub fn connect() {\n    let message = \"database connection refused\";\n}\n",
    );
    write(
        &repo.path().join("src/scattered.rs"),
        "pub fn connect() {\n    let database = \"primary\";\n    let connection = \"pool\";\n    let refused = true;\n}\n",
    );

    let fallback = search_repo_fast_filtered(
        repo.path(),
        "\"database connection refused\"",
        10,
        &SearchFilters {
            explain: true,
            ..SearchFilters::default()
        },
    )
    .unwrap();
    assert_eq!(fallback.len(), 1);
    assert_eq!(fallback[0].path, "src/exact.rs");
    assert!(
        fallback[0]
            .reason
            .contains("phrase:database connection refused")
    );
    assert!(
        fallback[0]
            .explanation
            .as_ref()
            .unwrap()
            .iter()
            .any(|signal| signal.kind == "line_phrase")
    );

    let index = FastIndex::build(repo.path()).unwrap();
    let indexed = index
        .search_filtered(
            "\"database connection refused\"",
            10,
            &SearchFilters {
                explain: true,
                ..SearchFilters::default()
            },
        )
        .unwrap();
    assert_eq!(indexed.len(), 1);
    assert_eq!(indexed[0].path, "src/exact.rs");
    assert_eq!(
        indexed[0].query_plan.as_ref().unwrap().query_phrases,
        vec!["database connection refused"]
    );
    assert!(
        indexed[0]
            .explanation
            .as_ref()
            .unwrap()
            .iter()
            .any(|signal| signal.kind == "content_phrase")
    );
}

#[test]
fn indexed_search_supports_line_offsets_and_snippet_modes() {
    let repo = tempfile::tempdir().unwrap();
    write(
        &repo.path().join("src/auth.rs"),
        "pub struct SessionManager;\n\
         impl SessionManager {\n\
         fn helper() {}\n\
         pub fn issue_token() {\n\
         let token = \"session\";\n\
         }\n\
         }\n",
    );

    let index = FastIndex::build(repo.path()).unwrap();
    let auth = index
        .files
        .iter()
        .find(|file| file.path == "src/auth.rs")
        .unwrap();
    assert!(auth.line_offsets.len() >= 7);
    assert!(
        auth.term_lines
            .iter()
            .any(|entry| entry.term == "token" && entry.lines == vec![4, 5])
    );

    let short = index
        .search_filtered(
            "issue token",
            10,
            &SearchFilters {
                snippet: SnippetMode::Short,
                ..SearchFilters::default()
            },
        )
        .unwrap();
    assert_eq!(short[0].path, "src/auth.rs");
    assert_eq!(short[0].snippet.lines().count(), 1);
    assert!(short[0].snippet.contains("4:"));
    assert_eq!(short[0].line_range.as_ref().unwrap().start_line, 4);
    assert_eq!(short[0].line_range.as_ref().unwrap().end_line, 4);
    assert_eq!(short[0].match_lines, vec![4, 5]);

    let block = index
        .search_filtered(
            "issue token",
            10,
            &SearchFilters {
                snippet: SnippetMode::Block,
                ..SearchFilters::default()
            },
        )
        .unwrap();
    assert!(block[0].snippet.contains("1: pub struct SessionManager;"));
    assert!(block[0].snippet.contains("7: }"));
    assert_eq!(block[0].line_range.as_ref().unwrap().start_line, 1);
    assert_eq!(block[0].line_range.as_ref().unwrap().end_line, 7);

    let symbol = index
        .search_filtered(
            "SessionManager",
            10,
            &SearchFilters {
                snippet: SnippetMode::Symbol,
                ..SearchFilters::default()
            },
        )
        .unwrap();
    assert!(
        symbol[0]
            .snippet
            .starts_with("1: pub struct SessionManager;")
    );
    assert_eq!(symbol[0].line_range.as_ref().unwrap().start_line, 1);
}

#[test]
fn indexed_search_uses_trigram_postings_for_substring_queries() {
    let repo = tempfile::tempdir().unwrap();
    write(
        &repo.path().join("src/auth.rs"),
        "pub struct SessionManager;\npub fn issue_token() {}\n",
    );
    write(
        &repo.path().join("src/billing.rs"),
        "pub fn invoice_total() {}\n",
    );

    let index = FastIndex::build(repo.path()).unwrap();
    let stats = index.stats();
    assert!(stats.trigrams > 0);
    let auth = index
        .files
        .iter()
        .find(|file| file.path == "src/auth.rs")
        .unwrap();
    assert!(auth.trigrams.iter().any(|term| term.term == "ess"));

    let results = index.search("essionman", 10).unwrap();
    assert_eq!(results[0].path, "src/auth.rs");
    assert!(results[0].reason.contains("trigrams"));
    assert!(!results.iter().any(|result| result.path == "src/billing.rs"));
}

#[test]
fn indexed_search_uses_path_postings_for_path_only_matches() {
    let repo = tempfile::tempdir().unwrap();
    write(
        &repo.path().join("src/auth_gateway.rs"),
        "pub fn issue_token() {}\n",
    );
    write(&repo.path().join("src/billing.rs"), "pub fn auth() {}\n");

    let index = FastIndex::build(repo.path()).unwrap();
    let stats = index.stats();
    assert!(stats.path_terms > 0);
    let auth = index
        .files
        .iter()
        .find(|file| file.path == "src/auth_gateway.rs")
        .unwrap();
    assert!(auth.path_terms.iter().any(|term| term.term == "gateway"));

    let results = index.search("gateway", 10).unwrap();
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].path, "src/auth_gateway.rs");

    let explained = index
        .search_filtered(
            "gateway",
            10,
            &SearchFilters {
                explain: true,
                ..SearchFilters::default()
            },
        )
        .unwrap();
    assert!(
        explained[0]
            .explanation
            .as_ref()
            .unwrap()
            .iter()
            .any(|signal| signal.kind == "path_term")
    );
}

#[test]
fn indexed_trigram_planner_unions_single_literal_and_substring_candidates() {
    let repo = tempfile::tempdir().unwrap();
    write(
        &repo.path().join("src/auth.rs"),
        "pub struct SessionManager;\n",
    );
    write(
        &repo.path().join("tests/fixture.rs"),
        "const QUERY: &str = \"essionman\";\n",
    );

    let index = FastIndex::build(repo.path()).unwrap();
    let results = index.search("essionman", 10).unwrap();
    assert!(results.iter().any(|result| result.path == "src/auth.rs"));
    assert!(
        results
            .iter()
            .any(|result| result.path == "tests/fixture.rs")
    );
}

#[test]
fn search_explain_mode_returns_structured_rank_signals() {
    let repo = tempfile::tempdir().unwrap();
    write(
        &repo.path().join("src/auth.rs"),
        "pub struct SessionManager;\npub fn issue_token() {}\n",
    );

    let fallback = search_repo_fast_filtered(
        repo.path(),
        "SessionManager",
        10,
        &SearchFilters {
            explain: true,
            ..SearchFilters::default()
        },
    )
    .unwrap();
    let fallback_signals = fallback[0].explanation.as_ref().unwrap();
    assert!(
        fallback_signals
            .iter()
            .any(|signal| signal.kind == "symbol_exact" && signal.value == "SessionManager")
    );
    assert_eq!(fallback[0].line_range.as_ref().unwrap().start_line, 1);
    assert_eq!(fallback[0].line_range.as_ref().unwrap().end_line, 1);
    assert_eq!(fallback[0].match_lines, vec![1]);

    let index = FastIndex::build(repo.path()).unwrap();
    let indexed = index
        .search_filtered(
            "SessionManager",
            10,
            &SearchFilters {
                explain: true,
                ..SearchFilters::default()
            },
        )
        .unwrap();
    let indexed_signals = indexed[0].explanation.as_ref().unwrap();
    assert!(
        indexed_signals
            .iter()
            .any(|signal| signal.kind == "symbol_exact" && signal.value == "SessionManager")
    );
    assert_eq!(indexed[0].line_range.as_ref().unwrap().start_line, 1);
    assert_eq!(indexed[0].line_range.as_ref().unwrap().end_line, 2);
    assert_eq!(indexed[0].match_lines, vec![1]);
    let plan = indexed[0].query_plan.as_ref().unwrap();
    assert_eq!(plan.strategy, "posting_intersection");
    assert_eq!(plan.query_tokens, vec!["session", "manager"]);
    assert!(plan.candidate_count >= 1);
    assert!(
        plan.planned_postings
            .iter()
            .any(|posting| posting.kind == "content" && posting.value == "session")
    );

    let compact =
        search_repo_fast_filtered(repo.path(), "SessionManager", 10, &SearchFilters::default())
            .unwrap();
    assert!(compact[0].explanation.is_none());
    assert!(compact[0].query_plan.is_none());
}

#[test]
fn loading_corrupt_index_returns_error() {
    let repo = tempfile::tempdir().unwrap();
    let path = repo.path().join("corrupt.index");
    fs::write(&path, b"not a bincode orient index").unwrap();

    let error = FastIndex::load(&path).unwrap_err().to_string();
    assert!(error.contains("parse index"));
}

#[test]
fn fallback_search_matches_rg_for_golden_corpus() {
    if Command::new("rg").arg("--version").output().is_err() {
        return;
    }

    let repo = tempfile::tempdir().unwrap();
    write(
        &repo.path().join("src/auth.rs"),
        "pub fn issue_token() { let token = \"session\"; }\n",
    );
    write(
        &repo.path().join("src/billing.rs"),
        "pub fn invoice_total() -> usize { 42 }\n",
    );
    write(
        &repo.path().join("README.md"),
        "The auth module issues session tokens.\n",
    );

    let rg_output = Command::new("rg")
        .current_dir(repo.path())
        .args(["--files-with-matches", "issue|token"])
        .output()
        .unwrap();
    assert!(rg_output.status.success());
    let rg_paths = String::from_utf8(rg_output.stdout).unwrap();

    let results =
        search_repo_fast_filtered(repo.path(), "issue token", 10, &SearchFilters::default())
            .unwrap();
    assert!(rg_paths.contains("src/auth.rs"));
    assert!(results.iter().any(|result| result.path == "src/auth.rs"));
    assert!(!results.iter().any(|result| result.path == "src/billing.rs"));
}

#[test]
fn fallback_block_snippets_do_not_append_overlapping_blocks() {
    let repo = tempfile::tempdir().unwrap();
    write(
        &repo.path().join("src/auth.rs"),
        "pub struct SessionManager;\n\
         impl SessionManager {\n\
         pub fn issue_token() {\n\
         let token = \"session\";\n\
         let backup_token = \"backup\";\n\
         }\n\
         }\n",
    );

    let results = search_repo_fast_filtered(
        repo.path(),
        "token",
        10,
        &SearchFilters {
            snippet: SnippetMode::Block,
            ..SearchFilters::default()
        },
    )
    .unwrap();

    let snippet = &results[0].snippet;
    assert_eq!(snippet.matches("1: pub struct SessionManager;").count(), 1);
}

#[test]
fn fast_search_deduplicates_repeated_worktree_hits() {
    let repo = tempfile::tempdir().unwrap();
    write(
        &repo.path().join("one/src/auth.rs"),
        "pub fn issue_token() { let token = \"session\"; }\n",
    );
    write(
        &repo.path().join("two/src/auth.rs"),
        "pub fn issue_token() { let token = \"session\"; }\n",
    );

    let results = search_repo_fast_filtered(
        repo.path(),
        "issue token session",
        10,
        &SearchFilters {
            require_all: true,
            ..SearchFilters::default()
        },
    )
    .unwrap();

    assert_eq!(results.len(), 1);
    assert_eq!(results[0].path, "one/src/auth.rs");
    let group = results[0].duplicate_group.as_ref().unwrap();
    assert_eq!(group.canonical_path, "src/auth.rs");
    assert_eq!(group.duplicate_count, 1);
    assert_eq!(group.duplicate_paths, vec!["two/src/auth.rs"]);

    let indexed = FastIndex::build(repo.path())
        .unwrap()
        .search_filtered(
            "issue token session",
            10,
            &SearchFilters {
                require_all: true,
                ..SearchFilters::default()
            },
        )
        .unwrap();
    assert_eq!(indexed.len(), 1);
    assert_eq!(indexed[0].path, "one/src/auth.rs");
    let group = indexed[0].duplicate_group.as_ref().unwrap();
    assert_eq!(group.canonical_path, "src/auth.rs");
    assert_eq!(group.duplicate_count, 1);
    assert_eq!(group.duplicate_paths, vec!["two/src/auth.rs"]);
}

#[test]
fn fast_search_deduplicates_repeated_manifest_hits_by_snippet() {
    let repo = tempfile::tempdir().unwrap();
    let manifest = "[package]\nname = \"sample\"\nversion = \"0.1.0\"\n";
    write(&repo.path().join("alpha/Cargo.toml"), manifest);
    write(&repo.path().join("beta/Cargo.toml"), manifest);

    let results = search_repo_fast_filtered(
        repo.path(),
        "package sample",
        10,
        &SearchFilters {
            require_all: true,
            snippet: SnippetMode::Block,
            ..SearchFilters::default()
        },
    )
    .unwrap();

    assert_eq!(results.len(), 1);
    assert_eq!(results[0].path, "alpha/Cargo.toml");
    let group = results[0].duplicate_group.as_ref().unwrap();
    assert_eq!(group.canonical_path, "Cargo.toml");
    assert_eq!(group.duplicate_count, 1);
    assert_eq!(group.duplicate_paths, vec!["beta/Cargo.toml"]);
}

#[test]
fn fast_search_timeout_is_bounded() {
    let repo = tempfile::tempdir().unwrap();
    for index in 0..200 {
        write(
            &repo.path().join(format!("src/file_{index}.rs")),
            "pub fn unrelated_symbol() {}\n",
        );
    }

    let started = Instant::now();
    let results = search_repo_fast_filtered_with_timeout(
        repo.path(),
        "unrelated symbol",
        10,
        &SearchFilters::default(),
        Duration::from_nanos(1),
    )
    .unwrap();

    assert!(started.elapsed() < Duration::from_millis(500));
    assert!(results.len() <= 10);
}
