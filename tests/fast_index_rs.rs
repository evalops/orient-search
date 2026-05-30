use std::fs;
use std::path::Path;
use std::process::Command;
use std::time::{Duration, Instant};

use orient::fast_index::FastIndex;
use orient::repo_index::{
    MAX_READ_RANGE_LINES, MAX_SEARCH_RESULTS, SearchFilters, SearchResult, SnippetMode,
    attach_result_context, search_repo_fast_filtered, search_repo_fast_filtered_with_timeout,
};
use orient::shards::{build_shards, search_shards, shard_query_plans};

fn write(path: &Path, text: &str) {
    fs::create_dir_all(path.parent().unwrap()).unwrap();
    fs::write(path, text).unwrap();
}

fn result_paths(results: &[SearchResult]) -> Vec<String> {
    let mut paths = results
        .iter()
        .map(|result| result.path.clone())
        .collect::<Vec<_>>();
    paths.sort();
    paths
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
    assert!(
        refreshed
            .related_files("src/session.rs", 10)
            .iter()
            .all(|related| related.path != "src/auth.rs")
    );
    assert!(refreshed.related_files("src/auth.rs", 10).is_empty());
    let symbols = refreshed.find_symbol("SessionManager", 10);
    assert_eq!(symbols[0].path, "src/session.rs");
    let related_symbols = refreshed.related_symbols(Some("src/session.rs"), None, 10);
    assert!(
        related_symbols
            .iter()
            .any(|related| related.symbol.name == "SessionManager"
                && related.symbol.path == "src/session.rs"),
        "{related_symbols:?}"
    );
    assert!(
        refreshed
            .related_symbols(Some("src/auth.rs"), None, 10)
            .is_empty()
    );
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
fn indexed_freshness_reports_added_changed_and_deleted_files() {
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
    let clean = index.freshness().unwrap();
    assert!(!clean.stale);
    assert_eq!(clean.changed_files, 0);
    assert_eq!(clean.deleted_files, 0);
    assert_eq!(clean.added_files, 0);

    write(
        &repo.path().join("src/auth.rs"),
        "pub struct SessionManager;\npub fn issue_token() {}\npub fn rotate_secret_now() {}\n",
    );
    fs::remove_file(repo.path().join("src/billing.rs")).unwrap();
    write(
        &repo.path().join("src/new_session.rs"),
        "pub fn new_session_token() {}\n",
    );

    let stale = index.freshness().unwrap();
    assert!(stale.stale);
    assert_eq!(stale.changed_paths, vec!["src/auth.rs"]);
    assert_eq!(stale.deleted_paths, vec!["src/billing.rs"]);
    assert_eq!(stale.added_paths, vec!["src/new_session.rs"]);
}

#[test]
fn indexed_search_and_read_range_use_persisted_snapshot_text() {
    let repo = tempfile::tempdir().unwrap();
    write(
        &repo.path().join("src/auth.rs"),
        "pub struct SessionManager;\nimpl SessionManager {\n    pub fn issue_token(&self) -> String {\n        \"token\".to_string()\n    }\n}\n",
    );
    let long_text = (1..=MAX_READ_RANGE_LINES + 10)
        .map(|line| format!("pub fn line_{line}() {{}}\n"))
        .collect::<String>();
    write(&repo.path().join("src/long.rs"), &long_text);
    write(
        &repo.path().join("src/no_trailing_newline.rs"),
        "pub fn first() {}\npub fn second() {}",
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
    let dot_range = loaded.read_range("./src/auth.rs", 2, 1).unwrap();
    assert_eq!(dot_range.path, "src/auth.rs");
    assert!(dot_range.text.contains("2: impl SessionManager"));
    let cased_range = loaded.read_range("./SRC/AUTH.RS", 2, 1).unwrap();
    assert_eq!(cased_range.path, "src/auth.rs");
    assert!(cased_range.text.contains("2: impl SessionManager"));
    let capped = loaded
        .read_range("src/long.rs", 1, MAX_READ_RANGE_LINES + 10)
        .unwrap();
    assert_eq!(capped.end_line, MAX_READ_RANGE_LINES);
    assert_eq!(capped.total_lines, MAX_READ_RANGE_LINES + 10);
    assert!(!capped.text.contains(&format!(
        "{}: pub fn line_{}",
        MAX_READ_RANGE_LINES + 1,
        MAX_READ_RANGE_LINES + 1
    )));
    let no_trailing = loaded
        .read_range("src/no_trailing_newline.rs", 2, 1)
        .unwrap();
    assert_eq!(no_trailing.start_line, 2);
    assert_eq!(no_trailing.end_line, 2);
    assert_eq!(no_trailing.total_lines, 2);
    assert_eq!(no_trailing.text, "2: pub fn second() {}");
    assert!(loaded.read_range("../src/auth.rs", 1, 1).is_err());
    assert!(loaded.read_range("src/../auth.rs", 1, 1).is_err());
    assert!(loaded.read_range("src\\..\\auth.rs", 1, 1).is_err());
}

#[test]
fn saved_indexes_have_versioned_header_and_legacy_indexes_still_load() {
    let repo = tempfile::tempdir().unwrap();
    write(
        &repo.path().join("src/auth.rs"),
        "pub struct SessionManager;\npub fn issue_token() {}\n",
    );
    let index = FastIndex::build(repo.path()).unwrap();

    let index_path = repo.path().join(".orient/header.index");
    index.save(&index_path).unwrap();
    let bytes = fs::read(&index_path).unwrap();
    assert!(bytes.starts_with(b"ORIENTIDX\0"));
    let version = u32::from_le_bytes(bytes[10..14].try_into().unwrap());
    assert_eq!(version, index.version);
    let loaded = FastIndex::load(&index_path).unwrap();
    assert_eq!(loaded.version, index.version);
    assert_eq!(loaded.files[0].path_phrase_text, "src auth rs");
    assert_eq!(loaded.files[0].file_name_lower, "auth.rs");
    assert_eq!(loaded.files[0].extension_lower.as_deref(), Some("rs"));
    assert_eq!(loaded.files[0].symbols[0].name_lower, "sessionmanager");
    assert_eq!(
        loaded.search("SessionManager", 10).unwrap()[0].path,
        "src/auth.rs"
    );

    let legacy_path = repo.path().join(".orient/legacy.index");
    let mut legacy_raw_index = index.clone();
    for file in &mut legacy_raw_index.files {
        for symbol in &mut file.symbols {
            symbol.name_lower.clear();
        }
    }
    fs::write(&legacy_path, bincode::serialize(&legacy_raw_index).unwrap()).unwrap();
    let legacy = FastIndex::load(&legacy_path).unwrap();
    assert_eq!(legacy.version, index.version);
    assert_eq!(legacy.files[0].path_phrase_text, "src auth rs");
    assert_eq!(legacy.files[0].file_name_lower, "auth.rs");
    assert_eq!(legacy.files[0].extension_lower.as_deref(), Some("rs"));
    assert_eq!(legacy.files[0].symbols[0].name_lower, "sessionmanager");
    assert_eq!(
        legacy.search("issue token", 10).unwrap()[0].path,
        "src/auth.rs"
    );

    let legacy_header_path = repo.path().join(".orient/legacy-header.index");
    let mut legacy_index = index.clone();
    legacy_index.version = 9;
    let mut legacy_header_bytes = Vec::new();
    legacy_header_bytes.extend_from_slice(b"ORIENTIDX\0");
    legacy_header_bytes.extend_from_slice(&9u32.to_le_bytes());
    legacy_header_bytes.extend_from_slice(&bincode::serialize(&legacy_index).unwrap());
    fs::write(&legacy_header_path, legacy_header_bytes).unwrap();
    let legacy_header = FastIndex::load(&legacy_header_path).unwrap();
    assert_eq!(legacy_header.version, index.version);
    assert_eq!(
        legacy_header.search("issue token", 10).unwrap()[0].path,
        "src/auth.rs"
    );
}

#[test]
fn saved_indexes_compress_large_posting_lists_on_disk() {
    let repo = tempfile::tempdir().unwrap();
    for index in 0..160 {
        write(
            &repo.path().join(format!("src/file_{index:03}.rs")),
            &format!("pub fn repeated_symbol_{index:03}() {{ let common_token = {index}; }}\n"),
        );
    }
    let index = FastIndex::build(repo.path()).unwrap();
    let raw_len = bincode::serialize(&index).unwrap().len() + 14;
    let index_path = repo.path().join(".orient/compressed.index");
    index.save(&index_path).unwrap();
    let compressed_len = fs::read(&index_path).unwrap().len();

    assert!(
        compressed_len < raw_len,
        "compressed={compressed_len} raw={raw_len}"
    );
    assert_eq!(
        FastIndex::load(&index_path)
            .unwrap()
            .search("common token", 10)
            .unwrap()
            .len(),
        10
    );
}

#[test]
fn legacy_raw_indexes_normalize_posting_order_on_load() {
    let repo = tempfile::tempdir().unwrap();
    write(
        &repo.path().join("src/a.rs"),
        "pub fn shared_token() { let shared = true; }\n",
    );
    write(
        &repo.path().join("src/b.rs"),
        "pub fn shared_token_b() { let shared = false; }\n",
    );
    let mut index = FastIndex::build(repo.path()).unwrap();
    index.version = 9;
    index.postings.get_mut("shared").unwrap().reverse();

    let legacy_path = repo.path().join(".orient/unsorted-legacy.index");
    fs::create_dir_all(legacy_path.parent().unwrap()).unwrap();
    fs::write(&legacy_path, bincode::serialize(&index).unwrap()).unwrap();
    let loaded = FastIndex::load(&legacy_path).unwrap();
    let shared = loaded.postings.get("shared").unwrap();
    assert!(
        shared
            .windows(2)
            .all(|pair| pair[0].file_id < pair[1].file_id)
    );
    assert_eq!(loaded.search("shared token", 10).unwrap().len(), 2);
}

#[test]
fn indexed_posting_lists_are_sorted_for_direct_lookup() {
    let repo = tempfile::tempdir().unwrap();
    write(
        &repo.path().join("src/a.rs"),
        "pub fn shared_token() { let needle = 1; }\n",
    );
    write(
        &repo.path().join("src/b.rs"),
        "pub fn shared_token() { let needle = 2; }\n",
    );
    write(
        &repo.path().join("tests/shared_test.rs"),
        "#[test]\nfn shared_token_test() { assert!(true); }\n",
    );

    let index = FastIndex::build(repo.path()).unwrap();
    for postings in index
        .postings
        .values()
        .chain(index.path_postings.values())
        .chain(index.trigram_postings.values())
    {
        assert!(
            postings
                .windows(2)
                .all(|pair| pair[0].file_id <= pair[1].file_id)
        );
    }
    assert_eq!(index.search("shared token", 10).unwrap().len(), 3);
}

#[test]
fn indexed_planner_intersects_sorted_posting_lists() {
    let repo = tempfile::tempdir().unwrap();
    write(
        &repo.path().join("src/alpha_beta.rs"),
        "pub fn alpha_beta() { let alpha = true; let beta = true; }\n",
    );
    write(
        &repo.path().join("src/alpha_gamma.rs"),
        "pub fn alpha_gamma() { let alpha = true; let gamma = true; }\n",
    );
    write(
        &repo.path().join("src/beta_gamma.rs"),
        "pub fn beta_gamma() { let beta = true; let gamma = true; }\n",
    );

    let index = FastIndex::build(repo.path()).unwrap();
    let results = index.search("alpha beta", 10).unwrap();
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].path, "src/alpha_beta.rs");

    let plan = index
        .query_plan("alpha beta", &SearchFilters::default())
        .unwrap();
    assert_eq!(plan.candidate_count, 1);
    assert_eq!(plan.final_match_count, 1);
}

#[test]
fn saving_index_replaces_existing_file_without_leaving_temp_files() {
    let repo = tempfile::tempdir().unwrap();
    write(
        &repo.path().join("src/auth.rs"),
        "pub struct SessionManager;\npub fn issue_token() {}\n",
    );
    let index_path = repo.path().join(".orient/atomic.index");
    let first = FastIndex::build(repo.path()).unwrap();
    first.save(&index_path).unwrap();
    let first_bytes = fs::read(&index_path).unwrap();

    write(
        &repo.path().join("src/billing.rs"),
        "pub fn invoice_total() {}\n",
    );
    let second = FastIndex::build(repo.path()).unwrap();
    second.save(&index_path).unwrap();
    let second_bytes = fs::read(&index_path).unwrap();

    assert_ne!(first_bytes, second_bytes);
    let loaded = FastIndex::load(&index_path).unwrap();
    assert_eq!(
        loaded.search("invoice total", 10).unwrap()[0].path,
        "src/billing.rs"
    );
    let temp_files = fs::read_dir(index_path.parent().unwrap())
        .unwrap()
        .filter_map(|entry| entry.ok())
        .filter(|entry| entry.file_name().to_string_lossy().contains(".tmp-"))
        .collect::<Vec<_>>();
    assert!(temp_files.is_empty(), "{temp_files:?}");
}

#[test]
fn loading_header_with_unsupported_version_returns_clear_error() {
    let repo = tempfile::tempdir().unwrap();
    let path = repo.path().join("future.index");
    let mut bytes = Vec::new();
    bytes.extend_from_slice(b"ORIENTIDX\0");
    bytes.extend_from_slice(&u32::MAX.to_le_bytes());
    bytes.extend_from_slice(b"payload");
    fs::write(&path, bytes).unwrap();

    let error = FastIndex::load(&path).unwrap_err().to_string();
    assert!(error.contains("unsupported index version"), "{error}");
}

#[test]
fn search_result_limits_are_capped() {
    let repo = tempfile::tempdir().unwrap();
    for index in 0..MAX_SEARCH_RESULTS + 25 {
        write(
            &repo.path().join(format!("src/file_{index:03}.rs")),
            &format!("pub fn sharedcaptoken_{index:03}() {{}}\n"),
        );
    }

    let fallback = search_repo_fast_filtered(
        repo.path(),
        "sharedcaptoken",
        MAX_SEARCH_RESULTS + 25,
        &SearchFilters::default(),
    )
    .unwrap();
    assert_eq!(fallback.len(), MAX_SEARCH_RESULTS);

    let index = FastIndex::build(repo.path()).unwrap();
    let indexed = index
        .search_filtered(
            "sharedcaptoken",
            MAX_SEARCH_RESULTS + 25,
            &SearchFilters::default(),
        )
        .unwrap();
    assert_eq!(indexed.len(), MAX_SEARCH_RESULTS);
}

#[test]
fn indexed_search_warns_when_candidate_cap_is_hit() {
    let repo = tempfile::tempdir().unwrap();
    for index in 0..700 {
        write(
            &repo.path().join(format!("src/file_{index:04}.rs")),
            "pub fn shared_cap_token() {}\n",
        );
    }
    for index in 0..400 {
        write(
            &repo.path().join(format!("tests/file_{index:04}_test.rs")),
            "pub fn shared_cap_token() {}\n",
        );
    }

    let index = FastIndex::build(repo.path()).unwrap();
    let results = index
        .search_filtered(
            "shared cap token",
            1,
            &SearchFilters {
                explain: true,
                ..SearchFilters::default()
            },
        )
        .unwrap();
    assert_eq!(results.len(), 1);
    let plan = results[0].query_plan.as_ref().unwrap();
    assert_eq!(plan.candidate_count, 1100);
    assert_eq!(plan.candidate_cap, 1024);
    assert!(plan.candidate_cap_hit);
    assert!(plan.final_match_count > 0);
    assert!(plan.repair_hints.iter().any(|hint| {
        hint.kind == "narrow_query"
            && hint.message.contains("capped scoring at 1024")
            && hint.suggested_query.is_none()
    }));
    assert!(plan.repair_hints.iter().any(|hint| {
        hint.kind == "narrow_by_path"
            && hint.message.contains("from 1100 files to 700")
            && hint.suggested_query.as_deref() == Some("shared cap token path:src")
    }));
    assert!(plan.repair_hints.iter().any(|hint| {
        hint.kind == "narrow_by_test"
            && hint.message.contains("from 1100 files to 400")
            && hint.suggested_query.as_deref() == Some("shared cap token test:true")
    }));
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
fn explicit_any_terms_relaxes_default_and_for_orientation() {
    let repo = tempfile::tempdir().unwrap();
    write(
        &repo.path().join("src/alpha.rs"),
        "pub fn alpha_only() {}\n",
    );
    write(&repo.path().join("src/beta.rs"), "pub fn beta_only() {}\n");
    write(
        &repo.path().join("src/both.rs"),
        "pub fn alpha_beta() { let beta = 1; }\n",
    );

    let strict =
        search_repo_fast_filtered(repo.path(), "alpha beta", 10, &SearchFilters::default())
            .unwrap();
    assert_eq!(
        strict
            .iter()
            .map(|result| result.path.as_str())
            .collect::<Vec<_>>(),
        vec!["src/both.rs"]
    );

    let relaxed = search_repo_fast_filtered(
        repo.path(),
        "mode:any alpha beta",
        10,
        &SearchFilters::default(),
    )
    .unwrap();
    let relaxed_paths = relaxed
        .iter()
        .map(|result| result.path.as_str())
        .collect::<Vec<_>>();
    assert!(relaxed_paths.contains(&"src/alpha.rs"));
    assert!(relaxed_paths.contains(&"src/beta.rs"));
    assert!(relaxed_paths.contains(&"src/both.rs"));

    let index = FastIndex::build(repo.path()).unwrap();
    let indexed = index
        .search_filtered(
            "alpha beta",
            10,
            &SearchFilters {
                match_any: true,
                ..SearchFilters::default()
            },
        )
        .unwrap();
    let indexed_paths = indexed
        .iter()
        .map(|result| result.path.as_str())
        .collect::<Vec<_>>();
    assert!(indexed_paths.contains(&"src/alpha.rs"));
    assert!(indexed_paths.contains(&"src/beta.rs"));
    assert!(indexed_paths.contains(&"src/both.rs"));

    let plan = index
        .query_plan("mode:any alpha beta", &SearchFilters::default())
        .unwrap();
    assert!(!plan.require_all);
    assert!(plan.candidate_count >= 3);
    assert!(plan.final_match_count >= 3);
}

#[test]
fn fallback_strict_search_rescues_late_matches_after_rg_max_count() {
    let repo = tempfile::tempdir().unwrap();
    let mut text = String::new();
    for index in 0..20 {
        text.push_str(&format!("pub const ALPHA_{index}: &str = \"alpha\";\n"));
    }
    text.push_str("pub fn target_value() -> &'static str { \"done\" }\n");
    write(&repo.path().join("src/lib.rs"), &text);

    let results = search_repo_fast_filtered(
        repo.path(),
        "alpha target_value",
        5,
        &SearchFilters::default(),
    )
    .unwrap();

    assert_eq!(results.len(), 1);
    assert_eq!(results[0].path, "src/lib.rs");
    assert!(results[0].reason.contains("alpha"), "{results:?}");
    assert!(results[0].reason.contains("target"), "{results:?}");
    assert!(results[0].reason.contains("value"), "{results:?}");
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
    write(
        &repo.path().join("tests/auth_test.rs"),
        "pub struct SessionManager;\n",
    );

    let index = FastIndex::build(repo.path()).unwrap();
    let symbols = index.find_symbol("SessionManager", 10);

    assert_eq!(symbols[0].name, "SessionManager");
    assert_eq!(symbols[0].kind, "struct");
    assert_eq!(symbols[0].path, "src/auth.rs");
    assert_eq!(symbols[0].line, 1);
    assert!(
        symbols
            .iter()
            .any(|symbol| symbol.name == "session_manager_helper"),
        "{symbols:?}"
    );

    let exact_limited = index.find_symbol("SessionManager", 1);
    assert_eq!(exact_limited.len(), 1);
    assert_eq!(exact_limited[0].name, "SessionManager");

    let normalized = index.find_symbol("issue token", 10);
    assert_eq!(normalized[0].name, "issue_token");
    let source_symbols = index.find_symbol_filtered(
        "SessionManager",
        10,
        &SearchFilters {
            test: Some(false),
            ..SearchFilters::default()
        },
    );
    assert!(
        source_symbols
            .iter()
            .all(|symbol| symbol.path != "tests/auth_test.rs"),
        "{source_symbols:?}"
    );
    let function_symbols = index.find_symbol_filtered(
        "SessionManager",
        10,
        &SearchFilters {
            symbol_kind: Some("function".to_string()),
            ..SearchFilters::default()
        },
    );
    assert!(
        function_symbols
            .iter()
            .all(|symbol| symbol.kind == "function"),
        "{function_symbols:?}"
    );
    assert!(
        function_symbols
            .iter()
            .any(|symbol| symbol.name == "session_manager_helper"),
        "{function_symbols:?}"
    );
    assert!(index.find_symbol("", 10).is_empty());
    assert!(index.find_symbol("SessionManager", 0).is_empty());
}

#[test]
fn indexed_repo_map_returns_orientation_from_persisted_metadata() {
    let repo = tempfile::tempdir().unwrap();
    write(
        &repo.path().join("src/lib.rs"),
        "use serde::Serialize;\npub struct SessionManager;\npub fn issue_token() {}\n",
    );
    write(
        &repo.path().join("tests/auth_test.rs"),
        "use sample::SessionManager;\n#[test]\nfn issue_token_round_trip() {}\n",
    );
    write(
        &repo.path().join("Cargo.toml"),
        "[package]\nname='sample'\nversion='0.1.0'\nedition='2024'\n[dependencies]\nserde='1'\n[dev-dependencies]\ninsta='1'\n",
    );
    write(
        &repo.path().join("package.json"),
        r#"{"scripts":{"test":"vitest run","lint":"eslint .","build":"vite build"},"dependencies":{"react":"latest"},"devDependencies":{"vite":"latest"}}"#,
    );
    write(
        &repo.path().join("MODULE.bazel"),
        "module(name = \"sample\")\n",
    );
    write(
        &repo.path().join("BUILD.bazel"),
        "exports_files([\"Cargo.toml\"])\n",
    );
    write(
        &repo.path().join("Justfile"),
        "test:\n    cargo test\ncheck:\n    cargo check\n",
    );
    write(
        &repo.path().join("Makefile"),
        "test:\n\tcargo test\nbuild:\n\tcargo build\n",
    );
    write(
        &repo.path().join("pom.xml"),
        "<project><modelVersion>4.0.0</modelVersion></project>\n",
    );
    write(&repo.path().join("build.gradle"), "plugins { id 'java' }\n");
    write(&repo.path().join("gradlew"), "#!/bin/sh\n");
    write(&repo.path().join("yarn.lock"), "# yarn lockfile\n");

    let map = FastIndex::build(repo.path()).unwrap().repo_map(5, 5);

    assert!(map.entrypoints.contains(&"src/lib.rs".to_string()));
    assert!(map.test_files.contains(&"tests/auth_test.rs".to_string()));
    assert!(map.brief.manifest_files.contains(&"Cargo.toml".to_string()));
    assert!(
        map.brief
            .manifest_files
            .contains(&"MODULE.bazel".to_string())
    );
    assert!(map.brief.manifest_files.contains(&"pom.xml".to_string()));
    assert!(
        map.brief
            .manifest_files
            .contains(&"build.gradle".to_string())
    );
    assert!(
        map.brief
            .important_files
            .contains(&"Cargo.toml".to_string())
    );
    assert!(
        map.brief
            .important_files
            .contains(&"MODULE.bazel".to_string())
    );
    assert!(map.brief.important_files.contains(&"Justfile".to_string()));
    assert!(map.brief.important_files.contains(&"Makefile".to_string()));
    assert!(map.brief.important_files.contains(&"gradlew".to_string()));
    assert!(map.brief.known_commands.contains(&"cargo test".to_string()));
    assert!(
        map.brief
            .known_commands
            .contains(&"bazel build //...".to_string())
    );
    assert!(
        map.brief
            .known_commands
            .contains(&"bazel test //...".to_string())
    );
    assert!(map.brief.known_commands.contains(&"just test".to_string()));
    assert!(map.brief.known_commands.contains(&"just check".to_string()));
    assert!(map.brief.known_commands.contains(&"make test".to_string()));
    assert!(map.brief.known_commands.contains(&"make build".to_string()));
    assert!(map.brief.known_commands.contains(&"mvn test".to_string()));
    assert!(
        map.brief
            .known_commands
            .contains(&"mvn package".to_string())
    );
    assert!(
        map.brief
            .known_commands
            .contains(&"./gradlew test".to_string())
    );
    assert!(
        map.brief
            .known_commands
            .contains(&"./gradlew build".to_string())
    );
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
    assert!(map.brief.command_hints.iter().any(|hint| {
        hint.command == "yarn test" && hint.kind == "test" && hint.source == "package.json"
    }));
    assert!(map.brief.command_hints.iter().any(|hint| {
        hint.command == "cargo test" && hint.kind == "test" && hint.source == "Cargo.toml"
    }));
    assert!(map.brief.command_hints.iter().any(|hint| {
        hint.command == "bazel build //..." && hint.kind == "build" && hint.source == "MODULE.bazel"
    }));
    assert!(map.brief.command_hints.iter().any(|hint| {
        hint.command == "bazel test //..." && hint.kind == "test" && hint.source == "MODULE.bazel"
    }));
    assert!(map.brief.command_hints.iter().any(|hint| {
        hint.command == "just check" && hint.kind == "check" && hint.source == "Justfile"
    }));
    assert!(map.brief.command_hints.iter().any(|hint| {
        hint.command == "make build" && hint.kind == "build" && hint.source == "Makefile"
    }));
    assert!(map.brief.command_hints.iter().any(|hint| {
        hint.command == "mvn test" && hint.kind == "test" && hint.source == "pom.xml"
    }));
    assert!(map.brief.command_hints.iter().any(|hint| {
        hint.command == "mvn package" && hint.kind == "build" && hint.source == "pom.xml"
    }));
    assert!(map.brief.command_hints.iter().any(|hint| {
        hint.command == "./gradlew test" && hint.kind == "test" && hint.source == "build.gradle"
    }));
    assert!(map.brief.command_hints.iter().any(|hint| {
        hint.command == "./gradlew build" && hint.kind == "build" && hint.source == "build.gradle"
    }));
    assert!(map.brief.dependency_hints.iter().any(|hint| {
        hint.name == "serde" && hint.kind == "dependency" && hint.source == "Cargo.toml"
    }));
    assert!(map.brief.dependency_hints.iter().any(|hint| {
        hint.name == "insta" && hint.kind == "dev_dependency" && hint.source == "Cargo.toml"
    }));
    assert!(map.brief.dependency_hints.iter().any(|hint| {
        hint.name == "react" && hint.kind == "dependency" && hint.source == "package.json"
    }));
    assert!(map.brief.dependency_hints.iter().any(|hint| {
        hint.name == "vite" && hint.kind == "dev_dependency" && hint.source == "package.json"
    }));
    assert!(map.brief.import_hints.iter().any(|hint| {
        hint.module == "serde::Serialize" && hint.kind == "use" && hint.source == "src/lib.rs"
    }));
    assert!(map.brief.import_hints.iter().any(|hint| {
        hint.module == "sample::SessionManager"
            && hint.kind == "use"
            && hint.source == "tests/auth_test.rs"
    }));
    assert_eq!(map.top_symbols[0].name, "SessionManager");
    assert!(
        map.related_files.iter().any(|related| {
            related.source_path == "src/lib.rs" && related.path == "tests/auth_test.rs"
        }),
        "{:?}",
        map.related_files
    );
    assert!(
        map.related_symbols.iter().any(|related| {
            related.source_path == "src/lib.rs" && related.symbol.name == "SessionManager"
        }),
        "{:?}",
        map.related_symbols
    );
}

#[test]
fn indexed_query_plan_reports_missing_terms_without_results() {
    let repo = tempfile::tempdir().unwrap();
    write(
        &repo.path().join("src/auth.rs"),
        "pub struct SessionManager;\npub fn issue_token() {}\n",
    );
    write(
        &repo.path().join("tests/auth_test.rs"),
        "use sample::SessionManager;\n#[test]\nfn issue_token_round_trip() {}\n",
    );
    write(
        &repo.path().join("src/SessionManager.rs"),
        "pub struct MixedCaseSessionManager;\n",
    );

    let index = FastIndex::build(repo.path()).unwrap();
    let plan = index
        .query_plan(
            "SessionManager definitely_missing",
            &SearchFilters::default(),
        )
        .unwrap();

    assert_eq!(plan.strategy, "posting_intersection");
    assert!(plan.require_all);
    assert_eq!(
        plan.query_tokens,
        vec!["session", "manager", "definitely", "missing"]
    );
    assert_eq!(plan.missing_terms, vec!["definitely", "missing"]);
    assert_eq!(plan.candidate_count, 0);
    assert_eq!(plan.filtered_candidate_count, 0);
    assert_eq!(plan.scored_candidate_count, 0);
    assert_eq!(plan.final_match_count, 0);
    let diagnosis = plan.diagnosis.as_ref().unwrap();
    assert_eq!(diagnosis.status, "missing_terms");
    assert_eq!(
        diagnosis.primary_hint_kind.as_deref(),
        Some("drop_missing_terms")
    );
    assert_eq!(diagnosis.primary_hint_action.as_deref(), Some("drop_terms"));
    assert_eq!(
        diagnosis.suggested_query.as_deref(),
        Some("session manager")
    );
    assert!(diagnosis.next_action.contains("retry"));
    assert!(plan.repair_hints.iter().any(|hint| {
        hint.kind == "drop_missing_terms"
            && hint.suggested_query.as_deref() == Some("session manager")
    }));
    assert!(
        plan.planned_postings
            .iter()
            .any(|posting| posting.kind == "content" && posting.value == "session")
    );

    let symbol_typo_plan = index
        .query_plan("symbol:SessionManger", &SearchFilters::default())
        .unwrap();
    assert_eq!(symbol_typo_plan.final_match_count, 0);
    assert!(symbol_typo_plan.repair_hints.iter().any(|hint| {
        hint.kind == "replace_symbol_filter"
            && hint.suggested_query.as_deref() == Some("symbol:SessionManager")
            && hint.message.contains("No indexed symbol exactly matches")
    }));
    let diagnosis = symbol_typo_plan.diagnosis.as_ref().unwrap();
    assert_eq!(diagnosis.status, "filters_rejected");
    assert_eq!(
        diagnosis.primary_hint_kind.as_deref(),
        Some("replace_symbol_filter")
    );
    assert_eq!(
        diagnosis.primary_hint_action.as_deref(),
        Some("replace_filter")
    );
    assert_eq!(
        diagnosis.suggested_query.as_deref(),
        Some("symbol:SessionManager")
    );

    let filter_plan = index
        .query_plan(
            "lang:rust test:true",
            &SearchFilters {
                explain: true,
                ..SearchFilters::default()
            },
        )
        .unwrap();
    assert_eq!(filter_plan.strategy, "attribute_filter_postings");
    assert_eq!(filter_plan.candidate_count, 1);
    assert_eq!(filter_plan.filtered_candidate_count, 1);
    assert_eq!(filter_plan.scored_candidate_count, 1);
    assert_eq!(filter_plan.final_match_count, 1);
    assert!(filter_plan.repair_hints.is_empty());
    assert!(filter_plan.missing_terms.is_empty());
    assert!(
        filter_plan
            .planned_postings
            .iter()
            .any(|posting| posting.kind == "filter" && posting.value == "language:rust")
    );
    assert!(
        filter_plan
            .planned_postings
            .iter()
            .any(|posting| posting.kind == "filter" && posting.value == "test:true")
    );
    assert!(
        filter_plan.active_filters.iter().any(|filter| {
            filter.field == "language" && filter.value == "rust" && !filter.negated
        })
    );
    assert!(
        filter_plan
            .active_filters
            .iter()
            .any(|filter| { filter.field == "test" && filter.value == "true" && !filter.negated })
    );

    let kind_typo_plan = index
        .query_plan("kind:functoin", &SearchFilters::default())
        .unwrap();
    assert_eq!(kind_typo_plan.strategy, "symbol_kind_filter_postings");
    assert_eq!(kind_typo_plan.candidate_count, 0);
    assert_eq!(kind_typo_plan.filtered_candidate_count, 0);
    assert_eq!(kind_typo_plan.scored_candidate_count, 0);
    assert_eq!(kind_typo_plan.final_match_count, 0);
    assert_eq!(
        kind_typo_plan.repair_hints[0].kind,
        "replace_symbol_kind_filter"
    );
    assert_eq!(
        kind_typo_plan.repair_hints[0].suggested_query.as_deref(),
        Some("kind:function")
    );
    assert!(
        kind_typo_plan.repair_hints[0]
            .message
            .contains("Available kinds: function, struct"),
        "{:?}",
        kind_typo_plan.repair_hints
    );

    let kind_typo_search = index
        .search_filtered(
            "kind:functoin",
            10,
            &SearchFilters {
                explain: true,
                ..SearchFilters::default()
            },
        )
        .unwrap();
    assert!(kind_typo_search.is_empty());

    let bad_file_plan = index
        .query_plan("file:not_real.rs lang:rust", &SearchFilters::default())
        .unwrap();
    assert_eq!(bad_file_plan.strategy, "attribute_filter_postings");
    assert_eq!(bad_file_plan.final_match_count, 0);
    assert_eq!(bad_file_plan.repair_hints[0].kind, "relax_file_filter");
    assert_eq!(
        bad_file_plan.repair_hints[0].suggested_query.as_deref(),
        Some("")
    );
    assert!(bad_file_plan.repair_hints.iter().any(|hint| {
        hint.kind == "relax_language_filter" && hint.suggested_query.as_deref() == Some("")
    }));
    assert!(
        bad_file_plan
            .repair_hints
            .iter()
            .any(|hint| hint.kind == "relax_filters" && hint.suggested_query.is_none())
    );

    let single_filter_plan = index
        .query_plan("file:not_real.rs", &SearchFilters::default())
        .unwrap();
    assert_eq!(single_filter_plan.repair_hints[0].kind, "relax_file_filter");
    assert!(single_filter_plan.repair_hints[0].suggested_query.is_none());

    let file_typo_plan = index
        .query_plan("file:athu.rs", &SearchFilters::default())
        .unwrap();
    assert_eq!(file_typo_plan.repair_hints[0].kind, "replace_file_filter");
    assert_eq!(
        file_typo_plan.repair_hints[0].suggested_query.as_deref(),
        Some("file:auth.rs")
    );

    let path_typo_plan = index
        .query_plan("path:src/ath.rs", &SearchFilters::default())
        .unwrap();
    assert_eq!(path_typo_plan.repair_hints[0].kind, "replace_path_filter");
    assert_eq!(
        path_typo_plan.repair_hints[0].suggested_query.as_deref(),
        Some("path:src/auth.rs")
    );

    let cased_file_typo_plan = index
        .query_plan("file:SessionManger.rs", &SearchFilters::default())
        .unwrap();
    assert_eq!(
        cased_file_typo_plan.repair_hints[0]
            .suggested_query
            .as_deref(),
        Some("file:SessionManager.rs")
    );

    let accidental_path_in_file_plan = index
        .query_plan("file:src/ath.rs", &SearchFilters::default())
        .unwrap();
    assert_eq!(
        accidental_path_in_file_plan.repair_hints[0].kind,
        "replace_file_filter"
    );
    assert_eq!(
        accidental_path_in_file_plan.repair_hints[0]
            .suggested_query
            .as_deref(),
        Some("path:src/auth.rs")
    );

    let wildcard_file_plan = index
        .query_plan("file:*athu.rs", &SearchFilters::default())
        .unwrap();
    assert_eq!(wildcard_file_plan.repair_hints[0].kind, "relax_file_filter");

    let wildcard_path_plan = index
        .query_plan("path:src/*ath.rs", &SearchFilters::default())
        .unwrap();
    assert_eq!(wildcard_path_plan.repair_hints[0].kind, "relax_path_filter");

    let branch_mismatch = index
        .query_plan(
            "branch:not-real-branch SessionManager",
            &SearchFilters::default(),
        )
        .unwrap();
    assert_eq!(branch_mismatch.strategy, "repo_filter_mismatch");
    assert_eq!(branch_mismatch.repair_hints[0].kind, "relax_branch_filter");
    assert_eq!(
        branch_mismatch.repair_hints[0].suggested_query.as_deref(),
        Some("SessionManager")
    );
}

#[test]
fn indexed_query_plan_suggests_any_terms_for_strict_and_misses() {
    let repo = tempfile::tempdir().unwrap();
    write(
        &repo.path().join("src/alpha.rs"),
        "pub fn alpha_only() {}\n",
    );
    write(&repo.path().join("src/beta.rs"), "pub fn beta_only() {}\n");

    let index = FastIndex::build(repo.path()).unwrap();
    let plan = index
        .query_plan("alpha beta", &SearchFilters::default())
        .unwrap();

    assert_eq!(plan.strategy, "posting_intersection");
    assert!(plan.require_all);
    assert_eq!(plan.missing_terms, Vec::<String>::new());
    assert_eq!(plan.candidate_count, 0);
    assert_eq!(plan.final_match_count, 0);
    let diagnosis = plan.diagnosis.as_ref().unwrap();
    assert_eq!(diagnosis.status, "no_candidates");
    assert_eq!(
        diagnosis.primary_hint_kind.as_deref(),
        Some("try_any_terms")
    );
    assert_eq!(
        diagnosis.suggested_query.as_deref(),
        Some("mode:any alpha beta")
    );
    assert!(plan.repair_hints.iter().any(|hint| {
        hint.kind == "try_any_terms"
            && hint.action == "broaden_terms"
            && hint.suggested_query.as_deref() == Some("mode:any alpha beta")
            && hint.message.contains("no file contains all terms")
    }));

    let relaxed = index
        .query_plan("mode:any alpha beta", &SearchFilters::default())
        .unwrap();
    assert_eq!(relaxed.strategy, "posting_union");
    assert!(!relaxed.require_all);
    assert!(relaxed.final_match_count >= 2);
}

#[test]
fn indexed_query_plan_suggests_facets_for_noisy_successful_queries() {
    let repo = tempfile::tempdir().unwrap();
    for index in 0..5 {
        write(
            &repo.path().join(format!("src/worker_{index}.rs")),
            &format!("pub fn sharedneedle_worker_{index}() {{}}\n"),
        );
    }
    for index in 0..17 {
        write(
            &repo.path().join(format!("docs/guide_{index}.md")),
            &format!("sharedneedle setup guide {index}\n"),
        );
    }

    let index = FastIndex::build(repo.path()).unwrap();
    let plan = index
        .query_plan("sharedneedle", &SearchFilters::default())
        .unwrap();

    assert!(plan.final_match_count > 0);
    assert!(!plan.candidate_cap_hit);
    let diagnosis = plan.diagnosis.as_ref().unwrap();
    assert_eq!(diagnosis.status, "matched");
    assert!(
        diagnosis
            .primary_hint_kind
            .as_deref()
            .unwrap()
            .starts_with("narrow_by")
    );
    assert!(diagnosis.suggested_query.as_deref().is_some());
    assert!(plan.repair_hints.iter().any(|hint| {
        hint.kind == "narrow_by_code"
            && hint.action == "narrow"
            && hint.suggested_query.as_deref() == Some("sharedneedle code:true")
            && hint.message.contains("from 22 files to 5")
    }));
}

#[test]
fn indexed_query_plan_prefers_nested_path_facets_for_common_roots() {
    let repo = tempfile::tempdir().unwrap();
    for index in 0..12 {
        write(
            &repo.path().join(format!("src/auth/session_{index}.rs")),
            &format!("pub fn sharedneedle_auth_{index}() {{}}\n"),
        );
    }
    for index in 0..8 {
        write(
            &repo.path().join(format!("src/billing/invoice_{index}.rs")),
            &format!("pub fn sharedneedle_billing_{index}() {{}}\n"),
        );
    }

    let index = FastIndex::build(repo.path()).unwrap();
    let plan = index
        .query_plan("sharedneedle", &SearchFilters::default())
        .unwrap();

    assert_eq!(plan.candidate_count, 20);
    assert!(plan.final_match_count > 0);
    assert!(plan.repair_hints.iter().any(|hint| {
        hint.kind == "narrow_by_path"
            && hint.action == "narrow"
            && hint.suggested_query.as_deref() == Some("sharedneedle path:src/auth")
            && hint.message.contains("from 20 files to 12")
    }));
}

#[test]
fn indexed_query_plan_keeps_dotted_nested_path_facets() {
    let repo = tempfile::tempdir().unwrap();
    for index in 0..11 {
        write(
            &repo.path().join(format!("src/api.v2/handler_{index}.rs")),
            &format!("pub fn versionedneedle_api_{index}() {{}}\n"),
        );
    }
    for index in 0..7 {
        write(
            &repo.path().join(format!("src/ui/view_{index}.rs")),
            &format!("pub fn versionedneedle_ui_{index}() {{}}\n"),
        );
    }

    let index = FastIndex::build(repo.path()).unwrap();
    let plan = index
        .query_plan("versionedneedle", &SearchFilters::default())
        .unwrap();

    assert!(plan.repair_hints.iter().any(|hint| {
        hint.kind == "narrow_by_path"
            && hint.suggested_query.as_deref() == Some("versionedneedle path:src/api.v2")
            && hint.message.contains("from 18 files to 11")
    }));
}

#[test]
fn indexed_query_plan_facet_retries_preserve_existing_scope() {
    let repo = tempfile::tempdir().unwrap();
    for index in 0..12 {
        write(
            &repo.path().join(format!("src/auth/session_{index}.rs")),
            &format!("pub fn scopedneedle_auth_{index}() {{}}\n"),
        );
    }
    for index in 0..8 {
        write(
            &repo.path().join(format!("src/billing/invoice_{index}.rs")),
            &format!("pub fn scopedneedle_billing_{index}() {{}}\n"),
        );
    }

    let index = FastIndex::build(repo.path()).unwrap();
    let plan = index
        .query_plan(
            "mode:any lang:rust -path:legacy scopedneedle",
            &SearchFilters::default(),
        )
        .unwrap();

    assert!(plan.repair_hints.iter().any(|hint| {
        hint.kind == "narrow_by_path"
            && hint.suggested_query.as_deref()
                == Some("mode:any scopedneedle path:src/auth lang:rust -path:legacy")
    }));
}

#[test]
fn indexed_query_plan_suggests_symbol_kind_facet_for_noisy_definition_searches() {
    let repo = tempfile::tempdir().unwrap();
    for index in 0..5 {
        write(
            &repo.path().join(format!("src/handler_{index}.rs")),
            &format!("pub fn sharedneedle_handler_{index}() {{}}\n"),
        );
    }
    for index in 0..17 {
        write(
            &repo.path().join(format!("src/comment_{index}.rs")),
            &format!("// sharedneedle background note {index}\n"),
        );
    }

    let index = FastIndex::build(repo.path()).unwrap();
    let plan = index
        .query_plan("sharedneedle", &SearchFilters::default())
        .unwrap();

    assert!(plan.final_match_count > 0);
    assert!(!plan.candidate_cap_hit);
    assert!(plan.repair_hints.iter().any(|hint| {
        hint.kind == "narrow_by_symbol_kind"
            && hint.action == "narrow"
            && hint.suggested_query.as_deref() == Some("sharedneedle kind:function")
            && hint.message.contains("from 22 files to 5")
    }));
}

#[test]
fn indexed_query_plan_counts_filter_and_phrase_rejections() {
    let repo = tempfile::tempdir().unwrap();
    write(
        &repo.path().join("src/auth.rs"),
        "pub struct SessionManager;\npub fn issue_token() {}\n",
    );

    let index = FastIndex::build(repo.path()).unwrap();
    let filter_rejected = index
        .query_plan(
            "SessionManager path:tests",
            &SearchFilters {
                explain: true,
                ..SearchFilters::default()
            },
        )
        .unwrap();
    assert_eq!(filter_rejected.candidate_count, 0);
    assert_eq!(filter_rejected.filtered_candidate_count, 0);
    assert_eq!(filter_rejected.scored_candidate_count, 0);
    assert_eq!(filter_rejected.final_match_count, 0);
    assert_eq!(filter_rejected.active_filters.len(), 1);
    assert_eq!(filter_rejected.active_filters[0].field, "path");
    assert_eq!(filter_rejected.active_filters[0].value, "tests");
    assert!(!filter_rejected.active_filters[0].negated);
    assert_eq!(filter_rejected.active_filters[0].candidate_matches, Some(0));
    assert_eq!(
        filter_rejected.active_filters[0].candidate_rejections,
        Some(0)
    );
    assert!(filter_rejected.repair_hints.iter().any(|hint| {
        hint.kind == "relax_path_filter"
            && hint.action == "relax_filter"
            && hint.suggested_query.as_deref() == Some("session manager")
            && hint.message.contains("path:tests")
    }));

    let phrase_rejected = index
        .query_plan(
            "\"session token\"",
            &SearchFilters {
                explain: true,
                ..SearchFilters::default()
            },
        )
        .unwrap();
    assert!(phrase_rejected.candidate_count >= 1);
    assert_eq!(phrase_rejected.filtered_candidate_count, 1);
    assert_eq!(phrase_rejected.scored_candidate_count, 0);
    assert_eq!(phrase_rejected.final_match_count, 0);
    assert!(phrase_rejected.repair_hints.iter().any(|hint| {
        hint.kind == "relax_phrase" && hint.suggested_query.as_deref() == Some("session token")
    }));
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
        "use sample::sessionmanager;\n#[test]\nfn issue_token_round_trip() {}\n",
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
            .any(|file| file.path == "tests/auth_test.rs"
                && file.reason.contains("references symbol SessionManager")),
        "{related_files:?}"
    );
    let cased_related_files = loaded.related_files("./SRC/AUTH.RS", 10);
    assert!(
        cased_related_files
            .iter()
            .any(|file| file.path == "tests/auth_test.rs"
                && file.reason.contains("references symbol SessionManager")),
        "{cased_related_files:?}"
    );
    let test_related_files = loaded.related_files("tests/auth_test.rs", 10);
    assert!(
        test_related_files
            .iter()
            .any(|file| file.path == "src/auth.rs"),
        "{test_related_files:?}"
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
    let cased_related_symbols =
        loaded.related_symbols(Some("./SRC/AUTH.RS"), Some("SessionManager"), 10);
    assert!(
        cased_related_symbols.iter().any(|symbol| {
            symbol.symbol.name == "SessionManager"
                && symbol.symbol.path == "src/auth.rs"
                && symbol.reason.contains("same file")
        }),
        "{cased_related_symbols:?}"
    );
    let exact_query_symbols = loaded.related_symbols(None, Some("SessionManager"), 1);
    assert_eq!(exact_query_symbols.len(), 1);
    assert_eq!(exact_query_symbols[0].symbol.name, "SessionManager");
    assert_eq!(exact_query_symbols[0].symbol.path, "src/auth.rs");
    assert!(
        exact_query_symbols[0].reason.contains("exact query symbol"),
        "{exact_query_symbols:?}"
    );
    let test_related_symbols = loaded.related_symbols(Some("tests/auth_test.rs"), None, 10);
    assert!(
        test_related_symbols
            .iter()
            .any(|symbol| symbol.symbol.name == "SessionManager"
                && symbol.symbol.path == "src/auth.rs"
                && symbol.reason.contains("referenced by source")),
        "{test_related_symbols:?}"
    );
    let fuzzy_query_symbols = loaded.related_symbols(None, Some("issue"), 10);
    assert!(
        fuzzy_query_symbols
            .iter()
            .any(|symbol| symbol.symbol.name == "issue_token"),
        "{fuzzy_query_symbols:?}"
    );

    write(
        &repo.path().join("src/billing.rs"),
        "pub fn repo_lookup_total() {}\npub fn invoice_total() {}\npub struct InvoiceTotal;\n",
    );
    let billing_index = FastIndex::build(repo.path()).unwrap();
    let filter_query_symbols = billing_index.related_symbols(
        Some("src/billing.rs"),
        Some("path:billing invoice total"),
        5,
    );
    let invoice_rank = filter_query_symbols
        .iter()
        .position(|item| item.symbol.name == "invoice_total")
        .unwrap();
    let repo_lookup_rank = filter_query_symbols
        .iter()
        .position(|item| item.symbol.name == "repo_lookup_total")
        .unwrap();
    assert!(invoice_rank < repo_lookup_rank, "{filter_query_symbols:?}");
    assert!(
        filter_query_symbols[invoice_rank]
            .reason
            .contains("exact query symbol"),
        "{filter_query_symbols:?}"
    );
    assert!(
        filter_query_symbols[invoice_rank]
            .reason
            .contains("query overlap"),
        "{filter_query_symbols:?}"
    );
    let kind_filtered_symbols =
        billing_index.related_symbols(Some("src/billing.rs"), Some("kind:struct invoice total"), 5);
    assert_eq!(kind_filtered_symbols[0].symbol.name, "InvoiceTotal");
    assert!(
        kind_filtered_symbols
            .iter()
            .all(|item| item.symbol.kind == "struct"),
        "{kind_filtered_symbols:?}"
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
fn multi_token_identifier_fragments_boost_containing_symbols() {
    let repo = tempfile::tempdir().unwrap();
    write(
        &repo.path().join("docs/retry.md"),
        "primary retry result primary retry result primary retry result notes.\n",
    );
    write(
        &repo.path().join("tests/retry_test.rs"),
        "fn cli_primary_retry_result_mentions() {}\n",
    );
    write(
        &repo.path().join("src/server.rs"),
        "pub(crate) fn search_auto_primary_retry_result() {}\n",
    );

    let filters = SearchFilters {
        explain: true,
        ..SearchFilters::default()
    };
    let fallback =
        search_repo_fast_filtered(repo.path(), "primary_retry_result", 10, &filters).unwrap();
    assert_eq!(fallback[0].path, "src/server.rs");
    assert!(
        fallback[0]
            .explanation
            .as_ref()
            .unwrap()
            .iter()
            .any(|signal| {
                signal.kind == "symbol_boundary_contains"
                    && signal.value == "search_auto_primary_retry_result"
            }),
        "{fallback:?}"
    );

    let index = FastIndex::build(repo.path()).unwrap();
    let indexed = index
        .search_filtered("primary_retry_result", 10, &filters)
        .unwrap();
    assert_eq!(indexed[0].path, "src/server.rs");
    assert!(
        indexed[0]
            .explanation
            .as_ref()
            .unwrap()
            .iter()
            .any(|signal| {
                signal.kind == "symbol_boundary_contains"
                    && signal.value == "search_auto_primary_retry_result"
            }),
        "{indexed:?}"
    );
}

#[test]
fn symbol_filters_accept_multi_token_fragments_without_single_token_overreach() {
    let repo = tempfile::tempdir().unwrap();
    write(
        &repo.path().join("src/server.rs"),
        "pub(crate) fn search_auto_primary_retry_result() {}\n",
    );
    write(&repo.path().join("src/lower.rs"), "fn lower_path() {}\n");
    write(&repo.path().join("src/exact.rs"), "fn path() {}\n");

    let fallback = search_repo_fast_filtered(
        repo.path(),
        "symbol:primary_retry_result",
        10,
        &Default::default(),
    )
    .unwrap();
    assert_eq!(fallback.len(), 1);
    assert_eq!(fallback[0].path, "src/server.rs");
    assert!(
        fallback[0]
            .reason
            .contains("symbol:search_auto_primary_retry_result"),
        "{fallback:?}"
    );

    let fallback_single =
        search_repo_fast_filtered(repo.path(), "symbol:path", 10, &Default::default()).unwrap();
    assert_eq!(result_paths(&fallback_single), vec!["src/exact.rs"]);

    let index = FastIndex::build(repo.path()).unwrap();
    let indexed = index
        .search_filtered("symbol:primary_retry_result", 10, &Default::default())
        .unwrap();
    assert_eq!(indexed.len(), 1);
    assert_eq!(indexed[0].path, "src/server.rs");
    assert!(
        indexed[0]
            .reason
            .contains("symbol:search_auto_primary_retry_result"),
        "{indexed:?}"
    );

    let indexed_single = index
        .search_filtered("symbol:path", 10, &Default::default())
        .unwrap();
    assert_eq!(result_paths(&indexed_single), vec!["src/exact.rs"]);
}

#[test]
fn fallback_refresh_scores_symbols_beyond_ripgrep_match_cap() {
    let repo = tempfile::tempdir().unwrap();
    let mut noisy = String::new();
    for index in 0..40 {
        noisy.push_str(&format!("// symbol query padding {index}\n"));
    }
    noisy.push_str("pub(crate) fn symbol_query_match_score() {}\n");
    write(&repo.path().join("src/noisy.rs"), &noisy);
    write(
        &repo.path().join("src/other.rs"),
        "pub(crate) fn unrelated() {}\n// symbol query match once\n",
    );

    let filters = SearchFilters {
        explain: true,
        ..SearchFilters::default()
    };
    let results = search_repo_fast_filtered(repo.path(), "symbol_query_match", 10, &filters)
        .expect("fallback search succeeds");
    let noisy_result = results
        .iter()
        .find(|result| result.path == "src/noisy.rs")
        .expect("noisy result survives strict phrase verification");
    assert!(
        noisy_result
            .explanation
            .as_ref()
            .unwrap()
            .iter()
            .any(|signal| {
                signal.kind == "symbol_boundary_contains"
                    && signal.value == "symbol_query_match_score"
            }),
        "{noisy_result:?}"
    );
}

#[test]
fn generated_bundle_assets_are_demoted_unless_requested() {
    let repo = tempfile::tempdir().unwrap();
    write(
        &repo.path().join("src/auth.ts"),
        "export function issueToken() { return probeNeedle(); }\n",
    );
    write(
        &repo
            .path()
            .join("webview/assets/chunk-OIYGIGL5-CJrBIAxA.js"),
        "function minified(){return probeNeedle()}\n",
    );

    let filters = SearchFilters {
        explain: true,
        ..SearchFilters::default()
    };
    let fallback = search_repo_fast_filtered(repo.path(), "probeNeedle", 10, &filters).unwrap();
    assert_eq!(fallback[0].path, "src/auth.ts");
    let generated_fallback = fallback
        .iter()
        .find(|result| result.path == "webview/assets/chunk-OIYGIGL5-CJrBIAxA.js")
        .expect("generated fallback hit");
    assert!(
        generated_fallback
            .explanation
            .as_ref()
            .unwrap()
            .iter()
            .any(|signal| signal.kind == "generated_path_penalty")
    );

    let index = FastIndex::build(repo.path()).unwrap();
    let indexed = index.search_filtered("probeNeedle", 10, &filters).unwrap();
    assert_eq!(indexed[0].path, "src/auth.ts");
    let generated_indexed = indexed
        .iter()
        .find(|result| result.path == "webview/assets/chunk-OIYGIGL5-CJrBIAxA.js")
        .expect("generated indexed hit");
    assert!(
        generated_indexed
            .explanation
            .as_ref()
            .unwrap()
            .iter()
            .any(|signal| signal.kind == "generated_path_penalty")
    );

    let generated_only = SearchFilters {
        generated: Some(true),
        ..SearchFilters::default()
    };
    let fallback_generated =
        search_repo_fast_filtered(repo.path(), "probeNeedle", 10, &generated_only).unwrap();
    assert_eq!(
        result_paths(&fallback_generated),
        vec!["webview/assets/chunk-OIYGIGL5-CJrBIAxA.js"]
    );
    let indexed_generated = index
        .search_filtered("probeNeedle", 10, &generated_only)
        .unwrap();
    assert_eq!(
        result_paths(&indexed_generated),
        vec!["webview/assets/chunk-OIYGIGL5-CJrBIAxA.js"]
    );
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
        &repo.path().join("src/session.py"),
        "class SessionManager:\n    pass\n",
    );
    write(
        &repo.path().join("tests/auth_test.rs"),
        "pub fn issue_token_test() {}\n",
    );
    write(
        &repo.path().join("docs/auth.md"),
        "SessionManager issue token docs.\n",
    );
    write(
        &repo.path().join("notes/deprecated_auth.md"),
        "SessionManager issue token docs deprecated.\n",
    );
    write(
        &repo.path().join("src/generated/session.generated.rs"),
        "pub struct SessionManagerGenerated;\npub fn issue_token_generated() {}\n",
    );
    write(
        &repo.path().join("src/plain.js"),
        "export const rareagentneedle = 'handwritten';\n",
    );
    write(
        &repo.path().join("webview/assets/chunk-A1b2c3d4.js"),
        "rareagentneedle rareagentneedle rareagentneedle rareagentneedle rareagentneedle rareagentneedle rareagentneedle rareagentneedle rareagentneedle rareagentneedle rareagentneedle rareagentneedle\n",
    );
    write(
        &repo.path().join("README.md"),
        "SessionManager daemon status docs.\n",
    );

    let query = r#"symbol:sessionmanager lang:Rust ext:.RS dir:SRC -dir:DOCS "issue token""#;
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

    let cli_style_query = r#"file-name:auth.rs target-line:1 require-all:true exclude-path:docs exclude-symbol-kind:class "issue token""#;
    let cli_style_filters = SearchFilters::default();
    let fallback_cli_style =
        search_repo_fast_filtered(repo.path(), cli_style_query, 10, &cli_style_filters).unwrap();
    assert_eq!(result_paths(&fallback_cli_style), vec!["src/auth.rs"]);
    assert_eq!(fallback_cli_style[0].match_lines[0], 1);
    let indexed_cli_style = indexed
        .search_filtered(cli_style_query, 10, &cli_style_filters)
        .unwrap();
    assert_eq!(result_paths(&indexed_cli_style), vec!["src/auth.rs"]);
    assert_eq!(indexed_cli_style[0].match_lines[0], 1);

    let fallback_class_shorthand = search_repo_fast_filtered(
        repo.path(),
        "class:SessionManager",
        10,
        &SearchFilters::default(),
    )
    .unwrap();
    assert_eq!(
        result_paths(&fallback_class_shorthand),
        vec!["src/session.py"]
    );
    let indexed_class_shorthand = indexed
        .search_filtered("class:SessionManager", 10, &SearchFilters::default())
        .unwrap();
    assert_eq!(
        result_paths(&indexed_class_shorthand),
        vec!["src/session.py"]
    );

    let negative_term_query = "content:SessionManager -deprecated";
    let fallback_negative_term = search_repo_fast_filtered(
        repo.path(),
        negative_term_query,
        10,
        &SearchFilters::default(),
    )
    .unwrap();
    let fallback_negative_term_paths = result_paths(&fallback_negative_term);
    assert!(fallback_negative_term_paths.contains(&"docs/auth.md".to_string()));
    assert!(!fallback_negative_term_paths.contains(&"notes/deprecated_auth.md".to_string()));
    let indexed_negative_term = indexed
        .search_filtered(negative_term_query, 10, &SearchFilters::default())
        .unwrap();
    let indexed_negative_term_paths = result_paths(&indexed_negative_term);
    assert!(indexed_negative_term_paths.contains(&"docs/auth.md".to_string()));
    assert!(!indexed_negative_term_paths.contains(&"notes/deprecated_auth.md".to_string()));

    let line_query = "path:src/auth.rs line:1 issue token";
    let fallback_line =
        search_repo_fast_filtered(repo.path(), line_query, 10, &SearchFilters::default()).unwrap();
    assert_eq!(result_paths(&fallback_line), vec!["src/auth.rs"]);
    assert_eq!(fallback_line[0].match_lines[0], 1);
    assert!(
        fallback_line[0]
            .snippet
            .contains("1: pub struct SessionManager")
    );
    let indexed_line = indexed
        .search_filtered(line_query, 10, &SearchFilters::default())
        .unwrap();
    assert_eq!(result_paths(&indexed_line), vec!["src/auth.rs"]);
    assert_eq!(indexed_line[0].match_lines[0], 1);
    assert!(
        indexed_line[0]
            .snippet
            .contains("1: pub struct SessionManager")
    );

    let fallback_ts = search_repo_fast_filtered(
        repo.path(),
        "lang:ts SessionManager",
        10,
        &Default::default(),
    )
    .unwrap();
    assert_eq!(result_paths(&fallback_ts), vec!["src/session.ts"]);
    let indexed_ts = indexed
        .search_filtered("lang:ts SessionManager", 10, &Default::default())
        .unwrap();
    assert_eq!(result_paths(&indexed_ts), vec!["src/session.ts"]);

    let fallback_markdown = search_repo_fast_filtered(
        repo.path(),
        "lang:md SessionManager",
        10,
        &Default::default(),
    )
    .unwrap();
    let fallback_markdown_paths = result_paths(&fallback_markdown);
    assert!(fallback_markdown_paths.contains(&"docs/auth.md".to_string()));
    assert!(fallback_markdown_paths.contains(&"README.md".to_string()));
    let indexed_markdown = indexed
        .search_filtered("lang:md SessionManager", 10, &Default::default())
        .unwrap();
    let indexed_markdown_paths = result_paths(&indexed_markdown);
    assert!(indexed_markdown_paths.contains(&"docs/auth.md".to_string()));
    assert!(indexed_markdown_paths.contains(&"README.md".to_string()));

    let fallback_docs_wildcard = search_repo_fast_filtered(
        repo.path(),
        "path:docs/*.md SessionManager",
        10,
        &Default::default(),
    )
    .unwrap();
    assert_eq!(result_paths(&fallback_docs_wildcard), vec!["docs/auth.md"]);
    let indexed_docs_wildcard = indexed
        .search_filtered("path:docs/*.md SessionManager", 10, &Default::default())
        .unwrap();
    assert_eq!(result_paths(&indexed_docs_wildcard), vec!["docs/auth.md"]);
    let wildcard_plan = indexed
        .query_plan("path:docs/*.md SessionManager", &Default::default())
        .unwrap();
    assert!(
        !wildcard_plan
            .planned_postings
            .iter()
            .any(|posting| posting.kind == "symbol" && posting.value == "sessionmanager"),
        "{:?}",
        wildcard_plan.planned_postings
    );

    let indexed_extension_markdown = indexed
        .search_filtered("ext:md SessionManager", 10, &Default::default())
        .unwrap();
    assert!(result_paths(&indexed_extension_markdown).contains(&"docs/auth.md".to_string()));

    let fallback_excluding_rust = search_repo_fast_filtered(
        repo.path(),
        "-ext:rs SessionManager",
        10,
        &Default::default(),
    )
    .unwrap();
    let fallback_excluding_rust_paths = result_paths(&fallback_excluding_rust);
    assert!(fallback_excluding_rust_paths.contains(&"docs/auth.md".to_string()));
    assert!(!fallback_excluding_rust_paths.contains(&"src/auth.rs".to_string()));
    let indexed_excluding_rust = indexed
        .search_filtered("-ext:rs SessionManager", 10, &Default::default())
        .unwrap();
    let indexed_excluding_rust_paths = result_paths(&indexed_excluding_rust);
    assert!(indexed_excluding_rust_paths.contains(&"docs/auth.md".to_string()));
    assert!(!indexed_excluding_rust_paths.contains(&"src/auth.rs".to_string()));
    let negative_extension_plan = indexed
        .query_plan("-ext:rs SessionManager", &Default::default())
        .unwrap();
    assert!(
        !negative_extension_plan
            .planned_postings
            .iter()
            .any(|posting| posting.kind == "symbol" && posting.value == "sessionmanager"),
        "{:?}",
        negative_extension_plan.planned_postings
    );

    let fallback_source_scope = search_repo_fast_filtered(
        repo.path(),
        "test:false SessionManager",
        10,
        &Default::default(),
    )
    .unwrap();
    let fallback_source_scope_paths = result_paths(&fallback_source_scope);
    assert!(fallback_source_scope_paths.contains(&"docs/auth.md".to_string()));
    assert!(!fallback_source_scope_paths.contains(&"tests/auth_test.rs".to_string()));
    let indexed_source_scope = indexed
        .search_filtered("test:false SessionManager", 10, &Default::default())
        .unwrap();
    let indexed_source_scope_paths = result_paths(&indexed_source_scope);
    assert!(indexed_source_scope_paths.contains(&"docs/auth.md".to_string()));
    assert!(!indexed_source_scope_paths.contains(&"tests/auth_test.rs".to_string()));

    let fallback_code_scope = search_repo_fast_filtered(
        repo.path(),
        "code:true SessionManager",
        10,
        &Default::default(),
    )
    .unwrap();
    let fallback_code_scope_paths = result_paths(&fallback_code_scope);
    assert!(fallback_code_scope_paths.contains(&"src/auth.rs".to_string()));
    assert!(!fallback_code_scope_paths.contains(&"docs/auth.md".to_string()));
    assert!(!fallback_code_scope_paths.contains(&"Cargo.toml".to_string()));

    let indexed_code_scope = indexed
        .search_filtered("is:code SessionManager", 10, &Default::default())
        .unwrap();
    let indexed_code_scope_paths = result_paths(&indexed_code_scope);
    assert!(indexed_code_scope_paths.contains(&"src/auth.rs".to_string()));
    assert!(!indexed_code_scope_paths.contains(&"docs/auth.md".to_string()));

    let code_plan = indexed
        .query_plan("is:code SessionManager", &Default::default())
        .unwrap();
    assert!(
        code_plan
            .active_filters
            .iter()
            .any(|filter| filter.field == "code" && filter.value == "true")
    );

    let indexed_prose_scope = indexed
        .search_filtered("code:false content:SessionManager", 10, &Default::default())
        .unwrap();
    assert!(
        result_paths(&indexed_prose_scope).contains(&"docs/auth.md".to_string()),
        "{indexed_prose_scope:?}"
    );
    assert!(!result_paths(&indexed_prose_scope).contains(&"src/auth.rs".to_string()));

    let fallback_generated = search_repo_fast_filtered(
        repo.path(),
        "is:generated issue token",
        10,
        &Default::default(),
    )
    .unwrap();
    assert_eq!(
        result_paths(&fallback_generated),
        vec!["src/generated/session.generated.rs"]
    );
    let indexed_generated = indexed
        .search_filtered("is:generated issue token", 10, &Default::default())
        .unwrap();
    assert_eq!(
        result_paths(&indexed_generated),
        vec!["src/generated/session.generated.rs"]
    );
    let indexed_hand_authored = indexed
        .search_filtered("-is:generated issue token", 10, &Default::default())
        .unwrap();
    assert!(
        !result_paths(&indexed_hand_authored)
            .contains(&"src/generated/session.generated.rs".to_string())
    );
    let generated_plan = indexed
        .query_plan("is:generated issue token", &Default::default())
        .unwrap();
    assert!(generated_plan.active_filters.iter().any(|filter| {
        filter.field == "generated" && filter.value == "true" && filter.candidate_matches == Some(1)
    }));

    let fallback_generated_bundle_default =
        search_repo_fast_filtered(repo.path(), "rareagentneedle", 2, &Default::default()).unwrap();
    assert_eq!(fallback_generated_bundle_default[0].path, "src/plain.js");
    let indexed_generated_bundle_default = indexed
        .search_filtered("rareagentneedle", 2, &Default::default())
        .unwrap();
    assert_eq!(indexed_generated_bundle_default[0].path, "src/plain.js");

    let fallback_generated_bundle_only = search_repo_fast_filtered(
        repo.path(),
        "is:generated rareagentneedle",
        10,
        &Default::default(),
    )
    .unwrap();
    assert_eq!(
        result_paths(&fallback_generated_bundle_only),
        vec!["webview/assets/chunk-A1b2c3d4.js"]
    );
    let indexed_generated_bundle_only = indexed
        .search_filtered("is:generated rareagentneedle", 10, &Default::default())
        .unwrap();
    assert_eq!(
        result_paths(&indexed_generated_bundle_only),
        vec!["webview/assets/chunk-A1b2c3d4.js"]
    );

    let fallback_without_generated_content = search_repo_fast_filtered(
        repo.path(),
        "issue token -content:generated",
        10,
        &Default::default(),
    )
    .unwrap();
    assert!(
        !result_paths(&fallback_without_generated_content)
            .contains(&"src/generated/session.generated.rs".to_string())
    );
    let indexed_without_generated_content = indexed
        .search_filtered("issue token -content:generated", 10, &Default::default())
        .unwrap();
    assert!(
        !result_paths(&indexed_without_generated_content)
            .contains(&"src/generated/session.generated.rs".to_string())
    );
    let content_exclusion_plan = indexed
        .query_plan("issue token -content:generated", &Default::default())
        .unwrap();
    assert!(content_exclusion_plan.active_filters.iter().any(|filter| {
        filter.field == "content"
            && filter.value == "generated"
            && filter.negated
            && filter.candidate_rejections == Some(1)
    }));

    let fallback_without_markdown = search_repo_fast_filtered(
        repo.path(),
        "-lang:md SessionManager",
        10,
        &Default::default(),
    )
    .unwrap();
    let fallback_without_markdown_paths = result_paths(&fallback_without_markdown);
    assert!(!fallback_without_markdown_paths.contains(&"docs/auth.md".to_string()));
    assert!(!fallback_without_markdown_paths.contains(&"README.md".to_string()));
    let indexed_without_markdown = indexed
        .search_filtered("-lang:md SessionManager", 10, &Default::default())
        .unwrap();
    let indexed_without_markdown_paths = result_paths(&indexed_without_markdown);
    assert!(!indexed_without_markdown_paths.contains(&"docs/auth.md".to_string()));
    assert!(!indexed_without_markdown_paths.contains(&"README.md".to_string()));

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
fn scoped_fallback_prefilters_preserve_file_path_lang_and_ext_semantics() {
    let repo = tempfile::tempdir().unwrap();
    write(
        &repo.path().join("src/auth.rs"),
        "pub fn issue_token() { let token = \"session\"; }\n",
    );
    write(
        &repo.path().join("src/auth.ts"),
        "export function issueToken() { return 'session token' }\n",
    );
    write(
        &repo.path().join("tests/auth_test.rs"),
        "pub fn issue_token_test() { let token = \"session\"; }\n",
    );
    write(
        &repo.path().join("generated/auth.rs"),
        "pub fn issue_token_generated() { let token = \"session\"; }\n",
    );
    write(
        &repo.path().join("docs/auth.md"),
        "issue token documentation\n",
    );
    write(
        &repo.path().join("src/noise.py"),
        "def issue_token(): return 'session token'\n",
    );

    let index = FastIndex::build(repo.path()).unwrap();
    for (query, expected) in [
        (
            "file:AUTH.RS issue token",
            vec!["generated/auth.rs", "src/auth.rs"],
        ),
        (
            "path:src/auth issue token",
            vec!["src/auth.rs", "src/auth.ts"],
        ),
        ("path:src/*auth.rs issue token", vec!["src/auth.rs"]),
        ("lang:typescript issue token", vec!["src/auth.ts"]),
        (
            "ext:.rs issue token -path:tests",
            vec!["generated/auth.rs", "src/auth.rs"],
        ),
        (
            "ext:rs issue token -path:generated",
            vec!["src/auth.rs", "tests/auth_test.rs"],
        ),
        (
            "ext:rs issue token -file:auth_test.rs",
            vec!["generated/auth.rs", "src/auth.rs"],
        ),
        (
            "-lang:markdown issue token",
            vec![
                "generated/auth.rs",
                "src/auth.rs",
                "src/auth.ts",
                "src/noise.py",
                "tests/auth_test.rs",
            ],
        ),
    ] {
        let fallback =
            search_repo_fast_filtered(repo.path(), query, 10, &SearchFilters::default()).unwrap();
        let indexed = index
            .search_filtered(query, 10, &SearchFilters::default())
            .unwrap();
        assert_eq!(result_paths(&fallback), expected, "{query}");
        assert_eq!(result_paths(&indexed), expected, "{query}");
    }
}

#[test]
fn filter_only_queries_discover_files_without_content_terms() {
    let repo = tempfile::tempdir().unwrap();
    write(
        &repo.path().join("src/auth.rs"),
        "pub struct SessionManager;\npub fn issue_token() {}\n",
    );
    write(
        &repo.path().join("src/lib.rs"),
        "pub mod auth;\npub fn boot() {}\n",
    );
    write(
        &repo.path().join("tests/auth_test.rs"),
        "use sample::SessionManager;\n#[test]\nfn issue_token_round_trip() {}\n",
    );
    write(&repo.path().join("docs/auth.md"), "issue token docs\n");

    let fallback = search_repo_fast_filtered(
        repo.path(),
        "file:AUTH.RS",
        10,
        &SearchFilters {
            explain: true,
            ..SearchFilters::default()
        },
    )
    .unwrap();
    assert_eq!(fallback.len(), 1);
    assert_eq!(fallback[0].path, "src/auth.rs");
    assert!(fallback[0].reason.contains("file_filter:AUTH.RS"));
    assert_eq!(fallback[0].line_range.as_ref().unwrap().start_line, 1);
    assert!(
        fallback[0]
            .explanation
            .as_ref()
            .unwrap()
            .iter()
            .any(|signal| signal.kind == "file_filter")
    );

    let index = FastIndex::build(repo.path()).unwrap();
    let indexed = index
        .search_filtered(
            "lang:rust test:true",
            10,
            &SearchFilters {
                explain: true,
                ..SearchFilters::default()
            },
        )
        .unwrap();
    assert_eq!(indexed.len(), 1);
    assert_eq!(indexed[0].path, "tests/auth_test.rs");
    assert!(indexed[0].reason.contains("language_filter:rust"));
    assert!(indexed[0].reason.contains("test_filter:true"));
    assert!(
        indexed[0]
            .snippet
            .starts_with("1: use sample::SessionManager;")
    );
    let plan = indexed[0].query_plan.as_ref().unwrap();
    assert_eq!(plan.strategy, "attribute_filter_postings");
    assert_eq!(plan.candidate_count, 1);
    assert!(
        plan.planned_postings
            .iter()
            .any(|posting| posting.kind == "filter" && posting.value == "language:rust")
    );
    assert!(
        plan.planned_postings
            .iter()
            .any(|posting| posting.kind == "filter" && posting.value == "test:true")
    );

    let negative_only = index
        .search_filtered("-path:docs", 10, &SearchFilters::default())
        .unwrap();
    assert!(negative_only.is_empty());

    let no_tests_repo = tempfile::tempdir().unwrap();
    write(
        &no_tests_repo.path().join("src/auth.rs"),
        "pub fn issue_token() {}\n",
    );
    let no_tests_index = FastIndex::build(no_tests_repo.path()).unwrap();
    assert!(!no_tests_index.query_may_match("file:*_test.rs", &SearchFilters::default()));
    assert!(index.query_may_match("file:*_test.rs", &SearchFilters::default()));
}

#[test]
fn filter_only_symbol_kind_results_anchor_on_matching_definition() {
    let repo = tempfile::tempdir().unwrap();
    let mut source = String::new();
    for line in 1..=30 {
        source.push_str(&format!("// intro filler {line}\n"));
    }
    source.push_str("fn target_definition() -> bool { true }\n");
    write(&repo.path().join("src/lib.rs"), &source);
    write(&repo.path().join("src/types.rs"), "pub struct OnlyType;\n");

    let fallback =
        search_repo_fast_filtered(repo.path(), "kind:function", 10, &SearchFilters::default())
            .unwrap();
    assert_eq!(fallback.len(), 1);
    assert_eq!(fallback[0].path, "src/lib.rs");
    assert!(fallback[0].reason.contains("symbol:target_definition"));
    assert!(fallback[0].snippet.contains("fn target_definition()"));
    assert_eq!(fallback[0].match_lines.first().copied(), Some(31));
    assert_eq!(fallback[0].read_range.as_ref().unwrap().start, 31);

    let index = FastIndex::build(repo.path()).unwrap();
    let indexed = index
        .search_filtered("kind:function", 10, &SearchFilters::default())
        .unwrap();
    assert_eq!(indexed.len(), 1);
    assert_eq!(indexed[0].path, "src/lib.rs");
    assert!(indexed[0].reason.contains("symbol:target_definition"));
    assert!(indexed[0].snippet.contains("fn target_definition()"));
    assert_eq!(indexed[0].match_lines.first().copied(), Some(31));
    assert_eq!(indexed[0].read_range.as_ref().unwrap().start, 31);
}

#[test]
fn bare_path_like_queries_use_filter_only_fast_paths() {
    let repo = tempfile::tempdir().unwrap();
    write(
        &repo.path().join("Cargo.toml"),
        "[package]\nname='sample'\nversion='0.1.0'\n",
    );
    let source_text = (1..=60)
        .map(|line| {
            if line == 40 {
                "pub fn target_entrypoint() {}\n".to_string()
            } else {
                format!("// filler line {line}\n")
            }
        })
        .collect::<String>();
    write(&repo.path().join("src/lib.rs"), &source_text);
    write(
        &repo.path().join("other/src/lib.rs"),
        "pub fn wrong_suffix_match() {}\n",
    );
    write(
        &repo.path().join("go.mod"),
        "module example.com/sample\n\ngo 1.22\n",
    );
    write(
        &repo.path().join("Dockerfile"),
        "FROM rust:1\nRUN cargo build\n",
    );
    write(
        &repo.path().join("Cargo.lock"),
        "# This file is automatically @generated by Cargo.\nversion = 4\n",
    );
    write(
        &repo.path().join("tests/mentions.rs"),
        "const MANIFEST: &str = \"Cargo.toml\";\nconst SOURCE: &str = \"src/lib.rs\";\nconst GO_MOD: &str = \"go.mod\";\nconst DOCKERFILE: &str = \"Dockerfile\";\n",
    );

    let filters = SearchFilters {
        explain: true,
        ..SearchFilters::default()
    };
    let manifest_fallback =
        search_repo_fast_filtered(repo.path(), "Cargo.toml", 10, &filters).unwrap();
    assert_eq!(manifest_fallback[0].path, "Cargo.toml");
    assert!(
        manifest_fallback[0]
            .explanation
            .as_ref()
            .unwrap()
            .iter()
            .any(|signal| signal.kind == "file_filter" && signal.value == "Cargo.toml")
    );
    assert!(
        !manifest_fallback
            .iter()
            .take(1)
            .any(|result| result.path == "tests/mentions.rs")
    );

    let path_fallback = search_repo_fast_filtered(repo.path(), "src/lib.rs", 10, &filters).unwrap();
    assert_eq!(result_paths(&path_fallback), vec!["src/lib.rs"]);
    assert!(
        path_fallback[0]
            .explanation
            .as_ref()
            .unwrap()
            .iter()
            .any(|signal| signal.kind == "path_filter" && signal.value == "src/lib.rs")
    );
    let dot_path_fallback =
        search_repo_fast_filtered(repo.path(), "./src/lib.rs", 10, &filters).unwrap();
    assert_eq!(result_paths(&dot_path_fallback), vec!["src/lib.rs"]);
    let explicit_dot_path_fallback =
        search_repo_fast_filtered(repo.path(), "path:./src/lib.rs", 10, &filters).unwrap();
    assert_eq!(
        result_paths(&explicit_dot_path_fallback),
        vec!["src/lib.rs"]
    );
    let absolute_source_path = repo
        .path()
        .join("src/lib.rs")
        .to_string_lossy()
        .replace('\\', "/");
    let absolute_path_fallback =
        search_repo_fast_filtered(repo.path(), &absolute_source_path, 10, &filters).unwrap();
    assert_eq!(result_paths(&absolute_path_fallback), vec!["src/lib.rs"]);
    let explicit_absolute_path_fallback = search_repo_fast_filtered(
        repo.path(),
        &format!("path:{absolute_source_path}"),
        10,
        &filters,
    )
    .unwrap();
    assert_eq!(
        result_paths(&explicit_absolute_path_fallback),
        vec!["src/lib.rs"]
    );
    let location_fallback =
        search_repo_fast_filtered(repo.path(), "src/lib.rs:40:9", 10, &filters).unwrap();
    assert_eq!(result_paths(&location_fallback), vec!["src/lib.rs"]);
    assert!(
        location_fallback[0]
            .snippet
            .contains("40: pub fn target_entrypoint()")
    );
    assert_eq!(location_fallback[0].match_lines, vec![40]);
    assert!(
        location_fallback[0]
            .explanation
            .as_ref()
            .unwrap()
            .iter()
            .any(|signal| signal.kind == "line_filter" && signal.value == "40")
    );
    let dot_location_fallback =
        search_repo_fast_filtered(repo.path(), "./src/lib.rs:40:9", 10, &filters).unwrap();
    assert_eq!(result_paths(&dot_location_fallback), vec!["src/lib.rs"]);
    assert_eq!(dot_location_fallback[0].match_lines, vec![40]);
    let hash_location_fallback =
        search_repo_fast_filtered(repo.path(), "src/lib.rs#L40-L45", 10, &filters).unwrap();
    assert_eq!(result_paths(&hash_location_fallback), vec!["src/lib.rs"]);
    assert_eq!(hash_location_fallback[0].match_lines, vec![40]);
    let markdown_location_fallback = search_repo_fast_filtered(
        repo.path(),
        "[src/lib.rs#L40-L45](src/lib.rs#L40-L45)",
        10,
        &filters,
    )
    .unwrap();
    assert_eq!(
        result_paths(&markdown_location_fallback),
        vec!["src/lib.rs"]
    );
    assert_eq!(markdown_location_fallback[0].match_lines, vec![40]);
    let hosted_location_fallback = search_repo_fast_filtered(
        repo.path(),
        "https://github.com/evalops/orient-search/blob/main/src/lib.rs#L40",
        10,
        &filters,
    )
    .unwrap();
    assert_eq!(result_paths(&hosted_location_fallback), vec!["src/lib.rs"]);
    assert_eq!(hosted_location_fallback[0].match_lines, vec![40]);
    let hosted_query_location_fallback = search_repo_fast_filtered(
        repo.path(),
        "https://github.com/evalops/orient-search/blob/main/src/lib.rs?plain=1#L40-L45",
        10,
        &filters,
    )
    .unwrap();
    assert_eq!(
        result_paths(&hosted_query_location_fallback),
        vec!["src/lib.rs"]
    );
    assert_eq!(hosted_query_location_fallback[0].match_lines, vec![40]);
    let sourcegraph_location_fallback = search_repo_fast_filtered(
        repo.path(),
        "https://sourcegraph.com/github.com/evalops/orient-search/-/blob/src/lib.rs?L40:9",
        10,
        &filters,
    )
    .unwrap();
    assert_eq!(
        result_paths(&sourcegraph_location_fallback),
        vec!["src/lib.rs"]
    );
    assert_eq!(sourcegraph_location_fallback[0].match_lines, vec![40]);
    let copied_line_fallback = search_repo_fast_filtered(
        repo.path(),
        "src/lib.rs:40: pub fn target_entrypoint",
        10,
        &filters,
    )
    .unwrap();
    assert_eq!(result_paths(&copied_line_fallback), vec!["src/lib.rs"]);
    assert_eq!(copied_line_fallback[0].match_lines, vec![40]);
    let copied_column_line_fallback = search_repo_fast_filtered(
        repo.path(),
        "src/lib.rs:40:9:target_entrypoint",
        10,
        &filters,
    )
    .unwrap();
    assert_eq!(
        result_paths(&copied_column_line_fallback),
        vec!["src/lib.rs"]
    );
    assert_eq!(copied_column_line_fallback[0].match_lines, vec![40]);
    let absolute_copied_line_fallback = search_repo_fast_filtered(
        repo.path(),
        &format!("{absolute_source_path}:40: pub fn target_entrypoint"),
        10,
        &filters,
    )
    .unwrap();
    assert_eq!(
        result_paths(&absolute_copied_line_fallback),
        vec!["src/lib.rs"]
    );
    assert_eq!(absolute_copied_line_fallback[0].match_lines, vec![40]);
    let wrapped_location_fallback =
        search_repo_fast_filtered(repo.path(), "(src/lib.rs:40:9)", 10, &filters).unwrap();
    assert_eq!(result_paths(&wrapped_location_fallback), vec!["src/lib.rs"]);
    assert_eq!(wrapped_location_fallback[0].match_lines, vec![40]);
    let stack_location_fallback = search_repo_fast_filtered(
        repo.path(),
        &format!("at Object.target ({absolute_source_path}:40:9)"),
        10,
        &filters,
    )
    .unwrap();
    assert_eq!(result_paths(&stack_location_fallback), vec!["src/lib.rs"]);
    assert_eq!(stack_location_fallback[0].match_lines, vec![40]);
    let stack_hash_location_fallback = search_repo_fast_filtered(
        repo.path(),
        &format!("at Object.target ({absolute_source_path}#L40-L45)"),
        10,
        &filters,
    )
    .unwrap();
    assert_eq!(
        result_paths(&stack_hash_location_fallback),
        vec!["src/lib.rs"]
    );
    assert_eq!(stack_hash_location_fallback[0].match_lines, vec![40]);
    let python_location_fallback = search_repo_fast_filtered(
        repo.path(),
        r#"File "src/lib.rs", line 40, in target_entrypoint"#,
        10,
        &filters,
    )
    .unwrap();
    assert_eq!(result_paths(&python_location_fallback), vec!["src/lib.rs"]);
    assert_eq!(python_location_fallback[0].match_lines, vec![40]);
    assert!(
        search_repo_fast_filtered(repo.path(), "missing/src/lib.rs:40:9", 10, &filters)
            .unwrap()
            .is_empty()
    );
    let go_mod_fallback = search_repo_fast_filtered(repo.path(), "go.mod", 10, &filters).unwrap();
    assert_eq!(go_mod_fallback[0].path, "go.mod");

    let dockerfile_fallback =
        search_repo_fast_filtered(repo.path(), "Dockerfile", 10, &filters).unwrap();
    assert_eq!(dockerfile_fallback[0].path, "Dockerfile");

    let cargo_lock_fallback =
        search_repo_fast_filtered(repo.path(), "Cargo.lock", 10, &filters).unwrap();
    assert_eq!(cargo_lock_fallback[0].path, "Cargo.lock");

    let index = FastIndex::build(repo.path()).unwrap();
    let manifest_indexed = index.search_filtered("Cargo.toml", 10, &filters).unwrap();
    assert_eq!(manifest_indexed[0].path, "Cargo.toml");
    assert!(
        manifest_indexed[0]
            .query_plan
            .as_ref()
            .unwrap()
            .planned_postings
            .iter()
            .any(|posting| posting.kind == "path_filter_trigram")
    );

    let path_indexed = index.search_filtered("src/lib.rs", 10, &filters).unwrap();
    assert_eq!(result_paths(&path_indexed), vec!["src/lib.rs"]);
    assert!(
        path_indexed[0]
            .query_plan
            .as_ref()
            .unwrap()
            .planned_postings
            .iter()
            .any(|posting| posting.kind == "path_filter_trigram")
    );
    let dot_path_indexed = index.search_filtered("./src/lib.rs", 10, &filters).unwrap();
    assert_eq!(result_paths(&dot_path_indexed), vec!["src/lib.rs"]);
    let explicit_dot_path_indexed = index
        .search_filtered("path:./src/lib.rs", 10, &filters)
        .unwrap();
    assert_eq!(result_paths(&explicit_dot_path_indexed), vec!["src/lib.rs"]);
    let absolute_path_indexed = index
        .search_filtered(&absolute_source_path, 10, &filters)
        .unwrap();
    assert_eq!(result_paths(&absolute_path_indexed), vec!["src/lib.rs"]);
    let explicit_absolute_path_indexed = index
        .search_filtered(&format!("path:{absolute_source_path}"), 10, &filters)
        .unwrap();
    assert_eq!(
        result_paths(&explicit_absolute_path_indexed),
        vec!["src/lib.rs"]
    );
    let location_indexed = index
        .search_filtered("src/lib.rs:40:9", 10, &filters)
        .unwrap();
    assert_eq!(location_indexed[0].path, "src/lib.rs");
    assert!(
        location_indexed[0]
            .snippet
            .contains("40: pub fn target_entrypoint()")
    );
    assert_eq!(location_indexed[0].match_lines, vec![40]);
    let dot_location_indexed = index
        .search_filtered("./src/lib.rs:40:9", 10, &filters)
        .unwrap();
    assert_eq!(dot_location_indexed[0].path, "src/lib.rs");
    assert_eq!(dot_location_indexed[0].match_lines, vec![40]);
    let hash_location_indexed = index
        .search_filtered("src/lib.rs#L40-L45", 10, &filters)
        .unwrap();
    assert_eq!(result_paths(&hash_location_indexed), vec!["src/lib.rs"]);
    assert_eq!(hash_location_indexed[0].match_lines, vec![40]);
    let markdown_location_indexed = index
        .search_filtered("[src/lib.rs#L40-L45](src/lib.rs#L40-L45)", 10, &filters)
        .unwrap();
    assert_eq!(result_paths(&markdown_location_indexed), vec!["src/lib.rs"]);
    assert_eq!(markdown_location_indexed[0].match_lines, vec![40]);
    let hosted_location_indexed = index
        .search_filtered(
            "https://github.com/evalops/orient-search/blob/main/src/lib.rs#L40",
            10,
            &filters,
        )
        .unwrap();
    assert_eq!(result_paths(&hosted_location_indexed), vec!["src/lib.rs"]);
    assert_eq!(hosted_location_indexed[0].match_lines, vec![40]);
    let hosted_query_location_indexed = index
        .search_filtered(
            "https://github.com/evalops/orient-search/blob/main/src/lib.rs?plain=1#L40-L45",
            10,
            &filters,
        )
        .unwrap();
    assert_eq!(
        result_paths(&hosted_query_location_indexed),
        vec!["src/lib.rs"]
    );
    assert_eq!(hosted_query_location_indexed[0].match_lines, vec![40]);
    let sourcegraph_location_indexed = index
        .search_filtered(
            "https://sourcegraph.com/github.com/evalops/orient-search/-/blob/src/lib.rs?L40:9",
            10,
            &filters,
        )
        .unwrap();
    assert_eq!(
        result_paths(&sourcegraph_location_indexed),
        vec!["src/lib.rs"]
    );
    assert_eq!(sourcegraph_location_indexed[0].match_lines, vec![40]);
    let copied_line_indexed = index
        .search_filtered("src/lib.rs:40: pub fn target_entrypoint", 10, &filters)
        .unwrap();
    assert_eq!(result_paths(&copied_line_indexed), vec!["src/lib.rs"]);
    assert_eq!(copied_line_indexed[0].match_lines, vec![40]);
    let copied_column_line_indexed = index
        .search_filtered("src/lib.rs:40:9:target_entrypoint", 10, &filters)
        .unwrap();
    assert_eq!(
        result_paths(&copied_column_line_indexed),
        vec!["src/lib.rs"]
    );
    assert_eq!(copied_column_line_indexed[0].match_lines, vec![40]);
    let absolute_copied_line_indexed = index
        .search_filtered(
            &format!("{absolute_source_path}:40: pub fn target_entrypoint"),
            10,
            &filters,
        )
        .unwrap();
    assert_eq!(
        result_paths(&absolute_copied_line_indexed),
        vec!["src/lib.rs"]
    );
    assert_eq!(absolute_copied_line_indexed[0].match_lines, vec![40]);
    let wrapped_location_indexed = index
        .search_filtered("(src/lib.rs:40:9)", 10, &filters)
        .unwrap();
    assert_eq!(result_paths(&wrapped_location_indexed), vec!["src/lib.rs"]);
    assert_eq!(wrapped_location_indexed[0].match_lines, vec![40]);
    let stack_location_indexed = index
        .search_filtered(
            &format!("at Object.target ({absolute_source_path}:40:9)"),
            10,
            &filters,
        )
        .unwrap();
    assert_eq!(result_paths(&stack_location_indexed), vec!["src/lib.rs"]);
    assert_eq!(stack_location_indexed[0].match_lines, vec![40]);
    let stack_hash_location_indexed = index
        .search_filtered(
            &format!("at Object.target ({absolute_source_path}#L40-L45)"),
            10,
            &filters,
        )
        .unwrap();
    assert_eq!(
        result_paths(&stack_hash_location_indexed),
        vec!["src/lib.rs"]
    );
    assert_eq!(stack_hash_location_indexed[0].match_lines, vec![40]);
    let python_location_indexed = index
        .search_filtered(
            r#"File "src/lib.rs", line 40, in target_entrypoint"#,
            10,
            &filters,
        )
        .unwrap();
    assert_eq!(result_paths(&python_location_indexed), vec!["src/lib.rs"]);
    assert_eq!(python_location_indexed[0].match_lines, vec![40]);

    let go_mod_indexed = index.search_filtered("go.mod", 10, &filters).unwrap();
    assert_eq!(go_mod_indexed[0].path, "go.mod");

    let dockerfile_indexed = index.search_filtered("Dockerfile", 10, &filters).unwrap();
    assert_eq!(dockerfile_indexed[0].path, "Dockerfile");

    let map = index.repo_map(5, 5);
    assert!(map.brief.manifest_files.contains(&"go.mod".to_string()));
    assert!(map.brief.manifest_files.contains(&"Cargo.lock".to_string()));
    assert!(
        map.brief
            .important_files
            .contains(&"Dockerfile".to_string())
    );
}

#[test]
fn path_filter_only_queries_use_path_trigram_prefilter_after_load() {
    let repo = tempfile::tempdir().unwrap();
    for index in 0..200 {
        write(
            &repo.path().join(format!("src/module_{index}.rs")),
            &format!("pub fn module_{index}() {{}}\n"),
        );
    }
    write(
        &repo.path().join("src/routes/special_path_probe.rs"),
        "pub fn route_probe() {}\n",
    );
    write(&repo.path().join("x.rs"), "pub fn short_exact_path() {}\n");

    let index_path = repo.path().join("repo.orient");
    let index = FastIndex::build(repo.path()).unwrap();
    index.save(&index_path).unwrap();
    let loaded = FastIndex::load(&index_path).unwrap();

    let results = loaded
        .search_filtered(
            "path:special_path_probe",
            10,
            &SearchFilters {
                explain: true,
                ..SearchFilters::default()
            },
        )
        .unwrap();
    assert_eq!(
        result_paths(&results),
        vec!["src/routes/special_path_probe.rs"]
    );
    let plan = results[0].query_plan.as_ref().unwrap();
    assert_eq!(plan.strategy, "path_filter_trigram_postings");
    assert_eq!(plan.candidate_count, 1);
    assert!(
        plan.planned_postings
            .iter()
            .any(|posting| { posting.kind == "path_filter_trigram" && posting.value == "spe" })
    );

    let file_plan = loaded
        .query_plan("file:special_path_probe.rs", &SearchFilters::default())
        .unwrap();
    assert_eq!(file_plan.strategy, "file_name_filter");
    assert_eq!(file_plan.candidate_count, 1);
    assert_eq!(file_plan.final_match_count, 1);
    assert!(file_plan.planned_postings.iter().any(|posting| {
        posting.kind == "file_name_filter"
            && posting.value == "special_path_probe.rs"
            && posting.postings == 1
    }));

    let short_file_results = loaded
        .search_filtered(
            "file:X",
            10,
            &SearchFilters {
                explain: true,
                ..SearchFilters::default()
            },
        )
        .unwrap();
    assert_eq!(result_paths(&short_file_results), vec!["x.rs"]);
    let short_file_plan = short_file_results[0].query_plan.as_ref().unwrap();
    assert_eq!(short_file_plan.strategy, "file_name_filter");
    assert_eq!(short_file_plan.candidate_count, 1);
    assert!(
        short_file_plan
            .planned_postings
            .iter()
            .any(|posting| posting.kind == "file_name_filter"
                && posting.value == "X"
                && posting.postings == 1)
    );

    let short_exact_plan = loaded
        .query_plan("path:./X.RS", &SearchFilters::default())
        .unwrap();
    assert_eq!(short_exact_plan.strategy, "exact_path_filter");
    assert_eq!(short_exact_plan.candidate_count, 1);
    assert_eq!(short_exact_plan.final_match_count, 1);
    assert!(short_exact_plan.planned_postings.iter().any(|posting| {
        posting.kind == "exact_path_filter" && posting.value == "x.rs" && posting.postings == 1
    }));

    let short_exact_results = loaded
        .search_filtered(
            "path:./X.RS",
            10,
            &SearchFilters {
                explain: true,
                ..SearchFilters::default()
            },
        )
        .unwrap();
    assert_eq!(result_paths(&short_exact_results), vec!["x.rs"]);
    assert_eq!(
        short_exact_results[0].query_plan.as_ref().unwrap().strategy,
        "exact_path_filter"
    );
    assert!(
        short_exact_results[0]
            .query_plan
            .as_ref()
            .unwrap()
            .planned_postings
            .iter()
            .any(|posting| {
                posting.kind == "exact_path_filter"
                    && posting.value == "x.rs"
                    && posting.postings == 1
            })
    );
}

#[test]
fn filter_only_file_queries_keep_scanning_after_content_rejections() {
    let repo = tempfile::tempdir().unwrap();
    write(
        &repo.path().join("aaa-rejected/Cargo.toml"),
        "[package]\nname='skipme-one'\n",
    );
    write(
        &repo.path().join("bbb-rejected/Cargo.toml"),
        "[package]\nname='skipme-two'\n",
    );
    write(
        &repo.path().join("zzz-valid/Cargo.toml"),
        "[package]\nname='wanted'\n",
    );

    let results = search_repo_fast_filtered(
        repo.path(),
        "file:Cargo.toml -content:skipme",
        1,
        &SearchFilters::default(),
    )
    .unwrap();

    assert_eq!(result_paths(&results), vec!["zzz-valid/Cargo.toml"]);
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
fn quoted_phrases_match_camel_case_identifier_boundaries() {
    let repo = tempfile::tempdir().unwrap();
    write(
        &repo.path().join("src/exact.ts"),
        "export function issueToken() {\n  return 'session';\n}\n",
    );
    write(
        &repo.path().join("src/scattered.ts"),
        "export function issueLater() {\n  const token = 'session';\n}\n",
    );

    let fallback = search_repo_fast_filtered(
        repo.path(),
        "\"issue token\"",
        10,
        &SearchFilters::default(),
    )
    .unwrap();
    assert_eq!(fallback.len(), 1);
    assert_eq!(fallback[0].path, "src/exact.ts");
    assert!(fallback[0].reason.contains("phrase:issue token"));
    assert!(
        !fallback
            .iter()
            .any(|result| result.path == "src/scattered.ts")
    );

    let indexed = FastIndex::build(repo.path())
        .unwrap()
        .search_filtered("\"issue token\"", 10, &SearchFilters::default())
        .unwrap();
    assert_eq!(indexed.len(), 1);
    assert_eq!(indexed[0].path, "src/exact.ts");
    assert!(indexed[0].reason.contains("phrase:issue token"));
    assert!(
        !indexed
            .iter()
            .any(|result| result.path == "src/scattered.ts")
    );
}

#[test]
fn quoted_phrases_match_acronym_identifier_boundaries() {
    let repo = tempfile::tempdir().unwrap();
    write(
        &repo.path().join("src/server.ts"),
        "export class HTTPServer {\n  listen() {}\n}\n",
    );
    write(
        &repo.path().join("src/scattered.ts"),
        "const HTTP_STATUS = 200;\nconst server = 'dev';\n",
    );

    let fallback = search_repo_fast_filtered(
        repo.path(),
        "\"http server\"",
        10,
        &SearchFilters::default(),
    )
    .unwrap();
    assert_eq!(fallback.len(), 1);
    assert_eq!(fallback[0].path, "src/server.ts");
    assert!(
        !fallback
            .iter()
            .any(|result| result.path == "src/scattered.ts")
    );

    let indexed = FastIndex::build(repo.path())
        .unwrap()
        .search_filtered("\"http server\"", 10, &SearchFilters::default())
        .unwrap();
    assert_eq!(indexed.len(), 1);
    assert_eq!(indexed[0].path, "src/server.ts");
    assert!(
        !indexed
            .iter()
            .any(|result| result.path == "src/scattered.ts")
    );
}

#[test]
fn quoted_phrases_match_indexed_paths_without_per_candidate_normalization() {
    let repo = tempfile::tempdir().unwrap();
    write(
        &repo.path().join("src/auth_provider.rs"),
        "pub fn auth_provider() -> &'static str { \"provider\" }\n",
    );
    write(
        &repo.path().join("src/other.rs"),
        "pub fn auth() -> &'static str { \"provider\" }\n",
    );

    let index = FastIndex::build(repo.path()).unwrap();
    let results = index
        .search_filtered(
            "\"auth provider\"",
            10,
            &SearchFilters {
                explain: true,
                ..SearchFilters::default()
            },
        )
        .unwrap();

    assert_eq!(result_paths(&results), vec!["src/auth_provider.rs"]);
    assert!(
        results[0]
            .explanation
            .as_ref()
            .unwrap()
            .iter()
            .any(|signal| signal.kind == "path_phrase")
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
    let read_range = short[0].read_range.as_ref().unwrap();
    assert_eq!(read_range.path, "src/auth.rs");
    assert_eq!(read_range.start, 1);
    assert_eq!(read_range.lines, 80);

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
    assert_eq!(
        symbol[0].read_range.as_ref().unwrap().scope,
        Some(orient::repo_index::RangeScope::Symbol)
    );
}

#[test]
fn indexed_snippets_anchor_on_strongest_matching_line() {
    let repo = tempfile::tempdir().unwrap();
    let mut text = "//! Multi-repo shard manifests for local indexed search.\n".to_string();
    for line in 2..90 {
        text.push_str(&format!("pub fn unrelated_shard_helper_{line}() {{}}\n"));
    }
    text.push_str("pub fn shard_status_jobs() { ordered_status_items(); }\n");
    text.push_str("fn ordered_status_items() {}\n");
    write(&repo.path().join("src/shards.rs"), &text);

    let index = FastIndex::build(repo.path()).unwrap();
    let results = index
        .search_filtered(
            "shard_status_jobs ordered_status_items",
            10,
            &SearchFilters {
                snippet: SnippetMode::Short,
                ..SearchFilters::default()
            },
        )
        .unwrap();

    assert_eq!(results[0].path, "src/shards.rs");
    assert!(results[0].snippet.contains("90: pub fn shard_status_jobs"));
    assert!(!results[0].snippet.contains("1: //! Multi-repo shard"));
    assert_eq!(results[0].match_lines[0], 90);
    assert_eq!(results[0].read_range.as_ref().unwrap().start, 64);
}

#[test]
fn snippets_prefer_symbol_lines_over_early_broad_token_lines() {
    let repo = tempfile::tempdir().unwrap();
    let mut text = String::new();
    for line in 1..20 {
        text.push_str(&format!(
            "// broad header {line}: lower path score file metadata\n"
        ));
    }
    text.push_str("pub fn lower_path() {}\n");
    text.push_str("pub fn score_file() {}\n");
    write(&repo.path().join("src/noisy.rs"), &text);

    let filters = SearchFilters {
        snippet: SnippetMode::Short,
        ..SearchFilters::default()
    };
    let fallback =
        search_repo_fast_filtered(repo.path(), "lower_path score_file", 10, &filters).unwrap();
    assert_eq!(fallback[0].path, "src/noisy.rs");
    assert!(fallback[0].snippet.contains("20: pub fn lower_path()"));
    assert!(!fallback[0].snippet.contains("1: // broad header"));
    assert_eq!(fallback[0].line_range.as_ref().unwrap().start_line, 20);
    assert_eq!(fallback[0].match_lines[0], 20);
    assert_eq!(fallback[0].read_range.as_ref().unwrap().start, 1);

    let index = FastIndex::build(repo.path()).unwrap();
    let indexed = index
        .search_filtered("lower_path score_file", 10, &filters)
        .unwrap();
    assert_eq!(indexed[0].path, "src/noisy.rs");
    assert!(indexed[0].snippet.contains("20: pub fn lower_path()"));
    assert!(!indexed[0].snippet.contains("1: // broad header"));
    assert_eq!(indexed[0].line_range.as_ref().unwrap().start_line, 20);
    assert_eq!(indexed[0].match_lines[0], 20);
    assert_eq!(indexed[0].read_range.as_ref().unwrap().start, 1);
}

#[test]
fn symbol_filter_snippets_anchor_on_definition_line() {
    let repo = tempfile::tempdir().unwrap();
    write(
        &repo.path().join("src/lib.rs"),
        r#"
pub fn caller() {
    target_symbol();
}

pub fn unrelated() {}

pub fn target_symbol() {
}
"#,
    );

    let filters = SearchFilters {
        snippet: SnippetMode::Short,
        ..SearchFilters::default()
    };
    let fallback =
        search_repo_fast_filtered(repo.path(), "symbol:target_symbol", 10, &filters).unwrap();
    assert_eq!(fallback[0].path, "src/lib.rs");
    assert!(fallback[0].snippet.contains("8: pub fn target_symbol()"));
    assert!(!fallback[0].snippet.contains("3:     target_symbol();"));
    assert_eq!(fallback[0].line_range.as_ref().unwrap().start_line, 8);
    assert_eq!(fallback[0].match_lines[0], 8);

    let index = FastIndex::build(repo.path()).unwrap();
    let indexed = index
        .search_filtered("symbol:target_symbol", 10, &filters)
        .unwrap();
    assert_eq!(indexed[0].path, "src/lib.rs");
    assert!(indexed[0].snippet.contains("8: pub fn target_symbol()"));
    assert!(!indexed[0].snippet.contains("3:     target_symbol();"));
    assert_eq!(indexed[0].line_range.as_ref().unwrap().start_line, 8);
    assert_eq!(indexed[0].match_lines[0], 8);
}

#[test]
fn indexed_query_plan_dedupes_identifier_tokens() {
    let repo = tempfile::tempdir().unwrap();
    write(
        &repo.path().join("src/shards.rs"),
        "pub fn shard_status_jobs() { ordered_status_items(); }\nfn ordered_status_items() {}\n",
    );

    let index = FastIndex::build(repo.path()).unwrap();
    let plan = index
        .query_plan(
            "shard_status_jobs ordered_status_items",
            &SearchFilters::default(),
        )
        .unwrap();

    assert_eq!(
        plan.query_tokens,
        vec!["shard", "status", "jobs", "ordered", "items"]
    );
    assert!(plan.query_trigrams.is_empty());
    let serialized = serde_json::to_value(&plan).unwrap();
    assert!(serialized.get("query_trigrams").is_none());
    assert!(serialized.get("planned_postings").is_some());
    assert_eq!(
        plan.planned_postings
            .iter()
            .filter(|posting| posting.kind == "content" && posting.value == "status")
            .count(),
        1
    );
    assert!(plan.missing_trigrams.is_empty());

    let results = index
        .search_filtered(
            "shard_status_jobs ordered_status_items",
            10,
            &SearchFilters {
                snippet: SnippetMode::Short,
                ..SearchFilters::default()
            },
        )
        .unwrap();
    assert_eq!(results[0].match_lines[0], 1);
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
    assert!(stats.source_bytes > 0);
    assert!(stats.content_snapshot_bytes > 0);
    assert!(stats.line_offset_bytes > 0);
    assert!(stats.posting_entries > 0);
    assert!(stats.compressed_posting_bytes > 0);
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

    let plan = index
        .query_plan("essionman", &SearchFilters::default())
        .unwrap();
    assert_eq!(plan.strategy, "token_or_trigram_union");
    assert!(!plan.query_trigrams.is_empty());
    let serialized = serde_json::to_value(&plan).unwrap();
    assert!(serialized.get("query_trigrams").is_some());
    assert!(
        plan.planned_postings
            .iter()
            .any(|posting| posting.kind == "trigram")
    );
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
fn indexed_search_uses_symbol_postings_for_identifier_queries() {
    let repo = tempfile::tempdir().unwrap();
    write(
        &repo.path().join("src/lib.rs"),
        "pub fn agent_instructions() -> &'static str { \"ok\" }\n",
    );
    for index in 0..120 {
        write(
            &repo
                .path()
                .join(format!("docs/agent-instructions-{index}.md")),
            "agent instructions orient search guide\n",
        );
    }

    let index = FastIndex::build(repo.path()).unwrap();
    assert!(index.symbol_postings.contains_key("agentinstructions"));

    let results = index
        .search_filtered(
            "agent_instructions",
            5,
            &SearchFilters {
                explain: true,
                ..SearchFilters::default()
            },
        )
        .unwrap();

    assert_eq!(results[0].path, "src/lib.rs");
    assert!(results[0].reason.contains("symbol:agent_instructions"));
    let plan = results[0].query_plan.as_ref().unwrap();
    assert!(
        plan.planned_postings
            .iter()
            .any(|posting| posting.kind == "symbol" && posting.value == "agentinstructions"),
        "{:?}",
        plan.planned_postings
    );
    assert!(
        plan.candidate_count < 10,
        "symbol postings should keep exact identifier candidates tight: {:?}",
        plan
    );

    let content_results = index
        .search_filtered(
            "content:agent_instructions",
            10,
            &SearchFilters {
                explain: true,
                ..SearchFilters::default()
            },
        )
        .unwrap();
    let content_paths = result_paths(&content_results);
    assert!(content_paths.iter().any(|path| path.ends_with(".md")));
    let content_plan = content_results[0].query_plan.as_ref().unwrap();
    assert!(
        content_plan.candidate_count > 10,
        "content: should keep prose candidates instead of symbol-narrowing: {:?}",
        content_plan
    );
    assert!(
        !content_plan
            .planned_postings
            .iter()
            .any(|posting| posting.kind == "symbol" && posting.value == "agentinstructions"),
        "{:?}",
        content_plan.planned_postings
    );

    let filtered = index
        .search_filtered(
            "symbol:agent_instructions",
            5,
            &SearchFilters {
                explain: true,
                ..SearchFilters::default()
            },
        )
        .unwrap();
    assert_eq!(filtered[0].path, "src/lib.rs");
    let filtered_plan = filtered[0].query_plan.as_ref().unwrap();
    assert!(
        filtered_plan
            .planned_postings
            .iter()
            .any(|posting| posting.kind == "symbol" && posting.value == "agentinstructions"),
        "{:?}",
        filtered_plan.planned_postings
    );
    assert!(
        filtered_plan.candidate_count < 10,
        "symbol: filters should plan through exact symbol postings: {:?}",
        filtered_plan
    );

    let kind_only = index
        .search_filtered(
            "kind:function",
            5,
            &SearchFilters {
                explain: true,
                ..SearchFilters::default()
            },
        )
        .unwrap();
    assert_eq!(kind_only[0].path, "src/lib.rs");
    let kind_plan = kind_only[0].query_plan.as_ref().unwrap();
    assert_eq!(kind_plan.strategy, "symbol_kind_filter_postings");
    assert!(
        kind_plan
            .planned_postings
            .iter()
            .any(|posting| posting.kind == "symbol_kind" && posting.value == "function"),
        "{:?}",
        kind_plan.planned_postings
    );
    assert_eq!(kind_plan.candidate_count, 1);

    let direct_kind_plan = index
        .query_plan("kind:function", &SearchFilters::default())
        .unwrap();
    assert_eq!(direct_kind_plan.strategy, "symbol_kind_filter_postings");
    assert_eq!(direct_kind_plan.candidate_count, 1);
    assert!(
        direct_kind_plan
            .planned_postings
            .iter()
            .any(|posting| posting.kind == "symbol_kind" && posting.value == "function"),
        "{:?}",
        direct_kind_plan.planned_postings
    );
}

#[test]
fn indexed_kind_filter_intersects_content_terms() {
    let repo = tempfile::tempdir().unwrap();
    write(
        &repo.path().join("src/handler.rs"),
        "pub fn sharedneedle_handler() {}\n",
    );
    write(
        &repo.path().join("src/noise.rs"),
        "pub fn unrelated_alpha() {}\npub fn unrelated_beta() {}\n",
    );
    write(
        &repo.path().join("src/trigram_noise.rs"),
        "pub fn unrelated_gamma() {}\n// sha har are red edn dne nee eed edl dle\n",
    );
    for index in 0..20 {
        write(
            &repo.path().join(format!("docs/sharedneedle-{index}.md")),
            "sharedneedle operational notes\n",
        );
    }

    let index = FastIndex::build(repo.path()).unwrap();
    let results = index
        .search_filtered(
            "kind:function sharedneedle",
            10,
            &SearchFilters {
                explain: true,
                ..SearchFilters::default()
            },
        )
        .unwrap();

    assert_eq!(result_paths(&results), vec!["src/handler.rs".to_string()]);
    let plan = results[0].query_plan.as_ref().unwrap();
    assert_eq!(plan.candidate_count, 1);
    assert_eq!(plan.final_match_count, 1);
    assert!(
        plan.planned_postings
            .iter()
            .any(|posting| posting.kind == "symbol_kind" && posting.value == "function"),
        "{:?}",
        plan.planned_postings
    );

    let direct_plan = index
        .query_plan("kind:function sharedneedle", &SearchFilters::default())
        .unwrap();
    assert_eq!(direct_plan.candidate_count, 1);
    assert_eq!(direct_plan.final_match_count, 1);
}

#[test]
fn indexed_kind_filter_skips_trigram_only_false_positives() {
    let repo = tempfile::tempdir().unwrap();
    write(
        &repo.path().join("src/trigram_noise.rs"),
        "pub fn unrelated_gamma() {}\n// sha har are red edn dne nee eed edl dle\n",
    );

    let index = FastIndex::build(repo.path()).unwrap();
    let results = index
        .search_filtered("kind:function sharedneedle", 10, &SearchFilters::default())
        .unwrap();
    assert!(results.is_empty(), "{results:?}");
    assert!(!index.query_may_match("kind:function sharedneedle", &SearchFilters::default()));

    let plan = index
        .query_plan("kind:function sharedneedle", &SearchFilters::default())
        .unwrap();
    assert_eq!(plan.candidate_count, 0);
    assert_eq!(plan.final_match_count, 0);
}

#[test]
fn indexed_query_plan_suggests_replacement_for_misspelled_kind_with_terms() {
    let repo = tempfile::tempdir().unwrap();
    write(
        &repo.path().join("src/handler.rs"),
        "pub fn sharedneedle_handler() {}\n",
    );

    let index = FastIndex::build(repo.path()).unwrap();
    let plan = index
        .query_plan("kind:functoin sharedneedle", &SearchFilters::default())
        .unwrap();

    assert_eq!(plan.candidate_count, 0);
    assert_eq!(plan.final_match_count, 0);
    assert!(plan.repair_hints.iter().any(|hint| {
        hint.kind == "replace_symbol_kind_filter"
            && hint.suggested_query.as_deref() == Some("kind:function sharedneedle")
            && hint.message.contains("Available kinds: function")
    }));
}

#[test]
fn indexed_attribute_filters_intersect_before_scoring() {
    let repo = tempfile::tempdir().unwrap();
    write(
        &repo.path().join("src/lib.rs"),
        "pub fn sharedneedle_alpha() {}\n",
    );
    write(
        &repo.path().join("src/worker.ts"),
        "export function sharedneedleBeta() {}\n",
    );
    write(
        &repo.path().join("docs/guide.md"),
        "sharedneedle agent guide\n",
    );

    let index = FastIndex::build(repo.path()).unwrap();
    assert!(index.attribute_postings.contains_key("code:true"));
    assert!(index.attribute_postings.contains_key("code:false"));
    assert!(index.attribute_postings.contains_key("language:rust"));
    assert!(index.attribute_postings.contains_key("extension:md"));

    let code_results = index
        .search_filtered(
            "sharedneedle code:true",
            10,
            &SearchFilters {
                explain: true,
                ..SearchFilters::default()
            },
        )
        .unwrap();
    assert_eq!(
        result_paths(&code_results),
        vec!["src/lib.rs".to_string(), "src/worker.ts".to_string()]
    );
    let code_plan = code_results[0].query_plan.as_ref().unwrap();
    assert_eq!(code_plan.candidate_count, 2);
    assert!(
        code_plan
            .planned_postings
            .iter()
            .any(|posting| posting.kind == "filter" && posting.value == "code:true"),
        "{:?}",
        code_plan.planned_postings
    );

    let prose_results = index
        .search_filtered(
            "sharedneedle code:false",
            10,
            &SearchFilters {
                explain: true,
                ..SearchFilters::default()
            },
        )
        .unwrap();
    assert_eq!(
        result_paths(&prose_results),
        vec!["docs/guide.md".to_string()]
    );
    let prose_plan = prose_results[0].query_plan.as_ref().unwrap();
    assert_eq!(prose_plan.candidate_count, 1);
    assert!(
        prose_plan
            .planned_postings
            .iter()
            .any(|posting| posting.kind == "filter" && posting.value == "code:false"),
        "{:?}",
        prose_plan.planned_postings
    );

    let impossible_plan = index
        .query_plan(
            "sharedneedle lang:rust code:false",
            &SearchFilters::default(),
        )
        .unwrap();
    assert_eq!(impossible_plan.candidate_count, 0);
    assert_eq!(impossible_plan.final_match_count, 0);
}

#[test]
fn indexed_kind_filters_use_persisted_symbols_without_reparsing_source() {
    let repo = tempfile::tempdir().unwrap();
    write(
        &repo.path().join("src/lib.rs"),
        "pub fn persisted_symbol_kind() -> &'static str { \"ok\" }\n",
    );

    let mut index = FastIndex::build(repo.path()).unwrap();
    let file = index
        .files
        .iter_mut()
        .find(|file| file.path == "src/lib.rs")
        .unwrap();
    file.content = "not rust anymore".to_string();

    let results = index
        .search_filtered("kind:function", 5, &SearchFilters::default())
        .unwrap();
    assert_eq!(results[0].path, "src/lib.rs");

    let plan = index
        .query_plan("kind:function", &SearchFilters::default())
        .unwrap();
    assert_eq!(plan.strategy, "symbol_kind_filter_postings");
    assert_eq!(plan.candidate_count, 1);
    assert_eq!(plan.final_match_count, 1);
}

#[test]
fn indexed_kind_filter_snippets_anchor_on_matching_symbol_line() {
    let repo = tempfile::tempdir().unwrap();
    write(
        &repo.path().join("src/lib.rs"),
        "// module overview\n// setup notes\n\npub struct AuthSession;\n\npub fn target_kind_function() -> &'static str { \"ok\" }\n",
    );

    let index = FastIndex::build(repo.path()).unwrap();
    let results = index
        .search_filtered(
            "kind:function",
            5,
            &SearchFilters {
                snippet: SnippetMode::Short,
                ..SearchFilters::default()
            },
        )
        .unwrap();

    assert_eq!(results[0].path, "src/lib.rs");
    assert_eq!(
        results[0].snippet,
        "6: pub fn target_kind_function() -> &'static str { \"ok\" }"
    );
    assert!(
        results[0].reason.contains("symbol:target_kind_function"),
        "{:?}",
        results[0]
    );
    assert_eq!(results[0].match_lines, vec![6]);
    assert_eq!(results[0].line_range.as_ref().unwrap().start_line, 6);
    assert_eq!(results[0].line_range.as_ref().unwrap().end_line, 6);
}

#[test]
fn indexed_search_caps_broad_candidates_after_rank_aware_prefilter() {
    let repo = tempfile::tempdir().unwrap();
    for index in 0..1100 {
        write(
            &repo.path().join(format!("src/file_{index:04}.rs")),
            &format!("pub fn helper_{index:04}() {{ let _ = \"commonneedle\"; }}\n"),
        );
    }
    write(
        &repo.path().join("src/zzzz_commonneedle_target.rs"),
        "pub fn helper_target() { let _ = \"commonneedle\"; }\n",
    );

    let index = FastIndex::build(repo.path()).unwrap();
    let results = index
        .search_filtered(
            "commonneedle",
            1,
            &SearchFilters {
                explain: true,
                ..SearchFilters::default()
            },
        )
        .unwrap();

    assert_eq!(results[0].path, "src/zzzz_commonneedle_target.rs");
    let plan = results[0].query_plan.as_ref().unwrap();
    assert_eq!(plan.candidate_cap, 1024);
    assert!(plan.candidate_cap_hit);
    assert!(plan.candidate_count > plan.candidate_cap);
    assert!(plan.scored_candidate_count <= plan.candidate_cap);
}

#[test]
fn indexed_search_filters_candidates_before_cap() {
    let repo = tempfile::tempdir().unwrap();
    for index in 0..1100 {
        write(
            &repo.path().join(format!("src/file_{index:04}.rs")),
            "pub fn broad_match() { let _ = \"commonneedle\"; }\n",
        );
    }
    write(
        &repo.path().join("zzz_scope/target.rs"),
        "pub fn scoped_target() { let _ = \"commonneedle\"; }\n",
    );

    let index = FastIndex::build(repo.path()).unwrap();
    let results = index
        .search_filtered(
            "commonneedle path:zzz_scope",
            1,
            &SearchFilters {
                explain: true,
                ..SearchFilters::default()
            },
        )
        .unwrap();

    assert_eq!(result_paths(&results), vec!["zzz_scope/target.rs"]);
    let plan = results[0].query_plan.as_ref().unwrap();
    assert_eq!(plan.candidate_count, 1);
    assert_eq!(plan.filtered_candidate_count, 1);
    assert_eq!(plan.scored_candidate_count, 1);
    assert!(!plan.candidate_cap_hit);

    let query_plan = index
        .query_plan("commonneedle path:zzz_scope", &SearchFilters::default())
        .unwrap();
    assert_eq!(query_plan.candidate_count, 1);
    assert_eq!(query_plan.filtered_candidate_count, 1);
    assert_eq!(query_plan.final_match_count, 1);
    assert!(!query_plan.candidate_cap_hit);
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
fn indexed_single_literal_trigram_search_rejects_partial_overlap() {
    let repo = tempfile::tempdir().unwrap();
    write(
        &repo.path().join("src/near.rs"),
        "const NEAR: &str = \"abc def unique needle\";\n",
    );

    let index = FastIndex::build(repo.path()).unwrap();
    let results = index.search("abcdef", 10).unwrap();
    assert!(results.is_empty(), "{results:?}");

    let plan = index
        .query_plan("abcdef", &SearchFilters::default())
        .unwrap();
    assert_eq!(plan.final_match_count, 0);
    assert!(plan.missing_trigrams.iter().any(|value| value == "bcd"));
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
    assert_eq!(fallback[0].line_range.as_ref().unwrap().end_line, 2);
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
fn multi_token_symbol_scoring_does_not_overboost_single_token_symbols() {
    let repo = tempfile::tempdir().unwrap();
    write(
        &repo.path().join("src/noisy.rs"),
        "pub fn path() {}\npub fn file() {}\npub fn lower_path() {}\npub fn score_file() {}\n",
    );

    let filters = SearchFilters {
        explain: true,
        ..SearchFilters::default()
    };
    let fallback =
        search_repo_fast_filtered(repo.path(), "lower_path score_file", 10, &filters).unwrap();
    assert_eq!(fallback[0].path, "src/noisy.rs");
    assert!(!fallback[0].reason.contains("symbol:path"));
    assert!(!fallback[0].reason.contains("symbol:file"));
    let fallback_signals = fallback[0].explanation.as_ref().unwrap();
    assert!(
        !fallback_signals
            .iter()
            .any(|signal| signal.kind == "symbol_exact" && signal.value == "path")
    );
    assert!(
        !fallback_signals
            .iter()
            .any(|signal| signal.kind == "symbol_exact" && signal.value == "file")
    );
    assert!(
        fallback_signals
            .iter()
            .any(|signal| signal.kind == "symbol_overlap" && signal.value == "lower_path")
    );
    assert!(
        fallback_signals
            .iter()
            .any(|signal| signal.kind == "symbol_overlap" && signal.value == "score_file")
    );

    let index = FastIndex::build(repo.path()).unwrap();
    let indexed = index
        .search_filtered("lower_path score_file", 10, &filters)
        .unwrap();
    assert_eq!(indexed[0].path, "src/noisy.rs");
    assert!(!indexed[0].reason.contains("symbol:path"));
    assert!(!indexed[0].reason.contains("symbol:file"));
    let indexed_signals = indexed[0].explanation.as_ref().unwrap();
    assert!(
        !indexed_signals
            .iter()
            .any(|signal| signal.kind == "symbol_exact" && signal.value == "path")
    );
    assert!(
        !indexed_signals
            .iter()
            .any(|signal| signal.kind == "symbol_exact" && signal.value == "file")
    );
    assert!(
        indexed_signals
            .iter()
            .any(|signal| signal.kind == "symbol_overlap" && signal.value == "lower_path")
    );
    assert!(
        indexed_signals
            .iter()
            .any(|signal| signal.kind == "symbol_overlap" && signal.value == "score_file")
    );
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
fn load_reusable_discards_corrupt_index_for_refresh() {
    let repo = tempfile::tempdir().unwrap();
    let path = repo.path().join("corrupt.index");
    fs::write(&path, b"not a bincode orient index").unwrap();

    assert!(FastIndex::load_reusable(&path).unwrap().is_none());
}

#[test]
fn loading_empty_index_returns_error_without_mmap_failure() {
    let repo = tempfile::tempdir().unwrap();
    let path = repo.path().join("empty.index");
    fs::write(&path, b"").unwrap();

    let error = FastIndex::load(&path).unwrap_err().to_string();
    assert!(error.contains("parse index"), "{error}");
    assert!(!error.contains("mmap index"), "{error}");
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
fn fast_search_deduplicates_exact_content_clones_with_different_names() {
    let repo = tempfile::tempdir().unwrap();
    let source = "pub fn issue_token() { let token = \"session\"; }\n";
    write(&repo.path().join("packages/a/src/auth.rs"), source);
    write(&repo.path().join("packages/b/src/session.rs"), source);

    let fallback = search_repo_fast_filtered(
        repo.path(),
        "issue token",
        10,
        &SearchFilters {
            require_all: true,
            ..SearchFilters::default()
        },
    )
    .unwrap();
    assert_eq!(fallback.len(), 1);
    assert_eq!(fallback[0].path, "packages/a/src/auth.rs");
    let fallback_group = fallback[0].duplicate_group.as_ref().unwrap();
    assert_eq!(fallback_group.duplicate_count, 1);
    assert_eq!(
        fallback_group.duplicate_paths,
        vec!["packages/b/src/session.rs"]
    );

    let index = FastIndex::build(repo.path()).unwrap();
    let indexed = index
        .search_filtered(
            "issue token",
            10,
            &SearchFilters {
                require_all: true,
                ..SearchFilters::default()
            },
        )
        .unwrap();
    assert_eq!(indexed.len(), 1);
    assert_eq!(indexed[0].path, "packages/a/src/auth.rs");
    let indexed_group = indexed[0].duplicate_group.as_ref().unwrap();
    assert_eq!(indexed_group.duplicate_count, 1);
    assert_eq!(
        indexed_group.duplicate_paths,
        vec!["packages/b/src/session.rs"]
    );
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
fn shard_search_prefixes_duplicate_group_paths() {
    let repo = tempfile::tempdir().unwrap();
    write(
        &repo.path().join("one/src/auth.rs"),
        "pub fn issue_token() { let token = \"session\"; }\n",
    );
    write(
        &repo.path().join("two/src/auth.rs"),
        "pub fn issue_token() { let token = \"session\"; }\n",
    );

    let shard_dir = tempfile::tempdir().unwrap();
    build_shards(&[repo.path().to_path_buf()], shard_dir.path()).unwrap();
    let shard_name = repo.path().file_name().unwrap().to_string_lossy();
    let results = search_shards(
        shard_dir.path(),
        "issue token session",
        10,
        &SearchFilters {
            require_all: true,
            ..SearchFilters::default()
        },
    )
    .unwrap();

    assert_eq!(results.len(), 1);
    assert_eq!(results[0].path, format!("{shard_name}/one/src/auth.rs"));
    assert_eq!(
        results[0].read_range.as_ref().unwrap().path,
        format!("{shard_name}/one/src/auth.rs")
    );
    let group = results[0].duplicate_group.as_ref().unwrap();
    assert_eq!(group.canonical_path, "src/auth.rs");
    assert_eq!(group.duplicate_count, 1);
    assert_eq!(
        group.duplicate_paths,
        vec![format!("{shard_name}/two/src/auth.rs")]
    );
}

#[test]
fn shard_manifest_sketch_prunes_impossible_cold_shards() {
    let workspace = tempfile::tempdir().unwrap();
    let hit_repo = workspace.path().join("hit-service");
    let miss_repo = workspace.path().join("miss-service");
    write(
        &hit_repo.join("src/lib.rs"),
        "pub struct SessionManager;\npub fn uniquehitneedle() -> &'static str { \"ok\" }\npub fn read_batch_request_agent_guide_follow_up() { primary_retry_request(); refresh_request(); repo_map_request(); }\n",
    );
    write(
        &miss_repo.join("src/lib.rs"),
        "pub fn unrelated_service() -> &'static str { \"miss\" }\n",
    );

    let shard_dir = tempfile::tempdir().unwrap();
    build_shards(&[hit_repo.clone(), miss_repo], shard_dir.path()).unwrap();
    let manifest: serde_json::Value =
        serde_json::from_slice(&fs::read(shard_dir.path().join("manifest.json")).unwrap()).unwrap();
    assert!(manifest["shards"][0].get("sketch").is_none());
    assert!(shard_dir.path().join("manifest.bin").exists());
    assert!(shard_dir.path().join("manifest.prefilter.bin").exists());
    assert!(shard_dir.path().join("manifest.route.bin").exists());
    let hit_index = manifest["shards"]
        .as_array()
        .unwrap()
        .iter()
        .find(|shard| shard["name"] == "hit-service")
        .unwrap()["index"]
        .as_str()
        .unwrap()
        .to_string();
    let miss_index = manifest["shards"]
        .as_array()
        .unwrap()
        .iter()
        .find(|shard| shard["name"] == "miss-service")
        .unwrap()["index"]
        .as_str()
        .unwrap()
        .to_string();
    fs::remove_file(shard_dir.path().join(miss_index)).unwrap();

    let results = search_shards(
        shard_dir.path(),
        "uniquehitneedle",
        10,
        &SearchFilters::default(),
    )
    .unwrap();
    assert_eq!(result_paths(&results), vec!["hit-service/src/lib.rs"]);

    let routed_kind_results = search_shards(
        shard_dir.path(),
        "kind:function uniquehitneedle",
        10,
        &SearchFilters::default(),
    )
    .unwrap();
    assert_eq!(
        result_paths(&routed_kind_results),
        vec!["hit-service/src/lib.rs"]
    );

    let substring_results =
        search_shards(shard_dir.path(), "essionman", 10, &SearchFilters::default()).unwrap();
    assert_eq!(
        result_paths(&substring_results),
        vec!["hit-service/src/lib.rs"]
    );

    let absent_identifier_results = search_shards(
        shard_dir.path(),
        "definitely_absent_orient_prefilter_probe",
        10,
        &SearchFilters::default(),
    )
    .unwrap();
    assert!(absent_identifier_results.is_empty());

    let multi_identifier_results = search_shards(
        shard_dir.path(),
        "read_batch_request primary_retry_request refresh_request repo_map_request agent guide follow-up",
        10,
        &SearchFilters::default(),
    )
    .unwrap();
    assert_eq!(
        result_paths(&multi_identifier_results),
        vec!["hit-service/src/lib.rs"]
    );

    let plans = shard_query_plans(
        shard_dir.path(),
        "uniquehitneedle missingterm",
        &SearchFilters::default(),
    )
    .unwrap();
    assert_eq!(plans.len(), 1);
    assert_eq!(plans[0].name, "hit-service");
    assert!(
        plans[0]
            .plan
            .missing_terms
            .contains(&"missingterm".to_string())
    );

    fs::remove_file(shard_dir.path().join(hit_index)).unwrap();
    let globally_absent_results = search_shards(
        shard_dir.path(),
        "globally_missing_prefilter_probe_token_xyz",
        10,
        &SearchFilters::default(),
    )
    .unwrap();
    assert!(globally_absent_results.is_empty());
}

#[test]
fn dependency_filters_scope_fallback_indexed_and_shard_search() {
    let workspace = tempfile::tempdir().unwrap();
    let rust_repo = workspace.path().join("rust-api");
    let react_repo = workspace.path().join("react-ui");
    write(
        &rust_repo.join("Cargo.toml"),
        "[package]\nname='rust-api'\nversion='0.1.0'\n[dependencies]\nserde='1'\n",
    );
    write(
        &rust_repo.join("src/lib.rs"),
        "use serde::Serialize;\npub fn issue_token() { let token = \"serde backed\"; }\n",
    );
    write(
        &react_repo.join("package.json"),
        r#"{"dependencies":{"react":"latest"}}"#,
    );
    write(
        &react_repo.join("src/lib.ts"),
        "import React from 'react';\nexport function issueToken() { return 'react backed token'; }\n",
    );

    let fallback = search_repo_fast_filtered(
        &rust_repo,
        "dep:serde import:serde kind:function issue token",
        10,
        &SearchFilters::default(),
    )
    .unwrap();
    assert_eq!(fallback[0].path, "src/lib.rs");
    assert!(
        search_repo_fast_filtered(
            &rust_repo,
            "import:react kind:function issue token",
            10,
            &SearchFilters::default()
        )
        .unwrap()
        .is_empty()
    );

    let index = FastIndex::build(&rust_repo).unwrap();
    assert_eq!(
        index
            .search_filtered(
                "dependency:serde module:serde issue token",
                10,
                &SearchFilters::default()
            )
            .unwrap()[0]
            .path,
        "src/lib.rs"
    );
    assert!(
        index
            .search_filtered("kind:enum issue token", 10, &SearchFilters::default())
            .unwrap()
            .is_empty()
    );
    assert!(
        index
            .search_filtered("-import:serde issue token", 10, &SearchFilters::default())
            .unwrap()
            .is_empty()
    );
    let plan = index
        .query_plan("import:react issue token", &SearchFilters::default())
        .unwrap();
    assert_eq!(plan.final_match_count, 0);
    assert!(plan.active_filters.iter().any(|filter| {
        filter.field == "import" && filter.value == "react" && filter.candidate_matches == Some(0)
    }));

    let shard_dir = tempfile::tempdir().unwrap();
    build_shards(&[rust_repo.clone(), react_repo.clone()], shard_dir.path()).unwrap();
    let serde_results = search_shards(
        shard_dir.path(),
        "dep:serde import:serde issue token",
        10,
        &SearchFilters::default(),
    )
    .unwrap();
    assert_eq!(serde_results[0].path, "rust-api/src/lib.rs");
    let react_results = search_shards(
        shard_dir.path(),
        "dep:react import:react issue token",
        10,
        &SearchFilters::default(),
    )
    .unwrap();
    assert_eq!(react_results[0].path, "react-ui/src/lib.ts");
}

#[test]
fn indexed_query_prefilter_skips_impossible_shards_without_false_negatives() {
    let repo = tempfile::tempdir().unwrap();
    write(
        &repo.path().join("src/auth.rs"),
        "pub struct SessionManager;\npub fn issue_token() -> &'static str { \"token\" }\n",
    );
    write(
        &repo.path().join("Cargo.toml"),
        "[package]\nname='prefilter'\nversion='0.1.0'\nedition='2024'\n",
    );

    let index = FastIndex::build(repo.path()).unwrap();
    for query in [
        "issue token",
        "mode:any issue missing",
        "symbol:SessionManager",
        "kind:function issue token",
        "path:auth issue",
        "lang:rust issue",
        "file:Cargo.toml",
        "essionman",
    ] {
        assert!(
            index.query_may_match(query, &SearchFilters::default()),
            "prefilter rejected matching query {query:?}"
        );
        assert!(
            !index
                .search_filtered(query, 10, &SearchFilters::default())
                .unwrap()
                .is_empty(),
            "fixture query should have results: {query:?}"
        );
    }

    for query in [
        "issue missing",
        "kind:enum issue token",
        "lang:typescript issue",
        "file:*_test.rs",
        "definitely_absent_token",
    ] {
        assert!(
            !index.query_may_match(query, &SearchFilters::default()),
            "prefilter kept impossible query {query:?}"
        );
        assert!(
            index
                .search_filtered(query, 10, &SearchFilters::default())
                .unwrap()
                .is_empty(),
            "impossible query unexpectedly had results: {query:?}"
        );
    }
}

#[test]
fn test_filter_recognizes_common_multilanguage_test_paths() {
    let repo = tempfile::tempdir().unwrap();
    let test_paths = [
        "src/lib_test.rs",
        "pkg/client_test.go",
        "src/auth.spec.ts",
        "src/__tests__/widget.tsx",
        "spec/models/user_spec.rb",
        "src/test/java/AuthFlow.java",
    ];
    for (index, path) in test_paths.iter().enumerate() {
        write(
            &repo.path().join(path),
            &format!("// agent common token {index}\npub fn agent_common_token_{index}() {{}}\n"),
        );
    }
    for (index, path) in ["src/lib.rs", "src/testament.rs", "src/contest.ts"]
        .iter()
        .enumerate()
    {
        write(
            &repo.path().join(path),
            &format!(
                "// agent common token control {index}\npub fn agent_common_token_control_{index}() {{}}\n"
            ),
        );
    }

    let fallback = search_repo_fast_filtered(
        repo.path(),
        "test:true agent common token",
        20,
        &Default::default(),
    )
    .unwrap();
    assert_eq!(
        result_paths(&fallback),
        vec![
            "pkg/client_test.go",
            "spec/models/user_spec.rb",
            "src/__tests__/widget.tsx",
            "src/auth.spec.ts",
            "src/lib_test.rs",
            "src/test/java/AuthFlow.java",
        ]
    );

    let fallback_source = search_repo_fast_filtered(
        repo.path(),
        "test:false agent common token",
        20,
        &Default::default(),
    )
    .unwrap();
    assert_eq!(
        result_paths(&fallback_source),
        vec!["src/contest.ts", "src/lib.rs", "src/testament.rs"]
    );

    let index = FastIndex::build(repo.path()).unwrap();
    let indexed = index
        .search_filtered("is:test agent common token", 20, &Default::default())
        .unwrap();
    assert_eq!(result_paths(&indexed), result_paths(&fallback));

    let plan = index
        .query_plan("is:source agent common token", &SearchFilters::default())
        .unwrap();
    assert!(plan.active_filters.iter().any(|filter| {
        filter.field == "test"
            && filter.value == "false"
            && filter.candidate_matches == Some(3)
            && filter.candidate_rejections == Some(0)
    }));
    assert!(
        plan.planned_postings
            .iter()
            .any(|posting| posting.kind == "filter" && posting.value == "test:false")
    );

    let shard_dir = tempfile::tempdir().unwrap();
    build_shards(&[repo.path().to_path_buf()], shard_dir.path()).unwrap();
    let shard_name = repo.path().file_name().unwrap().to_string_lossy();
    let shard_results = search_shards(
        shard_dir.path(),
        "test:true agent common token",
        20,
        &SearchFilters::default(),
    )
    .unwrap();
    assert_eq!(
        result_paths(&shard_results),
        result_paths(&fallback)
            .into_iter()
            .map(|path| format!("{shard_name}/{path}"))
            .collect::<Vec<_>>()
    );
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

#[test]
fn filter_only_fast_search_timeout_is_bounded() {
    let repo = tempfile::tempdir().unwrap();
    for index in 0..500 {
        write(
            &repo.path().join(format!("src/file_{index}.rs")),
            "pub fn unrelated_symbol() {}\n",
        );
    }

    let started = Instant::now();
    let results = search_repo_fast_filtered_with_timeout(
        repo.path(),
        "file:*.rs",
        10,
        &SearchFilters::default(),
        Duration::from_nanos(1),
    )
    .unwrap();

    assert!(started.elapsed() < Duration::from_millis(500));
    assert!(results.len() <= 10);
}
