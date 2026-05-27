use std::fs;
use std::path::Path;

use orient::fast_index::FastIndex;
use orient::repo_index::{SearchFilters, search_repo_fast_filtered};
use orient::shards::{build_shards, search_shards, shard_query_plans};

fn write(path: &Path, text: &str) {
    fs::create_dir_all(path.parent().unwrap()).unwrap();
    fs::write(path, text).unwrap();
}

struct GoldenCase {
    query: &'static str,
    expected_path: &'static str,
    filters: SearchFilters,
}

fn golden_repo() -> tempfile::TempDir {
    let repo = tempfile::tempdir().unwrap();
    write(
        &repo.path().join("src/auth.rs"),
        r#"
pub struct SessionManager;

impl SessionManager {
    pub fn issue_token(&self, user_id: &str) -> String {
        format!("session-token-{user_id}")
    }
}
"#,
    );
    write(
        &repo.path().join("src/errors.rs"),
        r#"
pub fn database_error() -> &'static str {
    "database connection refused"
}
"#,
    );
    write(
        &repo.path().join("src/http_gateway.rs"),
        r#"
pub fn route_request() {
    let gateway = "http";
}
"#,
    );
    write(
        &repo.path().join("tests/auth_test.rs"),
        r#"
use sample::SessionManager;

#[test]
fn issue_token_round_trip() {
    let token = SessionManager.issue_token("u_123");
    assert!(token.contains("session-token"));
}
"#,
    );
    write(
        &repo.path().join("docs/auth.md"),
        "SessionManager issue token docs should not beat source.\n",
    );
    write(
        &repo.path().join("Cargo.toml"),
        "[package]\nname='golden'\nversion='0.1.0'\nedition='2024'\n",
    );
    repo
}

fn golden_cases() -> Vec<GoldenCase> {
    vec![
        GoldenCase {
            query: "symbol:SessionManager issue token",
            expected_path: "src/auth.rs",
            filters: SearchFilters::default(),
        },
        GoldenCase {
            query: "\"database connection refused\"",
            expected_path: "src/errors.rs",
            filters: SearchFilters::default(),
        },
        GoldenCase {
            query: "file:http_gateway.rs",
            expected_path: "src/http_gateway.rs",
            filters: SearchFilters::default(),
        },
        GoldenCase {
            query: "lang:rust test:true issue token",
            expected_path: "tests/auth_test.rs",
            filters: SearchFilters::default(),
        },
        GoldenCase {
            query: "path:src gateway",
            expected_path: "src/http_gateway.rs",
            filters: SearchFilters::default(),
        },
        GoldenCase {
            query: "issue token -path:docs -path:tests",
            expected_path: "src/auth.rs",
            filters: SearchFilters {
                require_all: true,
                ..SearchFilters::default()
            },
        },
    ]
}

#[test]
fn golden_corpus_retrieval_matches_across_fallback_indexed_and_shards() {
    let repo = golden_repo();
    let index = FastIndex::build(repo.path()).unwrap();
    let shard_dir = tempfile::tempdir().unwrap();
    build_shards(&[repo.path().to_path_buf()], shard_dir.path()).unwrap();
    let shard_name = repo.path().file_name().unwrap().to_string_lossy();

    for case in golden_cases() {
        let fallback = search_repo_fast_filtered(repo.path(), case.query, 5, &case.filters)
            .unwrap_or_else(|error| panic!("fallback search failed for {:?}: {error}", case.query));
        assert_eq!(
            fallback.first().map(|result| result.path.as_str()),
            Some(case.expected_path),
            "fallback top hit for {:?}: {fallback:?}",
            case.query
        );

        let indexed = index
            .search_filtered(case.query, 5, &case.filters)
            .unwrap_or_else(|error| panic!("indexed search failed for {:?}: {error}", case.query));
        assert_eq!(
            indexed.first().map(|result| result.path.as_str()),
            Some(case.expected_path),
            "indexed top hit for {:?}: {indexed:?}",
            case.query
        );

        let shard = search_shards(shard_dir.path(), case.query, 5, &case.filters)
            .unwrap_or_else(|error| panic!("shard search failed for {:?}: {error}", case.query));
        let expected_shard_path = format!("{shard_name}/{}", case.expected_path);
        assert_eq!(
            shard.first().map(|result| result.path.as_str()),
            Some(expected_shard_path.as_str()),
            "shard top hit for {:?}: {shard:?}",
            case.query
        );
    }
}

#[test]
fn golden_corpus_indexed_plan_explains_empty_queries() {
    let repo = golden_repo();
    let index = FastIndex::build(repo.path()).unwrap();

    let plan = index
        .query_plan("SessionManager zzzxxyneverterm", &SearchFilters::default())
        .unwrap();

    assert_eq!(plan.strategy, "posting_intersection");
    assert!(plan.require_all);
    assert_eq!(plan.missing_terms, vec!["zzzxxyneverterm"]);
    assert_eq!(plan.candidate_count, 0);

    let shard_dir = tempfile::tempdir().unwrap();
    build_shards(&[repo.path().to_path_buf()], shard_dir.path()).unwrap();
    let shard_plans = shard_query_plans(
        shard_dir.path(),
        "SessionManager zzzxxyneverterm",
        &SearchFilters::default(),
    )
    .unwrap();
    assert_eq!(shard_plans.len(), 1);
    assert_eq!(shard_plans[0].plan.missing_terms, vec!["zzzxxyneverterm"]);
    assert_eq!(shard_plans[0].plan.candidate_count, 0);
}
