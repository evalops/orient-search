use std::fs;
use std::path::Path;

use orient::fast_index::FastIndex;
use orient::repo_index::{SearchFilters, search_repo_fast_filtered};
use orient::server::{ToolRequest, ToolRuntime};
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

#[derive(Debug)]
struct RelevanceMetrics {
    surface: &'static str,
    cases: usize,
    hits_at_10: usize,
    reciprocal_rank_sum: f64,
    misses: Vec<String>,
}

impl RelevanceMetrics {
    fn new(surface: &'static str) -> Self {
        Self {
            surface,
            cases: 0,
            hits_at_10: 0,
            reciprocal_rank_sum: 0.0,
            misses: Vec::new(),
        }
    }

    fn observe(&mut self, query: &str, ranked_paths: &[String], expected_path: &str) {
        self.cases += 1;
        if let Some(index) = ranked_paths
            .iter()
            .take(10)
            .position(|path| path == expected_path)
        {
            self.hits_at_10 += 1;
            self.reciprocal_rank_sum += 1.0 / (index + 1) as f64;
        } else {
            self.misses.push(format!(
                "query={query:?} expected={expected_path:?} ranked={ranked_paths:?}"
            ));
        }
    }

    fn recall_at_10(&self) -> f64 {
        self.hits_at_10 as f64 / self.cases.max(1) as f64
    }

    fn mrr(&self) -> f64 {
        self.reciprocal_rank_sum / self.cases.max(1) as f64
    }

    fn summary(&self) -> String {
        format!(
            "{} relevance: cases={} recall@10={:.3} mrr={:.3} misses={:?}",
            self.surface,
            self.cases,
            self.recall_at_10(),
            self.mrr(),
            self.misses
        )
    }
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
        &repo.path().join("src/generated/session_manager.rs"),
        "pub struct SessionManagerGenerated; pub fn issue_token_generated() {}\n",
    );
    write(
        &repo.path().join("Cargo.lock"),
        "name = \"SessionManager\"\nversion = \"0.0.0\"\nissue token lockfile noise\n",
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
            query: "content:\"database connection refused\"",
            expected_path: "src/errors.rs",
            filters: SearchFilters::default(),
        },
        GoldenCase {
            query: "text:gateway",
            expected_path: "src/http_gateway.rs",
            filters: SearchFilters::default(),
        },
        GoldenCase {
            query: "type:function route request",
            expected_path: "src/http_gateway.rs",
            filters: SearchFilters::default(),
        },
        GoldenCase {
            query: "file:http_gateway.rs",
            expected_path: "src/http_gateway.rs",
            filters: SearchFilters::default(),
        },
        GoldenCase {
            query: "file:*.rs gateway",
            expected_path: "src/http_gateway.rs",
            filters: SearchFilters::default(),
        },
        GoldenCase {
            query: "path:src/*gateway.rs",
            expected_path: "src/http_gateway.rs",
            filters: SearchFilters::default(),
        },
        GoldenCase {
            query: "path:src/http_???????.rs gateway",
            expected_path: "src/http_gateway.rs",
            filters: SearchFilters::default(),
        },
        GoldenCase {
            query: "path:src\\http_gateway.rs gateway",
            expected_path: "src/http_gateway.rs",
            filters: SearchFilters::default(),
        },
        GoldenCase {
            query: "lang:rust test:true issue token",
            expected_path: "tests/auth_test.rs",
            filters: SearchFilters::default(),
        },
        GoldenCase {
            query: "is:test issue token",
            expected_path: "tests/auth_test.rs",
            filters: SearchFilters::default(),
        },
        GoldenCase {
            query: "path:src gateway",
            expected_path: "src/http_gateway.rs",
            filters: SearchFilters::default(),
        },
        GoldenCase {
            query: "is:source issue token -path:docs",
            expected_path: "src/auth.rs",
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
        GoldenCase {
            query: "issue token -file:*test.rs -path:docs",
            expected_path: "src/auth.rs",
            filters: SearchFilters {
                require_all: true,
                ..SearchFilters::default()
            },
        },
        GoldenCase {
            query: "issue token -path:docs\\auth.md -path:tests\\auth_test.rs",
            expected_path: "src/auth.rs",
            filters: SearchFilters {
                require_all: true,
                ..SearchFilters::default()
            },
        },
        GoldenCase {
            query: "is:source auth issue token",
            expected_path: "src/auth.rs",
            filters: SearchFilters {
                require_all: false,
                ..SearchFilters::default()
            },
        },
        GoldenCase {
            query: "issue token generated:false test:false",
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
    let runtime = ToolRuntime::default();
    let index_path = repo.path().join("golden.index");
    index.save(&index_path).unwrap();

    let mut fallback_metrics = RelevanceMetrics::new("fallback");
    let mut indexed_metrics = RelevanceMetrics::new("indexed");
    let mut shard_metrics = RelevanceMetrics::new("shards");
    let mut auto_index_metrics = RelevanceMetrics::new("search_auto(index)");
    let mut auto_shard_metrics = RelevanceMetrics::new("search_auto(shards)");

    for case in golden_cases() {
        let fallback = search_repo_fast_filtered(repo.path(), case.query, 5, &case.filters)
            .unwrap_or_else(|error| panic!("fallback search failed for {:?}: {error}", case.query));
        fallback_metrics.observe(case.query, &result_paths(&fallback), case.expected_path);

        let indexed = index
            .search_filtered(case.query, 5, &case.filters)
            .unwrap_or_else(|error| panic!("indexed search failed for {:?}: {error}", case.query));
        indexed_metrics.observe(case.query, &result_paths(&indexed), case.expected_path);

        let shard = search_shards(shard_dir.path(), case.query, 5, &case.filters)
            .unwrap_or_else(|error| panic!("shard search failed for {:?}: {error}", case.query));
        let expected_shard_path = format!("{shard_name}/{}", case.expected_path);
        shard_metrics.observe(case.query, &result_paths(&shard), &expected_shard_path);

        let auto_indexed = runtime.dispatch(ToolRequest {
            id: serde_json::json!("auto-indexed"),
            tool: "search_auto".to_string(),
            arguments: serde_json::json!({
                "index": index_path,
                "query": case.query,
                "limit": 5
            }),
        });
        assert!(
            auto_indexed.error.is_none(),
            "search_auto indexed failed for {:?}: {:?}",
            case.query,
            auto_indexed.error
        );
        auto_index_metrics.observe(
            case.query,
            &json_result_paths(&auto_indexed.result.unwrap()["results"]),
            case.expected_path,
        );

        let auto_shards = runtime.dispatch(ToolRequest {
            id: serde_json::json!("auto-shards"),
            tool: "search_auto".to_string(),
            arguments: serde_json::json!({
                "index_dir": shard_dir.path(),
                "query": case.query,
                "limit": 5
            }),
        });
        assert!(
            auto_shards.error.is_none(),
            "search_auto shards failed for {:?}: {:?}",
            case.query,
            auto_shards.error
        );
        auto_shard_metrics.observe(
            case.query,
            &json_result_paths(&auto_shards.result.unwrap()["results"]),
            &expected_shard_path,
        );
    }

    for metrics in [
        fallback_metrics,
        indexed_metrics,
        shard_metrics,
        auto_index_metrics,
        auto_shard_metrics,
    ] {
        assert_eq!(metrics.recall_at_10(), 1.0, "{}", metrics.summary());
        assert_eq!(metrics.mrr(), 1.0, "{}", metrics.summary());
    }
}

fn result_paths(results: &[orient::repo_index::SearchResult]) -> Vec<String> {
    results.iter().map(|result| result.path.clone()).collect()
}

fn json_result_paths(results: &serde_json::Value) -> Vec<String> {
    results
        .as_array()
        .unwrap()
        .iter()
        .filter_map(|result| result["path"].as_str().map(str::to_string))
        .collect()
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
