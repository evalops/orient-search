use std::fs;
use std::path::Path;

use orient::fast_index::FastIndex;
use orient::repo_index::{
    MAX_READ_RANGE_LINES, RangeScope, RepoIndexer, RepoMapDetail, SearchFilters, read_file_range,
    read_file_range_scoped, search_repo_fast_filtered,
};

fn write(path: &Path, text: &str) {
    fs::create_dir_all(path.parent().unwrap()).unwrap();
    fs::write(path, text).unwrap();
}

#[test]
fn indexes_repo_symbols_search_related_files_and_commands() {
    let temp = tempfile::tempdir().unwrap();
    write(
        &temp.path().join("src/auth.py"),
        r#"
import json

class SessionManager:
    def issue_token(self, user_id: str) -> str:
        return json.dumps({"sub": user_id})

def verify_token(token: str) -> bool:
    return token.startswith("{")
"#,
    );
    write(
        &temp.path().join("tests/test_auth.py"),
        r#"
from src.auth import SessionManager, verify_token

def test_issue_token_round_trip():
    assert verify_token(sessionmanager().issue_token("u_123"))
"#,
    );
    write(
        &temp.path().join("pyproject.toml"),
        "[project]\nname='sample'\ndependencies=['fastapi>=0.100', 'pydantic']\n[tool.ruff]\nline-length=100\n[tool.mypy]\npython_version='3.12'\n",
    );
    write(&temp.path().join("uv.lock"), "version = 1\n");
    write(
        &temp.path().join("package.json"),
        r#"{"scripts":{"test":"vitest run","lint":"eslint .","typecheck":"tsc --noEmit"},"dependencies":{"react":"latest"},"devDependencies":{"typescript":"latest"}}"#,
    );
    write(
        &temp.path().join("MODULE.bazel"),
        "module(name = \"sample\")\n",
    );
    write(
        &temp.path().join("BUILD.bazel"),
        "exports_files([\"pyproject.toml\"])\n",
    );
    write(
        &temp.path().join("pnpm-lock.yaml"),
        "lockfileVersion: '9.0'\n",
    );
    write(
        &temp.path().join("Justfile"),
        "test:\n    pytest\nfmt:\n    ruff format .\n",
    );
    write(
        &temp.path().join("Makefile"),
        "test:\n\tpytest\nlint:\n\truff check .\n",
    );
    write(
        &temp.path().join("pom.xml"),
        "<project><modelVersion>4.0.0</modelVersion></project>\n",
    );
    write(&temp.path().join("build.gradle.kts"), "plugins { java }\n");
    write(&temp.path().join("gradlew"), "#!/bin/sh\n");

    let index = RepoIndexer::new(temp.path()).build().unwrap();

    let symbol = index.find_symbol("SessionManager", 10).remove(0);
    assert_eq!(symbol.path, "src/auth.py");
    assert_eq!(symbol.kind, "class");
    assert!(index.find_symbol("", 10).is_empty());
    assert!(index.find_symbol("SessionManager", 0).is_empty());
    assert!(
        index
            .find_symbol_filtered(
                "SessionManager",
                10,
                &SearchFilters {
                    symbol_kind: Some("function".to_string()),
                    ..SearchFilters::default()
                },
            )
            .is_empty()
    );
    let function_symbols = index.find_symbol_filtered(
        "token",
        10,
        &SearchFilters {
            symbol_kind: Some("function".to_string()),
            path: Some("src/auth.py".to_string()),
            ..SearchFilters::default()
        },
    );
    assert!(
        function_symbols
            .iter()
            .all(|symbol| symbol.kind == "function" && symbol.path == "src/auth.py"),
        "{function_symbols:?}"
    );

    let search = index.search_code("issue token user session", 3);
    assert_eq!(search[0].path, "src/auth.py");
    assert!(search[0].snippet.contains("issue_token"));

    let related = index.related_files("src/auth.py", 10);
    assert!(
        related.iter().any(|item| item.path == "tests/test_auth.py"
            && item.reason.contains("references symbol SessionManager")),
        "{related:?}"
    );
    let related: Vec<_> = related.into_iter().map(|item| item.path).collect();
    assert!(related.contains(&"tests/test_auth.py".to_string()));
    let test_related: Vec<_> = index
        .related_files("tests/test_auth.py", 10)
        .into_iter()
        .map(|item| item.path)
        .collect();
    assert!(test_related.contains(&"src/auth.py".to_string()));
    assert!(index.related_files("src/missing_auth.py", 10).is_empty());

    let related_symbols = index.related_symbols(Some("src/auth.py"), Some("session token"), 10);
    assert_eq!(related_symbols[0].symbol.name, "SessionManager");
    assert_eq!(related_symbols[0].symbol.path, "src/auth.py");
    assert!(related_symbols[0].reason.contains("same file"));
    assert!(
        related_symbols
            .iter()
            .any(|item| item.symbol.name == "verify_token")
    );
    let test_related_symbols = index.related_symbols(Some("tests/test_auth.py"), None, 10);
    assert!(
        test_related_symbols
            .iter()
            .any(|item| item.symbol.name == "SessionManager"
                && item.symbol.path == "src/auth.py"
                && item.reason.contains("referenced by source")),
        "{test_related_symbols:?}"
    );
    assert!(
        index
            .related_symbols(Some("src/missing_auth.py"), Some("SessionManager"), 10)
            .is_empty()
    );

    write(
        &temp.path().join("src/billing.rs"),
        "pub fn repo_lookup_total() {}\npub fn invoice_total() {}\npub struct InvoiceTotal;\n",
    );
    let billing_index = RepoIndexer::new(temp.path()).build().unwrap();
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

    let brief = index.repo_brief();
    assert_eq!(brief.language_counts.get("python"), Some(&2));
    assert!(brief.known_commands.contains(&"uv run pytest".to_string()));
    assert!(
        brief
            .known_commands
            .contains(&"uv run ruff check .".to_string())
    );
    assert!(brief.known_commands.contains(&"uv run mypy .".to_string()));
    assert!(
        brief
            .known_commands
            .contains(&"bazel build //...".to_string())
    );
    assert!(
        brief
            .known_commands
            .contains(&"bazel test //...".to_string())
    );
    assert!(brief.known_commands.contains(&"pnpm test".to_string()));
    assert!(brief.known_commands.contains(&"just test".to_string()));
    assert!(brief.known_commands.contains(&"just fmt".to_string()));
    assert!(brief.known_commands.contains(&"make test".to_string()));
    assert!(brief.known_commands.contains(&"make lint".to_string()));
    assert!(brief.known_commands.contains(&"mvn test".to_string()));
    assert!(brief.known_commands.contains(&"mvn package".to_string()));
    assert!(brief.known_commands.contains(&"./gradlew test".to_string()));
    assert!(
        brief
            .known_commands
            .contains(&"./gradlew build".to_string())
    );
    assert!(brief.known_commands.contains(&"pnpm run lint".to_string()));
    assert!(
        brief
            .known_commands
            .contains(&"pnpm run typecheck".to_string())
    );
    assert!(brief.command_hints.iter().any(|hint| {
        hint.command == "pnpm test" && hint.kind == "test" && hint.source == "package.json"
    }));
    assert!(brief.command_hints.iter().any(|hint| {
        hint.command == "pnpm run lint" && hint.kind == "lint" && hint.source == "package.json"
    }));
    assert!(brief.command_hints.iter().any(|hint| {
        hint.command == "uv run pytest" && hint.kind == "test" && hint.source == "pyproject.toml"
    }));
    assert!(brief.command_hints.iter().any(|hint| {
        hint.command == "uv run ruff check ."
            && hint.kind == "lint"
            && hint.source == "pyproject.toml"
    }));
    assert!(brief.command_hints.iter().any(|hint| {
        hint.command == "uv run mypy ."
            && hint.kind == "typecheck"
            && hint.source == "pyproject.toml"
    }));
    assert!(brief.command_hints.iter().any(|hint| {
        hint.command == "bazel build //..." && hint.kind == "build" && hint.source == "MODULE.bazel"
    }));
    assert!(brief.command_hints.iter().any(|hint| {
        hint.command == "bazel test //..." && hint.kind == "test" && hint.source == "MODULE.bazel"
    }));
    assert!(brief.command_hints.iter().any(|hint| {
        hint.command == "just test" && hint.kind == "test" && hint.source == "Justfile"
    }));
    assert!(brief.command_hints.iter().any(|hint| {
        hint.command == "just fmt" && hint.kind == "format" && hint.source == "Justfile"
    }));
    assert!(brief.command_hints.iter().any(|hint| {
        hint.command == "make lint" && hint.kind == "lint" && hint.source == "Makefile"
    }));
    assert!(brief.command_hints.iter().any(|hint| {
        hint.command == "mvn test" && hint.kind == "test" && hint.source == "pom.xml"
    }));
    assert!(brief.command_hints.iter().any(|hint| {
        hint.command == "mvn package" && hint.kind == "build" && hint.source == "pom.xml"
    }));
    assert!(brief.command_hints.iter().any(|hint| {
        hint.command == "./gradlew test" && hint.kind == "test" && hint.source == "build.gradle.kts"
    }));
    assert!(brief.command_hints.iter().any(|hint| {
        hint.command == "./gradlew build"
            && hint.kind == "build"
            && hint.source == "build.gradle.kts"
    }));
    assert!(brief.dependency_hints.iter().any(|hint| {
        hint.name == "fastapi" && hint.kind == "dependency" && hint.source == "pyproject.toml"
    }));
    assert!(brief.dependency_hints.iter().any(|hint| {
        hint.name == "react" && hint.kind == "dependency" && hint.source == "package.json"
    }));
    assert!(brief.dependency_hints.iter().any(|hint| {
        hint.name == "typescript" && hint.kind == "dev_dependency" && hint.source == "package.json"
    }));
    assert!(brief.import_hints.iter().any(|hint| {
        hint.module == "json"
            && hint.kind == "import"
            && hint.source == "src/auth.py"
            && hint.line == 2
    }));
    assert!(brief.import_hints.iter().any(|hint| {
        hint.module == "src.auth"
            && hint.kind == "from"
            && hint.source == "tests/test_auth.py"
            && hint.line == 2
    }));
    assert!(brief.manifest_files.contains(&"pyproject.toml".to_string()));
    assert!(brief.manifest_files.contains(&"MODULE.bazel".to_string()));
    assert!(brief.manifest_files.contains(&"pom.xml".to_string()));
    assert!(
        brief
            .manifest_files
            .contains(&"build.gradle.kts".to_string())
    );
    assert!(
        brief
            .important_files
            .contains(&"pyproject.toml".to_string())
    );
    assert!(brief.important_files.contains(&"MODULE.bazel".to_string()));
    assert!(brief.important_files.contains(&"Justfile".to_string()));
    assert!(brief.important_files.contains(&"Makefile".to_string()));

    let map = index.repo_map(10, 10);
    assert_eq!(map.manifest_files, map.brief.manifest_files);
    assert_eq!(map.important_files, map.brief.important_files);
    assert_eq!(map.known_commands, map.brief.known_commands);
    assert_eq!(map.command_hints, map.brief.command_hints);
    assert_eq!(map.dependency_hints, map.brief.dependency_hints);
    assert_eq!(map.import_hints, map.brief.import_hints);
    assert_eq!(map.summary.status, "mapped");
    assert_eq!(map.summary.file_count, map.brief.file_count);
    assert_eq!(map.summary.entrypoint_count, map.entrypoints.len());
    assert_eq!(map.summary.manifest_count, map.manifest_files.len());
    assert_eq!(map.summary.important_file_count, map.important_files.len());
    assert_eq!(map.summary.test_file_count, map.test_files.len());
    assert_eq!(map.summary.top_symbol_count, map.top_symbols.len());
    assert_eq!(map.summary.related_file_count, map.related_files.len());
    assert_eq!(map.summary.related_symbol_count, map.related_symbols.len());
    assert_eq!(map.summary.command_count, map.known_commands.len());
    assert_eq!(map.summary.dependency_count, map.dependency_hints.len());
    assert_eq!(map.summary.import_count, map.import_hints.len());
    assert!(
        map.related_files.iter().any(|related| {
            (related.source_path == "src/auth.py" && related.path == "tests/test_auth.py")
                || (related.source_path == "tests/test_auth.py" && related.path == "src/auth.py")
        }),
        "{:?}",
        map.related_files
    );
    assert!(
        map.related_symbols.iter().any(|related| {
            related.source_path == "src/auth.py"
                && related.symbol.name == "SessionManager"
                && related.reason.contains("same file")
        }),
        "{:?}",
        map.related_symbols
    );
}

#[test]
fn related_files_ignore_low_information_reference_symbols() {
    let temp = tempfile::tempdir().unwrap();
    write(
        &temp.path().join("src/main.rs"),
        r#"
struct SessionManager;

impl From<String> for SessionManager {
    fn from(value: String) -> Self {
        let _ = value;
        SessionManager
    }
}

fn write() {}
fn output() {}
fn arguments() {}
fn index() {}
fn entry() {}
fn value() {}
"#,
    );
    write(
        &temp.path().join("docs/noise.md"),
        "This note talks about from, write, output, arguments, index, entry, and value.\n",
    );
    write(
        &temp.path().join("docs/usage.md"),
        "Use SessionManager when wiring the service.\n",
    );

    let index = RepoIndexer::new(temp.path()).build().unwrap();
    let related = index.related_files("src/main.rs", 10);
    assert!(
        related.iter().any(|item| item.path == "docs/usage.md"
            && item.reason.contains("references symbol SessionManager")),
        "{related:?}"
    );
    assert!(
        !related.iter().any(|item| item.path == "docs/noise.md"),
        "{related:?}"
    );

    let fast_index = FastIndex::build(temp.path()).unwrap();
    let indexed_related = fast_index.related_files("src/main.rs", 10);
    assert!(
        indexed_related
            .iter()
            .any(|item| item.path == "docs/usage.md"
                && item.reason.contains("references symbol SessionManager")),
        "{indexed_related:?}"
    );
    assert!(
        !indexed_related
            .iter()
            .any(|item| item.path == "docs/noise.md"),
        "{indexed_related:?}"
    );
}

#[test]
fn fallback_symbol_filter_finds_deep_private_definition_after_broad_hits() {
    let repo = tempfile::tempdir().unwrap();
    let mut source = String::new();
    for index in 0..40 {
        source.push_str(&format!(
            "// early broad token noise {index}: target symbol name routing\n"
        ));
    }
    source.push_str("fn target_symbol_name() -> bool { true }\n");
    write(&repo.path().join("src/lib.rs"), &source);

    let results = search_repo_fast_filtered(
        repo.path(),
        "symbol:target_symbol_name",
        5,
        &Default::default(),
    )
    .unwrap();

    assert_eq!(results.len(), 1);
    assert_eq!(results[0].path, "src/lib.rs");
    assert!(results[0].reason.contains("symbol:target_symbol_name"));
    assert!(results[0].snippet.contains("target_symbol_name"));

    let mixed = search_repo_fast_filtered(
        repo.path(),
        "routing symbol:target_symbol_name",
        5,
        &Default::default(),
    )
    .unwrap();
    assert_eq!(mixed.len(), 1);
    assert!(mixed[0].reason.contains("symbol:target_symbol_name"));
    assert!(mixed[0].snippet.contains("target_symbol_name"));
}

#[test]
fn go_symbols_work_across_live_fallback_and_persistent_indexes() {
    let repo = tempfile::tempdir().unwrap();
    write(
        &repo.path().join("server.go"),
        r#"
package server

type Server struct {}

func NewServer() *Server {
    return &Server{}
}

func (s *Server) ServeHTTP() {}
"#,
    );

    let live = RepoIndexer::new(repo.path()).build().unwrap();
    let live_symbols = live.find_symbol("ServeHTTP", 10);
    assert_eq!(live_symbols[0].path, "server.go");
    assert_eq!(live_symbols[0].kind, "function");
    assert_eq!(live_symbols[0].line, 10);
    assert!(live.find_symbol("Server", 10).iter().any(|symbol| {
        symbol.name == "Server" && symbol.kind == "struct" && symbol.path == "server.go"
    }));

    let fallback =
        search_repo_fast_filtered(repo.path(), "symbol:ServeHTTP", 5, &Default::default()).unwrap();
    assert_eq!(fallback[0].path, "server.go");
    assert!(fallback[0].reason.contains("symbol:ServeHTTP"));
    assert!(fallback[0].snippet.contains("ServeHTTP"));

    let indexed = FastIndex::build(repo.path()).unwrap();
    let indexed_symbols = indexed.find_symbol("ServeHTTP", 10);
    assert_eq!(indexed_symbols[0].path, "server.go");
    assert_eq!(indexed_symbols[0].kind, "function");
    let indexed_results = indexed
        .search_filtered("symbol:ServeHTTP", 5, &Default::default())
        .unwrap();
    assert_eq!(indexed_results[0].path, "server.go");
    assert!(indexed_results[0].reason.contains("symbol:ServeHTTP"));
}

#[test]
fn common_language_symbols_work_across_live_and_persistent_indexes() {
    let repo = tempfile::tempdir().unwrap();
    write(
        &repo.path().join("app.rb"),
        r#"
class BillingJob
  def perform!
  end
end
"#,
    );
    write(
        &repo.path().join("App.kt"),
        r#"
package sample

data class InvoiceState(val id: String)

fun String.slugify(): String = lowercase()
"#,
    );
    write(
        &repo.path().join("Client.swift"),
        r#"
protocol PaymentClient {
  func chargeCard()
}
"#,
    );
    write(
        &repo.path().join("Gateway.java"),
        r#"
public class Gateway {
  public String routePayment() {
    return "ok";
  }
}
"#,
    );

    let live = RepoIndexer::new(repo.path()).build().unwrap();
    assert_symbol(&live.find_symbol("BillingJob", 10)[0], "app.rb", "class");
    assert_symbol(&live.find_symbol("perform", 10)[0], "app.rb", "function");
    assert_symbol(&live.find_symbol("InvoiceState", 10)[0], "App.kt", "class");
    assert_symbol(&live.find_symbol("slugify", 10)[0], "App.kt", "function");
    assert_symbol(
        &live.find_symbol("PaymentClient", 10)[0],
        "Client.swift",
        "interface",
    );
    assert_symbol(
        &live.find_symbol("chargeCard", 10)[0],
        "Client.swift",
        "function",
    );
    assert_symbol(&live.find_symbol("Gateway", 10)[0], "Gateway.java", "class");
    assert_symbol(
        &live.find_symbol("routePayment", 10)[0],
        "Gateway.java",
        "function",
    );

    let fallback =
        search_repo_fast_filtered(repo.path(), "symbol:routePayment", 5, &Default::default())
            .unwrap();
    assert_eq!(fallback[0].path, "Gateway.java");
    assert!(fallback[0].snippet.contains("routePayment"));

    let indexed = FastIndex::build(repo.path()).unwrap();
    assert_symbol(&indexed.find_symbol("slugify", 10)[0], "App.kt", "function");
    let indexed_results = indexed
        .search_filtered("symbol:chargeCard", 5, &Default::default())
        .unwrap();
    assert_eq!(indexed_results[0].path, "Client.swift");
    assert!(indexed_results[0].reason.contains("symbol:chargeCard"));
}

#[test]
fn c_family_symbols_work_across_live_and_persistent_indexes() {
    let repo = tempfile::tempdir().unwrap();
    write(
        &repo.path().join("engine.cpp"),
        r#"
class SearchEngine {};

int score_candidate(int value) {
    return value;
}

void SearchEngine::refresh_index() {}
"#,
    );
    write(
        &repo.path().join("include/query.h"),
        r#"
struct QueryPlan {
    int candidate_count;
};
"#,
    );
    write(
        &repo.path().join("SearchController.cs"),
        r#"
public interface ISearchClient {}

public class SearchController {
    public SearchResult ExecuteSearch(QueryPlan plan) {
        return default;
    }
}
"#,
    );

    let live = RepoIndexer::new(repo.path()).build().unwrap();
    assert_symbol(
        &live.find_symbol("SearchEngine", 10)[0],
        "engine.cpp",
        "class",
    );
    assert_symbol(
        &live.find_symbol("score_candidate", 10)[0],
        "engine.cpp",
        "function",
    );
    assert_symbol(
        &live.find_symbol("refresh_index", 10)[0],
        "engine.cpp",
        "function",
    );
    assert_symbol(
        &live.find_symbol("QueryPlan", 10)[0],
        "include/query.h",
        "struct",
    );
    assert_symbol(
        &live.find_symbol("ISearchClient", 10)[0],
        "SearchController.cs",
        "interface",
    );
    assert_symbol(
        &live.find_symbol("ExecuteSearch", 10)[0],
        "SearchController.cs",
        "function",
    );

    let fallback = search_repo_fast_filtered(
        repo.path(),
        "lang:cpp symbol:score_candidate",
        5,
        &Default::default(),
    )
    .unwrap();
    assert_eq!(fallback[0].path, "engine.cpp");
    assert!(fallback[0].reason.contains("symbol:score_candidate"));

    let indexed = FastIndex::build(repo.path()).unwrap();
    assert_symbol(
        &indexed.find_symbol("ExecuteSearch", 10)[0],
        "SearchController.cs",
        "function",
    );
    let indexed_results = indexed
        .search_filtered("lang:csharp symbol:ISearchClient", 5, &Default::default())
        .unwrap();
    assert_eq!(indexed_results[0].path, "SearchController.cs");
    assert!(indexed_results[0].reason.contains("symbol:ISearchClient"));
}

#[test]
fn shell_symbols_work_across_live_and_persistent_indexes() {
    let repo = tempfile::tempdir().unwrap();
    write(
        &repo.path().join("scripts/bootstrap.sh"),
        r#"
#!/usr/bin/env bash

install_deps() {
    echo installing
}

function refresh_cache {
    echo refreshing
}
"#,
    );
    write(
        &repo.path().join("tests/bootstrap.bats"),
        r#"
@test "bootstrap installs deps" {
    run install_deps
}
"#,
    );

    let live = RepoIndexer::new(repo.path()).build().unwrap();
    assert_symbol(
        &live.find_symbol("install_deps", 10)[0],
        "scripts/bootstrap.sh",
        "function",
    );
    assert_symbol(
        &live.find_symbol("refresh_cache", 10)[0],
        "scripts/bootstrap.sh",
        "function",
    );

    let fallback = search_repo_fast_filtered(
        repo.path(),
        "lang:shell symbol:install_deps",
        5,
        &Default::default(),
    )
    .unwrap();
    assert_eq!(fallback[0].path, "scripts/bootstrap.sh");
    assert!(fallback[0].reason.contains("symbol:install_deps"));

    let indexed = FastIndex::build(repo.path()).unwrap();
    assert_symbol(
        &indexed.find_symbol("refresh_cache", 10)[0],
        "scripts/bootstrap.sh",
        "function",
    );
    let indexed_results = indexed
        .search_filtered(
            "lang:bash symbol:install_deps test:false",
            5,
            &Default::default(),
        )
        .unwrap();
    assert_eq!(indexed_results[0].path, "scripts/bootstrap.sh");
    assert!(indexed_results[0].reason.contains("symbol:install_deps"));
}

#[test]
fn task_file_targets_work_as_symbols_across_live_and_persistent_indexes() {
    let repo = tempfile::tempdir().unwrap();
    write(
        &repo.path().join("Makefile"),
        r#"
.PHONY: test deploy
test:
	pytest

deploy ENV=prod:
	./scripts/deploy.sh

clean-build:
	rm -rf build
"#,
    );
    write(
        &repo.path().join("Justfile"),
        r#"
test:
    cargo test

release target='prod':
    cargo build --release
"#,
    );

    let live = RepoIndexer::new(repo.path()).build().unwrap();
    assert_symbol(&live.find_symbol("deploy", 10)[0], "Makefile", "target");
    assert_symbol(
        &live.find_symbol("clean-build", 10)[0],
        "Makefile",
        "target",
    );
    assert_symbol(&live.find_symbol("release", 10)[0], "Justfile", "target");

    let fallback = search_repo_fast_filtered(
        repo.path(),
        "kind:target symbol:deploy",
        5,
        &Default::default(),
    )
    .unwrap();
    assert_eq!(fallback[0].path, "Makefile");
    assert!(fallback[0].reason.contains("symbol:deploy"));

    let indexed = FastIndex::build(repo.path()).unwrap();
    assert_symbol(&indexed.find_symbol("release", 10)[0], "Justfile", "target");
    let indexed_results = indexed
        .search_filtered("kind:target symbol:clean-build", 5, &Default::default())
        .unwrap();
    assert_eq!(indexed_results[0].path, "Makefile");
    assert!(indexed_results[0].reason.contains("symbol:clean-build"));
}

#[test]
fn github_actions_jobs_work_as_targets_across_live_and_persistent_indexes() {
    let repo = tempfile::tempdir().unwrap();
    write(
        &repo.path().join(".github/workflows/ci.yml"),
        r#"
name: CI

on:
  pull_request:

jobs:
  rust-tests:
    runs-on: ubuntu-latest
    steps:
      - run: cargo test
  lint:
    runs-on: ubuntu-latest
    steps:
      - run: cargo fmt --check
"#,
    );

    let live = RepoIndexer::new(repo.path()).build().unwrap();
    let live_job = &live.find_symbol("rust-tests", 10)[0];
    assert_symbol(live_job, ".github/workflows/ci.yml", "target");
    assert_eq!(live_job.line, 8);

    let fallback = search_repo_fast_filtered(
        repo.path(),
        "kind:target symbol:rust-tests",
        5,
        &Default::default(),
    )
    .unwrap();
    assert_eq!(fallback[0].path, ".github/workflows/ci.yml");
    assert!(fallback[0].reason.contains("symbol:rust-tests"));

    let indexed = FastIndex::build(repo.path()).unwrap();
    let indexed_lint = &indexed.find_symbol("lint", 10)[0];
    assert_symbol(indexed_lint, ".github/workflows/ci.yml", "target");
    assert_eq!(indexed_lint.line, 12);
    let indexed_results = indexed
        .search_filtered("recipe:lint", 5, &Default::default())
        .unwrap();
    assert_eq!(indexed_results[0].path, ".github/workflows/ci.yml");
    assert!(indexed_results[0].reason.contains("symbol:lint"));
}

#[test]
fn bazel_build_targets_work_as_symbols_across_live_and_persistent_indexes() {
    let repo = tempfile::tempdir().unwrap();
    write(
        &repo.path().join("BUILD.bazel"),
        r#"
load("@rules_rust//rust:defs.bzl", "rust_library")

rust_library(
    name = "orient_lib",
    srcs = ["src/lib.rs"],
)

py_test(name = "agent_smoke_test", srcs = ["agent_smoke_test.py"])

exports_files(["README.md"])
"#,
    );

    let live = RepoIndexer::new(repo.path()).build().unwrap();
    let live_lib = &live.find_symbol("orient_lib", 10)[0];
    assert_symbol(live_lib, "BUILD.bazel", "target");
    assert_eq!(live_lib.line, 5);
    let live_test = &live.find_symbol("agent_smoke_test", 10)[0];
    assert_symbol(live_test, "BUILD.bazel", "target");
    assert_eq!(live_test.line, 9);
    assert!(live.find_symbol("README.md", 10).is_empty());

    let fallback =
        search_repo_fast_filtered(repo.path(), "target:orient_lib", 5, &Default::default())
            .unwrap();
    assert_eq!(fallback[0].path, "BUILD.bazel");
    assert!(fallback[0].reason.contains("symbol:orient_lib"));

    let indexed = FastIndex::build(repo.path()).unwrap();
    let indexed_test = &indexed.find_symbol("agent_smoke_test", 10)[0];
    assert_symbol(indexed_test, "BUILD.bazel", "target");
    assert_eq!(indexed_test.line, 9);
    let indexed_results = indexed
        .search_filtered(
            "kind:target symbol:agent_smoke_test",
            5,
            &Default::default(),
        )
        .unwrap();
    assert_eq!(indexed_results[0].path, "BUILD.bazel");
    assert!(
        indexed_results[0]
            .reason
            .contains("symbol:agent_smoke_test")
    );
}

#[test]
fn bazel_labels_search_build_targets_by_package_and_symbol() {
    let repo = tempfile::tempdir().unwrap();
    write(
        &repo.path().join("BUILD.bazel"),
        r#"
rust_library(
    name = "root_lib",
    srcs = ["src/lib.rs"],
)
"#,
    );
    write(
        &repo.path().join("tools/search/BUILD.bazel"),
        r#"
rust_binary(
    name = "orient_cli",
    srcs = ["main.rs"],
)
"#,
    );

    let package_label = search_repo_fast_filtered(
        repo.path(),
        "//tools/search:orient_cli",
        5,
        &Default::default(),
    )
    .unwrap();
    assert_eq!(package_label[0].path, "tools/search/BUILD.bazel");
    assert!(package_label[0].reason.contains("symbol:orient_cli"));

    let relative_label =
        search_repo_fast_filtered(repo.path(), ":root_lib", 5, &Default::default()).unwrap();
    assert_eq!(relative_label[0].path, "BUILD.bazel");
    assert!(relative_label[0].reason.contains("symbol:root_lib"));

    let indexed = FastIndex::build(repo.path()).unwrap();
    let indexed_results = indexed
        .search_filtered("//tools/search:orient_cli", 5, &Default::default())
        .unwrap();
    assert_eq!(indexed_results[0].path, "tools/search/BUILD.bazel");
    assert!(indexed_results[0].reason.contains("symbol:orient_cli"));

    let command_label = search_repo_fast_filtered(
        repo.path(),
        "bazel test //tools/search:orient_cli",
        5,
        &Default::default(),
    )
    .unwrap();
    assert_eq!(command_label[0].path, "tools/search/BUILD.bazel");
    assert!(command_label[0].reason.contains("symbol:orient_cli"));

    let indexed_command = indexed
        .search_filtered(
            "bazel build //tools/search:orient_cli",
            5,
            &Default::default(),
        )
        .unwrap();
    assert_eq!(indexed_command[0].path, "tools/search/BUILD.bazel");
    assert!(indexed_command[0].reason.contains("symbol:orient_cli"));
}

#[test]
fn docker_compose_services_work_as_symbols_across_live_and_persistent_indexes() {
    let repo = tempfile::tempdir().unwrap();
    write(
        &repo.path().join("compose.yaml"),
        r#"
services:
  api:
    build: .
  worker-queue:
    image: example/worker
volumes:
  data:
"#,
    );

    let live = RepoIndexer::new(repo.path()).build().unwrap();
    let live_api = &live.find_symbol("api", 10)[0];
    assert_symbol(live_api, "compose.yaml", "service");
    assert_eq!(live_api.line, 3);
    let live_worker = &live.find_symbol("worker-queue", 10)[0];
    assert_symbol(live_worker, "compose.yaml", "service");
    assert_eq!(live_worker.line, 5);
    assert!(live.find_symbol("data", 10).is_empty());

    let fallback =
        search_repo_fast_filtered(repo.path(), "service:api", 5, &Default::default()).unwrap();
    assert_eq!(fallback[0].path, "compose.yaml");
    assert!(fallback[0].reason.contains("symbol:api"));

    let indexed = FastIndex::build(repo.path()).unwrap();
    let indexed_worker = &indexed.find_symbol("worker-queue", 10)[0];
    assert_symbol(indexed_worker, "compose.yaml", "service");
    assert_eq!(indexed_worker.line, 5);
    let indexed_results = indexed
        .search_filtered("kind:service symbol:worker-queue", 5, &Default::default())
        .unwrap();
    assert_eq!(indexed_results[0].path, "compose.yaml");
    assert!(indexed_results[0].reason.contains("symbol:worker-queue"));
}

#[test]
fn dockerfile_stages_work_as_symbols_across_live_and_persistent_indexes() {
    let repo = tempfile::tempdir().unwrap();
    write(
        &repo.path().join("Dockerfile"),
        r#"
FROM rust:1.82 AS builder
WORKDIR /app

FROM debian:bookworm AS runtime
COPY --from=builder /app/orient /usr/local/bin/orient
"#,
    );

    let live = RepoIndexer::new(repo.path()).build().unwrap();
    let live_builder = &live.find_symbol("builder", 10)[0];
    assert_symbol(live_builder, "Dockerfile", "stage");
    assert_eq!(live_builder.line, 2);
    let live_runtime = &live.find_symbol("runtime", 10)[0];
    assert_symbol(live_runtime, "Dockerfile", "stage");
    assert_eq!(live_runtime.line, 5);

    let fallback =
        search_repo_fast_filtered(repo.path(), "stage:builder", 5, &Default::default()).unwrap();
    assert_eq!(fallback[0].path, "Dockerfile");
    assert!(fallback[0].reason.contains("symbol:builder"));

    let indexed = FastIndex::build(repo.path()).unwrap();
    let indexed_runtime = &indexed.find_symbol("runtime", 10)[0];
    assert_symbol(indexed_runtime, "Dockerfile", "stage");
    assert_eq!(indexed_runtime.line, 5);
    let indexed_results = indexed
        .search_filtered("kind:stage symbol:runtime", 5, &Default::default())
        .unwrap();
    assert_eq!(indexed_results[0].path, "Dockerfile");
    assert!(indexed_results[0].reason.contains("symbol:runtime"));
}

#[test]
fn cargo_manifest_targets_work_as_symbols_across_live_and_persistent_indexes() {
    let repo = tempfile::tempdir().unwrap();
    write(
        &repo.path().join("Cargo.toml"),
        r#"
[package]
name = "auth-api"
version = "0.1.0"

[[bin]]
name = "auth-worker"
path = "src/bin/worker.rs"

[[example]]
name = "replay-session"
path = "examples/replay.rs"
"#,
    );

    let live = RepoIndexer::new(repo.path()).build().unwrap();
    let live_package = &live.find_symbol("auth-api", 10)[0];
    assert_symbol(live_package, "Cargo.toml", "package");
    assert_eq!(live_package.line, 3);
    let live_bin = &live.find_symbol("auth-worker", 10)[0];
    assert_symbol(live_bin, "Cargo.toml", "bin");
    assert_eq!(live_bin.line, 7);

    let fallback = search_repo_fast_filtered(
        repo.path(),
        "kind:bin symbol:auth-worker",
        5,
        &Default::default(),
    )
    .unwrap();
    assert_eq!(fallback[0].path, "Cargo.toml");
    assert!(fallback[0].reason.contains("symbol:auth-worker"));

    let indexed = FastIndex::build(repo.path()).unwrap();
    let indexed_package = &indexed.find_symbol("auth-api", 10)[0];
    assert_symbol(indexed_package, "Cargo.toml", "package");
    assert_eq!(indexed_package.line, 3);
    let indexed_example = &indexed.find_symbol("replay-session", 10)[0];
    assert_symbol(indexed_example, "Cargo.toml", "example");
    assert_eq!(indexed_example.line, 11);
    let indexed_results = indexed
        .search_filtered("bin:auth-worker", 5, &Default::default())
        .unwrap();
    assert_eq!(indexed_results[0].path, "Cargo.toml");
    assert!(indexed_results[0].reason.contains("symbol:auth-worker"));
}

#[test]
fn pyproject_entries_work_as_symbols_across_live_and_persistent_indexes() {
    let repo = tempfile::tempdir().unwrap();
    write(
        &repo.path().join("pyproject.toml"),
        r#"
[project]
name = "agent-tools"
version = "0.1.0"

[project.scripts]
orient-agent = "agent_tools.cli:main"

[tool.poetry.scripts]
legacy-agent = "agent_tools.legacy:main"
"#,
    );

    let live = RepoIndexer::new(repo.path()).build().unwrap();
    let live_package = &live.find_symbol("agent-tools", 10)[0];
    assert_symbol(live_package, "pyproject.toml", "package");
    assert_eq!(live_package.line, 3);
    let live_script = &live.find_symbol("orient-agent", 10)[0];
    assert_symbol(live_script, "pyproject.toml", "script");
    assert_eq!(live_script.line, 7);

    let fallback =
        search_repo_fast_filtered(repo.path(), "script:orient-agent", 5, &Default::default())
            .unwrap();
    assert_eq!(fallback[0].path, "pyproject.toml");
    assert!(fallback[0].reason.contains("symbol:orient-agent"));

    let indexed = FastIndex::build(repo.path()).unwrap();
    let indexed_package = &indexed.find_symbol("agent-tools", 10)[0];
    assert_symbol(indexed_package, "pyproject.toml", "package");
    assert_eq!(indexed_package.line, 3);
    let indexed_legacy = &indexed.find_symbol("legacy-agent", 10)[0];
    assert_symbol(indexed_legacy, "pyproject.toml", "script");
    assert_eq!(indexed_legacy.line, 10);
    let indexed_results = indexed
        .search_filtered("package:agent-tools", 5, &Default::default())
        .unwrap();
    assert_eq!(indexed_results[0].path, "pyproject.toml");
    assert!(indexed_results[0].reason.contains("symbol:agent-tools"));
}

#[test]
fn go_mod_module_works_as_package_symbol_across_live_and_persistent_indexes() {
    let repo = tempfile::tempdir().unwrap();
    write(
        &repo.path().join("go.mod"),
        r#"
module github.com/evalops/orient-search

go 1.22

require github.com/sourcegraph/zoekt v0.0.0
"#,
    );

    let live = RepoIndexer::new(repo.path()).build().unwrap();
    let live_package = &live.find_symbol("github.com/evalops/orient-search", 10)[0];
    assert_symbol(live_package, "go.mod", "package");
    assert_eq!(live_package.line, 2);

    let fallback = search_repo_fast_filtered(
        repo.path(),
        "package:github.com/evalops/orient-search",
        5,
        &Default::default(),
    )
    .unwrap();
    assert_eq!(fallback[0].path, "go.mod");
    assert!(
        fallback[0]
            .reason
            .contains("symbol:github.com/evalops/orient-search")
    );

    let indexed = FastIndex::build(repo.path()).unwrap();
    let indexed_package = &indexed.find_symbol("github.com/evalops/orient-search", 10)[0];
    assert_symbol(indexed_package, "go.mod", "package");
    assert_eq!(indexed_package.line, 2);
    let indexed_results = indexed
        .search_filtered(
            "package:github.com/evalops/orient-search",
            5,
            &Default::default(),
        )
        .unwrap();
    assert_eq!(indexed_results[0].path, "go.mod");
    assert!(
        indexed_results[0]
            .reason
            .contains("symbol:github.com/evalops/orient-search")
    );
}

#[test]
fn jvm_manifests_work_as_package_symbols_across_live_and_persistent_indexes() {
    let repo = tempfile::tempdir().unwrap();
    write(
        &repo.path().join("pom.xml"),
        r#"
<project>
  <modelVersion>4.0.0</modelVersion>
  <groupId>com.evalops</groupId>
  <artifactId>auth-service</artifactId>
  <dependencies>
    <dependency>
      <groupId>junit</groupId>
      <artifactId>junit</artifactId>
    </dependency>
  </dependencies>
</project>
"#,
    );
    write(
        &repo.path().join("settings.gradle.kts"),
        r#"
pluginManagement { repositories { gradlePluginPortal() } }
rootProject.name = "billing-worker"
"#,
    );
    write(
        &repo.path().join("build.gradle.kts"),
        r#"
plugins { kotlin("jvm") version "2.0.0" }
group = "com.evalops.gradle"
"#,
    );

    let live = RepoIndexer::new(repo.path()).build().unwrap();
    let live_artifact = &live.find_symbol("auth-service", 10)[0];
    assert_symbol(live_artifact, "pom.xml", "package");
    assert_eq!(live_artifact.line, 5);
    let live_coordinate = &live.find_symbol("com.evalops:auth-service", 10)[0];
    assert_symbol(live_coordinate, "pom.xml", "package");
    assert_eq!(live_coordinate.line, 5);
    assert!(live.find_symbol("junit", 10).is_empty());
    let live_gradle_root = &live.find_symbol("billing-worker", 10)[0];
    assert_symbol(live_gradle_root, "settings.gradle.kts", "package");
    assert_eq!(live_gradle_root.line, 3);

    let fallback = search_repo_fast_filtered(
        repo.path(),
        "package:com.evalops:auth-service",
        5,
        &Default::default(),
    )
    .unwrap();
    assert_eq!(fallback[0].path, "pom.xml");
    assert!(
        fallback[0]
            .reason
            .contains("symbol:com.evalops:auth-service")
    );

    let indexed = FastIndex::build(repo.path()).unwrap();
    let indexed_artifact = &indexed.find_symbol("auth-service", 10)[0];
    assert_symbol(indexed_artifact, "pom.xml", "package");
    assert_eq!(indexed_artifact.line, 5);
    let indexed_gradle_root = &indexed.find_symbol("billing-worker", 10)[0];
    assert_symbol(indexed_gradle_root, "settings.gradle.kts", "package");
    assert_eq!(indexed_gradle_root.line, 3);
    let indexed_gradle_group = &indexed.find_symbol("com.evalops.gradle", 10)[0];
    assert_symbol(indexed_gradle_group, "build.gradle.kts", "package");
    assert_eq!(indexed_gradle_group.line, 3);
    let indexed_results = indexed
        .search_filtered("package:billing-worker", 5, &Default::default())
        .unwrap();
    assert_eq!(indexed_results[0].path, "settings.gradle.kts");
    assert!(indexed_results[0].reason.contains("symbol:billing-worker"));
}

#[test]
fn package_json_scripts_work_as_symbols_across_live_and_persistent_indexes() {
    let repo = tempfile::tempdir().unwrap();
    write(
        &repo.path().join("package.json"),
        r#"
{
  "name": "@evalops/orient-web",
  "scripts": {
    "test": "vitest run",
    "typecheck": "tsc --noEmit",
    "build:prod": "vite build"
  },
  "dependencies": {
    "react": "latest"
  }
}
"#,
    );
    write(
        &repo.path().join("compact/package.json"),
        r#"
{
  "name": "@evalops/compact",
  "scripts": {"compact-build": "vite build", "compact:test": "vitest"}
}
"#,
    );

    let live = RepoIndexer::new(repo.path()).build().unwrap();
    let live_package = &live.find_symbol("@evalops/orient-web", 10)[0];
    assert_symbol(live_package, "package.json", "package");
    assert_eq!(live_package.line, 3);
    assert_symbol(
        &live.find_symbol("typecheck", 10)[0],
        "package.json",
        "script",
    );
    assert_symbol(
        &live.find_symbol("build:prod", 10)[0],
        "package.json",
        "script",
    );
    let live_compact = &live.find_symbol("compact-build", 10)[0];
    assert_symbol(live_compact, "compact/package.json", "script");
    assert_eq!(live_compact.line, 4);

    let fallback = search_repo_fast_filtered(
        repo.path(),
        "kind:script symbol:typecheck",
        5,
        &Default::default(),
    )
    .unwrap();
    assert_eq!(fallback[0].path, "package.json");
    assert!(fallback[0].reason.contains("symbol:typecheck"));

    let package_fallback = search_repo_fast_filtered(
        repo.path(),
        "package:@evalops/orient-web",
        5,
        &Default::default(),
    )
    .unwrap();
    assert_eq!(package_fallback[0].path, "package.json");
    assert!(
        package_fallback[0]
            .reason
            .contains("symbol:@evalops/orient-web")
    );

    let indexed = FastIndex::build(repo.path()).unwrap();
    let indexed_package = &indexed.find_symbol("@evalops/orient-web", 10)[0];
    assert_symbol(indexed_package, "package.json", "package");
    assert_eq!(indexed_package.line, 3);
    assert_symbol(
        &indexed.find_symbol("build:prod", 10)[0],
        "package.json",
        "script",
    );
    let indexed_compact_package = &indexed.find_symbol("@evalops/compact", 10)[0];
    assert_symbol(indexed_compact_package, "compact/package.json", "package");
    assert_eq!(indexed_compact_package.line, 3);
    let indexed_compact = &indexed.find_symbol("compact-build", 10)[0];
    assert_symbol(indexed_compact, "compact/package.json", "script");
    assert_eq!(indexed_compact.line, 4);
    let indexed_results = indexed
        .search_filtered("script:build:prod", 5, &Default::default())
        .unwrap();
    assert_eq!(indexed_results[0].path, "package.json");
    assert!(indexed_results[0].reason.contains("symbol:build:prod"));
}

#[test]
fn generic_extractor_indexes_traits_types_and_exported_symbols() {
    let repo = tempfile::tempdir().unwrap();
    write(
        &repo.path().join("src/lib.rs"),
        r#"
pub trait SearchBackend {
    fn search(&self);
}
"#,
    );
    write(
        &repo.path().join("types.ts"),
        r#"
export type SearchResult = { path: string };
export interface SearchClient {}
export const useSearch = () => null;
"#,
    );

    let live = RepoIndexer::new(repo.path()).build().unwrap();
    assert_symbol(
        &live.find_symbol("SearchBackend", 10)[0],
        "src/lib.rs",
        "trait",
    );
    assert_symbol(&live.find_symbol("SearchResult", 10)[0], "types.ts", "type");
    assert_symbol(
        &live.find_symbol("SearchClient", 10)[0],
        "types.ts",
        "interface",
    );
    assert_symbol(
        &live.find_symbol("useSearch", 10)[0],
        "types.ts",
        "function",
    );

    let fallback =
        search_repo_fast_filtered(repo.path(), "symbol:SearchBackend", 5, &Default::default())
            .unwrap();
    assert_eq!(fallback[0].path, "src/lib.rs");
    assert!(fallback[0].reason.contains("symbol:SearchBackend"));

    let indexed = FastIndex::build(repo.path()).unwrap();
    assert_symbol(
        &indexed.find_symbol("SearchResult", 10)[0],
        "types.ts",
        "type",
    );
    let indexed_results = indexed
        .search_filtered("symbol:useSearch", 5, &Default::default())
        .unwrap();
    assert_eq!(indexed_results[0].path, "types.ts");
    assert!(indexed_results[0].reason.contains("symbol:useSearch"));
}

#[test]
fn prose_files_do_not_pollute_symbol_indexes_or_repo_maps() {
    let repo = tempfile::tempdir().unwrap();
    write(
        &repo.path().join("src/lib.rs"),
        "pub struct RealSessionManager;\npub fn issue_token() {}\n",
    );
    write(
        &repo.path().join("README.md"),
        "# Benchmark\n\nThis paragraph mentions class FakeMarkdownSymbol and export type FakeType.\n",
    );
    write(
        &repo.path().join("docs/guide.md"),
        "## Usage\n\nUse class FakeGuideSymbol when explaining examples, not as code.\n",
    );
    write(&repo.path().join("config.yaml"), "class: FakeYamlSymbol\n");

    let live = RepoIndexer::new(repo.path()).build().unwrap();
    assert_symbol(
        &live.find_symbol("RealSessionManager", 10)[0],
        "src/lib.rs",
        "struct",
    );
    assert!(live.find_symbol("FakeMarkdownSymbol", 10).is_empty());
    assert!(live.find_symbol("FakeGuideSymbol", 10).is_empty());
    assert!(live.find_symbol("FakeYamlSymbol", 10).is_empty());
    assert!(
        live.repo_map(10, 10)
            .top_symbols
            .iter()
            .all(|symbol| !symbol.path.ends_with(".md") && !symbol.path.ends_with(".yaml"))
    );

    let indexed = FastIndex::build(repo.path()).unwrap();
    assert_symbol(
        &indexed.find_symbol("RealSessionManager", 10)[0],
        "src/lib.rs",
        "struct",
    );
    assert!(indexed.find_symbol("FakeMarkdownSymbol", 10).is_empty());
    assert!(indexed.find_symbol("FakeGuideSymbol", 10).is_empty());
    assert!(indexed.find_symbol("FakeYamlSymbol", 10).is_empty());
    assert!(
        indexed
            .repo_map(10, 10)
            .top_symbols
            .iter()
            .all(|symbol| !symbol.path.ends_with(".md") && !symbol.path.ends_with(".yaml"))
    );
}

#[test]
fn repo_maps_diversify_top_symbols_across_files() {
    let repo = tempfile::tempdir().unwrap();
    write(
        &repo.path().join("src/alpha.rs"),
        "pub struct AlphaOne;\npub struct AlphaTwo;\npub struct AlphaThree;\npub struct AlphaFour;\n",
    );
    write(
        &repo.path().join("src/beta.rs"),
        "pub struct BetaOne;\npub struct BetaTwo;\n",
    );
    write(&repo.path().join("src/gamma.rs"), "pub struct GammaOne;\n");

    let live = RepoIndexer::new(repo.path()).build().unwrap();
    let live_symbols = live.repo_map(5, 5).top_symbols;
    assert!(
        live_symbols
            .iter()
            .any(|symbol| symbol.path == "src/beta.rs")
    );
    assert!(
        live_symbols
            .iter()
            .any(|symbol| symbol.path == "src/gamma.rs")
    );
    assert_eq!(live_symbols[0].name, "AlphaOne");
    assert!(
        live_symbols
            .iter()
            .filter(|symbol| symbol.path == "src/alpha.rs")
            .count()
            <= 2
    );

    let indexed = FastIndex::build(repo.path()).unwrap();
    let indexed_symbols = indexed.repo_map(5, 5).top_symbols;
    assert!(
        indexed_symbols
            .iter()
            .any(|symbol| symbol.path == "src/beta.rs")
    );
    assert!(
        indexed_symbols
            .iter()
            .any(|symbol| symbol.path == "src/gamma.rs")
    );
    assert_eq!(indexed_symbols[0].name, "AlphaOne");
    assert!(
        indexed_symbols
            .iter()
            .filter(|symbol| symbol.path == "src/alpha.rs")
            .count()
            <= 2
    );
}

#[test]
fn repo_briefs_keep_import_hints_compact_without_breaking_import_filters() {
    let repo = tempfile::tempdir().unwrap();
    let bulk_imports = (0..40)
        .map(|index| format!("use alpha::Module{index};\n"))
        .collect::<String>();
    write(&repo.path().join("src/bulk.rs"), &bulk_imports);
    write(
        &repo.path().join("src/other.rs"),
        "use beta::Client;\nuse gamma::Config;\npub fn call() {}\n",
    );

    let live = RepoIndexer::new(repo.path()).build().unwrap();
    let live_brief = live.repo_brief();
    assert_eq!(live_brief.import_hints.len(), 32);
    let full_live_brief = live.repo_brief_with_detail(RepoMapDetail::Full);
    assert_eq!(full_live_brief.import_hints.len(), 42);
    assert!(
        live_brief
            .import_hints
            .iter()
            .any(|hint| hint.source == "src/other.rs" && hint.module == "beta::Client"),
        "{:?}",
        live_brief.import_hints
    );
    let live_filtered = search_repo_fast_filtered(
        repo.path(),
        "import:Module39",
        10,
        &SearchFilters::default(),
    )
    .unwrap();
    assert!(
        live_filtered
            .iter()
            .any(|result| result.path == "src/bulk.rs"),
        "{live_filtered:?}"
    );

    let indexed = FastIndex::build(repo.path()).unwrap();
    let indexed_map = indexed.repo_map(10, 10);
    assert_eq!(indexed_map.manifest_files, indexed_map.brief.manifest_files);
    assert_eq!(
        indexed_map.important_files,
        indexed_map.brief.important_files
    );
    assert_eq!(indexed_map.known_commands, indexed_map.brief.known_commands);
    assert_eq!(indexed_map.command_hints, indexed_map.brief.command_hints);
    assert_eq!(
        indexed_map.dependency_hints,
        indexed_map.brief.dependency_hints
    );
    assert_eq!(indexed_map.import_hints, indexed_map.brief.import_hints);
    assert_eq!(indexed_map.summary.status, "mapped");
    assert_eq!(indexed_map.summary.file_count, indexed_map.brief.file_count);
    assert_eq!(
        indexed_map.summary.entrypoint_count,
        indexed_map.entrypoints.len()
    );
    assert_eq!(
        indexed_map.summary.manifest_count,
        indexed_map.manifest_files.len()
    );
    assert_eq!(
        indexed_map.summary.important_file_count,
        indexed_map.important_files.len()
    );
    assert_eq!(
        indexed_map.summary.test_file_count,
        indexed_map.test_files.len()
    );
    assert_eq!(
        indexed_map.summary.top_symbol_count,
        indexed_map.top_symbols.len()
    );
    assert_eq!(
        indexed_map.summary.command_count,
        indexed_map.known_commands.len()
    );
    let indexed_brief = indexed_map.brief;
    assert_eq!(indexed_brief.import_hints.len(), 32);
    let full_indexed_brief = indexed
        .repo_map_with_detail(10, 10, RepoMapDetail::Full)
        .brief;
    assert_eq!(full_indexed_brief.import_hints.len(), 42);
    assert!(
        indexed_brief
            .import_hints
            .iter()
            .any(|hint| hint.source == "src/other.rs" && hint.module == "beta::Client"),
        "{:?}",
        indexed_brief.import_hints
    );
    let indexed_filtered = indexed
        .search_filtered("import:Module39", 10, &SearchFilters::default())
        .unwrap();
    assert!(
        indexed_filtered
            .iter()
            .any(|result| result.path == "src/bulk.rs"),
        "{indexed_filtered:?}"
    );
}

#[test]
fn pytest_commands_search_target_test_files() {
    let repo = tempfile::tempdir().unwrap();
    write(
        &repo.path().join("tests/test_auth.py"),
        r#"
def test_login():
    assert True
"#,
    );
    write(
        &repo.path().join("src/auth.py"),
        r#"
def pytest_login_helper():
    return True
"#,
    );

    let fallback = search_repo_fast_filtered(
        repo.path(),
        "pytest tests/test_auth.py::test_login -q",
        5,
        &Default::default(),
    )
    .unwrap();
    assert_eq!(fallback[0].path, "tests/test_auth.py");

    let indexed = FastIndex::build(repo.path()).unwrap();
    let indexed_results = indexed
        .search_filtered(
            "python -m pytest tests/test_auth.py::test_login",
            5,
            &Default::default(),
        )
        .unwrap();
    assert_eq!(indexed_results[0].path, "tests/test_auth.py");
}

#[test]
fn cargo_test_commands_search_target_test_functions() {
    let repo = tempfile::tempdir().unwrap();
    write(
        &repo.path().join("tests/parser_rs.rs"),
        r#"
#[test]
fn parser_accepts_locations() {
    assert!(true);
}
"#,
    );
    write(
        &repo.path().join("src/lib.rs"),
        r#"
pub fn cargo_test_helper() {}
"#,
    );

    let fallback = search_repo_fast_filtered(
        repo.path(),
        "cargo test parser_accepts_locations",
        5,
        &Default::default(),
    )
    .unwrap();
    assert_eq!(fallback[0].path, "tests/parser_rs.rs");
    assert!(
        fallback[0]
            .reason
            .contains("symbol:parser_accepts_locations")
    );

    let indexed = FastIndex::build(repo.path()).unwrap();
    let indexed_results = indexed
        .search_filtered(
            "cargo test parser_rs::parser_accepts_locations",
            5,
            &Default::default(),
        )
        .unwrap();
    assert_eq!(indexed_results[0].path, "tests/parser_rs.rs");
    assert!(
        indexed_results[0]
            .reason
            .contains("symbol:parser_accepts_locations")
    );
}

#[test]
fn fallback_line_range_tracks_displayed_contiguous_snippet_block() {
    let repo = tempfile::tempdir().unwrap();
    let mut source = String::from("alpha first hit\n");
    for line in 2..100 {
        source.push_str(&format!("filler {line}\n"));
    }
    source.push_str("omega second hit\n");
    write(&repo.path().join("src/lib.rs"), &source);

    let results =
        search_repo_fast_filtered(repo.path(), "mode:any alpha omega", 5, &Default::default())
            .unwrap();

    assert_eq!(results.len(), 1);
    assert!(results[0].snippet.contains("1: alpha first hit"));
    assert!(results[0].snippet.contains("100: omega second hit"));
    assert_eq!(results[0].line_range.as_ref().unwrap().start_line, 1);
    assert_eq!(results[0].line_range.as_ref().unwrap().end_line, 1);
    assert_eq!(results[0].match_lines, vec![1, 100]);
}

#[test]
fn symbol_scoped_ranges_anchor_live_and_indexed_reads_on_nearest_definition() {
    let repo = tempfile::tempdir().unwrap();
    let source = r#"#[inline]
/// Issues a token.
pub fn issue_token() {
    let token = 42;
}

pub fn verify_token() {}
"#;
    write(&repo.path().join("src/auth.rs"), &source);

    let exact = read_file_range(repo.path(), "src/auth.rs", 4, 3).unwrap();
    assert_eq!(exact.start_line, 4);
    assert_eq!(exact.symbol, None);
    assert_eq!(exact.summary.scope, RangeScope::Exact);
    assert!(!exact.summary.truncated);
    let exact_summary = serde_json::to_value(&exact.summary).unwrap();
    assert!(exact_summary.get("scope").is_none());
    assert!(exact_summary.get("truncated").is_none());

    let scoped =
        read_file_range_scoped(repo.path(), "src/auth.rs", 4, 2, RangeScope::Symbol).unwrap();
    assert_eq!(scoped.symbol.as_ref().unwrap().name, "issue_token");
    assert_eq!(scoped.summary.scope, RangeScope::Symbol);
    assert!(!scoped.summary.truncated);
    assert_eq!(scoped.start_line, 1);
    assert_eq!(scoped.end_line, 5);
    assert!(scoped.text.contains("#[inline]"));
    assert!(scoped.text.contains("/// Issues a token."));
    assert!(scoped.text.contains("pub fn issue_token() {"));
    assert!(scoped.text.contains("let token = 42;"));
    assert!(!scoped.text.contains("verify_token"));

    let indexed = FastIndex::build(repo.path()).unwrap();
    let indexed_range = indexed
        .read_range_scoped("src/auth.rs", 4, 2, RangeScope::Symbol)
        .unwrap();
    assert_eq!(indexed_range.start_line, scoped.start_line);
    assert_eq!(indexed_range.end_line, scoped.end_line);
    assert_eq!(indexed_range.summary.scope, scoped.summary.scope);
    assert_eq!(indexed_range.summary.truncated, scoped.summary.truncated);
    assert_eq!(
        indexed_range.symbol.as_ref().unwrap().name,
        scoped.symbol.as_ref().unwrap().name
    );
}

#[test]
fn symbol_scoped_ranges_use_definition_extent_for_python_containers_and_methods() {
    let repo = tempfile::tempdir().unwrap();
    write(
        &repo.path().join("src/auth.py"),
        r#"class SessionManager:
    def issue_token(self):
        return "issue"

    def verify_token(self):
        return True

def outside():
    return False
"#,
    );

    let class_range =
        read_file_range_scoped(repo.path(), "src/auth.py", 1, 2, RangeScope::Symbol).unwrap();
    assert_eq!(class_range.symbol.as_ref().unwrap().name, "SessionManager");
    assert_eq!(class_range.start_line, 1);
    assert_eq!(class_range.end_line, 6);
    assert!(class_range.text.contains("def issue_token"));
    assert!(class_range.text.contains("def verify_token"));
    assert!(!class_range.text.contains("def outside"));

    let method_range =
        read_file_range_scoped(repo.path(), "src/auth.py", 3, 2, RangeScope::Symbol).unwrap();
    assert_eq!(method_range.symbol.as_ref().unwrap().name, "issue_token");
    assert_eq!(method_range.start_line, 2);
    assert_eq!(method_range.end_line, 3);
    assert!(!method_range.text.contains("verify_token"));

    let indexed = FastIndex::build(repo.path()).unwrap();
    let indexed_class = indexed
        .read_range_scoped("src/auth.py", 1, 2, RangeScope::Symbol)
        .unwrap();
    assert_eq!(indexed_class.start_line, class_range.start_line);
    assert_eq!(indexed_class.end_line, class_range.end_line);

    let indexed_method = indexed
        .read_range_scoped("src/auth.py", 3, 2, RangeScope::Symbol)
        .unwrap();
    assert_eq!(indexed_method.start_line, method_range.start_line);
    assert_eq!(indexed_method.end_line, method_range.end_line);
}

#[test]
fn symbol_scoped_ranges_clamp_large_definitions_without_next_symbol() {
    let repo = tempfile::tempdir().unwrap();
    let mut source = String::from("pub fn huge_definition() {\n");
    for line in 0..(MAX_READ_RANGE_LINES + 50) {
        source.push_str(&format!("    let value_{line} = {line};\n"));
    }
    source.push_str("}\n");
    write(&repo.path().join("src/huge.rs"), &source);

    let scoped =
        read_file_range_scoped(repo.path(), "src/huge.rs", 10, 2, RangeScope::Symbol).unwrap();
    assert_eq!(scoped.start_line, 1);
    assert_eq!(scoped.summary.scope, RangeScope::Symbol);
    assert!(scoped.summary.truncated);
    let scoped_summary = serde_json::to_value(&scoped.summary).unwrap();
    assert_eq!(scoped_summary["scope"], "symbol");
    assert_eq!(scoped_summary["truncated"], true);
    assert_eq!(
        scoped.end_line - scoped.start_line + 1,
        MAX_READ_RANGE_LINES
    );

    let indexed = FastIndex::build(repo.path()).unwrap();
    let exact = read_file_range(repo.path(), "src/huge.rs", 1, MAX_READ_RANGE_LINES + 50).unwrap();
    assert_eq!(exact.summary.scope, RangeScope::Exact);
    assert!(exact.summary.truncated);
    let exact_summary = serde_json::to_value(&exact.summary).unwrap();
    assert!(exact_summary.get("scope").is_none());
    assert_eq!(exact_summary["truncated"], true);

    let indexed_exact = indexed
        .read_range("src/huge.rs", 1, MAX_READ_RANGE_LINES + 50)
        .unwrap();
    assert_eq!(indexed_exact.summary.scope, RangeScope::Exact);
    assert!(indexed_exact.summary.truncated);

    let indexed_range = indexed
        .read_range_scoped("src/huge.rs", 10, 2, RangeScope::Symbol)
        .unwrap();
    assert_eq!(indexed_range.start_line, scoped.start_line);
    assert_eq!(indexed_range.end_line, scoped.end_line);
    assert_eq!(indexed_range.summary.scope, RangeScope::Symbol);
    assert!(indexed_range.summary.truncated);
}

#[test]
fn symbol_scoped_ranges_fall_back_to_exact_without_summary_scope_when_no_symbol_exists() {
    let repo = tempfile::tempdir().unwrap();
    write(
        &repo.path().join("config/settings.json"),
        "{\"issue_token\": true}\n",
    );

    let fallback = read_file_range_scoped(
        repo.path(),
        "config/settings.json",
        1,
        1,
        RangeScope::Symbol,
    )
    .unwrap();
    assert_eq!(fallback.start_line, 1);
    assert_eq!(fallback.end_line, 1);
    assert_eq!(fallback.summary.scope, RangeScope::Exact);
    assert!(!fallback.summary.has_symbol);
    assert!(!fallback.summary.truncated);
    let summary = serde_json::to_value(&fallback.summary).unwrap();
    assert!(summary.get("scope").is_none());
    assert!(summary.get("truncated").is_none());
}

fn assert_symbol(symbol: &orient::repo_index::Symbol, path: &str, kind: &str) {
    assert_eq!(symbol.path, path);
    assert_eq!(symbol.kind, kind);
}

#[test]
fn read_file_range_rejects_paths_outside_repo() {
    let repo = tempfile::tempdir().unwrap();
    let outside = tempfile::tempdir().unwrap();
    write(
        &repo.path().join("src/auth.rs"),
        "pub fn issue_token() {}\n",
    );
    write(
        &outside.path().join("outside.rs"),
        "pub fn outside_repo() {}\n",
    );

    let range = read_file_range(repo.path(), "src/auth.rs", 1, 10).unwrap();
    assert_eq!(range.start_line, 1);
    assert!(range.text.contains("issue_token"));
    let backslash_range = read_file_range(repo.path(), "src\\auth.rs", 1, 10).unwrap();
    assert_eq!(backslash_range.path, "src/auth.rs");
    assert!(backslash_range.text.contains("issue_token"));
    let absolute_auth_path = repo.path().join("src/auth.rs");
    let absolute_range =
        read_file_range(repo.path(), absolute_auth_path.to_str().unwrap(), 1, 10).unwrap();
    assert_eq!(absolute_range.path, "src/auth.rs");
    assert!(absolute_range.text.contains("issue_token"));
    let absolute_search = search_repo_fast_filtered(
        repo.path(),
        &format!("{}:1:1", absolute_auth_path.display()),
        5,
        &SearchFilters::default(),
    )
    .unwrap();
    assert_eq!(absolute_search[0].path, "src/auth.rs");

    let long_text = (1..=MAX_READ_RANGE_LINES + 10)
        .map(|line| format!("line_{line}\n"))
        .collect::<String>();
    write(&repo.path().join("src/long.rs"), &long_text);
    let capped = read_file_range(repo.path(), "src/long.rs", 1, MAX_READ_RANGE_LINES + 10).unwrap();
    assert_eq!(capped.start_line, 1);
    assert_eq!(capped.end_line, MAX_READ_RANGE_LINES);
    assert!(!capped.text.contains(&format!(
        "{}: line_{}",
        MAX_READ_RANGE_LINES + 1,
        MAX_READ_RANGE_LINES + 1
    )));

    let error = read_file_range(repo.path(), "../outside.rs", 1, 1)
        .unwrap_err()
        .to_string();
    assert!(error.contains("repo-relative"));
    let backslash_parent_error = read_file_range(repo.path(), "src\\..\\auth.rs", 1, 1)
        .unwrap_err()
        .to_string();
    assert!(backslash_parent_error.contains("repo-relative"));

    let absolute_error = read_file_range(
        repo.path(),
        outside.path().join("outside.rs").to_str().unwrap(),
        1,
        1,
    )
    .unwrap_err()
    .to_string();
    assert!(absolute_error.contains("inside repository"));
}

#[cfg(unix)]
#[test]
fn read_file_range_rejects_symlink_escape() {
    let repo = tempfile::tempdir().unwrap();
    let outside = tempfile::tempdir().unwrap();
    write(
        &outside.path().join("outside.rs"),
        "pub fn outside_repo() {}\n",
    );
    std::os::unix::fs::symlink(
        outside.path().join("outside.rs"),
        repo.path().join("linked.rs"),
    )
    .unwrap();

    let error = read_file_range(repo.path(), "linked.rs", 1, 1)
        .unwrap_err()
        .to_string();
    assert!(error.contains("inside repository"));
}

#[cfg(unix)]
#[test]
fn repo_and_persistent_indexes_ignore_symlinked_files() {
    let repo = tempfile::tempdir().unwrap();
    let outside = tempfile::tempdir().unwrap();
    write(
        &repo.path().join("src/lib.rs"),
        "pub fn visible_symbol() {}\n",
    );
    write(
        &outside.path().join("secrets.rs"),
        "pub fn leaked_secret() {}\n",
    );
    std::os::unix::fs::symlink(
        outside.path().join("secrets.rs"),
        repo.path().join("src/leaked.rs"),
    )
    .unwrap();

    let live = RepoIndexer::new(repo.path()).build().unwrap();
    assert!(live.search_code("visible symbol", 10)[0].path == "src/lib.rs");
    assert!(live.search_code("leaked secret", 10).is_empty());

    let persistent = FastIndex::build(repo.path()).unwrap();
    assert!(persistent.search("visible symbol", 10).unwrap()[0].path == "src/lib.rs");
    assert!(persistent.search("leaked secret", 10).unwrap().is_empty());
    assert!(
        !persistent
            .files
            .iter()
            .any(|file| file.path == "src/leaked.rs")
    );
}
