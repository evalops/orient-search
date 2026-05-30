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
        "[project]\nname='sample'\ndependencies=['fastapi>=0.100', 'pydantic']\n",
    );
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
    assert!(brief.known_commands.contains(&"pytest".to_string()));
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
        hint.command == "pytest" && hint.kind == "test" && hint.source == "pyproject.toml"
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
            related.source_path == "src/auth.py" && related.path == "tests/test_auth.py"
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
    let mut source = String::new();
    for line in 1..=25 {
        source.push_str(&format!("// filler {line}\n"));
    }
    source.push_str("pub fn issue_token() {\n");
    source.push_str("    let token = 42;\n");
    source.push_str("}\n");
    source.push_str("pub fn verify_token() {}\n");
    write(&repo.path().join("src/auth.rs"), &source);

    let exact = read_file_range(repo.path(), "src/auth.rs", 27, 3).unwrap();
    assert_eq!(exact.start_line, 27);
    assert_eq!(exact.symbol, None);

    let scoped =
        read_file_range_scoped(repo.path(), "src/auth.rs", 27, 2, RangeScope::Symbol).unwrap();
    assert_eq!(scoped.symbol.as_ref().unwrap().name, "issue_token");
    let symbol_line = scoped.symbol.as_ref().unwrap().line;
    assert_eq!(scoped.start_line, symbol_line.saturating_sub(20).max(1));
    assert_eq!(scoped.end_line, symbol_line + 1);
    assert!(scoped.text.contains("pub fn issue_token() {"));
    assert!(scoped.text.contains("let token = 42;"));
    assert!(!scoped.text.contains("verify_token"));

    let indexed = FastIndex::build(repo.path()).unwrap();
    let indexed_range = indexed
        .read_range_scoped("src/auth.rs", 27, 2, RangeScope::Symbol)
        .unwrap();
    assert_eq!(indexed_range.start_line, scoped.start_line);
    assert_eq!(indexed_range.end_line, scoped.end_line);
    assert_eq!(
        indexed_range.symbol.as_ref().unwrap().name,
        scoped.symbol.as_ref().unwrap().name
    );
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
