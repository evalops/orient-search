use std::fs;
use std::io::{BufRead, BufReader, Read, Write};
use std::net::TcpStream;
#[cfg(unix)]
use std::os::unix::net::UnixStream;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::sync::Arc;
use std::thread;
use std::time::Duration;

use orient::fast_index::FastIndex;
use orient::repo_index::{
    DEFAULT_REPO_MAP_READ_BATCH_RANGES, MAX_ATTACHED_CONTEXT_LINES, MAX_READ_RANGE_LINES,
    MAX_RESULT_READ_BATCH_RANGES, MAX_SEARCH_RESULTS,
};
use orient::server::{
    DEFAULT_MAX_CACHED_INDEXES, MAX_BATCH_QUERIES, MAX_BATCH_RANGES, MAX_BATCH_READ_LINES,
    ToolRequest, ToolRuntime, agent_guide, agent_instructions, mcp_dispatch_value,
    mcp_tool_manifest, serve_mcp_with_runtime, tool_manifest,
};
use orient::shards::{DEFAULT_MAX_SHARD_WORKERS, build_shards, refresh_shards, shard_status};

fn write(path: &Path, text: &str) {
    fs::create_dir_all(path.parent().unwrap()).unwrap();
    fs::write(path, text).unwrap();
}

fn git(repo: &Path, args: &[&str]) {
    let status = Command::new("git")
        .arg("-C")
        .arg(repo)
        .args(args)
        .status()
        .unwrap();
    assert!(status.success(), "git {:?} failed", args);
}

fn tcp_tool_request(addr: &str, request: serde_json::Value) -> String {
    let mut stream = TcpStream::connect(addr).unwrap();
    let mut reader = BufReader::new(stream.try_clone().unwrap());
    writeln!(stream, "{request}").unwrap();
    let mut response = String::new();
    reader.read_line(&mut response).unwrap();
    response
}

#[test]
fn tool_manifest_exposes_typed_defaults_and_input_schemas() {
    let manifest = tool_manifest();
    let tools = manifest.as_array().unwrap();
    let search = tools
        .iter()
        .find(|tool| tool["name"] == "search_code")
        .unwrap();
    let search_alias = tools.iter().find(|tool| tool["name"] == "search").unwrap();
    let search_auto = tools
        .iter()
        .find(|tool| tool["name"] == "search_auto")
        .unwrap();
    let search_auto_batch = tools
        .iter()
        .find(|tool| tool["name"] == "search_auto_batch")
        .unwrap();
    let discover = tools
        .iter()
        .find(|tool| tool["name"] == "discover_repos")
        .unwrap();
    let refresh_index = tools
        .iter()
        .find(|tool| tool["name"] == "refresh_index")
        .unwrap();
    let ensure_index = tools
        .iter()
        .find(|tool| tool["name"] == "ensure_index")
        .unwrap();
    let index_status = tools
        .iter()
        .find(|tool| tool["name"] == "index_status")
        .unwrap();
    let shard_status = tools
        .iter()
        .find(|tool| tool["name"] == "shard_status")
        .unwrap();
    let read_ranges = tools
        .iter()
        .find(|tool| tool["name"] == "read_ranges")
        .unwrap();
    let read_range = tools
        .iter()
        .find(|tool| tool["name"] == "read_range")
        .unwrap();
    let search_batch = tools
        .iter()
        .find(|tool| tool["name"] == "search_batch")
        .unwrap();
    let search_plan = tools
        .iter()
        .find(|tool| tool["name"] == "search_query_plan")
        .unwrap();
    let search_plan_alias = tools
        .iter()
        .find(|tool| tool["name"] == "search_plan")
        .unwrap();
    let search_plan_batch = tools
        .iter()
        .find(|tool| tool["name"] == "search_query_plan_batch")
        .unwrap();
    let search_plan_batch_alias = tools
        .iter()
        .find(|tool| tool["name"] == "search_plan_batch")
        .unwrap();
    let indexed_plan_batch = tools
        .iter()
        .find(|tool| tool["name"] == "indexed_query_plan_batch")
        .unwrap();
    let shard_plan_batch = tools
        .iter()
        .find(|tool| tool["name"] == "shard_query_plan_batch")
        .unwrap();
    let read_shard_range = tools
        .iter()
        .find(|tool| tool["name"] == "read_shard_range")
        .unwrap();
    let read_shard_ranges = tools
        .iter()
        .find(|tool| tool["name"] == "read_shard_ranges")
        .unwrap();
    let read_index_range = tools
        .iter()
        .find(|tool| tool["name"] == "read_index_range")
        .unwrap();
    let read_index_ranges = tools
        .iter()
        .find(|tool| tool["name"] == "read_index_ranges")
        .unwrap();
    let agent_guide_tool = tools
        .iter()
        .find(|tool| tool["name"] == "agent_guide")
        .unwrap();
    let agent_instructions_tool = tools
        .iter()
        .find(|tool| tool["name"] == "agent_instructions")
        .unwrap();
    let repo_map = tools
        .iter()
        .find(|tool| tool["name"] == "repo_map")
        .unwrap();
    let related_files = tools
        .iter()
        .find(|tool| tool["name"] == "related_files")
        .unwrap();
    let related_symbols = tools
        .iter()
        .find(|tool| tool["name"] == "related_symbols")
        .unwrap();
    let find_symbol = tools
        .iter()
        .find(|tool| tool["name"] == "find_symbol")
        .unwrap();
    let find_symbol_batch = tools
        .iter()
        .find(|tool| tool["name"] == "find_symbol_batch")
        .unwrap();

    assert_eq!(search["required"], serde_json::json!(["repo", "query"]));
    assert_eq!(search_auto["required"], serde_json::json!(["query"]));
    assert_eq!(
        search_auto_batch["required"],
        serde_json::json!(["queries"])
    );
    assert_eq!(
        search_auto["input_schema"]["properties"]["limit"]["default"],
        10
    );
    assert_eq!(
        search_auto_batch["input_schema"]["properties"]["queries"]["maxItems"],
        serde_json::json!(MAX_BATCH_QUERIES)
    );
    assert_eq!(
        search_auto["input_schema"]["properties"]["limit"]["maximum"],
        serde_json::json!(MAX_SEARCH_RESULTS)
    );
    assert_eq!(
        search_auto["input_schema"]["properties"]["diagnose"]["type"],
        "boolean"
    );
    assert_eq!(
        search_auto["input_schema"]["properties"]["diagnose"]["default"],
        false
    );
    assert_eq!(
        search_auto["input_schema"]["properties"]["cwd"]["type"],
        "string"
    );
    assert_eq!(
        search_alias["input_schema"]["properties"]["cwd"]["type"],
        "string"
    );
    assert_eq!(
        search_batch["input_schema"]["properties"]["cwd"]["type"],
        "string"
    );
    assert_eq!(
        repo_map["input_schema"]["properties"]["cwd"]["type"],
        "string"
    );
    assert_eq!(
        search_plan_alias["input_schema"]["properties"]["cwd"]["type"],
        "string"
    );
    assert_eq!(
        find_symbol["input_schema"]["properties"]["cwd"]["type"],
        "string"
    );
    assert_eq!(
        find_symbol["input_schema"]["properties"]["include_read_batch"]["type"],
        "boolean"
    );
    assert_eq!(
        search_auto_batch["input_schema"]["properties"]["diagnose"]["type"],
        "boolean"
    );
    assert_eq!(search["input_schema"]["properties"]["limit"]["default"], 10);
    assert_eq!(
        search["input_schema"]["properties"]["limit"]["maximum"],
        serde_json::json!(MAX_SEARCH_RESULTS)
    );
    assert_eq!(
        search["arguments"]
            .as_array()
            .unwrap()
            .iter()
            .find(|argument| argument["name"] == "limit")
            .unwrap()["maximum"],
        serde_json::json!(MAX_SEARCH_RESULTS)
    );
    assert_eq!(
        search["input_schema"]["properties"]["snippet"]["enum"],
        serde_json::json!(["short", "medium", "block", "symbol"])
    );
    assert_eq!(
        search["input_schema"]["properties"]["snippet_mode"]["enum"],
        serde_json::json!(["short", "medium", "block", "symbol"])
    );
    assert_eq!(
        search["input_schema"]["properties"]["snippet_mode"]["default"],
        "medium"
    );
    assert_eq!(
        search["input_schema"]["properties"]["snippet_mode"]["description"],
        "Alias for snippet."
    );
    assert_eq!(
        search["input_schema"]["properties"]["snippet-mode"]["enum"],
        serde_json::json!(["short", "medium", "block", "symbol"])
    );
    assert_eq!(
        search["input_schema"]["properties"]["snippet-mode"]["description"],
        "Alias for snippet."
    );
    assert_eq!(
        search["input_schema"]["properties"]["lang"]["description"],
        "Alias for language."
    );
    assert_eq!(
        search["input_schema"]["properties"]["file-name"]["description"],
        "Alias for file."
    );
    assert_eq!(
        search["input_schema"]["properties"]["exclude-dir"]["oneOf"][1]["items"]["type"],
        "string"
    );
    assert_eq!(
        search["input_schema"]["properties"]["exclude-symbol-kind"]["oneOf"][1]["items"]["type"],
        "string"
    );
    assert_eq!(
        search["input_schema"]["properties"]["ext"]["description"],
        "Alias for extension."
    );
    assert_eq!(
        search["input_schema"]["properties"]["kind"]["description"],
        "Alias for symbol_kind."
    );
    assert_eq!(
        search["input_schema"]["properties"]["dep"]["description"],
        "Alias for dependency."
    );
    assert_eq!(
        search["input_schema"]["properties"]["module"]["description"],
        "Alias for import."
    );
    assert_eq!(
        search["input_schema"]["properties"]["code"]["type"],
        "boolean"
    );
    assert_eq!(
        search["input_schema"]["properties"]["code"]["description"],
        "When true, include only implementation source-code paths; when false, exclude implementation source-code paths."
    );
    assert_eq!(
        search["input_schema"]["properties"]["line"]["type"],
        "integer"
    );
    assert_eq!(
        search["input_schema"]["properties"]["target-line"]["type"],
        "integer"
    );
    assert_eq!(
        search["input_schema"]["properties"]["target-line"]["description"],
        "Alias for target_line."
    );
    assert_eq!(
        search["input_schema"]["properties"]["context-lines"]["maximum"],
        serde_json::json!(MAX_ATTACHED_CONTEXT_LINES)
    );
    assert_eq!(
        search["input_schema"]["properties"]["require-all"]["type"],
        "boolean"
    );
    assert_eq!(
        repo_map["input_schema"]["properties"]["read-limit"]["maximum"],
        serde_json::json!(MAX_RESULT_READ_BATCH_RANGES)
    );
    assert_eq!(
        related_files["input_schema"]["properties"]["include-read-batch"]["type"],
        "boolean"
    );
    assert_eq!(
        read_shard_range["input_schema"]["properties"]["index-dir"]["type"],
        "string"
    );
    assert_eq!(
        read_shard_range["input_schema"]["properties"]["line"]["type"],
        "integer"
    );
    assert_eq!(
        read_shard_range["input_schema"]["properties"]["target-line"]["description"],
        "Alias for target_line."
    );
    assert_eq!(
        read_shard_range["input_schema"]["properties"]["end"]["description"],
        "Alias for end_line."
    );
    assert_eq!(
        read_index_range["input_schema"]["properties"]["range"]["oneOf"][0]["properties"]["line-count"]
            ["maximum"],
        serde_json::json!(MAX_READ_RANGE_LINES)
    );
    assert_eq!(
        read_index_range["input_schema"]["properties"]["range"]["oneOf"][0]["properties"]["end"]["description"],
        "Alias for end_line."
    );
    assert!(
        search["input_schema"]["properties"]["line"]["description"]
            .as_str()
            .unwrap()
            .contains("anchor snippets")
    );
    assert!(
        search["input_schema"]["properties"]["generated"]["description"]
            .as_str()
            .unwrap()
            .contains("demoted in ranking")
    );
    assert_eq!(
        search["input_schema"]["properties"]["code"].get("default"),
        None
    );
    assert_eq!(
        search["input_schema"]["properties"]["exclude_lang"]["oneOf"][1]["items"]["type"],
        "string"
    );
    assert_eq!(
        search["input_schema"]["properties"]["exclude_path"]["oneOf"][1]["items"]["type"],
        "string"
    );
    assert_eq!(
        search["input_schema"]["properties"]["import"]["type"],
        "string"
    );
    assert_eq!(
        search["input_schema"]["properties"]["exclude_import"]["oneOf"][1]["items"]["type"],
        "string"
    );
    assert_eq!(
        search["input_schema"]["properties"]["exclude_content"]["oneOf"][1]["items"]["type"],
        "string"
    );
    assert_eq!(
        search["arguments"]
            .as_array()
            .unwrap()
            .iter()
            .find(|argument| argument["name"] == "exclude_symbol_kind")
            .unwrap()["type"],
        "string|string[]"
    );
    assert_eq!(
        search["input_schema"]["properties"]["exclude_symbol_kind"]["oneOf"][1]["items"]["type"],
        "string"
    );
    assert_eq!(
        search["input_schema"]["properties"]["any_terms"]["type"],
        "boolean"
    );
    assert_eq!(
        search["input_schema"]["properties"]["any_terms"]["default"],
        false
    );
    let indexed_search = tools
        .iter()
        .find(|tool| tool["name"] == "indexed_search_code")
        .unwrap();
    let indexed_search_alias = tools
        .iter()
        .find(|tool| tool["name"] == "indexed_search")
        .unwrap();
    let index_plan_alias = tools
        .iter()
        .find(|tool| tool["name"] == "index_plan")
        .unwrap();
    let shard_plan_alias = tools
        .iter()
        .find(|tool| tool["name"] == "shard_plan")
        .unwrap();
    assert_eq!(search_alias["required"], serde_json::json!(["query"]));
    assert_eq!(
        search_alias["input_schema"]["properties"]["index"]["type"],
        "string"
    );
    assert_eq!(
        search_alias["input_schema"]["properties"]["index_dir"]["type"],
        "string"
    );
    assert_eq!(
        search_alias["input_schema"]["properties"]["refresh_if_stale"]["default"],
        false
    );
    assert_eq!(
        search_alias["input_schema"]["properties"]["limit"]["maximum"],
        serde_json::json!(MAX_SEARCH_RESULTS)
    );
    assert_eq!(
        indexed_search["input_schema"]["properties"]["refresh_if_stale"]["default"],
        false
    );
    assert_eq!(
        indexed_search["daemon_default"]["argument"],
        serde_json::json!("index")
    );
    assert_eq!(
        indexed_search["daemon_default"]["source"],
        serde_json::json!("single_warmed_index")
    );
    assert_eq!(
        indexed_search["input_schema"]["properties"]["index"]["x-daemon-default"],
        serde_json::json!("single_warmed_index")
    );
    assert_eq!(
        indexed_search["arguments"][0]["daemon_default"],
        serde_json::json!("single_warmed_index")
    );
    assert_eq!(
        indexed_search_alias["daemon_default"]["source"],
        serde_json::json!("single_warmed_index")
    );
    assert_eq!(
        indexed_search_alias["input_schema"]["properties"]["limit"]["maximum"],
        serde_json::json!(MAX_SEARCH_RESULTS)
    );
    assert_eq!(
        index_plan_alias["daemon_default"]["source"],
        serde_json::json!("single_warmed_index")
    );
    assert_eq!(
        shard_plan_alias["daemon_default"]["source"],
        serde_json::json!("single_registered_shard_dir")
    );
    assert!(search.get("daemon_default").is_none());
    assert_eq!(search_batch["required"], serde_json::json!(["queries"]));
    assert_eq!(
        search_batch["input_schema"]["properties"]["index"]["type"],
        "string"
    );
    assert_eq!(
        search_batch["input_schema"]["properties"]["index_dir"]["type"],
        "string"
    );
    assert_eq!(
        search_batch["input_schema"]["properties"]["queries"]["items"]["type"],
        "string"
    );
    assert_eq!(
        search_batch["input_schema"]["properties"]["queries"]["maxItems"],
        serde_json::json!(MAX_BATCH_QUERIES)
    );
    assert_eq!(
        search_batch["input_schema"]["properties"]["queries"]["minItems"],
        serde_json::json!(1)
    );
    assert_eq!(
        search_batch["arguments"]
            .as_array()
            .unwrap()
            .iter()
            .find(|argument| argument["name"] == "queries")
            .unwrap()["max_items"],
        serde_json::json!(MAX_BATCH_QUERIES)
    );
    assert_eq!(
        search_plan["required"],
        serde_json::json!(["repo", "query"])
    );
    assert!(
        search_plan["input_schema"]["properties"]
            .get("refresh_if_stale")
            .is_none()
    );
    assert_eq!(search_plan_alias["required"], serde_json::json!(["query"]));
    assert_eq!(
        search_plan_alias["input_schema"]["properties"]["index"]["type"],
        "string"
    );
    assert_eq!(
        search_plan_alias["input_schema"]["properties"]["index_dir"]["type"],
        "string"
    );
    assert_eq!(
        search_plan_alias["input_schema"]["properties"]["refresh_if_stale"]["default"],
        false
    );
    assert_eq!(
        search_plan_alias["input_schema"]["properties"]["summary"]["type"],
        "boolean"
    );
    assert_eq!(
        search_plan_batch["required"],
        serde_json::json!(["repo", "queries"])
    );
    assert_eq!(
        search_plan_batch_alias["required"],
        serde_json::json!(["queries"])
    );
    assert_eq!(
        search_plan_batch_alias["input_schema"]["properties"]["index"]["type"],
        "string"
    );
    assert_eq!(
        search_plan_batch_alias["input_schema"]["properties"]["index_dir"]["type"],
        "string"
    );
    assert_eq!(
        search_plan_batch_alias["input_schema"]["properties"]["summary"]["description"],
        "When true for query-plan tools, return only compact summaries, retry requests, and next_action instead of full nested plan payloads."
    );
    assert_eq!(
        search_plan_batch["input_schema"]["properties"]["queries"]["maxItems"],
        serde_json::json!(MAX_BATCH_QUERIES)
    );
    assert_eq!(
        indexed_plan_batch["required"],
        serde_json::json!(["index", "queries"])
    );
    assert_eq!(
        indexed_plan_batch["input_schema"]["properties"]["refresh_if_stale"]["default"],
        false
    );
    assert_eq!(
        indexed_plan_batch["input_schema"]["properties"]["summary"]["type"],
        "boolean"
    );
    assert_eq!(
        shard_plan_batch["required"],
        serde_json::json!(["index_dir", "queries"])
    );
    assert_eq!(
        shard_plan_batch["daemon_default"]["argument"],
        serde_json::json!("index_dir")
    );
    assert_eq!(
        shard_plan_batch["daemon_default"]["source"],
        serde_json::json!("single_registered_shard_dir")
    );
    assert_eq!(
        shard_status["input_schema"]["properties"]["index_dir"]["x-daemon-default"],
        serde_json::json!("single_registered_shard_dir")
    );
    assert_eq!(
        shard_status["input_schema"]["properties"]["cwd"]["type"],
        "string"
    );
    assert_eq!(
        shard_status["input_schema"]["properties"]["repo_filter"]["type"],
        "string"
    );
    assert_eq!(
        discover["input_schema"]["properties"]["limit"]["default"],
        500
    );
    assert!(
        discover["input_schema"]["properties"]["limit"]
            .get("maximum")
            .is_none()
    );
    assert_eq!(
        discover["input_schema"]["properties"]["git_metadata"]["type"],
        "boolean"
    );
    assert_eq!(
        discover["input_schema"]["properties"]["tracked_files"]["default"],
        false
    );
    assert_eq!(
        discover["input_schema"]["properties"]["family_limit"]["default"],
        0
    );
    assert_eq!(
        discover["input_schema"]["properties"]["nested_manifests"]["default"],
        false
    );
    assert_eq!(
        refresh_index["required"],
        serde_json::json!(["repo", "index"])
    );
    assert_eq!(
        ensure_index["required"],
        serde_json::json!(["repo", "index"])
    );
    assert_eq!(index_status["required"], serde_json::json!(["index"]));
    assert_eq!(shard_status["required"], serde_json::json!(["index_dir"]));
    assert_eq!(read_range["required"], serde_json::json!([]));
    assert_eq!(
        read_range["input_schema"]["properties"]["index"]["type"],
        "string"
    );
    assert_eq!(
        read_range["input_schema"]["properties"]["index_dir"]["type"],
        "string"
    );
    assert_eq!(
        read_range["input_schema"]["properties"]["cwd"]["type"],
        "string"
    );
    assert_eq!(
        read_range["input_schema"]["properties"]["scope"]["enum"],
        serde_json::json!(["exact", "symbol"])
    );
    assert_eq!(
        read_range["input_schema"]["properties"]["range"]["oneOf"][0]["properties"]["path"]["description"],
        "Result path or copied location for the selected target; use repo/index-relative paths for repo or index targets, and shard-prefixed or unique shard-relative paths for index_dir targets."
    );
    assert_eq!(
        read_range["input_schema"]["properties"]["range"]["oneOf"][1]["type"],
        "string"
    );
    assert_eq!(
        read_range["input_schema"]["properties"]["start_line"]["description"],
        "Alias for start when passing line_range-shaped data."
    );
    assert_eq!(
        read_range["input_schema"]["properties"]["line_count"]["maximum"],
        serde_json::json!(MAX_READ_RANGE_LINES)
    );
    assert_eq!(
        read_range["input_schema"]["properties"]["end_line"]["type"],
        "integer"
    );
    assert_eq!(read_ranges["required"], serde_json::json!(["ranges"]));
    assert_eq!(
        read_ranges["input_schema"]["properties"]["index"]["type"],
        "string"
    );
    assert_eq!(
        read_ranges["input_schema"]["properties"]["index_dir"]["type"],
        "string"
    );
    assert_eq!(
        read_ranges["input_schema"]["properties"]["cwd"]["type"],
        "string"
    );
    assert_eq!(
        read_ranges["input_schema"]["properties"]["scope"]["default"],
        "exact"
    );
    assert_eq!(
        read_ranges["input_schema"]["properties"]["include_summary"]["type"],
        "boolean"
    );
    assert_eq!(
        read_ranges["input_schema"]["properties"]["include_summary"]["default"],
        false
    );
    assert_eq!(
        read_ranges["input_schema"]["properties"]["ranges"]["oneOf"][0]["properties"]["lines"]["default"],
        80
    );
    assert_eq!(
        read_ranges["input_schema"]["properties"]["ranges"]["oneOf"][0]["properties"]["scope"]["enum"],
        serde_json::json!(["exact", "symbol"])
    );
    assert_eq!(
        read_ranges["input_schema"]["properties"]["ranges"]["oneOf"][0]["properties"]["lines"]["maximum"],
        serde_json::json!(MAX_READ_RANGE_LINES)
    );
    assert_eq!(
        read_ranges["input_schema"]["properties"]["ranges"]["oneOf"][0]["properties"]["start_line"]
            ["description"],
        "Alias for start."
    );
    assert_eq!(
        read_ranges["input_schema"]["properties"]["ranges"]["oneOf"][0]["properties"]["line_count"]
            ["maximum"],
        serde_json::json!(MAX_READ_RANGE_LINES)
    );
    assert_eq!(
        read_ranges["input_schema"]["properties"]["ranges"]["oneOf"][0]["properties"]["end_line"]["description"],
        "Inclusive end line; use instead of lines or line_count."
    );
    assert_eq!(
        read_ranges["input_schema"]["properties"]["ranges"]["oneOf"][1]["type"],
        "string"
    );
    assert_eq!(
        read_ranges["input_schema"]["properties"]["ranges"]["oneOf"][2]["maxItems"],
        serde_json::json!(MAX_BATCH_RANGES)
    );
    assert_eq!(
        read_ranges["input_schema"]["properties"]["ranges"]["max_total_lines"],
        serde_json::json!(MAX_BATCH_READ_LINES)
    );
    assert_eq!(
        read_ranges["input_schema"]["properties"]["ranges"]["oneOf"][2]["minItems"],
        serde_json::json!(1)
    );
    assert_eq!(
        read_ranges["input_schema"]["properties"]["ranges"]["oneOf"][2]["items"]["oneOf"][0]["properties"]
            ["path"]["type"],
        "string"
    );
    assert_eq!(
        read_ranges["input_schema"]["properties"]["ranges"]["oneOf"][0]["properties"]["path"]["description"],
        "Result path or copied location for the selected target; use repo/index-relative paths for repo or index targets, and shard-prefixed or unique shard-relative paths for index_dir targets."
    );
    let read_ranges_range_arg = read_ranges["arguments"]
        .as_array()
        .unwrap()
        .iter()
        .find(|argument| argument["name"] == "ranges")
        .unwrap();
    assert_eq!(read_ranges_range_arg["type"], "range|string|range[]");
    assert_eq!(
        read_ranges_range_arg["max_items"],
        serde_json::json!(MAX_BATCH_RANGES)
    );
    assert_eq!(
        read_ranges_range_arg["max_total_lines"],
        serde_json::json!(MAX_BATCH_READ_LINES)
    );
    assert_eq!(
        read_shard_range["input_schema"]["properties"]["path"]["description"],
        "Shard-prefixed result path, unique unqualified shard-relative path, or copied location such as repo/src/lib.rs#L40-L45."
    );
    assert_eq!(
        read_shard_range["required"],
        serde_json::json!(["index_dir"])
    );
    assert_eq!(
        read_shard_range["arguments"][1]["description"],
        "Shard-prefixed result path, unique unqualified shard-relative path, or copied location such as repo/src/lib.rs#L40-L45."
    );
    assert_eq!(
        read_shard_ranges["input_schema"]["properties"]["ranges"]["description"],
        "A compact range string, copied path:start-end string, {path,start,lines} object, or array of them; path may be shard-prefixed or a unique unqualified shard-relative path."
    );
    assert_eq!(
        read_shard_ranges["input_schema"]["properties"]["include_summary"]["type"],
        "boolean"
    );
    assert_eq!(
        read_shard_ranges["input_schema"]["properties"]["ranges"]["oneOf"][0]["properties"]["path"]
            ["description"],
        "Shard-prefixed result path, unique unqualified shard-relative path, or copied location such as repo/src/lib.rs#L40-L45."
    );
    assert_eq!(
        read_index_range["input_schema"]["properties"]["path"]["description"],
        "Index-relative result path or copied location, such as src/lib.rs or src/lib.rs#L40-L45."
    );
    assert_eq!(read_index_range["required"], serde_json::json!(["index"]));
    assert_eq!(
        read_index_ranges["input_schema"]["properties"]["include_summary"]["default"],
        false
    );
    assert_eq!(related_files["required"], serde_json::json!(["path"]));
    assert_eq!(
        related_files["input_schema"]["properties"]["index"]["type"],
        "string"
    );
    assert_eq!(
        related_files["input_schema"]["properties"]["index_dir"]["type"],
        "string"
    );
    assert_eq!(
        related_files["input_schema"]["properties"]["cwd"]["type"],
        "string"
    );
    assert_eq!(
        related_files["input_schema"]["properties"]["include_read_batch"]["type"],
        "boolean"
    );
    assert_eq!(
        related_files["input_schema"]["properties"]["include_read_batch"]["default"],
        false
    );
    assert_eq!(
        related_files["input_schema"]["properties"]["path"]["description"],
        "Result path for the selected target; use repo/index-relative paths for repo or index targets, and shard-prefixed or unique shard-relative paths for index_dir targets."
    );
    assert_eq!(
        related_files["input_schema"]["properties"]["exclude_content"]["oneOf"][1]["items"]["type"],
        "string"
    );
    assert_eq!(related_symbols["required"], serde_json::json!([]));
    assert_eq!(
        related_symbols["input_schema"]["properties"]["index"]["type"],
        "string"
    );
    assert_eq!(
        related_symbols["input_schema"]["properties"]["index_dir"]["type"],
        "string"
    );
    assert_eq!(
        related_symbols["input_schema"]["properties"]["cwd"]["type"],
        "string"
    );
    assert_eq!(
        related_symbols["input_schema"]["properties"]["include_read_batch"]["type"],
        "boolean"
    );
    assert_eq!(
        related_symbols["input_schema"]["properties"]["include_read_batch"]["default"],
        false
    );
    assert_eq!(
        related_symbols["input_schema"]["properties"]["exclude_content"]["oneOf"][1]["items"]["type"],
        "string"
    );
    assert_eq!(find_symbol["required"], serde_json::json!(["name"]));
    assert_eq!(
        find_symbol["input_schema"]["properties"]["index"]["type"],
        "string"
    );
    assert_eq!(
        find_symbol["input_schema"]["properties"]["index_dir"]["type"],
        "string"
    );
    assert_eq!(find_symbol_batch["required"], serde_json::json!(["names"]));
    assert_eq!(
        find_symbol_batch["input_schema"]["properties"]["names"]["maxItems"],
        serde_json::json!(MAX_BATCH_QUERIES)
    );
    assert_eq!(agent_guide_tool["required"], serde_json::json!([]));
    assert_eq!(
        agent_guide_tool["input_schema"]["properties"]["addr"]["default"],
        "127.0.0.1:8796"
    );
    assert_eq!(
        agent_guide_tool["input_schema"]["properties"]["profile"]["enum"],
        serde_json::json!(["generic", "codex", "claude", "amp"])
    );
    assert_eq!(agent_instructions_tool["required"], serde_json::json!([]));
    assert_eq!(
        agent_instructions_tool["input_schema"]["properties"]["addr"]["default"],
        "127.0.0.1:8796"
    );
    assert_eq!(
        agent_instructions_tool["input_schema"]["properties"]["profile"]["default"],
        "generic"
    );
    assert_eq!(repo_map["required"], serde_json::json!([]));
    assert_eq!(
        repo_map["input_schema"]["properties"]["index"]["type"],
        "string"
    );
    assert_eq!(
        repo_map["input_schema"]["properties"]["index_dir"]["type"],
        "string"
    );
    assert_eq!(
        repo_map["input_schema"]["properties"]["detail"]["default"],
        "compact"
    );
    assert_eq!(
        repo_map["input_schema"]["properties"]["detail"]["enum"],
        serde_json::json!(["compact", "full"])
    );
    assert_eq!(
        repo_map["input_schema"]["properties"]["read_limit"]["default"],
        serde_json::json!(DEFAULT_REPO_MAP_READ_BATCH_RANGES)
    );
    assert_eq!(
        repo_map["input_schema"]["properties"]["read_limit"]["maximum"],
        serde_json::json!(MAX_RESULT_READ_BATCH_RANGES)
    );
    assert_eq!(
        search["input_schema"]["properties"]["context_lines"]["maximum"],
        serde_json::json!(MAX_ATTACHED_CONTEXT_LINES)
    );
    assert_eq!(
        search["arguments"]
            .as_array()
            .unwrap()
            .iter()
            .find(|argument| argument["name"] == "context_lines")
            .unwrap()["maximum"],
        serde_json::json!(MAX_ATTACHED_CONTEXT_LINES)
    );

    let listed = ToolRuntime::default().dispatch(ToolRequest {
        id: serde_json::json!("list"),
        tool: "list_tools".to_string(),
        arguments: serde_json::json!({}),
    });
    assert!(listed.error.is_none(), "{:?}", listed.error);
    let listed_names = listed
        .result
        .unwrap()
        .as_array()
        .unwrap()
        .iter()
        .map(|value| value.as_str().unwrap().to_string())
        .collect::<Vec<_>>();
    let manifest_names = tools
        .iter()
        .map(|tool| tool["name"].as_str().unwrap().to_string())
        .collect::<Vec<_>>();
    assert_eq!(listed_names, manifest_names);
}

#[test]
fn server_reports_tool_manifest_for_agent_wrappers() {
    let binary = assert_cmd::cargo::cargo_bin("orient");
    let mut child = Command::new(binary)
        .arg("serve-jsonl")
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .spawn()
        .unwrap();

    let list_request = serde_json::json!({
        "id": "list",
        "tool": "list_tools",
        "arguments": {}
    });
    let manifest_request = serde_json::json!({
        "id": "manifest",
        "tool": "tool_manifest",
        "arguments": {}
    });
    writeln!(child.stdin.as_mut().unwrap(), "{list_request}").unwrap();
    writeln!(child.stdin.as_mut().unwrap(), "{manifest_request}").unwrap();
    drop(child.stdin.take());

    let output = child.wait_with_output().unwrap();
    assert!(output.status.success());
    let stdout = String::from_utf8(output.stdout).unwrap();
    assert!(stdout.contains("\"id\":\"list\""));
    assert!(stdout.contains("tool_manifest"));
    assert!(stdout.contains("\"id\":\"manifest\""));
    assert!(stdout.contains("\"name\":\"search_code\""));
    assert!(stdout.contains("\"name\":\"search\""));
    assert!(stdout.contains("\"name\":\"search_auto\""));
    assert!(stdout.contains("\"name\":\"search_auto_batch\""));
    assert!(stdout.contains("\"name\":\"indexed_search\""));
    assert!(stdout.contains("\"name\":\"index_plan\""));
    assert!(stdout.contains("\"name\":\"shard_plan\""));
    assert!(stdout.contains("\"name\":\"mcp_manifest\""));
    assert!(stdout.contains("\"name\":\"agent_guide\""));
    assert!(stdout.contains("\"required\":[\"repo\",\"query\"]"));
    assert!(stdout.contains("\"optional\""));
    assert!(stdout.contains("\"arguments\""));
    assert!(stdout.contains("\"input_schema\""));
    assert!(stdout.contains("\"type\":\"integer\""));
    assert!(stdout.contains("\"default\":10"));
    assert!(stdout.contains("\"enum\":[\"short\",\"medium\",\"block\",\"symbol\"]"));
    assert!(stdout.contains("\"type\":\"range|string|range[]\""));
    assert!(stdout.contains("exclude_path"));
    assert!(stdout.contains("exclude_symbol"));
    assert!(stdout.contains("open_range"));
    assert!(stdout.contains("read_ranges"));
    assert!(stdout.contains("search_batch"));
    assert!(stdout.contains("open_index_range"));
    assert!(stdout.contains("read_index_ranges"));
    assert!(stdout.contains("indexed_search_batch"));
    assert!(stdout.contains("read_shard_ranges"));
    assert!(stdout.contains("read_index_range"));
    assert!(stdout.contains("index_status"));
    assert!(stdout.contains("indexed_query_plan"));
    assert!(stdout.contains("indexed_query_plan_batch"));
    assert!(stdout.contains("indexed_repo_map"));
    assert!(stdout.contains("shard_query_plan"));
    assert!(stdout.contains("shard_query_plan_batch"));
    assert!(stdout.contains("find_index_symbol"));
    assert!(stdout.contains("find_index_symbol_batch"));
    assert!(stdout.contains("related_index_files"));
    assert!(stdout.contains("related_index_symbols"));
    assert!(stdout.contains("open_shard_range"));
    assert!(stdout.contains("read_shard_range"));
    assert!(stdout.contains("related_shard_files"));
    assert!(stdout.contains("related_shard_symbols"));
    assert!(stdout.contains("unique unqualified shard-relative path"));
    assert!(stdout.contains("shard_repo_map"));
    assert!(stdout.contains("find_shard_symbol"));
    assert!(stdout.contains("find_shard_symbol_batch"));
    assert!(stdout.contains("find_symbol_batch"));
    assert!(stdout.contains("daemon_status"));
    assert!(stdout.contains("warm_index"));
    assert!(stdout.contains("register_shards"));
    assert!(stdout.contains("warm_shards"));
    assert!(stdout.contains("ensure_shards"));
    assert!(stdout.contains("shard_status"));
    assert!(stdout.contains("search_shards_batch"));
    assert!(stdout.contains("discover_repos"));
}

#[test]
fn mcp_manifest_exposes_input_schema_for_adapter_wrappers() {
    let manifest = mcp_tool_manifest();
    let tools = manifest["tools"].as_array().unwrap();
    let search = tools
        .iter()
        .find(|tool| tool["name"] == "search_code")
        .unwrap();
    let mcp_manifest = tools
        .iter()
        .find(|tool| tool["name"] == "mcp_manifest")
        .unwrap();
    let agent_guide_tool = tools
        .iter()
        .find(|tool| tool["name"] == "agent_guide")
        .unwrap();
    let agent_instructions_tool = tools
        .iter()
        .find(|tool| tool["name"] == "agent_instructions")
        .unwrap();
    let ensure_index = tools
        .iter()
        .find(|tool| tool["name"] == "ensure_index")
        .unwrap();
    let read_shard_range = tools
        .iter()
        .find(|tool| tool["name"] == "read_shard_range")
        .unwrap();
    let read_ranges = tools
        .iter()
        .find(|tool| tool["name"] == "read_ranges")
        .unwrap();
    let repo_map = tools
        .iter()
        .find(|tool| tool["name"] == "repo_map")
        .unwrap();

    assert_eq!(
        search["description"],
        "Search a local repository with the fast fallback path and return ranked snippets."
    );
    assert_eq!(
        search["inputSchema"]["required"],
        serde_json::json!(["repo", "query"])
    );
    assert_eq!(
        search["inputSchema"]["properties"]["limit"]["maximum"],
        serde_json::json!(MAX_SEARCH_RESULTS)
    );
    assert!(search.get("input_schema").is_none());
    assert_eq!(
        mcp_manifest["inputSchema"]["properties"],
        serde_json::json!({})
    );
    assert_eq!(
        agent_guide_tool["inputSchema"]["properties"]["addr"]["default"],
        "127.0.0.1:8796"
    );
    assert_eq!(
        agent_guide_tool["inputSchema"]["properties"]["profile"]["enum"],
        serde_json::json!(["generic", "codex", "claude", "amp"])
    );
    assert_eq!(agent_guide_tool["annotations"]["readOnlyHint"], true);
    assert_eq!(
        agent_instructions_tool["inputSchema"]["properties"]["addr"]["default"],
        "127.0.0.1:8796"
    );
    assert_eq!(
        agent_instructions_tool["inputSchema"]["properties"]["profile"]["default"],
        "generic"
    );
    assert_eq!(
        repo_map["inputSchema"]["properties"]["detail"]["enum"],
        serde_json::json!(["compact", "full"])
    );
    assert_eq!(
        repo_map["inputSchema"]["properties"]["read_limit"]["maximum"],
        serde_json::json!(MAX_RESULT_READ_BATCH_RANGES)
    );
    assert_eq!(agent_instructions_tool["annotations"]["readOnlyHint"], true);
    assert_eq!(search["annotations"]["readOnlyHint"], true);
    assert_eq!(search["annotations"]["destructiveHint"], false);
    assert_eq!(search["annotations"]["idempotentHint"], true);
    assert_eq!(search["annotations"]["openWorldHint"], false);
    assert_eq!(
        read_shard_range["inputSchema"]["properties"]["path"]["description"],
        "Shard-prefixed result path, unique unqualified shard-relative path, or copied location such as repo/src/lib.rs#L40-L45."
    );
    assert_eq!(
        read_ranges["inputSchema"]["properties"]["ranges"]["max_total_lines"],
        serde_json::json!(MAX_BATCH_READ_LINES)
    );
    assert_eq!(
        read_ranges["inputSchema"]["properties"]["include_summary"]["type"],
        "boolean"
    );
    assert!(
        read_shard_range["description"]
            .as_str()
            .unwrap()
            .contains("unique shard-relative path")
    );
    assert_eq!(ensure_index["annotations"]["readOnlyHint"], false);
    assert_eq!(ensure_index["annotations"]["destructiveHint"], false);
    assert_eq!(ensure_index["annotations"]["idempotentHint"], false);
    assert_eq!(ensure_index["annotations"]["openWorldHint"], false);
}

#[test]
fn mcp_stdio_serves_tool_list_and_calls_existing_runtime() {
    let runtime = ToolRuntime::default();
    let init = mcp_dispatch_value(
        &runtime,
        &serde_json::json!({"jsonrpc":"2.0","id":1,"method":"initialize","params":{}}),
    )
    .unwrap();
    assert_eq!(init["result"]["serverInfo"]["name"], "orient-search");
    assert_eq!(
        init["result"]["capabilities"]["tools"],
        serde_json::json!({})
    );

    let listed = mcp_dispatch_value(
        &runtime,
        &serde_json::json!({"jsonrpc":"2.0","id":2,"method":"tools/list","params":{}}),
    )
    .unwrap();
    assert!(
        listed["result"]["tools"]
            .as_array()
            .unwrap()
            .iter()
            .any(|tool| tool["name"] == "search_code")
    );

    let called = mcp_dispatch_value(
        &runtime,
        &serde_json::json!({
            "jsonrpc":"2.0",
            "id":3,
            "method":"tools/call",
            "params":{"name":"list_tools","arguments":{}}
        }),
    )
    .unwrap();
    assert_eq!(called["result"]["isError"], false);
    assert!(
        called["result"]["structuredContent"]
            .as_array()
            .unwrap()
            .contains(&serde_json::json!("search_code"))
    );

    let notification = mcp_dispatch_value(
        &runtime,
        &serde_json::json!({"jsonrpc":"2.0","method":"notifications/initialized","params":{}}),
    );
    assert!(notification.is_none());
}

#[test]
fn mcp_stdio_loop_writes_json_rpc_lines() {
    let runtime = ToolRuntime::default();
    let input = concat!(
        "{\"jsonrpc\":\"2.0\",\"id\":\"tools\",\"method\":\"tools/list\",\"params\":{}}\n",
        "{\"jsonrpc\":\"2.0\",\"method\":\"notifications/initialized\",\"params\":{}}\n",
        "{\"jsonrpc\":\"2.0\",\"id\":\"call\",\"method\":\"tools/call\",\"params\":{\"name\":\"list_tools\",\"arguments\":{}}}\n",
    );
    let mut output = Vec::new();
    serve_mcp_with_runtime(std::io::Cursor::new(input), &mut output, &runtime).unwrap();
    let output = String::from_utf8(output).unwrap();
    let lines = output.lines().collect::<Vec<_>>();
    assert_eq!(lines.len(), 2);
    assert!(lines[0].contains("\"id\":\"tools\""));
    assert!(lines[0].contains("\"tools\""));
    assert!(lines[1].contains("\"id\":\"call\""));
    assert!(lines[1].contains("\"structuredContent\""));
}

#[test]
fn agent_guide_returns_local_agent_request_templates() {
    let guide = agent_guide(
        Some("/work/repo"),
        Some("/tmp/repo.index"),
        Some("/tmp/orient-shards"),
        Some("127.0.0.1:9999"),
        Some("codex"),
    );
    assert_eq!(guide["profile"], "codex");
    assert_eq!(
        guide["instruction_target"],
        "the local instruction file read by the selected coding agent"
    );
    assert_eq!(
        guide["preferred_surfaces"]["many_local_repos"],
        "search_shards"
    );
    assert_eq!(
        guide["preferred_surfaces"]["warmed_daemon_default"],
        "search_auto"
    );
    assert_eq!(
        guide["request_templates"]["auto_search"]["tool"],
        "search_auto"
    );
    assert_eq!(
        guide["request_templates"]["auto_search_batch"]["tool"],
        "search_auto_batch"
    );
    assert_eq!(
        guide["quickstart"]["install"],
        "cargo install --git https://github.com/evalops/orient-search"
    );
    assert_eq!(
        guide["quickstart"]["client"],
        "orient client-jsonl --require-version --addr 127.0.0.1:9999"
    );
    assert_eq!(
        guide["quickstart"]["status"],
        "orient daemon-status --addr 127.0.0.1:9999 --format json"
    );
    assert_eq!(
        guide["quickstart"]["one_shot_search"],
        "orient search-auto --retry-if-empty \"symbol:SessionManager token\""
    );
    assert!(
        guide["quickstart"]["multi_repo"]
            .as_array()
            .unwrap()
            .iter()
            .any(|command| command.as_str().unwrap().contains(
                "orient ensure-shards --discover-root /path/to/workspaces --output-dir /tmp/orient-shards"
            ))
    );
    assert_eq!(
        guide["quickstart"]["agent_instructions"],
        "orient agent-instructions --profile codex --index-dir /tmp/orient-shards"
    );
    assert!(
        guide["quickstart"]["single_repo"]
            .as_array()
            .unwrap()
            .iter()
            .any(|command| command
                .as_str()
                .unwrap()
                .contains("orient ensure-index --repo /work/repo --index /tmp/repo.index"))
    );
    assert_eq!(
        guide["request_templates"]["shard_search"]["tool"],
        "search_shards"
    );
    assert_eq!(
        guide["request_templates"]["auto_search"]["arguments"]["retry_if_empty"],
        serde_json::json!(true)
    );
    assert_eq!(
        guide["request_templates"]["auto_search_batch"]["arguments"]["retry_if_empty"],
        serde_json::json!(true)
    );
    assert_eq!(
        guide["request_templates"]["live_search"]["arguments"]["repo"],
        "/work/repo"
    );
    assert_eq!(
        guide["request_templates"]["live_repo_map"]["arguments"]["detail"],
        "compact"
    );
    assert_eq!(
        guide["request_templates"]["live_repo_map"]["arguments"]["read_limit"],
        serde_json::json!(DEFAULT_REPO_MAP_READ_BATCH_RANGES)
    );
    assert_eq!(
        guide["request_templates"]["indexed_repo_map"]["arguments"]["detail"],
        "compact"
    );
    assert_eq!(
        guide["request_templates"]["indexed_repo_map"]["arguments"]["read_limit"],
        serde_json::json!(DEFAULT_REPO_MAP_READ_BATCH_RANGES)
    );
    assert_eq!(
        guide["request_templates"]["shard_repo_map"]["arguments"]["detail"],
        "compact"
    );
    assert_eq!(
        guide["request_templates"]["shard_repo_map"]["arguments"]["read_limit"],
        serde_json::json!(DEFAULT_REPO_MAP_READ_BATCH_RANGES)
    );
    assert_eq!(
        guide["request_templates"]["indexed_search"]["arguments"]["index"],
        "/tmp/repo.index"
    );
    assert_eq!(
        guide["request_templates"]["live_query_plan"]["tool"],
        "search_query_plan"
    );
    assert_eq!(
        guide["request_templates"]["indexed_query_plan"]["tool"],
        "indexed_query_plan"
    );
    assert_eq!(
        guide["request_templates"]["shard_query_plan"]["tool"],
        "shard_query_plan"
    );
    assert!(
        guide["transports"]["tcp_daemon"]
            .as_str()
            .unwrap()
            .contains("127.0.0.1:9999")
    );
    assert!(guide["purpose"].as_str().unwrap().contains("no telemetry"));
    assert!(
        guide["instruction_snippet"]
            .as_str()
            .unwrap()
            .contains("Use Orient for local code discovery and bounded file reads")
    );
    assert!(
        guide["recommended_loop"]
            .as_array()
            .unwrap()
            .iter()
            .any(|item| item.as_str().unwrap().contains("search_auto_default"))
    );
    assert!(
        guide["ranking_notes"]
            .as_array()
            .unwrap()
            .iter()
            .any(|item| item.as_str().unwrap().contains("generated:true"))
    );
    assert!(
        guide["adapter_notes"]
            .as_array()
            .unwrap()
            .iter()
            .any(|item| item.as_str().unwrap().contains("Selected adapter profile"))
    );
    assert_eq!(
        guide["hard_limits"]["max_batch_read_lines"],
        serde_json::json!(MAX_BATCH_READ_LINES)
    );
}

#[test]
fn agent_instructions_returns_copyable_local_agent_snippet() {
    let instructions = agent_instructions(
        Some("/work/repo"),
        Some("/tmp/repo.index"),
        Some("/tmp/orient-shards"),
        Some("127.0.0.1:9999"),
        Some("claude"),
    );
    for expected in [
        "## Orient Search",
        "Use Orient for local code discovery and bounded file reads",
        "orient client-jsonl --require-version --addr 127.0.0.1:9999",
        "selected coding agent",
        "Keep cache paths local",
        "Orient shares code-search artifacts only",
        "orient ensure-shards --discover-root /path/to/workspaces --output-dir /tmp/orient-shards --family-limit 2",
        "orient ensure-index --repo /work/repo --index /tmp/repo.index",
        "search_auto_batch",
        "search_auto` with `retry_if_empty:true",
        "search_auto_batch` with `retry_if_empty:true",
        "daemon_status.search_auto_default",
        "`file:`, `path:`, `lang:`, `ext:`, `symbol:`, `type:`, `repo:`, `test:`",
        "Generated paths, including hashed JavaScript bundles, are demoted by default",
        "run that request instead of translating it into a shell search/read command",
        "query_plan_summary",
        "Fall back to shell search only when Orient is unavailable",
        "has no telemetry",
    ] {
        assert!(
            instructions.contains(expected),
            "missing instruction text: {expected}\n{instructions}"
        );
    }
}

#[test]
fn agent_guidance_defaults_use_neutral_cache_placeholders() {
    let guide = agent_guide(None, None, None, None, None);
    let guide_json = serde_json::to_string(&guide).unwrap();
    assert_eq!(guide["profile"], "generic");
    assert!(guide_json.contains("/path/to/local/cache/orient.index"));
    assert!(guide_json.contains("/path/to/local/cache/orient-shards"));
    assert!(!guide_json.contains("/tmp/orient"));
    assert!(!guide_json.contains("/tmp/repo"));
    assert!(
        guide["adapter_notes"]
            .as_array()
            .unwrap()
            .iter()
            .any(|item| item.as_str().unwrap().contains("machine-specific layouts"))
    );
    assert!(!guide_json.contains("AGENTS.md"));
    assert!(!guide_json.contains("CLAUDE.md"));
    assert!(!guide_json.contains("Amp rules"));

    let instructions = agent_instructions(None, None, None, None, None);
    assert!(instructions.contains("/path/to/local/cache/orient.index"));
    assert!(instructions.contains("/path/to/local/cache/orient-shards"));
    assert!(!instructions.contains("/tmp/orient"));
    assert!(!instructions.contains("/tmp/repo"));
    assert!(instructions.contains("machine-specific layouts"));
    assert!(instructions.contains("has no telemetry"));
    assert!(instructions.contains("the local agent instruction file for this repo"));
    assert!(!instructions.contains("AGENTS.md"));
    assert!(!instructions.contains("CLAUDE.md"));
    assert!(!instructions.contains("Amp rules"));
}

#[test]
fn agent_guidance_profiles_keep_instruction_surfaces_neutral() {
    let codex = agent_instructions(None, None, None, None, Some("codex"));
    assert!(codex.contains("selected coding agent"));
    assert!(!codex.contains("Selected profile"));
    assert!(!codex.contains("AGENTS.md"));
    assert!(!codex.contains("CLAUDE.md"));

    let claude = agent_instructions(None, None, None, None, Some("claude-code"));
    assert!(claude.contains("selected coding agent"));
    assert!(!claude.contains("CLAUDE.md"));
    assert!(!claude.contains("AGENTS.md"));

    let amp = agent_guide(None, None, None, None, Some("amp"));
    assert_eq!(amp["profile"], "amp");
    assert!(
        amp["instruction_target"]
            .as_str()
            .unwrap()
            .contains("selected coding agent")
    );
    assert!(
        amp["adapter_notes"]
            .as_array()
            .unwrap()
            .iter()
            .any(|item| item.as_str().unwrap().contains("Selected adapter profile"))
    );
}

#[test]
fn runtime_serves_agent_guide_for_json_lines_wrappers() {
    let runtime = ToolRuntime::default();
    let response = runtime.dispatch(ToolRequest {
        id: serde_json::json!("guide"),
        tool: "agent_guide".to_string(),
        arguments: serde_json::json!({
            "repo": "/work/repo",
            "index_dir": "/tmp/orient-shards",
            "profile": "codex"
        }),
    });
    assert!(response.error.is_none(), "{:?}", response.error);
    let guide = response.result.unwrap();
    assert_eq!(
        guide["request_templates"]["manifest"]["tool"],
        "tool_manifest"
    );
    assert_eq!(
        guide["request_templates"]["shard_repo_map"]["arguments"]["index_dir"],
        "/tmp/orient-shards"
    );
    assert_eq!(guide["profile"], "codex");
    let followups = guide["result_followups"].as_array().unwrap();
    for expected in [
        "Use search_auto.query_plan_result or a search_auto_batch item query_plan_result immediately when an automatic search is empty.",
        "Use search_auto.query_plan_request, a search_auto_batch item query_plan_request, or a search batch item query_plan_request when results are empty or noisy.",
        "Use search_auto.repo_map_request, a search_auto_batch item repo_map_request, or a search batch item repo_map_request when the agent needs entrypoints, tests, commands, or top symbols for the chosen surface.",
        "Use search_auto.next_action or a search batch item next_action when the wrapper wants one prioritized follow-up request; empty search batch items point at query_plan_request.",
        "Use search_auto.read_batch_request, a search_auto_batch item read_batch_request, or a search batch item next_action/read_batch_request to read top ranges in one call.",
        "Use symbol batch item next_action/read_batch_request to read candidate definitions for one requested symbol name.",
        "Use read_batch_request.read_budget to keep batch reads under hard_limits.max_batch_read_lines; split large inspections instead of widening one call.",
        "Use result.read_request for one bounded file range.",
    ] {
        assert!(
            followups
                .iter()
                .any(|followup| followup.as_str() == Some(expected)),
            "missing follow-up: {expected}"
        );
    }
}

#[test]
fn runtime_serves_agent_instructions_for_local_instruction_files() {
    let runtime = ToolRuntime::default();
    let response = runtime.dispatch(ToolRequest {
        id: serde_json::json!("instructions"),
        tool: "agent_instructions".to_string(),
        arguments: serde_json::json!({
            "repo": "/work/repo",
            "index": "/tmp/repo.index",
            "index_dir": "/tmp/orient-shards",
            "addr": "127.0.0.1:9999",
            "profile": "amp"
        }),
    });
    assert!(response.error.is_none(), "{:?}", response.error);
    let result = response.result.unwrap();
    let instructions = result["instructions"].as_str().unwrap();
    assert!(instructions.contains("orient client-jsonl --require-version --addr 127.0.0.1:9999"));
    assert!(instructions.contains("search_auto"));
    assert!(instructions.contains("search_auto_default"));
    assert!(instructions.contains("next_action"));
    assert!(instructions.contains("read_batch_request"));
    assert!(instructions.contains("selected coding agent"));
    assert!(!instructions.contains("AGENTS.md"));
    assert!(!instructions.contains("CLAUDE.md"));
    assert!(instructions.contains("has no telemetry"));
}

#[test]
fn runtime_repo_map_detail_defaults_to_compact_and_allows_full_imports() {
    let repo = tempfile::tempdir().unwrap();
    let bulk_imports = (0..40)
        .map(|index| format!("use alpha::Module{index};\n"))
        .collect::<String>();
    write(&repo.path().join("src/bulk.rs"), &bulk_imports);
    write(
        &repo.path().join("src/other.rs"),
        "use beta::Client;\nuse gamma::Config;\npub fn call() {}\n",
    );
    write(&repo.path().join("README.md"), "# sample\n");
    write(
        &repo.path().join("Cargo.toml"),
        "[package]\nname='sample'\nversion='0.1.0'\n",
    );
    write(
        &repo.path().join("tests/other_test.rs"),
        "#[test]\nfn it_works() {}\n",
    );

    let runtime = ToolRuntime::default();
    let compact = runtime.dispatch(ToolRequest {
        id: serde_json::json!("compact"),
        tool: "repo_map".to_string(),
        arguments: serde_json::json!({
            "repo": repo.path(),
            "symbols": 5,
            "tests": 5
        }),
    });
    assert!(compact.error.is_none(), "{:?}", compact.error);
    let compact = compact.result.unwrap();
    assert_eq!(
        compact["brief"]["import_hints"].as_array().unwrap().len(),
        32
    );

    let narrow_reads = runtime.dispatch(ToolRequest {
        id: serde_json::json!("narrow-reads"),
        tool: "repo_map".to_string(),
        arguments: serde_json::json!({
            "repo": repo.path(),
            "symbols": 5,
            "tests": 5,
            "read_limit": 2
        }),
    });
    assert!(narrow_reads.error.is_none(), "{:?}", narrow_reads.error);
    let narrow_reads = narrow_reads.result.unwrap();
    assert_eq!(
        narrow_reads["read_batch_request"]["arguments"]["ranges"]
            .as_array()
            .unwrap()
            .len(),
        2
    );
    assert_eq!(
        narrow_reads["next_action"]["source"],
        serde_json::json!("read_batch_request")
    );
    assert_eq!(
        narrow_reads["next_action"]["request"],
        narrow_reads["read_batch_request"]
    );

    let full = runtime.dispatch(ToolRequest {
        id: serde_json::json!("full"),
        tool: "repo_map".to_string(),
        arguments: serde_json::json!({
            "repo": repo.path(),
            "symbols": 5,
            "tests": 5,
            "detail": "full"
        }),
    });
    assert!(full.error.is_none(), "{:?}", full.error);
    let full = full.result.unwrap();
    assert_eq!(full["brief"]["import_hints"].as_array().unwrap().len(), 42);

    let invalid = runtime.dispatch(ToolRequest {
        id: serde_json::json!("bad-detail"),
        tool: "repo_map".to_string(),
        arguments: serde_json::json!({
            "repo": repo.path(),
            "detail": "verbose"
        }),
    });
    assert!(invalid.error.unwrap().contains("expected compact or full"));

    let invalid_limit = runtime.dispatch(ToolRequest {
        id: serde_json::json!("bad-read-limit"),
        tool: "repo_map".to_string(),
        arguments: serde_json::json!({
            "repo": repo.path(),
            "read_limit": 0
        }),
    });
    assert!(
        invalid_limit
            .error
            .unwrap()
            .contains("argument read_limit must be a positive integer")
    );

    let index_path = repo.path().join("orient.index");
    FastIndex::build(repo.path())
        .unwrap()
        .save(&index_path)
        .unwrap();
    let indexed = runtime.dispatch(ToolRequest {
        id: serde_json::json!("indexed-map"),
        tool: "repo_map".to_string(),
        arguments: serde_json::json!({
            "index": index_path,
            "symbols": 5,
            "tests": 5
        }),
    });
    assert!(indexed.error.is_none(), "{:?}", indexed.error);
    let indexed = indexed.result.unwrap();
    assert!(indexed["brief"]["manifest_files"].is_array());
    assert_eq!(indexed["read_batch_request"]["tool"], "read_ranges");
    assert_eq!(
        indexed["next_action"]["request"],
        indexed["read_batch_request"]
    );
    assert_eq!(
        indexed["read_batch_request"]["arguments"]["index"],
        serde_json::json!(index_path)
    );

    let shard_dir = repo.path().join(".orient-shards");
    build_shards(&[repo.path().to_path_buf()], &shard_dir).unwrap();
    let sharded = runtime.dispatch(ToolRequest {
        id: serde_json::json!("sharded-map"),
        tool: "repo_map".to_string(),
        arguments: serde_json::json!({
            "index_dir": shard_dir,
            "symbols": 5,
            "tests": 5
        }),
    });
    assert!(sharded.error.is_none(), "{:?}", sharded.error);
    let sharded = sharded.result.unwrap();
    assert_eq!(
        sharded[0]["map"]["read_batch_request"]["tool"],
        "read_ranges"
    );
    assert_eq!(
        sharded[0]["map"]["next_action"]["request"],
        sharded[0]["map"]["read_batch_request"]
    );
    assert_eq!(
        sharded[0]["map"]["read_batch_request"]["arguments"]["index_dir"],
        serde_json::json!(shard_dir)
    );

    let conflicted = runtime.dispatch(ToolRequest {
        id: serde_json::json!("conflicted-map"),
        tool: "repo_map".to_string(),
        arguments: serde_json::json!({
            "index": repo.path().join("orient.index"),
            "index_dir": repo.path().join(".orient-shards")
        }),
    });
    assert!(
        conflicted
            .error
            .as_ref()
            .unwrap()
            .contains("only one of index or index_dir"),
        "{:?}",
        conflicted.error
    );
}

#[test]
fn runtime_search_auto_uses_live_repo_and_single_warmed_index() {
    let repo = tempfile::tempdir().unwrap();
    write(
        &repo.path().join("src/auth.rs"),
        "pub struct SessionManager;\npub fn issue_token() {}\n",
    );
    let runtime = ToolRuntime::default();

    let live = runtime.dispatch(ToolRequest {
        id: serde_json::json!("live"),
        tool: "search_auto".to_string(),
        arguments: serde_json::json!({
            "repo": repo.path(),
            "query": "issue_token",
            "limit": 5
        }),
    });
    assert!(live.error.is_none(), "{:?}", live.error);
    let live = live.result.unwrap();
    assert_eq!(live["surface"], "fallback");
    assert_eq!(live["summary"]["status"], serde_json::json!("matched"));
    assert_eq!(live["summary"]["result_count"], serde_json::json!(1));
    assert_eq!(
        live["summary"]["top_paths"],
        serde_json::json!(["src/auth.rs"])
    );
    assert_eq!(live["summary"]["top_dirs"], serde_json::json!(["src"]));
    assert_eq!(live["summary"]["top_exts"], serde_json::json!(["rs"]));
    assert_eq!(live["summary"]["top_langs"], serde_json::json!(["rust"]));
    assert!(
        live["summary"]["max_score"].as_f64().unwrap()
            >= live["summary"]["min_score"].as_f64().unwrap()
    );
    assert_eq!(live["query_plan_request"]["tool"], "search_query_plan");
    assert_eq!(live["repo_map_request"]["tool"], "repo_map");
    assert_eq!(
        live["repo_map_request"]["arguments"]["repo"],
        serde_json::json!(repo.path())
    );
    assert_eq!(live["repo_map_request"]["arguments"]["detail"], "compact");
    assert!(
        live["repo_map_request"]["cli"]
            .as_str()
            .unwrap()
            .contains("orient repo-map --repo")
    );
    assert_eq!(
        live["repo_map_request"]["arguments"]["read_limit"],
        serde_json::json!(DEFAULT_REPO_MAP_READ_BATCH_RANGES)
    );
    assert_eq!(
        live["query_plan_request"]["arguments"]["query"],
        "issue_token"
    );
    assert!(
        live["query_plan_request"]["cli"]
            .as_str()
            .unwrap()
            .contains("orient search-plan --repo")
    );
    assert_eq!(live["results"][0]["read_request"]["tool"], "read_range");
    assert_eq!(
        live["results"][0]["related_request"]["tool"],
        "related_files"
    );
    assert_eq!(
        live["results"][0]["related_request"]["arguments"]["include_read_batch"],
        serde_json::json!(true)
    );
    assert!(
        live["results"][0]["related_request"]["cli"]
            .as_str()
            .unwrap()
            .contains("--include-read-batch")
    );
    assert_eq!(
        live["results"][0]["related_symbols_request"]["tool"],
        "related_symbols"
    );
    assert_eq!(
        live["results"][0]["related_symbols_request"]["arguments"]["include_read_batch"],
        serde_json::json!(true)
    );
    assert!(
        live["results"][0]["related_symbols_request"]["cli"]
            .as_str()
            .unwrap()
            .contains("--include-read-batch")
    );
    assert!(
        live["results"][0]["read_request"]["cli"]
            .as_str()
            .unwrap()
            .contains("orient read-range --repo")
    );
    assert!(
        live["results"][0]["read_request"]["cli"]
            .as_str()
            .unwrap()
            .contains("src/auth.rs:")
    );
    assert_eq!(live["read_batch_request"]["tool"], "read_ranges");
    assert_eq!(
        live["read_batch_request"]["summary"],
        serde_json::json!("Read 1 bounded range (80 total lines).")
    );
    assert!(
        live["read_batch_request"]["cli"]
            .as_str()
            .unwrap()
            .contains("orient read-ranges --repo")
    );
    assert_eq!(
        live["read_batch_request"]["arguments"]["ranges"][0]["path"],
        "src/auth.rs"
    );
    assert_eq!(
        live["read_batch_request"]["arguments"]["include_summary"],
        serde_json::json!(true)
    );
    assert!(
        live["read_batch_request"]["cli"]
            .as_str()
            .unwrap()
            .contains("--summary")
    );
    assert_eq!(
        live["read_batch_request"]["read_budget"]["range_count"],
        serde_json::json!(1)
    );
    assert_eq!(
        live["read_batch_request"]["read_budget"]["max_total_lines"],
        serde_json::json!(MAX_BATCH_READ_LINES)
    );
    assert_eq!(live["next_action"]["kind"], serde_json::json!("read"));
    assert_eq!(
        live["next_action"]["request"],
        live["next_read_batch_request"]
    );
    let read_jsonl: serde_json::Value =
        serde_json::from_str(live["read_batch_request"]["jsonl"].as_str().unwrap()).unwrap();
    assert_eq!(read_jsonl["id"], serde_json::json!("read"));
    assert_eq!(read_jsonl["tool"], serde_json::json!("read_ranges"));
    assert!(
        live["read_batch_request"]["client_cli"]
            .as_str()
            .unwrap()
            .contains("| orient client-jsonl")
    );
    let read_batch_from_jsonl =
        runtime.dispatch_line(live["read_batch_request"]["jsonl"].as_str().unwrap());
    assert!(
        read_batch_from_jsonl.error.is_none(),
        "{:?}",
        read_batch_from_jsonl.error
    );
    let read_batch_from_jsonl = read_batch_from_jsonl.result.unwrap();
    assert_eq!(
        read_batch_from_jsonl["summary"]["range_count"],
        serde_json::json!(1)
    );
    assert!(
        read_batch_from_jsonl["ranges"][0]["text"]
            .as_str()
            .unwrap()
            .contains("issue_token")
    );
    assert!(live.get("query_plan_result").is_none());

    let diagnosed_live = runtime.dispatch(ToolRequest {
        id: serde_json::json!("diagnosed-live"),
        tool: "search_auto".to_string(),
        arguments: serde_json::json!({
            "repo": repo.path(),
            "query": "issue_token",
            "limit": 5,
            "diagnose": true
        }),
    });
    assert!(diagnosed_live.error.is_none(), "{:?}", diagnosed_live.error);
    let diagnosed_live = diagnosed_live.result.unwrap();
    assert!(!diagnosed_live["results"].as_array().unwrap().is_empty());
    assert_eq!(
        diagnosed_live["query_plan_result"]["final_match_count"],
        serde_json::json!(1)
    );
    assert_eq!(
        diagnosed_live["primary_diagnosis"],
        diagnosed_live["query_plan_result"]["diagnosis"]
    );
    assert_eq!(
        diagnosed_live["primary_diagnosis"]["status"],
        serde_json::json!("matched")
    );
    assert!(
        diagnosed_live["query_plan_request"]["arguments"]
            .get("diagnose")
            .is_none()
    );
    let read_batch = runtime.dispatch(ToolRequest {
        id: serde_json::json!("read-live-batch"),
        tool: live["read_batch_request"]["tool"]
            .as_str()
            .unwrap()
            .to_string(),
        arguments: live["read_batch_request"]["arguments"].clone(),
    });
    assert!(read_batch.error.is_none(), "{:?}", read_batch.error);
    let read_batch = read_batch.result.unwrap();
    assert_eq!(read_batch["summary"]["range_count"], serde_json::json!(1));
    assert!(
        read_batch["ranges"][0]["text"]
            .as_str()
            .unwrap()
            .contains("issue_token")
    );

    let empty_live = runtime.dispatch(ToolRequest {
        id: serde_json::json!("empty-live"),
        tool: "search_auto".to_string(),
        arguments: serde_json::json!({
            "repo": repo.path(),
            "query": "issue_token definitely_missing",
            "limit": 5
        }),
    });
    assert!(empty_live.error.is_none(), "{:?}", empty_live.error);
    let empty_live = empty_live.result.unwrap();
    assert!(empty_live["results"].as_array().unwrap().is_empty());
    assert_eq!(
        empty_live["summary"]["status"],
        serde_json::json!("not_found")
    );
    assert_eq!(empty_live["summary"]["result_count"], serde_json::json!(0));
    assert_eq!(
        empty_live["query_plan_result"]["repair_hints"][0]["kind"],
        "drop_missing_terms"
    );
    assert_eq!(
        empty_live["query_plan_result"]["repair_hints"][0]["action"],
        "drop_terms"
    );
    assert_eq!(
        empty_live["primary_diagnosis"],
        empty_live["query_plan_result"]["diagnosis"]
    );
    assert_eq!(
        empty_live["primary_diagnosis"]["status"],
        serde_json::json!("missing_terms")
    );
    assert_eq!(
        empty_live["primary_diagnosis"]["primary_hint_kind"],
        serde_json::json!("drop_missing_terms")
    );
    assert_eq!(
        empty_live["primary_diagnosis"]["primary_hint_action"],
        serde_json::json!("drop_terms")
    );
    assert_eq!(
        empty_live["query_plan_result"]["retry_requests"][0]["tool"],
        "search_code"
    );
    assert!(
        empty_live["query_plan_result"]["retry_requests"][0]["cli"]
            .as_str()
            .unwrap()
            .contains("orient search --repo")
    );
    let retry = runtime.dispatch(ToolRequest {
        id: serde_json::json!("empty-live-retry"),
        tool: empty_live["query_plan_result"]["retry_requests"][0]["tool"]
            .as_str()
            .unwrap()
            .to_string(),
        arguments: empty_live["query_plan_result"]["retry_requests"][0]["arguments"].clone(),
    });
    assert!(retry.error.is_none(), "{:?}", retry.error);
    assert!(
        serde_json::to_string(&retry.result)
            .unwrap()
            .contains("src/auth.rs")
    );
    let auto_retry = runtime.dispatch(ToolRequest {
        id: serde_json::json!("empty-live-auto-retry"),
        tool: "search_auto".to_string(),
        arguments: serde_json::json!({
            "repo": repo.path(),
            "query": "issue_token definitely_missing",
            "limit": 5,
            "retry_if_empty": true
        }),
    });
    assert!(auto_retry.error.is_none(), "{:?}", auto_retry.error);
    let auto_retry = auto_retry.result.unwrap();
    assert!(auto_retry["results"].as_array().unwrap().is_empty());
    assert_eq!(
        auto_retry["primary_retry_result"]["request"],
        auto_retry["primary_retry_request"]
    );
    assert!(
        serde_json::to_string(&auto_retry["primary_retry_result"]["results"])
            .unwrap()
            .contains("src/auth.rs")
    );
    assert_eq!(
        auto_retry["primary_retry_result"]["summary"]["status"],
        serde_json::json!("matched")
    );
    assert_eq!(
        auto_retry["primary_retry_result"]["summary"]["result_count"],
        serde_json::json!(1)
    );
    assert_eq!(
        auto_retry["primary_retry_result"]["summary"]["top_paths"],
        serde_json::json!(["src/auth.rs"])
    );
    assert_eq!(
        auto_retry["primary_retry_result"]["summary"]["top_dirs"],
        serde_json::json!(["src"])
    );
    assert_eq!(
        auto_retry["primary_retry_result"]["summary"]["top_exts"],
        serde_json::json!(["rs"])
    );
    assert_eq!(
        auto_retry["primary_retry_result"]["summary"]["top_langs"],
        serde_json::json!(["rust"])
    );
    assert_eq!(
        auto_retry["primary_retry_result"]["read_batch_request"]["tool"],
        serde_json::json!("read_ranges")
    );
    assert_eq!(
        auto_retry["primary_retry_result"]["read_batch_request"]["arguments"]["repo"],
        serde_json::json!(repo.path())
    );
    assert_eq!(
        auto_retry["primary_retry_result"]["read_batch_request"]["arguments"]["ranges"][0]["path"],
        serde_json::json!("src/auth.rs")
    );
    assert_eq!(
        auto_retry["next_read_batch_request"],
        auto_retry["primary_retry_result"]["read_batch_request"]
    );
    assert_eq!(
        auto_retry["next_action"]["source"],
        serde_json::json!("next_read_batch_request")
    );
    assert_eq!(
        auto_retry["next_action"]["request"],
        auto_retry["next_read_batch_request"]
    );

    let git_scope_miss = runtime.dispatch(ToolRequest {
        id: serde_json::json!("git-scope-miss"),
        tool: "search_auto".to_string(),
        arguments: serde_json::json!({
            "repo": repo.path(),
            "query": "branch:not-real-branch issue_token",
            "limit": 5
        }),
    });
    assert!(git_scope_miss.error.is_none(), "{:?}", git_scope_miss.error);
    let git_scope_miss = git_scope_miss.result.unwrap();
    assert_eq!(
        git_scope_miss["query_plan_result"]["repair_hints"][0]["kind"],
        "relax_branch_filter"
    );
    assert_eq!(
        git_scope_miss["primary_diagnosis"]["status"],
        serde_json::json!("scope_mismatch")
    );
    assert_eq!(
        git_scope_miss["query_plan_result"]["retry_requests"][0]["arguments"]["query"],
        "issue_token"
    );
    assert!(
        git_scope_miss["query_plan_result"]["retry_requests"][0]["arguments"]
            .get("branch")
            .is_none()
    );

    let index_path = repo.path().join("orient.index");
    let ensure = runtime.dispatch(ToolRequest {
        id: serde_json::json!("ensure"),
        tool: "ensure_index".to_string(),
        arguments: serde_json::json!({
            "repo": repo.path(),
            "index": index_path
        }),
    });
    assert!(ensure.error.is_none(), "{:?}", ensure.error);

    let explicit_live_after_warm = runtime.dispatch(ToolRequest {
        id: serde_json::json!("explicit-live-after-warm"),
        tool: "search_auto".to_string(),
        arguments: serde_json::json!({
            "repo": repo.path(),
            "query": "issue_token",
            "limit": 5
        }),
    });
    assert!(
        explicit_live_after_warm.error.is_none(),
        "{:?}",
        explicit_live_after_warm.error
    );
    let explicit_live_after_warm = explicit_live_after_warm.result.unwrap();
    assert_eq!(explicit_live_after_warm["surface"], "fallback");
    assert_eq!(
        explicit_live_after_warm["query_plan_request"]["tool"],
        "search_query_plan"
    );

    let indexed = runtime.dispatch(ToolRequest {
        id: serde_json::json!("indexed"),
        tool: "search_auto".to_string(),
        arguments: serde_json::json!({
            "query": "issue_token",
            "limit": 5
        }),
    });
    assert!(indexed.error.is_none(), "{:?}", indexed.error);
    let indexed = indexed.result.unwrap();
    assert_eq!(indexed["surface"], "indexed");
    assert_eq!(indexed["summary"]["status"], serde_json::json!("matched"));
    assert_eq!(indexed["summary"]["result_count"], serde_json::json!(1));
    assert_eq!(indexed["query_plan_request"]["tool"], "indexed_query_plan");
    assert_eq!(indexed["repo_map_request"]["tool"], "repo_map");
    assert_eq!(
        indexed["repo_map_request"]["arguments"]["index"],
        serde_json::json!(index_path.canonicalize().unwrap())
    );
    assert_eq!(
        indexed["repo_map_request"]["arguments"]["detail"],
        "compact"
    );
    assert_eq!(
        indexed["repo_map_request"]["arguments"]["read_limit"],
        serde_json::json!(DEFAULT_REPO_MAP_READ_BATCH_RANGES)
    );
    assert_eq!(indexed["results"][0]["read_request"]["tool"], "read_range");
    assert_eq!(
        indexed["results"][0]["read_request"]["arguments"]["index"],
        serde_json::json!(index_path.canonicalize().unwrap())
    );
    assert_eq!(indexed["read_batch_request"]["tool"], "read_ranges");

    let empty_indexed = runtime.dispatch(ToolRequest {
        id: serde_json::json!("empty-indexed"),
        tool: "search_auto".to_string(),
        arguments: serde_json::json!({
            "query": "issue_token definitely_missing",
            "limit": 5
        }),
    });
    assert!(empty_indexed.error.is_none(), "{:?}", empty_indexed.error);
    let empty_indexed = empty_indexed.result.unwrap();
    assert_eq!(empty_indexed["surface"], "indexed");
    assert!(empty_indexed["results"].as_array().unwrap().is_empty());
    assert_eq!(
        empty_indexed["summary"]["status"],
        serde_json::json!("not_found")
    );
    assert_eq!(
        empty_indexed["summary"]["result_count"],
        serde_json::json!(0)
    );
    assert_eq!(
        empty_indexed["query_plan_result"]["retry_requests"][0]["tool"],
        "indexed_search_code"
    );
    assert_eq!(
        empty_indexed["primary_diagnosis"],
        empty_indexed["query_plan_result"]["diagnosis"]
    );
    assert_eq!(
        empty_indexed["primary_diagnosis"]["status"],
        serde_json::json!("missing_terms")
    );
    assert_eq!(
        empty_indexed["primary_retry_request"],
        empty_indexed["query_plan_result"]["retry_requests"][0]
    );
    let auto_retry_indexed = runtime.dispatch(ToolRequest {
        id: serde_json::json!("empty-indexed-auto-retry"),
        tool: "search_auto".to_string(),
        arguments: serde_json::json!({
            "query": "issue_token definitely_missing",
            "limit": 5,
            "retry_if_empty": true
        }),
    });
    assert!(
        auto_retry_indexed.error.is_none(),
        "{:?}",
        auto_retry_indexed.error
    );
    let auto_retry_indexed = auto_retry_indexed.result.unwrap();
    assert_eq!(
        auto_retry_indexed["primary_retry_result"]["request"],
        auto_retry_indexed["primary_retry_request"]
    );
    assert!(
        serde_json::to_string(&auto_retry_indexed["primary_retry_result"]["results"])
            .unwrap()
            .contains("src/auth.rs")
    );
    assert_eq!(
        auto_retry_indexed["primary_retry_result"]["read_batch_request"]["tool"],
        serde_json::json!("read_ranges")
    );
    assert_eq!(
        auto_retry_indexed["primary_retry_result"]["read_batch_request"]["arguments"]["index"],
        serde_json::json!(index_path.canonicalize().unwrap())
    );
    assert_eq!(
        auto_retry_indexed["primary_retry_result"]["read_batch_request"]["arguments"]["ranges"][0]
            ["path"],
        serde_json::json!("src/auth.rs")
    );
    assert_eq!(
        auto_retry_indexed["next_read_batch_request"],
        auto_retry_indexed["primary_retry_result"]["read_batch_request"]
    );

    let kind_typo = runtime.dispatch(ToolRequest {
        id: serde_json::json!("kind-typo"),
        tool: "search_auto".to_string(),
        arguments: serde_json::json!({
            "query": "kind:functoin",
            "limit": 5
        }),
    });
    assert!(kind_typo.error.is_none(), "{:?}", kind_typo.error);
    let kind_typo = kind_typo.result.unwrap();
    assert_eq!(
        kind_typo["query_plan_result"]["repair_hints"][0]["kind"],
        "replace_symbol_kind_filter"
    );
    assert_eq!(
        kind_typo["query_plan_result"]["repair_hints"][0]["action"],
        "replace_filter"
    );
    assert_eq!(
        kind_typo["primary_diagnosis"]["primary_hint_kind"],
        serde_json::json!("replace_symbol_kind_filter")
    );
    assert_eq!(
        kind_typo["query_plan_result"]["retry_requests"][0]["arguments"]["query"],
        "kind:function"
    );
    assert_eq!(
        kind_typo["primary_retry_request"],
        kind_typo["query_plan_result"]["retry_requests"][0]
    );
    assert!(
        kind_typo["query_plan_result"]["retry_requests"][0]["arguments"]
            .get("symbol_kind")
            .is_none(),
        "{:?}",
        kind_typo["query_plan_result"]["retry_requests"][0]["arguments"]
    );
    let kind_retry = runtime.dispatch(ToolRequest {
        id: serde_json::json!("kind-typo-retry"),
        tool: kind_typo["query_plan_result"]["retry_requests"][0]["tool"]
            .as_str()
            .unwrap()
            .to_string(),
        arguments: kind_typo["query_plan_result"]["retry_requests"][0]["arguments"].clone(),
    });
    assert!(kind_retry.error.is_none(), "{:?}", kind_retry.error);
    assert!(
        serde_json::to_string(&kind_retry.result)
            .unwrap()
            .contains("issue_token")
    );

    let symbol_typo = runtime.dispatch(ToolRequest {
        id: serde_json::json!("symbol-typo"),
        tool: "search_auto".to_string(),
        arguments: serde_json::json!({
            "query": "symbol:SessionManger",
            "limit": 5
        }),
    });
    assert!(symbol_typo.error.is_none(), "{:?}", symbol_typo.error);
    let symbol_typo = symbol_typo.result.unwrap();
    assert_eq!(
        symbol_typo["query_plan_result"]["repair_hints"][0]["kind"],
        "replace_symbol_filter"
    );
    assert_eq!(
        symbol_typo["primary_diagnosis"]["primary_hint_kind"],
        serde_json::json!("replace_symbol_filter")
    );
    assert_eq!(
        symbol_typo["primary_diagnosis"]["primary_hint_action"],
        serde_json::json!("replace_filter")
    );
    assert_eq!(
        symbol_typo["query_plan_result"]["retry_requests"][0]["arguments"]["query"],
        "symbol:SessionManager"
    );
    assert!(
        symbol_typo["query_plan_result"]["retry_requests"][0]["arguments"]
            .get("symbol")
            .is_none(),
        "{:?}",
        symbol_typo["query_plan_result"]["retry_requests"][0]["arguments"]
    );
    let symbol_retry = runtime.dispatch(ToolRequest {
        id: serde_json::json!("symbol-typo-retry"),
        tool: symbol_typo["query_plan_result"]["retry_requests"][0]["tool"]
            .as_str()
            .unwrap()
            .to_string(),
        arguments: symbol_typo["query_plan_result"]["retry_requests"][0]["arguments"].clone(),
    });
    assert!(symbol_retry.error.is_none(), "{:?}", symbol_retry.error);
    assert!(
        serde_json::to_string(&symbol_retry.result)
            .unwrap()
            .contains("SessionManager")
    );

    let symbol_typo_terms = runtime.dispatch(ToolRequest {
        id: serde_json::json!("symbol-typo-terms"),
        tool: "search_auto".to_string(),
        arguments: serde_json::json!({
            "query": "symbol:SessionManger issue_token",
            "limit": 5
        }),
    });
    assert!(
        symbol_typo_terms.error.is_none(),
        "{:?}",
        symbol_typo_terms.error
    );
    let symbol_typo_terms = symbol_typo_terms.result.unwrap();
    let symbol_drop_terms_retry = symbol_typo_terms["query_plan_result"]["retry_requests"]
        .as_array()
        .unwrap()
        .iter()
        .find(|request| {
            request["arguments"]["query"]
                .as_str()
                .is_some_and(|query| query == "issue token session")
        })
        .expect("expected symbol typo drop-terms retry");
    assert!(
        symbol_drop_terms_retry["arguments"].get("symbol").is_none(),
        "{symbol_drop_terms_retry:?}"
    );
    let symbol_drop_terms_retry_result = runtime.dispatch(ToolRequest {
        id: serde_json::json!("symbol-typo-drop-terms-retry"),
        tool: symbol_drop_terms_retry["tool"]
            .as_str()
            .unwrap()
            .to_string(),
        arguments: symbol_drop_terms_retry["arguments"].clone(),
    });
    assert!(
        symbol_drop_terms_retry_result.error.is_none(),
        "{:?}",
        symbol_drop_terms_retry_result.error
    );
    assert!(
        serde_json::to_string(&symbol_drop_terms_retry_result.result)
            .unwrap()
            .contains("issue_token")
    );

    let path_typo = runtime.dispatch(ToolRequest {
        id: serde_json::json!("path-typo"),
        tool: "search_auto".to_string(),
        arguments: serde_json::json!({
            "query": "path:src/ath.rs",
            "limit": 5
        }),
    });
    assert!(path_typo.error.is_none(), "{:?}", path_typo.error);
    let path_typo = path_typo.result.unwrap();
    assert_eq!(
        path_typo["query_plan_result"]["repair_hints"][0]["kind"],
        "replace_path_filter"
    );
    assert_eq!(
        path_typo["query_plan_result"]["retry_requests"][0]["arguments"]["query"],
        "path:src/auth.rs"
    );
    assert!(
        path_typo["query_plan_result"]["retry_requests"][0]["arguments"]
            .get("path")
            .is_none(),
        "{:?}",
        path_typo["query_plan_result"]["retry_requests"][0]["arguments"]
    );
    let path_retry = runtime.dispatch(ToolRequest {
        id: serde_json::json!("path-typo-retry"),
        tool: path_typo["query_plan_result"]["retry_requests"][0]["tool"]
            .as_str()
            .unwrap()
            .to_string(),
        arguments: path_typo["query_plan_result"]["retry_requests"][0]["arguments"].clone(),
    });
    assert!(path_retry.error.is_none(), "{:?}", path_retry.error);
    assert!(
        serde_json::to_string(&path_retry.result)
            .unwrap()
            .contains("src/auth.rs")
    );

    let file_path_typo = runtime.dispatch(ToolRequest {
        id: serde_json::json!("file-path-typo"),
        tool: "search_auto".to_string(),
        arguments: serde_json::json!({
            "query": "file:src/ath.rs",
            "limit": 5
        }),
    });
    assert!(file_path_typo.error.is_none(), "{:?}", file_path_typo.error);
    let file_path_typo = file_path_typo.result.unwrap();
    assert_eq!(
        file_path_typo["query_plan_result"]["repair_hints"][0]["kind"],
        "replace_file_filter"
    );
    assert_eq!(
        file_path_typo["query_plan_result"]["retry_requests"][0]["arguments"]["query"],
        "path:src/auth.rs"
    );
    assert!(
        file_path_typo["query_plan_result"]["retry_requests"][0]["arguments"]
            .get("file")
            .is_none(),
        "{:?}",
        file_path_typo["query_plan_result"]["retry_requests"][0]["arguments"]
    );
    let file_path_retry = runtime.dispatch(ToolRequest {
        id: serde_json::json!("file-path-typo-retry"),
        tool: file_path_typo["query_plan_result"]["retry_requests"][0]["tool"]
            .as_str()
            .unwrap()
            .to_string(),
        arguments: file_path_typo["query_plan_result"]["retry_requests"][0]["arguments"].clone(),
    });
    assert!(
        file_path_retry.error.is_none(),
        "{:?}",
        file_path_retry.error
    );
    assert!(
        serde_json::to_string(&file_path_retry.result)
            .unwrap()
            .contains("src/auth.rs")
    );

    let map_request = indexed["repo_map_request"].clone();
    let map = runtime.dispatch(ToolRequest {
        id: serde_json::json!("map"),
        tool: map_request["tool"].as_str().unwrap().to_string(),
        arguments: map_request["arguments"].clone(),
    });
    assert!(map.error.is_none(), "{:?}", map.error);
    assert!(map.result.unwrap()["brief"]["manifest_files"].is_array());
}

#[test]
fn runtime_search_alias_accepts_live_index_and_shard_targets() {
    let repo = tempfile::tempdir().unwrap();
    write(
        &repo.path().join("src/auth.rs"),
        "pub struct SessionManager;\npub fn issue_token() {}\n",
    );
    write(
        &repo.path().join("Cargo.toml"),
        "[package]\nname='sample'\nversion='0.1.0'\nedition='2024'\n",
    );
    let index_path = repo.path().join(".orient/index");
    FastIndex::build(repo.path())
        .unwrap()
        .save(&index_path)
        .unwrap();
    let shard_dir = repo.path().join(".orient-shards");
    build_shards(&[repo.path().to_path_buf()], &shard_dir).unwrap();

    let runtime = ToolRuntime::default();
    let live = runtime.dispatch(ToolRequest {
        id: serde_json::json!("live"),
        tool: "search".to_string(),
        arguments: serde_json::json!({
            "repo": repo.path(),
            "query": "issue token",
            "limit": 3,
            "require_all": true
        }),
    });
    assert!(live.error.is_none(), "{:?}", live.error);
    let live = live.result.unwrap();
    assert_eq!(live[0]["path"], "src/auth.rs");
    assert_eq!(live[0]["read_request"]["tool"], "read_range");

    let indexed = runtime.dispatch(ToolRequest {
        id: serde_json::json!("indexed"),
        tool: "search".to_string(),
        arguments: serde_json::json!({
            "index": index_path,
            "query": "issue token",
            "limit": 3,
            "require_all": true
        }),
    });
    assert!(indexed.error.is_none(), "{:?}", indexed.error);
    let indexed = indexed.result.unwrap();
    assert_eq!(indexed[0]["path"], "src/auth.rs");
    assert_eq!(indexed[0]["read_request"]["tool"], "read_range");
    assert_eq!(indexed[0]["related_request"]["tool"], "related_files");
    assert_eq!(
        indexed[0]["related_symbols_request"]["tool"],
        "related_symbols"
    );
    assert_eq!(
        indexed[0]["read_request"]["arguments"]["index"],
        serde_json::json!(index_path)
    );

    let indexed_symbol_query = runtime.dispatch(ToolRequest {
        id: serde_json::json!("indexed-symbol-query"),
        tool: "search".to_string(),
        arguments: serde_json::json!({
            "index": repo.path().join(".orient/index"),
            "query": "symbol:SessionManager",
            "limit": 3
        }),
    });
    assert!(
        indexed_symbol_query.error.is_none(),
        "{:?}",
        indexed_symbol_query.error
    );
    let indexed_symbol_query = indexed_symbol_query.result.unwrap();
    assert_eq!(
        indexed_symbol_query[0]["read_range"]["scope"],
        serde_json::json!("symbol")
    );
    assert_eq!(
        indexed_symbol_query[0]["read_request"]["arguments"]["range"]["scope"],
        serde_json::json!("symbol")
    );

    let symbol_snippet = runtime.dispatch(ToolRequest {
        id: serde_json::json!("indexed-symbol-snippet"),
        tool: "search".to_string(),
        arguments: serde_json::json!({
            "index": repo.path().join(".orient/index"),
            "query": "SessionManager",
            "limit": 3,
            "snippet": "symbol"
        }),
    });
    assert!(symbol_snippet.error.is_none(), "{:?}", symbol_snippet.error);
    let symbol_snippet = symbol_snippet.result.unwrap();
    assert_eq!(
        symbol_snippet[0]["read_range"]["scope"],
        serde_json::json!("symbol")
    );
    assert_eq!(
        symbol_snippet[0]["read_request"]["arguments"]["range"]["scope"],
        serde_json::json!("symbol")
    );

    let hyphen_snippet = runtime.dispatch(ToolRequest {
        id: serde_json::json!("hyphen-symbol-snippet"),
        tool: "search".to_string(),
        arguments: serde_json::json!({
            "repo": repo.path(),
            "query": "SessionManager",
            "limit": 3,
            "snippet-mode": "symbol"
        }),
    });
    assert!(hyphen_snippet.error.is_none(), "{:?}", hyphen_snippet.error);
    let hyphen_snippet = hyphen_snippet.result.unwrap();
    assert_eq!(
        hyphen_snippet[0]["read_range"]["scope"],
        serde_json::json!("symbol")
    );
    assert_eq!(
        hyphen_snippet[0]["read_request"]["arguments"]["range"]["scope"],
        serde_json::json!("symbol")
    );

    let sharded = runtime.dispatch(ToolRequest {
        id: serde_json::json!("sharded"),
        tool: "search".to_string(),
        arguments: serde_json::json!({
            "index_dir": shard_dir,
            "query": "issue token",
            "limit": 3,
            "require_all": true
        }),
    });
    assert!(sharded.error.is_none(), "{:?}", sharded.error);
    let sharded = sharded.result.unwrap();
    assert!(
        sharded[0]["path"]
            .as_str()
            .unwrap()
            .ends_with("src/auth.rs")
    );
    assert_eq!(sharded[0]["read_request"]["tool"], "read_range");
    assert_eq!(sharded[0]["related_request"]["tool"], "related_files");
    assert_eq!(
        sharded[0]["related_symbols_request"]["tool"],
        "related_symbols"
    );
    assert_eq!(
        sharded[0]["read_request"]["arguments"]["index_dir"],
        serde_json::json!(shard_dir)
    );

    let conflicted = runtime.dispatch(ToolRequest {
        id: serde_json::json!("conflicted"),
        tool: "search".to_string(),
        arguments: serde_json::json!({
            "index": repo.path().join(".orient/index"),
            "index_dir": repo.path().join(".orient-shards"),
            "query": "issue token"
        }),
    });
    assert!(
        conflicted
            .error
            .as_ref()
            .unwrap()
            .contains("only one of index or index_dir"),
        "{:?}",
        conflicted.error
    );

    let live_batch = runtime.dispatch(ToolRequest {
        id: serde_json::json!("live-batch"),
        tool: "search_batch".to_string(),
        arguments: serde_json::json!({
            "repo": repo.path(),
            "queries": ["issue token", "SessionManager"],
            "limit": 3,
            "require_all": true
        }),
    });
    assert!(live_batch.error.is_none(), "{:?}", live_batch.error);
    let live_batch = live_batch.result.unwrap();
    assert_eq!(live_batch[0]["read_batch_request"]["tool"], "read_ranges");
    assert_eq!(
        live_batch[0]["next_action"]["source"],
        serde_json::json!("read_batch_request")
    );
    assert_eq!(
        live_batch[0]["next_action"]["request"],
        live_batch[0]["read_batch_request"]
    );
    assert_eq!(
        live_batch[0]["results"][0]["read_request"]["tool"],
        "read_range"
    );
    assert_eq!(live_batch[0]["query_plan_request"]["tool"], "search_plan");
    assert_eq!(live_batch[0]["repo_map_request"]["tool"], "repo_map");
    assert_eq!(
        live_batch[0]["query_plan_request"]["arguments"]["repo"],
        serde_json::json!(repo.path())
    );
    assert_eq!(
        live_batch[0]["repo_map_request"]["arguments"]["repo"],
        serde_json::json!(repo.path())
    );
    assert_eq!(
        live_batch[0]["repo_map_request"]["arguments"]["detail"],
        "compact"
    );
    assert_eq!(
        live_batch[0]["repo_map_request"]["arguments"]["read_limit"],
        DEFAULT_REPO_MAP_READ_BATCH_RANGES
    );

    let indexed_batch = runtime.dispatch(ToolRequest {
        id: serde_json::json!("indexed-batch"),
        tool: "search_batch".to_string(),
        arguments: serde_json::json!({
            "index": repo.path().join(".orient/index"),
            "queries": ["issue token", "SessionManager"],
            "limit": 3,
            "require_all": true
        }),
    });
    assert!(indexed_batch.error.is_none(), "{:?}", indexed_batch.error);
    let indexed_batch = indexed_batch.result.unwrap();
    assert_eq!(
        indexed_batch[0]["read_batch_request"]["tool"],
        "read_ranges"
    );
    assert_eq!(
        indexed_batch[0]["next_action"]["request"],
        indexed_batch[0]["read_batch_request"]
    );
    assert_eq!(
        indexed_batch[0]["results"][0]["read_request"]["tool"],
        "read_range"
    );
    assert_eq!(
        indexed_batch[0]["results"][0]["read_request"]["arguments"]["index"],
        serde_json::json!(repo.path().join(".orient/index"))
    );
    assert_eq!(
        indexed_batch[0]["query_plan_request"]["tool"],
        "search_plan"
    );
    assert_eq!(indexed_batch[0]["repo_map_request"]["tool"], "repo_map");
    assert_eq!(
        indexed_batch[0]["query_plan_request"]["arguments"]["index"],
        serde_json::json!(repo.path().join(".orient/index"))
    );
    assert_eq!(
        indexed_batch[0]["repo_map_request"]["arguments"]["index"],
        serde_json::json!(repo.path().join(".orient/index"))
    );

    let shard_batch = runtime.dispatch(ToolRequest {
        id: serde_json::json!("shard-batch"),
        tool: "search_batch".to_string(),
        arguments: serde_json::json!({
            "index_dir": repo.path().join(".orient-shards"),
            "queries": ["issue token", "SessionManager"],
            "limit": 3,
            "require_all": true
        }),
    });
    assert!(shard_batch.error.is_none(), "{:?}", shard_batch.error);
    let shard_batch = shard_batch.result.unwrap();
    assert_eq!(shard_batch[0]["read_batch_request"]["tool"], "read_ranges");
    assert_eq!(
        shard_batch[0]["next_action"]["request"],
        shard_batch[0]["read_batch_request"]
    );
    assert_eq!(
        shard_batch[0]["results"][0]["read_request"]["tool"],
        "read_range"
    );
    assert_eq!(
        shard_batch[0]["results"][0]["read_request"]["arguments"]["index_dir"],
        serde_json::json!(repo.path().join(".orient-shards"))
    );
    assert_eq!(shard_batch[0]["query_plan_request"]["tool"], "search_plan");
    assert_eq!(shard_batch[0]["repo_map_request"]["tool"], "repo_map");
    assert_eq!(
        shard_batch[0]["query_plan_request"]["arguments"]["index_dir"],
        serde_json::json!(repo.path().join(".orient-shards"))
    );
    assert_eq!(
        shard_batch[0]["repo_map_request"]["arguments"]["index_dir"],
        serde_json::json!(repo.path().join(".orient-shards"))
    );

    let conflicted_batch = runtime.dispatch(ToolRequest {
        id: serde_json::json!("conflicted-batch"),
        tool: "search_batch".to_string(),
        arguments: serde_json::json!({
            "index": repo.path().join(".orient/index"),
            "index_dir": repo.path().join(".orient-shards"),
            "queries": ["issue token"]
        }),
    });
    assert!(
        conflicted_batch
            .error
            .as_ref()
            .unwrap()
            .contains("only one of index or index_dir"),
        "{:?}",
        conflicted_batch.error
    );
}

#[test]
fn runtime_search_auto_summary_reports_grouped_duplicates() {
    let repo = tempfile::tempdir().unwrap();
    write(
        &repo.path().join("one/src/auth.rs"),
        "pub fn issue_token() { let token = \"session\"; }\n",
    );
    write(
        &repo.path().join("two/src/auth.rs"),
        "pub fn issue_token() { let token = \"session\"; }\n",
    );

    let runtime = ToolRuntime::default();
    let result = runtime.dispatch(ToolRequest {
        id: serde_json::json!("dedupe-live"),
        tool: "search_auto".to_string(),
        arguments: serde_json::json!({
            "repo": repo.path(),
            "query": "issue token session",
            "limit": 10
        }),
    });
    assert!(result.error.is_none(), "{:?}", result.error);
    let result = result.result.unwrap();
    assert_eq!(
        result["summary"]["grouped_duplicate_count"],
        serde_json::json!(1)
    );
    assert_eq!(
        result["read_batch_request"]["read_budget"]["grouped_duplicate_count"],
        serde_json::json!(1)
    );

    let retry = runtime.dispatch(ToolRequest {
        id: serde_json::json!("dedupe-live-retry"),
        tool: "search_auto".to_string(),
        arguments: serde_json::json!({
            "repo": repo.path(),
            "query": "issue token session definitely_missing",
            "limit": 10,
            "retry_if_empty": true
        }),
    });
    assert!(retry.error.is_none(), "{:?}", retry.error);
    let retry = retry.result.unwrap();
    assert_eq!(
        retry["primary_retry_result"]["summary"]["grouped_duplicate_count"],
        serde_json::json!(1)
    );
}

#[test]
fn runtime_read_alias_accepts_live_index_and_shard_targets() {
    let repo = tempfile::tempdir().unwrap();
    write(
        &repo.path().join("src/auth.rs"),
        "pub struct SessionManager;\npub fn issue_token() {}\n",
    );
    write(
        &repo.path().join("Cargo.toml"),
        "[package]\nname='sample'\nversion='0.1.0'\nedition='2024'\n",
    );
    let index_path = repo.path().join(".orient/index");
    FastIndex::build(repo.path())
        .unwrap()
        .save(&index_path)
        .unwrap();
    let shard_dir = repo.path().join(".orient-shards");
    build_shards(&[repo.path().to_path_buf()], &shard_dir).unwrap();

    let runtime = ToolRuntime::default();
    let live = runtime.dispatch(ToolRequest {
        id: serde_json::json!("live-read"),
        tool: "read_range".to_string(),
        arguments: serde_json::json!({
            "repo": repo.path(),
            "path": "src/auth.rs",
            "start": 2,
            "lines": 1
        }),
    });
    assert!(live.error.is_none(), "{:?}", live.error);
    assert_eq!(live.result.as_ref().unwrap()["path"], "src/auth.rs");
    assert!(
        live.result.as_ref().unwrap()["text"]
            .as_str()
            .unwrap()
            .contains("issue_token")
    );

    let live_line_range = runtime.dispatch(ToolRequest {
        id: serde_json::json!("live-line-range-read"),
        tool: "open_range".to_string(),
        arguments: serde_json::json!({
            "repo": repo.path(),
            "path": "src/auth.rs",
            "start_line": 1,
            "end_line": 2
        }),
    });
    assert!(
        live_line_range.error.is_none(),
        "{:?}",
        live_line_range.error
    );
    let live_line_range = live_line_range.result.unwrap();
    assert_eq!(live_line_range["start_line"], serde_json::json!(1));
    assert_eq!(live_line_range["end_line"], serde_json::json!(2));

    let live_line_alias_range = runtime.dispatch(ToolRequest {
        id: serde_json::json!("live-line-alias-range-read"),
        tool: "open_range".to_string(),
        arguments: serde_json::json!({
            "repo": repo.path(),
            "path": "src/auth.rs",
            "line": 1,
            "end": 2
        }),
    });
    assert!(
        live_line_alias_range.error.is_none(),
        "{:?}",
        live_line_alias_range.error
    );
    let live_line_alias_range = live_line_alias_range.result.unwrap();
    assert_eq!(live_line_alias_range["start_line"], serde_json::json!(1));
    assert_eq!(live_line_alias_range["end_line"], serde_json::json!(2));

    let live_range_object = runtime.dispatch(ToolRequest {
        id: serde_json::json!("live-single-range-object"),
        tool: "open_range".to_string(),
        arguments: serde_json::json!({
            "repo": repo.path(),
            "range": {
                "path": "src/auth.rs",
                "start_line": 2,
                "end_line": 2
            }
        }),
    });
    assert!(
        live_range_object.error.is_none(),
        "{:?}",
        live_range_object.error
    );
    assert_eq!(
        live_range_object.result.as_ref().unwrap()["start_line"],
        serde_json::json!(2)
    );

    let live_kebab_range_object = runtime.dispatch(ToolRequest {
        id: serde_json::json!("live-kebab-range-object"),
        tool: "open_range".to_string(),
        arguments: serde_json::json!({
            "repo": repo.path(),
            "range": {
                "path": "src/auth.rs",
                "target-line": 1,
                "end": 2
            }
        }),
    });
    assert!(
        live_kebab_range_object.error.is_none(),
        "{:?}",
        live_kebab_range_object.error
    );
    let live_kebab_range_object = live_kebab_range_object.result.unwrap();
    assert_eq!(live_kebab_range_object["start_line"], serde_json::json!(1));
    assert_eq!(live_kebab_range_object["end_line"], serde_json::json!(2));

    let indexed = runtime.dispatch(ToolRequest {
        id: serde_json::json!("indexed-read"),
        tool: "read_range".to_string(),
        arguments: serde_json::json!({
            "index": repo.path().join(".orient/index"),
            "path": "src/auth.rs",
            "start": 1,
            "lines": 1
        }),
    });
    assert!(indexed.error.is_none(), "{:?}", indexed.error);
    assert_eq!(indexed.result.as_ref().unwrap()["path"], "src/auth.rs");
    assert!(
        indexed.result.as_ref().unwrap()["text"]
            .as_str()
            .unwrap()
            .contains("SessionManager")
    );

    let indexed_copied_location = runtime.dispatch(ToolRequest {
        id: serde_json::json!("indexed-copied-location"),
        tool: "read_index_range".to_string(),
        arguments: serde_json::json!({
            "index": repo.path().join(".orient/index"),
            "path": "src/auth.rs#L2-L2"
        }),
    });
    assert!(
        indexed_copied_location.error.is_none(),
        "{:?}",
        indexed_copied_location.error
    );
    assert_eq!(
        indexed_copied_location.result.as_ref().unwrap()["start_line"],
        serde_json::json!(2)
    );
    assert!(
        indexed_copied_location.result.as_ref().unwrap()["text"]
            .as_str()
            .unwrap()
            .contains("issue_token")
    );

    let indexed_copied_column_location = runtime.dispatch(ToolRequest {
        id: serde_json::json!("indexed-copied-column-location"),
        tool: "read_index_range".to_string(),
        arguments: serde_json::json!({
            "index": repo.path().join(".orient/index"),
            "path": "Cargo.toml#L1C1-L2C1"
        }),
    });
    assert!(
        indexed_copied_column_location.error.is_none(),
        "{:?}",
        indexed_copied_column_location.error
    );
    assert_eq!(
        indexed_copied_column_location.result.as_ref().unwrap()["path"],
        serde_json::json!("Cargo.toml")
    );
    assert_eq!(
        indexed_copied_column_location.result.as_ref().unwrap()["start_line"],
        serde_json::json!(1)
    );
    assert_eq!(
        indexed_copied_column_location.result.as_ref().unwrap()["end_line"],
        serde_json::json!(2)
    );
    assert_eq!(
        indexed_copied_column_location.result.as_ref().unwrap()["summary"]["line_count"],
        serde_json::json!(2)
    );

    let live_bitbucket_range = runtime.dispatch(ToolRequest {
        id: serde_json::json!("live-bitbucket-range"),
        tool: "read_range".to_string(),
        arguments: serde_json::json!({
            "repo": repo.path(),
            "range": "https://bitbucket.org/evalops/orient-search/src/main/Cargo.toml#lines-1:2"
        }),
    });
    assert!(
        live_bitbucket_range.error.is_none(),
        "{:?}",
        live_bitbucket_range.error
    );
    let live_bitbucket_range = live_bitbucket_range.result.unwrap();
    assert_eq!(
        live_bitbucket_range["path"],
        serde_json::json!("Cargo.toml")
    );
    assert_eq!(live_bitbucket_range["start_line"], serde_json::json!(1));
    assert_eq!(live_bitbucket_range["end_line"], serde_json::json!(2));
    assert_eq!(
        live_bitbucket_range["summary"]["line_count"],
        serde_json::json!(2)
    );

    let indexed_range_string = runtime.dispatch(ToolRequest {
        id: serde_json::json!("indexed-range-string"),
        tool: "read_index_range".to_string(),
        arguments: serde_json::json!({
            "index": repo.path().join(".orient/index"),
            "range": "src/auth.rs:2:1"
        }),
    });
    assert!(
        indexed_range_string.error.is_none(),
        "{:?}",
        indexed_range_string.error
    );
    assert_eq!(
        indexed_range_string.result.as_ref().unwrap()["start_line"],
        serde_json::json!(2)
    );

    let symbol_scoped = runtime.dispatch(ToolRequest {
        id: serde_json::json!("symbol-scoped-read"),
        tool: "read_range".to_string(),
        arguments: serde_json::json!({
            "index": repo.path().join(".orient/index"),
            "path": "src/auth.rs",
            "start": 2,
            "lines": 1,
            "scope": "symbol"
        }),
    });
    assert!(symbol_scoped.error.is_none(), "{:?}", symbol_scoped.error);
    let symbol_scoped = symbol_scoped.result.unwrap();
    assert_eq!(symbol_scoped["start_line"], 1);
    assert_eq!(symbol_scoped["end_line"], 2);
    assert_eq!(symbol_scoped["symbol"]["name"], "issue_token");
    assert!(
        symbol_scoped["text"]
            .as_str()
            .unwrap()
            .contains("SessionManager")
    );

    let sharded = runtime.dispatch(ToolRequest {
        id: serde_json::json!("shard-read"),
        tool: "read_range".to_string(),
        arguments: serde_json::json!({
            "index_dir": repo.path().join(".orient-shards"),
            "path": "src/auth.rs",
            "start": 2,
            "lines": 1
        }),
    });
    assert!(sharded.error.is_none(), "{:?}", sharded.error);
    assert!(
        sharded.result.as_ref().unwrap()["path"]
            .as_str()
            .unwrap()
            .ends_with("src/auth.rs")
    );
    assert!(
        sharded.result.as_ref().unwrap()["text"]
            .as_str()
            .unwrap()
            .contains("issue_token")
    );

    let sharded_kebab = runtime.dispatch(ToolRequest {
        id: serde_json::json!("shard-kebab-read"),
        tool: "read_range".to_string(),
        arguments: serde_json::json!({
            "index-dir": repo.path().join(".orient-shards"),
            "path": "src/auth.rs",
            "start-line": 1,
            "line-count": 1
        }),
    });
    assert!(sharded_kebab.error.is_none(), "{:?}", sharded_kebab.error);
    assert!(
        sharded_kebab.result.as_ref().unwrap()["text"]
            .as_str()
            .unwrap()
            .contains("SessionManager")
    );

    let indexed_batch = runtime.dispatch(ToolRequest {
        id: serde_json::json!("indexed-read-batch"),
        tool: "read_ranges".to_string(),
        arguments: serde_json::json!({
            "index": repo.path().join(".orient/index"),
            "ranges": [
                {"path": "Cargo.toml", "start": 1, "lines": 1},
                {"path": "src/auth.rs", "start": 2, "lines": 1, "scope": "symbol"},
                {"path": "src/auth.rs", "start_line": 1, "line_count": 2}
            ]
        }),
    });
    assert!(indexed_batch.error.is_none(), "{:?}", indexed_batch.error);
    let indexed_batch = indexed_batch.result.unwrap();
    assert_eq!(indexed_batch.as_array().unwrap().len(), 3);
    assert!(
        indexed_batch[1]["text"]
            .as_str()
            .unwrap()
            .contains("issue_token")
    );
    assert_eq!(indexed_batch[1]["symbol"]["name"], "issue_token");

    let indexed_batch_summary = runtime.dispatch(ToolRequest {
        id: serde_json::json!("indexed-read-batch-summary"),
        tool: "read_ranges".to_string(),
        arguments: serde_json::json!({
            "index": repo.path().join(".orient/index"),
            "include_summary": true,
            "ranges": [
                {"path": "Cargo.toml", "start": 1, "lines": 1},
                {"path": "src/auth.rs", "start": 1, "lines": 2}
            ]
        }),
    });
    assert!(
        indexed_batch_summary.error.is_none(),
        "{:?}",
        indexed_batch_summary.error
    );
    let indexed_batch_summary = indexed_batch_summary.result.unwrap();
    assert_eq!(
        indexed_batch_summary["summary"]["status"],
        serde_json::json!("read")
    );
    assert_eq!(
        indexed_batch_summary["summary"]["range_count"],
        serde_json::json!(2)
    );
    assert_eq!(
        indexed_batch_summary["summary"]["total_lines"],
        serde_json::json!(3)
    );
    assert_eq!(
        indexed_batch_summary["summary"]["path_count"],
        serde_json::json!(2)
    );
    assert_eq!(
        indexed_batch_summary["summary"]["top_dirs"],
        serde_json::json!([".", "src"])
    );
    assert_eq!(
        indexed_batch_summary["summary"]["top_exts"],
        serde_json::json!(["toml", "rs"])
    );
    assert_eq!(
        indexed_batch_summary["ranges"][1]["path"],
        serde_json::json!("src/auth.rs")
    );

    let shard_batch = runtime.dispatch(ToolRequest {
        id: serde_json::json!("shard-read-batch"),
        tool: "read_ranges".to_string(),
        arguments: serde_json::json!({
            "index_dir": repo.path().join(".orient-shards"),
            "ranges": {"path": "src/auth.rs", "start": 1, "lines": 1}
        }),
    });
    assert!(shard_batch.error.is_none(), "{:?}", shard_batch.error);
    assert!(
        shard_batch.result.unwrap()[0]["text"]
            .as_str()
            .unwrap()
            .contains("SessionManager")
    );

    let shard_line_range = runtime.dispatch(ToolRequest {
        id: serde_json::json!("shard-line-range-read"),
        tool: "open_shard_range".to_string(),
        arguments: serde_json::json!({
            "index_dir": repo.path().join(".orient-shards"),
            "path": "src/auth.rs",
            "start_line": 2,
            "end_line": 2
        }),
    });
    assert!(
        shard_line_range.error.is_none(),
        "{:?}",
        shard_line_range.error
    );
    assert_eq!(
        shard_line_range.result.as_ref().unwrap()["start_line"],
        serde_json::json!(2)
    );

    let shard_range_object = runtime.dispatch(ToolRequest {
        id: serde_json::json!("shard-single-range-object"),
        tool: "read_shard_range".to_string(),
        arguments: serde_json::json!({
            "index_dir": repo.path().join(".orient-shards"),
            "range": {
                "path": "src/auth.rs",
                "start": 1,
                "lines": 1
            }
        }),
    });
    assert!(
        shard_range_object.error.is_none(),
        "{:?}",
        shard_range_object.error
    );
    assert_eq!(
        shard_range_object.result.as_ref().unwrap()["start_line"],
        serde_json::json!(1)
    );

    let conflicted = runtime.dispatch(ToolRequest {
        id: serde_json::json!("conflicted-read"),
        tool: "read_range".to_string(),
        arguments: serde_json::json!({
            "index": repo.path().join(".orient/index"),
            "index_dir": repo.path().join(".orient-shards"),
            "path": "src/auth.rs"
        }),
    });
    assert!(
        conflicted
            .error
            .as_ref()
            .unwrap()
            .contains("only one of index or index_dir"),
        "{:?}",
        conflicted.error
    );
}

#[test]
fn runtime_related_alias_accepts_live_index_and_shard_targets() {
    let repo = tempfile::tempdir().unwrap();
    write(
        &repo.path().join("src/auth.rs"),
        "pub struct SessionManager;\npub fn issue_token() {}\n",
    );
    write(
        &repo.path().join("tests/auth_test.rs"),
        "#[test]\nfn issues_tokens() { super::issue_token(); }\n",
    );
    write(
        &repo.path().join("Cargo.toml"),
        "[package]\nname='sample'\nversion='0.1.0'\nedition='2024'\n",
    );
    let index_path = repo.path().join(".orient/index");
    FastIndex::build(repo.path())
        .unwrap()
        .save(&index_path)
        .unwrap();
    let shard_dir = repo.path().join(".orient-shards");
    build_shards(&[repo.path().to_path_buf()], &shard_dir).unwrap();

    let runtime = ToolRuntime::default();
    let live = runtime.dispatch(ToolRequest {
        id: serde_json::json!("live-related"),
        tool: "related_files".to_string(),
        arguments: serde_json::json!({
            "repo": repo.path(),
            "path": "src/auth.rs",
            "limit": 5
        }),
    });
    assert!(live.error.is_none(), "{:?}", live.error);
    let live = live.result.unwrap();
    assert!(
        live.as_array()
            .unwrap()
            .iter()
            .any(|file| file["path"] == "tests/auth_test.rs"),
        "{live:?}"
    );
    assert_eq!(live[0]["read_request"]["tool"], "read_range");

    let live_batch = runtime.dispatch(ToolRequest {
        id: serde_json::json!("live-related-batch"),
        tool: "related_files".to_string(),
        arguments: serde_json::json!({
            "repo": repo.path(),
            "path": "src/auth.rs",
            "limit": 5,
            "include_read_batch": true
        }),
    });
    assert!(live_batch.error.is_none(), "{:?}", live_batch.error);
    let live_batch = live_batch.result.unwrap();
    assert_eq!(
        live_batch["summary"]["status"],
        serde_json::json!("matched")
    );
    assert_eq!(
        live_batch["summary"]["result_count"],
        serde_json::json!(live_batch["results"].as_array().unwrap().len())
    );
    assert_eq!(
        live_batch["summary"]["top_paths"],
        serde_json::json!(["tests/auth_test.rs"])
    );
    assert_eq!(
        live_batch["summary"]["top_dirs"],
        serde_json::json!(["tests"])
    );
    assert_eq!(live_batch["summary"]["top_exts"], serde_json::json!(["rs"]));
    assert_eq!(
        live_batch["summary"]["top_langs"],
        serde_json::json!(["rust"])
    );
    assert!(live_batch["summary"]["max_score"].is_number());
    assert!(live_batch["summary"]["min_score"].is_number());
    assert!(
        live_batch["results"]
            .as_array()
            .unwrap()
            .iter()
            .any(|file| file["path"] == "tests/auth_test.rs"),
        "{live_batch:?}"
    );
    assert_eq!(
        live_batch["read_batch_request"]["tool"],
        serde_json::json!("read_ranges")
    );
    assert_eq!(
        live_batch["read_batch_request"]["arguments"]["repo"],
        serde_json::json!(repo.path())
    );
    assert!(
        live_batch["read_batch_request"]["arguments"]["ranges"]
            .as_array()
            .unwrap()
            .iter()
            .any(|range| range["path"] == "tests/auth_test.rs"),
        "{live_batch:?}"
    );
    assert_eq!(
        live_batch["next_action"]["request"],
        live_batch["read_batch_request"]
    );

    let indexed = runtime.dispatch(ToolRequest {
        id: serde_json::json!("indexed-related"),
        tool: "related_files".to_string(),
        arguments: serde_json::json!({
            "index": repo.path().join(".orient/index"),
            "path": "src/auth.rs",
            "limit": 5
        }),
    });
    assert!(indexed.error.is_none(), "{:?}", indexed.error);
    let indexed = indexed.result.unwrap();
    assert_eq!(indexed[0]["read_request"]["tool"], "read_range");
    assert_eq!(
        indexed[0]["read_request"]["arguments"]["index"],
        serde_json::json!(index_path)
    );

    let sharded = runtime.dispatch(ToolRequest {
        id: serde_json::json!("shard-related"),
        tool: "related_files".to_string(),
        arguments: serde_json::json!({
            "index_dir": repo.path().join(".orient-shards"),
            "path": "src/auth.rs",
            "limit": 5
        }),
    });
    assert!(sharded.error.is_none(), "{:?}", sharded.error);
    let sharded = sharded.result.unwrap();
    assert_eq!(sharded[0]["read_request"]["tool"], "read_range");
    assert_eq!(
        sharded[0]["read_request"]["arguments"]["index_dir"],
        serde_json::json!(shard_dir)
    );

    let indexed_symbols = runtime.dispatch(ToolRequest {
        id: serde_json::json!("indexed-related-symbols"),
        tool: "related_symbols".to_string(),
        arguments: serde_json::json!({
            "index": repo.path().join(".orient/index"),
            "path": "src/auth.rs",
            "query": "SessionManager",
            "limit": 5
        }),
    });
    assert!(
        indexed_symbols.error.is_none(),
        "{:?}",
        indexed_symbols.error
    );
    let indexed_symbols = indexed_symbols.result.unwrap();
    assert_eq!(
        indexed_symbols[0]["read_request"]["arguments"]["index"],
        serde_json::json!(index_path)
    );
    assert_eq!(indexed_symbols[0]["read_request"]["tool"], "read_range");

    let indexed_symbol_batch = runtime.dispatch(ToolRequest {
        id: serde_json::json!("indexed-related-symbols-batch"),
        tool: "related_symbols".to_string(),
        arguments: serde_json::json!({
            "index": repo.path().join(".orient/index"),
            "path": "src/auth.rs",
            "query": "SessionManager",
            "limit": 5,
            "include_read_batch": true
        }),
    });
    assert!(
        indexed_symbol_batch.error.is_none(),
        "{:?}",
        indexed_symbol_batch.error
    );
    let indexed_symbol_batch = indexed_symbol_batch.result.unwrap();
    assert_eq!(
        indexed_symbol_batch["summary"]["status"],
        serde_json::json!("matched")
    );
    assert_eq!(
        indexed_symbol_batch["summary"]["result_count"],
        serde_json::json!(indexed_symbol_batch["results"].as_array().unwrap().len())
    );
    assert!(
        indexed_symbol_batch["summary"]["top_paths"]
            .as_array()
            .unwrap()
            .contains(&serde_json::json!("src/auth.rs")),
        "{indexed_symbol_batch:?}"
    );
    assert!(
        indexed_symbol_batch["summary"]["top_dirs"]
            .as_array()
            .unwrap()
            .contains(&serde_json::json!("src")),
        "{indexed_symbol_batch:?}"
    );
    assert!(
        indexed_symbol_batch["summary"]["top_exts"]
            .as_array()
            .unwrap()
            .contains(&serde_json::json!("rs")),
        "{indexed_symbol_batch:?}"
    );
    assert!(
        indexed_symbol_batch["summary"]["top_langs"]
            .as_array()
            .unwrap()
            .contains(&serde_json::json!("rust")),
        "{indexed_symbol_batch:?}"
    );
    assert!(
        indexed_symbol_batch["summary"]["top_symbols"]
            .as_array()
            .unwrap()
            .contains(&serde_json::json!("SessionManager")),
        "{indexed_symbol_batch:?}"
    );
    assert!(
        indexed_symbol_batch["summary"]["symbol_kinds"]
            .as_array()
            .unwrap()
            .contains(&serde_json::json!("struct")),
        "{indexed_symbol_batch:?}"
    );
    assert!(indexed_symbol_batch["summary"]["max_score"].is_number());
    assert!(indexed_symbol_batch["summary"]["min_score"].is_number());
    assert!(
        indexed_symbol_batch["results"].as_array().unwrap().len() >= 1,
        "{indexed_symbol_batch:?}"
    );
    assert_eq!(
        indexed_symbol_batch["read_batch_request"]["tool"],
        serde_json::json!("read_ranges")
    );
    assert_eq!(
        indexed_symbol_batch["read_batch_request"]["arguments"]["index"],
        serde_json::json!(index_path)
    );
    assert_eq!(
        indexed_symbol_batch["next_action"]["source"],
        serde_json::json!("read_batch_request")
    );
    assert_eq!(
        indexed_symbol_batch["next_action"]["request"],
        indexed_symbol_batch["read_batch_request"]
    );

    let missing_related_symbols = runtime.dispatch(ToolRequest {
        id: serde_json::json!("missing-related-symbols-batch"),
        tool: "related_symbols".to_string(),
        arguments: serde_json::json!({
            "index": repo.path().join(".orient/index"),
            "query": "DefinitelyMissingSymbol",
            "limit": 5,
            "include_read_batch": true
        }),
    });
    assert!(
        missing_related_symbols.error.is_none(),
        "{:?}",
        missing_related_symbols.error
    );
    let missing_related_symbols = missing_related_symbols.result.unwrap();
    assert_eq!(
        missing_related_symbols["summary"]["status"],
        serde_json::json!("not_found")
    );
    assert_eq!(
        missing_related_symbols["summary"]["result_count"],
        serde_json::json!(0)
    );
    assert_eq!(missing_related_symbols["results"], serde_json::json!([]));
    assert!(missing_related_symbols["read_batch_request"].is_null());
    assert!(missing_related_symbols["next_action"].is_null());

    let missing_shard_path = runtime.dispatch(ToolRequest {
        id: serde_json::json!("missing-shard-path"),
        tool: "related_symbols".to_string(),
        arguments: serde_json::json!({
            "index_dir": repo.path().join(".orient-shards"),
            "query": "SessionManager"
        }),
    });
    assert!(
        missing_shard_path
            .error
            .as_ref()
            .unwrap()
            .contains("path is required for shard related_symbols"),
        "{:?}",
        missing_shard_path.error
    );
}

#[test]
fn runtime_context_tools_scope_live_fallback_to_client_cwd() {
    let repo = tempfile::tempdir().unwrap();
    fs::create_dir_all(repo.path().join(".git")).unwrap();
    write(
        &repo.path().join("src/agent_context.rs"),
        "pub struct AgentCwdMarker;\npub fn agent_cwd_marker() {}\n",
    );
    write(
        &repo.path().join("tests/agent_context_test.rs"),
        "use sample::AgentCwdMarker;\n#[test]\nfn agent_context_smoke() {}\n",
    );
    write(
        &repo.path().join("Cargo.toml"),
        "[package]\nname='sample'\nversion='0.1.0'\nedition='2024'\n",
    );
    let cwd = repo.path().join("src");

    let runtime = ToolRuntime::default();
    let read = runtime.dispatch(ToolRequest {
        id: serde_json::json!("cwd-read"),
        tool: "read_range".to_string(),
        arguments: serde_json::json!({
            "cwd": cwd,
            "path": "src/agent_context.rs",
            "start": 1,
            "lines": 2
        }),
    });
    assert!(read.error.is_none(), "{:?}", read.error);
    assert!(
        read.result.as_ref().unwrap()["text"]
            .as_str()
            .unwrap()
            .contains("AgentCwdMarker"),
        "{:?}",
        read.result
    );

    let search = runtime.dispatch(ToolRequest {
        id: serde_json::json!("cwd-search"),
        tool: "search".to_string(),
        arguments: serde_json::json!({
            "cwd": repo.path().join("src"),
            "query": "AgentCwdMarker",
            "limit": 5
        }),
    });
    assert!(search.error.is_none(), "{:?}", search.error);
    let search = search.result.unwrap();
    assert_eq!(search[0]["path"], serde_json::json!("src/agent_context.rs"));
    assert_eq!(
        search[0]["read_request"]["arguments"]["repo"],
        serde_json::json!(repo.path().canonicalize().unwrap())
    );

    let search_batch = runtime.dispatch(ToolRequest {
        id: serde_json::json!("cwd-search-batch"),
        tool: "search_batch".to_string(),
        arguments: serde_json::json!({
            "cwd": repo.path().join("src"),
            "queries": ["AgentCwdMarker"],
            "limit": 5
        }),
    });
    assert!(search_batch.error.is_none(), "{:?}", search_batch.error);
    let search_batch = search_batch.result.unwrap();
    assert_eq!(
        search_batch[0]["results"][0]["path"],
        serde_json::json!("src/agent_context.rs")
    );
    assert_eq!(
        search_batch[0]["read_batch_request"]["arguments"]["repo"],
        serde_json::json!(repo.path().canonicalize().unwrap())
    );

    let batch = runtime.dispatch(ToolRequest {
        id: serde_json::json!("cwd-read-batch"),
        tool: "read_ranges".to_string(),
        arguments: serde_json::json!({
            "cwd": repo.path().join("src"),
            "ranges": {"path": "src/agent_context.rs", "start": 2, "lines": 1}
        }),
    });
    assert!(batch.error.is_none(), "{:?}", batch.error);
    assert!(
        batch.result.as_ref().unwrap()[0]["text"]
            .as_str()
            .unwrap()
            .contains("agent_cwd_marker"),
        "{:?}",
        batch.result
    );

    let related_files = runtime.dispatch(ToolRequest {
        id: serde_json::json!("cwd-related-files"),
        tool: "related_files".to_string(),
        arguments: serde_json::json!({
            "cwd": repo.path().join("src"),
            "path": "src/agent_context.rs",
            "limit": 5
        }),
    });
    assert!(related_files.error.is_none(), "{:?}", related_files.error);
    let related_files = related_files.result.unwrap();
    assert!(
        related_files
            .as_array()
            .unwrap()
            .iter()
            .any(|file| file["path"] == "tests/agent_context_test.rs"),
        "{related_files:?}"
    );
    assert_eq!(
        related_files[0]["read_request"]["arguments"]["repo"],
        serde_json::json!(repo.path().canonicalize().unwrap())
    );

    let related_symbols = runtime.dispatch(ToolRequest {
        id: serde_json::json!("cwd-related-symbols"),
        tool: "related_symbols".to_string(),
        arguments: serde_json::json!({
            "cwd": repo.path().join("src"),
            "path": "src/agent_context.rs",
            "query": "AgentCwdMarker",
            "limit": 5
        }),
    });
    assert!(
        related_symbols.error.is_none(),
        "{:?}",
        related_symbols.error
    );
    let related_symbols = serde_json::to_string(&related_symbols.result).unwrap();
    assert!(
        related_symbols.contains("AgentCwdMarker"),
        "{related_symbols}"
    );
    assert!(
        related_symbols.contains("src/agent_context.rs"),
        "{related_symbols}"
    );
    assert!(
        related_symbols.contains(
            &repo
                .path()
                .canonicalize()
                .unwrap()
                .to_string_lossy()
                .to_string()
        ),
        "{related_symbols}"
    );
}

#[test]
fn runtime_find_symbol_alias_accepts_live_index_and_shard_targets() {
    let repo = tempfile::tempdir().unwrap();
    write(
        &repo.path().join("src/auth.rs"),
        "pub struct SessionManager;\npub fn issue_token() {}\n",
    );
    write(
        &repo.path().join("Cargo.toml"),
        "[package]\nname='sample'\nversion='0.1.0'\nedition='2024'\n",
    );
    let index_path = repo.path().join(".orient/index");
    FastIndex::build(repo.path())
        .unwrap()
        .save(&index_path)
        .unwrap();
    let shard_dir = repo.path().join(".orient-shards");
    build_shards(&[repo.path().to_path_buf()], &shard_dir).unwrap();

    let runtime = ToolRuntime::default();
    let live = runtime.dispatch(ToolRequest {
        id: serde_json::json!("live-symbol"),
        tool: "find_symbol".to_string(),
        arguments: serde_json::json!({
            "repo": repo.path(),
            "name": "SessionManager",
            "limit": 5
        }),
    });
    assert!(live.error.is_none(), "{:?}", live.error);
    let live = live.result.unwrap();
    assert_eq!(live[0]["path"], "src/auth.rs");
    assert_eq!(live[0]["read_request"]["tool"], "read_range");
    assert_eq!(
        live[0]["read_request"]["arguments"]["repo"],
        serde_json::json!(repo.path())
    );

    let indexed = runtime.dispatch(ToolRequest {
        id: serde_json::json!("indexed-symbol"),
        tool: "find_symbol".to_string(),
        arguments: serde_json::json!({
            "index": repo.path().join(".orient/index"),
            "name": "SessionManager",
            "limit": 5
        }),
    });
    assert!(indexed.error.is_none(), "{:?}", indexed.error);
    let indexed = indexed.result.unwrap();
    assert_eq!(indexed[0]["read_request"]["tool"], "read_range");
    assert_eq!(
        indexed[0]["read_request"]["arguments"]["index"],
        serde_json::json!(index_path)
    );

    let indexed_wrapped = runtime.dispatch(ToolRequest {
        id: serde_json::json!("indexed-symbol-wrapped"),
        tool: "find_symbol".to_string(),
        arguments: serde_json::json!({
            "index": repo.path().join(".orient/index"),
            "name": "SessionManager",
            "limit": 5,
            "include_read_batch": true
        }),
    });
    assert!(
        indexed_wrapped.error.is_none(),
        "{:?}",
        indexed_wrapped.error
    );
    let indexed_wrapped = indexed_wrapped.result.unwrap();
    assert_eq!(
        indexed_wrapped["summary"]["status"],
        serde_json::json!("matched")
    );
    assert_eq!(
        indexed_wrapped["summary"]["symbol_count"],
        serde_json::json!(indexed_wrapped["results"].as_array().unwrap().len())
    );
    assert_eq!(
        indexed_wrapped["summary"]["top_paths"],
        serde_json::json!(["src/auth.rs"])
    );
    assert_eq!(
        indexed_wrapped["summary"]["top_dirs"],
        serde_json::json!(["src"])
    );
    assert_eq!(
        indexed_wrapped["summary"]["top_exts"],
        serde_json::json!(["rs"])
    );
    assert_eq!(
        indexed_wrapped["summary"]["top_langs"],
        serde_json::json!(["rust"])
    );
    assert_eq!(
        indexed_wrapped["summary"]["kinds"],
        serde_json::json!(["struct"])
    );
    assert_eq!(indexed_wrapped["results"][0]["path"], "src/auth.rs");
    assert_eq!(
        indexed_wrapped["read_batch_request"]["tool"],
        serde_json::json!("read_ranges")
    );
    assert_eq!(
        indexed_wrapped["read_batch_request"]["arguments"]["index"],
        serde_json::json!(index_path)
    );
    assert_eq!(
        indexed_wrapped["next_action"]["request"],
        indexed_wrapped["read_batch_request"]
    );

    let missing_wrapped = runtime.dispatch(ToolRequest {
        id: serde_json::json!("missing-symbol-wrapped"),
        tool: "find_symbol".to_string(),
        arguments: serde_json::json!({
            "index": repo.path().join(".orient/index"),
            "name": "MissingSymbol",
            "limit": 5,
            "include_read_batch": true
        }),
    });
    assert!(
        missing_wrapped.error.is_none(),
        "{:?}",
        missing_wrapped.error
    );
    let missing_wrapped = missing_wrapped.result.unwrap();
    assert_eq!(
        missing_wrapped["summary"]["status"],
        serde_json::json!("not_found")
    );
    assert_eq!(
        missing_wrapped["summary"]["symbol_count"],
        serde_json::json!(0)
    );
    assert_eq!(missing_wrapped["results"], serde_json::json!([]));
    assert!(missing_wrapped["read_batch_request"].is_null());
    assert!(missing_wrapped["next_action"].is_null());

    let explicit_indexed_wrapped = runtime.dispatch(ToolRequest {
        id: serde_json::json!("explicit-indexed-symbol-wrapped"),
        tool: "find_index_symbol".to_string(),
        arguments: serde_json::json!({
            "index": repo.path().join(".orient/index"),
            "name": "SessionManager",
            "limit": 5,
            "include_read_batch": true
        }),
    });
    assert!(
        explicit_indexed_wrapped.error.is_none(),
        "{:?}",
        explicit_indexed_wrapped.error
    );
    let explicit_indexed_wrapped = explicit_indexed_wrapped.result.unwrap();
    assert_eq!(
        explicit_indexed_wrapped["summary"]["status"],
        serde_json::json!("matched")
    );
    assert_eq!(
        explicit_indexed_wrapped["read_batch_request"]["tool"],
        serde_json::json!("read_index_ranges")
    );

    let sharded = runtime.dispatch(ToolRequest {
        id: serde_json::json!("shard-symbol"),
        tool: "find_symbol".to_string(),
        arguments: serde_json::json!({
            "index_dir": repo.path().join(".orient-shards"),
            "name": "SessionManager",
            "limit": 5
        }),
    });
    assert!(sharded.error.is_none(), "{:?}", sharded.error);
    let sharded = sharded.result.unwrap();
    assert_eq!(sharded[0]["read_request"]["tool"], "read_range");
    assert!(
        sharded[0]["path"]
            .as_str()
            .unwrap()
            .ends_with("src/auth.rs")
    );
    assert_eq!(
        sharded[0]["read_request"]["arguments"]["index_dir"],
        serde_json::json!(shard_dir)
    );

    let explicit_sharded_wrapped = runtime.dispatch(ToolRequest {
        id: serde_json::json!("explicit-shard-symbol-wrapped"),
        tool: "find_shard_symbol".to_string(),
        arguments: serde_json::json!({
            "index_dir": repo.path().join(".orient-shards"),
            "name": "SessionManager",
            "limit": 5,
            "include_read_batch": true
        }),
    });
    assert!(
        explicit_sharded_wrapped.error.is_none(),
        "{:?}",
        explicit_sharded_wrapped.error
    );
    let explicit_sharded_wrapped = explicit_sharded_wrapped.result.unwrap();
    assert_eq!(
        explicit_sharded_wrapped["summary"]["status"],
        serde_json::json!("matched")
    );
    assert_eq!(
        explicit_sharded_wrapped["read_batch_request"]["tool"],
        serde_json::json!("read_shard_ranges")
    );
    assert_eq!(
        explicit_sharded_wrapped["next_action"]["request"],
        explicit_sharded_wrapped["read_batch_request"]
    );

    let indexed_batch = runtime.dispatch(ToolRequest {
        id: serde_json::json!("indexed-symbol-batch"),
        tool: "find_symbol_batch".to_string(),
        arguments: serde_json::json!({
            "index": repo.path().join(".orient/index"),
            "names": ["SessionManager", "issue_token", "MissingSymbol"],
            "limit": 5
        }),
    });
    assert!(indexed_batch.error.is_none(), "{:?}", indexed_batch.error);
    let indexed_batch = indexed_batch.result.unwrap();
    assert_eq!(
        indexed_batch[0]["summary"]["status"],
        serde_json::json!("matched")
    );
    assert_eq!(
        indexed_batch[0]["summary"]["symbol_count"],
        serde_json::json!(1)
    );
    assert_eq!(
        indexed_batch[0]["summary"]["top_paths"],
        serde_json::json!(["src/auth.rs"])
    );
    assert_eq!(
        indexed_batch[0]["summary"]["top_dirs"],
        serde_json::json!(["src"])
    );
    assert_eq!(
        indexed_batch[0]["summary"]["top_exts"],
        serde_json::json!(["rs"])
    );
    assert_eq!(
        indexed_batch[0]["summary"]["top_langs"],
        serde_json::json!(["rust"])
    );
    assert_eq!(
        indexed_batch[0]["summary"]["kinds"],
        serde_json::json!(["struct"])
    );
    assert_eq!(
        indexed_batch[0]["read_batch_request"]["tool"],
        "read_ranges"
    );
    assert_eq!(
        indexed_batch[0]["read_batch_request"]["arguments"]["index"],
        serde_json::json!(index_path)
    );
    assert_eq!(
        indexed_batch[0]["next_action"]["source"],
        serde_json::json!("read_batch_request")
    );
    assert_eq!(
        indexed_batch[0]["next_action"]["request"],
        indexed_batch[0]["read_batch_request"]
    );
    assert_eq!(
        indexed_batch[2]["summary"]["status"],
        serde_json::json!("not_found")
    );
    assert_eq!(
        indexed_batch[2]["summary"]["symbol_count"],
        serde_json::json!(0)
    );
    assert!(indexed_batch[2]["read_batch_request"].is_null());

    let shard_batch = runtime.dispatch(ToolRequest {
        id: serde_json::json!("shard-symbol-batch"),
        tool: "find_symbol_batch".to_string(),
        arguments: serde_json::json!({
            "index_dir": repo.path().join(".orient-shards"),
            "names": ["SessionManager"],
            "limit": 5
        }),
    });
    assert!(shard_batch.error.is_none(), "{:?}", shard_batch.error);
    let shard_batch = shard_batch.result.unwrap();
    assert_eq!(shard_batch[0]["read_batch_request"]["tool"], "read_ranges");
    assert_eq!(
        shard_batch[0]["read_batch_request"]["arguments"]["index_dir"],
        serde_json::json!(shard_dir)
    );
    assert_eq!(
        shard_batch[0]["next_action"]["request"],
        shard_batch[0]["read_batch_request"]
    );

    let conflicted = runtime.dispatch(ToolRequest {
        id: serde_json::json!("conflicted-symbol"),
        tool: "find_symbol".to_string(),
        arguments: serde_json::json!({
            "index": repo.path().join(".orient/index"),
            "index_dir": repo.path().join(".orient-shards"),
            "name": "SessionManager"
        }),
    });
    assert!(
        conflicted
            .error
            .as_ref()
            .unwrap()
            .contains("only one of index or index_dir"),
        "{:?}",
        conflicted.error
    );
}

#[test]
fn runtime_search_plan_alias_accepts_live_index_and_shard_targets() {
    let repo = tempfile::tempdir().unwrap();
    write(
        &repo.path().join("src/auth.rs"),
        "pub struct SessionManager;\npub fn issue_token() {}\n",
    );
    write(
        &repo.path().join("Cargo.toml"),
        "[package]\nname='sample'\nversion='0.1.0'\nedition='2024'\n",
    );
    let index_path = repo.path().join(".orient/index");
    FastIndex::build(repo.path())
        .unwrap()
        .save(&index_path)
        .unwrap();
    let shard_dir = repo.path().join(".orient-shards");
    build_shards(&[repo.path().to_path_buf()], &shard_dir).unwrap();

    let runtime = ToolRuntime::default();
    let live = runtime.dispatch(ToolRequest {
        id: serde_json::json!("live-plan"),
        tool: "search_plan".to_string(),
        arguments: serde_json::json!({
            "repo": repo.path(),
            "query": "issue definitely_missing"
        }),
    });
    assert!(live.error.is_none(), "{:?}", live.error);
    let live = live.result.unwrap();
    assert_eq!(live["retry_requests"][0]["tool"], "search");
    assert_eq!(
        live["retry_requests"][0]["arguments"]["repo"],
        serde_json::json!(repo.path().canonicalize().unwrap())
    );

    let live_summary = runtime.dispatch(ToolRequest {
        id: serde_json::json!("live-plan-summary"),
        tool: "search_plan".to_string(),
        arguments: serde_json::json!({
            "repo": repo.path(),
            "query": "issue definitely_missing",
            "summary": true
        }),
    });
    assert!(live_summary.error.is_none(), "{:?}", live_summary.error);
    let live_summary = live_summary.result.unwrap();
    assert_eq!(
        live_summary["primary_retry_request"]["tool"],
        serde_json::json!("search")
    );
    assert!(
        live_summary["primary_retry_request"]["arguments"]
            .get("summary")
            .is_none()
    );
    assert!(live_summary.get("retry_requests").is_none());
    assert!(live_summary.get("planned_postings").is_none());
    assert!(live_summary.get("query_tokens").is_none());

    let indexed = runtime.dispatch(ToolRequest {
        id: serde_json::json!("indexed-plan"),
        tool: "search_plan".to_string(),
        arguments: serde_json::json!({
            "index": repo.path().join(".orient/index"),
            "query": "issue definitely_missing"
        }),
    });
    assert!(indexed.error.is_none(), "{:?}", indexed.error);
    let indexed = indexed.result.unwrap();
    assert_eq!(indexed["retry_requests"][0]["tool"], "search");
    assert_eq!(
        indexed["retry_requests"][0]["arguments"]["index"],
        serde_json::json!(index_path)
    );

    let direct_index_summary = runtime.dispatch(ToolRequest {
        id: serde_json::json!("direct-index-plan-summary"),
        tool: "indexed_query_plan".to_string(),
        arguments: serde_json::json!({
            "index": repo.path().join(".orient/index"),
            "query": "issue definitely_missing",
            "summary": true
        }),
    });
    assert!(
        direct_index_summary.error.is_none(),
        "{:?}",
        direct_index_summary.error
    );
    let direct_index_summary = direct_index_summary.result.unwrap();
    assert_eq!(
        direct_index_summary["primary_retry_request"]["tool"],
        serde_json::json!("indexed_search_code")
    );
    assert!(
        direct_index_summary["primary_retry_request"]["arguments"]
            .get("summary")
            .is_none()
    );
    assert!(direct_index_summary.get("retry_requests").is_none());
    assert!(direct_index_summary.get("planned_postings").is_none());

    let sharded = runtime.dispatch(ToolRequest {
        id: serde_json::json!("shard-plan"),
        tool: "search_plan".to_string(),
        arguments: serde_json::json!({
            "index_dir": repo.path().join(".orient-shards"),
            "query": "issue definitely_missing"
        }),
    });
    assert!(sharded.error.is_none(), "{:?}", sharded.error);
    let sharded = sharded.result.unwrap();
    assert_eq!(sharded[0]["plan"]["retry_requests"][0]["tool"], "search");
    assert_eq!(
        sharded[0]["plan"]["retry_requests"][0]["arguments"]["index_dir"],
        serde_json::json!(shard_dir)
    );

    let sharded_summary = runtime.dispatch(ToolRequest {
        id: serde_json::json!("shard-plan-summary"),
        tool: "shard_query_plan".to_string(),
        arguments: serde_json::json!({
            "index_dir": repo.path().join(".orient-shards"),
            "query": "issue definitely_missing",
            "summary": true
        }),
    });
    assert!(
        sharded_summary.error.is_none(),
        "{:?}",
        sharded_summary.error
    );
    let sharded_summary = sharded_summary.result.unwrap();
    assert!(sharded_summary.as_array().unwrap()[0].get("plan").is_none());
    assert_eq!(
        sharded_summary[0]["summary"]["primary_retry_request"]["tool"],
        serde_json::json!("search_shards")
    );
    assert!(
        sharded_summary[0]["summary"]["primary_retry_request"]["arguments"]
            .get("summary")
            .is_none()
    );

    let indexed_batch = runtime.dispatch(ToolRequest {
        id: serde_json::json!("indexed-plan-batch"),
        tool: "search_plan_batch".to_string(),
        arguments: serde_json::json!({
            "index": repo.path().join(".orient/index"),
            "queries": ["issue definitely_missing", "SessionManager definitely_missing"]
        }),
    });
    assert!(indexed_batch.error.is_none(), "{:?}", indexed_batch.error);
    let indexed_batch = indexed_batch.result.unwrap();
    assert_eq!(
        indexed_batch[0]["plan"]["retry_requests"][0]["tool"],
        "search"
    );
    assert_eq!(
        indexed_batch[0]["next_action"],
        indexed_batch[0]["plan"]["next_action"]
    );
    assert_eq!(
        indexed_batch[0]["plan"]["retry_requests"][0]["arguments"]["index"],
        serde_json::json!(index_path)
    );

    let indexed_batch_summary = runtime.dispatch(ToolRequest {
        id: serde_json::json!("indexed-plan-batch-summary"),
        tool: "search_plan_batch".to_string(),
        arguments: serde_json::json!({
            "index": repo.path().join(".orient/index"),
            "queries": ["issue definitely_missing", "SessionManager definitely_missing"],
            "summary": true
        }),
    });
    assert!(
        indexed_batch_summary.error.is_none(),
        "{:?}",
        indexed_batch_summary.error
    );
    let indexed_batch_summary = indexed_batch_summary.result.unwrap();
    assert_eq!(
        indexed_batch_summary[0]["summary"]["primary_retry_request"]["tool"],
        serde_json::json!("search")
    );
    assert!(
        indexed_batch_summary[0]["summary"]["primary_retry_request"]["arguments"]
            .get("summary")
            .is_none()
    );
    assert!(indexed_batch_summary[0].get("plan").is_none());
    assert!(indexed_batch_summary[0].get("plans").is_none());

    let shard_batch = runtime.dispatch(ToolRequest {
        id: serde_json::json!("shard-plan-batch"),
        tool: "search_plan_batch".to_string(),
        arguments: serde_json::json!({
            "index_dir": repo.path().join(".orient-shards"),
            "queries": ["issue definitely_missing"]
        }),
    });
    assert!(shard_batch.error.is_none(), "{:?}", shard_batch.error);
    let shard_batch = shard_batch.result.unwrap();
    assert_eq!(
        shard_batch[0]["plans"][0]["plan"]["retry_requests"][0]["tool"],
        "search"
    );
    assert_eq!(
        shard_batch[0]["next_action"],
        shard_batch[0]["plans"][0]["plan"]["next_action"]
    );
    assert_eq!(
        shard_batch[0]["plans"][0]["plan"]["retry_requests"][0]["arguments"]["index_dir"],
        serde_json::json!(shard_dir)
    );

    let shard_batch_summary = runtime.dispatch(ToolRequest {
        id: serde_json::json!("shard-plan-batch-summary"),
        tool: "search_plan_batch".to_string(),
        arguments: serde_json::json!({
            "index_dir": repo.path().join(".orient-shards"),
            "queries": ["issue definitely_missing"],
            "summary": true
        }),
    });
    assert!(
        shard_batch_summary.error.is_none(),
        "{:?}",
        shard_batch_summary.error
    );
    let shard_batch_summary = shard_batch_summary.result.unwrap();
    assert_eq!(
        shard_batch_summary[0]["summary"]["primary_retry_request"]["tool"],
        serde_json::json!("search")
    );
    assert!(
        shard_batch_summary[0]["summary"]["primary_retry_request"]["arguments"]
            .get("summary")
            .is_none()
    );
    assert!(shard_batch_summary[0].get("plans").is_none());
    assert!(shard_batch_summary[0]["shards"].is_array());

    let conflicted = runtime.dispatch(ToolRequest {
        id: serde_json::json!("conflicted-plan"),
        tool: "search_plan".to_string(),
        arguments: serde_json::json!({
            "index": repo.path().join(".orient/index"),
            "index_dir": repo.path().join(".orient-shards"),
            "query": "issue"
        }),
    });
    assert!(
        conflicted
            .error
            .as_ref()
            .unwrap()
            .contains("only one of index or index_dir"),
        "{:?}",
        conflicted.error
    );
}

#[test]
fn runtime_search_auto_batch_uses_single_warmed_index() {
    let repo = tempfile::tempdir().unwrap();
    write(
        &repo.path().join("src/auth.rs"),
        "pub struct SessionManager;\npub fn issue_token() {}\n",
    );
    let runtime = ToolRuntime::default();
    let index_path = repo.path().join("orient.index");
    let ensure = runtime.dispatch(ToolRequest {
        id: serde_json::json!("ensure"),
        tool: "ensure_index".to_string(),
        arguments: serde_json::json!({
            "repo": repo.path(),
            "index": index_path
        }),
    });
    assert!(ensure.error.is_none(), "{:?}", ensure.error);

    let batch = runtime.dispatch(ToolRequest {
        id: serde_json::json!("batch"),
        tool: "search_auto_batch".to_string(),
        arguments: serde_json::json!({
            "queries": ["issue_token", "SessionManager"],
            "limit": 5
        }),
    });
    assert!(batch.error.is_none(), "{:?}", batch.error);
    let batch = batch.result.unwrap();
    assert_eq!(batch.as_array().unwrap().len(), 2);
    assert_eq!(batch[0]["query"], "issue_token");
    assert_eq!(batch[0]["surface"], "indexed");
    assert_eq!(
        batch[0]["summary"]["top_paths"],
        serde_json::json!(["src/auth.rs"])
    );
    assert_eq!(batch[0]["query_plan_request"]["tool"], "indexed_query_plan");
    assert_eq!(batch[0]["repo_map_request"]["tool"], "repo_map");
    assert_eq!(
        batch[0]["repo_map_request"]["arguments"]["detail"],
        "compact"
    );
    assert_eq!(
        batch[0]["repo_map_request"]["arguments"]["read_limit"],
        serde_json::json!(DEFAULT_REPO_MAP_READ_BATCH_RANGES)
    );
    assert_eq!(batch[0]["results"][0]["read_request"]["tool"], "read_range");
    assert_eq!(batch[0]["read_batch_request"]["tool"], "read_ranges");
    assert_eq!(
        batch[0]["read_batch_request"]["summary"],
        serde_json::json!("Read 1 bounded range (80 total lines).")
    );
    assert!(batch[0]["read_batch_request"]["arguments"]["ranges"].is_array());
    assert_eq!(batch[1]["query"], "SessionManager");
    assert_eq!(batch[1]["surface"], "indexed");
    assert!(batch[0].get("query_plan_result").is_none());

    let diagnosed_batch = runtime.dispatch(ToolRequest {
        id: serde_json::json!("diagnosed-batch"),
        tool: "search_auto_batch".to_string(),
        arguments: serde_json::json!({
            "queries": ["issue_token"],
            "limit": 5,
            "diagnose": true
        }),
    });
    assert!(
        diagnosed_batch.error.is_none(),
        "{:?}",
        diagnosed_batch.error
    );
    let diagnosed_batch = diagnosed_batch.result.unwrap();
    assert!(!diagnosed_batch[0]["results"].as_array().unwrap().is_empty());
    assert_eq!(
        diagnosed_batch[0]["query_plan_result"]["final_match_count"],
        serde_json::json!(1)
    );
    assert_eq!(
        diagnosed_batch[0]["primary_diagnosis"],
        diagnosed_batch[0]["query_plan_result"]["diagnosis"]
    );
    assert_eq!(
        diagnosed_batch[0]["primary_diagnosis"]["status"],
        serde_json::json!("matched")
    );

    let empty_batch = runtime.dispatch(ToolRequest {
        id: serde_json::json!("empty-batch"),
        tool: "search_auto_batch".to_string(),
        arguments: serde_json::json!({
            "queries": ["issue_token definitely_missing"],
            "limit": 5
        }),
    });
    assert!(empty_batch.error.is_none(), "{:?}", empty_batch.error);
    let empty_batch = empty_batch.result.unwrap();
    assert!(empty_batch[0]["results"].as_array().unwrap().is_empty());
    assert_eq!(
        empty_batch[0]["query_plan_result"]["retry_requests"][0]["tool"],
        "indexed_search_code"
    );
    assert_eq!(
        empty_batch[0]["primary_diagnosis"]["status"],
        serde_json::json!("missing_terms")
    );

    let explicit_live_batch = runtime.dispatch(ToolRequest {
        id: serde_json::json!("explicit-live-batch"),
        tool: "search_auto_batch".to_string(),
        arguments: serde_json::json!({
            "repo": repo.path(),
            "queries": ["issue_token"],
            "limit": 5
        }),
    });
    assert!(
        explicit_live_batch.error.is_none(),
        "{:?}",
        explicit_live_batch.error
    );
    let explicit_live_batch = explicit_live_batch.result.unwrap();
    assert_eq!(explicit_live_batch[0]["surface"], "fallback");
    assert_eq!(
        explicit_live_batch[0]["query_plan_request"]["tool"],
        "search_query_plan"
    );
}

#[test]
fn runtime_rejects_oversized_batches() {
    let repo = tempfile::tempdir().unwrap();
    write(
        &repo.path().join("src/auth.rs"),
        "pub struct SessionManager;\npub fn issue_token() {}\n// line 3\n// line 4\n// line 5\n// line 6\n// line 7\n// line 8\n// line 9\n",
    );
    let runtime = ToolRuntime::default();
    let too_many_queries = (0..=MAX_BATCH_QUERIES)
        .map(|index| format!("query_{index}"))
        .collect::<Vec<_>>();

    let response = runtime.dispatch(ToolRequest {
        id: serde_json::json!("too-many-queries"),
        tool: "search_batch".to_string(),
        arguments: serde_json::json!({
            "repo": repo.path(),
            "queries": too_many_queries
        }),
    });
    let error = response.error.unwrap();
    assert!(error.contains("max 32"), "{error}");

    let response = runtime.dispatch(ToolRequest {
        id: serde_json::json!("empty-queries"),
        tool: "search_batch".to_string(),
        arguments: serde_json::json!({
            "repo": repo.path(),
            "queries": []
        }),
    });
    let error = response.error.unwrap();
    assert!(error.contains("must not be empty"), "{error}");

    let too_many_ranges = (0..=MAX_BATCH_RANGES)
        .map(|_| {
            serde_json::json!({
                "path": "src/auth.rs",
                "start": 1,
                "lines": 1
            })
        })
        .collect::<Vec<_>>();
    let response = runtime.dispatch(ToolRequest {
        id: serde_json::json!("too-many-ranges"),
        tool: "read_ranges".to_string(),
        arguments: serde_json::json!({
            "repo": repo.path(),
            "ranges": too_many_ranges
        }),
    });
    let error = response.error.unwrap();
    assert!(error.contains("max 64"), "{error}");

    let compacted_ranges = runtime.dispatch(ToolRequest {
        id: serde_json::json!("compacted-ranges"),
        tool: "read_ranges".to_string(),
        arguments: serde_json::json!({
            "repo": repo.path(),
            "ranges": [
                {"path": "src/auth.rs", "start": 1, "lines": 5},
                {"path": "src/auth.rs", "start": 4, "lines": 5},
                {"path": "src/auth.rs", "start": 1, "lines": 5}
            ]
        }),
    });
    assert!(
        compacted_ranges.error.is_none(),
        "{:?}",
        compacted_ranges.error
    );
    let compacted_ranges = compacted_ranges.result.unwrap();
    assert_eq!(compacted_ranges.as_array().unwrap().len(), 1);
    assert_eq!(compacted_ranges[0]["start_line"], serde_json::json!(1));
    assert_eq!(compacted_ranges[0]["end_line"], serde_json::json!(8));

    let symbol_scoped_ranges = runtime.dispatch(ToolRequest {
        id: serde_json::json!("symbol-scoped-ranges"),
        tool: "read_ranges".to_string(),
        arguments: serde_json::json!({
            "repo": repo.path(),
            "scope": "symbol",
            "ranges": [
                {"path": "src/auth.rs", "start": 2, "lines": 5},
                {"path": "src/auth.rs", "start": 4, "lines": 5}
            ]
        }),
    });
    assert!(
        symbol_scoped_ranges.error.is_none(),
        "{:?}",
        symbol_scoped_ranges.error
    );
    assert_eq!(
        symbol_scoped_ranges
            .result
            .unwrap()
            .as_array()
            .unwrap()
            .len(),
        2
    );

    let too_many_range_lines = (0..=(MAX_BATCH_READ_LINES / MAX_READ_RANGE_LINES))
        .map(|index| {
            serde_json::json!({
                "path": format!("src/auth_{index}.rs"),
                "start": 1,
                "lines": MAX_READ_RANGE_LINES
            })
        })
        .collect::<Vec<_>>();
    let response = runtime.dispatch(ToolRequest {
        id: serde_json::json!("too-many-range-lines"),
        tool: "read_ranges".to_string(),
        arguments: serde_json::json!({
            "repo": repo.path(),
            "ranges": too_many_range_lines
        }),
    });
    let error = response.error.unwrap();
    assert!(error.contains("total lines"), "{error}");
    assert!(
        error.contains(&format!("max {MAX_BATCH_READ_LINES}")),
        "{error}"
    );
    assert!(error.contains("split into smaller read_ranges"), "{error}");

    let too_many_open_range_lines = (0..=(MAX_BATCH_READ_LINES / MAX_READ_RANGE_LINES))
        .map(|index| {
            serde_json::json!({
                "path": format!("src/open_auth_{index}.rs"),
                "start": 1,
                "lines": MAX_READ_RANGE_LINES
            })
        })
        .collect::<Vec<_>>();
    let response = runtime.dispatch(ToolRequest {
        id: serde_json::json!("too-many-open-range-lines"),
        tool: "open_ranges".to_string(),
        arguments: serde_json::json!({
            "repo": repo.path(),
            "ranges": too_many_open_range_lines
        }),
    });
    let error = response.error.unwrap();
    assert!(error.contains("total lines"), "{error}");
    assert!(error.contains("split into smaller open_ranges"), "{error}");

    let response = runtime.dispatch(ToolRequest {
        id: serde_json::json!("empty-ranges"),
        tool: "open_ranges".to_string(),
        arguments: serde_json::json!({
            "repo": repo.path(),
            "ranges": []
        }),
    });
    let error = response.error.unwrap();
    assert!(error.contains("must not be empty"), "{error}");

    let response = runtime.dispatch(ToolRequest {
        id: serde_json::json!("conflicting-line-count-and-end-line"),
        tool: "read_ranges".to_string(),
        arguments: serde_json::json!({
            "repo": repo.path(),
            "ranges": {
                "path": "src/auth.rs",
                "start_line": 1,
                "line_count": 2,
                "end_line": 3
            }
        }),
    });
    let error = response.error.unwrap();
    assert!(
        error.contains("accepts only one of lines/line_count or end_line/end"),
        "{error}"
    );

    let response = runtime.dispatch(ToolRequest {
        id: serde_json::json!("conflicting-path-and-range"),
        tool: "open_range".to_string(),
        arguments: serde_json::json!({
            "repo": repo.path(),
            "path": "src/auth.rs",
            "range": {"path": "src/auth.rs", "start": 1, "lines": 1}
        }),
    });
    let error = response.error.unwrap();
    assert!(
        error.contains("accepts one of path/start/lines, range, or ranges"),
        "{error}"
    );

    let response = runtime.dispatch(ToolRequest {
        id: serde_json::json!("too-many-single-ranges"),
        tool: "open_range".to_string(),
        arguments: serde_json::json!({
            "repo": repo.path(),
            "ranges": [
                {"path": "src/auth.rs", "start": 1, "lines": 1},
                {"path": "src/auth.rs", "start": 2, "lines": 1}
            ]
        }),
    });
    let error = response.error.unwrap();
    assert!(error.contains("accepts exactly one range"), "{error}");

    let response = runtime.dispatch(ToolRequest {
        id: serde_json::json!("open-range-conflicting-indexes"),
        tool: "open_range".to_string(),
        arguments: serde_json::json!({
            "path": "src/auth.rs",
            "start": 1,
            "lines": 1,
            "index": repo.path().join("orient.index"),
            "index_dir": repo.path().join(".orient-shards")
        }),
    });
    let error = response.error.unwrap();
    assert!(
        error.contains("open_range accepts only one of index or index_dir"),
        "{error}"
    );

    let response = runtime.dispatch(ToolRequest {
        id: serde_json::json!("open-ranges-conflicting-indexes"),
        tool: "open_ranges".to_string(),
        arguments: serde_json::json!({
            "ranges": {"path": "src/auth.rs", "start": 1, "lines": 1},
            "index": repo.path().join("orient.index"),
            "index_dir": repo.path().join(".orient-shards")
        }),
    });
    let error = response.error.unwrap();
    assert!(
        error.contains("open_ranges accepts only one of index or index_dir"),
        "{error}"
    );

    let response = runtime.dispatch(ToolRequest {
        id: serde_json::json!("zero-start"),
        tool: "open_range".to_string(),
        arguments: serde_json::json!({
            "repo": repo.path(),
            "path": "src/auth.rs",
            "start": 0,
            "lines": 1
        }),
    });
    let error = response.error.unwrap();
    assert!(error.contains("positive integer"), "{error}");

    let response = runtime.dispatch(ToolRequest {
        id: serde_json::json!("zero-limit"),
        tool: "search_code".to_string(),
        arguments: serde_json::json!({
            "repo": repo.path(),
            "query": "SessionManager",
            "limit": 0
        }),
    });
    let error = response.error.unwrap();
    assert!(error.contains("positive integer"), "{error}");

    let response = runtime.dispatch(ToolRequest {
        id: serde_json::json!("negative-limit"),
        tool: "search_code".to_string(),
        arguments: serde_json::json!({
            "repo": repo.path(),
            "query": "SessionManager",
            "limit": -1
        }),
    });
    let error = response.error.unwrap();
    assert!(error.contains("non-negative integer"), "{error}");

    let response = runtime.dispatch(ToolRequest {
        id: serde_json::json!("too-many-results"),
        tool: "search_code".to_string(),
        arguments: serde_json::json!({
            "repo": repo.path(),
            "query": "SessionManager",
            "limit": MAX_SEARCH_RESULTS + 1
        }),
    });
    let error = response.error.unwrap();
    assert!(error.contains("max 100"), "{error}");

    let response = runtime.dispatch(ToolRequest {
        id: serde_json::json!("too-much-context"),
        tool: "search_code".to_string(),
        arguments: serde_json::json!({
            "repo": repo.path(),
            "query": "SessionManager",
            "context_lines": MAX_ATTACHED_CONTEXT_LINES + 1
        }),
    });
    let error = response.error.unwrap();
    assert!(
        error.contains(&format!("max {MAX_ATTACHED_CONTEXT_LINES}")),
        "{error}"
    );

    let response = runtime.dispatch(ToolRequest {
        id: serde_json::json!("invalid-snippet"),
        tool: "search_code".to_string(),
        arguments: serde_json::json!({
            "repo": repo.path(),
            "query": "SessionManager",
            "snippet": "wide"
        }),
    });
    let error = response.error.unwrap();
    assert!(
        error.contains("snippet mode must be one of: short, medium, block, symbol"),
        "{error}"
    );

    let response = runtime.dispatch(ToolRequest {
        id: serde_json::json!("valid-snippet-mode-alias"),
        tool: "search_code".to_string(),
        arguments: serde_json::json!({
            "repo": repo.path(),
            "query": "SessionManager",
            "snippet_mode": "short"
        }),
    });
    assert!(response.error.is_none(), "{response:?}");
    assert!(
        response.result.is_some(),
        "snippet_mode alias search should return a result payload"
    );

    let response = runtime.dispatch(ToolRequest {
        id: serde_json::json!("valid-dashed-snippet-mode-alias"),
        tool: "search_code".to_string(),
        arguments: serde_json::json!({
            "repo": repo.path(),
            "query": "SessionManager",
            "snippet-mode": "short"
        }),
    });
    assert!(response.error.is_none(), "{response:?}");
    assert!(
        response.result.is_some(),
        "snippet-mode alias search should return a result payload"
    );

    let response = runtime.dispatch(ToolRequest {
        id: serde_json::json!("invalid-snippet-mode-alias"),
        tool: "search_code".to_string(),
        arguments: serde_json::json!({
            "repo": repo.path(),
            "query": "SessionManager",
            "snippet_mode": "wide"
        }),
    });
    let error = response.error.unwrap();
    assert!(
        error.contains("snippet mode must be one of: short, medium, block, symbol"),
        "{error}"
    );

    let response = runtime.dispatch(ToolRequest {
        id: serde_json::json!("too-many-lines"),
        tool: "open_range".to_string(),
        arguments: serde_json::json!({
            "repo": repo.path(),
            "path": "src/auth.rs",
            "start": 1,
            "lines": MAX_READ_RANGE_LINES + 1
        }),
    });
    let error = response.error.unwrap();
    assert!(error.contains("max 1000"), "{error}");
}

#[test]
fn runtime_batches_searches_and_query_plans_against_repo_index_and_shards() {
    let repo = tempfile::tempdir().unwrap();
    write(
        &repo.path().join("src/auth.rs"),
        "pub struct SessionManager;\npub fn issue_token() {}\n",
    );
    write(
        &repo.path().join("src/billing.rs"),
        "pub fn invoice_total() {}\n",
    );
    let index_path = repo.path().join(".orient/index");
    FastIndex::build(repo.path())
        .unwrap()
        .save(&index_path)
        .unwrap();
    let runtime = ToolRuntime::default();

    let fallback = runtime.dispatch(ToolRequest {
        id: serde_json::json!("fallback-batch"),
        tool: "search_batch".to_string(),
        arguments: serde_json::json!({
            "repo": repo.path(),
            "queries": ["SessionManager", "invoice total", "orient_absent_token_zzq_20260529"],
            "limit": 2,
            "require_all": true
        }),
    });
    assert!(fallback.error.is_none(), "{:?}", fallback.error);
    let fallback = fallback.result.unwrap();
    assert_eq!(
        fallback[0]["next_action"]["source"],
        serde_json::json!("read_batch_request")
    );
    assert_eq!(
        fallback[0]["summary"]["status"],
        serde_json::json!("matched")
    );
    assert_eq!(
        fallback[0]["summary"]["result_count"],
        serde_json::json!(fallback[0]["results"].as_array().unwrap().len())
    );
    assert_eq!(
        fallback[2]["summary"]["status"],
        serde_json::json!("not_found")
    );
    assert_eq!(fallback[2]["summary"]["result_count"], serde_json::json!(0));
    assert_eq!(fallback[2]["read_batch_request"], serde_json::Value::Null);
    assert_eq!(
        fallback[2]["next_action"]["source"],
        serde_json::json!("query_plan_request")
    );
    assert_eq!(
        fallback[2]["next_action"]["request"],
        fallback[2]["query_plan_request"]
    );
    assert_eq!(
        fallback[2]["next_action"]["request"]["arguments"]["query"],
        "orient_absent_token_zzq_20260529"
    );
    let result = serde_json::to_string(&fallback).unwrap();
    assert!(result.contains("\"query\":\"SessionManager\""), "{result}");
    assert!(result.contains("src/auth.rs"), "{result}");
    assert!(result.contains("\"query\":\"invoice total\""), "{result}");
    assert!(result.contains("src/billing.rs"), "{result}");

    let indexed = runtime.dispatch(ToolRequest {
        id: serde_json::json!("indexed-batch"),
        tool: "indexed_search_batch".to_string(),
        arguments: serde_json::json!({
            "index": index_path,
            "queries": ["SessionManager", "invoice total"],
            "limit": 2,
            "require_all": true
        }),
    });
    assert!(indexed.error.is_none(), "{:?}", indexed.error);
    let indexed = indexed.result.unwrap();
    assert_eq!(
        indexed[0]["query_plan_request"]["tool"],
        "indexed_query_plan"
    );
    assert_eq!(indexed[0]["repo_map_request"]["tool"], "indexed_repo_map");
    assert_eq!(
        indexed[0]["summary"]["status"],
        serde_json::json!("matched")
    );
    assert_eq!(
        indexed[0]["summary"]["result_count"],
        serde_json::json!(indexed[0]["results"].as_array().unwrap().len())
    );
    assert_eq!(
        indexed[0]["query_plan_request"]["arguments"]["index"],
        serde_json::json!(&index_path)
    );
    assert_eq!(
        indexed[0]["repo_map_request"]["arguments"]["index"],
        serde_json::json!(&index_path)
    );
    let result = serde_json::to_string(&indexed).unwrap();
    assert!(result.contains("src/auth.rs"), "{result}");
    assert!(result.contains("src/billing.rs"), "{result}");

    let live_plan = runtime.dispatch(ToolRequest {
        id: serde_json::json!("live-plan"),
        tool: "search_plan".to_string(),
        arguments: serde_json::json!({
            "repo": repo.path(),
            "query": "SessionManager missingterm",
            "require_all": true
        }),
    });
    assert!(live_plan.error.is_none(), "{:?}", live_plan.error);
    let result = serde_json::to_string(&live_plan.result).unwrap();
    assert!(result.contains("\"missing_terms\""), "{result}");
    assert!(result.contains("missingterm"), "{result}");
    assert!(result.contains("drop_missing_terms"), "{result}");
    let live_plan_result = live_plan.result.as_ref().unwrap();
    assert_eq!(live_plan_result["retry_requests"][0]["tool"], "search");
    assert_eq!(
        live_plan_result["primary_retry_request"],
        live_plan_result["retry_requests"][0]
    );
    assert_eq!(
        live_plan_result["next_action"]["source"],
        serde_json::json!("primary_retry_request")
    );
    assert_eq!(
        live_plan_result["next_action"]["request"],
        live_plan_result["primary_retry_request"]
    );
    assert_eq!(
        live_plan_result["retry_requests"][0]["arguments"]["query"],
        "session manager"
    );
    let retry = runtime.dispatch(ToolRequest {
        id: serde_json::json!("live-retry"),
        tool: live_plan_result["retry_requests"][0]["tool"]
            .as_str()
            .unwrap()
            .to_string(),
        arguments: live_plan_result["retry_requests"][0]["arguments"].clone(),
    });
    assert!(retry.error.is_none(), "{:?}", retry.error);
    let retry_result = serde_json::to_string(&retry.result).unwrap();
    assert!(retry_result.contains("src/auth.rs"), "{retry_result}");

    let relax_filter_plan = runtime.dispatch(ToolRequest {
        id: serde_json::json!("relax-filter-plan"),
        tool: "search_plan".to_string(),
        arguments: serde_json::json!({
            "repo": repo.path(),
            "query": "SessionManager path:not-real lang:rust",
            "require_all": true
        }),
    });
    assert!(
        relax_filter_plan.error.is_none(),
        "{:?}",
        relax_filter_plan.error
    );
    let relax_result = relax_filter_plan.result.as_ref().unwrap();
    assert_eq!(
        relax_result["repair_hints"][0]["kind"],
        serde_json::json!("relax_path_filter")
    );
    assert_eq!(
        relax_result["retry_requests"][0]["arguments"]["query"],
        "session manager"
    );
    assert!(relax_result["retry_requests"][0]["arguments"]["path"].is_null());
    assert_eq!(
        relax_result["retry_requests"][0]["arguments"]["language"],
        "rust"
    );
    assert_eq!(
        relax_result["repair_hints"][1]["kind"],
        serde_json::json!("relax_language_filter")
    );
    assert_eq!(relax_result["retry_requests"].as_array().unwrap().len(), 2);

    let filter_only_plan = runtime.dispatch(ToolRequest {
        id: serde_json::json!("filter-only-plan"),
        tool: "search_plan".to_string(),
        arguments: serde_json::json!({
            "repo": repo.path(),
            "query": "file:not-real.rs lang:rust"
        }),
    });
    assert!(
        filter_only_plan.error.is_none(),
        "{:?}",
        filter_only_plan.error
    );
    let filter_only_result = filter_only_plan.result.as_ref().unwrap();
    assert_eq!(
        filter_only_result["repair_hints"][0]["kind"],
        serde_json::json!("relax_file_filter")
    );
    assert_eq!(
        filter_only_result["retry_requests"][0]["arguments"]["query"],
        ""
    );
    assert!(filter_only_result["retry_requests"][0]["arguments"]["file"].is_null());
    assert_eq!(
        filter_only_result["retry_requests"][0]["arguments"]["language"],
        "rust"
    );

    let filter_only_retry = runtime.dispatch(ToolRequest {
        id: serde_json::json!("filter-only-retry"),
        tool: filter_only_result["retry_requests"][0]["tool"]
            .as_str()
            .unwrap()
            .to_string(),
        arguments: filter_only_result["retry_requests"][0]["arguments"].clone(),
    });
    assert!(
        filter_only_retry.error.is_none(),
        "{:?}",
        filter_only_retry.error
    );
    let retry_result = serde_json::to_string(&filter_only_retry.result).unwrap();
    assert!(retry_result.contains("src/auth.rs"), "{retry_result}");

    let invalid_kind_any_plan = runtime.dispatch(ToolRequest {
        id: serde_json::json!("invalid-kind-any-plan"),
        tool: "search_plan".to_string(),
        arguments: serde_json::json!({
            "repo": repo.path(),
            "query": "kind:fn session manager invoice",
            "require_all": true
        }),
    });
    assert!(
        invalid_kind_any_plan.error.is_none(),
        "{:?}",
        invalid_kind_any_plan.error
    );
    let invalid_kind_any = invalid_kind_any_plan.result.as_ref().unwrap();
    let any_retry = invalid_kind_any["retry_requests"]
        .as_array()
        .unwrap()
        .iter()
        .find(|request| {
            request["arguments"]["query"]
                .as_str()
                .is_some_and(|query| query.starts_with("mode:any "))
        })
        .expect("expected mode:any retry request");
    assert!(
        any_retry["arguments"].get("symbol_kind").is_none(),
        "{any_retry:?}"
    );
    let any_retry_result = runtime.dispatch(ToolRequest {
        id: serde_json::json!("invalid-kind-any-retry"),
        tool: any_retry["tool"].as_str().unwrap().to_string(),
        arguments: any_retry["arguments"].clone(),
    });
    assert!(
        any_retry_result.error.is_none(),
        "{:?}",
        any_retry_result.error
    );
    let any_retry_result = serde_json::to_string(&any_retry_result.result).unwrap();
    assert!(
        any_retry_result.contains("src/auth.rs"),
        "{any_retry_result}"
    );
    assert!(
        any_retry_result.contains("src/billing.rs"),
        "{any_retry_result}"
    );

    let live_plan_batch = runtime.dispatch(ToolRequest {
        id: serde_json::json!("live-plan-batch"),
        tool: "search_query_plan_batch".to_string(),
        arguments: serde_json::json!({
            "repo": repo.path(),
            "queries": ["SessionManager missingterm", "invoice absentterm"],
            "require_all": true
        }),
    });
    assert!(
        live_plan_batch.error.is_none(),
        "{:?}",
        live_plan_batch.error
    );
    let result = serde_json::to_string(&live_plan_batch.result).unwrap();
    assert!(
        result.contains("\"query\":\"SessionManager missingterm\""),
        "{result}"
    );
    assert!(
        result.contains("\"query\":\"invoice absentterm\""),
        "{result}"
    );
    assert!(result.contains("drop_missing_terms"), "{result}");
    let live_plan_batch = live_plan_batch.result.as_ref().unwrap();
    assert_eq!(
        live_plan_batch[0]["summary"]["status"],
        serde_json::json!("missing_terms")
    );
    assert_eq!(
        live_plan_batch[0]["summary"]["missing_terms"][0],
        serde_json::json!("missingterm")
    );
    assert_eq!(
        live_plan_batch[0]["summary"]["promoted_next_action"],
        live_plan_batch[0]["next_action"]
    );

    let indexed_plans = runtime.dispatch(ToolRequest {
        id: serde_json::json!("indexed-plan-batch"),
        tool: "indexed_query_plan_batch".to_string(),
        arguments: serde_json::json!({
            "index": index_path,
            "queries": ["SessionManager missingterm", "invoice absentterm"],
            "require_all": true
        }),
    });
    assert!(indexed_plans.error.is_none(), "{:?}", indexed_plans.error);
    let result = serde_json::to_string(&indexed_plans.result).unwrap();
    assert!(
        result.contains("\"query\":\"SessionManager missingterm\""),
        "{result}"
    );
    assert!(
        result.contains("\"query\":\"invoice absentterm\""),
        "{result}"
    );
    assert!(result.contains("\"missing_terms\""), "{result}");
    assert!(result.contains("missingterm"), "{result}");
    assert!(result.contains("absentterm"), "{result}");
    assert!(result.contains("drop_missing_terms"), "{result}");
    assert!(result.contains("\"next_action\""), "{result}");
    let indexed_plans = indexed_plans.result.as_ref().unwrap();
    assert_eq!(
        indexed_plans[1]["summary"]["status"],
        serde_json::json!("missing_terms")
    );
    assert_eq!(
        indexed_plans[1]["summary"]["suggested_query"],
        serde_json::json!("invoice")
    );
    assert_eq!(
        indexed_plans[1]["summary"]["primary_retry_request"],
        indexed_plans[1]["plan"]["primary_retry_request"]
    );

    let indexed_plan_alias = runtime.dispatch(ToolRequest {
        id: serde_json::json!("index-plan-alias"),
        tool: "index_plan".to_string(),
        arguments: serde_json::json!({
            "index": index_path,
            "query": "SessionManager missingterm",
            "require_all": true
        }),
    });
    assert!(
        indexed_plan_alias.error.is_none(),
        "{:?}",
        indexed_plan_alias.error
    );
    let result = serde_json::to_string(&indexed_plan_alias.result).unwrap();
    assert!(result.contains("missingterm"), "{result}");
    assert!(result.contains("drop_missing_terms"), "{result}");
    let indexed_plan_result = indexed_plan_alias.result.as_ref().unwrap();
    assert_eq!(
        indexed_plan_result["retry_requests"][0]["tool"],
        "indexed_search_code"
    );
    assert_eq!(
        indexed_plan_result["retry_requests"][0]["arguments"]["query"],
        "session manager"
    );

    let shard_dir = tempfile::tempdir().unwrap();
    let build = runtime.dispatch(ToolRequest {
        id: serde_json::json!("build-shards"),
        tool: "index_shards".to_string(),
        arguments: serde_json::json!({
            "repos": [repo.path()],
            "output_dir": shard_dir.path()
        }),
    });
    assert!(build.error.is_none(), "{:?}", build.error);
    let shards = runtime.dispatch(ToolRequest {
        id: serde_json::json!("shard-batch"),
        tool: "search_shards_batch".to_string(),
        arguments: serde_json::json!({
            "index_dir": shard_dir.path(),
            "queries": ["SessionManager", "invoice total"],
            "limit": 2,
            "require_all": true
        }),
    });
    assert!(shards.error.is_none(), "{:?}", shards.error);
    let result = serde_json::to_string(&shards.result).unwrap();
    assert!(result.contains("src/auth.rs"), "{result}");
    assert!(result.contains("src/billing.rs"), "{result}");

    let shard_plans = runtime.dispatch(ToolRequest {
        id: serde_json::json!("shard-plan-batch"),
        tool: "shard_query_plan_batch".to_string(),
        arguments: serde_json::json!({
            "index_dir": shard_dir.path(),
            "queries": ["SessionManager missingterm", "invoice absentterm"],
            "require_all": true
        }),
    });
    assert!(shard_plans.error.is_none(), "{:?}", shard_plans.error);
    let result = serde_json::to_string(&shard_plans.result).unwrap();
    assert!(
        result.contains("\"query\":\"SessionManager missingterm\""),
        "{result}"
    );
    assert!(
        result.contains("\"query\":\"invoice absentterm\""),
        "{result}"
    );
    assert!(result.contains("\"plans\""), "{result}");
    assert!(result.contains("\"summary\""), "{result}");
    assert!(result.contains("\"missing_terms\""), "{result}");
    assert!(result.contains("missingterm"), "{result}");
    assert!(result.contains("absentterm"), "{result}");
    assert!(result.contains("drop_missing_terms"), "{result}");
    let shard_plans = shard_plans.result.unwrap();
    assert_eq!(
        shard_plans[1]["summary"]["status"],
        serde_json::json!("missing_terms")
    );
    assert_eq!(
        shard_plans[1]["summary"]["suggested_query"],
        serde_json::json!("invoice")
    );
    assert_eq!(
        shard_plans[0]["next_action"],
        shard_plans[0]["plans"][0]["plan"]["next_action"]
    );

    let shard_plan_alias = runtime.dispatch(ToolRequest {
        id: serde_json::json!("shard-plan-alias"),
        tool: "shard_plan".to_string(),
        arguments: serde_json::json!({
            "index_dir": shard_dir.path(),
            "query": "SessionManager missingterm",
            "require_all": true
        }),
    });
    assert!(
        shard_plan_alias.error.is_none(),
        "{:?}",
        shard_plan_alias.error
    );
    let result = serde_json::to_string(&shard_plan_alias.result).unwrap();
    assert!(result.contains("missingterm"), "{result}");
    let shard_plan_result = shard_plan_alias.result.as_ref().unwrap();
    assert_eq!(
        shard_plan_result[0]["summary"]["status"],
        serde_json::json!("missing_terms")
    );
    assert_eq!(
        shard_plan_result[0]["summary"]["primary_retry_request"],
        shard_plan_result[0]["plan"]["primary_retry_request"]
    );
    assert_eq!(
        shard_plan_result[0]["next_action"],
        shard_plan_result[0]["plan"]["next_action"]
    );
    assert_eq!(
        shard_plan_result[0]["plan"]["retry_requests"][0]["tool"],
        "search_shards"
    );
    assert_eq!(
        shard_plan_result[0]["plan"]["retry_requests"][0]["arguments"]["query"],
        "session manager"
    );
}

#[test]
fn retry_requests_preserve_boolean_test_filters() {
    let repo = tempfile::tempdir().unwrap();
    write(
        &repo.path().join("src/auth.rs"),
        "pub struct SessionManager;\npub fn issue_token() {}\n",
    );
    write(
        &repo.path().join("tests/auth_test.rs"),
        "use app::SessionManager;\n#[test]\nfn session_manager_round_trip() {}\n",
    );
    let runtime = ToolRuntime::default();

    let plan = runtime.dispatch(ToolRequest {
        id: serde_json::json!("plan"),
        tool: "search_plan".to_string(),
        arguments: serde_json::json!({
            "repo": repo.path(),
            "query": "SessionManager missingterm test:true",
            "require_all": true
        }),
    });
    assert!(plan.error.is_none(), "{:?}", plan.error);
    let plan = plan.result.as_ref().unwrap();
    assert_eq!(
        plan["retry_requests"][0]["arguments"]["test"],
        serde_json::json!(true)
    );

    let retry = runtime.dispatch(ToolRequest {
        id: serde_json::json!("retry"),
        tool: plan["retry_requests"][0]["tool"]
            .as_str()
            .unwrap()
            .to_string(),
        arguments: plan["retry_requests"][0]["arguments"].clone(),
    });
    assert!(retry.error.is_none(), "{:?}", retry.error);
    let retry_result = serde_json::to_string(&retry.result).unwrap();
    assert!(
        retry_result.contains("tests/auth_test.rs"),
        "{retry_result}"
    );
    assert!(!retry_result.contains("src/auth.rs"), "{retry_result}");

    let source_plan = runtime.dispatch(ToolRequest {
        id: serde_json::json!("source-plan"),
        tool: "search_plan".to_string(),
        arguments: serde_json::json!({
            "repo": repo.path(),
            "query": "SessionManager missingterm test:false",
            "require_all": true
        }),
    });
    assert!(source_plan.error.is_none(), "{:?}", source_plan.error);
    let source_plan = source_plan.result.as_ref().unwrap();
    assert_eq!(
        source_plan["retry_requests"][0]["arguments"]["test"],
        serde_json::json!(false)
    );
}

#[test]
fn relax_filter_retries_preserve_term_match_mode() {
    let repo = tempfile::tempdir().unwrap();
    write(
        &repo.path().join("src/alpha.rs"),
        "pub fn alpha_only() {}\n",
    );
    write(
        &repo.path().join("src/gamma.rs"),
        "pub fn gamma_only() {}\n",
    );
    let runtime = ToolRuntime::default();

    let plan = runtime.dispatch(ToolRequest {
        id: serde_json::json!("plan"),
        tool: "search_plan".to_string(),
        arguments: serde_json::json!({
            "repo": repo.path(),
            "query": "alpha gamma path:not-real",
            "any_terms": true
        }),
    });
    assert!(plan.error.is_none(), "{:?}", plan.error);
    let plan = plan.result.as_ref().unwrap();
    assert_eq!(plan["repair_hints"][0]["kind"], "relax_path_filter");
    assert_eq!(
        plan["retry_requests"][0]["arguments"]["any_terms"],
        serde_json::json!(true)
    );
    assert!(plan["retry_requests"][0]["arguments"]["path"].is_null());

    let retry = runtime.dispatch(ToolRequest {
        id: serde_json::json!("retry"),
        tool: plan["retry_requests"][0]["tool"]
            .as_str()
            .unwrap()
            .to_string(),
        arguments: plan["retry_requests"][0]["arguments"].clone(),
    });
    assert!(retry.error.is_none(), "{:?}", retry.error);
    let retry_result = serde_json::to_string(&retry.result).unwrap();
    assert!(retry_result.contains("src/alpha.rs"), "{retry_result}");
    assert!(retry_result.contains("src/gamma.rs"), "{retry_result}");
}

#[test]
fn runtime_accepts_structured_negative_search_filters() {
    let repo = tempfile::tempdir().unwrap();
    write(
        &repo.path().join("src/auth.rs"),
        "pub struct SessionManager;\npub fn issue_token() {}\n",
    );
    write(
        &repo.path().join("generated/auth.rs"),
        "pub struct GeneratedSessionManager;\npub fn issue_token() {}\n",
    );
    write(
        &repo.path().join("src/generated_symbol.rs"),
        "pub struct GeneratedSessionManager;\npub fn issue_token() {}\n",
    );
    write(
        &repo.path().join("tests/auth_test.rs"),
        "use crate::auth::SessionManager;\n#[test]\nfn issue_token_test() {}\n",
    );
    write(
        &repo.path().join("generated/auth_test.rs"),
        "use crate::auth::GeneratedSessionManager;\n#[test]\nfn issue_token_test() {}\n",
    );
    write(
        &repo.path().join("src/view.ts"),
        "import React from 'react';\nexport function renderToken() { return React.createElement('div'); }\n",
    );
    write(
        &repo.path().join("src/view_class.ts"),
        "import React from 'react';\nexport class TokenView {}\n",
    );
    let index_path = repo.path().join(".orient/index");
    FastIndex::build(repo.path())
        .unwrap()
        .save(&index_path)
        .unwrap();
    let runtime = ToolRuntime::default();

    let fallback = runtime.dispatch(ToolRequest {
        id: serde_json::json!("fallback"),
        tool: "search".to_string(),
        arguments: serde_json::json!({
            "repo": repo.path(),
            "query": "issue token",
            "limit": 10,
            "dir": "src",
            "symbol_kind": "function",
            "require_all": true,
            "exclude_folder": ["generated"]
        }),
    });
    assert!(fallback.error.is_none(), "{:?}", fallback.error);
    let result = serde_json::to_string(&fallback.result).unwrap();
    assert!(result.contains("src/auth.rs"), "{result}");
    assert!(!result.contains("generated/auth.rs"), "{result}");
    assert!(!result.contains("src/generated_symbol.rs"), "{result}");

    let fallback_exclude_content = runtime.dispatch(ToolRequest {
        id: serde_json::json!("fallback-exclude-content"),
        tool: "search".to_string(),
        arguments: serde_json::json!({
            "repo": repo.path(),
            "query": "issue token",
            "limit": 10,
            "exclude_content": "GeneratedSessionManager"
        }),
    });
    assert!(
        fallback_exclude_content.error.is_none(),
        "{:?}",
        fallback_exclude_content.error
    );
    let result = serde_json::to_string(&fallback_exclude_content.result).unwrap();
    assert!(result.contains("src/auth.rs"), "{result}");
    assert!(!result.contains("generated/auth.rs"), "{result}");
    assert!(!result.contains("src/generated_symbol.rs"), "{result}");
    let related_args =
        &fallback_exclude_content.result.as_ref().unwrap()[0]["related_request"]["arguments"];
    assert_eq!(related_args["path"], serde_json::json!("src/auth.rs"));
    assert_eq!(
        related_args["exclude_content"][0],
        serde_json::json!("GeneratedSessionManager")
    );
    assert!(
        fallback_exclude_content.result.as_ref().unwrap()[0]["related_request"]["cli"]
            .as_str()
            .unwrap_or_default()
            .contains("--exclude-content GeneratedSessionManager")
    );

    let alias_filters = runtime.dispatch(ToolRequest {
        id: serde_json::json!("alias-filters"),
        tool: "search".to_string(),
        arguments: serde_json::json!({
            "repo": repo.path(),
            "query": "React",
            "limit": 10,
            "folder": "src",
            "filename": "view.ts",
            "lang": "ts",
            "ext": "ts",
            "type": "functions",
            "require_all": true,
            "exclude_lang": "rs",
            "exclude_ext": "rs",
            "exclude_type": "classes"
        }),
    });
    assert!(alias_filters.error.is_none(), "{:?}", alias_filters.error);
    let result = serde_json::to_string(&alias_filters.result).unwrap();
    assert!(result.contains("src/view.ts"), "{result}");
    assert!(!result.contains("src/view_class.ts"), "{result}");
    assert!(!result.contains("src/auth.rs"), "{result}");

    let dashed_alias_filters = runtime.dispatch(ToolRequest {
        id: serde_json::json!("dashed-alias-filters"),
        tool: "search".to_string(),
        arguments: serde_json::json!({
            "repo": repo.path(),
            "query": "React",
            "limit": 10,
            "folder": "src",
            "file-name": "view.ts",
            "lang": "ts",
            "ext": "ts",
            "symbol-kind": "function",
            "require-all": true,
            "exclude-dir": ["generated"],
            "exclude-symbol-kind": "class"
        }),
    });
    assert!(
        dashed_alias_filters.error.is_none(),
        "{:?}",
        dashed_alias_filters.error
    );
    let result = serde_json::to_string(&dashed_alias_filters.result).unwrap();
    assert!(result.contains("src/view.ts"), "{result}");
    assert!(!result.contains("src/view_class.ts"), "{result}");
    assert!(!result.contains("generated/auth.rs"), "{result}");

    let indexed = runtime.dispatch(ToolRequest {
        id: serde_json::json!("indexed"),
        tool: "indexed_search".to_string(),
        arguments: serde_json::json!({
            "index": index_path,
            "query": "issue token",
            "limit": 10,
            "dir": "src",
            "symbol_kind": "function",
            "require_all": true,
            "exclude_path": ["generated"],
            "exclude_symbol": "GeneratedSessionManager",
            "exclude_symbol_kind": "enum",
            "exclude_text": "GeneratedSessionManager"
        }),
    });
    assert!(indexed.error.is_none(), "{:?}", indexed.error);
    let result = serde_json::to_string(&indexed.result).unwrap();
    assert!(result.contains("src/auth.rs"), "{result}");
    assert!(!result.contains("generated/auth.rs"), "{result}");
    assert!(!result.contains("src/generated_symbol.rs"), "{result}");

    let generated_false = runtime.dispatch(ToolRequest {
        id: serde_json::json!("generated-false"),
        tool: "search".to_string(),
        arguments: serde_json::json!({
            "repo": repo.path(),
            "query": "issue token",
            "limit": 10,
            "generated": false
        }),
    });
    assert!(
        generated_false.error.is_none(),
        "{:?}",
        generated_false.error
    );
    let result = serde_json::to_string(&generated_false.result).unwrap();
    assert!(result.contains("src/auth.rs"), "{result}");
    assert!(!result.contains("generated/auth.rs"), "{result}");
    assert!(!result.contains("src/generated_symbol.rs"), "{result}");

    let filter_only = runtime.dispatch(ToolRequest {
        id: serde_json::json!("filter-only"),
        tool: "indexed_search".to_string(),
        arguments: serde_json::json!({
            "index": index_path,
            "query": "file:auth.rs",
            "limit": 10,
            "explain": true
        }),
    });
    assert!(filter_only.error.is_none(), "{:?}", filter_only.error);
    let result = serde_json::to_string(&filter_only.result).unwrap();
    assert!(result.contains("src/auth.rs"), "{result}");
    assert!(result.contains("file_name_filter"), "{result}");
    assert!(result.contains("file_filter"), "{result}");

    let location_filter = runtime.dispatch(ToolRequest {
        id: serde_json::json!("location-filter"),
        tool: "indexed_search".to_string(),
        arguments: serde_json::json!({
            "index": index_path,
            "query": "file:auth.rs",
            "limit": 10,
            "dir": "src",
            "line": 2,
            "explain": true
        }),
    });
    assert!(
        location_filter.error.is_none(),
        "{:?}",
        location_filter.error
    );
    let first = &location_filter.result.as_ref().unwrap()[0];
    assert_eq!(first["path"], serde_json::json!("src/auth.rs"));
    assert_eq!(first["match_lines"], serde_json::json!([2]));
    assert!(
        first["snippet"]
            .as_str()
            .unwrap()
            .contains("2: pub fn issue_token"),
        "{first}"
    );
    assert!(
        first["explanation"]
            .as_array()
            .unwrap()
            .iter()
            .any(|signal| signal["kind"] == serde_json::json!("line_filter")
                && signal["value"] == serde_json::json!("2")),
        "{first}"
    );

    let generated_true = runtime.dispatch(ToolRequest {
        id: serde_json::json!("generated-true"),
        tool: "indexed_search".to_string(),
        arguments: serde_json::json!({
            "index": index_path,
            "query": "issue token",
            "limit": 10,
            "generated": true
        }),
    });
    assert!(generated_true.error.is_none(), "{:?}", generated_true.error);
    let result = serde_json::to_string(&generated_true.result).unwrap();
    assert!(result.contains("generated/auth.rs"), "{result}");
    assert!(result.contains("src/generated_symbol.rs"), "{result}");
    assert!(!result.contains("src/auth.rs"), "{result}");

    let related_symbols = runtime.dispatch(ToolRequest {
        id: serde_json::json!("related-symbol-filters"),
        tool: "related_symbols".to_string(),
        arguments: serde_json::json!({
            "repo": repo.path(),
            "query": "issue token",
            "limit": 10,
            "kind": "function",
            "exclude_content": "GeneratedSessionManager"
        }),
    });
    assert!(
        related_symbols.error.is_none(),
        "{:?}",
        related_symbols.error
    );
    let result = serde_json::to_string(&related_symbols.result).unwrap();
    assert!(result.contains("src/auth.rs"), "{result}");
    assert!(!result.contains("generated/auth.rs"), "{result}");
    assert!(!result.contains("src/generated_symbol.rs"), "{result}");

    let related_index_symbols = runtime.dispatch(ToolRequest {
        id: serde_json::json!("related-index-symbol-filters"),
        tool: "related_index_symbols".to_string(),
        arguments: serde_json::json!({
            "index": index_path,
            "query": "issue token",
            "limit": 10,
            "kind": "function",
            "exclude_text": "GeneratedSessionManager"
        }),
    });
    assert!(
        related_index_symbols.error.is_none(),
        "{:?}",
        related_index_symbols.error
    );
    let result = serde_json::to_string(&related_index_symbols.result).unwrap();
    assert!(result.contains("src/auth.rs"), "{result}");
    assert!(!result.contains("generated/auth.rs"), "{result}");
    assert!(!result.contains("src/generated_symbol.rs"), "{result}");

    let related_files = runtime.dispatch(ToolRequest {
        id: serde_json::json!("related-file-filters"),
        tool: "related_files".to_string(),
        arguments: serde_json::json!({
            "repo": repo.path(),
            "path": "src/auth.rs",
            "limit": 10,
            "test": true,
            "exclude_content": "GeneratedSessionManager"
        }),
    });
    assert!(related_files.error.is_none(), "{:?}", related_files.error);
    let result = serde_json::to_string(&related_files.result).unwrap();
    assert!(result.contains("tests/auth_test.rs"), "{result}");
    assert!(!result.contains("generated/auth_test.rs"), "{result}");
    assert!(!result.contains("generated/auth.rs"), "{result}");

    let related_index_files = runtime.dispatch(ToolRequest {
        id: serde_json::json!("related-index-file-filters"),
        tool: "related_index_files".to_string(),
        arguments: serde_json::json!({
            "index": index_path,
            "path": "src/auth.rs",
            "limit": 10,
            "test": true,
            "exclude_text": "GeneratedSessionManager"
        }),
    });
    assert!(
        related_index_files.error.is_none(),
        "{:?}",
        related_index_files.error
    );
    let result = serde_json::to_string(&related_index_files.result).unwrap();
    assert!(result.contains("tests/auth_test.rs"), "{result}");
    assert!(!result.contains("generated/auth_test.rs"), "{result}");
    assert!(!result.contains("generated/auth.rs"), "{result}");
}

#[test]
fn runtime_discovers_repos_by_tool_request() {
    let root = tempfile::tempdir().unwrap();
    write(
        &root.path().join("workspace/billing/Cargo.toml"),
        "[package]\nname='billing'\nversion='0.1.0'\nedition='2024'\n",
    );
    write(
        &root.path().join("workspace/auth/package.json"),
        "{\"scripts\":{\"test\":\"vitest\"}}\n",
    );
    write(
        &root
            .path()
            .join("workspace/node_modules/ignored/Cargo.toml"),
        "[package]\nname='ignored'\nversion='0.1.0'\nedition='2024'\n",
    );

    let runtime = ToolRuntime::default();
    let response = runtime.dispatch(ToolRequest {
        id: serde_json::json!("discover"),
        tool: "discover_repos".to_string(),
        arguments: serde_json::json!({
            "root": root.path(),
            "max_depth": 2,
            "limit": 10
        }),
    });
    assert!(response.error.is_none(), "{:?}", response.error);
    let result = serde_json::to_string(&response.result).unwrap();
    assert!(result.contains("\"repos_found\":2"), "{result}");
    assert!(result.contains("\"name\":\"auth\""), "{result}");
    assert!(result.contains("\"name\":\"billing\""), "{result}");
    assert!(!result.contains("node_modules"), "{result}");
}

#[test]
fn runtime_discovery_suppresses_nested_manifest_projects_under_git_roots() {
    let root = tempfile::tempdir().unwrap();
    let repo = root.path().join("workspace/service");
    write(
        &repo.join("package.json"),
        "{\"scripts\":{\"test\":\"vitest\"}}\n",
    );
    write(
        &repo.join("packages/ui/package.json"),
        "{\"scripts\":{\"test\":\"vitest\"}}\n",
    );
    git(&repo, &["init", "-b", "main"]);

    let runtime = ToolRuntime::default();
    let default = runtime.dispatch(ToolRequest {
        id: serde_json::json!("discover"),
        tool: "discover_repos".to_string(),
        arguments: serde_json::json!({
            "root": root.path(),
            "max_depth": 4,
            "limit": 20
        }),
    });
    assert!(default.error.is_none(), "{:?}", default.error);
    let result = serde_json::to_string(&default.result).unwrap();
    assert!(result.contains("\"repos_found\":1"), "{result}");
    assert!(result.contains("\"name\":\"service\""), "{result}");
    assert!(!result.contains("\"name\":\"ui\""), "{result}");

    let nested = runtime.dispatch(ToolRequest {
        id: serde_json::json!("discover"),
        tool: "discover_repos".to_string(),
        arguments: serde_json::json!({
            "root": root.path(),
            "max_depth": 4,
            "limit": 20,
            "nested_manifests": true
        }),
    });
    assert!(nested.error.is_none(), "{:?}", nested.error);
    let result = serde_json::to_string(&nested.result).unwrap();
    assert!(result.contains("\"repos_found\":2"), "{result}");
    assert!(result.contains("\"name\":\"ui\""), "{result}");
}

#[test]
fn runtime_discovers_repo_families_with_git_metadata() {
    let root = tempfile::tempdir().unwrap();
    let repo = root.path().join("workspace/project");
    write(
        &repo.join("Cargo.toml"),
        "[package]\nname='project'\nversion='0.1.0'\nedition='2024'\n",
    );
    git(&repo, &["init", "-b", "main"]);
    git(
        &repo,
        &[
            "remote",
            "add",
            "origin",
            "https://github.com/example/project.git",
        ],
    );
    git(&repo, &["add", "Cargo.toml"]);
    git(
        &repo,
        &[
            "-c",
            "user.name=Orient Tests",
            "-c",
            "user.email=orient@example.com",
            "commit",
            "-m",
            "init",
        ],
    );
    git(
        &repo,
        &[
            "worktree",
            "add",
            "-b",
            "feature/search",
            "../project-feature",
        ],
    );

    let runtime = ToolRuntime::default();
    let response = runtime.dispatch(ToolRequest {
        id: serde_json::json!("discover"),
        tool: "discover_repos".to_string(),
        arguments: serde_json::json!({
            "root": root.path(),
            "max_depth": 2,
            "limit": 10,
            "git_metadata": true,
            "tracked_files": true
        }),
    });
    assert!(response.error.is_none(), "{:?}", response.error);
    let result = serde_json::to_string(&response.result).unwrap();
    assert!(result.contains("\"repos_found\":2"), "{result}");
    assert!(result.contains("\"families\""), "{result}");
    assert!(result.contains("\"checkouts\":2"), "{result}");
    assert!(result.contains("\"worktrees\":1"), "{result}");
    assert!(result.contains("\"clones\":1"), "{result}");
    assert!(result.contains("\"tracked_files\":2"), "{result}");
    assert!(result.contains("\"git_kind\":\"worktree\""), "{result}");
    assert!(result.contains("\"branch\":\"feature/search\""), "{result}");
}

#[test]
fn runtime_discovery_can_limit_repeated_repo_families() {
    let root = tempfile::tempdir().unwrap();
    let repo = root.path().join("workspace/project");
    write(
        &repo.join("Cargo.toml"),
        "[package]\nname='project'\nversion='0.1.0'\nedition='2024'\n",
    );
    git(&repo, &["init", "-b", "main"]);
    git(
        &repo,
        &[
            "remote",
            "add",
            "origin",
            "https://github.com/example/project.git",
        ],
    );
    git(&repo, &["add", "Cargo.toml"]);
    git(
        &repo,
        &[
            "-c",
            "user.name=Orient Tests",
            "-c",
            "user.email=orient@example.com",
            "commit",
            "-m",
            "init",
        ],
    );
    git(
        &repo,
        &[
            "worktree",
            "add",
            "-b",
            "feature/search",
            "../project-feature",
        ],
    );

    let runtime = ToolRuntime::default();
    let response = runtime.dispatch(ToolRequest {
        id: serde_json::json!("discover"),
        tool: "discover_repos".to_string(),
        arguments: serde_json::json!({
            "root": root.path(),
            "max_depth": 2,
            "limit": 10,
            "family_limit": 1
        }),
    });
    assert!(response.error.is_none(), "{:?}", response.error);
    let result = response.result.unwrap();
    assert_eq!(result["candidates_found"], 2);
    assert_eq!(result["repos_found"], 1);
    assert_eq!(result["family_limit"], 1);
    assert_eq!(result["repos"][0]["name"], "project");
    assert_eq!(result["families"][0]["checkouts"], 2);
    assert!(
        result["families"][0]["paths"]
            .as_array()
            .unwrap()
            .iter()
            .any(|path| path.as_str().unwrap().contains("project-feature"))
    );
}

#[test]
fn runtime_indexes_shards_from_discovered_root() {
    let root = tempfile::tempdir().unwrap();
    write(
        &root.path().join("workspace/billing/src/lib.rs"),
        "pub fn invoice_total() -> usize { 42 }\n",
    );
    write(
        &root.path().join("workspace/billing/Cargo.toml"),
        "[package]\nname='billing'\nversion='0.1.0'\nedition='2024'\n",
    );
    write(
        &root.path().join("workspace/auth/src/lib.rs"),
        "pub fn issue_token() -> &'static str { \"token\" }\n",
    );
    write(
        &root.path().join("workspace/auth/Cargo.toml"),
        "[package]\nname='auth'\nversion='0.1.0'\nedition='2024'\n",
    );
    let shard_dir = tempfile::tempdir().unwrap();

    let runtime = ToolRuntime::default();
    let build = runtime.dispatch(ToolRequest {
        id: serde_json::json!("index"),
        tool: "index_shards".to_string(),
        arguments: serde_json::json!({
            "discover-roots": [root.path()],
            "max-depth": 2,
            "nested-manifests": true,
            "output-dir": shard_dir.path()
        }),
    });
    assert!(build.error.is_none(), "{:?}", build.error);
    let build_result = build.result.unwrap();
    assert_eq!(build_result["shards"], serde_json::json!(2));
    assert_eq!(build_result["discovery"][0]["selected_repos"], 2);
    assert_eq!(build_result["discovery"][0]["candidates_found"], 2);

    let search = runtime.dispatch(ToolRequest {
        id: serde_json::json!("search"),
        tool: "search_shards".to_string(),
        arguments: serde_json::json!({
            "index_dir": shard_dir.path(),
            "query": "invoice_total"
        }),
    });
    assert!(search.error.is_none(), "{:?}", search.error);
    let result = serde_json::to_string(&search.result).unwrap();
    assert!(result.contains("billing/src/lib.rs"), "{result}");
}

#[test]
fn runtime_shard_repo_map_reports_git_metadata() {
    let repo = tempfile::tempdir().unwrap();
    write(
        &repo.path().join("src/lib.rs"),
        "pub fn unique_branch_token() -> &'static str { \"needle\" }\n",
    );
    write(
        &repo.path().join("Cargo.toml"),
        "[package]\nname='shard-project'\nversion='0.1.0'\nedition='2024'\n",
    );
    write(
        &repo.path().join("MODULE.bazel"),
        "module(name = \"shard_project\")\n",
    );
    write(&repo.path().join("Justfile"), "test:\n    cargo test\n");
    git(repo.path(), &["init", "-b", "shard-feature-branch"]);
    git(
        repo.path(),
        &[
            "remote",
            "add",
            "origin",
            "https://github.com/example/shard-project.git",
        ],
    );
    git(repo.path(), &["add", "."]);
    git(
        repo.path(),
        &[
            "-c",
            "user.name=Orient Tests",
            "-c",
            "user.email=orient@example.com",
            "commit",
            "-m",
            "init",
        ],
    );
    let shard_dir = tempfile::tempdir().unwrap();

    let runtime = ToolRuntime::default();
    let build = runtime.dispatch(ToolRequest {
        id: serde_json::json!("index"),
        tool: "index_shards".to_string(),
        arguments: serde_json::json!({
            "repos": [repo.path()],
            "output_dir": shard_dir.path()
        }),
    });
    assert!(build.error.is_none(), "{:?}", build.error);

    let search = runtime.dispatch(ToolRequest {
        id: serde_json::json!("search"),
        tool: "search_shards".to_string(),
        arguments: serde_json::json!({
            "index_dir": shard_dir.path(),
            "query": "unique branch token",
            "repo": "shard-feature-branch",
            "require_all": true
        }),
    });
    assert!(search.error.is_none(), "{:?}", search.error);
    let search_result = serde_json::to_string(&search.result).unwrap();
    assert!(search_result.contains("src/lib.rs"), "{search_result}");

    let search_by_branch = runtime.dispatch(ToolRequest {
        id: serde_json::json!("search-branch"),
        tool: "search_shards".to_string(),
        arguments: serde_json::json!({
            "index_dir": shard_dir.path(),
            "query": "unique branch token",
            "branch": "shard-feature-branch",
            "require_all": true
        }),
    });
    assert!(
        search_by_branch.error.is_none(),
        "{:?}",
        search_by_branch.error
    );
    let branch_result = serde_json::to_string(&search_by_branch.result).unwrap();
    assert!(branch_result.contains("src/lib.rs"), "{branch_result}");

    let search_by_origin = runtime.dispatch(ToolRequest {
        id: serde_json::json!("search-origin"),
        tool: "search_shards".to_string(),
        arguments: serde_json::json!({
            "index_dir": shard_dir.path(),
            "query": "unique branch token",
            "origin": "example/shard-project",
            "require_all": true
        }),
    });
    assert!(
        search_by_origin.error.is_none(),
        "{:?}",
        search_by_origin.error
    );
    let origin_result = serde_json::to_string(&search_by_origin.result).unwrap();
    assert!(origin_result.contains("src/lib.rs"), "{origin_result}");

    let auto_by_git_scope = runtime.dispatch(ToolRequest {
        id: serde_json::json!("auto-git-scope"),
        tool: "search_auto".to_string(),
        arguments: serde_json::json!({
            "index_dir": shard_dir.path(),
            "query": "branch:shard-feature-branch origin:example/shard-project unique_branch_token",
            "limit": 5
        }),
    });
    assert!(
        auto_by_git_scope.error.is_none(),
        "{:?}",
        auto_by_git_scope.error
    );
    let auto_by_git_scope = auto_by_git_scope.result.unwrap();
    assert_eq!(auto_by_git_scope["surface"], "shards");
    assert_eq!(
        auto_by_git_scope["repo_map_request"]["arguments"]["branch"],
        "shard-feature-branch"
    );
    assert_eq!(
        auto_by_git_scope["repo_map_request"]["arguments"]["origin"],
        "example/shard-project"
    );
    let scoped_auto_map = runtime.dispatch(ToolRequest {
        id: serde_json::json!("auto-git-scope-map"),
        tool: auto_by_git_scope["repo_map_request"]["tool"]
            .as_str()
            .unwrap()
            .to_string(),
        arguments: auto_by_git_scope["repo_map_request"]["arguments"].clone(),
    });
    assert!(
        scoped_auto_map.error.is_none(),
        "{:?}",
        scoped_auto_map.error
    );
    let scoped_auto_map = serde_json::to_string(&scoped_auto_map.result).unwrap();
    assert!(
        scoped_auto_map.contains("\"branch\":\"shard-feature-branch\""),
        "{scoped_auto_map}"
    );

    let excluded_branch = runtime.dispatch(ToolRequest {
        id: serde_json::json!("exclude-branch"),
        tool: "search_shards".to_string(),
        arguments: serde_json::json!({
            "index_dir": shard_dir.path(),
            "query": "unique branch token",
            "exclude_branch": "shard-feature-branch",
            "require_all": true
        }),
    });
    assert!(
        excluded_branch.error.is_none(),
        "{:?}",
        excluded_branch.error
    );
    assert_eq!(excluded_branch.result, Some(serde_json::json!([])));

    let map = runtime.dispatch(ToolRequest {
        id: serde_json::json!("map"),
        tool: "shard_repo_map".to_string(),
        arguments: serde_json::json!({
            "index_dir": shard_dir.path(),
            "origin": "example/shard-project",
            "symbols": 5,
            "tests": 5
        }),
    });
    assert!(map.error.is_none(), "{:?}", map.error);
    let result = serde_json::to_string(&map.result).unwrap();
    assert!(result.contains("\"git\""), "{result}");
    assert!(
        result.contains("\"branch\":\"shard-feature-branch\""),
        "{result}"
    );
    assert!(
        result.contains("https://github.com/example/shard-project.git"),
        "{result}"
    );
    assert!(result.contains("bazel test //..."), "{result}");
    assert!(result.contains("MODULE.bazel"), "{result}");
    assert!(result.contains("just test"), "{result}");
    assert!(result.contains("Justfile"), "{result}");
}

#[test]
fn runtime_refresh_if_stale_picks_up_git_branch_metadata_changes() {
    let repo = tempfile::tempdir().unwrap();
    let matching_repo = tempfile::tempdir().unwrap();
    let other_repo = tempfile::tempdir().unwrap();
    write(
        &repo.path().join("src/lib.rs"),
        "pub fn branch_switch_token() -> &'static str { \"needle\" }\n",
    );
    write(
        &repo.path().join("Cargo.toml"),
        "[package]\nname='branch-project'\nversion='0.1.0'\nedition='2024'\n",
    );
    write(
        &other_repo.path().join("src/lib.rs"),
        "pub fn other_branch_switch_token() {}\n",
    );
    write(
        &matching_repo.path().join("src/lib.rs"),
        "pub fn already_on_new_branch_token() {}\n",
    );
    write(
        &matching_repo.path().join("Cargo.toml"),
        "[package]\nname='matching-branch-project'\nversion='0.1.0'\nedition='2024'\n",
    );
    write(
        &other_repo.path().join("Cargo.toml"),
        "[package]\nname='other-branch-project'\nversion='0.1.0'\nedition='2024'\n",
    );
    git(repo.path(), &["init", "-b", "old-branch"]);
    git(repo.path(), &["add", "."]);
    git(
        repo.path(),
        &[
            "-c",
            "user.name=Orient Tests",
            "-c",
            "user.email=orient@example.com",
            "commit",
            "-m",
            "init",
        ],
    );
    git(matching_repo.path(), &["init", "-b", "new-branch"]);
    git(matching_repo.path(), &["add", "."]);
    git(
        matching_repo.path(),
        &[
            "-c",
            "user.name=Orient Tests",
            "-c",
            "user.email=orient@example.com",
            "commit",
            "-m",
            "init",
        ],
    );
    git(other_repo.path(), &["init", "-b", "other-old-branch"]);
    git(other_repo.path(), &["add", "."]);
    git(
        other_repo.path(),
        &[
            "-c",
            "user.name=Orient Tests",
            "-c",
            "user.email=orient@example.com",
            "commit",
            "-m",
            "init",
        ],
    );

    let shard_dir = tempfile::tempdir().unwrap();
    build_shards(
        &[
            repo.path().to_path_buf(),
            matching_repo.path().to_path_buf(),
            other_repo.path().to_path_buf(),
        ],
        shard_dir.path(),
    )
    .unwrap();
    let repo_root = repo.path().canonicalize().unwrap();
    let other_repo_root = other_repo.path().canonicalize().unwrap();

    git(repo.path(), &["checkout", "-b", "new-branch"]);
    git(other_repo.path(), &["checkout", "-b", "other-new-branch"]);
    let stale = shard_status(shard_dir.path()).unwrap();
    assert!(stale.stale, "{stale:?}");
    assert_eq!(stale.stale_shards, 2);
    assert_eq!(stale.git_metadata_changed, 2);
    assert!(
        stale.shards.iter().any(|shard| !shard.git_metadata_stale
            && shard
                .indexed_git
                .as_ref()
                .and_then(|git| git.branch.as_deref())
                == Some("new-branch")),
        "{stale:?}"
    );
    let stale_current = stale
        .shards
        .iter()
        .find(|shard| shard.root == repo_root)
        .unwrap();
    assert!(stale_current.git_metadata_stale);
    assert_eq!(
        stale_current
            .indexed_git
            .as_ref()
            .and_then(|git| git.branch.as_deref()),
        Some("old-branch")
    );
    assert_eq!(
        stale_current
            .current_git
            .as_ref()
            .and_then(|git| git.branch.as_deref()),
        Some("new-branch")
    );

    let runtime = ToolRuntime::default();
    runtime.warm_shards(shard_dir.path().to_path_buf()).unwrap();
    let refreshed = runtime.dispatch(ToolRequest {
        id: serde_json::json!("search-new-branch"),
        tool: "search_auto".to_string(),
        arguments: serde_json::json!({
            "cwd": repo.path().join("src"),
            "index_dir": shard_dir.path(),
            "query": "branch:new-branch branch switch token",
            "limit": 5,
            "require_all": true,
            "refresh_if_stale": true
        }),
    });
    assert!(refreshed.error.is_none(), "{:?}", refreshed.error);
    let refreshed = refreshed.result.unwrap();
    assert_eq!(refreshed["surface"], "shards");
    assert_eq!(
        refreshed["repo_map_request"]["arguments"]["branch"],
        "new-branch"
    );
    let refreshed = serde_json::to_string(&refreshed).unwrap();
    assert!(refreshed.contains("src/lib.rs"), "{refreshed}");
    assert!(refreshed.contains("branch_switch_token"), "{refreshed}");

    let fresh = shard_status(shard_dir.path()).unwrap();
    assert!(fresh.stale, "{fresh:?}");
    assert_eq!(fresh.git_metadata_changed, 1);
    let fresh_current = fresh
        .shards
        .iter()
        .find(|shard| shard.root == repo_root)
        .unwrap();
    assert!(!fresh_current.git_metadata_stale, "{fresh_current:?}");
    assert_eq!(
        fresh_current
            .indexed_git
            .as_ref()
            .and_then(|git| git.branch.as_deref()),
        Some("new-branch")
    );
    let stale_other = fresh
        .shards
        .iter()
        .find(|shard| shard.root == other_repo_root)
        .unwrap();
    assert!(stale_other.git_metadata_stale, "{stale_other:?}");
    assert_eq!(
        stale_other
            .indexed_git
            .as_ref()
            .and_then(|git| git.branch.as_deref()),
        Some("other-old-branch")
    );
}

#[test]
fn runtime_indexes_shards_from_multiple_discovered_roots() {
    let left = tempfile::tempdir().unwrap();
    let right = tempfile::tempdir().unwrap();
    write(
        &left.path().join("service/src/lib.rs"),
        "pub fn service_session() {}\n",
    );
    write(
        &left.path().join("service/Cargo.toml"),
        "[package]\nname='service'\nversion='0.1.0'\nedition='2024'\n",
    );
    write(
        &right.path().join("worker/src/lib.rs"),
        "pub fn worker_session() {}\n",
    );
    write(
        &right.path().join("worker/Cargo.toml"),
        "[package]\nname='worker'\nversion='0.1.0'\nedition='2024'\n",
    );
    let shard_dir = tempfile::tempdir().unwrap();

    let runtime = ToolRuntime::default();
    let build = runtime.dispatch(ToolRequest {
        id: serde_json::json!("index"),
        tool: "index_shards".to_string(),
        arguments: serde_json::json!({
            "discover_roots": [left.path(), right.path()],
            "max_depth": 2,
            "output_dir": shard_dir.path()
        }),
    });
    assert!(build.error.is_none(), "{:?}", build.error);
    assert_eq!(build.result.unwrap()["shards"], serde_json::json!(2));

    let search = runtime.dispatch(ToolRequest {
        id: serde_json::json!("search"),
        tool: "search_shards".to_string(),
        arguments: serde_json::json!({
            "index_dir": shard_dir.path(),
            "query": "worker_session"
        }),
    });
    assert!(search.error.is_none(), "{:?}", search.error);
    let result = serde_json::to_string(&search.result).unwrap();
    assert!(result.contains("worker/src/lib.rs"), "{result}");
}

#[test]
fn runtime_index_shards_refuses_accidental_manifest_shrink_without_force() {
    let workspace = tempfile::tempdir().unwrap();
    let auth_repo = workspace.path().join("auth");
    write(&auth_repo.join("src/lib.rs"), "pub fn issue_token() {}\n");
    write(
        &auth_repo.join("Cargo.toml"),
        "[package]\nname='auth'\nversion='0.1.0'\nedition='2024'\n",
    );
    let billing_repo = workspace.path().join("billing");
    write(
        &billing_repo.join("src/lib.rs"),
        "pub fn invoice_total() {}\n",
    );
    write(
        &billing_repo.join("Cargo.toml"),
        "[package]\nname='billing'\nversion='0.1.0'\nedition='2024'\n",
    );
    let shard_dir = tempfile::tempdir().unwrap();
    let runtime = ToolRuntime::default();

    let build = runtime.dispatch(ToolRequest {
        id: serde_json::json!("build"),
        tool: "index_shards".to_string(),
        arguments: serde_json::json!({
            "repos": [auth_repo, billing_repo],
            "output_dir": shard_dir.path()
        }),
    });
    assert!(build.error.is_none(), "{:?}", build.error);
    assert_eq!(build.result.unwrap()["shards"], serde_json::json!(2));

    let shrink = runtime.dispatch(ToolRequest {
        id: serde_json::json!("shrink"),
        tool: "index_shards".to_string(),
        arguments: serde_json::json!({
            "repos": [workspace.path().join("auth")],
            "output_dir": shard_dir.path()
        }),
    });
    let error = shrink.error.unwrap();
    assert!(
        error.contains("refusing to overwrite shard directory"),
        "{error}"
    );

    let forced = runtime.dispatch(ToolRequest {
        id: serde_json::json!("force"),
        tool: "index_shards".to_string(),
        arguments: serde_json::json!({
            "repos": [workspace.path().join("auth")],
            "output_dir": shard_dir.path(),
            "force": true
        }),
    });
    assert!(forced.error.is_none(), "{:?}", forced.error);
    assert_eq!(forced.result.unwrap()["shards"], serde_json::json!(1));
}

#[test]
fn runtime_ensures_shards_builds_refreshes_and_registers() {
    let root = tempfile::tempdir().unwrap();
    write(
        &root.path().join("service/src/lib.rs"),
        "pub fn service_session() {}\n",
    );
    write(
        &root.path().join("service/Cargo.toml"),
        "[package]\nname='service'\nversion='0.1.0'\nedition='2024'\n",
    );
    let shard_dir = root.path().join(".orient-shards");

    let runtime = ToolRuntime::default();
    let build = runtime.dispatch(ToolRequest {
        id: serde_json::json!("ensure-build"),
        tool: "ensure_shards".to_string(),
        arguments: serde_json::json!({
            "discover_root": root.path(),
            "max_depth": 2,
            "output_dir": shard_dir
        }),
    });
    assert!(build.error.is_none(), "{:?}", build.error);
    let build_result = build.result.unwrap();
    assert_eq!(build_result["stats"]["action"], serde_json::json!("build"));
    assert_eq!(
        build_result["stats"]["discovery"][0]["selected_repos"],
        serde_json::json!(1)
    );
    assert_eq!(build_result["registered_indexes"], serde_json::json!(1));
    assert_eq!(build_result["cached_indexes"], serde_json::json!(0));

    write(
        &root.path().join("service/src/extra.rs"),
        "pub fn extra_service_session() {}\n",
    );
    let refresh = runtime.dispatch(ToolRequest {
        id: serde_json::json!("ensure-refresh"),
        tool: "ensure_shards".to_string(),
        arguments: serde_json::json!({
            "output_dir": root.path().join(".orient-shards")
        }),
    });
    assert!(refresh.error.is_none(), "{:?}", refresh.error);
    let refresh_result = refresh.result.unwrap();
    assert_eq!(
        refresh_result["stats"]["action"],
        serde_json::json!("refresh")
    );
    assert_eq!(refresh_result["registered_indexes"], serde_json::json!(1));
    assert_eq!(refresh_result["cached_indexes"], serde_json::json!(0));
    assert!(refresh_result["stats"]["refreshed_files"].as_u64().unwrap() >= 1);

    write(
        &root.path().join("billing/src/lib.rs"),
        "pub fn invoice_total() -> usize { 42 }\n",
    );
    write(
        &root.path().join("billing/Cargo.toml"),
        "[package]\nname='billing'\nversion='0.1.0'\nedition='2024'\n",
    );
    let add = runtime.dispatch(ToolRequest {
        id: serde_json::json!("ensure-add"),
        tool: "ensure_shards".to_string(),
        arguments: serde_json::json!({
            "discover_root": root.path(),
            "max_depth": 2,
            "output_dir": root.path().join(".orient-shards")
        }),
    });
    assert!(add.error.is_none(), "{:?}", add.error);
    let add_result = add.result.unwrap();
    assert_eq!(
        add_result["stats"]["action"],
        serde_json::json!("refresh+add")
    );
    assert_eq!(add_result["stats"]["added_shards"], serde_json::json!(1));
    assert_eq!(add_result["stats"]["shards"], serde_json::json!(2));
    assert_eq!(add_result["registered_indexes"], serde_json::json!(2));

    let search = runtime.dispatch(ToolRequest {
        id: serde_json::json!("search-added"),
        tool: "search_shards".to_string(),
        arguments: serde_json::json!({
            "index_dir": root.path().join(".orient-shards"),
            "query": "invoice_total"
        }),
    });
    assert!(search.error.is_none(), "{:?}", search.error);
    let result = serde_json::to_string(&search.result).unwrap();
    assert!(result.contains("billing/src/lib.rs"), "{result}");

    write(
        &root.path().join("service/src/after_status.rs"),
        "pub fn after_status_session() {}\n",
    );
    let status = runtime.dispatch(ToolRequest {
        id: serde_json::json!("shard-status"),
        tool: "shard_status".to_string(),
        arguments: serde_json::json!({
            "index_dir": root.path().join(".orient-shards")
        }),
    });
    assert!(status.error.is_none(), "{:?}", status.error);
    let status = status.result.unwrap();
    assert_eq!(status["stale"], serde_json::json!(true));
    assert_eq!(status["shard_count"], serde_json::json!(2));
    assert_eq!(status["stale_shards"], serde_json::json!(1));
    assert!(status["manifest_bytes"].as_u64().unwrap() > 0);
    assert!(status["manifest_sidecar_bytes"].as_u64().unwrap() > 0);
    assert!(status["manifest_prefilter_bytes"].as_u64().unwrap() > 0);
    assert!(status["manifest_route_bytes"].as_u64().unwrap() > 0);
    assert!(status["manifest_route_exact_terms"].as_u64().unwrap() > 0);
    assert!(status["manifest_route_trigram_terms"].as_u64().unwrap() > 0);
    assert!(
        status["manifest_route_substring_filter_shards"]
            .as_u64()
            .unwrap()
            > 0
    );
    assert!(status["index_bytes"].as_u64().unwrap() > 0);
    assert!(status["source_bytes"].as_u64().unwrap() > 0);
    assert!(status["content_snapshot_bytes"].as_u64().unwrap() > 0);
    assert!(status["line_offset_bytes"].as_u64().unwrap() > 0);
    assert!(status["posting_entries"].as_u64().unwrap() > 0);
    assert!(status["compressed_posting_bytes"].as_u64().unwrap() > 0);
    let shard_names = status["shards"]
        .as_array()
        .unwrap()
        .iter()
        .map(|shard| shard["name"].as_str().unwrap())
        .collect::<Vec<_>>();
    assert_eq!(shard_names, vec!["service", "billing"]);
    assert!(
        status["shards"][0]["status"]["source_bytes"]
            .as_u64()
            .unwrap()
            > 0
    );
    assert!(
        status["shards"][0]["status"]["content_snapshot_bytes"]
            .as_u64()
            .unwrap()
            > 0
    );
    assert_eq!(status["added_files"], serde_json::json!(1));

    let stale_search = runtime.dispatch(ToolRequest {
        id: serde_json::json!("stale-shard-search"),
        tool: "search_shards".to_string(),
        arguments: serde_json::json!({
            "index_dir": root.path().join(".orient-shards"),
            "query": "after_status_session"
        }),
    });
    assert!(stale_search.error.is_none(), "{:?}", stale_search.error);
    assert_eq!(stale_search.result.unwrap(), serde_json::json!([]));

    let refreshed_search = runtime.dispatch(ToolRequest {
        id: serde_json::json!("fresh-shard-search"),
        tool: "search_shards".to_string(),
        arguments: serde_json::json!({
            "index_dir": root.path().join(".orient-shards"),
            "query": "after_status_session",
            "refresh_if_stale": true
        }),
    });
    assert!(
        refreshed_search.error.is_none(),
        "{:?}",
        refreshed_search.error
    );
    let result = serde_json::to_string(&refreshed_search.result).unwrap();
    assert!(result.contains("service/src/after_status.rs"), "{result}");
}

#[test]
fn runtime_warms_index_by_tool_request() {
    let repo = tempfile::tempdir().unwrap();
    write(
        &repo.path().join("src/auth.rs"),
        "pub struct SessionManager;\npub fn issue_token() {}\n",
    );
    write(
        &repo.path().join("Cargo.toml"),
        "[package]\nname='sample'\nversion='0.1.0'\nedition='2024'\n",
    );
    let index_path = repo.path().join(".orient/index");
    FastIndex::build(repo.path())
        .unwrap()
        .save(&index_path)
        .unwrap();

    let runtime = ToolRuntime::default();
    let warm = runtime.dispatch(ToolRequest {
        id: serde_json::json!("warm"),
        tool: "warm_index".to_string(),
        arguments: serde_json::json!({
            "index": index_path
        }),
    });
    assert!(warm.error.is_none(), "{:?}", warm.error);
    assert_eq!(warm.result.unwrap()["cached_indexes"], serde_json::json!(1));

    let status = runtime.dispatch(ToolRequest {
        id: serde_json::json!("status"),
        tool: "daemon_status".to_string(),
        arguments: serde_json::json!({ "details": true }),
    });
    let result = status.result.unwrap();
    assert_eq!(
        result["max_cached_indexes"],
        serde_json::json!(DEFAULT_MAX_CACHED_INDEXES)
    );
    assert_eq!(result["process_id"], serde_json::json!(std::process::id()));
    assert!(result["started_at_unix_secs"].as_u64().unwrap() > 0);
    assert!(result["uptime_secs"].as_u64().unwrap() < 60);
    assert_eq!(
        result["max_shard_workers"],
        serde_json::json!(DEFAULT_MAX_SHARD_WORKERS)
    );
    assert_eq!(result["cached_indexes"], serde_json::json!(1));
    assert_eq!(
        result["search_auto_default"]["surface"],
        serde_json::json!("indexed")
    );
    assert_eq!(
        result["search_auto_default"]["source"],
        serde_json::json!("single_warmed_index")
    );
    assert!(
        result["search_auto_default"]["target"]
            .as_str()
            .unwrap()
            .ends_with(".orient/index")
    );
    assert!(result.get("process_cwd").is_none(), "{result}");
    assert!(
        result["cached_index_paths"]
            .as_array()
            .unwrap()
            .iter()
            .any(|path| path.as_str().unwrap().ends_with(".orient/index"))
    );
    assert_eq!(
        result["cached_index_details"][0]["files"],
        serde_json::json!(2)
    );
    assert_eq!(
        result["cached_index_details"][0]["root"],
        serde_json::json!(repo.path().canonicalize().unwrap().to_string_lossy())
    );
    assert_eq!(
        result["cached_index_details"][0]["symbols"],
        serde_json::json!(2)
    );
    assert!(
        result["cached_index_details"][0]["index_bytes"]
            .as_u64()
            .unwrap()
            > 0
    );
    assert_eq!(
        result["cached_index_details"][0]["disk_missing"],
        serde_json::json!(false)
    );
    assert_eq!(
        result["cached_index_details"][0]["disk_changed"],
        serde_json::json!(false)
    );
    assert!(
        result["cached_index_details"][0]["content_snapshot_bytes"]
            .as_u64()
            .unwrap()
            > 0
    );
    assert!(
        result["cached_index_details"][0]["line_offset_bytes"]
            .as_u64()
            .unwrap()
            > 0
    );
    assert!(
        result["cached_index_details"][0]["index"]
            .as_str()
            .unwrap()
            .ends_with(".orient/index")
    );
    assert_eq!(result["footprint"]["loaded_indexes"], serde_json::json!(1));
    assert_eq!(result["footprint"]["loaded_files"], serde_json::json!(2));
    assert_eq!(result["footprint"]["loaded_symbols"], serde_json::json!(2));
    assert_eq!(
        result["footprint"]["cached_shard_manifests"],
        serde_json::json!(0)
    );
    assert!(result["footprint"]["loaded_index_bytes"].as_u64().unwrap() > 0);
    assert!(
        result["footprint"]["loaded_content_snapshot_bytes"]
            .as_u64()
            .unwrap()
            > 0
    );
}

#[test]
fn daemon_status_suggests_registering_warmed_shard_indexes() {
    let repo = tempfile::tempdir().unwrap();
    write(
        &repo.path().join("src/auth.rs"),
        "pub struct SessionManager;\npub fn issue_token() {}\n",
    );
    write(
        &repo.path().join("Cargo.toml"),
        "[package]\nname='auth'\nversion='0.1.0'\nedition='2024'\n",
    );
    let shard_dir = tempfile::tempdir().unwrap();
    build_shards(&[repo.path().to_path_buf()], shard_dir.path()).unwrap();
    let manifest: serde_json::Value =
        serde_json::from_slice(&fs::read(shard_dir.path().join("manifest.json")).unwrap()).unwrap();
    let shard_index = shard_dir
        .path()
        .join(manifest["shards"][0]["index"].as_str().unwrap());

    let runtime = ToolRuntime::default();
    let warm = runtime.dispatch(ToolRequest {
        id: serde_json::json!("warm"),
        tool: "warm_index".to_string(),
        arguments: serde_json::json!({
            "index": shard_index
        }),
    });
    assert!(warm.error.is_none(), "{:?}", warm.error);

    let status = runtime.daemon_status();
    assert_eq!(
        status["daemon_version"],
        serde_json::json!(env!("CARGO_PKG_VERSION"))
    );
    assert_eq!(status["process_id"], serde_json::json!(std::process::id()));
    assert!(status["started_at_unix_secs"].as_u64().unwrap() > 0);
    assert!(status["uptime_secs"].as_u64().unwrap() < 60);
    assert_eq!(
        status["max_shard_workers"],
        serde_json::json!(DEFAULT_MAX_SHARD_WORKERS)
    );
    assert_eq!(
        status["search_auto_default"]["surface"],
        serde_json::json!("shards")
    );
    assert_eq!(
        status["search_auto_default"]["source"],
        serde_json::json!("single_warmed_shard_dir")
    );
    assert_eq!(status["cached_shard_manifests"], serde_json::json!(0));
    assert_eq!(
        status["repair_requests"][0]["kind"],
        serde_json::json!("register_warmed_shard_dir")
    );
    assert_eq!(
        status["repair_requests"][0]["request"]["tool"],
        serde_json::json!("register_shards")
    );
    assert_eq!(
        status["repair_requests"][0]["request"]["arguments"]["index_dir"],
        serde_json::json!(shard_dir.path().canonicalize().unwrap())
    );
    assert!(
        status["repair_requests"][0]["request"]["client_cli"]
            .as_str()
            .unwrap()
            .contains("orient client-jsonl")
    );

    let search = runtime.dispatch(ToolRequest {
        id: serde_json::json!("search"),
        tool: "search_auto".to_string(),
        arguments: serde_json::json!({
            "query": "issue token",
            "limit": 3,
            "require_all": true
        }),
    });
    assert!(search.error.is_none(), "{:?}", search.error);
    let search_result = search.result.unwrap();
    assert_eq!(search_result["surface"], serde_json::json!("shards"));
    assert!(
        serde_json::to_string(&search_result)
            .unwrap()
            .contains("src/auth.rs")
    );

    let request = &status["repair_requests"][0]["request"];
    let repair = runtime.dispatch(ToolRequest {
        id: serde_json::json!("repair"),
        tool: request["tool"].as_str().unwrap().to_string(),
        arguments: request["arguments"].clone(),
    });
    assert!(repair.error.is_none(), "{:?}", repair.error);
    let repaired_status = runtime.daemon_status();
    assert_eq!(
        repaired_status["search_auto_default"]["surface"],
        serde_json::json!("shards")
    );
    assert!(repaired_status.get("repair_requests").is_none());
}

#[test]
fn runtime_reuses_cached_index_after_initial_load() {
    let repo = tempfile::tempdir().unwrap();
    write(
        &repo.path().join("src/auth.rs"),
        "pub struct SessionManager;\npub fn issue_token() {}\n",
    );
    write(
        &repo.path().join("Cargo.toml"),
        "[package]\nname='sample'\nversion='0.1.0'\nedition='2024'\n",
    );
    let index_path = repo.path().join(".orient/index");
    FastIndex::build(repo.path())
        .unwrap()
        .save(&index_path)
        .unwrap();

    let runtime = ToolRuntime::default();
    let first = runtime.dispatch(ToolRequest {
        id: serde_json::json!("first"),
        tool: "indexed_search_code".to_string(),
        arguments: serde_json::json!({
            "index": index_path,
            "query": "issue token",
            "limit": 3,
            "require_all": true
        }),
    });
    assert!(first.error.is_none(), "{:?}", first.error);
    assert!(
        serde_json::to_string(&first.result)
            .unwrap()
            .contains("src/auth.rs"),
        "{:?}",
        first.result
    );

    fs::remove_file(&index_path).unwrap();
    let second = runtime.dispatch(ToolRequest {
        id: serde_json::json!("second"),
        tool: "indexed_search_code".to_string(),
        arguments: serde_json::json!({
            "index": index_path,
            "query": "issue token",
            "limit": 3,
            "require_all": true
        }),
    });
    assert!(second.error.is_none(), "{:?}", second.error);
    assert!(
        serde_json::to_string(&second.result)
            .unwrap()
            .contains("src/auth.rs"),
        "{:?}",
        second.result
    );

    let status = runtime.dispatch(ToolRequest {
        id: serde_json::json!("status"),
        tool: "daemon_status".to_string(),
        arguments: serde_json::json!({}),
    });
    assert_eq!(
        status.result.unwrap()["cached_indexes"],
        serde_json::json!(1)
    );
    let status = runtime.dispatch(ToolRequest {
        id: serde_json::json!("missing-status"),
        tool: "daemon_status".to_string(),
        arguments: serde_json::json!({ "details": true }),
    });
    assert_eq!(
        status.result.unwrap()["cached_index_details"][0]["disk_missing"],
        serde_json::json!(true)
    );
}

#[test]
fn runtime_evicts_least_recently_used_cached_indexes() {
    let root = tempfile::tempdir().unwrap();
    let auth_repo = root.path().join("auth");
    write(
        &auth_repo.join("src/lib.rs"),
        "pub fn issue_token() -> usize { 1 }\n",
    );
    let auth_index = auth_repo.join(".orient/index");
    FastIndex::build(&auth_repo)
        .unwrap()
        .save(&auth_index)
        .unwrap();

    let billing_repo = root.path().join("billing");
    write(
        &billing_repo.join("src/lib.rs"),
        "pub fn invoice_total() -> usize { 42 }\n",
    );
    let billing_index = billing_repo.join(".orient/index");
    FastIndex::build(&billing_repo)
        .unwrap()
        .save(&billing_index)
        .unwrap();

    let runtime = ToolRuntime::with_max_cached_indexes(1);
    let first = runtime.dispatch(ToolRequest {
        id: serde_json::json!("first"),
        tool: "indexed_search_code".to_string(),
        arguments: serde_json::json!({
            "index": auth_index,
            "query": "issue token",
            "limit": 3,
            "require_all": true
        }),
    });
    assert!(first.error.is_none(), "{:?}", first.error);
    assert_eq!(runtime.cached_index_count(), 1);

    let second = runtime.dispatch(ToolRequest {
        id: serde_json::json!("second"),
        tool: "indexed_search_code".to_string(),
        arguments: serde_json::json!({
            "index": billing_index,
            "query": "invoice total",
            "limit": 3,
            "require_all": true
        }),
    });
    assert!(second.error.is_none(), "{:?}", second.error);
    assert_eq!(runtime.cached_index_count(), 1);
    let status = runtime.daemon_status();
    assert_eq!(status["max_cached_indexes"], serde_json::json!(1));
    assert_eq!(
        status["cached_index_details"][0]["root"],
        serde_json::json!(billing_repo.canonicalize().unwrap().to_string_lossy())
    );

    let third = runtime.dispatch(ToolRequest {
        id: serde_json::json!("third"),
        tool: "indexed_search_code".to_string(),
        arguments: serde_json::json!({
            "index": auth_index,
            "query": "issue token",
            "limit": 3,
            "require_all": true
        }),
    });
    assert!(third.error.is_none(), "{:?}", third.error);
    assert_eq!(runtime.cached_index_count(), 1);
    let status = runtime.daemon_status();
    assert_eq!(
        status["cached_index_details"][0]["root"],
        serde_json::json!(auth_repo.canonicalize().unwrap().to_string_lossy())
    );
}

#[test]
fn runtime_reloads_cached_index_when_file_changes() {
    let repo = tempfile::tempdir().unwrap();
    write(
        &repo.path().join("src/auth.rs"),
        "pub struct SessionManager;\npub fn issue_token() {}\n",
    );
    write(
        &repo.path().join("Cargo.toml"),
        "[package]\nname='sample'\nversion='0.1.0'\nedition='2024'\n",
    );
    let index_path = repo.path().join(".orient/index");
    FastIndex::build(repo.path())
        .unwrap()
        .save(&index_path)
        .unwrap();

    let runtime = ToolRuntime::default();
    let first = runtime.dispatch(ToolRequest {
        id: serde_json::json!("first"),
        tool: "indexed_search_code".to_string(),
        arguments: serde_json::json!({
            "index": index_path,
            "query": "issue token",
            "limit": 3,
            "require_all": true
        }),
    });
    assert!(first.error.is_none(), "{:?}", first.error);
    assert!(
        serde_json::to_string(&first.result)
            .unwrap()
            .contains("src/auth.rs"),
        "{:?}",
        first.result
    );
    assert_eq!(runtime.cached_index_count(), 1);

    write(
        &repo.path().join("src/billing.rs"),
        "pub fn invoice_total() -> usize { 42 }\n",
    );
    FastIndex::build(repo.path())
        .unwrap()
        .save(&index_path)
        .unwrap();

    let stale_status = runtime.dispatch(ToolRequest {
        id: serde_json::json!("stale-status"),
        tool: "daemon_status".to_string(),
        arguments: serde_json::json!({ "details": true }),
    });
    assert!(stale_status.error.is_none(), "{:?}", stale_status.error);
    let stale_status = stale_status.result.unwrap();
    assert_eq!(
        stale_status["cached_index_details"][0]["disk_missing"],
        serde_json::json!(false)
    );
    assert_eq!(
        stale_status["cached_index_details"][0]["disk_changed"],
        serde_json::json!(true)
    );

    let second = runtime.dispatch(ToolRequest {
        id: serde_json::json!("second"),
        tool: "indexed_search_code".to_string(),
        arguments: serde_json::json!({
            "index": index_path,
            "query": "invoice total",
            "limit": 3,
            "require_all": true
        }),
    });
    assert!(second.error.is_none(), "{:?}", second.error);
    let result = serde_json::to_string(&second.result).unwrap();
    assert!(result.contains("src/billing.rs"), "{result}");
    assert!(result.contains("invoice_total"), "{result}");
    assert_eq!(runtime.cached_index_count(), 1);

    let range = runtime.dispatch(ToolRequest {
        id: serde_json::json!("range"),
        tool: "read_index_range".to_string(),
        arguments: serde_json::json!({
            "index": index_path,
            "path": "src/billing.rs",
            "start": 1,
            "lines": 1
        }),
    });
    assert!(range.error.is_none(), "{:?}", range.error);
    assert!(
        serde_json::to_string(&range.result)
            .unwrap()
            .contains("invoice_total"),
        "{:?}",
        range.result
    );
}

#[test]
fn runtime_refresh_index_updates_cached_single_repo_index() {
    let repo = tempfile::tempdir().unwrap();
    write(
        &repo.path().join("src/auth.rs"),
        "pub struct SessionManager;\npub fn issue_token() {}\n",
    );
    write(
        &repo.path().join("Cargo.toml"),
        "[package]\nname='sample'\nversion='0.1.0'\nedition='2024'\n",
    );
    let index_path = repo.path().join(".orient/index");
    FastIndex::build(repo.path())
        .unwrap()
        .save(&index_path)
        .unwrap();

    let runtime = ToolRuntime::default();
    let first = runtime.dispatch(ToolRequest {
        id: serde_json::json!("first"),
        tool: "indexed_search_code".to_string(),
        arguments: serde_json::json!({
            "index": index_path,
            "query": "rotate secret",
            "limit": 3,
            "require_all": true
        }),
    });
    assert!(first.error.is_none(), "{:?}", first.error);
    assert_eq!(first.result.unwrap(), serde_json::json!([]));

    write(
        &repo.path().join("src/auth.rs"),
        "pub struct SessionManager;\npub fn issue_token() {}\npub fn rotate_secret() {}\n",
    );
    let refresh = runtime.dispatch(ToolRequest {
        id: serde_json::json!("refresh"),
        tool: "refresh_index".to_string(),
        arguments: serde_json::json!({
            "repo": repo.path(),
            "index": index_path
        }),
    });
    assert!(refresh.error.is_none(), "{:?}", refresh.error);
    assert_eq!(refresh.result.as_ref().unwrap()["refreshed_files"], 1);
    assert_eq!(refresh.result.as_ref().unwrap()["files"], 2);

    let second = runtime.dispatch(ToolRequest {
        id: serde_json::json!("second"),
        tool: "indexed_search_code".to_string(),
        arguments: serde_json::json!({
            "index": index_path,
            "query": "rotate secret",
            "limit": 3,
            "require_all": true
        }),
    });
    assert!(second.error.is_none(), "{:?}", second.error);
    let result = serde_json::to_string(&second.result).unwrap();
    assert!(result.contains("rotate_secret"), "{result}");

    let range = runtime.dispatch(ToolRequest {
        id: serde_json::json!("range"),
        tool: "read_index_range".to_string(),
        arguments: serde_json::json!({
            "index": index_path,
            "path": "src/auth.rs",
            "start": 3,
            "lines": 1
        }),
    });
    assert!(range.error.is_none(), "{:?}", range.error);
    assert!(
        serde_json::to_string(&range.result)
            .unwrap()
            .contains("rotate_secret"),
        "{:?}",
        range.result
    );
}

#[test]
fn runtime_ensure_index_builds_missing_index_and_warms_cache() {
    let repo = tempfile::tempdir().unwrap();
    write(
        &repo.path().join("src/auth.rs"),
        "pub struct SessionManager;\npub fn issue_token() {}\n",
    );
    write(
        &repo.path().join("Cargo.toml"),
        "[package]\nname='sample'\nversion='0.1.0'\nedition='2024'\n",
    );
    let index_path = repo.path().join(".orient/index");
    let runtime = ToolRuntime::default();

    let ensure = runtime.dispatch(ToolRequest {
        id: serde_json::json!("ensure"),
        tool: "ensure_index".to_string(),
        arguments: serde_json::json!({
            "repo": repo.path(),
            "index": index_path
        }),
    });
    assert!(ensure.error.is_none(), "{:?}", ensure.error);
    assert!(index_path.exists());
    assert_eq!(ensure.result.as_ref().unwrap()["refreshed_files"], 2);
    assert_eq!(ensure.result.as_ref().unwrap()["files"], 2);

    let status = runtime.dispatch(ToolRequest {
        id: serde_json::json!("status"),
        tool: "daemon_status".to_string(),
        arguments: serde_json::json!({ "details": true }),
    });
    assert!(status.error.is_none(), "{:?}", status.error);
    let status = status.result.unwrap();
    assert_eq!(status["cached_indexes"], serde_json::json!(1));
    assert_eq!(
        status["cached_index_details"][0]["files"],
        serde_json::json!(2)
    );

    let search = runtime.dispatch(ToolRequest {
        id: serde_json::json!("search"),
        tool: "indexed_search_code".to_string(),
        arguments: serde_json::json!({
            "index": index_path,
            "query": "issue token",
            "limit": 3,
            "require_all": true
        }),
    });
    assert!(search.error.is_none(), "{:?}", search.error);
    let result = serde_json::to_string(&search.result).unwrap();
    assert!(result.contains("issue_token"), "{result}");
}

#[test]
fn runtime_ensure_index_rebuilds_corrupt_index() {
    let repo = tempfile::tempdir().unwrap();
    write(
        &repo.path().join("src/auth.rs"),
        "pub fn issue_token() {}\n",
    );
    let index_path = repo.path().join(".orient/index");
    write(&index_path, "not a bincode orient index");
    let runtime = ToolRuntime::default();

    let ensure = runtime.dispatch(ToolRequest {
        id: serde_json::json!("ensure"),
        tool: "ensure_index".to_string(),
        arguments: serde_json::json!({
            "repo": repo.path(),
            "index": index_path
        }),
    });
    assert!(ensure.error.is_none(), "{:?}", ensure.error);
    assert_eq!(ensure.result.as_ref().unwrap()["files"], 1);

    let search = runtime.dispatch(ToolRequest {
        id: serde_json::json!("search"),
        tool: "indexed_search_code".to_string(),
        arguments: serde_json::json!({
            "index": repo.path().join(".orient/index"),
            "query": "issue token",
            "limit": 3
        }),
    });
    assert!(search.error.is_none(), "{:?}", search.error);
}

#[test]
fn runtime_reports_index_status_for_cached_indexes() {
    let repo = tempfile::tempdir().unwrap();
    write(
        &repo.path().join("src/auth.rs"),
        "pub struct SessionManager;\npub fn issue_token() {}\n",
    );
    let index_path = repo.path().join(".orient/index");
    FastIndex::build(repo.path())
        .unwrap()
        .save(&index_path)
        .unwrap();
    let runtime = ToolRuntime::default();

    let clean = runtime.dispatch(ToolRequest {
        id: serde_json::json!("status"),
        tool: "index_status".to_string(),
        arguments: serde_json::json!({ "index": index_path }),
    });
    assert!(clean.error.is_none(), "{:?}", clean.error);
    let clean_result = clean.result.as_ref().unwrap();
    assert_eq!(clean_result["stale"], serde_json::json!(false));
    assert!(clean_result["index_bytes"].as_u64().unwrap() > 0);
    assert!(clean_result["source_bytes"].as_u64().unwrap() > 0);
    assert!(clean_result["content_snapshot_bytes"].as_u64().unwrap() > 0);
    assert!(clean_result["line_offset_bytes"].as_u64().unwrap() > 0);
    assert!(clean_result["posting_entries"].as_u64().unwrap() > 0);
    assert!(clean_result["compressed_posting_bytes"].as_u64().unwrap() > 0);

    write(
        &repo.path().join("src/auth.rs"),
        "pub struct SessionManager;\npub fn issue_token() {}\npub fn rotate_secret_now() {}\n",
    );
    write(
        &repo.path().join("src/new_session.rs"),
        "pub fn new_session() {}\n",
    );

    let stale = runtime.dispatch(ToolRequest {
        id: serde_json::json!("status"),
        tool: "index_status".to_string(),
        arguments: serde_json::json!({ "index": index_path }),
    });
    assert!(stale.error.is_none(), "{:?}", stale.error);
    let result = stale.result.unwrap();
    assert_eq!(result["stale"], serde_json::json!(true));
    assert_eq!(result["changed_paths"], serde_json::json!(["src/auth.rs"]));
    assert_eq!(
        result["added_paths"],
        serde_json::json!(["src/new_session.rs"])
    );
}

#[test]
fn runtime_can_refresh_stale_index_before_search() {
    let repo = tempfile::tempdir().unwrap();
    write(
        &repo.path().join("src/auth.rs"),
        "pub struct SessionManager;\npub fn issue_token() {}\n",
    );
    let index_path = repo.path().join(".orient/index");
    FastIndex::build(repo.path())
        .unwrap()
        .save(&index_path)
        .unwrap();
    write(
        &repo.path().join("src/new_session.rs"),
        "pub fn new_session_token() {}\n",
    );
    let runtime = ToolRuntime::default();

    let stale_search = runtime.dispatch(ToolRequest {
        id: serde_json::json!("stale"),
        tool: "indexed_search_code".to_string(),
        arguments: serde_json::json!({
            "index": index_path,
            "query": "new session token",
            "limit": 3,
            "require_all": true
        }),
    });
    assert!(stale_search.error.is_none(), "{:?}", stale_search.error);
    assert_eq!(stale_search.result.unwrap(), serde_json::json!([]));

    let refreshed_search = runtime.dispatch(ToolRequest {
        id: serde_json::json!("fresh"),
        tool: "indexed_search_code".to_string(),
        arguments: serde_json::json!({
            "index": index_path,
            "query": "new session token",
            "limit": 3,
            "require_all": true,
            "refresh_if_stale": true
        }),
    });
    assert!(
        refreshed_search.error.is_none(),
        "{:?}",
        refreshed_search.error
    );
    let result = serde_json::to_string(&refreshed_search.result).unwrap();
    assert!(result.contains("src/new_session.rs"), "{result}");
}

#[test]
fn runtime_search_auto_reports_stale_index_refresh_request_on_empty_results() {
    let repo = tempfile::tempdir().unwrap();
    write(
        &repo.path().join("src/auth.rs"),
        "pub struct SessionManager;\npub fn issue_token() {}\n",
    );
    let index_path = repo.path().join(".orient/index");
    FastIndex::build(repo.path())
        .unwrap()
        .save(&index_path)
        .unwrap();
    write(
        &repo.path().join("src/new_session.rs"),
        "pub fn new_session_token() {}\n",
    );

    let runtime = ToolRuntime::default();
    runtime.warm_index(index_path.clone()).unwrap();
    let stale = runtime.dispatch(ToolRequest {
        id: serde_json::json!("stale-auto-index"),
        tool: "search_auto".to_string(),
        arguments: serde_json::json!({
            "query": "new_session_token",
            "limit": 3,
            "require_all": true
        }),
    });
    assert!(stale.error.is_none(), "{:?}", stale.error);
    let stale = stale.result.unwrap();
    assert_eq!(stale["surface"], serde_json::json!("indexed"));
    assert_eq!(stale["results"], serde_json::json!([]));
    assert_eq!(stale["freshness"]["stale"], serde_json::json!(true));
    assert_eq!(stale["freshness"]["added_files"], serde_json::json!(1));
    assert_eq!(
        stale["freshness"]["refresh_request"]["tool"],
        serde_json::json!("search_auto")
    );
    assert_eq!(
        stale["refresh_request"],
        stale["freshness"]["refresh_request"]
    );
    assert_eq!(
        stale["freshness"]["refresh_request"]["arguments"]["refresh_if_stale"],
        serde_json::json!(true)
    );
    assert_eq!(
        stale["freshness"]["refresh_request"]["arguments"]["index"],
        serde_json::json!(index_path.canonicalize().unwrap())
    );

    let refresh = runtime.dispatch(ToolRequest {
        id: serde_json::json!("fresh-auto-index"),
        tool: stale["freshness"]["refresh_request"]["tool"]
            .as_str()
            .unwrap()
            .to_string(),
        arguments: stale["freshness"]["refresh_request"]["arguments"].clone(),
    });
    assert!(refresh.error.is_none(), "{:?}", refresh.error);
    let refreshed = serde_json::to_string(&refresh.result).unwrap();
    assert!(refreshed.contains("src/new_session.rs"), "{refreshed}");
}

#[test]
fn runtime_coalesces_parallel_cold_index_requests() {
    let repo = tempfile::tempdir().unwrap();
    write(
        &repo.path().join("src/auth.rs"),
        "pub struct SessionManager;\npub fn issue_token() {}\npub fn rotate_secret() {}\n",
    );
    write(
        &repo.path().join("Cargo.toml"),
        "[package]\nname='sample'\nversion='0.1.0'\nedition='2024'\n",
    );
    let index_path = repo.path().join(".orient/index");
    FastIndex::build(repo.path())
        .unwrap()
        .save(&index_path)
        .unwrap();

    let runtime = Arc::new(ToolRuntime::default());
    let mut handles = Vec::new();
    for index in 0..12 {
        let runtime = Arc::clone(&runtime);
        let index_path = index_path.clone();
        handles.push(thread::spawn(move || {
            let query = if index % 2 == 0 {
                "issue token"
            } else {
                "rotate secret"
            };
            runtime.dispatch(ToolRequest {
                id: serde_json::json!(index),
                tool: "indexed_search_code".to_string(),
                arguments: serde_json::json!({
                    "index": index_path,
                    "query": query,
                    "limit": 3,
                    "require_all": true
                }),
            })
        }));
    }

    for handle in handles {
        let response = handle.join().unwrap();
        assert!(response.error.is_none(), "{:?}", response.error);
        assert!(
            serde_json::to_string(&response.result)
                .unwrap()
                .contains("src/auth.rs"),
            "{:?}",
            response.result
        );
    }
    assert_eq!(runtime.cached_index_count(), 1);
}

#[test]
fn runtime_reuses_cached_shard_index_after_initial_load() {
    let repo = tempfile::tempdir().unwrap();
    write(
        &repo.path().join("src/billing.rs"),
        "pub fn invoice_total() -> usize { 42 }\n",
    );
    write(
        &repo.path().join("tests/billing_test.rs"),
        "use billing::invoice_total;\n#[test]\nfn totals_invoice() {}\n",
    );
    write(
        &repo.path().join("Cargo.toml"),
        "[package]\nname='billing'\nversion='0.1.0'\nedition='2024'\n",
    );
    let shard_dir = tempfile::tempdir().unwrap();
    let runtime = ToolRuntime::default();

    let build = runtime.dispatch(ToolRequest {
        id: serde_json::json!("build"),
        tool: "index_shards".to_string(),
        arguments: serde_json::json!({
            "repos": [repo.path()],
            "output_dir": shard_dir.path()
        }),
    });
    assert!(build.error.is_none(), "{:?}", build.error);

    let search_args = serde_json::json!({
        "index_dir": shard_dir.path(),
        "query": "invoice total",
        "limit": 3,
        "require_all": true
    });
    let first = runtime.dispatch(ToolRequest {
        id: serde_json::json!("first"),
        tool: "search_shards".to_string(),
        arguments: search_args.clone(),
    });
    assert!(first.error.is_none(), "{:?}", first.error);
    assert!(
        serde_json::to_string(&first.result)
            .unwrap()
            .contains("src/billing.rs"),
        "{:?}",
        first.result
    );

    let manifest: serde_json::Value =
        serde_json::from_slice(&fs::read(shard_dir.path().join("manifest.json")).unwrap()).unwrap();
    let shard_index = manifest["shards"][0]["index"].as_str().unwrap();
    let shard_name = manifest["shards"][0]["name"].as_str().unwrap();
    fs::remove_file(shard_dir.path().join(shard_index)).unwrap();
    let second = runtime.dispatch(ToolRequest {
        id: serde_json::json!("second"),
        tool: "search_shards".to_string(),
        arguments: search_args,
    });
    assert!(second.error.is_none(), "{:?}", second.error);
    assert!(
        serde_json::to_string(&second.result)
            .unwrap()
            .contains("src/billing.rs"),
        "{:?}",
        second.result
    );

    let range = runtime.dispatch(ToolRequest {
        id: serde_json::json!("range"),
        tool: "read_shard_range".to_string(),
        arguments: serde_json::json!({
            "index_dir": shard_dir.path(),
            "path": format!("{shard_name}/src/billing.rs"),
            "start": 1,
            "lines": 1
        }),
    });
    assert!(range.error.is_none(), "{:?}", range.error);
    assert!(
        serde_json::to_string(&range.result)
            .unwrap()
            .contains("invoice_total"),
        "{:?}",
        range.result
    );

    let related = runtime.dispatch(ToolRequest {
        id: serde_json::json!("related"),
        tool: "related_shard_files".to_string(),
        arguments: serde_json::json!({
            "index_dir": shard_dir.path(),
            "path": format!("{shard_name}/src/billing.rs"),
            "limit": 5
        }),
    });
    assert!(related.error.is_none(), "{:?}", related.error);
    let related_result = serde_json::to_string(&related.result).unwrap();
    assert!(
        related_result.contains(&format!("{shard_name}/tests/billing_test.rs")),
        "{related_result}"
    );
    assert!(
        related_result.contains("\"read_request\""),
        "{related_result}"
    );
    assert!(
        related_result.contains("\"tool\":\"read_shard_range\""),
        "{related_result}"
    );

    let related_symbols = runtime.dispatch(ToolRequest {
        id: serde_json::json!("related-symbols"),
        tool: "related_shard_symbols".to_string(),
        arguments: serde_json::json!({
            "index_dir": shard_dir.path(),
            "path": format!("{shard_name}/src/billing.rs"),
            "query": "invoice total",
            "limit": 5
        }),
    });
    assert!(
        related_symbols.error.is_none(),
        "{:?}",
        related_symbols.error
    );
    assert!(
        serde_json::to_string(&related_symbols.result)
            .unwrap()
            .contains("invoice_total"),
        "{:?}",
        related_symbols.result
    );
    let related_symbols_result = serde_json::to_string(&related_symbols.result).unwrap();
    assert!(
        related_symbols_result.contains("\"read_request\""),
        "{related_symbols_result}"
    );
    assert!(
        related_symbols_result.contains("\"tool\":\"read_shard_range\""),
        "{related_symbols_result}"
    );

    let plan = runtime.dispatch(ToolRequest {
        id: serde_json::json!("plan"),
        tool: "shard_query_plan".to_string(),
        arguments: serde_json::json!({
            "index_dir": shard_dir.path(),
            "query": "invoice missingterm",
            "require_all": true
        }),
    });
    assert!(plan.error.is_none(), "{:?}", plan.error);
    let plan_result = serde_json::to_string(&plan.result).unwrap();
    assert!(plan_result.contains("\"missing_terms\""), "{plan_result}");
    assert!(plan_result.contains("missingterm"), "{plan_result}");
    assert!(plan_result.contains("drop_missing_terms"), "{plan_result}");

    let selection_miss = runtime.dispatch(ToolRequest {
        id: serde_json::json!("selection-miss"),
        tool: "shard_query_plan".to_string(),
        arguments: serde_json::json!({
            "index_dir": shard_dir.path(),
            "query": "branch:missing-branch invoice total",
            "require_all": true
        }),
    });
    assert!(selection_miss.error.is_none(), "{:?}", selection_miss.error);
    let selection = selection_miss.result.as_ref().unwrap();
    assert_eq!(selection[0]["name"], "__shard_selection__");
    assert_eq!(selection[0]["plan"]["strategy"], "shard_filter_mismatch");
    assert_eq!(selection[0]["plan"]["candidate_count"], 1);
    assert_eq!(selection[0]["plan"]["active_filters"][0]["field"], "branch");
    assert_eq!(
        selection[0]["plan"]["repair_hints"][0]["kind"],
        "relax_filters"
    );
    assert_eq!(
        selection[0]["plan"]["retry_requests"][0]["tool"],
        "search_shards"
    );
    assert_eq!(
        selection[0]["plan"]["retry_requests"][0]["arguments"]["query"],
        "invoice total"
    );
}

#[test]
fn runtime_reuses_cached_shard_manifest_after_initial_load() {
    let repo = tempfile::tempdir().unwrap();
    write(
        &repo.path().join("src/billing.rs"),
        "pub fn invoice_total() -> usize { 42 }\n",
    );
    write(
        &repo.path().join("Cargo.toml"),
        "[package]\nname='billing'\nversion='0.1.0'\nedition='2024'\n",
    );
    let shard_dir = tempfile::tempdir().unwrap();
    let runtime = ToolRuntime::default();

    let build = runtime.dispatch(ToolRequest {
        id: serde_json::json!("build"),
        tool: "index_shards".to_string(),
        arguments: serde_json::json!({
            "repos": [repo.path()],
            "output_dir": shard_dir.path()
        }),
    });
    assert!(build.error.is_none(), "{:?}", build.error);
    let shard_dir_canonical = shard_dir.path().canonicalize().unwrap();
    let manifest: serde_json::Value =
        serde_json::from_slice(&fs::read(shard_dir.path().join("manifest.json")).unwrap()).unwrap();
    let shard_name = manifest["shards"][0]["name"].as_str().unwrap();

    let first = runtime.dispatch(ToolRequest {
        id: serde_json::json!("first"),
        tool: "search_shards".to_string(),
        arguments: serde_json::json!({
            "index_dir": shard_dir.path(),
            "query": "invoice total",
            "limit": 3,
            "require_all": true
        }),
    });
    assert!(first.error.is_none(), "{:?}", first.error);
    assert!(
        serde_json::to_string(&first.result)
            .unwrap()
            .contains("src/billing.rs"),
        "{:?}",
        first.result
    );

    fs::remove_file(shard_dir.path().join("manifest.json")).unwrap();
    let range = runtime.dispatch(ToolRequest {
        id: serde_json::json!("range"),
        tool: "read_shard_range".to_string(),
        arguments: serde_json::json!({
            "index_dir": shard_dir.path(),
            "path": format!("{shard_name}/src/billing.rs"),
            "start": 1,
            "lines": 1
        }),
    });
    assert!(range.error.is_none(), "{:?}", range.error);
    assert!(
        serde_json::to_string(&range.result)
            .unwrap()
            .contains("invoice_total"),
        "{:?}",
        range.result
    );

    let status = runtime.dispatch(ToolRequest {
        id: serde_json::json!("status"),
        tool: "daemon_status".to_string(),
        arguments: serde_json::json!({ "details": true }),
    });
    let result = status.result.unwrap();
    assert_eq!(result["cached_indexes"], serde_json::json!(1));
    assert_eq!(result["cached_shard_manifests"], serde_json::json!(1));
    assert!(
        result["cached_shard_manifest_paths"]
            .as_array()
            .unwrap()
            .iter()
            .any(|path| path.as_str().unwrap() == shard_dir_canonical.to_str().unwrap())
    );
    assert_eq!(
        result["cached_shard_manifest_details"][0]["index_dir"],
        serde_json::json!(shard_dir_canonical.to_str().unwrap())
    );
    assert_eq!(
        result["cached_shard_manifest_details"][0]["shards"],
        serde_json::json!(1)
    );
    assert_eq!(
        result["cached_shard_manifest_details"][0]["manifest_disk_missing"],
        serde_json::json!(true)
    );
    assert_eq!(
        result["cached_shard_manifest_details"][0]["manifest_disk_changed"],
        serde_json::json!(false)
    );
    assert!(
        result["cached_shard_manifest_details"][0]["index_bytes"]
            .as_u64()
            .unwrap()
            > 0
    );
    assert!(
        result["cached_shard_manifest_details"][0]["content_snapshot_bytes"]
            .as_u64()
            .unwrap()
            > 0
    );
    assert!(
        result["cached_shard_manifest_details"][0]["line_offset_bytes"]
            .as_u64()
            .unwrap()
            > 0
    );
    assert!(
        result["cached_shard_manifest_details"][0]["repos"][0]["index_bytes"]
            .as_u64()
            .unwrap()
            > 0
    );
    assert_eq!(
        result["cached_shard_manifest_details"][0]["repos"][0]["index_disk_missing"],
        serde_json::json!(false)
    );
    assert_eq!(
        result["cached_shard_manifest_details"][0]["repos"][0]["index_disk_changed"],
        serde_json::json!(false)
    );
    assert!(
        result["cached_shard_manifest_details"][0]["repos"][0]["content_snapshot_bytes"]
            .as_u64()
            .unwrap()
            > 0
    );
    assert!(
        result["cached_shard_manifest_details"][0]["repos"][0]["line_offset_bytes"]
            .as_u64()
            .unwrap()
            > 0
    );
    assert_eq!(
        result["cached_shard_manifest_details"][0]["repos"][0]["name"],
        serde_json::json!(shard_name)
    );
    assert_eq!(
        result["cached_shard_manifest_details"][0]["repos"][0]["aliases"][0],
        serde_json::json!(shard_name)
    );
    assert_eq!(result["footprint"]["loaded_indexes"], serde_json::json!(1));
    assert_eq!(
        result["footprint"]["cached_shard_manifests"],
        serde_json::json!(1)
    );
    assert_eq!(
        result["footprint"]["known_shard_repos"],
        serde_json::json!(1)
    );
    assert_eq!(
        result["footprint"]["manifest_disk_missing"],
        serde_json::json!(1)
    );
    assert!(
        result["footprint"]["known_shard_index_bytes"]
            .as_u64()
            .unwrap()
            > 0
    );
    assert!(
        result["footprint"]["known_shard_content_snapshot_bytes"]
            .as_u64()
            .unwrap()
            > 0
    );
}

#[test]
fn runtime_reloads_cached_shard_manifest_when_file_changes() {
    let workspace = tempfile::tempdir().unwrap();
    let auth_repo = workspace.path().join("auth");
    write(
        &auth_repo.join("src/lib.rs"),
        "pub fn issue_token() -> usize { 1 }\n",
    );
    write(
        &auth_repo.join("Cargo.toml"),
        "[package]\nname='auth'\nversion='0.1.0'\nedition='2024'\n",
    );
    let billing_repo = workspace.path().join("billing");
    write(
        &billing_repo.join("src/lib.rs"),
        "pub fn invoice_total() -> usize { 42 }\n",
    );
    write(
        &billing_repo.join("Cargo.toml"),
        "[package]\nname='billing'\nversion='0.1.0'\nedition='2024'\n",
    );

    let shard_dir = tempfile::tempdir().unwrap();
    build_shards(&[auth_repo.clone()], shard_dir.path()).unwrap();
    let runtime = ToolRuntime::default();
    let first = runtime.dispatch(ToolRequest {
        id: serde_json::json!("first"),
        tool: "search_shards".to_string(),
        arguments: serde_json::json!({
            "index_dir": shard_dir.path(),
            "query": "issue token",
            "require_all": true
        }),
    });
    assert!(first.error.is_none(), "{:?}", first.error);
    assert_eq!(runtime.cached_shard_manifest_count(), 1);
    assert_eq!(runtime.cached_index_count(), 1);

    write(
        &auth_repo.join("src/lib.rs"),
        "pub fn revoke_token() -> usize { 2 }\n",
    );
    build_shards(&[auth_repo, billing_repo], shard_dir.path()).unwrap();

    let stale_status = runtime.dispatch(ToolRequest {
        id: serde_json::json!("stale-status"),
        tool: "daemon_status".to_string(),
        arguments: serde_json::json!({ "details": true }),
    });
    assert!(stale_status.error.is_none(), "{:?}", stale_status.error);
    let stale_status = stale_status.result.unwrap();
    assert_eq!(
        stale_status["cached_shard_manifest_details"][0]["manifest_disk_missing"],
        serde_json::json!(false)
    );
    assert_eq!(
        stale_status["cached_shard_manifest_details"][0]["manifest_disk_changed"],
        serde_json::json!(true)
    );
    assert_eq!(
        stale_status["cached_shard_manifest_details"][0]["repos"][0]["index_disk_changed"],
        serde_json::json!(true)
    );

    let second = runtime.dispatch(ToolRequest {
        id: serde_json::json!("second"),
        tool: "search_shards".to_string(),
        arguments: serde_json::json!({
            "index_dir": shard_dir.path(),
            "query": "invoice total",
            "require_all": true
        }),
    });
    assert!(second.error.is_none(), "{:?}", second.error);
    let result = serde_json::to_string(&second.result).unwrap();
    assert!(result.contains("billing/src/lib.rs"), "{result}");
    assert_eq!(runtime.cached_shard_manifest_count(), 1);
    assert_eq!(runtime.cached_index_count(), 1);

    let third = runtime.dispatch(ToolRequest {
        id: serde_json::json!("third"),
        tool: "search_shards".to_string(),
        arguments: serde_json::json!({
            "index_dir": shard_dir.path(),
            "query": "revoke token",
            "require_all": true
        }),
    });
    assert!(third.error.is_none(), "{:?}", third.error);
    let result = serde_json::to_string(&third.result).unwrap();
    assert!(result.contains("auth/src/lib.rs"), "{result}");
    assert!(!result.contains("issue_token"), "{result}");
    assert_eq!(runtime.cached_index_count(), 2);
}

#[test]
fn runtime_warms_shards_by_tool_request() {
    let repo = tempfile::tempdir().unwrap();
    write(
        &repo.path().join("src/billing.rs"),
        "pub fn invoice_total() -> usize { 42 }\n",
    );
    write(
        &repo.path().join("Cargo.toml"),
        "[package]\nname='billing'\nversion='0.1.0'\nedition='2024'\n",
    );
    let shard_dir = tempfile::tempdir().unwrap();
    let runtime = ToolRuntime::default();
    let build = runtime.dispatch(ToolRequest {
        id: serde_json::json!("build"),
        tool: "index_shards".to_string(),
        arguments: serde_json::json!({
            "repos": [repo.path()],
            "output_dir": shard_dir.path()
        }),
    });
    assert!(build.error.is_none(), "{:?}", build.error);

    let register = runtime.dispatch(ToolRequest {
        id: serde_json::json!("register-shards"),
        tool: "register_shards".to_string(),
        arguments: serde_json::json!({
            "index_dir": shard_dir.path()
        }),
    });
    assert!(register.error.is_none(), "{:?}", register.error);
    let result = register.result.unwrap();
    assert_eq!(result["cached_indexes"], serde_json::json!(0));
    assert_eq!(result["registered_indexes"], serde_json::json!(1));
    assert_eq!(result["registered_shards"]["shards"], serde_json::json!(1));

    let warm = runtime.dispatch(ToolRequest {
        id: serde_json::json!("warm-shards"),
        tool: "warm_shards".to_string(),
        arguments: serde_json::json!({
            "index_dir": shard_dir.path()
        }),
    });
    assert!(warm.error.is_none(), "{:?}", warm.error);
    let result = warm.result.unwrap();
    assert_eq!(result["cached_indexes"], serde_json::json!(1));
    assert_eq!(result["warmed_indexes"], serde_json::json!(1));
    assert_eq!(result["warmed_shards"]["shards"], serde_json::json!(1));
    assert_eq!(
        result["warmed_shards"]["repos"][0]["aliases"][0],
        result["warmed_shards"]["repos"][0]["name"]
    );
}

#[test]
fn runtime_search_auto_scopes_warmed_shards_to_client_cwd() {
    let workspace = tempfile::tempdir().unwrap();
    let current_repo = workspace.path().join("current-app");
    let other_repo = workspace.path().join("other-app");
    fs::create_dir_all(current_repo.join(".git")).unwrap();
    fs::create_dir_all(other_repo.join(".git")).unwrap();
    write(
        &current_repo.join("src/lib.rs"),
        "pub fn shared_lookup_token() -> &'static str { \"current\" }\n",
    );
    write(
        &other_repo.join("src/lib.rs"),
        "pub fn shared_lookup_token() -> &'static str { \"other\" }\n",
    );
    let shard_dir = workspace.path().join("shards");
    build_shards(
        &[PathBuf::from(&current_repo), PathBuf::from(&other_repo)],
        &shard_dir,
    )
    .unwrap();

    let runtime = ToolRuntime::default();
    runtime.warm_shards(shard_dir.clone()).unwrap();

    let search = runtime.dispatch(ToolRequest {
        id: serde_json::json!("search-auto-cwd"),
        tool: "search_auto".to_string(),
        arguments: serde_json::json!({
            "cwd": current_repo.join("src"),
            "query": "shared_lookup_token",
            "limit": 5
        }),
    });
    assert!(search.error.is_none(), "{:?}", search.error);
    let value = search.result.unwrap();
    assert_eq!(value["surface"], serde_json::json!("shards"));
    assert_eq!(
        value["query_plan_request"]["arguments"]["repo_filter"],
        serde_json::json!(current_repo.canonicalize().unwrap().to_string_lossy())
    );
    let serialized = serde_json::to_string(&value).unwrap();
    assert!(serialized.contains("current-app/src/lib.rs"), "{value}");
    assert!(!serialized.contains("other-app/src/lib.rs"), "{value}");

    let retry = runtime.dispatch(ToolRequest {
        id: serde_json::json!("search-auto-cwd-retry"),
        tool: "search_auto".to_string(),
        arguments: serde_json::json!({
            "cwd": current_repo.join("src"),
            "query": "shared_lookup_token definitely_missing",
            "limit": 5,
            "retry_if_empty": true
        }),
    });
    assert!(retry.error.is_none(), "{:?}", retry.error);
    let retry = retry.result.unwrap();
    assert_eq!(retry["surface"], serde_json::json!("shards"));
    assert_eq!(retry["results"], serde_json::json!([]));
    assert_eq!(
        retry["primary_retry_result"]["read_batch_request"]["tool"],
        serde_json::json!("read_ranges")
    );
    assert_eq!(
        retry["primary_retry_result"]["read_batch_request"]["arguments"]["index_dir"],
        serde_json::json!(shard_dir.canonicalize().unwrap())
    );
    assert_eq!(
        retry["primary_retry_result"]["read_batch_request"]["arguments"]["ranges"][0]["path"],
        serde_json::json!("current-app/src/lib.rs")
    );
    assert_eq!(
        retry["query_plan_summary"][0]["name"],
        serde_json::json!("current-app")
    );
    assert_eq!(
        retry["query_plan_summary"][0]["summary"]["status"],
        serde_json::json!("missing_terms")
    );
    assert_eq!(
        retry["query_plan_summary"][0]["summary"]["primary_retry_request"],
        retry["primary_retry_request"]
    );
    assert_eq!(
        retry["query_plan_summary"][0]["next_action"],
        retry["query_plan_result"][0]["next_action"]
    );
    assert_eq!(
        retry["query_plan_summary"][0]["next_action"]["request"],
        retry["primary_retry_request"]
    );
    assert_eq!(
        retry["next_read_batch_request"],
        retry["primary_retry_result"]["read_batch_request"]
    );
    let retry_read = runtime.dispatch(ToolRequest {
        id: serde_json::json!("search-auto-cwd-retry-read"),
        tool: retry["primary_retry_result"]["read_batch_request"]["tool"]
            .as_str()
            .unwrap()
            .to_string(),
        arguments: retry["primary_retry_result"]["read_batch_request"]["arguments"].clone(),
    });
    assert!(retry_read.error.is_none(), "{:?}", retry_read.error);
    assert_eq!(
        retry["primary_retry_result"]["read_batch_request"]["arguments"]["include_summary"],
        serde_json::json!(true)
    );
    assert!(
        retry_read.result.unwrap()["ranges"][0]["text"]
            .as_str()
            .unwrap()
            .contains("shared_lookup_token")
    );

    let explicit_repo_search = runtime.dispatch(ToolRequest {
        id: serde_json::json!("search-auto-cwd-explicit-repo"),
        tool: "search_auto".to_string(),
        arguments: serde_json::json!({
            "cwd": current_repo.join("src"),
            "query": "repo:other-app shared_lookup_token",
            "limit": 5
        }),
    });
    assert!(
        explicit_repo_search.error.is_none(),
        "{:?}",
        explicit_repo_search.error
    );
    let explicit_repo_search = explicit_repo_search.result.unwrap();
    assert!(
        explicit_repo_search["query_plan_request"]["arguments"]
            .get("repo_filter")
            .is_none(),
        "{explicit_repo_search}"
    );
    let explicit_serialized = serde_json::to_string(&explicit_repo_search).unwrap();
    assert!(
        explicit_serialized.contains("other-app/src/lib.rs"),
        "{explicit_repo_search}"
    );
    assert!(
        !explicit_serialized.contains("current-app/src/lib.rs"),
        "{explicit_repo_search}"
    );

    let batch = runtime.dispatch(ToolRequest {
        id: serde_json::json!("search-auto-batch-cwd"),
        tool: "search_auto_batch".to_string(),
        arguments: serde_json::json!({
            "cwd": current_repo.join("src"),
            "queries": [
                "shared_lookup_token",
                "repo:other-app shared_lookup_token"
            ],
            "limit": 5
        }),
    });
    assert!(batch.error.is_none(), "{:?}", batch.error);
    let batch = batch.result.unwrap();
    assert_eq!(
        batch[0]["query_plan_request"]["arguments"]["repo_filter"],
        serde_json::json!(current_repo.canonicalize().unwrap().to_string_lossy())
    );
    let first_batch_item = serde_json::to_string(&batch[0]).unwrap();
    assert!(first_batch_item.contains("current-app/src/lib.rs"));
    assert!(!first_batch_item.contains("other-app/src/lib.rs"));
    assert!(
        batch[1]["query_plan_request"]["arguments"]
            .get("repo_filter")
            .is_none(),
        "{batch}"
    );
    let second_batch_item = serde_json::to_string(&batch[1]).unwrap();
    assert!(second_batch_item.contains("other-app/src/lib.rs"));
    assert!(!second_batch_item.contains("current-app/src/lib.rs"));
}

#[test]
fn runtime_search_auto_refreshes_only_client_cwd_shard_when_scoped() {
    let workspace = tempfile::tempdir().unwrap();
    let current_repo = workspace.path().join("current-app");
    let other_repo = workspace.path().join("other-app");
    fs::create_dir_all(current_repo.join(".git")).unwrap();
    fs::create_dir_all(other_repo.join(".git")).unwrap();
    write(
        &current_repo.join("src/lib.rs"),
        "pub fn baseline_current_token() {}\n",
    );
    write(
        &other_repo.join("src/lib.rs"),
        "pub fn baseline_other_token() {}\n",
    );
    let shard_dir = workspace.path().join("shards");
    build_shards(
        &[PathBuf::from(&current_repo), PathBuf::from(&other_repo)],
        &shard_dir,
    )
    .unwrap();

    write(
        &current_repo.join("src/new_current.rs"),
        "pub fn current_after_refresh_token() {}\n",
    );
    write(
        &other_repo.join("src/new_other.rs"),
        "pub fn other_after_refresh_token() {}\n",
    );

    let runtime = ToolRuntime::default();
    runtime.warm_shards(shard_dir.clone()).unwrap();

    let current_search = runtime.dispatch(ToolRequest {
        id: serde_json::json!("fresh-current"),
        tool: "search_auto".to_string(),
        arguments: serde_json::json!({
            "cwd": current_repo.join("src"),
            "query": "current_after_refresh_token",
            "limit": 5,
            "refresh_if_stale": true
        }),
    });
    assert!(current_search.error.is_none(), "{:?}", current_search.error);
    let current_search = serde_json::to_string(&current_search.result).unwrap();
    assert!(
        current_search.contains("current-app/src/new_current.rs"),
        "{current_search}"
    );

    let other_search = runtime.dispatch(ToolRequest {
        id: serde_json::json!("stale-other"),
        tool: "search_shards".to_string(),
        arguments: serde_json::json!({
            "index_dir": shard_dir,
            "query": "repo:other-app other_after_refresh_token",
            "limit": 5
        }),
    });
    assert!(other_search.error.is_none(), "{:?}", other_search.error);
    let other_search = serde_json::to_string(&other_search.result).unwrap();
    assert!(
        !other_search.contains("other-app/src/new_other.rs"),
        "{other_search}"
    );
}

#[test]
fn runtime_search_auto_refreshes_only_query_selected_shard_when_repo_filter_is_in_query() {
    let workspace = tempfile::tempdir().unwrap();
    let current_repo = workspace.path().join("current-app");
    let other_repo = workspace.path().join("other-app");
    fs::create_dir_all(current_repo.join(".git")).unwrap();
    fs::create_dir_all(other_repo.join(".git")).unwrap();
    write(
        &current_repo.join("src/lib.rs"),
        "pub fn baseline_current_token() {}\n",
    );
    write(
        &other_repo.join("src/lib.rs"),
        "pub fn baseline_other_token() {}\n",
    );
    let shard_dir = workspace.path().join("shards");
    build_shards(
        &[PathBuf::from(&current_repo), PathBuf::from(&other_repo)],
        &shard_dir,
    )
    .unwrap();

    write(
        &current_repo.join("src/new_current.rs"),
        "pub fn current_after_refresh_token() {}\n",
    );
    write(
        &other_repo.join("src/new_other.rs"),
        "pub fn other_after_refresh_token() {}\n",
    );

    let runtime = ToolRuntime::default();
    runtime.warm_shards(shard_dir.clone()).unwrap();

    let other_search = runtime.dispatch(ToolRequest {
        id: serde_json::json!("fresh-other"),
        tool: "search_auto".to_string(),
        arguments: serde_json::json!({
            "cwd": current_repo.join("src"),
            "query": "repo:other-app other_after_refresh_token",
            "limit": 5,
            "refresh_if_stale": true
        }),
    });
    assert!(other_search.error.is_none(), "{:?}", other_search.error);
    let other_search = serde_json::to_string(&other_search.result).unwrap();
    assert!(
        other_search.contains("other-app/src/new_other.rs"),
        "{other_search}"
    );

    let current_search = runtime.dispatch(ToolRequest {
        id: serde_json::json!("stale-current"),
        tool: "search_shards".to_string(),
        arguments: serde_json::json!({
            "index_dir": shard_dir,
            "query": "repo:current-app current_after_refresh_token",
            "limit": 5
        }),
    });
    assert!(current_search.error.is_none(), "{:?}", current_search.error);
    let current_search = serde_json::to_string(&current_search.result).unwrap();
    assert!(
        !current_search.contains("current-app/src/new_current.rs"),
        "{current_search}"
    );
}

#[test]
fn runtime_search_auto_batch_refreshes_selected_shards_without_unselected_shards() {
    let workspace = tempfile::tempdir().unwrap();
    let current_repo = workspace.path().join("current-app");
    let other_repo = workspace.path().join("other-app");
    let third_repo = workspace.path().join("third-app");
    fs::create_dir_all(current_repo.join(".git")).unwrap();
    fs::create_dir_all(other_repo.join(".git")).unwrap();
    fs::create_dir_all(third_repo.join(".git")).unwrap();
    write(
        &current_repo.join("src/lib.rs"),
        "pub fn baseline_current_token() {}\n",
    );
    write(
        &other_repo.join("src/lib.rs"),
        "pub fn baseline_other_token() {}\n",
    );
    write(
        &third_repo.join("src/lib.rs"),
        "pub fn baseline_third_token() {}\n",
    );
    let shard_dir = workspace.path().join("shards");
    build_shards(
        &[
            PathBuf::from(&current_repo),
            PathBuf::from(&other_repo),
            PathBuf::from(&third_repo),
        ],
        &shard_dir,
    )
    .unwrap();

    write(
        &current_repo.join("src/new_current.rs"),
        "pub fn current_after_batch_refresh_token() {}\n",
    );
    write(
        &other_repo.join("src/new_other.rs"),
        "pub fn other_after_batch_refresh_token() {}\n",
    );
    write(
        &third_repo.join("src/new_third.rs"),
        "pub fn third_after_batch_refresh_token() {}\n",
    );

    let runtime = ToolRuntime::default();
    runtime.warm_shards(shard_dir.clone()).unwrap();

    let batch = runtime.dispatch(ToolRequest {
        id: serde_json::json!("fresh-batch"),
        tool: "search_auto_batch".to_string(),
        arguments: serde_json::json!({
            "cwd": current_repo.join("src"),
            "queries": [
                "current_after_batch_refresh_token",
                "repo:other-app other_after_batch_refresh_token"
            ],
            "limit": 5,
            "refresh_if_stale": true
        }),
    });
    assert!(batch.error.is_none(), "{:?}", batch.error);
    let batch = batch.result.unwrap();
    assert_eq!(batch[0]["query_plan_request"]["tool"], "shard_query_plan");
    assert_eq!(batch[0]["repo_map_request"]["tool"], "repo_map");
    assert!(
        batch[0]["query_plan_request"]["arguments"]["index_dir"]
            .as_str()
            .unwrap()
            .ends_with("/shards")
    );
    assert!(
        batch[0]["repo_map_request"]["arguments"]["index_dir"]
            .as_str()
            .unwrap()
            .ends_with("/shards")
    );
    let first_batch_item = serde_json::to_string(&batch[0]).unwrap();
    assert!(
        first_batch_item.contains("current-app/src/new_current.rs"),
        "{first_batch_item}"
    );
    let second_batch_item = serde_json::to_string(&batch[1]).unwrap();
    assert!(
        second_batch_item.contains("other-app/src/new_other.rs"),
        "{second_batch_item}"
    );

    let third_search = runtime.dispatch(ToolRequest {
        id: serde_json::json!("stale-third"),
        tool: "search_shards".to_string(),
        arguments: serde_json::json!({
            "index_dir": shard_dir,
            "query": "repo:third-app third_after_batch_refresh_token",
            "limit": 5
        }),
    });
    assert!(third_search.error.is_none(), "{:?}", third_search.error);
    let third_search = serde_json::to_string(&third_search.result).unwrap();
    assert!(
        !third_search.contains("third-app/src/new_third.rs"),
        "{third_search}"
    );
}

#[test]
fn runtime_search_shards_batch_refreshes_selected_shards_without_unselected_shards() {
    let workspace = tempfile::tempdir().unwrap();
    let current_repo = workspace.path().join("current-app");
    let other_repo = workspace.path().join("other-app");
    let third_repo = workspace.path().join("third-app");
    fs::create_dir_all(current_repo.join(".git")).unwrap();
    fs::create_dir_all(other_repo.join(".git")).unwrap();
    fs::create_dir_all(third_repo.join(".git")).unwrap();
    write(
        &current_repo.join("src/lib.rs"),
        "pub fn baseline_current_token() {}\n",
    );
    write(
        &other_repo.join("src/lib.rs"),
        "pub fn baseline_other_token() {}\n",
    );
    write(
        &third_repo.join("src/lib.rs"),
        "pub fn baseline_third_token() {}\n",
    );
    let shard_dir = workspace.path().join("shards");
    build_shards(
        &[
            PathBuf::from(&current_repo),
            PathBuf::from(&other_repo),
            PathBuf::from(&third_repo),
        ],
        &shard_dir,
    )
    .unwrap();

    write(
        &current_repo.join("src/new_current.rs"),
        "pub fn direct_current_batch_refresh_token() {}\n",
    );
    write(
        &other_repo.join("src/new_other.rs"),
        "pub fn direct_other_batch_refresh_token() {}\n",
    );
    write(
        &third_repo.join("src/new_third.rs"),
        "pub fn direct_third_batch_refresh_token() {}\n",
    );

    let runtime = ToolRuntime::default();
    runtime.warm_shards(shard_dir.clone()).unwrap();

    let batch = runtime.dispatch(ToolRequest {
        id: serde_json::json!("fresh-direct-batch"),
        tool: "search_shards_batch".to_string(),
        arguments: serde_json::json!({
            "index_dir": shard_dir.clone(),
            "cwd": current_repo.join("src"),
            "queries": [
                "direct_current_batch_refresh_token",
                "repo:other-app direct_other_batch_refresh_token"
            ],
            "limit": 5,
            "refresh_if_stale": true
        }),
    });
    assert!(batch.error.is_none(), "{:?}", batch.error);
    let batch = batch.result.unwrap();
    assert_eq!(batch[0]["query_plan_request"]["tool"], "shard_query_plan");
    assert_eq!(batch[0]["repo_map_request"]["tool"], "shard_repo_map");
    assert_eq!(
        batch[0]["query_plan_request"]["arguments"]["index_dir"],
        serde_json::json!(&shard_dir)
    );
    assert_eq!(
        batch[0]["repo_map_request"]["arguments"]["index_dir"],
        serde_json::json!(&shard_dir)
    );
    let first_batch_item = serde_json::to_string(&batch[0]).unwrap();
    assert!(
        first_batch_item.contains("current-app/src/new_current.rs"),
        "{first_batch_item}"
    );
    let second_batch_item = serde_json::to_string(&batch[1]).unwrap();
    assert!(
        second_batch_item.contains("other-app/src/new_other.rs"),
        "{second_batch_item}"
    );

    let third_search = runtime.dispatch(ToolRequest {
        id: serde_json::json!("stale-third"),
        tool: "search_shards".to_string(),
        arguments: serde_json::json!({
            "index_dir": shard_dir,
            "query": "repo:third-app direct_third_batch_refresh_token",
            "limit": 5
        }),
    });
    assert!(third_search.error.is_none(), "{:?}", third_search.error);
    let third_search = serde_json::to_string(&third_search.result).unwrap();
    assert!(
        !third_search.contains("third-app/src/new_third.rs"),
        "{third_search}"
    );
}

#[test]
fn runtime_search_auto_reports_stale_shard_refresh_request_on_empty_results() {
    let workspace = tempfile::tempdir().unwrap();
    let current_repo = workspace.path().join("current-app");
    let other_repo = workspace.path().join("other-app");
    fs::create_dir_all(current_repo.join(".git")).unwrap();
    fs::create_dir_all(other_repo.join(".git")).unwrap();
    write(
        &current_repo.join("src/lib.rs"),
        "pub fn baseline_current_token() {}\n",
    );
    write(
        &other_repo.join("src/lib.rs"),
        "pub fn baseline_other_token() {}\n",
    );
    let shard_dir = workspace.path().join("shards");
    build_shards(
        &[PathBuf::from(&current_repo), PathBuf::from(&other_repo)],
        &shard_dir,
    )
    .unwrap();

    write(
        &current_repo.join("src/new_current.rs"),
        "pub fn current_after_refresh_token() {}\n",
    );
    write(
        &other_repo.join("src/new_other.rs"),
        "pub fn other_after_refresh_token() {}\n",
    );

    let runtime = ToolRuntime::default();
    runtime.warm_shards(shard_dir.clone()).unwrap();

    let stale = runtime.dispatch(ToolRequest {
        id: serde_json::json!("stale-auto-shard"),
        tool: "search_auto".to_string(),
        arguments: serde_json::json!({
            "cwd": current_repo.join("src"),
            "query": "current_after_refresh_token",
            "limit": 5,
            "require_all": true
        }),
    });
    assert!(stale.error.is_none(), "{:?}", stale.error);
    let stale = stale.result.unwrap();
    assert_eq!(stale["surface"], serde_json::json!("shards"));
    assert_eq!(stale["results"], serde_json::json!([]));
    assert_eq!(stale["freshness"]["stale"], serde_json::json!(true));
    assert_eq!(stale["freshness"]["stale_shards"], serde_json::json!(1));
    assert_eq!(stale["freshness"]["added_files"], serde_json::json!(1));
    assert_eq!(
        stale["freshness"]["refresh_request"]["tool"],
        serde_json::json!("search_auto")
    );
    assert_eq!(
        stale["refresh_request"],
        stale["freshness"]["refresh_request"]
    );
    assert_eq!(
        stale["freshness"]["refresh_request"]["arguments"]["refresh_if_stale"],
        serde_json::json!(true)
    );
    assert_eq!(
        stale["freshness"]["refresh_request"]["arguments"]["index_dir"],
        serde_json::json!(shard_dir.canonicalize().unwrap())
    );

    let refreshed = runtime.dispatch(ToolRequest {
        id: serde_json::json!("fresh-auto-shard"),
        tool: stale["freshness"]["refresh_request"]["tool"]
            .as_str()
            .unwrap()
            .to_string(),
        arguments: stale["freshness"]["refresh_request"]["arguments"].clone(),
    });
    assert!(refreshed.error.is_none(), "{:?}", refreshed.error);
    let refreshed = serde_json::to_string(&refreshed.result).unwrap();
    assert!(
        refreshed.contains("current-app/src/new_current.rs"),
        "{refreshed}"
    );
    assert!(
        !refreshed.contains("other-app/src/new_other.rs"),
        "{refreshed}"
    );
}

#[test]
fn runtime_orientation_tools_scope_warmed_shards_to_client_cwd() {
    let workspace = tempfile::tempdir().unwrap();
    let current_repo = workspace.path().join("current-app");
    let other_repo = workspace.path().join("other-app");
    fs::create_dir_all(current_repo.join(".git")).unwrap();
    fs::create_dir_all(other_repo.join(".git")).unwrap();
    write(
        &current_repo.join("src/lib.rs"),
        "pub struct SharedThing;\npub fn shared_lookup_token() -> &'static str { \"current\" }\n",
    );
    write(
        &current_repo.join("tests/lib_test.rs"),
        "#[test]\nfn current_related_test() { assert_eq!(current_app::shared_lookup_token(), \"current\"); }\n",
    );
    write(
        &other_repo.join("src/lib.rs"),
        "pub struct SharedThing;\npub fn shared_lookup_token() -> &'static str { \"other\" }\n",
    );
    write(
        &other_repo.join("tests/lib_test.rs"),
        "#[test]\nfn other_related_test() { assert_eq!(other_app::shared_lookup_token(), \"other\"); }\n",
    );
    let shard_dir = workspace.path().join("shards");
    build_shards(
        &[PathBuf::from(&current_repo), PathBuf::from(&other_repo)],
        &shard_dir,
    )
    .unwrap();

    let runtime = ToolRuntime::default();
    runtime.warm_shards(shard_dir.clone()).unwrap();
    let cwd = current_repo.join("src");
    let current_root = current_repo
        .canonicalize()
        .unwrap()
        .to_string_lossy()
        .to_string();

    let repo_map = runtime.dispatch(ToolRequest {
        id: serde_json::json!("repo-map-cwd"),
        tool: "repo_map".to_string(),
        arguments: serde_json::json!({
            "cwd": cwd,
            "symbols": 5,
            "tests": 5
        }),
    });
    assert!(repo_map.error.is_none(), "{:?}", repo_map.error);
    let repo_map = repo_map.result.unwrap();
    assert_eq!(repo_map.as_array().unwrap().len(), 1, "{repo_map}");
    assert!(
        serde_json::to_string(&repo_map)
            .unwrap()
            .contains("current-app/src/lib.rs")
    );
    assert!(
        !serde_json::to_string(&repo_map)
            .unwrap()
            .contains("other-app/src/lib.rs")
    );

    let search = runtime.dispatch(ToolRequest {
        id: serde_json::json!("search-cwd"),
        tool: "search".to_string(),
        arguments: serde_json::json!({
            "cwd": current_repo.join("src"),
            "query": "shared_lookup_token",
            "limit": 5
        }),
    });
    assert!(search.error.is_none(), "{:?}", search.error);
    let search = search.result.unwrap();
    let search_json = serde_json::to_string(&search).unwrap();
    assert!(
        search_json.contains("current-app/src/lib.rs"),
        "{search_json}"
    );
    assert!(
        !search_json.contains("other-app/src/lib.rs"),
        "{search_json}"
    );
    assert_eq!(
        search[0]["read_request"]["arguments"]["index_dir"],
        serde_json::json!(shard_dir.canonicalize().unwrap())
    );

    let explicit_search = runtime.dispatch(ToolRequest {
        id: serde_json::json!("search-cwd-explicit-repo"),
        tool: "search".to_string(),
        arguments: serde_json::json!({
            "cwd": current_repo.join("src"),
            "query": "repo:other-app shared_lookup_token",
            "limit": 5
        }),
    });
    assert!(
        explicit_search.error.is_none(),
        "{:?}",
        explicit_search.error
    );
    let explicit_search = serde_json::to_string(&explicit_search.result.unwrap()).unwrap();
    assert!(
        explicit_search.contains("other-app/src/lib.rs"),
        "{explicit_search}"
    );
    assert!(
        !explicit_search.contains("current-app/src/lib.rs"),
        "{explicit_search}"
    );

    let search_batch = runtime.dispatch(ToolRequest {
        id: serde_json::json!("search-batch-cwd"),
        tool: "search_batch".to_string(),
        arguments: serde_json::json!({
            "cwd": current_repo.join("src"),
            "queries": [
                "shared_lookup_token",
                "repo:other-app shared_lookup_token"
            ],
            "limit": 5
        }),
    });
    assert!(search_batch.error.is_none(), "{:?}", search_batch.error);
    let search_batch = search_batch.result.unwrap();
    let first_search_batch_item = serde_json::to_string(&search_batch[0]).unwrap();
    assert!(
        first_search_batch_item.contains("current-app/src/lib.rs"),
        "{first_search_batch_item}"
    );
    assert!(
        !first_search_batch_item.contains("other-app/src/lib.rs"),
        "{first_search_batch_item}"
    );
    let second_search_batch_item = serde_json::to_string(&search_batch[1]).unwrap();
    assert!(
        second_search_batch_item.contains("other-app/src/lib.rs"),
        "{second_search_batch_item}"
    );
    assert!(
        !second_search_batch_item.contains("current-app/src/lib.rs"),
        "{second_search_batch_item}"
    );

    let read = runtime.dispatch(ToolRequest {
        id: serde_json::json!("read-cwd"),
        tool: "read_range".to_string(),
        arguments: serde_json::json!({
            "cwd": current_repo.join("src"),
            "path": "src/lib.rs",
            "start": 2,
            "lines": 1
        }),
    });
    assert!(read.error.is_none(), "{:?}", read.error);
    let read = read.result.unwrap();
    assert_eq!(read["path"], serde_json::json!("current-app/src/lib.rs"));
    assert!(read["text"].as_str().unwrap().contains("\"current\""));
    assert!(!read["text"].as_str().unwrap().contains("\"other\""));

    let read_batch = runtime.dispatch(ToolRequest {
        id: serde_json::json!("read-batch-cwd"),
        tool: "read_ranges".to_string(),
        arguments: serde_json::json!({
            "cwd": current_repo.join("src"),
            "ranges": [{"path": "src/lib.rs", "start": 1, "lines": 2}]
        }),
    });
    assert!(read_batch.error.is_none(), "{:?}", read_batch.error);
    let read_batch = read_batch.result.unwrap();
    assert_eq!(
        read_batch[0]["path"],
        serde_json::json!("current-app/src/lib.rs")
    );
    assert!(
        read_batch[0]["text"]
            .as_str()
            .unwrap()
            .contains("\"current\"")
    );
    assert!(
        !read_batch[0]["text"]
            .as_str()
            .unwrap()
            .contains("\"other\"")
    );

    let related = runtime.dispatch(ToolRequest {
        id: serde_json::json!("related-cwd"),
        tool: "related_files".to_string(),
        arguments: serde_json::json!({
            "cwd": current_repo.join("src"),
            "path": "src/lib.rs",
            "limit": 5
        }),
    });
    assert!(related.error.is_none(), "{:?}", related.error);
    let related = serde_json::to_string(&related.result.unwrap()).unwrap();
    assert!(
        related.contains("current-app/tests/lib_test.rs"),
        "{related}"
    );
    assert!(
        !related.contains("other-app/tests/lib_test.rs"),
        "{related}"
    );

    let related_symbols = runtime.dispatch(ToolRequest {
        id: serde_json::json!("related-symbols-cwd"),
        tool: "related_symbols".to_string(),
        arguments: serde_json::json!({
            "cwd": current_repo.join("src"),
            "path": "src/lib.rs",
            "query": "SharedThing",
            "limit": 5
        }),
    });
    assert!(
        related_symbols.error.is_none(),
        "{:?}",
        related_symbols.error
    );
    let related_symbols = serde_json::to_string(&related_symbols.result.unwrap()).unwrap();
    assert!(
        related_symbols.contains("current-app/src/lib.rs"),
        "{related_symbols}"
    );
    assert!(
        !related_symbols.contains("other-app/src/lib.rs"),
        "{related_symbols}"
    );

    let query_related_symbols = runtime.dispatch(ToolRequest {
        id: serde_json::json!("related-symbols-query-cwd"),
        tool: "related_symbols".to_string(),
        arguments: serde_json::json!({
            "cwd": current_repo.join("src"),
            "query": "SharedThing",
            "limit": 5
        }),
    });
    assert!(
        query_related_symbols.error.is_none(),
        "{:?}",
        query_related_symbols.error
    );
    let query_related_symbols =
        serde_json::to_string(&query_related_symbols.result.unwrap()).unwrap();
    assert!(
        query_related_symbols.contains("current-app/src/lib.rs"),
        "{query_related_symbols}"
    );
    assert!(
        query_related_symbols.contains("\"index_dir\""),
        "{query_related_symbols}"
    );
    assert!(
        !query_related_symbols.contains("other-app/src/lib.rs"),
        "{query_related_symbols}"
    );

    let plan = runtime.dispatch(ToolRequest {
        id: serde_json::json!("plan-cwd"),
        tool: "search_plan".to_string(),
        arguments: serde_json::json!({
            "cwd": current_repo.join("src"),
            "query": "shared_lookup_token definitely_missing",
            "limit": 5
        }),
    });
    assert!(plan.error.is_none(), "{:?}", plan.error);
    let plan = plan.result.unwrap();
    assert_eq!(
        plan[0]["plan"]["retry_requests"][0]["arguments"]["repo_filter"],
        serde_json::json!(current_root)
    );
    let plan_json = serde_json::to_string(&plan).unwrap();
    assert!(plan_json.contains("current-app"), "{plan_json}");
    assert!(!plan_json.contains("other-app/src/lib.rs"), "{plan_json}");

    let plan_batch = runtime.dispatch(ToolRequest {
        id: serde_json::json!("plan-batch-cwd"),
        tool: "search_plan_batch".to_string(),
        arguments: serde_json::json!({
            "cwd": current_repo.join("src"),
            "queries": [
                "shared_lookup_token definitely_missing",
                "repo:other-app shared_lookup_token definitely_missing"
            ],
            "limit": 5
        }),
    });
    assert!(plan_batch.error.is_none(), "{:?}", plan_batch.error);
    let plan_batch = plan_batch.result.unwrap();
    assert_eq!(
        plan_batch[0]["plans"][0]["plan"]["retry_requests"][0]["arguments"]["repo_filter"],
        serde_json::json!(current_root)
    );
    assert!(
        plan_batch[1]["plans"][0]["plan"]["retry_requests"][0]["arguments"]
            .get("repo_filter")
            .is_none(),
        "{plan_batch}"
    );
    let first_plan_batch_item = serde_json::to_string(&plan_batch[0]).unwrap();
    assert!(
        first_plan_batch_item.contains("current-app"),
        "{first_plan_batch_item}"
    );
    assert!(
        !first_plan_batch_item.contains("other-app/src/lib.rs"),
        "{first_plan_batch_item}"
    );
    let second_plan_batch_item = serde_json::to_string(&plan_batch[1]).unwrap();
    assert!(
        second_plan_batch_item.contains("other-app"),
        "{second_plan_batch_item}"
    );
    assert!(
        !second_plan_batch_item.contains("current-app/src/lib.rs"),
        "{second_plan_batch_item}"
    );

    let symbol = runtime.dispatch(ToolRequest {
        id: serde_json::json!("symbol-cwd"),
        tool: "find_symbol".to_string(),
        arguments: serde_json::json!({
            "cwd": current_repo.join("src"),
            "name": "SharedThing",
            "limit": 5
        }),
    });
    assert!(symbol.error.is_none(), "{:?}", symbol.error);
    let symbol = symbol.result.unwrap();
    let symbol_json = serde_json::to_string(&symbol).unwrap();
    assert!(
        symbol_json.contains("current-app/src/lib.rs"),
        "{symbol_json}"
    );
    assert!(
        !symbol_json.contains("other-app/src/lib.rs"),
        "{symbol_json}"
    );

    let symbol_batch = runtime.dispatch(ToolRequest {
        id: serde_json::json!("symbol-batch-cwd"),
        tool: "find_symbol_batch".to_string(),
        arguments: serde_json::json!({
            "cwd": current_repo.join("src"),
            "names": ["SharedThing"],
            "limit": 5
        }),
    });
    assert!(symbol_batch.error.is_none(), "{:?}", symbol_batch.error);
    let symbol_batch = symbol_batch.result.unwrap();
    let symbol_batch_json = serde_json::to_string(&symbol_batch).unwrap();
    assert!(
        symbol_batch_json.contains("current-app/src/lib.rs"),
        "{symbol_batch_json}"
    );
    assert!(
        !symbol_batch_json.contains("other-app/src/lib.rs"),
        "{symbol_batch_json}"
    );
}

#[test]
fn refresh_shards_repairs_corrupt_shard_index() {
    let repo = tempfile::tempdir().unwrap();
    write(
        &repo.path().join("src/billing.rs"),
        "pub fn invoice_total() -> usize { 42 }\n",
    );
    write(
        &repo.path().join("Cargo.toml"),
        "[package]\nname='billing'\nversion='0.1.0'\nedition='2024'\n",
    );
    let shard_dir = tempfile::tempdir().unwrap();
    build_shards(&[repo.path().to_path_buf()], shard_dir.path()).unwrap();

    let shard_index = fs::read_dir(shard_dir.path())
        .unwrap()
        .map(|entry| entry.unwrap().path())
        .find(|path| path.extension().is_some_and(|ext| ext == "orient"))
        .expect("shard index file");
    fs::write(&shard_index, b"not a valid orient shard").unwrap();

    let stats = refresh_shards(shard_dir.path()).unwrap();
    assert_eq!(stats.shards, 1);
    assert_eq!(stats.files, 2);

    let loaded = FastIndex::load(&shard_index).unwrap();
    let results = loaded
        .search_filtered(
            "invoice total",
            5,
            &orient::repo_index::SearchFilters::default(),
        )
        .unwrap();
    assert_eq!(results[0].path, "src/billing.rs");
}

#[test]
fn refresh_shards_waits_for_existing_writer_lock() {
    let repo = tempfile::tempdir().unwrap();
    write(
        &repo.path().join("src/billing.rs"),
        "pub fn invoice_total() -> usize { 42 }\n",
    );
    write(
        &repo.path().join("Cargo.toml"),
        "[package]\nname='billing'\nversion='0.1.0'\nedition='2024'\n",
    );
    let shard_dir = tempfile::tempdir().unwrap();
    build_shards(&[repo.path().to_path_buf()], shard_dir.path()).unwrap();

    let lock_path = shard_dir.path().join(".orient-shards.lock");
    fs::write(&lock_path, b"held by test\n").unwrap();

    let shard_dir_path = shard_dir.path().to_path_buf();
    let handle = thread::spawn(move || refresh_shards(shard_dir_path));
    thread::sleep(Duration::from_millis(75));
    assert!(
        !handle.is_finished(),
        "refresh_shards should wait for the shard writer lock"
    );

    fs::remove_file(lock_path).unwrap();
    let stats = handle.join().unwrap().unwrap();
    assert_eq!(stats.shards, 1);
    assert_eq!(stats.files, 2);
}

#[test]
fn runtime_indexed_result_query_plan_includes_retry_requests() {
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
    let index_path = repo.path().join(".orient/index");
    FastIndex::build(repo.path())
        .unwrap()
        .save(&index_path)
        .unwrap();

    let runtime = ToolRuntime::default();
    let response = runtime.dispatch(ToolRequest {
        id: serde_json::json!("search"),
        tool: "indexed_search".to_string(),
        arguments: serde_json::json!({
            "index": index_path,
            "query": "shared cap token",
            "limit": 1,
            "explain": true
        }),
    });
    assert!(response.error.is_none(), "{:?}", response.error);
    let result = response.result.unwrap();
    let plan = &result[0]["query_plan"];
    assert_eq!(plan["candidate_cap_hit"], serde_json::json!(true));
    assert_eq!(
        plan["retry_requests"][0]["tool"],
        serde_json::json!("indexed_search_code")
    );
    assert_eq!(
        plan["retry_requests"][0]["arguments"]["query"],
        serde_json::json!("shared cap token path:src")
    );
    assert_eq!(
        plan["retry_requests"][0]["arguments"]["index"],
        serde_json::json!(index_path)
    );
    assert_eq!(
        plan["summary"]["top_repair_hints"][0]["kind"],
        serde_json::json!("narrow_query")
    );
    assert_eq!(
        plan["summary"]["top_repair_hints"][1]["suggested_query"],
        serde_json::json!("shared cap token path:src")
    );
}

#[test]
fn shard_manifest_save_replaces_existing_file_without_leaving_temp_files() {
    let workspace = tempfile::tempdir().unwrap();
    let repo = workspace.path().join("billing");
    write(
        &repo.join("src/lib.rs"),
        "pub fn invoice_total() -> usize { 42 }\n",
    );
    write(
        &repo.join("Cargo.toml"),
        "[package]\nname='billing'\nversion='0.1.0'\nedition='2024'\n",
    );
    let shard_dir = tempfile::tempdir().unwrap();
    build_shards(&[repo.clone()], shard_dir.path()).unwrap();
    let manifest_path = shard_dir.path().join("manifest.json");
    let original_manifest = fs::read_to_string(&manifest_path).unwrap();
    assert!(original_manifest.contains("\"shards\""));

    fs::remove_dir_all(&repo).unwrap();
    let refreshed = refresh_shards(shard_dir.path()).unwrap();
    assert_eq!(refreshed.removed_shards, 1);
    let updated_manifest: serde_json::Value =
        serde_json::from_slice(&fs::read(&manifest_path).unwrap()).unwrap();
    assert_eq!(updated_manifest["shards"], serde_json::json!([]));
    let temp_files = fs::read_dir(shard_dir.path())
        .unwrap()
        .filter_map(|entry| entry.ok())
        .filter(|entry| entry.file_name().to_string_lossy().contains(".tmp-"))
        .collect::<Vec<_>>();
    assert!(temp_files.is_empty(), "{temp_files:?}");
}

#[test]
fn shard_manifest_rejects_unsafe_or_ambiguous_entries() {
    let shard_dir = tempfile::tempdir().unwrap();
    let runtime = ToolRuntime::default();
    let manifest_path = shard_dir.path().join("manifest.json");

    fs::write(
        &manifest_path,
        serde_json::to_vec(&serde_json::json!({
            "version": 1,
            "shards": [{
                "name": "bad",
                "root": shard_dir.path(),
                "index": "../escape.orient",
                "aliases": []
            }]
        }))
        .unwrap(),
    )
    .unwrap();
    let response = runtime.dispatch(ToolRequest {
        id: serde_json::json!("status"),
        tool: "shard_status".to_string(),
        arguments: serde_json::json!({
            "index_dir": shard_dir.path()
        }),
    });
    let error = response.error.unwrap();
    assert!(
        error.contains("invalid shard manifest index path"),
        "{error}"
    );

    fs::write(
        &manifest_path,
        serde_json::to_vec(&serde_json::json!({
            "version": 1,
            "shards": [
                {
                    "name": "duplicate",
                    "root": shard_dir.path(),
                    "index": "one.orient",
                    "aliases": []
                },
                {
                    "name": "duplicate",
                    "root": shard_dir.path(),
                    "index": "two.orient",
                    "aliases": []
                }
            ]
        }))
        .unwrap(),
    )
    .unwrap();
    let response = runtime.dispatch(ToolRequest {
        id: serde_json::json!("status"),
        tool: "shard_status".to_string(),
        arguments: serde_json::json!({
            "index_dir": shard_dir.path()
        }),
    });
    let error = response.error.unwrap();
    assert!(error.contains("duplicate shard name"), "{error}");
}

#[test]
fn runtime_shard_status_scopes_to_client_cwd_or_absolute_repo_filter() {
    let root = tempfile::tempdir().unwrap();
    let current_repo = root.path().join("current-app");
    let other_repo = root.path().join("other-app");
    write(
        &current_repo.join("src/lib.rs"),
        "pub fn current_marker() -> usize { 1 }\n",
    );
    write(
        &current_repo.join("Cargo.toml"),
        "[package]\nname='current-app'\nversion='0.1.0'\nedition='2024'\n",
    );
    write(
        &other_repo.join("src/lib.rs"),
        "pub fn other_marker() -> usize { 2 }\n",
    );
    write(
        &other_repo.join("Cargo.toml"),
        "[package]\nname='other-app'\nversion='0.1.0'\nedition='2024'\n",
    );
    git(&current_repo, &["init"]);
    git(&other_repo, &["init"]);

    let shard_dir = tempfile::tempdir().unwrap();
    build_shards(
        &[current_repo.clone(), other_repo.clone()],
        shard_dir.path(),
    )
    .unwrap();
    write(
        &current_repo.join("src/new_file.rs"),
        "pub fn current_added_after_index() {}\n",
    );

    let manifest: serde_json::Value =
        serde_json::from_slice(&fs::read(shard_dir.path().join("manifest.json")).unwrap()).unwrap();
    let other_index = manifest["shards"]
        .as_array()
        .unwrap()
        .iter()
        .find(|shard| shard["name"] == "other-app")
        .unwrap()["index"]
        .as_str()
        .unwrap();
    fs::write(shard_dir.path().join(other_index), b"not an orient index").unwrap();

    let runtime = ToolRuntime::default();
    let cwd_status = runtime.dispatch(ToolRequest {
        id: serde_json::json!("cwd-status"),
        tool: "shard_status".to_string(),
        arguments: serde_json::json!({
            "index_dir": shard_dir.path(),
            "cwd": current_repo.join("src")
        }),
    });
    assert!(cwd_status.error.is_none(), "{:?}", cwd_status.error);
    let result = cwd_status.result.unwrap();
    assert_eq!(result["shard_count"], serde_json::json!(1));
    assert_eq!(result["stale"], serde_json::json!(true));
    assert_eq!(result["stale_shards"], serde_json::json!(1));
    assert_eq!(result["added_files"], serde_json::json!(1));
    let rendered = serde_json::to_string(&result).unwrap();
    assert!(rendered.contains("current-app"), "{rendered}");
    assert!(!rendered.contains("other-app"), "{rendered}");

    let repo_filter_status = runtime.dispatch(ToolRequest {
        id: serde_json::json!("repo-filter-status"),
        tool: "shard_status".to_string(),
        arguments: serde_json::json!({
            "index_dir": shard_dir.path(),
            "repo_filter": current_repo.canonicalize().unwrap()
        }),
    });
    assert!(
        repo_filter_status.error.is_none(),
        "{:?}",
        repo_filter_status.error
    );
    let result = repo_filter_status.result.unwrap();
    assert_eq!(result["shard_count"], serde_json::json!(1));
    assert_eq!(result["stale_shards"], serde_json::json!(1));
    let rendered = serde_json::to_string(&result).unwrap();
    assert!(rendered.contains("current-app"), "{rendered}");
    assert!(!rendered.contains("other-app"), "{rendered}");
}

#[test]
fn runtime_serves_parallel_warm_shard_searches() {
    let root = tempfile::tempdir().unwrap();
    let mut repos = Vec::new();
    for index in 0..6 {
        let repo = root.path().join(format!("service_{index}"));
        write(
            &repo.join("src/lib.rs"),
            &format!("pub fn shared_search_token_{index}() -> usize {{ {index} }}\n"),
        );
        write(
            &repo.join("Cargo.toml"),
            &format!("[package]\nname='service-{index}'\nversion='0.1.0'\nedition='2024'\n"),
        );
        repos.push(repo);
    }
    let shard_dir = tempfile::tempdir().unwrap();
    let runtime = Arc::new(ToolRuntime::default());
    let build = runtime.dispatch(ToolRequest {
        id: serde_json::json!("build"),
        tool: "index_shards".to_string(),
        arguments: serde_json::json!({
            "repos": repos,
            "output_dir": shard_dir.path()
        }),
    });
    assert!(build.error.is_none(), "{:?}", build.error);
    assert_eq!(build.result.unwrap()["shards"], serde_json::json!(6));

    runtime.warm_shards(shard_dir.path().to_path_buf()).unwrap();
    assert_eq!(runtime.cached_index_count(), 6);

    let mut handles = Vec::new();
    for index in 0..8 {
        let runtime = Arc::clone(&runtime);
        let shard_dir = shard_dir.path().to_path_buf();
        handles.push(thread::spawn(move || {
            runtime.dispatch(ToolRequest {
                id: serde_json::json!(index),
                tool: "search_shards".to_string(),
                arguments: serde_json::json!({
                    "index_dir": shard_dir,
                    "query": "shared search token",
                    "limit": 5,
                    "require_all": true
                }),
            })
        }));
    }

    for handle in handles {
        let response = handle.join().unwrap();
        assert!(response.error.is_none(), "{:?}", response.error);
        let result = serde_json::to_string(&response.result).unwrap();
        assert!(result.contains("shared_search_token"), "{result}");
        assert!(result.contains("service_"), "{result}");
    }
    assert_eq!(runtime.cached_index_count(), 6);
}

#[test]
fn runtime_bounds_lazy_shard_index_cache() {
    let workspace = tempfile::tempdir().unwrap();
    let auth_repo = workspace.path().join("auth");
    write(
        &auth_repo.join("src/lib.rs"),
        "pub fn issue_token() -> usize { 1 }\n",
    );
    let billing_repo = workspace.path().join("billing");
    write(
        &billing_repo.join("src/lib.rs"),
        "pub fn invoice_total() -> usize { 42 }\n",
    );

    let shard_dir = tempfile::tempdir().unwrap();
    build_shards(&[auth_repo.clone(), billing_repo.clone()], shard_dir.path()).unwrap();

    let runtime = ToolRuntime::with_max_cached_indexes(1);
    runtime
        .register_shards(shard_dir.path().to_path_buf())
        .unwrap();
    assert_eq!(runtime.cached_index_count(), 0);

    let first = runtime.dispatch(ToolRequest {
        id: serde_json::json!("first"),
        tool: "search_shards".to_string(),
        arguments: serde_json::json!({
            "index_dir": shard_dir.path(),
            "query": "issue token",
            "limit": 3,
            "require_all": true
        }),
    });
    assert!(first.error.is_none(), "{:?}", first.error);
    assert_eq!(runtime.cached_index_count(), 1);
    assert_eq!(
        runtime.daemon_status()["cached_index_details"][0]["root"],
        serde_json::json!(auth_repo.canonicalize().unwrap().to_string_lossy())
    );

    let second = runtime.dispatch(ToolRequest {
        id: serde_json::json!("second"),
        tool: "search_shards".to_string(),
        arguments: serde_json::json!({
            "index_dir": shard_dir.path(),
            "query": "invoice total",
            "limit": 3,
            "require_all": true
        }),
    });
    assert!(second.error.is_none(), "{:?}", second.error);
    assert_eq!(runtime.cached_index_count(), 1);
    let status = runtime.daemon_status();
    assert_eq!(status["max_cached_indexes"], serde_json::json!(1));
    assert_eq!(
        status["cached_index_details"][0]["root"],
        serde_json::json!(billing_repo.canonicalize().unwrap().to_string_lossy())
    );

    let third = runtime.dispatch(ToolRequest {
        id: serde_json::json!("third"),
        tool: "search_shards".to_string(),
        arguments: serde_json::json!({
            "index_dir": shard_dir.path(),
            "query": "issue token",
            "limit": 3,
            "require_all": true
        }),
    });
    assert!(third.error.is_none(), "{:?}", third.error);
    assert_eq!(runtime.cached_index_count(), 1);
    assert_eq!(
        runtime.daemon_status()["cached_index_details"][0]["root"],
        serde_json::json!(auth_repo.canonicalize().unwrap().to_string_lossy())
    );
}

#[test]
fn daemon_status_with_cwd_returns_checkout_scoped_default_requests() {
    let workspace = tempfile::tempdir().unwrap();
    let auth_repo = workspace.path().join("auth");
    write(
        &auth_repo.join("src/lib.rs"),
        "pub struct AuthSession;\npub fn issue_token() -> AuthSession { AuthSession }\n",
    );
    write(&auth_repo.join("Cargo.toml"), "[package]\nname='auth'\n");
    git(&auth_repo, &["init"]);

    let billing_repo = workspace.path().join("billing");
    write(
        &billing_repo.join("src/lib.rs"),
        "pub struct BillingInvoice;\npub fn invoice_total() -> BillingInvoice { BillingInvoice }\n",
    );
    write(
        &billing_repo.join("Cargo.toml"),
        "[package]\nname='billing'\n",
    );
    git(&billing_repo, &["init"]);

    let shard_dir = tempfile::tempdir().unwrap();
    build_shards(&[auth_repo.clone(), billing_repo.clone()], shard_dir.path()).unwrap();
    write(
        &auth_repo.join("src/session.rs"),
        "pub struct SessionManager;\npub fn issue_token() -> SessionManager { SessionManager }\n",
    );

    let runtime = ToolRuntime::default();
    runtime
        .register_shards(shard_dir.path().to_path_buf())
        .unwrap();

    let cwd = auth_repo.join("src");
    let status = runtime.dispatch(ToolRequest {
        id: serde_json::json!("status"),
        tool: "daemon_status".to_string(),
        arguments: serde_json::json!({
            "cwd": cwd
        }),
    });
    assert!(status.error.is_none(), "{:?}", status.error);
    let status = status.result.unwrap();
    assert_eq!(status["details_omitted"], serde_json::json!(true));
    assert!(
        status.get("cached_shard_manifest_details").is_none(),
        "{status}"
    );
    assert_eq!(
        status["client_scope"]["cwd"],
        serde_json::json!(auth_repo.join("src"))
    );
    assert_eq!(
        status["default_requests"]["repo_map"]["tool"],
        serde_json::json!("repo_map")
    );
    assert_eq!(
        status["default_requests"]["repo_map"]["arguments"]["cwd"],
        serde_json::json!(auth_repo.join("src"))
    );
    assert_eq!(
        status["default_requests"]["repo_map"]["arguments"]["refresh_if_stale"],
        serde_json::json!(true)
    );
    assert!(status["default_requests"]["repo_map"]["arguments"]["index_dir"].is_null());
    assert_eq!(
        status["default_requests"]["search"]["arguments"]["cwd"],
        serde_json::json!(auth_repo.join("src"))
    );
    assert_eq!(
        status["default_requests"]["search"]["arguments"]["refresh_if_stale"],
        serde_json::json!(true)
    );
    assert_eq!(
        status["default_requests"]["search_batch"]["arguments"]["refresh_if_stale"],
        serde_json::json!(true)
    );
    assert_eq!(
        status["default_requests"]["query_plan"]["tool"],
        serde_json::json!("search_plan")
    );
    assert_eq!(
        status["default_requests"]["query_plan"]["arguments"]["cwd"],
        serde_json::json!(auth_repo.join("src"))
    );
    assert_eq!(
        status["default_requests"]["query_plan"]["arguments"]["refresh_if_stale"],
        serde_json::json!(true)
    );

    let map_request = &status["default_requests"]["repo_map"];
    let map = runtime.dispatch(ToolRequest {
        id: serde_json::json!("map"),
        tool: map_request["tool"].as_str().unwrap().to_string(),
        arguments: map_request["arguments"].clone(),
    });
    assert!(map.error.is_none(), "{:?}", map.error);
    let map = serde_json::to_string(&map.result).unwrap();
    assert!(map.contains("AuthSession"), "{map}");
    assert!(map.contains("SessionManager"), "{map}");
    assert!(!map.contains("BillingInvoice"), "{map}");

    let search_request = &status["default_requests"]["search"];
    let search = runtime.dispatch(ToolRequest {
        id: serde_json::json!("search"),
        tool: search_request["tool"].as_str().unwrap().to_string(),
        arguments: search_request["arguments"].clone(),
    });
    assert!(search.error.is_none(), "{:?}", search.error);
    let search = serde_json::to_string(&search.result).unwrap();
    assert!(search.contains("auth/src/session.rs"), "{search}");
    assert!(!search.contains("billing/src/lib.rs"), "{search}");

    let plan_request = &status["default_requests"]["query_plan"];
    let plan = runtime.dispatch(ToolRequest {
        id: serde_json::json!("plan"),
        tool: plan_request["tool"].as_str().unwrap().to_string(),
        arguments: plan_request["arguments"].clone(),
    });
    assert!(plan.error.is_none(), "{:?}", plan.error);

    let direct_plan = runtime.dispatch(ToolRequest {
        id: serde_json::json!("direct-plan"),
        tool: "shard_query_plan".to_string(),
        arguments: serde_json::json!({
            "cwd": cwd,
            "query": "issue token"
        }),
    });
    assert!(direct_plan.error.is_none(), "{:?}", direct_plan.error);
    let direct_plan = serde_json::to_string(&direct_plan.result).unwrap();
    assert!(direct_plan.contains("\"name\":\"auth\""), "{direct_plan}");
    assert!(
        !direct_plan.contains("\"name\":\"billing\""),
        "{direct_plan}"
    );

    let direct_search = runtime.dispatch(ToolRequest {
        id: serde_json::json!("direct-search"),
        tool: "search_shards".to_string(),
        arguments: serde_json::json!({
            "cwd": cwd,
            "query": "issue token",
            "require_all": true
        }),
    });
    assert!(direct_search.error.is_none(), "{:?}", direct_search.error);
    let direct_search = serde_json::to_string(&direct_search.result).unwrap();
    assert!(direct_search.contains("auth/src/lib.rs"), "{direct_search}");
    assert!(
        !direct_search.contains("billing/src/lib.rs"),
        "{direct_search}"
    );

    let direct_plan_batch = runtime.dispatch(ToolRequest {
        id: serde_json::json!("direct-plan-batch"),
        tool: "shard_query_plan_batch".to_string(),
        arguments: serde_json::json!({
            "cwd": cwd,
            "queries": ["issue token", "repo:billing invoice total"]
        }),
    });
    assert!(
        direct_plan_batch.error.is_none(),
        "{:?}",
        direct_plan_batch.error
    );
    let direct_plan_batch = direct_plan_batch.result.unwrap();
    let first = serde_json::to_string(&direct_plan_batch[0]).unwrap();
    assert!(first.contains("\"name\":\"auth\""), "{first}");
    assert!(!first.contains("\"name\":\"billing\""), "{first}");
    let second = serde_json::to_string(&direct_plan_batch[1]).unwrap();
    assert!(second.contains("\"name\":\"billing\""), "{second}");
    assert!(!second.contains("\"name\":\"auth\""), "{second}");
}

#[test]
fn runtime_shard_search_uses_global_prefilter_before_manifest_cache() {
    let workspace = tempfile::tempdir().unwrap();
    let repo = workspace.path().join("service");
    write(
        &repo.join("src/lib.rs"),
        "pub fn present_runtime_prefilter_symbol() -> bool { true }\n",
    );

    let shard_dir = tempfile::tempdir().unwrap();
    build_shards(&[repo], shard_dir.path()).unwrap();

    let runtime = ToolRuntime::default();
    let response = runtime.dispatch(ToolRequest {
        id: serde_json::json!("search"),
        tool: "search_shards".to_string(),
        arguments: serde_json::json!({
            "index_dir": shard_dir.path(),
            "query": "globally_absent_runtime_prefilter_probe_xyz",
            "limit": 10
        }),
    });
    assert!(response.error.is_none(), "{:?}", response.error);
    let results = response.result.unwrap();
    assert_eq!(results.as_array().unwrap().len(), 0);
    assert_eq!(runtime.cached_shard_manifest_count(), 0);
    assert_eq!(runtime.cached_index_count(), 0);
}

#[test]
fn runtime_shard_search_uses_manifest_sketch_before_cold_load() {
    let workspace = tempfile::tempdir().unwrap();
    let hit_repo = workspace.path().join("hit-service");
    let miss_repo = workspace.path().join("miss-service");
    write(
        &hit_repo.join("src/lib.rs"),
        "pub fn zzprefilteronlyneedle() -> bool { true }\n",
    );
    write(
        &miss_repo.join("src/lib.rs"),
        "pub fn unrelated_runtime_service() -> bool { false }\n",
    );

    let shard_dir = tempfile::tempdir().unwrap();
    build_shards(&[hit_repo, miss_repo], shard_dir.path()).unwrap();
    assert!(shard_dir.path().join("manifest.route.bin").exists());
    let manifest: serde_json::Value =
        serde_json::from_slice(&fs::read(shard_dir.path().join("manifest.json")).unwrap()).unwrap();
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

    let runtime = ToolRuntime::default();
    let response = runtime.dispatch(ToolRequest {
        id: serde_json::json!("search"),
        tool: "search_shards".to_string(),
        arguments: serde_json::json!({
            "index_dir": shard_dir.path(),
            "query": "zzprefilteronlyneedle",
            "limit": 10
        }),
    });
    assert!(response.error.is_none(), "{:?}", response.error);
    let result = serde_json::to_string(&response.result).unwrap();
    assert!(result.contains("hit-service/src/lib.rs"), "{result}");
    assert!(!result.contains("miss-service/src/lib.rs"), "{result}");
    assert_eq!(runtime.cached_index_count(), 1);

    let plan_response = runtime.dispatch(ToolRequest {
        id: serde_json::json!("plan"),
        tool: "shard_query_plan".to_string(),
        arguments: serde_json::json!({
            "index_dir": shard_dir.path(),
            "query": "zzprefilteronlyneedle missingterm"
        }),
    });
    assert!(plan_response.error.is_none(), "{:?}", plan_response.error);
    let plan_result = serde_json::to_string(&plan_response.result).unwrap();
    assert!(
        plan_result.contains("\"name\":\"hit-service\""),
        "{plan_result}"
    );
    assert!(
        !plan_result.contains("\"name\":\"miss-service\""),
        "{plan_result}"
    );
    assert!(plan_result.contains("\"missing_terms\""), "{plan_result}");
    assert!(plan_result.contains("missingterm"), "{plan_result}");
    assert_eq!(runtime.cached_index_count(), 1);
}

#[test]
fn runtime_shard_search_uses_route_before_manifest_cache() {
    let workspace = tempfile::tempdir().unwrap();
    let hit_repo = workspace.path().join("hit-service");
    let miss_repo = workspace.path().join("miss-service");
    write(
        &hit_repo.join("src/lib.rs"),
        "pub fn runtime_route_probe_symbol() -> bool { true }\n",
    );
    write(
        &miss_repo.join("src/lib.rs"),
        "pub fn unrelated_runtime_route_service() -> bool { false }\n",
    );

    let shard_dir = tempfile::tempdir().unwrap();
    build_shards(&[hit_repo, miss_repo], shard_dir.path()).unwrap();
    let manifest: serde_json::Value =
        serde_json::from_slice(&fs::read(shard_dir.path().join("manifest.json")).unwrap()).unwrap();
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

    let runtime = ToolRuntime::default();
    let response = runtime.dispatch(ToolRequest {
        id: serde_json::json!("search"),
        tool: "search_shards".to_string(),
        arguments: serde_json::json!({
            "index_dir": shard_dir.path(),
            "query": "kind:function runtime_route_probe_symbol",
            "limit": 10
        }),
    });
    assert!(response.error.is_none(), "{:?}", response.error);
    let result = serde_json::to_string(&response.result).unwrap();
    assert!(result.contains("hit-service/src/lib.rs"), "{result}");
    assert!(!result.contains("miss-service/src/lib.rs"), "{result}");
    assert_eq!(runtime.cached_shard_manifest_count(), 0);
    assert_eq!(runtime.cached_index_count(), 1);
}

#[test]
fn runtime_cached_shard_search_prefers_route_over_cached_manifest() {
    let workspace = tempfile::tempdir().unwrap();
    let hit_repo = workspace.path().join("hit-service");
    let miss_repo = workspace.path().join("miss-service");
    write(
        &hit_repo.join("src/lib.rs"),
        "pub fn prefixruntime_trigramprobesuffix() -> bool { true }\n",
    );
    write(
        &miss_repo.join("src/lib.rs"),
        "pub fn unrelated_cached_runtime_trigram_service() -> bool { false }\n\
         // tri rig igr gra ram amp mpr pro rob obe\n",
    );

    let shard_dir = tempfile::tempdir().unwrap();
    build_shards(&[hit_repo, miss_repo], shard_dir.path()).unwrap();
    let manifest: serde_json::Value =
        serde_json::from_slice(&fs::read(shard_dir.path().join("manifest.json")).unwrap()).unwrap();
    let miss_index = manifest["shards"]
        .as_array()
        .unwrap()
        .iter()
        .find(|shard| shard["name"] == "miss-service")
        .unwrap()["index"]
        .as_str()
        .unwrap()
        .to_string();

    let runtime = ToolRuntime::default();
    assert_eq!(
        runtime
            .register_shards(shard_dir.path().to_path_buf())
            .unwrap(),
        2
    );
    fs::remove_file(shard_dir.path().join(miss_index)).unwrap();

    let response = runtime.dispatch(ToolRequest {
        id: serde_json::json!("search"),
        tool: "search_shards".to_string(),
        arguments: serde_json::json!({
            "index_dir": shard_dir.path(),
            "query": "trigramprobe",
            "limit": 10
        }),
    });
    assert!(response.error.is_none(), "{:?}", response.error);
    let result = serde_json::to_string(&response.result).unwrap();
    assert!(result.contains("hit-service/src/lib.rs"), "{result}");
    assert!(!result.contains("miss-service/src/lib.rs"), "{result}");
    assert_eq!(runtime.cached_shard_manifest_count(), 1);
    assert_eq!(runtime.cached_index_count(), 1);
}

#[test]
fn runtime_routed_shard_search_applies_filter_sketch_before_cold_load() {
    let workspace = tempfile::tempdir().unwrap();
    let rust_repo = workspace.path().join("rust-service");
    let python_repo = workspace.path().join("python-service");
    write(
        &rust_repo.join("src/lib.rs"),
        "pub fn shared_route_filter_probe_symbol() -> bool { true }\n",
    );
    write(
        &python_repo.join("app.py"),
        "def shared_route_filter_probe_symbol():\n    return False\n",
    );

    let shard_dir = tempfile::tempdir().unwrap();
    build_shards(&[rust_repo, python_repo], shard_dir.path()).unwrap();
    let manifest: serde_json::Value =
        serde_json::from_slice(&fs::read(shard_dir.path().join("manifest.json")).unwrap()).unwrap();
    let python_index = manifest["shards"]
        .as_array()
        .unwrap()
        .iter()
        .find(|shard| shard["name"] == "python-service")
        .unwrap()["index"]
        .as_str()
        .unwrap()
        .to_string();
    fs::remove_file(shard_dir.path().join(python_index)).unwrap();

    let runtime = ToolRuntime::default();
    let response = runtime.dispatch(ToolRequest {
        id: serde_json::json!("search"),
        tool: "search_shards".to_string(),
        arguments: serde_json::json!({
            "index_dir": shard_dir.path(),
            "query": "lang:rust shared_route_filter_probe_symbol",
            "limit": 10
        }),
    });
    assert!(response.error.is_none(), "{:?}", response.error);
    let result = serde_json::to_string(&response.result).unwrap();
    assert!(result.contains("rust-service/src/lib.rs"), "{result}");
    assert!(!result.contains("python-service/app.py"), "{result}");
    assert_eq!(runtime.cached_shard_manifest_count(), 0);
    assert_eq!(runtime.cached_index_count(), 1);
}

#[test]
fn runtime_filter_only_shard_search_uses_route_before_manifest_sidecar() {
    let workspace = tempfile::tempdir().unwrap();
    let rust_repo = workspace.path().join("rust-service");
    let python_repo = workspace.path().join("python-service");
    write(
        &rust_repo.join("src/lib.rs"),
        "pub fn route_filter_only_rust() -> bool { true }\n",
    );
    write(
        &python_repo.join("app.py"),
        "def route_filter_only_python():\n    return False\n",
    );

    let shard_dir = tempfile::tempdir().unwrap();
    build_shards(&[rust_repo, python_repo], shard_dir.path()).unwrap();
    let manifest: serde_json::Value =
        serde_json::from_slice(&fs::read(shard_dir.path().join("manifest.json")).unwrap()).unwrap();
    let python_index = manifest["shards"]
        .as_array()
        .unwrap()
        .iter()
        .find(|shard| shard["name"] == "python-service")
        .unwrap()["index"]
        .as_str()
        .unwrap()
        .to_string();
    fs::remove_file(shard_dir.path().join("manifest.bin")).unwrap();
    fs::remove_file(shard_dir.path().join(python_index)).unwrap();

    let runtime = ToolRuntime::default();
    let response = runtime.dispatch(ToolRequest {
        id: serde_json::json!("search"),
        tool: "search_shards".to_string(),
        arguments: serde_json::json!({
            "index_dir": shard_dir.path(),
            "query": "lang:rust",
            "limit": 10
        }),
    });
    assert!(response.error.is_none(), "{:?}", response.error);
    let result = serde_json::to_string(&response.result).unwrap();
    assert!(result.contains("rust-service/src/lib.rs"), "{result}");
    assert!(!result.contains("python-service/app.py"), "{result}");
    assert_eq!(runtime.cached_shard_manifest_count(), 0);
    assert_eq!(runtime.cached_index_count(), 1);
}

#[test]
fn runtime_filter_only_routed_shard_search_uses_filter_sketch_before_cold_load() {
    let workspace = tempfile::tempdir().unwrap();
    let rust_repo = workspace.path().join("rust-service");
    let python_repo = workspace.path().join("python-service");
    write(
        &rust_repo.join("src/lib.rs"),
        "pub fn only_rust_route_filter_symbol() -> bool { true }\n",
    );
    write(
        &python_repo.join("app.py"),
        "def only_python_route_filter_symbol():\n    return False\n",
    );

    let shard_dir = tempfile::tempdir().unwrap();
    build_shards(&[rust_repo, python_repo], shard_dir.path()).unwrap();
    let manifest: serde_json::Value =
        serde_json::from_slice(&fs::read(shard_dir.path().join("manifest.json")).unwrap()).unwrap();
    let python_index = manifest["shards"]
        .as_array()
        .unwrap()
        .iter()
        .find(|shard| shard["name"] == "python-service")
        .unwrap()["index"]
        .as_str()
        .unwrap()
        .to_string();
    fs::remove_file(shard_dir.path().join(python_index)).unwrap();

    let runtime = ToolRuntime::default();
    let response = runtime.dispatch(ToolRequest {
        id: serde_json::json!("search"),
        tool: "search_shards".to_string(),
        arguments: serde_json::json!({
            "index_dir": shard_dir.path(),
            "query": "lang:rust",
            "limit": 10
        }),
    });
    assert!(response.error.is_none(), "{:?}", response.error);
    let result = serde_json::to_string(&response.result).unwrap();
    assert!(result.contains("rust-service/src/lib.rs"), "{result}");
    assert!(!result.contains("python-service/app.py"), "{result}");
    assert_eq!(runtime.cached_shard_manifest_count(), 0);
    assert_eq!(runtime.cached_index_count(), 1);
}

#[test]
fn runtime_search_auto_diagnose_prefers_retry_next_action_for_noisy_hits() {
    let repo = tempfile::tempdir().unwrap();
    for index in 0..8 {
        write(
            &repo.path().join(format!("src/service_{index}.rs")),
            &format!("pub fn needle_service_{index}() -> &'static str {{ \"needle\" }}\n"),
        );
        write(
            &repo.path().join(format!("docs/service_{index}.md")),
            "needle service documentation\n",
        );
    }

    let runtime = ToolRuntime::default();
    let response = runtime.dispatch(ToolRequest {
        id: serde_json::json!("diagnosed-noisy"),
        tool: "search_auto".to_string(),
        arguments: serde_json::json!({
            "repo": repo.path(),
            "query": "needle",
            "limit": 3,
            "diagnose": true
        }),
    });
    assert!(response.error.is_none(), "{:?}", response.error);
    let value = response.result.unwrap();
    assert!(!value["results"].as_array().unwrap().is_empty());
    assert!(!value["read_batch_request"].is_null());
    assert_eq!(
        value["next_action"]["source"],
        serde_json::json!("primary_retry_request")
    );
    assert_eq!(
        value["next_action"]["request"],
        value["primary_retry_request"]
    );
    assert_eq!(
        value["query_plan_summary"]["suggested_query"],
        value["primary_diagnosis"]["suggested_query"]
    );
    assert_eq!(
        value["query_plan_summary"]["primary_retry_request"],
        value["primary_retry_request"]
    );
    assert!(value["primary_diagnosis"]["suggested_query"].is_string());
}

#[test]
fn runtime_shard_query_plan_uses_route_before_manifest_cache() {
    let workspace = tempfile::tempdir().unwrap();
    let hit_repo = workspace.path().join("hit-service");
    let miss_repo = workspace.path().join("miss-service");
    write(
        &hit_repo.join("src/lib.rs"),
        "pub fn runtime_route_plan_symbol() -> bool { true }\n",
    );
    write(
        &miss_repo.join("src/lib.rs"),
        "pub fn unrelated_runtime_plan_service() -> bool { false }\n",
    );

    let shard_dir = tempfile::tempdir().unwrap();
    build_shards(&[hit_repo, miss_repo], shard_dir.path()).unwrap();
    let manifest: serde_json::Value =
        serde_json::from_slice(&fs::read(shard_dir.path().join("manifest.json")).unwrap()).unwrap();
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

    let runtime = ToolRuntime::default();
    let response = runtime.dispatch(ToolRequest {
        id: serde_json::json!("plan"),
        tool: "shard_query_plan".to_string(),
        arguments: serde_json::json!({
            "index_dir": shard_dir.path(),
            "query": "kind:function runtime_route_plan_symbol"
        }),
    });
    assert!(response.error.is_none(), "{:?}", response.error);
    let result = serde_json::to_string(&response.result).unwrap();
    assert!(result.contains("\"name\":\"hit-service\""), "{result}");
    assert!(!result.contains("\"name\":\"miss-service\""), "{result}");
    assert_eq!(runtime.cached_shard_manifest_count(), 0);
    assert_eq!(runtime.cached_index_count(), 1);

    fs::remove_file(shard_dir.path().join(hit_index)).unwrap();
    let absent = runtime.dispatch(ToolRequest {
        id: serde_json::json!("absent-plan"),
        tool: "shard_query_plan".to_string(),
        arguments: serde_json::json!({
            "index_dir": shard_dir.path(),
            "query": "definitely_absent_runtime_route_plan_symbol"
        }),
    });
    assert!(absent.error.is_none(), "{:?}", absent.error);
    let absent = absent.result.unwrap();
    assert_eq!(absent[0]["name"], serde_json::json!("__shard_selection__"));
    assert_eq!(
        absent[0]["plan"]["summary"]["status"],
        serde_json::json!("scope_mismatch")
    );
    assert_eq!(runtime.cached_shard_manifest_count(), 0);
    assert_eq!(runtime.cached_index_count(), 1);
}

#[test]
fn runtime_shard_search_uses_trigram_route_before_manifest_cache() {
    let workspace = tempfile::tempdir().unwrap();
    let hit_repo = workspace.path().join("hit-service");
    let miss_repo = workspace.path().join("miss-service");
    write(
        &hit_repo.join("src/lib.rs"),
        "pub fn prefixruntime_trigramprobesuffix() -> bool { true }\n",
    );
    write(
        &miss_repo.join("src/lib.rs"),
        "pub fn unrelated_runtime_trigram_service() -> bool { false }\n",
    );

    let shard_dir = tempfile::tempdir().unwrap();
    build_shards(&[hit_repo, miss_repo], shard_dir.path()).unwrap();
    let manifest: serde_json::Value =
        serde_json::from_slice(&fs::read(shard_dir.path().join("manifest.json")).unwrap()).unwrap();
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

    let runtime = ToolRuntime::default();
    let response = runtime.dispatch(ToolRequest {
        id: serde_json::json!("search"),
        tool: "search_shards".to_string(),
        arguments: serde_json::json!({
            "index_dir": shard_dir.path(),
            "query": "trigramprobe",
            "limit": 10
        }),
    });
    assert!(response.error.is_none(), "{:?}", response.error);
    let result = serde_json::to_string(&response.result).unwrap();
    assert!(result.contains("hit-service/src/lib.rs"), "{result}");
    assert!(!result.contains("miss-service/src/lib.rs"), "{result}");
    assert_eq!(runtime.cached_shard_manifest_count(), 0);
    assert_eq!(runtime.cached_index_count(), 1);
}

#[test]
fn runtime_serves_parallel_warm_shard_query_plans() {
    let root = tempfile::tempdir().unwrap();
    let mut repos = Vec::new();
    for index in 0..6 {
        let repo = root.path().join(format!("service_{index}"));
        write(
            &repo.join("src/lib.rs"),
            &format!("pub fn shared_plan_token_{index}() -> usize {{ {index} }}\n"),
        );
        write(
            &repo.join("Cargo.toml"),
            &format!("[package]\nname='service-{index}'\nversion='0.1.0'\nedition='2024'\n"),
        );
        repos.push(repo);
    }
    let shard_dir = tempfile::tempdir().unwrap();
    let runtime = Arc::new(ToolRuntime::default());
    let build = runtime.dispatch(ToolRequest {
        id: serde_json::json!("build"),
        tool: "index_shards".to_string(),
        arguments: serde_json::json!({
            "repos": repos,
            "output_dir": shard_dir.path()
        }),
    });
    assert!(build.error.is_none(), "{:?}", build.error);
    assert_eq!(build.result.unwrap()["shards"], serde_json::json!(6));

    runtime.warm_shards(shard_dir.path().to_path_buf()).unwrap();
    assert_eq!(runtime.cached_index_count(), 6);

    let mut handles = Vec::new();
    for index in 0..8 {
        let runtime = Arc::clone(&runtime);
        let shard_dir = shard_dir.path().to_path_buf();
        handles.push(thread::spawn(move || {
            runtime.dispatch(ToolRequest {
                id: serde_json::json!(index),
                tool: "shard_query_plan".to_string(),
                arguments: serde_json::json!({
                    "index_dir": shard_dir,
                    "query": "shared plan missingterm",
                    "require_all": true
                }),
            })
        }));
    }

    for handle in handles {
        let response = handle.join().unwrap();
        assert!(response.error.is_none(), "{:?}", response.error);
        let result = serde_json::to_string(&response.result).unwrap();
        assert!(result.contains("\"missing_terms\""), "{result}");
        assert!(result.contains("missingterm"), "{result}");
        assert!(result.contains("drop_missing_terms"), "{result}");
        assert!(result.contains("service_"), "{result}");
    }
    assert_eq!(runtime.cached_index_count(), 6);
}

#[test]
fn runtime_shard_query_plan_suggests_repo_facet_for_broad_queries() {
    let root = tempfile::tempdir().unwrap();
    let service_a = root.path().join("service_a");
    let service_b = root.path().join("service_b");
    for index in 0..12 {
        write(
            &service_a.join(format!("src/a_{index}.rs")),
            &format!("pub fn shared_facet_token_a_{index}() -> usize {{ {index} }}\n"),
        );
    }
    for index in 0..4 {
        write(
            &service_b.join(format!("src/b_{index}.rs")),
            &format!("pub fn shared_facet_token_b_{index}() -> usize {{ {index} }}\n"),
        );
    }
    write(
        &service_a.join("Cargo.toml"),
        "[package]\nname='service-a'\nversion='0.1.0'\nedition='2024'\n",
    );
    write(
        &service_b.join("Cargo.toml"),
        "[package]\nname='service-b'\nversion='0.1.0'\nedition='2024'\n",
    );

    let shard_dir = tempfile::tempdir().unwrap();
    build_shards(&[service_a, service_b], shard_dir.path()).unwrap();
    let runtime = ToolRuntime::default();
    let plan = runtime.dispatch(ToolRequest {
        id: serde_json::json!("plan"),
        tool: "shard_query_plan".to_string(),
        arguments: serde_json::json!({
            "index_dir": shard_dir.path(),
            "query": "shared facet token",
            "require_all": true
        }),
    });
    assert!(plan.error.is_none(), "{:?}", plan.error);
    let plans = plan.result.unwrap();
    let service_a_plan = plans
        .as_array()
        .unwrap()
        .iter()
        .find(|plan| plan["name"] == "service_a")
        .unwrap();
    let repo_hint = service_a_plan["plan"]["repair_hints"]
        .as_array()
        .unwrap()
        .iter()
        .find(|hint| hint["kind"] == "narrow_by_repo")
        .unwrap();
    assert_eq!(
        repo_hint["suggested_query"],
        serde_json::json!("shared facet token repo:service_a")
    );
    assert!(
        repo_hint["message"]
            .as_str()
            .unwrap()
            .contains("from 16 files to 12"),
        "{repo_hint:?}"
    );
    assert_eq!(
        service_a_plan["plan"]["retry_requests"][0]["tool"],
        serde_json::json!("search_shards")
    );
    assert_eq!(
        service_a_plan["plan"]["retry_requests"][0]["arguments"]["query"],
        serde_json::json!("shared facet token repo:service_a")
    );
}

#[test]
fn runtime_filters_shard_search_by_nested_repo_alias() {
    let workspace = tempfile::tempdir().unwrap();
    let billing_repo = workspace.path().join("billing");
    let auth_repo = workspace.path().join("auth");
    write(
        &billing_repo.join("src/billing.rs"),
        "pub fn invoice_total() -> usize { 42 }\n",
    );
    write(
        &billing_repo.join("tests/billing_test.rs"),
        "use billing::invoice_total;\n#[test]\nfn totals_invoice() {}\n",
    );
    write(
        &billing_repo.join("Cargo.toml"),
        "[package]\nname='billing'\nversion='0.1.0'\nedition='2024'\n",
    );
    write(
        &billing_repo.join("package.json"),
        r#"{"scripts":{"test":"vitest run","lint":"eslint .","typecheck":"tsc --noEmit"}}"#,
    );
    write(
        &billing_repo.join("pnpm-lock.yaml"),
        "lockfileVersion: '9.0'\n",
    );
    write(
        &auth_repo.join("src/auth.rs"),
        "pub fn issue_token() -> String { \"token\".to_string() }\n",
    );
    write(
        &auth_repo.join("Cargo.toml"),
        "[package]\nname='auth'\nversion='0.1.0'\nedition='2024'\n",
    );
    let shard_dir = tempfile::tempdir().unwrap();
    let runtime = ToolRuntime::default();
    let build = runtime.dispatch(ToolRequest {
        id: serde_json::json!("build"),
        tool: "index_shards".to_string(),
        arguments: serde_json::json!({
            "repos": [workspace.path()],
            "output_dir": shard_dir.path()
        }),
    });
    assert!(build.error.is_none(), "{:?}", build.error);

    let search = runtime.dispatch(ToolRequest {
        id: serde_json::json!("search"),
        tool: "search_shards".to_string(),
        arguments: serde_json::json!({
            "index_dir": shard_dir.path(),
            "query": "repo:billing invoice total",
            "limit": 5,
            "require_all": true
        }),
    });
    assert!(search.error.is_none(), "{:?}", search.error);
    let search_result = search.result.as_ref().unwrap().as_array().unwrap();
    assert_eq!(
        search_result[0]["read_range"]["path"],
        serde_json::json!("billing/src/billing.rs")
    );
    assert_eq!(
        search_result[0]["read_request"]["tool"],
        serde_json::json!("read_shard_range")
    );
    assert_eq!(
        search_result[0]["read_request"]["arguments"]["range"]["path"],
        serde_json::json!("billing/src/billing.rs")
    );
    assert_eq!(
        search_result[0]["read_request"]["arguments"]["index_dir"],
        serde_json::json!(shard_dir.path())
    );
    assert_eq!(
        search_result[0]["related_request"]["tool"],
        serde_json::json!("related_shard_files")
    );
    assert_eq!(
        search_result[0]["related_request"]["arguments"]["path"],
        serde_json::json!("billing/src/billing.rs")
    );
    assert_eq!(
        search_result[0]["related_request"]["arguments"]["index_dir"],
        serde_json::json!(shard_dir.path())
    );
    assert_eq!(
        search_result[0]["related_symbols_request"]["tool"],
        serde_json::json!("related_shard_symbols")
    );
    assert_eq!(
        search_result[0]["related_symbols_request"]["arguments"]["path"],
        serde_json::json!("billing/src/billing.rs")
    );
    assert_eq!(
        search_result[0]["related_symbols_request"]["arguments"]["query"],
        serde_json::json!("repo:billing invoice total")
    );
    assert_eq!(
        search_result[0]["related_symbols_request"]["arguments"]["index_dir"],
        serde_json::json!(shard_dir.path())
    );
    let related_symbols = runtime.dispatch(ToolRequest {
        id: serde_json::json!("related-symbols"),
        tool: search_result[0]["related_symbols_request"]["tool"]
            .as_str()
            .unwrap()
            .to_string(),
        arguments: search_result[0]["related_symbols_request"]["arguments"].clone(),
    });
    assert!(
        related_symbols.error.is_none(),
        "{:?}",
        related_symbols.error
    );
    let related_symbols = serde_json::to_string(&related_symbols.result).unwrap();
    assert!(
        related_symbols.contains("\"path\":\"billing/src/billing.rs\""),
        "{related_symbols}"
    );
    assert!(
        related_symbols.contains("invoice_total"),
        "{related_symbols}"
    );
    let result = serde_json::to_string(&search.result).unwrap();
    assert!(result.contains("billing/src/billing.rs"), "{result}");
    assert!(!result.contains("auth/src/auth.rs"), "{result}");

    let map = runtime.dispatch(ToolRequest {
        id: serde_json::json!("map"),
        tool: "shard_repo_map".to_string(),
        arguments: serde_json::json!({
            "index_dir": shard_dir.path(),
            "repo": "billing",
            "symbols": 10,
            "tests": 10
        }),
    });
    assert!(map.error.is_none(), "{:?}", map.error);
    let result = serde_json::to_string(&map.result).unwrap();
    assert!(result.contains("billing/Cargo.toml"), "{result}");
    assert!(result.contains("cargo test"), "{result}");
    assert!(result.contains("pnpm test"), "{result}");
    assert!(result.contains("pnpm run lint"), "{result}");
    assert!(result.contains("pnpm run typecheck"), "{result}");
    assert!(result.contains("\"command_hints\""), "{result}");
    assert!(
        result.contains("\"source\":\"billing/Cargo.toml\""),
        "{result}"
    );
    assert!(
        result.contains("\"source\":\"billing/package.json\""),
        "{result}"
    );
    assert!(!result.contains("auth/src/auth.rs"), "{result}");

    let symbol = runtime.dispatch(ToolRequest {
        id: serde_json::json!("symbol"),
        tool: "find_shard_symbol".to_string(),
        arguments: serde_json::json!({
            "index_dir": shard_dir.path(),
            "repo": "billing",
            "name": "invoice_total"
        }),
    });
    assert!(symbol.error.is_none(), "{:?}", symbol.error);
    let result = serde_json::to_string(&symbol.result).unwrap();
    assert!(
        result.contains("\"path\":\"billing/src/billing.rs\""),
        "{result}"
    );

    let range = runtime.dispatch(ToolRequest {
        id: serde_json::json!("range"),
        tool: "read_shard_range".to_string(),
        arguments: serde_json::json!({
            "index_dir": shard_dir.path(),
            "path": "billing/src/billing.rs",
            "start": 1,
            "lines": 1
        }),
    });
    assert!(range.error.is_none(), "{:?}", range.error);
    let result = serde_json::to_string(&range.result).unwrap();
    assert!(
        result.contains("\"path\":\"billing/src/billing.rs\""),
        "{result}"
    );
    assert!(result.contains("invoice_total"), "{result}");

    let related = runtime.dispatch(ToolRequest {
        id: serde_json::json!("related"),
        tool: "related_shard_files".to_string(),
        arguments: serde_json::json!({
            "index_dir": shard_dir.path(),
            "path": "billing/src/billing.rs",
            "limit": 5
        }),
    });
    assert!(related.error.is_none(), "{:?}", related.error);
    let result = serde_json::to_string(&related.result).unwrap();
    assert!(result.contains("billing/tests/billing_test.rs"), "{result}");
    assert!(!result.contains("auth/src/auth.rs"), "{result}");
    assert!(result.contains("\"read_request\""), "{result}");
    assert!(result.contains("\"tool\":\"read_shard_range\""), "{result}");

    let related_symbols = runtime.dispatch(ToolRequest {
        id: serde_json::json!("related-symbols"),
        tool: "related_shard_symbols".to_string(),
        arguments: serde_json::json!({
            "index_dir": shard_dir.path(),
            "path": "billing/src/billing.rs",
            "query": "invoice total",
            "limit": 5
        }),
    });
    assert!(
        related_symbols.error.is_none(),
        "{:?}",
        related_symbols.error
    );
    let result = serde_json::to_string(&related_symbols.result).unwrap();
    assert!(
        result.contains("\"path\":\"billing/src/billing.rs\""),
        "{result}"
    );
    assert!(result.contains("invoice_total"), "{result}");
    assert!(result.contains("\"read_request\""), "{result}");
    assert!(result.contains("\"tool\":\"read_shard_range\""), "{result}");
}

#[test]
fn runtime_refresh_shards_updates_nested_repo_aliases() {
    let workspace = tempfile::tempdir().unwrap();
    let billing_repo = workspace.path().join("billing");
    write(
        &billing_repo.join("src/billing.rs"),
        "pub fn invoice_total() -> usize { 42 }\n",
    );
    write(
        &billing_repo.join("Cargo.toml"),
        "[package]\nname='billing'\nversion='0.1.0'\nedition='2024'\n",
    );
    let shard_dir = tempfile::tempdir().unwrap();
    let runtime = ToolRuntime::default();
    let build = runtime.dispatch(ToolRequest {
        id: serde_json::json!("build"),
        tool: "index_shards".to_string(),
        arguments: serde_json::json!({
            "repos": [workspace.path()],
            "output_dir": shard_dir.path()
        }),
    });
    assert!(build.error.is_none(), "{:?}", build.error);

    let auth_repo = workspace.path().join("auth");
    write(
        &auth_repo.join("src/auth.rs"),
        "pub fn issue_token() -> String { \"token\".to_string() }\n",
    );
    write(
        &auth_repo.join("Cargo.toml"),
        "[package]\nname='auth'\nversion='0.1.0'\nedition='2024'\n",
    );

    let refresh = runtime.dispatch(ToolRequest {
        id: serde_json::json!("refresh"),
        tool: "refresh_shards".to_string(),
        arguments: serde_json::json!({
            "index_dir": shard_dir.path()
        }),
    });
    assert!(refresh.error.is_none(), "{:?}", refresh.error);

    let search = runtime.dispatch(ToolRequest {
        id: serde_json::json!("search"),
        tool: "search_shards".to_string(),
        arguments: serde_json::json!({
            "index_dir": shard_dir.path(),
            "query": "repo:auth issue token",
            "limit": 5,
            "require_all": true
        }),
    });
    assert!(search.error.is_none(), "{:?}", search.error);
    let result = serde_json::to_string(&search.result).unwrap();
    assert!(result.contains("auth/src/auth.rs"), "{result}");
    assert!(!result.contains("billing/src/billing.rs"), "{result}");
}

#[test]
fn runtime_refresh_shards_prunes_missing_repo_roots() {
    let workspace = tempfile::tempdir().unwrap();
    let auth_repo = workspace.path().join("auth");
    write(
        &auth_repo.join("src/auth.rs"),
        "pub fn issue_token() -> &'static str { \"token\" }\n",
    );
    write(
        &auth_repo.join("Cargo.toml"),
        "[package]\nname='auth'\nversion='0.1.0'\nedition='2024'\n",
    );
    let billing_repo = workspace.path().join("billing");
    write(
        &billing_repo.join("src/billing.rs"),
        "pub fn invoice_total() -> usize { 42 }\n",
    );
    write(
        &billing_repo.join("Cargo.toml"),
        "[package]\nname='billing'\nversion='0.1.0'\nedition='2024'\n",
    );
    let shard_dir = tempfile::tempdir().unwrap();
    let runtime = ToolRuntime::default();
    let build = runtime.dispatch(ToolRequest {
        id: serde_json::json!("build"),
        tool: "index_shards".to_string(),
        arguments: serde_json::json!({
            "repos": [auth_repo, billing_repo],
            "output_dir": shard_dir.path()
        }),
    });
    assert!(build.error.is_none(), "{:?}", build.error);
    assert_eq!(build.result.unwrap()["shards"], serde_json::json!(2));

    fs::remove_dir_all(workspace.path().join("billing")).unwrap();

    let refresh = runtime.dispatch(ToolRequest {
        id: serde_json::json!("refresh"),
        tool: "refresh_shards".to_string(),
        arguments: serde_json::json!({
            "index_dir": shard_dir.path()
        }),
    });
    assert!(refresh.error.is_none(), "{:?}", refresh.error);
    let result = refresh.result.unwrap();
    assert_eq!(result["removed_shards"], serde_json::json!(1));
    assert_eq!(result["shards"], serde_json::json!(1));

    let search = runtime.dispatch(ToolRequest {
        id: serde_json::json!("search"),
        tool: "search_shards".to_string(),
        arguments: serde_json::json!({
            "index_dir": shard_dir.path(),
            "query": "issue_token"
        }),
    });
    assert!(search.error.is_none(), "{:?}", search.error);
    let result = serde_json::to_string(&search.result).unwrap();
    assert!(result.contains("auth/src/auth.rs"), "{result}");
}

#[test]
fn tcp_daemon_serves_json_lines_requests() {
    let binary = assert_cmd::cargo::cargo_bin("orient");
    let mut child = Command::new(binary)
        .args(["serve-tcp", "--addr", "127.0.0.1:0"])
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    let stdout = child.stdout.take().unwrap();
    let mut startup_reader = BufReader::new(stdout);
    let mut startup = String::new();
    startup_reader.read_line(&mut startup).unwrap();
    let startup_json: serde_json::Value = serde_json::from_str(&startup).unwrap();
    assert_eq!(
        startup_json["daemon_version"],
        serde_json::json!(env!("CARGO_PKG_VERSION"))
    );
    assert_eq!(
        startup_json["daemon_status"]["daemon_version"],
        serde_json::json!(env!("CARGO_PKG_VERSION"))
    );
    let addr = startup_json["addr"].as_str().unwrap();
    assert!(
        startup_json["daemon_status"]["default_requests"]["search"]["client_cli"]
            .as_str()
            .unwrap()
            .contains(&format!(
                "orient client-jsonl --require-version --addr {addr}"
            )),
        "{startup_json}"
    );

    let mut stream = TcpStream::connect(addr).unwrap();
    let mut reader = BufReader::new(stream.try_clone().unwrap());
    let request = serde_json::json!({
        "id": "status",
        "tool": "daemon_status",
        "arguments": {}
    });
    writeln!(stream, "{request}").unwrap();
    let mut response = String::new();
    reader.read_line(&mut response).unwrap();
    let search_request = serde_json::json!({
        "id": "search",
        "tool": "search_auto",
        "arguments": {
            "repo": ".",
            "query": "client_cli",
            "limit": 2
        }
    });
    writeln!(stream, "{search_request}").unwrap();
    let mut search_response = String::new();
    reader.read_line(&mut search_response).unwrap();

    child.kill().unwrap();
    let _ = child.wait();

    assert!(response.contains("\"id\":\"status\""));
    assert!(response.contains("\"cached_indexes\":0"));
    let search_response: serde_json::Value = serde_json::from_str(&search_response).unwrap();
    assert_eq!(search_response["id"], serde_json::json!("search"));
    assert!(
        search_response["result"]["read_batch_request"]["client_cli"]
            .as_str()
            .unwrap()
            .contains(&format!(
                "| orient client-jsonl --require-version --addr {addr}"
            )),
        "{search_response}"
    );
}

#[test]
fn tcp_daemon_status_cli_reports_runtime_cache() {
    let binary = assert_cmd::cargo::cargo_bin("orient");
    let mut child = Command::new(&binary)
        .args(["serve-tcp", "--addr", "127.0.0.1:0"])
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    let stdout = child.stdout.take().unwrap();
    let mut startup_reader = BufReader::new(stdout);
    let mut startup = String::new();
    startup_reader.read_line(&mut startup).unwrap();
    let startup_json: serde_json::Value = serde_json::from_str(&startup).unwrap();
    let addr = startup_json["addr"].as_str().unwrap();

    let output = Command::new(&binary)
        .args(["daemon-status", "--addr", addr])
        .output()
        .unwrap();
    let full_output = Command::new(&binary)
        .args(["daemon-status", "--addr", addr, "--format", "json"])
        .output()
        .unwrap();

    child.kill().unwrap();
    let _ = child.wait();

    assert!(output.status.success(), "{output:?}");
    let status: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(
        status["daemon_version"],
        serde_json::json!(env!("CARGO_PKG_VERSION"))
    );
    assert_eq!(
        status["client_version"],
        serde_json::json!(env!("CARGO_PKG_VERSION"))
    );
    assert!(status["process_id"].as_u64().unwrap() > 0);
    assert!(status["started_at_unix_secs"].as_u64().unwrap() > 0);
    assert!(status["uptime_secs"].as_u64().unwrap() < 60);
    assert_eq!(
        status["max_shard_workers"],
        serde_json::json!(DEFAULT_MAX_SHARD_WORKERS)
    );
    assert_eq!(status["cached_indexes"], serde_json::json!(0));
    assert_eq!(status["cached_shard_manifests"], serde_json::json!(0));
    assert_eq!(
        status["search_auto_default"]["surface"],
        serde_json::json!("fallback")
    );
    assert_eq!(
        status["search_auto_default"]["source"],
        serde_json::json!("process_current_dir")
    );
    assert_eq!(
        status["search_auto_default"]["target_present"],
        serde_json::json!(true)
    );
    assert_eq!(status["details_omitted"], serde_json::json!(true));
    assert!(status.get("default_requests").is_none(), "{status}");
    assert!(status.get("cached_index_details").is_none(), "{status}");
    assert!(
        status.get("cached_shard_manifest_details").is_none(),
        "{status}"
    );

    assert!(full_output.status.success(), "{full_output:?}");
    let status: serde_json::Value = serde_json::from_slice(&full_output.stdout).unwrap();
    assert_eq!(
        status["daemon_version"],
        serde_json::json!(env!("CARGO_PKG_VERSION"))
    );
    assert!(status["process_id"].as_u64().unwrap() > 0);
    assert!(status["started_at_unix_secs"].as_u64().unwrap() > 0);
    assert!(status["uptime_secs"].as_u64().unwrap() < 60);
    assert_eq!(
        status["max_shard_workers"],
        serde_json::json!(DEFAULT_MAX_SHARD_WORKERS)
    );
    assert!(status["search_auto_default"]["target"].as_str().is_some());
    let default_target = status["search_auto_default"]["target"].clone();
    assert!(default_target.as_str().is_some());
    assert_eq!(
        status["default_requests"]["repo_map"]["tool"],
        serde_json::json!("repo_map")
    );
    assert_eq!(
        status["default_requests"]["repo_map"]["arguments"]["repo"],
        default_target
    );
    assert_eq!(
        status["default_requests"]["search"]["tool"],
        serde_json::json!("search_auto")
    );
    assert_eq!(
        status["default_requests"]["query_plan"]["tool"],
        serde_json::json!("search_query_plan")
    );
    let search_jsonl: serde_json::Value = serde_json::from_str(
        status["default_requests"]["search"]["jsonl"]
            .as_str()
            .unwrap(),
    )
    .unwrap();
    assert_eq!(search_jsonl["tool"], serde_json::json!("search_auto"));
    assert_eq!(
        search_jsonl["arguments"]["query"],
        serde_json::json!("symbol:SessionManager token")
    );
    assert!(
        status["default_requests"]["search"]["client_cli"]
            .as_str()
            .unwrap()
            .contains(&format!(
                "| orient client-jsonl --require-version --addr {addr}"
            ))
    );
    assert!(status.get("id").is_none(), "{status}");
}

#[test]
fn cli_search_auto_uses_warm_daemon_before_live_fallback() {
    let repo = tempfile::tempdir().unwrap();
    write(
        &repo.path().join("src/auth.rs"),
        "pub fn issue_token() -> &'static str { \"ok\" }\n",
    );
    let index_path = repo.path().join("orient.index");
    FastIndex::build(repo.path())
        .unwrap()
        .save(&index_path)
        .unwrap();

    let binary = assert_cmd::cargo::cargo_bin("orient");
    let mut child = Command::new(&binary)
        .args([
            "serve-tcp",
            "--addr",
            "127.0.0.1:0",
            "--index",
            index_path.to_str().unwrap(),
        ])
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    let stdout = child.stdout.take().unwrap();
    let mut startup_reader = BufReader::new(stdout);
    let mut startup = String::new();
    startup_reader.read_line(&mut startup).unwrap();
    let startup_json: serde_json::Value = serde_json::from_str(&startup).unwrap();
    let addr = startup_json["addr"].as_str().unwrap();

    let empty_cwd = tempfile::tempdir().unwrap();
    let output = Command::new(&binary)
        .current_dir(empty_cwd.path())
        .args(["search-auto", "--daemon-addr", addr, "issue_token"])
        .output()
        .unwrap();
    let batch_output = Command::new(&binary)
        .current_dir(empty_cwd.path())
        .args([
            "search-auto-batch",
            "--daemon-addr",
            addr,
            "issue_token",
            "missing",
        ])
        .output()
        .unwrap();
    let local_output = Command::new(&binary)
        .current_dir(empty_cwd.path())
        .args([
            "search-auto",
            "--daemon-addr",
            addr,
            "--no-daemon",
            "issue_token",
        ])
        .output()
        .unwrap();

    child.kill().unwrap();
    let _ = child.wait();

    assert!(output.status.success(), "{output:?}");
    let value: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(value["surface"], serde_json::json!("indexed"));
    assert!(
        value["target"].as_str().unwrap().ends_with("/orient.index"),
        "{value}"
    );
    assert!(
        serde_json::to_string(&value)
            .unwrap()
            .contains("src/auth.rs")
    );
    assert!(
        value["read_batch_request"]["client_cli"]
            .as_str()
            .unwrap()
            .contains(&format!(
                "| orient client-jsonl --require-version --addr {addr}"
            )),
        "{value}"
    );

    assert!(batch_output.status.success(), "{batch_output:?}");
    let batch: serde_json::Value = serde_json::from_slice(&batch_output.stdout).unwrap();
    assert_eq!(batch[0]["surface"], serde_json::json!("indexed"));
    assert!(
        batch[0]["target"]
            .as_str()
            .unwrap()
            .ends_with("/orient.index"),
        "{batch}"
    );

    assert!(local_output.status.success(), "{local_output:?}");
    let local: serde_json::Value = serde_json::from_slice(&local_output.stdout).unwrap();
    assert_eq!(local["surface"], serde_json::json!("fallback"));
    assert!(local["results"].as_array().unwrap().is_empty(), "{local}");
}

#[test]
fn cli_search_auto_scopes_warm_shards_to_current_git_repo() {
    let workspace = tempfile::tempdir().unwrap();
    let current_repo = workspace.path().join("current-app");
    let other_repo = workspace.path().join("other-app");
    fs::create_dir_all(current_repo.join(".git")).unwrap();
    fs::create_dir_all(other_repo.join(".git")).unwrap();
    write(
        &current_repo.join("src/lib.rs"),
        "pub fn shared_lookup_token() -> &'static str { \"current\" }\n",
    );
    write(
        &other_repo.join("src/lib.rs"),
        "pub fn shared_lookup_token() -> &'static str { \"other\" }\n",
    );
    let shard_dir = workspace.path().join("shards");
    build_shards(
        &[PathBuf::from(&current_repo), PathBuf::from(&other_repo)],
        &shard_dir,
    )
    .unwrap();

    let binary = assert_cmd::cargo::cargo_bin("orient");
    let mut child = Command::new(&binary)
        .args([
            "serve-tcp",
            "--addr",
            "127.0.0.1:0",
            "--index-dir",
            shard_dir.to_str().unwrap(),
        ])
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    let stdout = child.stdout.take().unwrap();
    let mut startup_reader = BufReader::new(stdout);
    let mut startup = String::new();
    startup_reader.read_line(&mut startup).unwrap();
    let startup_json: serde_json::Value = serde_json::from_str(&startup).unwrap();
    let addr = startup_json["addr"].as_str().unwrap();

    let output = Command::new(&binary)
        .current_dir(current_repo.join("src"))
        .args(["search-auto", "--daemon-addr", addr, "shared_lookup_token"])
        .output()
        .unwrap();
    let batch_output = Command::new(&binary)
        .current_dir(current_repo.join("src"))
        .args([
            "search-auto-batch",
            "--daemon-addr",
            addr,
            "shared_lookup_token",
        ])
        .output()
        .unwrap();

    child.kill().unwrap();
    let _ = child.wait();

    assert!(output.status.success(), "{output:?}");
    let value: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(value["surface"], serde_json::json!("shards"));
    assert_eq!(
        value["query_plan_request"]["arguments"]["repo_filter"],
        serde_json::json!(current_repo.canonicalize().unwrap().to_string_lossy())
    );
    let serialized = serde_json::to_string(&value).unwrap();
    assert!(serialized.contains("current-app/src/lib.rs"), "{value}");
    assert!(!serialized.contains("other-app/src/lib.rs"), "{value}");

    assert!(batch_output.status.success(), "{batch_output:?}");
    let batch: serde_json::Value = serde_json::from_slice(&batch_output.stdout).unwrap();
    assert_eq!(batch[0]["surface"], serde_json::json!("shards"));
    assert_eq!(
        batch[0]["query_plan_request"]["arguments"]["repo_filter"],
        serde_json::json!(current_repo.canonicalize().unwrap().to_string_lossy())
    );
    let batch_serialized = serde_json::to_string(&batch).unwrap();
    assert!(batch_serialized.contains("current-app/src/lib.rs"));
    assert!(!batch_serialized.contains("other-app/src/lib.rs"));
}

#[test]
fn client_jsonl_search_auto_falls_back_when_cwd_is_not_in_registered_shards() {
    let workspace = tempfile::tempdir().unwrap();
    let current_repo = workspace.path().join("current-app");
    let other_repo = workspace.path().join("other-app");
    fs::create_dir_all(current_repo.join(".git")).unwrap();
    fs::create_dir_all(other_repo.join(".git")).unwrap();
    write(
        &current_repo.join("src/lib.rs"),
        "pub fn local_only_client_jsonl_token() -> &'static str { \"current\" }\n",
    );
    write(
        &other_repo.join("src/lib.rs"),
        "pub fn local_only_client_jsonl_token() -> &'static str { \"other\" }\n",
    );
    let shard_dir = workspace.path().join("shards");
    build_shards(&[PathBuf::from(&other_repo)], &shard_dir).unwrap();

    let binary = assert_cmd::cargo::cargo_bin("orient");
    let mut child = Command::new(&binary)
        .args([
            "serve-tcp",
            "--addr",
            "127.0.0.1:0",
            "--index-dir",
            shard_dir.to_str().unwrap(),
        ])
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    let stdout = child.stdout.take().unwrap();
    let mut startup_reader = BufReader::new(stdout);
    let mut startup = String::new();
    startup_reader.read_line(&mut startup).unwrap();
    let startup_json: serde_json::Value = serde_json::from_str(&startup).unwrap();
    let addr = startup_json["addr"].as_str().unwrap();

    let mut client = Command::new(&binary)
        .current_dir(current_repo.join("src"))
        .args(["client-jsonl", "--addr", addr])
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .spawn()
        .unwrap();
    let request = serde_json::json!({
        "id": "search",
        "tool": "search_auto",
        "arguments": {
            "query": "local_only_client_jsonl_token",
            "limit": 5
        }
    });
    writeln!(client.stdin.as_mut().unwrap(), "{request}").unwrap();
    drop(client.stdin.take());
    let output = client.wait_with_output().unwrap();

    child.kill().unwrap();
    let _ = child.wait();

    assert!(output.status.success(), "{output:?}");
    let response: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(response["id"], serde_json::json!("search"));
    let result = &response["result"];
    assert_eq!(result["surface"], serde_json::json!("fallback"));
    assert_eq!(
        result["target"],
        serde_json::json!(current_repo.canonicalize().unwrap().to_string_lossy())
    );
    let serialized = serde_json::to_string(result).unwrap();
    assert!(serialized.contains("\"path\":\"src/lib.rs\""), "{result}");
    assert!(
        result["results"][0]["snippet"]
            .as_str()
            .unwrap()
            .contains("current"),
        "{result}"
    );
    assert!(!serialized.contains("other-app/src/lib.rs"), "{result}");
}

#[test]
fn tcp_client_uses_default_addr_when_omitted() {
    let binary = assert_cmd::cargo::cargo_bin("orient");
    let mut child = Command::new(&binary)
        .args(["serve-tcp"])
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    let stdout = child.stdout.take().unwrap();
    let mut startup_reader = BufReader::new(stdout);
    let mut startup = String::new();
    startup_reader.read_line(&mut startup).unwrap();
    let owns_daemon = if startup.trim().is_empty() {
        let mut stderr = String::new();
        child
            .stderr
            .take()
            .unwrap()
            .read_to_string(&mut stderr)
            .unwrap();
        let _ = child.wait();
        assert!(
            stderr.contains("Address already in use"),
            "serve-tcp produced no startup JSON: {stderr}"
        );
        false
    } else {
        let startup_json: serde_json::Value = serde_json::from_str(&startup).unwrap();
        assert_eq!(startup_json["addr"], serde_json::json!("127.0.0.1:8796"));
        true
    };

    let mut client = Command::new(binary)
        .arg("client-jsonl")
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .spawn()
        .unwrap();
    let request = serde_json::json!({
        "id": "status",
        "tool": "daemon_status",
        "arguments": {}
    });
    writeln!(client.stdin.as_mut().unwrap(), "{request}").unwrap();
    drop(client.stdin.take());
    let output = client.wait_with_output().unwrap();

    if owns_daemon {
        child.kill().unwrap();
        let _ = child.wait();
    }

    assert!(output.status.success());
    let stdout = String::from_utf8(output.stdout).unwrap();
    assert!(stdout.contains("\"id\":\"status\""), "{stdout}");
    assert!(stdout.contains("\"default_requests\""), "{stdout}");
}

#[test]
fn tcp_client_require_version_streams_against_matching_daemon() {
    let binary = assert_cmd::cargo::cargo_bin("orient");
    let mut child = Command::new(&binary)
        .args(["serve-tcp", "--addr", "127.0.0.1:0"])
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    let stdout = child.stdout.take().unwrap();
    let mut startup_reader = BufReader::new(stdout);
    let mut startup = String::new();
    startup_reader.read_line(&mut startup).unwrap();
    let startup_json: serde_json::Value = serde_json::from_str(&startup).unwrap();
    let addr = startup_json["addr"].as_str().unwrap();

    let mut client = Command::new(&binary)
        .args(["client-jsonl", "--require-version", "--addr", addr])
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    let request = serde_json::json!({
        "id": "status",
        "tool": "daemon_status",
        "arguments": {}
    });
    writeln!(client.stdin.as_mut().unwrap(), "{request}").unwrap();
    drop(client.stdin.take());
    let output = client.wait_with_output().unwrap();

    child.kill().unwrap();
    let _ = child.wait();

    assert!(output.status.success(), "{output:?}");
    assert!(output.stderr.is_empty(), "{output:?}");
    let response: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(response["id"], serde_json::json!("status"));
    assert_eq!(
        response["result"]["daemon_version"],
        serde_json::json!(env!("CARGO_PKG_VERSION"))
    );
}

#[cfg(unix)]
#[test]
fn unix_daemon_serves_json_lines_requests() {
    let socket_dir = tempfile::tempdir().unwrap();
    let socket = socket_dir.path().join("orient.sock");
    let binary = assert_cmd::cargo::cargo_bin("orient");
    let mut child = Command::new(binary)
        .args(["serve-unix", "--socket", socket.to_str().unwrap()])
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    let stdout = child.stdout.take().unwrap();
    let mut startup_reader = BufReader::new(stdout);
    let mut startup = String::new();
    startup_reader.read_line(&mut startup).unwrap();
    let startup_json: serde_json::Value = serde_json::from_str(&startup).unwrap();
    assert_eq!(startup_json["transport"], serde_json::json!("unix"));
    assert_eq!(startup_json["socket"], serde_json::json!(socket));

    let mut stream = UnixStream::connect(&socket).unwrap();
    let mut reader = BufReader::new(stream.try_clone().unwrap());
    let request = serde_json::json!({
        "id": "status",
        "tool": "daemon_status",
        "arguments": {}
    });
    writeln!(stream, "{request}").unwrap();
    let mut response = String::new();
    reader.read_line(&mut response).unwrap();
    let search_request = serde_json::json!({
        "id": "search",
        "tool": "search_auto",
        "arguments": {
            "repo": ".",
            "query": "client_cli",
            "limit": 2
        }
    });
    writeln!(stream, "{search_request}").unwrap();
    let mut search_response = String::new();
    reader.read_line(&mut search_response).unwrap();

    child.kill().unwrap();
    let _ = child.wait();

    assert!(response.contains("\"id\":\"status\""));
    assert!(response.contains("\"cached_indexes\":0"));
    let search_response: serde_json::Value = serde_json::from_str(&search_response).unwrap();
    assert_eq!(search_response["id"], serde_json::json!("search"));
    assert!(
        search_response["result"]["read_batch_request"]["client_cli"]
            .as_str()
            .unwrap()
            .contains(&format!(
                "| orient client-jsonl --require-version --socket {}",
                socket.to_str().unwrap()
            )),
        "{search_response}"
    );
}

#[cfg(unix)]
#[test]
fn unix_client_forwards_json_lines_requests() {
    let socket_dir = tempfile::tempdir().unwrap();
    let socket = socket_dir.path().join("orient.sock");
    let binary = assert_cmd::cargo::cargo_bin("orient");
    let mut child = Command::new(&binary)
        .args(["serve-unix", "--socket", socket.to_str().unwrap()])
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    let stdout = child.stdout.take().unwrap();
    let mut startup_reader = BufReader::new(stdout);
    let mut startup = String::new();
    startup_reader.read_line(&mut startup).unwrap();

    let mut client = Command::new(binary)
        .args(["client-jsonl", "--socket", socket.to_str().unwrap()])
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .spawn()
        .unwrap();
    let request = serde_json::json!({
        "id": "status",
        "tool": "daemon_status",
        "arguments": {}
    });
    writeln!(client.stdin.as_mut().unwrap(), "{request}").unwrap();
    drop(client.stdin.take());
    let output = client.wait_with_output().unwrap();

    child.kill().unwrap();
    let _ = child.wait();

    assert!(output.status.success());
    let stdout = String::from_utf8(output.stdout).unwrap();
    assert!(stdout.contains("\"id\":\"status\""), "{stdout}");
    assert!(stdout.contains("\"cached_indexes\":0"), "{stdout}");
}

#[cfg(unix)]
#[test]
fn unix_daemon_status_cli_reports_runtime_cache() {
    let socket_dir = tempfile::tempdir().unwrap();
    let socket = socket_dir.path().join("orient.sock");
    let binary = assert_cmd::cargo::cargo_bin("orient");
    let mut child = Command::new(&binary)
        .args(["serve-unix", "--socket", socket.to_str().unwrap()])
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    let stdout = child.stdout.take().unwrap();
    let mut startup_reader = BufReader::new(stdout);
    let mut startup = String::new();
    startup_reader.read_line(&mut startup).unwrap();
    let startup_json: serde_json::Value = serde_json::from_str(&startup).unwrap();
    assert_eq!(
        startup_json["daemon_version"],
        serde_json::json!(env!("CARGO_PKG_VERSION"))
    );
    assert_eq!(
        startup_json["daemon_status"]["daemon_version"],
        serde_json::json!(env!("CARGO_PKG_VERSION"))
    );

    let output = Command::new(&binary)
        .args(["daemon-status", "--socket", socket.to_str().unwrap()])
        .output()
        .unwrap();
    let full_output = Command::new(&binary)
        .args([
            "daemon-status",
            "--socket",
            socket.to_str().unwrap(),
            "--format",
            "json",
        ])
        .output()
        .unwrap();

    child.kill().unwrap();
    let _ = child.wait();

    assert!(output.status.success(), "{output:?}");
    let status: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(
        status["daemon_version"],
        serde_json::json!(env!("CARGO_PKG_VERSION"))
    );
    assert_eq!(
        status["client_version"],
        serde_json::json!(env!("CARGO_PKG_VERSION"))
    );
    assert_eq!(
        status["max_shard_workers"],
        serde_json::json!(DEFAULT_MAX_SHARD_WORKERS)
    );
    assert_eq!(status["cached_indexes"], serde_json::json!(0));
    assert_eq!(status["cached_shard_manifests"], serde_json::json!(0));
    assert_eq!(status["details_omitted"], serde_json::json!(true));
    assert!(status.get("default_requests").is_none(), "{status}");

    assert!(full_output.status.success(), "{full_output:?}");
    let status: serde_json::Value = serde_json::from_slice(&full_output.stdout).unwrap();
    assert_eq!(
        status["daemon_version"],
        serde_json::json!(env!("CARGO_PKG_VERSION"))
    );
    assert!(
        status["default_requests"]["search"]["client_cli"]
            .as_str()
            .unwrap()
            .contains(&format!(
                "| orient client-jsonl --require-version --socket {}",
                socket.to_str().unwrap()
            )),
        "{status}"
    );
}

#[cfg(unix)]
#[test]
fn unix_daemon_refuses_to_replace_non_socket_path() {
    let socket_dir = tempfile::tempdir().unwrap();
    let socket = socket_dir.path().join("orient.sock");
    fs::write(&socket, "not a socket").unwrap();
    let binary = assert_cmd::cargo::cargo_bin("orient");
    let output = Command::new(binary)
        .args(["serve-unix", "--socket", socket.to_str().unwrap()])
        .output()
        .unwrap();

    assert!(!output.status.success());
    let stderr = String::from_utf8(output.stderr).unwrap();
    assert!(
        stderr.contains("refusing to remove non-socket path"),
        "{stderr}"
    );
    assert_eq!(fs::read_to_string(&socket).unwrap(), "not a socket");
}

#[cfg(unix)]
#[test]
fn unix_daemon_refuses_to_replace_active_socket() {
    let socket_dir = tempfile::tempdir().unwrap();
    let socket = socket_dir.path().join("orient.sock");
    let binary = assert_cmd::cargo::cargo_bin("orient");
    let mut child = Command::new(&binary)
        .args(["serve-unix", "--socket", socket.to_str().unwrap()])
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    let stdout = child.stdout.take().unwrap();
    let mut startup_reader = BufReader::new(stdout);
    let mut startup = String::new();
    startup_reader.read_line(&mut startup).unwrap();

    let output = Command::new(&binary)
        .args(["serve-unix", "--socket", socket.to_str().unwrap()])
        .output()
        .unwrap();

    assert!(!output.status.success());
    let stderr = String::from_utf8(output.stderr).unwrap();
    assert!(
        stderr.contains("refusing to replace active unix socket"),
        "{stderr}"
    );

    let mut stream = UnixStream::connect(&socket).unwrap();
    let mut reader = BufReader::new(stream.try_clone().unwrap());
    let request = serde_json::json!({
        "id": "status",
        "tool": "daemon_status",
        "arguments": {}
    });
    writeln!(stream, "{request}").unwrap();
    let mut response = String::new();
    reader.read_line(&mut response).unwrap();

    child.kill().unwrap();
    let _ = child.wait();

    assert!(response.contains("\"id\":\"status\""), "{response}");
}

#[test]
fn tcp_daemon_starts_with_warmed_index() {
    let repo = tempfile::tempdir().unwrap();
    write(
        &repo.path().join("src/auth.rs"),
        "pub struct SessionManager;\npub fn issue_token() {}\n",
    );
    write(
        &repo.path().join("Cargo.toml"),
        "[package]\nname='sample'\nversion='0.1.0'\nedition='2024'\n",
    );
    let index_path = repo.path().join(".orient/index");
    FastIndex::build(repo.path())
        .unwrap()
        .save(&index_path)
        .unwrap();

    let binary = assert_cmd::cargo::cargo_bin("orient");
    let mut child = Command::new(binary)
        .args([
            "serve-tcp",
            "--addr",
            "127.0.0.1:0",
            "--index",
            index_path.to_str().unwrap(),
        ])
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    let stdout = child.stdout.take().unwrap();
    let mut startup_reader = BufReader::new(stdout);
    let mut startup = String::new();
    startup_reader.read_line(&mut startup).unwrap();
    let startup_json: serde_json::Value = serde_json::from_str(&startup).unwrap();
    let addr = startup_json["addr"].as_str().unwrap();
    assert_eq!(startup_json["cached_indexes"], serde_json::json!(1));
    assert_eq!(
        startup_json["daemon_status"]["cached_indexes"],
        serde_json::json!(1)
    );
    assert_eq!(
        startup_json["daemon_status"]["cached_shard_manifests"],
        serde_json::json!(0)
    );
    assert_eq!(
        startup_json["daemon_status"]["details_omitted"],
        serde_json::json!(true)
    );
    assert!(
        startup_json["daemon_status"]
            .get("cached_index_details")
            .is_none(),
        "{startup_json}"
    );
    assert_eq!(
        startup_json["daemon_status"]["search_auto_default"]["surface"],
        serde_json::json!("indexed")
    );
    assert_eq!(
        startup_json["daemon_status"]["search_auto_default"]["source"],
        serde_json::json!("single_warmed_index")
    );
    assert_eq!(
        startup_json["daemon_status"]["default_requests"]["repo_map"]["tool"],
        serde_json::json!("indexed_repo_map")
    );
    assert_eq!(
        startup_json["daemon_status"]["default_requests"]["repo_map"]["arguments"]["index"],
        serde_json::json!(index_path.canonicalize().unwrap().to_string_lossy())
    );
    assert_eq!(
        startup_json["daemon_status"]["default_requests"]["query_plan"]["tool"],
        serde_json::json!("indexed_query_plan")
    );
    let map_jsonl: serde_json::Value = serde_json::from_str(
        startup_json["daemon_status"]["default_requests"]["repo_map"]["jsonl"]
            .as_str()
            .unwrap(),
    )
    .unwrap();
    assert_eq!(map_jsonl["tool"], serde_json::json!("indexed_repo_map"));
    assert_eq!(
        map_jsonl["arguments"]["index"],
        serde_json::json!(index_path.canonicalize().unwrap().to_string_lossy())
    );

    let mut stream = TcpStream::connect(addr).unwrap();
    let mut reader = BufReader::new(stream.try_clone().unwrap());
    let request = serde_json::json!({
        "id": "search",
        "tool": "indexed_search_code",
        "arguments": {
            "query": "issue token",
            "limit": 3,
            "require_all": true
        }
    });
    writeln!(stream, "{request}").unwrap();
    let mut response = String::new();
    reader.read_line(&mut response).unwrap();
    let plan_response = tcp_tool_request(
        addr,
        serde_json::json!({
            "id": "plan",
            "tool": "indexed_query_plan",
            "arguments": {
                "query": "SessionManager missingterm",
                "require_all": true
            }
        }),
    );
    let read_response = tcp_tool_request(
        addr,
        serde_json::json!({
            "id": "read",
            "tool": "read_index_range",
            "arguments": {
                "path": "src/auth.rs",
                "start": 1,
                "lines": 2
            }
        }),
    );

    child.kill().unwrap();
    let _ = child.wait();

    assert!(response.contains("\"id\":\"search\""));
    assert!(response.contains("src/auth.rs"));
    assert!(plan_response.contains("\"id\":\"plan\""));
    assert!(plan_response.contains("\"missing_terms\""));
    assert!(plan_response.contains("missingterm"));
    assert!(read_response.contains("\"id\":\"read\""));
    assert!(read_response.contains("SessionManager"));
}

#[test]
fn tcp_daemon_can_ensure_and_register_shards_on_startup() {
    let repo = tempfile::tempdir().unwrap();
    write(
        &repo.path().join("src/auth.rs"),
        "pub struct SessionManager;\npub fn issue_token() {}\n",
    );
    write(
        &repo.path().join("Cargo.toml"),
        "[package]\nname='sample'\nversion='0.1.0'\nedition='2024'\n",
    );
    let shard_dir = tempfile::tempdir().unwrap();

    let binary = assert_cmd::cargo::cargo_bin("orient");
    let mut child = Command::new(binary)
        .args([
            "serve-tcp",
            "--addr",
            "127.0.0.1:0",
            "--ensure-shards-dir",
            shard_dir.path().to_str().unwrap(),
            "--repo",
            repo.path().to_str().unwrap(),
        ])
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    let stdout = child.stdout.take().unwrap();
    let mut startup_reader = BufReader::new(stdout);
    let mut startup = String::new();
    startup_reader.read_line(&mut startup).unwrap();
    let startup_json: serde_json::Value = serde_json::from_str(&startup).unwrap();
    let addr = startup_json["addr"].as_str().unwrap().to_string();

    assert_eq!(startup_json["ensured_shards"][0]["action"], "build");
    assert_eq!(
        startup_json["ensured_shards"][0]["shards"],
        serde_json::json!(1)
    );
    assert_eq!(
        startup_json["daemon_status"]["cached_shard_manifests"],
        serde_json::json!(1)
    );
    assert_eq!(
        startup_json["daemon_status"]["cached_indexes"],
        serde_json::json!(0)
    );
    assert_eq!(
        startup_json["daemon_status"]["details_omitted"],
        serde_json::json!(true)
    );
    assert_eq!(
        startup_json["daemon_status"]["search_auto_default"]["surface"],
        serde_json::json!("shards")
    );
    assert_eq!(
        startup_json["daemon_status"]["search_auto_default"]["source"],
        serde_json::json!("single_registered_shard_dir")
    );

    let response = tcp_tool_request(
        &addr,
        serde_json::json!({
            "id": "search",
            "tool": "search_shards",
            "arguments": {
                "index_dir": shard_dir.path(),
                "query": "issue token",
                "limit": 3,
                "require_all": true
            }
        }),
    );

    child.kill().unwrap();
    let _ = child.wait();

    assert!(response.contains("\"id\":\"search\""));
    assert!(response.contains("auth.rs"), "{response}");
}

#[test]
fn tcp_daemon_serves_parallel_cached_index_requests() {
    let repo = tempfile::tempdir().unwrap();
    write(
        &repo.path().join("src/auth.rs"),
        "pub struct SessionManager;\npub fn issue_token() {}\npub fn rotate_secret() {}\n",
    );
    write(
        &repo.path().join("Cargo.toml"),
        "[package]\nname='sample'\nversion='0.1.0'\nedition='2024'\n",
    );
    let index_path = repo.path().join(".orient/index");
    FastIndex::build(repo.path())
        .unwrap()
        .save(&index_path)
        .unwrap();

    let binary = assert_cmd::cargo::cargo_bin("orient");
    let mut child = Command::new(binary)
        .args([
            "serve-tcp",
            "--addr",
            "127.0.0.1:0",
            "--index",
            index_path.to_str().unwrap(),
        ])
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    let stdout = child.stdout.take().unwrap();
    let mut startup_reader = BufReader::new(stdout);
    let mut startup = String::new();
    startup_reader.read_line(&mut startup).unwrap();
    let startup_json: serde_json::Value = serde_json::from_str(&startup).unwrap();
    let addr = startup_json["addr"].as_str().unwrap().to_string();

    let first_addr = addr.clone();
    let first = thread::spawn(move || {
        tcp_tool_request(
            &first_addr,
            serde_json::json!({
                "id": "first",
                "tool": "indexed_search_code",
                "arguments": {
                    "query": "issue token",
                    "limit": 3,
                    "require_all": true
                }
            }),
        )
    });
    let second = thread::spawn(move || {
        tcp_tool_request(
            &addr,
            serde_json::json!({
                "id": "second",
                "tool": "indexed_search_code",
                "arguments": {
                    "query": "rotate secret",
                    "limit": 3,
                    "require_all": true
                }
            }),
        )
    });

    let first_response = first.join().unwrap();
    let second_response = second.join().unwrap();

    child.kill().unwrap();
    let _ = child.wait();

    assert!(first_response.contains("\"id\":\"first\""));
    assert!(first_response.contains("src/auth.rs"));
    assert!(second_response.contains("\"id\":\"second\""));
    assert!(second_response.contains("src/auth.rs"));
}

#[test]
fn tcp_daemon_starts_with_warm_index_dir() {
    let repo = tempfile::tempdir().unwrap();
    write(
        &repo.path().join("src/billing.rs"),
        "pub fn invoice_total() -> usize { 42 }\n",
    );
    write(
        &repo.path().join("Cargo.toml"),
        "[package]\nname='billing'\nversion='0.1.0'\nedition='2024'\n",
    );
    let shard_read_path = format!(
        "{}/src/billing.rs",
        repo.path().file_name().unwrap().to_string_lossy()
    );
    let shard_dir = tempfile::tempdir().unwrap();
    let runtime = ToolRuntime::default();
    let build = runtime.dispatch(ToolRequest {
        id: serde_json::json!("build"),
        tool: "index_shards".to_string(),
        arguments: serde_json::json!({
            "repos": [repo.path()],
            "output_dir": shard_dir.path()
        }),
    });
    assert!(build.error.is_none(), "{:?}", build.error);

    let binary = assert_cmd::cargo::cargo_bin("orient");
    let mut child = Command::new(binary)
        .args([
            "serve-tcp",
            "--addr",
            "127.0.0.1:0",
            "--warm-index-dir",
            shard_dir.path().to_str().unwrap(),
        ])
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    let stdout = child.stdout.take().unwrap();
    let mut startup_reader = BufReader::new(stdout);
    let mut startup = String::new();
    startup_reader.read_line(&mut startup).unwrap();
    let startup_json: serde_json::Value = serde_json::from_str(&startup).unwrap();
    let addr = startup_json["addr"].as_str().unwrap();
    assert_eq!(startup_json["cached_indexes"], serde_json::json!(1));
    assert_eq!(
        startup_json["daemon_status"]["cached_shard_manifests"],
        serde_json::json!(1)
    );
    assert_eq!(
        startup_json["daemon_status"]["details_omitted"],
        serde_json::json!(true)
    );
    assert!(
        startup_json["daemon_status"]
            .get("cached_shard_manifest_details")
            .is_none(),
        "{startup_json}"
    );
    assert_eq!(
        startup_json["daemon_status"]["search_auto_default"]["surface"],
        serde_json::json!("shards")
    );
    assert_eq!(
        startup_json["daemon_status"]["search_auto_default"]["target"],
        serde_json::json!(shard_dir.path().canonicalize().unwrap().to_string_lossy())
    );
    assert_eq!(
        startup_json["daemon_status"]["default_requests"]["repo_map"]["tool"],
        serde_json::json!("shard_repo_map")
    );
    assert_eq!(
        startup_json["daemon_status"]["default_requests"]["repo_map"]["arguments"]["index_dir"],
        serde_json::json!(shard_dir.path().canonicalize().unwrap().to_string_lossy())
    );
    assert_eq!(
        startup_json["daemon_status"]["default_requests"]["query_plan"]["tool"],
        serde_json::json!("shard_query_plan")
    );
    let plan_jsonl: serde_json::Value = serde_json::from_str(
        startup_json["daemon_status"]["default_requests"]["query_plan"]["jsonl"]
            .as_str()
            .unwrap(),
    )
    .unwrap();
    assert_eq!(plan_jsonl["tool"], serde_json::json!("shard_query_plan"));
    assert_eq!(
        plan_jsonl["arguments"]["index_dir"],
        serde_json::json!(shard_dir.path().canonicalize().unwrap().to_string_lossy())
    );

    let mut stream = TcpStream::connect(addr).unwrap();
    let mut reader = BufReader::new(stream.try_clone().unwrap());
    let request = serde_json::json!({
        "id": "search",
        "tool": "search_shards",
        "arguments": {
            "query": "invoice total",
            "limit": 3,
            "require_all": true
        }
    });
    writeln!(stream, "{request}").unwrap();
    let mut response = String::new();
    reader.read_line(&mut response).unwrap();
    let read_response = tcp_tool_request(
        addr,
        serde_json::json!({
            "id": "read",
            "tool": "read_shard_range",
            "arguments": {
                "path": shard_read_path,
                "start": 1,
                "lines": 1
            }
        }),
    );

    child.kill().unwrap();
    let _ = child.wait();

    assert!(response.contains("\"id\":\"search\""));
    assert!(response.contains("src/billing.rs"));
    assert!(read_response.contains("\"id\":\"read\""));
    assert!(read_response.contains("invoice_total"));
}

#[test]
fn tcp_daemon_registers_shards_without_warming_indexes() {
    let repo = tempfile::tempdir().unwrap();
    write(
        &repo.path().join("src/billing.rs"),
        "pub fn invoice_total() -> usize { 42 }\n",
    );
    write(
        &repo.path().join("Cargo.toml"),
        "[package]\nname='billing'\nversion='0.1.0'\nedition='2024'\n",
    );
    let shard_dir = tempfile::tempdir().unwrap();
    let runtime = ToolRuntime::default();
    let build = runtime.dispatch(ToolRequest {
        id: serde_json::json!("build"),
        tool: "index_shards".to_string(),
        arguments: serde_json::json!({
            "repos": [repo.path()],
            "output_dir": shard_dir.path()
        }),
    });
    assert!(build.error.is_none(), "{:?}", build.error);

    let binary = assert_cmd::cargo::cargo_bin("orient");
    let mut child = Command::new(binary)
        .args([
            "serve-tcp",
            "--addr",
            "127.0.0.1:0",
            "--index-dir",
            shard_dir.path().to_str().unwrap(),
        ])
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    let stdout = child.stdout.take().unwrap();
    let mut startup_reader = BufReader::new(stdout);
    let mut startup = String::new();
    startup_reader.read_line(&mut startup).unwrap();
    let startup_json: serde_json::Value = serde_json::from_str(&startup).unwrap();
    let addr = startup_json["addr"].as_str().unwrap();
    assert_eq!(startup_json["cached_indexes"], serde_json::json!(0));
    assert_eq!(
        startup_json["daemon_status"]["cached_shard_manifests"],
        serde_json::json!(1)
    );

    let response = tcp_tool_request(
        addr,
        serde_json::json!({
            "id": "search",
            "tool": "search_shards",
            "arguments": {
                "query": "invoice total",
                "limit": 3,
                "require_all": true
            }
        }),
    );
    let status = tcp_tool_request(
        addr,
        serde_json::json!({
            "id": "status",
            "tool": "daemon_status",
            "arguments": {"details": true}
        }),
    );

    child.kill().unwrap();
    let _ = child.wait();

    assert!(response.contains("\"id\":\"search\""));
    assert!(response.contains("src/billing.rs"));
    assert!(
        status.contains("\"cached_indexes\":1"),
        "search should lazily load only the touched shard: {status}"
    );
}

#[test]
fn tcp_daemon_honors_max_cached_indexes_for_lazy_shards() {
    let workspace = tempfile::tempdir().unwrap();
    let auth_repo = workspace.path().join("auth");
    write(
        &auth_repo.join("src/lib.rs"),
        "pub fn issue_token() -> usize { 1 }\n",
    );
    let billing_repo = workspace.path().join("billing");
    write(
        &billing_repo.join("src/lib.rs"),
        "pub fn invoice_total() -> usize { 42 }\n",
    );
    let shard_dir = tempfile::tempdir().unwrap();
    build_shards(&[auth_repo, billing_repo], shard_dir.path()).unwrap();

    let binary = assert_cmd::cargo::cargo_bin("orient");
    let mut child = Command::new(binary)
        .args([
            "serve-tcp",
            "--addr",
            "127.0.0.1:0",
            "--index-dir",
            shard_dir.path().to_str().unwrap(),
            "--max-cached-indexes",
            "1",
        ])
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    let stdout = child.stdout.take().unwrap();
    let mut startup_reader = BufReader::new(stdout);
    let mut startup = String::new();
    startup_reader.read_line(&mut startup).unwrap();
    let startup_json: serde_json::Value = serde_json::from_str(&startup).unwrap();
    let addr = startup_json["addr"].as_str().unwrap().to_string();
    assert_eq!(startup_json["max_cached_indexes"], serde_json::json!(1));
    assert_eq!(
        startup_json["daemon_status"]["cached_indexes"],
        serde_json::json!(0)
    );

    let first = tcp_tool_request(
        &addr,
        serde_json::json!({
            "id": "first",
            "tool": "search_shards",
            "arguments": {
                "query": "issue token",
                "limit": 3,
                "require_all": true
            }
        }),
    );
    let second = tcp_tool_request(
        &addr,
        serde_json::json!({
            "id": "second",
            "tool": "search_shards",
            "arguments": {
                "query": "invoice total",
                "limit": 3,
                "require_all": true
            }
        }),
    );
    let status = tcp_tool_request(
        &addr,
        serde_json::json!({
            "id": "status",
            "tool": "daemon_status",
            "arguments": {"details": true}
        }),
    );

    child.kill().unwrap();
    let _ = child.wait();

    assert!(first.contains("src/lib.rs"), "{first}");
    assert!(second.contains("src/lib.rs"), "{second}");
    let status_json: serde_json::Value = serde_json::from_str(&status).unwrap();
    assert_eq!(
        status_json["result"]["max_cached_indexes"],
        serde_json::json!(1)
    );
    assert_eq!(
        status_json["result"]["cached_indexes"],
        serde_json::json!(1)
    );
    assert_eq!(
        status_json["result"]["cached_index_details"][0]["root"],
        serde_json::json!(
            workspace
                .path()
                .join("billing")
                .canonicalize()
                .unwrap()
                .to_string_lossy()
        )
    );
}

#[test]
fn server_handles_json_lines_tool_request() {
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
        &repo.path().join("Cargo.toml"),
        "[package]\nname='sample'\nversion='0.1.0'\nedition='2024'\n",
    );

    let binary = assert_cmd::cargo::cargo_bin("orient");
    let mut child = Command::new(binary)
        .arg("serve-jsonl")
        .current_dir(repo.path())
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .spawn()
        .unwrap();

    let request = serde_json::json!({
        "id": 1,
        "tool": "search_code",
        "arguments": {
            "repo": repo.path(),
            "query": "issue token",
            "limit": 3,
            "extension": "rs",
            "require_all": true,
            "explain": true,
            "context_lines": 3
        }
    });
    let auto_request = serde_json::json!({
        "id": 2,
        "tool": "search_auto",
        "arguments": {
            "query": "issue_token",
            "limit": 3
        }
    });
    let copied_read_request = serde_json::json!({
        "id": 3,
        "tool": "read_range",
        "arguments": {
            "repo": repo.path(),
            "path": "src/auth.rs#L2-L2"
        }
    });
    let copied_batch_read_request = serde_json::json!({
        "id": 4,
        "tool": "read_ranges",
        "arguments": {
            "repo": repo.path(),
            "ranges": [
                "src/auth.rs:2: pub fn issue_token",
                "tests/auth_test.rs#L2-L3"
            ]
        }
    });
    writeln!(child.stdin.as_mut().unwrap(), "{request}").unwrap();
    writeln!(child.stdin.as_mut().unwrap(), "{auto_request}").unwrap();
    writeln!(child.stdin.as_mut().unwrap(), "{copied_read_request}").unwrap();
    writeln!(child.stdin.as_mut().unwrap(), "{copied_batch_read_request}").unwrap();
    drop(child.stdin.take());

    let output = child.wait_with_output().unwrap();
    assert!(output.status.success());
    let stdout = String::from_utf8(output.stdout).unwrap();
    assert!(stdout.contains("\"id\":1"));
    assert!(stdout.contains("src/auth.rs"));
    assert!(stdout.contains("\"explanation\""));
    assert!(stdout.contains("\"context\""));
    assert!(stdout.contains("\"read_range\""));
    assert!(stdout.contains("\"lines\":80"));
    assert!(stdout.contains("symbol_exact"));
    assert!(stdout.contains("\"id\":2"));
    assert!(stdout.contains("\"surface\":\"fallback\""));
    assert!(stdout.contains("\"tool\":\"search_query_plan\""));
    assert!(stdout.contains("\"id\":3"));
    assert!(stdout.contains("\"start_line\":2"));
    assert!(stdout.contains("\"end_line\":2"));
    assert!(stdout.contains("\"id\":4"));
    assert!(stdout.contains("\"path\":\"tests/auth_test.rs\""));
    assert!(stdout.contains("\"end_line\":3"));
}

#[test]
fn server_handles_indexed_search_request() {
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
        &repo.path().join("Cargo.toml"),
        "[package]\nname='sample'\nversion='0.1.0'\nedition='2024'\n",
    );
    let index_path = repo.path().join(".orient/index");

    let binary = assert_cmd::cargo::cargo_bin("orient");
    let status = Command::new(&binary)
        .args([
            "index",
            "--repo",
            repo.path().to_str().unwrap(),
            "--output",
            index_path.to_str().unwrap(),
        ])
        .stdout(Stdio::null())
        .status()
        .unwrap();
    assert!(status.success());
    fs::remove_file(repo.path().join("src/auth.rs")).unwrap();

    let mut child = Command::new(binary)
        .arg("serve-jsonl")
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .spawn()
        .unwrap();

    let request = serde_json::json!({
        "id": 2,
        "tool": "indexed_search",
        "arguments": {
            "index": index_path,
            "query": "issue token",
            "limit": 3,
            "language": "rust",
            "require_all": true,
            "context_lines": 2
        }
    });
    let read_request = serde_json::json!({
        "id": "read-index-range",
        "tool": "read_index_range",
        "arguments": {
            "index": index_path,
            "path": "src/auth.rs",
            "start": 1,
            "lines": 2
        }
    });
    let read_ranges_request = serde_json::json!({
        "id": "read-index-ranges",
        "tool": "read_index_ranges",
        "arguments": {
            "index": index_path,
            "ranges": {"path": "src/auth.rs", "start": 1, "lines": 1}
        }
    });
    let open_request = serde_json::json!({
        "id": "open-index-range",
        "tool": "open_index_range",
        "arguments": {
            "index": index_path,
            "path": "src/auth.rs#L2-L2"
        }
    });
    let open_ranges_request = serde_json::json!({
        "id": "open-index-ranges",
        "tool": "open_index_ranges",
        "arguments": {
            "index": index_path,
            "ranges": ["src/auth.rs#L2-L2"]
        }
    });
    let symbol_request = serde_json::json!({
        "id": "find-index-symbol",
        "tool": "find_index_symbol",
        "arguments": {
            "index": index_path,
            "name": "SessionManager",
            "kind": "struct",
            "limit": 5
        }
    });
    let symbol_batch_request = serde_json::json!({
        "id": "find-index-symbol-batch",
        "tool": "find_index_symbol_batch",
        "arguments": {
            "index": index_path,
            "names": ["SessionManager", "issue_token"],
            "kind": "function",
            "limit": 5
        }
    });
    let map_request = serde_json::json!({
        "id": "indexed-repo-map",
        "tool": "indexed_repo_map",
        "arguments": {
            "index": index_path,
            "symbols": 5,
            "tests": 5
        }
    });
    let related_request = serde_json::json!({
        "id": "related-index-files",
        "tool": "related_index_files",
        "arguments": {
            "index": index_path,
            "path": "src/auth.rs",
            "limit": 5
        }
    });
    let related_symbols_request = serde_json::json!({
        "id": "related-index-symbols",
        "tool": "related_index_symbols",
        "arguments": {
            "index": index_path,
            "path": "src/auth.rs",
            "query": "SessionManager",
            "limit": 5
        }
    });
    let plan_request = serde_json::json!({
        "id": "indexed-query-plan",
        "tool": "indexed_query_plan",
        "arguments": {
            "index": index_path,
            "query": "SessionManager definitely_missing",
            "path": "src"
        }
    });
    writeln!(child.stdin.as_mut().unwrap(), "{request}").unwrap();
    writeln!(child.stdin.as_mut().unwrap(), "{read_request}").unwrap();
    writeln!(child.stdin.as_mut().unwrap(), "{read_ranges_request}").unwrap();
    writeln!(child.stdin.as_mut().unwrap(), "{open_request}").unwrap();
    writeln!(child.stdin.as_mut().unwrap(), "{open_ranges_request}").unwrap();
    writeln!(child.stdin.as_mut().unwrap(), "{symbol_request}").unwrap();
    writeln!(child.stdin.as_mut().unwrap(), "{symbol_batch_request}").unwrap();
    writeln!(child.stdin.as_mut().unwrap(), "{map_request}").unwrap();
    writeln!(child.stdin.as_mut().unwrap(), "{related_request}").unwrap();
    writeln!(child.stdin.as_mut().unwrap(), "{related_symbols_request}").unwrap();
    writeln!(child.stdin.as_mut().unwrap(), "{plan_request}").unwrap();
    drop(child.stdin.take());

    let output = child.wait_with_output().unwrap();
    assert!(output.status.success());
    let stdout = String::from_utf8(output.stdout).unwrap();
    assert!(stdout.contains("\"id\":2"));
    assert!(stdout.contains("src/auth.rs"));
    assert!(stdout.contains("\"match_lines\""));
    assert!(stdout.contains("\"read_range\""));
    assert!(stdout.contains("\"read_request\""));
    assert!(stdout.contains("\"tool\":\"read_index_range\""));
    assert!(stdout.contains("\"related_request\""));
    assert!(stdout.contains("\"tool\":\"related_index_files\""));
    assert!(stdout.contains("\"related_symbols_request\""));
    assert!(stdout.contains("\"tool\":\"related_index_symbols\""));
    assert!(stdout.contains("\"context\""));
    assert!(stdout.contains("\"id\":\"read-index-range\""));
    assert!(stdout.contains("\"path\":\"src/auth.rs\""));
    assert!(stdout.contains("issue_token"));
    assert!(stdout.contains("\"id\":\"read-index-ranges\""));
    assert!(stdout.contains("\"path\":\"tests/auth_test.rs\""));
    assert!(stdout.contains("\"id\":\"open-index-range\""));
    assert!(stdout.contains("\"start_line\":2"));
    assert!(stdout.contains("\"id\":\"open-index-ranges\""));
    assert!(stdout.contains("\"id\":\"find-index-symbol\""));
    assert!(stdout.contains("\"kind\":\"struct\""));
    assert!(stdout.contains("\"read_request\""));
    assert!(stdout.contains("\"tool\":\"read_index_range\""));
    assert!(stdout.contains("\"id\":\"find-index-symbol-batch\""));
    assert!(stdout.contains("\"name\":\"SessionManager\""));
    assert!(stdout.contains("\"symbols\":[]"));
    assert!(stdout.contains("\"name\":\"issue_token\""));
    assert!(stdout.contains("\"read_batch_request\""));
    assert!(stdout.contains("\"tool\":\"read_index_ranges\""));
    assert!(stdout.contains("\"id\":\"indexed-repo-map\""));
    assert!(stdout.contains("\"entrypoints\""));
    assert!(stdout.contains("\"manifest_files\""));
    assert!(stdout.contains("\"related_files\""));
    assert!(stdout.contains("\"related_symbols\""));
    assert!(stdout.contains("\"read_batch_request\""));
    assert!(stdout.contains("\"tool\":\"read_index_ranges\""));
    assert!(stdout.contains("tests/auth_test.rs"));
    assert!(stdout.contains("\"id\":\"related-index-files\""));
    assert!(stdout.contains("tests/auth_test.rs"));
    assert!(stdout.contains("\"read_request\""));
    assert!(stdout.contains("\"tool\":\"read_index_range\""));
    assert!(stdout.contains("\"id\":\"related-index-symbols\""));
    assert!(stdout.contains("SessionManager"));
    assert!(stdout.contains("\"id\":\"indexed-query-plan\""));
    assert!(stdout.contains("\"active_filters\""));
    assert!(stdout.contains("\"field\":\"path\""));
    assert!(stdout.contains("\"candidate_rejections\""));
    assert!(stdout.contains("\"missing_terms\""));
    assert!(stdout.contains("definitely"));
    assert!(stdout.contains("missing"));
    assert!(stdout.contains("\"filtered_candidate_count\":0"));
    assert!(stdout.contains("\"final_match_count\":0"));
    assert!(stdout.contains("\"repair_hints\""));
    assert!(stdout.contains("drop_missing_terms"));
}

#[test]
fn indexed_search_result_query_plan_includes_retry_requests() {
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
    let index_path = repo.path().join(".orient/index");
    FastIndex::build(repo.path())
        .unwrap()
        .save(&index_path)
        .unwrap();
    let runtime = ToolRuntime::default();

    let response = runtime.dispatch(ToolRequest {
        id: serde_json::json!("search"),
        tool: "indexed_search_code".to_string(),
        arguments: serde_json::json!({
            "index": index_path,
            "query": "shared cap token",
            "limit": 1,
            "explain": true
        }),
    });
    assert!(response.error.is_none(), "{:?}", response.error);
    let results = response.result.unwrap();
    let plan = &results[0]["query_plan"];
    assert_eq!(plan["candidate_cap_hit"], serde_json::json!(true));
    assert!(
        plan["retry_requests"]
            .as_array()
            .unwrap()
            .iter()
            .any(|request| {
                request["tool"] == serde_json::json!("indexed_search_code")
                    && request["arguments"]["query"]
                        == serde_json::json!("shared cap token path:src")
                    && request["arguments"]["explain"] == serde_json::json!(true)
            }),
        "{plan}"
    );
}

#[test]
fn server_handles_shard_index_search_and_read_requests() {
    let parent = tempfile::tempdir().unwrap();
    let auth_repo = parent.path().join("auth");
    let billing_repo = parent.path().join("billing");
    write(
        &auth_repo.join("src/auth.rs"),
        "pub fn issue_token() -> String { \"token\".to_string() }\n",
    );
    write(
        &auth_repo.join("Cargo.toml"),
        "[package]\nname='auth'\nversion='0.1.0'\nedition='2024'\n",
    );
    write(
        &billing_repo.join("src/billing.rs"),
        "pub fn invoice_total() -> usize { 42 }\n",
    );
    write(
        &billing_repo.join("src/legacy.rs"),
        "pub fn legacy_invoice() -> usize { 1 }\n",
    );
    write(
        &billing_repo.join("tests/billing_test.rs"),
        "use billing::invoice_total;\n#[test]\nfn totals_invoice() {}\n",
    );
    write(
        &billing_repo.join("Cargo.toml"),
        "[package]\nname='billing'\nversion='0.1.0'\nedition='2024'\n",
    );
    let shard_dir = parent.path().join(".orient-shards");

    let binary = assert_cmd::cargo::cargo_bin("orient");
    let mut child = Command::new(binary)
        .arg("serve-jsonl")
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .spawn()
        .unwrap();

    let index_request = serde_json::json!({
        "id": "index-shards",
        "tool": "index_shards",
        "arguments": {
            "repos": [auth_repo, billing_repo],
            "output_dir": shard_dir
        }
    });
    let search_request = serde_json::json!({
        "id": "search-shards",
        "tool": "search_shards",
        "arguments": {
            "index_dir": parent.path().join(".orient-shards"),
            "query": "invoice total",
            "repo": "BILLING",
            "limit": 5,
            "require_all": true,
            "explain": true,
            "context_lines": 2
        }
    });
    let read_request = serde_json::json!({
        "id": "read-shard-range",
        "tool": "read_shard_range",
        "arguments": {
            "index_dir": parent.path().join(".orient-shards"),
            "path": "src/billing.rs",
            "start": 1,
            "lines": 1
        }
    });
    let read_ranges_request = serde_json::json!({
        "id": "read-shard-ranges",
        "tool": "read_shard_ranges",
        "arguments": {
            "index_dir": parent.path().join(".orient-shards"),
            "ranges": {"path": "src/billing.rs", "start": 1, "lines": 1}
        }
    });
    let open_request = serde_json::json!({
        "id": "open-shard-range",
        "tool": "open_shard_range",
        "arguments": {
            "index_dir": parent.path().join(".orient-shards"),
            "path": "src/billing.rs#L1-L1"
        }
    });
    let open_ranges_request = serde_json::json!({
        "id": "open-shard-ranges",
        "tool": "open_shard_ranges",
        "arguments": {
            "index_dir": parent.path().join(".orient-shards"),
            "ranges": ["src/billing.rs#L1-L1"]
        }
    });
    let symbol_request = serde_json::json!({
        "id": "find-shard-symbol",
        "tool": "find_shard_symbol",
        "arguments": {
            "index_dir": parent.path().join(".orient-shards"),
            "name": "invoice total",
            "repo": "BILLING",
            "limit": 5
        }
    });
    let map_request = serde_json::json!({
        "id": "shard-repo-map",
        "tool": "shard_repo_map",
        "arguments": {
            "index_dir": parent.path().join(".orient-shards"),
            "repo": "BILLING",
            "symbols": 5,
            "tests": 5
        }
    });
    let related_request = serde_json::json!({
        "id": "related-shard-files",
        "tool": "related_shard_files",
        "arguments": {
            "index_dir": parent.path().join(".orient-shards"),
            "path": "src/billing.rs",
            "limit": 5
        }
    });
    let related_symbols_request = serde_json::json!({
        "id": "related-shard-symbols",
        "tool": "related_shard_symbols",
        "arguments": {
            "index_dir": parent.path().join(".orient-shards"),
            "path": "src/billing.rs",
            "query": "invoice total",
            "limit": 5
        }
    });
    let plan_request = serde_json::json!({
        "id": "shard-query-plan",
        "tool": "shard_plan",
        "arguments": {
            "index_dir": parent.path().join(".orient-shards"),
            "query": "repo:BILLING invoice missingterm",
            "require_all": true
        }
    });
    writeln!(child.stdin.as_mut().unwrap(), "{index_request}").unwrap();
    writeln!(child.stdin.as_mut().unwrap(), "{search_request}").unwrap();
    writeln!(child.stdin.as_mut().unwrap(), "{symbol_request}").unwrap();
    writeln!(child.stdin.as_mut().unwrap(), "{map_request}").unwrap();
    writeln!(child.stdin.as_mut().unwrap(), "{read_request}").unwrap();
    writeln!(child.stdin.as_mut().unwrap(), "{read_ranges_request}").unwrap();
    writeln!(child.stdin.as_mut().unwrap(), "{open_request}").unwrap();
    writeln!(child.stdin.as_mut().unwrap(), "{open_ranges_request}").unwrap();
    writeln!(child.stdin.as_mut().unwrap(), "{related_request}").unwrap();
    writeln!(child.stdin.as_mut().unwrap(), "{related_symbols_request}").unwrap();
    writeln!(child.stdin.as_mut().unwrap(), "{plan_request}").unwrap();
    drop(child.stdin.take());

    let output = child.wait_with_output().unwrap();
    assert!(output.status.success());
    let stdout = String::from_utf8(output.stdout).unwrap();
    assert!(stdout.contains("\"id\":\"index-shards\""));
    assert!(stdout.contains("\"shards\":2"));
    assert!(stdout.contains("\"id\":\"search-shards\""));
    assert!(stdout.contains("billing/src/billing.rs"));
    assert!(stdout.contains("shard:billing"));
    assert!(stdout.contains("\"read_range\""));
    assert!(stdout.contains("\"lines\":80"));
    assert!(stdout.contains("\"context\""));
    assert!(!stdout.contains("auth/src/auth.rs"));
    assert!(stdout.contains("\"id\":\"find-shard-symbol\""));
    assert!(stdout.contains("\"path\":\"billing/src/billing.rs\""));
    assert!(stdout.contains("\"name\":\"invoice_total\""));
    assert!(stdout.contains("\"id\":\"shard-repo-map\""));
    assert!(stdout.contains("\"entrypoints\":[\"billing/Cargo.toml\"]"));
    assert!(stdout.contains("\"manifest_files\":[\"billing/Cargo.toml\"]"));
    assert!(stdout.contains("\"read_batch_request\""));
    assert!(stdout.contains("\"tool\":\"read_shard_ranges\""));
    assert!(stdout.contains("\"id\":\"read-shard-range\""));
    assert!(stdout.contains("\"path\":\"billing/src/billing.rs\""));
    assert!(stdout.contains("invoice_total"));
    assert!(stdout.contains("\"id\":\"read-shard-ranges\""));
    assert!(stdout.contains("\"path\":\"billing/tests/billing_test.rs\""));
    assert!(stdout.contains("\"id\":\"open-shard-range\""));
    assert!(stdout.contains("\"id\":\"open-shard-ranges\""));
    assert!(stdout.contains("\"id\":\"related-shard-files\""));
    assert!(stdout.contains("billing/tests/billing_test.rs"));
    assert!(stdout.contains("\"read_request\""));
    assert!(stdout.contains("\"tool\":\"read_shard_range\""));
    assert!(stdout.contains("\"id\":\"related-shard-symbols\""));
    assert!(stdout.contains("\"path\":\"billing/src/billing.rs\""));
    assert!(stdout.contains("\"id\":\"shard-query-plan\""));
    assert!(stdout.contains("\"name\":\"billing\""));
    assert!(stdout.contains("\"missing_terms\""));
    assert!(stdout.contains("missingterm"));
    assert!(stdout.contains("\"filtered_candidate_count\":0"));
    assert!(stdout.contains("\"final_match_count\":0"));
    assert!(stdout.contains("\"repair_hints\""));
    assert!(stdout.contains("drop_missing_terms"));
}

#[test]
fn server_handles_shard_refresh_request() {
    let parent = tempfile::tempdir().unwrap();
    let auth_repo = parent.path().join("auth");
    let billing_repo = parent.path().join("billing");
    write(
        &auth_repo.join("src/auth.rs"),
        "pub fn issue_token() -> String { \"token\".to_string() }\n",
    );
    write(
        &auth_repo.join("Cargo.toml"),
        "[package]\nname='auth'\nversion='0.1.0'\nedition='2024'\n",
    );
    write(
        &billing_repo.join("src/billing.rs"),
        "pub fn invoice_total() -> usize { 42 }\n",
    );
    write(
        &billing_repo.join("src/legacy.rs"),
        "pub fn legacy_invoice() -> usize { 1 }\n",
    );
    write(
        &billing_repo.join("Cargo.toml"),
        "[package]\nname='billing'\nversion='0.1.0'\nedition='2024'\n",
    );
    let shard_dir = parent.path().join(".orient-shards");

    let binary = assert_cmd::cargo::cargo_bin("orient");
    let status = Command::new(&binary)
        .args([
            "index-shards",
            "--repo",
            auth_repo.to_str().unwrap(),
            "--repo",
            billing_repo.to_str().unwrap(),
            "--output-dir",
            shard_dir.to_str().unwrap(),
        ])
        .stdout(Stdio::null())
        .status()
        .unwrap();
    assert!(status.success());

    write(
        &billing_repo.join("src/billing.rs"),
        "pub fn invoice_total() -> usize { 42 }\npub fn credit_memo() -> usize { 7 }\n",
    );
    fs::rename(
        billing_repo.join("src/legacy.rs"),
        billing_repo.join("src/refunds.rs"),
    )
    .unwrap();
    write(
        &billing_repo.join("src/refunds.rs"),
        "pub fn refund_credit() -> usize { 1 }\n",
    );

    let mut child = Command::new(binary)
        .arg("serve-jsonl")
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .spawn()
        .unwrap();

    let refresh_request = serde_json::json!({
        "id": "refresh-shards",
        "tool": "refresh_shards",
        "arguments": {
            "index_dir": shard_dir
        }
    });
    let search_request = serde_json::json!({
        "id": "search-after-refresh",
        "tool": "search_shards",
        "arguments": {
            "index_dir": parent.path().join(".orient-shards"),
            "query": "repo:billing credit memo",
            "limit": 5,
            "require_all": true
        }
    });
    writeln!(child.stdin.as_mut().unwrap(), "{refresh_request}").unwrap();
    writeln!(child.stdin.as_mut().unwrap(), "{search_request}").unwrap();
    drop(child.stdin.take());

    let output = child.wait_with_output().unwrap();
    assert!(output.status.success());
    let stdout = String::from_utf8(output.stdout).unwrap();
    assert!(stdout.contains("\"id\":\"refresh-shards\""));
    assert!(stdout.contains("\"reused_files\""));
    assert!(stdout.contains("\"refreshed_files\""));
    assert!(stdout.contains("\"deleted_files\":1"));
    assert!(stdout.contains("\"id\":\"search-after-refresh\""));
    assert!(stdout.contains("credit_memo"));
}

#[test]
fn server_handles_repo_map_and_read_range_requests() {
    let repo = tempfile::tempdir().unwrap();
    write(
        &repo.path().join("src/auth.rs"),
        "pub struct SessionManager;\nimpl SessionManager {\n    pub fn issue_token() {}\n}\n",
    );
    write(
        &repo.path().join("tests/auth_test.rs"),
        "#[test]\nfn issues_tokens() {}\n",
    );
    write(
        &repo.path().join("Cargo.toml"),
        "[package]\nname='sample'\nversion='0.1.0'\nedition='2024'\n",
    );

    let binary = assert_cmd::cargo::cargo_bin("orient");
    let mut child = Command::new(binary)
        .arg("serve-jsonl")
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .spawn()
        .unwrap();

    let map_request = serde_json::json!({
        "id": "map",
        "tool": "repo_map",
        "arguments": {
            "repo": repo.path(),
            "symbols": 5,
            "tests": 5
        }
    });
    let range_request = serde_json::json!({
        "id": "range",
        "tool": "open_range",
        "arguments": {
            "repo": repo.path(),
            "path": "src/auth.rs",
            "start": 2,
            "lines": 2
        }
    });
    let copied_range_request = serde_json::json!({
        "id": "copied-range",
        "tool": "open_range",
        "arguments": {
            "repo": repo.path(),
            "path": "src/auth.rs:2-3"
        }
    });
    let ranges_request = serde_json::json!({
        "id": "ranges",
        "tool": "open_ranges",
        "arguments": {
            "repo": repo.path(),
            "ranges": {"path": "src/auth.rs", "start": 1, "lines": 1}
        }
    });
    let copied_ranges_request = serde_json::json!({
        "id": "copied-ranges",
        "tool": "open_ranges",
        "arguments": {
            "repo": repo.path(),
            "ranges": ["src/auth.rs:2-3"]
        }
    });
    let symbols_request = serde_json::json!({
        "id": "symbols",
        "tool": "related_symbols",
        "arguments": {
            "repo": repo.path(),
            "path": "src/auth.rs",
            "query": "SessionManager",
            "limit": 5
        }
    });
    writeln!(child.stdin.as_mut().unwrap(), "{map_request}").unwrap();
    writeln!(child.stdin.as_mut().unwrap(), "{range_request}").unwrap();
    writeln!(child.stdin.as_mut().unwrap(), "{copied_range_request}").unwrap();
    writeln!(child.stdin.as_mut().unwrap(), "{ranges_request}").unwrap();
    writeln!(child.stdin.as_mut().unwrap(), "{copied_ranges_request}").unwrap();
    writeln!(child.stdin.as_mut().unwrap(), "{symbols_request}").unwrap();
    drop(child.stdin.take());

    let output = child.wait_with_output().unwrap();
    assert!(output.status.success());
    let stdout = String::from_utf8(output.stdout).unwrap();
    assert!(stdout.contains("\"id\":\"map\""));
    assert!(stdout.contains("SessionManager"));
    assert!(stdout.contains("\"command_hints\""));
    assert!(stdout.contains("\"command\":\"cargo test\""));
    assert!(stdout.contains("\"source\":\"Cargo.toml\""));
    assert!(stdout.contains("tests/auth_test.rs"));
    assert!(stdout.contains("\"id\":\"range\""));
    assert!(stdout.contains("\"summary\""));
    assert!(stdout.contains("\"status\":\"read\""));
    assert!(stdout.contains("\"line_count\":2"));
    assert!(stdout.contains("\"start_line\":2"));
    assert!(stdout.contains("\"id\":\"copied-range\""));
    assert!(stdout.contains("\"end_line\":3"));
    assert!(stdout.contains("issue_token"));
    assert!(stdout.contains("\"id\":\"ranges\""));
    assert!(stdout.contains("\"id\":\"copied-ranges\""));
    assert!(stdout.contains("\"path\":\"src/auth.rs\""));
    assert!(stdout.contains("\"path\":\"tests/auth_test.rs\""));
    assert!(stdout.contains("\"id\":\"symbols\""));
    assert!(stdout.contains("same file"));
    assert!(stdout.contains("\"read_request\""));
    assert!(stdout.contains("\"tool\":\"read_range\""));
}
