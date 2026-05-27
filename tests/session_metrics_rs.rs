use std::fs;
use std::path::Path;

use orient::session_metrics::{ActionKind, ScanOptions, scan_jsonl_roots};

fn write(path: &Path, text: &str) {
    fs::create_dir_all(path.parent().unwrap()).unwrap();
    fs::write(path, text).unwrap();
}

#[test]
fn scans_codex_and_claude_tool_calls_into_metrics() {
    let temp = tempfile::tempdir().unwrap();
    write(
        &temp.path().join(".codex/sessions/sample.jsonl"),
        r#"
{"timestamp":"2026-05-27T00:00:00Z","type":"response_item","payload":{"type":"function_call","name":"exec_command","arguments":"{\"cmd\":\"rg auth src\"}","call_id":"c1"}}
{"timestamp":"2026-05-27T00:00:01Z","type":"response_item","payload":{"type":"function_call_output","call_id":"c1","output":"Process exited with code 1\nOutput:\n"}}
{"timestamp":"2026-05-27T00:00:02Z","type":"response_item","payload":{"type":"custom_tool_call","name":"apply_patch","input":"*** Begin Patch","call_id":"c2"}}
"#,
    );
    write(
        &temp.path().join(".claude/projects/sample/session.jsonl"),
        r#"
{"type":"assistant","sessionId":"s1","timestamp":"2026-05-27T00:00:03Z","message":{"role":"assistant","content":[{"type":"tool_use","id":"t1","name":"Read","input":{"file_path":"src/auth.py"}}]}}
{"type":"user","sessionId":"s1","timestamp":"2026-05-27T00:00:04Z","message":{"role":"user","content":[{"type":"tool_result","tool_use_id":"t1","content":"ok"}]}}
"#,
    );

    let metrics = scan_jsonl_roots(ScanOptions {
        roots: vec![temp.path().to_path_buf()],
        max_files: None,
        max_file_bytes: None,
    })
    .unwrap();

    assert_eq!(metrics.total_calls, 3);
    assert_eq!(metrics.failed_calls, 1);
    assert_eq!(
        metrics
            .by_kind
            .get(&ActionKind::SearchDiscovery)
            .unwrap()
            .calls,
        1
    );
    assert_eq!(
        metrics.by_kind.get(&ActionKind::ReadFetch).unwrap().calls,
        1
    );
    assert_eq!(
        metrics.by_kind.get(&ActionKind::WriteEdit).unwrap().calls,
        1
    );
    assert!(metrics.orientation_share() > 0.6);
}
