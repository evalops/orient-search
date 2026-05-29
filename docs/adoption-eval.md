# Adoption Eval

The useful question is not whether Orient can beat `rg` in isolation. The useful
question is whether agents reach the right files faster and waste fewer local
tool calls.

This eval must stay local and explicit. It should use opted-in task fixtures and
captured transcripts, not background session analytics.

## Protocol

Run the same realistic repo-editing tasks twice:

- Baseline: no Orient instruction; normal `rg`, `find`, `ls`, and `cat`.
- Orient: daemon running; agent instruction from `docs/agent-adoption.md`.

Use tasks that require file discovery before editing, not tasks where the target
file is named in the prompt.

Recommended first corpus:

- 20 tasks total
- 10 tasks in the primary repo
- 10 tasks across a multi-repo shard set
- at least 5 tasks where the right file is not obvious from the prompt
- at least 5 tasks involving tests or related files
- at least 3 tasks with duplicated names across packages or worktrees

## Task Manifest

Each task should be represented by a small local JSON file:

```json
{
  "id": "auth-token-refresh",
  "repo": "/path/to/repo",
  "prompt": "Fix stale auth token refresh behavior.",
  "relevant_files": ["src/auth/session.rs", "tests/session_refresh.rs"],
  "success_check": {
    "kind": "command",
    "command": "cargo test session_refresh"
  }
}
```

For shard tasks, `repo` can be replaced with an `index_dir` and `repo_filter`.

## Transcript Events

`orient eval-adoption` accepts normalized events like:

```jsonl
{"ts":"2026-05-28T10:00:00Z","kind":"tool_call","tool":"shell","command":"rg \"token refresh\""}
{"ts":"2026-05-28T10:00:02Z","kind":"file_open","path":"src/auth/session.rs"}
{"ts":"2026-05-28T10:00:20Z","kind":"edit","path":"src/auth/session.rs"}
{"ts":"2026-05-28T10:01:10Z","kind":"success","passed":true}
```

Adapters can convert Codex, Claude Code, or Amp transcripts into this schema.
The scorer does not need model-specific behavior after normalization. Transcript
events can carry `task_id`; when omitted, the transcript filename stem is used as
the task id. Timestamps may be RFC3339 `...Z`, numeric seconds in `ts`, or
numeric milliseconds in `ts_ms`.

## Metrics

For each task, record:

- time to first relevant file
- local-search command count
- Orient request count
- wrong file opens before the first relevant file
- total tool calls before first edit
- whether the first edit touched a relevant file
- whether the final edit succeeded
- wall-clock time

Local-search commands include `rg`, `grep`, `find`, `fd`, `ls`, `tree`, `cat`,
`sed`, `awk`, and similar file-discovery or file-reading commands. Orient
requests should be counted separately so the eval shows whether Orient replaces
scattered exploration rather than hiding it.

## Success Bar

Orient is worth making default when it reduces wasted local discovery without
hurting edit success. A strong result is:

- fewer repeated `rg`, `find`, `ls`, and `cat` calls
- fewer wrong file opens
- lower median time to first relevant file
- lower median tool calls before first edit
- equal or better edit success
- clear query-plan diagnostics on failed searches

## CLI

Run:

```bash
orient eval-adoption \
  --tasks eval/tasks.jsonl \
  --baseline-transcript eval/baseline/auth-token-refresh.jsonl \
  --orient-transcript eval/orient/auth-token-refresh.jsonl \
  --format json
```

Shell globs can pass many transcript paths by repeating `--baseline-transcript`
and `--orient-transcript`. Passing a directory recursively loads `.json` and
`.jsonl` transcripts in deterministic path order. `--format text` prints a
compact local summary for quick iteration.
