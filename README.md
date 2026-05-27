# Agent Orientation Layer

Rust-first local orientation layer for coding agents. It gives Codex, Claude, and similar agents a cheap way to answer “where is the relevant thing?” before they burn tool calls on repeated `rg`, `find`, `cat`, and failed path probes.

The repo also includes the original Streamlit JSONL explorer for inspecting Codex and Claude session logs.

## What It Does

- Indexes a local repo and returns compact orientation answers.
- Searches code with lexical + symbol-aware ranking.
- Finds symbols and related test/source files.
- Infers known commands from repo manifests.
- Scans Codex/Claude JSONL logs for tool-call metrics.
- Exposes a Rust CLI and JSON-lines tool server suitable for MCP-style wrapping.

## Rust Quickstart

```bash
cargo build
cargo test

# Brief a repo.
cargo run -- brief --repo /path/to/repo

# Search code.
cargo run -- search --repo /path/to/repo "session token auth"

# Find a symbol.
cargo run -- symbol --repo /path/to/repo SessionManager

# Find related tests/files.
cargo run -- related --repo /path/to/repo src/auth.py

# Measure agent orientation behavior from JSONL logs.
cargo run -- metrics \
  --root /Users/jonathanhaas/.codex \
  --root /Users/jonathanhaas/.claude \
  --max-files 100 \
  --max-file-mb 20
```

Use `--max-files 500 --max-file-mb 50` for a wider offline scan; current dense Codex logs can make that too slow for interactive use.

## JSON-Lines Server

`orient serve-jsonl` reads one request per line from stdin and writes one response per line to stdout.

```bash
cargo run -- serve-jsonl
```

Example request:

```json
{"id":1,"tool":"search_code","arguments":{"repo":"/path/to/repo","query":"issue token","limit":5}}
```

Supported tools:

- `list_tools`
- `repo_brief`
- `search_code`
- `find_symbol`
- `related_files`
- `metrics`

## Success Criteria

The build is useful when it can:

- Answer repo brief/search/symbol/related-file questions through Rust CLI and JSON-lines server.
- Parse recent Codex/Claude logs and report total calls, failed calls, action-kind counts, and orientation share.
- Establish a baseline for search/read behavior so future agent runs can be compared.
- Pass the Rust test suite and keep the Streamlit explorer usable.

Current interactive baseline on Jonathan's recent logs, using `--max-files 100 --max-file-mb 20`:

- `17,707` tool calls.
- `864` failed calls.
- `5,742` search/read orientation calls.
- `32.4%` orientation share.

Product impact criteria for follow-up adoption:

- 20-40% fewer search/read calls in comparable sessions.
- 30% fewer failed search commands.
- Fewer calls before first edit.
- No task-quality regression.

## Dashboard

```bash
cd /Users/jonathanhaas/agent-jsonl-explorer
python3 -m venv .venv
. .venv/bin/activate
pip install -r requirements.txt
cargo build
streamlit run app.py
```

The dashboard scans these default roots:

- `/Users/jonathanhaas/.codex`
- `/Users/jonathanhaas/.claude`

Use the sidebar to limit files, date ranges, sources, tools, and whether raw text snippets are retained. If `target/debug/orient` exists, the dashboard also displays Rust-core metrics.

## Architecture

- `src-rs/repo_index.rs`: repo indexing, symbol extraction, code search, related-file lookup.
- `src-rs/session_metrics.rs`: Codex/Claude JSONL tool-call parsing and action classification.
- `src-rs/server.rs`: JSON-lines tool dispatch.
- `src-rs/main.rs`: CLI.
- `agent_logs.py` and `app.py`: exploratory Python dashboard.
