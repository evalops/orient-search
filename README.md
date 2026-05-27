# Orient Search

Rust-native fast local code search for coding agents. It gives Codex, Claude, Amp-style agents, and similar tools a cheap way to answer “where is the relevant thing?” before they burn tool calls on repeated `rg`, `find`, `cat`, and failed path probes.

## What It Does

- Indexes a local repo and returns compact search answers.
- Searches code with a fast `rg`-backed hot path plus an experimental persistent Rust index.
- Finds symbols and related test/source files.
- Infers known commands from repo manifests.
- Exposes a Rust CLI and JSON-lines tool server suitable for MCP-style wrapping.

## Rust Quickstart

```bash
cargo build
cargo test

# Brief a repo.
cargo run -- brief --repo /path/to/repo

# Search code.
cargo run -- search --repo /path/to/repo "session token auth"

# Build and query a persistent local index.
cargo run -- index --repo /path/to/repo --output /tmp/orient.index
cargo run -- refresh-index --repo /path/to/repo --index /tmp/orient.index
cargo run -- indexed-search --index /tmp/orient.index "session token auth" \
  --path src/ \
  --language rust \
  --extension rs \
  --require-all

# Find a symbol.
cargo run -- symbol --repo /path/to/repo SessionManager

# Find related tests/files.
cargo run -- related --repo /path/to/repo src/auth.py
```

## JSON-Lines Server

`orient serve-jsonl` reads one request per line from stdin and writes one response per line to stdout.

```bash
cargo run -- serve-jsonl
```

Example request:

```json
{"id":1,"tool":"search_code","arguments":{"repo":"/path/to/repo","query":"issue token","limit":5,"extension":"rs","require_all":true}}
```

Supported tools:

- `list_tools`
- `repo_brief`
- `search_code`
- `indexed_search_code`
- `find_symbol`
- `related_files`

## Success Criteria

The build is useful when it can:

- Answer repo brief/search/symbol/related-file questions through Rust CLI and JSON-lines server.
- Return wide-tree search results in hundreds of milliseconds, not seconds.
- Provide a persistent indexed search mode that can evolve toward Zoekt-style shards/postings.
- Refresh the persistent index incrementally by reusing unchanged file metadata and postings.
- Establish a baseline for fast local code search so future agent runs can use fewer exploratory commands.
- Pass the Rust test suite.

Product impact criteria for follow-up adoption:

- Fewer exploratory search/read calls in comparable coding sessions.
- Fewer failed path probes and dead-end search commands.
- Faster useful file discovery before first edit.
- No task-quality regression.

Current search baseline:

- `orient search --repo . "indexed search symbol filters"`: about `11ms` mean after warmup.
- `orient search --repo /Users/jonathanhaas/Documents/Projects "session token auth"`: about `19ms` mean after warmup.
- `orient search --repo /Users/jonathanhaas/Documents/Projects "postgres migration user"`: about `30ms` mean after warmup.
- `orient index --repo . --output /tmp/orient-self.index`: versioned binary index with file metadata, terms, and symbol boosts.
- `orient refresh-index --repo . --index /tmp/orient-self.index`: reuses unchanged files and refreshes changed/deleted files.
- `orient indexed-search --index /tmp/orient-self.index "indexed search symbol filters"`: about `3ms` mean after warmup.

See [docs/fast-search-roadmap.md](docs/fast-search-roadmap.md) for the Zoekt/Sourcegraph/Amp-inspired roadmap.

## Architecture

- `src/fast_index.rs`: experimental persistent token index and indexed search.
- `src/repo_index.rs`: repo indexing, symbol extraction, code search, related-file lookup.
- `src/server.rs`: JSON-lines tool dispatch.
- `src/main.rs`: CLI.
