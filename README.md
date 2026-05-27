# Orient Search

Rust-native fast local code search for coding agents. It gives Codex, Claude, Amp-style agents, and similar tools a cheap way to answer “where is the relevant thing?” before they burn tool calls on repeated `rg`, `find`, `cat`, and failed path probes.

## What It Does

- Indexes a local repo and returns compact search answers.
- Searches code with a fast `rg`-backed hot path plus an experimental persistent Rust index.
- Boosts exact symbol definitions in both fallback and indexed search.
- Finds symbols plus related test/source files and nearby definitions.
- Reads bounded line ranges after search hits, with line-numbered output.
- Builds repo maps with entrypoints, tests, top symbols, commands, and important files.
- Infers known commands from repo manifests.
- Exposes a Rust CLI and JSON-lines tool server suitable for MCP-style wrapping.

## Rust Quickstart

```bash
cargo build
cargo test

# Brief a repo.
cargo run -- brief --repo /path/to/repo
cargo run -- repo-map --repo /path/to/repo --symbols 50 --tests 50

# Search code.
cargo run -- search --repo /path/to/repo "session token auth"
cargo run -- search --repo /path/to/repo 'symbol:SessionManager lang:rust -path:docs "issue token"' \
  --snippet block \
  --explain

# Build and query a persistent local index.
cargo run -- index --repo /path/to/repo --output /tmp/orient.index
cargo run -- refresh-index --repo /path/to/repo --index /tmp/orient.index
cargo run -- indexed-search --index /tmp/orient.index "session token auth" \
  --path src/ \
  --language rust \
  --extension rs \
  --require-all \
  --snippet symbol

# Find a symbol.
cargo run -- symbol --repo /path/to/repo SessionManager

# Find related tests/files.
cargo run -- related --repo /path/to/repo src/auth.py
cargo run -- related-symbols --repo /path/to/repo --path src/auth.py --query "session token"

# Read a bounded, line-numbered file range.
cargo run -- read-range --repo /path/to/repo src/auth.py --start 40 --lines 80

# Measure p50/p95/max search latency with the same code paths agents use.
cargo run --release -- bench-search \
  --repo /Users/jonathanhaas/Documents/Projects \
  --runs 10 \
  --warmup 3 \
  --fail-p95-ms 300 \
  "session token auth" \
  "browser session implementation" \
  "postgres migration user"
```

## JSON-Lines Server

`orient serve-jsonl` reads one request per line from stdin and writes one response per line to stdout.

```bash
cargo run -- serve-jsonl
```

Example request:

```json
{"id":1,"tool":"search_code","arguments":{"repo":"/path/to/repo","query":"issue token","limit":5,"extension":"rs","require_all":true,"snippet":"block","explain":true}}
```

Supported tools:

- `list_tools`
- `repo_brief`
- `repo_map`
- `read_range`
- `search_code`
- `indexed_search_code`
- `find_symbol`
- `related_files`
- `related_symbols`

## Query Language

Search queries support agent-friendly filters inline with normal terms:

- `file:auth.rs`: match file basename.
- `path:src/auth`: require a path substring.
- `lang:rust` or `language:rust`: require a detected language.
- `ext:rs`: require a file extension.
- `symbol:SessionManager`: require/boost an exact symbol definition.
- `repo:orient-search`: require the root repo name.
- `test:true` or `test:false`: include only test or non-test paths.
- `-path:docs`, `-file:generated`, `-lang:markdown`, `-ext:md`, `-symbol:Foo`, `-repo:old`: exclude matches.
- `"issue token"`: keep multi-word literals grouped while parsing.

Multiple positive terms use AND behavior by default, so `session token auth` means all three terms should be represented in the returned result.

## Snippet Modes

Search tools and CLI commands accept `--snippet <mode>` or JSON `"snippet":"<mode>"`:

- `short`: one matching line.
- `medium`: a compact default context window.
- `block`: a larger context block for deciding whether to edit.
- `symbol`: prefer the matching symbol definition when a symbol signal is available.

Indexed search persists line-offset tables in the binary index and uses them to render bounded snippets without reparsing the file into an in-memory repo index.

## Ranking Explanations

Search commands and JSON-lines tools accept `--explain` or JSON `"explain":true`. Normal output stays compact; explain mode adds an `explanation` array to each result with structured ranking signals such as:

- `path_match`: query token appeared in the path.
- `line_match` or `content_match`: query token appeared in matched content.
- `term_frequency`: indexed term frequency contributed to score.
- `symbol_exact` or `symbol_overlap`: symbol matching contributed to score.

## Success Criteria

The build is useful when it can:

- Answer repo brief/search/symbol/related-file questions through Rust CLI and JSON-lines server.
- Let an agent search, inspect a repo map, and read bounded file ranges without shelling out to `cat`/`sed`.
- Return wide-tree search results in hundreds of milliseconds, not seconds.
- Bound the hot path with a wall-clock timeout and match caps so pathological trees cannot hang searches.
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

- `orient bench-search --repo . "indexed search symbol filters"`: `10.053ms` p95 after warmup.
- `orient bench-search --repo /Users/jonathanhaas/Documents/Projects "session token auth"`: `16.640ms` p95 after warmup.
- `orient bench-search --repo /Users/jonathanhaas/Documents/Projects "browser session implementation"`: `25.193ms` p95 after warmup.
- `orient bench-search --repo /Users/jonathanhaas/Documents/Projects "postgres migration user"`: `36.082ms` p95 after warmup.
- The `rg` hot path has a `250ms` wall-clock timeout plus a bounded match cap; timed-out searches return partial results rather than hanging.
- `orient index --repo . --output /tmp/orient-self.index`: versioned binary index with file metadata, content token postings, path token postings, trigram postings, line offsets, and symbol boosts.
- `orient refresh-index --repo . --index /tmp/orient-self.index`: reuses unchanged files and refreshes changed/deleted files.
- `orient bench-search --repo . --index /tmp/orient-self.index "indexed search symbol filters"`: `0.628ms` p95 after warmup.

Benchmark methodology:

- Use `cargo build --release`, then run `orient bench-search`.
- Warm up each query before collecting samples.
- Report `p95_ms` and `max_ms` from repeated searches, not one-off timings.
- Benchmark the fallback path without `--index`; benchmark the persistent indexed path with `--index /tmp/orient-self.index`.
- Use `--fail-p95-ms <milliseconds>` in CI or local regression checks when you want slow queries to fail the command.

See [docs/fast-search-roadmap.md](docs/fast-search-roadmap.md) for the Zoekt/Sourcegraph/Amp-inspired roadmap.

## Architecture

- `src/fast_index.rs`: experimental persistent content/path token plus trigram index and indexed search.
- `src/repo_index.rs`: repo indexing, symbol extraction, snippet rendering, code search, related-file lookup.
- `src/query.rs`: inline query-language parsing and filter merging.
- `src/server.rs`: JSON-lines tool dispatch.
- `src/main.rs`: CLI.
