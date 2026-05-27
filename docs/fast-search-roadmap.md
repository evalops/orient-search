# Fast Search Roadmap

This project should become a Rust-native, local-agent-first search layer: the useful architecture shape of Zoekt, but optimized for Codex/Claude/Amp-style tool calls instead of a human web UI.

## Research Notes

The GitHub sweep covered 722 code-search, agent-context, semantic-search, and repository-understanding projects. The strongest references:

| Project | Useful signal |
| --- | --- |
| [sourcegraph/zoekt](https://github.com/sourcegraph/zoekt) | Fast trigram code search, rich query language, shards, mmap-friendly index files, branch masks, ranking with symbol/path signals. |
| [Sourcegraph Zoekt blog](https://sourcegraph.com/blog/zoekt-creating-internal-tools-at-google) | Origin story and rationale for internal-code-search tooling that needs to be fast enough to become default behavior. |
| [Zoekt design docs](https://github.com/sourcegraph/zoekt/blob/main/doc/design.md) | Explicit target of sub-50ms search over large corpora, positional trigrams, shard format, query trees, and ranking signals. |
| [probelabs/probe](https://github.com/probelabs/probe) | Agent-oriented search that combines ripgrep speed with tree-sitter/AST-aware snippets. |
| [MinishLab/semble](https://github.com/MinishLab/semble) | Agent-first code search framing: reduce grep/read token burn, local CPU, fast indexing/search. |
| [BloopAI/bloop](https://github.com/BloopAI/bloop) | Rust code-search engine precedent, hybrid regex/semantic direction. |
| [zilliztech/claude-context](https://github.com/zilliztech/claude-context) | MCP-shaped code search for Claude Code with vector DB integration. |
| [colbymchenry/codegraph](https://github.com/colbymchenry/codegraph) | Pre-indexed local graph for coding agents, framed around fewer tokens and fewer tool calls. |
| [lemon07r/Vera](https://github.com/lemon07r/Vera) | Rust local code search with BM25, vector similarity, reranking, tree-sitter metadata. |

## Product Thesis

Agents already search. The win is not convincing them to search; it is making search cheap, low-latency, and structured enough that they stop doing dozens of exploratory `rg`, `find`, `ls`, and `cat` calls.

The Ceramic-level insight for this product is: agents already search, so the leverage is making local code search cheap enough that it becomes the default first action before scattered `rg`, `find`, `ls`, and `cat` exploration.

## Current Baseline

Implemented now:

- `orient search`: fast `rg`-backed candidate collection with Rust-side scoring/snippets.
- Agent-oriented query language for `file:`, `path:`, `lang:`, `ext:`, `symbol:`, `repo:`, `test:`, quoted literals, negative filters, and default multi-term AND behavior.
- `orient index`: persistent Rust content-token, path-token, and trigram posting index.
- `orient refresh-index`: incremental refresh that reuses unchanged file metadata/terms and refreshes changed files.
- `orient indexed-search`: indexed query path.
- `orient index-shards`, `orient refresh-shards`, `orient search-shards`, and `orient read-shard-range`: local multi-repo shard manifest with one versioned index file per repo, incremental shard refresh, and bounded range reads from prefixed shard paths.
- `orient bench-search`: built-in p50/p95/max latency reporting for fallback and indexed search, with `--fail-p95-ms`, `--write-baseline`, and `--baseline` for regression gates.
- JSON-lines tools: `tool_manifest`, `search_code`, `indexed_search_code`, `indexed_repo_map`, `read_index_range`, `find_index_symbol`, `shard_repo_map`, `find_shard_symbol`, `related_index_files`, `related_index_symbols`, `index_shards`, `refresh_shards`, `search_shards`, `read_shard_range`, `repo_map`, `read_range`, and `related_symbols`.
- CLI tools: `repo-map`, `index-map`, `shard-map`, `read-range`, `read-index-range`, `index-symbol`, `shard-symbol`, `related-index`, `related-index-symbols`, and `related-symbols`, so agents can inspect entrypoints/tests/top symbols, open bounded file context, and jump to nearby definitions after a search hit.
- `orient tool-manifest`: emits descriptions plus required/optional argument metadata for JSON-lines wrappers.
- Search snippet modes: `short`, `medium`, `block`, and `symbol`.
- Path, file, repo, extension, language, and symbol filters match case-insensitively across fallback, indexed, and shard search surfaces.
- Optional structured ranking explanations with path/content/term-frequency/symbol signals.
- Indexed search plans candidates from the rarest content/path token postings, falling back to rare trigram postings for substring queries.
- Indexed files persist line-offset tables for bounded snippet rendering.
- Result de-duping for repeated worktree copies using normalized path suffixes and snippet signatures.
- Exact symbol definition boosting in both fallback and indexed search.
- Direct symbol lookup from persistent indexes, so agent wrappers can jump to definitions without rebuilding a repo index.
- Direct symbol lookup across local shard directories, returning repo-prefixed paths that can be passed to `read-shard-range`.
- Repo-map orientation from persistent indexes and shard directories, so agents can inspect entrypoints, tests, symbols, important files, and command hints without rebuilding a separate live repo index.

Measured on this machine:

- Wide tree fallback: `/Users/jonathanhaas/Documents/Projects`, common top-10 literal/token queries at `20-35ms` p95 after warmup across the sampled runs.
- Local repo fallback: query `indexed search symbol filters`, top 10 at about `8.4ms` p95 after warmup.
- Hot-path fallback has a `250ms` wall-clock timeout plus match caps; if the timeout fires it returns partial results instead of blocking the agent.
- Local repo index build: about `0.25s`.
- Local repo refresh after build: reuses unchanged files and rebuilds postings from per-file term lists.
- Local repo indexed search: query `indexed search symbol filters`, top 10 at about `0.13ms` p95 after warmup.

## Exit Conditions

High-performance definition:

- Wide-tree hot path returns useful top-10 results from `/Users/jonathanhaas/Documents/Projects` in `<=300ms` p95 for common literal/token queries.
- Repo-local searches return `<=100ms` p95 after warmup.
- Indexed search beats fallback search on repeated repo-local queries.
- No multi-second hangs: candidate collection has bounded match caps and a hard wall-clock timeout.
- Top results avoid obvious duplicate spam from repeated worktrees.

Search quality definition:

- Query support covers literals, multi-token AND semantics, path filters, extension/language filters, and exact-symbol boosts.
- Snippets include line numbers and enough context for an agent to decide whether to read/edit.
- Explain mode returns structured ranking signals when an agent needs to compare close results.
- CLI and JSON-lines server expose the same search capabilities, including multi-repo shard search.

Engineering definition:

- Persistent index has a versioned on-disk format.
- Persistent index stores separate content-token, path-token, and trigram postings.
- Multi-repo shard directories store a manifest plus one versioned index per repo, and can refresh those indexes incrementally.
- Persistent indexed files include line-offset tables for snippet retrieval.
- Incremental refresh exists.
- Tests cover fallback search, indexed search, shard search/read tools, incremental refresh, filters, query parser stress cases, ranking explanations, duplicate suppression, JSON-lines server calls, corrupt index errors, path safety including symlink escapes, snippet modes, and a guarded `rg` differential check.
- Every release claim is backed by `cargo fmt --check`, `cargo test`, `cargo build --release`, and `orient bench-search` or equivalent timed searches, with saved baselines available for local or CI regression checks.

## Architecture Direction

Near term:

- Keep `rg` as the brutally fast no-index baseline.
- Add more query filters and aliases after the current path/language/extension/require-all surface.
- Add CI wiring around saved benchmark baselines once a stable runner is available.

Zoekt-inspired indexed mode:

- Move from token postings to trigram postings for substring and regex-like queries.
- Store the index in versioned shards, one shard per repo or repo slice.
- Use mmap-friendly binary layout rather than JSON once the schema stabilizes.
- Store compact file metadata, path postings, content postings, and richer line/term offset tables.
- Keep snippets source-backed when files are present, but allow shard-backed snippets when searching detached snapshots.
- Add query planning: choose the rarest required posting lists first, then verify candidates.

Agent-specific differences from Zoekt:

- Return compact JSON objects built for tool use, not web UI rendering.
- Prefer de-duped, high-diversity top results over exhaustive match lists.
- Keep ranking local and deterministic; do not add session analytics or telemetry.
- Treat failed searches as an offline test-corpus signal when the user explicitly captures examples.
