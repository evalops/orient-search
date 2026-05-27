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
- `orient index`: persistent Rust token/path posting index.
- `orient refresh-index`: incremental refresh that reuses unchanged file metadata/terms and refreshes changed files.
- `orient indexed-search`: indexed query path.
- JSON-lines tools: `search_code` and `indexed_search_code`.
- Result de-duping for repeated worktree copies where practical.

Measured on this machine:

- Wide tree fallback: `/Users/jonathanhaas/Documents/Projects`, common top-10 literal/token queries in about `17-31ms` mean after warmup, with max observed `43ms` across the sampled runs.
- Local repo fallback: query `indexed search symbol filters`, top 10 in about `11ms` mean after warmup.
- Hot-path fallback has a `250ms` wall-clock timeout plus match caps; if the timeout fires it returns partial results instead of blocking the agent.
- Local repo index build: about `0.25s`.
- Local repo refresh after build: reuses unchanged files and rebuilds postings from per-file term lists.
- Local repo indexed search: query `indexed search symbol filters`, top 10 in about `3ms` mean after warmup.

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
- CLI and JSON-lines server expose the same search capabilities.

Engineering definition:

- Persistent index has a versioned on-disk format.
- Incremental refresh exists.
- Tests cover fallback search, indexed search, incremental refresh, filters, ranking, duplicate suppression, and JSON-lines server calls.
- Every release claim is backed by `cargo fmt --check`, `cargo test`, `cargo build --release`, and timed searches.

## Architecture Direction

Near term:

- Keep `rg` as the brutally fast no-index baseline.
- Add more query filters and aliases after the current path/language/extension/require-all surface.
- Add a repeated-query benchmark command so p50/p95 are visible without external scripts.
- Tighten duplicate suppression based on normalized path suffixes and snippet signatures.

Zoekt-inspired indexed mode:

- Move from token postings to trigram postings for substring and regex-like queries.
- Store the index in versioned shards, one shard per repo or repo slice.
- Use mmap-friendly binary layout rather than JSON once the schema stabilizes.
- Store compact file metadata, path postings, content postings, and line offset tables.
- Keep snippets source-backed when files are present, but allow shard-backed snippets when searching detached snapshots.
- Add query planning: choose the rarest required posting lists first, then verify candidates.

Agent-specific differences from Zoekt:

- Return compact JSON objects built for tool use, not web UI rendering.
- Prefer de-duped, high-diversity top results over exhaustive match lists.
- Track which search results led to reads/edits so ranking can learn from current sessions.
- Treat repeated failed searches as product feedback and recommend index/filter improvements.
