# Deep Improvement Plan

Orient is already fast enough to be useful. The next step is making it reliable
enough, obvious enough, and measurable enough that local coding agents use it
before repeated `rg`, `find`, `ls`, and `cat` exploration.

## Product Thesis

The durable wedge is not "search is faster than grep." The wedge is "agents
already search, and local search can become a cheap structured primitive."

That means the main product metric is agent waste reduction:

- fewer local discovery commands before the first edit
- fewer wrong file opens
- faster time to first relevant file
- equal or better edit success
- useful recovery hints when search fails

Orient should remain local code search only. No session analytics, no telemetry,
and no hosted dependency.

## Highest-Leverage Work

### 1. Build the adoption eval

The adoption eval is the proof layer. Today the repo documents the idea, but it
does not yet ship a runnable harness.

Build `orient eval-adoption` around local task fixtures and explicit transcript
input. It should compare baseline agent runs against Orient-assisted runs for
the same tasks without collecting background analytics.

Exit condition:

- a task manifest format exists
- transcript parsing supports Codex/Claude-style JSONL or a small normalized
  event schema
- scoring reports time to first relevant file, wrong opens, local-search
  commands, tool calls before edit, final success, and wall-clock time
- the command emits JSON and a compact terminal summary
- docs include a 20-task recommended protocol

### 2. Make multi-agent use boring

The user has many local agents touching a small number of codebases. Orient
should feel like one shared local search appliance for that setup.

Add `orient doctor` and a bootstrap command that answers:

- is `orient` installed and on `PATH`?
- is a daemon reachable?
- what shard directory or index is warmed?
- are shards stale?
- what command should a new agent copy first?
- is the repo target ambiguous?

Exit condition:

- a fresh Codex/Claude/Amp session can run one command and know exactly how to
  use the shared daemon
- stale or missing index states produce copyable repair commands
- daemon status is short enough to paste into agent instructions

### 3. Ship a real MCP stdio surface

Orient already has an MCP-shaped manifest and JSON-lines transport. The next
adoption jump is a real MCP server mode so tools can be mounted directly instead
of explained through shell snippets.

Exit condition:

- `orient serve-mcp` exposes the existing read-only search/read/map/plan tools
- tool names, schemas, bounds, and read-only annotations match the native
  manifest
- integration docs cover Codex, Claude Code, and Amp
- native JSONL stays as the simple fallback

### 4. Improve failure recovery with facets

Current repair hints handle typos, bad filters, missing terms, candidate-cap
diagnostics, and candidate-set facets for broad indexed searches. The next useful
step is making those facets richer across shard plans and non-cap noisy results.

When candidate caps or broad results happen, sample candidates and return top
facets such as:

- path prefixes
- extensions and languages
- test/generated/source split
- symbol kinds
- repo/branch/origin for shard results

Exit condition:

- broad searches produce `narrow_by_*` hints only when the facet meaningfully
  reduces the candidate set
- hints include replayable retry requests when safe
- candidate-cap hints never create no-op retry loops

### 5. Move the index toward zero-copy sections

The index now has mmap-backed loading and compressed posting maps, but search
still deserializes into owned Rust structures. That is a good current shape, not
the final large-monorepo shape.

Next architecture:

- header and section table
- file metadata section
- string table
- term dictionary
- compressed posting blocks with skip data
- line-offset table
- snapshot content blob

Exit condition:

- loading a large index does not require decoding every posting list
- search only touches the sections needed for the query
- index status reports file count, source bytes, index bytes, posting bytes, and
  content-snapshot bytes separately
- legacy index compatibility remains covered by tests

### 6. Upgrade ranking quality tests

Latency is only useful if top results are right. The existing golden and
differential tests are valuable, but they should grow into a relevance suite.

Add adversarial fixtures for:

- the same identifier in source, docs, tests, generated files, and lockfiles
- prompts that imply a target file without naming it
- near-symbol typos
- path/file filter confusion
- duplicated worktrees and monorepo-style packages
- broad terms that need facet narrowing

Exit condition:

- the suite reports Recall@10 and MRR for labeled queries
- fallback, indexed, shard, and daemon surfaces share expected top-k behavior
- regressions fail locally and in CI

### 7. Tighten performance gates where they matter

The current gates prove the project is not accidentally slow on itself. Add
local wide-corpus gates for the user's real project layout and keep CI focused
on deterministic synthetic fixtures.

Exit condition:

- CI keeps deterministic repo-local p95 gates
- a local script runs wide-corpus shard benches against `~/Documents/Projects`
- reports include cold load, warm daemon, memory, index size, build time, and
  p95/p99 search latency; benchmark JSON now includes p99 alongside p50/p95/max
- failure output names the slow query and surface

## Immediate Build Order

1. `orient doctor`
2. adoption-eval manifest and scorer
3. candidate facet hints
4. real MCP stdio server
5. sectioned zero-copy index prototype
6. relevance suite and wide local perf script

This order makes Orient more useful to today's coding sessions before spending
engineering time on deeper index internals.
