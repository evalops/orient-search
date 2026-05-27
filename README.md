# Orient Search

Rust-native local code search for coding agents. Orient gives Codex, Claude, Amp-style agents, and similar tools a cheap way to answer "where is the relevant thing?" before they burn time on repeated `rg`, `find`, `cat`, and failed path probes.

## What It Does

- Fast repo search through an `rg` fallback plus a persistent Rust index.
- Local multi-repo shard indexes for repeated worktree-heavy machines.
- Repo maps with entrypoints, manifests, tests, symbols, important files, and command hints.
- Bounded line-range reads after search hits.
- Symbol lookup and related source/test discovery.
- JSON-lines and localhost TCP server modes for agent wrappers.

## Quickstart

```bash
cargo build
cargo test

# Search a repo.
cargo run -- search --repo /path/to/repo "session token auth"

# Build and use a persistent index.
cargo run -- index --repo /path/to/repo --output /tmp/orient.index
cargo run -- indexed-search --index /tmp/orient.index "session token auth"
cargo run -- read-index-range --index /tmp/orient.index src/auth.rs --start 40 --lines 80

# Get orientation before editing.
cargo run -- repo-map --repo /path/to/repo --symbols 50 --tests 50
cargo run -- index-map --index /tmp/orient.index --symbols 50 --tests 50
```

## Multi-Repo Search

For machines with many local clones and worktrees, discover repo roots and build a shard directory:

```bash
cargo run -- discover-repos \
  --root /Users/jonathanhaas/Documents/Projects \
  --max-depth 4 \
  --limit 500 \
  --git-metadata

cargo run -- index-shards \
  --discover-root /Users/jonathanhaas/Documents/Projects \
  --discover-root /Users/jonathanhaas/repos \
  --max-depth 4 \
  --discover-limit 500 \
  --family-limit 2 \
  --output-dir /tmp/orient-shards

cargo run -- search-shards --index-dir /tmp/orient-shards "repo:maestro app server"
cargo run -- shard-map --index-dir /tmp/orient-shards --repo maestro --symbols 50 --tests 50
cargo run -- read-shard-range --index-dir /tmp/orient-shards maestro/src/app.rs --start 40 --lines 80
```

Discovery treats git checkouts as boundaries by default so monorepo package manifests do not explode into separate shard candidates. Use `--family-limit N` to select at most `N` checkouts per repeated git family, and use `--nested-manifests` only when package-level directories inside a checkout should become separate shard roots.

## Shared Daemon

When several local agents are working on the same codebases, run one warm TCP daemon and have each agent use `client-jsonl` or a thin wrapper:

```bash
cargo run -- serve-tcp --addr 127.0.0.1:8796 \
  --index-dir /tmp/orient-shards

cargo run -- client-jsonl --addr 127.0.0.1:8796
```

Useful JSON-lines tools include `discover_repos`, `ensure_shards`, `warm_shards`, `search_shards`, `shard_repo_map`, `read_shard_range`, `read_shard_ranges`, `find_shard_symbol`, `related_shard_files`, and `related_shard_symbols`.

Example request:

```json
{"id":1,"tool":"search_shards","arguments":{"index_dir":"/tmp/orient-shards","query":"repo:platform session token auth","limit":5,"context_lines":80}}
```

Use `tool_manifest` to get the full tool list, argument metadata, defaults, and input schemas.

## Query Cheatsheet

- `file:auth.rs`
- `path:src/auth`
- `lang:rust` or `language:rust`
- `ext:rs`
- `symbol:SessionManager`
- `repo:platform`
- `test:true` or `test:false`
- `-path:docs`
- `"issue token"`

Multiple positive terms use AND behavior by default. Search results include line ranges so agents can jump directly to bounded reads.

## More Detail

- [Fast search roadmap](docs/fast-search-roadmap.md): research notes, architecture direction, current measured baselines, and exit criteria.
- [Research notes](docs/research): OSS and literature notes behind the project.
