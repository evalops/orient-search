# Orient Search

Fast local code search for coding agents. Orient indexes repos on disk, answers targeted searches, and returns bounded file ranges without making every agent repeatedly crawl the same tree.

## Use

```bash
cargo build --release
cargo test

# Direct repo search.
target/release/orient search --repo /path/to/repo "session token auth"
target/release/orient repo-map --repo /path/to/repo --symbols 50 --tests 50

# Persistent index.
target/release/orient index --repo /path/to/repo --output /tmp/orient.index
target/release/orient indexed-search --index /tmp/orient.index "session token auth"
target/release/orient read-index-range --index /tmp/orient.index src/auth.rs --start 40 --lines 80
```

## Multi-Repo

```bash
target/release/orient index-shards \
  --discover-root /Users/jonathanhaas/Documents/Projects \
  --max-depth 4 \
  --discover-limit 500 \
  --family-limit 2 \
  --output-dir /tmp/orient-shards

target/release/orient search-shards --index-dir /tmp/orient-shards "repo:platform token auth"
target/release/orient read-shard-range --index-dir /tmp/orient-shards platform/src/auth.rs --start 40 --lines 80
```

## Daemon

Use one warm daemon when several local agents are working over the same codebases:

```bash
target/release/orient serve-tcp --addr 127.0.0.1:8796 --index-dir /tmp/orient-shards
target/release/orient client-jsonl --addr 127.0.0.1:8796
```

Ask `tool_manifest` for the JSON-lines tool list and schemas.

Useful filters: `repo:platform`, `path:src/auth`, `file:auth.rs`, `lang:rust`, `ext:rs`, `symbol:SessionManager`, `test:false`, `-path:docs`, and quoted phrases like `"issue token"`.

## Docs

- [Fast search roadmap](docs/fast-search-roadmap.md)
- [Research notes](docs/research)
