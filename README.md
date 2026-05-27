# Orient Search

Fast local code search for coding agents. Orient keeps repo and shard indexes on disk so many local agents can share one warm search surface instead of repeatedly crawling the same codebases.

## Quick Start

```bash
cargo build --release

target/release/orient search --repo /path/to/repo "session token auth"
target/release/orient index --repo /path/to/repo --output /tmp/orient.index
target/release/orient indexed-search --index /tmp/orient.index "session token auth"
target/release/orient serve-tcp --addr 127.0.0.1:8796 --index-dir /tmp/orient-shards
```

## Agent Surface

Run `client-jsonl` against `serve-tcp`, then ask `tool_manifest` for the current JSON-lines tools and schemas. Common query filters include `repo:`, `path:`, `file:`, `lang:`, `ext:`, `symbol:`, `test:`, negative filters such as `-path:docs`, and quoted phrases.

## Docs

- [Agent protocol](docs/agent-protocol.md): JSON-lines tools and agent workflow.
- [Fast search roadmap](docs/fast-search-roadmap.md): architecture, performance targets, and next work.
