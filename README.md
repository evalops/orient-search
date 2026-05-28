# Orient Search

Fast local code search for coding agents.

```bash
cargo build --release

target/release/orient index --repo /path/to/repo --output /tmp/orient.index
target/release/orient indexed-search --index /tmp/orient.index "session token auth"

target/release/orient index-shards --repo /path/to/repo --output-dir /tmp/orient-shards
target/release/orient serve-tcp --addr 127.0.0.1:8796 --index-dir /tmp/orient-shards
```

Filters: `repo:`, `path:`/`dir:`, `file:`, `lang:`, `ext:`, `symbol:`, `kind:`, `dep:`, `import:`, `test:`, `-path:docs`, quoted phrases.

More: [Agent protocol](docs/agent-protocol.md), [Fast search roadmap](docs/fast-search-roadmap.md).
