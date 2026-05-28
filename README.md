# Orient Search

Orient Search is a local code-search daemon for coding agents. It gives Codex,
Claude Code, Amp, and other local agents repo maps, indexed search, query plans,
and bounded file ranges so they stop burning runs on repeated `rg`, `find`,
`ls`, and `cat`.

```bash
cargo install --git https://github.com/evalops/orient-search

orient ensure-shards \
  --discover-root ~/Documents/Projects \
  --output-dir /tmp/orient-shards \
  --family-limit 2

orient serve-tcp \
  --addr 127.0.0.1:8796 \
  --index-dir /tmp/orient-shards
```

Agents can talk JSON-lines over TCP, Unix sockets, or stdio:

```bash
orient agent-guide --index-dir /tmp/orient-shards
orient client-jsonl --addr 127.0.0.1:8796
```

```jsonl
{"id":"tools","tool":"tool_manifest","arguments":{}}
{"id":"guide","tool":"agent_guide","arguments":{"index_dir":"/tmp/orient-shards"}}
{"id":"map","tool":"shard_repo_map","arguments":{"symbols":25,"tests":25}}
{"id":"auto","tool":"search_auto","arguments":{"query":"repo:api symbol:AuthSession token","limit":10,"explain":true}}
{"id":"search","tool":"search_shards","arguments":{"query":"repo:api symbol:AuthSession token","limit":10,"explain":true}}
{"id":"read","tool":"read_shard_ranges","arguments":{"ranges":[{"path":"api/src/auth.rs","start":40,"lines":80}]}}
```

The intended agent loop is simple: ask for the tool manifest, get a repo map,
search the shard set, read the returned `read_range` objects, and inspect the
query plan when results are empty or noisy.
Once a daemon has exactly one shard directory or index warmed, `search_auto`
lets wrappers search that target with just a query.

For a repo without a saved index, use `orient search-plan --repo . "query"` or
the JSON-lines `search_query_plan` tool to get the same missing-term and retry
diagnostics from a transient local index.

Filters: `repo:`, `path:`/`dir:`, `file:`, `lang:`, `ext:`, `symbol:`,
`kind:`/`type:`, `dep:`, `import:`, `test:`, `is:test`, `is:source`,
`content:`, `text:`, `-path:docs`, quoted phrases, and `mode:any`.
`file:` and `path:` accept `*` and `?` wildcards; `path:` accepts `/` or `\`
separators.

The adoption eval: run the same 20 repo-editing tasks with and without Orient.
Measure time to first relevant file, local-search command count, wrong file
opens, tool calls before edit, edit success rate, and wall-clock time.

More: [Agent protocol](docs/agent-protocol.md), [Fast search roadmap](docs/fast-search-roadmap.md).
