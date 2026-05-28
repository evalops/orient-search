# Orient Search

Orient Search is a local code-search daemon for coding agents. It gives Codex,
Claude Code, Amp, and other local agents repo maps, indexed search, query plans,
and bounded file ranges so they stop burning runs on repeated `rg`, `find`,
`ls`, and `cat`.

## Run it

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

## Give it to agents

Give an agent the generated local rule snippet:

```bash
orient agent-instructions --index-dir /tmp/orient-shards
orient agent-guide --index-dir /tmp/orient-shards
orient client-jsonl
```

The intended loop is simple: get the tool manifest or agent guide, ask for a
repo map, search the warmed shard set, follow the returned `read_*` and
`related_*` requests, and inspect the query plan when results are empty or
noisy. Follow-up requests include replayable `cli`, `jsonl`, and `client_cli`
hints for terminal-native agents.

## Search locally

For one-shot CLI use inside a repo:

```bash
orient search-auto "symbol:AuthSession token"
orient search --repo . "issue token"
orient search --index /tmp/repo.index "issue token"
orient search --index-dir /tmp/orient-shards "repo:api issue token"
orient search --index-dir /tmp/orient-shards "branch:feature/auth origin:evalops/api issue token"
orient read-range --index /tmp/repo.index src/lib.rs:40:80
```

## Protocol

JSON-lines requests look like this:

```jsonl
{"id":"tools","tool":"tool_manifest","arguments":{}}
{"id":"guide","tool":"agent_guide","arguments":{"index_dir":"/tmp/orient-shards"}}
{"id":"map","tool":"shard_repo_map","arguments":{"symbols":25,"tests":25,"detail":"compact","read_limit":16}}
{"id":"search","tool":"search_auto","arguments":{"query":"repo:api branch:main symbol:AuthSession token","limit":10,"explain":true}}
{"id":"batch","tool":"search_auto_batch","arguments":{"queries":["repo:api symbol:AuthSession token","origin:evalops/api path:auth token"],"limit":10}}
{"id":"read","tool":"read_ranges","arguments":{"index_dir":"/tmp/orient-shards","ranges":[{"path":"api/src/auth.rs","start":40,"lines":80}]}}
```

## Filters

Useful filters: `repo:`, `path:`/`dir:`, `file:`, `lang:`, `ext:`, `symbol:`,
`kind:`/`type:`, `dep:`, `import:`, `test:`, `generated:`, `is:test`,
`is:source`, `is:generated`, `content:`, quoted phrases, negative filters like
`-path:vendor` and `-content:generated`, and `mode:any` for broad orientation.

## Eval

The adoption eval is the money chart: run the same repo-editing tasks with and
without Orient, then compare time to first relevant file, local-search command
count, wrong file opens, tool calls before edit, edit success rate, and
wall-clock time.

## Docs

More detail:

- [Agent adoption](docs/agent-adoption.md)
- [Agent protocol](docs/agent-protocol.md)
- [Adoption eval](docs/adoption-eval.md)
- [Fast search roadmap](docs/fast-search-roadmap.md)
