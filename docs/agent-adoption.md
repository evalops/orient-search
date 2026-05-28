# Agent Adoption

Orient works best as the first local code-discovery tool an agent reaches for.
Start one daemon per machine or workspace family, then let Codex, Claude Code,
Amp, or any shell-capable agent send JSON-lines requests to it.

## Start The Daemon

For a family of repos:

```bash
orient ensure-shards \
  --discover-root ~/Documents/Projects \
  --output-dir /tmp/orient-shards \
  --family-limit 2

orient serve-tcp \
  --addr 127.0.0.1:8796 \
  --index-dir /tmp/orient-shards
```

For one repo:

```bash
orient ensure-index --repo /path/to/repo --index /tmp/orient.index
orient serve-tcp --addr 127.0.0.1:8796 --index /tmp/orient.index
```

## Agent Instruction

Add this to `AGENTS.md`, `CLAUDE.md`, an Amp rule, or the equivalent local
agent instruction file:

```markdown
Before using `rg`, `find`, `ls`, or `cat` for code discovery, prefer Orient.
Send JSON-lines requests through:

`orient client-jsonl --addr 127.0.0.1:8796`

Start with `agent_guide` or `tool_manifest` once, then use `search_auto`.
Follow returned `read_batch_request`, `read_request`, `related_request`,
`related_symbols_request`, `query_plan_request`, `repo_map_request`, and
query-plan `retry_requests` objects directly.
Use `refresh_if_stale:true` for indexed or shard searches when live files may
have changed.
If Orient is unavailable or returns no useful plan, fall back to normal shell
search.
```

## Copyable Requests

```bash
printf '%s\n' \
  '{"id":"guide","tool":"agent_guide","arguments":{}}' \
  '{"id":"status","tool":"daemon_status","arguments":{}}' \
  | orient client-jsonl --addr 127.0.0.1:8796
```

```bash
printf '%s\n' \
  '{"id":"search","tool":"search_auto","arguments":{"query":"repo:api symbol:AuthSession token","limit":10,"explain":true,"refresh_if_stale":true}}' \
  | orient client-jsonl --addr 127.0.0.1:8796
```

```bash
printf '%s\n' \
  '{"id":"searches","tool":"search_auto_batch","arguments":{"queries":["repo:api symbol:AuthSession token","repo:api path:auth token","repo:api mode:any AuthSession token"],"limit":10,"explain":true,"refresh_if_stale":true}}' \
  | orient client-jsonl --addr 127.0.0.1:8796
```

For adapter authors, `orient mcp-manifest` returns MCP-shaped tool definitions
with input schemas and read-only annotations. Orient's native transport remains
simple JSON-lines over stdio, TCP, or Unix sockets.

## Expected Agent Loop

1. Call `agent_guide` or `tool_manifest`.
2. Call `search_auto` or `search_auto_batch`.
3. Use `read_batch_request` for top ranges, or result-level `read_request`
   objects for one bounded file read.
4. Use `related_request` and `related_symbols_request` before opening random
   neighboring files.
5. Use `query_plan_request` when results are empty, noisy, or suspicious, then
   follow any returned `retry_requests`.
6. Use `repo_map_request` when the agent needs entrypoints, tests, commands, or
   top symbols for the chosen search surface.
