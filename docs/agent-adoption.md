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

Generate the current recommended snippet with:

```bash
orient agent-instructions --index-dir /tmp/orient-shards
orient agent-guide --index-dir /tmp/orient-shards
```

Add this to `AGENTS.md`, `CLAUDE.md`, an Amp rule, or the equivalent local
agent instruction file. `agent-guide` also returns a machine-readable
`quickstart` block with the install, daemon, client, status, one-shot search,
and local-rule commands an adapter can render directly:

```markdown
Before using `rg`, `find`, `ls`, or `cat` for code discovery, prefer Orient.
Send JSON-lines requests through:

`orient client-jsonl`

Use `orient daemon-status` when a new agent session
needs to confirm the shared daemon has the expected warmed index or shard set.
Check `search_auto_default` and `default_requests` in that response to see the
exact target no-argument `search_auto` will use and copy the right first
repo-map/search/query-plan requests. Each default request also includes `jsonl`
and `client_cli` fields for direct terminal use. When you call
`daemon-status --addr` or `daemon-status --socket`, those `client_cli` commands
target that same daemon transport. Responses served over TCP or Unix sockets do
the same for generated search/read/related follow-up requests.

Start with `agent_guide` or `tool_manifest` once, then use `search_auto`.
Follow returned `read_batch_request`, `read_request`, `related_request`,
`related_symbols_request`, `query_plan_result`, `query_plan_request`,
`repo_map_request`, and query-plan `retry_requests` objects directly. These
follow-up request objects include complete `jsonl` and `client_cli` fields, so
shell-native agents can replay the exact next call through `orient client-jsonl`
without constructing JSON by hand.
Use `refresh_if_stale:true` for indexed or shard searches when live files may
have changed.
If Orient is unavailable or returns no useful plan, fall back to normal shell
search.
```

Use `orient doctor --index-dir /tmp/orient-shards` when a fresh local agent
session needs a compact health check. It verifies the repo path, local tool
availability, shard or index freshness, daemon reachability, and emits copyable
repair/start commands without collecting session analytics.

For one-shot CLI use from inside a repo, start with
`orient search-auto "query"` or `orient search-auto-batch "query one" "query two"`.
Pass `--index`, `--index-dir`, or `--repo` only when the current directory is
not the desired live search target.
The JSON-lines `search_auto` tools use the same target priority: explicit
`index_dir`, `index`, or `repo`; then one warmed daemon target; then the daemon
process current directory as a live repo.

## Copyable Requests

```bash
printf '%s\n' \
  '{"id":"instructions","tool":"agent_instructions","arguments":{"index_dir":"/tmp/orient-shards"}}' \
  '{"id":"guide","tool":"agent_guide","arguments":{}}' \
  '{"id":"status","tool":"daemon_status","arguments":{}}' \
  | orient client-jsonl
```

```bash
printf '%s\n' \
  '{"id":"search","tool":"search_auto","arguments":{"query":"repo:api branch:main symbol:AuthSession token","limit":10,"explain":true,"refresh_if_stale":true}}' \
  | orient client-jsonl
```

```bash
printf '%s\n' \
  '{"id":"searches","tool":"search_auto_batch","arguments":{"queries":["repo:api symbol:AuthSession token","origin:evalops/api path:auth token","repo:api mode:any AuthSession token"],"limit":10,"explain":true,"refresh_if_stale":true}}' \
  | orient client-jsonl
```

For adapter authors, `orient mcp-manifest` returns MCP-shaped tool definitions
with input schemas and read-only annotations. `orient serve-mcp` exposes those
tools over stdio JSON-RPC for MCP clients. Orient's native transport remains
simple JSON-lines over stdio, TCP, or Unix sockets.

## Expected Agent Loop

1. Call `agent_guide` or `tool_manifest`.
2. Call `search_auto` or `search_auto_batch`.
3. Use `read_batch_request` for top ranges, or result-level `read_request`
   objects for one bounded file read.
4. Use `related_request` and `related_symbols_request` before opening random
   neighboring files. Related-file and related-symbol results carry their own
   `read_request` objects; search-generated `related_symbols_request` objects
   also carry the originating query so agents can follow them directly.
5. For empty automatic searches, inspect `query_plan_result`; use
   `diagnose:true` on `search_auto` / `search_auto_batch` when results are
   noisy or suspicious and the agent wants search plus diagnostics in one call.
   Otherwise use `query_plan_request`, then follow any returned `retry_requests`.
6. Use `repo_map_request` when the agent needs entrypoints, tests, commands, or
   top symbols for the chosen search surface. Repo-map responses include a
   `read_batch_request` for the map's highest-value files and definitions.
