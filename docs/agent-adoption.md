# Agent Adoption

Orient adoption should be boring: start one shared daemon, give each local
agent a small rule, and let returned follow-up requests drive search, reads,
and query-plan recovery.

For setup and shared-runtime operations, use [Shared Daemon](shared-daemon.md).
For transport details and tool schemas, use [Agent Protocol](agent-protocol.md).

## Minimal Agent Rule

Generate the live rule with:

```bash
export ORIENT_SHARDS=/path/to/local/cache/orient-shards

orient agent-instructions --profile codex --index-dir "$ORIENT_SHARDS"
```

Keep that cache path local to the machine running the agents; the generated
rule should not contain private workspace layouts.

Place the generated snippet in the local rule file read by the coding agent.
The snippet is intentionally tool-agnostic: it tells the agent to use
JSON-lines/MCP calls and returned follow-up requests before repeated shell
scans. Use `--profile codex`, `--profile claude`, `--profile amp`, or
`--profile generic` to tailor the placement hint without changing the search
protocol or generated tool calls.

The rule should tell agents:

- Prefer Orient before `rg`, `find`, `ls`, or `cat` for code discovery.
- Start with `daemon_status` or `agent_guide`.
- Use `search_auto` for normal lookup and `search_auto_batch` for alternate
  query phrasings.
- For CLI use, prefer bare `orient search-auto ...`; it uses the shared TCP
  daemon first when no explicit target is supplied and falls back locally when
  no daemon is reachable. From inside a git checkout, bare CLI searches are
  scoped to that checkout. Use `--no-daemon` only when forcing
  current-directory fallback.
- For JSON-lines or MCP-style clients, include `cwd` on no-target search, map,
  plan, symbol, read, and related-file calls so the shared daemon applies the
  same checkout scope.
- Include `cwd` on `daemon_status` when asking for copyable default requests;
  the returned map, search, batch, and query-plan calls will keep the active
  checkout scope and set `refresh_if_stale:true`.
- Follow returned `read_*`, `related_*`, `repo_map_request`, and
  `query_plan_request` objects directly.
- Prefer `next_read_batch_request` after `search_auto` or `search_auto_batch`;
  it points at normal hits when present and retry hits after automatic repair.
- When `next_action` is present, run `next_action.request` first; it chooses
  between refresh, read, retry, and map follow-ups.
- When opening context manually from a line inside a definition, pass
  `scope:"symbol"` on `read_range` or `read_ranges` so the returned window
  starts from the nearest enclosing function, class, or type definition.
- Use `refresh_if_stale:true` when indexed files may have changed. With `cwd`,
  Orient refreshes only the active checkout's shard.
- Treat generated hits as searchable but lower-priority by default; use
  `generated:true` / `is:generated` only when intentionally inspecting
  generated output.
- Fall back to shell search only when the daemon is unavailable or the plan is
  not useful.

## Copyable Requests

```bash
printf '%s\n' \
  '{"id":"guide","tool":"agent_guide","arguments":{}}' \
  '{"id":"status","tool":"daemon_status","arguments":{"cwd":"/path/to/current/repo"}}' \
  | orient client-jsonl
```

Pass `details:true` to `daemon_status` only when an adapter needs cached paths
or per-target runtime details.

```bash
printf '%s\n' \
  '{"id":"search","tool":"search_auto","arguments":{"query":"repo:service branch:main symbol:SessionManager token","limit":10,"explain":true,"refresh_if_stale":true}}' \
  | orient client-jsonl
```

```bash
printf '%s\n' \
  '{"id":"searches","tool":"search_auto_batch","arguments":{"queries":["repo:service symbol:SessionManager token","origin:example/service path:auth token","repo:service mode:any SessionManager token"],"limit":10,"explain":true,"refresh_if_stale":true}}' \
  | orient client-jsonl
```

On a registered shard daemon, `search_auto_batch` resolves all query scopes and
refreshes the selected shard roots once before running the batch, so agents can
try alternate phrasings without paying repeated freshness scans.

## Expected Loop

1. Call `agent_guide` or `tool_manifest`.
2. Search with `search_auto` or `search_auto_batch`.
3. Read top hits with returned bounded range requests.
4. Use related-file and related-symbol requests before opening neighboring
   files manually.
5. Use the query plan for empty or noisy results; safe retry requests are
   included when Orient can suggest one.
6. Use repo maps when the agent needs entrypoints, tests, commands, or top
   symbols for the selected surface.
