# Agent Adoption

Orient adoption should be boring: start one shared daemon, give each local agent
a small instruction snippet, and let returned follow-up requests drive search,
reads, and query-plan recovery. Keep the guidance local and code-search focused;
do not put machine-specific layouts, prompts, transcripts, memories, or tool
history into shared docs.

For setup and shared-runtime operations, use [Shared Daemon](shared-daemon.md).
For transport details and tool schemas, use [Agent Protocol](agent-protocol.md).

## Minimal Instructions

Generate the live snippet with:

```bash
export ORIENT_SHARDS=/path/to/local/cache/orient-shards

orient agent-instructions --profile generic --index-dir "$ORIENT_SHARDS"
```

Keep that cache path local to the machine running the agents; the generated
snippet should not contain machine-specific layouts.

Place the generated snippet in the local instruction file read by the coding
agent. The snippet is intentionally tool-agnostic: it tells the agent to use
JSON-lines/MCP calls and returned follow-up requests before repeated shell
scans. Use `--profile generic` for neutral output, or an explicit adapter
profile when you want a placement hint for that agent. The selected profile
does not change the search protocol or generated tool calls.

The snippet should tell agents:

- Use Orient before `rg`, `find`, `ls`, `grep`, `cat`, or ad hoc filesystem
  scans for code discovery and bounded context reads.
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
- For direct definition jumps, use `find_symbol` with `include_read_batch:true`
  or `find_symbol_batch`, then run the returned `read_batch_request`.
- Include `cwd` on `daemon_status` when asking for copyable default requests;
  the returned map, search, batch, and query-plan calls will keep the active
  checkout scope and set `refresh_if_stale:true`.
- Follow returned `read_*`, `related_*`, `repo_map_request`, and
  `query_plan_request` objects directly. `search_auto_batch`, `search_batch`,
  `indexed_search_batch`, and `search_shards_batch` return these per item.
- When Orient returns a usable `next_action`, `read_request`, or
  `read_batch_request`, run that request instead of translating it into a shell
  search/read command.
- Prefer `next_read_batch_request` after `search_auto` or `search_auto_batch`;
  it points at normal hits when present and retry hits after automatic repair.
- When `next_action` is present, run `next_action.request` first; it chooses
  between refresh, read, retry, map, and empty-result query-plan follow-ups.
- Use `query_plan_summary` on `search_auto` / `search_auto_batch` and `summary`
  on plan batch items before parsing full nested query plans.
- When opening context manually from a line inside a definition, pass
  `scope:"symbol"` on `read_range` or `read_ranges` so the returned window
  starts from the nearest enclosing function, class, or type definition.
- Use `refresh_if_stale:true` when indexed files may have changed. With `cwd`,
  Orient refreshes only the active checkout's shard.
- Treat generated hits as searchable but lower-priority by default; use
  `generated:true` / `is:generated` only when intentionally inspecting
  generated output.
- Fall back to shell search only when Orient is unavailable or its query plan is
  not useful for the task.

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
  '{"id":"search","tool":"search_auto","arguments":{"query":"repo:service branch:main symbol:SessionManager token","limit":10,"explain":true,"refresh_if_stale":true,"retry_if_empty":true}}' \
  | orient client-jsonl
```

```bash
printf '%s\n' \
  '{"id":"searches","tool":"search_auto_batch","arguments":{"queries":["repo:service symbol:SessionManager token","origin:example/service path:auth token","repo:service mode:any SessionManager token"],"limit":10,"explain":true,"refresh_if_stale":true,"retry_if_empty":true}}' \
  | orient client-jsonl
```

On a registered shard daemon, `search_auto_batch` resolves all query scopes and
refreshes the selected shard roots once before running the batch, so agents can
try alternate phrasings without paying repeated freshness scans.

## Expected Loop

1. Call `agent_guide` or `tool_manifest`.
2. Search with `search_auto` or `search_auto_batch`.
3. Read top hits with returned bounded range requests.
4. Use compact `next_action` and query-plan summary fields before digging into
   full diagnostic plans.
5. Use related-file and related-symbol requests before opening neighboring
   files manually.
6. Use the query plan for empty or noisy results; safe retry requests are
   included when Orient can suggest one.
7. Use repo maps when the agent needs entrypoints, tests, commands, or top
   symbols for the selected surface.
