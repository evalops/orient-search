# Agent Adoption

Orient adoption should be boring: start one shared daemon, give each local
agent a small rule, and let returned follow-up requests drive search, reads,
and query-plan recovery.

For setup and shared-runtime operations, use [Shared Daemon](shared-daemon.md).
For transport details and tool schemas, use [Agent Protocol](agent-protocol.md).

## Minimal Agent Rule

Generate the live rule with:

```bash
orient agent-instructions --index-dir /tmp/orient-shards
```

The rule should tell agents:

- Prefer Orient before `rg`, `find`, `ls`, or `cat` for code discovery.
- Start with `daemon_status` or `agent_guide`.
- Use `search_auto` for normal lookup and `search_auto_batch` for alternate
  query phrasings.
- For CLI use, prefer bare `orient search-auto ...`; it uses the warm TCP
  daemon first when no explicit target is supplied and falls back locally when
  no daemon is reachable. From inside a git checkout, bare CLI searches are
  scoped to that checkout in the shared shard daemon. Use `--no-daemon` only
  when forcing current-directory fallback.
- For JSON-lines or MCP-style clients, include `"cwd"` on no-target
  `search`, `search_batch`, `search_auto`, `search_auto_batch`, `repo_map`,
  `search_plan`, and `find_symbol` calls so the shared daemon applies the same
  current-checkout scope. Include it on no-target `read_range`, `read_ranges`,
  `related_files`, and `related_symbols` when opening context manually rather
  than following a returned request.
- Follow returned `read_*`, `related_*`, `repo_map_request`, and
  `query_plan_request` objects directly.
- Use `refresh_if_stale:true` when indexed files may have changed. With `cwd`
  on a shared shard daemon, Orient refreshes the active checkout's shard rather
  than rebuilding every warmed repo.
- Treat generated hits as searchable but lower-priority by default; use
  `generated:true` / `is:generated` only when intentionally inspecting
  generated output.
- Fall back to shell search only when the daemon is unavailable or the plan is
  not useful.

## Copyable Requests

```bash
printf '%s\n' \
  '{"id":"guide","tool":"agent_guide","arguments":{}}' \
  '{"id":"status","tool":"daemon_status","arguments":{}}' \
  | orient client-jsonl
```

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
