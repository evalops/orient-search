# Shared Daemon

Run one warmed Orient daemon for the repos agents are actively editing. Local
agents share repo maps, indexes, query plans, and bounded reads without each one
rescanning the same files. The daemon stays local and does not collect telemetry.

## Start

For several repos:

```bash
orient ensure-shards \
  --discover-root /path/to/workspaces \
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

Unix sockets are available when a TCP port is inconvenient:

```bash
orient serve-unix --socket /tmp/orient.sock --index-dir /tmp/orient-shards
orient client-jsonl --socket /tmp/orient.sock
```

## Agent Setup

Generate a local rule for the current daemon target:

```bash
orient agent-instructions --index-dir /tmp/orient-shards
```

The generated rule should keep agents on this loop:

- Start with `daemon_status` or `agent_guide`.
- Use `search_auto` for normal lookup and `search_auto_batch` for alternate
  query phrasings.
- From shell, use bare `orient search-auto ...` or `orient search-auto-batch
  ...`. They try the default TCP daemon first and fall back to the current
  directory when no daemon is reachable.
- From JSON-lines or MCP-style clients, pass `cwd` on no-target search, map,
  plan, symbol, read, and related-file calls. The daemon scopes those requests
  to the active checkout.
- Follow returned `read_*`, `related_*`, `repo_map_request`, and
  `query_plan_request` objects directly.
- Pass `refresh_if_stale:true` when live files may have changed. With `cwd`,
  Orient refreshes only the active checkout's shard.
- Call `shard_status` with `cwd` or `repo_filter` when only one repo's
  freshness matters.
- Treat generated bundle output as searchable but lower-priority by default;
  use `generated:true` only when intentionally inspecting generated files.
- Fall back to shell search only when Orient is unavailable or unhelpful.

## Operations

Check local readiness:

```bash
orient doctor --index-dir /tmp/orient-shards
orient daemon-status
orient daemon-status --format json
```

The compact status is meant for humans. JSON status adds warmed-target summaries
and copyable default requests.

Refresh explicitly when needed:

```bash
orient refresh-index --repo /path/to/repo --index /tmp/orient.index
orient refresh-shards --index-dir /tmp/orient-shards
```

`ensure-shards` is the preferred shared-directory bootstrap. It adds missing
repos and refreshes existing shards without shrinking the shard set. Use
`index-shards --force` only when intentionally replacing a shard directory.
Keep shard directories in a local cache, not in source control.

## Local Data

Shard directories contain source snapshots and line tables so reads can be
served without reopening every file. Treat them like local build artifacts:
place them in a cache directory, do not commit them, and do not copy them to
shared storage unless the indexed source is allowed there too.
