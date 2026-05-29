# Shared Daemon

Run one shared Orient daemon for the repos agents are actively editing. Local
agents share repo maps, indexes, query plans, and bounded reads without each one
rescanning the same files. The daemon stays local and does not collect telemetry.

In this doc, "shared" means shared by local clients on the same machine. Orient
is a local code-search service and does not collect telemetry.

## Start

For several repos, build or refresh a shard directory and serve it locally:

```bash
export ORIENT_WORKSPACES=/path/to/workspaces
export ORIENT_SHARDS=/path/to/local/cache/orient-shards
export ORIENT_INDEX=/path/to/local/cache/orient.index
export ORIENT_REPO=/path/to/repo
export ORIENT_SOCKET=/path/to/local/cache/orient.sock

orient ensure-shards \
  --discover-root "$ORIENT_WORKSPACES" \
  --output-dir "$ORIENT_SHARDS" \
  --family-limit 2

orient serve-tcp \
  --addr 127.0.0.1:8796 \
  --index-dir "$ORIENT_SHARDS"
```

`--index-dir` registers the shard manifest and loads individual repo indexes
lazily when a search, read, map, or symbol request touches them. The daemon
keeps at most 64 ready indexes by default; pass `--max-cached-indexes N` when a
shared daemon should stay tighter or keep more hot repos resident. Use
`--warm-index-dir "$ORIENT_SHARDS"` only when you explicitly want shard
indexes loaded at startup.

For one repo, use a single persisted index:

```bash
orient ensure-index --repo "$ORIENT_REPO" --index "$ORIENT_INDEX"
orient serve-tcp --addr 127.0.0.1:8796 --index "$ORIENT_INDEX"
```

Unix sockets are available when a TCP port is inconvenient:

```bash
orient serve-unix --socket "$ORIENT_SOCKET" --index-dir "$ORIENT_SHARDS"
orient client-jsonl --socket "$ORIENT_SOCKET"
```

## Agent Setup

Generate a short local rule for the current daemon target:

```bash
orient agent-instructions --profile codex --index-dir "$ORIENT_SHARDS"
```

The generated rule should keep agents on this loop. See
[Agent adoption](agent-adoption.md) for adapter examples.

- Start with `daemon_status` or `agent_guide`.
- Use `search_auto` for normal lookup and `search_auto_batch` for alternate
  query phrasings.
- From shell, use bare `orient search-auto ...` or `orient search-auto-batch
  ...`. They try the default TCP daemon first and fall back to the current
  directory when no daemon is reachable.
- From JSON-lines or MCP-style clients, pass `cwd` on no-target search, map,
  plan, symbol, read, and related-file calls. The daemon scopes those requests
  to the active checkout.
- Call `daemon_status` with `cwd` when you want copyable `default_requests`
  for the active checkout; the returned map, search, batch, and query-plan
  requests keep that same scope and set `refresh_if_stale:true`.
- Follow returned `read_*`, `related_*`, `repo_map_request`, and
  `query_plan_request` objects directly.
- Pass `refresh_if_stale:true` when live files may have changed. With `cwd`,
  Orient refreshes only the active checkout's shard. For `search_auto_batch`,
  Orient coalesces refresh across the batch's selected shard roots before
  running the searches.
- Branch, origin, and worktree metadata are part of shard freshness. If a
  checkout moves branches without file changes, `refresh_if_stale:true` updates
  the manifest before branch-scoped searches run.
- Call `shard_status` with `cwd` or `repo_filter` when only one repo's
  freshness matters.
- Treat generated bundle output as searchable but lower-priority by default;
  use `generated:true` only when intentionally inspecting generated files.
- Fall back to shell search only when Orient is unavailable or unhelpful.

## Check And Refresh

Check local readiness:

```bash
orient doctor --index-dir "$ORIENT_SHARDS"
orient daemon-status
orient daemon-status --format json
```

The compact CLI status is meant for humans. The JSON-lines `daemon_status` tool
is also compact by default; pass `details:true` only when cached paths and
per-target details are needed. When called with `cwd`, its default requests are
scoped to the active checkout and set `refresh_if_stale:true`.

Refresh explicitly when needed:

```bash
orient refresh-index --repo "$ORIENT_REPO" --index "$ORIENT_INDEX"
orient refresh-shards --index-dir "$ORIENT_SHARDS"
```

`ensure-shards` is the preferred shared-directory bootstrap. It adds missing
repos and refreshes existing shards without shrinking the shard set. Use
`index-shards --force` only when intentionally replacing a shard directory.
Keep shard directories in a local cache, not in source control.

## Local Data Boundary

Shard directories contain source snapshots and line tables so reads can be
served without reopening every file. Treat them like local build artifacts:
place them in a cache directory, do not commit them, and do not copy them to
shared storage unless the indexed source is allowed there too.

The daemon cache contains code-search artifacts only: opened index files,
derived postings, repo metadata, and freshness state. It is not a general agent
state store.

Public docs and examples should use environment variables or neutral
placeholders. Keep machine-specific paths, user names, and private workspace
layouts out of shared documentation.
