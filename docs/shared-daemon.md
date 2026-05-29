# Shared Daemon

Run one warmed Orient daemon per machine or workspace family so local agents
reuse the same repo maps, indexes, query plans, and bounded reads.

## Start

For several repos:

```bash
orient ensure-shards \
  --discover-root ~/code \
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

## Agent Rule

Generate a local rule for the current daemon target:

```bash
orient agent-instructions --index-dir /tmp/orient-shards
```

The important behavior is:

- Start with `daemon_status` or `agent_guide`.
- Use `search_auto` for normal lookup and `search_auto_batch` for alternate
  query phrasings.
- Follow returned `read_*`, `related_*`, `repo_map_request`, and
  `query_plan_request` objects directly.
- Pass `refresh_if_stale:true` when live files may have changed.
- Fall back to shell search only when Orient is unavailable or unhelpful.

## Operations

Check local readiness:

```bash
orient doctor --index-dir /tmp/orient-shards
orient daemon-status
orient daemon-status --format json
```

The compact status is meant for humans. JSON status adds warmed-target details
and copyable default requests for adapters.

Refresh explicitly when needed:

```bash
orient refresh-index --repo /path/to/repo --index /tmp/orient.index
orient refresh-shards --index-dir /tmp/orient-shards
```

`ensure-shards` is the preferred shared-directory bootstrap. It adds missing
repos and refreshes existing shards without shrinking the shard set. Use
`index-shards --force` only when intentionally replacing a shard directory.
