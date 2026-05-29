# Shared Daemon

Orient is most useful when many local agents share one warmed runtime instead of
each session repeatedly walking the same repos with shell commands.

## Start One Shared Runtime

For a multi-repo workspace:

```bash
orient ensure-shards \
  --discover-root ~/code \
  --output-dir /tmp/orient-shards \
  --family-limit 2

orient serve-tcp \
  --addr 127.0.0.1:8796 \
  --index-dir /tmp/orient-shards
```

For a single repo:

```bash
orient ensure-index --repo /path/to/repo --index /tmp/orient.index
orient serve-tcp --addr 127.0.0.1:8796 --index /tmp/orient.index
```

Unix sockets are also supported:

```bash
orient serve-unix --socket /tmp/orient.sock --index-dir /tmp/orient-shards
orient daemon-status --socket /tmp/orient.sock
orient client-jsonl --socket /tmp/orient.sock
```

## Give Agents One Rule

Generate the current local instruction:

```bash
orient agent-instructions --index-dir /tmp/orient-shards
```

The important behavior is:

- Call `daemon_status` first.
- Trust `search_auto_default` when exactly one target is warmed.
- Use `default_requests` for the first repo map, search, and query-plan calls.
- Use returned `read_request`, `read_batch_request`, `related_request`, and
  `related_symbols_request` objects directly.
- Prefer `search_auto_batch` for alternate query phrasings.
- Use `refresh_if_stale:true` when live files may have changed.

## Health Check

Run:

```bash
orient doctor --index-dir /tmp/orient-shards
```

`doctor` checks local tool availability, daemon reachability, index/shard
freshness, and emits copyable repair/start commands. It does not inspect or
record agent sessions.

## Freshness Model

Shared use has two freshness layers:

- `index_status` and `shard_status` compare persisted indexes to live files.
- The daemon cache fingerprints loaded index and manifest files and reloads
  changed persisted files automatically.

For one-call repair, pass `refresh_if_stale:true` to indexed or shard search
tools. For explicit maintenance, run:

```bash
orient refresh-index --repo /path/to/repo --index /tmp/orient.index
orient refresh-shards --index-dir /tmp/orient-shards
```

Shard writes use a bounded local writer lock. `index-shards` refuses to shrink
an existing shard directory unless `--force` is passed; `ensure-shards` is the
preferred command for shared directories because it adds or refreshes without
accidentally dropping existing shards.

## Recommended Agent Loop

1. Call `daemon_status` or `agent_guide`.
2. Call `search_auto` or `search_auto_batch`.
3. Read top hits with the returned bounded range requests.
4. Use related-file and related-symbol follow-ups before opening random files.
5. If results are empty or noisy, call the returned query-plan request and use
   safe retry requests.
6. Fall back to shell search only when the daemon is unavailable or the plan is
   not useful.
