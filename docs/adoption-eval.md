# Adoption Eval

The useful question is not whether Orient can beat `rg` in isolation. The useful
question is whether agents reach the right files faster and waste fewer local
tool calls.

## Protocol

Run the same 20 realistic repo-editing tasks twice:

- Baseline: no Orient instruction; normal `rg`, `find`, `ls`, and `cat`.
- Orient: daemon running; agent instruction from `docs/agent-adoption.md`.

Use tasks that require file discovery before editing, not tasks where the target
file is named in the prompt.

## Capture

For each task, record:

- Time to first relevant file.
- Local-search command count.
- Wrong file opens before the first relevant file.
- Total tool calls before first edit.
- Whether the final edit succeeded.
- Wall-clock time.

## Success Bar

Orient is worth making default when it reduces wasted local discovery without
hurting edit success. A strong result is:

- Fewer repeated `rg`, `find`, `ls`, and `cat` calls.
- Fewer wrong file opens.
- Lower median time to first relevant file.
- Equal or better edit success.
- Clear query-plan diagnostics on failed searches.

