# Agent JSONL Explorer

Streamlit explorer for Codex and Claude Code JSONL logs.

## Run

```bash
cd /Users/jonathanhaas/agent-jsonl-explorer
python3 -m venv .venv
. .venv/bin/activate
pip install -r requirements.txt
streamlit run app.py
```

The app scans these default roots:

- `/Users/jonathanhaas/.codex`
- `/Users/jonathanhaas/.claude`

Use the sidebar to limit files, date ranges, sources, tools, and whether raw text snippets are retained.
