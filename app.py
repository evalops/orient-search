from __future__ import annotations

from datetime import datetime, time, timezone
from pathlib import Path

import pandas as pd
import plotly.express as px
import streamlit as st

from agent_logs import DEFAULT_ROOTS, ScanOptions, aid_suggestions, scan_logs, shell_command_parts


st.set_page_config(page_title="Agent JSONL Explorer", layout="wide")


def to_epoch(date_value, end_of_day: bool = False):
    if not date_value:
        return None
    t = time.max if end_of_day else time.min
    return datetime.combine(date_value, t).replace(tzinfo=timezone.utc).timestamp()


@st.cache_data(show_spinner=False)
def cached_scan(roots, include_raw_snippets, max_files, max_file_mb, min_mtime, max_mtime):
    return scan_logs(
        ScanOptions(
            roots=tuple(roots),
            include_raw_snippets=include_raw_snippets,
            max_files=max_files,
            max_file_mb=max_file_mb,
            min_mtime=min_mtime,
            max_mtime=max_mtime,
        )
    )


st.title("Agent JSONL Explorer")

with st.sidebar:
    st.header("Scan")
    roots_text = st.text_area("JSONL roots", "\n".join(str(p) for p in DEFAULT_ROOTS), height=88)
    roots = [line.strip() for line in roots_text.splitlines() if line.strip()]
    max_files = st.number_input("Newest files to scan", min_value=10, max_value=10000, value=100, step=50)
    max_file_mb = st.number_input("Max file size (MB)", min_value=1, max_value=1000, value=20, step=10)
    include_raw = st.checkbox("Keep raw snippets", value=True)
    date_range = st.date_input("File modified date range", value=())
    date_min = date_range[0] if len(date_range) >= 1 else None
    date_max = date_range[1] if len(date_range) >= 2 else None
    min_mtime = to_epoch(date_min) if date_min else None
    max_mtime = to_epoch(date_max, end_of_day=True) if date_max else None
    if st.button("Clear cache"):
        st.cache_data.clear()

with st.spinner("Scanning JSONL files..."):
    data = cached_scan(tuple(roots), include_raw, int(max_files), int(max_file_mb), min_mtime, max_mtime)

files = data["files"]
sessions = data["sessions"]
events = data["events"]
calls = data["tool_calls"].copy()

if calls.empty:
    st.warning("No tool calls found in the selected files.")
    st.stop()

calls["timestamp_dt"] = pd.to_datetime(calls["timestamp"], errors="coerce", utc=True)
calls["date"] = calls["timestamp_dt"].dt.date

with st.sidebar:
    st.header("Filter")
    sources = st.multiselect("Sources", sorted(calls["source"].dropna().unique()), default=sorted(calls["source"].dropna().unique()))
    families = st.multiselect("Action families", sorted(calls["action_family"].dropna().unique()), default=sorted(calls["action_family"].dropna().unique()))
    top_tool_options = sorted(calls["tool_name"].dropna().unique())
    tools = st.multiselect("Tools", top_tool_options)
    failed_only = st.checkbox("Failed only", value=False)

filtered = calls[calls["source"].isin(sources) & calls["action_family"].isin(families)]
if tools:
    filtered = filtered[filtered["tool_name"].isin(tools)]
if failed_only:
    filtered = filtered[filtered["failed"]]

total_calls = len(filtered)
failed_calls = int(filtered["failed"].sum()) if total_calls else 0
failure_rate = failed_calls / total_calls if total_calls else 0
unique_sessions = filtered["session_id"].nunique()

metric_cols = st.columns(5)
metric_cols[0].metric("Files scanned", f"{len(files):,}")
metric_cols[1].metric("Sessions", f"{unique_sessions:,}")
metric_cols[2].metric("Tool calls", f"{total_calls:,}")
metric_cols[3].metric("Failed calls", f"{failed_calls:,}")
metric_cols[4].metric("Failure rate", f"{failure_rate:.1%}")

tab_overview, tab_tools, tab_failures, tab_commands, tab_sessions, tab_raw = st.tabs(
    ["Overview", "Tools", "Failures", "Commands", "Sessions", "Raw"]
)

with tab_overview:
    left, right = st.columns([1, 1])
    by_family = filtered.groupby(["action_family", "source"], as_index=False).size()
    left.plotly_chart(px.bar(by_family, x="action_family", y="size", color="source", barmode="group", title="Tool calls by action family"), width='stretch')

    if filtered["date"].notna().any():
        by_day = filtered.groupby(["date", "source"], as_index=False).agg(calls=("call_id", "count"), failures=("failed", "sum"))
        right.plotly_chart(px.line(by_day, x="date", y="calls", color="source", markers=True, title="Calls over time"), width='stretch')
    else:
        right.info("No timestamps available for a timeline.")

    st.subheader("Suggested ways to aid agents")
    suggestions = aid_suggestions(filtered)
    if suggestions:
        st.dataframe(pd.DataFrame(suggestions), width='stretch', hide_index=True)
    else:
        st.success("No obvious recurring failure pattern in the current filter.")

with tab_tools:
    tool_stats = (
        filtered.groupby(["source", "tool_name", "action_family"], as_index=False)
        .agg(calls=("call_id", "count"), failures=("failed", "sum"), sessions=("session_id", "nunique"))
        .sort_values("calls", ascending=False)
    )
    tool_stats["failure_rate"] = tool_stats["failures"] / tool_stats["calls"]
    top_n = st.slider("Top N tools", 5, 100, 30)
    st.plotly_chart(px.bar(tool_stats.head(top_n), x="calls", y="tool_name", color="source", orientation="h", title="Most-used tools"), width='stretch')
    st.dataframe(tool_stats, width='stretch', hide_index=True)

with tab_failures:
    failure_rows = filtered[filtered["failed"]].copy()
    if failure_rows.empty:
        st.success("No failures in the current filter.")
    else:
        fail_stats = (
            failure_rows.groupby(["source", "tool_name", "action_family"], as_index=False)
            .agg(failures=("call_id", "count"), sessions=("session_id", "nunique"))
            .sort_values("failures", ascending=False)
        )
        st.plotly_chart(px.bar(fail_stats.head(40), x="failures", y="tool_name", color="source", orientation="h", title="Failure hotspots"), width='stretch')
        show_cols = ["source", "tool_name", "action_family", "timestamp", "exit_code", "status", "status_out", "input_summary", "stderr", "output", "path", "line_number"]
        st.dataframe(failure_rows[[c for c in show_cols if c in failure_rows.columns]].sort_values("timestamp", ascending=False), width='stretch', hide_index=True)

with tab_commands:
    commandish = filtered[filtered["action_family"].eq("shell") | filtered["tool_name"].str.lower().str.contains("exec|bash|shell", na=False)].copy()
    if commandish.empty:
        st.info("No shell-like tool calls in the current filter.")
    else:
        parts = commandish["input"].map(shell_command_parts).apply(pd.Series)
        commandish = pd.concat([commandish.reset_index(drop=True), parts.reset_index(drop=True)], axis=1)
        commandish["program"] = commandish["cmd"].str.extract(r"^\s*([^\s;&|]+)")
        program_stats = commandish.groupby("program", as_index=False).agg(calls=("call_id", "count"), failures=("failed", "sum")).sort_values("calls", ascending=False)
        program_stats["failure_rate"] = program_stats["failures"] / program_stats["calls"]
        st.plotly_chart(px.bar(program_stats.head(40), x="calls", y="program", orientation="h", title="Shell programs used most"), width='stretch')
        st.dataframe(commandish[["source", "timestamp", "tool_name", "program", "cmd", "workdir", "exit_code", "failed", "stderr", "path", "line_number"]], width='stretch', hide_index=True)

with tab_sessions:
    session_ids = filtered["session_id"].unique()
    session_view = sessions[sessions["session_id"].isin(session_ids)].copy() if not sessions.empty else pd.DataFrame()
    if not session_view.empty:
        session_view["failure_rate"] = session_view["failed_tool_calls"] / session_view["tool_calls"].replace(0, pd.NA)
        st.dataframe(session_view.sort_values("tool_calls", ascending=False), width='stretch', hide_index=True)
    else:
        st.info("No session metadata for the current filter.")

with tab_raw:
    st.subheader("Tool calls")
    st.dataframe(filtered.sort_values("timestamp", ascending=False), width='stretch', hide_index=True)
    st.subheader("Events")
    if not events.empty:
        selected_sessions = st.multiselect("Raw event sessions", sorted(filtered["session_id"].dropna().unique())[:500], default=[])
        raw = events[events["session_id"].isin(selected_sessions)] if selected_sessions else events.head(1000)
        st.dataframe(raw, width='stretch', hide_index=True)

st.caption("Tip: start with the default fast scan, then widen files or size once the cache is warm.")
