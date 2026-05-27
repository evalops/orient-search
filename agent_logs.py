from __future__ import annotations

import hashlib
import json
import os
import re
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable

import pandas as pd


DEFAULT_ROOTS = [Path.home() / ".codex", Path.home() / ".claude"]
TEXT_LIMIT = 900


@dataclass(frozen=True)
class ScanOptions:
    roots: tuple[str, ...]
    include_raw_snippets: bool = True
    max_files: int | None = 500
    max_file_mb: int | None = 50
    min_mtime: float | None = None
    max_mtime: float | None = None


def discover_jsonl_files(options: ScanOptions) -> list[Path]:
    files: list[Path] = []
    for root_text in options.roots:
        root = Path(root_text).expanduser()
        if not root.exists():
            continue
        for path in root.rglob("*.jsonl"):
            try:
                stat = path.stat()
            except OSError:
                continue
            if options.min_mtime and stat.st_mtime < options.min_mtime:
                continue
            if options.max_mtime and stat.st_mtime > options.max_mtime:
                continue
            if options.max_file_mb and stat.st_size > options.max_file_mb * 1024 * 1024:
                continue
            files.append(path)
    files.sort(key=lambda p: p.stat().st_mtime, reverse=True)
    if options.max_files:
        files = files[: options.max_files]
    return files


def scan_logs(options: ScanOptions) -> dict[str, pd.DataFrame]:
    files = discover_jsonl_files(options)
    sessions: dict[str, dict[str, Any]] = {}
    events: list[dict[str, Any]] = []
    calls: list[dict[str, Any]] = []
    outputs_by_call: dict[tuple[str, str], dict[str, Any]] = {}

    for file_index, path in enumerate(files):
        source = infer_source(path)
        current_turn_id = None
        current_session_id = session_id_from_path(path, source)
        file_stat = safe_stat(path)
        file_key = stable_id(str(path))

        sessions.setdefault(
            current_session_id,
            {
                "session_id": current_session_id,
                "source": source,
                "path": str(path),
                "project": project_from_path(path, source),
                "cwd": None,
                "started_at": None,
                "updated_at": datetime.fromtimestamp(file_stat.st_mtime, timezone.utc).isoformat() if file_stat else None,
                "bytes": file_stat.st_size if file_stat else None,
                "lines": 0,
                "tool_calls": 0,
                "failed_tool_calls": 0,
            },
        )

        try:
            handle = path.open("r", encoding="utf-8", errors="replace")
        except OSError:
            continue

        with handle:
            for line_number, line in enumerate(handle, start=1):
                line = line.rstrip("\n")
                if not line:
                    continue
                sessions[current_session_id]["lines"] += 1
                try:
                    record = json.loads(line)
                except json.JSONDecodeError as exc:
                    events.append(
                        event_row(
                            source,
                            current_session_id,
                            current_turn_id,
                            path,
                            line_number,
                            "parse_error",
                            None,
                            None,
                            {"error": str(exc)},
                            options.include_raw_snippets,
                        )
                    )
                    continue

                if source == "codex":
                    parsed = parse_codex_record(record, current_session_id, current_turn_id, path, line_number, options.include_raw_snippets)
                elif source == "claude":
                    parsed = parse_claude_record(record, current_session_id, current_turn_id, path, line_number, options.include_raw_snippets)
                else:
                    parsed = parse_unknown_record(record, current_session_id, current_turn_id, path, line_number, options.include_raw_snippets)

                if parsed.get("session_updates"):
                    sessions[current_session_id].update({k: v for k, v in parsed["session_updates"].items() if v is not None})
                    if parsed["session_updates"].get("session_id") and parsed["session_updates"]["session_id"] != current_session_id:
                        current_session_id = parsed["session_updates"]["session_id"]
                        sessions.setdefault(current_session_id, sessions[session_id_from_path(path, source)].copy())
                        sessions[current_session_id]["session_id"] = current_session_id

                current_turn_id = parsed.get("turn_id") or current_turn_id
                events.extend(parsed.get("events", []))

                for call in parsed.get("calls", []):
                    call["file_index"] = file_index
                    calls.append(call)
                    sessions[current_session_id]["tool_calls"] += 1

                for output in parsed.get("outputs", []):
                    outputs_by_call[(current_session_id, output.get("call_id") or output.get("tool_use_id") or "")] = output

    calls_df = pd.DataFrame(calls)
    outputs_df = pd.DataFrame(outputs_by_call.values())
    if not calls_df.empty:
        calls_df = attach_outputs(calls_df, outputs_df)
        calls_df["failed"] = calls_df.apply(classify_failure, axis=1)
        calls_df["action_family"] = calls_df["tool_name"].map(action_family)
        calls_df["input_summary"] = calls_df.apply(summarize_input, axis=1)
        for session_id, group in calls_df.groupby("session_id"):
            sessions.setdefault(session_id, {"session_id": session_id})
            sessions[session_id]["tool_calls"] = len(group)
            sessions[session_id]["failed_tool_calls"] = int(group["failed"].sum())
    else:
        calls_df = pd.DataFrame(columns=call_columns())

    events_df = pd.DataFrame(events)
    sessions_df = pd.DataFrame(sessions.values())
    raw_df = events_df.copy()

    return {
        "files": pd.DataFrame([file_row(p) for p in files]),
        "sessions": sessions_df,
        "events": events_df,
        "tool_calls": calls_df,
        "raw_events": raw_df,
    }


def parse_codex_record(record: dict[str, Any], session_id: str, turn_id: str | None, path: Path, line_number: int, keep_raw: bool) -> dict[str, Any]:
    top_type = record.get("type")
    payload = record.get("payload")
    timestamp = record.get("timestamp")
    out = {"events": [], "calls": [], "outputs": [], "session_updates": {}, "turn_id": turn_id}

    if top_type == "session_meta" and isinstance(payload, dict):
        out["session_updates"] = {
            "session_id": payload.get("id") or session_id,
            "cwd": payload.get("cwd"),
            "started_at": payload.get("timestamp") or timestamp,
            "model": payload.get("model") or payload.get("model_provider"),
            "originator": payload.get("originator"),
            "cli_version": payload.get("cli_version"),
        }

    if top_type == "turn_context" and isinstance(payload, dict):
        out["turn_id"] = payload.get("turn_id") or turn_id
        out["session_updates"] = {"cwd": payload.get("cwd")}

    if top_type == "event_msg" and isinstance(payload, dict):
        event_type = payload.get("type")
        out["turn_id"] = payload.get("turn_id") or out["turn_id"]
        out["events"].append(event_row("codex", session_id, out["turn_id"], path, line_number, top_type, event_type, timestamp, payload, keep_raw))
        if event_type in {"exec_command_end", "patch_apply_end", "mcp_tool_call_end", "web_search_end"}:
            out["outputs"].append(codex_event_output(payload, session_id, timestamp, path, line_number))

    elif top_type == "response_item" and isinstance(payload, dict):
        item_type = payload.get("type")
        out["events"].append(event_row("codex", session_id, out["turn_id"], path, line_number, top_type, item_type, timestamp, payload, keep_raw))
        if item_type in {"function_call", "custom_tool_call", "web_search_call", "tool_search_call"}:
            out["calls"].append(codex_call_row(payload, session_id, out["turn_id"], timestamp, path, line_number))
        elif item_type in {"function_call_output", "custom_tool_call_output", "tool_search_output"}:
            out["outputs"].append(codex_response_output(payload, session_id, timestamp, path, line_number))
    else:
        out["events"].append(event_row("codex", session_id, out["turn_id"], path, line_number, top_type, None, timestamp, payload or record, keep_raw))

    return out


def parse_claude_record(record: dict[str, Any], session_id: str, turn_id: str | None, path: Path, line_number: int, keep_raw: bool) -> dict[str, Any]:
    top_type = record.get("type")
    timestamp = normalize_ts(record.get("timestamp"))
    session_id = record.get("sessionId") or session_id
    prompt_id = record.get("promptId") or turn_id
    message = record.get("message")
    out = {
        "events": [event_row("claude", session_id, prompt_id, path, line_number, top_type, None, timestamp, record, keep_raw)],
        "calls": [],
        "outputs": [],
        "session_updates": {
            "session_id": session_id,
            "cwd": record.get("cwd"),
            "started_at": timestamp,
            "model": message.get("model") if isinstance(message, dict) else None,
            "git_branch": record.get("gitBranch"),
            "entrypoint": record.get("entrypoint"),
        },
        "turn_id": prompt_id,
    }

    if isinstance(message, dict):
        for content in ensure_list(message.get("content")):
            if not isinstance(content, dict):
                continue
            ctype = content.get("type")
            out["events"].append(event_row("claude", session_id, prompt_id, path, line_number, top_type, ctype, timestamp, content, keep_raw))
            if ctype == "tool_use":
                out["calls"].append(claude_call_row(content, session_id, prompt_id, timestamp, path, line_number, record))
            elif ctype == "tool_result":
                out["outputs"].append(claude_output_row(content, session_id, timestamp, path, line_number))

    return out


def parse_unknown_record(record: dict[str, Any], session_id: str, turn_id: str | None, path: Path, line_number: int, keep_raw: bool) -> dict[str, Any]:
    return {
        "events": [event_row("unknown", session_id, turn_id, path, line_number, record.get("type"), None, normalize_ts(record.get("timestamp")), record, keep_raw)],
        "calls": [],
        "outputs": [],
        "session_updates": {},
        "turn_id": turn_id,
    }


def codex_call_row(payload: dict[str, Any], session_id: str, turn_id: str | None, timestamp: str | None, path: Path, line_number: int) -> dict[str, Any]:
    item_type = payload.get("type")
    name = payload.get("name") or item_type
    if item_type == "web_search_call":
        name = "web_search"
    if item_type == "tool_search_call":
        name = "tool_search"
    raw_input = payload.get("arguments", payload.get("input", payload.get("action")))
    parsed_input = parse_maybe_json(raw_input)
    return {
        "source": "codex",
        "session_id": session_id,
        "turn_id": turn_id,
        "call_id": payload.get("call_id") or payload.get("id") or stable_id(str(path) + str(line_number)),
        "tool_name": name,
        "tool_type": item_type,
        "timestamp": timestamp,
        "path": str(path),
        "line_number": line_number,
        "input": compact_json(parsed_input),
        "input_raw": compact_text(raw_input),
        "status": payload.get("status"),
    }


def claude_call_row(content: dict[str, Any], session_id: str, turn_id: str | None, timestamp: str | None, path: Path, line_number: int, record: dict[str, Any]) -> dict[str, Any]:
    return {
        "source": "claude",
        "session_id": session_id,
        "turn_id": turn_id,
        "call_id": content.get("id"),
        "tool_name": content.get("name"),
        "tool_type": content.get("type"),
        "timestamp": timestamp,
        "path": str(path),
        "line_number": line_number,
        "input": compact_json(content.get("input")),
        "input_raw": compact_text(content.get("input")),
        "status": record.get("message", {}).get("stop_reason") if isinstance(record.get("message"), dict) else None,
    }


def codex_response_output(payload: dict[str, Any], session_id: str, timestamp: str | None, path: Path, line_number: int) -> dict[str, Any]:
    return {
        "source": "codex",
        "session_id": session_id,
        "call_id": payload.get("call_id"),
        "timestamp_out": timestamp,
        "output": compact_text(payload.get("output")),
        "exit_code": extract_exit_code(payload.get("output")),
        "success": None,
        "stderr": extract_labeled_section(payload.get("output"), "stderr"),
        "stdout": extract_labeled_section(payload.get("output"), "stdout"),
        "duration_secs": None,
        "output_path": str(path),
        "output_line_number": line_number,
    }


def codex_event_output(payload: dict[str, Any], session_id: str, timestamp: str | None, path: Path, line_number: int) -> dict[str, Any]:
    duration = payload.get("duration") or {}
    duration_secs = duration.get("secs") if isinstance(duration, dict) else None
    if isinstance(duration, dict) and duration_secs is not None:
        duration_secs += duration.get("nanos", 0) / 1_000_000_000
    return {
        "source": "codex",
        "session_id": session_id,
        "call_id": payload.get("call_id"),
        "timestamp_out": timestamp,
        "output": compact_text(payload.get("aggregated_output") or payload.get("formatted_output") or payload.get("stdout") or payload.get("stderr")),
        "exit_code": payload.get("exit_code"),
        "success": payload.get("success"),
        "stderr": compact_text(payload.get("stderr")),
        "stdout": compact_text(payload.get("stdout")),
        "duration_secs": duration_secs,
        "command": " ".join(payload.get("command") or []) if isinstance(payload.get("command"), list) else payload.get("command"),
        "cwd": payload.get("cwd"),
        "status_out": payload.get("status"),
        "output_path": str(path),
        "output_line_number": line_number,
    }


def claude_output_row(content: dict[str, Any], session_id: str, timestamp: str | None, path: Path, line_number: int) -> dict[str, Any]:
    return {
        "source": "claude",
        "session_id": session_id,
        "call_id": content.get("tool_use_id"),
        "timestamp_out": timestamp,
        "output": compact_text(content.get("content")),
        "exit_code": extract_exit_code(content.get("content")),
        "success": False if content.get("is_error") else None,
        "stderr": compact_text(content.get("stderr")),
        "stdout": compact_text(content.get("stdout")),
        "duration_secs": None,
        "output_path": str(path),
        "output_line_number": line_number,
    }


def attach_outputs(calls_df: pd.DataFrame, outputs_df: pd.DataFrame) -> pd.DataFrame:
    if outputs_df.empty:
        return calls_df
    suffix_cols = ["session_id", "call_id"]
    merged = calls_df.merge(outputs_df, on=suffix_cols, how="left", suffixes=("", "_out"))
    return merged


def classify_failure(row: pd.Series) -> bool:
    exit_code = row.get("exit_code")
    success = row.get("success")
    status = str(row.get("status_out") or row.get("status") or "").lower()
    stderr = str(row.get("stderr") or "")
    output = str(row.get("output") or "")
    if success is False:
        return True
    if pd.notna(exit_code):
        try:
            return int(float(exit_code)) != 0
        except (TypeError, ValueError):
            pass
    if "failed" in status or "error" in status:
        return True
    failure_patterns = ["traceback", "permission denied", "command not found", "no such file or directory", "fatal:", "error:"]
    text = (stderr + "\n" + output).lower()
    return any(pattern in text for pattern in failure_patterns)


def action_family(tool_name: str | None) -> str:
    name = (tool_name or "").lower()
    if any(x in name for x in ["exec", "bash", "shell", "terminal"]):
        return "shell"
    if any(x in name for x in ["apply_patch", "edit", "write", "notebookedit"]):
        return "file_edit"
    if any(x in name for x in ["read", "cat", "list", "find", "rg", "glob", "grep"]):
        return "file_read_search"
    if any(x in name for x in ["web", "browser", "chrome", "fetch", "search"]):
        return "web_browser"
    if "mcp" in name or name.startswith("mcp__"):
        return "mcp"
    if any(x in name for x in ["todo", "plan"]):
        return "planning"
    return "other"


def summarize_input(row: pd.Series) -> str:
    raw = row.get("input") or row.get("input_raw") or ""
    obj = parse_maybe_json(raw)
    if isinstance(obj, dict):
        for key in ("cmd", "command", "query", "q", "pattern", "path", "file_path", "workdir"):
            if key in obj:
                return compact_text(obj[key], 220)
        if "search_query" in obj:
            return compact_text(obj["search_query"], 220)
    return compact_text(raw, 220)


def aid_suggestions(calls_df: pd.DataFrame) -> list[dict[str, Any]]:
    if calls_df.empty:
        return []
    failed = calls_df[calls_df["failed"]]
    if failed.empty:
        return []
    suggestions: list[dict[str, Any]] = []
    text = (failed.get("stderr", pd.Series(dtype=str)).fillna("") + "\n" + failed.get("output", pd.Series(dtype=str)).fillna("")).str.lower()
    patterns = [
        ("Missing commands", r"command not found|no such file or directory: .*?(?:python|node|npm|pnpm|uv|rg|gh)", "Add repo-local setup docs and preflight dependency checks; agents are burning calls discovering missing binaries or wrong PATH."),
        ("Permission or sandbox blocks", r"permission denied|operation not permitted|not permitted|sandbox", "Expose a clear permission profile and writable roots early in prompts; include approved paths for generated artifacts."),
        ("Git/GitHub friction", r"fatal: not a git repository|could not read from remote|authentication failed|gh:|not a git repository", "Have agents run a bounded git preflight and provide repo/branch/remote context in task prompts."),
        ("Test/build failures", r"test failed|tests failed|compilation failed|build failed|failed to compile|exit code 1", "Point agents to the smallest intended verification command and expected runtime so they can iterate on failures cheaply."),
        ("Rate limits/timeouts", r"rate limit|timed out|timeout|deadline exceeded", "Favor bounded one-shot polling, cached API reads, and shorter command timeouts around known slow services."),
    ]
    joined = "\n".join(text.tolist())
    for label, pattern, suggestion in patterns:
        count = int(text.str.contains(pattern, regex=True).sum())
        if count:
            suggestions.append({"pattern": label, "failed_calls": count, "suggestion": suggestion})
    top_failed_tools = failed["tool_name"].value_counts().head(5)
    if not top_failed_tools.empty:
        suggestions.append(
            {
                "pattern": "Highest-failure tools",
                "failed_calls": int(top_failed_tools.sum()),
                "suggestion": "Prioritize guardrails and examples for: " + ", ".join(f"{k} ({v})" for k, v in top_failed_tools.items()),
            }
        )
    return suggestions


def event_row(source: str, session_id: str, turn_id: str | None, path: Path, line_number: int, event_type: str | None, sub_type: str | None, timestamp: str | None, payload: Any, keep_raw: bool) -> dict[str, Any]:
    return {
        "source": source,
        "session_id": session_id,
        "turn_id": turn_id,
        "timestamp": normalize_ts(timestamp),
        "event_type": event_type,
        "sub_type": sub_type,
        "path": str(path),
        "line_number": line_number,
        "raw_snippet": compact_text(payload) if keep_raw else "",
    }


def file_row(path: Path) -> dict[str, Any]:
    stat = safe_stat(path)
    return {
        "source": infer_source(path),
        "path": str(path),
        "bytes": stat.st_size if stat else None,
        "mtime": datetime.fromtimestamp(stat.st_mtime, timezone.utc).isoformat() if stat else None,
        "project": project_from_path(path, infer_source(path)),
    }


def call_columns() -> list[str]:
    return ["source", "session_id", "turn_id", "call_id", "tool_name", "tool_type", "timestamp", "path", "line_number", "input", "input_raw", "status", "failed", "action_family", "input_summary"]


def infer_source(path: Path) -> str:
    parts = set(path.parts)
    if ".codex" in parts:
        return "codex"
    if ".claude" in parts:
        return "claude"
    return "unknown"


def project_from_path(path: Path, source: str) -> str:
    parts = path.parts
    if source == "claude" and "projects" in parts:
        idx = parts.index("projects")
        if idx + 1 < len(parts):
            return parts[idx + 1]
    if source == "codex" and "sessions" in parts:
        return "codex/sessions"
    if source == "codex" and "archived_sessions" in parts:
        return "codex/archived_sessions"
    return path.parent.name


def session_id_from_path(path: Path, source: str) -> str:
    stem = path.stem
    if source == "codex" and stem.startswith("rollout-"):
        return stem.split("rollout-", 1)[1]
    return stem


def safe_stat(path: Path):
    try:
        return path.stat()
    except OSError:
        return None


def stable_id(text: str) -> str:
    return hashlib.sha1(text.encode("utf-8", errors="ignore")).hexdigest()[:16]


def normalize_ts(value: Any) -> str | None:
    if value is None:
        return None
    if isinstance(value, (int, float)):
        seconds = value / 1000 if value > 10_000_000_000 else value
        return datetime.fromtimestamp(seconds, timezone.utc).isoformat()
    if isinstance(value, str):
        return value
    return str(value)


def ensure_list(value: Any) -> list[Any]:
    if value is None:
        return []
    if isinstance(value, list):
        return value
    return [value]


def parse_maybe_json(value: Any) -> Any:
    if not isinstance(value, str):
        return value
    try:
        return json.loads(value)
    except Exception:
        return value


def compact_json(value: Any, limit: int = TEXT_LIMIT) -> str:
    if isinstance(value, str):
        return compact_text(value, limit)
    try:
        return compact_text(json.dumps(value, ensure_ascii=False, sort_keys=True), limit)
    except TypeError:
        return compact_text(value, limit)


def compact_text(value: Any, limit: int = TEXT_LIMIT) -> str:
    if value is None:
        return ""
    if isinstance(value, (dict, list)):
        try:
            text = json.dumps(value, ensure_ascii=False)
        except TypeError:
            text = str(value)
    else:
        text = str(value)
    text = re.sub(r"\s+", " ", text).strip()
    if len(text) > limit:
        return text[: limit - 1] + "..."
    return text


def extract_exit_code(value: Any) -> int | None:
    text = compact_text(value, 4000)
    match = re.search(r"(?:exit code|Process exited with code)\s+(-?\d+)", text, re.I)
    if match:
        return int(match.group(1))
    return None


def extract_labeled_section(value: Any, label: str) -> str:
    text = compact_text(value, 4000)
    match = re.search(rf"{label}:\s*(.*?)(?:\s+[A-Z][A-Za-z ]+:|$)", text, re.S)
    return compact_text(match.group(1), 900) if match else ""


def shell_command_parts(input_text: str) -> dict[str, str]:
    obj = parse_maybe_json(input_text)
    if isinstance(obj, dict):
        return {
            "cmd": compact_text(obj.get("cmd") or obj.get("command"), 500),
            "workdir": compact_text(obj.get("workdir"), 300),
        }
    return {"cmd": compact_text(input_text, 500), "workdir": ""}
