from __future__ import annotations

import ast
import math
import re
from collections import Counter
from dataclasses import dataclass, field
from pathlib import Path
from typing import Iterable


IGNORED_DIRS = {
    ".git",
    ".venv",
    "__pycache__",
    ".pytest_cache",
    "node_modules",
    "dist",
    "build",
    ".next",
    "coverage",
}

EXT_LANG = {
    ".py": "python",
    ".js": "javascript",
    ".jsx": "javascript",
    ".ts": "typescript",
    ".tsx": "typescript",
    ".go": "go",
    ".rs": "rust",
    ".rb": "ruby",
    ".java": "java",
    ".kt": "kotlin",
    ".swift": "swift",
    ".md": "markdown",
    ".toml": "toml",
    ".json": "json",
    ".yaml": "yaml",
    ".yml": "yaml",
}


@dataclass(frozen=True)
class Symbol:
    name: str
    kind: str
    path: str
    line: int


@dataclass(frozen=True)
class SearchResult:
    path: str
    score: float
    reason: str
    snippet: str


@dataclass(frozen=True)
class RelatedFile:
    path: str
    reason: str
    score: float


@dataclass(frozen=True)
class RepoBrief:
    root_name: str
    file_count: int
    language_counts: dict[str, int]
    known_commands: list[str]
    important_files: list[str]


@dataclass
class IndexedFile:
    path: str
    language: str
    text: str
    tokens: Counter[str] = field(default_factory=Counter)
    symbols: list[Symbol] = field(default_factory=list)


class RepoIndex:
    def __init__(self, root: Path, files: dict[str, IndexedFile]):
        self.root = root
        self.files = files
        self.symbols = [symbol for file in files.values() for symbol in file.symbols]
        self._doc_freq = self._build_doc_freq()

    def find_symbol(self, name: str, limit: int = 10) -> list[Symbol]:
        needle = normalize_token(name)
        scored: list[tuple[int, Symbol]] = []
        for symbol in self.symbols:
            symbol_token = normalize_token(symbol.name)
            if symbol.name == name:
                scored.append((100, symbol))
            elif symbol_token == needle:
                scored.append((90, symbol))
            elif needle in symbol_token:
                scored.append((60, symbol))
        return [symbol for _, symbol in sorted(scored, key=lambda item: (-item[0], item[1].path, item[1].line))[:limit]]

    def search_code(self, query: str, limit: int = 10) -> list[SearchResult]:
        query_tokens = tokenize(query)
        if not query_tokens:
            return []
        results: list[SearchResult] = []
        total_docs = max(len(self.files), 1)
        for file in self.files.values():
            score = 0.0
            reasons: list[str] = []
            for token in query_tokens:
                tf = file.tokens.get(token, 0)
                if not tf:
                    continue
                idf = math.log((total_docs + 1) / (self._doc_freq[token] + 1)) + 1
                score += (1 + math.log(tf)) * idf
                reasons.append(token)
            for symbol in file.symbols:
                symbol_tokens = tokenize(symbol.name)
                overlap = set(symbol_tokens) & set(query_tokens)
                if overlap:
                    score += 2.0 * len(overlap)
                    reasons.extend(sorted(overlap))
            if score:
                results.append(
                    SearchResult(
                        path=file.path,
                        score=round(score, 4),
                        reason="matched " + ", ".join(sorted(set(reasons))[:8]),
                        snippet=best_snippet(file.text, query_tokens),
                    )
                )
        return sorted(results, key=lambda item: (-item.score, item.path))[:limit]

    def related_files(self, path: str, limit: int = 10) -> list[RelatedFile]:
        normalized = path.strip("/")
        stem = Path(normalized).stem
        directory = str(Path(normalized).parent)
        related: list[RelatedFile] = []
        for file_path in self.files:
            if file_path == normalized:
                continue
            score = 0.0
            reasons: list[str] = []
            lower = file_path.lower()
            if stem and stem.lower() in lower:
                score += 4
                reasons.append(f"shares stem {stem}")
            if lower.startswith("test") or "/test" in lower or lower.endswith("_test.py") or lower.endswith(".test.ts"):
                source_stem = stem.replace("test_", "")
                if source_stem and source_stem.lower() in lower:
                    score += 5
                    reasons.append("test coverage candidate")
            if directory != "." and str(Path(file_path).parent) == directory:
                score += 1
                reasons.append("same directory")
            if score:
                related.append(RelatedFile(file_path, "; ".join(reasons), score))
        return sorted(related, key=lambda item: (-item.score, item.path))[:limit]

    def repo_brief(self) -> RepoBrief:
        language_counts = Counter(file.language for file in self.files.values())
        important = [
            name
            for name in ["AGENTS.md", "CLAUDE.md", "README.md", "pyproject.toml", "package.json", "Makefile"]
            if name in self.files
        ]
        return RepoBrief(
            root_name=self.root.name,
            file_count=len(self.files),
            language_counts=dict(language_counts),
            known_commands=known_commands(self.root, self.files),
            important_files=important,
        )

    def _build_doc_freq(self) -> Counter[str]:
        counts: Counter[str] = Counter()
        for file in self.files.values():
            counts.update(file.tokens.keys())
        return counts


class RepoIndexer:
    def __init__(self, root: Path | str):
        self.root = Path(root).resolve()

    def build(self) -> RepoIndex:
        files: dict[str, IndexedFile] = {}
        for path in walk_files(self.root):
            rel = path.relative_to(self.root).as_posix()
            language = EXT_LANG.get(path.suffix.lower(), "text")
            try:
                text = path.read_text(encoding="utf-8", errors="replace")
            except OSError:
                continue
            if "\0" in text:
                continue
            symbols = extract_symbols(rel, text, language)
            files[rel] = IndexedFile(rel, language, text, Counter(tokenize(text + " " + rel)), symbols)
        return RepoIndex(self.root, files)


def walk_files(root: Path) -> Iterable[Path]:
    for path in root.rglob("*"):
        if path.is_dir():
            continue
        if any(part in IGNORED_DIRS for part in path.relative_to(root).parts):
            continue
        if path.stat().st_size > 512_000:
            continue
        if path.suffix.lower() not in EXT_LANG and path.name not in {"README", "Makefile", "AGENTS.md", "CLAUDE.md"}:
            continue
        yield path


def extract_symbols(path: str, text: str, language: str) -> list[Symbol]:
    if language == "python":
        return extract_python_symbols(path, text)
    symbols: list[Symbol] = []
    for line_number, line in enumerate(text.splitlines(), start=1):
        match = re.search(r"\b(?:function|class|interface|const|let|var)\s+([A-Za-z_$][\w$]*)", line)
        if match:
            kind = "class" if "class " in line else "function"
            symbols.append(Symbol(match.group(1), kind, path, line_number))
    return symbols


def extract_python_symbols(path: str, text: str) -> list[Symbol]:
    try:
        tree = ast.parse(text)
    except SyntaxError:
        return []
    symbols: list[Symbol] = []
    for node in ast.walk(tree):
        if isinstance(node, ast.ClassDef):
            symbols.append(Symbol(node.name, "class", path, node.lineno))
        elif isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
            symbols.append(Symbol(node.name, "function", path, node.lineno))
    return symbols


def known_commands(root: Path, files: dict[str, IndexedFile]) -> list[str]:
    commands: list[str] = []
    if "pyproject.toml" in files:
        commands.append("pytest")
    if "package.json" in files:
        commands.extend(["npm test", "npm run lint"])
    if "Makefile" in files:
        commands.append("make test")
    return commands


def tokenize(text: str) -> list[str]:
    return [token for token in re.findall(r"[A-Za-z][A-Za-z0-9_]*", split_camel(text).lower()) if len(token) > 1]


def split_camel(text: str) -> str:
    return re.sub(r"([a-z0-9])([A-Z])", r"\1 \2", text)


def normalize_token(text: str) -> str:
    return "".join(tokenize(text))


def best_snippet(text: str, query_tokens: list[str]) -> str:
    lines = text.splitlines()
    if not lines:
        return ""
    for idx, line in enumerate(lines):
        lowered = line.lower()
        if any(token in lowered for token in query_tokens):
            start = max(0, idx - 1)
            end = min(len(lines), idx + 3)
            return "\n".join(lines[start:end])[:700]
    return "\n".join(lines[:6])[:700]
