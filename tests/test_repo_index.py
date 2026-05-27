from pathlib import Path

from orient.repo_index import RepoIndexer


def write(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")


def test_repo_index_finds_symbols_text_and_related_tests(tmp_path: Path) -> None:
    write(
        tmp_path / "src" / "auth.py",
        """
import json

class SessionManager:
    def issue_token(self, user_id: str) -> str:
        return json.dumps({"sub": user_id})

def verify_token(token: str) -> bool:
    return token.startswith("{")
""".strip(),
    )
    write(
        tmp_path / "tests" / "test_auth.py",
        """
from src.auth import SessionManager, verify_token

def test_issue_token_round_trip():
    assert verify_token(SessionManager().issue_token("u_123"))
""".strip(),
    )
    write(tmp_path / "pyproject.toml", "[project]\nname='sample'\n")

    index = RepoIndexer(tmp_path).build()

    symbol = index.find_symbol("SessionManager")[0]
    assert symbol.path == "src/auth.py"
    assert symbol.kind == "class"

    search = index.search_code("issue token user session", limit=3)
    assert search[0].path == "src/auth.py"
    assert "issue_token" in search[0].snippet

    related = index.related_files("src/auth.py")
    assert "tests/test_auth.py" in [item.path for item in related]

    brief = index.repo_brief()
    assert brief.root_name == tmp_path.name
    assert brief.language_counts["python"] == 2
    assert "pytest" in brief.known_commands


def test_repo_index_ignores_heavy_directories(tmp_path: Path) -> None:
    write(tmp_path / "src" / "app.ts", "export function run() { return true }\n")
    write(tmp_path / "node_modules" / "ignored.js", "function ignored() {}\n")

    index = RepoIndexer(tmp_path).build()

    assert "src/app.ts" in index.files
    assert "node_modules/ignored.js" not in index.files
