import json
from pathlib import Path
from typing import Any, Dict, List, Optional

from src.errors import RepositoryNotInitializedError


class RepoState:
    def __init__(self, project_root: Optional[Path] = None) -> None:
        self.project_root = project_root or Path(__file__).resolve().parents[1]
        self.repo_root = self.project_root / ".mydata"
        self.versions_root = self.repo_root / "versions"
        self.head_file = self.repo_root / "HEAD"
        self.logs_file = self.repo_root / "logs.json"
        self.meta_file = self.repo_root / "repo_meta.json"
        self._ensure_initialized()

    def _ensure_initialized(self) -> None:
        required_paths = [
            self.repo_root,
            self.versions_root,
            self.head_file,
            self.logs_file,
            self.meta_file,
        ]
        if not all(path.exists() for path in required_paths):
            raise RepositoryNotInitializedError(
                f"Repository not initialized under: {self.repo_root}"
            )

    def get_head(self) -> Optional[str]:
        raw_value = self.head_file.read_text(encoding="utf-8").strip()
        if raw_value in {"", "null", "None"}:
            return None
        return raw_value

    def set_head(self, version_id: str) -> None:
        self.head_file.write_text(f"{version_id}\n", encoding="utf-8")

    def read_logs(self) -> List[Dict[str, Any]]:
        try:
            content = self.logs_file.read_text(encoding="utf-8").strip()
            if not content:
                return []
            data = json.loads(content)
            if not isinstance(data, list):
                return []
            return data
        except json.JSONDecodeError:
            return []

    def append_log(self, record: Dict[str, Any]) -> None:
        logs = self.read_logs()
        logs.append(record)
        self.logs_file.write_text(
            json.dumps(logs, ensure_ascii=False, indent=2) + "\n",
            encoding="utf-8",
        )

    def version_exists(self, version_id: str) -> bool:
        return (self.versions_root / version_id).exists()
