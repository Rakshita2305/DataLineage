import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict

import pandas as pd

from src.errors import DataLineageError
from src.repo import RepoState


def _load_processed_dataframe(version_dir: Path) -> pd.DataFrame:
    processed_file = version_dir / "processed.csv"
    if not processed_file.exists():
        raise DataLineageError(f"Processed artifact missing: {processed_file}")
    return pd.read_csv(processed_file)


def _load_metadata(version_dir: Path) -> Dict[str, Any]:
    metadata_file = version_dir / "metadata.json"
    if not metadata_file.exists():
        raise DataLineageError(f"Metadata artifact missing: {metadata_file}")
    try:
        return json.loads(metadata_file.read_text(encoding="utf-8"))
    except json.JSONDecodeError as error:
        raise DataLineageError(f"Invalid metadata JSON: {metadata_file}") from error


def _now_utc_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def compare_versions(repo: RepoState, version_a: str, version_b: str) -> Dict[str, Any]:
    if not repo.version_exists(version_a):
        raise DataLineageError(f"Version not found: {version_a}")
    if not repo.version_exists(version_b):
        raise DataLineageError(f"Version not found: {version_b}")

    version_a_dir = repo.versions_root / version_a
    version_b_dir = repo.versions_root / version_b

    dataframe_a = _load_processed_dataframe(version_a_dir)
    dataframe_b = _load_processed_dataframe(version_b_dir)

    metadata_a = _load_metadata(version_a_dir)
    metadata_b = _load_metadata(version_b_dir)

    columns_a = [str(column) for column in dataframe_a.columns.tolist()]
    columns_b = [str(column) for column in dataframe_b.columns.tolist()]

    label_dist_a = metadata_a.get("label_distribution", {})
    label_dist_b = metadata_b.get("label_distribution", {})

    config_hash_a = metadata_a.get("config_hash")
    config_hash_b = metadata_b.get("config_hash")

    diff_report: Dict[str, Any] = {
        "generated_at": _now_utc_iso(),
        "version_a": version_a,
        "version_b": version_b,
        "summary": {
            "row_count_a": int(len(dataframe_a)),
            "row_count_b": int(len(dataframe_b)),
            "row_delta": int(len(dataframe_b) - len(dataframe_a)),
            "column_count_a": int(len(columns_a)),
            "column_count_b": int(len(columns_b)),
            "config_changed": bool(config_hash_a != config_hash_b),
        },
        "columns": {
            "only_in_a": sorted(list(set(columns_a) - set(columns_b))),
            "only_in_b": sorted(list(set(columns_b) - set(columns_a))),
            "common": sorted(list(set(columns_a).intersection(set(columns_b)))),
        },
        "label_distribution": {
            "a": label_dist_a,
            "b": label_dist_b,
            "changed": bool(label_dist_a != label_dist_b),
        },
        "hashes": {
            "config_hash_a": config_hash_a,
            "config_hash_b": config_hash_b,
            "input_hash_a": metadata_a.get("input_hash"),
            "input_hash_b": metadata_b.get("input_hash"),
            "version_hash_a": metadata_a.get("version_hash"),
            "version_hash_b": metadata_b.get("version_hash"),
        },
    }

    reports_dir = repo.project_root / "reports"
    reports_dir.mkdir(parents=True, exist_ok=True)

    report_file = reports_dir / f"diff_{version_a[:8]}__{version_b[:8]}.json"
    report_file.write_text(
        json.dumps(diff_report, ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )

    return {
        "version_a": version_a,
        "version_b": version_b,
        "row_count_a": int(len(dataframe_a)),
        "row_count_b": int(len(dataframe_b)),
        "row_delta": int(len(dataframe_b) - len(dataframe_a)),
        "config_changed": bool(config_hash_a != config_hash_b),
        "label_distribution_changed": bool(label_dist_a != label_dist_b),
        "report_path": str(report_file),
    }
