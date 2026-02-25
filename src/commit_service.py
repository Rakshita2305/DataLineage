import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict

import pandas as pd

from src.hasher import (
    dataframe_to_stable_csv_bytes,
    sha256_from_bytes,
    sha256_from_json,
)
from src.io_loader import load_dataset, read_config, validate_schema
from src.errors import DataLineageError
from src.models import VersionRecord
from src.preprocess import apply_deterministic_preprocessing, get_default_preprocess_config
from src.repo import RepoState


def _label_distribution(processed_rows) -> Dict[str, int]:
    if "label" not in processed_rows.columns:
        return {}
    distribution = processed_rows["label"].value_counts(dropna=False).to_dict()
    return {str(key): int(value) for key, value in distribution.items()}


def _now_utc_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def _normalize_user_path(path_value: str) -> str:
    normalized = path_value.strip()
    if len(normalized) >= 2 and normalized[0] == normalized[-1] and normalized[0] in {'"', "'"}:
        normalized = normalized[1:-1]
    return normalized


def _persist_version(
    repo: RepoState,
    source_data_path: str,
    source_config_path: str,
    input_bytes: bytes,
    raw_dataframe,
    processed_dataframe,
    config: Dict[str, Any],
    commit_message: str,
) -> Dict[str, Any]:
    processed_csv_bytes = dataframe_to_stable_csv_bytes(processed_dataframe)

    input_hash = sha256_from_bytes(input_bytes)
    config_hash = sha256_from_json(config)
    version_hash = sha256_from_bytes(processed_csv_bytes)

    parent_id = repo.get_head()
    version_dir = repo.versions_root / version_hash
    timestamp = _now_utc_iso()

    if version_dir.exists():
        dedupe_event = {
            "event_type": "dedupe_hit",
            "timestamp": timestamp,
            "requested_source_data_path": source_data_path,
            "requested_source_config_path": source_config_path,
            "requested_input_hash": input_hash,
            "requested_config_hash": config_hash,
            "resolved_version_id": version_hash,
            "message": "Run recorded. No new version created; existing version reused.",
        }
        repo.append_log(dedupe_event)
        return {
            "status": "duplicate",
            "version_id": version_hash,
            "head": repo.get_head(),
            "message": "Identical processed output already committed.",
        }

    version_dir.mkdir(parents=True, exist_ok=False)

    raw_snapshot_path = version_dir / "raw_snapshot.csv"
    processed_snapshot_path = version_dir / "processed.csv"
    config_snapshot_path = version_dir / "config_snapshot.json"
    metadata_path = version_dir / "metadata.json"

    raw_snapshot_path.write_bytes(input_bytes)
    processed_snapshot_path.write_bytes(processed_csv_bytes)
    config_snapshot_path.write_text(
        json.dumps(config, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )

    record = VersionRecord(
        version_id=version_hash,
        parent_id=parent_id,
        timestamp=timestamp,
        commit_message=commit_message.strip(),
        source_data_path=source_data_path,
        source_config_path=source_config_path,
        input_hash=input_hash,
        config_hash=config_hash,
        version_hash=version_hash,
        row_count=int(len(processed_dataframe)),
        label_distribution=_label_distribution(processed_dataframe),
        eval_metrics=None,
    )

    metadata: Dict[str, Any] = {
        "event_type": "commit",
        **record.to_dict(),
        "preprocess_stats": {
            "rows_before": int(len(raw_dataframe)),
            "rows_after": int(len(processed_dataframe)),
            "columns_before": list(map(str, raw_dataframe.columns.tolist())),
            "columns_after": list(map(str, processed_dataframe.columns.tolist())),
        },
        "artifacts": {
            "raw_snapshot": raw_snapshot_path.name,
            "processed_snapshot": processed_snapshot_path.name,
            "config_snapshot": config_snapshot_path.name,
        },
    }

    if not str(source_data_path).startswith("HEAD:"):
        source_name = Path(source_data_path).name or "raw_input.csv"
        raw_archive_dir = repo.project_root / "raw_data"
        raw_archive_dir.mkdir(parents=True, exist_ok=True)
        archived_name = f"{version_hash[:8]}__{source_name}"
        archived_path = raw_archive_dir / archived_name
        archived_path.write_bytes(input_bytes)
        metadata["artifacts"]["raw_archive"] = str(archived_path.relative_to(repo.project_root))

    metadata_path.write_text(
        json.dumps(metadata, ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )

    repo.append_log(metadata)
    repo.set_head(version_hash)

    return {
        "status": "created",
        "version_id": version_hash,
        "parent_id": parent_id,
        "head": version_hash,
        "rows_before": int(len(raw_dataframe)),
        "rows_after": int(len(processed_dataframe)),
        "version_path": str(version_dir),
        "metadata_path": str(metadata_path),
    }


def create_version_from_paths(
    repo: RepoState,
    dataset_path: str,
    config_path: str,
    commit_message: str,
) -> Dict[str, Any]:
    dataset_path = _normalize_user_path(dataset_path)
    config_path = _normalize_user_path(config_path)

    dataset_file = Path(dataset_path)
    config_file = Path(config_path)

    raw_bytes = dataset_file.read_bytes()
    config = read_config(config_path)

    raw_dataframe, _ = load_dataset(dataset_path)
    validate_schema(raw_dataframe)

    processed_dataframe = apply_deterministic_preprocessing(raw_dataframe, config)
    return _persist_version(
        repo=repo,
        source_data_path=str(dataset_file),
        source_config_path=str(config_file),
        input_bytes=raw_bytes,
        raw_dataframe=raw_dataframe,
        processed_dataframe=processed_dataframe,
        config=config,
        commit_message=commit_message,
    )


def create_version_from_head(
    repo: RepoState,
    config_path: str,
    commit_message: str,
) -> Dict[str, Any]:
    config_path = _normalize_user_path(config_path)

    head_version = repo.get_head()
    if not head_version:
        raise DataLineageError("HEAD is not set. First commit must use a dataset path.")

    head_processed_path = repo.versions_root / head_version / "processed.csv"
    if not head_processed_path.exists():
        raise DataLineageError(
            f"HEAD processed snapshot not found: {head_processed_path}"
        )

    config_file = Path(config_path)
    config = read_config(config_path)

    input_bytes = head_processed_path.read_bytes()
    raw_dataframe = pd.read_csv(head_processed_path)
    validate_schema(raw_dataframe)
    processed_dataframe = apply_deterministic_preprocessing(raw_dataframe, config)

    return _persist_version(
        repo=repo,
        source_data_path=f"HEAD:{head_version}",
        source_config_path=str(config_file),
        input_bytes=input_bytes,
        raw_dataframe=raw_dataframe,
        processed_dataframe=processed_dataframe,
        config=config,
        commit_message=commit_message,
    )


def create_version_from_raw_default(
    repo: RepoState,
    dataset_path: str,
    commit_message: str,
) -> Dict[str, Any]:
    dataset_path = _normalize_user_path(dataset_path)

    dataset_file = Path(dataset_path)
    raw_bytes = dataset_file.read_bytes()

    raw_dataframe, _ = load_dataset(dataset_path)
    validate_schema(raw_dataframe)

    config = get_default_preprocess_config()
    processed_dataframe = apply_deterministic_preprocessing(raw_dataframe, config)

    return _persist_version(
        repo=repo,
        source_data_path=str(dataset_file),
        source_config_path="DEFAULT_CONFIG",
        input_bytes=raw_bytes,
        raw_dataframe=raw_dataframe,
        processed_dataframe=processed_dataframe,
        config=config,
        commit_message=commit_message,
    )
