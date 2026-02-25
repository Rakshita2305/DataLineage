import json
from pathlib import Path
from typing import Dict, Tuple

import pandas as pd

from src.errors import ValidationError


def read_config(config_path: str) -> Dict:
    path = Path(config_path)
    if not path.exists() or not path.is_file():
        raise ValidationError(f"Config file not found: {config_path}")

    if path.suffix.lower() != ".json":
        raise ValidationError("Config file must be a .json file in this prototype.")

    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as error:
        raise ValidationError(f"Invalid JSON config: {error}") from error


def load_dataset(dataset_path: str) -> Tuple[pd.DataFrame, str]:
    path = Path(dataset_path)
    if not path.exists() or not path.is_file():
        raise ValidationError(f"Dataset file not found: {dataset_path}")

    suffix = path.suffix.lower()
    if suffix == ".csv":
        dataframe = pd.read_csv(path)
    elif suffix == ".json":
        try:
            dataframe = pd.read_json(path)
        except ValueError:
            dataframe = pd.read_json(path, lines=True)
    else:
        raise ValidationError("Only CSV and JSON input are supported in this prototype.")

    if dataframe.empty:
        raise ValidationError("Loaded dataset is empty.")

    return dataframe, suffix


def validate_schema(dataframe: pd.DataFrame) -> None:
    if dataframe is None or dataframe.empty:
        raise ValidationError("Dataset schema invalid: dataset is empty.")

    if len(dataframe.columns) == 0:
        raise ValidationError("Dataset schema invalid: no columns found.")
