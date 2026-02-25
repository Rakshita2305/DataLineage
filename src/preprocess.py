import re
import unicodedata
from typing import Dict

import pandas as pd


def get_default_preprocess_config() -> Dict:
    return {
        "drop_nulls": True,
        "drop_duplicates": True,
        "cleanup_text": True,
        "strip_text": True,
        "lowercase_text": True,
        "remove_punctuation": True,
        "collapse_spaces": True,
        "normalize_unicode": True,
        "remove_urls": False,
        "coerce_numeric_columns": True,
        "null_strategy": "drop_any",  # drop_any | drop_all | fill | keep
        "null_fill_text": "",
        "null_fill_numeric": 0,
        "sort_rows": True,
    }


def _normalize_columns(dataframe: pd.DataFrame) -> pd.DataFrame:
    dataframe = dataframe.copy()
    dataframe.columns = [str(column).strip().lower() for column in dataframe.columns]
    return dataframe


def _cleanup_text(value: str) -> str:
    text = str(value)
    text = unicodedata.normalize("NFKC", text)
    text = text.strip().lower()
    text = re.sub(r"[^a-z0-9\s]", " ", text)
    text = re.sub(r"\s+", " ", text).strip()
    return text


def _cleanup_text_with_config(value: str, config: Dict) -> str:
    text = str(value)

    if bool(config.get("normalize_unicode", True)):
        text = unicodedata.normalize("NFKC", text)

    if bool(config.get("strip_text", True)):
        text = text.strip()

    if bool(config.get("remove_urls", False)):
        text = re.sub(r"https?://\S+|www\.\S+", " ", text)

    if bool(config.get("lowercase_text", True)):
        text = text.lower()

    if bool(config.get("remove_punctuation", True)):
        text = re.sub(r"[^a-z0-9\s]", " ", text)

    if bool(config.get("collapse_spaces", True)):
        text = re.sub(r"\s+", " ", text)

    return text.strip()


def _normalize_unwanted_values(dataframe: pd.DataFrame, config: Dict) -> pd.DataFrame:
    processed = dataframe.copy()
    unwanted_values = config.get(
        "unwanted_values",
        ["", "na", "n/a", "null", "none", "-", "?"],
    )
    unwanted_lookup = {str(item).strip().lower() for item in unwanted_values}

    object_columns = processed.select_dtypes(include=["object", "string"]).columns
    for column in object_columns:
        normalized = processed[column].astype(str).str.strip().str.lower()
        processed.loc[normalized.isin(unwanted_lookup), column] = pd.NA

    return processed


def _looks_like_numeric_series(series: pd.Series) -> bool:
    non_null = series.dropna()
    if non_null.empty:
        return False

    normalized = non_null.astype(str).str.replace(",", "", regex=False).str.strip()
    numeric_pattern = r"^-?\d+(\.\d+)?$"
    return bool(normalized.str.match(numeric_pattern).all())


def _coerce_numeric_like_columns(dataframe: pd.DataFrame, config: Dict) -> pd.DataFrame:
    if not bool(config.get("coerce_numeric_columns", True)):
        return dataframe

    processed = dataframe.copy()
    object_columns = processed.select_dtypes(include=["object", "string"]).columns
    for column in object_columns:
        if _looks_like_numeric_series(processed[column]):
            processed[column] = pd.to_numeric(
                processed[column].astype(str).str.replace(",", "", regex=False),
                errors="coerce",
            )

    return processed


def _apply_null_strategy(dataframe: pd.DataFrame, config: Dict) -> pd.DataFrame:
    processed = dataframe.copy()

    strategy = str(config.get("null_strategy", "drop_any")).strip().lower()
    if not strategy:
        strategy = "drop_any"

    if bool(config.get("drop_nulls", True)) and strategy == "keep":
        strategy = "drop_any"

    if strategy == "drop_any":
        return processed.dropna(how="any")

    if strategy == "drop_all":
        return processed.dropna(how="all")

    if strategy == "fill":
        text_fill_value = config.get("null_fill_text", "")
        numeric_fill_value = config.get("null_fill_numeric", 0)

        object_columns = processed.select_dtypes(include=["object", "string"]).columns
        numeric_columns = processed.select_dtypes(include=["number", "boolean"]).columns

        if len(object_columns) > 0:
            processed[object_columns] = processed[object_columns].fillna(text_fill_value)

        if len(numeric_columns) > 0:
            processed[numeric_columns] = processed[numeric_columns].fillna(numeric_fill_value)

        remaining_columns = [
            column
            for column in processed.columns
            if column not in set(object_columns).union(set(numeric_columns))
        ]
        if remaining_columns:
            processed[remaining_columns] = processed[remaining_columns].fillna(text_fill_value)

        return processed

    return processed


def apply_deterministic_preprocessing(dataframe: pd.DataFrame, config: Dict) -> pd.DataFrame:
    merged_config = {**get_default_preprocess_config(), **(config or {})}

    processed = _normalize_columns(dataframe)

    processed = _normalize_unwanted_values(processed, merged_config)

    object_columns = processed.select_dtypes(include=["object", "string"]).columns
    for column in object_columns:
        if bool(merged_config.get("cleanup_text", True)):
            processed[column] = processed[column].map(
                lambda value: value
                if pd.isna(value)
                else _cleanup_text_with_config(str(value), merged_config)
            )

    processed = _coerce_numeric_like_columns(processed, merged_config)
    processed = _apply_null_strategy(processed, merged_config)

    if bool(merged_config.get("drop_duplicates", True)):
        processed = processed.drop_duplicates(keep="first")

    if bool(merged_config.get("sort_rows", True)):
        sort_columns = list(processed.columns)
        if sort_columns:
            processed = processed.sort_values(
                by=sort_columns,
                kind="mergesort",
                na_position="last",
            )
    processed = processed.reset_index(drop=True)

    return processed
