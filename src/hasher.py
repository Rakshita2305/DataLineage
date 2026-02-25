import hashlib
import json
from typing import Dict

import pandas as pd


def dataframe_to_stable_csv_bytes(dataframe: pd.DataFrame) -> bytes:
    csv_text = dataframe.to_csv(index=False, lineterminator="\n")
    return csv_text.encode("utf-8")


def sha256_from_bytes(payload: bytes) -> str:
    return hashlib.sha256(payload).hexdigest()


def sha256_from_json(data: Dict) -> str:
    canonical_json = json.dumps(data, sort_keys=True, ensure_ascii=False, separators=(",", ":"))
    return sha256_from_bytes(canonical_json.encode("utf-8"))


def build_version_hash(input_hash: str, config_hash: str) -> str:
    combined = f"{input_hash}:{config_hash}".encode("utf-8")
    return sha256_from_bytes(combined)
