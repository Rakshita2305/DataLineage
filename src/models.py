from dataclasses import dataclass, field
from dataclasses import asdict
from typing import Any, Dict, Optional


@dataclass
class VersionRecord:
    version_id: str
    parent_id: Optional[str]
    timestamp: str
    commit_message: str
    source_data_path: str
    source_config_path: str
    input_hash: str
    config_hash: str
    version_hash: str
    row_count: int = 0
    label_distribution: Dict[str, int] = field(default_factory=dict)
    eval_metrics: Optional[Dict[str, Any]] = None

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)
