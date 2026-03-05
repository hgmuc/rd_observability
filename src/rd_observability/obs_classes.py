from __future__ import annotations
from dataclasses import dataclass, field
from typing import Any


@dataclass
class Result:
    # core semantic fields
    data_cnt: int = 0
    error_cnt: int = 0
    warnings: int = 0
    success: bool = True

    # free-form metrics
    metrics: dict[str, Any] = field(default_factory=dict)

    # metadata (optional)
    duration_sec: float | None = None
    stage: str | None = None

    def add_metric(self, key: str, value: Any):
        self.metrics[key] = value

    def to_dict(self) -> dict[str, Any]:
        """Flatten for JSON logging."""
        base: dict[str, Any] = {
            "rel_cnt": self.data_cnt + self.error_cnt,
            "data_cnt": self.data_cnt,
            "error_cnt": self.error_cnt,
            "warnings": self.warnings,
            "success": self.success,
            "duration_sec": self.duration_sec,
            "stage": self.stage,
            "type": "Result"
        }
        base.update(self.metrics)
        return base
