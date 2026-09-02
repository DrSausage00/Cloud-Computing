from dataclasses import dataclass, field
from typing import Any


@dataclass
class MachineEvent:
    timestamp: str
    machine_id: str
    machine_type: str
    measurements: dict[str, Any] = field(default_factory=dict)
    schema_version: str = "1.0"