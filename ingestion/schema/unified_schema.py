from dataclasses import dataclass
from typing import Optional


@dataclass
class MachineEvent:
    timestamp: str
    machine_id: str
    machine_type: str

    temperature: Optional[float] = None
    pressure: Optional[float] = None
    vibration: Optional[float] = None
    status: Optional[str] = None


# einheitliches datan schema