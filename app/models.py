from dataclasses import dataclass
from typing import Optional


@dataclass
class Job:
    id: str
    payload: str
    status: str = "queued"
    error: Optional[str] = None

    def to_dict(self) -> dict:
        return {
            "id": self.id,
            "payload": self.payload,
            "status": self.status,
            "error": self.error,
        }
