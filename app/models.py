from dataclasses import dataclass
from enum import Enum
from typing import Optional


class JobStatus(str, Enum):
    QUEUED = "queued"
    PROCESSING = "processing"
    COMPLETED = "completed"
    FAILED = "failed"


@dataclass
class Job:
    id: str
    payload: str
    status: JobStatus = JobStatus.QUEUED
    error: Optional[str] = None

    def to_dict(self) -> dict:
        return {
            "id": self.id,
            "payload": self.payload,
            "status": self.status.value,
            "error": self.error,
        }
