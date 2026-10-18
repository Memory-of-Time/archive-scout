from __future__ import annotations

from dataclasses import asdict, dataclass, field
from typing import Any


class Stopped(RuntimeError):
    pass


class ConnectivityPaused(RuntimeError):
    """A recoverable network pause with the exact queue safely persisted."""


class IndexResponsePaused(ConnectivityPaused):
    """A query-specific response failure; completed pages remain saved."""

    scope = "query"


@dataclass(slots=True)
class ProgressEvent:
    stage: str
    message: str
    current: int | None = None
    total: int | None = None
    detail: dict = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return {
            "stage": self.stage,
            "message": self.message,
            "current": self.current,
            "total": self.total,
            "detail": dict(self.detail or {}),
        }


def progress_event_payload(event: ProgressEvent) -> dict[str, Any]:
    return event.to_dict()
