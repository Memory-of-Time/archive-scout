from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


@dataclass(slots=True)
class AIRequest:
    instructions: str
    payload: dict[str, Any]
    schema_name: str
    schema: dict[str, Any]
    max_output_tokens: int = 1200


@dataclass(slots=True)
class AIResponse:
    data: dict[str, Any]
    provider: str
    model: str
    request_id: str = ""
    usage: dict[str, Any] = field(default_factory=dict)
