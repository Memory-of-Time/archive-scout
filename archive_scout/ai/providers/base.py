from __future__ import annotations

from typing import Protocol

from ..models import AIRequest, AIResponse


class AIProvider(Protocol):
    name: str
    model: str

    def generate(self, request: AIRequest) -> AIResponse:
        ...

    def close(self) -> None:
        ...
