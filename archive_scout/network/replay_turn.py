from __future__ import annotations

import time
from contextvars import ContextVar
from dataclasses import dataclass


class ReplayTurnExpired(RuntimeError):
    """A text replay exceeded its active budget and remains retryable."""


class ReplayTurnInterrupted(RuntimeError):
    """A shared pause interrupted an already started replay."""


class ReplayDispatchDeferred(RuntimeError):
    """No request reached the network because shared admission is closed."""


@dataclass(slots=True)
class ReplayTurn:
    seconds: float
    deadline: float | None = None

    def start(self) -> None:
        if self.deadline is None:
            self.deadline = time.monotonic() + self.seconds
        self.remaining()

    def remaining(self, timeout: float | None = None) -> float | None:
        if self.deadline is None:
            return timeout
        remaining = self.deadline - time.monotonic()
        if remaining <= 0:
            raise ReplayTurnExpired("text replay exceeded its active deadline; capture remains retryable")
        return remaining if timeout is None else min(remaining, timeout)


replay_turn: ContextVar[ReplayTurn | None] = ContextVar("text_replay_turn", default=None)


def replay_timeout(timeout: float | None = None) -> float | None:
    turn = replay_turn.get()
    return turn.remaining(timeout) if turn is not None else timeout
