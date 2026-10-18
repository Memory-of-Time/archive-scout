from __future__ import annotations

from collections import deque
from dataclasses import dataclass, field
from typing import Iterable

try:
    import ahocorasick_rs
except Exception:  # pragma: no cover - source checkouts can run without optional wheel
    ahocorasick_rs = None


@dataclass(slots=True)
class _Node:
    transitions: dict[str, int] = field(default_factory=dict)
    failure: int = 0
    outputs: tuple[str, ...] = ()


class LiteralAutomaton:
    """Compact Aho-Corasick matcher for normalized literal keyword rules.

    Archive Scout keeps the full regular-expression matcher for rules that need
    case sensitivity, whole-word boundaries, or regex semantics. This matcher is
    only the fast gate for ordinary normalized literals, so one pass over a field
    replaces an increasingly expensive giant alternation regex.
    """

    __slots__ = ("_nodes", "_native", "patterns")

    def __init__(self, patterns: Iterable[str]) -> None:
        unique = tuple(dict.fromkeys(value for value in patterns if value))
        self.patterns = unique
        self._native = None
        if unique and ahocorasick_rs is not None:
            # Rust-backed matching releases the GIL for strings, allowing the
            # download/scanning worker pool to keep multiple pages moving.
            self._native = ahocorasick_rs.AhoCorasick(unique, store_patterns=True)
        self._nodes: list[_Node] = [_Node()]
        if self._native is not None:
            return
        for pattern in unique:
            state = 0
            for character in pattern:
                next_state = self._nodes[state].transitions.get(character)
                if next_state is None:
                    next_state = len(self._nodes)
                    self._nodes[state].transitions[character] = next_state
                    self._nodes.append(_Node())
                state = next_state
            self._nodes[state].outputs = (*self._nodes[state].outputs, pattern)
        self._build_failures()

    def __bool__(self) -> bool:
        return bool(self.patterns)

    def _build_failures(self) -> None:
        queue: deque[int] = deque()
        for state in self._nodes[0].transitions.values():
            self._nodes[state].failure = 0
            queue.append(state)
        while queue:
            state = queue.popleft()
            for character, next_state in self._nodes[state].transitions.items():
                queue.append(next_state)
                failure = self._nodes[state].failure
                while failure and character not in self._nodes[failure].transitions:
                    failure = self._nodes[failure].failure
                fallback = self._nodes[failure].transitions.get(character, 0)
                self._nodes[next_state].failure = fallback
                inherited = self._nodes[fallback].outputs
                if inherited:
                    self._nodes[next_state].outputs = tuple(
                        dict.fromkeys((*self._nodes[next_state].outputs, *inherited))
                    )

    def search_any(self, text: str) -> bool:
        if not text or not self.patterns:
            return False
        if self._native is not None:
            return bool(self._native.find_matches_as_indexes(text))
        state = 0
        nodes = self._nodes
        for character in text:
            while state and character not in nodes[state].transitions:
                state = nodes[state].failure
            state = nodes[state].transitions.get(character, 0)
            if nodes[state].outputs:
                return True
        return False

    def find_into(self, text: str, matches: set[str]) -> None:
        """Add matches to an existing set without allocating one set per field."""
        if not text or not self.patterns:
            return
        if self._native is not None:
            for pattern_index, _start, _end in self._native.find_matches_as_indexes(text, overlapping=True):
                matches.add(self.patterns[pattern_index])
            return
        state = 0
        nodes = self._nodes
        for character in text:
            while state and character not in nodes[state].transitions:
                state = nodes[state].failure
            state = nodes[state].transitions.get(character, 0)
            if nodes[state].outputs:
                matches.update(nodes[state].outputs)

    def find(self, text: str) -> set[str]:
        matches: set[str] = set()
        self.find_into(text, matches)
        return matches
