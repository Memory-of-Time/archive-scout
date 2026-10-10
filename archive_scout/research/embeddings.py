from __future__ import annotations

import hashlib
import math
import re
import struct
from dataclasses import dataclass
from functools import lru_cache
from typing import Iterable

from ..utils import normalize_search

TOKEN_RE = re.compile(r"[\w'-]+", re.UNICODE)


@dataclass(slots=True)
class EncodedVector:
    values: tuple[float, ...]
    blob: bytes
    norm: float
    token_count: int


def _feature_stream(tokens: list[str]):
    # Yield features instead of materializing a second potentially huge list.
    for token in tokens:
        if len(token) > 1:
            yield "w:" + token
            padded = f"^{token}$"
            for index in range(max(0, len(padded) - 2)):
                yield "c:" + padded[index:index + 3]
    for index in range(max(0, len(tokens) - 1)):
        yield "b:" + tokens[index] + " " + tokens[index + 1]


def local_hash_vector(text: str, dimensions: int = 256) -> EncodedVector:
    dimensions = max(64, min(1024, int(dimensions)))
    values = [0.0] * dimensions
    tokens = TOKEN_RE.findall(normalize_search(text))
    token_count = len(tokens)
    counts: dict[str, int] = {}
    for feature in _feature_stream(tokens):
        counts[feature] = counts.get(feature, 0) + 1
    for feature, count in counts.items():
        digest = hashlib.blake2b(feature.encode("utf-8", "replace"), digest_size=16).digest()
        index = int.from_bytes(digest[:8], "little") % dimensions
        sign = 1.0 if digest[8] & 1 else -1.0
        # Log-scaled term frequency prevents long pages from dominating.
        values[index] += sign * (1.0 + math.log1p(count))
    norm = math.sqrt(sum(value * value for value in values)) or 1.0
    unit = tuple(value / norm for value in values)
    blob = bytes(struct.pack(f"<{dimensions}b", *(max(-127, min(127, int(round(v * 127)))) for v in unit)))
    return EncodedVector(unit, blob, 1.0 if counts else 0.0, token_count)


def decode_int8_vector(blob: bytes, dimensions: int) -> tuple[float, ...]:
    dimensions = max(1, int(dimensions))
    if len(blob) != dimensions:
        return ()
    raw = struct.unpack(f"<{dimensions}b", blob)
    values = tuple(value / 127.0 for value in raw)
    norm = math.sqrt(sum(value * value for value in values)) or 1.0
    return tuple(value / norm for value in values)


def cosine(left: Iterable[float], right: Iterable[float]) -> float:
    return sum(a * b for a, b in zip(left, right))


def cosine_int8(left: Iterable[float], blob: bytes, dimensions: int) -> float:
    """Cosine against a stored normalized int8 vector without tuple decoding."""
    dimensions = max(1, int(dimensions))
    if len(blob) != dimensions:
        return 0.0
    right = memoryview(blob).cast("b")
    dot = 0.0
    norm_sq = 0
    for a, b in zip(left, right):
        value = int(b)
        dot += float(a) * value
        norm_sq += value * value
    if not norm_sq:
        return 0.0
    return dot / math.sqrt(norm_sq)


def vector_bands(values: tuple[float, ...], bands: int = 8) -> list[tuple[int, str]]:
    """Return compact locality buckets for approximate nearest-neighbour lookup.

    Each bucket uses sign bits from evenly distributed dimensions plus the four
    strongest dimensions in the band. It is deterministic, tiny, and requires
    no native vector database extension.
    """
    if not values:
        return []
    bands = max(2, min(16, int(bands)))
    width = max(1, len(values) // bands)
    result: list[tuple[int, str]] = []
    for band in range(bands):
        start = band * width
        end = len(values) if band == bands - 1 else min(len(values), start + width)
        chunk = values[start:end]
        if not chunk:
            continue
        step = max(1, len(chunk) // 12)
        sign_bits = 0
        for bit, index in enumerate(range(0, len(chunk), step)):
            if bit >= 16:
                break
            if chunk[index] >= 0:
                sign_bits |= 1 << bit
        strongest = sorted(range(len(chunk)), key=lambda i: abs(chunk[i]), reverse=True)[:4]
        payload = f"{sign_bits:04x}:" + ",".join(str(i) for i in sorted(strongest))
        result.append((band, payload))
    return result


@lru_cache(maxsize=2)
def _fastembed_model(model_name: str):
    try:
        from fastembed import TextEmbedding  # type: ignore
    except Exception as exc:  # pragma: no cover - optional dependency
        raise RuntimeError(
            "FastEmbed is not installed. Use the built-in local-hash backend or install the optional research extra."
        ) from exc
    return TextEmbedding(model_name=model_name)


def fastembed_vector(text: str, dimensions: int = 384, model_name: str = "BAAI/bge-small-en-v1.5") -> EncodedVector:
    model = _fastembed_model(model_name)
    try:
        raw = next(iter(model.embed([text])))
    except StopIteration as exc:  # pragma: no cover
        raise RuntimeError("FastEmbed returned no vector") from exc
    values = tuple(float(value) for value in raw)
    norm = math.sqrt(sum(value * value for value in values)) or 1.0
    unit = tuple(value / norm for value in values)
    blob = bytes(struct.pack(f"<{len(unit)}b", *(max(-127, min(127, int(round(v * 127)))) for v in unit)))
    return EncodedVector(unit, blob, 1.0, len(TOKEN_RE.findall(normalize_search(text))))


def encode_text(text: str, backend: str, dimensions: int) -> tuple[str, EncodedVector]:
    backend = (backend or "local-hash").strip().casefold()
    if backend == "fastembed":
        vector = fastembed_vector(text)
        return "fastembed:bge-small-en-v1.5", vector
    return "local-hash-v1", local_hash_vector(text, dimensions)
