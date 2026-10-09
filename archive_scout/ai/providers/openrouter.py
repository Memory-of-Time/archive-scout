from __future__ import annotations

import json
import re

import httpx

from ...environment import environment_value
from ...constants import VERSION
from ..models import AIRequest, AIResponse

OPENROUTER_CHAT_URL = "https://openrouter.ai/api/v1/chat/completions"
_FENCE = re.compile(r"^\s*```(?:json)?\s*(.*?)\s*```\s*$", re.I | re.S)


def _json_text(value: object) -> dict:
    text = str(value or "").strip()
    match = _FENCE.match(text)
    if match:
        text = match.group(1).strip()
    return json.loads(text)


class OpenRouterProvider:
    name = "openrouter"

    def __init__(self, api_key: str, model: str, timeout: float = 120.0) -> None:
        self.model = model.strip() or "anthropic/claude-sonnet-4.5"
        headers = {
            "Authorization": f"Bearer {api_key}",
            "Content-Type": "application/json",
            "User-Agent": f"ArchiveScout/{VERSION}",
        }
        referer = environment_value("OPENROUTER_HTTP_REFERER").strip()
        title = environment_value("OPENROUTER_APP_TITLE", "Archive Scout").strip()
        if referer:
            headers["HTTP-Referer"] = referer
        if title:
            headers["X-Title"] = title
        self.client = httpx.Client(
            timeout=httpx.Timeout(timeout, connect=min(30.0, timeout)),
            headers=headers,
        )

    def close(self) -> None:
        self.client.close()

    def generate(self, request: AIRequest) -> AIResponse:
        schema_text = json.dumps(request.schema, ensure_ascii=False, separators=(",", ":"))
        system = (
            request.instructions
            + "\nReturn only one JSON object. It must conform to this JSON Schema: "
            + schema_text
        )
        body = {
            "model": self.model,
            "messages": [
                {"role": "system", "content": system},
                {"role": "user", "content": json.dumps(request.payload, ensure_ascii=False)},
            ],
            "temperature": 0,
            "max_tokens": max(256, int(request.max_output_tokens)),
        }
        response = self.client.post(OPENROUTER_CHAT_URL, json=body)
        if response.status_code >= 400:
            detail = ""
            try:
                detail = str((response.json().get("error") or {}).get("message") or "")
            except Exception:
                detail = response.text[:500]
            raise RuntimeError(f"OpenRouter HTTP {response.status_code}: {detail or 'request failed'}")
        payload = response.json()
        choices = payload.get("choices") or []
        if not choices:
            raise RuntimeError("OpenRouter returned no completion")
        content = (choices[0].get("message") or {}).get("content")
        if isinstance(content, list):
            content = "".join(str(item.get("text") or "") for item in content if isinstance(item, dict))
        data = _json_text(content)
        return AIResponse(
            data=data,
            provider=self.name,
            model=self.model,
            request_id=str(payload.get("id") or ""),
            usage=dict(payload.get("usage") or {}),
        )
