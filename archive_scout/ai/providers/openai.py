from __future__ import annotations

import json

import httpx

from ...constants import VERSION
from ..models import AIRequest, AIResponse

OPENAI_RESPONSES_URL = "https://api.openai.com/v1/responses"


class OpenAIProvider:
    name = "openai"

    def __init__(self, api_key: str, model: str, timeout: float = 120.0) -> None:
        self.model = model.strip() or "gpt-5-mini"
        self.client = httpx.Client(
            timeout=httpx.Timeout(timeout, connect=min(30.0, timeout)),
            headers={
                "Authorization": f"Bearer {api_key}",
                "Content-Type": "application/json",
                "User-Agent": f"ArchiveScout/{VERSION}",
            },
        )

    def close(self) -> None:
        self.client.close()

    @staticmethod
    def _output_text(payload: dict) -> str:
        for item in payload.get("output") or []:
            if item.get("type") != "message":
                continue
            for content in item.get("content") or []:
                if content.get("type") == "output_text" and content.get("text"):
                    return str(content["text"])
        raise RuntimeError("OpenAI returned a response without usable structured text")

    def generate(self, request: AIRequest) -> AIResponse:
        body = {
            "model": self.model,
            "store": False,
            "instructions": request.instructions,
            "input": json.dumps(request.payload, ensure_ascii=False),
            "text": {
                "format": {
                    "type": "json_schema",
                    "name": request.schema_name,
                    "schema": request.schema,
                    "strict": True,
                }
            },
            "max_output_tokens": max(256, int(request.max_output_tokens)),
        }
        response = self.client.post(OPENAI_RESPONSES_URL, json=body)
        if response.status_code >= 400:
            detail = ""
            try:
                detail = str((response.json().get("error") or {}).get("message") or "")
            except Exception:
                detail = response.text[:500]
            raise RuntimeError(f"OpenAI HTTP {response.status_code}: {detail or 'request failed'}")
        payload = response.json()
        text = self._output_text(payload)
        data = json.loads(text)
        return AIResponse(
            data=data,
            provider=self.name,
            model=self.model,
            request_id=str(payload.get("id") or ""),
            usage=dict(payload.get("usage") or {}),
        )
