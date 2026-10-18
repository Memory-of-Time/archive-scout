from __future__ import annotations

import threading
import time

from ..events import Stopped
from .models import AIRequest, AIResponse
from .providers.openai import OpenAIProvider
from .providers.openrouter import OpenRouterProvider
from .settings import AIConfigurationError, resolve_provider_settings


class AIServiceError(RuntimeError):
    pass


class AIService:
    def __init__(self, provider: str, model: str, timeout: float, api_key: str = "") -> None:
        try:
            settings = resolve_provider_settings(provider, model, timeout, api_key)
        except AIConfigurationError as exc:
            raise AIServiceError(str(exc)) from exc
        self.settings = settings
        if settings.provider == "openrouter":
            self.provider = OpenRouterProvider(settings.api_key, settings.model, settings.timeout)
        else:
            self.provider = OpenAIProvider(settings.api_key, settings.model, settings.timeout)

    def close(self) -> None:
        self.provider.close()

    def generate_json(self, request: AIRequest, stop_event: threading.Event, attempts: int = 3) -> AIResponse:
        last: Exception | None = None
        for attempt in range(max(1, int(attempts))):
            if stop_event.is_set():
                raise Stopped
            try:
                response = self.provider.generate(request)
                if not isinstance(response.data, dict):
                    raise AIServiceError("AI provider returned a non-object JSON response")
                return response
            except Stopped:
                raise
            except Exception as exc:
                last = exc
                if attempt + 1 >= attempts:
                    break
                stop_event.wait(min(8.0, 1.5 * (2**attempt)))
        raise AIServiceError(str(last or "AI request failed")) from last
