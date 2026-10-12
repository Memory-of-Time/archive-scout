from __future__ import annotations

from dataclasses import dataclass

from ..environment import environment_value, load_environment


class AIConfigurationError(RuntimeError):
    pass


@dataclass(slots=True)
class ProviderSettings:
    provider: str
    model: str
    api_key: str
    timeout: float


def default_model(provider: str) -> str:
    return "anthropic/claude-sonnet-4.5" if provider == "openrouter" else "gpt-5-mini"


def resolve_provider_settings(
    provider: str,
    model: str,
    timeout: float,
    explicit_key: str = "",
) -> ProviderSettings:
    load_environment()
    provider = provider.strip().casefold() or "openai"
    if provider not in {"openai", "openrouter"}:
        raise AIConfigurationError("AI provider must be openai or openrouter")
    env_name = "OPENROUTER_API_KEY" if provider == "openrouter" else "OPENAI_API_KEY"
    key = explicit_key.strip() or environment_value(env_name).strip()
    if not key:
        raise AIConfigurationError(f"{env_name} is not configured")
    selected_model = model.strip() or environment_value("AI_MODEL").strip() or default_model(provider)
    return ProviderSettings(provider, selected_model, key, max(15.0, float(timeout)))
