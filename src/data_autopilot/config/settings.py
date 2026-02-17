from functools import lru_cache
import json

from pydantic import Field, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file='.env', extra='ignore')

    app_name: str = "Data Team Autopilot"
    environment: str = Field(default="dev")
    database_url: str = Field(default="sqlite+pysqlite:///./autopilot.db")

    allow_real_query_execution: bool = Field(default=False)
    default_query_limit: int = Field(default=10_000)
    per_org_max_workflows: int = Field(default=3)
    per_org_max_profile_workflows: int = Field(default=2)

    redis_url: str = Field(default="redis://localhost:6379/0")
    query_cache_ttl_seconds: int = Field(default=300)
    schema_cache_ttl_seconds: int = Field(default=3600)
    org_hourly_budget_bytes: int = Field(default=50 * 1024**3)
    per_query_max_bytes: int = Field(default=10 * 1024**3)
    per_query_max_bytes_with_approval: int = Field(default=100 * 1024**3)

    # Metabase
    metabase_mock_mode: bool = Field(default=True)
    metabase_url: str = Field(default="http://localhost:3000")
    metabase_api_key: str = Field(default="")

    # BigQuery
    bigquery_mock_mode: bool = Field(default=True)
    bigquery_project_id: str = Field(default="")
    bigquery_location: str = Field(default="US")
    bigquery_service_account_json: str = Field(default="")
    run_startup_connection_tests: bool = Field(default=False)
    simulate_llm_unavailable: bool = Field(default=False)
    simulate_warehouse_unavailable: bool = Field(default=False)

    # LLM — primary provider (used for all production requests)
    llm_api_base_url: str = Field(default="https://api.openai.com/v1")
    llm_api_key: str = Field(default="")
    llm_model: str = Field(default="")
    llm_timeout_seconds: int = Field(default=30)
    llm_temperature: float = Field(default=0.0)

    # LLM — GPT-5 Mini (evaluation provider)
    gpt5_mini_api_key: str = Field(default="")
    gpt5_mini_model: str = Field(default="gpt-5-mini")
    gpt5_mini_base_url: str = Field(default="https://api.openai.com/v1")
    gpt5_mini_enabled: bool = Field(default=False)

    # LLM — Claude Sonnet (evaluation provider)
    claude_sonnet_api_key: str = Field(default="")
    claude_sonnet_model: str = Field(default="claude-sonnet-4-5-20250929")
    claude_sonnet_base_url: str = Field(default="https://api.anthropic.com/v1")
    claude_sonnet_enabled: bool = Field(default=False)

    # LLM — evaluation (legacy JSON override still supported)
    llm_eval_providers_json: str = Field(
        default="[]",
        description="Optional JSON array of additional eval providers beyond GPT-5 Mini and Claude Sonnet",
    )
    llm_eval_enabled: bool = Field(default=False, description="Enable parallel evaluation runs")
    llm_monthly_budget_usd: float = Field(default=100.0, description="Per-org monthly LLM budget soft cap in USD")

    # Slack integration
    slack_signing_secret: str = Field(default="")
    slack_bot_token: str = Field(default="")
    slack_default_org_id: str = Field(default="")

    # Telegram integration
    telegram_bot_token: str = Field(default="")
    telegram_webhook_secret: str = Field(default="")
    telegram_default_org_id: str = Field(default="")

    @model_validator(mode="after")
    def validate_runtime_modes(self) -> "Settings":
        # Hard safety gate: never allow real execution while connector is in mock mode.
        if self.allow_real_query_execution and self.bigquery_mock_mode:
            raise ValueError("ALLOW_REAL_QUERY_EXECUTION=true requires BIGQUERY_MOCK_MODE=false")

        if not self.metabase_mock_mode:
            if not self.metabase_url or not self.metabase_api_key:
                raise ValueError(
                    "METABASE_MOCK_MODE=false requires METABASE_URL and METABASE_API_KEY"
                )

        if not self.bigquery_mock_mode and not self.bigquery_project_id:
            raise ValueError("BIGQUERY_MOCK_MODE=false requires BIGQUERY_PROJECT_ID")

        if self.bigquery_service_account_json:
            try:
                parsed = json.loads(self.bigquery_service_account_json)
            except Exception as exc:
                raise ValueError("BIGQUERY_SERVICE_ACCOUNT_JSON must be valid JSON") from exc
            if not isinstance(parsed, dict):
                raise ValueError("BIGQUERY_SERVICE_ACCOUNT_JSON must be a JSON object")

        if self.llm_temperature < 0 or self.llm_temperature > 2:
            raise ValueError("LLM_TEMPERATURE must be between 0 and 2")

        if self.llm_eval_providers_json:
            try:
                providers = json.loads(self.llm_eval_providers_json)
            except Exception as exc:
                raise ValueError("LLM_EVAL_PROVIDERS_JSON must be valid JSON") from exc
            if not isinstance(providers, list):
                raise ValueError("LLM_EVAL_PROVIDERS_JSON must be a JSON array")
            for p in providers:
                if not isinstance(p, dict):
                    raise ValueError("Each eval provider must be a JSON object")
                for key in ("name", "base_url", "api_key", "model"):
                    if not p.get(key):
                        raise ValueError(f"Eval provider missing required key: {key}")

        return self


@lru_cache
def get_settings() -> Settings:
    return Settings()
