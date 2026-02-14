from functools import lru_cache

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
    run_startup_connection_tests: bool = Field(default=False)
    simulate_llm_unavailable: bool = Field(default=False)
    simulate_warehouse_unavailable: bool = Field(default=False)

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

        return self


@lru_cache
def get_settings() -> Settings:
    return Settings()
