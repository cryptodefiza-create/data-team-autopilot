from __future__ import annotations

from datetime import datetime, timezone
from enum import Enum
from typing import Any

from pydantic import BaseModel, Field


class Intent(str, Enum):
    SNAPSHOT = "snapshot"
    TREND = "trend"
    LOOKUP = "lookup"
    FOLLOW_UP = "follow_up"
    EXPORT = "export"
    TRACK = "track"


class Chain(str, Enum):
    SOLANA = "solana"
    ETHEREUM = "ethereum"
    CROSS_CHAIN = "cross_chain"


class Entity(str, Enum):
    TOKEN_HOLDERS = "token_holders"
    TOKEN_BALANCES = "token_balances"
    TOKEN_PRICE = "token_price"
    TOKEN_INFO = "token_info"
    PRICE_HISTORY = "price_history"
    ASSET_TRANSFERS = "asset_transfers"
    TRANSACTION_HISTORY = "transaction_history"
    NFT_ASSET = "nft_asset"
    LOGS = "logs"
    DEX_PAIR = "dex_pair"
    PROTOCOL_TVL = "protocol_tvl"
    CHAIN_TVL = "chain_tvl"


class OutputFormat(str, Enum):
    TABLE = "table"
    RAW = "raw"
    XLSX = "xlsx"
    CSV = "csv"


class RoutingMode(str, Enum):
    PUBLIC_API = "public_api"
    WAREHOUSE = "warehouse"
    SAAS = "saas"
    ASK_USER = "ask_user"


class DataRequest(BaseModel):
    raw_message: str
    intent: Intent = Intent.SNAPSHOT
    chain: Chain = Chain.CROSS_CHAIN
    entity: Entity = Entity.TOKEN_PRICE
    token: str = ""
    address: str = ""
    time_range_days: int = 0
    output_format: OutputFormat = OutputFormat.TABLE


class RoutingDecision(BaseModel):
    mode: RoutingMode = RoutingMode.PUBLIC_API
    confidence: float = Field(default=0.0, ge=0.0, le=1.0)
    provider_name: str = ""
    method_name: str = ""
    reason: str = ""


class ProviderResult(BaseModel):
    provider: str
    method: str
    records: list[dict[str, Any]] = Field(default_factory=list)
    total_available: int = 0
    truncated: bool = False
    error: str | None = None

    @property
    def succeeded(self) -> bool:
        return self.error is None


class Provenance(BaseModel):
    source: str = ""
    timestamp: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    chain: str | None = None
    params: dict[str, Any] = Field(default_factory=dict)
    record_count: int = 0
    truncated: bool = False
    sampling_note: str | None = None
    disclaimer: str | None = None
    filters: dict[str, Any] = Field(default_factory=dict)

    def format_footer(self) -> str:
        lines = [
            "\u2500" * 35,
            f"Source: {self.source}",
            f"Queried: {self.timestamp.strftime('%b %d, %Y %H:%M UTC')}",
            f"Records: {self.record_count:,}"
            + (" (truncated)" if self.truncated else " (complete)"),
        ]
        if self.sampling_note:
            lines.append(f"Note: {self.sampling_note}")
        if self.disclaimer:
            lines.append(f"Disclaimer: {self.disclaimer}")
        lines.append("\u2500" * 35)
        return "\n".join(lines)


class Interpretation(BaseModel):
    text: str = ""
    stats: dict[str, Any] = Field(default_factory=dict)
    disclaimer: str = "These are data observations, not financial or investment advice."


class ConversationTurn(BaseModel):
    request: DataRequest
    data: list[dict[str, Any]] = Field(default_factory=list)
    timestamp: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))


class RawDataset(BaseModel):
    records: list[dict[str, Any]] = Field(default_factory=list)
    source: str = "file_upload"
    record_count: int = 0


class StorageConfig(BaseModel):
    type: str = "mock"  # "mock" | "neon_postgres"
    project_id: str = ""
    connection_uri: str = ""
    schemas: list[str] = Field(default_factory=lambda: ["raw", "staging", "marts", "analytics"])


class PipelineStatus(str, Enum):
    ACTIVE = "active"
    PAUSED = "paused"
    FAILED = "failed"
    STALE = "stale"


class Pipeline(BaseModel):
    id: str = ""
    org_id: str = ""
    entity: str = ""
    chain: str = ""
    token: str = ""
    address: str = ""
    query_params: dict[str, Any] = Field(default_factory=dict)
    schedule: str = "daily"
    status: PipelineStatus = PipelineStatus.ACTIVE
    last_run: datetime | None = None
    last_success: datetime | None = None
    last_error: str | None = None
    run_count: int = 0
    created_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))


class SnapshotRecord(BaseModel):
    id: int = 0
    ingested_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    source: str = ""
    entity: str = ""
    query_params: dict[str, Any] = Field(default_factory=dict)
    record_id: str = ""
    payload_hash: str = ""
    payload: dict[str, Any] = Field(default_factory=dict)


class PipelineHealth(BaseModel):
    pipeline_id: str = ""
    source: str = ""
    entity: str = ""
    schedule: str = ""
    last_run: datetime | None = None
    last_success: datetime | None = None
    status: PipelineStatus = PipelineStatus.ACTIVE
    error: str | None = None
    hours_since_sync: float | None = None
    alert: str | None = None


# ---------- Phase 4: Connected Sources ----------


class CredentialRecord(BaseModel):
    id: str = ""
    org_id: str = ""
    source: str = ""
    created_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    validated: bool = False


class ThinContract(BaseModel):
    revenue_definition: str = "gross"  # "gross" | "net_after_refunds" | "net_after_refunds_and_tax"
    timezone: str = "UTC"
    exclude_test_orders: bool = True
    currency: str = "USD"


class SyncStatus(BaseModel):
    connection_id: str = ""
    status: str = "pending"  # "pending" | "running" | "completed" | "failed"
    rows_synced: int = 0
    started_at: datetime | None = None
    completed_at: datetime | None = None
    error: str | None = None


class ConnectedSource(BaseModel):
    org_id: str = ""
    source: str = ""  # "shopify" | "stripe"
    shop_domain: str = ""
    credential_id: str = ""
    contract: ThinContract = Field(default_factory=ThinContract)
    connected_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    stats: dict[str, Any] = Field(default_factory=dict)


# ---------- Phase 5: Warehouse Connection ----------


class ColumnProfile(BaseModel):
    name: str = ""
    data_type: str = ""
    nullable: bool = True
    is_primary_key: bool = False


class TableProfile(BaseModel):
    name: str = ""
    columns: list[ColumnProfile] = Field(default_factory=list)
    row_count: int = 0
    sample: list[dict[str, Any]] = Field(default_factory=list)
    detected_keys: list[str] = Field(default_factory=list)
    detected_time_columns: list[str] = Field(default_factory=list)
    detected_relationships: list[dict[str, str]] = Field(default_factory=list)


class SchemaProfile(BaseModel):
    tables: list[TableProfile] = Field(default_factory=list)

    @property
    def table_names(self) -> list[str]:
        return [t.name for t in self.tables]

    def to_llm_format(self) -> str:
        lines = []
        for t in self.tables:
            cols = ", ".join(f"{c.name} ({c.data_type})" for c in t.columns)
            lines.append(f"- {t.name} ({t.row_count:,} rows): {cols}")
        return "\n".join(lines)


class EntityAlias(BaseModel):
    table_name: str = ""
    alias: str = ""
    org_id: str = ""


class SQLQuery(BaseModel):
    sql: str = ""
    validated: bool = False
    estimated_cost: float | None = None
    error: str | None = None
