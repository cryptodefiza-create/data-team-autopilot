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
