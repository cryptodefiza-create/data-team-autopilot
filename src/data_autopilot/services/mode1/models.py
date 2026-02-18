from __future__ import annotations

from enum import Enum
from typing import Any

from pydantic import BaseModel, Field


class Intent(str, Enum):
    SNAPSHOT = "snapshot"
    TREND = "trend"
    LOOKUP = "lookup"


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


class OutputFormat(str, Enum):
    TABLE = "table"
    RAW = "raw"


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
