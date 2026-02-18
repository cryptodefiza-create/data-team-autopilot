from __future__ import annotations

import logging
from typing import Any

from data_autopilot.services.mode1.credential_vault import CredentialVault
from data_autopilot.services.mode1.models import PortfolioAsset, PortfolioReport

logger = logging.getLogger(__name__)

# Permissions that are NOT allowed for connected CEX keys
_UNSAFE_PERMISSIONS = {"trading", "withdrawal", "transfer", "futures", "margin"}


class CEXConnected:
    """Customer's exchange account data — requires read-only API key.

    Validates that API keys are read-only before storing.
    """

    def __init__(self, vault: CredentialVault | None = None, mock_mode: bool = False) -> None:
        self._vault = vault
        self._mock_mode = mock_mode
        self._mock_balances: dict[str, list[dict[str, Any]]] = {}
        self._mock_prices: dict[str, float] = {}

    def register_mock_balances(self, org_id: str, balances: list[dict[str, Any]]) -> None:
        self._mock_balances[org_id] = balances

    def register_mock_prices(self, prices: dict[str, float]) -> None:
        self._mock_prices = prices

    def validate_key_permissions(
        self, exchange: str, permissions: list[str]
    ) -> dict[str, Any]:
        """Verify that API key is read-only. Reject if trading/withdrawal enabled."""
        perm_set = {p.lower() for p in permissions}
        unsafe = perm_set & _UNSAFE_PERMISSIONS

        if unsafe:
            return {
                "valid": False,
                "reason": (
                    f"API key has unsafe permissions: {', '.join(sorted(unsafe))}. "
                    "Please create a new key with read-only permissions only."
                ),
                "unsafe_permissions": sorted(unsafe),
            }

        return {"valid": True, "permissions": sorted(perm_set)}

    def connect_exchange(
        self,
        org_id: str,
        exchange: str,
        api_key: str,
        api_secret: str,
        permissions: list[str],
    ) -> dict[str, Any]:
        """Connect an exchange with read-only API key."""
        validation = self.validate_key_permissions(exchange, permissions)
        if not validation["valid"]:
            return {
                "status": "rejected",
                "reason": validation["reason"],
                "unsafe_permissions": validation.get("unsafe_permissions", []),
            }

        # Store credentials in vault
        if self._vault:
            self._vault.store(
                org_id=org_id,
                source=exchange,
                credentials={"api_key": api_key, "api_secret": api_secret},
            )

        return {
            "status": "connected",
            "exchange": exchange,
            "permissions": validation["permissions"],
        }

    def portfolio_balance(self, org_id: str, exchange: str = "binance") -> PortfolioReport:
        """Total portfolio value across all assets on the exchange."""
        if self._mock_mode:
            balances = self._mock_balances.get(org_id, [])
            assets: list[PortfolioAsset] = []
            for b in balances:
                symbol = b.get("asset", b.get("symbol", ""))
                balance = float(b.get("balance", 0))
                price = self._mock_prices.get(symbol, 0.0)
                value = balance * price
                if balance > 0:
                    assets.append(PortfolioAsset(
                        asset=symbol,
                        balance=balance,
                        price_usd=price,
                        value_usd=value,
                    ))

            assets.sort(key=lambda a: a.value_usd, reverse=True)
            total = sum(a.value_usd for a in assets)
            return PortfolioReport(
                exchange=exchange,
                total_value_usd=total,
                assets=assets,
            )

        raise NotImplementedError("Live CEX portfolio requires production credentials")
