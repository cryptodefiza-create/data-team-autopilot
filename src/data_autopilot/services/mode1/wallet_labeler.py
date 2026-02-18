from __future__ import annotations

import logging

from data_autopilot.services.mode1.models import WalletLabel

logger = logging.getLogger(__name__)

# Built-in known wallet labels (abbreviated addresses for readability)
KNOWN_EXCHANGE_WALLETS: dict[str, dict[str, str]] = {
    # Solana exchanges
    "5tzFkiKscXHK5ZXCGbXZxdw7gTjjD1mBwuoFbhUvuAi9": {"label": "Binance Hot Wallet", "type": "exchange"},
    "2AQdpR5LAWYFjgvELBnSDEoLiV7Kig7z2nLpsAvPmns8": {"label": "Coinbase Deposit", "type": "exchange"},
    "H8sMJSCQxfKiFTCfDR3DUKo8YdBTJSt8aQ2tqC3HvPnM": {"label": "Kraken", "type": "exchange"},
    "FWznbcNXWQuHTawe9RxvQ2LdCENssh12wSGXkPxr2N7c": {"label": "OKX", "type": "exchange"},
    # Ethereum exchanges
    "0x28C6c06298d514Db089934071355E5743bf21d60": {"label": "Binance Hot Wallet 14", "type": "exchange"},
    "0x21a31Ee1afC51d94C2eFcCAa2092aD1028285549": {"label": "Binance Hot Wallet 20", "type": "exchange"},
    "0xA090e606E30bD747d4E6245a1517EbE430F0057e": {"label": "Coinbase Commerce", "type": "exchange"},
    "0x503828976D22510aad0201ac7EC88293211D23Da": {"label": "Coinbase Hot Wallet", "type": "exchange"},
}

KNOWN_PROTOCOL_WALLETS: dict[str, dict[str, str]] = {
    # Solana protocols
    "JUP6LkbZbjS1jKKwapdHNy74zcZ3tLUZoi5QNyVTaV4": {"label": "Jupiter Aggregator", "type": "protocol"},
    "whirLbMiicVdio4qvUfM5KAg6Ct8VwpYzGff3uctyCc": {"label": "Orca Whirlpool", "type": "protocol"},
    "675kPX9MHTjS2zt1qfr1NYHuzeLXfQM9H24wFSUt1Mp8": {"label": "Raydium AMM", "type": "protocol"},
}

KNOWN_MARKET_MAKERS: dict[str, dict[str, str]] = {
    "wintermute_sol_1": {"label": "Wintermute", "type": "market_maker"},
    "jump_trading_sol_1": {"label": "Jump Trading", "type": "market_maker"},
}


_ALL_KNOWN: dict[str, dict[str, str]] = {
    **KNOWN_EXCHANGE_WALLETS,
    **KNOWN_PROTOCOL_WALLETS,
    **KNOWN_MARKET_MAKERS,
}


class WalletLabeler:
    """Maps wallet addresses to known entities."""

    def __init__(self) -> None:
        self._custom_labels: dict[str, dict[str, dict[str, str]]] = {}  # org_id -> {address -> info}

    def add_custom_label(
        self, org_id: str, address: str, label: str, label_type: str = "custom"
    ) -> WalletLabel:
        """Customer labels their own wallets: treasury, team, vesting, etc."""
        if org_id not in self._custom_labels:
            self._custom_labels[org_id] = {}
        self._custom_labels[org_id][address] = {"label": label, "type": label_type}
        logger.info("Custom label for %s: %s = %s", org_id, address, label)
        return WalletLabel(
            address=address, label=label, type=label_type, source="custom"
        )

    def enrich(self, address: str, org_id: str = "") -> WalletLabel:
        """Look up label for an address. Check custom labels first, then built-in."""
        # 1. Check customer's custom labels
        if org_id and org_id in self._custom_labels:
            custom = self._custom_labels[org_id].get(address)
            if custom:
                return WalletLabel(
                    address=address,
                    label=custom["label"],
                    type=custom["type"],
                    source="custom",
                )

        # 2. Check built-in known wallets
        known = _ALL_KNOWN.get(address)
        if known:
            return WalletLabel(
                address=address,
                label=known["label"],
                type=known["type"],
                source="built_in",
            )

        # 3. Return unknown with address type hint
        addr_type = ""
        if address.startswith("0x") and len(address) == 42:
            addr_type = "ethereum_eoa_or_contract"
        elif len(address) > 30 and not address.startswith("0x"):
            addr_type = "solana_account"

        return WalletLabel(address=address, label="Unknown", type=addr_type, source="")

    def is_exchange(self, address: str) -> bool:
        """Check if address belongs to a known exchange."""
        known = _ALL_KNOWN.get(address)
        return known is not None and known.get("type") == "exchange"

    def get_custom_labels(self, org_id: str) -> list[WalletLabel]:
        """Get all custom labels for an org."""
        if org_id not in self._custom_labels:
            return []
        return [
            WalletLabel(address=addr, label=info["label"], type=info["type"], source="custom")
            for addr, info in self._custom_labels[org_id].items()
        ]
