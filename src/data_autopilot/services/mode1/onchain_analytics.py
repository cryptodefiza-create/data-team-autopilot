from __future__ import annotations

import logging
from datetime import datetime, timedelta, timezone
from typing import Any

from data_autopilot.services.mode1.models import (
    DailySnapshot,
    ExchangeFlowReport,
    OverlapReport,
    ProviderResult,
    WhaleMovement,
)
from data_autopilot.services.mode1.persistence import PersistenceManager
from data_autopilot.services.mode1.wallet_labeler import KNOWN_EXCHANGE_WALLETS, WalletLabeler

logger = logging.getLogger(__name__)


class OnChainAnalytics:
    """Complex analytics built on top of basic provider calls."""

    def __init__(
        self,
        helius: Any = None,
        persistence: PersistenceManager | None = None,
        labeler: WalletLabeler | None = None,
        mock_mode: bool = False,
    ) -> None:
        self._helius = helius
        self._persistence = persistence
        self._labeler = labeler or WalletLabeler()
        self._mock_mode = mock_mode
        self._mock_holders: dict[str, list[dict[str, Any]]] = {}
        self._mock_transfers: dict[str, list[dict[str, Any]]] = {}

    def register_mock_holders(self, mint: str, holders: list[dict[str, Any]]) -> None:
        self._mock_holders[mint] = holders

    def register_mock_transfers(self, mint: str, transfers: list[dict[str, Any]]) -> None:
        self._mock_transfers[mint] = transfers

    def holder_history(self, mint: str, days: int = 30) -> list[DailySnapshot]:
        """Track holder count + distribution over time using stored snapshots."""
        if self._persistence is None:
            return []

        # Query snapshots from persistence — group by day
        now = datetime.now(timezone.utc)
        snapshots_by_day: dict[str, list[dict[str, Any]]] = {}

        for org_id in self._persistence._storages:
            backend = self._persistence.get_storage(org_id)
            if backend is None:
                continue
            results = backend.query_snapshots(entity="token_holders")
            for snap in results:
                if snap.ingested_at >= now - timedelta(days=days):
                    day = snap.ingested_at.strftime("%Y-%m-%d")
                    if day not in snapshots_by_day:
                        snapshots_by_day[day] = []
                    snapshots_by_day[day].append(snap.payload)

        daily: list[DailySnapshot] = []
        for day in sorted(snapshots_by_day.keys()):
            records = snapshots_by_day[day]
            balances = [float(r.get("balance", 0)) for r in records]
            total = sum(balances) if balances else 1.0
            sorted_balances = sorted(balances, reverse=True)
            top10_sum = sum(sorted_balances[:10])
            daily.append(DailySnapshot(
                date=day,
                holder_count=len(records),
                total_supply=total,
                top10_pct=(top10_sum / total * 100) if total > 0 else 0.0,
            ))
        return daily

    def whale_tracker(
        self, mint: str, threshold_pct: float = 1.0
    ) -> list[WhaleMovement]:
        """Detect wallets holding >X% of supply and track their movements."""
        if self._mock_mode:
            holders = self._mock_holders.get(mint, [])
        else:
            result: ProviderResult = self._helius.fetch(
                "get_token_accounts", {"mint": mint}
            )
            holders = result.records if result.succeeded else []

        if not holders:
            return []

        total_supply = sum(float(h.get("balance", 0)) for h in holders)
        if total_supply == 0:
            return []

        threshold = threshold_pct / 100.0
        whales = [
            h for h in holders
            if float(h.get("balance", 0)) / total_supply >= threshold
        ]

        movements: list[WhaleMovement] = []
        for whale in whales:
            address = whale.get("address", whale.get("wallet", ""))
            balance = float(whale.get("balance", 0))
            pct = balance / total_supply * 100

            # Classify activity based on recent transactions
            activity = self._classify_activity(address, mint)
            label_info = self._labeler.enrich(address)

            movements.append(WhaleMovement(
                address=address,
                balance=balance,
                pct_supply=pct,
                recent_activity=activity,
                label=label_info.label,
            ))

        movements.sort(key=lambda w: w.balance, reverse=True)
        return movements

    def _classify_activity(self, address: str, mint: str) -> str:
        """Classify whale activity as accumulating, distributing, or holding."""
        if self._mock_mode:
            transfers = self._mock_transfers.get(mint, [])
            incoming = sum(
                1 for t in transfers if t.get("to") == address
            )
            outgoing = sum(
                1 for t in transfers if t.get("from") == address
            )
        else:
            result = self._helius.fetch(
                "get_signatures", {"address": address, "limit": 20}
            )
            txns = result.records if result.succeeded else []
            incoming = sum(1 for t in txns if t.get("type") == "receive")
            outgoing = sum(1 for t in txns if t.get("type") == "send")

        if incoming > outgoing + 2:
            return "accumulating"
        elif outgoing > incoming + 2:
            return "distributing"
        return "holding"

    def exchange_flow(self, mint: str, days: int = 7) -> ExchangeFlowReport:
        """Track token flow to/from known exchange wallets."""
        if self._mock_mode:
            transfers = self._mock_transfers.get(mint, [])
        else:
            result = self._helius.fetch(
                "get_signatures", {"address": mint, "limit": 500}
            )
            transfers = result.records if result.succeeded else []

        exchange_addresses = set(KNOWN_EXCHANGE_WALLETS.keys())

        inflows = [t for t in transfers if t.get("to") in exchange_addresses]
        outflows = [t for t in transfers if t.get("from") in exchange_addresses]

        inflow_volume = sum(float(t.get("amount", 0)) for t in inflows)
        outflow_volume = sum(float(t.get("amount", 0)) for t in outflows)
        net_flow = outflow_volume - inflow_volume

        return ExchangeFlowReport(
            mint=mint,
            period_days=days,
            inflow_volume=inflow_volume,
            outflow_volume=outflow_volume,
            net_flow=net_flow,
            interpretation="net_outflow" if net_flow > 0 else "net_inflow",
            inflow_count=len(inflows),
            outflow_count=len(outflows),
        )

    def wallet_overlap(self, mint_a: str, mint_b: str) -> OverlapReport:
        """Find wallets that hold both tokens."""
        if self._mock_mode:
            holders_a_list = self._mock_holders.get(mint_a, [])
            holders_b_list = self._mock_holders.get(mint_b, [])
        else:
            result_a = self._helius.fetch("get_token_accounts", {"mint": mint_a})
            result_b = self._helius.fetch("get_token_accounts", {"mint": mint_b})
            holders_a_list = result_a.records if result_a.succeeded else []
            holders_b_list = result_b.records if result_b.succeeded else []

        holders_a = {h.get("address", h.get("wallet", "")) for h in holders_a_list}
        holders_b = {h.get("address", h.get("wallet", "")) for h in holders_b_list}
        overlap = holders_a & holders_b

        return OverlapReport(
            token_a=mint_a,
            token_b=mint_b,
            token_a_holders=len(holders_a),
            token_b_holders=len(holders_b),
            overlap_count=len(overlap),
            overlap_pct_a=(len(overlap) / len(holders_a) * 100) if holders_a else 0.0,
            overlap_pct_b=(len(overlap) / len(holders_b) * 100) if holders_b else 0.0,
            overlap_wallets=sorted(list(overlap))[:1000],
        )
