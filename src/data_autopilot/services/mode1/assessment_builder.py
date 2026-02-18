from __future__ import annotations

import logging

from data_autopilot.services.mode1.models import AssessmentPanel, AssessmentReport
from data_autopilot.services.mode1.onchain_analytics import OnChainAnalytics
from data_autopilot.services.mode1.defi_analytics import DeFiAnalytics
from data_autopilot.services.mode1.cex_public import CEXPublicData
from data_autopilot.services.mode1.wallet_labeler import WalletLabeler

logger = logging.getLogger(__name__)


class AssessmentBuilder:
    """Builds assessment-ready deliverables for DAOs and DeFi protocols."""

    def __init__(
        self,
        analytics: OnChainAnalytics | None = None,
        defi: DeFiAnalytics | None = None,
        cex: CEXPublicData | None = None,
        labeler: WalletLabeler | None = None,
    ) -> None:
        self._analytics = analytics
        self._defi = defi
        self._cex = cex
        self._labeler = labeler or WalletLabeler()

    def build_dao_assessment(
        self,
        org_id: str,
        mint: str,
        token_symbol: str = "",
        pool_address: str = "",
        compare_mint: str = "",
    ) -> AssessmentReport:
        """Full DAO/Token project assessment."""
        panels: list[AssessmentPanel] = []

        # Panel 1: Token holder distribution
        if self._analytics:
            whales = self._analytics.whale_tracker(mint, threshold_pct=1.0)
            panels.append(AssessmentPanel(
                title="Top Holders & Whale Tracker",
                panel_type="table",
                data={
                    "whales": [
                        {
                            "address": w.address,
                            "balance": w.balance,
                            "pct_supply": round(w.pct_supply, 2),
                            "activity": w.recent_activity,
                            "label": w.label,
                        }
                        for w in whales
                    ],
                    "total_whales": len(whales),
                },
                source="helius + wallet_labels",
            ))

        # Panel 2: Holder trend over time
        if self._analytics:
            history = self._analytics.holder_history(mint, days=30)
            panels.append(AssessmentPanel(
                title="Holder Trend (30 Days)",
                panel_type="chart",
                data={
                    "daily_snapshots": [
                        {"date": s.date, "holder_count": s.holder_count, "top10_pct": round(s.top10_pct, 2)}
                        for s in history
                    ],
                },
                source="stored_snapshots",
            ))

        # Panel 3: Exchange flow analysis
        if self._analytics:
            flow = self._analytics.exchange_flow(mint, days=7)
            panels.append(AssessmentPanel(
                title="Exchange Flow (7 Days)",
                panel_type="metric",
                data={
                    "net_flow": flow.net_flow,
                    "inflow_volume": flow.inflow_volume,
                    "outflow_volume": flow.outflow_volume,
                    "interpretation": flow.interpretation,
                },
                source="helius + wallet_labels",
            ))

        # Panel 4: Community overlap (if compare token provided)
        if self._analytics and compare_mint:
            overlap = self._analytics.wallet_overlap(mint, compare_mint)
            panels.append(AssessmentPanel(
                title="Community Overlap Analysis",
                panel_type="table",
                data={
                    "overlap_count": overlap.overlap_count,
                    "overlap_pct_a": round(overlap.overlap_pct_a, 2),
                    "overlap_pct_b": round(overlap.overlap_pct_b, 2),
                },
                source="helius",
            ))

        # Panel 5: DEX liquidity
        if self._defi and pool_address:
            pool = self._defi.pool_analytics(pool_address)
            panels.append(AssessmentPanel(
                title="DEX Liquidity & Volume",
                panel_type="metric",
                data={
                    "tvl": pool.tvl,
                    "volume_24h": pool.volume_24h,
                    "fees_24h": pool.fees_24h,
                    "fee_apr": round(pool.fee_apr, 2),
                    "pair": f"{pool.token_0}/{pool.token_1}",
                },
                source="dexscreener",
            ))

        # Panel 6: CEX trading data
        if self._cex and token_symbol:
            symbol = f"{token_symbol}USDT"
            volume = self._cex.trading_volume(symbol)
            panels.append(AssessmentPanel(
                title="CEX Trading Volume",
                panel_type="metric",
                data={
                    "volume_24h": volume.volume_24h,
                    "price_change_pct": volume.price_change_pct,
                    "high_24h": volume.high_24h,
                    "low_24h": volume.low_24h,
                },
                source="binance",
            ))

        # Generate memo narrative
        memo = self._generate_dao_memo(panels, token_symbol or mint)

        return AssessmentReport(
            org_id=org_id,
            assessment_type="dao",
            panels=panels,
            memo=memo,
        )

    def build_defi_assessment(
        self,
        org_id: str,
        protocol_name: str,
        pool_addresses: list[str] | None = None,
        governance_space: str = "",
    ) -> AssessmentReport:
        """Full DeFi protocol assessment."""
        panels: list[AssessmentPanel] = []

        # Panel 1: TVL dashboard
        if self._defi:
            tvl_data = self._defi.protocol_tvl_breakdown(protocol_name)
            panels.append(AssessmentPanel(
                title="Protocol TVL Breakdown",
                panel_type="metric",
                data=tvl_data,
                source="defillama",
            ))

        # Panel 2: Fee/revenue tracking
        if self._defi:
            revenue = self._defi.protocol_revenue(protocol_name)
            panels.append(AssessmentPanel(
                title="Fee & Revenue Analysis",
                panel_type="chart",
                data={
                    "total_fees": revenue.total_fees,
                    "protocol_revenue": revenue.protocol_revenue,
                    "trend": revenue.trend,
                    "daily_breakdown": revenue.daily_breakdown[:7],  # Last 7 days
                },
                source="defillama_fees",
            ))

        # Panel 3: Top pool analytics
        if self._defi and pool_addresses:
            pool_data = []
            for addr in pool_addresses[:5]:
                pool = self._defi.pool_analytics(addr)
                pool_data.append({
                    "address": addr,
                    "tvl": pool.tvl,
                    "volume_24h": pool.volume_24h,
                    "fee_apr": round(pool.fee_apr, 2),
                    "pair": f"{pool.token_0}/{pool.token_1}",
                })
            panels.append(AssessmentPanel(
                title="Top Pool Analytics",
                panel_type="table",
                data={"pools": pool_data},
                source="dexscreener",
            ))

        # Panel 4: Governance participation
        if self._defi:
            gov = self._defi.governance_activity(protocol_name, space=governance_space)
            panels.append(AssessmentPanel(
                title="Governance Activity",
                panel_type="table",
                data=gov,
                source="snapshot.org",
            ))

        memo = self._generate_defi_memo(panels, protocol_name)

        return AssessmentReport(
            org_id=org_id,
            assessment_type="defi",
            panels=panels,
            memo=memo,
        )

    def _generate_dao_memo(
        self, panels: list[AssessmentPanel], token: str
    ) -> str:
        """Generate weekly intelligence memo narrative for a DAO."""
        lines = [f"# Weekly Intelligence Memo — {token}", ""]

        for panel in panels:
            lines.append(f"## {panel.title}")
            data = panel.data
            if panel.title.startswith("Top Holders"):
                count = data.get("total_whales", 0)
                lines.append(f"- {count} whale(s) detected holding >1% of supply")
                for w in data.get("whales", [])[:3]:
                    label = w.get("label", "Unknown")
                    lines.append(
                        f"  - {label}: {w['pct_supply']}% — {w['activity']}"
                    )
            elif panel.title.startswith("Exchange Flow"):
                interp = data.get("interpretation", "")
                net = data.get("net_flow", 0)
                lines.append(f"- Net flow: {net:,.0f} tokens ({interp})")
                if interp == "net_outflow":
                    lines.append("  - Signal: Accumulation — tokens leaving exchanges")
                else:
                    lines.append("  - Signal: Potential selling pressure — tokens entering exchanges")
            elif panel.title.startswith("CEX Trading"):
                vol = data.get("volume_24h", 0)
                pct = data.get("price_change_pct", 0)
                lines.append(f"- 24h volume: ${vol:,.0f}")
                lines.append(f"- Price change: {pct:+.2f}%")
            lines.append("")

        return "\n".join(lines)

    def _generate_defi_memo(
        self, panels: list[AssessmentPanel], protocol: str
    ) -> str:
        """Generate weekly intelligence memo narrative for a DeFi protocol."""
        lines = [f"# Weekly Intelligence Memo — {protocol}", ""]

        for panel in panels:
            lines.append(f"## {panel.title}")
            data = panel.data
            if panel.title.startswith("Protocol TVL"):
                tvl = data.get("tvl", 0)
                lines.append(f"- Total TVL: ${tvl:,.0f}")
                chains = data.get("chains", {})
                for chain, val in list(chains.items())[:3]:
                    lines.append(f"  - {chain}: ${val:,.0f}")
            elif panel.title.startswith("Fee"):
                fees = data.get("total_fees", 0)
                rev = data.get("protocol_revenue", 0)
                trend = data.get("trend", "stable")
                lines.append(f"- Total fees: ${fees:,.0f}")
                lines.append(f"- Protocol revenue: ${rev:,.0f}")
                lines.append(f"- Trend: {trend}")
            elif panel.title.startswith("Governance"):
                active = data.get("active_proposals", 0)
                total = data.get("total_proposals", 0)
                lines.append(f"- Active proposals: {active}")
                lines.append(f"- Total recent: {total}")
            lines.append("")

        return "\n".join(lines)
