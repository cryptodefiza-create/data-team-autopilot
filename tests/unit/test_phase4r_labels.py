"""Phase 4R tests: Wallet labeling — known wallets, custom labels."""

from data_autopilot.services.mode1.wallet_labeler import (
    KNOWN_EXCHANGE_WALLETS,
    WalletLabeler,
)


def test_wallet_labeling_known() -> None:
    """4.9: Transaction involving Binance hot wallet → labeled correctly."""
    labeler = WalletLabeler()

    # Check a known exchange wallet
    addr = list(KNOWN_EXCHANGE_WALLETS.keys())[0]
    label = labeler.enrich(addr)

    assert label.label != "Unknown"
    assert label.type == "exchange"
    assert label.source == "built_in"
    assert labeler.is_exchange(addr)

    # Check an unknown wallet
    unknown = labeler.enrich("0x1234567890abcdef1234567890abcdef12345678")
    assert unknown.label == "Unknown"
    assert unknown.type == "ethereum_eoa_or_contract"


def test_custom_wallet_label() -> None:
    """4.10: Customer labels their treasury wallet → subsequent queries use that label."""
    labeler = WalletLabeler()
    treasury_addr = "TreasuryWallet123456789abcdefghijk"

    # Before labeling — unknown
    before = labeler.enrich(treasury_addr, org_id="org_dao")
    assert before.label == "Unknown"

    # Add custom label
    labeler.add_custom_label("org_dao", treasury_addr, "DAO Treasury", "treasury")

    # After labeling — labeled
    after = labeler.enrich(treasury_addr, org_id="org_dao")
    assert after.label == "DAO Treasury"
    assert after.type == "treasury"
    assert after.source == "custom"

    # Other orgs don't see the label
    other_org = labeler.enrich(treasury_addr, org_id="org_other")
    assert other_org.label == "Unknown"

    # Custom labels list
    customs = labeler.get_custom_labels("org_dao")
    assert len(customs) == 1
    assert customs[0].address == treasury_addr
