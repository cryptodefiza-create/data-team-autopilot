from data_autopilot.services.mode1.models import Chain, DataRequest, Entity, Intent, RoutingMode
from data_autopilot.services.mode1.request_router import RequestRouter


router = RequestRouter()


def test_route_token_holders_solana() -> None:
    """1.5: Route token_holders on solana → helius, get_token_accounts."""
    req = DataRequest(
        raw_message="Show holders of BONK on Solana",
        intent=Intent.SNAPSHOT,
        chain=Chain.SOLANA,
        entity=Entity.TOKEN_HOLDERS,
        token="BONK",
    )
    decision = router.route(req)
    assert decision.mode == RoutingMode.PUBLIC_API
    assert decision.provider_name == "helius"
    assert decision.method_name == "get_token_accounts"


def test_route_private_signal_penalized() -> None:
    """1.6: Private signal 'my' lowers confidence, does NOT route to public_api."""
    req = DataRequest(
        raw_message="Show my token balances on Solana",
        intent=Intent.SNAPSHOT,
        chain=Chain.SOLANA,
        entity=Entity.TOKEN_BALANCES,
        token="",
    )
    decision = router.route(req)
    assert decision.confidence < 0.7
    assert decision.mode != RoutingMode.PUBLIC_API


def test_route_ambiguous_low_confidence() -> None:
    """1.7: Ambiguous request with no registry match → ask_user, confidence < 0.7."""
    req = DataRequest(
        raw_message="Show me something interesting about tokens",
        intent=Intent.SNAPSHOT,
        chain=Chain.CROSS_CHAIN,
        entity=Entity.LOGS,  # no cross_chain+logs mapping
    )
    decision = router.route(req)
    assert decision.confidence < 0.7
    assert decision.mode == RoutingMode.ASK_USER
