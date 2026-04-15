from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from weather_trading.services.market_discovery.resolved_markets import extract_yes_price, find_resolved_winner_market


@dataclass(slots=True)
class ContractualResolutionComparison:
    snapshot_as_of_date: str
    event_slug: str
    event_date: str
    station_code: str
    actual_temperature_source: str | None
    event_closed: bool | None
    event_active: bool | None
    event_archived: bool | None
    resolution_source_url: str | None
    openmeteo_winner_market_id: str
    openmeteo_winner_question: str
    contractual_winner_market_id: str
    contractual_winner_question: str
    contractual_winner_yes_price: float | None
    market_id_match: bool
    question_match: bool
    top_edge_market_id: str
    top_edge_question: str
    paper_trade_taken: bool
    openmeteo_paper_trade_pnl: float
    contractual_top_edge_hit: bool | None
    contractual_paper_trade_pnl: float
    contractual_paper_pnl_delta: float


def compare_contractual_resolution(
    evaluation: dict[str, Any],
    contractual_event_payload: dict[str, Any],
) -> ContractualResolutionComparison | None:
    markets = list(contractual_event_payload.get("markets", []))
    if not markets:
        return None

    winner_market = find_resolved_winner_market(markets)
    if winner_market is None:
        return None

    contractual_winner_market_id = str(winner_market.get("id") or winner_market.get("marketId") or "")
    contractual_winner_question = str(winner_market.get("question") or "")
    contractual_winner_yes_price = extract_yes_price(winner_market)

    openmeteo_winner_market_id = str(evaluation.get("winner_market_id") or "")
    openmeteo_winner_question = str(evaluation.get("winner_question") or "")
    top_edge_market_id = str(evaluation.get("top_edge_market_id") or "")
    top_edge_question = str(evaluation.get("top_edge_question") or "")

    market_id_match = contractual_winner_market_id == openmeteo_winner_market_id
    question_match = normalize_question(contractual_winner_question) == normalize_question(openmeteo_winner_question)

    paper_trade_taken = bool(evaluation.get("paper_trade_taken"))
    openmeteo_paper_trade_pnl = float(evaluation.get("paper_trade_pnl", 0.0))
    paper_trade_stake = float(evaluation.get("paper_trade_stake", 0.0))
    contractual_top_edge_hit = None if not paper_trade_taken else contractual_winner_market_id == top_edge_market_id
    if paper_trade_taken:
        contractual_paper_trade_pnl = (1.0 - paper_trade_stake) if contractual_top_edge_hit else -paper_trade_stake
    else:
        contractual_paper_trade_pnl = 0.0

    return ContractualResolutionComparison(
        snapshot_as_of_date=str(evaluation.get("snapshot_as_of_date") or ""),
        event_slug=str(evaluation.get("event_slug") or ""),
        event_date=str(evaluation.get("event_date") or ""),
        station_code=str(evaluation.get("station_code") or ""),
        actual_temperature_source=(
            None if evaluation.get("actual_temperature_source") is None else str(evaluation.get("actual_temperature_source"))
        ),
        event_closed=_to_bool(contractual_event_payload.get("closed")),
        event_active=_to_bool(contractual_event_payload.get("active")),
        event_archived=_to_bool(contractual_event_payload.get("archived")),
        resolution_source_url=(
            None
            if contractual_event_payload.get("resolution_source_url") in (None, "")
            else str(contractual_event_payload.get("resolution_source_url"))
        ),
        openmeteo_winner_market_id=openmeteo_winner_market_id,
        openmeteo_winner_question=openmeteo_winner_question,
        contractual_winner_market_id=contractual_winner_market_id,
        contractual_winner_question=contractual_winner_question,
        contractual_winner_yes_price=contractual_winner_yes_price,
        market_id_match=market_id_match,
        question_match=question_match,
        top_edge_market_id=top_edge_market_id,
        top_edge_question=top_edge_question,
        paper_trade_taken=paper_trade_taken,
        openmeteo_paper_trade_pnl=openmeteo_paper_trade_pnl,
        contractual_top_edge_hit=contractual_top_edge_hit,
        contractual_paper_trade_pnl=contractual_paper_trade_pnl,
        contractual_paper_pnl_delta=contractual_paper_trade_pnl - openmeteo_paper_trade_pnl,
    )


def summarize_contractual_comparisons(comparisons: list[ContractualResolutionComparison]) -> dict[str, Any]:
    if not comparisons:
        return {
            "events": 0,
            "market_id_match_rate": 0.0,
            "question_match_rate": 0.0,
            "discrepancies": 0,
            "openmeteo_paper_total_pnl": 0.0,
            "contractual_paper_total_pnl": 0.0,
            "contractual_paper_pnl_delta": 0.0,
            "paper_trade_events": 0,
            "paper_trade_question_match_rate": 0.0,
        }

    paper_trade_comparisons = [comparison for comparison in comparisons if comparison.paper_trade_taken]
    question_matches = sum(comparison.question_match for comparison in comparisons)
    market_matches = sum(comparison.market_id_match for comparison in comparisons)
    discrepancies = [
        comparison
        for comparison in comparisons
        if not (comparison.market_id_match or comparison.question_match)
    ]
    paper_trade_question_matches = sum(
        comparison.contractual_top_edge_hit is True
        for comparison in paper_trade_comparisons
    )
    openmeteo_paper_total_pnl = sum(comparison.openmeteo_paper_trade_pnl for comparison in comparisons)
    contractual_paper_total_pnl = sum(comparison.contractual_paper_trade_pnl for comparison in comparisons)

    return {
        "events": len(comparisons),
        "market_id_match_rate": market_matches / len(comparisons),
        "question_match_rate": question_matches / len(comparisons),
        "discrepancies": len(discrepancies),
        "openmeteo_paper_total_pnl": openmeteo_paper_total_pnl,
        "contractual_paper_total_pnl": contractual_paper_total_pnl,
        "contractual_paper_pnl_delta": contractual_paper_total_pnl - openmeteo_paper_total_pnl,
        "paper_trade_events": len(paper_trade_comparisons),
        "paper_trade_question_match_rate": (
            paper_trade_question_matches / len(paper_trade_comparisons)
            if paper_trade_comparisons
            else 0.0
        ),
        "by_actual_temperature_source": summarize_groups(
            comparisons,
            key_fn=lambda comparison: comparison.actual_temperature_source or "unknown",
        ),
    }


def summarize_groups(comparisons: list[ContractualResolutionComparison], *, key_fn) -> dict[str, dict[str, Any]]:
    grouped: dict[str, list[ContractualResolutionComparison]] = {}
    for comparison in comparisons:
        key = str(key_fn(comparison) or "unknown")
        grouped.setdefault(key, []).append(comparison)

    return {
        key: {
            "events": len(items),
            "question_match_rate": sum(item.question_match for item in items) / len(items),
            "market_id_match_rate": sum(item.market_id_match for item in items) / len(items),
            "contractual_paper_pnl_delta": sum(item.contractual_paper_pnl_delta for item in items),
        }
        for key, items in sorted(grouped.items())
    }


def normalize_question(value: str | None) -> str:
    if not value:
        return ""
    return " ".join(str(value).strip().lower().split())


def _to_bool(value: Any) -> bool | None:
    if value is None:
        return None
    return bool(value)
