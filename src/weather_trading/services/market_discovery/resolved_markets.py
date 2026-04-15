import json
from typing import Any


def extract_yes_price(market: dict[str, Any]) -> float | None:
    outcome_prices = market.get("outcomePrices")
    if isinstance(outcome_prices, str):
        try:
            outcome_prices = json.loads(outcome_prices)
        except json.JSONDecodeError:
            outcome_prices = None

    if isinstance(outcome_prices, list) and outcome_prices:
        try:
            return float(outcome_prices[0])
        except (TypeError, ValueError):
            return None

    last_trade_price = market.get("lastTradePrice")
    if last_trade_price is None:
        return None
    try:
        return float(last_trade_price)
    except (TypeError, ValueError):
        return None


def find_resolved_winner_market(markets: list[dict[str, Any]]) -> dict[str, Any] | None:
    valid_markets = [
        (extract_yes_price(market), market)
        for market in markets
    ]
    valid_markets = [(yes_price, market) for yes_price, market in valid_markets if yes_price is not None]
    if not valid_markets:
        return None

    valid_markets.sort(key=lambda item: item[0], reverse=True)
    return valid_markets[0][1]
