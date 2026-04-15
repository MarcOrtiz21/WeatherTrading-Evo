from weather_trading.services.market_discovery.resolved_markets import extract_yes_price, find_resolved_winner_market


def test_extract_yes_price_reads_outcome_prices_first():
    market = {"outcomePrices": "[\"1\", \"0\"]", "lastTradePrice": "0.5"}

    assert extract_yes_price(market) == 1.0


def test_find_resolved_winner_market_selects_highest_yes_price():
    markets = [
        {"id": "m1", "outcomePrices": "[\"0\", \"1\"]"},
        {"id": "m2", "outcomePrices": "[\"1\", \"0\"]"},
        {"id": "m3", "outcomePrices": "[\"0\", \"1\"]"},
    ]

    winner = find_resolved_winner_market(markets)

    assert winner is not None
    assert winner["id"] == "m2"
