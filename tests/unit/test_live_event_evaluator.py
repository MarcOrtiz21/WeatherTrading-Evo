from datetime import date, datetime
from types import SimpleNamespace

import pytest

from weather_trading.domain.models import (
    ForecastDistribution,
    MarketSpec,
    ResolutionSource,
    RoundingMethod,
    TimeAggregation,
    MetricKind,
    TradingSignal,
)
from weather_trading.services.evaluation import live_event_evaluator


class FakeParser:
    def __init__(self, spec):
        self.spec = spec

    def parse(self, market_data):
        return self.spec


class FakeMapper:
    def __init__(self, *, station, region="Europe", allowed=True):
        self.station = station
        self.region = region
        self.allowed = allowed

    def enrich(self, spec):
        return spec

    def get_station(self, station_code):
        return self.station

    def get_station_region(self, station_code):
        return self.region

    def is_region_allowed(self, region):
        return self.allowed


class FakePricing:
    def __init__(self, signal):
        self.signal = signal

    def generate_signal(self, spec, forecast_distribution, quote):
        return self.signal


class FakeEvidenceGate:
    def assess(self, **kwargs):
        return SimpleNamespace(
            is_operable=True,
            score=0.95,
            tier="A",
            blockers=(),
            notes=("ok",),
        )


class FakeWatchlist:
    def summarize_event_alignment(self, **kwargs):
        return {
            "signal": "aligned",
            "alignment_score": 1.0,
            "match_count": 1,
            "active_traders": ["ColdMath"],
            "aligned_traders": ["ColdMath"],
            "opposed_traders": [],
            "event_only_traders": [],
            "trades": [{"label": "ColdMath"}],
        }


class FakeOpposedWatchlist:
    def summarize_event_alignment(self, **kwargs):
        return {
            "signal": "opposed",
            "alignment_score": -1.0,
            "match_count": 1,
            "active_traders": ["ColdMath"],
            "aligned_traders": [],
            "opposed_traders": ["ColdMath"],
            "event_only_traders": [],
            "trades": [{"label": "ColdMath"}],
        }


class FakeActiveUnclassifiedWatchlist:
    def summarize_event_alignment(self, **kwargs):
        return {
            "signal": "active_unclassified",
            "alignment_score": 0.0,
            "match_count": 1,
            "active_traders": ["ColdMath"],
            "aligned_traders": [],
            "opposed_traders": [],
            "event_only_traders": ["ColdMath"],
            "trades": [{"label": "ColdMath"}],
        }


def make_spec() -> MarketSpec:
    return MarketSpec(
        market_id="m1",
        question="Will the highest temperature in Madrid be 15°C on April 12?",
        rules_text="Use Wunderground EHAM equivalent.",
        city="Madrid",
        country="Spain",
        station_code="LEMD",
        timezone="Europe/Madrid",
        local_date=date(2026, 4, 12),
        resolution_source=ResolutionSource.WUNDERGROUND,
        metric=MetricKind.TEMPERATURE_BIN,
        aggregation=TimeAggregation.DAILY_MAX,
        rounding_method=RoundingMethod.NONE,
        bin_low_c=15.0,
        bin_high_c=15.9,
        confidence_score=0.99,
        notes=(),
    )


@pytest.mark.asyncio
async def test_evaluate_event_returns_region_not_allowed(monkeypatch):
    payload = {
        "event_slug": "highest-temperature-in-madrid-on-april-12-2026",
        "event_description": "Rule text",
        "event_date": "2026-04-12",
        "markets": [{"id": "m1", "question": "Q1"}],
    }

    rows, summary = await live_event_evaluator.evaluate_event(
        payload=payload,
        parser=FakeParser(make_spec()),
        mapper=FakeMapper(station={"latitude": 40.0, "longitude": -3.0, "timezone": "Europe/Madrid"}, allowed=False),
        openmeteo=SimpleNamespace(),
        baseline_builder=SimpleNamespace(),
        optimized_builder=SimpleNamespace(),
        pricing=FakePricing(signal=None),
        evidence_gate=FakeEvidenceGate(),
        wallet_watchlist=FakeWatchlist(),
        as_of_date=date(2026, 4, 11),
    )

    assert rows is None
    assert summary["reason"] == "region_not_allowed"


@pytest.mark.asyncio
async def test_evaluate_event_returns_summary_with_watchlist(monkeypatch):
    distribution = ForecastDistribution(
        market_id="e1",
        generated_at_utc=datetime(2026, 4, 11, 12, 0, 0),
        model_name="baseline",
        calibration_version="v1",
        probabilities_by_temp_c={15: 0.6, 16: 0.4},
        notes=("center=15.4", "std_dev=0.8"),
    )
    signal = TradingSignal(
        market_id="m1",
        outcome="Yes",
        fair_probability=0.6,
        market_probability=0.3,
        edge_gross=0.35,
        estimated_costs=0.02,
        safety_margin=0.05,
        execution_price=0.31,
        spread_width=0.02,
        relative_spread_width=0.06,
        quality_score=0.92,
        quality_tier="A",
    )

    async def fake_build_live_distribution(**kwargs):
        return distribution, {
            "center_c": 15.4,
            "std_dev_c": 0.8,
            "strategy": "baseline_short_horizon",
            "ensemble_members": 10,
            "calibration_days": 7,
            "intraday_active": False,
            "intraday_source": None,
            "intraday_max_so_far_c": None,
            "intraday_remaining_hours": None,
        }

    monkeypatch.setattr(live_event_evaluator, "build_live_distribution", fake_build_live_distribution)

    payload = {
        "event_slug": "highest-temperature-in-madrid-on-april-12-2026",
        "event_title": "Highest temperature in Madrid on April 12?",
        "event_description": "Rule text",
        "event_date": "2026-04-12",
        "markets": [
            {
                "id": "m1",
                "slug": "highest-temperature-in-madrid-on-april-12-2026-15c",
                "question": "Will the highest temperature in Madrid be 15°C on April 12?",
                "bestBid": 0.30,
                "bestAsk": 0.32,
                "lastTradePrice": 0.31,
            }
        ],
    }

    rows, summary = await live_event_evaluator.evaluate_event(
        payload=payload,
        parser=FakeParser(make_spec()),
        mapper=FakeMapper(
            station={"latitude": 40.0, "longitude": -3.0, "timezone": "Europe/Madrid"},
            allowed=True,
        ),
        openmeteo=SimpleNamespace(),
        baseline_builder=SimpleNamespace(),
        optimized_builder=SimpleNamespace(),
        pricing=FakePricing(signal=signal),
        evidence_gate=FakeEvidenceGate(),
        wallet_watchlist=FakeWatchlist(),
        as_of_date=date(2026, 4, 11),
    )

    assert rows is not None
    assert len(rows) == 1
    assert summary["top_edge_tradeable"] is True
    assert summary["watchlist_signal"] == "aligned"
    assert summary["watchlist_aligned_traders"] == ["ColdMath"]


@pytest.mark.asyncio
async def test_evaluate_event_applies_watchlist_opposed_veto(monkeypatch):
    distribution = ForecastDistribution(
        market_id="e1",
        generated_at_utc=datetime(2026, 4, 11, 12, 0, 0),
        model_name="baseline",
        calibration_version="v1",
        probabilities_by_temp_c={15: 0.6, 16: 0.4},
        notes=("center=15.4", "std_dev=0.8"),
    )
    signal = TradingSignal(
        market_id="m1",
        outcome="Yes",
        fair_probability=0.6,
        market_probability=0.3,
        edge_gross=0.35,
        estimated_costs=0.02,
        safety_margin=0.05,
        execution_price=0.31,
        spread_width=0.02,
        relative_spread_width=0.06,
        quality_score=0.92,
        quality_tier="A",
    )

    async def fake_build_live_distribution(**kwargs):
        return distribution, {
            "center_c": 15.4,
            "std_dev_c": 0.8,
            "strategy": "baseline_short_horizon",
            "ensemble_members": 10,
            "calibration_days": 7,
            "intraday_active": False,
            "intraday_source": None,
            "intraday_max_so_far_c": None,
            "intraday_remaining_hours": None,
        }

    def fake_get(key, default=None):
        if key == "watchlist_risk.opposed_signal_veto_enabled":
            return True
        if key == "watchlist_risk.min_opposed_trader_count_for_veto":
            return 1
        return default

    monkeypatch.setattr(live_event_evaluator, "build_live_distribution", fake_build_live_distribution)
    monkeypatch.setattr(live_event_evaluator.ConfigLoader, "get", staticmethod(fake_get))

    payload = {
        "event_slug": "highest-temperature-in-madrid-on-april-12-2026",
        "event_title": "Highest temperature in Madrid on April 12?",
        "event_description": "Rule text",
        "event_date": "2026-04-12",
        "markets": [
            {
                "id": "m1",
                "slug": "highest-temperature-in-madrid-on-april-12-2026-15c",
                "question": "Will the highest temperature in Madrid be 15°C on April 12?",
                "bestBid": 0.30,
                "bestAsk": 0.32,
                "lastTradePrice": 0.31,
            }
        ],
    }

    rows, summary = await live_event_evaluator.evaluate_event(
        payload=payload,
        parser=FakeParser(make_spec()),
        mapper=FakeMapper(
            station={"latitude": 40.0, "longitude": -3.0, "timezone": "Europe/Madrid"},
            allowed=True,
        ),
        openmeteo=SimpleNamespace(),
        baseline_builder=SimpleNamespace(),
        optimized_builder=SimpleNamespace(),
        pricing=FakePricing(signal=signal),
        evidence_gate=FakeEvidenceGate(),
        wallet_watchlist=FakeOpposedWatchlist(),
        as_of_date=date(2026, 4, 11),
    )

    assert rows is not None
    assert rows[0].is_tradeable is False
    assert live_event_evaluator.WATCHLIST_OPPOSED_VETO_BLOCKER in rows[0].blockers
    assert summary["event_operable"] is False
    assert summary["top_edge_tradeable"] is False
    assert summary["watchlist_veto_applied"] is True
    assert live_event_evaluator.WATCHLIST_OPPOSED_VETO_BLOCKER in summary["event_blockers"]


@pytest.mark.asyncio
async def test_evaluate_event_applies_experimental_celsius_active_unclassified_filter(monkeypatch):
    distribution = ForecastDistribution(
        market_id="e1",
        generated_at_utc=datetime(2026, 4, 11, 12, 0, 0),
        model_name="baseline",
        calibration_version="v1",
        probabilities_by_temp_c={15: 0.6, 16: 0.4},
        notes=("center=15.4", "std_dev=0.8"),
    )
    signal = TradingSignal(
        market_id="m1",
        outcome="Yes",
        fair_probability=0.6,
        market_probability=0.3,
        edge_gross=0.35,
        estimated_costs=0.02,
        safety_margin=0.05,
        execution_price=0.31,
        spread_width=0.02,
        relative_spread_width=0.06,
        quality_score=0.92,
        quality_tier="A",
    )

    async def fake_build_live_distribution(**kwargs):
        return distribution, {
            "center_c": 15.4,
            "std_dev_c": 0.8,
            "strategy": "baseline_short_horizon",
            "ensemble_members": 10,
            "calibration_days": 7,
            "intraday_active": False,
            "intraday_source": None,
            "intraday_max_so_far_c": None,
            "intraday_remaining_hours": None,
        }

    def fake_get(key, default=None):
        if key == "watchlist_risk.opposed_signal_veto_enabled":
            return False
        if key == "experimental_filters.celsius_range_bin_active_unclassified_veto_enabled":
            return True
        return default

    monkeypatch.setattr(live_event_evaluator, "build_live_distribution", fake_build_live_distribution)
    monkeypatch.setattr(live_event_evaluator.ConfigLoader, "get", staticmethod(fake_get))

    payload = {
        "event_slug": "highest-temperature-in-madrid-on-april-12-2026",
        "event_title": "Highest temperature in Madrid on April 12?",
        "event_description": "Rule text",
        "event_date": "2026-04-12",
        "markets": [
            {
                "id": "m1",
                "slug": "highest-temperature-in-madrid-on-april-12-2026-15-16c",
                "question": "Will the highest temperature in Madrid be between 15-16°C on April 12?",
                "bestBid": 0.30,
                "bestAsk": 0.32,
                "lastTradePrice": 0.31,
            }
        ],
    }
    spec = make_spec()
    spec.question = "Will the highest temperature in Madrid be between 15-16°C on April 12?"
    spec.bin_low_c = 15.0
    spec.bin_high_c = 16.0

    rows, summary = await live_event_evaluator.evaluate_event(
        payload=payload,
        parser=FakeParser(spec),
        mapper=FakeMapper(
            station={"latitude": 40.0, "longitude": -3.0, "timezone": "Europe/Madrid"},
            allowed=True,
        ),
        openmeteo=SimpleNamespace(),
        baseline_builder=SimpleNamespace(),
        optimized_builder=SimpleNamespace(),
        pricing=FakePricing(signal=signal),
        evidence_gate=FakeEvidenceGate(),
        wallet_watchlist=FakeActiveUnclassifiedWatchlist(),
        as_of_date=date(2026, 4, 11),
    )

    assert rows is not None
    assert rows[0].is_tradeable is False
    assert (
        live_event_evaluator.EXPERIMENTAL_CELSIUS_RANGE_ACTIVE_UNCLASSIFIED_BLOCKER
        in rows[0].blockers
    )
    assert summary["event_operable"] is False
    assert summary["top_edge_tradeable"] is False
    assert summary["top_edge_market_family"] == "celsius|range_bin"
    assert summary["experimental_filter_applied"] is True
