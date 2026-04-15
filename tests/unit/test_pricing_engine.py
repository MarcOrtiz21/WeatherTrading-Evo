from datetime import date

import pytest

from weather_trading.domain.models import (
    ForecastDistribution,
    MarketQuote,
    MarketSpec,
    MetricKind,
    ResolutionSource,
    RoundingMethod,
    TimeAggregation,
)
from weather_trading.infrastructure.utils import utc_now
from weather_trading.services.pricing_engine.service import PricingEngine


def build_spec(**overrides):
    base = dict(
        market_id="pricing-market",
        question="¿Hará 32.9°C o más?",
        rules_text="Se usará el primer decimal reportado.",
        city="Madrid",
        country="Spain",
        station_code="LEMD",
        timezone="Europe/Madrid",
        local_date=date(2026, 4, 15),
        resolution_source=ResolutionSource.WUNDERGROUND,
        metric=MetricKind.MAX_TEMP_C,
        aggregation=TimeAggregation.DAILY_MAX,
        rounding_method=RoundingMethod.STRICT_DECIMAL,
        threshold_c=32.9,
        outcomes=("Yes", "No"),
        confidence_score=1.0,
        notes=(),
    )
    base.update(overrides)
    return MarketSpec(**base)


def test_decimal_threshold_pricing_uses_upper_bucket_and_blocks():
    engine = PricingEngine()
    spec = build_spec()
    forecast = ForecastDistribution(
        market_id=spec.market_id,
        generated_at_utc=utc_now(),
        model_name="unit-test",
        calibration_version="1.0",
        probabilities_by_temp_c={32: 0.4, 33: 0.6},
    )
    quote = MarketQuote(
        market_id=spec.market_id,
        outcome="Yes",
        best_bid=0.40,
        best_ask=0.50,
        captured_at_utc=utc_now(),
    )

    signal = engine.generate_signal(spec, forecast, quote)

    assert signal.fair_probability == 0.6
    assert signal.market_probability == 0.45
    assert signal.execution_price == 0.50
    assert "integer_distribution_used_for_decimal_threshold" in signal.blockers
    assert not signal.is_tradeable


def test_manual_review_blocks_signal():
    engine = PricingEngine()
    spec = build_spec(local_date=None, confidence_score=0.5)
    forecast = ForecastDistribution(
        market_id=spec.market_id,
        generated_at_utc=utc_now(),
        model_name="unit-test",
        calibration_version="1.0",
        probabilities_by_temp_c={33: 1.0},
    )
    quote = MarketQuote(market_id=spec.market_id, outcome="Yes", best_bid=0.10, best_ask=0.20)

    signal = engine.generate_signal(spec, forecast, quote)

    assert "market_spec_requires_manual_review" in signal.blockers


def test_temperature_bin_pricing_uses_exact_bucket_probability():
    engine = PricingEngine()
    spec = build_spec(
        metric=MetricKind.TEMPERATURE_BIN,
        threshold_c=None,
        bin_low_c=25.0,
        bin_high_c=25.0,
        question="¿Será la temperatura máxima de 25°C?",
    )
    forecast = ForecastDistribution(
        market_id=spec.market_id,
        generated_at_utc=utc_now(),
        model_name="unit-test",
        calibration_version="1.0",
        probabilities_by_temp_c={24: 0.2, 25: 0.5, 26: 0.3},
    )
    quote = MarketQuote(
        market_id=spec.market_id,
        outcome="Yes",
        best_bid=0.30,
        best_ask=0.40,
        captured_at_utc=utc_now(),
    )

    signal = engine.generate_signal(spec, forecast, quote)

    assert signal.fair_probability == 0.5
    assert signal.market_probability == 0.35
    assert signal.execution_price == 0.40
    assert not signal.blockers


def test_temperature_bin_pricing_uses_open_ended_bounds():
    engine = PricingEngine()
    spec = build_spec(
        metric=MetricKind.TEMPERATURE_BIN,
        threshold_c=None,
        bin_low_c=29.0,
        bin_high_c=None,
        question="¿Será la temperatura máxima de 29°C o más?",
    )
    forecast = ForecastDistribution(
        market_id=spec.market_id,
        generated_at_utc=utc_now(),
        model_name="unit-test",
        calibration_version="1.0",
        probabilities_by_temp_c={27: 0.2, 28: 0.3, 29: 0.1, 30: 0.4},
    )
    quote = MarketQuote(
        market_id=spec.market_id,
        outcome="Yes",
        best_bid=0.20,
        best_ask=0.25,
        captured_at_utc=utc_now(),
    )

    signal = engine.generate_signal(spec, forecast, quote)

    assert signal.fair_probability == 0.5
    assert signal.market_probability == 0.225
    assert signal.execution_price == 0.25


def test_temperature_bin_pricing_uses_gaussian_metadata_for_decimal_bounds():
    engine = PricingEngine()
    spec = build_spec(
        metric=MetricKind.TEMPERATURE_BIN,
        threshold_c=None,
        bin_low_c=21.1,
        bin_high_c=21.7,
        question="Will the highest temperature be between 70-71°F?",
    )
    forecast = ForecastDistribution(
        market_id=spec.market_id,
        generated_at_utc=utc_now(),
        model_name="openmeteo_gaussian_v1",
        calibration_version="1.0",
        probabilities_by_temp_c={20: 0.5, 21: 0.5},
        notes=("center=21.4", "std_dev=1.2"),
    )
    quote = MarketQuote(
        market_id=spec.market_id,
        outcome="Yes",
        best_bid=0.10,
        best_ask=0.12,
        captured_at_utc=utc_now(),
    )

    signal = engine.generate_signal(spec, forecast, quote)

    assert 0 < signal.fair_probability < 1
    assert "integer_distribution_used_for_decimal_bin" not in signal.blockers


def test_wide_spread_quote_is_blocked_by_market_quality_filter():
    engine = PricingEngine()
    spec = build_spec(
        metric=MetricKind.TEMPERATURE_BIN,
        threshold_c=None,
        bin_low_c=25.0,
        bin_high_c=25.0,
        question="¿Será la temperatura máxima de 25°C?",
    )
    forecast = ForecastDistribution(
        market_id=spec.market_id,
        generated_at_utc=utc_now(),
        model_name="unit-test",
        calibration_version="1.0",
        probabilities_by_temp_c={25: 0.7, 26: 0.3},
    )
    quote = MarketQuote(
        market_id=spec.market_id,
        outcome="Yes",
        best_bid=0.10,
        best_ask=0.28,
        captured_at_utc=utc_now(),
    )

    signal = engine.generate_signal(spec, forecast, quote)

    assert "spread_too_wide" in signal.blockers
    assert signal.spread_width == pytest.approx(0.18)
    assert signal.quality_tier == "C"
    assert signal.is_tradeable is False
