import pytest

from weather_trading.domain.models import MarketQuote
from weather_trading.services.execution_engine.market_quality import MarketQualityFilter


def test_market_quality_assessment_for_narrow_two_sided_quote():
    quality_filter = MarketQualityFilter(
        max_spread_width=0.10,
        min_quality_score=0.55,
        slippage_spread_multiplier=0.25,
        min_slippage=0.0025,
        max_slippage=0.02,
    )
    quote = MarketQuote(
        market_id="m1",
        outcome="Yes",
        best_bid=0.41,
        best_ask=0.45,
        last_price=0.44,
    )

    assessment = quality_filter.assess_yes_quote(quote)

    assert assessment.reference_price == pytest.approx(0.43)
    assert assessment.execution_price == pytest.approx(0.45)
    assert assessment.spread_width == pytest.approx(0.04)
    assert assessment.relative_spread_width == pytest.approx(0.04 / 0.45)
    assert assessment.estimated_slippage == pytest.approx(0.01)
    assert assessment.quality_tier == "A"
    assert assessment.blockers == ()


def test_market_quality_assessment_blocks_missing_two_sided_quote():
    quality_filter = MarketQualityFilter(
        max_spread_width=0.10,
        min_quality_score=0.55,
        slippage_spread_multiplier=0.25,
        min_slippage=0.0025,
        max_slippage=0.02,
    )
    quote = MarketQuote(
        market_id="m2",
        outcome="Yes",
        best_bid=None,
        best_ask=0.63,
        last_price=0.61,
    )

    assessment = quality_filter.assess_yes_quote(quote)

    assert assessment.reference_price == pytest.approx(0.61)
    assert assessment.execution_price == pytest.approx(0.63)
    assert assessment.spread_width is None
    assert assessment.relative_spread_width is None
    assert assessment.estimated_slippage == 0.0025
    assert "missing_two_sided_quote" in assessment.blockers


def test_market_quality_penalizes_large_relative_spread_on_cheap_bin():
    quality_filter = MarketQualityFilter(
        max_spread_width=0.10,
        relative_spread_score_threshold=0.35,
        relative_spread_score_weight=0.15,
        min_quality_score=0.55,
        slippage_spread_multiplier=0.25,
        min_slippage=0.0025,
        max_slippage=0.02,
    )
    quote = MarketQuote(
        market_id="m3",
        outcome="Yes",
        best_bid=0.013,
        best_ask=0.021,
        last_price=0.017,
    )

    assessment = quality_filter.assess_yes_quote(quote)

    assert assessment.spread_width == pytest.approx(0.008)
    assert assessment.relative_spread_width == pytest.approx(0.008 / 0.021)
    assert assessment.quality_tier == "B"
    assert assessment.blockers == ()
