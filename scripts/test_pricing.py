import sys
import os
from datetime import date

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
SRC = os.path.join(ROOT, "src")
if SRC not in sys.path:
    sys.path.insert(0, SRC)

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


def test_decimal_threshold_pricing():
    engine = PricingEngine()
    spec = MarketSpec(
        market_id="pricing-decimal-threshold",
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
    )

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

    print(f"Fair probability: {signal.fair_probability:.2f}")
    print(f"Blockers: {signal.blockers}")

    assert signal.fair_probability == 0.6
    assert "integer_distribution_used_for_decimal_threshold" in signal.blockers

    print("\n¡Pruebas del Pricing Engine completadas con éxito!")


if __name__ == "__main__":
    test_decimal_threshold_pricing()
