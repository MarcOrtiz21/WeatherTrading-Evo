from __future__ import annotations

import math
from dataclasses import dataclass, field
from datetime import date, datetime
from enum import StrEnum


class ResolutionSource(StrEnum):
    UNKNOWN = "unknown"
    POLYMARKET = "polymarket"
    AVIATION_WEATHER = "aviation_weather"
    METAR = "metar"
    NOAA = "noaa"
    WUNDERGROUND = "wunderground"
    OPEN_METEO = "open_meteo"
    AEMET = "aemet"
    METEOSTAT = "meteostat"
    NWS = "nws"
    HONG_KONG_OBSERVATORY = "hong_kong_observatory"
    ECMWF = "ecmwf"
    MANUAL_REVIEW = "manual_review"


class MetricKind(StrEnum):
    MAX_TEMP_C = "max_temp_c"
    MIN_TEMP_C = "min_temp_c"
    TEMPERATURE_BIN = "temperature_bin"


class TimeAggregation(StrEnum):
    DAILY_MAX = "daily_max"
    DAILY_MIN = "daily_min"
    THRESHOLD = "threshold"


class RoundingMethod(StrEnum):
    NONE = "none"
    NEAREST_HALF_UP = "nearest_half_up"
    FLOOR = "floor"
    CEIL = "ceil"
    STRICT_DECIMAL = "strict_decimal"


class DecisionAction(StrEnum):
    NO_TRADE = "no_trade"
    PAPER_BUY = "paper_buy"
    PLACE_LIMIT = "place_limit"
    CANCEL = "cancel"
    REVIEW = "review"


@dataclass(slots=True)
class MarketSpec:
    market_id: str
    question: str
    rules_text: str
    city: str
    country: str
    station_code: str
    timezone: str
    local_date: date | None
    resolution_source: ResolutionSource
    metric: MetricKind
    aggregation: TimeAggregation
    rounding_method: RoundingMethod = RoundingMethod.NONE
    threshold_c: float | None = None
    bin_low_c: float | None = None
    bin_high_c: float | None = None
    outcomes: tuple[str, ...] = ()
    confidence_score: float = 0.0
    notes: tuple[str, ...] = ()

    @property
    def requires_manual_review(self) -> bool:
        has_pricing_target = self.threshold_c is not None or self.bin_low_c is not None or self.bin_high_c is not None
        return (
            self.local_date is None
            or self.confidence_score < 0.8
            or self.resolution_source == ResolutionSource.UNKNOWN
            or self.station_code == "UNKNOWN"
            or not has_pricing_target
        )


@dataclass(slots=True)
class WeatherObservation:
    station_code: str
    provider: ResolutionSource
    observed_at_utc: datetime
    temp_c: float
    dewpoint_c: float | None = None
    wind_speed_kph: float | None = None
    cloud_cover_pct: float | None = None
    pressure_hpa: float | None = None
    is_resolution_source: bool = False
    raw_reference: str | None = None


@dataclass(slots=True)
class ForecastDistribution:
    market_id: str
    generated_at_utc: datetime
    model_name: str
    calibration_version: str
    probabilities_by_temp_c: dict[int, float]
    notes: tuple[str, ...] = ()

    def total_probability(self) -> float:
        return sum(self.probabilities_by_temp_c.values())

    def probability_at_or_above(self, threshold_c: float) -> float:
        threshold_bucket = math.ceil(threshold_c)
        return sum(
            probability
            for temperature_c, probability in self.probabilities_by_temp_c.items()
            if temperature_c >= threshold_bucket
        )

    def probability_between(
        self,
        low_c: float | None = None,
        high_c: float | None = None,
    ) -> float:
        low_bucket = None if low_c is None else math.ceil(low_c)
        high_bucket = None if high_c is None else math.floor(high_c)

        return sum(
            probability
            for temperature_c, probability in self.probabilities_by_temp_c.items()
            if (low_bucket is None or temperature_c >= low_bucket)
            and (high_bucket is None or temperature_c <= high_bucket)
        )

    def most_likely_temperature(self) -> int | None:
        if not self.probabilities_by_temp_c:
            return None
        return max(self.probabilities_by_temp_c, key=self.probabilities_by_temp_c.get)


@dataclass(slots=True)
class MarketQuote:
    market_id: str
    outcome: str
    best_bid: float | None = None
    best_ask: float | None = None
    last_price: float | None = None
    captured_at_utc: datetime | None = None

    @property
    def midpoint(self) -> float | None:
        if self.best_bid is None or self.best_ask is None:
            return None
        return (self.best_bid + self.best_ask) / 2


@dataclass(slots=True)
class TradingSignal:
    market_id: str
    outcome: str
    fair_probability: float
    market_probability: float
    edge_gross: float
    estimated_costs: float
    safety_margin: float
    blockers: tuple[str, ...] = ()
    execution_price: float | None = None
    spread_width: float | None = None
    relative_spread_width: float | None = None
    quality_score: float | None = None
    quality_tier: str | None = None

    @property
    def edge_net(self) -> float:
        return self.edge_gross - self.estimated_costs - self.safety_margin

    @property
    def is_tradeable(self) -> bool:
        return not self.blockers and self.edge_net > 0


@dataclass(slots=True)
class TradeDecision:
    market_id: str
    action: DecisionAction
    signal: TradingSignal
    decided_at_utc: datetime | None
    rationale: tuple[str, ...] = field(default_factory=tuple)
