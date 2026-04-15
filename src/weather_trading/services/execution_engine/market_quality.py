from __future__ import annotations

from dataclasses import dataclass

from weather_trading.domain.models import MarketQuote
from weather_trading.infrastructure.config import ConfigLoader


@dataclass(slots=True)
class MarketQualityAssessment:
    reference_price: float | None
    execution_price: float | None
    spread_width: float | None
    relative_spread_width: float | None
    estimated_slippage: float
    quality_score: float
    quality_tier: str
    blockers: tuple[str, ...] = ()


class MarketQualityFilter:
    """Evalua si una cotizacion es operable para compra paper/live de YES."""

    EPSILON = 1e-9

    def __init__(
        self,
        *,
        max_spread_width: float | None = None,
        relative_spread_low_price_threshold: float | None = None,
        relative_spread_score_threshold: float | None = None,
        relative_spread_score_weight: float | None = None,
        min_quality_score: float | None = None,
        slippage_spread_multiplier: float | None = None,
        min_slippage: float | None = None,
        max_slippage: float | None = None,
    ) -> None:
        self.max_spread_width = float(
            max_spread_width
            if max_spread_width is not None
            else ConfigLoader.get("risk_limits.max_spread_width", 0.10)
        )
        self.relative_spread_low_price_threshold = float(
            relative_spread_low_price_threshold
            if relative_spread_low_price_threshold is not None
            else ConfigLoader.get("risk_limits.relative_spread_low_price_threshold", 0.10)
        )
        self.relative_spread_score_threshold = float(
            relative_spread_score_threshold
            if relative_spread_score_threshold is not None
            else ConfigLoader.get("risk_limits.relative_spread_score_threshold", 0.35)
        )
        self.relative_spread_score_weight = float(
            relative_spread_score_weight
            if relative_spread_score_weight is not None
            else ConfigLoader.get("risk_limits.relative_spread_score_weight", 0.15)
        )
        self.min_quality_score = float(
            min_quality_score
            if min_quality_score is not None
            else ConfigLoader.get("risk_limits.min_quality_score", 0.55)
        )
        self.slippage_spread_multiplier = float(
            slippage_spread_multiplier
            if slippage_spread_multiplier is not None
            else ConfigLoader.get("risk_limits.slippage_spread_multiplier", 0.25)
        )
        self.min_slippage = float(
            min_slippage
            if min_slippage is not None
            else ConfigLoader.get("risk_limits.min_slippage", 0.0025)
        )
        self.max_slippage = float(
            max_slippage
            if max_slippage is not None
            else ConfigLoader.get("risk_limits.max_slippage_allowed", 0.02)
        )

    def assess_yes_quote(self, quote: MarketQuote) -> MarketQualityAssessment:
        blockers: list[str] = []
        spread_width = None
        relative_spread_width = None
        reference_price = quote.midpoint

        if reference_price is None:
            reference_price = quote.last_price
        if reference_price is None:
            reference_price = quote.best_ask if quote.best_ask is not None else quote.best_bid

        execution_price = quote.best_ask if quote.best_ask is not None else reference_price
        if execution_price is None:
            blockers.append("missing_execution_price")

        if quote.best_bid is None or quote.best_ask is None:
            blockers.append("missing_two_sided_quote")
        else:
            spread_width = quote.best_ask - quote.best_bid
            if spread_width <= 0:
                blockers.append("locked_or_crossed_market")
            elif spread_width - self.max_spread_width > self.EPSILON:
                blockers.append("spread_too_wide")

        if (
            spread_width is not None
            and execution_price is not None
            and execution_price > self.EPSILON
        ):
            relative_spread_width = spread_width / execution_price

        estimated_slippage = self._estimate_slippage(spread_width)
        quality_score = self._estimate_quality_score(
            execution_price=execution_price,
            spread_width=spread_width,
            relative_spread_width=relative_spread_width,
            has_two_sided_quote=quote.best_bid is not None and quote.best_ask is not None,
        )
        quality_tier = self._score_to_tier(quality_score)

        if quality_score < self.min_quality_score:
            blockers.append("quality_score_too_low")

        return MarketQualityAssessment(
            reference_price=reference_price,
            execution_price=execution_price,
            spread_width=spread_width,
            relative_spread_width=relative_spread_width,
            estimated_slippage=estimated_slippage,
            quality_score=quality_score,
            quality_tier=quality_tier,
            blockers=tuple(dict.fromkeys(blockers)),
        )

    def _estimate_slippage(self, spread_width: float | None) -> float:
        if spread_width is None:
            return self.min_slippage
        slippage = spread_width * self.slippage_spread_multiplier
        return max(self.min_slippage, min(self.max_slippage, slippage))

    def _estimate_quality_score(
        self,
        *,
        execution_price: float | None,
        spread_width: float | None,
        relative_spread_width: float | None,
        has_two_sided_quote: bool,
    ) -> float:
        if execution_price is None:
            return 0.0

        score = 1.0
        if not has_two_sided_quote:
            score -= 0.30

        if spread_width is not None and self.max_spread_width > 0:
            score -= min(spread_width / self.max_spread_width, 1.0) * 0.40

        if (
            relative_spread_width is not None
            and execution_price <= self.relative_spread_low_price_threshold
            and self.relative_spread_score_threshold > 0
        ):
            score -= (
                min(relative_spread_width / self.relative_spread_score_threshold, 1.0)
                * self.relative_spread_score_weight
            )

        if execution_price <= 0.03 or execution_price >= 0.97:
            score -= 0.05

        return max(0.0, min(1.0, score))

    @staticmethod
    def _score_to_tier(score: float) -> str:
        if score >= 0.80:
            return "A"
        if score >= 0.70:
            return "B"
        if score >= 0.55:
            return "C"
        return "D"
