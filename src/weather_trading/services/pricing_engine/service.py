import math

from weather_trading.domain.models import (
    MarketSpec, ForecastDistribution, MarketQuote, TradingSignal, MetricKind, RoundingMethod
)
from weather_trading.infrastructure.config import ConfigLoader
from weather_trading.services.execution_engine.market_quality import MarketQualityFilter

class PricingEngine:
    """Motor de pricing para comparar fair value con precio de mercado."""

    def __init__(
        self,
        fee_pct: float | None = None,
        default_slippage: float | None = None,
        safety_margin_base: float | None = None,
        quality_filter: MarketQualityFilter | None = None,
    ):
        self.fee_pct = float(
            fee_pct if fee_pct is not None else ConfigLoader.get("risk_limits.fee_buffer", 0.02)
        )
        self.default_slippage = float(
            default_slippage
            if default_slippage is not None
            else ConfigLoader.get("risk_limits.min_slippage", 0.0025)
        )
        self.safety_margin_base = float(
            safety_margin_base
            if safety_margin_base is not None
            else ConfigLoader.get("risk_limits.safety_margin_base", 0.05)
        )
        self.quality_filter = quality_filter or MarketQualityFilter()

    def generate_signal(
        self, 
        spec: MarketSpec, 
        forecast: ForecastDistribution, 
        quote: MarketQuote
    ) -> TradingSignal:
        """Calcula el edge neto para un resultado concreto."""
        blockers: list[str] = []
        
        # 1. Calcular Fair Value (Probabilidad Fair)
        # Por ahora asumimos mercados de umbral (¿>= X?)
        fair_prob = 0.0
        if spec.metric == MetricKind.MAX_TEMP_C and spec.threshold_c is not None:
            if not float(spec.threshold_c).is_integer():
                gaussian_prob = self._probability_from_gaussian_notes(
                    forecast,
                    low_c=spec.threshold_c,
                    high_c=None,
                )
                if gaussian_prob is not None:
                    fair_prob = gaussian_prob
                else:
                    fair_prob = forecast.probability_at_or_above(spec.threshold_c)
                    if spec.rounding_method in {
                        RoundingMethod.NONE,
                        RoundingMethod.STRICT_DECIMAL,
                    }:
                        blockers.append("integer_distribution_used_for_decimal_threshold")
            else:
                fair_prob = forecast.probability_at_or_above(spec.threshold_c)
        elif spec.metric == MetricKind.TEMPERATURE_BIN and (
            spec.bin_low_c is not None or spec.bin_high_c is not None
        ):
            has_decimal_bound = any(
                bound is not None and not float(bound).is_integer()
                for bound in (spec.bin_low_c, spec.bin_high_c)
            )
            if has_decimal_bound:
                gaussian_prob = self._probability_from_gaussian_notes(
                    forecast,
                    low_c=spec.bin_low_c,
                    high_c=spec.bin_high_c,
                )
                if gaussian_prob is not None:
                    fair_prob = gaussian_prob
                else:
                    fair_prob = forecast.probability_between(spec.bin_low_c, spec.bin_high_c)
                    blockers.append("integer_distribution_used_for_decimal_bin")
            else:
                fair_prob = forecast.probability_between(spec.bin_low_c, spec.bin_high_c)
        else:
            # TODO: Otros tipos de métricas y bins multinomiales
            fair_prob = 0.5
            blockers.append("unsupported_market_metric")
            
        # 2. Obtener Probabilidad Implícita del Mercado (del Quote)
        # Asumimos que el "midpoint" es la mejor estimación de la probabilidad de mercado.
        quality = self.quality_filter.assess_yes_quote(quote)
        market_prob = quality.reference_price
        if market_prob is None:
            market_prob = 0.5
            blockers.append("missing_market_quote")

        execution_price = quality.execution_price if quality.execution_price is not None else market_prob

        # 3. Calcular Edge Bruto (Direccional)
        # Por ahora solo consideramos operar "YES" si fair > market
        # O "NO" si market > fair. 
        # Aquí simplificamos para operar sobre el outcome 'Yes'.
        edge_gross = fair_prob - execution_price

        # 4. Estimar costes (Fees + Slippage)
        estimated_costs = self.fee_pct + (
            quality.estimated_slippage if quality.execution_price is not None else self.default_slippage
        )

        # 5. Margen de Seguridad
        # Podría depender de la confianza del parser o del modelo.
        safety_margin = self.safety_margin_base + (1.0 - spec.confidence_score) * 0.1

        if spec.requires_manual_review:
            blockers.append("market_spec_requires_manual_review")
        blockers.extend(quality.blockers)

        return TradingSignal(
            market_id=spec.market_id,
            outcome=quote.outcome,
            fair_probability=fair_prob,
            market_probability=market_prob,
            edge_gross=edge_gross,
            estimated_costs=estimated_costs,
            safety_margin=safety_margin,
            blockers=tuple(dict.fromkeys(blockers)),
            execution_price=execution_price,
            spread_width=quality.spread_width,
            relative_spread_width=quality.relative_spread_width,
            quality_score=quality.quality_score,
            quality_tier=quality.quality_tier,
        )

    def _probability_from_gaussian_notes(
        self,
        forecast: ForecastDistribution,
        low_c: float | None,
        high_c: float | None,
    ) -> float | None:
        metadata = self._extract_gaussian_metadata(forecast)
        if metadata is None:
            return None

        center, std_dev = metadata
        return self._normal_interval_probability(center, std_dev, low_c, high_c)

    def _extract_gaussian_metadata(self, forecast: ForecastDistribution) -> tuple[float, float] | None:
        values: dict[str, float] = {}
        for note in forecast.notes:
            if "=" not in note:
                continue
            key, raw_value = note.split("=", 1)
            try:
                values[key] = float(raw_value)
            except ValueError:
                continue

        center = values.get("center")
        std_dev = values.get("std_dev")
        if center is None or std_dev is None or std_dev <= 0:
            return None
        return center, std_dev

    def _normal_interval_probability(
        self,
        center: float,
        std_dev: float,
        low_c: float | None,
        high_c: float | None,
    ) -> float:
        def cdf(x: float) -> float:
            z_score = (x - center) / (std_dev * math.sqrt(2))
            return 0.5 * (1 + math.erf(z_score))

        lower_cdf = 0.0 if low_c is None else cdf(low_c)
        upper_cdf = 1.0 if high_c is None else cdf(high_c)
        return max(0.0, min(1.0, upper_cdf - lower_cdf))
