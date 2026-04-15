from __future__ import annotations

from dataclasses import dataclass

from weather_trading.infrastructure.config import ConfigLoader


@dataclass(slots=True)
class OperationalEvidenceAssessment:
    score: float
    tier: str
    is_operable: bool
    blockers: tuple[str, ...] = ()
    notes: tuple[str, ...] = ()


class OperationalEvidenceGate:
    """Evalua si un evento tiene suficiente evidencia para ser operable."""

    def __init__(
        self,
        *,
        min_event_score: float | None = None,
        min_parser_confidence: float | None = None,
        min_calibration_days_for_calibrated: int | None = None,
        max_forecast_std_dev_c: float | None = None,
        max_late_intraday_remaining_hours_without_observed_max: int | None = None,
    ) -> None:
        self.min_event_score = float(
            min_event_score
            if min_event_score is not None
            else ConfigLoader.get("operational_evidence.min_event_score", 0.62)
        )
        self.min_parser_confidence = float(
            min_parser_confidence
            if min_parser_confidence is not None
            else ConfigLoader.get("operational_evidence.min_parser_confidence", 0.85)
        )
        self.min_calibration_days_for_calibrated = int(
            min_calibration_days_for_calibrated
            if min_calibration_days_for_calibrated is not None
            else ConfigLoader.get("operational_evidence.min_calibration_days_for_calibrated", 3)
        )
        self.max_forecast_std_dev_c = float(
            max_forecast_std_dev_c
            if max_forecast_std_dev_c is not None
            else ConfigLoader.get("operational_evidence.max_forecast_std_dev_c", 3.0)
        )
        self.max_late_intraday_remaining_hours_without_observed_max = int(
            max_late_intraday_remaining_hours_without_observed_max
            if max_late_intraday_remaining_hours_without_observed_max is not None
            else ConfigLoader.get(
                "operational_evidence.max_late_intraday_remaining_hours_without_observed_max",
                6,
            )
        )

    def assess(
        self,
        *,
        parser_confidence_score: float,
        forecast_strategy: str,
        horizon_days: int,
        calibration_days: int,
        ensemble_members: int,
        forecast_std_dev_c: float,
        intraday_active: bool,
        intraday_source: str | None,
        intraday_remaining_hours: int | None,
    ) -> OperationalEvidenceAssessment:
        parser_confidence = self._clamp(parser_confidence_score)
        score = 1.0
        blockers: list[str] = []
        notes = [
            f"parser_confidence={parser_confidence:.2f}",
            f"forecast_strategy={forecast_strategy}",
            f"calibration_days={int(calibration_days)}",
            f"ensemble_members={int(ensemble_members)}",
            f"forecast_std_dev_c={float(forecast_std_dev_c):.2f}",
        ]

        score -= (1.0 - parser_confidence) * 0.35
        if parser_confidence < self.min_parser_confidence:
            blockers.append("parser_confidence_too_low")

        if forecast_strategy not in {"baseline_short_horizon", "baseline_fallback"}:
            if calibration_days < self.min_calibration_days_for_calibrated:
                score -= 0.20
                blockers.append("insufficient_calibration_history")
            elif calibration_days < self.min_calibration_days_for_calibrated + 2:
                score -= 0.10
                notes.append("calibration_window_shallow")
        elif horizon_days >= 2 and calibration_days <= 0:
            score -= 0.05
            notes.append("baseline_without_recent_calibration")

        if ensemble_members <= 0:
            score -= 0.05
            notes.append("missing_ensemble_members")

        if forecast_std_dev_c > self.max_forecast_std_dev_c:
            score -= 0.20
            blockers.append("forecast_dispersion_too_high")
        elif forecast_std_dev_c > self.max_forecast_std_dev_c * 0.8:
            score -= 0.10
            notes.append("forecast_dispersion_elevated")

        if horizon_days == 0:
            notes.append(f"intraday_active={intraday_active}")
            notes.append(f"intraday_source={intraday_source or 'none'}")
            if intraday_remaining_hours is not None:
                notes.append(f"intraday_remaining_hours={int(intraday_remaining_hours)}")

            if not intraday_active:
                score -= 0.15
                blockers.append("same_day_without_intraday_context")
            elif intraday_source == "local_weather_observations":
                score += 0.05
            else:
                score -= 0.10
                if (
                    intraday_remaining_hours is not None
                    and intraday_remaining_hours
                    <= self.max_late_intraday_remaining_hours_without_observed_max
                ):
                    score -= 0.10
                    blockers.append("late_intraday_without_observed_max")

        score = self._clamp(score)
        if score < self.min_event_score:
            blockers.append("event_evidence_score_too_low")

        blockers = list(dict.fromkeys(blockers))
        tier = self._score_to_tier(score)
        return OperationalEvidenceAssessment(
            score=score,
            tier=tier,
            is_operable=not blockers and score >= self.min_event_score,
            blockers=tuple(blockers),
            notes=tuple(notes),
        )

    @staticmethod
    def _clamp(value: float) -> float:
        return max(0.0, min(1.0, float(value)))

    @staticmethod
    def _score_to_tier(score: float) -> str:
        if score >= 0.85:
            return "A"
        if score >= 0.72:
            return "B"
        if score >= 0.62:
            return "C"
        return "D"
