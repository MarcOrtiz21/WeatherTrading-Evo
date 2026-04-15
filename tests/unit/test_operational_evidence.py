from weather_trading.services.execution_engine.operational_evidence import OperationalEvidenceGate


def test_operational_evidence_gate_accepts_well_supported_short_horizon_event():
    gate = OperationalEvidenceGate(
        min_event_score=0.62,
        min_parser_confidence=0.85,
        min_calibration_days_for_calibrated=3,
        max_forecast_std_dev_c=3.0,
        max_late_intraday_remaining_hours_without_observed_max=6,
    )

    assessment = gate.assess(
        parser_confidence_score=0.95,
        forecast_strategy="baseline_short_horizon",
        horizon_days=1,
        calibration_days=7,
        ensemble_members=20,
        forecast_std_dev_c=1.4,
        intraday_active=False,
        intraday_source=None,
        intraday_remaining_hours=None,
    )

    assert assessment.is_operable is True
    assert assessment.tier in {"A", "B"}
    assert assessment.blockers == ()


def test_operational_evidence_gate_blocks_late_intraday_without_observed_max():
    gate = OperationalEvidenceGate(
        min_event_score=0.62,
        min_parser_confidence=0.85,
        min_calibration_days_for_calibrated=3,
        max_forecast_std_dev_c=3.0,
        max_late_intraday_remaining_hours_without_observed_max=6,
    )

    assessment = gate.assess(
        parser_confidence_score=0.92,
        forecast_strategy="baseline_short_horizon",
        horizon_days=0,
        calibration_days=0,
        ensemble_members=0,
        forecast_std_dev_c=1.8,
        intraday_active=True,
        intraday_source="forecast_proxy",
        intraday_remaining_hours=3,
    )

    assert assessment.is_operable is False
    assert "late_intraday_without_observed_max" in assessment.blockers
    assert "missing_ensemble_members" in assessment.notes


def test_operational_evidence_gate_blocks_calibrated_strategy_without_history():
    gate = OperationalEvidenceGate(
        min_event_score=0.62,
        min_parser_confidence=0.85,
        min_calibration_days_for_calibrated=3,
        max_forecast_std_dev_c=3.0,
        max_late_intraday_remaining_hours_without_observed_max=6,
    )

    assessment = gate.assess(
        parser_confidence_score=0.90,
        forecast_strategy="optimized_multimodel",
        horizon_days=3,
        calibration_days=1,
        ensemble_members=15,
        forecast_std_dev_c=2.2,
        intraday_active=False,
        intraday_source=None,
        intraday_remaining_hours=None,
    )

    assert assessment.is_operable is False
    assert "insufficient_calibration_history" in assessment.blockers
