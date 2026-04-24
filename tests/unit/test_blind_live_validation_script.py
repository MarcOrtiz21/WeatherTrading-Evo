import json
from pathlib import Path
from types import SimpleNamespace

import pytest

from scripts import run_blind_live_validation


def test_build_snapshot_path_uses_expected_filename(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.setattr(run_blind_live_validation, "ROOT", str(tmp_path))

    path = run_blind_live_validation.build_snapshot_path("2026-04-17")

    assert path == tmp_path / "logs" / "snapshots" / "2026-04-17_polymarket_blind_live_validation.json"


@pytest.mark.asyncio
async def test_main_reuses_existing_snapshot_without_network(monkeypatch, tmp_path: Path, capsys) -> None:
    monkeypatch.setattr(run_blind_live_validation, "ROOT", str(tmp_path))
    snapshot_path = run_blind_live_validation.build_snapshot_path("2026-04-17")
    snapshot_path.parent.mkdir(parents=True, exist_ok=True)
    snapshot_path.write_text(
        json.dumps(
            {
                "as_of_date": "2026-04-17",
                "evaluated_events": [
                    {
                        "event_title": "Highest temperature in Madrid on April 18, 2026",
                        "station_code": "LEMD",
                        "event_date": "2026-04-18",
                        "forecast_center_c": 21.4,
                        "model_mode_question": "20-21C",
                        "model_mode_probability": 0.31,
                        "market_mode_question": "22-23C",
                        "market_mode_probability": 0.28,
                        "top_edge_question": "20-21C",
                        "top_edge_net": 0.12,
                        "event_evidence_tier": "A",
                        "event_evidence_score": 1.0,
                        "event_operable": True,
                        "tradeable_markets": 2,
                        "watchlist_signal": "silent",
                        "watchlist_active_traders": [],
                        "watchlist_aligned_traders": [],
                        "watchlist_opposed_traders": [],
                        "watchlist_veto_applied": False,
                    }
                ],
                "skipped_events": [],
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )
    monkeypatch.setattr(
        run_blind_live_validation,
        "parse_args",
        lambda: SimpleNamespace(
            as_of_date="2026-04-17",
            max_events=20,
            min_horizon_days=0,
            max_horizon_days=4,
            force_refresh=False,
        ),
    )

    def fail_if_called(*_args, **_kwargs):
        raise AssertionError("Network clients should not be built when reusing an existing snapshot.")

    monkeypatch.setattr(run_blind_live_validation, "PolymarketPublicPageClient", fail_if_called)

    await run_blind_live_validation.main()

    output = capsys.readouterr().out
    assert "Snapshot existente reutilizado" in output
    assert "Eventos evaluados: 1" in output
