from __future__ import annotations

import json
from collections import defaultdict
from datetime import date, datetime, timezone
from pathlib import Path
from types import SimpleNamespace

from weather_trading.infrastructure.utils import utc_now
from weather_trading.services.evaluation.bin_family_diagnostics import build_temperature_market_family
from weather_trading.services.market_discovery.data_api_client import PolymarketDataApiClient
from weather_trading.services.market_discovery.wallet_watchlist import WalletWatchlistService
from weather_trading.services.rule_parser.deterministic_parser import DeterministicParser


async def build_watchlist_strategy_summary(
    *,
    reference_date: date,
    audit_snapshot: dict,
    root: Path,
    allow_remote_reconstruction: bool = False,
) -> dict:
    watchlist = WalletWatchlistService()
    data_api = PolymarketDataApiClient()
    snapshot_maps = await build_snapshot_watchlist_maps(
        audit_snapshot=audit_snapshot,
        watchlist=watchlist,
        data_api=data_api,
        allow_remote_reconstruction=allow_remote_reconstruction,
        root=root,
    )

    evaluations = list(audit_snapshot.get("evaluations", []))
    strategies = {
        "model_current": [],
        "model_skip_opposed": [],
        "model_skip_celsius_active_unclassified": [],
        "model_skip_opposed_and_celsius_active_unclassified": [],
        "model_confirmed_only": [],
        "model_aligned_only": [],
        "copy_coldmath": [],
        "copy_poligarch": [],
        "copy_watchlist_consensus": [],
    }
    parser = DeterministicParser()

    overlay_breakdown = defaultdict(int)
    model_current_by_overlay_signal: dict[str, list[dict]] = defaultdict(list)

    for evaluation in evaluations:
        key = (evaluation["snapshot_as_of_date"], evaluation["event_slug"])
        event = snapshot_maps["events"].get(key)
        alignment = snapshot_maps["alignments"].get(key)
        if event is None or alignment is None:
            continue

        current_trade = evaluate_current_model_trade(evaluation)
        if current_trade is not None:
            strategies["model_current"].append(current_trade)

        signal = alignment["signal"]
        experimental_filter_applies = should_skip_celsius_active_unclassified(
            evaluation=evaluation,
            alignment=alignment,
            parser=parser,
        )
        overlay_breakdown[signal] += 1
        if current_trade is not None:
            model_current_by_overlay_signal[signal].append(current_trade)
            if signal != "opposed":
                strategies["model_skip_opposed"].append(current_trade)
            if not experimental_filter_applies:
                strategies["model_skip_celsius_active_unclassified"].append(current_trade)
            if signal != "opposed" and not experimental_filter_applies:
                strategies["model_skip_opposed_and_celsius_active_unclassified"].append(current_trade)
            if signal in {"aligned", "mixed"}:
                strategies["model_confirmed_only"].append(current_trade)
        if signal == "aligned" and current_trade is not None:
            strategies["model_aligned_only"].append(current_trade)

        trader_candidates = build_trader_candidates(event, alignment["trades"])
        for strategy_name, trader_label in (
            ("copy_coldmath", "ColdMath"),
            ("copy_poligarch", "Poligarch"),
        ):
            candidate = trader_candidates["by_trader"].get(trader_label)
            trade = evaluate_candidate_trade(candidate, evaluation)
            if trade is not None:
                strategies[strategy_name].append(trade)

        consensus_trade = evaluate_candidate_trade(trader_candidates["consensus"], evaluation)
        if consensus_trade is not None:
            strategies["copy_watchlist_consensus"].append(consensus_trade)

    summary = {
        "captured_at_utc": utc_now().isoformat(),
        "reference_date": reference_date.isoformat(),
        "audit_snapshot": str(
            (root / "logs" / "snapshots" / f"{reference_date.isoformat()}_blind_snapshot_resolution_audit.json").relative_to(
                root
            )
        ),
        "evaluated_events": len(evaluations),
        "watchlist_events_with_local_snapshot_data": snapshot_maps["local_watchlist_events"],
        "watchlist_events_with_remote_reconstruction": snapshot_maps["remote_watchlist_events"],
        "watchlist_events_without_overlay": snapshot_maps["missing_watchlist_events"],
        "watchlist_overlay_breakdown": dict(sorted(overlay_breakdown.items())),
        "tracked_traders": snapshot_maps["tracked_traders"],
        "strategies": {name: summarize_trades(trades) for name, trades in strategies.items()},
        "model_current_by_overlay_signal": {
            signal: summarize_trades(trades)
            for signal, trades in sorted(model_current_by_overlay_signal.items())
        },
        "notes": [
            "prioriza watchlist ya congelada dentro de snapshots live cuando existe, y solo intenta reconstruccion remota como fallback",
            "copy_* usa solo trades de watchlist anteriores al snapshot y dentro de la ventana reciente configurada",
            "copy_* solo simula sesgo YES positivo en mercados que podemos mapear al snapshot",
            "model_aligned_only toma exactamente el top-edge del modelo cuando la watchlist esta alineada",
            "model_skip_opposed mantiene el trade del modelo salvo cuando la watchlist va en contra",
            "model_skip_celsius_active_unclassified bloquea el top-edge si la watchlist esta en active_unclassified y el top-edge pertenece a celsius|range_bin",
            "model_skip_opposed_and_celsius_active_unclassified combina ambos filtros",
            "model_confirmed_only solo mantiene trades del modelo con watchlist aligned o mixed",
        ],
    }
    return summary


def build_strategy_comparison_digest(strategy_summary: dict) -> dict:
    strategies = strategy_summary.get("strategies", {})
    selected_names = [
        "model_current",
        "model_skip_opposed",
        "model_skip_celsius_active_unclassified",
        "model_skip_opposed_and_celsius_active_unclassified",
    ]
    selected = {name: dict(strategies.get(name, {})) for name in selected_names}
    baseline = selected.get("model_current", {})
    baseline_pnl = float(baseline.get("total_pnl", 0.0))
    baseline_roi = float(baseline.get("roi_on_stake", 0.0))
    baseline_hit_rate = float(baseline.get("selected_market_hit_rate", 0.0))
    baseline_trades = int(baseline.get("trades", 0))

    deltas = {}
    for name in selected_names[1:]:
        metrics = selected.get(name, {})
        deltas[name] = {
            "trades_delta": int(metrics.get("trades", 0)) - baseline_trades,
            "hit_rate_delta": float(metrics.get("selected_market_hit_rate", 0.0)) - baseline_hit_rate,
            "pnl_delta": float(metrics.get("total_pnl", 0.0)) - baseline_pnl,
            "roi_delta": float(metrics.get("roi_on_stake", 0.0)) - baseline_roi,
        }

    best_by_pnl = max(
        selected_names,
        key=lambda name: float(selected.get(name, {}).get("total_pnl", float("-inf"))),
    )
    best_by_roi = max(
        selected_names,
        key=lambda name: float(selected.get(name, {}).get("roi_on_stake", float("-inf"))),
    )

    return {
        "selected_strategies": selected,
        "deltas_vs_model_current": deltas,
        "best_strategy_by_pnl": best_by_pnl,
        "best_strategy_by_roi": best_by_roi,
    }


def persist_watchlist_strategy_snapshot(*, root: Path, reference_date: date, payload: dict) -> Path:
    output_dir = root / "logs" / "snapshots"
    output_dir.mkdir(parents=True, exist_ok=True)
    path = output_dir / f"{reference_date.isoformat()}_watchlist_strategy_simulation.json"
    path.write_text(json.dumps(payload, indent=2, ensure_ascii=False), encoding="utf-8")
    return path


async def build_snapshot_watchlist_maps(
    *,
    audit_snapshot: dict,
    watchlist: WalletWatchlistService,
    data_api: PolymarketDataApiClient,
    allow_remote_reconstruction: bool,
    root: Path,
) -> dict:
    events_by_key: dict[tuple[str, str], dict] = {}
    alignments_by_key: dict[tuple[str, str], dict] = {}
    tracked_traders: list[dict] = []
    local_watchlist_events = 0
    remote_watchlist_events = 0
    missing_watchlist_events = 0

    for snapshot_rel_path in audit_snapshot.get("snapshot_files", []):
        snapshot_path = root / snapshot_rel_path
        if not snapshot_path.exists():
            continue
        snapshot = json.loads(snapshot_path.read_text(encoding="utf-8"))
        captured_at = parse_utc(snapshot.get("captured_at_utc"))
        evaluated_events = list(snapshot.get("evaluated_events", []))
        if snapshot.get("wallet_watchlist_tracked_traders"):
            tracked_traders = snapshot.get("wallet_watchlist_tracked_traders", tracked_traders)

        local_alignment_keys = set()
        for event in evaluated_events:
            key = (str(snapshot["as_of_date"]), str(event["event_slug"]))
            events_by_key[key] = event
            local_alignment = build_alignment_from_snapshot_event(event)
            if local_alignment is None:
                continue
            alignments_by_key[key] = local_alignment
            local_alignment_keys.add(key)
            local_watchlist_events += 1

        unresolved_events = [
            event
            for event in evaluated_events
            if (str(snapshot["as_of_date"]), str(event["event_slug"])) not in local_alignment_keys
        ]
        if not unresolved_events:
            continue

        if allow_remote_reconstruction:
            try:
                watchlist_snapshot = await watchlist.build_watchlist_snapshot(
                    data_client=data_api,
                    event_slugs={str(event.get("event_slug")) for event in unresolved_events if event.get("event_slug")},
                    as_of_utc=captured_at,
                )
                watchlist.remember_snapshot(watchlist_snapshot)
                tracked_traders = watchlist_snapshot.get("tracked_traders", tracked_traders)
            except Exception:
                watchlist_snapshot = None
        else:
            watchlist_snapshot = None

        for event in unresolved_events:
            key = (str(snapshot["as_of_date"]), str(event["event_slug"]))
            top_edge_market_id = find_top_edge_market_id(event)
            rows = [
                SimpleNamespace(
                    market_id=str(market["market_id"]),
                    market_slug=market.get("market_slug"),
                    question=str(market["question"]),
                )
                for market in event.get("markets", [])
            ]
            if watchlist_snapshot is None:
                alignments_by_key[key] = build_missing_alignment()
                missing_watchlist_events += 1
            else:
                alignments_by_key[key] = watchlist.summarize_event_alignment(
                    event_slug=str(event["event_slug"]),
                    rows=rows,
                    top_edge_market_id=top_edge_market_id,
                )
                remote_watchlist_events += 1

    return {
        "events": events_by_key,
        "alignments": alignments_by_key,
        "tracked_traders": tracked_traders,
        "local_watchlist_events": local_watchlist_events,
        "remote_watchlist_events": remote_watchlist_events,
        "missing_watchlist_events": missing_watchlist_events,
    }


def parse_utc(value: str | None) -> datetime:
    if not value:
        return datetime.now(timezone.utc)
    parsed = datetime.fromisoformat(value)
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def find_top_edge_market_id(event: dict) -> str:
    tradeable = [market for market in event.get("markets", []) if bool(market.get("is_tradeable"))]
    candidate_pool = tradeable or list(event.get("markets", []))
    candidate = max(candidate_pool, key=lambda market: float(market.get("edge_net", 0.0)))
    return str(candidate["market_id"])


def build_alignment_from_snapshot_event(event: dict) -> dict | None:
    if "watchlist_signal" not in event:
        return None
    return {
        "signal": str(event.get("watchlist_signal") or "silent"),
        "alignment_score": float(event.get("watchlist_alignment_score") or 0.0),
        "match_count": int(event.get("watchlist_match_count") or 0),
        "active_traders": list(event.get("watchlist_active_traders") or []),
        "aligned_traders": list(event.get("watchlist_aligned_traders") or []),
        "opposed_traders": list(event.get("watchlist_opposed_traders") or []),
        "event_only_traders": list(event.get("watchlist_event_only_traders") or []),
        "trades": list(event.get("watchlist_trades") or []),
    }


def build_missing_alignment() -> dict:
    return {
        "signal": "unavailable",
        "alignment_score": 0.0,
        "match_count": 0,
        "active_traders": [],
        "aligned_traders": [],
        "opposed_traders": [],
        "event_only_traders": [],
        "trades": [],
    }


def evaluate_current_model_trade(evaluation: dict) -> dict | None:
    if not bool(evaluation.get("paper_trade_taken")):
        return None
    return {
        "event_slug": evaluation["event_slug"],
        "market_id": evaluation["top_edge_market_id"],
        "hit": bool(evaluation.get("top_edge_hit")),
        "stake": float(evaluation.get("paper_trade_stake", 0.0)),
        "pnl": float(evaluation.get("paper_trade_pnl", 0.0)),
        "execution_price": float(evaluation.get("paper_trade_execution_price", 0.0)),
        "costs": float(evaluation.get("paper_trade_costs", 0.0)),
    }


def should_skip_celsius_active_unclassified(*, evaluation: dict, alignment: dict, parser: DeterministicParser) -> bool:
    if str(alignment.get("signal") or "") != "active_unclassified":
        return False
    family = build_temperature_market_family(
        str(evaluation.get("top_edge_question") or ""),
        str(evaluation.get("event_date") or ""),
        parser,
    )
    return family == "celsius|range_bin"


def build_trader_candidates(event: dict, watchlist_trades: list[dict]) -> dict:
    market_slug_map = {
        normalize_text(market.get("market_slug")): market
        for market in event.get("markets", [])
        if normalize_text(market.get("market_slug"))
    }
    question_map = {
        normalize_text(market.get("question")): market
        for market in event.get("markets", [])
        if normalize_text(market.get("question"))
    }

    scores_by_trader: dict[str, dict[str, float]] = defaultdict(lambda: defaultdict(float))
    market_by_id: dict[str, dict] = {}
    for trade in watchlist_trades:
        matched_market = None
        trade_market_slug = normalize_text(trade.get("market_slug"))
        trade_market_title = normalize_text(trade.get("market_title"))
        if trade_market_slug and trade_market_slug in market_slug_map:
            matched_market = market_slug_map[trade_market_slug]
        elif trade_market_title and trade_market_title in question_map:
            matched_market = question_map[trade_market_title]

        if matched_market is None or not bool(matched_market.get("is_tradeable")):
            continue

        bias = infer_yes_bias(trade)
        if bias <= 0:
            continue

        trader_label = str(trade.get("label") or trade.get("username") or trade.get("proxy_wallet"))
        magnitude = float(trade.get("size") or 1.0)
        market_id = str(matched_market["market_id"])
        scores_by_trader[trader_label][market_id] += magnitude
        scores_by_trader["__consensus__"][market_id] += magnitude
        market_by_id[market_id] = matched_market

    return {
        "by_trader": {
            trader: select_market_candidate(scores, market_by_id)
            for trader, scores in scores_by_trader.items()
            if trader != "__consensus__"
        },
        "consensus": select_market_candidate(scores_by_trader.get("__consensus__", {}), market_by_id),
    }


def select_market_candidate(scores: dict[str, float], market_by_id: dict[str, dict]) -> dict | None:
    if not scores:
        return None
    market_id, conviction = max(scores.items(), key=lambda item: item[1])
    if conviction <= 0:
        return None
    market = market_by_id[market_id]
    return {
        "market_id": market_id,
        "question": market["question"],
        "conviction": conviction,
        "execution_price": float(market.get("execution_price") or 0.0),
        "costs": float(market.get("estimated_costs") or 0.0),
    }


def infer_yes_bias(trade: dict) -> int:
    outcome = normalize_text(trade.get("outcome"))
    side = normalize_text(trade.get("side"))
    if outcome == "yes" and side == "buy":
        return 1
    if outcome == "yes" and side == "sell":
        return -1
    if outcome == "no" and side == "buy":
        return -1
    if outcome == "no" and side == "sell":
        return 1
    return 0


def normalize_text(value) -> str | None:
    if value in (None, ""):
        return None
    return " ".join(str(value).strip().lower().split())


def evaluate_candidate_trade(candidate: dict | None, evaluation: dict) -> dict | None:
    if candidate is None:
        return None

    stake = float(candidate["execution_price"]) + float(candidate["costs"])
    hit = str(candidate["market_id"]) == str(evaluation["winner_market_id"])
    pnl = (1.0 - stake) if hit else -stake
    return {
        "event_slug": evaluation["event_slug"],
        "market_id": candidate["market_id"],
        "hit": hit,
        "stake": stake,
        "pnl": pnl,
        "execution_price": float(candidate["execution_price"]),
        "costs": float(candidate["costs"]),
        "question": candidate["question"],
        "conviction": float(candidate.get("conviction", 0.0)),
    }


def summarize_trades(trades: list[dict]) -> dict:
    if not trades:
        return {
            "trades": 0,
            "selected_market_hit_rate": 0.0,
            "total_stake": 0.0,
            "total_pnl": 0.0,
            "roi_on_stake": 0.0,
            "avg_execution_price": 0.0,
        }

    total_stake = sum(float(trade["stake"]) for trade in trades)
    total_pnl = sum(float(trade["pnl"]) for trade in trades)
    return {
        "trades": len(trades),
        "selected_market_hit_rate": sum(bool(trade["hit"]) for trade in trades) / len(trades),
        "total_stake": total_stake,
        "total_pnl": total_pnl,
        "roi_on_stake": (total_pnl / total_stake) if total_stake else 0.0,
        "avg_execution_price": sum(float(trade["execution_price"]) for trade in trades) / len(trades),
    }
