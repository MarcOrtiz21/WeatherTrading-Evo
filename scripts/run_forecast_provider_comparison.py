import argparse
import asyncio
import json
import os
import sys
from dataclasses import asdict, dataclass
from datetime import date, timedelta

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
SRC = os.path.join(ROOT, "src")
if SRC not in sys.path:
    sys.path.insert(0, SRC)

from weather_trading.domain.models import MarketQuote
from weather_trading.infrastructure.utils import utc_now
from weather_trading.services.forecast_engine.openmeteo_distribution import OpenMeteoDistributionBuilder
from weather_trading.services.market_discovery.gamma_client import PolymarketGammaClient
from weather_trading.services.market_discovery.public_page_client import PolymarketPublicPageClient
from weather_trading.services.pricing_engine.service import PricingEngine
from weather_trading.services.rule_parser.deterministic_parser import DeterministicParser
from weather_trading.services.station_mapper.service import StationMapperService
from weather_trading.services.weather_ingestion.openmeteo_client import OpenMeteoClient
from weather_trading.services.weather_ingestion.weather_company_client import WeatherCompanyClient


CATEGORY_URLS = (
    "https://polymarket.com/es/predictions/temperature",
    "https://polymarket.com/es/predictions/weather",
)


@dataclass(slots=True)
class ProviderEventComparison:
    provider: str
    event_slug: str
    event_title: str
    event_date: str
    station_code: str
    forecast_center_c: float
    forecast_mode_c: float | int | None
    model_mode_question: str
    model_mode_probability: float
    top_edge_question: str
    top_edge_net: float
    tradeable_markets: int


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Compara Open-Meteo y The Weather Company sobre bins live de Polymarket."
    )
    parser.add_argument("--as-of-date", default=date.today().isoformat(), help="Fecha de referencia YYYY-MM-DD.")
    parser.add_argument("--max-events", type=int, default=5, help="Número máximo de eventos a comparar.")
    parser.add_argument("--min-horizon-days", type=int, default=1, help="Horizonte mínimo.")
    parser.add_argument("--max-horizon-days", type=int, default=4, help="Horizonte máximo.")
    parser.add_argument(
        "--cities",
        default="",
        help="Filtro opcional por ciudad, separado por comas. Ej: NYC,London,Dallas",
    )
    return parser.parse_args()


async def main() -> None:
    args = parse_args()
    as_of_date = date.fromisoformat(args.as_of_date)
    city_filters = {
        value.strip().lower()
        for value in args.cities.split(",")
        if value.strip()
    }

    parser = DeterministicParser()
    mapper = StationMapperService()
    pricing = PricingEngine()
    baseline_builder = OpenMeteoDistributionBuilder()
    gamma = PolymarketGammaClient()
    client = PolymarketPublicPageClient(locale="es")

    try:
        event_payloads = await discover_temperature_event_payloads(
            gamma=gamma,
            client=client,
            as_of_date=as_of_date,
            min_horizon_days=args.min_horizon_days,
            max_horizon_days=args.max_horizon_days,
        )
    except Exception as exc:
        print(
            json.dumps(
                {
                    "captured_at_utc": utc_now().isoformat(),
                    "as_of_date": as_of_date.isoformat(),
                    "providers_compared": [],
                    "events_compared": [],
                    "comparisons": [],
                    "skipped_events": [],
                    "error": f"market_discovery_failed:{exc}",
                },
                indent=2,
                ensure_ascii=False,
            )
        )
        return

    provider_clients = {
        "open_meteo": OpenMeteoClient(),
    }
    try:
        provider_clients["weather_company"] = WeatherCompanyClient()
        provider_clients["weather_company"]._require_api_key()
    except ValueError:
        pass

    comparisons: list[ProviderEventComparison] = []
    skipped_events: list[dict] = []

    for payload in event_payloads:
        if len({item.event_slug for item in comparisons}) >= args.max_events:
            break

        representative_spec, station, skip_reason = build_representative_spec(
            payload=payload,
            parser=parser,
            mapper=mapper,
        )
        if representative_spec is None or station is None:
            skipped_events.append(
                {
                    "event_slug": payload.get("event_slug"),
                    "reason": skip_reason or "unsupported_event",
                }
            )
            continue

        if city_filters and representative_spec.city.lower() not in city_filters and representative_spec.station_code.lower() not in city_filters:
            continue

        horizon_days = (representative_spec.local_date - as_of_date).days
        if horizon_days < args.min_horizon_days or horizon_days > args.max_horizon_days:
            continue

        parsed_specs = []
        for market in payload.get("markets", []):
            market_data = {
                "id": str(market.get("id")),
                "question": market.get("question", ""),
                "description": market.get("description") or payload.get("event_description", ""),
                "rules": payload.get("event_description", ""),
                "outcomes": ("Yes", "No"),
                "event_date": payload.get("event_date"),
                "resolution_source_url": market.get("resolution_source") or market.get("resolutionSource") or "",
                "endDate": market.get("endDate"),
            }
            spec = parser.parse(market_data)
            if spec is None:
                continue
            spec = mapper.enrich(spec)
            if not spec.requires_manual_review:
                parsed_specs.append((spec, market))

        if not parsed_specs:
            skipped_events.append({"event_slug": payload.get("event_slug"), "reason": "no_parseable_markets"})
            continue

        for provider_name, weather_client in provider_clients.items():
            try:
                forecast_payload = await weather_client.fetch_forecast(
                    latitude=station["latitude"],
                    longitude=station["longitude"],
                    local_date=representative_spec.local_date,
                )
            except Exception as exc:
                skipped_events.append(
                    {
                        "event_slug": payload.get("event_slug"),
                        "reason": f"forecast_fetch_failed:{provider_name}",
                        "detail": str(exc),
                    }
                )
                continue
            if not forecast_payload or forecast_payload.get("model_max_temp") is None:
                skipped_events.append(
                    {"event_slug": payload.get("event_slug"), "reason": f"missing_forecast:{provider_name}"}
                )
                continue

            distribution = baseline_builder.build(
                market_id=str(payload.get("event_slug")),
                model_max_temp_c=float(forecast_payload["model_max_temp"]),
                horizon_days=horizon_days,
                hourly_temperatures_c=forecast_payload.get("model_hourly_temps"),
                cloud_cover_avg=forecast_payload.get("model_cloud_cover_avg"),
            )

            signals = []
            for spec, market in parsed_specs:
                quote = MarketQuote(
                    market_id=spec.market_id,
                    outcome="Yes",
                    best_bid=to_float(market.get("bestBid")),
                    best_ask=to_float(market.get("bestAsk")),
                    last_price=extract_last_price(market),
                    captured_at_utc=utc_now(),
                )
                signals.append((spec.question, pricing.generate_signal(spec, distribution, quote)))

            model_mode_question, model_mode_probability = max(
                ((question, signal.fair_probability) for question, signal in signals),
                key=lambda item: item[1],
            )
            top_edge_question, top_edge_signal = max(signals, key=lambda item: item[1].edge_net)

            comparisons.append(
                ProviderEventComparison(
                    provider=provider_name,
                    event_slug=str(payload.get("event_slug")),
                    event_title=str(payload.get("event_title")),
                    event_date=representative_spec.local_date.isoformat(),
                    station_code=representative_spec.station_code,
                    forecast_center_c=extract_note_value(distribution.notes, "center", float(forecast_payload["model_max_temp"])),
                    forecast_mode_c=distribution.most_likely_temperature(),
                    model_mode_question=model_mode_question,
                    model_mode_probability=model_mode_probability,
                    top_edge_question=top_edge_question,
                    top_edge_net=top_edge_signal.edge_net,
                    tradeable_markets=sum(1 for _, signal in signals if signal.is_tradeable),
                )
            )

    result = {
        "captured_at_utc": utc_now().isoformat(),
        "as_of_date": as_of_date.isoformat(),
        "providers_compared": sorted({item.provider for item in comparisons}),
        "events_compared": sorted({item.event_slug for item in comparisons}),
        "comparisons": [asdict(item) for item in comparisons],
        "skipped_events": skipped_events,
    }
    print(json.dumps(result, indent=2, ensure_ascii=False))


async def discover_temperature_event_payloads(
    gamma: PolymarketGammaClient,
    client: PolymarketPublicPageClient,
    as_of_date: date,
    min_horizon_days: int,
    max_horizon_days: int,
) -> list[dict]:
    start_date = as_of_date + timedelta(days=min_horizon_days)
    end_date = as_of_date + timedelta(days=max_horizon_days)
    try:
        return await gamma.discover_temperature_event_payloads(
            active=True,
            closed=False,
            start_date=start_date,
            end_date=end_date,
        )
    except Exception:
        slugs: list[str] = []
        for category_url in CATEGORY_URLS:
            slugs.extend(await client.fetch_category_event_slugs(category_url))
        unique_slugs = sorted({slug for slug in slugs if slug.startswith("highest-temperature-in-")})
        payloads = [await client.fetch_event_payload(slug) for slug in unique_slugs]
        return [
            payload
            for payload in payloads
            if payload.get("event_date")
            and start_date <= date.fromisoformat(payload["event_date"]) <= end_date
        ]


def build_representative_spec(payload: dict, parser: DeterministicParser, mapper: StationMapperService):
    parsed_specs = []
    for market in payload.get("markets", []):
        market_data = {
            "id": str(market.get("id")),
            "question": market.get("question", ""),
            "description": market.get("description") or payload.get("event_description", ""),
            "rules": payload.get("event_description", ""),
            "outcomes": ("Yes", "No"),
            "event_date": payload.get("event_date"),
            "resolution_source_url": market.get("resolution_source") or market.get("resolutionSource") or "",
            "endDate": market.get("endDate"),
        }
        spec = parser.parse(market_data)
        if spec is None:
            continue
        parsed_specs.append(mapper.enrich(spec))

    if not parsed_specs:
        return None, None, "no_parseable_markets"

    representative_spec = next(
        (spec for spec in parsed_specs if not spec.requires_manual_review),
        parsed_specs[0],
    )
    if representative_spec.requires_manual_review:
        return None, None, "manual_review_required"

    station = mapper.get_station(representative_spec.station_code)
    if not station:
        return None, None, "missing_station_catalog"
    if not mapper.is_region_allowed(mapper.get_station_region(representative_spec.station_code)):
        return None, None, "region_not_allowed"
    return representative_spec, station, None


def extract_note_value(notes: tuple[str, ...], key: str, default: float) -> float:
    prefix = f"{key}="
    for note in notes:
        if note.startswith(prefix):
            try:
                return float(note.split("=", 1)[1])
            except ValueError:
                return default
    return default


def to_float(value) -> float | None:
    if value in (None, ""):
        return None
    return float(value)


def extract_last_price(market: dict) -> float | None:
    direct = to_float(market.get("lastTradePrice"))
    if direct is not None:
        return direct

    outcome_prices = market.get("outcomePrices")
    if isinstance(outcome_prices, str):
        try:
            outcome_prices = json.loads(outcome_prices)
        except json.JSONDecodeError:
            outcome_prices = None

    if isinstance(outcome_prices, list) and outcome_prices:
        return to_float(outcome_prices[0])
    return None


if __name__ == "__main__":
    asyncio.run(main())
