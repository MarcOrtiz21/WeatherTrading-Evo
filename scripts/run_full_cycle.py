import sys
import os
import asyncio
from datetime import datetime

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
SRC = os.path.join(ROOT, "src")
if SRC not in sys.path:
    sys.path.insert(0, SRC)

from weather_trading.services.rule_parser.deterministic_parser import DeterministicParser
from weather_trading.services.station_mapper.service import StationMapperService
from weather_trading.services.weather_ingestion.metar_client import MetarIngestor
from weather_trading.infrastructure.database import init_db, AsyncSessionLocal
from weather_trading.infrastructure.utils import utc_now
from weather_trading.services.persistence.repository import WeatherRepository
from weather_trading.services.forecast_engine.baseline import BaselineForecastModel
from weather_trading.services.pricing_engine.service import PricingEngine
from weather_trading.domain.models import MarketQuote

async def run_full_cycle():
    print("=== INICIANDO CICLO COMPLETO DE WEATHER TRADING ===\n")
    
    # 0. Inicializar Infraestructura
    await init_db()
    parser = DeterministicParser()
    mapper = StationMapperService()
    ingestor = MetarIngestor()
    forecaster = BaselineForecastModel()
    pricing = PricingEngine()

    # 1. Discovery (Mockup de mercado de Polymarket)
    market_data = {
        "id": "madrid-max-32-apr-15",
        "question": "¿Hará 32°C o más en Madrid el 15 de abril de 2026?",
        "description": "Máxima diaria en Aeropuerto de Madrid (LEMD) el 15 de abril de 2026 según Wunderground.",
        "rules": "Se usará el valor reportado por Wunderground para LEMD el 15 de abril de 2026.",
        "outcomes": ["Yes", "No"]
    }
    print(f"[1] Mercado detectado: {market_data['question']}")

    # 2. Parsing y Mapping
    spec = parser.parse(market_data)
    if spec is None:
        raise RuntimeError("No se pudo construir un MarketSpec válido para el mercado de prueba.")
    spec = mapper.enrich(spec)
    if spec.requires_manual_review:
        raise RuntimeError(f"El MarketSpec requiere revisión manual: {spec.notes}")
    print(f"[2] Especificación extraída: {spec.station_code} ({spec.city}), Umbral: {spec.threshold_c}°C")

    # 3. Ingestión Meteorológica (Simulada para el test, o real si hay red)
    print(f"[3] Buscando observaciones para {spec.station_code}...")
    raw_metar = None
    try:
        raw_metar = await ingestor.fetch_metar(spec.station_code)
    except Exception as exc:
        print(f"    Error al obtener METAR real ({exc}), usando mock.")
    if raw_metar:
        observation = ingestor.parse_metar(raw_metar)
        print(f"    Observación actual: {observation.temp_c}°C a las {observation.observed_at_utc.strftime('%H:%M')} UTC")
    else:
        from weather_trading.domain.models import WeatherObservation, ResolutionSource
        observation = WeatherObservation(
            station_code=spec.station_code,
            provider=ResolutionSource.METAR,
            observed_at_utc=utc_now(),
            temp_c=28.5 # Simulamos calor intradía
        )

    # 4. Persistencia
    async with AsyncSessionLocal() as session:
        repo = WeatherRepository(session)
        print(f"[4] Persistiendo datos en DB...")
        await repo.save_market_spec(spec)
        await repo.save_observation(observation)
        
        # Recuperar histórico reciente para el forecast
        history = await repo.get_latest_observations(spec.station_code, limit=24)

    # 5. Forecast
    print(f"[5] Generando forecast probabilístico...")
    # Simulamos que son las 13:00 local para el cálculo de delta
    now_local = datetime.now().replace(hour=13, minute=0)
    forecast = forecaster.estimate_max_distribution(spec.market_id, history, now_local)
    
    p_win = forecast.probability_at_or_above(spec.threshold_c)
    print(f"    Probabilidad estimada de éxito (T_max >= {spec.threshold_c}): {p_win:.2%}")

    # 6. Pricing y Señal
    print(f"[6] Comparando con precios de mercado...")
    # Mock de cotización de Polymarket: el "Yes" cotiza a 0.55 (55%)
    quote = MarketQuote(
        market_id=spec.market_id,
        outcome="Yes",
        best_bid=0.54,
        best_ask=0.56,
        captured_at_utc=utc_now()
    )
    
    signal = pricing.generate_signal(spec, forecast, quote)
    
    print(f"    Fair Value: {signal.fair_probability:.2f}")
    print(f"    Market Value (Mid): {signal.market_probability:.2f}")
    print(f"    Edge Neto: {signal.edge_net:.2%}")

    if signal.is_tradeable:
        print("\n>>> ESTRATEGIA: [VÁLIDA] Existe edge positivo tras costes y margen.")
    else:
        print("\n>>> ESTRATEGIA: [BLOQUEADA] No hay edge suficiente o riesgo alto.")

    print("\n=== CICLO COMPLETADO CON ÉXITO ===")

if __name__ == "__main__":
    asyncio.run(run_full_cycle())
