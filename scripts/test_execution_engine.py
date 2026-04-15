import sys
import os
import asyncio
from eth_account import Account

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
SRC = os.path.join(ROOT, "src")
if SRC not in sys.path:
    sys.path.insert(0, SRC)

from weather_trading.domain.models import TradingSignal, DecisionAction
from weather_trading.services.execution_engine.signer import PolymarketSigner
from weather_trading.services.execution_engine.order_router import OrderRouter

async def test_execution_flow():
    print("=== PROBANDO FLUJO DE EJECUCIÓN (SIGNER EIP-712) ===\n")

    # 1. Generar clave privada de prueba (Wallet efímera)
    test_key = Account.create().key.hex()
    signer = PolymarketSigner(private_key=test_key)
    router = OrderRouter(signer=signer)

    # 2. Mock de señal de trading POSITIVA (Con Edge Neto)
    positive_signal = TradingSignal(
        market_id="test-execution-market",
        outcome="Yes",
        fair_probability=0.85,
        market_probability=0.70,
        edge_gross=0.15,
        estimated_costs=0.03,
        safety_margin=0.05,
        blockers=()
    )

    print(f"Señal generada: Edge Neto: {positive_signal.edge_net:.2%}")

    # 3. Procesar señal a través del Router
    decision = await router.execute_signal(positive_signal)

    print(f"Decisión final: {decision.action}")
    print(f"Justificación: {decision.rationale}")
    
    # 4. Verificar firma criptográfica (Simulada internamente en el router)
    if decision.action == DecisionAction.PLACE_LIMIT:
        print("\n>>> FLUJO EXITOSO: La orden ha sido generada y firmada criptográficamente.")
    else:
        print("\n>>> FLUJO FALLIDO: La orden no fue procesada.")

    print("\n=== PRUEBA DE EJECUCIÓN COMPLETADA ===")

if __name__ == "__main__":
    asyncio.run(test_execution_flow())
