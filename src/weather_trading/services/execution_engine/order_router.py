from typing import Dict, Any, Optional
import logging
import uuid
from weather_trading.domain.models import TradingSignal, TradeDecision, DecisionAction
from weather_trading.infrastructure.utils import utc_now
from weather_trading.services.execution_engine.signer import PolymarketSigner

logger = logging.getLogger(__name__)

class OrderRouter:
    """Gestiona la colocación de órdenes en el CLOB de Polymarket."""

    def __init__(self, signer: Optional[PolymarketSigner] = None):
        self.signer = signer

    async def execute_signal(self, signal: TradingSignal) -> TradeDecision:
        """Procesa una señal y decide la acción final."""
        
        if not signal.is_tradeable:
            return TradeDecision(
                market_id=signal.market_id,
                action=DecisionAction.NO_TRADE,
                signal=signal,
                decided_at_utc=utc_now(),
                rationale=("Edge insuficiente o bloqueadores activos",)
            )

        # 1. Definir parámetros de la orden
        # TODO: En producción, estos valores vendrían de MarketSpec (token_id)
        mock_token_id = "1234567890" 
        price = signal.market_probability
        quantity = 100 # Mock de cantidad de contratos a comprar
        
        # 2. Construir la orden
        order_payload = {
            "token_id": mock_token_id,
            "maker_amount": int(quantity * price * 1e6), # Cantidad en USDC (6 decimales)
            "taker_amount": int(quantity * 1e6), # Cantidad en Tokens
            "side": 0, # BUY
            "nonce": int(uuid.uuid4().int >> 64) # Nonce aleatorio
        }

        # 3. Firmar la orden si hay un signer disponible
        signature = None
        if self.signer:
            try:
                signature = self.signer.sign_order(order_payload)
                logger.info(f"Orden firmada con éxito para el mercado {signal.market_id}")
            except Exception as e:
                logger.error(f"Error al firmar la orden: {e}")
                return TradeDecision(
                    market_id=signal.market_id,
                    action=DecisionAction.REVIEW,
                    signal=signal,
                    decided_at_utc=utc_now(),
                    rationale=(f"Error en firma criptográfica: {e}",)
                )

        # 4. Simulación de envío al Exchange (CLOB API)
        # TODO: Implementar httpx.post a la CLOB API de Polymarket
        logger.info(f"[SIMULACIÓN] Enviando orden firmada a Polymarket para {signal.market_id}")

        return TradeDecision(
            market_id=signal.market_id,
            action=DecisionAction.PLACE_LIMIT,
            signal=signal,
            decided_at_utc=utc_now(),
            rationale=(f"Orden generada y firmada. Precio: {price:.2f}, Cantidad: {quantity}",)
        )
