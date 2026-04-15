import time
from eth_account import Account
from eth_account.messages import encode_typed_data
from typing import Dict, Any
import logging

logger = logging.getLogger(__name__)

class PolymarketSigner:
    """Encargado de firmar órdenes EIP-712 para Polymarket."""

    def __init__(self, private_key: str, chain_id: int = 137): # 137 = Polygon Mainnet
        self.account = Account.from_key(private_key)
        self.chain_id = chain_id
        logger.info(f"Signer inicializado para la dirección: {self.account.address}")

    def sign_order(self, order_data: Dict[str, Any]) -> str:
        """
        Firma una orden siguiendo el estándar EIP-712 de Polymarket.
        Nota: Este es un esquema simplificado del CTF (Conditional Token Framework).
        """
        # Estructura del dominio EIP-712
        domain = {
            "name": "Polymarket CTF Exchange",
            "version": "1",
            "chainId": self.chain_id,
            "verifyingContract": "0x4bFb9e68f09497932d4375167AF1d4393049b161" # Dirección real del Exchange en Polygon
        }

        # Tipos de la orden
        types = {
            "Order": [
                {"name": "maker", "type": "address"},
                {"name": "taker", "type": "address"},
                {"name": "tokenId", "type": "uint256"},
                {"name": "makerAmount", "type": "uint256"},
                {"name": "takerAmount", "type": "uint256"},
                {"name": "side", "type": "uint8"}, # 0 para BUY, 1 para SELL
                {"name": "expiration", "type": "uint256"},
                {"name": "nonce", "type": "uint256"}
            ]
        }

        # Datos del mensaje
        message = {
            "maker": self.account.address,
            "taker": "0x0000000000000000000000000000000000000000",
            "tokenId": int(order_data["token_id"]),
            "makerAmount": int(order_data["maker_amount"]),
            "takerAmount": int(order_data["taker_amount"]),
            "side": int(order_data["side"]),
            "expiration": int(order_data.get("expiration", time.time() + 3600)),
            "nonce": int(order_data.get("nonce", 0))
        }

        # Codificar y firmar
        structured_data = {
            "types": {
                "EIP712Domain": [
                    {"name": "name", "type": "string"},
                    {"name": "version", "type": "string"},
                    {"name": "chainId", "type": "uint256"},
                    {"name": "verifyingContract", "type": "address"}
                ],
                **types
            },
            "domain": domain,
            "primaryType": "Order",
            "message": message
        }

        encoded_data = encode_typed_data(full_message=structured_data)
        signed_message = self.account.sign_message(encoded_data)
        
        return signed_message.signature.hex()
