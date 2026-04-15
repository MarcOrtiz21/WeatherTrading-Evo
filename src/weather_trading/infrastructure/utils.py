import asyncio
import functools
import logging
from datetime import datetime, timezone
from typing import Callable, Any
from weather_trading.infrastructure.config import ConfigLoader

logger = logging.getLogger(__name__)


def utc_now() -> datetime:
    """Devuelve el instante actual en UTC como datetime naive en UTC."""
    return datetime.now(timezone.utc).replace(tzinfo=None)

def retry_async(func: Callable) -> Callable:
    """Decorador para reintentar funciones asíncronas con backoff exponencial."""
    
    @functools.wraps(func)
    async def wrapper(*args, **kwargs):
        attempts = ConfigLoader.get("weather_apis.retry_attempts", 3)
        factor = ConfigLoader.get("weather_apis.retry_backoff_factor", 2.0)
        
        last_exception = None
        for i in range(attempts):
            try:
                return await func(*args, **kwargs)
            except Exception as e:
                last_exception = e
                wait = factor ** i
                logger.warning(
                    f"Intento {i+1}/{attempts} fallido para {func.__name__}: {e}. "
                    f"Reintentando en {wait}s..."
                )
                await asyncio.sleep(wait)
        
        logger.error(f"Todos los intentos fallidos para {func.__name__}")
        raise last_exception
        
    return wrapper
