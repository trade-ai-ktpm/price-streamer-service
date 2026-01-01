from pydantic import BaseModel

class PriceEvent(BaseModel):
    symbol: str
    price: float
    timestamp: int
    source: str = "binance"
