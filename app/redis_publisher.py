import json
import redis.asyncio as redis
from app.config import REDIS_HOST, REDIS_PORT, REDIS_DB

class RedisPublisher:

    def __init__(self):
        self.redis = redis.Redis(
            host=REDIS_HOST,
            port=REDIS_PORT,
            db=REDIS_DB,
            decode_responses=True
        )

    async def publish_price(self, channel: str, data: dict):
        await self.redis.publish(channel, json.dumps(data))
