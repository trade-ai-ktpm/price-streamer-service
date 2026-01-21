import json
import redis.asyncio as redis
from config import REDIS_HOST, REDIS_PORT, REDIS_DB, REDIS_PASSWORD

class RedisPublisher:

    def __init__(self):
        self.redis = redis.Redis(
            host=REDIS_HOST,
            port=REDIS_PORT,
            db=REDIS_DB,
            password=REDIS_PASSWORD,
            decode_responses=True
        )

    async def publish_price(self, channel: str, data: dict):
        await self.redis.publish(channel, json.dumps(data))
