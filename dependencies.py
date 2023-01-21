import asyncio
import aioredis
from typing import AsyncIterator
from aioredis import create_redis_pool, Redis


async def init_redis_pool(host: str, password: str) -> AsyncIterator[Redis]:
    pool = aioredis.ConnectionPool.from_url(
        "redis://localhost", decode_responses=True
    )
    yield pool
    pool.close()
    await pool.wait_closed()