# -*- coding: utf-8 -*-

# built ins
import asyncio

# installed
import uvloop
from dataclassy import dataclass
import orjson

import redis.asyncio as aioredis
import json


asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())


from ws_streamer.messaging.telegram_bot import telegram_bot_sendtext
from ws_streamer.utilities.system_tools import parse_error_message


class RedisPubSubManager:
    """

    https://medium.com/@nandagopal05/scaling-websockets-with-pub-sub-using-python-redis-fastapi-b16392ffe291
        Initializes the RedisPubSubManager.

    Args:
        host (str): Redis server host.
        port (int): Redis server port.
    """

    def __init__(self, host="localhost", port=6379):
        self.redis_host = host
        self.redis_port = port
        self.pubsub = None

    async def _get_redis_connection(self) -> aioredis.Redis:
        """
        Establishes a connection to Redis.

        Returns:
            aioredis.Redis: Redis connection object.
        """
        return aioredis.Redis(
            host=self.redis_host, port=self.redis_port, auto_close_connection_pool=False
        )

    async def connect(self) -> None:
        """
        Connects to the Redis server and initializes the pubsub client.
        """
        self.redis_connection = await self._get_redis_connection()
        self.pubsub = self.redis_connection.pubsub()

    async def _publish(self, room_id: str, message: str) -> None:
        """
        Publishes a message to a specific Redis channel.

        Args:
            room_id (str): Channel or room ID.
            message (str): Message to be published.
        """
        await self.redis_connection.publish(room_id, message)

    async def subscribe(self, room_id: str) -> aioredis.Redis:
        """
        Subscribes to a Redis channel.

        Args:
            room_id (str): Channel or room ID to subscribe to.

        Returns:
            aioredis.ChannelSubscribe: PubSub object for the subscribed channel.
        """
        await self.pubsub.subscribe(room_id)
        return self.pubsub

    async def unsubscribe(self, room_id: str) -> None:
        """
        Unsubscribes from a Redis channel.

        Args:
            room_id (str): Channel or room ID to unsubscribe from.
        """
        await self.pubsub.unsubscribe(room_id)


class Singleton(type):
    """
    https://stackoverflow.com/questions/49398590/correct-way-of-using-redis-connection-pool-in-python
    """

    _instances = {}

    def __call__(cls, *args, **kwargs):
        if cls not in cls._instances:
            cls._instances[cls] = super(Singleton, cls).__call__(*args, **kwargs)
        return cls._instances[cls]


@dataclass(unsafe_hash=True, slots=True)
class RedisClient(metaclass=Singleton):

    host: str = "redis://localhost"
    port: int = 6379
    db: int = 0
    protocol: int = 3
    pool: object = None

    def __post_init__(self):
        return redis.Redis.from_pool(
            redis.ConnectionPool.from_url(
                self.host,
                port=self.port,
                db=self.db,
                protocol=self.protocol,
            )
        )

    def conn(self):
        return self.pool


async def saving_and_publishing_result(
    client_redis: object,
    channel: str,
    keys: str,
    cached_data: list,
    message: dict,
) -> None:
    """ """

    try:
        # updating cached data
        if cached_data:
            await saving_result(
                client_redis,
                channel,
                keys,
                cached_data,
            )

        # publishing message
        await publishing_result(
            client_redis,
            channel,
            message,
        )

    except Exception as error:

        parse_error_message(error)

        await telegram_bot_sendtext(
            f"redis saving and publishing result - {error}",
            "general_error",
        )


async def publishing_result(
    client_redis: object,
    channel: str,
    message: dict,
) -> None:
    """ """

    try:

        # publishing message
        await client_redis.publish(
            channel,
            orjson.dumps(message),
        )

    except Exception as error:

        parse_error_message(error)

        await telegram_bot_sendtext(
            f"redis publishing result - {error}",
            "general_error",
        )


async def saving_result(
    client_redis: object,
    channel: str,
    keys: str,
    cached_data: list,
) -> None:
    """ """

    try:

        await client_redis.hset(
            keys,
            channel,
            orjson.dumps(cached_data),
        )

    except Exception as error:

        parse_error_message(error)

        await telegram_bot_sendtext(
            f"redis saving result - {error}",
            "general_error",
        )


async def querying_data(
    client_redis: object,
    channel: str,
    keys: str,
) -> None:
    """ """

    try:

        return await client_redis.hget(
            keys,
            channel,
        )

    except Exception as error:

        parse_error_message(error)

        await telegram_bot_sendtext(
            f"redis qurying result - {error}",
            "general_error",
        )
