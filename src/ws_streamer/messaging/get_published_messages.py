# -*- coding: utf-8 -*-

# built ins
import asyncio

# installed
import orjson


async def get_redis_message(message_byte: bytes) -> dict:
    """ """

    try:

        if message_byte and message_byte["type"] == "message":

            message_byte_data = orjson.loads(message_byte["data"])

            params = message_byte_data["params"]

            return dict(
                data=params["data"],
                channel=params["channel"],
            )

        else:

            return dict(
                data=[],
                channel=[],
            )

    except Exception as error:

        from messaging.telegram_bot import telegram_bot_sendtext

        from utilities.system_tools import parse_error_message

        parse_error_message(f"get_message redis {error}")

        await telegram_bot_sendtext(
            f"get_message redis - {error}",
            "general_error",
        )
