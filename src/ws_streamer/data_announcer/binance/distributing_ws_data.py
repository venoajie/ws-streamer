# -*- coding: utf-8 -*-

# built ins
import asyncio

from ws_streamer.db_management import redis_client
from ws_streamer.messaging import telegram_bot as tlgrm
from ws_streamer.utilities import string_modification as str_mod, system_tools


async def caching_distributing_data(
    client_redis: object,
    redis_channels: list,
    queue_general: object,
) -> None:

    """ """

    try:

        # preparing redis connection
        pubsub: object = client_redis.pubsub()

        abnormal_trading_notices_channel: str = redis_channels[
            "abnormal_trading_notices"
        ]

        result: dict = str_mod.message_template()

        while True:

            message_params: str = await queue_general.get()

            async with client_redis.pipeline() as pipe:

                try:

                    message_channel: str = message_params.get("stream")

                    if message_channel:

                        data: dict = message_params["data"]

                        pub_message = dict(
                            data=data,
                        )
                        
                        if "abnormaltradingnotices" in message_channel:
                            
                            data: dict = message_params["data"]

                            pub_message = dict(
                                data=data,
                            )

                            await abnormal_trading_notices_in_message_channel(
                                pipe,
                                abnormal_trading_notices_channel,
                                pub_message,
                                result,
                            )

                        await pipe.execute()

                except Exception as error:

                    system_tools.parse_error_message(error)

    except Exception as error:

        system_tools.parse_error_message(error)

        await tlgrm.telegram_bot_sendtext(
            f"saving result {error}",
            "general_error",
        )


async def abnormal_trading_notices_in_message_channel(
    pipe: object,
    abnormal_trading_notices_channel: str,
    pub_message: list,
    result: dict,
) -> None:

    result["params"].update({"channel": abnormal_trading_notices_channel})
    result["params"].update(pub_message)

    await redis_client.publishing_result(
        pipe,
        result,
    )
