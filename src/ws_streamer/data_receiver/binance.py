# -*- coding: utf-8 -*-

# built ins
import asyncio
import json
import time
import os, sys

# installed
import orjson
import websockets
from dataclassy import dataclass, fields
from loguru import logger as log

# -----------------------------------------------------------------------------

this_folder = os.path.dirname(os.path.abspath(__file__))
root_folder = os.path.dirname(os.path.dirname(this_folder))
sys.path.append(root_folder + "/python")
sys.path.append(this_folder)

# -----------------------------------------------------------------------------

# user defined formula
from ws_streamer.configuration import config
from ws_streamer.messaging.telegram_bot import telegram_bot_sendtext
from ws_streamer.utilities import system_tools


def parse_dotenv(sub_account: str) -> dict:
    return config.main_dotenv(sub_account)


def get_timestamp():
    return int(time.time() * 1000)


@dataclass(unsafe_hash=True, slots=True)
class StreamingDataBinance:
    """
    https://www.binance.com/en/support/faq/detail/18c97e8ab67a4e1b824edd590cae9f16

    """

    sub_account_id: str
    client_id: str = fields
    client_secret: str = fields
    # Async Event Loop
    loop = asyncio.get_event_loop()
    ws_connection_notice_url: str = "wss://bstream.binance.com:9443/stream?"
    # Instance Variables
    websocket_client: websockets.WebSocketClientProtocol = None
    refresh_token: str = None
    refresh_token_expiry_time: int = None

    async def ws_manager(
        self,
        client_redis: object,
        exchange: str,
        redis_channels,
        queue_general: object,
    ) -> None:

        async with websockets.connect(
            self.ws_connection_notice_url,
            ping_interval=None,
            compression=None,
            close_timeout=60,
        ) as self.websocket_client:

            try:

                # timestamp = get_timestamp()

                # encoding_result = hashing(timestamp, self.client_id, self.client_secret)

                msg = {
                    "method": "SUBSCRIBE",
                    "params": ["abnormaltradingnotices"],
                    "id": 1,
                }

                while True:

                    ws_channel = ["abnormaltradingnotices"]

                    await self.ws_operation(
                        operation="SUBSCRIBE",
                        ws_channel=ws_channel,
                        source="ws",
                    )

                    while True:

                        # Receive WebSocket messages
                        message: bytes = await self.websocket_client.recv()
                        message: dict = orjson.loads(message)

                        if message:
                            
                            data = message.get("data", None)
                            
                            if data:
                                
                                data.update({"exchange": exchange})
                                data.update({"account_id": self.sub_account_id})
                          
                                # queing message to dispatcher
                                await queue_general.put(data)

            except Exception as error:

                system_tools.parse_error_message(error)

                await telegram_bot_sendtext(
                    (f"""data producer {exchange} - {error}"""),
                    "general_error",
                )


    async def ws_operation(
        self,
        operation: str,
        ws_channel: str,
        source: str = "ws",
    ) -> None:
        """ """
        sleep_time: int = 0.05

        await asyncio.sleep(sleep_time)

        id = 1

        msg: dict = {}

        if "ws" in source:
            extra_params: dict = dict(
                id=id,
                method=f"{operation}",
                params=ws_channel,
            )

            msg.update(extra_params)

            await self.websocket_client.send(json.dumps(msg))
