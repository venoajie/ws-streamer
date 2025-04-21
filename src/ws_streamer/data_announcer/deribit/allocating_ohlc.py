#!/usr/bin/python3
# -*- coding: utf-8 -*-


# built ins
import asyncio
import orjson

from ws_streamer.db_management.redis_client import publishing_result
from ws_streamer.db_management.sqlite_management import (
    executing_query_with_return,
    insert_tables,
    querying_arithmetic_operator,
    update_status_data,
)
from ws_streamer.messaging.telegram_bot import telegram_bot_sendtext
from ws_streamer.restful_api.deribit.api_requests import get_ohlc_data
from ws_streamer.utilities.string_modification import remove_apostrophes_from_json
from ws_streamer.utilities.system_tools import parse_error_message
from loguru import logger as log


async def last_tick_fr_sqlite(last_tick_query_ohlc1: str) -> int:
    """ """
    last_tick = await executing_query_with_return(last_tick_query_ohlc1)

    return last_tick[0]["MAX (tick)"]


async def updating_ohlc(
    client_redis: object,
    redis_channels: list,
) -> None:
    """ """

    try:

        # connecting to redis pubsub
        pubsub: object = client_redis.pubsub()

        chart_channel: str = redis_channels["chart_update"]
        chart_low_high_tick_channel: str = redis_channels["chart_low_high_tick"]

        # prepare channels placeholders
        channels = [
            chart_channel,
        ]

        # subscribe to channels
        [await pubsub.subscribe(o) for o in channels]

        WHERE_FILTER_TICK: str = "tick"

        is_updated = True

        start_timestamp = 0

        while is_updated:

            try:

                message_byte = await pubsub.get_message()

                #                log.info(f"message_byte {message_byte}")

                if message_byte and message_byte["type"] == "message":

                    message_byte_data = orjson.loads(message_byte["data"])

                    message_channel = message_byte["channel"]

                    if chart_channel in message_channel:

                        data = message_byte_data["data"]

                        instrument_name = message_byte_data["instrument_name"]

                        currency = message_byte_data["currency"]

                        resolution = message_byte_data["resolution"]

                        end_timestamp = data["tick"]

                        table_ohlc = f"ohlc{resolution}_{currency.lower()}_perp_json"

                        last_tick_query_ohlc_resolution: str = (
                            querying_arithmetic_operator(
                                WHERE_FILTER_TICK,
                                "MAX",
                                table_ohlc,
                            )
                        )

                        # need cached
                        start_timestamp: int = await last_tick_fr_sqlite(
                            last_tick_query_ohlc_resolution
                        )

                        delta_time = end_timestamp - start_timestamp

                        pub_message = dict(
                            instrument_name=instrument_name,
                            currency=currency,
                            resolution=resolution,
                        )

                        if delta_time == 0:

                            # refilling current ohlc table with updated data
                            await update_status_data(
                                table_ohlc,
                                "data",
                                end_timestamp,
                                WHERE_FILTER_TICK,
                                data,
                                "is",
                            )

                            if resolution != 1:

                                table_ohlc = (
                                    f"ohlc{resolution}_{currency.lower()}_perp_json"
                                )

                                ohlc_query = f"SELECT data FROM {table_ohlc} WHERE tick = {end_timestamp}"

                                result_from_sqlite = await executing_query_with_return(
                                    ohlc_query
                                )

                                high_from_ws = data["high"]
                                low_from_ws = data["low"]

                                ohlc_from_sqlite = remove_apostrophes_from_json(
                                    o["data"] for o in result_from_sqlite
                                )[0]

                                high_from_db = ohlc_from_sqlite["high"]
                                low_from_db = ohlc_from_sqlite["low"]

                                if (
                                    high_from_ws > high_from_db
                                    or low_from_ws < low_from_db
                                ):

                                    # log.warning(f"ohlc_from_sqlite {ohlc_from_sqlite}")
                                    # log.info(f"resolution {resolution}  data {data}")

                                    # log.warning(
                                    #    f"high_from_ws > high_from_db or low_from_ws < low_from_db {high_from_ws > high_from_db or low_from_ws < low_from_db}"
                                    # )

                                    await publishing_result(
                                        client_redis,
                                        chart_low_high_tick_channel,
                                        pub_message,
                                    )

                                # is_updated = False
                                # break

                        else:

                            # catch up data through FIX
                            result_all = await get_ohlc_data(
                                instrument_name,
                                resolution,
                                start_timestamp,
                                end_timestamp,
                                False,
                            )

                            await publishing_result(
                                client_redis,
                                chart_low_high_tick_channel,
                                pub_message,
                            )

                            for result in result_all:

                                await insert_tables(
                                    table_ohlc,
                                    result,
                                )

                            # is_updated = False
                            # break

            except Exception as error:

                parse_error_message(error)

                await telegram_bot_sendtext(
                    f"updating ticker - {error}",
                    "general_error",
                )

                continue

            finally:
                await asyncio.sleep(0.001)

    except Exception as error:

        await telegram_bot_sendtext(
            f"updating_ohlc - {error}",
            "general_error",
        )

        parse_error_message(error)


async def inserting_open_interest(
    currency,
    WHERE_FILTER_TICK,
    TABLE_OHLC1,
    data_orders,
) -> None:
    """ """
    try:

        if (
            currency_inline_with_database_address(currency, TABLE_OHLC1)
            and "open_interest" in data_orders
        ):

            open_interest = data_orders["open_interest"]

            last_tick_query_ohlc1: str = querying_arithmetic_operator(
                "tick", "MAX", TABLE_OHLC1
            )

            last_tick1_fr_sqlite: int = await last_tick_fr_sqlite(last_tick_query_ohlc1)

            await update_status_data(
                TABLE_OHLC1,
                "open_interest",
                last_tick1_fr_sqlite,
                WHERE_FILTER_TICK,
                open_interest,
                "is",
            )

    except Exception as error:

        await telegram_bot_sendtext(
            f"error inserting open interest - {error}",
            "general_error",
        )

        parse_error_message(error)


def currency_inline_with_database_address(
    currency: str,
    database_address: str,
) -> bool:

    return currency.lower() in str(database_address)
