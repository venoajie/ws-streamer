# -*- coding: utf-8 -*-

import asyncio

from ws_streamer.restful_api.deribit.api_requests import get_tickers
from ws_streamer.utilities.pickling import read_data
from ws_streamer.utilities.system_tools import (
    provide_path_for_file,
)


def reading_from_pkl_data(
    end_point: str,
    currency: str,
    status: str = None,
) -> dict:
    """ """

    path: str = provide_path_for_file(end_point, currency, status)
    return read_data(path)


def combining_ticker_data(instruments_name: str) -> list:
    """_summary_
    https://blog.apify.com/python-cache-complete-guide/]
    https://medium.com/@jodielovesmaths/memoization-in-python-using-cache-36b676cb21ef
    data caching
    https://medium.com/@ryan_forrester_/python-return-statement-complete-guide-138c80bcfdc7

    Args:
        instrument_ticker (_type_): _description_

    Returns:
        _type_: _description_
    """

    result = []
    for instrument_name in instruments_name:

        result_instrument = reading_from_pkl_data("ticker", instrument_name)

        if result_instrument:
            result_instrument = result_instrument[0]

        else:
            result_instrument = get_tickers(instrument_name)
        result.append(result_instrument)

    return result


async def update_cached_ticker(
    instrument_name: str,
    ticker: list,
    data_orders: dict,
) -> None:
    """_summary_
    https://stackoverflow.com/questions/73064997/update-values-in-a-list-of-dictionaries

    Args:
        instrument_ticker (_type_): _description_

    Returns:
        _type_: _description_
    """

    instrument_ticker: list = [
        o for o in ticker if instrument_name in o["instrument_name"]
    ]

    if instrument_ticker:

        for item in data_orders:

            if (
                "stats" not in item
                and "instrument_name" not in item
                and "type" not in item
            ):
                [o for o in ticker if instrument_name in o["instrument_name"]][0][
                    item
                ] = data_orders[item]

            if "stats" in item:

                data_orders_stat = data_orders[item]

                for item in data_orders_stat:
                    [o for o in ticker if instrument_name in o["instrument_name"]][0][
                        "stats"
                    ][item] = data_orders_stat[item]

    else:
        from loguru import logger as log

        log.warning(f" {ticker}")
        log.debug(f" {instrument_name}")

        log.critical(f"instrument_ticker before {instrument_ticker}")
        # combining_order_data(currencies)
        log.debug(f"instrument_ticker after []-not ok {instrument_ticker}")


def update_cached_orders(
    orders_all: list,
    sub_account_data: dict,
    source: str = "ws",
):
    """_summary_

    source: ws/rest
    https://stackoverflow.com/questions/73064997/update-values-in-a-list-of-dictionaries

    Args:
        instrument_ticker (_type_): _description_

    Returns:
        _type_: _description_
    """

    if source == "ws":

        try:

            orders = sub_account_data["orders"]

            trades = sub_account_data["trades"]

            if orders:

                if trades:

                    for trade in trades:

                        order_id = trade["order_id"]

                        selected_order = [
                            o for o in orders_all if order_id in o["order_id"]
                        ]

                        if selected_order:

                            orders_all.remove(selected_order[0])

                if orders:

                    for order in orders:

                        order_state = order["order_state"]

                        if order_state == "cancelled" or order_state == "filled":

                            order_id = order["order_id"]

                            selected_order = [
                                o for o in orders_all if order_id in o["order_id"]
                            ]

                            if selected_order:

                                orders_all.remove(selected_order[0])

                        else:

                            orders_all.append(order)
        except:

            from loguru import logger as log

            log.debug(sub_account_data)
            try:
                order = sub_account_data[0]

            except:
                order = sub_account_data

            try:
                order_state = order["order_state"]
            except:
                order_state = order["state"]

            if order_state == "cancelled" or order_state == "filled":

                order_id = order["order_id"]

                selected_order = [o for o in orders_all if order_id in o["order_id"]]

                if selected_order:

                    orders_all.remove(selected_order[0])

            else:

                orders_all.append(order)

    if source == "rest":

        orders = sub_account_data

        trades = []


def positions_updating_cached(
    positions_cached: list,
    sub_account_data: list,
    source: str = "ws",
):
    """ """

    if source == "ws":
        positions = sub_account_data["positions"]

    if source == "rest":

        positions = sub_account_data

    if positions:

        for position in positions:

            position_instrument_name = position["instrument_name"]

            selected_position = [
                o
                for o in positions_cached
                if position_instrument_name in o["instrument_name"]
            ]

            if selected_position:

                positions_cached.remove(selected_position[0])

            positions_cached.append(position)
