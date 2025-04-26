"""
why aiohttp over httpx?
    - Our module is fully using asynchronous which is aiohttp spesialization 
    - has more mature asyncio support than httpx
    - aiohttp is more suitable for applications that require high concurrency and low latency, such as web scraping or real-time data processing.

references:
    - https://github.com/encode/httpx/issues/3215#issuecomment-2157885121
    - https://github.com/encode/httpx/discussions/3100
    - https://brightdata.com/blog/web-data/requests-vs-httpx-vs-aiohttp


"""

# built ins
import asyncio
from typing import Dict

# installed
import aiohttp
import httpx
from aiohttp.helpers import BasicAuth
from dataclassy import dataclass
from loguru import logger as log

# user defined formula
from ws_streamer.messaging import telegram_bot as tlgrm
from ws_streamer.utilities import string_modification as str_mod, time_modification as time_mod


async def get_connected(
    endpoint: str,
    connection_url: str,
    client_id: str= None,
    client_secret: str= None,
    params: str = None,
    ) -> None:
    
    async with aiohttp.ClientSession() as session:
        
        connection_endpoint = connection_url + endpoint

        if client_id:
            
            if "telegram" in connection_url: 
                
                response = await session.get(connection_url + endpoint)
            
            if "deribit" in connection_url: 

                id = str_mod.id_numbering(
                    endpoint,
                    endpoint,
                )

                payload: Dict = {
                    "jsonrpc": "2.0",
                    "id": id,
                    "method": f"{endpoint}",
                    "params": params,
                }
                
                async with session.post(
                    connection_endpoint,
                    auth=BasicAuth(client_id, client_secret),
                    json=payload,
                ) as response:

                    # RESToverHTTP Status Code
                    status_code: int = response.status

                    # RESToverHTTP Response Content
                    response: Dict = await response.json()

        else:
            
                async with session.get(connection_endpoint) as response:

                    # RESToverHTTP Response Content
                    response: Dict = await response.json()

        return response
        

async def private_connection(
    endpoint: str,
    client_id: str,
    client_secret: str,
    connection_url: str = "https://www.deribit.com/api/v2/",
    params: str = {},
) -> None:

    id = str_mod.id_numbering(
        endpoint,
        endpoint,
    )

    payload: Dict = {
        "jsonrpc": "2.0",
        "id": id,
        "method": f"{endpoint}",
        "params": params,
    }
    
    async with aiohttp.ClientSession() as session:
        async with session.post(
            connection_url + endpoint,
            auth=BasicAuth(client_id, client_secret),
            json=payload,
        ) as response:

            # RESToverHTTP Status Code
            status_code: int = response.status

            # RESToverHTTP Response Content
            response: Dict = await response.json()

        return response


async def public_connection(
    endpoint: str,
    connection_url: str = "https://www.deribit.com/api/v2/",
) -> None:

    async with aiohttp.ClientSession() as session:
        async with session.get(connection_url + endpoint) as response:

            # RESToverHTTP Response Content
            response: Dict = await response.json()

        return response


async def get_currencies() -> list:
    # Set endpoint
    endpoint: str = f"public/get_currencies?"

    return await public_connection(endpoint=endpoint)


async def get_server_time() -> int:
    """
    Returning server time
    """
    # Set endpoint
    endpoint: str = "public/get_time?"

    # Get result
    result = await public_connection(endpoint=endpoint)

    return result


async def send_requests_to_url(end_point: str) -> list:

    try:

        async with httpx.AsyncClient() as client:
            result = await client.get(
                end_point,
                follow_redirects=True,
            )

        return result.json()["result"]

    except Exception as error:

        await tlgrm.telegram_bot_sendtext(
            f"error send_requests_to_url - {error} {end_point}",
            "general_error",
        )


async def get_instruments(currency) -> list:
    # Set endpoint
    endpoint: str = f"public/get_instruments?currency={currency.upper()}"

    return await public_connection(endpoint=endpoint)


def get_tickers(instrument_name: str) -> list:
    # Set endpoint

    end_point = (
        f"https://deribit.com/api/v2/public/ticker?instrument_name={instrument_name}"
    )

    with httpx.Client() as client:
        result = client.get(end_point, follow_redirects=True).json()["result"]

    return result


async def async_get_tickers(instrument_name: str) -> list:
    # Set endpoint

    end_point = (
        f"https://deribit.com/api/v2/public/ticker?instrument_name={instrument_name}"
    )

    return await send_requests_to_url(end_point)


def ohlc_end_point(
    instrument_name: str,
    resolution: int,
    qty_or_start_time_stamp: int,
    provided_end_timestamp: int = None,
    qty_as_start_time_stamp: bool = False,
) -> str:

    url = f"https://deribit.com/api/v2/public/get_tradingview_chart_data?"

    now_unix = time_mod.get_now_unix_time()

    # start timestamp is provided
    start_timestamp = qty_or_start_time_stamp

    # recalculate start timestamp using qty as basis point
    if qty_as_start_time_stamp:
        start_timestamp = now_unix - (60000 * resolution) * qty_as_start_time_stamp

    if provided_end_timestamp:
        end_timestamp = provided_end_timestamp
    else:
        end_timestamp = now_unix

    return f"{url}end_timestamp={end_timestamp}&instrument_name={instrument_name}&resolution={resolution}&start_timestamp={start_timestamp}"


def get_end_point_based_on_side(side: str) -> str:

    if side == "buy":
        return "private/buy"

    if side == "sell":
        return "private/sell"


async def get_ohlc_data(
    instrument_name: str,
    resolution: int,
    qty_or_start_time_stamp: int,
    provided_end_timestamp: bool = None,
    qty_as_start_time_stamp: bool = False,
) -> list:

    # Set endpoint
    end_point = ohlc_end_point(
        instrument_name,
        resolution,
        qty_or_start_time_stamp,
        provided_end_timestamp,
        qty_as_start_time_stamp,
    )

    result = await send_requests_to_url(end_point)

    return str_mod.transform_nested_dict_to_list_ohlc(result)


@dataclass(unsafe_hash=True, slots=True)
class SendApiRequest:
    """ """

    sub_account_id: str
    client_id: str
    client_secret: str
    
    async def send_order(
        self,
        side: str,
        instrument,
        amount,
        label: str = None,
        price: float = None,
        type: str = "limit",
        otoco_config: list = None,
        linked_order_type: str = None,
        trigger_price: float = None,
        trigger: str = "last_price",
        time_in_force: str = "fill_or_kill",
        reduce_only: bool = False,
        post_only: bool = True,
        reject_post_only: bool = False,
    ) -> None:

        params = {}

        params.update({"instrument_name": instrument})
        params.update({"amount": amount})
        params.update({"label": label})
        params.update({"instrument_name": instrument})
        params.update({"type": type})

        if trigger_price is not None:

            params.update({"trigger": trigger})
            params.update({"trigger_price": trigger_price})
            params.update({"reduce_only": reduce_only})

        if "market" not in type:
            params.update({"price": price})
            params.update({"post_only": post_only})
            params.update({"reject_post_only": reject_post_only})

        if otoco_config:
            params.update({"otoco_config": otoco_config})

            if linked_order_type is not None:
                params.update({"linked_order_type": linked_order_type})
            else:
                params.update({"linked_order_type": "one_triggers_other"})

            params.update({"trigger_fill_condition": "incremental"})

            log.debug(f"params otoco_config {params}")

        result = None

        if side is not None:

            endpoint: str = get_end_point_based_on_side(side)

            result = await private_connection(
            endpoint,
            self.client_id,
            self.client_secret,        
            params,
            )

        return result

    async def get_open_orders(
        self,
        kind: str,
        type: str,
    ) -> list:

        # Set endpoint
        endpoint: str = "private/get_open_orders"

        params = {"kind": kind, "type": type}

        result_open_order = await private_connection(
            endpoint,
            self.client_id,
            self.client_secret,        
            params,
            )

        return result_open_order["result"]

    async def send_limit_order(
        self,
        params: dict,
    ) -> None:
        """ """

        # basic params
        log.info(f"params {params}")
        side = params["side"]
        instrument = params["instrument_name"]
        label_numbered = params["label"]
        size = params["size"]
        type = params["type"]
        limit_prc = params["entry_price"]

        try:
            otoco_config = params["otoco_config"]
        except:
            otoco_config = None

        try:
            linked_order_type = params["linked_order_type"]

        except:
            linked_order_type = None

        order_result = None

        if side != None:

            if type == "limit":  # limit has various state

                order_result = await self.send_order(
                    side,
                    instrument,
                    size,
                    label_numbered,
                    limit_prc,
                    type,
                    otoco_config,
                )

            else:

                trigger_price = params["trigger_price"]
                trigger = params["trigger"]

                order_result = await self.send_order(
                    side,
                    instrument,
                    size,
                    label_numbered,
                    limit_prc,
                    type,
                    otoco_config,
                    linked_order_type,
                    trigger_price,
                    trigger,
                )

        # log.warning(f"order_result {order_result}")

        if order_result != None and (
            "error" in order_result or "message" in order_result
        ):

            error = order_result["error"]
            message = error["message"]

            try:
                data = error["data"]
            except:
                data = message

            await tlgrm.telegram_bot_sendtext(
                f"message: {message}, \
                                         data: {data}, \
                                         (params: {params}"
            )

        log.warning(f"order_result {order_result}")
        return order_result

    async def get_subaccounts(self) -> list:
        """
        portfolio
        """
        # Set endpoint
        endpoint: str = "private/get_subaccounts"

        params = {"with_portfolio": True}

        result_sub_account = await private_connection(
            endpoint,
            self.client_id,
            self.client_secret,        
            params,
            )

        return result_sub_account["result"]

    async def get_subaccounts_details(
        self,
        currency: str,
    ) -> list:

        """

        currency= BTC/ETH
        example= https://www.deribit.com/api/v2/private/get_subaccounts_details?currency=BTC&with_open_orders=true


        result_sub_account["result"]

        Returns example:

         [
             {
                'positions': [
                     {
                         'estimated_liquidation_price': None,
                         'size_currency': -0.031537551,
                         'total_profit_loss': -0.005871738,
                         'realized_profit_loss': 0.0,
                         'floating_profit_loss': -0.002906191,
                         'leverage': 25,
                         'average_price': 74847.72,
                         'delta': -0.031537551,
                         'mark_price': 88783.05,
                         'settlement_price': 81291.98,
                         'instrument_name': 'BTC-15NOV24',
                         'index_price': 88627.96,
                         'direction': 'sell',
                         'open_orders_margin': 0.0,
                         'initial_margin': 0.001261552,
                         'maintenance_margin': 0.000630801,
                         'kind': 'future',
                         'size': -2800.0
                     },
                     {
                         'estimated_liquidation_price': None,
                         'size_currency': -0.006702271,
                         'total_profit_loss': -0.001912148,
                         'realized_profit_loss': 0.0,
                         'floating_profit_loss': -0.000624473,
                         'leverage': 25,
                         'average_price': 69650.67,
                         'delta': -0.006702271,
                         'mark_price': 89521.9,
                         'settlement_price': 81891.77,
                         'instrument_name': 'BTC-29NOV24',
                         'index_price': 88627.96,
                         'direction': 'sell',
                         'open_orders_margin': 0.0,
                         'initial_margin': 0.000268093,
                         'maintenance_margin': 0.000134048,
                         'kind': 'future',
                         'size': -600.0
                     },
                     {
                         'estimated_liquidation_price': None,
                         'size_currency': 0.036869785,
                         'realized_funding': -2.372e-05,
                         'total_profit_loss': 0.005782196,
                         'realized_profit_loss': 0.000591453,
                         'floating_profit_loss': 0.002789786,
                         'leverage': 50,
                         'average_price': 76667.01,
                         'delta': 0.036869785,
                         'interest_value': 0.2079087278497569,
                         'mark_price': 88690.51,
                         'settlement_price': 81217.47,
                         'instrument_name': 'BTC-PERPETUAL',
                         'index_price': 88627.96,
                         'direction': 'buy',
                         'open_orders_margin': 3.489e-06,
                         'initial_margin': 0.000737464,
                         'maintenance_margin': 0.000368766,
                         'kind': 'future',
                         'size': 3270.0
                     }
                     ],

                'open_orders': [
                     {
                         'is_liquidation': False,
                         'risk_reducing': False,
                         'order_type': 'limit',
                         'creation_timestamp': 1731390729846,
                         'order_state': 'open',
                         'reject_post_only': False,
                         'contracts': 1.0,
                         'average_price': 0.0,
                         'reduce_only': False,
                         'post_only': True,
                         'last_update_timestamp': 1731390729846,
                         'filled_amount': 0.0,
                         'replaced': False,
                         'mmp': False,
                         'web': False,
                         'api': True,
                         'instrument_name': 'BTC-PERPETUAL',
                         'amount': 10.0,
                         'order_id': '80616245864',
                         'max_show': 10.0,
                         'time_in_force': 'good_til_cancelled',
                         'direction': 'buy',
                         'price': 88569.5,
                         'label': 'hedgingSpot-closed-1731387973670'
                     }
                     ],

                'uid': 148510
                }
        ]

        """

        # Set endpoint
        endpoint: str = "private/get_subaccounts_details"

        params = {
            "currency": currency,
            "with_open_orders": True,
        }

        result_sub_account = await private_connection(
            endpoint,
            self.client_id,
            self.client_secret,  
                        params,
      
            )

        return result_sub_account["result"]

    async def get_user_trades_by_currency(
        self,
        currency: str,
        count: int = 1000,
    ):

        # Set endpoint
        endpoint: str = f"private/get_user_trades_by_currency"

        params = {
            "currency": currency.upper(),
            "kind": "any",
            "count": count,
        }

        user_trades = await private_connection(
            endpoint,
            self.client_id,
            self.client_secret,       
                        params,
 
        )
        
        return [] if user_trades == [] else user_trades["result"]["trades"]

    async def get_user_trades_by_instrument_and_time(
        self,
        instrument_name: str,
        start_timestamp: int,
        count: int = 1000,
    ) -> list:

        # Set endpoint
        endpoint: str = f"private/get_user_trades_by_instrument_and_time"

        now_unix = time_mod.get_now_unix_time()

        params = {
            "count": count,
            "end_timestamp": now_unix,
            "instrument_name": instrument_name,
            "start_timestamp": start_timestamp,
        }

        user_trades = await private_connection(
            endpoint,
            self.client_id,
            self.client_secret,   
                        params,
     
            )

        return [] if user_trades == [] else user_trades["result"]["trades"]

    async def get_cancel_order_all(self):

        # Set endpoint
        endpoint: str = "private/cancel_all"

        params = {"detailed": False}

        result = await private_connection(
            endpoint,
            self.client_id,
            self.client_secret,        
            params,
            )

        return result

    async def get_transaction_log(
        self,
        currency: str,
        start_timestamp: int,
        count: int = 1000,
        query: str = "trade",
    ) -> list:

        """
        query:
            trade, maker, taker, open, close, liquidation, buy, sell,
            withdrawal, delivery, settlement, deposit, transfer,
            option, future, correction, block_trade, swap

        """

        now_unix = time_mod.get_now_unix_time()

        # Set endpoint
        endpoint: str = f"private/get_transaction_log"
        params = {
            "count": count,
            "currency": currency.upper(),
            "end_timestamp": now_unix,
            "query": query,
            "start_timestamp": start_timestamp,
        }

        result_transaction_log_to_result = await private_connection(
            endpoint,
            self.client_id,
            self.client_secret,        
            params,
            )

        try:
            result = result_transaction_log_to_result["result"]

            return [] if not result else result["logs"]

        except:

            error = result_transaction_log_to_result["error"]
            message = error["message"]
            await tlgrm.telegram_bot_sendtext(
                f"transaction_log message: {message}, (params: {params})"
            )

    async def get_cancel_order_byOrderId(
        self,
        order_id: str,
    ) -> None:
        # Set endpoint
        endpoint: str = "private/cancel"

        params = {"order_id": order_id}

        result = await private_connection(
            endpoint,
            self.client_id,
            self.client_secret,        
            params,
            )

        return result


def get_api_end_point(
    endpoint: str,
    parameters: dict = None,
) -> dict:

    private_endpoint = f"private/{endpoint}"

    params = {}
    params.update({"jsonrpc": "2.0"})
    params.update({"method": private_endpoint})
    if endpoint == "get_subaccounts":
        params.update({"params": {"with_portfolio": True}})

    if endpoint == "get_open_orders":
        end_point_params = dict(kind=parameters["kind"], type=parameters["type"])

        params.update({"params": end_point_params})

    return params


async def get_cancel_order_byOrderId(
    private_connection: object,
    order_id: str,
) -> None:
    # Set endpoint
    endpoint: str = "private/cancel"

    params = {"order_id": order_id}

    result = await private_connection(
        endpoint=endpoint,
        params=params,
    )
    return result
