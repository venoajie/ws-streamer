# -*- coding: utf-8 -*-


def extract_currency_from_text(words: str) -> str:
    """

    some variables:
    chart.trades.BTC-PERPETUAL.1
    incremental_ticker.BTC-4OCT24
    """

    if "." in words:
        filter1 = (words.partition(".")[2]).lower()

        if "." in filter1:
            filter1 = (filter1.partition(".")[2]).lower()

            if "chart.trades" in words:
                filter1 = (words.partition(".")[2]).lower()

            if "." in filter1:
                filter1 = (filter1.partition(".")[2]).lower()

                if "." in filter1:
                    filter1 = (filter1.partition(".")[0]).lower()

    else:
        filter1 = (words.partition(".")[0]).lower()

    return (filter1.partition("-")[0]).lower()


def id_numbering(
    operation: str,
    ws_channel: str,
) -> str:
    """

    id convention

    method
    subscription    3
    get             4

    auth
    public	        1
    private	        2

    instruments
    all             0
    btc             1
    eth             2

    subscription
    --------------  method      auth    seq    inst
    portfolio	        3	    1	    01
    user_order	        3	    1	    02
    my_trade	        3	    1	    03
    order_book	        3	    2	    04
    trade	            3	    1	    05
    index	            3	    1	    06
    announcement	    3	    1	    07

    get
    --------------
    currencies	        4	    2	    01
    instruments	        4	    2	    02
    positions	        4	    1	    03

    """
    id_auth = 1
    if "user" in ws_channel:
        id_auth = 9

    id_method = 0
    if "subscribe" in operation:
        id_method = 3
    if "get" in operation:
        id_method = 4
    id_item = 0
    if "book" in ws_channel:
        id_item = 1
    if "user" in ws_channel:
        id_item = 2
    if "chart" in ws_channel:
        id_item = 3
    if "index" in ws_channel:
        id_item = 4
    if "order" in ws_channel:
        id_item = 5
    if "position" in ws_channel:
        id_item = 6
    id_instrument = 0
    if "BTC" or "btc" in ws_channel:
        id_instrument = 1
    if "ETH" or "eth" in ws_channel:
        id_instrument = 2
    return int(f"{id_auth}{id_method}{id_item}{id_instrument}")


def message_template() -> str:

    """ """

    result = {}
    result.update({"params": {}})
    result.update({"method": "subscription"})
    result["params"].update({"data": None})
    result["params"].update({"channel": None})
    result["params"].update({"stream": None})

    return result


def extract_integers_from_text(words: list) -> int:
    """
    Extracting integers from label text. More general than get integer in parsing label function
    """

    words_to_str = str(
        words
    )  # ensuring if integer used as argument, will be returned as itself

    return int("".join([o for o in words_to_str if o.isdigit()]))
