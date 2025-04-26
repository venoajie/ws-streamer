# built ins
import asyncio

# import orjson
import httpx

async def private_connection(
    endpoint: str,
    client_id: str,
    bot_chatID: str,
    connection_url: str = "https://api.telegram.org/bot",
) -> None:

    async with httpx.AsyncClient() as session:

        respons = await session.get(connection_url + endpoint)

        return respons.json()


async def telegram_bot_sendtext(
    bot_token: str,
    bot_chatID: str,
    bot_message: str, 
    purpose: str = "general_error",
) -> str:
    """
    # simple telegram
    #https://stackoverflow.com/questions/32423837/telegram-bot-how-to-get-a-group-chat-id
    """

    connection_url = "https://api.telegram.org/bot"

    endpoint = (
        bot_token
        + ("/sendMessage?chat_id=")
        + bot_chatID
        + ("&parse_mode=HTML&text=")
        + str(bot_message)
    )

    return await private_connection(
        endpoint=endpoint,
        client_id=bot_token,
        bot_chatID=bot_chatID,
        connection_url=connection_url,
        )
