#!/usr/bin/env python
# pylint: disable=unused-argument, wrong-import-position
# This program is dedicated to the public domain under the CC0 license.
import logging
import sqlite3
from telegram import Chat, ChatMember, ChatMemberUpdated, Update
from telegram.constants import ParseMode
from telegram.ext import (
    Application,
    ChatMemberHandler,
    CommandHandler,
    ContextTypes,
    MessageHandler,
    filters,
)

logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s", level=logging.INFO
)

# set higher logging level for httpx to avoid all GET and POST requests being logged
logging.getLogger("httpx").setLevel(logging.WARNING)

logger = logging.getLogger(__name__)


# Enable logging

token = "5607537560:AAFqi0KVdQZ8U418NVZOHpZ6xhNTTuUeDF4"
application = Application.builder().token(token).build()
chat_id_ascended = -1001783119795
chat_id_pinnacle = -941116957


async def check_double(content):
    pass


async def insert_record(content):
    pass


async def meme_forward_ascended(
    update: Update, context: ContextTypes.DEFAULT_TYPE
) -> None:
    message = update.message
    reply = message.reply_to_message
    print("inside asc")

    if reply is None:
        return

    ch_m = await application.bot.get_chat_member(chat_id_ascended, message.from_user.id)
    print(ch_m)
    if ch_m.status == "administrator" or ch_m.status == "creator":
        pass
    else:
        return
    print("xd")
    await application.bot.copy_message(
        chat_id=chat_id_ascended, from_chat_id=chat_id_ascended, message_id=reply.id
    )


async def meme_forward_pinnacle(
    update: Update, context: ContextTypes.DEFAULT_TYPE
) -> None:
    message = update.message
    reply = message.reply_to_message

    if reply is None:
        return

    ch_m = await application.bot.get_chat_member(chat_id_pinnacle, message.from_user.id)

    if reply.from_user.id == message.from_user.id:
        pass
    elif ch_m.status == "administrator" or ch_m.status == "creator":
        pass
    else:
        return

    await application.bot.send_message(chat_id=chat_id_pinnacle, text=reply.text)


def main() -> None:
    """Start the bot."""
    print("inside2")
    # Create the Application and pass it your bot's token.

    # Keep track of which chats the bot is in
    # This will record the user as being in a private chat with bot.
    application.add_handler(
        MessageHandler(
            filters.Regex("/fwd") & filters.Chat(chat_id=chat_id_ascended),
            meme_forward_ascended,
        )
    )
    application.add_handler(
        MessageHandler(
            filters.Regex("/fwd") & filters.Chat(chat_id=chat_id_pinnacle),
            meme_forward_pinnacle,
        )
    )

    # Run the bot until the user presses Ctrl-C
    # We pass 'allowed_updates' handle *all* updates including `chat_member` updates
    # To reset this, simply pass `allowed_updates=[]`
    application.run_polling(allowed_updates=Update.ALL_TYPES)


if __name__ == "__main__":
    main()
