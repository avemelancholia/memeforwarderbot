#!/usr/bin/env python
# This program is dedicated to the public domain under the CC0 license.
import yaml
import logging
import sqlite3
import hashlib
from sql_queries import get_sql_query
import pendulum as pdl
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

with open("config.yaml", "r") as f:
    data = yaml.load(f, yaml.loader.SafeLoader)

token = data["token"]
limit_ids = 300

application = Application.builder().token(token).build()


async def user_can_forward(message, chat_type):
    can_forward = False
    chat_id = chats[chat_type]
    user_id = message.from_user.id
    if message.reply_to_message is None:
        return can_forward
    reply_user_id = message.reply_to_message.from_user.id
    chat_member = await application.bot.get_chat_member(chat_id, user_id)

    role_ok = (chat_member.status == "administrator") or (
        chat_member.status == "creator"
    )
    member_id_ok = reply_user_id == user_id

    if (chat_type == "ascended") and role_ok:
        can_forward = True
    elif (chat_type == "pinnacle") and (role_ok or member_id_ok):
        can_forward = True
    return can_forward


async def insert_meme_in_db_if_ok(message, chat_type):
    can_forward = False
    if message.reply_to_message is None:
        return can_forward
    reply = message.reply_to_message

    animation_present = reply.animation is not None
    video_present = reply.video is not None
    photo_present = len(reply.photo) > 0
    audio_present = reply.audio is not None
    text_present = reply.text is not None
    caption_present = reply.caption is not None

    content_present = (
        animation_present
        or video_present
        or photo_present
        or audio_present
        or text_present
    )

    if not content_present:
        return can_forward

    hash_str = ""
    if animation_present:
        hash_str += reply.animation.file_unique_id
    if video_present:
        hash_str += reply.video.file_unique_id
    if photo_present:
        hash_str += reply.photo[0].file_unique_id
    if audio_present:
        hash_str += reply.audio.file_unique_id
    if text_present:
        hash_str += reply.text
    if caption_present:
        hash_str += reply.caption

    hash_str = hash_str.encode("utf8")
    hash_str = hashlib.sha256(hash_str).hexdigest()
    datetime = str(pdl.now().int_timestamp)
    args = {"table": f"{chat_type}_meme"}

    con = sqlite3.connect("meme_forwarder.db")
    cur = con.cursor()
    res = cur.execute(get_sql_query("select_hashed", args) + f"'{hash_str}'")
    if res.fetchone() is None:
        can_forward = True
    id_to_insert = cur.execute(get_sql_query(
        "get_ids_by_ids", args)).fetchone()

    if id_to_insert is None:
        id_to_insert = 0
    else:
        id_to_insert = id_to_insert[0] + 1

    cur.execute(get_sql_query("insert", args),
                (id_to_insert, datetime, hash_str))
    con.commit()
    con.close()

    return can_forward


def create_tables_if_missing(chats):
    tables = [f"{key}_meme" for key in chats.keys()]
    con = sqlite3.connect("meme_forwarder.db")
    cur = con.cursor()
    for table in tables:
        print(table)
        args = {"table": table}
        cur.execute(get_sql_query("create_memes", args))
    con.close()


def manage_db_rows(chat_type):
    args = {"table": f"{chat_type}_meme"}
    con = sqlite3.connect("meme_forwarder.db")
    cur = con.cursor()
    ids = cur.execute(get_sql_query("get_ids_by_timestamp", args)).fetchall()

    if len(ids) - 1 > limit_ids:
        cur.executemany(get_sql_query("delete_ids", args), ids[-limit_ids:])

    con.commit()
    con.close()


async def meme_forward_ascended(
    update: Update, context: ContextTypes.DEFAULT_TYPE
) -> None:
    chat_type = "ascended"
    message = update.message
    reply = message.reply_to_message

    if not await user_can_forward(message, chat_type):
        return
    if not await insert_meme_in_db_if_ok(message, chat_type):
        return

    manage_db_rows(chat_type)

    await application.bot.copy_message(
        chat_id=chats["pinnacle"], from_chat_id=chats["ascended"], message_id=reply.id
    )


async def meme_forward_pinnacle(
    update: Update, context: ContextTypes.DEFAULT_TYPE
) -> None:
    chat_type = "pinnacle"
    message = update.message
    reply = message.reply_to_message

    if not await user_can_forward(message, chat_type):
        return
    if not await insert_meme_in_db_if_ok(message, chat_type):
        return

    manage_db_rows(chat_type)

    await application.bot.copy_message(
        chat_id=chats["channel"], from_chat_id=chats["pinnacle"], message_id=reply.id
    )


def main() -> None:
    """Start the bot."""
    chats = {
        "ascended": data["ascended"],
        "pinnacle": data["pinnacle"],
        "channel": data["channel"],
    }
    create_tables_if_missing(chats)
    application.add_handler(
        MessageHandler(
            filters.Regex("/fwd") & filters.Chat(chat_id=chats["ascended"]),
            meme_forward_ascended,
        )
    )
    application.add_handler(
        MessageHandler(
            filters.Regex("/fwd") & filters.Chat(chat_id=chats["pinnacle"]),
            meme_forward_pinnacle,
        )
    )

    application.run_polling(allowed_updates=Update.ALL_TYPES)


if __name__ == "__main__":
    main()
