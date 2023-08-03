#!/usr/bin/env python
# This program is dedicated to the public domain under the CC0 license.
import yaml
import logging
import sqlite3
import hashlib
from sql_queries import get_sql_query
import pendulum as pdl
from telegram import Update
from telegram.ext import (
    Application,
    ContextTypes,
    MessageHandler,
    filters,
)


logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO,
)

logging.getLogger('httpx').setLevel(logging.WARNING)
logger = logging.getLogger(__name__)
# Enable logging


def update_user_table(user_id, chat_type):
    timestamp = pdl.now().int_timestamp
    abuse = 0
    args = {'table': chat_type}
    con = sqlite3.connect('db/meme_forwarder.db')
    cur = con.cursor()
    res = cur.execute(
        get_sql_query('select_user', args) + f"'{user_id}'"
    ).fetchone()

    if res is not None:
        abuse = res[2] + 1
        cur.execute(get_sql_query('delete_users', args), (user_id,))
        con.commit()
    cur.execute(
        get_sql_query('insert_user', args), (user_id, timestamp, abuse)
    )
    con.commit()

    con.close()


def get_user_time_diff_abuse(user_id, chat_type):
    time_diff = 0
    abuse = 0
    now = pdl.now().int_timestamp
    args = {'table': chat_type}
    con = sqlite3.connect('db/meme_forwarder.db')
    cur = con.cursor()
    res = cur.execute(
        get_sql_query('select_user', args) + f"'{user_id}'"
    ).fetchone()
    con.close()

    if res is not None:
        time_diff = now - res[1]
        abuse = res[2]

    return time_diff, abuse


async def user_can_forward(context, message, chat_type, chats, cooldown):
    can_forward = False
    chat_id = chats[chat_type]
    user_id = message.from_user.id
    if message.reply_to_message is None:
        return can_forward
    reply_user_id = message.reply_to_message.from_user.id
    chat_member = await context.bot.get_chat_member(chat_id, user_id)

    role_ok = (chat_member.status == 'administrator') or (
        chat_member.status == 'creator'
    )
    member_id_ok = reply_user_id == user_id
    time_diff, abuse = get_user_time_diff_abuse(user_id, chat_type)
    print(time_diff, cooldown + abuse * 10)
    time_ok = True if (time_diff < cooldown + abuse * 10) else False

    if (chat_type == 'ascended') and role_ok and time_ok:
        can_forward = True
    elif (chat_type == 'pinnacle') and (role_ok or member_id_ok):
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

    hash_str = ''
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

    hash_str = hash_str.encode('utf8')
    hash_str = hashlib.sha256(hash_str).hexdigest()
    datetime = str(pdl.now().int_timestamp)
    args = {'table': f'{chat_type}'}

    con = sqlite3.connect('db/meme_forwarder.db')
    cur = con.cursor()
    res = cur.execute(get_sql_query('select_hashed', args) + f"'{hash_str}'")
    if res.fetchone() is None:
        can_forward = True
    id_to_insert = cur.execute(
        get_sql_query('get_ids_by_ids', args)
    ).fetchone()

    if id_to_insert is None:
        id_to_insert = 0
    else:
        id_to_insert = id_to_insert[0] + 1

    cur.execute(
        get_sql_query('insert_meme', args), (id_to_insert, datetime, hash_str)
    )
    con.commit()
    con.close()

    return can_forward


def create_tables_if_missing(chats):
    con = sqlite3.connect('db/meme_forwarder.db')
    cur = con.cursor()
    for table in chats.keys():
        args = {'table': table}
        cur.execute(get_sql_query('create_memes', args))
        cur.execute(get_sql_query('create_users', args))
    con.close()


def manage_db_rows(chat_type, limit_ids):
    args = {'table': f'{chat_type}'}
    con = sqlite3.connect('db/meme_forwarder.db')
    cur = con.cursor()
    ids = cur.execute(get_sql_query('get_ids_by_timestamp', args)).fetchall()

    if len(ids) - 1 > limit_ids:
        cur.executemany(get_sql_query('delete_ids', args), ids[-limit_ids:])

    con.commit()
    con.close()


async def meme_forward_ascended(
    update: Update, context: ContextTypes.DEFAULT_TYPE
) -> None:

    chat_type = 'ascended'
    chats = context.bot_data['chats']
    message = update.message
    limit_ids = context.bot_data['limit_ids']
    cooldown = context.bot_data['cooldown']
    reply = message.reply_to_message

    if not await user_can_forward(
        context, message, chat_type, chats, cooldown
    ):
        return
    if not await insert_meme_in_db_if_ok(message, chat_type):
        return

    update_user_table(message.from_user.id, chat_type)
    manage_db_rows(chat_type, limit_ids)

    await context.bot.copy_message(
        chat_id=chats['pinnacle'],
        from_chat_id=chats['ascended'],
        message_id=reply.id,
        caption='',
    )


async def print_faq_ascended(
    update: Update, context: ContextTypes.DEFAULT_TYPE
) -> None:
    text = f"""
Привет, я мем. То есть бот для мемов. Чтобы отправить с
моей помощью мем в чат патронов FuryDrops,надо
использовать команду /fwd с ответом на мем. После
этого мем, который понравится патронами или Баженову,
поедет в мемарню @memesdotorg
Обрати внимание, что сейчас отправлять мемы могут
только модераторы. Функция отправки мема обычными
чаттерсами находится в разработке!
Попытка спамить командой /fwd будет увеличивать кулдаун
этой команды лично для тебя вплоть  до плюс
бесконечности. Так что не стоит.
Текущий кулдаун - {context.bot_data['cooldown']} секунд!

@сделано меланхолией для чатов FD

"""
    await update.effective_message.reply_text(text)


async def print_faq_pinnacle(
    update: Update, context: ContextTypes.DEFAULT_TYPE
) -> None:
    text = """
Привет, я бот для форварда мемов. Чтобы отправить
мем в канал @memesdotorg, надо ответить на мем
с командой /fwd, будучи админом или автором
исходного сообщения.

@сделано меланхолией для чатов FD
"""
    await update.effective_message.reply_text(text)


async def meme_forward_pinnacle(
    update: Update, context: ContextTypes.DEFAULT_TYPE
) -> None:

    chat_type = 'pinnacle'
    message = update.message
    print(context.bot_data)
    limit_ids = context.bot_data['limit_ids']
    cooldown = context.bot_data['cooldown']
    chats = context.bot_data['chats']
    reply = message.reply_to_message

    if not await user_can_forward(
        context, message, chat_type, chats, cooldown
    ):
        return
    if not await insert_meme_in_db_if_ok(message, chat_type):
        return

    update_user_table(message.from_user.id, chat_type)
    manage_db_rows(chat_type, limit_ids)

    await context.bot.copy_message(
        chat_id=chats['channel'],
        from_chat_id=chats['pinnacle'],
        message_id=reply.id,
        caption='',
    )


def main() -> None:

    with open('config.yaml', 'r') as f:
        data = yaml.load(f, yaml.loader.SafeLoader)

    limit_ids = 300
    token = data['token']
    cooldown = data['cooldown']
    bot_name = data['bot_name']
    chats = {
        'ascended': data['ascended'],
        'pinnacle': data['pinnacle'],
        'channel': data['channel'],
    }

    application = Application.builder().token(token).build()
    application.bot_data['limit_ids'] = limit_ids
    application.bot_data['cooldown'] = cooldown
    application.bot_data['chats'] = chats

    create_tables_if_missing(chats)
    application.add_handler(
        MessageHandler(
            (filters.Regex('/fwd') or filters.Regex('/fwd{bot_name}'))
            & filters.Chat(chat_id=chats['ascended']),
            meme_forward_ascended,
        )
    )
    application.add_handler(
        MessageHandler(
            (filters.Regex('/fwd') or filters.Regex('/fwd{bot_name}'))
            & filters.Chat(chat_id=chats['pinnacle']),
            meme_forward_pinnacle,
        )
    )
    application.add_handler(
        MessageHandler(
            filters.Regex(f'/about{bot_name}')
            & filters.Chat(chat_id=chats['ascended']),
            print_faq_ascended,
        )
    )
    application.add_handler(
        MessageHandler(
            filters.Regex(f'/about{bot_name}')
            & filters.Chat(chat_id=chats['pinnacle']),
            print_faq_pinnacle,
        )
    )

    application.run_polling(allowed_updates=Update.ALL_TYPES)


if __name__ == '__main__':
    main()
