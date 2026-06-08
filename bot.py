#!/usr/bin/env python
# This program is dedicated to the public domain under the CC0 license.
import yaml
import logging
import sqlite3
import hashlib
from sql_queries import get_sql_query
import pendulum as pdl
from telegram import Update
from telegram.error import NetworkError, RetryAfter, TelegramError, TimedOut
from telegram.ext import (
    Application,
    CommandHandler,
    ContextTypes,
    MessageHandler,
    filters,
)
from telegram.request import HTTPXRequest


logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO,
)

logging.getLogger('httpx').setLevel(logging.WARNING)
logger = logging.getLogger(__name__)
# Enable logging

DB_PATH = 'db/meme_forwarder.db'
DB_TIMEOUT = 10


def update_user_table(user_id, chat_type):
    timestamp = pdl.now().int_timestamp
    abuse = 0
    args = {'table': chat_type}
    try:
        with sqlite3.connect(DB_PATH, timeout=DB_TIMEOUT) as con:
            cur = con.cursor()
            res = cur.execute(
                get_sql_query('select_user', args) + f"'{user_id}'"
            ).fetchone()

            if res is not None:
                abuse = res[2] + 1
                cur.execute(get_sql_query('delete_users', args), (user_id,))
            cur.execute(
                get_sql_query('insert_user', args), (user_id, timestamp, abuse)
            )
    except sqlite3.Error:
        logger.exception('Failed to update user table for %s', chat_type)


def get_user_time_diff_abuse(user_id, chat_type, cooldown):
    time_diff = cooldown + 10
    abuse = 0
    now = pdl.now().int_timestamp
    args = {'table': chat_type}
    try:
        with sqlite3.connect(DB_PATH, timeout=DB_TIMEOUT) as con:
            cur = con.cursor()
            res = cur.execute(
                get_sql_query('select_user', args) + f"'{user_id}'"
            ).fetchone()
    except sqlite3.Error:
        logger.exception('Failed to read user table for %s', chat_type)
        return time_diff, abuse

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
    try:
        chat_member = await context.bot.get_chat_member(chat_id, user_id)
    except (TimedOut, NetworkError, RetryAfter) as exc:
        logger.warning('Transient Telegram error while checking member: %s', exc)
        return can_forward
    except TelegramError:
        logger.exception('Telegram error while checking member')
        return can_forward

    role_ok = (chat_member.status == 'administrator') or (
        chat_member.status == 'creator'
    )
    member_id_ok = reply_user_id == user_id
    time_diff, abuse = get_user_time_diff_abuse(user_id, chat_type, cooldown)
    logger.debug('Forward cooldown: time_diff=%s required=%s', time_diff, cooldown + abuse * 10)
    time_ok = True if (time_diff > cooldown + abuse * 10) else False

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
    logger.debug('Meme content hash source: %s', hash_str)

    hash_str = hash_str.encode('utf8')
    hash_str = hashlib.sha256(hash_str).hexdigest()
    datetime = str(pdl.now().int_timestamp)
    args = {'table': f'{chat_type}'}

    try:
        with sqlite3.connect(DB_PATH, timeout=DB_TIMEOUT) as con:
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
    except sqlite3.Error:
        logger.exception('Failed to insert meme for %s', chat_type)
        return False

    return can_forward


def create_tables_if_missing(chats):
    with sqlite3.connect(DB_PATH, timeout=DB_TIMEOUT) as con:
        cur = con.cursor()
        for table in chats.keys():
            args = {'table': table}
            cur.execute(get_sql_query('create_memes', args))
            cur.execute(get_sql_query('create_users', args))


def manage_db_rows(chat_type, limit_ids):
    args = {'table': f'{chat_type}'}
    try:
        with sqlite3.connect(DB_PATH, timeout=DB_TIMEOUT) as con:
            cur = con.cursor()
            ids = cur.execute(get_sql_query('get_ids_by_timestamp', args)).fetchall()

            if len(ids) - 1 > limit_ids:
                cur.executemany(get_sql_query('delete_ids', args), ids[-limit_ids:])
    except sqlite3.Error:
        logger.exception('Failed to manage DB rows for %s', chat_type)


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

    try:
        await context.bot.copy_message(
            chat_id=chats['pinnacle'],
            from_chat_id=chats['ascended'],
            message_id=reply.id,
            caption='',
        )
    except (TimedOut, NetworkError, RetryAfter) as exc:
        logger.warning('Transient Telegram error while copying ascended meme: %s', exc)
    except TelegramError:
        logger.exception('Telegram error while copying ascended meme')


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
    try:
        await update.effective_message.reply_text(text)
    except (TimedOut, NetworkError, RetryAfter) as exc:
        logger.warning('Transient Telegram error while sending ascended FAQ: %s', exc)
    except TelegramError:
        logger.exception('Telegram error while sending ascended FAQ')


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
    try:
        await update.effective_message.reply_text(text)
    except (TimedOut, NetworkError, RetryAfter) as exc:
        logger.warning('Transient Telegram error while sending pinnacle FAQ: %s', exc)
    except TelegramError:
        logger.exception('Telegram error while sending pinnacle FAQ')


async def error_handler(update, context):
    exc_info = None
    if context.error is not None:
        exc_info = (
            type(context.error),
            context.error,
            context.error.__traceback__,
        )
    if isinstance(context.error, RetryAfter):
        logger.warning('Telegram rate limit, retry after %s seconds', context.error.retry_after)
    elif isinstance(context.error, (TimedOut, NetworkError)):
        logger.warning('Transient Telegram network error: %s', context.error)
    elif isinstance(context.error, TelegramError):
        logger.error('Telegram error while handling update', exc_info=exc_info)
    else:
        logger.error('Unexpected error while handling update', exc_info=exc_info)


async def log_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    message = update.effective_message
    if message is None:
        return

    logger.info(
        'Received command in chat_id=%s text=%r from_user=%s',
        update.effective_chat.id if update.effective_chat else None,
        message.text,
        update.effective_user.id if update.effective_user else None,
    )


async def meme_forward_pinnacle(
    update: Update, context: ContextTypes.DEFAULT_TYPE
) -> None:

    chat_type = 'pinnacle'
    message = update.message
    logger.debug('Bot data: %s', context.bot_data)
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

    try:
        await context.bot.copy_message(
            chat_id=chats['channel'],
            from_chat_id=chats['pinnacle'],
            message_id=reply.id,
            caption='',
        )
    except (TimedOut, NetworkError, RetryAfter) as exc:
        logger.warning('Transient Telegram error while copying pinnacle meme: %s', exc)
    except TelegramError:
        logger.exception('Telegram error while copying pinnacle meme')


async def post_init(application: Application) -> None:
    try:
        me = await application.bot.get_me()
        logger.info('Bot started as @%s', me.username)
    except (TimedOut, NetworkError, RetryAfter) as exc:
        logger.warning('Transient Telegram error while reading bot info: %s', exc)
    except TelegramError:
        logger.exception('Telegram error while reading bot info')

    logger.info('Configured chats: %s', application.bot_data['chats'])


def main() -> None:

    with open('config.yaml', 'r') as f:
        data = yaml.load(f, yaml.loader.SafeLoader)

    limit_ids = 300
    token = data['token']
    cooldown = data['cooldown']
    chats = {
        'ascended': data['ascended'],
        'pinnacle': data['pinnacle'],
        'channel': data['channel'],
    }

    request = HTTPXRequest(
        connect_timeout=15,
        read_timeout=30,
        write_timeout=30,
        pool_timeout=15,
    )
    application = (
        Application.builder()
        .token(token)
        .request(request)
        .post_init(post_init)
        .build()
    )
    application.bot_data['limit_ids'] = limit_ids
    application.bot_data['cooldown'] = cooldown
    application.bot_data['chats'] = chats

    create_tables_if_missing(chats)
    application.add_handler(
        CommandHandler(
            'fwd',
            meme_forward_ascended,
            filters=filters.Chat(chat_id=chats['ascended']),
        )
    )
    application.add_handler(
        CommandHandler(
            'fwd',
            meme_forward_pinnacle,
            filters=filters.Chat(chat_id=chats['pinnacle']),
        )
    )
    application.add_handler(
        CommandHandler(
            'about',
            print_faq_ascended,
            filters=filters.Chat(chat_id=chats['ascended']),
        )
    )
    application.add_handler(
        CommandHandler(
            'about',
            print_faq_pinnacle,
            filters=filters.Chat(chat_id=chats['pinnacle']),
        )
    )
    application.add_handler(MessageHandler(filters.COMMAND, log_command), group=1)
    application.add_error_handler(error_handler)

    logger.info('Starting polling')
    application.run_polling(
        allowed_updates=Update.ALL_TYPES,
        timeout=30,
        bootstrap_retries=-1,
        drop_pending_updates=False,
    )


if __name__ == '__main__':
    main()
