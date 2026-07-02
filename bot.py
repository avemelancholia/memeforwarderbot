#!/usr/bin/env python
# This program is dedicated to the public domain under the CC0 license.
import yaml
import asyncio
import html
import json
import logging
import os
import signal
import time
import sqlite3
import hashlib
from io import BytesIO
from pathlib import Path
from sql_queries import get_sql_query
import pendulum as pdl
from PIL import Image, ImageDraw, ImageFont
from telegram import Update
from telegram.error import NetworkError, RetryAfter, TelegramError, TimedOut
from telegram.ext import (
    Application,
    CommandHandler,
    ContextTypes,
    MessageReactionHandler,
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
WATCHDOG_INTERVAL = 60
WATCHDOG_TIMEOUT = 20
POLLING_TIMEOUT = 20
POLLING_READ_TIMEOUT = 5
REACTION_FLUSH_INTERVAL = 60
SUBSCRIBER_CHECK_INTERVAL = 60
HEARTBEAT_INTERVAL = 5
HEARTBEAT_PATH = Path(
    os.environ.get('BOT_HEARTBEAT_PATH', '/tmp/meme-forwarder-heartbeat.json')
)
BOOT_ID = os.environ.get('BOT_BOOT_ID', f'standalone-{os.getpid()}')
SHUTDOWN_TIMEOUT = 20


class RestartRequired(RuntimeError):
    """Signal that the process must exit so Docker can restart the container."""


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


def get_message_id(message):
    return getattr(message, 'message_id', getattr(message, 'id', message))


def create_reaction_tables(cur):
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS forwarded_meme(
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            hash TEXT,
            submitter_user_id INTEGER,
            source_chat_type TEXT,
            source_chat_id TEXT,
            source_message_id INTEGER,
            destination_chat_type TEXT,
            destination_chat_id TEXT,
            destination_message_id INTEGER,
            created_at INTEGER,
            UNIQUE(destination_chat_type, destination_message_id)
        )
        """
    )
    cur.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_forwarded_meme_destination
        ON forwarded_meme(destination_chat_type, destination_message_id)
        """
    )
    cur.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_forwarded_meme_submitter
        ON forwarded_meme(source_chat_type, destination_chat_type, submitter_user_id)
        """
    )
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS meme_reaction_count(
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            forwarded_meme_id INTEGER NOT NULL,
            reaction_type TEXT NOT NULL,
            reaction_count INTEGER NOT NULL,
            updated_at INTEGER NOT NULL,
            UNIQUE(forwarded_meme_id, reaction_type)
        )
        """
    )
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS meme_contributor(
            user_id INTEGER PRIMARY KEY,
            username TEXT,
            first_name TEXT,
            last_name TEXT,
            updated_at INTEGER NOT NULL
        )
        """
    )
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS channel_subscriber_state(
            channel_chat_id TEXT PRIMARY KEY,
            subscriber_count INTEGER,
            latest_forwarded_meme_id INTEGER,
            updated_at INTEGER NOT NULL
        )
        """
    )
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS meme_bad_score(
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            forwarded_meme_id INTEGER NOT NULL,
            drop_count INTEGER NOT NULL,
            created_at INTEGER NOT NULL
        )
        """
    )
    cur.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_meme_bad_score_forwarded_meme
        ON meme_bad_score(forwarded_meme_id)
        """
    )
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS bot_report_message(
            chat_id TEXT PRIMARY KEY,
            message_id INTEGER NOT NULL,
            report_type TEXT NOT NULL,
            updated_at INTEGER NOT NULL
        )
        """
    )


def store_forwarded_meme(
    meme_hash,
    submitter_user_id,
    source_chat_type,
    source_chat_id,
    source_message_id,
    destination_chat_type,
    destination_chat_id,
    destination_message_id,
):
    if destination_message_id is None:
        logger.warning('Cannot store forwarded meme without destination message ID')
        return None

    timestamp = pdl.now().int_timestamp
    try:
        with sqlite3.connect(DB_PATH, timeout=DB_TIMEOUT) as con:
            cur = con.cursor()
            cur.execute(
                """
                INSERT INTO forwarded_meme(
                    hash,
                    submitter_user_id,
                    source_chat_type,
                    source_chat_id,
                    source_message_id,
                    destination_chat_type,
                    destination_chat_id,
                    destination_message_id,
                    created_at
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(destination_chat_type, destination_message_id)
                DO UPDATE SET
                    hash = excluded.hash,
                    submitter_user_id = excluded.submitter_user_id,
                    source_chat_type = excluded.source_chat_type,
                    source_chat_id = excluded.source_chat_id,
                    source_message_id = excluded.source_message_id,
                    destination_chat_id = excluded.destination_chat_id
                """,
                (
                    meme_hash,
                    submitter_user_id,
                    source_chat_type,
                    str(source_chat_id),
                    source_message_id,
                    destination_chat_type,
                    str(destination_chat_id),
                    destination_message_id,
                    timestamp,
                ),
            )
            res = cur.execute(
                """
                SELECT id
                FROM forwarded_meme
                WHERE destination_chat_type = ? AND destination_message_id = ?
                ORDER BY id DESC
                LIMIT 1
                """,
                (destination_chat_type, destination_message_id),
            ).fetchone()
    except sqlite3.Error:
        logger.exception('Failed to store forwarded meme attribution')
        return None

    if res is None:
        return None
    return res[0]


def set_latest_channel_meme(channel_chat_id, forwarded_meme_id, subscriber_count):
    timestamp = pdl.now().int_timestamp
    try:
        with sqlite3.connect(DB_PATH, timeout=DB_TIMEOUT) as con:
            cur = con.cursor()
            cur.execute(
                """
                INSERT INTO channel_subscriber_state(
                    channel_chat_id,
                    subscriber_count,
                    latest_forwarded_meme_id,
                    updated_at
                )
                VALUES (?, ?, ?, ?)
                ON CONFLICT(channel_chat_id)
                DO UPDATE SET
                    subscriber_count = excluded.subscriber_count,
                    latest_forwarded_meme_id = excluded.latest_forwarded_meme_id,
                    updated_at = excluded.updated_at
                """,
                (
                    str(channel_chat_id),
                    subscriber_count,
                    forwarded_meme_id,
                    timestamp,
                ),
            )
    except sqlite3.Error:
        logger.exception('Failed to set latest channel meme')


def set_channel_subscriber_baseline(channel_chat_id, subscriber_count):
    timestamp = pdl.now().int_timestamp
    try:
        with sqlite3.connect(DB_PATH, timeout=DB_TIMEOUT) as con:
            cur = con.cursor()
            cur.execute(
                """
                INSERT INTO channel_subscriber_state(
                    channel_chat_id,
                    subscriber_count,
                    latest_forwarded_meme_id,
                    updated_at
                )
                VALUES (?, ?, NULL, ?)
                ON CONFLICT(channel_chat_id)
                DO UPDATE SET
                    subscriber_count = excluded.subscriber_count,
                    updated_at = excluded.updated_at
                """,
                (str(channel_chat_id), subscriber_count, timestamp),
            )
    except sqlite3.Error:
        logger.exception('Failed to set channel subscriber baseline')


def get_channel_subscriber_state(channel_chat_id):
    try:
        with sqlite3.connect(DB_PATH, timeout=DB_TIMEOUT) as con:
            cur = con.cursor()
            return cur.execute(
                """
                SELECT subscriber_count, latest_forwarded_meme_id
                FROM channel_subscriber_state
                WHERE channel_chat_id = ?
                """,
                (str(channel_chat_id),),
            ).fetchone()
    except sqlite3.Error:
        logger.exception('Failed to read channel subscriber state')
        return None


def record_bad_meme_score(forwarded_meme_id, drop_count):
    if forwarded_meme_id is None or drop_count <= 1:
        return

    timestamp = pdl.now().int_timestamp
    try:
        with sqlite3.connect(DB_PATH, timeout=DB_TIMEOUT) as con:
            cur = con.cursor()
            cur.execute(
                """
                INSERT INTO meme_bad_score(
                    forwarded_meme_id,
                    drop_count,
                    created_at
                )
                VALUES (?, ?, ?)
                """,
                (forwarded_meme_id, drop_count, timestamp),
            )
    except sqlite3.Error:
        logger.exception('Failed to record bad meme score')


def get_last_report_message(chat_id):
    try:
        with sqlite3.connect(DB_PATH, timeout=DB_TIMEOUT) as con:
            cur = con.cursor()
            return cur.execute(
                """
                SELECT message_id
                FROM bot_report_message
                WHERE chat_id = ?
                """,
                (str(chat_id),),
            ).fetchone()
    except sqlite3.Error:
        logger.exception('Failed to read last report message')
        return None


def store_last_report_message(chat_id, message_id, report_type):
    timestamp = pdl.now().int_timestamp
    try:
        with sqlite3.connect(DB_PATH, timeout=DB_TIMEOUT) as con:
            cur = con.cursor()
            cur.execute(
                """
                INSERT INTO bot_report_message(
                    chat_id,
                    message_id,
                    report_type,
                    updated_at
                )
                VALUES (?, ?, ?, ?)
                ON CONFLICT(chat_id)
                DO UPDATE SET
                    message_id = excluded.message_id,
                    report_type = excluded.report_type,
                    updated_at = excluded.updated_at
                """,
                (str(chat_id), message_id, report_type, timestamp),
            )
    except sqlite3.Error:
        logger.exception('Failed to store last report message')


def store_contributor_profile(user):
    if user is None:
        return

    timestamp = pdl.now().int_timestamp
    try:
        with sqlite3.connect(DB_PATH, timeout=DB_TIMEOUT) as con:
            cur = con.cursor()
            cur.execute(
                """
                INSERT INTO meme_contributor(
                    user_id,
                    username,
                    first_name,
                    last_name,
                    updated_at
                )
                VALUES (?, ?, ?, ?, ?)
                ON CONFLICT(user_id)
                DO UPDATE SET
                    username = excluded.username,
                    first_name = excluded.first_name,
                    last_name = excluded.last_name,
                    updated_at = excluded.updated_at
                """,
                (
                    user.id,
                    user.username,
                    user.first_name,
                    user.last_name,
                    timestamp,
                ),
            )
    except sqlite3.Error:
        logger.exception('Failed to store contributor profile for %s', user.id)


def format_contributor(user_id, username=None, first_name=None, last_name=None):
    if username:
        return f'@{username}'

    full_name = ' '.join(
        part for part in (first_name, last_name) if part
    ).strip()
    if full_name:
        return full_name

    return str(user_id)


def format_contributor_no_mention(
    user_id, username=None, first_name=None, last_name=None
):
    full_name = ' '.join(
        part for part in (first_name, last_name) if part
    ).strip()
    if full_name:
        return full_name

    return str(user_id)


def fit_text(text, width):
    text = str(text)
    if len(text) > width:
        return text[: width - 1] + '…'
    return text.ljust(width)


def markdown_code_block(text):
    return '```\n' + text.replace('```', "'''") + '\n```'


def format_top_html(rows, limit):
    lines = [
        f'🏆 <b>Топ-{limit} мемеров</b>',
        '<i>по среднему последних 100 мемов</i>',
        '',
    ]

    for index, contributor in enumerate(rows, start=1):
        (
            user_id,
            username,
            first_name,
            last_name,
            meme_count,
            total_score,
            best_score,
            mean_last_100,
            bad_score,
        ) = contributor
        name = html.escape(
            format_contributor_no_mention(
                user_id, username, first_name, last_name
            )
        )
        lines.extend(
            [
                f'{index}. <b>{name}</b>',
                (
                    f'{mean_last_100:.1f} ср · '
                    f'{format_int_short(meme_count, 5).strip()} мемов · '
                    f'{format_int_short(total_score, 7).strip()} реакций · '
                    f'лучший {format_int_short(best_score, 6).strip()} · '
                    f'🪦 {format_int_short(bad_score, 3).strip()}'
                ),
                '',
            ]
        )

    return '\n'.join(lines).rstrip()


def format_int_short(value, width):
    value = int(value or 0)
    text = str(value)
    if len(text) <= width:
        return text.rjust(width)

    abs_value = abs(value)
    for suffix, divisor in (('m', 1_000_000), ('k', 1_000)):
        if abs_value >= divisor:
            compact_value = value / divisor
            if abs(compact_value) < 10:
                text = f'{compact_value:.1f}{suffix}'
            else:
                text = f'{compact_value:.0f}{suffix}'
            if len(text) <= width:
                return text.rjust(width)

    return ('>' + '9' * (width - 1)).rjust(width)


def format_decimal_short(value, width):
    text = f'{value:.1f}'
    if len(text) <= width:
        return text.rjust(width)
    return format_int_short(round(value), width)


def format_top_table(rows):
    widths = {
        'rank': 2,
        'name': 20,
        'mean': 7,
        'memes': 5,
        'reactions': 7,
        'best': 6,
        'grave': 3,
    }
    columns = [
        widths['rank'],
        widths['name'],
        widths['mean'],
        widths['memes'],
        widths['reactions'],
        widths['best'],
        widths['grave'],
    ]

    def border(left, middle, right):
        return left + middle.join('─' * (width + 2) for width in columns) + right

    def row(rank, name, mean, memes, reactions, best, grave):
        return (
            f'│ {rank:>{widths["rank"]}} '
            f'│ {fit_text(name, widths["name"])} '
            f'│ {format_decimal_short(mean, widths["mean"])} '
            f'│ {format_int_short(memes, widths["memes"])} '
            f'│ {format_int_short(reactions, widths["reactions"])} '
            f'│ {format_int_short(best, widths["best"])} '
            f'│ {format_int_short(grave, widths["grave"])} │'
        )

    lines = [
        border('┌', '┬', '┐'),
        (
            f'│ {"#":>{widths["rank"]}} '
            f'│ {fit_text("Мемер", widths["name"])} '
            f'│ {"Среднее":>{widths["mean"]}} '
            f'│ {"Мемы":>{widths["memes"]}} '
            f'│ {"Реакции":>{widths["reactions"]}} '
            f'│ {"Лучший":>{widths["best"]}} '
            f'│ {"🪦":>{widths["grave"]}} │'
        ),
        border('├', '┼', '┤'),
    ]

    for index, contributor in enumerate(rows, start=1):
        (
            user_id,
            username,
            first_name,
            last_name,
            meme_count,
            total_score,
            best_score,
            mean_last_100,
            bad_score,
        ) = contributor
        name = format_contributor(user_id, username, first_name, last_name)
        lines.append(
            row(
                index,
                name,
                mean_last_100,
                meme_count,
                total_score,
                best_score,
                bad_score,
            )
        )

    lines.append(border('└', '┴', '┘'))
    return '\n'.join(lines)


def load_font(size, bold=False):
    font_names = (
        (
            '/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf',
            '/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf',
        ),
        (
            '/usr/share/fonts/dejavu/DejaVuSans-Bold.ttf',
            '/usr/share/fonts/dejavu/DejaVuSans.ttf',
        ),
    )
    for bold_path, regular_path in font_names:
        path = bold_path if bold else regular_path
        if os.path.exists(path):
            return ImageFont.truetype(path, size=size)
    return ImageFont.load_default()


def draw_text(draw, position, text, font, fill, anchor='la'):
    draw.text(position, str(text), font=font, fill=fill, anchor=anchor)


def text_width(draw, text, font):
    box = draw.textbbox((0, 0), str(text), font=font)
    return box[2] - box[0]


def fit_text_pixels(draw, text, font, max_width):
    text = str(text)
    if text_width(draw, text, font) <= max_width:
        return text

    ellipsis = '…'
    while text and text_width(draw, text + ellipsis, font) > max_width:
        text = text[:-1]
    return text + ellipsis if text else ellipsis


def render_top_image(rows, limit):
    title_font = load_font(34, bold=True)
    header_font = load_font(21, bold=True)
    row_font = load_font(22)
    small_font = load_font(18)

    columns = [
        ('#', 44, 'right'),
        ('Мемер', 285, 'left'),
        ('Среднее', 105, 'right'),
        ('Мемы', 82, 'right'),
        ('Реакции', 112, 'right'),
        ('Лучший', 95, 'right'),
        ('🪦', 58, 'right'),
    ]
    padding_x = 34
    padding_y = 28
    title_h = 48
    header_h = 40
    row_h = 42
    footer_h = 18
    gap = 16
    table_width = sum(width for _, width, _ in columns)
    width = padding_x * 2 + table_width
    height = padding_y * 2 + title_h + gap + header_h + row_h * len(rows) + footer_h

    image = Image.new('RGBA', (width, height), (0, 0, 0, 0))
    draw = ImageDraw.Draw(image)

    panel = (14, 16, 24, 224)
    border = (255, 255, 255, 42)
    row_alt = (255, 255, 255, 16)
    text = (246, 248, 252, 255)
    muted = (174, 184, 198, 255)
    accent = (161, 214, 255, 255)

    draw.rounded_rectangle(
        (8, 8, width - 8, height - 8),
        radius=26,
        fill=panel,
        outline=border,
        width=1,
    )
    draw_text(draw, (padding_x, padding_y), f'Топ-{limit} мемеров', title_font, text)
    draw_text(
        draw,
        (width - padding_x, padding_y + 7),
        'по среднему последних 100',
        small_font,
        muted,
        anchor='ra',
    )

    y = padding_y + title_h + gap
    x = padding_x
    draw.rounded_rectangle(
        (x - 10, y - 6, x + table_width + 10, y + header_h - 3),
        radius=12,
        fill=(255, 255, 255, 22),
    )

    col_x = x
    for label, col_width, align in columns:
        anchor = 'ra' if align == 'right' else 'la'
        draw_x = col_x + col_width - 10 if align == 'right' else col_x + 10
        draw_text(draw, (draw_x, y + 7), label, header_font, accent, anchor=anchor)
        col_x += col_width

    y += header_h
    for index, row in enumerate(rows, start=1):
        (
            user_id,
            username,
            first_name,
            last_name,
            meme_count,
            total_score,
            best_score,
            mean_last_100,
            bad_score,
        ) = row
        if index % 2 == 0:
            draw.rounded_rectangle(
                (x - 10, y, x + table_width + 10, y + row_h - 2),
                radius=10,
                fill=row_alt,
            )

        values = [
            index,
            format_contributor(user_id, username, first_name, last_name),
            f'{mean_last_100:.1f}',
            format_int_short(meme_count, 5).strip(),
            format_int_short(total_score, 7).strip(),
            format_int_short(best_score, 6).strip(),
            format_int_short(bad_score, 3).strip(),
        ]
        col_x = x
        for value, (_, col_width, align) in zip(values, columns):
            font = row_font
            display = value
            if align == 'left':
                display = fit_text_pixels(draw, display, font, col_width - 20)
                draw_x = col_x + 10
                anchor = 'la'
            else:
                draw_x = col_x + col_width - 10
                anchor = 'ra'
            draw_text(draw, (draw_x, y + 8), display, font, text, anchor=anchor)
            col_x += col_width
        y += row_h

    buffer = BytesIO()
    image.save(buffer, format='PNG')
    buffer.seek(0)
    buffer.name = 'top_memers.png'
    return buffer


def get_user_stats(user_id):
    try:
        with sqlite3.connect(DB_PATH, timeout=DB_TIMEOUT) as con:
            cur = con.cursor()
            return cur.execute(
                """
                WITH meme_scores AS (
                    SELECT
                        fm.id,
                        fm.created_at,
                        COALESCE(SUM(mrc.reaction_count), 0) AS score
                    FROM forwarded_meme fm
                    LEFT JOIN meme_reaction_count mrc
                        ON mrc.forwarded_meme_id = fm.id
                    WHERE fm.source_chat_type = 'pinnacle'
                        AND fm.destination_chat_type = 'channel'
                        AND fm.submitter_user_id = ?
                    GROUP BY fm.id
                ),
                last_100 AS (
                    SELECT score
                    FROM meme_scores
                    ORDER BY created_at DESC, id DESC
                    LIMIT 100
                )
                SELECT
                    (SELECT COUNT(*) FROM meme_scores) AS meme_count,
                    (SELECT COALESCE(SUM(score), 0) FROM meme_scores) AS total_score,
                    (SELECT COALESCE(MAX(score), 0) FROM meme_scores) AS best_score,
                    (SELECT COALESCE(AVG(score), 0) FROM last_100) AS mean_last_100,
                    (
                        SELECT COALESCE(SUM(mbs.drop_count), 0)
                        FROM meme_scores ms
                        JOIN meme_bad_score mbs
                            ON mbs.forwarded_meme_id = ms.id
                    ) AS bad_score
                """,
                (user_id,),
            ).fetchone()
    except sqlite3.Error:
        logger.exception('Failed to get stats for user %s', user_id)
        return 0, 0, 0, 0, 0


def get_top_contributors(limit):
    try:
        with sqlite3.connect(DB_PATH, timeout=DB_TIMEOUT) as con:
            cur = con.cursor()
            return cur.execute(
                """
                WITH meme_scores AS (
                    SELECT
                        fm.id,
                        fm.submitter_user_id,
                        fm.created_at,
                        COALESCE(SUM(mrc.reaction_count), 0) AS score
                    FROM forwarded_meme fm
                    LEFT JOIN meme_reaction_count mrc
                        ON mrc.forwarded_meme_id = fm.id
                    WHERE fm.source_chat_type = 'pinnacle'
                        AND fm.destination_chat_type = 'channel'
                    GROUP BY fm.id
                ),
                ranked_scores AS (
                    SELECT
                        id,
                        submitter_user_id,
                        score,
                        ROW_NUMBER() OVER (
                            PARTITION BY submitter_user_id
                            ORDER BY created_at DESC, id DESC
                        ) AS row_num
                    FROM meme_scores
                ),
                contributor_scores AS (
                    SELECT
                        submitter_user_id,
                        COUNT(*) AS meme_count,
                        COALESCE(SUM(score), 0) AS total_score,
                        COALESCE(MAX(score), 0) AS best_score,
                        COALESCE(
                            SUM(CASE WHEN row_num <= 100 THEN score END),
                            0
                        ) AS total_last_100,
                        COALESCE(
                            AVG(CASE WHEN row_num <= 100 THEN score END),
                            0
                        ) AS mean_last_100
                    FROM ranked_scores
                    GROUP BY submitter_user_id
                ),
                bad_scores AS (
                    SELECT
                        ms.submitter_user_id,
                        COALESCE(SUM(mbs.drop_count), 0) AS bad_score
                    FROM meme_scores ms
                    JOIN meme_bad_score mbs
                        ON mbs.forwarded_meme_id = ms.id
                    GROUP BY ms.submitter_user_id
                )
                SELECT
                    cs.submitter_user_id,
                    mc.username,
                    mc.first_name,
                    mc.last_name,
                    cs.meme_count,
                    cs.total_score,
                    cs.best_score,
                    cs.mean_last_100,
                    COALESCE(bs.bad_score, 0) AS bad_score
                FROM contributor_scores cs
                LEFT JOIN meme_contributor mc
                    ON mc.user_id = cs.submitter_user_id
                LEFT JOIN bad_scores bs
                    ON bs.submitter_user_id = cs.submitter_user_id
                ORDER BY total_last_100 DESC,
                    mean_last_100 DESC,
                    total_score DESC,
                    meme_count DESC,
                    best_score DESC
                LIMIT ?
                """,
                (limit,),
            ).fetchall()
    except sqlite3.Error:
        logger.exception('Failed to get top contributors')
        return []


def reaction_type_key(reaction_type):
    if hasattr(reaction_type, 'emoji'):
        return f'emoji:{reaction_type.emoji}'
    if hasattr(reaction_type, 'custom_emoji_id'):
        return f'custom_emoji:{reaction_type.custom_emoji_id}'
    return getattr(reaction_type, 'type', str(reaction_type))


def flush_reaction_count_update(chat_id, message_id, reactions):
    timestamp = pdl.now().int_timestamp
    try:
        with sqlite3.connect(DB_PATH, timeout=DB_TIMEOUT) as con:
            cur = con.cursor()
            res = cur.execute(
                """
                SELECT id
                FROM forwarded_meme
                WHERE destination_chat_type = 'channel'
                    AND destination_message_id = ?
                ORDER BY id DESC
                LIMIT 1
                """,
                (message_id,),
            ).fetchone()

            if res is None:
                logger.info(
                    'Reaction update for unattributed channel message chat_id=%s message_id=%s',
                    chat_id,
                    message_id,
                )
                return True

            forwarded_meme_id = res[0]
            cur.execute(
                'DELETE FROM meme_reaction_count WHERE forwarded_meme_id = ?',
                (forwarded_meme_id,),
            )
            cur.executemany(
                """
                INSERT INTO meme_reaction_count(
                    forwarded_meme_id,
                    reaction_type,
                    reaction_count,
                    updated_at
                )
                VALUES (?, ?, ?, ?)
                """,
                [
                    (forwarded_meme_id, reaction_type, reaction_count, timestamp)
                    for reaction_type, reaction_count in reactions
                    if reaction_count > 0
                ],
            )
    except sqlite3.Error:
        logger.exception(
            'Failed to flush reaction counts chat_id=%s message_id=%s',
            chat_id,
            message_id,
        )
        return False

    return True


async def user_can_forward(context, message, chat_type, chats, cooldown):
    can_forward = False
    chat_id = chats[chat_type]
    user_id = message.from_user.id
    if message.reply_to_message is None:
        return can_forward
    reply_user = message.reply_to_message.from_user
    reply_user_id = reply_user.id if reply_user is not None else None
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

    if (chat_type == 'pinnacle') and (role_ok or member_id_ok):
        can_forward = True
    return can_forward


async def insert_meme_in_db_if_ok(message, chat_type):
    can_forward = False
    if message.reply_to_message is None:
        return can_forward, None
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
        return can_forward, None

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
        return False, None

    return can_forward, hash_str


def create_tables_if_missing(chats):
    with sqlite3.connect(DB_PATH, timeout=DB_TIMEOUT) as con:
        cur = con.cursor()
        for table in chats.keys():
            args = {'table': table}
            cur.execute(get_sql_query('create_memes', args))
            cur.execute(get_sql_query('create_users', args))
        create_reaction_tables(cur)


async def print_faq_pinnacle(
    update: Update, context: ContextTypes.DEFAULT_TYPE
) -> None:
    text = """
Привет, я бот для форварда мемов. Чтобы отправить
мем в канал @memesdotorg, надо ответить на мем
с командой /fwd, будучи админом или автором
исходного сообщения.

/stats покажет твою мемную статистику. Если ответить
командой /stats на сообщение пользователя, покажет
статистику этого пользователя.

/top покажет топ мемеров по реакциям в канале.

@сделано меланхолией для чатов FD
"""
    try:
        await update.effective_message.reply_text(text)
    except (TimedOut, NetworkError, RetryAfter) as exc:
        logger.warning('Transient Telegram error while sending pinnacle FAQ: %s', exc)
    except TelegramError:
        logger.exception('Telegram error while sending pinnacle FAQ')


async def reply_text_safely(
    update: Update,
    text: str,
    action: str,
    parse_mode=None,
):
    message = update.effective_message
    if message is None:
        logger.warning('Cannot send %s response without effective message', action)
        return None

    try:
        return await message.reply_text(text, parse_mode=parse_mode)
    except (TimedOut, NetworkError, RetryAfter) as exc:
        logger.warning('Transient Telegram error while sending %s: %s', action, exc)
    except TelegramError:
        logger.exception('Telegram error while sending %s', action)

    return None


async def send_report_safely(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
    text: str,
    action: str,
    parse_mode=None,
) -> None:
    chat = update.effective_chat
    if chat is None:
        await reply_text_safely(update, text, action, parse_mode=parse_mode)
        return

    previous_report = get_last_report_message(chat.id)
    if previous_report is not None:
        previous_message_id = previous_report[0]
        try:
            await context.bot.delete_message(
                chat_id=chat.id,
                message_id=previous_message_id,
            )
        except (TimedOut, NetworkError, RetryAfter) as exc:
            logger.warning('Transient Telegram error while deleting report: %s', exc)
        except TelegramError as exc:
            logger.warning('Previous report could not be deleted: %s', exc)

    sent_message = await reply_text_safely(
        update,
        text,
        action,
        parse_mode=parse_mode,
    )
    if sent_message is not None:
        store_last_report_message(chat.id, get_message_id(sent_message), action)


async def send_report_photo_safely(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
    photo,
    caption: str,
    action: str,
) -> None:
    chat = update.effective_chat
    if chat is None:
        logger.warning('Cannot send %s photo without effective chat', action)
        return

    previous_report = get_last_report_message(chat.id)
    if previous_report is not None:
        previous_message_id = previous_report[0]
        try:
            await context.bot.delete_message(
                chat_id=chat.id,
                message_id=previous_message_id,
            )
        except (TimedOut, NetworkError, RetryAfter) as exc:
            logger.warning('Transient Telegram error while deleting report: %s', exc)
        except TelegramError as exc:
            logger.warning('Previous report could not be deleted: %s', exc)

    try:
        sent_message = await context.bot.send_photo(
            chat_id=chat.id,
            photo=photo,
            caption=caption,
        )
    except (TimedOut, NetworkError, RetryAfter) as exc:
        logger.warning('Transient Telegram error while sending %s photo: %s', action, exc)
        return
    except TelegramError:
        logger.exception('Telegram error while sending %s photo', action)
        return

    store_last_report_message(chat.id, get_message_id(sent_message), action)


async def print_stats(
    update: Update, context: ContextTypes.DEFAULT_TYPE
) -> None:
    message = update.effective_message
    user = update.effective_user

    if (
        message is not None
        and message.reply_to_message is not None
        and message.reply_to_message.from_user is not None
    ):
        user = message.reply_to_message.from_user

    if user is None:
        await reply_text_safely(
            update,
            'Не могу понять, для кого показать статистику.',
            'stats',
        )
        return

    store_contributor_profile(user)
    (
        meme_count,
        total_score,
        best_score,
        mean_last_100,
        bad_score,
    ) = get_user_stats(user.id)
    name = format_contributor(
        user.id,
        user.username,
        user.first_name,
        user.last_name,
    )

    text = (
        f'Статистика {name}.\n\n'
        f'Мемы: {meme_count}\n'
        f'Реакции: {total_score}\n'
        f'Среднее: {mean_last_100:.1f}\n'
        f'Лучший: {best_score}\n'
        f'🪦: {bad_score}'
    )
    await send_report_safely(update, context, text, 'stats')


async def print_top(
    update: Update, context: ContextTypes.DEFAULT_TYPE
) -> None:
    limit = 10
    if context.args:
        try:
            limit = int(context.args[0])
        except ValueError:
            limit = 10
    limit = max(1, min(limit, 20))

    rows = get_top_contributors(limit)
    if not rows:
        await send_report_safely(
            update,
            context,
            f'Топ-{limit} мемеров.\n\n'
            'Пока нет засчитанных мемов с реакциями.',
            'top',
        )
        return

    text = format_top_html(rows, limit)
    await send_report_safely(
        update,
        context,
        text,
        'top',
        parse_mode='HTML',
    )


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


async def handle_reaction_count(
    update: Update, context: ContextTypes.DEFAULT_TYPE
) -> None:
    reaction_count = update.message_reaction_count
    if reaction_count is None:
        return

    reactions = [
        (reaction_type_key(reaction.type), reaction.total_count)
        for reaction in reaction_count.reactions
    ]
    key = (reaction_count.chat.id, reaction_count.message_id)
    lock = context.bot_data.get('reaction_lock')
    if lock is None:
        logger.warning('Reaction count update received before lock initialized')
        return

    async with lock:
        context.bot_data['pending_reaction_counts'][key] = reactions

    logger.info(
        'Queued reaction count update chat_id=%s message_id=%s reactions=%s',
        reaction_count.chat.id,
        reaction_count.message_id,
        reactions,
    )


async def reaction_flush_loop(application: Application) -> None:
    while True:
        await asyncio.sleep(REACTION_FLUSH_INTERVAL)
        lock = application.bot_data.get('reaction_lock')
        if lock is None:
            logger.warning('Reaction flush skipped because lock is missing')
            continue

        async with lock:
            pending = application.bot_data['pending_reaction_counts']
            updates = dict(pending)
            pending.clear()

        if not updates:
            continue

        failed_updates = {}
        for (chat_id, message_id), reactions in updates.items():
            if not flush_reaction_count_update(chat_id, message_id, reactions):
                failed_updates[(chat_id, message_id)] = reactions

        if failed_updates:
            async with lock:
                pending = application.bot_data['pending_reaction_counts']
                for key, reactions in failed_updates.items():
                    pending.setdefault(key, reactions)

        logger.info(
            'Flushed reaction count updates: ok=%s failed=%s',
            len(updates) - len(failed_updates),
            len(failed_updates),
        )


async def get_channel_subscriber_count(bot, channel_chat_id):
    try:
        return await asyncio.wait_for(
            bot.get_chat_member_count(channel_chat_id),
            timeout=WATCHDOG_TIMEOUT,
        )
    except (TimedOut, NetworkError, RetryAfter) as exc:
        logger.warning('Transient Telegram error while reading subscriber count: %s', exc)
    except TimeoutError as exc:
        logger.warning('Timed out while reading subscriber count: %s', exc)
    except TelegramError:
        logger.exception('Telegram error while reading subscriber count')
    except Exception:
        logger.exception('Unexpected error while reading subscriber count')

    return None


async def initialize_subscriber_baseline(application: Application) -> None:
    channel_chat_id = application.bot_data['chats']['channel']
    subscriber_count = await get_channel_subscriber_count(
        application.bot,
        channel_chat_id,
    )
    if subscriber_count is None:
        logger.warning('Subscriber baseline not initialized')
        return

    set_channel_subscriber_baseline(channel_chat_id, subscriber_count)
    logger.info('Channel subscriber baseline initialized: %s', subscriber_count)


async def check_channel_subscribers(application: Application) -> None:
    channel_chat_id = application.bot_data['chats']['channel']
    subscriber_count = await get_channel_subscriber_count(
        application.bot,
        channel_chat_id,
    )
    if subscriber_count is None:
        return

    state = get_channel_subscriber_state(channel_chat_id)
    if state is None or state[0] is None:
        set_channel_subscriber_baseline(channel_chat_id, subscriber_count)
        logger.info('Channel subscriber baseline set: %s', subscriber_count)
        return

    previous_count, latest_forwarded_meme_id = state
    drop_count = previous_count - subscriber_count
    if drop_count > 1:
        if latest_forwarded_meme_id is None:
            logger.warning(
                'Subscriber count dropped by %s without a latest meme',
                drop_count,
            )
        else:
            record_bad_meme_score(latest_forwarded_meme_id, drop_count)
            logger.warning(
                'Recorded tombstone score=%s for forwarded_meme_id=%s',
                drop_count,
                latest_forwarded_meme_id,
            )

    set_channel_subscriber_baseline(channel_chat_id, subscriber_count)


async def subscriber_monitor_loop(application: Application) -> None:
    await initialize_subscriber_baseline(application)
    while True:
        await asyncio.sleep(SUBSCRIBER_CHECK_INTERVAL)
        await check_channel_subscribers(application)


async def meme_forward_pinnacle(
    update: Update, context: ContextTypes.DEFAULT_TYPE
) -> None:

    chat_type = 'pinnacle'
    message = update.message
    logger.debug('Bot data: %s', context.bot_data)
    cooldown = context.bot_data['cooldown']
    chats = context.bot_data['chats']
    reply = message.reply_to_message

    if not await user_can_forward(
        context, message, chat_type, chats, cooldown
    ):
        return
    can_forward, meme_hash = await insert_meme_in_db_if_ok(message, chat_type)
    if not can_forward:
        return

    update_user_table(message.from_user.id, chat_type)

    try:
        copied_message = await context.bot.copy_message(
            chat_id=chats['channel'],
            from_chat_id=chats['pinnacle'],
            message_id=reply.id,
            caption='',
        )
    except (TimedOut, NetworkError, RetryAfter) as exc:
        logger.warning('Transient Telegram error while copying pinnacle meme: %s', exc)
    except TelegramError:
        logger.exception('Telegram error while copying pinnacle meme')
    else:
        source_message_id = get_message_id(reply)
        if reply.from_user is None:
            logger.warning(
                'Skipping channel attribution for message_id=%s without author',
                source_message_id,
            )
            return

        store_contributor_profile(reply.from_user)
        forwarded_meme_id = store_forwarded_meme(
            meme_hash=meme_hash,
            submitter_user_id=reply.from_user.id,
            source_chat_type=chat_type,
            source_chat_id=chats['pinnacle'],
            source_message_id=source_message_id,
            destination_chat_type='channel',
            destination_chat_id=chats['channel'],
            destination_message_id=get_message_id(copied_message),
        )
        if forwarded_meme_id is not None:
            subscriber_count = await get_channel_subscriber_count(
                context.bot,
                chats['channel'],
            )
            set_latest_channel_meme(
                chats['channel'],
                forwarded_meme_id,
                subscriber_count,
            )


async def telegram_watchdog(application: Application) -> None:
    while True:
        await asyncio.sleep(WATCHDOG_INTERVAL)
        try:
            await asyncio.wait_for(
                application.bot.get_me(),
                timeout=WATCHDOG_TIMEOUT,
            )
            logger.debug('Telegram watchdog ping ok')
        except RetryAfter as exc:
            logger.warning(
                'Telegram watchdog rate limited, retry after %s seconds',
                exc.retry_after,
            )
        except asyncio.CancelledError:
            raise
        except (TimedOut, NetworkError, TimeoutError) as exc:
            logger.critical(
                'Telegram watchdog network check failed; requesting restart',
                exc_info=True,
            )
            raise RestartRequired('Telegram watchdog network failure') from exc
        except Exception as exc:
            logger.critical(
                'Unexpected Telegram watchdog failure; requesting restart',
                exc_info=True,
            )
            raise RestartRequired('Unexpected Telegram watchdog failure') from exc


def write_heartbeat(phase: str, path: Path = HEARTBEAT_PATH) -> None:
    payload = {
        'boot_id': BOOT_ID,
        'pid': os.getpid(),
        'phase': phase,
        'timestamp': time.time(),
    }
    temporary_path = path.with_name(f'.{path.name}.{os.getpid()}.tmp')
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary_path.write_text(json.dumps(payload), encoding='utf-8')
    os.replace(temporary_path, path)


async def heartbeat_loop(state: dict, path: Path = HEARTBEAT_PATH) -> None:
    while True:
        write_heartbeat(state['phase'], path)
        await asyncio.sleep(HEARTBEAT_INTERVAL)


def build_polling_error_callback(polling_errors: asyncio.Queue):
    def polling_error_callback(error: TelegramError) -> None:
        if isinstance(error, RetryAfter):
            logger.warning(
                'Telegram polling rate limited, retry after %s seconds',
                error.retry_after,
            )
            return

        if polling_errors.empty():
            polling_errors.put_nowait(error)

    return polling_error_callback


async def wait_for_runtime_failure(
    tasks: list[asyncio.Task],
    polling_errors: asyncio.Queue,
    stop_event: asyncio.Event,
) -> None:
    polling_error_task = asyncio.create_task(
        polling_errors.get(),
        name='polling-error-waiter',
    )
    stop_task = asyncio.create_task(stop_event.wait(), name='stop-signal-waiter')
    waiters = {polling_error_task, stop_task}

    try:
        done, _ = await asyncio.wait(
            {*tasks, *waiters},
            return_when=asyncio.FIRST_COMPLETED,
        )

        if stop_task in done:
            return

        if polling_error_task in done:
            error = polling_error_task.result()
            raise RestartRequired(
                f'Telegram polling failed: {type(error).__name__}: {error}'
            ) from error

        for task in tasks:
            if task not in done:
                continue
            try:
                task.result()
            except asyncio.CancelledError as exc:
                raise RestartRequired(
                    f'Background task {task.get_name()} was cancelled unexpectedly'
                ) from exc
            except RestartRequired:
                raise
            except Exception as exc:
                raise RestartRequired(
                    f'Background task {task.get_name()} failed'
                ) from exc
            raise RestartRequired(
                f'Background task {task.get_name()} stopped unexpectedly'
            )
    finally:
        for waiter in waiters:
            waiter.cancel()
        await asyncio.gather(*waiters, return_exceptions=True)


async def stop_application(
    application: Application,
    background_tasks: list[asyncio.Task],
) -> None:
    for task in background_tasks:
        task.cancel()
    await asyncio.gather(*background_tasks, return_exceptions=True)

    if application.updater is not None and application.updater.running:
        await asyncio.wait_for(
            application.updater.stop(),
            timeout=SHUTDOWN_TIMEOUT,
        )
    if application.running:
        await asyncio.wait_for(
            application.stop(),
            timeout=SHUTDOWN_TIMEOUT,
        )


def install_signal_handlers(stop_event: asyncio.Event) -> list[signal.Signals]:
    loop = asyncio.get_running_loop()
    registered = []
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, stop_event.set)
        except (NotImplementedError, RuntimeError):
            logger.warning('Could not register signal handler for %s', sig.name)
        else:
            registered.append(sig)
    return registered


async def run_application(
    application: Application,
    *,
    stop_event: asyncio.Event | None = None,
    heartbeat_path: Path = HEARTBEAT_PATH,
) -> None:
    owns_stop_event = stop_event is None
    stop_event = stop_event or asyncio.Event()
    registered_signals = (
        install_signal_handlers(stop_event) if owns_stop_event else []
    )
    heartbeat_state = {'phase': 'starting'}
    heartbeat_task = asyncio.create_task(
        heartbeat_loop(heartbeat_state, heartbeat_path),
        name='event-loop-heartbeat',
    )
    background_tasks = []
    polling_errors = asyncio.Queue(maxsize=1)

    try:
        async with application:
            try:
                if application.updater is None:
                    raise RuntimeError('Application has no polling updater')

                logger.info('Starting polling boot_id=%s', BOOT_ID)
                await application.updater.start_polling(
                    allowed_updates=Update.ALL_TYPES,
                    timeout=POLLING_TIMEOUT,
                    bootstrap_retries=0,
                    drop_pending_updates=False,
                    error_callback=build_polling_error_callback(polling_errors),
                )
                await application.start()

                heartbeat_state['phase'] = 'running'
                write_heartbeat('running', heartbeat_path)
                logger.info(
                    'Bot started as @%s boot_id=%s',
                    getattr(application.bot, 'username', None),
                    BOOT_ID,
                )
                logger.info(
                    'Configured chats: %s',
                    application.bot_data['chats'],
                )

                background_tasks = [
                    asyncio.create_task(
                        telegram_watchdog(application),
                        name='telegram-watchdog',
                    ),
                    asyncio.create_task(
                        reaction_flush_loop(application),
                        name='reaction-flush',
                    ),
                    asyncio.create_task(
                        subscriber_monitor_loop(application),
                        name='subscriber-monitor',
                    ),
                ]
                await wait_for_runtime_failure(
                    [heartbeat_task, *background_tasks],
                    polling_errors,
                    stop_event,
                )
            finally:
                heartbeat_state['phase'] = 'stopping'
                try:
                    write_heartbeat('stopping', heartbeat_path)
                except OSError:
                    logger.exception('Failed to write stopping heartbeat')
                await stop_application(application, background_tasks)
    finally:
        heartbeat_task.cancel()
        await asyncio.gather(heartbeat_task, return_exceptions=True)
        loop = asyncio.get_running_loop()
        for sig in registered_signals:
            loop.remove_signal_handler(sig)


def main() -> None:

    with open('config.yaml', 'r') as f:
        data = yaml.load(f, yaml.loader.SafeLoader)

    token = data['token']
    cooldown = data['cooldown']
    chats = {
        'pinnacle': data['pinnacle'],
        'channel': data['channel'],
    }

    request = HTTPXRequest(
        connect_timeout=15,
        read_timeout=30,
        write_timeout=30,
        pool_timeout=15,
    )
    polling_request = HTTPXRequest(
        connect_timeout=15,
        read_timeout=POLLING_READ_TIMEOUT,
        write_timeout=30,
        pool_timeout=15,
    )
    application = (
        Application.builder()
        .token(token)
        .request(request)
        .get_updates_request(polling_request)
        .build()
    )
    application.bot_data['cooldown'] = cooldown
    application.bot_data['chats'] = chats
    application.bot_data['pending_reaction_counts'] = {}
    application.bot_data['reaction_lock'] = asyncio.Lock()

    create_tables_if_missing(chats)
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
            print_faq_pinnacle,
            filters=filters.Chat(chat_id=chats['pinnacle']),
        )
    )
    application.add_handler(
        CommandHandler(
            'stats',
            print_stats,
            filters=filters.Chat(chat_id=chats['pinnacle']),
        )
    )
    application.add_handler(
        CommandHandler(
            'top',
            print_top,
            filters=filters.Chat(chat_id=chats['pinnacle']),
        )
    )
    application.add_handler(
        MessageReactionHandler(
            handle_reaction_count,
            message_reaction_types=MessageReactionHandler.MESSAGE_REACTION_COUNT_UPDATED,
        )
    )
    application.add_handler(MessageHandler(filters.COMMAND, log_command), group=1)
    application.add_error_handler(error_handler)

    try:
        asyncio.run(run_application(application))
    except KeyboardInterrupt:
        logger.info('Bot stopped by keyboard interrupt')
    except Exception:
        logger.critical(
            'Bot runtime failed; exiting for container restart',
            exc_info=True,
        )
        raise SystemExit(1)


if __name__ == '__main__':
    main()
