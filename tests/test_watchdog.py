import asyncio
import unittest
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

import bot
from telegram.error import NetworkError, RetryAfter


class WatchdogTests(unittest.IsolatedAsyncioTestCase):
    async def test_first_watchdog_timeout_exits_for_restart(self):
        application = SimpleNamespace(
            bot=SimpleNamespace(get_me=AsyncMock(side_effect=TimeoutError()))
        )

        with (
            patch.object(bot.asyncio, 'sleep', new=AsyncMock(return_value=None)),
            patch.object(
                bot.os,
                '_exit',
                side_effect=SystemExit(1),
            ) as exit_mock,
        ):
            with self.assertRaises(SystemExit):
                await bot.telegram_watchdog(application)

        exit_mock.assert_called_once_with(1)
        application.bot.get_me.assert_awaited_once_with()

    async def test_successful_watchdog_probe_continues(self):
        application = SimpleNamespace(
            bot=SimpleNamespace(get_me=AsyncMock(return_value=object()))
        )
        sleep = AsyncMock(side_effect=[None, asyncio.CancelledError()])

        with (
            patch.object(bot.asyncio, 'sleep', new=sleep),
            patch.object(bot.os, '_exit') as exit_mock,
        ):
            with self.assertRaises(asyncio.CancelledError):
                await bot.telegram_watchdog(application)

        exit_mock.assert_not_called()
        application.bot.get_me.assert_awaited_once_with()

    async def test_watchdog_rate_limit_is_not_fatal(self):
        application = SimpleNamespace(
            bot=SimpleNamespace(get_me=AsyncMock(side_effect=RetryAfter(5)))
        )
        sleep = AsyncMock(side_effect=[None, asyncio.CancelledError()])

        with (
            patch.object(bot.asyncio, 'sleep', new=sleep),
            patch.object(bot.os, '_exit') as exit_mock,
        ):
            with self.assertRaises(asyncio.CancelledError):
                await bot.telegram_watchdog(application)

        exit_mock.assert_not_called()
        application.bot.get_me.assert_awaited_once_with()


class ErrorHandlerTests(unittest.IsolatedAsyncioTestCase):
    async def test_polling_network_error_exits_for_restart(self):
        context = SimpleNamespace(error=NetworkError('polling failed'))

        with patch.object(
            bot.os,
            '_exit',
            side_effect=SystemExit(1),
        ) as exit_mock:
            with self.assertRaises(SystemExit):
                await bot.error_handler(None, context)

        exit_mock.assert_called_once_with(1)

    async def test_handler_network_error_remains_non_fatal(self):
        context = SimpleNamespace(error=NetworkError('handler failed'))

        with patch.object(bot.os, '_exit') as exit_mock:
            await bot.error_handler(object(), context)

        exit_mock.assert_not_called()


if __name__ == '__main__':
    unittest.main()
