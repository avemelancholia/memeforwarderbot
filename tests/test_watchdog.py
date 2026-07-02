import asyncio
import json
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

import bot
from telegram.error import NetworkError, RetryAfter


class WatchdogTests(unittest.IsolatedAsyncioTestCase):
    async def test_first_watchdog_timeout_requests_restart(self):
        application = SimpleNamespace(
            bot=SimpleNamespace(get_me=AsyncMock(side_effect=TimeoutError()))
        )

        with patch.object(
            bot.asyncio,
            'sleep',
            new=AsyncMock(return_value=None),
        ):
            with self.assertRaises(bot.RestartRequired):
                await bot.telegram_watchdog(application)

        application.bot.get_me.assert_awaited_once_with()

    async def test_successful_watchdog_probe_continues(self):
        application = SimpleNamespace(
            bot=SimpleNamespace(get_me=AsyncMock(return_value=object()))
        )
        sleep = AsyncMock(side_effect=[None, asyncio.CancelledError()])

        with patch.object(bot.asyncio, 'sleep', new=sleep):
            with self.assertRaises(asyncio.CancelledError):
                await bot.telegram_watchdog(application)

        application.bot.get_me.assert_awaited_once_with()

    async def test_watchdog_rate_limit_is_not_fatal(self):
        application = SimpleNamespace(
            bot=SimpleNamespace(get_me=AsyncMock(side_effect=RetryAfter(5)))
        )
        sleep = AsyncMock(side_effect=[None, asyncio.CancelledError()])

        with patch.object(bot.asyncio, 'sleep', new=sleep):
            with self.assertRaises(asyncio.CancelledError):
                await bot.telegram_watchdog(application)

        application.bot.get_me.assert_awaited_once_with()


class RuntimeFailureTests(unittest.IsolatedAsyncioTestCase):
    async def test_polling_network_error_requests_restart(self):
        polling_errors = asyncio.Queue(maxsize=1)
        callback = bot.build_polling_error_callback(polling_errors)
        error = NetworkError('polling failed')

        callback(error)

        self.assertIs(polling_errors.get_nowait(), error)

    async def test_polling_rate_limit_is_not_fatal(self):
        polling_errors = asyncio.Queue(maxsize=1)
        callback = bot.build_polling_error_callback(polling_errors)

        callback(RetryAfter(5))

        self.assertTrue(polling_errors.empty())

    async def test_polling_error_stops_runtime(self):
        polling_errors = asyncio.Queue(maxsize=1)
        await polling_errors.put(NetworkError('polling failed'))
        worker = asyncio.create_task(
            asyncio.sleep(3600),
            name='test-worker',
        )

        try:
            with self.assertRaisesRegex(
                bot.RestartRequired,
                'Telegram polling failed',
            ):
                await bot.wait_for_runtime_failure(
                    [worker],
                    polling_errors,
                    asyncio.Event(),
                )
        finally:
            worker.cancel()
            await asyncio.gather(worker, return_exceptions=True)

    async def test_background_task_failure_stops_runtime(self):
        async def fail():
            raise ValueError('broken worker')

        worker = asyncio.create_task(fail(), name='test-worker')

        with self.assertRaisesRegex(
            bot.RestartRequired,
            'Background task test-worker failed',
        ):
            await bot.wait_for_runtime_failure(
                [worker],
                asyncio.Queue(maxsize=1),
                asyncio.Event(),
            )


class ApplicationLifecycleTests(unittest.IsolatedAsyncioTestCase):
    async def test_runtime_starts_tasks_after_application(self):
        events = []

        class FakeUpdater:
            running = False

            async def start_polling(self, **kwargs):
                events.append('updater.start')
                self.running = True
                self.options = kwargs

            async def stop(self):
                events.append('updater.stop')
                self.running = False

        class FakeApplication:
            def __init__(self):
                self.updater = FakeUpdater()
                self.running = False
                self.bot = SimpleNamespace(username='test_bot')
                self.bot_data = {'chats': {}}

            async def __aenter__(self):
                events.append('application.initialize')
                return self

            async def __aexit__(self, *_args):
                events.append('application.shutdown')

            async def start(self):
                events.append('application.start')
                self.running = True

            async def stop(self):
                events.append('application.stop')
                self.running = False

        async def parked_task(_application):
            await asyncio.Event().wait()

        application = FakeApplication()
        stop_event = asyncio.Event()
        stop_event.set()

        with tempfile.TemporaryDirectory() as directory:
            heartbeat_path = Path(directory) / 'heartbeat.json'
            with (
                patch.object(bot, 'telegram_watchdog', new=parked_task),
                patch.object(bot, 'reaction_flush_loop', new=parked_task),
                patch.object(bot, 'subscriber_monitor_loop', new=parked_task),
            ):
                await bot.run_application(
                    application,
                    stop_event=stop_event,
                    heartbeat_path=heartbeat_path,
                )

        self.assertEqual(
            events,
            [
                'application.initialize',
                'updater.start',
                'application.start',
                'updater.stop',
                'application.stop',
                'application.shutdown',
            ],
        )
        self.assertEqual(
            application.updater.options['timeout'],
            bot.POLLING_TIMEOUT,
        )
        self.assertIn('error_callback', application.updater.options)


class ErrorHandlerTests(unittest.IsolatedAsyncioTestCase):
    async def test_handler_network_error_remains_non_fatal(self):
        context = SimpleNamespace(error=NetworkError('handler failed'))

        await bot.error_handler(object(), context)


class HeartbeatTests(unittest.TestCase):
    def test_write_heartbeat_is_valid_running_state(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / 'heartbeat.json'

            bot.write_heartbeat('running', path)

            payload = json.loads(path.read_text(encoding='utf-8'))
            self.assertEqual(payload['phase'], 'running')
            self.assertEqual(payload['pid'], bot.os.getpid())
            self.assertEqual(payload['boot_id'], bot.BOOT_ID)


if __name__ == '__main__':
    unittest.main()
