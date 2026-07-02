import json
import signal
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock, patch

import supervisor


class HeartbeatHealthTests(unittest.TestCase):
    def test_fresh_running_heartbeat_is_healthy(self):
        payload = {
            'boot_id': 'boot',
            'pid': 123,
            'phase': 'running',
            'timestamp': 100,
        }

        with patch.object(supervisor, 'process_exists', return_value=True):
            self.assertTrue(
                supervisor.heartbeat_is_healthy(
                    payload,
                    now=110,
                    timeout=30,
                )
            )

    def test_stale_heartbeat_is_unhealthy(self):
        payload = {
            'boot_id': 'boot',
            'pid': 123,
            'phase': 'running',
            'timestamp': 100,
        }

        with patch.object(supervisor, 'process_exists', return_value=True):
            self.assertFalse(
                supervisor.heartbeat_is_healthy(
                    payload,
                    now=131,
                    timeout=30,
                )
            )

    def test_read_heartbeat_rejects_invalid_phase(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / 'heartbeat.json'
            path.write_text(
                json.dumps(
                    {
                        'boot_id': 'boot',
                        'pid': 123,
                        'phase': 'unknown',
                        'timestamp': 100,
                    }
                ),
                encoding='utf-8',
            )

            self.assertIsNone(supervisor.read_heartbeat(path))


class SupervisorTests(unittest.TestCase):
    def test_unexpected_child_exit_returns_failure(self):
        child = Mock(pid=123)
        child.poll.return_value = 7

        with (
            patch.object(supervisor.subprocess, 'Popen', return_value=child),
            patch.object(supervisor, 'remove_old_heartbeat'),
            patch.object(supervisor, 'MIN_CONTAINER_UPTIME', 0),
            patch.object(supervisor.signal, 'signal', return_value=signal.SIG_DFL),
        ):
            self.assertEqual(supervisor.run_supervisor(), 7)

    def test_stale_heartbeat_terminates_child(self):
        child = Mock(pid=123)
        child.poll.return_value = None
        heartbeat = {
            'boot_id': 'fixed-boot',
            'pid': 123,
            'phase': 'running',
            'timestamp': 100,
        }

        with (
            patch.object(supervisor.subprocess, 'Popen', return_value=child),
            patch.object(supervisor, 'remove_old_heartbeat'),
            patch.object(supervisor, 'read_heartbeat', return_value=heartbeat),
            patch.object(supervisor, 'terminate_child') as terminate_child,
            patch.object(supervisor, 'wait_for_restart_policy_window'),
            patch.object(
                supervisor.uuid,
                'uuid4',
                return_value=SimpleNamespace(hex='fixed-boot'),
            ),
            patch.object(supervisor.time, 'monotonic', return_value=200),
            patch.object(supervisor.time, 'time', return_value=200),
            patch.object(supervisor.signal, 'signal', return_value=signal.SIG_DFL),
        ):
            self.assertEqual(supervisor.run_supervisor(), 1)

        terminate_child.assert_called_once_with(child)

    def test_hung_child_is_force_killed_after_grace(self):
        child = Mock(pid=123)
        child.poll.return_value = None

        with (
            patch.object(supervisor.os, 'killpg') as killpg,
            patch.object(supervisor.time, 'monotonic', side_effect=[0, 10]),
        ):
            supervisor.terminate_child(child, grace=1)

        self.assertEqual(
            killpg.call_args_list,
            [
                unittest.mock.call(123, signal.SIGTERM),
                unittest.mock.call(123, signal.SIGKILL),
            ],
        )
        child.wait.assert_called_once_with(timeout=1)


if __name__ == '__main__':
    unittest.main()
