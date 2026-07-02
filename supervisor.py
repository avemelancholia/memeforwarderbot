#!/usr/bin/env python3
import argparse
import json
import logging
import os
import signal
import subprocess
import sys
import time
import uuid
from pathlib import Path


logging.basicConfig(
    format='%(asctime)s - supervisor - %(levelname)s - %(message)s',
    level=logging.INFO,
)
logger = logging.getLogger(__name__)

APP_DIR = Path(__file__).resolve().parent
HEARTBEAT_PATH = Path(
    os.environ.get('BOT_HEARTBEAT_PATH', '/tmp/meme-forwarder-heartbeat.json')
)
MONITOR_INTERVAL = 1
HEARTBEAT_TIMEOUT = 30
INITIAL_HEARTBEAT_TIMEOUT = 30
STARTUP_TIMEOUT = 90
STOPPING_TIMEOUT = 25
TERMINATION_GRACE = 5
MIN_CONTAINER_UPTIME = 11


def read_heartbeat(path: Path = HEARTBEAT_PATH):
    try:
        payload = json.loads(path.read_text(encoding='utf-8'))
        if not isinstance(payload, dict):
            return None
        if not isinstance(payload.get('pid'), int):
            return None
        if not isinstance(payload.get('timestamp'), (int, float)):
            return None
        if payload.get('phase') not in {'starting', 'running', 'stopping'}:
            return None
        if not isinstance(payload.get('boot_id'), str):
            return None
        return payload
    except (FileNotFoundError, OSError, ValueError, TypeError):
        return None


def process_exists(pid: int) -> bool:
    try:
        os.kill(pid, 0)
    except (ProcessLookupError, PermissionError):
        return False
    return True


def heartbeat_is_healthy(
    payload,
    *,
    now: float | None = None,
    timeout: float = HEARTBEAT_TIMEOUT,
) -> bool:
    if payload is None or payload.get('phase') != 'running':
        return False
    now = time.time() if now is None else now
    age = now - payload['timestamp']
    return -5 <= age <= timeout and process_exists(payload['pid'])


def remove_old_heartbeat(path: Path) -> None:
    try:
        path.unlink()
    except FileNotFoundError:
        pass


def terminate_child(child: subprocess.Popen, grace: float = TERMINATION_GRACE) -> None:
    if child.poll() is not None:
        return

    logger.warning('Stopping bot child pid=%s', child.pid)
    try:
        os.killpg(child.pid, signal.SIGTERM)
    except ProcessLookupError:
        return

    deadline = time.monotonic() + grace
    while child.poll() is None and time.monotonic() < deadline:
        time.sleep(0.1)

    if child.poll() is None:
        logger.error('Bot child did not stop in %.1fs; sending SIGKILL', grace)
        try:
            os.killpg(child.pid, signal.SIGKILL)
        except ProcessLookupError:
            return
        child.wait(timeout=grace)


def wait_for_restart_policy_window(
    started_at: float,
    stop_requested,
) -> bool:
    while time.monotonic() - started_at < MIN_CONTAINER_UPTIME:
        if stop_requested():
            return False
        time.sleep(0.1)
    return True


def run_supervisor() -> int:
    remove_old_heartbeat(HEARTBEAT_PATH)
    boot_id = uuid.uuid4().hex
    child_environment = os.environ.copy()
    child_environment['BOT_BOOT_ID'] = boot_id
    child_environment['BOT_HEARTBEAT_PATH'] = str(HEARTBEAT_PATH)

    child = subprocess.Popen(
        [sys.executable, '-u', 'bot.py'],
        cwd=APP_DIR,
        env=child_environment,
        start_new_session=True,
    )
    started_at = time.monotonic()
    requested_signal = [None]
    running_seen = False
    stopping_seen_at = None

    def request_stop(signum, _frame):
        requested_signal[0] = signum

    previous_handlers = {}
    for sig in (signal.SIGINT, signal.SIGTERM):
        previous_handlers[sig] = signal.signal(sig, request_stop)

    logger.info('Started bot child pid=%s boot_id=%s', child.pid, boot_id)

    try:
        while True:
            if requested_signal[0] is not None:
                logger.info(
                    'Received %s; forwarding shutdown to child',
                    signal.Signals(requested_signal[0]).name,
                )
                terminate_child(child)
                return 0

            return_code = child.poll()
            if return_code is not None:
                if not wait_for_restart_policy_window(
                    started_at,
                    lambda: requested_signal[0] is not None,
                ):
                    return 0
                restart_code = return_code if return_code > 0 else 1
                logger.critical(
                    'Bot child exited unexpectedly status=%s; '
                    'exiting status=%s for Docker restart',
                    return_code,
                    restart_code,
                )
                return restart_code

            payload = read_heartbeat(HEARTBEAT_PATH)
            elapsed = time.monotonic() - started_at
            valid_heartbeat = (
                payload is not None
                and payload['pid'] == child.pid
                and payload['boot_id'] == boot_id
            )

            if valid_heartbeat:
                age = time.time() - payload['timestamp']
                if age < -5 or age > HEARTBEAT_TIMEOUT:
                    logger.critical(
                        'Bot heartbeat is stale age=%.1fs phase=%s',
                        age,
                        payload['phase'],
                    )
                    terminate_child(child)
                    wait_for_restart_policy_window(started_at, lambda: False)
                    return 1

                if payload['phase'] == 'running':
                    running_seen = True
                    stopping_seen_at = None
                elif payload['phase'] == 'stopping':
                    stopping_seen_at = stopping_seen_at or time.monotonic()
                    if time.monotonic() - stopping_seen_at > STOPPING_TIMEOUT:
                        logger.critical('Bot shutdown exceeded %.1fs', STOPPING_TIMEOUT)
                        terminate_child(child)
                        wait_for_restart_policy_window(started_at, lambda: False)
                        return 1
            elif elapsed > INITIAL_HEARTBEAT_TIMEOUT:
                logger.critical(
                    'Bot did not produce a valid heartbeat within %.1fs',
                    INITIAL_HEARTBEAT_TIMEOUT,
                )
                terminate_child(child)
                wait_for_restart_policy_window(started_at, lambda: False)
                return 1

            if not running_seen and elapsed > STARTUP_TIMEOUT:
                logger.critical(
                    'Bot did not reach running state within %.1fs',
                    STARTUP_TIMEOUT,
                )
                terminate_child(child)
                wait_for_restart_policy_window(started_at, lambda: False)
                return 1

            time.sleep(MONITOR_INTERVAL)
    finally:
        for sig, handler in previous_handlers.items():
            signal.signal(sig, handler)


def healthcheck() -> int:
    payload = read_heartbeat(HEARTBEAT_PATH)
    return 0 if heartbeat_is_healthy(payload) else 1


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '--healthcheck',
        action='store_true',
        help='Return success only for a fresh, running bot heartbeat.',
    )
    args = parser.parse_args()
    if args.healthcheck:
        return healthcheck()

    try:
        return run_supervisor()
    except Exception:
        logger.exception('Supervisor failed')
        return 1


if __name__ == '__main__':
    raise SystemExit(main())
