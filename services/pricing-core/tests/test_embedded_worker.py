import unittest
from unittest.mock import patch

from fastapi import FastAPI

import src.main as main_module


class _FakeStopEvent:
    def __init__(self):
        self.set_called = False

    def set(self):
        self.set_called = True


class _FakeThread:
    def __init__(self, target=None, args=(), daemon=None, name=None):
        self.target = target
        self.args = args
        self.daemon = daemon
        self.name = name
        self.started = False
        self.join_timeout = None
        self._alive = False

    def start(self):
        self.started = True
        self._alive = True

    def join(self, timeout=None):
        self.join_timeout = timeout
        self._alive = False

    def is_alive(self):
        return self._alive


class EmbeddedWorkerTests(unittest.TestCase):
    def test_start_embedded_operation_worker_skips_when_disabled(self):
        app = FastAPI()
        app.state.enable_embedded_operation_worker = False

        self.assertIsNone(main_module._start_embedded_operation_worker(app))
        self.assertIsNone(getattr(app.state, "embedded_operation_worker_thread", None))

    def test_start_embedded_operation_worker_starts_daemon_thread(self):
        app = FastAPI()
        app.state.enable_embedded_operation_worker = True

        with patch.object(main_module.threading, "Event", return_value=_FakeStopEvent()) as event_mock:
            with patch.object(main_module.threading, "Thread", side_effect=_FakeThread) as thread_mock:
                worker_thread = main_module._start_embedded_operation_worker(app)

        self.assertIs(worker_thread, app.state.embedded_operation_worker_thread)
        self.assertTrue(worker_thread.started)
        self.assertTrue(worker_thread.daemon)
        self.assertEqual(worker_thread.name, "embedded-operation-worker")
        event_mock.assert_called_once()
        thread_mock.assert_called_once()

    def test_stop_embedded_operation_worker_signals_and_clears_state(self):
        app = FastAPI()
        stop_event = _FakeStopEvent()
        worker_thread = _FakeThread()
        worker_thread.start()
        app.state.embedded_operation_worker_stop_event = stop_event
        app.state.embedded_operation_worker_thread = worker_thread

        with patch.object(main_module, "_embedded_worker_poll_seconds", return_value=3):
            main_module._stop_embedded_operation_worker(app)

        self.assertTrue(stop_event.set_called)
        self.assertEqual(worker_thread.join_timeout, 4)
        self.assertIsNone(app.state.embedded_operation_worker_stop_event)
        self.assertIsNone(app.state.embedded_operation_worker_thread)


if __name__ == "__main__":
    unittest.main()
