import unittest
from unittest.mock import patch

from src import worker_main


class WorkerMainTests(unittest.TestCase):
    def test_resolved_poll_seconds_uses_minimum_of_one_second(self):
        self.assertEqual(worker_main._resolved_poll_seconds(0), 1)

    def test_run_worker_loop_exits_when_idle_if_requested(self):
        with patch.object(worker_main, "process_next_operation_job", return_value=None):
            processed = worker_main.run_worker_loop(stop_when_idle=True)

        self.assertEqual(processed, 0)

    def test_run_worker_loop_honors_max_jobs(self):
        jobs = [object(), object()]
        with patch.object(worker_main, "process_next_operation_job", side_effect=jobs):
            processed = worker_main.run_worker_loop(max_jobs=2)

        self.assertEqual(processed, 2)


if __name__ == "__main__":
    unittest.main()
