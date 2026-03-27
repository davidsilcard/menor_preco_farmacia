import argparse
import logging
import time

from src.core.config import settings
from src.core.logging import get_logger, log_event
from src.services.operation_jobs import process_next_operation_job

LOGGER = get_logger(__name__)


def _resolved_poll_seconds(poll_seconds: int | None = None) -> int:
    candidate = settings.EMBED_OPERATION_WORKER_POLL_SECONDS if poll_seconds is None else poll_seconds
    return max(int(candidate), 1)


def run_worker_loop(
    *,
    poll_seconds: int | None = None,
    stop_when_idle: bool = False,
    max_jobs: int | None = None,
):
    resolved_poll_seconds = _resolved_poll_seconds(poll_seconds)
    processed_jobs = 0

    log_event(
        LOGGER,
        logging.INFO,
        "operation_worker_started",
        poll_seconds=resolved_poll_seconds,
        stop_when_idle=stop_when_idle,
        max_jobs=max_jobs,
    )

    while True:
        try:
            job = process_next_operation_job()
        except Exception as exc:
            log_event(
                LOGGER,
                logging.ERROR,
                "operation_worker_iteration_failed",
                error_message=str(exc)[:500],
            )
            if stop_when_idle:
                return processed_jobs
            time.sleep(resolved_poll_seconds)
            continue

        if not job:
            if stop_when_idle:
                log_event(LOGGER, logging.INFO, "operation_worker_idle_exit", processed_jobs=processed_jobs)
                return processed_jobs
            time.sleep(resolved_poll_seconds)
            continue

        processed_jobs += 1
        if max_jobs is not None and processed_jobs >= max_jobs:
            log_event(LOGGER, logging.INFO, "operation_worker_max_jobs_reached", processed_jobs=processed_jobs)
            return processed_jobs


def main():
    parser = argparse.ArgumentParser(description="Processa operation jobs continuamente em worker dedicado.")
    parser.add_argument(
        "--poll-seconds",
        type=int,
        default=None,
        help="Intervalo de espera quando nao houver jobs na fila. Usa EMBED_OPERATION_WORKER_POLL_SECONDS por padrao.",
    )
    parser.add_argument("--stop-when-idle", action="store_true", help="Encerra o worker quando a fila ficar vazia.")
    parser.add_argument("--max-jobs", type=int, default=None, help="Encerra apos processar a quantidade informada de jobs.")
    args = parser.parse_args()

    try:
        run_worker_loop(
            poll_seconds=args.poll_seconds,
            stop_when_idle=args.stop_when_idle,
            max_jobs=args.max_jobs,
        )
    except KeyboardInterrupt:
        log_event(LOGGER, logging.INFO, "operation_worker_stopped_by_signal")


if __name__ == "__main__":
    main()
