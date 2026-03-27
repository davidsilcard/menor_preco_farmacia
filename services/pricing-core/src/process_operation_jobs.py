import argparse

from src.core.logging import get_logger, log_event
from src.services.operation_jobs import process_next_operation_job, process_operation_job

LOGGER = get_logger(__name__)


def main():
    parser = argparse.ArgumentParser(description="Processa operation jobs da fila.")
    parser.add_argument("--job-id", type=int, default=None, help="Processa um operation job especifico.")
    args = parser.parse_args()

    if args.job_id is not None:
        job = process_operation_job(args.job_id)
    else:
        job = process_next_operation_job()

    if not job:
        log_event(LOGGER, 20, "operation_job_cli_no_pending_jobs")
        return

    log_event(LOGGER, 20, "operation_job_cli_processed", operation_job_id=job.id, status=job.status)
    print(
        {
            "operation_job_id": job.id,
            "job_type": job.job_type,
            "status": job.status,
            "finished_at": job.finished_at,
        }
    )


if __name__ == "__main__":
    main()
