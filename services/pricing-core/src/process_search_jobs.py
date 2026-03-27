import argparse

from src.core.logging import get_logger, log_event
from src.services.search_jobs import process_next_search_job, process_search_job

LOGGER = get_logger(__name__)


def main():
    parser = argparse.ArgumentParser(description="Processa jobs de busca sob demanda.")
    parser.add_argument("--job-id", type=int, default=None, help="Processa um job especifico.")
    parser.add_argument("--once", action="store_true", help="Processa apenas um job da fila.")
    args = parser.parse_args()

    if args.job_id is not None:
        job = process_search_job(args.job_id)
    else:
        job = process_next_search_job()

    if not job:
        log_event(LOGGER, 20, "search_job_cli_no_pending_jobs")
        return

    log_event(LOGGER, 20, "search_job_cli_processed", search_job_id=job.id, status=job.status)
    print(
        {
            "job_id": job.id,
            "status": job.status,
            "query": job.query,
            "finished_at": job.finished_at,
        }
    )


if __name__ == "__main__":
    main()
