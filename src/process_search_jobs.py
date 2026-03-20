import argparse

from src.services.search_jobs import process_next_search_job, process_search_job


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
        print("Nenhum search job pendente na fila.")
        return

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
