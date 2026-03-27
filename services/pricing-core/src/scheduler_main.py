import argparse
from pprint import pprint

from src.core.logging import get_logger, log_event
from src.services.operational_cycle import collection_schedule_status, run_operational_cycle

LOGGER = get_logger(__name__)


def main():
    parser = argparse.ArgumentParser(description="Executa o ciclo operacional do pricing-core para uso em cron/scheduler.")
    parser.add_argument("--cep", type=str, default=None, help="Limita a execucao a um CEP especifico.")
    parser.add_argument("--schedule-only", action="store_true", help="Mostra apenas o status atual do agendamento.")
    parser.add_argument("--force-collection", action="store_true", help="Executa a coleta mesmo fora da janela configurada.")
    args = parser.parse_args()

    if args.schedule_only:
        log_event(LOGGER, 20, "pricing_core_scheduler_status_requested", cep=args.cep)
        pprint(collection_schedule_status())
        return

    log_event(
        LOGGER,
        20,
        "pricing_core_scheduler_cycle_requested",
        cep=args.cep,
        force_collection=args.force_collection,
    )
    pprint(run_operational_cycle(cep=args.cep, force_collection=args.force_collection))


if __name__ == "__main__":
    main()
