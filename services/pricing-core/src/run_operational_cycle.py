import argparse
from pprint import pprint

from src.core.logging import get_logger, log_event
from src.services.operational_cycle import collection_schedule_status, run_operational_cycle

LOGGER = get_logger(__name__)


def main():
    parser = argparse.ArgumentParser(description="Executa o ciclo operacional de coleta e retencao.")
    parser.add_argument("--cep", type=str, default=None, help="Limita o ciclo a um CEP especifico.")
    parser.add_argument("--schedule-only", action="store_true", help="Mostra apenas o status do agendamento.")
    parser.add_argument("--force-collection", action="store_true", help="Executa a coleta mesmo fora da janela configurada.")
    args = parser.parse_args()

    if args.schedule_only:
        log_event(LOGGER, 20, "operational_cycle_schedule_requested")
        pprint(collection_schedule_status())
        return

    log_event(
        LOGGER,
        20,
        "operational_cycle_cli_requested",
        cep=args.cep,
        force_collection=args.force_collection,
    )
    pprint(run_operational_cycle(cep=args.cep, force_collection=args.force_collection))


if __name__ == "__main__":
    main()
