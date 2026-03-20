import argparse
from pprint import pprint

from src.services.operational_cycle import collection_schedule_status, run_operational_cycle


def main():
    parser = argparse.ArgumentParser(description="Executa o ciclo operacional de coleta e retencao.")
    parser.add_argument("--cep", type=str, default=None, help="Limita o ciclo a um CEP especifico.")
    parser.add_argument("--schedule-only", action="store_true", help="Mostra apenas o status do agendamento.")
    parser.add_argument("--force-collection", action="store_true", help="Executa a coleta mesmo fora da janela configurada.")
    args = parser.parse_args()

    if args.schedule_only:
        pprint(collection_schedule_status())
        return

    pprint(run_operational_cycle(cep=args.cep, force_collection=args.force_collection))


if __name__ == "__main__":
    main()
