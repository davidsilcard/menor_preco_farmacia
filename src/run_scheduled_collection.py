import argparse

from src.services.scheduled_collection import build_scheduled_collection_plan, run_scheduled_collection


def main():
    parser = argparse.ArgumentParser(description="Planeja ou executa coleta agendada por CEP.")
    parser.add_argument("--cep", type=str, default=None, help="Limita a coleta a um CEP especifico.")
    parser.add_argument("--plan-only", action="store_true", help="Mostra apenas o plano atual.")
    args = parser.parse_args()

    if args.plan_only:
        print(build_scheduled_collection_plan(args.cep))
        return

    print(run_scheduled_collection(args.cep))


if __name__ == "__main__":
    main()
