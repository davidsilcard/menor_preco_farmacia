import argparse
import json
from pathlib import Path

from sqlalchemy.orm import Session

from src.core.config import settings
from src.models.base import engine
from src.services.reference_data import import_reference_data


def _default_file_path(file_name: str) -> str:
    return str(Path(settings.REFERENCE_DATA_DIR) / file_name)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Importa ou atualiza dados regulatorios e de CMED.")
    parser.add_argument("--regulatory-file", default=_default_file_path("regulatory_products.csv"))
    parser.add_argument("--dcb-file", default=_default_file_path("dcb_aliases.csv"))
    parser.add_argument("--cmed-file", default=_default_file_path("cmed_prices.csv"))
    parser.add_argument("--skip-regulatory", action="store_true")
    parser.add_argument("--skip-dcb", action="store_true")
    parser.add_argument("--skip-cmed", action="store_true")
    parser.add_argument("--replace", action="store_true", help="Substitui integralmente os datasets informados antes de importar.")
    args = parser.parse_args()

    with Session(engine) as session:
        summaries = import_reference_data(
            session,
            regulatory_file=None if args.skip_regulatory else args.regulatory_file,
            dcb_file=None if args.skip_dcb else args.dcb_file,
            cmed_file=None if args.skip_cmed else args.cmed_file,
            replace=args.replace,
        )

    print(
        json.dumps(
            [
                {
                    "dataset": summary.dataset,
                    "file_path": summary.file_path,
                    "rows_read": summary.rows_read,
                    "created": summary.created,
                    "updated": summary.updated,
                    "skipped": summary.skipped,
                    "replaced": summary.replaced,
                }
                for summary in summaries
            ],
            ensure_ascii=True,
        )
    )
