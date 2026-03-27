import json
import unittest
from pathlib import Path
from shutil import rmtree
from uuid import uuid4

from sqlalchemy import create_engine
from sqlalchemy.orm import Session

from src.models.base import Base, CmedPriceEntry, RegulatoryAlias, RegulatoryProduct
from src.services.reference_data import import_cmed_prices, import_dcb_aliases, import_regulatory_products


class ReferenceDataImportTests(unittest.TestCase):
    def setUp(self):
        self.engine = create_engine("sqlite:///:memory:")
        Base.metadata.create_all(bind=self.engine)
        self.base_path = Path("tests") / f".tmp_reference_data_{uuid4().hex}"
        self.base_path.mkdir(parents=True, exist_ok=True)

    def tearDown(self):
        if self.base_path.exists():
            rmtree(self.base_path, ignore_errors=True)

    def test_import_regulatory_products_is_idempotent(self):
        file_path = self.base_path / "regulatory_products.json"
        file_path.write_text(
            json.dumps(
                [
                    {
                        "product_name": "Jardiance 25mg 30 Comprimidos",
                        "dcb_name": "empagliflozina",
                        "ean_gtin": "7891234567890",
                        "anvisa_code": "123456789",
                        "manufacturer": "Boehringer",
                    }
                ]
            ),
            encoding="utf-8",
        )

        with Session(self.engine) as session:
            first = import_regulatory_products(session, file_path)
            second = import_regulatory_products(session, file_path)
            rows = session.query(RegulatoryProduct).all()

        self.assertEqual(first.created, 1)
        self.assertEqual(second.updated, 1)
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0].dcb_name, "empagliflozina")

    def test_import_dcb_aliases_replace_reloads_dataset(self):
        first_file = self.base_path / "dcb_aliases_first.csv"
        first_file.write_text("dcb_name,alias\nempagliflozina,Jardiance\n", encoding="utf-8")
        second_file = self.base_path / "dcb_aliases_second.csv"
        second_file.write_text("dcb_name,alias\nmetamizol sodico,Dipirona\n", encoding="utf-8")

        with Session(self.engine) as session:
            import_dcb_aliases(session, first_file)
            summary = import_dcb_aliases(session, second_file, replace=True)
            rows = session.query(RegulatoryAlias).all()

        self.assertTrue(summary.replaced)
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0].dcb_name, "metamizol sodico")

    def test_import_cmed_prices_upserts_by_fingerprint(self):
        file_path = self.base_path / "cmed_prices.csv"
        file_path.write_text(
            "\n".join(
                [
                    "product_name,presentation,laboratory,ean_gtin,anvisa_code,pmc_price",
                    "Buscopan Composto,20 comprimidos,Boehringer,7896094921399,123456789,23.90",
                ]
            ),
            encoding="utf-8",
        )

        with Session(self.engine) as session:
            first = import_cmed_prices(session, file_path)
            second = import_cmed_prices(session, file_path)
            rows = session.query(CmedPriceEntry).all()

        self.assertEqual(first.created, 1)
        self.assertEqual(second.updated, 1)
        self.assertEqual(len(rows), 1)


if __name__ == "__main__":
    unittest.main()
