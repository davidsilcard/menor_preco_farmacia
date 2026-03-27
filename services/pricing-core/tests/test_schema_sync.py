import unittest

from src.init_db import _schema_patch_statements


class SchemaSyncTests(unittest.TestCase):
    def test_schema_patch_statements_includes_missing_catalog_request_resolution_source(self):
        statements = _schema_patch_statements(
            {"catalog_requests", "search_jobs"},
            {
                "catalog_requests": {"id", "query", "normalized_query", "cep", "status"},
                "search_jobs": {"id", "query", "normalized_query", "cep", "status"},
            },
        )

        self.assertIn(
            "ALTER TABLE catalog_requests ADD COLUMN resolution_source VARCHAR",
            statements,
        )

    def test_schema_patch_statements_skips_columns_that_already_exist(self):
        statements = _schema_patch_statements(
            {"catalog_requests"},
            {
                "catalog_requests": {
                    "id",
                    "query",
                    "normalized_query",
                    "cep",
                    "status",
                    "resolution_source",
                    "last_requested_by_tool",
                }
            },
        )

        self.assertEqual(statements, [])


if __name__ == "__main__":
    unittest.main()
