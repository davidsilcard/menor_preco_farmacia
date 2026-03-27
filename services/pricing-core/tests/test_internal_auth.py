import unittest

from fastapi import HTTPException
from unittest.mock import patch

from src.api.internal_auth import _presented_internal_token, require_internal_api_auth
from src.core.config import settings


class InternalAuthTests(unittest.TestCase):
    def test_presented_internal_token_prefers_explicit_header(self):
        token = _presented_internal_token(
            authorization="Bearer ignored-token",
            x_internal_api_key="expected-token",
        )
        self.assertEqual(token, "expected-token")

    def test_require_internal_api_auth_allows_when_disabled(self):
        with patch.object(settings, "INTERNAL_API_AUTH_ENABLED", False), patch.object(settings, "INTERNAL_API_TOKEN", ""):
            self.assertIsNone(require_internal_api_auth())

    def test_require_internal_api_auth_accepts_bearer_token(self):
        with patch.object(settings, "INTERNAL_API_AUTH_ENABLED", True), patch.object(settings, "INTERNAL_API_TOKEN", "secret-token"):
            self.assertIsNone(require_internal_api_auth(authorization="Bearer secret-token"))

    def test_require_internal_api_auth_rejects_invalid_token(self):
        with patch.object(settings, "INTERNAL_API_AUTH_ENABLED", True), patch.object(settings, "INTERNAL_API_TOKEN", "secret-token"):
            with self.assertRaises(HTTPException) as context:
                require_internal_api_auth(authorization="Bearer wrong-token")

        self.assertEqual(context.exception.status_code, 401)

    def test_require_internal_api_auth_rejects_missing_server_token(self):
        with patch.object(settings, "INTERNAL_API_AUTH_ENABLED", True), patch.object(settings, "INTERNAL_API_TOKEN", ""):
            with self.assertRaises(HTTPException) as context:
                require_internal_api_auth(authorization="Bearer anything")

        self.assertEqual(context.exception.status_code, 503)


if __name__ == "__main__":
    unittest.main()
