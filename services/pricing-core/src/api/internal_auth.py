import secrets

from fastapi import Header, HTTPException, status

from src.core.config import settings

INTERNAL_API_KEY_HEADER = "X-Internal-API-Key"


def _presented_internal_token(authorization: str | None = None, x_internal_api_key: str | None = None):
    if isinstance(x_internal_api_key, str) and x_internal_api_key.strip():
        return x_internal_api_key.strip()

    if not isinstance(authorization, str) or not authorization:
        return None

    scheme, _, credentials = authorization.partition(" ")
    if scheme.lower() != "bearer" or not credentials.strip():
        return None
    return credentials.strip()


def require_internal_api_auth(
    authorization: str | None = Header(None),
    x_internal_api_key: str | None = Header(None, alias=INTERNAL_API_KEY_HEADER),
):
    if not settings.INTERNAL_API_AUTH_ENABLED:
        return

    expected_token = (settings.INTERNAL_API_TOKEN or "").strip()
    if not expected_token:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Autenticacao interna habilitada sem INTERNAL_API_TOKEN configurado.",
        )

    presented_token = _presented_internal_token(
        authorization=authorization,
        x_internal_api_key=x_internal_api_key,
    )
    if not presented_token or not secrets.compare_digest(presented_token, expected_token):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Credenciais internas invalidas.",
            headers={"WWW-Authenticate": "Bearer"},
        )
