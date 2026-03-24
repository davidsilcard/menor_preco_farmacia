import json
import sys
from typing import Any

from src.core.config import settings
from src.models.base import SearchJob, SessionLocal
from src.services.catalog_queries import normalize_cep
from src.services.demand_tracking import search_job_payload
from src.services.tool_models import (
    InvoiceComparisonRequest,
    ObservedItemRequest,
    ReceiptComparisonRequest,
    ShoppingListRequest,
)
from src.services.tool_use import (
    compare_basket_service,
    compare_canonical_product_service,
    compare_invoice_items_service,
    compare_receipt_service,
    compare_shopping_list_service,
    list_review_matches_service,
    search_observed_item_service,
    search_products_service,
)

SERVER_NAME = "super-melhor-preco-farmacia"
SERVER_VERSION = "0.1.0"


def _admin_tools_enabled():
    return settings.MCP_EXPOSE_ADMIN_TOOLS


def _read_message():
    headers = {}

    while True:
        line = sys.stdin.buffer.readline()
        if not line:
            return None
        if line in (b"\r\n", b"\n"):
            break
        key, value = line.decode("utf-8").split(":", 1)
        headers[key.strip().lower()] = value.strip()

    content_length = int(headers.get("content-length", "0"))
    if content_length <= 0:
        return None

    body = sys.stdin.buffer.read(content_length)
    return json.loads(body.decode("utf-8"))


def _write_message(message: dict):
    payload = json.dumps(message, ensure_ascii=False, default=str).encode("utf-8")
    sys.stdout.buffer.write(f"Content-Length: {len(payload)}\r\n\r\n".encode("utf-8"))
    sys.stdout.buffer.write(payload)
    sys.stdout.buffer.flush()


def _success_response(message_id: Any, result: Any):
    return {"jsonrpc": "2.0", "id": message_id, "result": result}


def _error_response(message_id: Any, code: int, message: str):
    return {
        "jsonrpc": "2.0",
        "id": message_id,
        "error": {"code": code, "message": message},
    }


def _tool_definitions():
    tools = [
        {
            "name": "search_products",
            "description": "Busca produtos canonicos e ofertas atuais a partir de um texto livre.",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "query": {"type": "string"},
                    "cep": {"type": "string"},
                },
                "required": ["query", "cep"],
            },
        },
        {
            "name": "compare_shopping_list",
            "description": "Compara uma lista de compras e devolve a melhor oferta por item.",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "cep": {"type": "string"},
                    "items": {
                        "type": "array",
                        "items": {"type": "string"},
                    }
                },
                "required": ["cep", "items"],
            },
        },
        {
            "name": "compare_basket",
            "description": "Alias de compare_shopping_list com foco em total da cesta por farmacia.",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "cep": {"type": "string"},
                    "items": {
                        "type": "array",
                        "items": {"type": "string"},
                    },
                },
                "required": ["cep", "items"],
            },
        },
        {
            "name": "compare_invoice_items",
            "description": "Compara itens ja comprados e calcula economia potencial.",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "cep": {"type": "string"},
                    "items": {
                        "type": "array",
                        "items": {
                            "type": "object",
                            "properties": {
                                "description": {"type": "string"},
                                "paid_price": {"type": "number"},
                                "quantity": {"type": "integer"},
                            },
                            "required": ["description"],
                        },
                    }
                },
                "required": ["cep", "items"],
            },
        },
        {
            "name": "compare_receipt",
            "description": "Compara uma nota inteira e devolve total, cesta e economia potencial.",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "cep": {"type": "string"},
                    "merchant_name": {"type": "string"},
                    "captured_at": {"type": "string"},
                    "items": {
                        "type": "array",
                        "items": {
                            "type": "object",
                            "properties": {
                                "description": {"type": "string"},
                                "paid_price": {"type": "number"},
                                "quantity": {"type": "integer"},
                            },
                            "required": ["description"],
                        },
                    },
                },
                "required": ["cep", "items"],
            },
        },
        {
            "name": "search_observed_item",
            "description": "Busca um item a partir de observacoes extraidas de OCR, caixa ou texto livre.",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "cep": {"type": "string"},
                    "source_type": {"type": "string"},
                    "observations": {
                        "type": "array",
                        "items": {"type": "string"},
                    },
                },
                "required": ["cep", "observations"],
            },
        },
        {
            "name": "compare_canonical_product",
            "description": "Compara as ofertas atuais de um produto canonico entre farmacias.",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "canonical_product_id": {"type": "integer"},
                    "cep": {"type": "string"},
                },
                "required": ["canonical_product_id", "cep"],
            },
        },
        {
            "name": "get_search_job",
            "description": "Consulta o status de um job de busca sob demanda.",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "job_id": {"type": "integer"},
                    "cep": {"type": "string"},
                },
                "required": ["job_id", "cep"],
            },
        },
    ]

    if _admin_tools_enabled():
        tools.extend(
            [
                {
                    "name": "list_review_matches",
                    "description": "Lista matches que ainda precisam de revisao manual.",
                    "inputSchema": {
                        "type": "object",
                        "properties": {
                            "cep": {"type": "string"},
                        },
                        "required": ["cep"],
                    },
                },
                {
                    "name": "list_search_jobs",
                    "description": "Lista os jobs de busca sob demanda mais recentes.",
                    "inputSchema": {
                        "type": "object",
                        "properties": {
                            "cep": {"type": "string"},
                        },
                        "required": ["cep"],
                    },
                },
            ]
        )
    return tools


def _tool_result(payload: Any):
    return {
        "content": [
            {
                "type": "text",
                "text": json.dumps(payload, ensure_ascii=False, default=str),
            }
        ],
        "structuredContent": payload,
    }


def _require(arguments: dict, field_name: str):
    if field_name not in arguments or arguments[field_name] in (None, ""):
        raise ValueError(f"Parametro obrigatorio ausente: {field_name}")
    return arguments[field_name]


def _call_tool(name: str, arguments: dict):
    session = SessionLocal()
    try:
        if name == "search_products":
            result = search_products_service(
                query=_require(arguments, "query"),
                cep=_require(arguments, "cep"),
                db=session,
            )
        elif name == "compare_shopping_list":
            result = compare_shopping_list_service(
                ShoppingListRequest(
                    cep=_require(arguments, "cep"),
                    items=arguments.get("items", []),
                ),
                session,
            )
        elif name == "compare_basket":
            result = compare_basket_service(
                ShoppingListRequest(
                    cep=_require(arguments, "cep"),
                    items=arguments.get("items", []),
                ),
                session,
            )
        elif name == "compare_invoice_items":
            result = compare_invoice_items_service(InvoiceComparisonRequest.model_validate(arguments), session)
        elif name == "compare_receipt":
            result = compare_receipt_service(ReceiptComparisonRequest.model_validate(arguments), session)
        elif name == "search_observed_item":
            result = search_observed_item_service(ObservedItemRequest.model_validate(arguments), session)
        elif name == "compare_canonical_product":
            result = compare_canonical_product_service(
                int(_require(arguments, "canonical_product_id")),
                _require(arguments, "cep"),
                session,
            )
        elif name == "list_review_matches":
            if not _admin_tools_enabled():
                raise ValueError("Tool desabilitada no MCP atual: list_review_matches")
            result = list_review_matches_service(session, cep=_require(arguments, "cep"))
        elif name == "get_search_job":
            requested_cep = normalize_cep(_require(arguments, "cep"))
            job = session.get(SearchJob, int(_require(arguments, "job_id")))
            if not job or job.cep != requested_cep:
                raise ValueError("Search job nao encontrado")
            result = search_job_payload(job, session)
        elif name == "list_search_jobs":
            if not _admin_tools_enabled():
                raise ValueError("Tool desabilitada no MCP atual: list_search_jobs")
            requested_cep = normalize_cep(_require(arguments, "cep"))
            jobs = (
                session.query(SearchJob)
                .filter(SearchJob.cep == requested_cep)
                .order_by(SearchJob.created_at.desc(), SearchJob.id.desc())
                .all()
            )
            result = [search_job_payload(job, session) for job in jobs]
        else:
            raise ValueError(f"Tool desconhecida: {name}")
        return _tool_result(result)
    finally:
        session.close()


def _handle_request(request: dict):
    method = request.get("method")
    message_id = request.get("id")
    params = request.get("params", {})

    if method == "initialize":
        return _success_response(
            message_id,
            {
                "protocolVersion": "2024-11-05",
                "capabilities": {"tools": {"listChanged": False}},
                "serverInfo": {"name": SERVER_NAME, "version": SERVER_VERSION},
            },
        )

    if method == "ping":
        return _success_response(message_id, {})

    if method == "tools/list":
        return _success_response(message_id, {"tools": _tool_definitions()})

    if method == "tools/call":
        try:
            tool_name = params["name"]
            arguments = params.get("arguments", {})
            return _success_response(message_id, _call_tool(tool_name, arguments))
        except Exception as exc:
            return _error_response(message_id, -32000, str(exc))

    if method == "notifications/initialized":
        return None

    return _error_response(message_id, -32601, f"Método não suportado: {method}")


def main():
    while True:
        request = _read_message()
        if request is None:
            break
        response = _handle_request(request)
        if response is not None and request.get("id") is not None:
            _write_message(response)


if __name__ == "__main__":
    main()
