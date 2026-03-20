import json
import sys
from typing import Any

from src.main import (
    InvoiceComparisonRequest,
    ObservedItemRequest,
    ReceiptComparisonRequest,
    ShoppingListRequest,
    compare_single_canonical_product,
    get_search_job,
    list_pending_reviews,
    list_search_jobs,
    tool_compare_basket,
    tool_compare_invoice_items,
    tool_compare_receipt,
    tool_compare_shopping_list,
    tool_search_observed_item,
    tool_search_products,
)
from src.models.base import SessionLocal

SERVER_NAME = "super-melhor-preco-farmacia"
SERVER_VERSION = "0.1.0"


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
    return [
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
                },
                "required": ["canonical_product_id"],
            },
        },
        {
            "name": "list_review_matches",
            "description": "Lista matches que ainda precisam de revisao manual.",
            "inputSchema": {
                "type": "object",
                "properties": {},
            },
        },
        {
            "name": "get_search_job",
            "description": "Consulta o status de um job de busca sob demanda.",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "job_id": {"type": "integer"},
                },
                "required": ["job_id"],
            },
        },
        {
            "name": "list_search_jobs",
            "description": "Lista os jobs de busca sob demanda mais recentes.",
            "inputSchema": {
                "type": "object",
                "properties": {},
            },
        },
    ]


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
            result = tool_search_products(
                query=_require(arguments, "query"),
                cep=_require(arguments, "cep"),
                db=session,
            )
        elif name == "compare_shopping_list":
            result = tool_compare_shopping_list(
                ShoppingListRequest(
                    cep=_require(arguments, "cep"),
                    items=arguments.get("items", []),
                ),
                session,
            )
        elif name == "compare_basket":
            result = tool_compare_basket(
                ShoppingListRequest(
                    cep=_require(arguments, "cep"),
                    items=arguments.get("items", []),
                ),
                session,
            )
        elif name == "compare_invoice_items":
            result = tool_compare_invoice_items(InvoiceComparisonRequest.model_validate(arguments), session)
        elif name == "compare_receipt":
            result = tool_compare_receipt(ReceiptComparisonRequest.model_validate(arguments), session)
        elif name == "search_observed_item":
            result = tool_search_observed_item(ObservedItemRequest.model_validate(arguments), session)
        elif name == "compare_canonical_product":
            result = compare_single_canonical_product(int(_require(arguments, "canonical_product_id")), session)
        elif name == "list_review_matches":
            result = list_pending_reviews(session)
        elif name == "get_search_job":
            result = get_search_job(int(_require(arguments, "job_id")), session)
        elif name == "list_search_jobs":
            result = list_search_jobs(session)
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
