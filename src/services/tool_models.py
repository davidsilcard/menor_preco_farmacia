from pydantic import BaseModel, Field


class ShoppingListRequest(BaseModel):
    cep: str
    items: list[str] = Field(default_factory=list)


class InvoiceItemInput(BaseModel):
    description: str
    paid_price: float | None = None
    quantity: int | None = 1


class InvoiceComparisonRequest(BaseModel):
    cep: str
    items: list[InvoiceItemInput] = Field(default_factory=list)


class ReceiptComparisonRequest(BaseModel):
    cep: str
    items: list[InvoiceItemInput] = Field(default_factory=list)
    merchant_name: str | None = None
    captured_at: str | None = None


class ObservedItemRequest(BaseModel):
    cep: str
    observations: list[str] = Field(default_factory=list)
    source_type: str = "free_text"
