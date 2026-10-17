from datetime import datetime
from decimal import Decimal

from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    field_validator,
)

from app.core.constants import TransactionStatus


class TransactionWebhookRequest(BaseModel):
    transaction_id: str = Field(min_length=1, max_length=255)
    source_account: str = Field(min_length=1, max_length=255)
    destination_account: str = Field(min_length=1, max_length=255)
    amount: Decimal = Field(gt=0, max_digits=18, decimal_places=2)
    currency: str = Field(pattern=r"^[A-Z]{3}$")

    @field_validator("transaction_id", "source_account", "destination_account")
    @classmethod
    def normalize_nonblank_strings(cls, value: str) -> str:
        value = value.strip()
        if not value:
            raise ValueError("Value must not be blank")
        return value


class TransactionWebhookResponse(BaseModel):
    transaction_id: str
    status: TransactionStatus
    message: str


class TransactionStatusResponse(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    transaction_id: str
    source_account: str
    destination_account: str
    amount: Decimal
    currency: str
    status: TransactionStatus
    created_at: datetime = Field(validation_alias="received_at")
    processed_at: datetime | None
