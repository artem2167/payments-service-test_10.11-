from dataclasses import dataclass
from decimal import Decimal
from uuid import UUID

from app.domain.payment import Currency, JsonValue


@dataclass(frozen=True, slots=True)
class CreatePayment:
    amount: Decimal
    currency: Currency
    description: str
    metadata: dict[str, JsonValue]
    webhook_url: str
    idempotency_key: str


@dataclass(frozen=True, slots=True)
class ProcessPayment:
    event_id: UUID
    payment_id: UUID
    attempt_no: int
