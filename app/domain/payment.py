from dataclasses import dataclass
from datetime import datetime, timedelta
from decimal import Decimal
from enum import StrEnum
from math import isfinite
from uuid import UUID

MAX_ATTEMPTS = 3
MAX_ERROR_LENGTH = 500
MAX_AMOUNT = Decimal("9999999999999999.99")

type JsonValue = str | int | float | bool | None | list[JsonValue] | dict[str, JsonValue]


class Currency(StrEnum):
    RUB = "RUB"
    USD = "USD"
    EUR = "EUR"


class PaymentStatus(StrEnum):
    PENDING = "pending"
    SUCCEEDED = "succeeded"
    FAILED = "failed"


class UnexpectedAttempt(ValueError):
    pass


@dataclass(frozen=True, slots=True)
class RetryPolicy:
    base_delay: float

    def __post_init__(self) -> None:
        if not isfinite(self.base_delay) or self.base_delay <= 0:
            raise ValueError("Retry delay must be positive and finite")

    def delay_after(self, failed_attempt: int) -> timedelta:
        if not 1 <= failed_attempt < MAX_ATTEMPTS:
            raise UnexpectedAttempt
        return timedelta(seconds=self.base_delay * 2 ** (failed_attempt - 1))


@dataclass(slots=True)
class Payment:
    id: UUID
    amount: Decimal
    currency: Currency
    description: str
    metadata: dict[str, JsonValue]
    idempotency_key: str
    webhook_url: str
    webhook_event_id: UUID
    created_at: datetime
    status: PaymentStatus = PaymentStatus.PENDING
    processed_at: datetime | None = None
    current_attempt: int = 1
    webhook_delivered_at: datetime | None = None
    exhausted_at: datetime | None = None
    last_error: str | None = None

    def __post_init__(self) -> None:
        if not isinstance(self.amount, Decimal) or not self.amount.is_finite():
            raise ValueError("Amount must be a finite decimal")
        if not 0 < self.amount <= MAX_AMOUNT:
            raise ValueError("Amount is outside the supported range")
        _, digits, exponent = self.amount.as_tuple()
        if exponent < -2 and any(digits[exponent + 2 :]):
            raise ValueError("Amount must be representable in whole cents")
        if not isinstance(self.currency, Currency):
            raise ValueError("Unsupported currency")
        if type(self.current_attempt) is not int or not 1 <= self.current_attempt <= MAX_ATTEMPTS:
            raise UnexpectedAttempt

    def accepts_attempt(self, attempt: int) -> bool:
        if self.webhook_delivered_at or self.exhausted_at or attempt < self.current_attempt:
            return False
        if attempt != self.current_attempt:
            raise UnexpectedAttempt
        return True

    def apply_gateway_result(self, result: PaymentStatus, now: datetime) -> None:
        if result not in (PaymentStatus.SUCCEEDED, PaymentStatus.FAILED):
            raise ValueError("Invalid gateway result")
        if self.status == PaymentStatus.PENDING and self.exhausted_at is None:
            self.status = result
            self.processed_at = now

    def delivery_result(self) -> tuple[PaymentStatus, datetime]:
        if self.processed_at is None or self.status == PaymentStatus.PENDING:
            raise ValueError("Cannot notify an unprocessed payment")
        return self.status, self.processed_at

    def mark_delivered(self, now: datetime) -> None:
        self.delivery_result()
        if self.exhausted_at is not None:
            raise ValueError("Cannot mark an exhausted delivery as delivered")
        if self.webhook_delivered_at is None:
            self.webhook_delivered_at = now
            self.last_error = None

    def record_failure(self, reason: str, now: datetime) -> int | None:
        if self.webhook_delivered_at or self.exhausted_at:
            raise ValueError("Cannot retry a terminal delivery")
        self.last_error = reason[:MAX_ERROR_LENGTH]
        if self.current_attempt == MAX_ATTEMPTS:
            self.exhausted_at = now
            return None
        self.current_attempt += 1
        return self.current_attempt
