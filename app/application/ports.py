from collections.abc import Awaitable, Callable
from contextlib import AbstractAsyncContextManager
from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import Decimal
from typing import Protocol, Self
from uuid import UUID

from app.application.commands import ProcessPayment
from app.application.events import OutboxEvent
from app.domain.payment import Currency, JsonValue, Payment, PaymentStatus


def utc_now() -> datetime:
    return datetime.now(UTC)


@dataclass(frozen=True, slots=True)
class WebhookNotification:
    url: str
    event_id: UUID
    payment_id: UUID
    status: PaymentStatus
    amount: Decimal
    currency: Currency
    metadata: dict[str, JsonValue]
    created_at: datetime
    processed_at: datetime

    @classmethod
    def from_payment(cls, payment: Payment) -> Self:
        status, processed_at = payment.delivery_result()
        return cls(
            url=payment.webhook_url,
            event_id=payment.webhook_event_id,
            payment_id=payment.id,
            status=status,
            amount=payment.amount,
            currency=payment.currency,
            metadata=payment.metadata,
            created_at=payment.created_at,
            processed_at=processed_at,
        )


class GatewayUnavailable(Exception):
    pass


class WebhookDeliveryError(Exception):
    pass


class WebhookDestinationRejected(WebhookDeliveryError):
    pass


class PublicationError(Exception):
    pass


class PaymentRepository(Protocol):
    async def add_if_new(self, payment: Payment, fingerprint: str) -> Payment | None: ...
    async def by_key(self, key: str) -> tuple[Payment, str]: ...
    async def get(self, payment_id: UUID) -> Payment | None: ...
    async def get_for_update(self, payment_id: UUID) -> Payment | None: ...
    def save_progress(self, payment: Payment) -> None: ...


class OutboxRepository(Protocol):
    def add(self, event: OutboxEvent) -> None: ...
    async def matches_work(self, command: ProcessPayment) -> bool: ...
    async def next_ready(self, now: datetime) -> OutboxEvent | None: ...
    def mark_published(self, event_id: UUID, now: datetime) -> None: ...
    def postpone(self, event_id: UUID, until: datetime, reason: str) -> None: ...


class UnitOfWork(Protocol):
    @property
    def payments(self) -> PaymentRepository: ...
    @property
    def outbox(self) -> OutboxRepository: ...
    async def commit(self) -> None: ...


type UowFactory = Callable[[], AbstractAsyncContextManager[UnitOfWork]]
type Clock = Callable[[], datetime]
type Gateway = Callable[[], Awaitable[str]]
type Webhook = Callable[[WebhookNotification], Awaitable[None]]
type Publisher = Callable[[OutboxEvent], Awaitable[None]]
