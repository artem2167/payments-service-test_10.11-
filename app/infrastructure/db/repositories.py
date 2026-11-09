from datetime import datetime
from uuid import UUID

from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession

from app.application.commands import ProcessPayment
from app.application.events import OutboxEvent
from app.domain.payment import Currency, Payment, PaymentStatus
from app.infrastructure.db.models import OutboxRow, PaymentRow


def _payment(row: PaymentRow) -> Payment:
    return Payment(
        id=row.id,
        amount=row.amount,
        currency=Currency(row.currency),
        description=row.description,
        metadata=row.metadata_json,
        idempotency_key=row.idempotency_key,
        webhook_url=row.webhook_url,
        webhook_event_id=row.webhook_event_id,
        created_at=row.created_at,
        status=PaymentStatus(row.status),
        processed_at=row.processed_at,
        current_attempt=row.current_attempt,
        webhook_delivered_at=row.webhook_delivered_at,
        exhausted_at=row.exhausted_at,
        last_error=row.last_error,
    )


class SqlAlchemyPayments:
    def __init__(self, session: AsyncSession) -> None:
        self._session = session
        self._locked: dict[UUID, PaymentRow] = {}

    async def add_if_new(self, payment: Payment, fingerprint: str) -> Payment | None:
        # PostgreSQL arbitrates concurrent inserts before the replay lookup.
        row = await self._session.scalar(
            insert(PaymentRow)
            .values(
                id=payment.id,
                amount=payment.amount,
                currency=payment.currency.value,
                description=payment.description,
                metadata_json=payment.metadata,
                idempotency_key=payment.idempotency_key,
                request_hash=fingerprint,
                webhook_url=payment.webhook_url,
                webhook_event_id=payment.webhook_event_id,
                created_at=payment.created_at,
                status=payment.status.value,
                current_attempt=payment.current_attempt,
            )
            .on_conflict_do_nothing(constraint="uq_payments_idempotency_key")
            .returning(PaymentRow)
        )
        return _payment(row) if row is not None else None

    async def by_key(self, key: str) -> tuple[Payment, str]:
        result = await self._session.execute(
            select(PaymentRow).where(PaymentRow.idempotency_key == key)
        )
        row = result.scalar_one()
        return _payment(row), row.request_hash

    async def get(self, payment_id: UUID) -> Payment | None:
        row = await self._session.get(PaymentRow, payment_id)
        return _payment(row) if row is not None else None

    async def get_for_update(self, payment_id: UUID) -> Payment | None:
        row = await self._session.scalar(
            select(PaymentRow).where(PaymentRow.id == payment_id).with_for_update()
        )
        if row is None:
            return None
        self._locked[payment_id] = row
        return _payment(row)

    def save_progress(self, payment: Payment) -> None:
        row = self._locked[payment.id]
        row.status = payment.status.value
        row.processed_at = payment.processed_at
        row.current_attempt = payment.current_attempt
        row.webhook_delivered_at = payment.webhook_delivered_at
        row.exhausted_at = payment.exhausted_at
        row.last_error = payment.last_error


class SqlAlchemyOutbox:
    def __init__(self, session: AsyncSession) -> None:
        self._session = session
        self._locked: dict[UUID, OutboxRow] = {}

    def add(self, event: OutboxEvent) -> None:
        self._session.add(
            OutboxRow(
                id=event.id,
                payment_id=event.payment_id,
                event_type=event.event_type,
                attempt_no=event.attempt_no,
                routing_key=event.routing_key,
                payload=event.payload,
                available_at=event.available_at,
            )
        )

    async def matches_work(self, command: ProcessPayment) -> bool:
        row = await self._session.get(OutboxRow, command.event_id)
        return (
            row is not None
            and row.payment_id == command.payment_id
            and row.attempt_no == command.attempt_no
            and row.event_type == "payment.process"
        )

    async def next_ready(self, now: datetime) -> OutboxEvent | None:
        row = await self._session.scalar(
            select(OutboxRow)
            .where(OutboxRow.published_at.is_(None), OutboxRow.available_at <= now)
            .order_by(OutboxRow.available_at, OutboxRow.id)
            .limit(1)
            .with_for_update(skip_locked=True)
        )
        if row is None:
            return None
        self._locked[row.id] = row
        return OutboxEvent(
            id=row.id,
            payment_id=row.payment_id,
            event_type=row.event_type,
            attempt_no=row.attempt_no,
            routing_key=row.routing_key,
            payload=row.payload,
            available_at=row.available_at,
        )

    def mark_published(self, event_id: UUID, now: datetime) -> None:
        row = self._locked[event_id]
        row.publish_attempts += 1
        row.published_at = now
        row.last_error = None

    def postpone(self, event_id: UUID, until: datetime, reason: str) -> None:
        row = self._locked[event_id]
        row.publish_attempts += 1
        row.available_at = until
        row.last_error = reason
