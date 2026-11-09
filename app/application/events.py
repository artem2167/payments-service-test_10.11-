from dataclasses import dataclass
from datetime import datetime
from uuid import UUID, uuid4

from app.application.commands import ProcessPayment
from app.domain.payment import MAX_ERROR_LENGTH, JsonValue, Payment


@dataclass(frozen=True, slots=True)
class OutboxEvent:
    id: UUID
    payment_id: UUID
    event_type: str
    attempt_no: int
    routing_key: str
    payload: dict[str, JsonValue]
    available_at: datetime


def work_event(payment_id: UUID, attempt: int, available_at: datetime) -> OutboxEvent:
    event_id = uuid4()
    return OutboxEvent(
        id=event_id,
        payment_id=payment_id,
        event_type="payment.process",
        attempt_no=attempt,
        routing_key="payments.new",
        payload={
            "schema_version": 1,
            "event_id": str(event_id),
            "payment_id": str(payment_id),
            "attempt_no": attempt,
        },
        available_at=available_at,
    )


def dead_letter_event(
    payment: Payment, source: ProcessPayment, reason: str, stage: str, now: datetime
) -> OutboxEvent:
    event_id = uuid4()
    return OutboxEvent(
        id=event_id,
        payment_id=payment.id,
        event_type="payment.dead_letter",
        attempt_no=source.attempt_no,
        routing_key="payments.dlq",
        payload={
            "schema_version": 1,
            "event_id": str(event_id),
            "event_type": "payment.dead_letter",
            "payment_id": str(payment.id),
            "original_event_id": str(source.event_id),
            "attempt_no": source.attempt_no,
            "status": payment.status.value,
            "stage": stage[:64],
            "error": reason[:MAX_ERROR_LENGTH],
            "exhausted_at": (payment.exhausted_at or now).isoformat(),
        },
        available_at=now,
    )
