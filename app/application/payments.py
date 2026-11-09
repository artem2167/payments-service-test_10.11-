import hashlib
import json
from collections.abc import Callable
from uuid import UUID, uuid4

from app.application.commands import CreatePayment
from app.application.events import work_event
from app.application.ports import Clock, UowFactory, utc_now
from app.domain.payment import Payment


class IdempotencyConflict(Exception):
    pass


def request_hash(command: CreatePayment) -> str:
    canonical = json.dumps(
        {
            "amount": format(command.amount, ".2f"),
            "currency": command.currency.value,
            "description": command.description,
            "metadata": command.metadata,
            "webhook_url": command.webhook_url,
        },
        sort_keys=True,
        ensure_ascii=False,
        allow_nan=False,
        separators=(",", ":"),
    )
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


class PaymentService:
    def __init__(
        self, uow: UowFactory, check_webhook: Callable[[str], object], clock: Clock = utc_now
    ) -> None:
        self._uow = uow
        self._check_webhook = check_webhook
        self._clock = clock

    async def create(self, command: CreatePayment) -> Payment:
        payment = Payment(
            id=uuid4(),
            amount=command.amount,
            currency=command.currency,
            description=command.description,
            metadata=command.metadata,
            idempotency_key=command.idempotency_key,
            webhook_url=command.webhook_url,
            webhook_event_id=uuid4(),
            created_at=self._clock(),
        )
        self._check_webhook(payment.webhook_url)
        fingerprint = request_hash(command)
        async with self._uow() as uow:
            created = await uow.payments.add_if_new(payment, fingerprint)
            if created is None:
                existing, stored_fingerprint = await uow.payments.by_key(command.idempotency_key)
                if stored_fingerprint != fingerprint:
                    raise IdempotencyConflict
                return existing
            uow.outbox.add(work_event(created.id, created.current_attempt, self._clock()))
            await uow.commit()
            return created

    async def get(self, payment_id: UUID) -> Payment | None:
        async with self._uow() as uow:
            return await uow.payments.get(payment_id)
