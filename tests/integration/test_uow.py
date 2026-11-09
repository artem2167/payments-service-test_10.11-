from datetime import UTC, datetime
from decimal import Decimal
from uuid import uuid4

import pytest

from app.application.events import work_event
from app.domain.payment import Currency, Payment
from app.infrastructure.db.models import OutboxRow, PaymentRow

pytestmark = [pytest.mark.integration, pytest.mark.asyncio]


async def test_exiting_without_commit_rolls_back_both_repositories(database):
    now = datetime.now(UTC)
    payment = Payment(
        id=uuid4(),
        amount=Decimal("1500.00"),
        currency=Currency.RUB,
        description="Rollback example",
        metadata={},
        idempotency_key="rollback",
        webhook_url="http://receiver.test/events",
        webhook_event_id=uuid4(),
        created_at=now,
    )
    event = work_event(payment.id, 1, now)
    async with database.unit_of_work() as uow:
        assert await uow.payments.add_if_new(payment, "f" * 64) is not None
        uow.outbox.add(event)
        assert (await uow.outbox.next_ready(now)).id == event.id  # Query flushes the insert.

    async with database.sessions() as session:
        assert await session.get(PaymentRow, payment.id) is None
        assert await session.get(OutboxRow, event.id) is None
