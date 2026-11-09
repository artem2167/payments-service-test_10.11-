from contextlib import asynccontextmanager
from copy import deepcopy
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from types import SimpleNamespace
from uuid import uuid4

import pytest

from app.application.commands import ProcessPayment
from app.application.events import work_event
from app.application.ports import GatewayUnavailable, WebhookDeliveryError
from app.application.processing import PaymentProcessor
from app.domain.payment import Currency, Payment, PaymentStatus, RetryPolicy


class InMemoryStore:
    def __init__(self, payment, event):
        self.payment = payment
        self.events = [event]
        self.open = False

    @asynccontextmanager
    async def unit_of_work(self):
        assert not self.open
        self.open = True
        progress = deepcopy(self.payment)
        saved = None
        queued = []

        async def get(payment_id):
            return progress if progress.id == payment_id else None

        async def matches(command):
            return any(
                event.id == command.event_id
                and event.payment_id == command.payment_id
                and event.attempt_no == command.attempt_no
                for event in self.events
            )

        def save_progress(payment):
            nonlocal saved
            saved = deepcopy(payment)

        async def commit():
            if saved is not None:
                self.payment = saved
            self.events.extend(queued)

        try:
            yield SimpleNamespace(
                payments=SimpleNamespace(get=get, get_for_update=get, save_progress=save_progress),
                outbox=SimpleNamespace(matches_work=matches, add=queued.append),
                commit=commit,
            )
        finally:
            self.open = False


@pytest.mark.asyncio
async def test_gateway_and_webhook_failures_share_budget_without_database_or_broker():
    now = datetime(2026, 11, 9, tzinfo=UTC)
    payment = Payment(
        id=uuid4(),
        amount=Decimal("1500.00"),
        currency=Currency.RUB,
        description="Mixed failure",
        metadata={},
        idempotency_key="mixed-failure",
        webhook_url="http://receiver.test/events",
        webhook_event_id=uuid4(),
        created_at=now,
    )
    store = InMemoryStore(payment, work_event(payment.id, 1, now))
    gateway_calls = 0
    notifications = []

    async def gateway():
        nonlocal gateway_calls
        assert not store.open
        gateway_calls += 1
        if gateway_calls == 1:
            raise GatewayUnavailable
        return PaymentStatus.SUCCEEDED

    async def webhook(notification):
        assert not store.open
        assert store.payment.status == PaymentStatus.SUCCEEDED
        assert store.payment.processed_at == now
        notifications.append(notification)
        raise WebhookDeliveryError("webhook_timeout")

    processor = PaymentProcessor(store.unit_of_work, RetryPolicy(5), gateway, webhook, lambda: now)
    for attempt in (1, 2, 3):
        event = store.events[-1]
        assert event.attempt_no == attempt
        await processor.process(ProcessPayment(event.id, payment.id, attempt))
        if attempt == 1:
            assert store.payment.status == PaymentStatus.PENDING
            assert not notifications
    assert [event.available_at for event in store.events[:3]] == [
        now,
        now + timedelta(seconds=5),
        now + timedelta(seconds=10),
    ]
    assert [(event.event_type, event.attempt_no) for event in store.events] == [
        ("payment.process", 1),
        ("payment.process", 2),
        ("payment.process", 3),
        ("payment.dead_letter", 3),
    ]
    assert store.payment.exhausted_at == now
    assert store.payment.status == PaymentStatus.SUCCEEDED
    assert gateway_calls == 2
    assert [item.event_id for item in notifications] == [payment.webhook_event_id] * 2
