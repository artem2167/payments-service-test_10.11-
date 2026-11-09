from datetime import UTC, datetime, timedelta
from decimal import Decimal
from uuid import uuid4

import pytest

from app.domain.payment import Currency, Payment, PaymentStatus, UnexpectedAttempt


@pytest.fixture
def payment():
    return Payment(
        id=uuid4(),
        amount=Decimal("1500.00"),
        currency=Currency.RUB,
        description="Order 42",
        metadata={},
        idempotency_key="domain-test",
        webhook_url="http://receiver.test/events",
        webhook_event_id=uuid4(),
        created_at=datetime(2026, 11, 9, tzinfo=UTC),
    )


def test_late_result_cannot_replace_committed_business_outcome(payment):
    first = payment.created_at + timedelta(seconds=2)
    payment.apply_gateway_result(PaymentStatus.FAILED, first)
    payment.apply_gateway_result(PaymentStatus.SUCCEEDED, first + timedelta(seconds=3))
    assert payment.delivery_result() == (PaymentStatus.FAILED, first)


def test_future_attempt_is_rejected_but_old_delivery_is_ignored(payment):
    with pytest.raises(UnexpectedAttempt):
        payment.accepts_attempt(2)
    assert payment.record_failure("timeout", payment.created_at) == 2
    assert not payment.accepts_attempt(1)
    assert payment.accepts_attempt(2)


def test_delivered_payment_cannot_reopen_or_change_delivery_timestamp(payment):
    payment.apply_gateway_result(PaymentStatus.SUCCEEDED, payment.created_at)
    payment.mark_delivered(payment.created_at)
    payment.mark_delivered(payment.created_at + timedelta(seconds=1))
    assert payment.webhook_delivered_at == payment.created_at
    assert not payment.accepts_attempt(1)
    with pytest.raises(ValueError, match="terminal"):
        payment.record_failure("timeout", payment.created_at)
