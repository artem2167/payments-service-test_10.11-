import asyncio
import json
from datetime import UTC, datetime
from decimal import Decimal
from uuid import uuid4

import httpx
import pytest

from app.application.ports import (
    WebhookDeliveryError,
    WebhookDestinationRejected,
    WebhookNotification,
)
from app.domain.payment import Currency, Payment, PaymentStatus
from app.infrastructure.webhooks import WebhookDestinations, WebhookSender

DESTINATIONS = WebhookDestinations(("http://receiver.test",))


@pytest.fixture
def payment():
    return Payment(
        id=uuid4(),
        amount=Decimal("1500.00"),
        currency=Currency.RUB,
        description="Order 42",
        metadata={"order_id": "42"},
        idempotency_key="webhook-test",
        webhook_url="http://receiver.test/events",
        webhook_event_id=uuid4(),
        created_at=datetime.now(UTC),
    )


@pytest.fixture
def notification(payment):
    payment.apply_gateway_result(PaymentStatus.SUCCEEDED, datetime.now(UTC))
    return WebhookNotification.from_payment(payment)


@pytest.mark.asyncio
@pytest.mark.parametrize("status", [200, 201, 204, 299])
async def test_any_2xx_response_confirms_delivery_with_exact_payload(notification, status):
    requests = []

    def receive(request):
        requests.append(request)
        return httpx.Response(status)

    async with httpx.AsyncClient(transport=httpx.MockTransport(receive)) as client:
        sender = WebhookSender(client, 1, DESTINATIONS)
        await sender(notification)
        await sender(notification)

    assert len(requests) == 2
    assert requests[0].method == "POST"
    assert str(requests[0].url) == notification.url
    assert requests[0].headers["Webhook-Event-Id"] == str(notification.event_id)
    assert "X-API-Key" not in requests[0].headers
    first_payload = json.loads(requests[0].content)
    assert first_payload == json.loads(requests[1].content)
    assert first_payload["event_id"] == str(notification.event_id)
    assert first_payload["payment_id"] == str(notification.payment_id)
    assert first_payload["event_type"] == "payment.completed"
    assert first_payload["status"] == "succeeded"
    assert first_payload["amount"] == "1500.00"
    assert first_payload["metadata"] == notification.metadata
    assert first_payload["processed_at"]


@pytest.mark.asyncio
@pytest.mark.parametrize("status", [301, 302, 400, 404, 429, 500, 503])
async def test_non_2xx_is_retryable_and_redirects_are_not_followed(notification, status):
    requests = []

    def receive(request):
        requests.append(request)
        return httpx.Response(status, headers={"Location": "http://elsewhere.test/private"})

    async with httpx.AsyncClient(
        transport=httpx.MockTransport(receive), follow_redirects=True
    ) as client:
        with pytest.raises(WebhookDeliveryError, match=f"http_status_{status}"):
            await WebhookSender(client, 1, DESTINATIONS)(notification)
    assert len(requests) == 1


@pytest.mark.asyncio
async def test_total_timeout_bounds_even_a_transport_that_never_returns(notification):
    entered = asyncio.Event()
    cancelled = asyncio.Event()

    async def never_returns(request):
        entered.set()
        try:
            await asyncio.Event().wait()
        finally:
            cancelled.set()

    async with httpx.AsyncClient(transport=httpx.MockTransport(never_returns)) as client:
        with pytest.raises(WebhookDeliveryError, match="webhook_timeout"):
            await asyncio.wait_for(WebhookSender(client, 0.02, DESTINATIONS)(notification), 1)
    assert entered.is_set()
    assert cancelled.is_set()


@pytest.mark.asyncio
async def test_sender_rechecks_stored_destination_before_connecting(notification):
    async with httpx.AsyncClient() as client:
        with pytest.raises(WebhookDestinationRejected):
            await WebhookSender(client, 1, WebhookDestinations(()))(notification)


def test_pending_payment_cannot_produce_a_webhook_notification(payment):
    with pytest.raises(ValueError, match="unprocessed"):
        WebhookNotification.from_payment(payment)
