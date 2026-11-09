import asyncio
from datetime import datetime
from decimal import Decimal
from typing import Any, Literal
from uuid import UUID

import httpx
from pydantic import BaseModel, field_serializer

from app.application.ports import (
    WebhookDeliveryError,
    WebhookDestinationRejected,
    WebhookNotification,
)
from app.domain.payment import Currency, PaymentStatus


class WebhookPayload(BaseModel):
    event_id: UUID
    event_type: Literal["payment.completed"] = "payment.completed"
    payment_id: UUID
    status: PaymentStatus
    amount: Decimal
    currency: Currency
    metadata: dict[str, Any]
    created_at: datetime
    processed_at: datetime

    @field_serializer("amount")
    def serialize_amount(self, value: Decimal) -> str:
        return format(value, ".2f")


class WebhookDestinations:
    def __init__(self, origins: tuple[str, ...]) -> None:
        self._allowed = set()
        for origin in origins:
            url = self._parse(origin)
            if url.path != "/" or url.query or url.fragment:
                raise ValueError("WEBHOOK_ALLOWED_ORIGINS entries must be HTTP(S) origins")
            self._allowed.add((url.scheme, url.raw_host, url.port))

    @staticmethod
    def _parse(value: str) -> httpx.URL:
        try:
            url = httpx.URL(value)
        except httpx.InvalidURL:
            raise WebhookDestinationRejected("webhook_destination_rejected") from None
        if url.scheme not in {"http", "https"} or not url.host or url.userinfo:
            raise WebhookDestinationRejected("webhook_destination_rejected")
        return url

    def require_allowed(self, value: str) -> httpx.URL:
        url = self._parse(value)
        if (url.scheme, url.raw_host, url.port) not in self._allowed:
            raise WebhookDestinationRejected("webhook_destination_rejected")
        return url


class WebhookSender:
    def __init__(
        self, client: httpx.AsyncClient, timeout: float, destinations: WebhookDestinations
    ) -> None:
        self.client = client
        self.timeout = timeout
        self.destinations = destinations

    async def __call__(self, notification: WebhookNotification) -> None:
        url = self.destinations.require_allowed(notification.url)
        payload = WebhookPayload(
            event_id=notification.event_id,
            payment_id=notification.payment_id,
            status=notification.status,
            amount=notification.amount,
            currency=notification.currency,
            metadata=notification.metadata,
            created_at=notification.created_at,
            processed_at=notification.processed_at,
        )
        # Bound the whole exchange, including a server sending bytes very slowly.
        try:
            async with asyncio.timeout(self.timeout):
                async with self.client.stream(
                    "POST",
                    url,
                    json=payload.model_dump(mode="json"),
                    headers={"Webhook-Event-Id": str(notification.event_id)},
                    timeout=self.timeout,
                    follow_redirects=False,
                ) as response:
                    if not 200 <= response.status_code < 300:
                        raise WebhookDeliveryError(f"http_status_{response.status_code}")
        except (TimeoutError, httpx.TimeoutException) as error:
            raise WebhookDeliveryError("webhook_timeout") from error
        except (httpx.NetworkError, httpx.RemoteProtocolError) as error:
            raise WebhookDeliveryError(type(error).__name__) from error
