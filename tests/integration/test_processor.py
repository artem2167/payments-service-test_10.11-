import asyncio
from dataclasses import replace
from unittest.mock import AsyncMock

import pytest
from sqlalchemy import event, select
from sqlalchemy.exc import OperationalError
from sqlalchemy.orm import Session

from app.application.ports import GatewayUnavailable, WebhookDeliveryError
from app.application.processing import InvalidWorkMessage, PaymentProcessor
from app.domain.payment import RetryPolicy
from app.infrastructure.db.models import OutboxRow as Outbox
from app.infrastructure.db.models import PaymentRow as Payment
from app.messaging.messages import WorkMessage

pytestmark = [pytest.mark.integration, pytest.mark.asyncio]


async def create_work(client, database, payment_body):
    response = await client.post(
        "/api/v1/payments", json=payment_body, headers={"Idempotency-Key": "processor"}
    )
    assert response.status_code == 202
    async with database.sessions() as session:
        row = await session.scalar(select(Outbox).where(Outbox.event_type == "payment.process"))
        return WorkMessage.model_validate(row.payload).to_command()


async def state(database, payment_id):
    async with database.sessions() as session:
        payment = await session.get(Payment, payment_id)
        rows = list(
            (
                await session.scalars(
                    select(Outbox)
                    .where(Outbox.payment_id == payment_id)
                    .order_by(Outbox.attempt_no)
                )
            ).all()
        )
        return payment, rows


def work_for_attempt(rows, attempt):
    row = next(
        row for row in rows if row.event_type == "payment.process" and row.attempt_no == attempt
    )
    return WorkMessage.model_validate(row.payload).to_command()


@pytest.mark.parametrize("result", ["succeeded", "failed"])
async def test_gateway_result_is_committed_before_webhook_and_duplicates_are_ignored(
    client, database, settings, payment_body, result
):
    work = await create_work(client, database, payment_body)
    gateway = AsyncMock(return_value=result)
    observed = []

    async def send_webhook(notification):
        persisted, _ = await state(database, notification.payment_id)
        assert persisted.status == result
        assert persisted.processed_at is not None
        observed.append((notification.payment_id, notification.status, notification.event_id))

    processor = PaymentProcessor(
        database.unit_of_work, RetryPolicy(settings.retry_base_delay), gateway, send_webhook
    )
    await processor.process(work)
    await processor.process(work)
    payment, rows = await state(database, work.payment_id)
    assert payment.status == result
    assert payment.webhook_delivered_at is not None
    assert payment.exhausted_at is None
    assert payment.current_attempt == 1
    assert len(rows) == 1
    assert len(observed) == 1
    gateway.assert_awaited_once()


async def test_retry_survives_processor_restart_and_preserves_gateway_result(
    client, database, settings, payment_body
):
    first = await create_work(client, database, payment_body)
    gateway = AsyncMock(return_value="succeeded")
    notifications = []

    async def flaky_webhook(notification):
        notifications.append(
            (notification.event_id, notification.status, notification.processed_at)
        )
        if len(notifications) < 3:
            raise WebhookDeliveryError("http_status_500")

    processor = PaymentProcessor(
        database.unit_of_work, RetryPolicy(settings.retry_base_delay), gateway, flaky_webhook
    )
    await processor.process(first)
    payment, rows = await state(database, first.payment_id)
    assert payment.current_attempt == 2
    assert payment.status == "succeeded"
    second = work_for_attempt(rows, 2)
    retry_row = next(row for row in rows if row.attempt_no == 2)
    assert (retry_row.available_at - retry_row.created_at).total_seconds() >= 4.8
    await processor.process(first)  # The old delivery after retry commit must do nothing.
    assert len(notifications) == 1

    processor = PaymentProcessor(
        database.unit_of_work, RetryPolicy(settings.retry_base_delay), gateway, flaky_webhook
    )
    await processor.process(second)
    payment, rows = await state(database, first.payment_id)
    assert payment.current_attempt == 3
    third = work_for_attempt(rows, 3)
    retry_row = next(row for row in rows if row.attempt_no == 3)
    assert (retry_row.available_at - retry_row.created_at).total_seconds() >= 9.8

    processor = PaymentProcessor(
        database.unit_of_work, RetryPolicy(settings.retry_base_delay), gateway, flaky_webhook
    )
    await processor.process(third)
    payment, rows = await state(database, first.payment_id)
    assert payment.webhook_delivered_at is not None
    assert payment.exhausted_at is None
    assert payment.last_error is None
    assert len(rows) == 3
    assert len(notifications) == 3
    assert len(set(notifications)) == 1
    gateway.assert_awaited_once()


async def test_third_failed_delivery_creates_single_dead_letter_without_changing_payment(
    client, database, settings, payment_body
):
    work = await create_work(client, database, payment_body)
    gateway = AsyncMock(return_value="failed")
    webhook = AsyncMock(side_effect=WebhookDeliveryError("webhook_timeout"))
    for attempt in range(1, 4):
        processor = PaymentProcessor(
            database.unit_of_work, RetryPolicy(settings.retry_base_delay), gateway, webhook
        )
        await processor.process(work)
        payment, rows = await state(database, work.payment_id)
        if attempt < 3:
            work = work_for_attempt(rows, attempt + 1)

    assert payment.status == "failed"
    assert payment.processed_at is not None
    assert payment.current_attempt == 3
    assert payment.exhausted_at is not None
    assert payment.webhook_delivered_at is None
    dead = [row for row in rows if row.event_type == "payment.dead_letter"]
    assert len(dead) == 1
    assert dead[0].routing_key == "payments.dlq"
    assert dead[0].attempt_no == 3
    assert str(payment.id) in str(dead[0].payload)
    assert payment.last_error == "webhook_timeout"
    assert webhook.await_count == 3
    gateway.assert_awaited_once()

    await processor.process(work)
    _, after_duplicate = await state(database, work.payment_id)
    assert len(after_duplicate) == len(rows) == 4
    assert webhook.await_count == 3


async def test_technical_gateway_failure_retries_without_fabricating_business_result(
    client, database, settings, payment_body
):
    work = await create_work(client, database, payment_body)
    gateway = AsyncMock(side_effect=GatewayUnavailable())
    webhook = AsyncMock()
    processor = PaymentProcessor(
        database.unit_of_work, RetryPolicy(settings.retry_base_delay), gateway, webhook
    )
    for attempt in range(1, 4):
        await processor.process(work)
        payment, rows = await state(database, work.payment_id)
        if attempt < 3:
            work = work_for_attempt(rows, attempt + 1)
    assert payment.status == "pending"
    assert payment.processed_at is None
    assert payment.exhausted_at is not None
    assert len([row for row in rows if row.event_type == "payment.dead_letter"]) == 1
    assert gateway.await_count == 3
    webhook.assert_not_awaited()


async def test_concurrent_duplicate_does_not_process_gateway_or_webhook_twice(
    client, database, settings, payment_body
):
    work = await create_work(client, database, payment_body)
    gateway = AsyncMock(return_value="succeeded")
    webhook = AsyncMock()
    processor = PaymentProcessor(
        database.unit_of_work, RetryPolicy(settings.retry_base_delay), gateway, webhook
    )
    await asyncio.gather(processor.process(work), processor.process(work))
    gateway.assert_awaited_once()
    webhook.assert_awaited_once()
    payment, rows = await state(database, work.payment_id)
    assert payment.webhook_delivered_at is not None
    assert len(rows) == 1


async def test_unknown_event_identity_is_quarantinable_and_does_not_spend_attempt(
    client, database, settings, payment_body
):
    from uuid import uuid4

    work = await create_work(client, database, payment_body)
    corrupted = replace(work, event_id=uuid4())
    gateway = AsyncMock()
    webhook = AsyncMock()
    processor = PaymentProcessor(
        database.unit_of_work, RetryPolicy(settings.retry_base_delay), gateway, webhook
    )
    with pytest.raises(InvalidWorkMessage):
        await processor.process(corrupted)
    payment, rows = await state(database, work.payment_id)
    assert payment.current_attempt == 1
    assert payment.status == "pending"
    assert len(rows) == 1
    gateway.assert_not_awaited()
    webhook.assert_not_awaited()


async def test_database_failure_saving_gateway_result_never_sends_unsaved_webhook(
    client, database, settings, payment_body
):
    work = await create_work(client, database, payment_body)
    gateway = AsyncMock(return_value="succeeded")
    webhook = AsyncMock()
    processor = PaymentProcessor(
        database.unit_of_work, RetryPolicy(settings.retry_base_delay), gateway, webhook
    )

    def interrupt_result_commit(session, flush_context, instances):
        if any(isinstance(item, Payment) and item.status == "succeeded" for item in session.dirty):
            raise OperationalError("UPDATE payments", {}, RuntimeError("database unavailable"))

    event.listen(Session, "before_flush", interrupt_result_commit)
    try:
        with pytest.raises(OperationalError):
            await processor.process(work)
    finally:
        event.remove(Session, "before_flush", interrupt_result_commit)
    payment, rows = await state(database, work.payment_id)
    assert payment.status == "pending"
    assert payment.processed_at is None
    assert payment.current_attempt == 1
    assert len(rows) == 1
    webhook.assert_not_awaited()

    await processor.process(work)
    payment, _ = await state(database, work.payment_id)
    assert payment.status == "succeeded"
    assert payment.webhook_delivered_at is not None
    webhook.assert_awaited_once()


async def test_retry_insert_failure_rolls_back_attempt_transition(
    client, database, settings, payment_body
):
    work = await create_work(client, database, payment_body)
    gateway = AsyncMock(return_value="succeeded")
    webhook = AsyncMock(side_effect=WebhookDeliveryError("webhook_timeout"))
    processor = PaymentProcessor(
        database.unit_of_work, RetryPolicy(settings.retry_base_delay), gateway, webhook
    )

    def fail_retry_insert(mapper, connection, target):
        raise OperationalError("INSERT outbox", {}, RuntimeError("database unavailable"))

    event.listen(Outbox, "before_insert", fail_retry_insert)
    try:
        with pytest.raises(OperationalError):
            await processor.process(work)
    finally:
        event.remove(Outbox, "before_insert", fail_retry_insert)
    payment, rows = await state(database, work.payment_id)
    assert payment.status == "succeeded"
    assert payment.current_attempt == 1
    assert payment.exhausted_at is None
    assert len(rows) == 1

    await processor.process(work)
    payment, rows = await state(database, work.payment_id)
    assert payment.current_attempt == 2
    assert len(rows) == 2
    gateway.assert_awaited_once()


async def test_successful_webhook_is_replayed_with_same_id_if_delivery_commit_fails(
    client, database, settings, payment_body
):
    work = await create_work(client, database, payment_body)
    gateway = AsyncMock(return_value="succeeded")
    webhook = AsyncMock()
    processor = PaymentProcessor(
        database.unit_of_work, RetryPolicy(settings.retry_base_delay), gateway, webhook
    )

    def interrupt_delivery_commit(session):
        if any(
            isinstance(item, Payment) and item.webhook_delivered_at is not None
            for item in session.dirty
        ):
            raise RuntimeError("failure after webhook accepted")

    event.listen(Session, "before_commit", interrupt_delivery_commit)
    try:
        with pytest.raises(RuntimeError, match="after webhook accepted"):
            await processor.process(work)
    finally:
        event.remove(Session, "before_commit", interrupt_delivery_commit)

    before_replay, _ = await state(database, work.payment_id)
    assert before_replay.webhook_delivered_at is None
    assert before_replay.status == "succeeded"

    await processor.process(work)
    await processor.process(work)  # An ACK lost after commit must not repeat delivery.
    delivered, rows = await state(database, work.payment_id)
    assert delivered.webhook_delivered_at is not None
    assert delivered.processed_at == before_replay.processed_at
    assert delivered.current_attempt == 1
    assert len(rows) == 1
    assert webhook.await_count == 2
    assert webhook.await_args_list[0] == webhook.await_args_list[1]
    gateway.assert_awaited_once()


async def test_unexpected_adapter_bug_does_not_spend_delivery_attempts(
    client, database, settings, payment_body
):
    work = await create_work(client, database, payment_body)
    gateway = AsyncMock(side_effect=TypeError("adapter bug"))
    webhook = AsyncMock()
    processor = PaymentProcessor(
        database.unit_of_work, RetryPolicy(settings.retry_base_delay), gateway, webhook
    )
    with pytest.raises(TypeError, match="adapter bug"):
        await processor.process(work)

    payment, rows = await state(database, work.payment_id)
    assert payment.status == "pending"
    assert payment.current_attempt == 1
    assert payment.exhausted_at is None
    assert len(rows) == 1
    webhook.assert_not_awaited()
