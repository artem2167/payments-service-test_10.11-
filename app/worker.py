import asyncio
import base64
import contextlib
import hashlib
import logging
import signal

import httpx
from aio_pika.exceptions import AMQPError
from faststream import AckPolicy
from faststream.rabbit import Channel, RabbitBroker
from faststream.rabbit.annotations import RabbitMessage
from pamqp.commands import Basic
from pydantic import ValidationError

from app.application.outbox import OutboxRelay
from app.application.processing import InvalidWorkMessage, PaymentProcessor
from app.config import Settings, get_settings
from app.diagnostics import safe_exception_trace
from app.domain.payment import RetryPolicy
from app.infrastructure.db.database import Database, DatabaseUnavailable
from app.infrastructure.gateway import EmulatedGateway
from app.infrastructure.webhooks import WebhookDestinations, WebhookSender
from app.messaging.messages import WorkMessage
from app.messaging.outbox import PublicationNotConfirmed, RabbitPublisher
from app.messaging.topology import (
    DLX_EXCHANGE,
    PAYMENTS_EXCHANGE,
    PAYMENTS_QUEUE,
    declare_topology,
)

logger = logging.getLogger(__name__)
UNEXPECTED_RECOVERY_DELAY = 10


def create_broker(settings: Settings) -> RabbitBroker:
    return RabbitBroker(
        settings.rabbitmq_url,
        default_channel=Channel(prefetch_count=1, publisher_confirms=True, on_return_raises=True),
        graceful_timeout=settings.webhook_timeout + settings.gateway_max_delay + 2,
        timeout=settings.publish_timeout,
        reconnect_interval=settings.recovery_delay,
        fail_fast=True,
        logger=logger,
    )


async def quarantine(
    broker: RabbitBroker, message: RabbitMessage, reason: str, publish_limit: float = 5
) -> None:
    body = message.body
    identifier = message.raw_message.message_id or hashlib.sha256(body).hexdigest()
    confirmation = await broker.publish(
        {
            "schema_version": 1,
            "event_type": "payment.invalid_message",
            "original_message_id": identifier,
            "error": reason,
            "original_body_base64": base64.b64encode(body[:4096]).decode("ascii"),
            "body_truncated": len(body) > 4096,
        },
        exchange=DLX_EXCHANGE,
        routing_key="payments.dlq",
        message_id=f"invalid-{identifier}",
        persist=True,
        mandatory=True,
        timeout=publish_limit,
    )
    if confirmation is not True and not isinstance(confirmation, Basic.Ack):
        raise PublicationNotConfirmed("Quarantine was not confirmed")
    await message.ack()


async def run_connected(
    settings: Settings,
    database: Database,
    client: httpx.AsyncClient,
    stop: asyncio.Event,
) -> bool:
    broker = create_broker(settings)
    recovery = asyncio.Event()
    unexpected_failure = False
    processor = PaymentProcessor(
        database.unit_of_work,
        RetryPolicy(settings.retry_base_delay),
        EmulatedGateway(settings),
        WebhookSender(
            client, settings.webhook_timeout, WebhookDestinations(settings.webhook_allowed_origins)
        ),
    )

    @broker.subscriber(
        PAYMENTS_QUEUE, PAYMENTS_EXCHANGE, ack_policy=AckPolicy.MANUAL, no_reply=True
    )
    async def handle(message: RabbitMessage) -> None:
        nonlocal unexpected_failure
        event = None
        try:
            try:
                event = WorkMessage.model_validate_json(message.body)
            except ValidationError:
                await quarantine(
                    broker, message, "invalid_message_schema", settings.publish_timeout
                )
                return
            try:
                await processor.process(event.to_command())
            except InvalidWorkMessage as error:
                await quarantine(broker, message, str(error), settings.publish_timeout)
                return
            await message.ack()
        except (DatabaseUnavailable, PublicationNotConfirmed, AMQPError) as error:
            logger.warning("consumer_recovery_required reason=%s", type(error).__name__)
            # Closing the channel requeues this unacked delivery; do not consume
            # its logical retry budget when the DB or connection is unavailable.
            recovery.set()
        except Exception as error:
            logger.error(
                "consumer_unexpected_failure payment_id=%s event_id=%s %s",
                event.payment_id if event else None,
                event.event_id if event else None,
                safe_exception_trace(error),
            )
            unexpected_failure = True
            recovery.set()

    tasks: list[asyncio.Task] = []
    try:
        await broker.connect()
        await declare_topology(broker)
        await broker.start()
        relay = OutboxRelay(
            database.unit_of_work,
            RabbitPublisher(broker, settings.publish_timeout),
            publish_timeout=settings.publish_timeout,
            retry_delay=settings.recovery_delay,
        )
        tasks = [
            asyncio.create_task(run_outbox(relay, settings), name="outbox-relay"),
            asyncio.create_task(recovery.wait(), name="consumer-recovery"),
            asyncio.create_task(stop.wait(), name="worker-stop"),
        ]
        logger.info("worker_ready queue=payments.new")
        done, _ = await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)
        for task in done:
            task.result()
        return unexpected_failure
    finally:
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        await broker.stop()


async def run_outbox(relay: OutboxRelay, settings: Settings) -> None:
    while True:
        try:
            published = await relay.publish_once()
        except DatabaseUnavailable as error:
            logger.warning("outbox_database_unavailable reason=%s", type(error).__name__)
            await asyncio.sleep(settings.recovery_delay)
            continue
        if not published:
            await asyncio.sleep(settings.outbox_poll_interval)


async def run_worker(settings: Settings, stop: asyncio.Event | None = None) -> None:
    stop = stop or asyncio.Event()
    WebhookDestinations(settings.webhook_allowed_origins)
    loop = asyncio.get_running_loop()
    for name in (signal.SIGINT, signal.SIGTERM):
        with contextlib.suppress(NotImplementedError):
            loop.add_signal_handler(name, stop.set)
    database = Database(settings.database_url)
    try:
        async with httpx.AsyncClient(follow_redirects=False, trust_env=False) as client:
            while not stop.is_set():
                delay = settings.recovery_delay
                try:
                    if await run_connected(settings, database, client, stop):
                        delay = max(delay, UNEXPECTED_RECOVERY_DELAY)
                except (AMQPError, OSError) as error:
                    logger.warning("worker_connection_failed reason=%s", type(error).__name__)
                except Exception as error:
                    logger.error("worker_unexpected_failure %s", safe_exception_trace(error))
                    delay = max(delay, UNEXPECTED_RECOVERY_DELAY)
                if not stop.is_set():
                    with contextlib.suppress(TimeoutError):
                        await asyncio.wait_for(stop.wait(), timeout=delay)
    finally:
        await database.close()


def main() -> None:
    settings = get_settings()
    logging.basicConfig(
        level=settings.log_level,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    try:
        asyncio.run(run_worker(settings))
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()
