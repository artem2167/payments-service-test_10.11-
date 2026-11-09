from aio_pika.exceptions import AMQPError
from faststream.rabbit import RabbitBroker
from pamqp.commands import Basic

from app.application.events import OutboxEvent
from app.application.ports import PublicationError
from app.messaging.topology import DLX_EXCHANGE, PAYMENTS_EXCHANGE


class PublicationNotConfirmed(PublicationError):
    pass


class RabbitPublisher:
    def __init__(self, broker: RabbitBroker, timeout: float) -> None:
        self.broker = broker
        self.timeout = timeout

    async def __call__(self, event: OutboxEvent) -> None:
        exchange = DLX_EXCHANGE if event.routing_key == "payments.dlq" else PAYMENTS_EXCHANGE
        try:
            confirmation = await self.broker.publish(
                event.payload,
                exchange=exchange,
                routing_key=event.routing_key,
                message_id=str(event.id),
                message_type=event.event_type,
                persist=True,
                mandatory=True,
                timeout=self.timeout,
            )
        except (AMQPError, OSError) as error:
            raise PublicationError(type(error).__name__) from error
        if confirmation is not True and not isinstance(confirmation, Basic.Ack):
            raise PublicationNotConfirmed("Broker did not confirm publication")
