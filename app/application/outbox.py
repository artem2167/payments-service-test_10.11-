import asyncio
import logging
from datetime import timedelta

from app.application.ports import Clock, PublicationError, Publisher, UowFactory, utc_now

logger = logging.getLogger(__name__)


class OutboxRelay:
    def __init__(
        self,
        uow: UowFactory,
        publisher: Publisher,
        *,
        publish_timeout: float,
        retry_delay: float,
        clock: Clock = utc_now,
    ) -> None:
        self._uow = uow
        self._publisher = publisher
        self._publish_timeout = publish_timeout
        self._retry_delay = timedelta(seconds=retry_delay)
        self._clock = clock

    async def publish_once(self) -> int:
        async with self._uow() as uow:
            event = await uow.outbox.next_ready(self._clock())
            if event is None:
                return 0
            # Keep the claim until confirm and its database update are complete.
            try:
                async with asyncio.timeout(self._publish_timeout):
                    await self._publisher(event)
            except (PublicationError, TimeoutError) as error:
                reason = type(error).__name__
                uow.outbox.postpone(event.id, self._clock() + self._retry_delay, reason)
                await uow.commit()
                logger.warning("outbox_publish_failed message_id=%s reason=%s", event.id, reason)
                return 0
            uow.outbox.mark_published(event.id, self._clock())
            await uow.commit()
            logger.info(
                "outbox_published message_id=%s routing_key=%s", event.id, event.routing_key
            )
            return 1
