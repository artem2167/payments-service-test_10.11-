import asyncio
import logging

from app.application.commands import ProcessPayment
from app.application.events import dead_letter_event, work_event
from app.application.ports import (
    Clock,
    Gateway,
    GatewayUnavailable,
    UnitOfWork,
    UowFactory,
    Webhook,
    WebhookDeliveryError,
    WebhookNotification,
    utc_now,
)
from app.domain.payment import Payment, PaymentStatus, RetryPolicy, UnexpectedAttempt

logger = logging.getLogger(__name__)


class InvalidWorkMessage(Exception):
    pass


class PaymentProcessor:
    def __init__(
        self,
        uow: UowFactory,
        retry_policy: RetryPolicy,
        gateway: Gateway,
        webhook: Webhook,
        clock: Clock = utc_now,
    ) -> None:
        self._uow = uow
        self._retry_policy = retry_policy
        self._gateway = gateway
        self._webhook = webhook
        self._clock = clock
        self._lock = asyncio.Lock()

    async def process(self, command: ProcessPayment) -> None:
        async with self._lock:
            await self._process(command)

    async def _process(self, command: ProcessPayment) -> None:
        async with self._uow() as uow:
            payment = self._active(await uow.payments.get(command.payment_id), command)
            if payment is None:
                return
            if not await uow.outbox.matches_work(command):
                raise InvalidWorkMessage("unknown_event")

        if payment.status == PaymentStatus.PENDING:
            try:
                result = PaymentStatus(await self._gateway())
                if result == PaymentStatus.PENDING:
                    raise ValueError("Invalid gateway result")
            except GatewayUnavailable as error:
                await self._record_failure(command, type(error).__name__, "gateway")
                return

            async with self._uow() as uow:
                payment = await self._lock_payment(uow, command)
                if payment is None:
                    return
                payment.apply_gateway_result(result, self._clock())
                uow.payments.save_progress(payment)
                await uow.commit()
                logger.info("payment_processed payment_id=%s status=%s", payment.id, payment.status)

        notification = WebhookNotification.from_payment(payment)
        try:
            await self._webhook(notification)
        except WebhookDeliveryError as error:
            await self._record_failure(command, str(error), "webhook")
            return

        async with self._uow() as uow:
            payment = await self._lock_payment(uow, command)
            if payment is None:
                return
            payment.mark_delivered(self._clock())
            uow.payments.save_progress(payment)
            await uow.commit()
            logger.info(
                "webhook_delivered payment_id=%s event_id=%s attempt=%s",
                payment.id,
                payment.webhook_event_id,
                command.attempt_no,
            )

    @staticmethod
    def _active(payment: Payment | None, command: ProcessPayment) -> Payment | None:
        if payment is None:
            raise InvalidWorkMessage("payment_not_found")
        try:
            return payment if payment.accepts_attempt(command.attempt_no) else None
        except UnexpectedAttempt as error:
            raise InvalidWorkMessage("unexpected_attempt") from error

    async def _lock_payment(self, uow: UnitOfWork, command: ProcessPayment) -> Payment | None:
        return self._active(await uow.payments.get_for_update(command.payment_id), command)

    async def _record_failure(self, command: ProcessPayment, reason: str, stage: str) -> None:
        async with self._uow() as uow:
            payment = await self._lock_payment(uow, command)
            if payment is None:
                return
            now = self._clock()
            next_attempt = payment.record_failure(reason, now)
            if next_attempt is None:
                event = dead_letter_event(payment, command, reason, stage, now)
            else:
                due = now + self._retry_policy.delay_after(command.attempt_no)
                event = work_event(payment.id, next_attempt, due)
            uow.payments.save_progress(payment)
            uow.outbox.add(event)
            await uow.commit()
            logger.warning(
                "%s payment_id=%s attempt=%s stage=%s reason=%s",
                "payment_exhausted" if next_attempt is None else "payment_retry_scheduled",
                payment.id,
                command.attempt_no,
                stage,
                reason,
            )
