from sqlalchemy.ext.asyncio import AsyncSession

from app.infrastructure.db.repositories import SqlAlchemyOutbox, SqlAlchemyPayments


class SqlAlchemyUnitOfWork:
    def __init__(self, session: AsyncSession) -> None:
        self._session = session
        self.payments = SqlAlchemyPayments(session)
        self.outbox = SqlAlchemyOutbox(session)

    async def commit(self) -> None:
        await self._session.commit()
