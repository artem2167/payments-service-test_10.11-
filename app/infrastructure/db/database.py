from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from socket import gaierror

from asyncpg import PostgresError
from sqlalchemy import text
from sqlalchemy.exc import DBAPIError
from sqlalchemy.exc import TimeoutError as PoolTimeout
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from app.application.ports import UnitOfWork
from app.infrastructure.db.uow import SqlAlchemyUnitOfWork


class DatabaseUnavailable(Exception):
    pass


class Database:
    def __init__(self, database_url: str, *, schema: str | None = None) -> None:
        connect_args = {"timeout": 5}
        if schema is not None:
            connect_args["server_settings"] = {"search_path": schema}
        self.engine = create_async_engine(
            database_url, pool_pre_ping=True, hide_parameters=True, connect_args=connect_args
        )
        self.sessions = async_sessionmaker(self.engine, expire_on_commit=False)

    @asynccontextmanager
    async def _session(self) -> AsyncIterator[AsyncSession]:
        try:
            async with self.sessions() as session:
                yield session
        except (ConnectionError, gaierror, TimeoutError, PoolTimeout) as error:
            raise DatabaseUnavailable(type(error).__name__) from error
        except (DBAPIError, PostgresError) as error:
            original = error.orig if isinstance(error, DBAPIError) else error
            sqlstate = getattr(original, "sqlstate", None) or ""
            disconnected = isinstance(error, DBAPIError) and error.connection_invalidated
            if (
                disconnected
                or sqlstate.startswith("08")
                or sqlstate in {"53300", "57P01", "57P02", "57P03"}
            ):
                raise DatabaseUnavailable(type(original).__name__) from error
            raise

    @asynccontextmanager
    async def unit_of_work(self) -> AsyncIterator[UnitOfWork]:
        async with self._session() as session:
            await session.begin()
            try:
                yield SqlAlchemyUnitOfWork(session)
            finally:
                if session.in_transaction():
                    await session.rollback()

    async def ping(self) -> None:
        async with self._session() as session:
            await session.execute(text("SELECT 1"))

    async def close(self) -> None:
        await self.engine.dispose()
