"""Explicit service opt-in and isolated schemas keep tests away from app data."""

import os
from collections.abc import AsyncIterator
from pathlib import Path
from uuid import uuid4

import httpx
import pytest
import pytest_asyncio
from alembic import command
from alembic.config import Config
from sqlalchemy import text
from sqlalchemy.ext.asyncio import create_async_engine

from app.config import Settings
from app.infrastructure.db.database import Database
from app.main import create_app


def pytest_addoption(parser):
    parser.addoption("--fail-on-skip", action="store_true", help="Require all tests to run")


def pytest_sessionfinish(session, exitstatus):
    if exitstatus == pytest.ExitCode.OK and session.config.getoption("--fail-on-skip"):
        reporter = session.config.pluginmanager.get_plugin("terminalreporter")
        if reporter.stats.get("skipped"):
            reporter.write_line("The full suite must run without skips.", red=True)
            session.exitstatus = pytest.ExitCode.TESTS_FAILED


@pytest.fixture
def settings() -> Settings:
    return Settings(
        api_key="test-secret",
        webhook_allowed_origins=("http://receiver.test",),
        retry_base_delay=5,
        gateway_min_delay=0,
        gateway_max_delay=0,
        recovery_delay=0.01,
    )


@pytest_asyncio.fixture
async def database() -> AsyncIterator[Database]:
    url = os.environ.get("TEST_DATABASE_URL")
    if not url:
        pytest.skip("Set TEST_DATABASE_URL to an explicitly designated PostgreSQL test database")
    if not url.startswith("postgresql+asyncpg://"):
        pytest.fail(
            "TEST_DATABASE_URL must use PostgreSQL with asyncpg; "
            "SQLite does not test these guarantees"
        )

    schema = f"payments_test_{uuid4().hex}"
    admin_engine = create_async_engine(url)
    database = Database(url, schema=schema)

    def migrate(connection):
        config = Config(Path(__file__).resolve().parents[1] / "alembic.ini")
        config.attributes["connection"] = connection
        command.upgrade(config, "head")
        command.check(config)

    try:
        async with admin_engine.begin() as connection:
            await connection.execute(text(f'CREATE SCHEMA "{schema}"'))
        async with database.engine.begin() as connection:
            await connection.run_sync(migrate)
        yield database
    finally:
        await database.close()
        async with admin_engine.begin() as connection:
            await connection.execute(text(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE'))
        await admin_engine.dispose()


@pytest_asyncio.fixture
async def client(database: Database, settings: Settings) -> AsyncIterator[httpx.AsyncClient]:
    app = create_app(settings=settings, database=database)
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app),
        base_url="http://payments.test",
        headers={"X-API-Key": settings.api_key},
    ) as client:
        yield client


@pytest.fixture
def payment_body() -> dict:
    return {
        "amount": "1500.00",
        "currency": "RUB",
        "description": "Order 42",
        "metadata": {"order_id": "42", "nested": {"items": [1, 2]}},
        "webhook_url": "http://receiver.test/events",
    }
