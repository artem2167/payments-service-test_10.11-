import logging
import secrets
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

from fastapi import FastAPI, Request
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from sqlalchemy.exc import SQLAlchemyError

from app.api.payments import router
from app.application.payments import PaymentService
from app.config import Settings, get_settings
from app.diagnostics import safe_exception_trace
from app.infrastructure.db.database import Database, DatabaseUnavailable
from app.infrastructure.webhooks import WebhookDestinations

logger = logging.getLogger(__name__)


def _safe_error_text(value: object) -> str:
    return str(value).encode("utf-8", errors="backslashreplace").decode("utf-8")


def create_app(settings: Settings | None = None, database: Database | None = None) -> FastAPI:
    settings = settings if settings is not None else get_settings()
    destinations = WebhookDestinations(settings.webhook_allowed_origins)
    owns_database = database is None
    database = database if database is not None else Database(settings.database_url)

    @asynccontextmanager
    async def lifespan(app: FastAPI) -> AsyncIterator[None]:
        logging.basicConfig(
            level=settings.log_level, format="%(asctime)s %(levelname)s %(name)s %(message)s"
        )
        try:
            yield
        finally:
            if owns_database:
                await database.close()

    app = FastAPI(
        title="Payments", lifespan=lifespan, docs_url=None, redoc_url=None, openapi_url=None
    )
    app.state.payments = PaymentService(database.unit_of_work, destinations.require_allowed)
    app.state.settings = settings
    expected_api_key = settings.api_key.encode("utf-8")

    @app.middleware("http")
    async def authenticate(request: Request, call_next):
        provided = request.headers.get("X-API-Key", "").encode("utf-8")
        if not secrets.compare_digest(provided, expected_api_key):
            return JSONResponse(status_code=401, content={"detail": "Invalid API key"})
        return await call_next(request)

    @app.exception_handler(RequestValidationError)
    async def validation_error(request: Request, error: RequestValidationError) -> JSONResponse:
        # Raw input and exception context may contain non-finite numbers or lone
        # surrogates. Report the location and reason without serializing them again.
        detail = [
            {
                "type": _safe_error_text(item["type"]),
                "loc": [
                    part if isinstance(part, int) else _safe_error_text(part)
                    for part in item["loc"]
                ],
                "msg": _safe_error_text(item["msg"]),
            }
            for item in error.errors()
        ]
        return JSONResponse(status_code=422, content={"detail": detail})

    @app.exception_handler(DatabaseUnavailable)
    async def database_unavailable(request: Request, error: DatabaseUnavailable) -> JSONResponse:
        logger.warning("database_unavailable reason=%s", error)
        return JSONResponse(status_code=503, content={"detail": "Database temporarily unavailable"})

    @app.exception_handler(SQLAlchemyError)
    async def database_error(request: Request, error: SQLAlchemyError) -> JSONResponse:
        logger.error("database_error %s", safe_exception_trace(error))
        return JSONResponse(status_code=500, content={"detail": "Internal server error"})

    @app.get("/health")
    async def health() -> dict[str, str]:
        await database.ping()
        return {"status": "ok"}

    app.include_router(router)
    return app
