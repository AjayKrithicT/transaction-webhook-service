
from pathlib import Path

import pytest
from sqlalchemy import delete
from sqlalchemy.ext.asyncio import (
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)
from sqlalchemy.pool import NullPool

from app.core.config import settings
from app.db.base import Base
from app.db.session import get_db
from app.main import app
from app.models.processing_job import ProcessingJob
from app.models.transaction import Transaction
from app.worker import processor


# Create a separate engine for the test database.
# NullPool prevents connections from being reused across
# different event loops.
test_engine = create_async_engine(
    settings.TEST_DATABASE_URL,
    poolclass=NullPool,
    pool_pre_ping=True,
)

TestSessionLocal = async_sessionmaker(
    bind=test_engine,
    class_=AsyncSession,
    expire_on_commit=False,
)


# Make worker operations use the test database.
processor.SessionLocal = TestSessionLocal


@pytest.fixture(scope="session", autouse=True)
async def setup_test_database():
    """Create the test schema and install the database function."""

    async with test_engine.begin() as connection:
        await connection.run_sync(Base.metadata.drop_all)
        await connection.run_sync(Base.metadata.create_all)

        function_path = (
            Path(__file__).resolve().parents[1]
            / "database"
            / "accept_transaction_webhook.sql"
        )
        function_sql = function_path.read_text(encoding="utf-8")

        await connection.exec_driver_sql(function_sql)

    yield

    async with test_engine.begin() as connection:
        await connection.run_sync(Base.metadata.drop_all)

    await test_engine.dispose()


@pytest.fixture(autouse=True)
async def clean_test_data():
    """Keep each test isolated from previously created records."""

    async with TestSessionLocal() as session:
        await session.execute(delete(ProcessingJob))
        await session.execute(delete(Transaction))
        await session.commit()

    yield


async def override_get_db():
    """Provide test-database sessions to FastAPI endpoints."""

    async with TestSessionLocal() as session:
        yield session


# Override the database dependency used by the API.
app.dependency_overrides[get_db] = override_get_db
