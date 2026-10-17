
import asyncio

from sqlalchemy.ext.asyncio import create_async_engine

from app.core.config import settings
from app.db.base import Base
from app.models.transaction import Transaction
from app.models.processing_job import ProcessingJob


async def setup_test_database():
    # Read the dedicated test database URL.
    test_database_url = settings.TEST_DATABASE_URL

    # Create an engine connected only to the test database.
    engine = create_async_engine(
        test_database_url,
        pool_pre_ping=True,
    )

    try:
        async with engine.begin() as connection:
            # Create tables defined by our SQLAlchemy models.
            await connection.run_sync(Base.metadata.create_all)

        print("Test database tables created successfully.")
        print("Database: transactions_test_db")
        print("Tables: transactions, processing_jobs")

    finally:
        await engine.dispose()


if __name__ == "__main__":
    asyncio.run(setup_test_database())
