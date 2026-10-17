import asyncio
from time import perf_counter

from sqlalchemy import text
from app.db.session import engine


async def main():
    try:
        async with engine.connect() as connection:
            for i in range(1, 6):
                started = perf_counter()
                await connection.execute(text("SELECT 1"))
                elapsed_ms = (perf_counter() - started) * 1000

                print(f"Query {i}: {elapsed_ms:.2f} ms")
    finally:
        await engine.dispose()


asyncio.run(main())