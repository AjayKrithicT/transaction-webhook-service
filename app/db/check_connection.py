import asyncio

from sqlalchemy import text
from app.db.session import engine


async def main():
    try:
        async with engine.connect() as connection:
            result = await connection.execute(text("SELECT 1"))
            print("Database connected successfully:", result.scalar())
    except Exception as error:
        print("Database connection failed:", error)
    finally:
        await engine.dispose()


if __name__ == "__main__":
    asyncio.run(main())
