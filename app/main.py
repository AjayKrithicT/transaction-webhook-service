import asyncio
from contextlib import asynccontextmanager

from fastapi import FastAPI

from app.api.routes.health import router as health_router
from app.api.routes.webhooks import router as webhooks_router
from app.api.routes.transactions import router as transactions_router
from app.db.session import engine
from app.worker.processor import worker_loop


@asynccontextmanager
async def lifespan(app: FastAPI):
    # Start the background worker when the API starts.
    worker_task = asyncio.create_task(worker_loop())

    try:
        yield
    finally:
        # Stop the worker gracefully when the API shuts down.
        worker_task.cancel()

        try:
            await worker_task
        except asyncio.CancelledError:
            pass

        # Close database connections cleanly.
        await engine.dispose()


app = FastAPI(
    title="Transaction Webhook Service",
    lifespan=lifespan,
)

app.include_router(health_router)
app.include_router(webhooks_router)
app.include_router(transactions_router)
