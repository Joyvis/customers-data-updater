from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware


@asynccontextmanager
async def lifespan(app: FastAPI):
    yield


def create_app() -> FastAPI:
    app = FastAPI(
        title="Customers Data Updater",
        description="Multi-tenant SaaS for real estate data refresh via AI + WhatsApp",
        version="0.1.0",
        lifespan=lifespan,
    )

    app.add_middleware(
        CORSMiddleware,
        allow_origins=["*"],
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )

    from app.routers import (
        auth,
        batches,
        conversations,
        dead_letter,
        erasure,
        export,
        mappings,
        tenants,
        upload,
        usage,
    )

    app.include_router(auth.router, prefix="/auth", tags=["auth"])
    app.include_router(tenants.router, prefix="/tenants", tags=["tenants"])
    app.include_router(upload.router, prefix="/batches", tags=["upload"])
    app.include_router(batches.router, prefix="/batches", tags=["batches"])
    app.include_router(mappings.router, prefix="/mappings", tags=["mappings"])
    app.include_router(
        conversations.router, prefix="/conversations", tags=["conversations"]
    )
    app.include_router(dead_letter.router, prefix="/batches", tags=["dead-letter"])
    app.include_router(export.router, prefix="/batches", tags=["export"])
    app.include_router(erasure.router, prefix="/erasure", tags=["erasure"])
    app.include_router(usage.router, prefix="/usage", tags=["usage"])

    return app


app = create_app()
