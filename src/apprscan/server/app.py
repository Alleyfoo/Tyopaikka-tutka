"""FastAPI app factory for the companion service."""

from __future__ import annotations

import os
import secrets
import time

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.responses import JSONResponse

from .routes import router
from .service import purge_runs


class BodySizeLimitMiddleware(BaseHTTPMiddleware):
    def __init__(self, app: FastAPI, max_bytes: int):
        super().__init__(app)
        self.max_bytes = max_bytes

    async def dispatch(self, request, call_next):  # type: ignore[override]
        if request.method in {"POST", "PUT", "PATCH"}:
            length = request.headers.get("content-length")
            if length and length.isdigit() and int(length) > self.max_bytes:
                return JSONResponse({"detail": "Request body too large."}, status_code=413)
            body = await request.body()
            if len(body) > self.max_bytes:
                return JSONResponse({"detail": "Request body too large."}, status_code=413)
            request._body = body  # type: ignore[attr-defined]
        return await call_next(request)


def create_app(token: str | None = None) -> FastAPI:
    app = FastAPI(title="apprscan companion", version="0.1")
    cors_origins = [
        origin.strip()
        for origin in os.getenv("APPRSCAN_CORS_ORIGINS", "").split(",")
        if origin.strip()
    ]
    if cors_origins:
        app.add_middleware(
            CORSMiddleware,
            allow_origins=cors_origins,
            allow_credentials=False,
            allow_methods=["*"],
            allow_headers=["*"],
        )
    max_body = int(os.getenv("APPRSCAN_MAX_BODY_BYTES", "10240"))
    app.add_middleware(BodySizeLimitMiddleware, max_bytes=max_body)
    app.include_router(router)

    token = token or os.getenv("APPRSCAN_TOKEN") or secrets.token_urlsafe(24)
    app.state.token = token
    app.state.rate_limit = {}
    app.state.rate_limit_window_s = int(os.getenv("APPRSCAN_RATE_LIMIT_WINDOW_S", "60"))
    app.state.rate_limit_max = int(os.getenv("APPRSCAN_RATE_LIMIT_MAX", "10"))
    app.state.start_ts = time.time()
    retention_days = int(os.getenv("APPRSCAN_RETENTION_DAYS", "30"))
    app.state.retention_days = retention_days
    app.state.purged_runs = purge_runs(max_age_days=retention_days)
    return app


app = create_app()
