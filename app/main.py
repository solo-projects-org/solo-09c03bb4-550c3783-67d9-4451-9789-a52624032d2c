"""FastAPI application for merging per-machine probe results into a wafer map."""

from __future__ import annotations

import logging
import os
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from dataclasses import dataclass

from fastapi import FastAPI, Path, Query, Request
from fastapi.encoders import jsonable_encoder
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse

from .db import init_db
from .errors import DomainError
from .scheduler import QcScheduler, RecomputeScheduler
from .schemas import (
    BatchAcceptedResponse,
    BatchListResponse,
    BatchRequest,
    CellLookupResponse,
    CheckId,
    ErrorResponse,
    HealthResponse,
    MapResponse,
    QcCheckAcceptedResponse,
    QcCheckResponse,
    QcSubmitRequest,
    RecomputeResponse,
    WaferId,
    WaferListResponse,
)
from .service import WaferService

logger = logging.getLogger(__name__)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
)
logging.getLogger("httpx").setLevel(logging.WARNING)


@dataclass(frozen=True)
class Settings:
    db_path: str = "data/wafer.db"
    scheduler_workers: int = 2
    qc_scheduler_workers: int = 2
    recompute_delay_seconds: float = 0.0

    @classmethod
    def from_env(cls) -> "Settings":
        return cls(
            db_path=os.getenv("WAFER_DB_PATH", "data/wafer.db"),
            scheduler_workers=_int_env("SCHEDULER_WORKERS", 2),
            qc_scheduler_workers=_int_env("QC_SCHEDULER_WORKERS", 2),
            recompute_delay_seconds=float(os.getenv("RECOMPUTE_DELAY_SECONDS", "0")),
        )


def _int_env(name: str, default: int) -> int:
    raw = os.getenv(name)
    if raw is None or raw == "":
        return default
    return int(raw)


@asynccontextmanager
async def _lifespan(app: FastAPI) -> AsyncIterator[None]:
    settings: Settings = app.state.settings
    init_db(settings.db_path)
    service = app.state.service or WaferService(
        settings.db_path,
        recompute_delay_seconds=settings.recompute_delay_seconds,
    )
    scheduler = app.state.scheduler or RecomputeScheduler(
        service, workers=settings.scheduler_workers
    )
    qc_scheduler = app.state.qc_scheduler or QcScheduler(
        service, workers=settings.qc_scheduler_workers
    )
    app.state.service = service
    app.state.scheduler = scheduler
    app.state.qc_scheduler = qc_scheduler
    # Interrupted work is resumed from persisted state, never from memory.
    scheduler.recover_pending()
    qc_scheduler.recover_pending()
    try:
        yield
    finally:
        scheduler.shutdown(wait=True)
        qc_scheduler.shutdown(wait=True)


def create_app(
    settings: Settings | None = None,
    *,
    service: WaferService | None = None,
    scheduler: RecomputeScheduler | None = None,
    qc_scheduler: QcScheduler | None = None,
) -> FastAPI:
    app = FastAPI(
        title="Wafer Probe Map Merger",
        version="1.1.0",
        description=(
            "Merge rotated local probe-machine grids into one full-wafer map. "
            "Highest retest number wins per position; untested positions stay blank. "
            "Conflicting verdicts for the same position and retest number reject the "
            "whole batch and preserve the previous map. "
            "A retest-chain QC checklist verifies per position that accepted "
            "records run continuously from 0 to the expected highest retest "
            "number, that nothing higher exists, and that the published map "
            "matches those records; the release it publishes stays valid only "
            "until the next upload or checklist submission."
        ),
        lifespan=_lifespan,
    )
    app.state.settings = settings or Settings.from_env()
    app.state.service = service
    app.state.scheduler = scheduler
    app.state.qc_scheduler = qc_scheduler

    @app.exception_handler(DomainError)
    async def domain_error_handler(_: Request, exc: DomainError) -> JSONResponse:
        return JSONResponse(
            status_code=exc.status_code,
            content={
                "error": {
                    "code": exc.code,
                    "message": exc.message,
                    "details": exc.details,
                }
            },
        )

    @app.exception_handler(RequestValidationError)
    async def validation_error_handler(
        _: Request, exc: RequestValidationError
    ) -> JSONResponse:
        return JSONResponse(
            status_code=422,
            content={
                "error": {
                    "code": "VALIDATION_ERROR",
                    "message": "request payload failed validation",
                    "details": {"errors": jsonable_encoder(exc.errors())},
                }
            },
        )

    def svc(request: Request) -> WaferService:
        return request.app.state.service

    def sched(request: Request) -> RecomputeScheduler:
        return request.app.state.scheduler

    def qsched(request: Request) -> QcScheduler:
        return request.app.state.qc_scheduler

    @app.get("/healthz", response_model=HealthResponse, tags=["system"])
    def healthz() -> HealthResponse:
        return HealthResponse(status="ok", database=app.state.settings.db_path)

    @app.post(
        "/wafers/{wafer_id}/batches",
        response_model=BatchAcceptedResponse,
        status_code=202,
        tags=["uploads"],
        summary="Upload one machine's local grid",
        responses={
            409: {"model": ErrorResponse, "description": "Conflicting verdict or wafer dimension mismatch"},
            422: {"model": ErrorResponse, "description": "Invalid payload or rotated cells outside the wafer"},
        },
    )
    def upload_batch(
        wafer_id: WaferId,
        payload: BatchRequest,
        request: Request,
    ) -> BatchAcceptedResponse:
        result = svc(request).accept_batch(wafer_id, payload)
        sched(request).submit(wafer_id)
        return BatchAcceptedResponse(recompute_scheduled=True, **result)

    @app.get(
        "/wafers/{wafer_id}/map",
        response_model=MapResponse,
        tags=["maps"],
        summary="Full-wafer map with per-cell source",
        responses={404: {"model": ErrorResponse, "description": "Wafer not found"}},
    )
    def get_map(
        wafer_id: WaferId,
        request: Request,
        dense: bool = Query(
            False,
            description=(
                "Also return a rows x cols grid of verdicts (null = untested). "
                "Limited to 250000 cells."
            ),
        ),
    ) -> MapResponse:
        return MapResponse(**svc(request).get_map(wafer_id, dense=dense))

    @app.get(
        "/wafers/{wafer_id}/map/cells/{row}/{col}",
        response_model=CellLookupResponse,
        tags=["maps"],
        summary="One map cell and its source",
        responses={404: {"model": ErrorResponse, "description": "Wafer or position not found"}},
    )
    def get_cell(
        request: Request,
        wafer_id: WaferId,
        row: int = Path(ge=0, description="Zero-based full-wafer row."),
        col: int = Path(ge=0, description="Zero-based full-wafer column."),
    ) -> CellLookupResponse:
        return CellLookupResponse(**svc(request).get_cell(wafer_id, row, col))

    @app.get(
        "/wafers/{wafer_id}/batches",
        response_model=BatchListResponse,
        tags=["uploads"],
        summary="Accepted batches for a wafer",
    )
    def list_batches(wafer_id: WaferId, request: Request) -> BatchListResponse:
        return BatchListResponse(
            wafer_id=wafer_id, batches=svc(request).list_batches(wafer_id)
        )

    @app.get(
        "/wafers/{wafer_id}/status",
        response_model=RecomputeResponse,
        tags=["maps"],
        summary="Acceptance/publication state",
        responses={404: {"model": ErrorResponse, "description": "Wafer not found"}},
    )
    def status(wafer_id: WaferId, request: Request) -> RecomputeResponse:
        return RecomputeResponse(
            **svc(request).recompute_status(wafer_id, scheduled=False)
        )

    @app.post(
        "/wafers/{wafer_id}/recompute",
        response_model=RecomputeResponse,
        status_code=202,
        tags=["maps"],
        summary="Schedule a map rebuild from accepted data",
        responses={404: {"model": ErrorResponse, "description": "Wafer not found"}},
    )
    def recompute(wafer_id: WaferId, request: Request) -> RecomputeResponse:
        # Existence check before scheduling so a typo returns 404, not 202.
        svc(request).get_wafer_state(wafer_id)
        sched(request).submit(wafer_id)
        return RecomputeResponse(
            **svc(request).recompute_status(wafer_id, scheduled=True)
        )

    @app.post(
        "/wafers/{wafer_id}/retest-qc",
        response_model=QcCheckAcceptedResponse,
        status_code=202,
        tags=["retest-qc"],
        summary="Submit a retest-chain QC checklist",
        description=(
            "Verify per position that accepted records run continuously from "
            "0 to the expected highest retest number, that no higher number "
            "exists, and that the published map matches those records. "
            "Submitting immediately invalidates the previous release; the "
            "conclusion is published asynchronously once map, observations "
            "and checklist are on the same current revision."
        ),
        responses={
            404: {"model": ErrorResponse, "description": "Wafer not found"},
            422: {
                "model": ErrorResponse,
                "description": "Invalid payload or checklist positions outside the wafer",
            },
        },
    )
    def submit_retest_qc(
        wafer_id: WaferId,
        payload: QcSubmitRequest,
        request: Request,
    ) -> QcCheckAcceptedResponse:
        result = svc(request).submit_qc_check(wafer_id, payload)
        qsched(request).submit(result["check_id"])
        return QcCheckAcceptedResponse(scheduled=True, **result)

    @app.get(
        "/wafers/{wafer_id}/retest-qc/latest",
        response_model=QcCheckResponse,
        tags=["retest-qc"],
        summary="Latest checklist and whether its release is still valid",
        responses={
            404: {
                "model": ErrorResponse,
                "description": "Wafer not found or no checklist submitted yet",
            }
        },
    )
    def latest_retest_qc(wafer_id: WaferId, request: Request) -> QcCheckResponse:
        return QcCheckResponse(**svc(request).get_latest_qc_check(wafer_id))

    @app.get(
        "/wafers/{wafer_id}/retest-qc/{check_id}",
        response_model=QcCheckResponse,
        tags=["retest-qc"],
        summary="One checklist with findings, conclusion and live validity",
        responses={404: {"model": ErrorResponse, "description": "Wafer or check not found"}},
    )
    def get_retest_qc(
        wafer_id: WaferId, check_id: CheckId, request: Request
    ) -> QcCheckResponse:
        return QcCheckResponse(**svc(request).get_qc_check(wafer_id, check_id))

    @app.post(
        "/wafers/{wafer_id}/retest-qc/{check_id}/retry",
        response_model=QcCheckResponse,
        status_code=202,
        tags=["retest-qc"],
        summary="Retry a pending checklist that failed or was interrupted",
        responses={
            404: {"model": ErrorResponse, "description": "Wafer or check not found"},
            409: {
                "model": ErrorResponse,
                "description": "Check is no longer pending",
            },
        },
    )
    def retry_retest_qc(
        wafer_id: WaferId, check_id: CheckId, request: Request
    ) -> QcCheckResponse:
        svc(request).retry_qc_check(wafer_id, check_id)
        qsched(request).submit(check_id)
        return QcCheckResponse(**svc(request).get_qc_check(wafer_id, check_id))

    @app.get(
        "/wafers",
        response_model=WaferListResponse,
        tags=["maps"],
        summary="List wafers and their pending state",
    )
    def list_wafers(request: Request) -> WaferListResponse:
        return WaferListResponse(wafers=svc(request).list_wafers())

    return app


app = create_app()
