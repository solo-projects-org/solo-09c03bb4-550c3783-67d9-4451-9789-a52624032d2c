"""Request/response models and their invariants."""

from __future__ import annotations

from typing import Annotated, Any, Literal

from pydantic import BaseModel, ConfigDict, Field, StringConstraints, model_validator

ID_PATTERN = r"^[A-Za-z0-9][A-Za-z0-9._:-]{0,63}$"
MAX_WAFER_DIM = 2000
MAX_LOCAL_DIM = 2000
MAX_CELLS = 250_000
MAX_RETEST_NO = 1_000_000_000
MAX_QC_ITEMS = 250_000

WaferId = Annotated[
    str,
    StringConstraints(
        strip_whitespace=True, min_length=1, max_length=64, pattern=ID_PATTERN
    ),
]

CheckId = Annotated[
    str,
    StringConstraints(
        strip_whitespace=True, min_length=1, max_length=64, pattern=ID_PATTERN
    ),
]

Verdict = Annotated[
    str,
    StringConstraints(strip_whitespace=True, min_length=1, max_length=64),
]
Identifier = Annotated[str, StringConstraints(pattern=ID_PATTERN)]
Rotation = Literal[0, 90, 180, 270]


class LocalCell(BaseModel):
    """One measured cell inside the local probe grid."""

    model_config = ConfigDict(extra="forbid")

    verdict: Verdict = Field(
        description="Opaque measurement verdict, e.g. PASS / FAIL / 1 / 0.",
        examples=["PASS"],
    )
    retest_no: int = Field(
        ge=0,
        le=MAX_RETEST_NO,
        description="Retest counter. At one wafer position the highest number wins.",
        examples=[0],
    )


class BatchRequest(BaseModel):
    """A single probe-machine upload of a rotated local grid."""

    model_config = ConfigDict(extra="forbid")

    wafer_rows: int = Field(ge=1, le=MAX_WAFER_DIM, description="Full wafer row count.")
    wafer_cols: int = Field(ge=1, le=MAX_WAFER_DIM, description="Full wafer column count.")
    machine_id: Identifier = Field(description="Probe machine identifier.")
    origin_row: int = Field(
        ge=0,
        le=MAX_WAFER_DIM - 1,
        description="Full-wafer row occupied by the local grid's (0,0) cell.",
    )
    origin_col: int = Field(
        ge=0,
        le=MAX_WAFER_DIM - 1,
        description="Full-wafer column occupied by the local grid's (0,0) cell.",
    )
    rotation_deg: Rotation = Field(
        description="Clockwise rotation of the local grid around its (0,0) cell.",
    )
    local_grid: list[list[LocalCell | None]] = Field(
        description=(
            "Rectangular local grid. A null cell means that position was not "
            "measured by this upload; it does not clear an existing result."
        ),
        min_length=1,
    )

    @model_validator(mode="after")
    def _validate_shape_and_origin(self) -> "BatchRequest":
        local_rows = len(self.local_grid)
        local_cols = len(self.local_grid[0])
        if local_rows > MAX_LOCAL_DIM or local_cols > MAX_LOCAL_DIM:
            raise ValueError(
                f"local grid must be at most {MAX_LOCAL_DIM}x{MAX_LOCAL_DIM} cells"
            )
        if local_rows * local_cols > MAX_CELLS:
            raise ValueError(f"local grid must contain at most {MAX_CELLS} cells")
        for row_no, row in enumerate(self.local_grid):
            if len(row) != local_cols:
                raise ValueError(
                    f"local_grid is ragged: row {row_no} has {len(row)} cells, "
                    f"expected {local_cols}"
                )
        if not any(cell is not None for row in self.local_grid for cell in row):
            raise ValueError("local_grid must contain at least one measured cell")
        if self.origin_row >= self.wafer_rows:
            raise ValueError("origin_row must be inside the wafer")
        if self.origin_col >= self.wafer_cols:
            raise ValueError("origin_col must be inside the wafer")
        return self


class SourceInfo(BaseModel):
    """Where the winning measurement for a cell came from."""

    machine_id: str
    batch_id: str
    batch_seq: int
    local_row: int
    local_col: int


class CellInfo(BaseModel):
    row: int
    col: int
    verdict: str
    retest_no: int
    source: SourceInfo


class RecomputeError(BaseModel):
    seq: int
    message: str
    at: str


class MapResponse(BaseModel):
    wafer_id: str
    rows: int
    cols: int
    status: Literal["empty", "pending_update", "ready"]
    pending_update: bool = Field(
        description="True when accepted data is newer than the served map."
    )
    recompute_failed: bool = Field(
        description=(
            "True when the latest accepted sequence failed to publish. The "
            "previous map remains served and the wafer stays pending."
        )
    )
    accepted_seq: int = Field(description="Latest accepted batch sequence.")
    map_seq: int = Field(description="Accepted sequence the served map was built from.")
    tested_cells: int
    total_cells: int
    last_recompute_error: RecomputeError | None = None
    cells: list[CellInfo] = Field(
        description="Measured cells only. Any position not listed is untested/blank."
    )
    grid: list[list[str | None]] | None = Field(
        default=None,
        description=(
            "Dense rows x cols verdict grid (null = untested). Only present when "
            "the request asked for it; limited to 250000 cells."
        ),
    )


class BatchAcceptedResponse(BaseModel):
    wafer_id: str
    batch_id: str
    seq: int
    accepted_seq: int
    cells_accepted: int
    status: Literal["pending_update"] = "pending_update"
    recompute_scheduled: bool


class BatchInfo(BaseModel):
    batch_id: str
    seq: int
    machine_id: str
    origin_row: int
    origin_col: int
    rotation_deg: int
    local_rows: int
    local_cols: int
    measured_cells: int
    received_at: str


class BatchListResponse(BaseModel):
    wafer_id: str
    batches: list[BatchInfo]


class CellLookupResponse(BaseModel):
    wafer_id: str
    row: int
    col: int
    tested: bool = Field(description="False means untested/blank.")
    status: Literal["empty", "pending_update", "ready"]
    accepted_seq: int
    map_seq: int
    cell: CellInfo | None = None


class WaferSummary(BaseModel):
    wafer_id: str
    rows: int
    cols: int
    status: Literal["empty", "pending_update", "ready"]
    recompute_failed: bool
    pending_update: bool
    accepted_seq: int
    map_seq: int


class WaferListResponse(BaseModel):
    wafers: list[WaferSummary]


class ErrorDetail(BaseModel):
    code: str
    message: str
    details: dict[str, Any] = Field(default_factory=dict)


class ErrorResponse(BaseModel):
    error: ErrorDetail


class RecomputeResponse(BaseModel):
    wafer_id: str
    accepted_seq: int
    map_seq: int
    status: Literal["empty", "pending_update", "ready"]
    recompute_failed: bool = False
    scheduled: bool


class HealthResponse(BaseModel):
    status: Literal["ok"]
    database: str


# ------------------------------------------------------------- retest QC
class QcItem(BaseModel):
    """One checklist entry: a wafer position and the highest retest number
    it should have completed."""

    model_config = ConfigDict(extra="forbid")

    row: int = Field(ge=0, description="Zero-based full-wafer row.")
    col: int = Field(ge=0, description="Zero-based full-wafer column.")
    expected_max_retest_no: int = Field(
        ge=0,
        le=MAX_RETEST_NO,
        description=(
            "Highest retest number this position should have completed. "
            "Accepted records must then cover every number from 0 up to and "
            "including this one, and nothing higher."
        ),
        examples=[2],
    )


class QcSubmitRequest(BaseModel):
    """A retest-chain QC checklist. Submitting it immediately invalidates the
    previous release for the wafer."""

    model_config = ConfigDict(extra="forbid")

    items: list[QcItem] = Field(
        min_length=1,
        max_length=MAX_QC_ITEMS,
        description="Non-empty checklist of positions to verify.",
    )

    @model_validator(mode="after")
    def _no_duplicate_positions(self) -> "QcSubmitRequest":
        seen: set[tuple[int, int]] = set()
        for item in self.items:
            position = (item.row, item.col)
            if position in seen:
                raise ValueError(
                    f"duplicate checklist position ({item.row}, {item.col})"
                )
            seen.add(position)
        return self


class QcMissingFinding(BaseModel):
    row: int
    col: int
    expected_max_retest_no: int
    missing_count: int = Field(
        description="How many retest numbers in 0..expected are absent."
    )
    missing_retest_nos: list[int] = Field(
        description="The absent numbers, truncated to 50 per cell."
    )
    truncated: bool


class QcExceededFinding(BaseModel):
    row: int
    col: int
    expected_max_retest_no: int
    extra_count: int = Field(
        description="How many accepted retest numbers exceed the expected one."
    )
    extra_retest_nos: list[int] = Field(
        description="The exceeding numbers, truncated to 50 per cell."
    )
    truncated: bool


class QcMapInconsistencyFinding(BaseModel):
    row: int
    col: int
    expected_max_retest_no: int
    reason: Literal[
        "map_missing_cell",
        "map_retest_mismatch",
        "map_source_mismatch",
        "map_without_observation",
    ]
    observed_max_retest_no: int | None = Field(
        description="Highest retest number in the accepted records, if any."
    )
    map_retest_no: int | None = Field(
        description="Retest number the published map currently shows, if any."
    )


class QcFindings(BaseModel):
    """Problem coordinates, grouped by defect. Each list is capped at 500
    entries; the summary always carries exact totals."""

    missing: list[QcMissingFinding] = Field(
        description="Positions whose accepted records are not continuous 0..N."
    )
    exceeded: list[QcExceededFinding] = Field(
        description="Positions with accepted retest numbers above N."
    )
    map_inconsistent: list[QcMapInconsistencyFinding] = Field(
        description="Positions where the published map does not match the accepted records."
    )


class QcSummary(BaseModel):
    item_count: int
    cells_with_missing: int
    cells_with_exceeded: int
    cells_with_map_inconsistency: int
    findings_truncated: bool = Field(
        description="True when a findings list was capped; totals stay exact."
    )


class QcCheckError(BaseModel):
    message: str
    at: str


class QcCheckAcceptedResponse(BaseModel):
    wafer_id: str
    check_id: str
    revision: int = Field(
        description="Per-wafer checklist revision. A resubmission bumps it."
    )
    status: Literal["pending"] = "pending"
    item_count: int
    previous_release_invalidated: bool = Field(
        description=(
            "True when a still-valid release existed just before this "
            "submission and is now invalid."
        )
    )
    scheduled: bool


class QcCheckResponse(BaseModel):
    wafer_id: str
    check_id: str
    revision: int
    status: Literal["pending", "completed", "superseded"] = Field(
        description=(
            "pending: verification not (successfully) finished yet, safe to "
            "retry. superseded: a newer checklist replaced this one."
        )
    )
    conclusion: Literal["releasable", "not_releasable"] | None = Field(
        description="Published verdict of the checklist; null while pending."
    )
    release_valid: bool = Field(
        description=(
            "True only while this check is the latest revision AND no new "
            "measurements were accepted AND the published map still matches "
            "the revision the conclusion was built from."
        )
    )
    item_count: int
    obs_seq: int | None = Field(
        description="Accepted sequence the conclusion was verified against."
    )
    map_seq: int | None = Field(
        description="Published-map sequence the conclusion was verified against."
    )
    current_accepted_seq: int = Field(description="Live accepted sequence.")
    current_map_seq: int = Field(description="Live published-map sequence.")
    attempts: int = Field(description="How many verification runs were started.")
    last_error: QcCheckError | None = None
    findings: QcFindings | None = None
    summary: QcSummary | None = None
    created_at: str
    updated_at: str
    completed_at: str | None = None
