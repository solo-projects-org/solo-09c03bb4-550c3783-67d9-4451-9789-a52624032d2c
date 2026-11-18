"""Acceptance and map-publication rules.

Two mutating paths exist and they are deliberately separated:

* ``accept_batch`` persists an upload and advances ``accepted_seq``.  It never
  touches the published map, so the previously visible map is immediately
  marked as pending update and remains queryable.
* ``recompute`` builds a complete snapshot from the accepted observations and
  publishes it **only if** ``accepted_seq`` still equals the sequence the
  snapshot was built from.  A late recompute can therefore never overwrite a
  map that already contains newer measurements.

Map cells, their sources and ``committed_seq`` are written in one SQLite
transaction, so after a failure or restart they always belong to the same
accepted snapshot.
"""

from __future__ import annotations

import json
import sqlite3
import time
import uuid
from dataclasses import dataclass
from typing import Any, Literal

from .db import connect, read_snapshot, transaction, utcnow
from .errors import DomainError
from .mapping import LocalMeasurement, MappedMeasurement, map_local_to_full
from .schemas import BatchInfo, BatchRequest, QcSubmitRequest, WaferSummary

MAX_REPORTED_OFFENDING_CELLS = 50
MAX_DENSE_CELLS = 250_000
MAX_QC_FINDINGS_PER_CATEGORY = 500
MAX_QC_NUMBERS_PER_CELL = 50


@dataclass(frozen=True)
class Snapshot:
    """A consistent view of accepted observations for one sequence."""

    wafer_id: str
    seq: int
    rows: int
    cols: int
    cells: list[sqlite3.Row]


@dataclass(frozen=True)
class QcSnapshot:
    """A consistent view of one pending checklist and the data it covers.

    ``obs_seq``/``map_seq`` are the wafer's accepted/committed sequences at
    the moment the rows were read; the conclusion may only be published while
    they are still the live values.
    """

    check_id: str
    wafer_id: str
    revision: int
    obs_seq: int
    map_seq: int
    items: list[sqlite3.Row]
    observations: list[sqlite3.Row]
    map_cells: list[sqlite3.Row]


WaferStatus = Literal["empty", "pending_update", "ready"]


def _status(wafer: sqlite3.Row) -> WaferStatus:
    accepted = int(wafer["accepted_seq"])
    committed = int(wafer["committed_seq"])
    if accepted == 0:
        return "empty"
    if accepted > committed:
        # A failed rebuild never hides the pending state: the old map remains
        # queryable and is still marked as awaiting update.
        return "pending_update"
    return "ready"


def _recompute_failed(wafer: sqlite3.Row) -> bool:
    """True when the latest accepted sequence failed to publish."""
    return (
        wafer["last_error_seq"] is not None
        and int(wafer["last_error_seq"]) == int(wafer["accepted_seq"])
        and int(wafer["accepted_seq"]) > int(wafer["committed_seq"])
    )


def _error(wafer: sqlite3.Row) -> dict[str, Any] | None:
    if wafer["last_error"] is None:
        return None
    return {
        "seq": wafer["last_error_seq"],
        "message": wafer["last_error"],
        "at": wafer["last_error_at"],
    }


class WaferService:
    def __init__(
        self,
        db_path: str,
        *,
        recompute_delay_seconds: float = 0.0,
        max_recompute_rounds: int = 10,
        qc_delay_seconds: float = 0.0,
        max_qc_rounds: int = 10,
    ) -> None:
        self.db_path = db_path
        # Test hook: widens the window between snapshot and publication so a
        # competing acceptance can be interleaved deterministically.
        self.recompute_delay_seconds = recompute_delay_seconds
        self.max_recompute_rounds = max_recompute_rounds
        # Same kind of test hook for the retest-QC verification path.
        self.qc_delay_seconds = qc_delay_seconds
        self.max_qc_rounds = max_qc_rounds

    # ------------------------------------------------------------------ accept
    def accept_batch(self, wafer_id: str, request: BatchRequest) -> dict[str, Any]:
        measurements = self._collect_measurements(request)
        mapped = map_local_to_full(
            measurements,
            origin_row=request.origin_row,
            origin_col=request.origin_col,
            rotation_deg=request.rotation_deg,
        )
        self._reject_out_of_bounds(mapped, request.wafer_rows, request.wafer_cols)

        conn = connect(self.db_path)
        try:
            with transaction(conn, immediate=True):
                self._upsert_wafer(conn, wafer_id, request)
                conflicts = self._find_conflicts(conn, wafer_id, mapped)
                if conflicts:
                    raise DomainError(
                        409,
                        "VERDICT_CONFLICT",
                        (
                            "batch rejected: a position already has a different "
                            "verdict for the same retest number"
                        ),
                        {
                            "conflicts": conflicts,
                            "map_preserved": True,
                            "accepted_seq_unchanged": True,
                        },
                    )

                wafer = conn.execute(
                    "SELECT accepted_seq FROM wafers WHERE wafer_id = ?",
                    (wafer_id,),
                ).fetchone()
                seq = int(wafer["accepted_seq"]) + 1
                batch_id = uuid.uuid4().hex
                now = utcnow()
                conn.execute(
                    """
                    INSERT INTO batches (
                        batch_id, wafer_id, seq, machine_id, origin_row, origin_col,
                        rotation_deg, local_rows, local_cols, cell_count, received_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        batch_id,
                        wafer_id,
                        seq,
                        request.machine_id,
                        request.origin_row,
                        request.origin_col,
                        request.rotation_deg,
                        len(request.local_grid),
                        len(request.local_grid[0]),
                        len(mapped),
                        now,
                    ),
                )
                # A re-report of the same position/retest/verdict is not an
                # error.  The newest upload becomes the source of that reading.
                conn.executemany(
                    """
                    INSERT INTO observations (
                        wafer_id, full_row, full_col, retest_no, verdict,
                        machine_id, batch_id, local_row, local_col
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT (wafer_id, full_row, full_col, retest_no)
                    DO UPDATE SET
                        verdict = excluded.verdict,
                        machine_id = excluded.machine_id,
                        batch_id = excluded.batch_id,
                        local_row = excluded.local_row,
                        local_col = excluded.local_col
                    """,
                    [
                        (
                            wafer_id,
                            cell.full_row,
                            cell.full_col,
                            cell.retest_no,
                            cell.verdict,
                            request.machine_id,
                            batch_id,
                            cell.local_row,
                            cell.local_col,
                        )
                        for cell in mapped
                    ],
                )
                conn.execute(
                    "UPDATE wafers SET accepted_seq = ?, updated_at = ? WHERE wafer_id = ?",
                    (seq, now, wafer_id),
                )
        finally:
            conn.close()

        return {
            "wafer_id": wafer_id,
            "batch_id": batch_id,
            "seq": seq,
            "accepted_seq": seq,
            "cells_accepted": len(mapped),
            "status": "pending_update",
        }

    def _collect_measurements(self, request: BatchRequest) -> list[LocalMeasurement]:
        # Rotation is a bijection, so distinct local positions can never map to
        # the same full-wafer position.  Duplicate verdicts can therefore only
        # arise between uploads, which ``_find_conflicts`` handles.
        return [
            LocalMeasurement(
                local_row=local_row,
                local_col=local_col,
                verdict=cell.verdict,
                retest_no=cell.retest_no,
            )
            for local_row, row in enumerate(request.local_grid)
            for local_col, cell in enumerate(row)
            if cell is not None
        ]

    def _reject_out_of_bounds(
        self, mapped: list[MappedMeasurement], rows: int, cols: int
    ) -> None:
        offending = [
            cell
            for cell in mapped
            if not (0 <= cell.full_row < rows and 0 <= cell.full_col < cols)
        ]
        if not offending:
            return
        examples = [
            {
                "local_row": cell.local_row,
                "local_col": cell.local_col,
                "full_row": cell.full_row,
                "full_col": cell.full_col,
            }
            for cell in offending[:MAX_REPORTED_OFFENDING_CELLS]
        ]
        raise DomainError(
            422,
            "CELL_OUT_OF_BOUNDS",
            (
                f"batch rejected: {len(offending)} rotated cell(s) fall outside "
                f"the {rows}x{cols} wafer"
            ),
            {
                "offending_count": len(offending),
                "examples": examples,
                "map_preserved": True,
            },
        )

    def _upsert_wafer(
        self, conn: sqlite3.Connection, wafer_id: str, request: BatchRequest
    ) -> None:
        now = utcnow()
        row = conn.execute(
            "SELECT * FROM wafers WHERE wafer_id = ?", (wafer_id,)
        ).fetchone()
        if row is None:
            conn.execute(
                """
                INSERT INTO wafers (wafer_id, rows, cols, created_at, updated_at)
                VALUES (?, ?, ?, ?, ?)
                """,
                (wafer_id, request.wafer_rows, request.wafer_cols, now, now),
            )
            return
        if (row["rows"], row["cols"]) != (request.wafer_rows, request.wafer_cols):
            raise DomainError(
                409,
                "WAFER_DIMENSION_MISMATCH",
                (
                    f"wafer {wafer_id!r} is {row['rows']}x{row['cols']}, but the "
                    f"upload declares {request.wafer_rows}x{request.wafer_cols}"
                ),
                {
                    "existing": {"rows": row["rows"], "cols": row["cols"]},
                    "uploaded": {
                        "rows": request.wafer_rows,
                        "cols": request.wafer_cols,
                    },
                    "map_preserved": True,
                },
            )

    def _find_conflicts(
        self,
        conn: sqlite3.Connection,
        wafer_id: str,
        mapped: list[MappedMeasurement],
    ) -> list[dict[str, Any]]:
        """Return stored readings that disagree with this upload.

        The whole batch is rejected on any mismatch; no stored row is touched.
        """
        conn.execute("DROP TABLE IF EXISTS temp._incoming")
        conn.execute(
            """
            CREATE TEMP TABLE _incoming (
                full_row INTEGER NOT NULL,
                full_col INTEGER NOT NULL,
                retest_no INTEGER NOT NULL,
                verdict TEXT NOT NULL
            )
            """
        )
        try:
            conn.executemany(
                "INSERT INTO temp._incoming (full_row, full_col, retest_no, verdict) "
                "VALUES (?, ?, ?, ?)",
                [
                    (c.full_row, c.full_col, c.retest_no, c.verdict)
                    for c in mapped
                ],
            )
            total = conn.execute(
                """
                SELECT COUNT(*)
                  FROM temp._incoming AS i
                  JOIN observations AS o
                    ON o.wafer_id = ?
                   AND o.full_row = i.full_row
                   AND o.full_col = i.full_col
                   AND o.retest_no = i.retest_no
                 WHERE o.verdict <> i.verdict
                """,
                (wafer_id,),
            ).fetchone()[0]
            if total == 0:
                return []
            rows = conn.execute(
                """
                SELECT i.full_row, i.full_col, i.retest_no,
                       i.verdict AS incoming_verdict, o.verdict AS stored_verdict
                  FROM temp._incoming AS i
                  JOIN observations AS o
                    ON o.wafer_id = ?
                   AND o.full_row = i.full_row
                   AND o.full_col = i.full_col
                   AND o.retest_no = i.retest_no
                 WHERE o.verdict <> i.verdict
                 ORDER BY i.full_row, i.full_col
                 LIMIT ?
                """,
                (wafer_id, MAX_REPORTED_OFFENDING_CELLS),
            ).fetchall()
        finally:
            conn.execute("DROP TABLE IF EXISTS temp._incoming")

        conflicts = [
            {
                "full_row": row["full_row"],
                "full_col": row["full_col"],
                "retest_no": row["retest_no"],
                "stored_verdict": row["stored_verdict"],
                "incoming_verdict": row["incoming_verdict"],
            }
            for row in rows
        ]
        if total > len(rows):
            conflicts.append(
                {"truncated": True, "total_conflicts": total, "returned": len(rows)}
            )
        return conflicts

    # -------------------------------------------------------------------- read
    def get_wafer_state(self, wafer_id: str) -> sqlite3.Row:
        conn = connect(self.db_path)
        try:
            return self._wafer_or_404(conn, wafer_id)
        finally:
            conn.close()

    def get_map(self, wafer_id: str, *, dense: bool = False) -> dict[str, Any]:
        conn = connect(self.db_path)
        try:
            with read_snapshot(conn):
                wafer = self._wafer_or_404(conn, wafer_id)
                cells = conn.execute(
                    """
                    SELECT full_row, full_col, verdict, source_retest_no,
                           source_machine_id, source_batch_id, source_batch_seq,
                           source_local_row, source_local_col
                      FROM map_cells
                     WHERE wafer_id = ?
                     ORDER BY full_row, full_col
                    """,
                    (wafer_id,),
                ).fetchall()
        finally:
            conn.close()

        total_cells = int(wafer["rows"]) * int(wafer["cols"])
        if dense and total_cells > MAX_DENSE_CELLS:
            raise DomainError(
                413,
                "DENSE_MAP_TOO_LARGE",
                (
                    f"dense grid for {wafer['rows']}x{wafer['cols']} would exceed "
                    f"{MAX_DENSE_CELLS} cells; use the sparse cells list"
                ),
            )

        grid: list[list[str | None]] | None = None
        if dense:
            grid = [
                [None for _ in range(int(wafer["cols"]))]
                for _ in range(int(wafer["rows"]))
            ]
            for cell in cells:
                grid[cell["full_row"]][cell["full_col"]] = cell["verdict"]

        return {
            "wafer_id": wafer["wafer_id"],
            "rows": wafer["rows"],
            "cols": wafer["cols"],
            "status": _status(wafer),
            "recompute_failed": _recompute_failed(wafer),
            "pending_update": int(wafer["accepted_seq"]) > int(wafer["committed_seq"]),
            "accepted_seq": wafer["accepted_seq"],
            "map_seq": wafer["committed_seq"],
            "tested_cells": len(cells),
            "total_cells": total_cells,
            "last_recompute_error": _error(wafer),
            "cells": [_cell_payload(cell) for cell in cells],
            "grid": grid,
        }

    def get_cell(self, wafer_id: str, row: int, col: int) -> dict[str, Any]:
        conn = connect(self.db_path)
        try:
            with read_snapshot(conn):
                wafer = self._wafer_or_404(conn, wafer_id)
                if not (0 <= row < wafer["rows"] and 0 <= col < wafer["cols"]):
                    raise DomainError(
                        404,
                        "CELL_OUT_OF_BOUNDS",
                        (
                            f"position ({row}, {col}) is outside the "
                            f"{wafer['rows']}x{wafer['cols']} wafer"
                        ),
                    )
                cell = conn.execute(
                    """
                    SELECT full_row, full_col, verdict, source_retest_no,
                           source_machine_id, source_batch_id, source_batch_seq,
                           source_local_row, source_local_col
                      FROM map_cells
                     WHERE wafer_id = ? AND full_row = ? AND full_col = ?
                    """,
                    (wafer_id, row, col),
                ).fetchone()
        finally:
            conn.close()

        return {
            "wafer_id": wafer_id,
            "row": row,
            "col": col,
            "tested": cell is not None,
            "status": _status(wafer),
            "accepted_seq": wafer["accepted_seq"],
            "map_seq": wafer["committed_seq"],
            "cell": _cell_payload(cell) if cell is not None else None,
        }

    def list_batches(self, wafer_id: str) -> list[BatchInfo]:
        conn = connect(self.db_path)
        try:
            self._wafer_or_404(conn, wafer_id)
            rows = conn.execute(
                "SELECT * FROM batches WHERE wafer_id = ? ORDER BY seq",
                (wafer_id,),
            ).fetchall()
        finally:
            conn.close()
        return [
            BatchInfo(
                batch_id=row["batch_id"],
                seq=row["seq"],
                machine_id=row["machine_id"],
                origin_row=row["origin_row"],
                origin_col=row["origin_col"],
                rotation_deg=row["rotation_deg"],
                local_rows=row["local_rows"],
                local_cols=row["local_cols"],
                measured_cells=row["cell_count"],
                received_at=row["received_at"],
            )
            for row in rows
        ]

    def list_wafers(self) -> list[WaferSummary]:
        conn = connect(self.db_path)
        try:
            rows = conn.execute("SELECT * FROM wafers ORDER BY created_at").fetchall()
        finally:
            conn.close()
        return [
            WaferSummary(
                wafer_id=row["wafer_id"],
                rows=row["rows"],
                cols=row["cols"],
                status=_status(row),
                recompute_failed=_recompute_failed(row),
                pending_update=int(row["accepted_seq"]) > int(row["committed_seq"]),
                accepted_seq=row["accepted_seq"],
                map_seq=row["committed_seq"],
            )
            for row in rows
        ]

    # --------------------------------------------------------------- recompute
    def recompute(self, wafer_id: str) -> dict[str, Any]:
        """Bring the published map up to date with accepted data.

        Re-snapshots if newer data was accepted during computation, but never
        publishes a snapshot whose sequence is no longer the latest.
        """
        self.get_wafer_state(wafer_id)  # 404 early
        published = False
        for _ in range(self.max_recompute_rounds):
            try:
                snapshot = self._build_snapshot(wafer_id)
            except Exception as exc:
                seq = int(self.get_wafer_state(wafer_id)["accepted_seq"])
                if seq:
                    self.record_failure(wafer_id, seq, str(exc))
                raise
            if snapshot.seq == 0:
                return self.recompute_status(wafer_id, scheduled=False)
            if self.recompute_delay_seconds:
                time.sleep(self.recompute_delay_seconds)
            try:
                published = self._publish(snapshot)
            except Exception as exc:
                # Record which snapshot failed, so a newer acceptance afterwards
                # is reported as pending rather than as a failure.
                self.record_failure(wafer_id, snapshot.seq, str(exc))
                raise
            if published:
                break
        return self.recompute_status(wafer_id, scheduled=not published)

    def _build_snapshot(self, wafer_id: str) -> Snapshot:
        conn = connect(self.db_path)
        try:
            # One read transaction gives a consistent (accepted_seq, data) pair
            # even if acceptances commit while we read.
            with read_snapshot(conn):
                wafer = self._wafer_or_404(conn, wafer_id)
                target = int(wafer["accepted_seq"])
                cells = conn.execute(
                    """
                    SELECT full_row, full_col, verdict, retest_no, machine_id,
                           batch_id, batch_seq, local_row, local_col
                      FROM (
                            SELECT o.full_row, o.full_col, o.verdict, o.retest_no,
                                   o.machine_id, o.batch_id, o.local_row, o.local_col,
                                   b.seq AS batch_seq,
                                   ROW_NUMBER() OVER (
                                       PARTITION BY o.full_row, o.full_col
                                       ORDER BY o.retest_no DESC, b.seq DESC
                                   ) AS rn
                              FROM observations AS o
                              JOIN batches AS b ON b.batch_id = o.batch_id
                             WHERE o.wafer_id = ?
                           )
                     WHERE rn = 1
                     ORDER BY full_row, full_col
                    """,
                    (wafer_id,),
                ).fetchall()
            return Snapshot(
                wafer_id=wafer_id,
                seq=target,
                rows=int(wafer["rows"]),
                cols=int(wafer["cols"]),
                cells=cells,
            )
        finally:
            conn.close()

    def _publish(self, snapshot: Snapshot) -> bool:
        conn = connect(self.db_path)
        try:
            with transaction(conn, immediate=True):
                wafer = conn.execute(
                    "SELECT accepted_seq, committed_seq FROM wafers WHERE wafer_id = ?",
                    (snapshot.wafer_id,),
                ).fetchone()
                if wafer is None:
                    raise DomainError(
                        404,
                        "WAFER_NOT_FOUND",
                        f"wafer {snapshot.wafer_id!r} was removed",
                    )
                if int(wafer["accepted_seq"]) != snapshot.seq:
                    # Newer data accepted while computing: discard this snapshot.
                    return False
                if int(wafer["committed_seq"]) >= snapshot.seq:
                    # Someone already published this sequence.
                    return True
                now = utcnow()
                conn.execute(
                    "DELETE FROM map_cells WHERE wafer_id = ?", (snapshot.wafer_id,)
                )
                conn.executemany(
                    """
                    INSERT INTO map_cells (
                        wafer_id, full_row, full_col, verdict, source_retest_no,
                        source_machine_id, source_batch_id, source_batch_seq,
                        source_local_row, source_local_col, computed_seq
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    [
                        (
                            snapshot.wafer_id,
                            cell["full_row"],
                            cell["full_col"],
                            cell["verdict"],
                            cell["retest_no"],
                            cell["machine_id"],
                            cell["batch_id"],
                            cell["batch_seq"],
                            cell["local_row"],
                            cell["local_col"],
                            snapshot.seq,
                        )
                        for cell in snapshot.cells
                    ],
                )
                conn.execute(
                    """
                    UPDATE wafers
                       SET committed_seq = ?, updated_at = ?,
                           last_error = NULL, last_error_at = NULL,
                           last_error_seq = NULL
                     WHERE wafer_id = ?
                    """,
                    (snapshot.seq, now, snapshot.wafer_id),
                )
            return True
        finally:
            conn.close()

    def record_failure(self, wafer_id: str, seq: int, message: str) -> None:
        """Persist a failed recompute for the sequence it was building."""
        conn = connect(self.db_path)
        try:
            with transaction(conn, immediate=True):
                wafer = self._wafer_or_404(conn, wafer_id)
                if int(wafer["committed_seq"]) >= seq:
                    # This sequence is (or was) already published; a late
                    # failure must not mark the wafer as broken.
                    return
                now = utcnow()
                conn.execute(
                    """
                    UPDATE wafers
                       SET last_error = ?, last_error_at = ?, last_error_seq = ?,
                           updated_at = ?
                     WHERE wafer_id = ?
                    """,
                    (message[:2000], now, seq, now, wafer_id),
                )
        finally:
            conn.close()

    def recompute_status(self, wafer_id: str, *, scheduled: bool) -> dict[str, Any]:
        row = self.get_wafer_state(wafer_id)
        return {
            "wafer_id": wafer_id,
            "accepted_seq": row["accepted_seq"],
            "map_seq": row["committed_seq"],
            "status": _status(row),
            "recompute_failed": _recompute_failed(row),
            "scheduled": scheduled,
        }

    def pending_wafer_ids(self) -> list[str]:
        conn = connect(self.db_path)
        try:
            rows = conn.execute(
                """
                SELECT wafer_id FROM wafers
                 WHERE accepted_seq > committed_seq
                 ORDER BY updated_at
                """
            ).fetchall()
        finally:
            conn.close()
        return [row["wafer_id"] for row in rows]

    # ------------------------------------------------------------- retest QC
    # A checklist submission starts a new per-wafer ``revision`` and a
    # background verification.  The published conclusion stays valid only
    # while (revision, obs_seq, map_seq) still match the wafer's live
    # counters, so both a resubmitted checklist and a newly accepted batch
    # invalidate the old release *immediately*, without touching its row.
    def submit_qc_check(self, wafer_id: str, request: QcSubmitRequest) -> dict[str, Any]:
        conn = connect(self.db_path)
        try:
            with transaction(conn, immediate=True):
                wafer = self._wafer_or_404(conn, wafer_id)
                self._reject_qc_out_of_bounds(wafer, request.items)
                previous_release = self._current_release_locked(conn, wafer_id)
                row = conn.execute(
                    "SELECT COALESCE(MAX(revision), 0) AS rev "
                    "FROM qc_checks WHERE wafer_id = ?",
                    (wafer_id,),
                ).fetchone()
                revision = int(row["rev"]) + 1
                now = utcnow()
                # A resubmitted checklist supersedes whatever was still
                # waiting; a superseded check can never publish again.
                conn.execute(
                    """
                    UPDATE qc_checks
                       SET status = 'superseded', updated_at = ?
                     WHERE wafer_id = ? AND status = 'pending'
                    """,
                    (now, wafer_id),
                )
                check_id = uuid.uuid4().hex
                conn.execute(
                    """
                    INSERT INTO qc_checks (
                        check_id, wafer_id, revision, status, item_count,
                        created_at, updated_at
                    ) VALUES (?, ?, ?, 'pending', ?, ?, ?)
                    """,
                    (check_id, wafer_id, revision, len(request.items), now, now),
                )
                conn.executemany(
                    """
                    INSERT INTO qc_check_items (
                        check_id, full_row, full_col, expected_max_retest_no
                    ) VALUES (?, ?, ?, ?)
                    """,
                    [
                        (check_id, item.row, item.col, item.expected_max_retest_no)
                        for item in request.items
                    ],
                )
        finally:
            conn.close()

        return {
            "wafer_id": wafer_id,
            "check_id": check_id,
            "revision": revision,
            "status": "pending",
            "item_count": len(request.items),
            "previous_release_invalidated": previous_release is not None,
        }

    def _reject_qc_out_of_bounds(
        self, wafer: sqlite3.Row, items: list[Any]
    ) -> None:
        rows, cols = int(wafer["rows"]), int(wafer["cols"])
        offending = [
            item
            for item in items
            if not (0 <= item.row < rows and 0 <= item.col < cols)
        ]
        if not offending:
            return
        raise DomainError(
            422,
            "QC_CELL_OUT_OF_BOUNDS",
            (
                f"checklist rejected: {len(offending)} position(s) fall outside "
                f"the {rows}x{cols} wafer"
            ),
            {
                "offending_count": len(offending),
                "examples": [
                    {"row": item.row, "col": item.col}
                    for item in offending[:MAX_REPORTED_OFFENDING_CELLS]
                ],
            },
        )

    def _current_release_locked(
        self, conn: sqlite3.Connection, wafer_id: str
    ) -> sqlite3.Row | None:
        """The completed check whose conclusion is still the live release."""
        return conn.execute(
            """
            SELECT c.* FROM qc_checks AS c
              JOIN wafers AS w ON w.wafer_id = c.wafer_id
             WHERE c.wafer_id = ?
               AND c.status = 'completed'
               AND c.revision = (
                       SELECT MAX(revision) FROM qc_checks WHERE wafer_id = ?
                   )
               AND c.obs_seq = w.accepted_seq
               AND c.map_seq = w.committed_seq
            """,
            (wafer_id, wafer_id),
        ).fetchone()

    def get_qc_check(self, wafer_id: str, check_id: str) -> dict[str, Any]:
        conn = connect(self.db_path)
        try:
            with read_snapshot(conn):
                self._wafer_or_404(conn, wafer_id)
                check = conn.execute(
                    "SELECT * FROM qc_checks WHERE check_id = ? AND wafer_id = ?",
                    (check_id, wafer_id),
                ).fetchone()
                if check is None:
                    raise DomainError(
                        404,
                        "QC_CHECK_NOT_FOUND",
                        f"wafer {wafer_id!r} has no retest QC check {check_id!r}",
                    )
                return self._qc_payload(conn, check)
        finally:
            conn.close()

    def get_latest_qc_check(self, wafer_id: str) -> dict[str, Any]:
        conn = connect(self.db_path)
        try:
            with read_snapshot(conn):
                self._wafer_or_404(conn, wafer_id)
                check = conn.execute(
                    """
                    SELECT * FROM qc_checks
                     WHERE wafer_id = ?
                     ORDER BY revision DESC
                     LIMIT 1
                    """,
                    (wafer_id,),
                ).fetchone()
                if check is None:
                    raise DomainError(
                        404,
                        "QC_CHECK_NOT_FOUND",
                        f"wafer {wafer_id!r} has no retest QC checklist yet",
                    )
                return self._qc_payload(conn, check)
        finally:
            conn.close()

    def _qc_payload(
        self, conn: sqlite3.Connection, check: sqlite3.Row
    ) -> dict[str, Any]:
        wafer = self._wafer_or_404(conn, check["wafer_id"])
        latest_revision = int(
            conn.execute(
                "SELECT COALESCE(MAX(revision), 0) AS rev "
                "FROM qc_checks WHERE wafer_id = ?",
                (check["wafer_id"],),
            ).fetchone()["rev"]
        )
        # The release is valid only while checklist, observations and map all
        # still sit at the revision the conclusion was verified against.
        release_valid = (
            check["status"] == "completed"
            and int(check["revision"]) == latest_revision
            and check["obs_seq"] is not None
            and int(check["obs_seq"]) == int(wafer["accepted_seq"])
            and int(check["map_seq"]) == int(wafer["committed_seq"])
        )
        last_error = None
        if check["last_error"] is not None:
            last_error = {
                "message": check["last_error"],
                "at": check["last_error_at"],
            }
        return {
            "wafer_id": check["wafer_id"],
            "check_id": check["check_id"],
            "revision": check["revision"],
            "status": check["status"],
            "conclusion": check["conclusion"],
            "release_valid": release_valid,
            "item_count": check["item_count"],
            "obs_seq": check["obs_seq"],
            "map_seq": check["map_seq"],
            "current_accepted_seq": wafer["accepted_seq"],
            "current_map_seq": wafer["committed_seq"],
            "attempts": check["attempts"],
            "last_error": last_error,
            "findings": json.loads(check["findings"]) if check["findings"] else None,
            "summary": json.loads(check["summary"]) if check["summary"] else None,
            "created_at": check["created_at"],
            "updated_at": check["updated_at"],
            "completed_at": check["completed_at"],
        }

    def retry_qc_check(self, wafer_id: str, check_id: str) -> None:
        """Re-arm a pending check (after a failure or restart) for scheduling."""
        conn = connect(self.db_path)
        try:
            with transaction(conn, immediate=True):
                self._wafer_or_404(conn, wafer_id)
                check = conn.execute(
                    "SELECT * FROM qc_checks WHERE check_id = ? AND wafer_id = ?",
                    (check_id, wafer_id),
                ).fetchone()
                if check is None:
                    raise DomainError(
                        404,
                        "QC_CHECK_NOT_FOUND",
                        f"wafer {wafer_id!r} has no retest QC check {check_id!r}",
                    )
                if check["status"] != "pending":
                    raise DomainError(
                        409,
                        "QC_CHECK_NOT_PENDING",
                        (
                            f"check {check_id!r} is {check['status']}; only a "
                            "pending check can be retried (resubmit the "
                            "checklist to supersede a finished one)"
                        ),
                        {"status": check["status"]},
                    )
                conn.execute(
                    """
                    UPDATE qc_checks
                       SET last_error = NULL, last_error_at = NULL, updated_at = ?
                     WHERE check_id = ?
                    """,
                    (utcnow(), check_id),
                )
        finally:
            conn.close()

    def pending_qc_check_ids(self) -> list[str]:
        conn = connect(self.db_path)
        try:
            rows = conn.execute(
                "SELECT check_id FROM qc_checks WHERE status = 'pending' "
                "ORDER BY created_at"
            ).fetchall()
        finally:
            conn.close()
        return [row["check_id"] for row in rows]

    def run_qc_check(self, check_id: str) -> None:
        """Verify one pending checklist and publish its conclusion.

        A conclusion is published only while the checklist is still the
        latest revision and the snapshot's sequences are still the live ones;
        anything else is discarded and re-verified, so a late verification
        can never resurrect superseded data or an old release.
        """
        self._mark_qc_attempt(check_id)
        for _ in range(self.max_qc_rounds):
            try:
                snapshot = self._build_qc_snapshot(check_id)
            except Exception as exc:
                self.record_qc_failure(check_id, str(exc))
                raise
            if snapshot is None:
                return  # superseded, completed or removed while scheduled
            if snapshot.map_seq != snapshot.obs_seq:
                # Map and observations must describe the same revision before
                # any conclusion may be published: bring the map up to date.
                try:
                    self.recompute(snapshot.wafer_id)
                except Exception as exc:
                    self.record_qc_failure(check_id, str(exc))
                    raise
                continue
            if self.qc_delay_seconds:
                time.sleep(self.qc_delay_seconds)
            conclusion, findings, summary = _evaluate_qc_snapshot(snapshot)
            try:
                outcome = self._publish_qc(snapshot, conclusion, findings, summary)
            except Exception as exc:
                self.record_qc_failure(check_id, str(exc))
                raise
            if outcome == "stale":
                continue
            return  # published or superseded
        # Data kept changing faster than we could verify: the check stays
        # pending and can be retried explicitly.

    def _mark_qc_attempt(self, check_id: str) -> None:
        conn = connect(self.db_path)
        try:
            with transaction(conn, immediate=True):
                conn.execute(
                    """
                    UPDATE qc_checks
                       SET attempts = attempts + 1, updated_at = ?
                     WHERE check_id = ? AND status = 'pending'
                    """,
                    (utcnow(), check_id),
                )
        finally:
            conn.close()

    def _build_qc_snapshot(self, check_id: str) -> QcSnapshot | None:
        conn = connect(self.db_path)
        try:
            check = conn.execute(
                "SELECT * FROM qc_checks WHERE check_id = ?", (check_id,)
            ).fetchone()
            if check is None or check["status"] != "pending":
                return None
            items = conn.execute(
                """
                SELECT full_row, full_col, expected_max_retest_no
                  FROM qc_check_items
                 WHERE check_id = ?
                 ORDER BY full_row, full_col
                """,
                (check_id,),
            ).fetchall()
            # The item list drives joins against observations and the map.
            # It is connection-local and immutable for this check.
            conn.execute("DROP TABLE IF EXISTS temp._qc_items")
            conn.execute(
                """
                CREATE TEMP TABLE _qc_items (
                    full_row INTEGER NOT NULL,
                    full_col  INTEGER NOT NULL,
                    PRIMARY KEY (full_row, full_col)
                )
                """
            )
            try:
                conn.executemany(
                    "INSERT INTO temp._qc_items (full_row, full_col) VALUES (?, ?)",
                    [(item["full_row"], item["full_col"]) for item in items],
                )
                # One read transaction: sequences, observations and map cells
                # all come from the same database snapshot.
                with read_snapshot(conn):
                    wafer = self._wafer_or_404(conn, check["wafer_id"])
                    observations = conn.execute(
                        """
                        SELECT o.full_row, o.full_col, o.retest_no, o.verdict,
                               o.machine_id, o.batch_id, o.local_row, o.local_col
                          FROM observations AS o
                          JOIN temp._qc_items AS i
                            ON i.full_row = o.full_row AND i.full_col = o.full_col
                         WHERE o.wafer_id = ?
                         ORDER BY o.full_row, o.full_col, o.retest_no
                        """,
                        (check["wafer_id"],),
                    ).fetchall()
                    map_cells = conn.execute(
                        """
                        SELECT m.full_row, m.full_col, m.verdict,
                               m.source_retest_no, m.source_machine_id,
                               m.source_batch_id, m.source_local_row,
                               m.source_local_col
                          FROM map_cells AS m
                          JOIN temp._qc_items AS i
                            ON i.full_row = m.full_row AND i.full_col = m.full_col
                         WHERE m.wafer_id = ?
                        """,
                        (check["wafer_id"],),
                    ).fetchall()
            finally:
                conn.execute("DROP TABLE IF EXISTS temp._qc_items")
            return QcSnapshot(
                check_id=check_id,
                wafer_id=check["wafer_id"],
                revision=int(check["revision"]),
                obs_seq=int(wafer["accepted_seq"]),
                map_seq=int(wafer["committed_seq"]),
                items=items,
                observations=observations,
                map_cells=map_cells,
            )
        finally:
            conn.close()

    def _publish_qc(
        self,
        snapshot: QcSnapshot,
        conclusion: str,
        findings: dict[str, Any],
        summary: dict[str, Any],
    ) -> str:
        """Publish a conclusion; returns 'published', 'stale' or 'superseded'."""
        conn = connect(self.db_path)
        try:
            with transaction(conn, immediate=True):
                check = conn.execute(
                    "SELECT * FROM qc_checks WHERE check_id = ?",
                    (snapshot.check_id,),
                ).fetchone()
                if check is None or check["status"] != "pending":
                    return "superseded"
                wafer = conn.execute(
                    "SELECT accepted_seq, committed_seq FROM wafers WHERE wafer_id = ?",
                    (snapshot.wafer_id,),
                ).fetchone()
                if wafer is None:
                    raise DomainError(
                        404,
                        "WAFER_NOT_FOUND",
                        f"wafer {snapshot.wafer_id!r} was removed",
                    )
                if (
                    int(wafer["accepted_seq"]) != snapshot.obs_seq
                    or int(wafer["committed_seq"]) != snapshot.map_seq
                ):
                    # New measurements (or a newer map) landed while we were
                    # verifying: this conclusion is stale, discard it.
                    return "stale"
                latest = conn.execute(
                    "SELECT COALESCE(MAX(revision), 0) AS rev "
                    "FROM qc_checks WHERE wafer_id = ?",
                    (snapshot.wafer_id,),
                ).fetchone()
                now = utcnow()
                if int(latest["rev"]) != snapshot.revision:
                    # A newer checklist was submitted in the meantime.
                    conn.execute(
                        "UPDATE qc_checks SET status = 'superseded', "
                        "updated_at = ? WHERE check_id = ?",
                        (now, snapshot.check_id),
                    )
                    return "superseded"
                conn.execute(
                    """
                    UPDATE qc_checks
                       SET status = 'completed', conclusion = ?, findings = ?,
                           summary = ?, obs_seq = ?, map_seq = ?,
                           completed_at = ?, updated_at = ?,
                           last_error = NULL, last_error_at = NULL
                     WHERE check_id = ?
                    """,
                    (
                        conclusion,
                        json.dumps(findings),
                        json.dumps(summary),
                        snapshot.obs_seq,
                        snapshot.map_seq,
                        now,
                        now,
                        snapshot.check_id,
                    ),
                )
                return "published"
        finally:
            conn.close()

    def record_qc_failure(self, check_id: str, message: str) -> None:
        """Persist a failed verification attempt; the check stays pending."""
        conn = connect(self.db_path)
        try:
            with transaction(conn, immediate=True):
                check = conn.execute(
                    "SELECT status FROM qc_checks WHERE check_id = ?", (check_id,)
                ).fetchone()
                if check is None or check["status"] != "pending":
                    return
                now = utcnow()
                conn.execute(
                    """
                    UPDATE qc_checks
                       SET last_error = ?, last_error_at = ?, updated_at = ?
                     WHERE check_id = ?
                    """,
                    (message[:2000], now, now, check_id),
                )
        finally:
            conn.close()

    @staticmethod
    def _wafer_or_404(conn: sqlite3.Connection, wafer_id: str) -> sqlite3.Row:
        row = conn.execute(
            "SELECT * FROM wafers WHERE wafer_id = ?", (wafer_id,)
        ).fetchone()
        if row is None:
            raise DomainError(
                404, "WAFER_NOT_FOUND", f"wafer {wafer_id!r} does not exist"
            )
        return row


def _cell_payload(cell: sqlite3.Row) -> dict[str, Any]:
    return {
        "row": cell["full_row"],
        "col": cell["full_col"],
        "verdict": cell["verdict"],
        "retest_no": cell["source_retest_no"],
        "source": {
            "machine_id": cell["source_machine_id"],
            "batch_id": cell["source_batch_id"],
            "batch_seq": cell["source_batch_seq"],
            "local_row": cell["source_local_row"],
            "local_col": cell["source_local_col"],
        },
    }


def _evaluate_qc_snapshot(
    snapshot: QcSnapshot,
) -> tuple[str, dict[str, Any], dict[str, Any]]:
    """Compare one checklist against its snapshot.

    For every position the accepted records must run continuously from 0 to
    the expected highest retest number (no 缺号), contain nothing above it
    (no 超号), and the published map's highest retest number and source must
    correspond to those records (no 地图不一致).  Returns the conclusion, the
    (capped) findings and the exact summary counts.
    """
    observations: dict[tuple[int, int], dict[int, sqlite3.Row]] = {}
    for row in snapshot.observations:
        observations.setdefault((row["full_row"], row["full_col"]), {})[
            row["retest_no"]
        ] = row
    map_by_cell = {
        (cell["full_row"], cell["full_col"]): cell for cell in snapshot.map_cells
    }

    missing: list[dict[str, Any]] = []
    exceeded: list[dict[str, Any]] = []
    inconsistent: list[dict[str, Any]] = []
    cells_missing = 0
    cells_exceeded = 0
    cells_inconsistent = 0
    truncated = False

    for item in snapshot.items:
        position = (item["full_row"], item["full_col"])
        expected = int(item["expected_max_retest_no"])
        observed = observations.get(position, {})

        # --- continuity: every number in 0..expected must be accepted
        observed_within = sum(1 for number in observed if number <= expected)
        missing_count = expected + 1 - observed_within
        if missing_count:
            cells_missing += 1
            if len(missing) < MAX_QC_FINDINGS_PER_CATEGORY:
                reported: list[int] = []
                number = 0
                while (
                    len(reported) < MAX_QC_NUMBERS_PER_CELL
                    and len(reported) < missing_count
                ):
                    if number not in observed:
                        reported.append(number)
                    number += 1
                missing.append(
                    {
                        "row": position[0],
                        "col": position[1],
                        "expected_max_retest_no": expected,
                        "missing_count": missing_count,
                        "missing_retest_nos": reported,
                        "truncated": missing_count > len(reported),
                    }
                )
            else:
                truncated = True

        # --- excess: nothing above the expected number may exist
        extra = sorted(number for number in observed if number > expected)
        if extra:
            cells_exceeded += 1
            if len(exceeded) < MAX_QC_FINDINGS_PER_CATEGORY:
                reported_extra = extra[:MAX_QC_NUMBERS_PER_CELL]
                exceeded.append(
                    {
                        "row": position[0],
                        "col": position[1],
                        "expected_max_retest_no": expected,
                        "extra_count": len(extra),
                        "extra_retest_nos": reported_extra,
                        "truncated": len(extra) > len(reported_extra),
                    }
                )
            else:
                truncated = True

        # --- map consistency: the published map must reflect these records
        map_cell = map_by_cell.get(position)
        observed_max = max(observed) if observed else None
        reason: str | None = None
        if observed:
            if map_cell is None:
                reason = "map_missing_cell"
            elif int(map_cell["source_retest_no"]) != observed_max:
                reason = "map_retest_mismatch"
            else:
                winner = observed[observed_max]
                if (
                    map_cell["verdict"] != winner["verdict"]
                    or map_cell["source_machine_id"] != winner["machine_id"]
                    or map_cell["source_batch_id"] != winner["batch_id"]
                    or int(map_cell["source_local_row"]) != int(winner["local_row"])
                    or int(map_cell["source_local_col"]) != int(winner["local_col"])
                ):
                    reason = "map_source_mismatch"
        elif map_cell is not None:
            reason = "map_without_observation"
        if reason is not None:
            cells_inconsistent += 1
            if len(inconsistent) < MAX_QC_FINDINGS_PER_CATEGORY:
                inconsistent.append(
                    {
                        "row": position[0],
                        "col": position[1],
                        "expected_max_retest_no": expected,
                        "reason": reason,
                        "observed_max_retest_no": observed_max,
                        "map_retest_no": (
                            int(map_cell["source_retest_no"])
                            if map_cell is not None
                            else None
                        ),
                    }
                )
            else:
                truncated = True

    conclusion = (
        "releasable"
        if not (cells_missing or cells_exceeded or cells_inconsistent)
        else "not_releasable"
    )
    findings = {
        "missing": missing,
        "exceeded": exceeded,
        "map_inconsistent": inconsistent,
    }
    summary = {
        "item_count": len(snapshot.items),
        "cells_with_missing": cells_missing,
        "cells_with_exceeded": cells_exceeded,
        "cells_with_map_inconsistency": cells_inconsistent,
        "findings_truncated": truncated,
    }
    return conclusion, findings, summary
