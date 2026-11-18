"""Retest-chain QC: checklist verification, release validity, interleaving."""

from __future__ import annotations

import sqlite3
import threading
import time

import pytest
from fastapi.testclient import TestClient

from app.db import init_db
from app.main import Settings, create_app
from app.schemas import BatchRequest, QcSubmitRequest
from app.service import WaferService
from tests.conftest import batch, cell, wait_until


def upload(
    client: TestClient,
    wafer: str,
    machine: str,
    grid: list[list[dict | None]],
    origin: tuple[int, int] = (0, 0),
) -> dict:
    response = client.post(
        f"/wafers/{wafer}/batches",
        json=batch(machine=machine, origin=origin, grid=grid),
    )
    assert response.status_code == 202, response.text
    return response.json()


def ready(client: TestClient, wafer: str) -> dict:
    return wait_until(client, wafer, lambda s: s["status"] == "ready")


def submit_qc(client: TestClient, wafer: str, items: list[dict]) -> dict:
    response = client.post(f"/wafers/{wafer}/retest-qc", json={"items": items})
    assert response.status_code == 202, response.text
    return response.json()


def wait_qc(
    client: TestClient, wafer: str, check_id: str, predicate, timeout: float = 5.0
) -> dict:
    deadline = time.monotonic() + timeout
    body: dict = {}
    while time.monotonic() < deadline:
        body = client.get(f"/wafers/{wafer}/retest-qc/{check_id}").json()
        if predicate(body):
            return body
        time.sleep(0.02)
    raise AssertionError(f"timed out waiting for qc check; last body was {body}")


def completed(client: TestClient, wafer: str, check_id: str) -> dict:
    return wait_qc(client, wafer, check_id, lambda b: b["status"] == "completed")


def item(row: int, col: int, expected: int) -> dict:
    return {"row": row, "col": col, "expected_max_retest_no": expected}


# ------------------------------------------------------------------ verdicts
def test_complete_chain_is_releasable(client: TestClient) -> None:
    upload(client, "W", "M1", [[cell("PASS", 0), cell("FAIL", 0)]])
    upload(client, "W", "M1", [[cell("PASS", 1), cell("PASS", 1)]])
    upload(client, "W", "M2", [[cell("FAIL", 2)]])
    ready(client, "W")

    accepted = submit_qc(client, "W", [item(0, 0, 2), item(0, 1, 1)])
    assert accepted["status"] == "pending"
    assert accepted["revision"] == 1
    assert accepted["previous_release_invalidated"] is False

    body = completed(client, "W", accepted["check_id"])
    assert body["conclusion"] == "releasable"
    assert body["release_valid"] is True
    assert body["obs_seq"] == body["current_accepted_seq"] == 3
    assert body["map_seq"] == body["current_map_seq"] == 3
    assert body["findings"] == {"missing": [], "exceeded": [], "map_inconsistent": []}
    assert body["summary"] == {
        "item_count": 2,
        "cells_with_missing": 0,
        "cells_with_exceeded": 0,
        "cells_with_map_inconsistency": 0,
        "findings_truncated": False,
    }
    # The same check is reachable through the "latest" view.
    latest = client.get("/wafers/W/retest-qc/latest").json()
    assert latest["check_id"] == accepted["check_id"]
    assert latest["release_valid"] is True


def test_missing_retest_numbers_are_reported(client: TestClient) -> None:
    # Map will show retest 2 for (0,0), but retest 1 never landed.
    upload(client, "W", "M1", [[cell("PASS", 0), cell("PASS", 0)]])
    upload(client, "W", "M1", [[cell("FAIL", 2)]])
    ready(client, "W")

    accepted = submit_qc(
        client, "W", [item(0, 0, 2), item(0, 1, 0), item(3, 3, 1)]
    )
    body = completed(client, "W", accepted["check_id"])
    assert body["conclusion"] == "not_releasable"
    # A negative conclusion is still the current, valid release.
    assert body["release_valid"] is True

    missing = {(f["row"], f["col"]): f for f in body["findings"]["missing"]}
    assert missing[(0, 0)]["missing_retest_nos"] == [1]
    assert missing[(0, 0)]["missing_count"] == 1
    # (3,3) was never tested: the whole chain 0..1 is absent.
    assert missing[(3, 3)]["missing_retest_nos"] == [0, 1]
    assert missing[(3, 3)]["missing_count"] == 2
    assert (0, 1) not in missing
    assert body["summary"]["cells_with_missing"] == 2
    assert body["summary"]["item_count"] == 3


def test_higher_retest_numbers_are_reported(client: TestClient) -> None:
    for retest in range(4):
        upload(client, "W", "M1", [[cell("PASS", retest)]])
    ready(client, "W")

    accepted = submit_qc(client, "W", [item(0, 0, 2)])
    body = completed(client, "W", accepted["check_id"])
    assert body["conclusion"] == "not_releasable"
    finding = body["findings"]["exceeded"][0]
    assert (finding["row"], finding["col"]) == (0, 0)
    assert finding["extra_retest_nos"] == [3]
    assert finding["extra_count"] == 1
    assert body["summary"]["cells_with_exceeded"] == 1


def test_missing_numbers_are_truncated_per_cell(client: TestClient) -> None:
    upload(client, "W", "M1", [[cell("PASS", 0)]])
    ready(client, "W")
    accepted = submit_qc(client, "W", [item(0, 0, 100)])
    body = completed(client, "W", accepted["check_id"])
    finding = body["findings"]["missing"][0]
    assert finding["missing_count"] == 100  # 1..100; retest 0 is present
    assert len(finding["missing_retest_nos"]) == 50
    assert finding["missing_retest_nos"][:3] == [1, 2, 3]
    assert finding["truncated"] is True


def test_map_inconsistency_is_detected(client: TestClient, db_path: str) -> None:
    upload(client, "W", "M1", [[cell("PASS", 0)]])
    upload(client, "W", "M1", [[cell("FAIL", 1)]])
    ready(client, "W")
    # Corrupt the published map: point the cell back at the older retest.
    conn = sqlite3.connect(db_path)
    try:
        conn.execute(
            "UPDATE map_cells SET source_retest_no = 0, verdict = 'PASS' "
            "WHERE wafer_id = 'W' AND full_row = 0 AND full_col = 0"
        )
        conn.commit()
    finally:
        conn.close()

    accepted = submit_qc(client, "W", [item(0, 0, 1)])
    body = completed(client, "W", accepted["check_id"])
    assert body["conclusion"] == "not_releasable"
    finding = body["findings"]["map_inconsistent"][0]
    assert finding["reason"] == "map_retest_mismatch"
    assert finding["observed_max_retest_no"] == 1
    assert finding["map_retest_no"] == 0


def test_map_source_mismatch_is_detected(client: TestClient, db_path: str) -> None:
    upload(client, "W", "M1", [[cell("PASS", 0)]])
    ready(client, "W")
    conn = sqlite3.connect(db_path)
    try:
        conn.execute(
            "UPDATE map_cells SET source_machine_id = 'GHOST' "
            "WHERE wafer_id = 'W' AND full_row = 0 AND full_col = 0"
        )
        conn.commit()
    finally:
        conn.close()

    accepted = submit_qc(client, "W", [item(0, 0, 0)])
    body = completed(client, "W", accepted["check_id"])
    assert body["conclusion"] == "not_releasable"
    finding = body["findings"]["map_inconsistent"][0]
    assert finding["reason"] == "map_source_mismatch"
    assert finding["observed_max_retest_no"] == 0
    assert finding["map_retest_no"] == 0


def test_map_cell_without_observation_is_detected(
    client: TestClient, db_path: str
) -> None:
    upload(client, "W", "M1", [[cell("PASS", 0)]])
    ready(client, "W")
    # Graft a phantom cell onto the map (as if a publish went wrong).
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    try:
        source = conn.execute(
            "SELECT * FROM map_cells WHERE wafer_id = 'W'"
        ).fetchone()
        conn.execute(
            """
            INSERT INTO map_cells (
                wafer_id, full_row, full_col, verdict, source_retest_no,
                source_machine_id, source_batch_id, source_batch_seq,
                source_local_row, source_local_col, computed_seq
            ) VALUES ('W', 4, 4, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                source["verdict"],
                source["source_retest_no"],
                source["source_machine_id"],
                source["source_batch_id"],
                source["source_batch_seq"],
                source["source_local_row"],
                source["source_local_col"],
                source["computed_seq"],
            ),
        )
        conn.commit()
    finally:
        conn.close()

    accepted = submit_qc(client, "W", [item(0, 0, 0), item(4, 4, 0)])
    body = completed(client, "W", accepted["check_id"])
    assert body["conclusion"] == "not_releasable"
    inconsistent = {
        (f["row"], f["col"]): f for f in body["findings"]["map_inconsistent"]
    }
    assert inconsistent[(4, 4)]["reason"] == "map_without_observation"
    assert inconsistent[(4, 4)]["observed_max_retest_no"] is None
    # (4,4) also has no accepted records at all: the whole chain is missing.
    missing = {(f["row"], f["col"]): f for f in body["findings"]["missing"]}
    assert missing[(4, 4)]["missing_retest_nos"] == [0]


# ----------------------------------------------------------- release validity
def test_new_acceptance_invalidates_release_immediately(client: TestClient) -> None:
    upload(client, "W", "M1", [[cell("PASS", 0)]])
    ready(client, "W")
    accepted = submit_qc(client, "W", [item(0, 0, 0)])
    assert completed(client, "W", accepted["check_id"])["release_valid"] is True

    upload(client, "W", "M1", [[cell("PASS", 1)]])
    # No QC rerun is needed: acceptance alone voids the old release at once.
    stale = client.get(f"/wafers/W/retest-qc/{accepted['check_id']}").json()
    assert stale["release_valid"] is False
    assert stale["obs_seq"] == 1
    assert stale["current_accepted_seq"] == 2

    # Even after the map catches up, the old release never revives itself.
    ready(client, "W")
    stale = client.get(f"/wafers/W/retest-qc/{accepted['check_id']}").json()
    assert stale["release_valid"] is False
    assert stale["current_map_seq"] == 2

    # Re-verifying the same checklist publishes a fresh, valid release.
    recheck = submit_qc(client, "W", [item(0, 0, 1)])
    assert recheck["previous_release_invalidated"] is False  # already invalid
    body = completed(client, "W", recheck["check_id"])
    assert body["conclusion"] == "releasable"
    assert body["release_valid"] is True
    assert body["obs_seq"] == 2


def test_resubmission_invalidates_previous_release(client: TestClient) -> None:
    upload(client, "W", "M1", [[cell("PASS", 0)]])
    ready(client, "W")
    first = submit_qc(client, "W", [item(0, 0, 0)])
    assert completed(client, "W", first["check_id"])["release_valid"] is True

    second = submit_qc(client, "W", [item(0, 0, 0)])
    assert second["revision"] == first["revision"] + 1
    assert second["previous_release_invalidated"] is True

    old = client.get(f"/wafers/W/retest-qc/{first['check_id']}").json()
    assert old["release_valid"] is False
    body = completed(client, "W", second["check_id"])
    assert body["release_valid"] is True
    assert client.get("/wafers/W/retest-qc/latest").json()["check_id"] == second[
        "check_id"
    ]


def test_superseded_check_cannot_publish(tmp_path) -> None:
    """A checklist resubmitted mid-verification wins; the late one is void."""
    db = str(tmp_path / "qc.db")
    init_db(db)
    service = WaferService(db, qc_delay_seconds=0.3)
    service.accept_batch("W", BatchRequest(**batch(machine="M1", grid=[[cell("PASS", 0)]])))
    service.recompute("W")
    first = service.submit_qc_check(
        "W", QcSubmitRequest(items=[item(0, 0, 0)])
    )

    worker = threading.Thread(target=service.run_qc_check, args=(first["check_id"],))
    worker.start()
    time.sleep(0.1)  # let the first check take its snapshot
    second = service.submit_qc_check(
        "W", QcSubmitRequest(items=[item(0, 0, 0)])
    )
    worker.join()

    old = service.get_qc_check("W", first["check_id"])
    assert old["status"] == "superseded"
    assert old["conclusion"] is None

    service.run_qc_check(second["check_id"])
    latest = service.get_latest_qc_check("W")
    assert latest["check_id"] == second["check_id"]
    assert latest["status"] == "completed"
    assert latest["release_valid"] is True


def test_upload_during_check_forces_reverification(tmp_path) -> None:
    """Uploads proceed during verification; a stale conclusion is discarded."""
    db = str(tmp_path / "qc2.db")
    init_db(db)
    service = WaferService(db, qc_delay_seconds=0.3)
    service.accept_batch("W", BatchRequest(**batch(machine="M1", grid=[[cell("PASS", 0)]])))
    service.recompute("W")
    check = service.submit_qc_check("W", QcSubmitRequest(items=[item(0, 0, 1)]))
    # Right now retest 1 is missing: a snapshot taken immediately would
    # conclude "not releasable".

    worker = threading.Thread(target=service.run_qc_check, args=(check["check_id"],))
    worker.start()
    time.sleep(0.1)  # snapshot taken, verifier now sleeping
    # The missing retest lands while the check is in flight.
    service.accept_batch("W", BatchRequest(**batch(machine="M1", grid=[[cell("PASS", 1)]])))
    worker.join()

    body = service.get_qc_check("W", check["check_id"])
    # The stale "not releasable" was never published; the conclusion reflects
    # the now-complete chain at the new revision.
    assert body["status"] == "completed"
    assert body["conclusion"] == "releasable"
    assert body["obs_seq"] == body["current_accepted_seq"] == 2
    assert body["map_seq"] == body["current_map_seq"] == 2
    assert body["release_valid"] is True


def test_check_waits_for_pending_map(client: TestClient) -> None:
    upload(client, "W", "M1", [[cell("PASS", 0)]])
    ready(client, "W")
    # Slow the map rebuild down so the checklist is submitted while the map
    # is still pending: the check must wait for the same revision.
    client.app.state.service.recompute_delay_seconds = 0.3
    upload(client, "W", "M1", [[cell("PASS", 1)]])
    assert client.get("/wafers/W/status").json()["status"] == "pending_update"

    accepted = submit_qc(client, "W", [item(0, 0, 1)])
    body = completed(client, "W", accepted["check_id"])
    assert body["conclusion"] == "releasable"
    assert body["obs_seq"] == body["map_seq"] == 2
    assert body["release_valid"] is True


# ------------------------------------------------------------------- recovery
def test_failed_check_stays_pending_and_can_be_retried(
    db_path: str, monkeypatch
) -> None:
    service = WaferService(db_path)
    service.accept_batch("W", BatchRequest(**batch(machine="M1", grid=[[cell("PASS", 0)]])))
    service.recompute("W")
    check = service.submit_qc_check("W", QcSubmitRequest(items=[item(0, 0, 0)]))

    def boom(*_args) -> None:
        raise RuntimeError("verifier exploded")

    monkeypatch.setattr(service, "_publish_qc", boom)
    with pytest.raises(RuntimeError):
        service.run_qc_check(check["check_id"])

    failed = service.get_qc_check("W", check["check_id"])
    assert failed["status"] == "pending"
    assert failed["last_error"]["message"] == "verifier exploded"
    assert failed["conclusion"] is None

    # "Restart": a new service over the same database sees the pending check.
    monkeypatch.undo()
    revived = WaferService(db_path)
    assert check["check_id"] in revived.pending_qc_check_ids()
    revived.run_qc_check(check["check_id"])
    healed = revived.get_qc_check("W", check["check_id"])
    assert healed["status"] == "completed"
    assert healed["conclusion"] == "releasable"
    assert healed["last_error"] is None
    assert healed["release_valid"] is True


def test_startup_recovers_pending_checks(db_path: str) -> None:
    service = WaferService(db_path)
    service.accept_batch("W", BatchRequest(**batch(machine="M1", grid=[[cell("PASS", 0)]])))
    service.recompute("W")
    check = service.submit_qc_check("W", QcSubmitRequest(items=[item(0, 0, 0)]))

    # "Restart" before any verifier ran: lifespan recovery picks it up.
    app = create_app(Settings(db_path=db_path, scheduler_workers=1, qc_scheduler_workers=1))
    with TestClient(app) as client:
        body = completed(client, "W", check["check_id"])
        assert body["conclusion"] == "releasable"
        assert body["release_valid"] is True


def test_retry_endpoint_reruns_a_failed_check(
    client: TestClient, monkeypatch
) -> None:
    upload(client, "W", "M1", [[cell("PASS", 0)]])
    ready(client, "W")

    service = client.app.state.service
    monkeypatch.setattr(
        service,
        "_publish_qc",
        lambda *_args: (_ for _ in ()).throw(RuntimeError("verifier exploded")),
    )
    accepted = submit_qc(client, "W", [item(0, 0, 0)])
    wait_qc(client, "W", accepted["check_id"], lambda b: b["last_error"] is not None)
    monkeypatch.undo()

    response = client.post(f"/wafers/W/retest-qc/{accepted['check_id']}/retry")
    assert response.status_code == 202
    assert response.json()["status"] == "pending"
    body = completed(client, "W", accepted["check_id"])
    assert body["conclusion"] == "releasable"
    assert body["last_error"] is None
    assert body["attempts"] >= 2


def test_retry_rejects_a_finished_check(client: TestClient) -> None:
    upload(client, "W", "M1", [[cell("PASS", 0)]])
    ready(client, "W")
    accepted = submit_qc(client, "W", [item(0, 0, 0)])
    completed(client, "W", accepted["check_id"])

    response = client.post(f"/wafers/W/retest-qc/{accepted['check_id']}/retry")
    assert response.status_code == 409
    assert response.json()["error"]["code"] == "QC_CHECK_NOT_PENDING"


# ------------------------------------------------------------------ validation
def test_qc_validation(client: TestClient) -> None:
    upload(client, "W", "M1", [[cell("PASS", 0)]])
    ready(client, "W")

    empty = client.post("/wafers/W/retest-qc", json={"items": []})
    assert empty.status_code == 422
    assert empty.json()["error"]["code"] == "VALIDATION_ERROR"

    duplicate = client.post(
        "/wafers/W/retest-qc", json={"items": [item(0, 0, 0), item(0, 0, 1)]}
    )
    assert duplicate.status_code == 422
    assert duplicate.json()["error"]["code"] == "VALIDATION_ERROR"

    negative = client.post("/wafers/W/retest-qc", json={"items": [item(0, 0, -1)]})
    assert negative.status_code == 422

    out_of_bounds = client.post("/wafers/W/retest-qc", json={"items": [item(9, 0, 0)]})
    assert out_of_bounds.status_code == 422
    assert out_of_bounds.json()["error"]["code"] == "QC_CELL_OUT_OF_BOUNDS"

    unknown_wafer = client.post("/wafers/NOPE/retest-qc", json={"items": [item(0, 0, 0)]})
    assert unknown_wafer.status_code == 404
    assert unknown_wafer.json()["error"]["code"] == "WAFER_NOT_FOUND"

    assert client.get("/wafers/W/retest-qc/latest").status_code == 404
    assert client.get("/wafers/W/retest-qc/deadbeef").status_code == 404
    assert client.get("/wafers/NOPE/retest-qc/deadbeef").status_code == 404


def test_checklist_on_empty_wafer_reports_everything_missing(
    client: TestClient,
) -> None:
    upload(client, "W", "M1", [[cell("PASS", 0)]], origin=(4, 4))
    ready(client, "W")
    accepted = submit_qc(client, "W", [item(0, 0, 1)])
    body = completed(client, "W", accepted["check_id"])
    assert body["conclusion"] == "not_releasable"
    finding = body["findings"]["missing"][0]
    assert finding["missing_retest_nos"] == [0, 1]


def test_openapi_lists_retest_qc_endpoints(client: TestClient) -> None:
    paths = client.get("/openapi.json").json()["paths"]
    assert "/wafers/{wafer_id}/retest-qc" in paths
    assert "/wafers/{wafer_id}/retest-qc/latest" in paths
    assert "/wafers/{wafer_id}/retest-qc/{check_id}" in paths
    assert "/wafers/{wafer_id}/retest-qc/{check_id}/retry" in paths
