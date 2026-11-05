"""Deterministic tests of campaign splits, manifests and safe resume, no live API."""
from __future__ import annotations

import hashlib
import json
import sys
from argparse import Namespace
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from fsi.client import ApiError
from fsi.harvest import (
    atomic_write, audit_campaign, campaign_id, campaign_path, count_preflight,
    create_campaign, execute_campaign, leaf_parts, load_campaign, oversized,
    partition_id, split_window, summary, try_adopt_existing, verified_silver,
)


def args(root: Path, **options) -> Namespace:
    base = dict(
        output=str(root), fy_start=2026, fy_end=2026,
        agency="Department of Transportation", all_agencies=False,
        execute=True, max_exports=1, adopt_existing=False,
        max_wait=60, poll=1, max_mb=50,
    )
    base.update(options)
    return Namespace(**base)


def test_fy2023_to_fy2026_48_partitions():
    result = create_campaign(2023, 2026, "Department of Transportation")
    parts = leaf_parts(result)
    assert len(parts) == 48
    assert parts[0][1]["start"] == "2022-10-01"
    assert parts[-1][1]["end"] == "2026-09-30"
    assert summary(result)["complete"] is False


def test_campaign_is_stable_and_agency_scoped():
    a = campaign_id(2023, 2026, "Department of Transportation")
    b = campaign_id(2023, 2026, "Department of Defense")
    assert a != b
    assert a == campaign_id(2023, 2026, "Department of Transportation")


def test_split_month_preserves_all_dates():
    original = {"start": "2026-09-01", "end": "2026-09-30", "fiscal_year": 2026}
    a, b = split_window(original)
    assert a["start"] == "2026-09-01"
    assert a["end"] == "2026-09-15"
    assert b["start"] == "2026-09-16"
    assert b["end"] == "2026-09-30"
    assert a["status"] == b["status"] == "planned"


def test_split_one_day_is_protected():
    with pytest.raises(ApiError, match="single day"):
        split_window({"start": "2026-09-10", "end": "2026-09-10"})


def test_count_never_ignores_cap_flags():
    assert oversized({"calculated_count": 400, "maximum_limit": 500,
                      "transaction_rows_gt_limit": True})
    assert oversized({"calculated_count": 501, "maximum_limit": 500})
    assert not oversized({"calculated_count": 400, "maximum_limit": 500})


def test_count_agency_and_transaction_count_must_agree():
    class Fake:
        def post(self, path, payload):
            assert payload["filters"]["agencies"][0]["type"] == "awarding"
            return {"calculated_count": 5, "calculated_transaction_count": 6,
                    "maximum_limit": 500}
    part = {"start": "2026-09-01", "end": "2026-09-30"}
    with pytest.raises(ApiError, match="disagreement"):
        count_preflight(Fake(), part, "Department of Transportation")


def test_campaign_checkpoint_persists_resumes(tmp_path):
    path = campaign_path(tmp_path, 2026, 2026, None)
    initial = load_campaign(path, 2026, 2026, None)
    key = partition_id("2025-10-01", "2025-10-31")
    initial["partitions"][key]["status"] = "empty"
    atomic_write(path, initial)
    restored = load_campaign(path, 2026, 2026, None)
    assert restored["partitions"][key]["status"] == "empty"
    assert not path.with_suffix(".json.tmp").exists()
    with pytest.raises(ApiError, match="parameters do not match"):
        load_campaign(path, 2026, 2026, "Department of Transportation")


def test_preflight_over_limit_splits_without_export(tmp_path, monkeypatch):
    import fsi.cli as cli
    calls = []

    class Fake:
        def post(self, path, body):
            calls.append(path)
            return {"calculated_count": 1000, "maximum_limit": 500,
                    "rows_gt_limit": True, "transaction_rows_gt_limit": True}

    def no_export(*_args, **_kwargs):
        raise AssertionError("Should never export oversized month")

    monkeypatch.setattr(cli, "submit_bulk_job", no_export)
    path = campaign_path(tmp_path, 2026, 2026, "Department of Transportation")
    state = load_campaign(path, 2026, 2026, "Department of Transportation")
    assert execute_campaign(state, path, args(tmp_path), Fake()) == 0
    stored = json.loads(path.read_text())
    assert stored["partitions"]["2025-10-01__2025-10-31"]["status"] == "split"
    # Every initial month was split; children will be processed next invocation.
    assert len(stored["partitions"]) == 36
    assert len(calls) == 12


def test_empty_partitions_are_final_without_file(tmp_path, monkeypatch):
    import fsi.cli as cli
    monkeypatch.setattr(cli, "submit_bulk_job", lambda *_a, **_kw:
                        (_ for _ in ()).throw(AssertionError("No export expected")))

    class Fake:
        def post(self, *a):
            return {"calculated_count": 0, "calculated_transaction_count": 0,
                    "maximum_limit": 500}

    path = campaign_path(tmp_path, 2026, 2026, None)
    state = load_campaign(path, 2026, 2026, None)
    assert execute_campaign(state, path, args(tmp_path), Fake()) == 0
    assert summary(state)["complete"] is True
    assert summary(state)["empty"] == 12


def test_adopt_previous_verified_job(tmp_path):
    raw = tmp_path / "data/raw/bulk" / "2026-09-01_2026-09-30_previous"
    raw.mkdir(parents=True)
    cov = {"start": "2026-09-01", "end": "2026-09-30",
           "agency": "Department of Transportation"}
    job = raw / "job.json"
    zip_file = raw / "previous.zip"
    zip_file.write_bytes(b"example source")
    job.write_text(json.dumps({"status": "verified", "coverage": cov,
                               "file_name": zip_file.name,
                               "download_sha256": hashlib.sha256(
                                   zip_file.read_bytes()
                               ).hexdigest()}),
                   encoding="utf-8")
    silver = tmp_path / "data/silver/jobs" / raw.name
    silver.mkdir(parents=True)
    fact = silver / "fact_contract_transactions.parquet"
    fact.write_bytes(b"test content")
    audit = {"status": "verified", "coverage": cov, "fact_file": str(fact),
             "fact_sha256": hashlib.sha256(fact.read_bytes()).hexdigest(),
             "quality": {"rows": 3629}}
    (silver / "silver_quality.json").write_text(json.dumps(audit), encoding="utf-8")
    adopted = try_adopt_existing(cov, cov["agency"], tmp_path)
    assert adopted is not None
    assert adopted["rows"] == 3629
    assert adopted["adopted"] is True
    fact.write_text("tampered")
    with pytest.raises(ApiError, match="changed"):
        try_adopt_existing(cov, cov["agency"], tmp_path)


def test_max_exports_is_one_and_checkpoint_saved(tmp_path, monkeypatch):
    import fsi.cli as cli
    import fsi.harvest as harvest
    exports = []

    class Fake:
        def post(self, path, body):
            return {"calculated_count": 3, "calculated_transaction_count": 3,
                    "maximum_limit": 500}

    def fake_submit(ns, client, estimated=None):
        exports.append((ns.start, ns.end))
        p = tmp_path / "data/raw/bulk" / f"job{len(exports)}" / "job.json"
        p.parent.mkdir(parents=True)
        p.write_text(json.dumps({"status": "verified"}))
        return p

    def fake_finish(client, args, part, root):
        part.update({"status": "verified", "rows": 3, "silver_file": "stub"})
        return True

    monkeypatch.setattr(cli, "submit_bulk_job", fake_submit)
    monkeypatch.setattr(harvest, "resume_or_transform", fake_finish)
    path = campaign_path(tmp_path, 2026, 2026, None)
    state = load_campaign(path, 2026, 2026, None)
    assert execute_campaign(state, path, args(tmp_path, agency=None), Fake()) == 0
    assert len(exports) == 1
    assert summary(state)["verified"] == 1
    assert summary(state)["planned"] == 11
    # Next invocation does not repeat first export.
    assert execute_campaign(state, path, args(tmp_path, agency=None), Fake()) == 0
    assert len(exports) == 2
    assert exports[0] != exports[1]


def test_earlier_pilot_can_be_adopted_despite_one_export_cap(tmp_path, monkeypatch):
    import fsi.cli as cli
    import fsi.harvest as harvest
    cov = {"start": "2026-09-01", "end": "2026-09-30",
           "agency": "Department of Transportation"}
    exports = []

    class Fake:
        def post(self, path, body):
            return {"calculated_count": 1, "calculated_transaction_count": 1,
                    "maximum_limit": 500}

    def fake_submit(ns, client, estimated=None):
        exports.append(ns.start)
        p = tmp_path / "data/raw/bulk/fakejob/job.json"
        p.parent.mkdir(parents=True)
        p.write_text(json.dumps({"status": "verified"}))
        return p

    def fake_finish(client, args, part, root):
        part.update({"status": "verified", "rows": 1})
        return True

    monkeypatch.setattr(cli, "submit_bulk_job", fake_submit)
    monkeypatch.setattr(harvest, "resume_or_transform", fake_finish)
    monkeypatch.setattr(harvest, "try_adopt_existing", lambda p, a, root: {
        "status": "verified", "rows": 3629, "job": "previous",
    } if p["start"] == "2026-09-01" else None)
    path = campaign_path(tmp_path, 2026, 2026, cov["agency"])
    state = load_campaign(path, 2026, 2026, cov["agency"])
    execute_campaign(state, path, args(tmp_path, adopt_existing=True), Fake())
    assert state["partitions"]["2026-09-01__2026-09-30"]["status"] == "verified"
    assert exports == ["2025-10-01"]


def test_adoption_rejects_corrupt_source_zip(tmp_path):
    agency = "Department of Transportation"
    cov = {"start": "2026-09-01", "end": "2026-09-30", "agency": agency}
    job_folder = tmp_path / "data/raw/bulk/candidate"
    job_folder.mkdir(parents=True)
    file = job_folder / "data.zip"
    file.write_bytes(b"old and verified")
    job = job_folder / "job.json"
    job.write_text(json.dumps({"status": "verified", "coverage": cov,
                               "file_name": "data.zip",
                               "download_sha256": hashlib.sha256(b"old and verified").hexdigest()}))
    file.write_bytes(b"tampered source")
    with pytest.raises(ApiError, match="source ZIP changed"):
        try_adopt_existing(cov, agency, tmp_path)
