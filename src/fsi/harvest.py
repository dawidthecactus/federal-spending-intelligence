"""Checkpointed multi-year contract harvesting.

Each partition is an inclusive date window. A too-large window is bisected;
no truncated data is silently accepted. A campaign remains local and resumable.
"""
from __future__ import annotations

import hashlib
import json
import os
import re
from argparse import Namespace
from datetime import date, timedelta
from pathlib import Path
from typing import Any

from .client import ApiError
from .core import build_filters, date_window, month_windows, utc_stamp
from .quality import quote_sql

CAMPAIGN_VERSION = 1
FINAL_STATES = {"verified", "empty"}


def partition_id(start: str, end: str) -> str:
    return f"{start}__{end}"


def campaign_id(first: int, last: int, agency: str | None) -> str:
    description = (agency or "all-agencies").lower()
    short = re.sub(r"[^a-z0-9]+", "-", description).strip("-")[:35]
    digest = hashlib.sha256(description.encode("utf-8")).hexdigest()[:8]
    return f"fy{first}-fy{last}_{short}-{digest}"


def create_campaign(first: int, last: int, agency: str | None) -> dict[str, Any]:
    if last < first or last - first > 10:
        raise ValueError("Choose an ascending range of up to 11 fiscal years")
    partitions = {}
    for fy in range(first, last + 1):
        for start, end in month_windows(fy):
            date_window(start, end)
            partitions[partition_id(start, end)] = {
                "start": start, "end": end, "status": "planned", "fiscal_year": fy
            }
    return {
        "schema_version": CAMPAIGN_VERSION,
        "created_at": utc_stamp(),
        "configuration": {"fy_start": first, "fy_end": last, "agency": agency},
        "partitions": partitions,
    }


def campaign_path(root: Path, first: int, last: int, agency: str | None) -> Path:
    name = campaign_id(first, last, agency)
    return root / "data" / "harvest" / name / "campaign.json"


def atomic_write(path: Path, data: dict[str, Any]) -> None:
    """Write checkpoint safely (replace on same filesystem)."""
    from .core import write_json

    path.parent.mkdir(parents=True, exist_ok=True)
    staging = path.with_suffix(".json.tmp")
    write_json(staging, data)
    os.replace(staging, path)


def load_campaign(path: Path, first: int, last: int, agency: str | None) -> dict[str, Any]:
    expected = {"fy_start": first, "fy_end": last, "agency": agency}
    if path.exists():
        content = json.loads(path.read_text(encoding="utf-8"))
        if content.get("schema_version") != CAMPAIGN_VERSION:
            raise ApiError(f"Unsupported campaign version in {path}")
        if content.get("configuration") != expected:
            raise ApiError("Campaign parameters do not match saved checkpoint")
        return content
    content = create_campaign(first, last, agency)
    atomic_write(path, content)
    return content


def split_window(part: dict[str, Any]) -> tuple[dict[str, Any], dict[str, Any]]:
    start, end = date_window(part["start"], part["end"])
    if start == end:
        raise ApiError(
            f"Cannot split overloaded single day {start}; select a specific agency "
            "or use an official bulk dataset."
        )
    midpoint = start + timedelta(days=(end - start).days // 2)
    halves = ((start, midpoint), (midpoint + timedelta(days=1), end))
    return tuple({
        "start": a.isoformat(), "end": b.isoformat(), "status": "planned",
        "fiscal_year": part["fiscal_year"],
    } for a, b in halves)  # type: ignore[return-value]


def leaf_parts(campaign: dict[str, Any]) -> list[tuple[str, dict[str, Any]]]:
    return sorted(
        ((key, part) for key, part in campaign["partitions"].items()
         if part["status"] != "split"),
        key=lambda pair: (pair[1]["start"], pair[1]["end"]),
    )


def count_preflight(client: Any, part: dict[str, Any], agency: str | None) -> dict:
    result = client.post("/api/v2/download/count/", {
        "filters": build_filters(part["start"], part["end"], agency),
        "spending_level": "transactions",
    })
    count = result.get("calculated_count")
    cap = result.get("maximum_limit")
    tx_count = result.get("calculated_transaction_count")
    if type(count) is not int or count < 0 or type(cap) is not int or cap <= 0:
        raise ApiError(f"Invalid download count response: {result}")
    if tx_count is not None and count != tx_count:
        raise ApiError(f"Count disagreement: calculated_count={count}, transactions={tx_count}")
    return result


def oversized(count_result: dict[str, Any]) -> bool:
    return bool(count_result.get("rows_gt_limit")
                or count_result.get("transaction_rows_gt_limit")
                or count_result["calculated_count"] > count_result["maximum_limit"])


def verified_silver(job: Path, root: Path) -> dict[str, Any] | None:
    """Validate cached Silver file and manifest digest; no network access."""
    path = root / "data/silver/jobs" / job.parent.name / "silver_quality.json"
    if not path.is_file():
        return None
    audit = json.loads(path.read_text(encoding="utf-8"))
    fact = Path(audit.get("fact_file", ""))
    if audit.get("status") != "verified" or not fact.is_file():
        return None
    with fact.open("rb") as handle:
        digest = hashlib.file_digest(handle, "sha256").hexdigest()
    if digest != audit.get("fact_sha256"):
        raise ApiError(f"Silver file changed since validation: {fact}")
    return audit


def try_adopt_existing(
    part: dict[str, Any], agency: str | None, root: Path,
) -> dict[str, Any] | None:
    """Look for an already-verified identical partition (e.g. Sep 2026 pilot)."""
    # Re-use only exact date windows, same awarding agency and verified facts.
    for job_file in sorted((root / "data/raw/bulk").glob("*/job.json"), reverse=True):
        try:
            job = json.loads(job_file.read_text(encoding="utf-8"))
        except (OSError, ValueError):
            continue
        if job.get("coverage") != {
            "start": part["start"], "end": part["end"], "agency": agency
        } or job.get("status") != "verified":
            continue
        # Do not adopt a stale/damaged source simply because its Silver exists.
        source_name, expected_sha = job.get("file_name"), job.get("download_sha256")
        if not source_name or not expected_sha:
            continue
        zip_path = job_file.parent / Path(source_name).name
        if not zip_path.is_file():
            continue
        with zip_path.open("rb") as source:
            if hashlib.file_digest(source, "sha256").hexdigest() != expected_sha:
                raise ApiError(f"Existing source ZIP changed: {zip_path}")
        silver = verified_silver(job_file, root)
        if silver is None:
            continue
        if silver.get("coverage") != job["coverage"]:
            continue
        # No need to re-download or retype a previously verified local job.
        return {
            "status": "verified", "job": str(job_file),
            "silver_file": silver["fact_file"],
            "rows": silver["quality"]["rows"], "adopted": True,
        }
    return None


def resume_or_transform(
    client: Any, args: Namespace, part: dict[str, Any], root: Path,
) -> bool:
    """Finish existing export job, then build Silver, recording only successful state."""
    from .cli import finalize_job
    from .silver import build_silver

    job = Path(part["job"])
    # Export jobs can complete while client is disconnected. Resume works via status URL.
    state = json.loads(job.read_text(encoding="utf-8")).get("status")
    if state != "verified":
        if finalize_job(client, job, args) != 0:
            return False
    audit = verified_silver(job, root)
    if audit is None:
        # Strict: refuse to overwrite partial / stale Silver automatically.
        target = root / "data/silver/jobs" / job.parent.name / "fact_contract_transactions.parquet"
        if target.exists():
            raise ApiError(f"Unverified Silver already exists: {target}. Review it manually.")
        audit = build_silver(job, root)
    part.update({"status": "verified", "silver_file": audit["fact_file"],
                 "rows": audit["quality"]["rows"]})
    print(f"PARTITION_VERIFIED {part['start']}..{part['end']} rows={part['rows']:,}")
    return True


def summary(campaign: dict[str, Any]) -> dict[str, Any]:
    parts = [p for _, p in leaf_parts(campaign)]
    counts = {key: sum(p["status"] == key for p in parts)
              for key in ("planned", "submitted", "verified", "empty")}
    counts["partitions"] = len(parts)
    counts["verified_rows"] = sum(p.get("rows", 0) for p in parts
                                  if p["status"] == "verified")
    counts["complete"] = all(p["status"] in FINAL_STATES for p in parts)
    return counts


def execute_campaign(
    campaign: dict[str, Any], checkpoint: Path, args: Namespace, client: Any,
) -> int:
    from .cli import submit_bulk_job

    root = Path(args.output).expanduser().resolve()
    if args.max_exports < 1:
        raise ValueError("--max-exports must be at least 1")
    agency = campaign["configuration"]["agency"]
    created = 0
    # Scan ALL windows first, including existing late months. This avoids paying
    # for a repeat export even when --max-exports=1 stops after the first new job.
    if args.adopt_existing:
        for _key, item in leaf_parts(campaign):
            if item["status"] != "planned":
                continue
            adopted = try_adopt_existing(item, agency, root)
            if adopted is not None:
                item.update(adopted)
                atomic_write(checkpoint, campaign)
                print(f"ADOPTED_EXISTING {item['start']}..{item['end']} rows={item['rows']:,}")
    # Iterate a snapshot; splits are processed on a subsequent invocation.
    for key, part in leaf_parts(campaign):
        if part["status"] in FINAL_STATES:
            continue
        if part["status"] == "planned":
            if created >= args.max_exports:
                break
            count_data = count_preflight(client, part, agency)
            part["expected"] = count_data["calculated_count"]
            if oversized(count_data):
                a, b = split_window(part)
                part["status"] = "split"
                for child in (a, b):
                    campaign["partitions"][partition_id(child["start"], child["end"])] = child
                atomic_write(checkpoint, campaign)
                print(f"SPLIT {part['start']}..{part['end']} (count={part['expected']:,})")
                continue
            if part["expected"] == 0:
                part["status"] = "empty"
                atomic_write(checkpoint, campaign)
                print(f"EMPTY_VERIFIED {part['start']}..{part['end']}")
                continue
            job_args = Namespace(**vars(args))
            job_args.start, job_args.end, job_args.agency = (
                part["start"], part["end"], agency
            )
            job = submit_bulk_job(job_args, client, estimated=count_data)
            part.update({"status": "submitted", "job": str(job)})
            atomic_write(checkpoint, campaign)
            created += 1
        if part["status"] == "submitted":
            ok = resume_or_transform(client, args, part, root)
            atomic_write(checkpoint, campaign)
            if not ok:
                print("Job is pending or needs review. Re-run same command to resume.")
                return 2
    s = summary(campaign)
    print(f"HARVEST_PROGRESS {s}")
    print(f"Checkpoint: {checkpoint}")
    return 0


def audit_campaign(campaign: dict[str, Any], root: Path, checkpoint: Path) -> int:
    """Global key uniqueness and signed obligation audit across all Silver partitions."""
    import duckdb

    leaves = [p for _, p in leaf_parts(campaign)]
    completed = [p for p in leaves if p["status"] == "verified"]
    incomplete = [p for p in leaves if p["status"] not in FINAL_STATES]
    if not completed:
        print("HARVEST_REPORT: no verified Silver partitions yet")
        return 2
    paths = []
    for part in completed:
        job = Path(part["job"])
        audit = verified_silver(job, root)
        if audit is None:
            raise ApiError(f"Missing/invalid Silver artifact for {part['start']}..{part['end']}")
        if audit["quality"]["rows"] != part["rows"]:
            raise ApiError(f"Saved row-count mismatch: {part['start']}")
        paths.append(Path(audit["fact_file"]))
    relation = "read_parquet([" + ",".join(quote_sql(p.as_posix()) for p in paths) + "])"
    with duckdb.connect() as conn:
        rows, unique, net, negative = conn.execute(f"""
            SELECT COUNT(*), COUNT(DISTINCT transaction_key),
                   SUM(obligation_change_usd),
                   COUNT(*) FILTER (WHERE obligation_change_usd < 0)
            FROM {relation}
        """).fetchone()
        by_year = conn.execute(f"""
            SELECT fiscal_year, COUNT(*) AS rows, SUM(obligation_change_usd) AS net
            FROM {relation}
            GROUP BY fiscal_year ORDER BY fiscal_year
        """).fetchall()
    errors = []
    expected = sum(p["rows"] for p in completed)
    if rows != expected:
        errors.append(f"rows={rows} expected={expected}")
    if unique != rows:
        errors.append(f"global_duplicate_transaction_keys={rows-unique}")
    report = {
        "status": "needs_review" if errors else ("complete" if not incomplete else "partial"),
        "source_campaign": str(checkpoint), "verified_partitions": len(completed),
        "empty_partitions": len(leaves) - len(completed) - len(incomplete),
        "incomplete_partitions": len(incomplete), "rows": rows,
        "unique_keys": unique, "negative_rows": negative,
        "net_obligations_usd": str(net), "errors": errors,
        "by_fiscal_year": [{"fiscal_year": y, "rows": r, "net_obligations_usd": str(n)}
                           for y, r, n in by_year],
        "note": "Preliminary totals if partial; not all federal obligations/outlays.",
    }
    atomic_write(checkpoint.parent / "harvest_quality.json", report)
    print("HARVEST_AUDIT " + json.dumps(report, ensure_ascii=False))
    return 3 if errors else (0 if not incomplete else 2)


def campaign_main(args: Namespace, client: Any = None) -> int:
    agency = None if getattr(args, "all_agencies", False) else args.agency
    root = Path(args.output).expanduser().resolve()
    checkpoint = campaign_path(root, args.fy_start, args.fy_end, agency)
    if args.command == "harvest-report":
        if not checkpoint.is_file():
            raise ApiError(f"Campaign not found: {checkpoint}")
        campaign = load_campaign(checkpoint, args.fy_start, args.fy_end, agency)
        return audit_campaign(campaign, root, checkpoint)
    campaign = load_campaign(checkpoint, args.fy_start, args.fy_end, agency)
    before = summary(campaign)
    print(f"HARVEST_PLAN FY{args.fy_start}..FY{args.fy_end} agency={agency or 'ALL'}")
    print(f"Partitions: {before['partitions']} | verified={before['verified']} "
          f"| empty={before['empty']} | remaining={before['planned'] + before['submitted']}")
    print(f"Checkpoint: {checkpoint}")
    if not args.execute:
        print("DRY_RUN_ONLY: no API calls; use --execute --max-exports 1 for first pilot")
        return 0
    if client is None:
        raise ValueError("Client required for live harvest")
    return execute_campaign(campaign, checkpoint, args, client)
