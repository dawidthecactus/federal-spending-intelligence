"""Command line entry point for contract transactions acquisition."""
from __future__ import annotations

import argparse
import hashlib
import json
import sys
import time
from datetime import date
from pathlib import Path
from typing import Any

from .client import ApiError, UsaSpendingClient, trusted_download_url
from .core import (
    SEARCH_FIELDS, build_filters, date_window, fiscal_year_range, month_windows,
    normalize_probe, utc_stamp, write_json,
)
from .quality import (
    download_zip, inspect_zip_and_convert, parquet_from_jsonl, verify_parquet_export,
)


def base_dir(args: argparse.Namespace) -> Path:
    return Path(args.output).expanduser().resolve()


def cmd_plan(args: argparse.Namespace) -> int:
    print(f"US Federal Fiscal Year {args.fy}")
    for start, end in month_windows(args.fy):
        print(f"  {start} .. {end}")
    return 0


def cmd_probe(args: argparse.Namespace, client: UsaSpendingClient) -> int:
    if not 1 <= args.limit <= 10000:
        raise ValueError("Probe limit must be 1..10000 (NOT a full-dataset export)")
    start, end = fiscal_year_range(args.fy)
    if date.fromisoformat(start) > date.today():
        raise ValueError("Selected FY has not started")
    if date.fromisoformat(end) > date.today():
        end = date.today().isoformat()
    filters = build_filters(start, end, args.agency)
    run = f"fy={args.fy}/{utc_stamp()}"
    raw = base_dir(args) / "data/raw/probe" / run
    staging = base_dir(args) / "data/staging/probe" / run
    raw.mkdir(parents=True, exist_ok=True)
    output = staging / "contract_transactions_probe.parquet"
    lines = raw / "normalized.jsonl"
    remaining = args.limit
    page = 1
    count = 0
    raw_count = 0
    first_errors = []

    with lines.open("w", encoding="utf-8") as jsonl:
        while remaining > 0:
            # The endpoint is search/paginated; deliberately never claim full coverage.
            payload = {
                "filters": filters, "fields": list(SEARCH_FIELDS),
                "sort": "Transaction Amount", "order": "desc",
                "page": page, "limit": min(100, remaining),
            }
            data = client.post("/api/v2/search/spending_by_transaction/", payload)
            write_json(raw / f"page_{page:04d}.json", data)
            results = data.get("results")
            if not isinstance(results, list):
                raise ApiError("Probe response has no 'results' list")
            raw_count += len(results)
            for row in results:
                item = normalize_probe(row)
                if item["transaction_amount_usd"] is None:
                    first_errors.append(f"Missing/invalid transaction amount at page {page}")
                jsonl.write(json.dumps(item, ensure_ascii=False, sort_keys=True) + "\n")
                count += 1
            remaining -= len(results)
            print(f"Probe page {page}: {len(results)} records (total={count})")
            if not results or not data.get("page_metadata", {}).get("hasNext"):
                break
            page += 1

    if not count:
        raise ApiError("API returned zero rows for the selected filters")
    if first_errors:
        raise ApiError(f"Probe validation failed: {first_errors[0]}")

    parquet_count = parquet_from_jsonl(lines, output)
    if count != parquet_count:
        raise ApiError(f"Probe row count mismatch: {count} vs {parquet_count}")
    manifest = {
        "workflow": "probe", "status": "sample_only_not_complete",
        "coverage": {"start": start, "end": end, "agency": args.agency},
        "sort": "Transaction Amount desc", "biased_sample": True,
        "rows_from_api": raw_count, "rows_in_parquet": parquet_count,
        "raw_json_folder": str(raw), "parquet_file": str(output),
        "note": "Do not sum probe rows to represent official spending.",
    }
    write_json(raw / "manifest.json", manifest)
    print(f"PROBE_OK rows={count}\nParquet: {output}\nManifest: {raw / 'manifest.json'}")
    return 0


def preflight(client: UsaSpendingClient, filters: dict[str, Any]) -> dict:
    data = client.post("/api/v2/download/count/", {
        "filters": filters, "spending_level": "transactions",
    })
    count = data.get("calculated_count")
    limit = data.get("maximum_limit")
    if not isinstance(count, int) or count < 0:
        raise ApiError(f"No reliable count in preflight response: {data}")
    if data.get("rows_gt_limit") or data.get("transaction_rows_gt_limit"):
        raise ApiError(
            f"API limit exceeded: {count} transactions, server max={limit}. "
            "Split by smaller date windows and/or awarding agency."
        )
    if isinstance(limit, int) and count > limit:
        raise ApiError(f"Expected {count} rows exceeds API max {limit}. Split filters.")
    if count == 0:
        raise ApiError("No transactions match the requested date window and agency")
    return data


def submit_bulk_job(
    args: argparse.Namespace, client: UsaSpendingClient,
    *, estimated: dict[str, Any] | None = None,
) -> Path:
    """Submit and persist one export. Caller may resume its job without resubmitting."""
    a, b = date_window(args.start, args.end)
    if (b - a).days > 31:
        raise ValueError("Bulk runs cover at most 32 days. Use monthly partitions.")
    filters = build_filters(args.start, args.end, args.agency)
    print(f"Preflight: {args.start}..{args.end}, awarding agency: {args.agency or 'ALL'}")
    estimated = estimated or preflight(client, filters)
    expected = estimated["calculated_count"]
    print(f"Expected transactions: {expected:,}; API limit: {estimated.get('maximum_limit')}")
    payload = {"filters": filters, "file_format": "csv"}
    response = client.post("/api/v2/download/transactions/", payload)
    for key in ("file_name", "status_url", "file_url"):
        if not response.get(key):
            raise ApiError(f"Download response missing {key}: {response}")
    status_url = trusted_download_url(response["status_url"])
    file_url = trusted_download_url(response["file_url"])
    job = base_dir(args) / "data/raw/bulk" / (
        f"{args.start}_{args.end}_{utc_stamp()}"
    )
    job.mkdir(parents=True, exist_ok=False)
    manifest = {
        "workflow": "bulk_transactions", "status": "submitted",
        "coverage": {"start": args.start, "end": args.end, "agency": args.agency},
        "expected_transaction_count": expected,
        "preflight": estimated, "payload": payload,
        "file_name": response["file_name"],
        "status_url": status_url, "file_url": file_url,
        "download_response": response,
    }
    manifest_path = job / "job.json"
    write_json(manifest_path, manifest)
    print(f"Submitted bulk job. Saved: {manifest_path}")
    return manifest_path


def cmd_bulk(args: argparse.Namespace, client: UsaSpendingClient) -> int:
    job_file = submit_bulk_job(args, client)
    return finalize_job(client, job_file, args)


def finalize_job(client: UsaSpendingClient, job_file: Path, args) -> int:
    manifest = json.loads(job_file.read_text(encoding="utf-8"))
    started = time.monotonic()
    while True:
        status = client.get(manifest["status_url"])
        state = str(status.get("status", "")).lower()
        if state == "failed":
            manifest["status"] = "failed"
            manifest["last_api_status"] = status
            write_json(job_file, manifest)
            raise ApiError(f"Bulk file generation failed: {status.get('message')}")
        if state == "finished":
            break
        if state not in {"running", "ready"}:
            raise ApiError(f"Unexpected bulk job state: {status}")
        if time.monotonic() - started >= args.max_wait:
            manifest["status"] = "pending"
            manifest["last_api_status"] = status
            write_json(job_file, manifest)
            print("Export still generating. Resume with:")
            print(f'  fsi resume --job "{job_file}"')
            return 2
        time.sleep(args.poll)

    url = trusted_download_url(status.get("file_url") or manifest["file_url"])
    archive = job_file.parent / Path(manifest["file_name"]).name
    sha256 = download_zip(client, url, archive, max_mb=args.max_mb)
    staging = base_dir(args) / "data/staging/bulk" / job_file.parent.name
    inspect_zip_and_convert(archive, staging)
    manifest.update({
        "download_sha256": sha256,
        "last_api_status": status,
        "parquet_folder": str(staging),
    })
    write_json(job_file, manifest)
    return verify_job(job_file)


def verify_job(job_file: Path) -> int:
    """Reconcile locally persisted source ZIP + Parquet; never repeat an API call."""
    manifest = json.loads(job_file.read_text(encoding="utf-8"))
    if manifest.get("workflow") != "bulk_transactions":
        raise ApiError("Only bulk transaction jobs support 'verify'")
    expected = manifest.get("expected_transaction_count")
    if not isinstance(expected, int):
        raise ApiError("Job manifest lacks a valid expected transaction count")
    staging_location = manifest.get("parquet_folder")
    if not staging_location:
        raise ApiError("Job lacks parquet_folder. Use 'resume' for unfinished downloads.")
    staging = Path(staging_location)
    archive = job_file.parent / Path(manifest["file_name"]).name
    if not archive.exists():
        raise ApiError(f"Original download ZIP not found: {archive}")
    with archive.open("rb") as source:
        sha = hashlib.file_digest(source, "sha256").hexdigest()
    if manifest.get("download_sha256") != sha:
        raise ApiError("Raw source ZIP SHA-256 differs from download manifest")
    report = verify_parquet_export(
        staging, expected, manifest["coverage"]["start"], manifest["coverage"]["end"]
    )
    manifest["status"] = report["status"]
    manifest["qa"] = {
        "preflight_prime_count": expected,
        "prime_transaction_rows": report["prime_transaction_rows"],
        "subaward_rows_excluded": report["subaward_rows"],
        "all_export_rows": report["all_export_rows"],
        "prime_counts_match": report["prime_count_matches"],
        "transaction_quality": report["transaction_quality"],
        "quality_problems": report["quality_problems"],
    }
    write_json(job_file, manifest)
    print(
        f"BULK_{manifest['status'].upper()} "
        f"prime={report['prime_transaction_rows']:,} / expected={expected:,}; "
        f"subawards={report['subaward_rows']:,} (separate)"
    )
    if report["transaction_quality"]:
        print("Transaction checks:", json.dumps(report["transaction_quality"]))
    print(f"Parquet: {staging}\nManifest: {job_file}")
    if report["status"] != "verified":
        print("WARNING: Quality Gate failed; do NOT publish totals.")
        return 3
    return 0


def parser() -> argparse.ArgumentParser:
    main = argparse.ArgumentParser(
        prog="fsi", description="Federal Spending Intelligence extraction"
    )
    main.add_argument("--output", default=".", help="Project directory (default: cwd)")
    sub = main.add_subparsers(dest="command", required=True)

    plan = sub.add_parser("plan", help="Print fiscal-year monthly partitions")
    plan.add_argument("--fy", type=int, default=2026)

    probe = sub.add_parser("probe", help="CAPPED search sample, never complete dataset")
    probe.add_argument("--fy", type=int, default=2026)
    probe.add_argument("--limit", type=int, default=300)
    probe.add_argument("--agency", default=None, help="Exact official awarding-agency name")

    bulk = sub.add_parser("bulk", help="Full API export for a narrow date window")
    bulk.add_argument("--start", required=True, help="YYYY-MM-DD")
    bulk.add_argument("--end", required=True, help="YYYY-MM-DD")
    bulk.add_argument("--agency", default="Department of Transportation",
                      help="Exact awarding-agency name (default: Department of Transportation)")
    bulk.add_argument("--all-agencies", action="store_true",
                      help="Remove agency filter (preflight enforces API row limit)")

    resume = sub.add_parser("resume", help="Resume a previously submitted bulk download")
    resume.add_argument("--job", type=Path, required=True)

    verify = sub.add_parser("verify", help="Reconcile existing local bulk files without API")
    verify.add_argument("--job", type=Path, required=True)

    silver = sub.add_parser(
        "silver", help="Build typed Silver contract transaction fact from verified bulk"
    )
    silver.add_argument("--job", type=Path, required=True)
    silver.add_argument("--overwrite", action="store_true")

    harvest = sub.add_parser(
        "harvest", help="Plan or incrementally download fiscal-year month partitions"
    )
    harvest.add_argument("--fy-start", type=int, default=2023)
    harvest.add_argument("--fy-end", type=int, default=2026)
    harvest.add_argument("--agency", default="Department of Transportation")
    harvest.add_argument("--all-agencies", action="store_true")
    harvest.add_argument("--execute", action="store_true", help="Opt in to network calls")
    harvest.add_argument("--max-exports", type=int, default=1,
                         help="Cap new export jobs per run; default one")
    harvest.add_argument("--adopt-existing", action="store_true",
                         help="Re-use already validated monthly bulk+Silver jobs")
    harvest.add_argument("--max-wait", type=int, default=900)
    harvest.add_argument("--poll", type=int, default=10)
    harvest.add_argument("--max-mb", type=int, default=1024)

    hreport = sub.add_parser("harvest-report", help="Audit saved partition manifest")
    hreport.add_argument("--fy-start", type=int, default=2023)
    hreport.add_argument("--fy-end", type=int, default=2026)
    hreport.add_argument("--agency", default="Department of Transportation")
    hreport.add_argument("--all-agencies", action="store_true")

    gold = sub.add_parser(
        "gold", help="Build audited Power BI star-schema Parquet from a harvest campaign"
    )
    gold.add_argument("--fy-start", type=int, default=2023)
    gold.add_argument("--fy-end", type=int, default=2026)
    gold.add_argument("--agency", default="Department of Transportation")
    gold.add_argument("--all-agencies", action="store_true")
    gold.add_argument("--allow-partial", action="store_true")
    gold.add_argument("--overwrite", action="store_true")

    gold_report = sub.add_parser("gold-report", help="Verify Gold Parquet file checksums")
    gold_report.add_argument("--fy-start", type=int, default=2023)
    gold_report.add_argument("--fy-end", type=int, default=2026)
    gold_report.add_argument("--agency", default="Department of Transportation")
    gold_report.add_argument("--all-agencies", action="store_true")

    for item in (bulk, resume):
        item.add_argument("--max-wait", type=int, default=900)
        item.add_argument("--poll", type=int, default=10)
        item.add_argument("--max-mb", type=int, default=1024)
    return main


def main(argv: list[str] | None = None) -> int:
    args = parser().parse_args(argv)
    if getattr(args, "all_agencies", False):
        args.agency = None
    if args.command in {"gold", "gold-report"}:
        try:
            from .gold import build_gold, inspect_gold
            if args.command == "gold":
                report = build_gold(
                    base_dir(args), args.fy_start, args.fy_end, args.agency,
                    allow_partial=args.allow_partial, overwrite=args.overwrite,
                )
            else:
                report = inspect_gold(base_dir(args), args.fy_start, args.fy_end, args.agency)
            q = report["quality"]
            print(
                f"GOLD_{report['status'].upper()} rows={q['rows']:,} "
                f"net_usd={q['net_obligations_usd']} "
                f"coverage={q['coverage_status']}"
            )
            from .harvest import campaign_path
            campaign = campaign_path(
                base_dir(args), args.fy_start, args.fy_end, args.agency
            )
            print("Quality report:", base_dir(args) / "data/marts/gold"
                  / campaign.parent.name / "gold_quality.json")
            return 0
        except (ApiError, ValueError, OSError, json.JSONDecodeError) as exc:
            print(f"ERROR: {exc}", file=sys.stderr)
            return 1
    if args.command == "plan":
        return cmd_plan(args)
    if args.command in {"harvest", "harvest-report"}:
        try:
            from .harvest import campaign_main
            return campaign_main(args, client=UsaSpendingClient() if args.command == "harvest"
                                 and args.execute else None)
        except (ApiError, ValueError, OSError, json.JSONDecodeError) as exc:
            print(f"ERROR: {exc}", file=sys.stderr)
            return 1
    if args.command == "silver":
        try:
            from .silver import build_silver

            report = build_silver(args.job, base_dir(args), overwrite=args.overwrite)
            q = report["quality"]
            print(
                f"SILVER_VERIFIED rows={q['rows']:,}; negative={q['negative_rows']:,}; "
                f"net_usd={q['net_obligations_usd']}"
            )
            print(f"Parquet: {report['fact_file']}")
            return 0
        except (ApiError, ValueError, OSError, json.JSONDecodeError) as exc:
            print(f"ERROR: {exc}", file=sys.stderr)
            return 1
    if args.command == "verify":
        try:
            return verify_job(args.job.resolve())
        except (ApiError, ValueError, OSError, json.JSONDecodeError) as exc:
            print(f"ERROR: {exc}", file=sys.stderr)
            return 1
    client = UsaSpendingClient()
    try:
        if args.command == "probe":
            return cmd_probe(args, client)
        if args.command == "bulk":
            return cmd_bulk(args, client)
        if args.command == "resume":
            return finalize_job(client, args.job.resolve(), args)
    except (ApiError, ValueError, OSError, json.JSONDecodeError) as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 1
    raise AssertionError(args.command)


if __name__ == "__main__":
    sys.exit(main())
