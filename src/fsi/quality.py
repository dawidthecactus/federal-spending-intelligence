"""Local data quality checks and DuckDB Parquet conversion."""
from __future__ import annotations

import hashlib
import json
import os
import zipfile
from pathlib import Path
from datetime import date
from typing import Any

from .client import ApiError
from .core import write_json


def quote_sql(text: str) -> str:
    return "'" + text.replace("'", "''") + "'"


def parquet_from_jsonl(source: Path, destination: Path) -> int:
    import duckdb

    destination.parent.mkdir(parents=True, exist_ok=True)
    with duckdb.connect() as conn:
        conn.execute(f"""
            COPY (
                SELECT
                    * EXCLUDE (transaction_amount_usd, action_date),
                    TRY_CAST(transaction_amount_usd AS DECIMAL(24, 2))
                        AS transaction_amount_usd,
                    TRY_CAST(action_date AS DATE) AS action_date
                FROM read_json_auto({quote_sql(source.as_posix())},
                                    format='newline_delimited', sample_size=-1)
            ) TO {quote_sql(destination.as_posix())} (FORMAT PARQUET, COMPRESSION ZSTD)
        """)
        return conn.execute(
            f"SELECT COUNT(*) FROM read_parquet({quote_sql(destination.as_posix())})"
        ).fetchone()[0]


def inspect_zip_and_convert(archive: Path, output_dir: Path) -> dict[str, Any]:
    """Never trust ZIP member paths; extract CSV streams by generated basename only."""
    output_dir.mkdir(parents=True, exist_ok=True)
    import duckdb
    results: list[dict[str, Any]] = []
    with zipfile.ZipFile(archive) as zipped:
        bad = zipped.testzip()
        if bad:
            raise ApiError(f"ZIP CRC failure in {bad}")
        csv_members = [m for m in zipped.infolist() if not m.is_dir()
                       and m.filename.lower().endswith(".csv")]
        if not csv_members:
            raise ApiError("Official archive contains no CSV files")

        with duckdb.connect() as conn:
            for index, member in enumerate(csv_members):
                name = Path(member.filename).name
                target = output_dir / f"{index:02d}_{name}"
                with zipped.open(member) as src, target.open("wb") as dest:
                    while buf := src.read(4 * 1024 * 1024):
                        dest.write(buf)
                parquet = target.with_suffix(".parquet")
                path = quote_sql(target.as_posix())
                pq_path = quote_sql(parquet.as_posix())
                # All source columns stay strings in Bronze. Financial types in Silver.
                conn.execute(f"""
                    COPY (
                        SELECT * FROM read_csv({path}, header=true, all_varchar=true,
                                               sample_size=-1)
                    ) TO {pq_path} (FORMAT PARQUET, COMPRESSION ZSTD)
                """)
                cols = [
                    r[0] for r in conn.execute(
                        f"DESCRIBE SELECT * FROM read_parquet({pq_path})"
                    ).fetchall()
                ]
                record_count = conn.execute(
                    f"SELECT COUNT(*) FROM read_parquet({pq_path})"
                ).fetchone()[0]
                results.append({
                    "csv": target.name, "parquet": parquet.name,
                    "rows": record_count, "columns": cols,
                })
                # We retain immutable compressed source ZIP, not an additional CSV copy.
                target.unlink()
    result = {"source_zip": archive.name, "files": results,
              "rows_in_csv_files": sum(item["rows"] for item in results)}
    write_json(output_dir / "quality_summary.json", result)
    return result



def classify_export(columns: list[str]) -> str:
    """Use actual schema, not filenames or row counts, to identify each export."""
    cols = set(columns)
    if "contract_transaction_unique_key" in cols and "federal_action_obligation" in cols:
        return "prime_transactions"
    if {"subaward_sam_report_id", "subaward_number", "subaward_amount"} <= cols:
        return "subawards"
    return "unclassified"


def summarize_export_files(files: list[dict[str, Any]], expected: int) -> dict[str, Any]:
    """Preflight /download/count counts prime transactions, not subawards."""
    counts = {"prime_transactions": 0, "subawards": 0, "unclassified": 0}
    enriched = []
    for item in files:
        kind = classify_export(item["columns"])
        counts[kind] += item["rows"]
        enriched.append({**item, "dataset": kind})
    prime_present = any(item["dataset"] == "prime_transactions" for item in enriched)
    unknown_present = any(item["dataset"] == "unclassified" for item in enriched)
    matched = (prime_present and not unknown_present
               and counts["prime_transactions"] == expected)
    return {
        "files": enriched,
        "prime_transaction_rows": counts["prime_transactions"],
        "subaward_rows": counts["subawards"],
        "unclassified_rows": counts["unclassified"],
        "all_export_rows": sum(counts.values()),
        "expected_prime_transactions": expected,
        "prime_count_matches": matched,
        "note": "Subawards are separate facts; never sum them into prime obligations.",
    }


def verify_parquet_export(
    staging: Path, expected: int, start: str, end: str
) -> dict[str, Any]:
    """Recheck EXISTING local Parquet: no network, no re-download."""
    import duckdb

    paths = sorted(staging.glob("*.parquet"))
    if not paths:
        raise ApiError(f"No Parquet files to verify in {staging}")
    files = []
    with duckdb.connect() as conn:
        for path in paths:
            file_sql = quote_sql(path.as_posix())
            columns = [r[0] for r in conn.execute(
                f"DESCRIBE SELECT * FROM read_parquet({file_sql})"
            ).fetchall()]
            count = conn.execute(
                f"SELECT COUNT(*) FROM read_parquet({file_sql})"
            ).fetchone()[0]
            files.append({"parquet": path.name, "columns": columns, "rows": count})
        report = summarize_export_files(files, expected)
        prime_files = [staging / f["parquet"] for f in report["files"]
                       if f["dataset"] == "prime_transactions"]
        problems: list[str] = []
        transaction_checks = None
        if not prime_files:
            problems.append("Missing prime transaction export")
        else:
            # One logical relation across all CSV shards. Award IDs are not unique
            # transactions; the contract transaction key is the correct identifier.
            paths_sql = "[" + ",".join(quote_sql(p.as_posix()) for p in prime_files) + "]"
            lo, hi = quote_sql(start), quote_sql(end)
            transaction_checks = conn.execute(f"""
                SELECT COUNT(*) AS total_rows,
                    COUNT(*) FILTER (WHERE contract_transaction_unique_key IS NULL
                        OR TRIM(contract_transaction_unique_key) = '') AS missing_keys,
                    COUNT(*) - COUNT(DISTINCT NULLIF(TRIM(contract_transaction_unique_key), ''))
                        AS duplicate_or_missing_keys,
                    COUNT(*) FILTER (WHERE TRY_CAST(action_date AS DATE) IS NULL)
                        AS invalid_dates,
                    COUNT(*) FILTER (WHERE TRY_CAST(action_date AS DATE) NOT BETWEEN
                        CAST({lo} AS DATE) AND CAST({hi} AS DATE)) AS dates_outside_window,
                    COUNT(*) FILTER (
                        WHERE TRY_CAST(federal_action_obligation AS DECIMAL(24,2)) IS NULL
                    ) AS invalid_amounts,
                    COUNT(*) FILTER (
                        WHERE TRY_CAST(federal_action_obligation AS DECIMAL(24,2)) < 0
                    ) AS negative_obligation_rows
                FROM read_parquet({paths_sql}, union_by_name=true)
            """).fetchone()
            keys = ["rows_checked", "missing_keys", "duplicate_or_missing_keys",
                    "invalid_dates", "dates_outside_window", "invalid_amounts",
                    "negative_obligation_rows"]
            transaction_checks = dict(zip(keys, transaction_checks))
            for field in ("missing_keys", "duplicate_or_missing_keys", "invalid_dates",
                          "dates_outside_window", "invalid_amounts"):
                if transaction_checks[field] > 0:
                    problems.append(f"{field}={transaction_checks[field]}")
        if not report["prime_count_matches"]:
            problems.append("Prime transaction count differs from API preflight or unknown files")
        report["transaction_quality"] = transaction_checks
        report["quality_problems"] = problems
        report["status"] = "verified" if not problems else "needs_review"
        write_json(staging / "quality_summary.json", report)
        return report

def download_zip(client, file_url: str, destination: Path,
                 *, max_mb: int = 1024) -> str:
    destination.parent.mkdir(parents=True, exist_ok=True)
    partial = destination.with_suffix(destination.suffix + ".part")
    digest = hashlib.sha256()
    total = 0
    response = client.stream(file_url)
    try:
        with partial.open("wb") as f:
            for chunk in response.iter_content(chunk_size=1024 * 1024):
                if not chunk:
                    continue
                total += len(chunk)
                if total > max_mb * 1024 * 1024:
                    raise ApiError(f"File exceeds --max-mb {max_mb} MB")
                digest.update(chunk)
                f.write(chunk)
        os.replace(partial, destination)
    finally:
        response.close()
        partial.unlink(missing_ok=True)
    if not zipfile.is_zipfile(destination):
        raise ApiError("Downloaded content is not a ZIP archive")
    return digest.hexdigest()
