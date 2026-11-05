"""Gold star schema for verified USAspending *contract transaction* campaigns.

Never mixes subawards, award-level cumulative values or separate campaign scopes.
"""
from __future__ import annotations

import hashlib
import json
import os
import shutil
import tempfile
from datetime import date, timedelta
from decimal import Decimal
from pathlib import Path
from typing import Any

from .client import ApiError
from .core import fiscal_year_range, utc_stamp, write_json
from .harvest import (
    FINAL_STATES, audit_campaign, campaign_path, leaf_parts, load_campaign, verified_silver,
)
from .quality import quote_sql

UNKNOWN = "__MISSING__"
GOLD_SCHEMA_VERSION = "0.4.0"


def validate_coverage(campaign: dict[str, Any]) -> dict[str, Any]:
    """Prove leaf partitions exactly tile the requested fiscal-year date interval."""
    config = campaign["configuration"]
    start = date.fromisoformat(fiscal_year_range(config["fy_start"])[0])
    end = date.fromisoformat(fiscal_year_range(config["fy_end"])[1])
    parts = [p for _, p in leaf_parts(campaign)]
    if not parts:
        raise ApiError("No leaf partitions")
    expected = start
    for part in parts:
        first, last = date.fromisoformat(part["start"]), date.fromisoformat(part["end"])
        if first != expected or last < first:
            raise ApiError(f"Gap or overlap in harvest partitions near {expected}: {part}")
        fy = first.year + (first.month >= 10)
        if part["fiscal_year"] != fy:
            raise ApiError(f"Partition fiscal year mismatch: {part}")
        expected = last + timedelta(days=1)
    if expected != end + timedelta(days=1):
        raise ApiError(f"Campaign ends at {expected - timedelta(days=1)}, expected {end}")
    incomplete = [p for p in parts if p["status"] not in FINAL_STATES]
    return {
        "start": start.isoformat(), "end": end.isoformat(),
        "leaf_partitions": len(parts), "incomplete_partitions": len(incomplete),
        "empty_partitions": sum(p["status"] == "empty" for p in parts),
    }


def source_files(campaign: dict[str, Any], root: Path) -> tuple[list[Path], list[dict]]:
    """Do not trust manifest paths without verifying artifact hashes and boundaries."""
    paths = []
    sources = []
    for _, part in leaf_parts(campaign):
        if part["status"] != "verified":
            continue
        job = Path(part["job"])
        audit = verified_silver(job, root)
        if audit is None:
            raise ApiError(f"Silver is missing/unverified: {job}")
        if audit.get("coverage", {}).get("start") != part["start"] or (
            audit.get("coverage", {}).get("end") != part["end"]
        ):
            raise ApiError(f"Silver scope disagrees with campaign: {job}")
        if audit.get("coverage", {}).get("agency") != campaign["configuration"]["agency"]:
            raise ApiError(f"Silver agency disagrees with campaign: {job}")
        if audit["quality"]["rows"] != part["rows"]:
            raise ApiError(f"Silver row count disagrees with campaign: {job}")
        path = Path(audit["fact_file"])
        paths.append(path)
        sources.append({
            "start": part["start"], "end": part["end"], "rows": part["rows"],
            "silver_sha256": audit["fact_sha256"], "silver_file": str(path),
        })
    if not paths:
        raise ApiError("No verified Silver files to promote")
    return paths, sources


def parquet_relation(paths: list[Path]) -> str:
    return "read_parquet([" + ",".join(quote_sql(p.as_posix()) for p in paths) + "])"


def clean_key(column: str) -> str:
    return f"COALESCE(NULLIF(TRIM({column}), ''), '{UNKNOWN}')"


def gold_queries(relation: str, fy_start: int, fy_end: int) -> dict[str, str]:
    """Pure SQL transformation definitions, separately testable without live data."""
    start = fiscal_year_range(fy_start)[0]
    end = fiscal_year_range(fy_end)[1]
    agency = clean_key("awarding_agency_code")
    funding = clean_key("funding_agency_code")
    recipient = clean_key("recipient_uei")
    naics = clean_key("naics_code")
    psc = clean_key("psc_code")

    return {
        "fact_contract_transactions": f"""
            SELECT transaction_key, award_key, award_id_piid, modification_number,
                transaction_number, action_date, fiscal_year, calendar_month,
                {agency} AS awarding_agency_key,
                {funding} AS funding_agency_key,
                {recipient} AS recipient_key,
                {naics} AS naics_key,
                {psc} AS psc_key,
                obligation_change_usd, is_deobligation, is_positive_obligation,
                action_type_code, award_type_code, performance_state_code,
                performance_country_code, extent_competed_code, set_aside_code
            FROM {relation}
        """,
        "dim_date": f"""
            SELECT CAST(day AS DATE) AS date,
                EXTRACT(YEAR FROM day)::INTEGER AS calendar_year,
                EXTRACT(MONTH FROM day)::INTEGER AS calendar_month,
                date_trunc('month', day)::DATE AS calendar_month_start,
                EXTRACT(YEAR FROM day)::INTEGER
                  + CASE WHEN EXTRACT(MONTH FROM day) >= 10 THEN 1 ELSE 0 END
                  AS fiscal_year,
                (((EXTRACT(MONTH FROM day)::INTEGER + 2) % 12) + 1)
                  AS fiscal_month_number,
                (FLOOR((((EXTRACT(MONTH FROM day)::INTEGER + 2) % 12)) / 3) + 1)::INTEGER
                  AS fiscal_quarter,
                STRFTIME(day, '%Y-%m') AS calendar_year_month,
                STRFTIME(day, '%b') AS calendar_month_label
            FROM generate_series(DATE '{start}', DATE '{end}', INTERVAL 1 DAY) AS t(day)
        """,
        "dim_agency": f"""
            WITH agency_labels AS (
                SELECT {agency} AS agency_key,
                    NULLIF(TRIM(awarding_agency_name), '') AS agency_name
                FROM {relation}
                UNION ALL
                SELECT {funding} AS agency_key,
                    NULLIF(TRIM(funding_agency_name), '') AS agency_name
                FROM {relation}
            )
            SELECT agency_key,
                CASE WHEN agency_key = '{UNKNOWN}' THEN '[Unknown agency]'
                     ELSE COALESCE(MAX(agency_name), '[Name unavailable]')
                END AS agency_name,
                COUNT(DISTINCT agency_name) AS observed_name_variants
            FROM agency_labels GROUP BY agency_key
        """,
        "dim_recipient": f"""
            SELECT {recipient} AS recipient_key,
                CASE WHEN recipient_key = '{UNKNOWN}' THEN '[Unknown recipient UEI]'
                     ELSE COALESCE(MAX(NULLIF(TRIM(recipient_name), '')),
                                   '[Name unavailable]') END AS recipient_name,
                COUNT(DISTINCT NULLIF(TRIM(recipient_name), '')) AS observed_name_variants
            FROM {relation}
            GROUP BY recipient_key
        """,
        "dim_naics": f"""
            SELECT {naics} AS naics_key,
                CASE WHEN naics_key = '{UNKNOWN}' THEN '[NAICS unavailable]'
                     ELSE COALESCE(MAX(NULLIF(TRIM(naics_description), '')),
                                   '[Description unavailable]') END AS naics_description,
                COUNT(DISTINCT NULLIF(TRIM(naics_description), '')) AS observed_name_variants
            FROM {relation}
            GROUP BY naics_key
        """,
        "dim_psc": f"""
            SELECT {psc} AS psc_key,
                CASE WHEN psc_key = '{UNKNOWN}' THEN '[PSC unavailable]'
                     ELSE COALESCE(MAX(NULLIF(TRIM(psc_description), '')),
                                   '[Description unavailable]') END AS psc_description,
                COUNT(DISTINCT NULLIF(TRIM(psc_description), '')) AS observed_name_variants
            FROM {relation}
            GROUP BY psc_key
        """,
    }


def _checksum(path: Path) -> str:
    with path.open("rb") as stream:
        return hashlib.file_digest(stream, "sha256").hexdigest()


def _sql_file(conn: Any, query: str, path: Path) -> None:
    conn.execute(
        f"COPY ({query}) TO {quote_sql(path.as_posix())} "
        "(FORMAT PARQUET, COMPRESSION ZSTD)"
    )


def _audit_gold(conn: Any, temp: Path, source_count: int, source_net: str,
                complete: bool) -> dict[str, Any]:
    f = f"read_parquet({quote_sql((temp / 'fact_contract_transactions.parquet').as_posix())})"
    row = conn.execute(f"""
        SELECT COUNT(*), COUNT(DISTINCT transaction_key),
            COUNT(*) FILTER (WHERE transaction_key IS NULL),
            COUNT(*) FILTER (WHERE obligation_change_usd IS NULL OR action_date IS NULL),
            SUM(obligation_change_usd),
            COUNT(*) FILTER (WHERE obligation_change_usd < 0),
            COUNT(*) FILTER (WHERE obligation_change_usd = 0),
            COUNT(*) FILTER (WHERE recipient_key = '{UNKNOWN}'),
            MIN(action_date), MAX(action_date)
        FROM {f}
    """).fetchone()
    (rows, unique, missing_keys, invalid_fields, net, negative, zero,
     missing_recipient, minimum, maximum) = row
    sizes = {}
    issues = []
    for table in ("dim_date", "dim_agency", "dim_recipient", "dim_naics", "dim_psc"):
        dim = f"read_parquet({quote_sql((temp / (table + '.parquet')).as_posix())})"
        key_col = "date" if table == "dim_date" else table.removeprefix("dim_") + "_key"
        dim_count, unique_count = conn.execute(
            f"SELECT COUNT(*), COUNT(DISTINCT {key_col}) FROM {dim}"
        ).fetchone()
        sizes[table] = dim_count
        if dim_count != unique_count:
            issues.append(f"{table}: non-unique dimension key")
        # Assert every fact foreign key can find its dimension row.
        if table != "dim_date":
            fact_column = "awarding_agency_key" if table == "dim_agency" else key_col
            for column in ([fact_column, "funding_agency_key"] if table == "dim_agency"
                           else [fact_column]):
                missing = conn.execute(f"""
                    SELECT COUNT(*) FROM {f} x LEFT JOIN {dim} d
                      ON x.{column} = d.{key_col}
                    WHERE d.{key_col} IS NULL
                """).fetchone()[0]
                if missing:
                    issues.append(f"{column}: {missing} unmatched foreign keys")
        else:
            unmatched = conn.execute(f"""
                SELECT COUNT(*) FROM {f} x LEFT JOIN {dim} d ON x.action_date = d.date
                WHERE d.date IS NULL
            """).fetchone()[0]
            if unmatched:
                issues.append(f"action_date: {unmatched} unmatched dates")
    if rows != source_count:
        issues.append(f"row count: Gold {rows} versus Silver {source_count}")
    if unique != rows or missing_keys:
        issues.append("transaction key not unique/non-null")
    if invalid_fields:
        issues.append("invalid amount/date in Gold")
    # DuckDB can display numerically identical DECIMAL values at differing scales.
    if Decimal(str(net)) != Decimal(source_net):
        issues.append(f"signed obligation total mismatch: {net} versus {source_net}")
    by_year = conn.execute(f"""
        SELECT fiscal_year, COUNT(*), SUM(obligation_change_usd)
        FROM {f} GROUP BY fiscal_year ORDER BY fiscal_year
    """).fetchall()
    return {
        "status": "verified" if not issues else "needs_review",
        "coverage_status": "complete" if complete else "partial",
        "rows": rows, "unique_transaction_keys": unique,
        "missing_transaction_keys": missing_keys, "invalid_dates_or_amounts": invalid_fields,
        "negative_rows": negative, "zero_rows": zero,
        "recipient_uei_missing_rows": missing_recipient,
        "net_obligations_usd": str(net),
        "min_action_date": str(minimum), "max_action_date": str(maximum),
        "dimensions": sizes,
        "by_fiscal_year": [
            {"fiscal_year": y, "rows": r, "net_obligations_usd": str(s)}
            for y, r, s in by_year
        ],
        "errors": issues,
    }


def build_gold(root: Path, first: int, last: int, agency: str | None,
               *, allow_partial: bool = False, overwrite: bool = False) -> dict[str, Any]:
    """Build a local 1:* star schema after re-auditing saved campaign and files.

    No API calls; output is staged and published only after all Gold checks pass.
    """
    import duckdb

    root = root.expanduser().resolve()
    checkpoint = campaign_path(root, first, last, agency)
    if not checkpoint.is_file():
        raise ApiError(f"Harvest campaign not found: {checkpoint}")
    campaign = load_campaign(checkpoint, first, last, agency)
    coverage = validate_coverage(campaign)
    if coverage["incomplete_partitions"] and not allow_partial:
        raise ApiError("Campaign is incomplete. Run 'fsi harvest-report' and finish "
                       "harvest, or pass --allow-partial for clearly labelled previews.")
    audit_result = audit_campaign(campaign, root, checkpoint)
    if audit_result == 3:
        raise ApiError("Harvest cross-partition audit failed")
    if audit_result == 2 and not allow_partial:
        raise ApiError("Harvest audit not complete")
    if audit_result not in {0, 2}:
        raise ApiError("Harvest audit has no verified data")
    harvested = json.loads((checkpoint.parent / "harvest_quality.json").read_text())
    if harvested.get("errors") or harvested.get("status") == "needs_review":
        raise ApiError("Harvest has unresolved data quality errors")

    paths, sources = source_files(campaign, root)
    target = root / "data" / "marts" / "gold" / checkpoint.parent.name
    if target.exists() and not overwrite:
        raise ApiError(f"Gold output already exists: {target}. Use --overwrite intentionally.")
    target.parent.mkdir(parents=True, exist_ok=True)
    work = Path(tempfile.mkdtemp(prefix=".gold-build-", dir=target.parent))
    succeeded = False
    try:
        with duckdb.connect() as conn:
            relation = parquet_relation(paths)
            source_count, source_net = conn.execute(
                f"SELECT COUNT(*), SUM(obligation_change_usd) FROM {relation}"
            ).fetchone()
            if source_count != harvested["rows"] or (
                Decimal(str(source_net)) != Decimal(harvested["net_obligations_usd"])
            ):
                raise ApiError("Silver facts disagree with harvest audit")
            queries = gold_queries(relation, first, last)
            for name, query in queries.items():
                path = work / f"{name}.parquet"
                _sql_file(conn, query, path)
            quality = _audit_gold(
                conn, work, source_count, str(source_net),
                complete=harvested["status"] == "complete",
            )
            if quality["errors"]:
                raise ApiError("Gold quality gate failed: " + "; ".join(quality["errors"]))
        output_checksums = {p.name: _checksum(p) for p in sorted(work.glob("*.parquet"))}
        manifest = {
            "schema_version": GOLD_SCHEMA_VERSION,
            "created_at_utc": utc_stamp(),
            "status": quality["status"],
            "coverage": coverage,
            "campaign_configuration": campaign["configuration"],
            "campaign_path": str(checkpoint),
            "harvest_status": harvested["status"],
            "harvest_rows": harvested["rows"],
            "harvest_net_obligations_usd": harvested["net_obligations_usd"],
            "source_silver_files": sources,
            "outputs_sha256": output_checksums,
            "quality": quality,
            "financial_measure": "obligation_change_usd (signed, not outlays)",
            "scope_warning": "DOT only" if agency == "Department of Transportation"
                             else "Selected agency or all agencies; not total federal outlays",
        }
        write_json(work / "gold_quality.json", manifest)
        if target.exists():
            backup = target.with_name(target.name + ".previous")
            if backup.exists():
                raise ApiError(f"Previous backup already exists: {backup}; review manually")
            os.replace(target, backup)
            try:
                os.replace(work, target)
            except BaseException:
                os.replace(backup, target)
                raise
            shutil.rmtree(backup)
        else:
            os.replace(work, target)
        succeeded = True
        manifest["output_directory"] = str(target)
        return manifest
    finally:
        if not succeeded and work.exists():
            shutil.rmtree(work)


def inspect_gold(root: Path, first: int, last: int, agency: str | None) -> dict[str, Any]:
    """Re-check produced Parquet hashes without modifying any files."""
    cp = campaign_path(root.expanduser().resolve(), first, last, agency)
    folder = root.expanduser().resolve() / "data/marts/gold" / cp.parent.name
    manifest_path = folder / "gold_quality.json"
    if not manifest_path.is_file():
        raise ApiError(f"No Gold manifest: {manifest_path}")
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    for name, expected in manifest["outputs_sha256"].items():
        path = folder / name
        if not path.is_file() or _checksum(path) != expected:
            raise ApiError(f"Gold Parquet missing or modified: {path}")
    return manifest
