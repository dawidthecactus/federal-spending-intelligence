"""Typed contract transaction fact, with a strict Bronze -> Silver quality gate.

The official PrimeTransactions export is the source; Subawards never enter this fact.
"""
from __future__ import annotations

import hashlib
import json
import os
from pathlib import Path
from typing import Any

from .client import ApiError
from .core import write_json
from .quality import classify_export, quote_sql

# Source columns grounded in the actual USAspending PrimeTransactions CSV schema.
REQUIRED_SOURCE_COLUMNS = frozenset({
    "contract_transaction_unique_key", "contract_award_unique_key", "action_date",
    "federal_action_obligation", "action_date_fiscal_year",
    "awarding_agency_code", "awarding_agency_name", "recipient_uei", "recipient_name",
    "naics_code", "product_or_service_code", "award_id_piid",
})

# No award-level cumulative obligations/outlays in the additive transaction fact.
# Their presence in the original Bronze dataset is deliberate, but they are
# semantically unsafe to sum once per transaction.
FACT_COLUMNS = (
    ("contract_transaction_unique_key", "transaction_key", "TEXT"),
    ("contract_award_unique_key", "award_key", "TEXT"),
    ("award_id_piid", "award_id_piid", "TEXT"),
    ("modification_number", "modification_number", "TEXT"),
    ("transaction_number", "transaction_number", "TEXT"),
    ("action_date", "action_date", "DATE"),
    ("federal_action_obligation", "obligation_change_usd", "DECIMAL(38, 10)"),
    ("awarding_agency_code", "awarding_agency_code", "TEXT"),
    ("awarding_agency_name", "awarding_agency_name", "TEXT"),
    ("awarding_sub_agency_code", "awarding_subagency_code", "TEXT"),
    ("awarding_sub_agency_name", "awarding_subagency_name", "TEXT"),
    ("funding_agency_code", "funding_agency_code", "TEXT"),
    ("funding_agency_name", "funding_agency_name", "TEXT"),
    ("recipient_uei", "recipient_uei", "TEXT"),
    ("recipient_name", "recipient_name", "TEXT"),
    ("recipient_parent_uei", "recipient_parent_uei", "TEXT"),
    ("recipient_parent_name", "recipient_parent_name", "TEXT"),
    ("naics_code", "naics_code", "TEXT"),
    ("naics_description", "naics_description", "TEXT"),
    ("product_or_service_code", "psc_code", "TEXT"),
    ("product_or_service_code_description", "psc_description", "TEXT"),
    ("award_type_code", "award_type_code", "TEXT"),
    ("action_type_code", "action_type_code", "TEXT"),
    ("primary_place_of_performance_state_code", "performance_state_code", "TEXT"),
    ("primary_place_of_performance_country_code", "performance_country_code", "TEXT"),
    ("extent_competed_code", "extent_competed_code", "TEXT"),
    ("type_of_set_aside_code", "set_aside_code", "TEXT"),
)


def prime_parquet_files(staging: Path, duckdb_module: Any) -> list[Path]:
    """Identify source by its schema, not a fragile filename convention."""
    files: list[Path] = []
    with duckdb_module.connect() as conn:
        for file in sorted(staging.glob("*.parquet")):
            sql_path = quote_sql(file.as_posix())
            cols = [r[0] for r in conn.execute(
                f"DESCRIBE SELECT * FROM read_parquet({sql_path})"
            ).fetchall()]
            if classify_export(cols) == "prime_transactions":
                files.append(file)
    if not files:
        raise ApiError(f"No prime transaction Parquet files found in {staging}")
    return files


def sql_fact_projection(columns: set[str]) -> str:
    missing = REQUIRED_SOURCE_COLUMNS - columns
    if missing:
        raise ApiError("Missing required Bronze columns: " + ", ".join(sorted(missing)))
    expressions = []
    for src, alias, kind in FACT_COLUMNS:
        if src not in columns:
            if kind != "TEXT":
                raise ApiError(f"Required field {src} is absent")
            expressions.append(f'CAST(NULL AS VARCHAR) AS "{alias}"')
        elif kind == "DATE":
            expressions.append(f'TRY_CAST("{src}" AS DATE) AS "{alias}"')
        elif kind.startswith("DECIMAL"):
            expressions.append(f'TRY_CAST("{src}" AS {kind}) AS "{alias}"')
        else:
            expressions.append(f'NULLIF(TRIM("{src}"), \'\') AS "{alias}"')
    # Using action date avoids assuming source FY strings are never missing.
    expressions += [
        'EXTRACT(YEAR FROM TRY_CAST("action_date" AS DATE))::INTEGER'
        ' + CASE WHEN EXTRACT(MONTH FROM TRY_CAST("action_date" AS DATE)) >= 10'
        ' THEN 1 ELSE 0 END AS fiscal_year',
        'EXTRACT(MONTH FROM TRY_CAST("action_date" AS DATE))::INTEGER AS calendar_month',
        'CASE WHEN TRY_CAST("federal_action_obligation" AS DECIMAL(38,10)) < 0'
        ' THEN TRUE ELSE FALSE END AS is_deobligation',
        'CASE WHEN TRY_CAST("federal_action_obligation" AS DECIMAL(38,10)) > 0'
        ' THEN TRUE ELSE FALSE END AS is_positive_obligation',
    ]
    return ",\n                    ".join(expressions)


def source_relation(paths: list[Path]) -> str:
    file_literals = ",".join(quote_sql(p.as_posix()) for p in paths)
    return f"read_parquet([{file_literals}], union_by_name=true)"


def build_silver(job_file: Path, output_root: Path, *, overwrite: bool = False) -> dict[str, Any]:
    """Build an independently audited Silver fact from an already-verified job."""
    import duckdb
    from .cli import verify_job

    job_file = job_file.resolve()
    # Re-run ALL existing gates, including source ZIP SHA-256, not just manifest state.
    if verify_job(job_file) != 0:
        raise ApiError("Bronze Quality Gate failed; Silver output NOT created")
    job = json.loads(job_file.read_text(encoding="utf-8"))
    staging = Path(job["parquet_folder"])
    files = prime_parquet_files(staging, duckdb)
    run_dir = output_root.resolve() / "data" / "silver" / "jobs" / job_file.parent.name
    target = run_dir / "fact_contract_transactions.parquet"
    audit_path = run_dir / "silver_quality.json"
    if (target.exists() or audit_path.exists()) and not overwrite:
        raise ApiError(f"Silver job already exists: {target}. Use --overwrite if intentional")
    run_dir.mkdir(parents=True, exist_ok=True)
    partial = run_dir / "fact_contract_transactions.parquet.part"
    partial.unlink(missing_ok=True)
    relation = source_relation(files)
    with duckdb.connect() as conn:
        desc = conn.execute(f"DESCRIBE SELECT * FROM {relation}").fetchall()
        columns = {row[0] for row in desc}
        projection = sql_fact_projection(columns)
        source_fy_mismatch = conn.execute(f"""
            SELECT COUNT(*) FROM {relation}
            WHERE TRY_CAST(action_date_fiscal_year AS INT) IS NOT NULL
            AND TRY_CAST(action_date_fiscal_year AS INT) != (
                EXTRACT(YEAR FROM TRY_CAST(action_date AS DATE))::INTEGER
                + CASE WHEN EXTRACT(MONTH FROM TRY_CAST(action_date AS DATE)) >= 10
                       THEN 1 ELSE 0 END)
        """).fetchone()[0]
        if source_fy_mismatch:
            raise ApiError(f"Official action_date_fiscal_year disagrees with date: {source_fy_mismatch}")
        fact_sql = f"SELECT {projection} FROM {relation}"
        try:
            conn.execute(f"COPY ({fact_sql}) TO {quote_sql(partial.as_posix())} "
                         "(FORMAT PARQUET, COMPRESSION ZSTD)")
            silver = f"read_parquet({quote_sql(partial.as_posix())})"
            # DuckDB decimal for exact aggregations; Python serializes Decimal as string.
            row = conn.execute(f"""
                SELECT COUNT(*) AS rows,
                    COUNT(DISTINCT transaction_key) AS unique_keys,
                    COUNT(*) FILTER (WHERE transaction_key IS NULL) AS missing_keys,
                    COUNT(*) FILTER (WHERE action_date IS NULL) AS invalid_dates,
                    COUNT(*) FILTER (WHERE obligation_change_usd IS NULL) AS invalid_amounts,
                    COUNT(*) FILTER (WHERE is_deobligation) AS negative_rows,
                    COUNT(*) FILTER (WHERE obligation_change_usd = 0) AS zero_rows,
                    SUM(obligation_change_usd) AS net_obligations_usd,
                    SUM(CASE WHEN obligation_change_usd > 0
                             THEN obligation_change_usd ELSE 0 END) AS positive_obligations_usd,
                    SUM(CASE WHEN obligation_change_usd < 0
                             THEN obligation_change_usd ELSE 0 END) AS negative_obligations_usd,
                    MIN(action_date) AS min_action_date, MAX(action_date) AS max_action_date,
                    COUNT(DISTINCT awarding_agency_code) AS awarding_agencies,
                    COUNT(DISTINCT recipient_uei) AS unique_recipient_ueis
                FROM {silver}
            """).fetchone()
            fields = ["rows", "unique_keys", "missing_keys", "invalid_dates", "invalid_amounts",
                      "negative_rows", "zero_rows", "net_obligations_usd",
                      "positive_obligations_usd", "negative_obligations_usd",
                      "min_action_date", "max_action_date", "awarding_agencies",
                      "unique_recipient_ueis"]
            stats = dict(zip(fields, row))
            expected = job["expected_transaction_count"]
            bronze_negative = job["qa"]["transaction_quality"]["negative_obligation_rows"]
            start, end = job["coverage"]["start"], job["coverage"]["end"]
            errors = []
            if stats["rows"] != expected:
                errors.append(f"row_count={stats['rows']} expected={expected}")
            if stats["unique_keys"] != stats["rows"] or stats["missing_keys"]:
                errors.append("transaction key is not unique/non-null")
            if stats["invalid_dates"] or stats["invalid_amounts"]:
                errors.append("invalid typed dates or money")
            if stats["negative_rows"] != bronze_negative:
                errors.append("negative value count changed during typing")
            if (stats["min_action_date"].isoformat() < start
                    or stats["max_action_date"].isoformat() > end):
                errors.append("date range changed during typing")
            # Check the SUM of original (typed) and Silver (typed), without rounding to float.
            raw_net = conn.execute(f"""
                SELECT SUM(TRY_CAST(federal_action_obligation AS DECIMAL(38,10)))
                FROM {relation}
            """).fetchone()[0]
            if raw_net != stats["net_obligations_usd"]:
                errors.append("net obligations differ from Bronze")
            if errors:
                raise ApiError("Silver Quality Gate FAILED: " + "; ".join(errors))
        except Exception:
            partial.unlink(missing_ok=True)
            raise
    # Windows: close DuckDB connections/readers before replacing a Parquet file.
    os.replace(partial, target)

    with target.open("rb") as f:
        digest = hashlib.file_digest(f, "sha256").hexdigest()
    audit = {
        "model_version": "0.2.0", "status": "verified",
        "source_job": str(job_file),
        "source_zip_sha256": job["download_sha256"],
        "source_prime_parquet": [str(p) for p in files],
        "source_subaward_rows_excluded": job["qa"]["subaward_rows_excluded"],
        "fact_file": str(target), "fact_sha256": digest,
        "coverage": job["coverage"],
        "quality": {k: str(v) if k.endswith("_usd") or k.endswith("_date") else v
                    for k, v in stats.items()},
        "grain": "one row per contract_transaction_unique_key",
        "additive_measure": "obligation_change_usd (signed, not outlays)",
        "nonadditive_excluded": ["total_dollars_obligated",
                                 "total_outlayed_amount_for_overall_award"],
    }
    write_json(audit_path, audit)
    return audit
