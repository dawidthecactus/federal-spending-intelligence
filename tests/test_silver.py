"""v0.2 tests use synthetic data only; never require real government exports."""
from __future__ import annotations

import importlib.util
import json
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
from fsi.client import ApiError
from fsi.silver import REQUIRED_SOURCE_COLUMNS, sql_fact_projection


def test_projection_requires_actual_prime_schema():
    with pytest.raises(ApiError, match="Missing required Bronze columns"):
        sql_fact_projection({"contract_award_unique_key", "award_id_piid"})


def test_projection_uses_correct_transaction_additive_measure():
    query = sql_fact_projection(set(REQUIRED_SOURCE_COLUMNS))
    assert 'TRY_CAST("federal_action_obligation" AS DECIMAL(38, 10))' in query
    assert 'AS "obligation_change_usd"' in query
    assert "total_dollars_obligated" not in query
    assert "total_outlayed_amount_for_overall_award" not in query
    assert "is_deobligation" in query


@pytest.mark.skipif(importlib.util.find_spec("duckdb") is None,
                    reason="DuckDB not installed in artifact test container")
def test_silver_etl_real_parquet_and_qa(tmp_path: Path, monkeypatch):
    import duckdb
    from fsi import cli
    from fsi.silver import build_silver
    from fsi.quality import quote_sql

    staging = tmp_path / "data/staging/bulk/2026-09-test"
    staging.mkdir(parents=True)
    sample = staging / "prime.parquet"
    subaward = staging / "subaward.parquet"
    with duckdb.connect() as db:
        db.execute(f"""
            COPY (SELECT * FROM (VALUES
              ('tx-1','award-a','PIID-A','2026-09-01','2026','120.00',
               'DOT','Department of Transportation','UEI1','Test Contracting A',
               '001234','B123'),
              ('tx-2','award-a','PIID-A','2026-09-28','2026','-20.25',
               'DOT','Department of Transportation','UEI1','Test Contracting A',
               '001234','B123')
            ) AS t(contract_transaction_unique_key,contract_award_unique_key,
                   award_id_piid,action_date,action_date_fiscal_year,
                   federal_action_obligation,awarding_agency_code,
                   awarding_agency_name,recipient_uei,recipient_name,
                   naics_code,product_or_service_code))
            TO {quote_sql(sample.as_posix())} (FORMAT PARQUET)
        """)
        db.execute(f"""
            COPY (SELECT 'report-1' AS subaward_sam_report_id,
                         'sub-1' AS subaward_number,
                         '5.00' AS subaward_amount)
            TO {quote_sql(subaward.as_posix())} (FORMAT PARQUET)
        """)

    jobdir = tmp_path / "data/raw/bulk/2026-09-test"
    jobdir.mkdir(parents=True)
    jobfile = jobdir / "job.json"
    jobfile.write_text(json.dumps({
        "workflow": "bulk_transactions", "status": "verified",
        "parquet_folder": str(staging), "expected_transaction_count": 2,
        "download_sha256": "sample_sha256_for_test",
        "coverage": {"start":"2026-09-01","end":"2026-09-30", "agency":"DOT"},
        "qa": {"subaward_rows_excluded": 1,
               "transaction_quality": {"negative_obligation_rows": 1}},
    }))
    # Preexisting v0.1.1 verify gate has independent tests; in this integration
    # check we substitute it so the fixture needs no compressed ZIP.
    monkeypatch.setattr(cli, "verify_job", lambda job_path: 0)
    audit = build_silver(jobfile, tmp_path)
    assert audit["status"] == "verified"
    assert audit["quality"]["rows"] == 2
    assert audit["quality"]["net_obligations_usd"] == "99.7500000000"
    assert audit["quality"]["negative_rows"] == 1
    assert audit["source_subaward_rows_excluded"] == 1
    file = audit["fact_file"]
    with duckdb.connect() as db:
        rows = db.execute("SELECT transaction_key, obligation_change_usd, "
                          "fiscal_year FROM read_parquet(?) ORDER BY transaction_key",
                          [file]).fetchall()
        columns = [r[0] for r in db.execute("DESCRIBE SELECT * FROM read_parquet(?)",
                                            [file]).fetchall()]
    assert rows[0][0] == "tx-1"
    assert str(rows[1][1]) == "-20.2500000000"
    assert all(r[2] == 2026 for r in rows)
    assert "total_dollars_obligated" not in columns
    with pytest.raises(ApiError, match="already exists"):
        build_silver(jobfile, tmp_path)


@pytest.mark.skipif(importlib.util.find_spec("duckdb") is None,
                    reason="DuckDB not installed in artifact test container")
def test_silver_fails_if_bronze_gate_fails(tmp_path: Path, monkeypatch):
    from fsi import cli
    from fsi.silver import build_silver
    monkeypatch.setattr(cli, "verify_job", lambda path: 3)
    with pytest.raises(ApiError, match="Bronze Quality Gate failed"):
        build_silver(tmp_path / "job.json", tmp_path)
