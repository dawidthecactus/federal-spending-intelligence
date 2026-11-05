"""Gold model unit tests; integration test uses DuckDB when installed."""
from __future__ import annotations

import hashlib
import json
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from fsi.client import ApiError
from fsi.gold import (
    UNKNOWN, clean_key, gold_queries, inspect_gold, parquet_relation, validate_coverage,
)
from fsi.harvest import campaign_path, create_campaign, leaf_parts, split_window


def test_full_campaign_date_tiling():
    campaign = create_campaign(2023, 2026, "Department of Transportation")
    all_partitions = validate_coverage(campaign)
    assert all_partitions == {
        "start": "2022-10-01", "end": "2026-09-30", "leaf_partitions": 48,
        "incomplete_partitions": 48, "empty_partitions": 0,
    }


def test_split_month_still_tiles_dates():
    campaign = create_campaign(2026, 2026, "Department of Transportation")
    first_id, first = leaf_parts(campaign)[0]
    left, right = split_window(first)
    first["status"] = "split"
    campaign["partitions"]["left"] = left
    campaign["partitions"]["right"] = right
    assert validate_coverage(campaign)["leaf_partitions"] == 13


def test_gap_rejected_even_if_marked_verified():
    campaign = create_campaign(2026, 2026, "Department of Transportation")
    campaign["partitions"].pop(next(iter(campaign["partitions"])))
    with pytest.raises(ApiError, match="Gap or overlap"):
        validate_coverage(campaign)


def test_overlap_rejected():
    campaign = create_campaign(2026, 2026, "Department of Transportation")
    first = leaf_parts(campaign)[0][1]
    first["end"] = "2025-11-01"
    with pytest.raises(ApiError, match="Gap or overlap"):
        validate_coverage(campaign)


def test_queries_keep_signed_obligations_and_distinct_dimensions():
    q = gold_queries("read_parquet('example.parquet')", 2023, 2026)
    assert set(q) == {
        "fact_contract_transactions", "dim_date", "dim_agency",
        "dim_recipient", "dim_naics", "dim_psc",
    }
    fact = q["fact_contract_transactions"]
    assert "obligation_change_usd" in fact
    assert "total_dollars_obligated" not in fact
    assert "outlayed_amount" not in fact
    assert "funding_agency_key" in fact
    assert "DATE '2022-10-01'" in q["dim_date"]
    assert "DATE '2026-09-30'" in q["dim_date"]
    assert "GROUP BY recipient_key" in q["dim_recipient"]


def test_unknown_key_is_explicit_and_parquet_path_quoted():
    assert UNKNOWN in clean_key("recipient_uei")
    p = Path("O'Brien.parquet")
    assert "O''Brien.parquet" in parquet_relation([p])


def test_gold_report_missing_is_error(tmp_path: Path):
    with pytest.raises(ApiError, match="No Gold manifest"):
        inspect_gold(tmp_path, 2023, 2026, "Department of Transportation")


@pytest.mark.skipif(__import__("importlib").util.find_spec("duckdb") is None,
                    reason="DuckDB is needed for real Parquet integration")
def test_real_gold_from_fully_verified_silver(tmp_path: Path):
    import duckdb

    from fsi.gold import build_gold
    from fsi.harvest import atomic_write

    agency = "Department of Transportation"
    cp = campaign_path(tmp_path, 2026, 2026, agency)
    campaign = create_campaign(2026, 2026, agency)
    # Synthetic complete campaign: 11 verified empty months + 1 month of fake Silver data.
    for part in campaign["partitions"].values():
        part["status"] = "empty"
    p = next(v for v in campaign["partitions"].values()
             if v["start"] == "2026-09-01")
    p["status"] = "verified"
    p["rows"] = 3
    bulk = tmp_path / "data/raw/bulk/synthetic" / "job.json"
    bulk.parent.mkdir(parents=True, exist_ok=True)
    bulk.write_text('{}')
    p["job"] = str(bulk)
    atomic_write(cp, campaign)

    parquet = tmp_path / "data/silver/jobs/synthetic" / "fact_contract_transactions.parquet"
    parquet.parent.mkdir(parents=True, exist_ok=True)
    with duckdb.connect() as conn:
        conn.execute(f"""
            COPY (
                SELECT
                    'tx-1' AS transaction_key, 'award1' AS award_key, 'X1' AS award_id_piid,
                    '0' AS modification_number, '1' AS transaction_number,
                    DATE '2026-09-10' AS action_date, 2026 AS fiscal_year, 9 AS calendar_month,
                    CAST(100.25 AS DECIMAL(38,10)) AS obligation_change_usd,
                    '69' AS awarding_agency_code,
                    'Department of Transportation' AS awarding_agency_name,
                    '69' AS funding_agency_code,
                    'Department of Transportation' AS funding_agency_name,
                    'UEI-1' AS recipient_uei, 'Acme' AS recipient_name,
                    '001234' AS naics_code, 'Industry' AS naics_description,
                    'P123' AS psc_code, 'PSC service' AS psc_description,
                    FALSE AS is_deobligation, TRUE AS is_positive_obligation,
                    'A' AS action_type_code, 'B' AS award_type_code,
                    'CA' AS performance_state_code, 'USA' AS performance_country_code,
                    'A' AS extent_competed_code, 'NONE' AS set_aside_code
                UNION ALL
                SELECT
                    'tx-2', 'award1', 'X1', 'P1', '2', DATE '2026-09-11', 2026, 9,
                    CAST(-25.25 AS DECIMAL(38,10)), '69', 'Department of Transportation',
                    '69', 'Department of Transportation', 'UEI-1', 'Acme',
                    '001234', 'Industry', 'P123', 'PSC service', TRUE, FALSE,
                    'B', 'B', 'CA', 'USA', 'A', 'NONE'
                UNION ALL
                SELECT
                    'tx-3', 'award2', 'X2', '0', '3', DATE '2026-09-12', 2026, 9,
                    CAST(0 AS DECIMAL(38,10)), '69', 'Department of Transportation',
                    NULL, NULL, NULL, 'Unknown recipient', NULL, NULL, NULL, NULL,
                    FALSE, FALSE, 'A', 'B', 'NY', 'USA', 'A', 'NONE'
            ) TO '{parquet.as_posix()}' (FORMAT PARQUET)
        """)
    sha = hashlib.sha256(parquet.read_bytes()).hexdigest()
    manifest = {
        "status": "verified", "fact_file": str(parquet), "fact_sha256": sha,
        "coverage": {"start": "2026-09-01", "end": "2026-09-30", "agency": agency},
        "quality": {"rows": 3},
    }
    (parquet.parent / "silver_quality.json").write_text(json.dumps(manifest))
    result = build_gold(tmp_path, 2026, 2026, agency)
    assert result["quality"]["rows"] == 3
    assert result["quality"]["net_obligations_usd"].startswith("75")
    assert result["quality"]["negative_rows"] == 1
    assert result["quality"]["recipient_uei_missing_rows"] == 1
    assert result["quality"]["dimensions"]["dim_date"] == 365
    assert inspect_gold(tmp_path, 2026, 2026, agency)["status"] == "verified"
    out = Path(result["output_directory"])
    with duckdb.connect() as conn:
        recipient_file = (out / "dim_recipient.parquet").as_posix()
        dims = conn.execute(
            f"SELECT recipient_key FROM read_parquet('{recipient_file}')"
        ).fetchall()
    assert (UNKNOWN,) in dims


@pytest.mark.skipif(__import__("importlib").util.find_spec("duckdb") is None,
                    reason="DuckDB needed for full build")
def test_partial_campaign_rejected_before_gold(tmp_path: Path):
    from fsi.gold import build_gold
    from fsi.harvest import atomic_write
    cp = campaign_path(tmp_path, 2026, 2026, "Department of Transportation")
    atomic_write(cp, create_campaign(2026, 2026, "Department of Transportation"))
    with pytest.raises(ApiError, match="incomplete"):
        build_gold(tmp_path, 2026, 2026, "Department of Transportation")
    assert not (tmp_path / "data/marts").exists()
