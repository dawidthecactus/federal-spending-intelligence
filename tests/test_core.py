from __future__ import annotations

import io
import json
import sys
import zipfile
from datetime import date
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from fsi.client import ApiError, trusted_download_url
from fsi.core import (
    build_filters, fiscal_year, fiscal_year_range, month_windows, normalize_probe, safe_amount,
)
from fsi.cli import preflight
from fsi.quality import download_zip


def test_us_fiscal_year_boundaries():
    assert fiscal_year(date(2025, 9, 30)) == 2025
    assert fiscal_year(date(2025, 10, 1)) == 2026
    assert fiscal_year_range(2026) == ("2025-10-01", "2026-09-30")


def test_monthly_partitions_no_gaps():
    periods = month_windows(2026)
    assert len(periods) == 12
    assert periods[0] == ("2025-10-01", "2025-10-31")
    assert periods[-1] == ("2026-09-01", "2026-09-30")
    assert periods[5] == ("2026-03-01", "2026-03-31")


def test_filter_contract_types_and_agency():
    filters = build_filters("2026-09-01", "2026-09-30", "Department of Transportation")
    assert filters["award_type_codes"] == ["A", "B", "C", "D"]
    assert filters["agencies"][0]["type"] == "awarding"
    assert filters["time_period"][0]["date_type"] == "action_date"


def test_filter_rejects_inverted_window():
    with pytest.raises(ValueError):
        build_filters("2026-09-30", "2026-09-01")


def test_money_and_negative_values():
    assert safe_amount("-1200.5") == "-1200.50"
    assert safe_amount("4.567") == "4.57"
    assert safe_amount("not_money") is None


def test_probe_normalization_preserves_negative_amount_and_award_key():
    row = {
        "Award ID": "EXAMPLE", "generated_internal_id": "CONT_AWD_123",
        "Mod": "P00001", "Action Date": "2025-10-01", "Transaction Amount": "-48.55",
        "NAICS": {"code": "012345"},
    }
    normalized = normalize_probe(row)
    assert normalized["fiscal_year"] == 2026
    assert normalized["generated_award_id"] == "CONT_AWD_123"
    assert normalized["transaction_amount_usd"] == "-48.55"
    assert "row_fingerprint" in normalized


def test_only_official_download_hosts_allowed():
    assert trusted_download_url("https://files.usaspending.gov/a.zip").endswith("/a.zip")
    with pytest.raises(ApiError):
        trusted_download_url("https://malicious.example/bad.zip")
    with pytest.raises(ApiError):
        trusted_download_url("http://files.usaspending.gov/a.zip")


class FakeCountClient:
    def __init__(self, result):
        self.result = result

    def post(self, route, body):
        assert route.endswith("/download/count/")
        assert body["spending_level"] == "transactions"
        return self.result


def test_preflight_accepts_under_limit():
    response = preflight(FakeCountClient({
        "calculated_count": 765, "maximum_limit": 500000, "rows_gt_limit": False,
        "transaction_rows_gt_limit": False
    }), {"award_type_codes": ["A"]})
    assert response["calculated_count"] == 765


def test_preflight_blocks_truncation():
    with pytest.raises(ApiError, match="limit exceeded"):
        preflight(FakeCountClient({
            "calculated_count": 600000, "maximum_limit": 500000,
            "rows_gt_limit": True, "transaction_rows_gt_limit": True
        }), {"award_type_codes": ["A"]})


def test_download_zip_and_sha(tmp_path: Path):
    source = io.BytesIO()
    with zipfile.ZipFile(source, "w") as z:
        z.writestr("a.csv", "id,amount\n1,-14.10\n")
    payload = source.getvalue()

    class Stream:
        def iter_content(self, chunk_size):
            yield payload[:6]
            yield payload[6:]
        def close(self):
            pass

    class Client:
        def stream(self, url):
            assert "usaspending.gov" in url
            return Stream()

    dest = tmp_path / "source.zip"
    digest = download_zip(Client(), "https://files.usaspending.gov/source.zip", dest)
    assert dest.read_bytes() == payload
    assert len(digest) == 64


def test_download_zip_size_guard(tmp_path: Path):
    class Stream:
        def iter_content(self, chunk_size):
            yield b"x" * 2_000
        def close(self):
            pass
    class Client:
        def stream(self, url):
            return Stream()
    with pytest.raises(ApiError, match="exceeds"):
        download_zip(Client(), "https://files.usaspending.gov/file.zip",
                     tmp_path / "too_big.zip", max_mb=0)


@pytest.mark.skipif(__import__("importlib").util.find_spec("duckdb") is None,
                    reason="DuckDB not installed in build environment")
def test_offline_parquet_probe_conversion(tmp_path: Path):
    import duckdb
    from fsi.quality import parquet_from_jsonl
    raw = tmp_path / "input.jsonl"
    raw.write_text(json.dumps({
        "award_id": "A", "transaction_amount_usd": "-100.25",
        "action_date": "2026-09-09", "row_fingerprint": "a"
    }) + "\n")
    out = tmp_path / "out.parquet"
    assert parquet_from_jsonl(raw, out) == 1
    with duckdb.connect() as conn:
        sql = f"SELECT transaction_amount_usd FROM read_parquet('{out}')"
        amount = conn.execute(sql).fetchone()[0]
    assert str(amount) == "-100.25"


def test_probe_writes_raw_pages_and_manifest_without_live_api(tmp_path, monkeypatch):
    from argparse import Namespace
    from fsi import cli

    class FakeApi:
        def post(self, path, payload):
            assert path.endswith("/spending_by_transaction/")
            assert payload["limit"] == 2
            return {
                "results": [
                    {"Award ID": "X1", "Mod": "0", "Action Date": "2026-09-30",
                     "Transaction Amount": "120.00"},
                    {"Award ID": "X2", "Mod": "P1", "Action Date": "2026-09-29",
                     "Transaction Amount": "-20.00"},
                ],
                "page_metadata": {"hasNext": True},
            }

    def fake_parquet(source, destination):
        destination.parent.mkdir(parents=True, exist_ok=True)
        destination.write_text("stub test only")
        return len(source.read_text().splitlines())

    monkeypatch.setattr(cli, "parquet_from_jsonl", fake_parquet)
    args = Namespace(output=str(tmp_path), fy=2026, limit=2, agency=None)
    assert cli.cmd_probe(args, FakeApi()) == 0
    manifests = list(tmp_path.glob("data/raw/probe/**/manifest.json"))
    assert len(manifests) == 1
    manifest = json.loads(manifests[0].read_text())
    assert manifest["status"] == "sample_only_not_complete"
    assert manifest["rows_in_parquet"] == 2
    assert manifest["biased_sample"] is True


def test_bulk_preflight_failure_does_not_create_job(tmp_path):
    from argparse import Namespace
    from fsi.cli import cmd_bulk

    class FakeApi:
        def post(self, route, payload):
            if route.endswith("/count/"):
                return {"calculated_count": 900000, "maximum_limit": 500000,
                        "rows_gt_limit": True, "transaction_rows_gt_limit": True}
            raise AssertionError("Must never submit an oversized export")

    args = Namespace(
        start="2026-09-01", end="2026-09-30", agency=None, output=str(tmp_path)
    )
    with pytest.raises(ApiError, match="limit exceeded"):
        cmd_bulk(args, FakeApi())
    assert not (tmp_path / "data").exists()


def test_split_prime_and_subaward_counts_from_real_log():
    from fsi.quality import classify_export, summarize_export_files
    files = [
        {"parquet": "00_Contracts_PrimeTransactions.parquet", "rows": 3629,
         "columns": ["contract_transaction_unique_key", "federal_action_obligation",
                     "action_date"]},
        {"parquet": "01_Contracts_Subawards.parquet", "rows": 104,
         "columns": ["subaward_sam_report_id", "subaward_number", "subaward_amount"]},
    ]
    output = summarize_export_files(files, 3629)
    assert output["prime_transaction_rows"] == 3629
    assert output["subaward_rows"] == 104
    assert output["all_export_rows"] == 3733
    assert output["prime_count_matches"] is True
    assert classify_export(["unknown_field"]) == "unclassified"


def test_unclassified_data_never_receives_verified_status():
    from fsi.quality import summarize_export_files
    files = [
        {"parquet": "prime.parquet", "rows": 3629,
         "columns": ["contract_transaction_unique_key", "federal_action_obligation"]},
        {"parquet": "unknown.parquet", "rows": 12, "columns": ["foo", "bar"]},
    ]
    result = summarize_export_files(files, 3629)
    assert result["prime_count_matches"] is False
    assert result["unclassified_rows"] == 12


def test_wrong_prime_count_never_receives_verified_status():
    from fsi.quality import summarize_export_files
    result = summarize_export_files([{
        "parquet": "prime.parquet", "rows": 3628,
        "columns": ["contract_transaction_unique_key", "federal_action_obligation"],
    }], 3629)
    assert result["prime_count_matches"] is False


def test_verify_cli_requires_valid_manifest(tmp_path):
    from fsi.cli import verify_job
    job = tmp_path / "job.json"
    job.write_text(json.dumps({"workflow": "bulk_transactions"}), encoding="utf-8")
    with pytest.raises(ApiError, match="expected transaction count"):
        verify_job(job)


@pytest.mark.skipif(__import__("importlib").util.find_spec("duckdb") is None,
                    reason="DuckDB not installed in artifact environment")
def test_full_offline_bulk_pipeline_separates_subawards(tmp_path: Path):
    from fsi.quality import inspect_zip_and_convert, verify_parquet_export
    archive = tmp_path / "PrimeTransactionsAndSubawards.zip"
    with zipfile.ZipFile(archive, "w") as z:
        z.writestr(
            "Contracts_PrimeTransactions.csv",
            "contract_transaction_unique_key,federal_action_obligation,action_date\n"
            "CONT_1,1000.25,2026-09-04\n"
            "CONT_2,-25.00,2026-09-15\n",
        )
        z.writestr(
            "Contracts_Subawards.csv",
            "subaward_sam_report_id,subaward_number,subaward_amount\n"
            "REPORT_1,SUB_1,100.00\n",
        )
    stage = tmp_path / "staging"
    raw = inspect_zip_and_convert(archive, stage)
    assert raw["rows_in_csv_files"] == 3
    result = verify_parquet_export(stage, 2, "2026-09-01", "2026-09-30")
    assert result["status"] == "verified"
    assert result["prime_transaction_rows"] == 2
    assert result["subaward_rows"] == 1
    assert result["transaction_quality"]["negative_obligation_rows"] == 1


@pytest.mark.skipif(__import__("importlib").util.find_spec("duckdb") is None,
                    reason="DuckDB not installed in artifact environment")
def test_full_bulk_fails_on_duplicate_transaction_key(tmp_path: Path):
    from fsi.quality import inspect_zip_and_convert, verify_parquet_export
    archive = tmp_path / "contracts.zip"
    with zipfile.ZipFile(archive, "w") as z:
        z.writestr(
            "Contracts_PrimeTransactions.csv",
            "contract_transaction_unique_key,federal_action_obligation,action_date\n"
            "CONT_1,1000.25,2026-09-04\n"
            "CONT_1,-25.00,2026-09-15\n",
        )
    stage = tmp_path / "staging"
    inspect_zip_and_convert(archive, stage)
    report = verify_parquet_export(stage, 2, "2026-09-01", "2026-09-30")
    assert report["status"] == "needs_review"
    assert report["transaction_quality"]["duplicate_or_missing_keys"] == 1
