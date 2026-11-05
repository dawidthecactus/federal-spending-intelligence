# Federal Spending Intelligence 🌵

**Data Engineering + Financial Analytics + Power BI | USAspending.gov**

Reproducible acquisition of **federal contract transactions**, not award totals.
Source: https://api.usaspending.gov/docs/endpoints (official US Treasury data).
Current version: **v0.2 typed Silver + corrected Bronze Quality Gate**. No fabricated data is included.

## Quick start (Windows PowerShell)

```powershell
cd "$HOME\Desktop\Porfolio\Power BI"
# Extract the ZIP here; then:
cd .\federal-spending-intelligence
python -m venv .venv
.\.venv\Scripts\python.exe -m pip install -e ".[dev]"
.\.venv\Scripts\python.exe -m pytest -q

# Check the 12 date windows in fiscal year 2026:
fsi plan --fy 2026

# Small API smoke test only (top transaction amounts, not representative):
fsi probe --fy 2026 --limit 300 --agency "Department of Transportation"

# Real CSV export job: 1 calendar month, Transportation, Sep 2026:
fsi bulk --start 2026-09-01 --end 2026-09-30 --agency "Department of Transportation"
```

If PowerShell execution policy prevents activation, skip activation:

```powershell
.\.venv\Scripts\python.exe -m pip install -e ".[dev]"
.\.venv\Scripts\fsi.exe plan --fy 2026
.\.venv\Scripts\fsi.exe probe --fy 2026 --limit 300 --agency "Department of Transportation"
.\.venv\Scripts\fsi.exe bulk --start 2026-09-01 --end 2026-09-30 --agency "Department of Transportation"
```

## v0.2: Typed Silver transaction fact

Once a bulk export has `BULK_VERIFIED`, run:

```powershell
.\.venv\Scripts\fsi.exe silver --job ".\data\raw\bulk\2026-09-01_2026-09-30_20261105T144143Z\job.json"
```

This **does not** call the USAspending API or download data again. It re-runs
`verify` (including original ZIP SHA-256), reads ONLY `PrimeTransactions`,
and writes one typed Parquet file plus a machine-readable audit:

- `data/silver/jobs/<bulk-job-id>/fact_contract_transactions.parquet`
- `data/silver/jobs/<bulk-job-id>/silver_quality.json`

Silver uses `contract_transaction_unique_key` as grain; `award_id_piid` is
NOT a transaction key. `obligation_change_usd` is additive with the **signed**
transaction value; the award-level cumulative `total_dollars_obligated` and
`total_outlayed_amount_for_overall_award` are intentionally **excluded** from
this fact to prevent double counting. Financial amounts are stored in a high
precision DuckDB DECIMAL type, not binary float. It also adds fiscal year
(October-to-September), calendar month and a negative-obligation flag.

Quality gates:
- no null or duplicate transaction keys;
- date values inside requested period and source FY agrees with derived FY;
- non-null typed amounts, no lost rows, same count of negative transactions;
- **exact** sum of signed obligations matches Bronze;
- subaward rows are **never** merged into contract transactions.

The audit records the input ZIP hash, output Parquet hash, record counts,
positive/negative/net totals, agency and recipient counts. It does not
claim the month or agency is representative of all federal spending.

To intentionally rebuild the same Silver file after modifying transformations,
add `--overwrite` (the original Bronze ZIP stays unchanged).

**Current limitation:** v0.2 outputs a typed fact for one VERIFIED extraction.
Across-partition duplicate handling and FY2023–FY2026 all-agency orchestration
are deferred to v0.3. Re-running the same bulk job does not create a new
national data point. All raw/staging/silver files remain local and Git-ignored.

## Fix in v0.1.1: prime transactions vs subawards

The USAspending bulk ZIP can contain separate `Contracts_PrimeTransactions` and
`Contracts_Subawards` CSVs. The count preflight covers **prime transactions only**.
The original v0.1 Quality Gate compared that count with the **sum of both CSVs**.
For the real September 2026 DOT export, the first run reported 3,629 expected,
3,629 prime transaction rows and 104 subaward rows, incorrectly labeling it
`BULK_NEEDS_REVIEW`.

This release:
- classifies data by actual CSV/Parquet schema (not filename or row total);
- compares `prime_transaction_rows` against the count preflight;
- retains subawards as a separate dataset, never added to contract obligations;
- checks transaction-key uniqueness/nulls, date bounds and numeric obligations;
- verifies original ZIP SHA-256 and updates the persisted job manifest;
- can revalidate **existing data without network or re-downloading**.

From the project root, after installing this release with `pip install -e ".[dev]"`:

```powershell
.\.venv\Scripts\fsi.exe verify --job ".\data\raw\bulk\2026-09-01_2026-09-30_20261105T144143Z\job.json"
```

A successful run prints `BULK_VERIFIED prime=3,629 / expected=3,629;
subawards=104 (separate)` if the extended data-integrity checks also pass.

If bulk generation exceeds the wait window, it saves a `job.json` for resuming:

```powershell
fsi resume --job ".\data\raw\bulk\2026-09-01_2026-09-30_YYYYMMDDTHHMMSSZ\job.json"
```

Copy the actual printed path. **Do not launch a new bulk job to resume.**

### What does this code download?

- `probe` calls `POST /api/v2/search/spending_by_transaction/`, stores full response pages
  as raw JSON and a **capped, nonrepresentative** Parquet sample.
- `bulk` calls `POST /api/v2/download/count/` to check the row limit, then
  `POST /api/v2/download/transactions/` for an export job. It polls the
  `GET /api/v2/download/status?file_name=...` URL.
- When ready, it downloads the original compressed ZIP, checks ZIP CRC,
  converts CSV members to Parquet with all source columns **preserved as strings**
  at Bronze stage, and compares row counts with the preflight response.
- The **original ZIP and request metadata** are retained for reproducibility.
- No network calls are made by the offline tests.

### Data locations

| Folder | Content | In Git? |
|---|---|---|
| `data/raw/probe/` | Original API pages, JSONL, run manifest | No |
| `data/raw/bulk/` | Full CSV-in-ZIP download, SHA256, job manifest | No |
| `data/staging/probe/` | Structural probe Parquet | No |
| `data/staging/bulk/` | Contract transaction columns from official CSV in Parquet | No |
| `data/silver/jobs/` | Typed and audited transaction fact, one export at a time | No |
| `docs/` | Definition of grain, dates and controls | Yes |

## Important financial semantics

- `Transaction Amount` / `federal_action_obligation`: signed **change in obligated funds**,
  **not** money already paid, and **not** the total contract award amount.
- A negative amount is often a **de-obligation** or correction, not necessarily an error.
- Multiple transactions can share the same award ID. **Do not use award ID as
  the primary key for the transaction fact table.** The transaction-specific key
  from the full official CSV will be used in v0.2.
- The USAspending fiscal year is **October 1–September 30**.
  Example: FY2026 = 2025-10-01 to 2026-09-30.
- A probe sorted by transaction amount is heavily biased and is for schema checks only.
- For all-agency exports, use `--all-agencies` and **smaller partitions**. The
  tool will reject requests exceeding the official row cap. It never silently
  treats a truncated export as complete.

## Suggested expansion plan

1. **v0.1** Verify a live probe and one monthly bulk export (Department of Transportation).
2. **v0.2** Build typed Silver transaction fact, stable transaction key and DQ tests (**implemented**).
3. **v0.3** Collect FY2023–FY2026 by agency × month, reconcile totals.
4. **v0.4** Build Power BI star schema and DAX.
5. **v0.5+** Financial diagnostics, contractor concentration, anomalies.

## Sources

- https://api.usaspending.gov/docs/endpoints
- https://github.com/fedspendingtransparency/usaspending-api/blob/master/usaspending_api/api_contracts/contracts/v2/search/spending_by_transaction.md
- https://github.com/fedspendingtransparency/usaspending-api/blob/master/usaspending_api/api_contracts/contracts/v2/download/count.md
- https://github.com/fedspendingtransparency/usaspending-api/blob/master/usaspending_api/api_contracts/contracts/v2/download/transactions.md
- https://github.com/fedspendingtransparency/usaspending-api/blob/master/usaspending_api/api_contracts/contracts/v2/download/status.md

**Limitations:** The v0.2 Silver step was tested structurally and with offline unit tests.
A live integration run against your downloaded 3,629-row local Parquet requires
your workstation (the raw files are not available in this environment).
