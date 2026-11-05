# Gold v0.4: Power BI semantic model

## Scope and grain

- **Campaign**: FY2023–FY2026, Department of Transportation (DOT) by default.
- **Fact grain**: one row per **contract_transaction_unique_key**. Source is the
  already-audited, deduplicated `obligation_change_usd` in Silver.
- **Money**: signed *contract obligation change*, NOT federal outlay and NOT
  cumulative award amount. Retain negative and zero rows.
- **Not national**: the DOT campaign cannot compare multiple top-level agencies.
  A nationwide campaign will be a *separate*, overlapping scope, never unioned
  with the DOT dataset.
- **Traceability**: hashes of every Silver source and generated Gold Parquet are
  included in `gold_quality.json`. A Gold run refuses partial harvests by default.

## Output location

`data/marts/gold/fy2023-fy2026_department-of-transportation-7fb31658/`

| Parquet | Primary key / foreign key | Meaning |
|---|---|---|
| `fact_contract_transactions.parquet` | `transaction_key` (PK) | Signed obligations and dimension FKs |
| `dim_date.parquet` | `date` (PK) | Complete calendar / US fiscal-year dates |
| `dim_agency.parquet` | `agency_key` (PK) | Awarding/funding agency codes, role-playing dimension |
| `dim_recipient.parquet` | `recipient_key` (PK) | UEI-based contractor identification |
| `dim_naics.parquet` | `naics_key` (PK) | NAICS industry code |
| `dim_psc.parquet` | `psc_key` (PK) | Product/service category |

All missing dimension keys are explicit `__MISSING__`. Supplier names can vary
per UEI over time; v0.4 deliberately selects deterministic display labels and
records `observed_name_variants` to highlight possible renames. It **does not**
make different UEIs the same corporate group. That needs a separate normalization
workstream.

## Run (offline; requires already downloaded FY campaigns)

```powershell
.\.venv\Scripts\fsi.exe harvest-report --fy-start 2023 --fy-end 2026
.\.venv\Scripts\fsi.exe gold --fy-start 2023 --fy-end 2026
.\.venv\Scripts\fsi.exe gold-report --fy-start 2023 --fy-end 2026
```

If the campaign is incomplete, the `gold` command **fails closed**. For
exploratory output only, `gold --allow-partial` marks the manifest `partial`,
which should never be published as a full-year dashboard. Re-running on the
same output requires explicit `--overwrite`.

## Power BI relationships

Use **Import** (Power BI -> Parquet connector) and single-direction filters:

- `DimDate[date] (1) → FactContractTransactions[action_date] (*)` **active**.
- `DimAgency[agency_key] (1) → FactContractTransactions[awarding_agency_key] (*)`
  **active**.
- `DimAgency[agency_key] (1) → FactContractTransactions[funding_agency_key] (*)`
  **inactive**. Use `USERELATIONSHIP` for funding-agency-specific measures.
- `DimRecipient[recipient_key] (1) → FactContractTransactions[recipient_key] (*)`
  active.
- `DimNAICS[naics_key] (1) → FactContractTransactions[naics_key] (*)` active.
- `DimPSC[psc_key] (1) → FactContractTransactions[psc_key] (*)` active.

Mark `DimDate` as the **date table** using `[date]`, hide technical FK and PK
fields in report view, and use consistent USD number formats. Avoid bidirectional
relationships and using `award_key` as the fact primary key.

## Example DAX measures

```dax
Obligations Net =
SUM ( FactContractTransactions[obligation_change_usd] )

Obligations Positive =
CALCULATE (
    [Obligations Net],
    FactContractTransactions[obligation_change_usd] > 0
)

Deobligations (absolute) =
- CALCULATE (
    [Obligations Net],
    FactContractTransactions[obligation_change_usd] < 0
)

Contract Actions =
COUNTROWS ( FactContractTransactions )

Distinct Awards =
DISTINCTCOUNT ( FactContractTransactions[award_key] )

Contractors with UEI =
CALCULATE (
    DISTINCTCOUNT ( FactContractTransactions[recipient_key] ),
    FactContractTransactions[recipient_key] <> "__MISSING__"
)

Obligations FY YTD =
CALCULATE ( [Obligations Net], DATESYTD ( DimDate[date], "9/30" ) )

Obligations Previous Year =
CALCULATE ( [Obligations Net], DATEADD ( DimDate[date], -1, YEAR ) )

Obligations YoY % =
DIVIDE ( [Obligations Net] - [Obligations Previous Year],
         [Obligations Previous Year] )

Funding Agency Obligations =
CALCULATE (
    [Obligations Net],
    USERELATIONSHIP ( DimAgency[agency_key],
                     FactContractTransactions[funding_agency_key] )
)
```

IMPORTANT: `Funding Agency Obligations` changes the active role of agency
filtering inside that measure; check the result when multiple agency filters
exist. FY2023–FY2026 includes four complete US fiscal years: FY begins Oct 1.

## Publish gate

- `harvest-report` returns `complete`, with 0 errors / 0 incomplete partitions.
- `gold` returns `GOLD_VERIFIED` and `coverage=complete`.
- All Gold Parquet SHA-256 hashes verify with `gold-report`.
- Never sum `Subawards` with prime transaction obligations.
- Do not claim national coverage from the DOT-only pilot.
