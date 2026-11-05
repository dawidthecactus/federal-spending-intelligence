# Data contract v0.1

## Grain

- `probe`: one row of the transaction search response (identification **provisional**).
  `generated_internal_id` is an award-level identifier, not transaction-unique.
- `bulk`: one row from an official *transaction* export CSV. Exact transaction
  identifier(s) must be inspected in the first live CSV before designing the
  Silver primary key.
- The original CSV column names are **not renamed in Bronze**; no forced
  `unique()`, truncation, negative-value filtering or award-level aggregation.

## Source and extraction metadata

Every extraction stores filter JSON, time-window, download request, expected
count and/or sample cap, run timestamp, file paths and (for ZIP) SHA-256 hash.

## Precision and signs

- Preserve transaction amounts as signed values.
- Read raw currency as text in Bronze and cast to DECIMAL(24, 2) in Silver.
- Do not sum `Award Amount` values as transaction amounts.
- Outlays are conceptually distinct from obligations; they are not part of v0.1.
- US fiscal years: Oct 1 previous calendar year .. Sep 30 fiscal year.

## Required quality gates for v0.2

1. Row-count reconciliation between count preflight and prime transaction rows only, excluding subawards.
2. Nullability and uniqueness of **real transaction key** (not award key).
3. Action date inside the requested window.
4. Numeric-amount parsing, negative amounts retained.
5. Consistent agency / recipient IDs, NAICS and PSC codes left padded if needed.
6. Aggregate controls by agency × fiscal month to public benchmark endpoints.
7. No arithmetic on the `probe` as official totals.
8. Document refreshed/revised historical data and extraction timestamps.

## Fiscal scope

FY2023: 2022-10-01 .. 2023-09-30
FY2024: 2023-10-01 .. 2024-09-30
FY2025: 2024-10-01 .. 2025-09-30
FY2026: 2025-10-01 .. 2026-09-30

No loan, grant, or direct-payment facts are mixed with the contract-only model.
Separate account-level federal outlays will be added in a later phase.

## v0.1.1 Quality Gate clarification

The bulk ZIP may contain both `Contracts_PrimeTransactions` and
`Contracts_Subawards`. The downloaded rows must be classified by column
structure (transaction key versus subaward key); only prime transactions
are reconciled with the `/download/count/` preflight for transaction-level
spending. Subawards remain separately available for future subcontracting
analysis and **must not be added** to prime federal obligations.

## v0.2 Silver model (contract transactions only)

`fsi silver --job <verified-job.json>` outputs `fact_contract_transactions.parquet`.

- **Primary key:** `transaction_key` from `contract_transaction_unique_key`.
- **Award-level foreign key:** `award_key` from `contract_award_unique_key`.
- **Additive financial measure:** `obligation_change_usd`, signed `DECIMAL(38,10)`.
- **Dates:** `action_date` (`DATE`), `fiscal_year` computed using Oct 1 FY boundary.
- **Identifiers:** strings (agency IDs, NAICS, PSC, UEIs), preserving leading zeros.
- **Flags:** `is_deobligation`, `is_positive_obligation`.
- **Key dimensions (degenerate attributes for now):** awarding/funding agencies,
  recipient UEI and parent UEI, NAICS, PSC, performance geography.
- **Deliberately excluded non-additive snapshot fields:**
  `total_dollars_obligated`, `total_outlayed_amount_for_overall_award`.
- **Subawards:** never joined/summed with transaction obligations.

The Bronze file keeps all original columns; Silver keeps a stable, curated
subset. Enforce the verified Bronze gate before building Silver. Silver checks
row count, unique key count, date bounds, FY consistency, signed-amount sum,
negative count and non-null typed amounts. Silver jobs are kept separate until
cross-partition reconciliation is implemented in v0.3.

## v0.3 Multi-year acquisition notes

- Month partitions are **inclusive** and non-overlapping. If above the API cap,
  split into two inclusive child date windows (no shared boundary day).
- A completed campaign requires all leaf windows to be verified or confirmed empty.
- Campaign configuration includes full FY range and awarding-agency scope.
- The nationwide (`--all-agencies`) campaign overlaps the agency-specific campaign.
  Never aggregate these campaigns together.
- A verified existing monthly extraction can be adopted only when its date window,
  agency and Silver fact digest match its saved audit. It is not re-downloaded.
- Cross-partition transaction-key duplicates are a strict quality failure.
- Financial net amounts across incomplete campaigns are provisional.
- USAspending amounts are subject to later source updates; extract snapshots
  should be retained and source freshness documented.
