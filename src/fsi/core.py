"""Definitions and data contracts shared by acquisition workflows."""
from __future__ import annotations

import calendar
import hashlib
import json
from datetime import date, datetime, timezone
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any

CONTRACT_TYPES = ("A", "B", "C", "D")
SEARCH_FIELDS = (
    "Award ID", "Mod", "Recipient Name", "Recipient UEI", "recipient_id",
    "Action Date", "Action Type", "Transaction Amount",
    "Awarding Agency", "Awarding Sub Agency", "Funding Agency", "Funding Sub Agency",
    "Award Type", "NAICS", "PSC", "generated_internal_id", "internal_id",
    "Transaction Description",
)


def utc_stamp() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def fiscal_year(d: date) -> int:
    return d.year + (d.month >= 10)


def fiscal_year_range(fy: int) -> tuple[str, str]:
    if not 2008 <= fy <= 2100:
        raise ValueError("Fiscal year outside supported API search period")
    return f"{fy - 1}-10-01", f"{fy}-09-30"


def date_window(start: str, end: str) -> tuple[date, date]:
    a, b = date.fromisoformat(start), date.fromisoformat(end)
    if a > b:
        raise ValueError("Start date must be earlier than or equal to end date")
    if a < date(2007, 10, 1):
        raise ValueError("Search downloads available only from 2007-10-01")
    if b > date.today():
        raise ValueError("End date must not be in the future")
    return a, b


def build_filters(start: str, end: str, agency: str | None = None) -> dict[str, Any]:
    date_window(start, end)
    filters: dict[str, Any] = {
        "award_type_codes": list(CONTRACT_TYPES),
        "time_period": [{"start_date": start, "end_date": end, "date_type": "action_date"}],
    }
    if agency:
        filters["agencies"] = [{
            "type": "awarding", "tier": "toptier", "name": agency
        }]
    return filters


def month_windows(fy: int) -> list[tuple[str, str]]:
    """Twelve calendar months that constitute a US fiscal year."""
    fiscal_year_range(fy)
    windows = []
    for i in range(12):
        month = ((10 + i - 1) % 12) + 1
        year = fy - 1 if month >= 10 else fy
        start = date(year, month, 1)
        end = date(year, month, calendar.monthrange(year, month)[1])
        windows.append((start.isoformat(), end.isoformat()))
    return windows


def safe_amount(value: Any) -> str | None:
    if value is None or value == "":
        return None
    try:
        return str(Decimal(str(value)).quantize(Decimal("0.01")))
    except (InvalidOperation, ValueError):
        return None


def stringify(value: Any) -> str | None:
    if value is None:
        return None
    if isinstance(value, (dict, list)):
        return json.dumps(value, ensure_ascii=False, sort_keys=True)
    return str(value)


def normalize_probe(row: dict[str, Any]) -> dict[str, Any]:
    action_date = row.get("Action Date")
    try:
        fy = fiscal_year(date.fromisoformat(action_date))
    except (ValueError, TypeError):
        fy = None
    normalized = {
        "award_id": stringify(row.get("Award ID")),
        "generated_award_id": stringify(row.get("generated_internal_id")),
        "internal_award_id": stringify(row.get("internal_id")),
        "modification_number": stringify(row.get("Mod")),
        "action_date": action_date,
        "fiscal_year": fy,
        "action_type": stringify(row.get("Action Type")),
        "transaction_amount_usd": safe_amount(row.get("Transaction Amount")),
        "recipient_id": stringify(row.get("recipient_id")),
        "recipient_name": stringify(row.get("Recipient Name")),
        "recipient_uei": stringify(row.get("Recipient UEI")),
        "awarding_agency": stringify(row.get("Awarding Agency")),
        "awarding_subagency": stringify(row.get("Awarding Sub Agency")),
        "funding_agency": stringify(row.get("Funding Agency")),
        "funding_subagency": stringify(row.get("Funding Sub Agency")),
        "award_type": stringify(row.get("Award Type")),
        "naics": stringify(row.get("NAICS")),
        "psc": stringify(row.get("PSC")),
        "transaction_description": stringify(row.get("Transaction Description")),
    }
    # Exact-row diagnostic fingerprint. NOT a guaranteed real transaction ID.
    blob = json.dumps(row, sort_keys=True, ensure_ascii=False, default=str)
    normalized["row_fingerprint"] = hashlib.sha256(blob.encode("utf-8")).hexdigest()
    return normalized


def write_json(path: Path, obj: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(obj, ensure_ascii=False, indent=2, default=str) + "\n",
        encoding="utf-8",
    )
