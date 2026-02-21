#!/usr/bin/env python3
"""Validate local demo CSV inputs for required columns and basic row counts."""

from __future__ import annotations

import csv
import sys
from pathlib import Path

BASE = Path(__file__).resolve().parents[1] / "data" / "sample_inputs"

EXPECTED = {
    "patient_screenings.csv": [
        "patient_id",
        "site_id",
        "screening_date",
        "age",
        "sex",
        "consent_flag",
    ],
    "lab_results.csv": ["patient_id", "site_id", "lab_date", "biomarker_a", "biomarker_b"],
    "site_reference.csv": ["site_id", "site_name", "city", "state"],
}


def validate_file(name: str, expected_cols: list[str]) -> tuple[bool, str]:
    path = BASE / name
    if not path.exists():
        return False, f"Missing file: {path}"

    with path.open(newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        if reader.fieldnames != expected_cols:
            return False, f"{name}: columns {reader.fieldnames} do not match expected {expected_cols}"
        rows = list(reader)

    if len(rows) < 1:
        return False, f"{name}: file has no data rows"

    return True, f"{name}: OK ({len(rows)} rows)"


def main() -> int:
    all_ok = True
    for file_name, columns in EXPECTED.items():
        ok, message = validate_file(file_name, columns)
        print(message)
        all_ok = all_ok and ok

    return 0 if all_ok else 1


if __name__ == "__main__":
    sys.exit(main())
