"""Salary parsing helpers shared by the speed-layer salary normalization."""

from __future__ import annotations

import re
from dataclasses import dataclass


NEGOTIABLE_PATTERNS = ("thỏa thuận", "thoả thuận", "negotiable", "cạnh tranh")


@dataclass(frozen=True)
class ParsedSalary:
    salary_min_million: float | None
    salary_max_million: float | None
    salary_avg_million: float | None
    salary_bin: str
    currency: str


def parse_salary_text(raw_salary: str | None) -> ParsedSalary:
    """Parse common Vietnamese job salary text into million-unit fields."""

    text = (raw_salary or "").strip().lower()
    if not text:
        return ParsedSalary(None, None, None, "unknown", "VND")

    currency = "USD" if "usd" in text else "VND"
    if any(pattern in text for pattern in NEGOTIABLE_PATTERNS):
        return ParsedSalary(None, None, None, "negotiable", currency)

    numbers = [float(match.replace(",", ".")) for match in re.findall(r"\d+(?:[\.,]\d+)?", text)]
    if not numbers:
        return ParsedSalary(None, None, None, "unknown", currency)

    salary_min = numbers[0]
    salary_max = numbers[1] if len(numbers) > 1 else numbers[0]
    salary_avg = (salary_min + salary_max) / 2

    if salary_avg < 10:
        salary_bin = "under_10m"
    elif salary_avg < 20:
        salary_bin = "10_20m"
    elif salary_avg < 30:
        salary_bin = "20_30m"
    elif salary_avg < 50:
        salary_bin = "30_50m"
    else:
        salary_bin = "over_50m"

    return ParsedSalary(salary_min, salary_max, salary_avg, salary_bin, currency)
