from __future__ import annotations

import re

_TOTAL_RE = re.compile(r"^Total record\(s\):\s*(\d+)\s*$", re.MULTILINE)


def _rmdb_total_records(text: str) -> int | None:
    match = _TOTAL_RE.search(text)
    if match is None:
        return None
    return int(match.group(1))


def parse_table_rows(text: str) -> list[list[str]]:
    rows: list[list[str]] = []
    total_records = _rmdb_total_records(text)
    for line in text.splitlines():
        stripped = line.strip()
        if (
            not stripped
            or stripped.startswith("+")
            or stripped.startswith("-")
            or stripped.startswith("Total record")
        ):
            continue
        if "|" in stripped:
            parts = [part.strip() for part in stripped.strip("|").split("|")]
            if parts and not all(part == "" for part in parts):
                rows.append(parts)
        elif "," in stripped:
            rows.append([part.strip() for part in stripped.split(",")])
        else:
            rows.append([stripped])
    if total_records is not None:
        if total_records == 0:
            return []
        return rows[1:]
    return rows


def scalar_text(text: str, default: str = "") -> str:
    rows = parse_table_rows(text)
    if not rows:
        return default
    return rows[-1][0]


def scalar_int(text: str, default: int = 0) -> int:
    match = re.search(r"-?\d+", scalar_text(text))
    if match:
        return int(match.group(0))
    return default


def scalar_float(text: str, default: float = 0.0) -> float:
    match = re.search(r"-?\d+(?:\.\d+)?", scalar_text(text))
    if match:
        return float(match.group(0))
    return default
