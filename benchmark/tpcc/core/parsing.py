from __future__ import annotations

import re


def parse_table_rows(text: str) -> list[list[str]]:
    rows: list[list[str]] = []
    for line in text.splitlines():
        stripped = line.strip()
        if not stripped or stripped.startswith("+") or stripped.startswith("-") or stripped.startswith("Total record"):
            continue
        if "|" in stripped:
            parts = [part.strip() for part in stripped.strip("|").split("|")]
            if parts and not all(part == "" for part in parts):
                rows.append(parts)
        elif "," in stripped:
            rows.append([part.strip() for part in stripped.split(",")])
        else:
            rows.append([stripped])
    return rows


def scalar_text(text: str, default: str = "") -> str:
    rows = parse_table_rows(text)
    if not rows:
        return default
    return rows[-1][0]


def scalar_int(text: str, default: int = 0) -> int:
    for match in re.finditer(r"-?\d+", text):
        return int(match.group(0))
    return default


def scalar_float(text: str, default: float = 0.0) -> float:
    for match in re.finditer(r"-?\d+(?:\.\d+)?", text):
        return float(match.group(0))
    return default

