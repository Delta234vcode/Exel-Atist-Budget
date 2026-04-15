#!/usr/bin/env python3
"""
Sync costs from internal Google Sheet to artist report Google Sheet.

What it does:
1. Reads rows from the source sheet.
2. Matches rows in target sheet by cost name (normalized text).
3. Copies amount and invoice link to target columns.
"""

from __future__ import annotations

import argparse
import re
from dataclasses import dataclass
import json
from typing import Dict, List, Optional, Tuple

import gspread
from google.oauth2.service_account import Credentials


SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive.readonly",
]


def normalize(text: str) -> str:
    """Normalize row labels for reliable matching."""
    text = (text or "").strip().lower()
    text = re.sub(r"\s+", " ", text)
    text = text.replace("ё", "е")
    return text


def parse_number(value: str) -> Optional[float]:
    """Parse numeric values like 1 234,56 or €1,234.50."""
    if value is None:
        return None
    raw = str(value).strip()
    if not raw:
        return None

    cleaned = re.sub(r"[^\d,.\-]", "", raw)
    if not cleaned:
        return None

    if "," in cleaned and "." in cleaned:
        # Use the last separator as decimal point.
        if cleaned.rfind(",") > cleaned.rfind("."):
            cleaned = cleaned.replace(".", "").replace(",", ".")
        else:
            cleaned = cleaned.replace(",", "")
    elif "," in cleaned and "." not in cleaned:
        cleaned = cleaned.replace(",", ".")

    try:
        return float(cleaned)
    except ValueError:
        return None


def discover_header_indexes(headers: List[str]) -> Dict[str, Optional[int]]:
    normalized = [normalize(h) for h in headers]

    def find(candidates: List[str]) -> Optional[int]:
        for i, h in enumerate(normalized):
            if h in candidates:
                return i
        for i, h in enumerate(normalized):
            for candidate in candidates:
                if candidate in h:
                    return i
        return None

    return {
        "name": find(["plan", "fee", "cost", "name", "статья", "витрата", "витрати"]),
        "amount": find(["fact eur", "fact eu", "fact", "amount", "netto", "sum", "сума"]),
        "link": find(["description", "invoice", "link", "посилання", "інвойс"]),
    }


def extract_sheet_id(sheet_url: str) -> str:
    match = re.search(r"/spreadsheets/d/([a-zA-Z0-9-_]+)", sheet_url)
    if not match:
        raise ValueError(f"Could not extract sheet id from URL: {sheet_url}")
    return match.group(1)


@dataclass
class SourceRow:
    name: str
    amount: Optional[float]
    link: Optional[str]


def get_source_data(ws: gspread.Worksheet, header_row: int) -> Dict[str, SourceRow]:
    values = ws.get_all_values()
    if len(values) < header_row:
        raise ValueError("Header row is outside the sheet range.")

    headers = values[header_row - 1]
    idx = discover_header_indexes(headers)

    name_idx = idx["name"] if idx["name"] is not None else 0
    amount_idx = idx["amount"] if idx["amount"] is not None else 1
    link_idx = idx["link"]

    result: Dict[str, SourceRow] = {}
    for row_number, row in enumerate(values[header_row:], start=header_row + 1):
        if name_idx >= len(row):
            continue
        name = row[name_idx].strip()
        if not name:
            continue

        amount_raw = row[amount_idx] if amount_idx < len(row) else ""
        amount = parse_number(amount_raw)
        if amount is None:
            continue

        link = None
        if link_idx is not None and link_idx < len(row):
            maybe_link = row[link_idx].strip()
            if "http://" in maybe_link or "https://" in maybe_link:
                link = maybe_link
            else:
                formula = ws.acell(
                    gspread.utils.rowcol_to_a1(row_number, link_idx + 1),
                    value_render_option="FORMULA",
                ).value
                if formula and "HYPERLINK" in formula.upper():
                    m = re.search(r'"(https?://[^"]+)"', formula)
                    if m:
                        link = m.group(1)

        result[normalize(name)] = SourceRow(name=name, amount=amount, link=link)
    return result


def format_amount_for_sheet(amount: float) -> str:
    return f"{amount:.2f}".rstrip("0").rstrip(".")


def sync(
    client: gspread.Client,
    source_url: str,
    target_url: str,
    source_sheet_name: Optional[str],
    target_sheet_name: Optional[str],
    source_header_row: int,
    target_header_row: int,
) -> Tuple[int, int]:
    source = client.open_by_key(extract_sheet_id(source_url))
    target = client.open_by_key(extract_sheet_id(target_url))

    source_ws = source.worksheet(source_sheet_name) if source_sheet_name else source.sheet1
    target_ws = target.worksheet(target_sheet_name) if target_sheet_name else target.sheet1

    source_map = get_source_data(source_ws, source_header_row)
    target_values = target_ws.get_all_values()
    if len(target_values) < target_header_row:
        raise ValueError("Target header row is outside the sheet range.")

    target_headers = target_values[target_header_row - 1]
    target_idx = discover_header_indexes(target_headers)
    target_name_idx = target_idx["name"] if target_idx["name"] is not None else 0
    target_amount_idx = target_idx["amount"] if target_idx["amount"] is not None else 2
    target_link_idx = target_idx["link"]

    updates: List[Tuple[int, int, str]] = []
    matched = 0

    for row_number, row in enumerate(target_values[target_header_row:], start=target_header_row + 1):
        if target_name_idx >= len(row):
            continue
        target_name = row[target_name_idx].strip()
        if not target_name:
            continue

        key = normalize(target_name)
        source_row = source_map.get(key)
        if not source_row:
            continue

        matched += 1
        updates.append((row_number, target_amount_idx + 1, format_amount_for_sheet(source_row.amount or 0)))

        if source_row.link and target_link_idx is not None:
            link_formula = f'=HYPERLINK("{source_row.link}","invoice")'
            updates.append((row_number, target_link_idx + 1, link_formula))

    for row, col, value in updates:
        target_ws.update_cell(row, col, value)

    return matched, len(updates)


def build_client(service_account_path: str) -> gspread.Client:
    creds = Credentials.from_service_account_file(service_account_path, scopes=SCOPES)
    return gspread.authorize(creds)


def build_client_from_info(service_account_info: Dict[str, str]) -> gspread.Client:
    creds = Credentials.from_service_account_info(service_account_info, scopes=SCOPES)
    return gspread.authorize(creds)


def parse_service_account_json(raw_json: str) -> Dict[str, str]:
    return json.loads(raw_json)


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Sync amount + invoice links from internal sheet to artist report sheet."
    )
    parser.add_argument("--source-url", required=True, help="Internal sheet URL")
    parser.add_argument("--target-url", required=True, help="Artist sheet URL")
    parser.add_argument("--service-account", required=True, help="Path to Google service account JSON")
    parser.add_argument("--source-sheet-name", default=None, help="Optional source tab name")
    parser.add_argument("--target-sheet-name", default=None, help="Optional target tab name")
    parser.add_argument("--source-header-row", type=int, default=9, help="Header row in source sheet")
    parser.add_argument("--target-header-row", type=int, default=9, help="Header row in target sheet")
    args = parser.parse_args()

    client = build_client(args.service_account)
    matched, updates = sync(
        client=client,
        source_url=args.source_url,
        target_url=args.target_url,
        source_sheet_name=args.source_sheet_name,
        target_sheet_name=args.target_sheet_name,
        source_header_row=args.source_header_row,
        target_header_row=args.target_header_row,
    )
    print(f"Done. Matched rows: {matched}. Updated cells: {updates}.")


if __name__ == "__main__":
    main()
