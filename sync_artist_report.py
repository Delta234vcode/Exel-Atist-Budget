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
from datetime import datetime
import difflib
import re
from dataclasses import dataclass
import json
import time
from typing import Any, Callable, Dict, List, Optional, Tuple, TypeVar

import gspread
from gspread.exceptions import APIError
from googleapiclient.discovery import build
from google.oauth2.service_account import Credentials


SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
]

T = TypeVar("T")

CATEGORY_HEADERS = {
    "fee",
    "advertisment",
    "advertisement",
    "accomodation",
    "accommodation",
    "transport",
    "food",
    "dressing room",
    "venue place",
    "services",
    "security",
    "staff",
    "ticketing service docs",
    "ticketing service",
    "vat tax",
    "techrider",
    "other costs",
}

SIMPLIFIED_COLUMNS_TO_KEEP = 5

SIMPLIFIED_ALLOWED_ROWS = {
    "city",
    "data",
    "venue",
    "artist",
    "selling total",
    "fee",
    "artist fee band",
    "advertisement",
    "advertisment",
    "meta targeting",
    "tik tok targeting",
    "tiktok targeting",
    "mticket targeting",
    "accommodation",
    "accomodation",
    "hotel band and artist",
    "hotel fca",
    "hotel",
    "transport",
    "artist transport",
    "fca",
    "food",
    "band and artist",
    "band",
    "dressing room",
    "catering",
    "venue place",
    "venue rent",
    "services",
    "other costs",
    "security",
    "staff",
    "marketing",
    "targeting",
    "technical director",
    "tour managers 2 people",
    "ticketing service docs",
    "ticket operator services",
    "vat tax",
    "vat 7",
    "techrider",
    "feyeria",
    "band stuf",
    "unexpected expenses",
    "бейджи",
    "таблички",
    "доставка",
    "author society service",
    "other",
    "total",
    "result",
    "balance",
    "profit artist 80",
    "profit fca 20",
}


def _is_rate_limited_error(exc: Exception) -> bool:
    if not isinstance(exc, APIError):
        return False
    status_code = getattr(getattr(exc, "response", None), "status_code", None)
    if status_code == 429:
        return True
    return "429" in str(exc)


def with_backoff(fn: Callable[[], T], retries: int = 5, base_delay: float = 1.0) -> T:
    """Retry Google API calls with exponential backoff on 429."""
    delay = base_delay
    last_exc: Optional[Exception] = None
    for attempt in range(retries + 1):
        try:
            return fn()
        except Exception as exc:  # noqa: BLE001
            last_exc = exc
            if not _is_rate_limited_error(exc) or attempt == retries:
                raise
            time.sleep(delay)
            delay *= 2
    assert last_exc is not None
    raise last_exc


def normalize(text: str) -> str:
    """Normalize row labels for reliable matching."""
    text = (text or "").strip().lower()
    text = text.replace("-", " ")
    text = text.replace("_", " ")
    # Remove punctuation to match labels like "tik-tok" vs "tik tok".
    text = re.sub(r"[^\w\s]", " ", text)
    text = re.sub(r"\s+", " ", text)
    text = text.replace("ё", "е")
    return text


def canonicalize(text: str) -> str:
    """
    Canonical form for fuzzy matching.
    Helps with minor naming differences across templates.
    """
    text = normalize(text)
    replacements = {
        "advertisment": "advertisement",
        "advertisments": "advertisements",
        "tik tok": "tiktok",
        "tik tok targeting": "tiktok targeting",
        "mticket": "m ticket",
        "ticket operator sevices": "ticket operator services",
    }
    for old, new in replacements.items():
        text = text.replace(old, new)
    return re.sub(r"\s+", " ", text).strip()


def is_category_header(text: str) -> bool:
    return canonicalize(text) in CATEGORY_HEADERS


def best_match_key(target_key: str, source_keys: List[str], threshold: float = 0.72) -> Optional[str]:
    """Find best fuzzy key match when exact match fails."""
    if not target_key:
        return None

    target_canon = canonicalize(target_key)
    best_key: Optional[str] = None
    best_score = 0.0

    for source_key in source_keys:
        source_canon = canonicalize(source_key)
        if not source_canon:
            continue
        score = difflib.SequenceMatcher(None, target_canon, source_canon).ratio()
        # Small boost for partial containment.
        if target_canon in source_canon or source_canon in target_canon:
            score += 0.08
        if score > best_score:
            best_score = score
            best_key = source_key

    if best_key and best_score >= threshold:
        return best_key
    return None


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


def is_probably_numeric(text: str) -> bool:
    return parse_number(text) is not None


def infer_name_column(
    values: List[List[str]], header_row: int, fallback_idx: int, sample_rows: int = 120
) -> int:
    """
    Choose the most likely "expense name" column by scanning content under headers.
    We prefer columns with many textual labels and few numeric cells.
    """
    if not values:
        return fallback_idx

    max_cols = max((len(r) for r in values), default=fallback_idx + 1)
    start = header_row
    end = min(len(values), header_row + sample_rows)
    best_idx = fallback_idx
    best_score = -1.0

    for col in range(max_cols):
        text_count = 0
        numeric_count = 0
        non_empty = 0
        for r in range(start, end):
            row = values[r]
            if col >= len(row):
                continue
            cell = (row[col] or "").strip()
            if not cell:
                continue
            non_empty += 1
            if is_probably_numeric(cell):
                numeric_count += 1
            else:
                text_count += 1

        if non_empty == 0:
            continue
        score = text_count - (numeric_count * 1.2)
        # Slight preference to the left-most columns (typical for row labels).
        score -= col * 0.05

        if score > best_score:
            best_score = score
            best_idx = col

    return best_idx


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

    result = {
        "name": find(["plan", "fee", "cost", "name", "статья", "витрата", "витрати"]),
        "amount": find(["fact eur", "fact eu", "fact", "amount", "netto", "sum", "сума"]),
        "link": find(["description", "invoice", "link", "посилання", "інвойс"]),
    }
    # Optional alternates for better amount extraction in mixed templates.
    result["amount_fact"] = find(["fact eur", "fact eu", "fact"])
    result["amount_brutto"] = find(["brutto", "gross"])
    result["amount_vat"] = find(["vat"])
    result["amount_netto"] = find(["netto", "net"])
    result["amount_sum"] = find(["sum", "сума", "amount"])
    return result


def has_amount_candidate(idx: Dict[str, Optional[int]]) -> bool:
    for key in ["amount_fact", "amount_brutto", "amount_vat", "amount_sum", "amount_netto", "amount"]:
        if idx.get(key) is not None:
            return True
    return False


def detect_header_row(
    values: List[List[str]], preferred_row: int, require_link: bool = False
) -> int:
    """
    Detect the best header row if preferred row looks invalid.
    Returns 1-indexed row number.
    """
    if values:
        if 1 <= preferred_row <= len(values):
            preferred_idx = discover_header_indexes(values[preferred_row - 1])
            if (
                preferred_idx.get("name") is not None
                and has_amount_candidate(preferred_idx)
                and (not require_link or preferred_idx.get("link") is not None)
            ):
                return preferred_row

    search_limit = min(len(values), 40)
    best_row = max(1, preferred_row)
    best_score = -1
    for row_number in range(1, search_limit + 1):
        idx = discover_header_indexes(values[row_number - 1])
        if idx.get("name") is None or not has_amount_candidate(idx):
            continue
        if require_link and idx.get("link") is None:
            continue

        score = 2
        if idx.get("link") is not None:
            score += 3
        # Prefer costs block headers over selling block headers.
        header_text = normalize(" ".join(values[row_number - 1]))
        if "plan costs" in header_text or "description" in header_text:
            score += 3
        if "netto" in header_text or "brutto" in header_text:
            score += 2
        # Slight bias towards preferred row neighborhood.
        score -= abs(row_number - preferred_row) * 0.05

        if score > best_score:
            best_score = score
            best_row = row_number

    return best_row


def extract_sheet_id(sheet_url: str) -> str:
    match = re.search(r"/spreadsheets/d/([a-zA-Z0-9-_]+)", sheet_url)
    if not match:
        raise ValueError(f"Could not extract sheet id from URL: {sheet_url}")
    return match.group(1)


def create_artist_sheet_from_source(
    client: gspread.Client,
    source_url: str,
    title_prefix: str = "Artist Report",
) -> str:
    """
    Create a new empty spreadsheet via Sheets API.
    Avoids Drive file-copy operations.
    """
    timestamp = datetime.utcnow().strftime("%Y-%m-%d %H-%M")
    sheets_service = build("sheets", "v4", credentials=client.auth, cache_discovery=False)
    created = with_backoff(
        lambda: sheets_service.spreadsheets()
        .create(body={"properties": {"title": f"{title_prefix} {timestamp}"}})
        .execute()
    )
    new_id = created.get("spreadsheetId")
    if not new_id:
        raise ValueError("Could not get new spreadsheet id after create.")

    return f"https://docs.google.com/spreadsheets/d/{new_id}/edit"


def list_city_sheets(client: gspread.Client, source_url: str) -> List[str]:
    spreadsheet = with_backoff(lambda: client.open_by_key(extract_sheet_id(source_url)))
    result: List[str] = []
    for ws in spreadsheet.worksheets():
        if is_city_sheet(ws):
            result.append(ws.title)
    return result


def is_city_sheet(ws: gspread.Worksheet) -> bool:
    preview = with_backoff(lambda: ws.get("A1:A8"))
    flattened = [normalize(row[0]) for row in preview if row]
    return "city" in flattened and "artist" in flattened


def _is_allowed_city_row(row_index: int, row: List[str]) -> bool:
    if row_index <= 7:
        return True
    title = canonicalize(row[0] if row else "")
    return title in SIMPLIFIED_ALLOWED_ROWS


def simplify_city_values(values: List[List[str]]) -> Tuple[List[List[str]], Dict[str, int]]:
    if not values:
        return [], {"rows_deleted": 0, "columns_deleted": 0}

    filtered_rows: List[List[str]] = []
    for idx, row in enumerate(values, start=1):
        if _is_allowed_city_row(idx, row):
            filtered_rows.append(row[:SIMPLIFIED_COLUMNS_TO_KEEP])

    max_cols_before = max((len(r) for r in values), default=0)
    max_cols_after = max((len(r) for r in filtered_rows), default=0)
    columns_deleted = max(0, max_cols_before - max_cols_after)

    stats = {
        "rows_deleted": max(0, len(values) - len(filtered_rows)),
        "columns_deleted": columns_deleted,
    }
    return filtered_rows, stats


def build_simplified_city_book(
    client: gspread.Client,
    source_url: str,
    selected_cities: Optional[List[str]] = None,
) -> Tuple[str, Dict[str, Any]]:
    source = with_backoff(lambda: client.open_by_key(extract_sheet_id(source_url)))
    selected_set = {c.strip() for c in (selected_cities or []) if c and c.strip()}
    all_cities = [ws.title for ws in source.worksheets() if is_city_sheet(ws)]
    cities_for_export = [title for title in all_cities if not selected_set or title in selected_set]

    if not cities_for_export:
        raise ValueError("No valid city sheets selected.")

    target_url = create_artist_sheet_from_source(
        client, source_url, title_prefix="Simplified Artist Report"
    )
    target = with_backoff(lambda: client.open_by_key(extract_sheet_id(target_url)))

    sheet_stats: Dict[str, Dict[str, int]] = {}
    source_sheets_by_title = {ws.title: ws for ws in source.worksheets()}
    created_tabs = 0
    for city_title in cities_for_export:
        source_ws = source_sheets_by_title.get(city_title)
        if not source_ws:
            continue
        source_values = with_backoff(lambda source_ws=source_ws: source_ws.get_all_values())
        simplified_values, stats = simplify_city_values(source_values)

        row_count = max(1, len(simplified_values))
        col_count = max(
            1, max((len(r) for r in simplified_values), default=SIMPLIFIED_COLUMNS_TO_KEEP)
        )
        city_ws = with_backoff(
            lambda city_title=city_title, row_count=row_count, col_count=col_count: target.add_worksheet(
                title=city_title,
                rows=row_count,
                cols=col_count,
            )
        )
        if simplified_values:
            with_backoff(
                lambda city_ws=city_ws, simplified_values=simplified_values: city_ws.update(
                    "A1", simplified_values
                )
            )
        sheet_stats[city_title] = stats
        created_tabs += 1

    for ws in list(target.worksheets()):
        if ws.title == "Sheet1":
            with_backoff(lambda ws=ws: target.del_worksheet(ws))

    debug = {
        "available_cities": all_cities,
        "selected_cities": cities_for_export,
        "created_tabs": created_tabs,
        "sheet_stats": sheet_stats,
    }
    return target_url, debug


@dataclass
class SourceRow:
    name: str
    amount: Optional[float]
    link: Optional[str]


def get_source_data(
    ws: gspread.Worksheet, header_row: int
) -> Tuple[Dict[str, SourceRow], int]:
    values = with_backoff(lambda: ws.get_all_values())
    formula_values = with_backoff(
        lambda: ws.get_all_values(value_render_option="FORMULA")
    )
    if not values:
        raise ValueError("Source sheet is empty.")
    header_row = detect_header_row(values, header_row, require_link=True)
    if len(values) < header_row:
        raise ValueError("Header row is outside the sheet range.")

    headers = values[header_row - 1]
    idx = discover_header_indexes(headers)

    name_idx = idx["name"] if idx["name"] is not None else 0
    name_idx = infer_name_column(values, header_row, name_idx)
    amount_candidates: List[int] = []
    for key in ["amount_fact", "amount_brutto", "amount_vat", "amount_sum", "amount_netto", "amount"]:
        col_idx = idx.get(key)
        if col_idx is not None and col_idx not in amount_candidates:
            amount_candidates.append(col_idx)
    if not amount_candidates:
        amount_candidates = [1]
    link_idx = idx["link"]

    result: Dict[str, SourceRow] = {}
    for row_number, row in enumerate(values[header_row:], start=header_row + 1):
        if name_idx >= len(row):
            continue
        name = row[name_idx].strip()
        if not name:
            continue

        amount = None
        for amount_idx in amount_candidates:
            amount_raw = row[amount_idx] if amount_idx < len(row) else ""
            amount = parse_number(amount_raw)
            # Prefer the first non-zero parsed value.
            if amount not in (None, 0):
                break
        if amount is None:
            amount = 0.0

        link = None
        if link_idx is not None and link_idx < len(row):
            maybe_link = row[link_idx].strip()
            if "http://" in maybe_link or "https://" in maybe_link:
                link = maybe_link
            else:
                formula_row = (
                    formula_values[row_number - 1]
                    if row_number - 1 < len(formula_values)
                    else []
                )
                formula = (
                    formula_row[link_idx]
                    if link_idx < len(formula_row)
                    else None
                )
                if isinstance(formula, str) and "HYPERLINK" in formula.upper():
                    m = re.search(r'"(https?://[^"]+)"', formula)
                    if m:
                        link = m.group(1)
        # Fallback: try finding a hyperlink formula in any cell of the row.
        if not link:
            formula_row = (
                formula_values[row_number - 1]
                if row_number - 1 < len(formula_values)
                else []
            )
            for formula in formula_row:
                if isinstance(formula, str) and "HYPERLINK" in formula.upper():
                    m = re.search(r'"(https?://[^"]+)"', formula)
                    if m:
                        link = m.group(1)
                        break

        result[normalize(name)] = SourceRow(name=name, amount=amount, link=link)
    return result, header_row


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
    category_filter: Optional[str] = None,
) -> Tuple[int, int, Dict[str, Any]]:
    source = with_backoff(lambda: client.open_by_key(extract_sheet_id(source_url)))
    target = with_backoff(lambda: client.open_by_key(extract_sheet_id(target_url)))

    source_ws = source.worksheet(source_sheet_name) if source_sheet_name else source.sheet1
    target_ws = target.worksheet(target_sheet_name) if target_sheet_name else target.sheet1

    source_map, detected_source_header_row = get_source_data(
        source_ws, source_header_row
    )
    target_values = with_backoff(lambda: target_ws.get_all_values())
    detected_target_header_row = detect_header_row(
        target_values, target_header_row, require_link=False
    )
    target_header_row = detected_target_header_row
    if len(target_values) < target_header_row:
        raise ValueError("Target header row is outside the sheet range.")

    target_headers = target_values[target_header_row - 1]
    target_idx = discover_header_indexes(target_headers)
    target_name_idx = target_idx["name"] if target_idx["name"] is not None else 0
    target_name_idx = infer_name_column(target_values, target_header_row, target_name_idx)
    target_amount_idx = target_idx["amount"] if target_idx["amount"] is not None else 2
    target_link_idx = target_idx["link"]

    updates: List[Tuple[int, int, str]] = []
    matched = 0
    source_keys = list(source_map.keys())
    unmatched_targets: List[str] = []
    category_filter_norm = canonicalize(category_filter or "")
    active_target_category = ""
    category_total = 0.0
    category_header_row: Optional[int] = None

    for row_number, row in enumerate(target_values[target_header_row:], start=target_header_row + 1):
        if target_name_idx >= len(row):
            continue
        target_name = row[target_name_idx].strip()
        if not target_name:
            continue

        target_name_norm = canonicalize(target_name)
        if is_category_header(target_name):
            active_target_category = target_name_norm
            if category_filter_norm and active_target_category == category_filter_norm:
                category_header_row = row_number

        if category_filter_norm and active_target_category != category_filter_norm:
            continue

        key = normalize(target_name)
        source_row = source_map.get(key)
        if not source_row:
            fuzzy_key = best_match_key(key, source_keys)
            if fuzzy_key:
                source_row = source_map.get(fuzzy_key)
        if not source_row:
            if len(unmatched_targets) < 20:
                unmatched_targets.append(target_name)
            continue

        matched += 1
        updates.append((row_number, target_amount_idx + 1, format_amount_for_sheet(source_row.amount or 0)))
        category_total += source_row.amount or 0.0

        if source_row.link and target_link_idx is not None:
            link_formula = f'=HYPERLINK("{source_row.link}","invoice")'
            updates.append((row_number, target_link_idx + 1, link_formula))

    if category_filter_norm and category_header_row is not None:
        updates.append((category_header_row, target_amount_idx + 1, format_amount_for_sheet(category_total)))

    if updates:
        payload: List[Dict[str, Any]] = []
        for row, col, value in updates:
            payload.append(
                {
                    "range": gspread.utils.rowcol_to_a1(row, col),
                    "values": [[value]],
                }
            )
        with_backoff(lambda: target_ws.batch_update(payload))

    debug = {
        "source_rows_loaded": len(source_map),
        "detected_source_header_row": detected_source_header_row,
        "source_keys_sample": list(source_map.keys())[:20],
        "category_filter": category_filter,
        "target_rows_total": max(0, len(target_values) - target_header_row),
        "detected_target_header_row": detected_target_header_row,
        "target_name_column_index": target_name_idx,
        "target_amount_column_index": target_amount_idx,
        "unmatched_targets_sample": unmatched_targets,
    }
    return matched, len(updates), debug


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
    parser.add_argument("--source-header-row", type=int, default=15, help="Header row in source sheet")
    parser.add_argument("--target-header-row", type=int, default=9, help="Header row in target sheet")
    args = parser.parse_args()

    client = build_client(args.service_account)
    matched, updates, debug = sync(
        client=client,
        source_url=args.source_url,
        target_url=args.target_url,
        source_sheet_name=args.source_sheet_name,
        target_sheet_name=args.target_sheet_name,
        source_header_row=args.source_header_row,
        target_header_row=args.target_header_row,
    )
    print(f"Done. Matched rows: {matched}. Updated cells: {updates}. Debug: {debug}")


if __name__ == "__main__":
    main()
