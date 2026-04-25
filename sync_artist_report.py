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
from google.oauth2.service_account import Credentials


SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
]

T = TypeVar("T")

ARTIST_LAYOUT_ROWS: List[Tuple[int, str, List[str]]] = [
    (10, "Artist fee band", ["artist fee band", "artist fee band 2"]),
    (17, "Meta Targeting", ["fca targeting", "meta targeting"]),
    (18, "Tik-Tok targeting", ["tik tok", "tik tok targeting", "tiktok targeting"]),
    (19, "Production videos and visual", ["production videos and visual"]),
    (20, "Instagram publics", ["instagram publics", "ads sofiya"]),
    (21, "Mticket targeting", ["mticket", "mticket targeting"]),
    (23, "Posters 500 p.", ["posters 500 p"]),
    (27, "hotel band and artist", ["hotel band and artist", "hotel"]),
    (28, "hotel FCA", ["hotel fca"]),
    (30, "artist transport", ["artist transport"]),
    (31, "band transport", ["band transport"]),
    (32, "FCA", ["avia fca", "avia", "local", "avia org", "bus"]),
    (34, "band and artist", ["band", "nosov"]),
    (35, "FCA", ["fca"]),
    (37, "Catering", ["catering"]),
    (38, "Other costs", ["other costs"]),
    (40, "venue rent", ["venue rent", "venue rent 2"]),
    (42, "services", ["services"]),
    (43, "other costs", ["other services"]),
    (45, "security", ["security"]),
    (47, "marketing", ["marketing"]),
    (48, "targeting", ["targeting"]),
    (49, "technical director", ["technical director"]),
    (50, "designer", ["designer"]),
    (51, "admin", ["admin"]),
    (52, "tour managers 2 people", ["tour managers 2 people", "tour managers 2 peple", "tour manager"]),
    (54, "ticket operator services", ["selling mticket", "selling artist production", "selling kontramarka de", "selling kartina"]),
    (56, "VAT 7%", ["vat difference", "author society 6"]),
    (58, "Feyeria", ["techrider artist", "feyeria"]),
    (59, "Band stuff", ["band stuff"]),
    (61, "unexpected expenses", ["unexpected expenses"]),
    (62, "бейджи", ["бейджи"]),
    (63, "таблички", ["таблички"]),
    (64, "доставка", ["доставка"]),
    (65, "Author society service", ["author society service"]),
    (66, "other", ["other"]),
]

ARTIST_STATIC_ROWS: Dict[int, List[Any]] = {
    7: ["", "PLAN EU", "FACT EU", "AMOUNT", "EXCHANGE"],
    9: ["FEE", "PLAN EUR", "FACT EUR", "Description", ""],
    11: ["Advertisment", "", "", "=SUM(C13:C25)", ""],
    12: ["Print", "", "", "", ""],
    13: ["", 0, 0, "", ""],
    14: ["", 0, 0, "", ""],
    15: ["", 0, 0, "", ""],
    16: ["Digital", "", "", "", ""],
    22: ["Outdoor", "", "", "", ""],
    24: ["", 0, 0, "", ""],
    25: ["", 0, 0, "", ""],
    26: ["Accomodation", "", "", "=SUM(C27:C28)", ""],
    29: ["Transport", "", "", "=SUM(C30:C32)", ""],
    33: ["Food", "", "", "=SUM(C34:C35)", ""],
    36: ["Dressing Room", "", "", "=SUM(C37:C38)", ""],
    39: ["Venue place", "", "", "=C40", ""],
    41: ["Services", "", "", "=SUM(C42:C43)", ""],
    44: ["Security", "", "", "=SUM(C45)", ""],
    46: ["Staff", "", "", "=SUM(C47:C52)", ""],
    53: ["Ticketing service & docs", "", "", "=SUM(C54)", ""],
    55: ["VAT & TAX", "", "", "=SUM(C56)", ""],
    57: ["Techrider", "", "", "=SUM(C58:C59)", ""],
    60: ["Other costs", "", "", "=SUM(C61:C66)", ""],
    67: ["Total:", "=SUM(B10:B66)", "=SUM(C10:C66)", "", ""],
    69: ["RESULT", "PLAN EUR", "FACT EUR", "", ""],
    70: ["Balance", "=B8-B67", "=C8-C67", "", ""],
    71: ["Profit Artist (80%)", "=B70*80%", "=C70*80%", "", ""],
    72: ["Profit FCA (20%)", "=B70-B71", "=C70-C71", "", ""],
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
    Create a new artist spreadsheet by copying source sheet file.
    This preserves original formatting/design as-is.
    """
    source_id = extract_sheet_id(source_url)
    timestamp = datetime.utcnow().strftime("%Y-%m-%d %H-%M")
    copied = with_backoff(
        lambda: client.copy(
            source_id,
            title=f"{title_prefix} {timestamp}",
            copy_permissions=False,
        )
    )

    if hasattr(copied, "id"):
        new_id = copied.id
    elif isinstance(copied, dict) and copied.get("id"):
        new_id = copied["id"]
    else:
        raise ValueError("Could not get new spreadsheet id after copy.")

    return f"https://docs.google.com/spreadsheets/d/{new_id}/edit"


def is_city_sheet(ws: gspread.Worksheet) -> bool:
    preview = with_backoff(lambda: ws.get("A1:A8"))
    flattened = [normalize(row[0]) for row in preview if row]
    return "city" in flattened and "artist" in flattened


def list_city_sheets(client: gspread.Client, source_url: str) -> List[str]:
    spreadsheet = with_backoff(lambda: client.open_by_key(extract_sheet_id(source_url)))
    return [ws.title for ws in spreadsheet.worksheets() if is_city_sheet(ws)]


def get_first_column_hyperlinks(ws: gspread.Worksheet) -> Dict[int, str]:
    """Read rich-text/plain hyperlinks from column A via Sheets grid data."""
    links: Dict[int, str] = {}
    try:
        metadata = with_backoff(
            lambda: ws.spreadsheet.fetch_sheet_metadata(
                params={
                    "includeGridData": "true",
                    "ranges": f"'{ws.title}'!A:A",
                }
            )
        )
    except Exception:
        return links

    for sheet in metadata.get("sheets", []):
        for data in sheet.get("data", []):
            for row_idx, row_data in enumerate(data.get("rowData", []), start=1):
                values = row_data.get("values", [])
                if not values:
                    continue
                cell = values[0]
                link = cell.get("hyperlink")
                if not link:
                    link = (
                        cell.get("userEnteredFormat", {})
                        .get("textFormat", {})
                        .get("link", {})
                        .get("uri")
                    )
                if not link:
                    for run in cell.get("textFormatRuns", []):
                        link = run.get("format", {}).get("link", {}).get("uri")
                        if link:
                            break
                if link:
                    links[row_idx] = link
    return links


def hyperlink_formula(label: str, link: Optional[str]) -> str:
    if not link:
        return label
    safe_link = link.replace('"', '""')
    safe_label = label.replace('"', '""')
    return f'=HYPERLINK("{safe_link}","{safe_label}")'


def resolve_source_sheet_names(
    client: gspread.Client,
    source_url: str,
    selected_cities: Optional[List[str]],
    source_sheet_name: Optional[str],
) -> Tuple[Optional[List[str]], Optional[str]]:
    """
    Returns (multi_sheet_titles, single_sheet_name) for sync().
    If selected_cities is None: legacy — use single_sheet_name (may be None → first sheet).
    If selected_cities is a list (including []): use exactly those tab titles (UI sends
    all names when user chose «вибрати всі»). Empty list is invalid.
    """
    if selected_cities is None:
        return None, source_sheet_name
    titles = [str(t).strip() for t in selected_cities if str(t).strip()]
    if not titles:
        raise ValueError("Обери хоча б одне місто або натисни «Вибрати всі».")
    return titles, None


@dataclass
class SourceRow:
    name: str
    amount: Optional[float]
    link: Optional[str]
    description: Optional[str] = None


def merge_source_row_maps(parts: List[Dict[str, SourceRow]]) -> Dict[str, SourceRow]:
    """Merge rows from several source tabs; same cost name → sum amounts, keep first link."""
    out: Dict[str, SourceRow] = {}
    for part in parts:
        for key, row in part.items():
            if key not in out:
                out[key] = SourceRow(
                    name=row.name,
                    amount=row.amount,
                    link=row.link,
                    description=row.description,
                )
            else:
                prev = out[key]
                amt = (prev.amount or 0.0) + (row.amount or 0.0)
                link = prev.link or row.link
                description = prev.description or row.description
                out[key] = SourceRow(
                    name=prev.name,
                    amount=amt,
                    link=link,
                    description=description,
                )
    return out


def get_source_data(
    ws: gspread.Worksheet, header_row: int
) -> Tuple[Dict[str, SourceRow], int]:
    values = with_backoff(lambda: ws.get_all_values())
    formula_values = with_backoff(
        lambda: ws.get_all_values(value_render_option="FORMULA")
    )
    rich_links = get_first_column_hyperlinks(ws)
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

        link = rich_links.get(row_number)
        description = None
        if link_idx is not None and link_idx < len(row):
            maybe_link = row[link_idx].strip()
            if "http://" in maybe_link or "https://" in maybe_link:
                link = link or maybe_link
            else:
                description = maybe_link or None
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
                        link = link or m.group(1)
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

        result[normalize(name)] = SourceRow(
            name=name,
            amount=amount,
            link=link,
            description=description,
        )
    return result, header_row


def format_amount_for_sheet(amount: float) -> str:
    return f"{amount:.2f}".rstrip("0").rstrip(".")


def _cell_value(row: List[str], idx: int) -> str:
    return row[idx].strip() if idx < len(row) and row[idx] is not None else ""


def _combine_display_values(values: List[str]) -> Any:
    cleaned = [v for v in values if str(v).strip()]
    if not cleaned:
        return 0
    if any(str(v).strip().upper() in {"#REF!", "#VALUE!", "#ERROR!"} for v in cleaned):
        return "#REF!"

    parsed = [parse_number(str(v)) for v in cleaned]
    if all(v is not None for v in parsed):
        total = sum(v or 0 for v in parsed)
        return format_amount_for_sheet(total)
    if len(cleaned) == 1:
        return cleaned[0]
    return " + ".join(cleaned)


def _build_source_rows_by_key(
    values: List[List[str]], hyperlinks: Dict[int, str]
) -> Dict[str, List[Dict[str, Any]]]:
    by_key: Dict[str, List[Dict[str, Any]]] = {}
    for row_number, row in enumerate(values, start=1):
        name = _cell_value(row, 0)
        if not name:
            continue
        key = normalize(name)
        by_key.setdefault(key, []).append(
            {
                "row": row_number,
                "name": name,
                "plan": _cell_value(row, 1),
                "fact": _cell_value(row, 4) or _cell_value(row, 2),
                "description": _cell_value(row, 5),
                "link": hyperlinks.get(row_number),
            }
        )
    return by_key


def _aggregate_artist_row(
    by_key: Dict[str, List[Dict[str, Any]]], aliases: List[str]
) -> Tuple[Any, Any, str, Optional[str]]:
    plan_values: List[str] = []
    fact_values: List[str] = []
    description = ""
    link: Optional[str] = None

    for alias in aliases:
        for row in by_key.get(normalize(alias), []):
            plan_values.append(str(row.get("plan") or ""))
            fact_values.append(str(row.get("fact") or ""))
            if not description and row.get("description"):
                description = str(row["description"])
            if not link and row.get("link"):
                link = str(row["link"])

    return (
        _combine_display_values(plan_values),
        _combine_display_values(fact_values),
        description,
        link,
    )


def transform_worksheet_to_artist_layout(ws: gspread.Worksheet) -> Dict[str, Any]:
    """
    Convert a copied source city worksheet into the compact artist-facing layout.
    This intentionally removes source-only columns/rows and keeps hyperlinks on row labels.
    """
    values = with_backoff(lambda: ws.get_all_values())
    hyperlinks = get_first_column_hyperlinks(ws)
    by_key = _build_source_rows_by_key(values, hyperlinks)

    def source(row: int, col: int) -> Any:
        if row - 1 < len(values):
            return _cell_value(values[row - 1], col - 1)
        return ""

    rows: List[List[Any]] = [["", "", "", "", ""] for _ in range(72)]
    rows[2] = ["City", source(3, 2), "", "", ""]
    rows[3] = ["Data", source(4, 2), "", "", ""]
    rows[4] = ["Venue", source(5, 2), "", "", ""]
    rows[5] = ["Artist", source(6, 2), "", "", ""]
    rows[7] = [
        hyperlink_formula("Selling total", hyperlinks.get(14) or hyperlinks.get(8)),
        source(14, 2),
        source(14, 3),
        source(14, 4),
        0,
    ]

    for row_number, row_values in ARTIST_STATIC_ROWS.items():
        rows[row_number - 1] = row_values

    for row_number, label, aliases in ARTIST_LAYOUT_ROWS:
        plan, fact, description, link = _aggregate_artist_row(by_key, aliases)
        if row_number == 54:
            plan = "=B8*15%"
            fact = "=C8*15%"
        elif row_number == 56:
            plan = "=B8*7%"
            fact = "=C8*7%"
        rows[row_number - 1] = [
            hyperlink_formula(label, link),
            plan,
            fact,
            description,
            "",
        ]

    with_backoff(lambda: ws.update("A1:E72", rows, value_input_option="USER_ENTERED"))
    if getattr(ws, "col_count", 0) > 5:
        with_backoff(lambda: ws.delete_columns(6, ws.col_count))
    if getattr(ws, "row_count", 0) > 72:
        with_backoff(lambda: ws.delete_rows(73, ws.row_count))

    return {
        "sheet": ws.title,
        "rows_written": 72,
        "columns_kept": 5,
        "source_rows_seen": len(values),
    }


def transform_artist_report_layout(
    client: gspread.Client,
    target_url: str,
    sheet_names: Optional[List[str]] = None,
    target_sheet_name: Optional[str] = None,
    remove_other_sheets: bool = False,
) -> Dict[str, Any]:
    target = with_backoff(lambda: client.open_by_key(extract_sheet_id(target_url)))
    if sheet_names:
        worksheets = [target.worksheet(title) for title in sheet_names]
        if remove_other_sheets:
            keep = set(sheet_names)
            for ws in list(target.worksheets()):
                if ws.title not in keep and len(target.worksheets()) > 1:
                    with_backoff(lambda ws=ws: target.del_worksheet(ws))
    else:
        worksheets = [target.worksheet(target_sheet_name)] if target_sheet_name else [target.sheet1]

    stats = []
    for ws in worksheets:
        stats.append(transform_worksheet_to_artist_layout(ws))
    return {"transformed_sheets": stats}


def sync(
    client: gspread.Client,
    source_url: str,
    target_url: str,
    source_sheet_name: Optional[str],
    target_sheet_name: Optional[str],
    source_header_row: int,
    target_header_row: int,
    source_sheet_names: Optional[List[str]] = None,
) -> Tuple[int, int, Dict[str, Any]]:
    source = with_backoff(lambda: client.open_by_key(extract_sheet_id(source_url)))
    target = with_backoff(lambda: client.open_by_key(extract_sheet_id(target_url)))

    target_ws = target.worksheet(target_sheet_name) if target_sheet_name else target.sheet1

    per_sheet_headers: List[Tuple[str, int]] = []
    if source_sheet_names:
        maps: List[Dict[str, SourceRow]] = []
        for title in source_sheet_names:
            ws = source.worksheet(title)
            smap, det = get_source_data(ws, source_header_row)
            maps.append(smap)
            per_sheet_headers.append((title, det))
        source_map = merge_source_row_maps(maps)
        detected_source_header_row = per_sheet_headers[0][1] if per_sheet_headers else source_header_row
        source_sheets_used = list(source_sheet_names)
    else:
        source_ws = source.worksheet(source_sheet_name) if source_sheet_name else source.sheet1
        source_map, detected_source_header_row = get_source_data(
            source_ws, source_header_row
        )
        source_sheets_used = [source_ws.title]
        per_sheet_headers = [(source_ws.title, detected_source_header_row)]

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

    for row_number, row in enumerate(target_values[target_header_row:], start=target_header_row + 1):
        if target_name_idx >= len(row):
            continue
        target_name = row[target_name_idx].strip()
        if not target_name:
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

        if source_row.link and target_link_idx is not None:
            link_formula = f'=HYPERLINK("{source_row.link}","invoice")'
            updates.append((row_number, target_link_idx + 1, link_formula))

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
        "source_sheets_used": source_sheets_used,
        "per_sheet_source_headers": per_sheet_headers,
        "source_keys_sample": list(source_map.keys())[:20],
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
