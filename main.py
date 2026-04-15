import os
from typing import Any, Dict

from flask import Flask, jsonify, render_template, request

from sync_artist_report import (
    build_client_from_info,
    create_artist_sheet_from_source,
    extract_sheet_id,
    parse_service_account_json,
    sync,
)


app = Flask(__name__)


def _json_error(message: str, status: int):
    return jsonify({"ok": False, "error": message}), status


def _run_sync(body: Dict[str, Any]):
    source_url = body.get("source_url") or os.getenv("SOURCE_SHEET_URL")
    target_url = (body.get("target_url") or os.getenv("TARGET_SHEET_URL") or "").strip()
    auto_create_target = bool(body.get("auto_create_target", True))
    new_target_created = False
    source_sheet_name = body.get("source_sheet_name")
    target_sheet_name = body.get("target_sheet_name")
    source_header_row = int(body.get("source_header_row", 15))
    target_header_row = int(body.get("target_header_row", 9))

    if not source_url:
        return None, _json_error(
            "source_url is required (body or env vars)",
            400,
        )

    service_account_json = os.getenv("SERVICE_ACCOUNT_JSON", "")
    if not service_account_json:
        return None, _json_error("SERVICE_ACCOUNT_JSON is not set", 500)

    client = build_client_from_info(parse_service_account_json(service_account_json))
    if not target_url:
        if not auto_create_target:
            return None, _json_error("target_url is empty and auto_create_target=false", 400)
        target_url = create_artist_sheet_from_source(client, source_url)
        new_target_created = True

    matched, updates, debug = sync(
        client=client,
        source_url=source_url,
        target_url=target_url,
        source_sheet_name=source_sheet_name,
        target_sheet_name=target_sheet_name,
        source_header_row=source_header_row,
        target_header_row=target_header_row,
    )
    target_sheet_id = extract_sheet_id(target_url)
    target_open_url = f"https://docs.google.com/spreadsheets/d/{target_sheet_id}/edit"
    target_xlsx_url = (
        f"https://docs.google.com/spreadsheets/d/{target_sheet_id}/export?format=xlsx"
    )
    return {
        "ok": True,
        "matched_rows": matched,
        "updated_cells": updates,
        "new_target_created": new_target_created,
        "target_open_url": target_open_url,
        "target_xlsx_url": target_xlsx_url,
        "debug": debug,
    }, None


def _is_same_origin() -> bool:
    origin = request.headers.get("Origin", "").rstrip("/")
    host_url = request.host_url.rstrip("/")
    return bool(origin and origin == host_url)


@app.get("/")
def home():
    return render_template("index.html")


@app.get("/health")
def health():
    return jsonify({"ok": True, "message": "Service is running"})


@app.get("/api/sync")
def sync_get():
    return jsonify({"ok": True, "message": "Use POST /api/sync"})


@app.post("/api/sync")
def sync_post():
    try:
        auth_header = request.headers.get("Authorization", "")
        expected_token = os.getenv("SYNC_TOKEN", "").strip()
        if expected_token:
            if not auth_header.startswith("Bearer "):
                return _json_error("Missing Bearer token", 401)
            token = auth_header.replace("Bearer ", "", 1).strip()
            if token != expected_token:
                return _json_error("Invalid token", 403)

        body: Dict[str, Any] = request.get_json(silent=True) or {}
        result, error_response = _run_sync(body)
        if error_response:
            return error_response
        return jsonify(result)
    except Exception as exc:
        return _json_error(str(exc), 500)


@app.post("/sync-ui")
def sync_ui_post():
    try:
        if not _is_same_origin():
            return _json_error("UI endpoint is same-origin only", 403)

        body: Dict[str, Any] = request.get_json(silent=True) or {}
        result, error_response = _run_sync(body)
        if error_response:
            return error_response
        return jsonify(result)
    except Exception as exc:
        return _json_error(str(exc), 500)
