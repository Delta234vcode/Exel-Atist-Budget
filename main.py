import os
from typing import Any, Dict

from flask import Flask, jsonify, request

from sync_artist_report import (
    build_client_from_info,
    parse_service_account_json,
    sync,
)


app = Flask(__name__)


def _json_error(message: str, status: int):
    return jsonify({"ok": False, "error": message}), status


@app.get("/")
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

        source_url = body.get("source_url") or os.getenv("SOURCE_SHEET_URL")
        target_url = body.get("target_url") or os.getenv("TARGET_SHEET_URL")
        source_sheet_name = body.get("source_sheet_name")
        target_sheet_name = body.get("target_sheet_name")
        source_header_row = int(body.get("source_header_row", 9))
        target_header_row = int(body.get("target_header_row", 9))

        if not source_url or not target_url:
            return _json_error(
                "source_url and target_url are required (body or env vars)",
                400,
            )

        service_account_json = os.getenv("SERVICE_ACCOUNT_JSON", "")
        if not service_account_json:
            return _json_error("SERVICE_ACCOUNT_JSON is not set", 500)

        client = build_client_from_info(parse_service_account_json(service_account_json))
        matched, updates = sync(
            client=client,
            source_url=source_url,
            target_url=target_url,
            source_sheet_name=source_sheet_name,
            target_sheet_name=target_sheet_name,
            source_header_row=source_header_row,
            target_header_row=target_header_row,
        )

        return jsonify({"ok": True, "matched_rows": matched, "updated_cells": updates})
    except Exception as exc:
        return _json_error(str(exc), 500)
