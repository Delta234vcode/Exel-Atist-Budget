import json
import os
from http.server import BaseHTTPRequestHandler

from sync_artist_report import (
    build_client_from_info,
    parse_service_account_json,
    sync,
)


def _json_response(handler: BaseHTTPRequestHandler, status: int, payload: dict) -> None:
    data = json.dumps(payload).encode("utf-8")
    handler.send_response(status)
    handler.send_header("Content-Type", "application/json; charset=utf-8")
    handler.send_header("Content-Length", str(len(data)))
    handler.end_headers()
    handler.wfile.write(data)


class handler(BaseHTTPRequestHandler):
    def do_GET(self) -> None:
        _json_response(self, 200, {"ok": True, "message": "Use POST /api/sync"})

    def do_POST(self) -> None:
        try:
            auth_header = self.headers.get("Authorization", "")
            expected_token = os.getenv("SYNC_TOKEN", "").strip()
            if expected_token:
                if not auth_header.startswith("Bearer "):
                    _json_response(self, 401, {"ok": False, "error": "Missing Bearer token"})
                    return
                token = auth_header.replace("Bearer ", "", 1).strip()
                if token != expected_token:
                    _json_response(self, 403, {"ok": False, "error": "Invalid token"})
                    return

            content_length = int(self.headers.get("Content-Length", "0"))
            raw_body = self.rfile.read(content_length) if content_length > 0 else b"{}"
            body = json.loads(raw_body.decode("utf-8"))

            source_url = body.get("source_url") or os.getenv("SOURCE_SHEET_URL")
            target_url = body.get("target_url") or os.getenv("TARGET_SHEET_URL")
            source_sheet_name = body.get("source_sheet_name")
            target_sheet_name = body.get("target_sheet_name")
            source_header_row = int(body.get("source_header_row", 9))
            target_header_row = int(body.get("target_header_row", 9))

            if not source_url or not target_url:
                _json_response(
                    self,
                    400,
                    {
                        "ok": False,
                        "error": "source_url and target_url are required (body or env vars)",
                    },
                )
                return

            service_account_json = os.getenv("SERVICE_ACCOUNT_JSON", "")
            if not service_account_json:
                _json_response(self, 500, {"ok": False, "error": "SERVICE_ACCOUNT_JSON is not set"})
                return

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

            _json_response(
                self,
                200,
                {"ok": True, "matched_rows": matched, "updated_cells": updates},
            )
        except Exception as exc:
            _json_response(self, 500, {"ok": False, "error": str(exc)})
