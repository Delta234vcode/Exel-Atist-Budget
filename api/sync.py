import json
import os
from http.server import BaseHTTPRequestHandler

from sync_artist_report import (
    build_client_from_info,
    create_artist_sheet_from_source,
    extract_sheet_id,
    parse_service_account_json,
    resolve_source_sheet_names,
    sync,
    transform_artist_report_layout,
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
            target_url = (body.get("target_url") or os.getenv("TARGET_SHEET_URL") or "").strip()
            auto_create_target = bool(body.get("auto_create_target", True))
            new_target_created = False
            source_sheet_name = body.get("source_sheet_name")
            target_sheet_name = body.get("target_sheet_name")
            selected_cities = body.get("selected_cities")
            source_header_row = int(body.get("source_header_row", 15))
            target_header_row = int(body.get("target_header_row", 9))

            if not source_url:
                _json_response(
                    self,
                    400,
                    {
                        "ok": False,
                        "error": "source_url is required (body or env vars)",
                    },
                )
                return

            service_account_json = os.getenv("SERVICE_ACCOUNT_JSON", "")
            if not service_account_json:
                _json_response(self, 500, {"ok": False, "error": "SERVICE_ACCOUNT_JSON is not set"})
                return

            client = build_client_from_info(parse_service_account_json(service_account_json))
            if not target_url:
                if not auto_create_target:
                    _json_response(self, 400, {"ok": False, "error": "target_url is empty and auto_create_target=false"})
                    return
                target_url = create_artist_sheet_from_source(client, source_url)
                new_target_created = True

            multi_sources, single_source = resolve_source_sheet_names(
                client, source_url, selected_cities, source_sheet_name
            )
            if new_target_created:
                debug = transform_artist_report_layout(
                    client=client,
                    target_url=target_url,
                    sheet_names=multi_sources,
                    target_sheet_name=single_source,
                    remove_other_sheets=bool(multi_sources),
                )
                target_sheet_id = extract_sheet_id(target_url)
                _json_response(
                    self,
                    200,
                    {
                        "ok": True,
                        "matched_rows": 0,
                        "updated_cells": 0,
                        "new_target_created": True,
                        "target_open_url": f"https://docs.google.com/spreadsheets/d/{target_sheet_id}/edit",
                        "target_xlsx_url": f"https://docs.google.com/spreadsheets/d/{target_sheet_id}/export?format=xlsx",
                        "debug": debug,
                    },
                )
                return

            matched, updates_count, debug = sync(
                client=client,
                source_url=source_url,
                target_url=target_url,
                source_sheet_name=single_source,
                target_sheet_name=target_sheet_name,
                source_header_row=source_header_row,
                target_header_row=target_header_row,
                source_sheet_names=multi_sources,
            )

            _json_response(
                self,
                200,
                {
                    "ok": True,
                    "matched_rows": matched,
                    "updated_cells": updates_count,
                    "new_target_created": False,
                    "debug": debug,
                },
            )
        except ValueError as exc:
            _json_response(self, 400, {"ok": False, "error": str(exc)})
        except Exception as exc:
            _json_response(self, 500, {"ok": False, "error": str(exc)})
