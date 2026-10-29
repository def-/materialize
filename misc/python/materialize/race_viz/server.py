# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

"""HTTP server behind the race viewer page."""

import json
import mimetypes
import threading
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any
from urllib.parse import parse_qs, urlparse

from materialize import MZ_ROOT
from materialize.race_viz.runner import (
    Order,
    RaceDir,
    Runner,
    aba_schedules,
    outcome_signature,
)

STATIC = Path(__file__).parent / "static"


class Explorer:
    """Runs A-B-A schedules in the background and reports progress."""

    def __init__(self, runner: Runner):
        self.runner = runner
        self.lock = threading.Lock()
        self.state: dict[str, Any] = {"running": False}

    def start(self, base_run: str, all_points: bool) -> None:
        with self.lock:
            if self.state.get("running"):
                raise ValueError("an exploration is already running")
            schedules = aba_schedules(
                self.runner.race_dir.events(base_run), all_points
            )
            self.state = {
                "running": True,
                "base": base_run,
                "total": len(schedules),
                "done": 0,
                "runs": [],
            }
        threading.Thread(target=self._explore, args=(schedules,), daemon=True).start()

    def _explore(self, schedules: list[tuple[str, list[Order]]]) -> None:
        try:
            for label, orders in schedules:
                result = self.runner.run(orders, label=f"aba: {label}")
                with self.lock:
                    self.state["done"] += 1
                    self.state["runs"].append(
                        {
                            "id": result.id,
                            "label": result.label,
                            "status": result.status,
                            "signature": outcome_signature(result),
                        }
                    )
        except Exception as e:
            with self.lock:
                self.state["error"] = f"{type(e).__name__}: {e}"
        finally:
            with self.lock:
                self.state["running"] = False

    def snapshot(self) -> dict[str, Any]:
        with self.lock:
            return json.loads(json.dumps(self.state))


def make_handler(runner: Runner, explorer: Explorer) -> type[BaseHTTPRequestHandler]:
    race_dir: RaceDir = runner.race_dir

    class Handler(BaseHTTPRequestHandler):
        def log_message(self, format: str, *args: Any) -> None:
            pass

        def send_json(self, value: Any, status: HTTPStatus = HTTPStatus.OK) -> None:
            body = json.dumps(value).encode()
            self.send_response(status)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def send_error_json(self, status: HTTPStatus, message: str) -> None:
            self.send_json({"error": message}, status)

        def read_json(self) -> Any:
            length = int(self.headers.get("Content-Length", "0"))
            return json.loads(self.rfile.read(length) or b"{}")

        def do_GET(self) -> None:
            url = urlparse(self.path)
            parts = [p for p in url.path.split("/") if p]
            try:
                if not parts:
                    self.send_static("index.html")
                elif parts[0] == "static" and len(parts) == 2:
                    self.send_static(parts[1])
                elif parts == ["api", "test"]:
                    self.send_json(
                        {
                            "name": runner.test.name,
                            "path": str(runner.test.path),
                            "actors": runner.test.actors,
                            "source": runner.test.path.read_text(),
                        }
                    )
                elif parts == ["api", "runs"]:
                    self.send_json(
                        [
                            {
                                "id": r.id,
                                "label": r.label,
                                "status": r.status,
                                "started": r.started,
                                "signature": outcome_signature(r),
                            }
                            for r in race_dir.runs()
                        ]
                    )
                elif parts[:2] == ["api", "runs"] and len(parts) == 3:
                    run_id = parts[2]
                    if not _safe_name(run_id):
                        return self.send_error_json(HTTPStatus.BAD_REQUEST, "bad run id")
                    result = race_dir.load(run_id)
                    self.send_json(
                        {
                            "run": result.to_json(),
                            "events": race_dir.events(run_id),
                        }
                    )
                elif parts == ["api", "explore"]:
                    self.send_json(explorer.snapshot())
                elif parts == ["api", "source"]:
                    q = parse_qs(url.query)
                    self.send_source(q.get("file", [""])[0], int(q.get("line", ["1"])[0]))
                else:
                    self.send_error_json(HTTPStatus.NOT_FOUND, "not found")
            except FileNotFoundError:
                self.send_error_json(HTTPStatus.NOT_FOUND, "not found")

        def do_POST(self) -> None:
            parts = [p for p in urlparse(self.path).path.split("/") if p]
            try:
                body = self.read_json()
                if parts == ["api", "runs"]:
                    orders = [Order.from_json(o) for o in body.get("orders", [])]
                    result = runner.run(orders, label=body.get("label", ""))
                    self.send_json({"id": result.id})
                elif parts == ["api", "explore"]:
                    base = body.get("base", "")
                    if not _safe_name(base):
                        return self.send_error_json(HTTPStatus.BAD_REQUEST, "bad run id")
                    explorer.start(base, bool(body.get("all_points", False)))
                    self.send_json(explorer.snapshot())
                else:
                    self.send_error_json(HTTPStatus.NOT_FOUND, "not found")
            except (ValueError, KeyError, TypeError) as e:
                self.send_error_json(HTTPStatus.BAD_REQUEST, str(e))
            except Exception as e:
                self.send_error_json(
                    HTTPStatus.INTERNAL_SERVER_ERROR, f"{type(e).__name__}: {e}"
                )

        def send_static(self, name: str) -> None:
            path = (STATIC / name).resolve()
            if path.parent != STATIC.resolve() or not path.is_file():
                return self.send_error_json(HTTPStatus.NOT_FOUND, "not found")
            body = path.read_bytes()
            self.send_response(HTTPStatus.OK)
            content_type = mimetypes.guess_type(name)[0] or "application/octet-stream"
            self.send_header("Content-Type", content_type)
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def send_source(self, file: str, line: int) -> None:
            # Race points record `file!()`, relative to the workspace root.
            root = MZ_ROOT.resolve()
            path = (root / file).resolve()
            if not path.is_relative_to(root) or not path.is_file():
                return self.send_error_json(HTTPStatus.NOT_FOUND, "not found")
            lines = path.read_text(errors="replace").splitlines()
            start = max(1, line - 15)
            end = min(len(lines), line + 15)
            self.send_json(
                {
                    "file": file,
                    "line": line,
                    "start": start,
                    "lines": lines[start - 1 : end],
                }
            )

    return Handler


def _safe_name(name: str) -> bool:
    return bool(name) and all(c.isalnum() or c in "-_" for c in name)


def serve(runner: Runner, host: str, port: int) -> ThreadingHTTPServer:
    explorer = Explorer(runner)
    return ThreadingHTTPServer((host, port), make_handler(runner, explorer))
