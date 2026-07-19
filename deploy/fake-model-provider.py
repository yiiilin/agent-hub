#!/usr/bin/env python3
import base64
import hashlib
import json
import os
import re
from http.server import BaseHTTPRequestHandler, HTTPServer


API_KEY = os.environ["FAKE_MODEL_PROVIDER_API_KEY"]
PORT = int(os.environ.get("FAKE_MODEL_PROVIDER_PORT", "8080"))
ASSET_NAMES = (
    "codex-x86_64-unknown-linux-musl.zst",
    "codex-aarch64-unknown-linux-musl.zst",
)
ARTIFACT_BYTES = base64.b64decode(
    "KLUv/WRgARUKAEaQNSQgq1YHa7yu/cetKosXUZPIEQBG+k/DnauR8CjjtxMnQUiI4AEqACsAKQCLYDC+JtaaRIVZDcmRBAjd7Mg/sMZ+Ffkrwaxvu6fUZPVzW0HdIis0gCXNGT6lbCR6zp8ZWUqqky4EKgbarGn5f3VgKokscbMRa2psWWLELZdBPdcBHJn9uLMZqv6LlbhGiAvNMyBfElxIdb2hqDDrjj98kMsgt1WS3O/ERwfqud4Bgk9WBBTcgTgaPnEeQW/ZAbN0F9wD3Ytgkz1pf0tZfhKfTFknPlpbRAEsIDACgszRHheiZ+cqMlxHc0yoCIK4rTYuDTM0GY09V8WtgQL25Huy1XLXZANUC2GKjN6vJiES05AGwMk4YtbJSbwgIEgCGMACTANLA7g7qGiu2WSryCzcDMcMVEmZDdSOXK8L6PDDWlDJhnjU="
)
ARTIFACT_DIGEST = hashlib.sha256(ARTIFACT_BYTES).hexdigest()
RELEASE_PATH = re.compile(
    r"/codex/releases/tags/rust-v([A-Za-z0-9._-]{1,64})"
)


class Handler(BaseHTTPRequestHandler):
    def do_GET(self):
        release = RELEASE_PATH.fullmatch(self.path)
        if release:
            version = release.group(1)
            origin = f"http://{self.headers['host']}"
            self.send_json(
                {
                    "tag_name": f"rust-v{version}",
                    "assets": [
                        {
                            "name": name,
                            "browser_download_url": (
                                f"{origin}/codex/artifacts/{name}"
                            ),
                            "digest": f"sha256:{ARTIFACT_DIGEST}",
                            "size": len(ARTIFACT_BYTES),
                        }
                        for name in ASSET_NAMES
                    ],
                }
            )
            return
        if self.path.removeprefix("/codex/artifacts/") in ASSET_NAMES:
            self.send_response(200)
            self.send_header("content-type", "application/zstd")
            self.send_header("content-length", str(len(ARTIFACT_BYTES)))
            self.end_headers()
            self.wfile.write(ARTIFACT_BYTES)
            return
        self.send_response(404)
        self.end_headers()

    def send_json(self, response):
        body = json.dumps(response).encode()
        self.send_response(200)
        self.send_header("content-type", "application/json")
        self.send_header("content-length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_POST(self):
        if self.path != "/v1/responses":
            self.send_response(404)
            self.end_headers()
            return
        if self.headers.get("authorization") != f"Bearer {API_KEY}":
            self.send_response(401)
            self.end_headers()
            return
        length = int(self.headers.get("content-length", "0"))
        try:
            request = json.loads(self.rfile.read(length) if length else b"{}")
        except (json.JSONDecodeError, UnicodeDecodeError):
            self.send_response(400)
            self.end_headers()
            return
        request_text = json.dumps(request, separators=(",", ":")).lower()
        failed = (
            request.get("model") == "hub-proxy-error"
            or "fixture:model-error" in request_text
        )
        if failed:
            response = {
                "id": "resp_proxy_fake_error",
                "object": "response",
                "model": request.get("model", "hub-proxy-smoke"),
                "status": "failed",
                "error": {
                    "code": "fake_model_error",
                    "message": "Deterministic fake provider failure.",
                },
                "usage": {
                    "input_tokens": 5,
                    "output_tokens": 2,
                    "total_tokens": 7,
                    "input_tokens_details": {"cached_tokens": 1},
                    "output_tokens_details": {"reasoning_tokens": 1},
                },
            }
        else:
            response = {
                "id": "resp_proxy_fake_completed",
                "object": "response",
                "model": request.get("model", "hub-proxy-smoke"),
                "status": "completed",
                "output_text": "Fake Codex completed run through the Hub model proxy.",
                "usage": {
                    "input_tokens": 11,
                    "output_tokens": 7,
                    "total_tokens": 18,
                    "input_tokens_details": {"cached_tokens": 3},
                    "output_tokens_details": {"reasoning_tokens": 5},
                },
            }
        body = json.dumps(response).encode()
        self.send_response(200)
        self.send_header("content-type", "application/json")
        self.send_header("content-length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, *_):
        return


HTTPServer(("0.0.0.0", PORT), Handler).serve_forever()
