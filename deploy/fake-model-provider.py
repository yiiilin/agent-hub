#!/usr/bin/env python3
import base64
import hashlib
import json
import os
import re
from http.server import BaseHTTPRequestHandler, HTTPServer
from urllib.parse import urlsplit


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

    def send_json(self, response, status=200):
        body = json.dumps(response).encode()
        self.send_response(status)
        self.send_header("content-type", "application/json")
        self.send_header("content-length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_POST(self):
        path = urlsplit(self.path).path
        if path == "/v1/responses":
            self.handle_responses()
            return
        if path == "/v1/chat/completions":
            self.handle_chat_completions()
            return
        if path == "/v1/messages":
            self.handle_anthropic_messages()
            return
        self.send_response(404)
        self.end_headers()

    def read_request_json(self):
        length = int(self.headers.get("content-length", "0"))
        try:
            return json.loads(self.rfile.read(length) if length else b"{}")
        except (json.JSONDecodeError, UnicodeDecodeError):
            self.send_response(400)
            self.end_headers()
            return None

    def handle_responses(self):
        if self.headers.get("authorization") != f"Bearer {API_KEY}":
            self.send_response(401)
            self.end_headers()
            return
        request = self.read_request_json()
        if request is None:
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
        self.send_json(response)

    def handle_chat_completions(self):
        if self.headers.get("authorization") != f"Bearer {API_KEY}":
            self.send_response(401)
            self.end_headers()
            return
        request = self.read_request_json()
        if request is None:
            return
        request_text = json.dumps(request, separators=(",", ":")).lower()
        if (
            request.get("model") == "hub-proxy-error"
            or "fixture:model-error" in request_text
        ):
            self.send_json(
                {
                    "error": {
                        "code": "fake_model_error",
                        "message": "Deterministic fake provider failure.",
                    }
                },
                status=429,
            )
            return
        if request.get("stream") is True:
            self.send_chat_stream(request)
            return
        self.send_json(
            {
                "id": "chatcmpl_proxy_fake_completed",
                "object": "chat.completion",
                "created": 1_710_000_000,
                "model": request.get("model", "hub-proxy-smoke"),
                "choices": [
                    {
                        "index": 0,
                        "message": {
                            "role": "assistant",
                            "content": "Fake Chat Completions run through the Hub model gateway.",
                        },
                        "finish_reason": "stop",
                    }
                ],
                "usage": {
                    "prompt_tokens": 14,
                    "completion_tokens": 9,
                    "total_tokens": 23,
                },
            }
        )

    def send_chat_stream(self, request):
        model = request.get("model", "hub-proxy-smoke")
        chunks = (
            {
                "id": "chatcmpl_proxy_fake_stream",
                "object": "chat.completion.chunk",
                "created": 1_710_000_000,
                "model": model,
                "choices": [
                    {
                        "index": 0,
                        "delta": {
                            "role": "assistant",
                            "content": "Fake Chat Completions streamed through the Hub model gateway.",
                        },
                        "finish_reason": None,
                    }
                ],
            },
            {
                "id": "chatcmpl_proxy_fake_stream",
                "object": "chat.completion.chunk",
                "created": 1_710_000_000,
                "model": model,
                "choices": [
                    {"index": 0, "delta": {}, "finish_reason": "stop"}
                ],
            },
            {
                "id": "chatcmpl_proxy_fake_stream",
                "object": "chat.completion.chunk",
                "created": 1_710_000_000,
                "model": model,
                "choices": [],
                "usage": {
                    "prompt_tokens": 14,
                    "completion_tokens": 9,
                    "total_tokens": 23,
                },
            },
        )
        body = "".join(
            f"data: {json.dumps(chunk)}\n\n" for chunk in chunks
        ) + "data: [DONE]\n\n"
        encoded = body.encode()
        self.send_response(200)
        self.send_header("content-type", "text/event-stream")
        self.send_header("cache-control", "no-cache")
        self.send_header("content-length", str(len(encoded)))
        self.end_headers()
        self.wfile.write(encoded)

    def handle_anthropic_messages(self):
        if (
            self.headers.get("x-api-key") != API_KEY
            or self.headers.get("anthropic-version") != "2023-06-01"
        ):
            self.send_response(401)
            self.end_headers()
            return
        request = self.read_request_json()
        if request is None:
            return
        request_text = json.dumps(request, separators=(",", ":")).lower()
        if (
            request.get("model") == "hub-proxy-error"
            or "fixture:model-error" in request_text
        ):
            self.send_json(
                {
                    "type": "error",
                    "error": {
                        "type": "rate_limit_error",
                        "message": "Deterministic fake provider failure.",
                    },
                },
                status=429,
            )
            return
        if request.get("stream") is True:
            self.send_anthropic_stream(request)
            return
        self.send_json(
            {
                "id": "msg_proxy_fake_completed",
                "type": "message",
                "role": "assistant",
                "model": request.get("model", "hub-proxy-smoke"),
                "content": [
                    {
                        "type": "text",
                        "text": "Fake Anthropic completed run through the Hub model gateway.",
                    }
                ],
                "stop_reason": "end_turn",
                "stop_sequence": None,
                "usage": {"input_tokens": 13, "output_tokens": 8},
            }
        )

    def send_anthropic_stream(self, request):
        model = request.get("model", "hub-proxy-smoke")
        events = (
            (
                "message_start",
                {
                    "type": "message_start",
                    "message": {
                        "id": "msg_proxy_fake_stream",
                        "type": "message",
                        "role": "assistant",
                        "model": model,
                        "content": [],
                        "stop_reason": None,
                        "stop_sequence": None,
                        "usage": {"input_tokens": 13, "output_tokens": 0},
                    },
                },
            ),
            (
                "content_block_start",
                {
                    "type": "content_block_start",
                    "index": 0,
                    "content_block": {"type": "text", "text": ""},
                },
            ),
            (
                "content_block_delta",
                {
                    "type": "content_block_delta",
                    "index": 0,
                    "delta": {
                        "type": "text_delta",
                        "text": "Fake Anthropic streamed through the Hub model gateway.",
                    },
                },
            ),
            (
                "content_block_stop",
                {"type": "content_block_stop", "index": 0},
            ),
            (
                "message_delta",
                {
                    "type": "message_delta",
                    "delta": {"stop_reason": "end_turn", "stop_sequence": None},
                    "usage": {"output_tokens": 8},
                },
            ),
            ("message_stop", {"type": "message_stop"}),
        )
        body = "".join(
            f"event: {event}\ndata: {json.dumps(payload)}\n\n"
            for event, payload in events
        ).encode()
        self.send_response(200)
        self.send_header("content-type", "text/event-stream")
        self.send_header("cache-control", "no-cache")
        self.send_header("content-length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, *_):
        return


HTTPServer(("0.0.0.0", PORT), Handler).serve_forever()
