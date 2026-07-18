# Proxy Responses transparently through Hub

Codex sends its native Responses API requests through Runtime to Hub, which preserves the path, query, body, safe end-to-end headers, upstream status, and streamed response bytes while replacing authentication with the selected Model Connection's credential. Hub validates the selected connection and model, observes terminal events for usage and sanitized errors, and otherwise avoids translating the protocol so Runtime never receives provider credentials and Codex retains its native request behavior.
