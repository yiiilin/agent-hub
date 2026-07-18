---
status: superseded by ADR-0015
---

# Archive resumable Session state without credentials

An offline Session is stored as an immutable Session Bundle containing its Workspace and a versioned manifest bound to one Hub-owned Session History checkpoint. Archiving occurs only after the active Run has reached a terminal state and the Workspace checkpoint is consistent; Codex State, credentials, authentication files, model proxy tokens, MCP secrets, logs, caches, and Hub-regenerable configuration or Skills are excluded and reconstructed when the Session is restored.
