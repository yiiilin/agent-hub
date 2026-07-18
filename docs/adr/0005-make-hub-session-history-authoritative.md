---
status: superseded by ADR-0012
---

# Make Hub Session History authoritative

Agent Hub owns the ordered model-visible Session History and treats each Codex app-server Thread as a disposable execution projection. A Run creates a fresh Thread from the Session Workspace and Hub history, captures raw response items back into Hub storage, and never uses a Codex thread ID or archived `CODEX_HOME` as the cross-Run source of truth. Each pinned Codex version must pass compatibility checks for thread creation, history injection, raw item capture, and compaction checkpoint recovery before activation; this keeps Session ownership and portability in Agent Hub while accepting a version-specific adapter at the Codex boundary.
