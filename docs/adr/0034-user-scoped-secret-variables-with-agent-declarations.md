---
status: accepted
---

# User-scoped Secret Variables with Agent declarations

Agent Hub stores user-owned Secret Variables (values or files) encrypted in the database and lets Agents declare only variable names (KEYs), a kind, and a usage hint, without binding to any user's secret. At Run start, Hub intersects the Agent's declarations with the invoking user's owned secrets and persistent Secret Grants (user + agent + key, remembered by default), then injects allowed values as AGENT_SECRET_<KEY> environment variables or places files under a Session-private engine-state/secrets directory with AGENT_SECRET_FILE_<KEY>. Console, Widget, and third-party access use the same authorization and injection contract. Secret files are excluded from Session Bundles and from bash/network-capable execution; only controlled read tools may access them. MCP and keypair auto-generation are out of scope; deleting a secret deletes its grants; revoking a grant stops new Runs without undoing in-flight Runs.

Considered options: Agent-bound secrets were rejected because an Agent cannot know which user owns or grants a secret; one-time session approval was rejected because the user asked to remember by default; workspace-visible secret files were rejected because Session Bundles would carry them to object storage.
