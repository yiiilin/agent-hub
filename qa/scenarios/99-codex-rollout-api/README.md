# Codex rollout API

Type: `api`

This is the destructive-last QA scenario. Its `99-` prefix is intentional: it
promotes a concrete Codex target globally and leaves that target active because
the rollout has no public rollback operation.

The scenario uses administrator and owner-facing HTTP APIs, the real Compose
Runtime, and read-only probes of the Runtime filesystem. It holds one real
native Turn with `fixture:hold`, prepares and verifies exact Linux `x86_64` and
`aarch64` artifacts, proves promotion is blocked before readiness, promotes only
after the real Runtime reports ready, and then releases the held Turn naturally
with the exact `fixture:release` steering message. It never calls Run stop.

An independently run QA environment has one real Runtime architecture. To make
the Hub fetch both required Linux artifacts, the scenario briefly registers the
other architecture with an administrator-created one-time enrollment token.
That platform-catalog record never heartbeats or receives work and is deleted
through the administrator API immediately after the pre-readiness `409` check.
All readiness, in-flight continuity, checkpoint, Bundle, Thread-resume, and
active-version assertions are against the real Compose Runtime.

The final ordinary message is sent to the same Session without restarting the
Runtime or its Codex process. Cleanup deletes the Agent and waits for its Runtime
Session directory to disappear. No Runtime credential or enrollment token is
written to logs or artifacts.
