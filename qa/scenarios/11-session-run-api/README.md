# Session Run API lifecycle

Exercises console Sessions and Runs through the real Compose API, Runtime, and
fake Codex app-server. The exact `fixture:hold` message keeps one native Turn
active so the scenario can verify ordered event history and authenticated SSE,
same-Turn steering, delivery acknowledgement, explicit interruption without
history rollback, and a subsequent completed Turn on the same native Thread.

The scenario also checks that an independent Session receives a different
native Thread, a second ordinary user cannot read the owner's Sessions or Runs,
and Agent deletion preserves Historical Session messages while rejecting new
messages. PostgreSQL is queried only for native Turn IDs and Turn status because
those fields have no owner-facing API.
