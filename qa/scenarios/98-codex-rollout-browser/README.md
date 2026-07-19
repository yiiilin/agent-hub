# Codex Rollout Browser Scenario

This destructive browser scenario covers only `ROL-002`. It uses the public API
to start an exact `fixture:hold` Turn on the real Compose Runtime, then uses the
Administration UI to prepare and promote an exact fake Codex release. The UI
must poll from distribution to readiness without a reload or overlapping
rollout requests.

Promotion is intentionally irreversible in QA and may leave the selected Codex
version active. This scenario must run after every ordinary scenario and
immediately before `99-codex-rollout-api`; keep its `98-` identifier so the
default runner preserves that order.

After promotion, public Run, Session, Message, Runtime, and Run-event APIs prove
that the held Run and its Hub Turn, native Thread, and active native Turn were
not replaced or interrupted. An exact `fixture:release` steer lets that Turn
complete naturally. A subsequent ordinary message must complete in the same
Session and native Thread after the Runtime reports the promoted version.

The scenario does not use PostgreSQL directly. Cleanup first attempts to delete
the scenario-owned Agent without stopping the held Turn. Only a failed scenario
may explicitly stop a still-active held Run before retrying deletion, and that
fallback is reported with public IDs.
