# Runtime administration browser

Exercises the real `/runtimes` console and Runtime HTTP protocol in the
isolated Compose environment. The administrator creates a one-time enrollment
in the UI, registers a fake Runtime through the public Runtime API, proves the
enrollment cannot be reused, revokes a second unused enrollment in the UI, and
checks Runtime details and Agent-binding controls.

The same Runtime completes a UI-requested credential rotation through public
heartbeats, claims and completes an Agent Run, then exposes its owned Session
in the drain impact dialog. The scenario checks exact hostname confirmation,
cancels the drain, and later force-deletes the uncheckpointed Runtime. A second
bound but empty Runtime proves ordinary deletion is available only after drain
and an empty impact preview. The recovery-failed Session is then inspected in
the real `/sessions` console, where its public recovery reason is visible and
continuation controls are absent.

A separately authenticated member opens `/runtimes` and receives no enrollment,
rotation, drain, or deletion controls. Desktop and 390px layouts include
horizontal-overflow checks, and both browser sessions require empty console and
network diagnostics. Setup, state checks, and cleanup use public APIs only; the
Compose-provided Runtime is recorded as a baseline and never modified.
