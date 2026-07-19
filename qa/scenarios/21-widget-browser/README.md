# Widget browser workflows

Type: `browser`

Uses the real Widget, backend, PostgreSQL, Runtime, fake Codex app-server, and
fake Responses provider. The scenario creates two private Agents and one scoped
Widget session for each Agent through authenticated public APIs.

The browser embeds `/widget` in a host page and verifies the parent protocol:
the initial and bound `ready` messages, the channel nonce, `resize`,
`session-select`, `message-submit`, `run-started`, and streamed `run-event`
notifications. A deliberately held first `POST /api/widget/runs` proves that a
same-token `session-select` cannot release the rapid-submit lock or create a
second Run. Selecting the other scoped session changes the Agent and clears the
previous Run output.

The Widget is checked at 1280x800 and 390x844 without horizontal overflow, and
the mobile view also exercises its Chinese controls. Browser diagnostics reject
unexpected console errors, page errors, same-origin HTTP failures, and request
failures.

Playwright tracing is paused before any scoped Widget token crosses the browser
boundary. The iframe and host message store are removed before tracing resumes,
so failure screenshots and traces cannot retain an embed credential. Both
scenario-owned Agents are deleted through public APIs even on failure.
