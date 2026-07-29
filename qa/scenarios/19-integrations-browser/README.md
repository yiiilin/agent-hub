# Integration App browser workflows

Type: `browser`

Uses the real console, backend, PostgreSQL, and authenticated public APIs. The
fixture Super Administrator creates a unique enabled, trusted External Platform
and Authentication Channel, then promotes a unique Mock OIDC owner to
Administrator. That owner starts with no Integration Apps and creates three
scenario-owned Agents through the browser session.

For IAP-001, the scenario verifies the empty state and table-first management
surface, then creates an Integration App through the UI with the trusted origin,
two redirect URIs, and two delegated Agents. It reads the persisted resource,
checks the table columns and origin, and edits the name, both redirect URIs, and
the delegated Agent set. The edit dialog exposes platform and channel as
read-only details, and its PATCH body cannot replace either origin identifier.

For IAP-002, creation and explicit rotation confirmation each reveal one new
client secret. The scenario copies each displayed secret, closes its dialog,
reloads the page, and proves list/detail reads and refreshed UI never reveal it
again. After delegation is changed, both remaining Agents independently generate
Widget links. Each link targets `/widget`, carries its opaque token only in the
URL fragment, has no query string, and supports copying.

For IAP-003, the Administrator creates a second, anonymous Integration App with
one Agent and exact HTTP and HTTPS Origins, while observing the HTTP bearer-token
and mixed-content warning. The Client Tool form rejects an invalid name, blank
description, malformed JSON, and a non-object schema; a valid tool is then added,
edited, and deleted before the final tool is added. The App tool allowlist selects
`read` and `grep`, automatically includes `integration`, and proves that
`integration` cannot be deselected while a Client Tool exists. The scenario
asserts `login_required`, `allowed_origins`, and `client_tool_definitions` in the
browser POST, persisted detail, browser PATCH, PATCH response, and updated detail.
The edit keeps the same single Agent while changing one Origin and the persisted
Client Tool description.

Tracing is stopped before every create, rotate, or Widget-session request that
returns a secret or token. Before tracing resumes, the scenario clears the
clipboard, redacts any credential-bearing `code` or `href`, closes the dialog,
and checks that no client secret or Widget token remains in the DOM. The same
cleanup runs on failures before shared browser failure screenshots and traces are
written, so artifacts and diagnostics cannot retain credentials.

The full workflow runs at 1280x800 in English, then retains the persisted table
and read-only authenticated-App edit-dialog check in Chinese at 390x844. Both
viewports reject document overflow, while the shared browser harness rejects
unexpected console errors, page errors, same-origin HTTP failures, and request
failures. The three Agents are archived through public APIs even on failure. The
promoted owner and Integration Apps, plus the platform/channel fixtures, remain
isolated to the scenario Compose environment and are removed by its teardown.
