# Integration App browser workflows

Type: `browser`

Uses the real console, backend, PostgreSQL, and authenticated public APIs. An
administrator creates a unique enabled, trusted External Platform and
Authentication Channel fixture. A unique Mock OIDC owner starts with no
Integration Apps, while three scenario-owned Agents are created through that
owner's browser session.

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

Tracing is stopped before every create, rotate, or Widget-session request that
returns a secret or token. Before tracing resumes, the scenario clears the
clipboard, redacts any credential-bearing `code` or `href`, closes the dialog,
and checks that no client secret or Widget token remains in the DOM. The same
cleanup runs on failures before shared browser failure screenshots and traces are
written, so artifacts and diagnostics cannot retain credentials.

The full workflow runs at 1280x800 in English, then verifies the persisted table
and read-only edit dialog in Chinese at 390x844. Both viewports reject document
overflow, while the shared browser harness rejects unexpected console errors,
page errors, same-origin HTTP failures, and request failures. The three Agents
are archived through public APIs even on failure. Integration Apps and identity
fixtures have no delete operation in the current product contract; the runner's
isolated Compose teardown removes their database state.
