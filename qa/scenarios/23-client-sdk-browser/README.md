# Published Client SDK browser workflow

Type: `browser`

This scenario serves the actual built `@agent-hub/client` package from a
temporary, separate localhost origin. Its small Node host keeps the test
Integration App client secret in the Node process and exchanges it only through
same-origin `/authorize`; the browser never receives that secret.

For authenticated access, it creates a model-backed Agent whose only enabled
Agent Tool is `integration`, then grants an `echo` Client Tool. The first normal
message materializes an external Session. A shortened client-visible expiry
forces a real credential renewal; a held model Turn then proves a second SDK
message steers the same Run and SDK `stop()` interrupts it. The same Client
then opens a fresh local draft for the tool workflow. The primary tab
subscribes, a real click opens a `window.open` observer tab, and the observer
must replace the sessionStorage-cloned Client Instance ID through the real
BroadcastChannel reservation. A later message makes the fake provider request
`echo`; the primary tab claims it, holds its handler, and the observer receives
the request but cannot execute it. Releasing the primary handler produces the real
`tool_request -> tool_result -> assistant` continuation. The scenario proves
there is exactly one `integration:tool_result` child Run, then replays the same
event stream from zero and proves the SDK resubmits its cached result without
executing the handler twice.

It reads `agent-hub-client/tool-journal` from real IndexedDB and verifies the
primary entry is `acknowledged`, while every row remains keyed by its Client
Instance ID. It also verifies that no `ahw_` or `ahp_` credential appears in a
BrowserContext-observed request URL, DOM, sessionStorage, localStorage, or
IndexedDB. Browser tracing is paused for every credential flow and resumes only
after every Client is disposed and all pages are navigated away from the host.

For anonymous access, the scenario uses an App with exact Origin policy, one
preconfigured `echo` Client Tool, and history disabled. It verifies the same
real tool continuation, persisted visitor key and exact Session recovery after a
reload, replay of the completed assistant event without rerunning the handler,
and the SDK's `anonymous_history_disabled` contract. The same host via
`localhost` (rather than `127.0.0.1`) is a distinct Origin: an authenticated
Session request and an anonymous Client Access request must both be rejected.
