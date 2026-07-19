# Public OpenAPI and console navigation

Type: `browser`

Uses the real frontend, backend, and PostgreSQL services. Before login, the
scenario requests `/openapi.json` and verifies its public JSON contract. After
login, it follows the primary sidebar through Sessions, Agents, Integration
Apps, Automations, Skills, Models, API Keys, Runtimes, Administration, and API
Docs. Every transition waits for the page's real service response, checks the
URL, current navigation state, page heading, and document width.

The full navigation order is exercised first in English at 1280x800 and then in
Chinese at 390x844. Sessions must remain the first workspace destination. API
Docs must load the same public OpenAPI document, expose its JSON link, and render
a registered endpoint. The shared browser harness rejects unexpected console
errors, page errors, same-origin HTTP failures, and request failures.

Page-specific loading, error, retry, filtered-empty, and empty states remain
covered by their existing Playwright evidence. This cross-cutting scenario uses
the isolated Compose database's natural states and does not inject synthetic
server failures or repeat each page's CRUD workflow.
