# Agent Hub QA Scenarios

This directory contains unattended end-to-end scenarios that use an isolated
Docker Compose environment. The environment uses a fake Pi RPC process, a fake
model provider, and Mock OIDC; it does not call a real AI service.

## Coverage contract

`features.json` is the canonical catalog of V1 behaviors. Each feature has a
stable ID, owning domain, required evidence layers, related OpenAPI operations
and console routes, plus any existing Rust or Playwright evidence. The
normative coverage rules are in `../docs/qa-spec.md`.

Every scenario declares the feature IDs it directly verifies in its
`scenario.json` manifest. A feature may use Rust tests for protocol, database,
or concurrency evidence, but every core domain must also have at least one
real Compose API or browser scenario. Mock-only Playwright evidence does not
count as full-stack coverage.

## Run scenarios

The QA runner loads the frontend TypeScript dependency, including for
`--coverage` and API-only runs. Install frontend dependencies first:

```bash
cd frontend
npm ci
cd ..
```

Browser scenarios additionally require Chromium:

```bash
cd frontend
npx playwright install chromium
cd ..
```

Run every scenario:

```bash
./qa/run-all.sh
```

Useful filters and diagnostics:

```bash
./qa/run-all.sh --type api
./qa/run-all.sh --type browser
./qa/run-all.sh 02-session-browser
./qa/run-all.sh --list
./qa/run-all.sh --coverage
./qa/run-all.sh --keep-env 02-session-browser
```

`--type api` does not import Playwright or start Chromium. All selected
scenarios share one freshly created Compose environment. Unless `--keep-env`
is set, the runner removes its containers, network, and volumes when it exits.
`--coverage` validates the catalog, manifests, evidence markers, OpenAPI
operations, UI routes, required layers, and Compose domains entirely offline.
It exits non-zero while planned scenario gaps remain.

Results are written under `qa/artifacts/<run-id>/`. Every run produces
`summary.json` and `junit.xml`. A failed scenario also records its error and
Compose logs; browser failures include a screenshot, browser diagnostics, and
a Playwright trace.

## Add a scenario

Create one directory under `qa/scenarios/` containing:

```text
qa/scenarios/NN-short-name/
  README.md
  scenario.json
  scenario.mjs
```

`scenario.json` declares the display name, type, hard timeout, and covered
feature IDs:

```json
{
  "name": "Short behavior description",
  "type": "api",
  "timeout_ms": 60000,
  "covers": ["AUTH-001", "AUTH-002"]
}
```

`type` must be `api` or `browser`; every `covers` entry must exist in
`features.json`. `scenario.mjs` must default-export one async function. Its
context provides `baseURL`, `artifactsDir`, `compose`, and `unique(prefix)`.
API scenarios should use `qa/support/api.mjs`. Browser scenarios should use
`qa/support/browser.mjs`, which reuses the Playwright installation in
`frontend/node_modules` and captures browser diagnostics.

Keep fixtures deterministic and local. A scenario must clean up resources it
creates only when later scenarios could observe them; the entire database and
workspace volumes are discarded after the run.
