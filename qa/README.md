# Agent Hub QA Scenarios

This directory contains unattended end-to-end scenarios that use an isolated
Docker Compose environment. The environment uses fake Codex, a fake Responses
provider, and Mock OIDC; it does not call a real AI service.

## Run scenarios

Install the existing frontend dependencies before running browser scenarios:

```bash
cd frontend
npm ci
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
./qa/run-all.sh --keep-env 02-session-browser
```

`--type api` does not import Playwright or start Chromium. All selected
scenarios share one freshly created Compose environment. Unless `--keep-env`
is set, the runner removes its containers, network, and volumes when it exits.

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

`scenario.json` declares the display name, type, and hard timeout:

```json
{
  "name": "Short behavior description",
  "type": "api",
  "timeout_ms": 60000
}
```

`type` must be `api` or `browser`. `scenario.mjs` must default-export one async
function. Its context provides `baseURL`, `artifactsDir`, `compose`, and
`unique(prefix)`. API scenarios should use `qa/support/api.mjs`. Browser
scenarios should use `qa/support/browser.mjs`, which reuses the Playwright
installation in `frontend/node_modules` and captures browser diagnostics.

Keep fixtures deterministic and local. A scenario must clean up resources it
creates only when later scenarios could observe them; the entire database and
workspace volumes are discarded after the run.
