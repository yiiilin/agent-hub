# Browser Session Round Trip

Type: `browser`

This scenario reuses the Playwright installation under `frontend/`. It signs in
through the real UI, creates an Agent and Run through authenticated browser API
requests, waits for fake Codex completion, and verifies the Session transcript,
collapsed readable activity, browser diagnostics, and horizontal overflow at
desktop and 390px mobile widths.
