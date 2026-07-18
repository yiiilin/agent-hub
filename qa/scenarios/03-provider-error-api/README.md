# Provider Error API

Type: `api`

This scenario sends the `fixture:model-error` marker through the real Hub,
Runtime, fake Codex app-server, and fake Responses provider. It verifies that
the Run fails and that both returned token usage and the provider error are
retained in the accounting ledgers.
