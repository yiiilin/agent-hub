# Model Connections and usage browser workflows

Uses the real console, backend, Model Gateway, PostgreSQL, and fake OpenAI
Responses/Anthropic Messages provider. A
member exercises Personal, Available, and Usage tabs, including Agent and
subagent model bindings and independent usage/error pagination. An
Administrator changes a Global connection from Responses to Anthropic, tests
the converted request, and exercises System Default copy semantics,
ordinary-delete conflict handling, and Force Delete.

The scenario checks 1280x800 and 390x844 layouts and relies on the shared
browser support for console, page error, request failure, and HTTP diagnostics.
Tracing is paused while API keys are entered and resumes only after the secret
fields have been removed from the DOM. The prior System Default is restored in
`finally`.
