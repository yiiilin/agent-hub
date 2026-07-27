# Widget integration examples

This zero-dependency Node server demonstrates both supported Integration App
Widget boundaries:

- `/anonymous/` embeds a public App by `client_id`. The browser receives no App
  secret or external identity, history discovery is disabled, and the effective
  tools are read-only.
- `/authenticated/` uses an HttpOnly example-site session. Its server exchanges
  the configured Integration App credentials and server-owned user profile for
  a short-lived Widget credential. The iframe receives only that scoped
  credential and can list the same external user's conversation history.

The authenticated profile in this example stands in for values read from the
integrating site's own login session. Do not accept `external_user_id`, tenant,
email, or attributes directly from an untrusted browser request.

## Run

Create one public Integration App and one login-required Integration App first.
The public App must delegate exactly one Agent, disable history, set the exact
example-server Origin, and use only `read`, `grep`, `find`, or `ls`. The
authenticated App can enable history and further restrict the delegated
Agent's tool allowlist.

```bash
HOST=0.0.0.0 \
PORT=15179 \
AGENT_HUB_URL=http://localhost:15173 \
PUBLIC_WIDGET_CLIENT_ID=<public-client-id> \
PUBLIC_WIDGET_APP_NAME='Public knowledge example' \
PUBLIC_WIDGET_AGENT_NAME='Public knowledge agent' \
PUBLIC_WIDGET_TOOLS=read,grep,find,ls \
AUTH_WIDGET_CLIENT_ID=<authenticated-client-id> \
AUTH_WIDGET_CLIENT_SECRET=<authenticated-client-secret> \
AUTH_WIDGET_APP_NAME='Customer workspace example' \
AUTH_WIDGET_AGENT_ID=<authenticated-agent-id> \
AUTH_WIDGET_AGENT_NAME='Customer workspace agent' \
AUTH_WIDGET_TOOLS=read,grep \
AUTH_WIDGET_EXTERNAL_USER_ID=customer-1001 \
AUTH_WIDGET_TENANT_ID=tenant-demo \
AUTH_WIDGET_USERNAME=lin \
AUTH_WIDGET_DISPLAY_NAME='Lin Chen' \
AUTH_WIDGET_EMAIL=lin@example.test \
AUTH_WIDGET_USER_ATTRIBUTES='{"plan":"pro","locale":"zh-CN"}' \
node examples/widget-integrations/server.mjs
```

The App secret is required only in the server process environment. Never put it
in HTML, browser JavaScript, URL parameters, logs, or committed environment
files.

For a persistent local process, the same variable names may be stored as a JSON
object in a repository-external file with mode `0600`, then loaded with
`node examples/widget-integrations/server.mjs --config /secure/path/config.json`.
