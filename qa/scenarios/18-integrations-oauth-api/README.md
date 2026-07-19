# Integration OAuth API lifecycle

Type: `api`

This scenario uses only public HTTP contracts. It covers Integration App
ownership and secret rotation, both OAuth grants, scoped userinfo, origin-bound
External Sessions, attachments, ordered events, SSE, tool result continuation,
concurrent message serialization, stopping, and immediate delegation
revocation.

The concurrency check registers a scenario-owned Runtime but never heartbeats
it. Binding one Agent to that Runtime keeps the accepted Run pending long
enough for two simultaneous messages to exercise the Session lock. The Run is
stopped through the Integration API, the Agent's original Runtime binding is
restored, and the temporary Runtime is force-deleted through the administrator
API. No SQL cleanup or database assertions are used.

Client secrets, access tokens, Widget tokens, Runtime credentials, enrollment
tokens, and authorization codes remain in memory. Assertions report only
status, shape, and resource IDs; they never include credential-bearing response
bodies or redirect locations. Integration Apps and External Platform fixtures
have no delete operation in the current contract, so the isolated Compose
teardown removes their database state.
