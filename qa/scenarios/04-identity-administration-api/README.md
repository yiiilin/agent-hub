# Identity and Administration API

Type: `api`

This scenario uses the real Compose backend, PostgreSQL, and Mock OIDC. It
verifies password registration, session login/me/logout, disabled policy
gates, required email verification, manual OIDC redirects and cookies, and a
stable external-identity binding for repeated logins.

It also verifies an explicitly expiring API key from one-time creation through
Bearer authentication, in-place renewal, self-mutation rejection, password
reset survival, and physical deletion. Administration coverage includes user
detail and role changes, the last-Super-Administrator boundary, an
administrator's inability to see or modify a Super Administrator, password
reset session invalidation, and uniquely keyed External Platform and
Authentication Channel create/list/update workflows. Platforms and channels
have no delete API, so their unique records remain until the disposable QA
database is removed.

The authentication policy is snapshotted before any change and is restored and
read back in `finally`; restoration failure fails the scenario. Exact-username
user erasure is the final functional mutation. The scenario performs only
read-only history polling afterward, apart from the mandatory policy restore,
and requires the erasure history to reach `completed`. API key plaintext is
kept only in memory and is never written to logs, SQL output, README content,
or artifacts.
