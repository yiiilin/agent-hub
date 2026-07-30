# Identity and Administration API

Type: `api`

This scenario uses the real Hub and PostgreSQL APIs. It verifies registration
with Display Name, Local Password Session login/me/logout, current-user Display
Name updates, registration and login-policy invariants, and hidden Super
Administrator emergency password access. A syntactically valid unreachable
LDAP configuration, including the Bind identity template, is saved only to
exercise policy gates. Its draft-test request has an invalid template and must
be rejected before any LDAP network access.

The Super Administrator creates an Administrator, which creates a distinct
member account through `POST /api/admin/users`. Role visibility and mutation
boundaries stay distinct. The member's email, Display Name, password, role, and
email-confirmed permanent deletion are exercised, including Session
invalidation after email/password changes and API key retention.

The Integration tail creates a trusted External Platform and Authentication
Channel. `client_credentials` External Session creation rejects missing and
invalid email, then binds a valid trusted email to the existing Hub user while
retaining the optional external username profile field.

Authentication policy and LDAP configuration are snapshotted and restored in
`finally`. API key and OAuth credentials remain only in memory and are not
written to artifacts.
