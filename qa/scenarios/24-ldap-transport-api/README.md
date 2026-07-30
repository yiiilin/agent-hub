# Real LDAP Transport API

Type: `api`

This scenario uses the pinned OpenLDAP service from the Compose `ldap` profile.
It validates the public `bind_identity_template` contract, including the
`{email}` default and exact-one-placeholder rule, then performs real direct Bind
and Subtree search operations over Plain LDAP, StartTLS, and LDAPS. The
self-signed certificate must fail by default and succeed only with explicit TLS
verification skip.

Deterministic directory users cover one, zero, and multiple search results,
authoritative email mapping, mapped Display Name, Display Name fallback, and a
real Bind plus Subtree search whose email contains both DN-special `+` and
filter-special `*` characters. A
blackhole address on the owned Compose subnet exercises the bounded timeout and
no-retry wall-clock oracle. Administrator errors are checked for useful stages
without credentials, Bind DNs, or raw attributes, while login errors remain
generic.

The scenario also proves that LDAP configuration and policy changes do not
invalidate an existing browser Session. Persistent email and forwarded-source
IP limits cover threshold, `Retry-After`, success reset, and window expiry. All
scenario throttle rows and the original authentication policy/configuration are
restored in `finally`; fixture files are never modified.
