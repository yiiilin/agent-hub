---
status: accepted
---

# Use email as the Hub login identity

A Hub User has an immutable internal ID, one required globally unique current email for Hub login and trusted email binding, and a non-unique Display Name; Agent Hub has no separate Hub Username. Authenticated Integration Apps must provide a valid email, while their external username remains optional profile context. External Identities keep their platform-scoped stable binding, but LDAP deliberately stores neither a permanent directory user ID nor historical email aliases: after an administrator changes a Hub email, a later LDAP login that returns the former email may correctly create a new Hub User. This keeps the identity model small and accepts that email continuity is an administrator and directory concern rather than hidden Hub state.
