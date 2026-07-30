# OpenLDAP QA Fixture

This fixture is used only by the optional `ldap` profile in `compose.dev.yml`.
It contains deterministic users, passwords, and a self-signed TLS private key so
Plain LDAP, StartTLS, LDAPS, certificate rejection, and explicit verification
skip can be tested without external infrastructure.

None of the credentials or certificates in this directory are secrets. They
must never be reused outside local development and QA.

Use this Bind identity template with the fixture:

```text
uid={email},ou=people,dc=example,dc=test
```

The fixture deliberately uses native OpenLDAP DN Bind behavior. It does not add
an authentication rewrite overlay.
