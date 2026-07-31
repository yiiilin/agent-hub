# OpenLDAP QA Fixture

This fixture is used only by the optional `ldap` profile in `compose.dev.yml`.
It contains deterministic users and passwords so Plain LDAP, StartTLS, LDAPS,
certificate rejection, and explicit verification skip can be tested without
external infrastructure.

`openldap-certs` runs `generate-certs.sh` when the LDAP profile starts and writes
an ephemeral CA, server certificate, and server key to a Compose volume. No
private key is stored in Git or copied into an Agent Hub image. Recreate the
Compose volumes to rotate the QA certificate.

Use this Bind identity template with the fixture:

```text
uid={email},ou=people,dc=example,dc=test
```

The fixture deliberately uses native OpenLDAP DN Bind behavior. It does not add
an authentication rewrite overlay.
