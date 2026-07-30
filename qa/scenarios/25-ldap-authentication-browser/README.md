# Real LDAP Authentication Browser

Type: `browser`

This scenario signs into the real backend-hosted console against the pinned
OpenLDAP service. It verifies the English desktop LDAP selector, generic wrong-
credential feedback, a successful direct-Bind Session, mapped Display Name,
and clean console/network diagnostics. Disabling LDAP removes it from new
ordinary login choices without invalidating the already-issued browser Session.

After logout, the scenario enables LDAP as the only ordinary method and checks
the Simplified Chinese 390px layout. `/login?method=password` must still expose
the hidden Super Administrator emergency Local Password form and establish a
real Session. Both views are checked for horizontal overflow. Authentication
policy and LDAP configuration are restored in `finally`; no OpenLDAP fixture
file is modified.
