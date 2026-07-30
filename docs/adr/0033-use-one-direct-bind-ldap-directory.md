---
status: accepted
---

# Use one direct-bind LDAP directory

Agent Hub supports one database-managed LDAP Directory and authenticates by substituting the user's complete email into one administrator-configured Bind identity template, binding directly with that identity and password, then performing one Subtree profile query under a configured Base DN. The default `{email}` supports AD/UPN, while fixed-DN directories can use a template such as `uid={email},ou=people,dc=example,dc=test`; substituted values are DN-escaped. Agent Hub deliberately stores no service-account credential, permanent LDAP user ID, group-role mapping, server failover list, or custom CA. This keeps deployment and secret ownership small, while accepting that Hub cannot proactively detect a disabled directory account, existing browser Sessions remain valid until their normal revocation or expiry, and directory high availability must be supplied through DNS or external network infrastructure.
