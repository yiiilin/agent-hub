# Administration Browser Lifecycle

Type: `browser`

This scenario uses the real backend-hosted console and PostgreSQL APIs. In
English at desktop width it verifies current-user Display Name editing, Local
Password/registration/LDAP policy controls, LDAP configuration fields including
the escaped Bind identity template, disabled states, persistent Plain LDAP and
registration warnings, and policy
persistence. The LDAP draft-test request is intercepted as a classified `503`
to verify the error and one-time credential-clearing UI without contacting an
LDAP service.

The user workflow covers duplicate-email error feedback, disabled short-
password submission, Super Administrator account creation, details, email and
Display Name editing, Session invalidation, password reset, role change, and
email-confirmed deletion. A separately provisioned member remains authenticated
as an isolation control. Current-user deletion stays disabled.

The scenario also creates a uniquely keyed External Platform. It switches to
Simplified Chinese at 390px and checks authentication warnings, user deletion
history, platform tables, horizontal overflow, and browser console/network
diagnostics. Authentication policy, LDAP configuration, and the current Super
Administrator Display Name are restored in `finally`.
