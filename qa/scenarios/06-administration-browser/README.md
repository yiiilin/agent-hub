# Administration Browser Lifecycle

Type: `browser`

This scenario uses the real Compose backend-hosted frontend and PostgreSQL database.
It registers a unique member through the public password API, signs in as the
seeded Super Administrator through the browser, and verifies all three
Administration tabs.

The browser changes and saves one authentication-policy value, creates and
renames a uniquely keyed External Platform, and creates then edits its nested
Authentication Channel, including the enabled and trusted-email flags. It also
combines real role APIs with the UI role display, checks the last-Super-
Administrator and current-user deletion protections, opens user details, resets
the member password, and proves the old member session is invalidated.

The authentication policy is snapshotted before mutation and is always restored
and read back in `finally`. A restoration failure fails the scenario. External
Platforms and Authentication Channels do not have delete APIs, so their unique
records remain only until the disposable QA database is removed.

The exact-username member deletion is intentionally the final functional
mutation. It is irreversible and is not restored; the scenario waits for the
member row to disappear and for deletion history to appear. The mandatory global
policy restoration still runs afterward. Desktop and 390px mobile checks include
directly visible English and Simplified Chinese Administration tables, horizontal
overflow checks, and zero browser diagnostics.
