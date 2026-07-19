# API Key Browser Lifecycle

Type: `browser`

This scenario directly asserts the AUTH-002 password session lifecycle through
the real login UI, authenticated `/auth/me`, Sessions-first navigation, logout,
and post-logout rejection. It then directly asserts AUTH-003 through the real
Mock OIDC UI with a unique email and verifies the resulting Sessions-first user.

For AUTH-004 it creates a uniquely named 180-day API key through the UI, checks
that the one-time credential is confined to the creation dialog, verifies that
the copy control is available on hover and keyboard focus, and confirms that the
row offers Delete without Revoke. An independent request context proves the key
authenticates, renewal returns no replacement and preserves authentication, and
physical deletion immediately rejects the old credential. Assertions use the
created row rather than fixed list lengths.

The one-time credential exists only in scenario memory. Browser tracing is
paused before creation, the secret dialog is closed in a `finally` block, and
tracing resumes only after the credential is gone. Failure messages, diagnostics,
and this document never include the credential value; successful runs do not
write a trace. The scenario also checks horizontal overflow at 1280px and 390px
and requires empty browser diagnostics. Chromium reports the two successful 204
fetches for Delete and logout as aborted after the SPA moves on; the scenario
allows only those exact method-and-URL events after first asserting their 204
responses, while every other browser diagnostic remains fatal.
