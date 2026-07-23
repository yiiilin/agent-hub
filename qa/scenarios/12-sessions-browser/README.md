# Browser Session Lifecycle

Type: `browser`

This scenario signs in through the real console and uses authenticated browser
requests only to create and later delete its Agent. Conversation creation and
normal continuation run through the Session UI.

It verifies Enter sends, Shift+Enter inserts a newline, the composer grows from
two to five lines, delivered messages leave the queued state, and both assistant
answers survive a two-Run Session reload. A normal subsequent Turn reuses the
native Pi Session, while an independent Session receives a different native Pi
Session id. Assistant assertions use message presence and count rather than
unstable model wording. It also covers Agent and platform filtering, exact old
SSE abort diagnostics, desktop and 390x844 overflow, the mobile Session drawer,
and read-only historical messages after Agent deletion.
