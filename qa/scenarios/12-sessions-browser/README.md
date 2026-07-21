# Browser Session Lifecycle

Type: `browser`

This scenario signs in through the real console and uses authenticated browser
requests only to create and later delete its Agent. Conversation creation,
steering, stopping, and post-stop continuation all run through the Session UI.

It verifies Enter sends, Shift+Enter inserts a newline, the composer grows from
two to five lines, delivered messages leave the queued state, and both assistant
answers survive a two-Run Session reload. An exact `fixture:hold` Turn stays
active for steering, readable Codex activity remains ordered and folded in the
transcript, Stop reaches `interrupted` without removing messages, and a
subsequent Turn reuses the native Thread. It also covers independent Sessions,
platform-first and Agent filtering, exact old-SSE abort diagnostics, desktop and
390x844 overflow, the mobile Session drawer, and read-only historical messages
after Agent deletion.
