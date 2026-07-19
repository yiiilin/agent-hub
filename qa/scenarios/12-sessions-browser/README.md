# Browser Session Lifecycle

Type: `browser`

This scenario signs in through the real console and uses authenticated browser
requests only to create and later delete its Agent. Conversation creation,
steering, stopping, and post-stop continuation all run through the Session UI.

It verifies an exact `fixture:hold` Turn stays active for steering, technical
events remain ordered and folded in the transcript, Stop reaches `interrupted`
without removing messages, and a subsequent Turn reuses the native Thread. It
also covers independent Sessions, search and Origin filtering, exact old-SSE
abort diagnostics, desktop and 390x844 overflow, the mobile Session drawer, and
read-only historical messages after Agent deletion.
