# Session Run API lifecycle

Exercises completed console Sessions and Runs through the real Compose API and
Pi Runtime. It creates an explicit fake-provider model fixture for the isolated
Compose environment, then verifies ordered event history and authenticated SSE
for a completed native Pi Turn. A normal second Turn retains the same native Pi
Session id while an independent Session receives a different id.

The scenario also checks that a second ordinary user cannot read the owner's
Sessions, and Agent deletion preserves historical Session identities while
rejecting new messages. It does not depend on synthetic held Turns, fixed model
text, or an app-server-specific native Turn fixture.
