# Bundle and recovery API

Exercises Session ownership, checkpoint, Bundle, release, and recovery behavior
through administrator and Runtime HTTP APIs. A small deterministic empty
`tar.zst` stream is sufficient because the Hub treats Bundle bodies as opaque;
archive contents and exclusions remain the Rust-only `BND-001` seam and are not
claimed here.

The scenario commits one generation, proves malformed metadata and a failed
object-store checksum upload leave that generation current, then commits the
next generation. It then releases ownership explicitly and checks that the
Session is waiting for a Runtime with the committed Bundle and no owner. A
second temporary Runtime downloads the exact bytes and metadata and resumes the
existing Native Session rather than creating a replacement. A separate Session
proves that force-deleting its Runtime without a current Bundle exposes
`recovery_failed`, an error, and read-only behavior through the Session API.

Checkpoint failure checks reject an empty error, preserve the attempt result
idempotently, leave the Session saving, and do not replace its current Bundle.
The submitted diagnostic is not expected in `HubSession.recovery_error`. All
temporary Agents and Runtime identities are removed in cleanup without
modifying the Compose-provided Runtime.
