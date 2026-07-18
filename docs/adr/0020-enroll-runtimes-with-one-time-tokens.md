# Enroll Runtimes with one-time tokens

Agent Hub does not give every Runtime a shared registration secret. An Administrator creates a Runtime Enrollment Token in the administration interface; the first Runtime to consume it atomically creates one new immutable Runtime identity and receives its own revocable credential, after which the enrollment token cannot be reused. Deleting that Runtime revokes its credential and does not let the process silently enroll again; joining again requires a new administrator-issued token.

An enrollment token expires thirty minutes after creation and may be revoked earlier while unused. Its plaintext is displayed only once, and Agent Hub stores only a cryptographic hash, consumption time, expiry, revocation state, creator, and audit timestamps.

The resulting Runtime Credential is a separate high-entropy secret stored in the Runtime's operating-system-protected persistent configuration, while Agent Hub stores only its hash. It survives process restarts, has no fixed expiry, and never enters a Session directory, Session Bundle, event payload, URL, or log. An Administrator may rotate it, and deleting the Runtime revokes it immediately.

An ordinary credential rotation is requested by an Administrator but completed by the Runtime itself on a later authenticated heartbeat. The Runtime generates and stores the replacement secret locally; Agent Hub confirms that the replacement credential works before revoking the old one, and neither plaintext is exposed in the administration interface. A request remains pending while the Runtime is offline. Suspected credential compromise uses Runtime deletion for immediate revocation instead of this non-disruptive rotation path.
