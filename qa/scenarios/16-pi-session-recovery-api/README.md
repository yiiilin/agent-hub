# Pi Session cold recovery API smoke

Type: `api`

This scenario uses the Compose-provided Pi Runtime rather than a registered
HTTP fixture. It temporarily shortens the Runtime idle timeout, completes a Pi
Turn, observes the committed Bundle and deleted local Session root, then
restarts the Runtime and completes a second Turn in the same Pi Session.

The smoke asserts that the compatibility `native_thread_id` remains the same Pi
Session id across recovery. Before checkpointing it writes a unique sentinel
into generated Pi configuration, then updates the Agent instructions while the
Session is offline. After recovery, the sentinel is absent, the new instructions
and model configuration have been materialized, and the second Turn completes.
The scenario restores the Runtime's normal idle-timeout configuration before it
returns so later selected scenarios remain isolated.
