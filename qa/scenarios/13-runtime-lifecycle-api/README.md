# Runtime lifecycle API

Exercises administrator and Runtime HTTP APIs against the isolated Compose
environment. The scenario verifies secret-once enrollment, rejected revoked,
consumed, and invalid enrollment credentials, Runtime-completed credential
rotation, Agent Runtime constraints, generation-fenced Run execution, drain and
cancel behavior, ordinary deletion, and force deletion with uncheckpointed
owned Sessions.

All behavioral assertions use public HTTP responses. The scenario does not use
PostgreSQL queries and does not fabricate `waiting_tool` or tool finalization.
Every Runtime and Agent is uniquely created by the scenario; the Compose
Runtime is only read as a compatible registration template and is never
modified. Cleanup force-deletes any surviving scenario Runtime, deletes the
Agent, and revokes any still-unused enrollment. Consumed and revoked enrollment
rows remain as the API's secret-free audit history because no public deletion
operation exists for them.
