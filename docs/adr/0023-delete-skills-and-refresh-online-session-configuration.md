# Delete Skills and refresh online Session configuration

Hub-managed Skills are physically deleted rather than archived, and Agent Hub does not support Agent-inline Skills. Deleting a Skill atomically removes all Agent bindings and requests a generation-fenced configuration refresh for every affected online Session. An idle Session applies the latest complete Agent Execution Configuration immediately; an active Session waits until its current Turn reaches a terminal state. Runtime uses the existing atomic materialization boundary, does not restart app-server, and never modifies the Session Workspace. Offline Sessions have no generated Skill files to clean and materialize the latest configuration before their next Turn.

This accepts irreversible deletion and delayed cleanup during an active Turn in exchange for one authoritative Skill source, stable in-flight execution, and removal of deleted derived files without waiting for another user message.
