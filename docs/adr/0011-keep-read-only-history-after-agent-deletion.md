# Keep read-only history after Agent deletion

Deleting an Agent is irreversible and does not archive a restorable Agent. Agent Hub cancels unfinished Runs and removes the Agent's instructions, Skills, MCP and OAuth credentials, automations, Workspaces, and Session Bundles, while retaining a minimal display snapshot plus each Session's messages, completed Run and tool records, and historical attachments as view-only Historical Sessions. No new Run can start from those records, and erasing the owning Hub User still removes them under the user-erasure policy.
