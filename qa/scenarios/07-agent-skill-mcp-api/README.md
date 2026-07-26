# Agent, Skill, and MCP API lifecycle

Exercises the real Agent and Skill APIs against an isolated Compose project,
then inspects the Runtime-owned Session directory. It covers visibility and
ownership boundaries, typed model and Subagent configuration, Skill revision
and atomic bulk deletion, between-Turn materialization refresh, MCP placeholder
preservation and redaction, Pi's explicit rejection of unsupported MCP/Subagent
execution, private Pi configuration, and Historical Session read-only behavior
after Agent deletion.

The MCP secret is generated in memory. Assertions use a non-secret prefix so
failure artifacts and Compose command lines cannot disclose the credential.
