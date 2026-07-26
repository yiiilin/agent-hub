# Reserve Agent for the domain entity

Agent Hub uses **Agent** only for the user-configured domain entity. Runtime control-plane contracts use **Execution Engine**, **Runtime Engine Version**, **Native Session**, and **Native Turn**, while implementation details use **Pi**; this avoids confusing Agent configuration revisions with executable or session state. The obsolete Codex binary rollout surface is removed because Pi is delivered only as part of a complete Runtime image.
