ALTER TABLE public.model_connections
    ADD COLUMN upstream_protocol text NOT NULL DEFAULT 'openai_responses';

ALTER TABLE ONLY public.model_connections
    ADD CONSTRAINT model_connections_upstream_protocol_check
    CHECK (upstream_protocol = ANY (ARRAY[
        'openai_responses'::text,
        'anthropic_messages'::text
    ]));

ALTER TABLE public.run_model_connection_snapshots
    ADD COLUMN upstream_protocol text NOT NULL DEFAULT 'openai_responses';

ALTER TABLE ONLY public.run_model_connection_snapshots
    ADD CONSTRAINT run_model_connection_snapshots_upstream_protocol_check
    CHECK (upstream_protocol = ANY (ARRAY[
        'openai_responses'::text,
        'anthropic_messages'::text
    ]));

ALTER TABLE public.model_token_usage
    ADD COLUMN upstream_protocol_snapshot text NOT NULL DEFAULT 'openai_responses';

ALTER TABLE ONLY public.model_token_usage
    ADD CONSTRAINT model_token_usage_upstream_protocol_snapshot_check
    CHECK (upstream_protocol_snapshot = ANY (ARRAY[
        'openai_responses'::text,
        'anthropic_messages'::text
    ]));

ALTER TABLE public.model_call_errors
    ADD COLUMN upstream_protocol_snapshot text NOT NULL DEFAULT 'openai_responses';

ALTER TABLE ONLY public.model_call_errors
    ADD CONSTRAINT model_call_errors_upstream_protocol_snapshot_check
    CHECK (upstream_protocol_snapshot = ANY (ARRAY[
        'openai_responses'::text,
        'anthropic_messages'::text
    ]));
