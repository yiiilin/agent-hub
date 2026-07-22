ALTER TABLE public.model_connections
    ADD COLUMN reasoning_effort text NOT NULL DEFAULT 'default',
    ADD COLUMN reasoning_summary text NOT NULL DEFAULT 'default',
    ADD COLUMN verbosity text NOT NULL DEFAULT 'default',
    ADD COLUMN context_window_tokens bigint,
    ADD COLUMN auto_compact_token_limit bigint,
    ADD COLUMN reasoning_summary_support text NOT NULL DEFAULT 'auto',
    ADD COLUMN service_tier text,
    ADD COLUMN request_max_retries integer,
    ADD COLUMN stream_max_retries integer,
    ADD COLUMN stream_idle_timeout_ms bigint;

ALTER TABLE ONLY public.model_connections
    ADD CONSTRAINT model_connections_reasoning_effort_check
        CHECK (reasoning_effort = ANY (ARRAY[
            'default'::text, 'none'::text, 'minimal'::text, 'low'::text,
            'medium'::text, 'high'::text, 'xhigh'::text, 'max'::text,
            'ultra'::text
        ])),
    ADD CONSTRAINT model_connections_reasoning_summary_check
        CHECK (reasoning_summary = ANY (ARRAY[
            'default'::text, 'auto'::text, 'concise'::text,
            'detailed'::text, 'none'::text
        ])),
    ADD CONSTRAINT model_connections_verbosity_check
        CHECK (verbosity = ANY (ARRAY[
            'default'::text, 'low'::text, 'medium'::text, 'high'::text
        ])),
    ADD CONSTRAINT model_connections_context_window_check
        CHECK (context_window_tokens IS NULL OR context_window_tokens > 0),
    ADD CONSTRAINT model_connections_auto_compact_check
        CHECK (
            auto_compact_token_limit IS NULL
            OR (
                auto_compact_token_limit > 0
                AND (
                    context_window_tokens IS NULL
                    OR auto_compact_token_limit <= context_window_tokens
                )
            )
        ),
    ADD CONSTRAINT model_connections_reasoning_summary_support_check
        CHECK (reasoning_summary_support = ANY (ARRAY[
            'auto'::text, 'supported'::text, 'unsupported'::text
        ])),
    ADD CONSTRAINT model_connections_service_tier_check
        CHECK (
            service_tier IS NULL
            OR (btrim(service_tier) <> ''::text AND char_length(service_tier) <= 64)
        ),
    ADD CONSTRAINT model_connections_request_retries_check
        CHECK (
            request_max_retries IS NULL
            OR request_max_retries BETWEEN 0 AND 100
        ),
    ADD CONSTRAINT model_connections_stream_retries_check
        CHECK (
            stream_max_retries IS NULL
            OR stream_max_retries BETWEEN 0 AND 100
        ),
    ADD CONSTRAINT model_connections_stream_idle_timeout_check
        CHECK (stream_idle_timeout_ms IS NULL OR stream_idle_timeout_ms > 0);
