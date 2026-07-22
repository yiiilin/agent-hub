ALTER TABLE public.model_connections
    ADD COLUMN request_parameters jsonb NOT NULL
        DEFAULT '{"protocol":"openai_responses"}'::jsonb;

ALTER TABLE public.run_model_connection_snapshots
    ADD COLUMN request_parameters jsonb NOT NULL
        DEFAULT '{"protocol":"openai_responses"}'::jsonb;

ALTER TABLE public.model_token_usage
    ADD COLUMN request_parameters_snapshot jsonb NOT NULL
        DEFAULT '{"protocol":"openai_responses"}'::jsonb;

ALTER TABLE public.model_call_errors
    ADD COLUMN request_parameters_snapshot jsonb NOT NULL
        DEFAULT '{"protocol":"openai_responses"}'::jsonb;

ALTER TABLE ONLY public.model_connections
    DROP CONSTRAINT model_connections_upstream_protocol_check,
    ADD CONSTRAINT model_connections_upstream_protocol_check
        CHECK (upstream_protocol = ANY (ARRAY[
            'openai_responses'::text,
            'openai_chat_completions'::text,
            'anthropic_messages'::text
        ])),
    ADD CONSTRAINT model_connections_request_parameters_check
        CHECK (
            jsonb_typeof(request_parameters) = 'object'
            AND request_parameters ? 'protocol'
            AND request_parameters->>'protocol' = upstream_protocol
            AND (
                (upstream_protocol = 'openai_responses'
                    AND request_parameters = '{"protocol":"openai_responses"}'::jsonb)
                OR (
                    upstream_protocol = 'openai_chat_completions'
                    AND (request_parameters - ARRAY[
                        'protocol', 'temperature', 'top_p', 'max_completion_tokens'
                    ]) = '{}'::jsonb
                    AND (
                        request_parameters->'temperature' IS NULL
                        OR jsonb_typeof(request_parameters->'temperature') IN ('number', 'null')
                    )
                    AND (
                        request_parameters->'top_p' IS NULL
                        OR jsonb_typeof(request_parameters->'top_p') IN ('number', 'null')
                    )
                    AND (
                        request_parameters->'max_completion_tokens' IS NULL
                        OR jsonb_typeof(request_parameters->'max_completion_tokens') IN ('number', 'null')
                    )
                )
                OR (
                    upstream_protocol = 'anthropic_messages'
                    AND (request_parameters - ARRAY[
                        'protocol', 'temperature', 'top_p', 'max_tokens'
                    ]) = '{}'::jsonb
                    AND (
                        request_parameters->'temperature' IS NULL
                        OR jsonb_typeof(request_parameters->'temperature') IN ('number', 'null')
                    )
                    AND (
                        request_parameters->'top_p' IS NULL
                        OR jsonb_typeof(request_parameters->'top_p') IN ('number', 'null')
                    )
                    AND (
                        request_parameters->'max_tokens' IS NULL
                        OR jsonb_typeof(request_parameters->'max_tokens') IN ('number', 'null')
                    )
                    AND NOT (
                        jsonb_typeof(request_parameters->'temperature') = 'number'
                        AND jsonb_typeof(request_parameters->'top_p') = 'number'
                    )
                )
            )
        );

ALTER TABLE ONLY public.run_model_connection_snapshots
    DROP CONSTRAINT run_model_connection_snapshots_upstream_protocol_check,
    ADD CONSTRAINT run_model_connection_snapshots_upstream_protocol_check
        CHECK (upstream_protocol = ANY (ARRAY[
            'openai_responses'::text,
            'openai_chat_completions'::text,
            'anthropic_messages'::text
        ])),
    ADD CONSTRAINT run_model_connection_snapshots_request_parameters_check
        CHECK (
            jsonb_typeof(request_parameters) = 'object'
            AND request_parameters ? 'protocol'
            AND request_parameters->>'protocol' = upstream_protocol
        );

ALTER TABLE ONLY public.model_token_usage
    DROP CONSTRAINT model_token_usage_upstream_protocol_snapshot_check,
    ADD CONSTRAINT model_token_usage_upstream_protocol_snapshot_check
        CHECK (upstream_protocol_snapshot = ANY (ARRAY[
            'openai_responses'::text,
            'openai_chat_completions'::text,
            'anthropic_messages'::text
        ])),
    ADD CONSTRAINT model_token_usage_request_parameters_snapshot_check
        CHECK (
            jsonb_typeof(request_parameters_snapshot) = 'object'
            AND request_parameters_snapshot ? 'protocol'
            AND request_parameters_snapshot->>'protocol' = upstream_protocol_snapshot
        );

ALTER TABLE ONLY public.model_call_errors
    DROP CONSTRAINT model_call_errors_upstream_protocol_snapshot_check,
    ADD CONSTRAINT model_call_errors_upstream_protocol_snapshot_check
        CHECK (upstream_protocol_snapshot = ANY (ARRAY[
            'openai_responses'::text,
            'openai_chat_completions'::text,
            'anthropic_messages'::text
        ])),
    ADD CONSTRAINT model_call_errors_request_parameters_snapshot_check
        CHECK (
            jsonb_typeof(request_parameters_snapshot) = 'object'
            AND request_parameters_snapshot ? 'protocol'
            AND request_parameters_snapshot->>'protocol' = upstream_protocol_snapshot
        );
