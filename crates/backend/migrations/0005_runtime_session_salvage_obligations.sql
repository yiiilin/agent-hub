CREATE TABLE public.runtime_session_salvage_obligations (
    runtime_id uuid NOT NULL,
    session_id uuid NOT NULL,
    ownership_generation bigint NOT NULL,
    history_checkpoint bigint NOT NULL,
    bundle_generation bigint NOT NULL,
    attempts integer NOT NULL DEFAULT 0,
    next_attempt_at timestamp with time zone NOT NULL DEFAULT now(),
    created_at timestamp with time zone NOT NULL DEFAULT now(),
    updated_at timestamp with time zone NOT NULL DEFAULT now(),
    PRIMARY KEY (runtime_id, session_id, ownership_generation)
);

CREATE INDEX runtime_session_salvage_obligations_due_idx
    ON public.runtime_session_salvage_obligations (next_attempt_at);
