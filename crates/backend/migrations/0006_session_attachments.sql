CREATE TABLE public.hub_session_attachments (
    id uuid NOT NULL,
    session_id uuid NOT NULL,
    owner_id uuid NOT NULL,
    message_id uuid,
    run_id uuid,
    name text NOT NULL,
    content_type text NOT NULL,
    size_bytes bigint NOT NULL,
    object_key text NOT NULL,
    checksum_sha256 text NOT NULL,
    created_at timestamp with time zone NOT NULL DEFAULT now(),
    CONSTRAINT hub_session_attachments_pkey PRIMARY KEY (id),
    CONSTRAINT hub_session_attachments_session_id_fkey
        FOREIGN KEY (session_id) REFERENCES public.hub_sessions(id) ON DELETE CASCADE,
    CONSTRAINT hub_session_attachments_owner_id_fkey
        FOREIGN KEY (owner_id) REFERENCES public.users(id) ON DELETE CASCADE,
    CONSTRAINT hub_session_attachments_size_nonnegative CHECK (size_bytes >= 0),
    CONSTRAINT hub_session_attachments_name_nonempty CHECK (btrim(name) <> ''),
    CONSTRAINT hub_session_attachments_checksum_shape
        CHECK (checksum_sha256 ~ '^[0-9a-f]{64}$')
);

CREATE INDEX hub_session_attachments_session_idx
    ON public.hub_session_attachments (session_id, created_at);
CREATE INDEX hub_session_attachments_orphan_idx
    ON public.hub_session_attachments (created_at)
    WHERE message_id IS NULL;

ALTER TABLE public.model_connections
    ADD COLUMN vision_model_id text;
