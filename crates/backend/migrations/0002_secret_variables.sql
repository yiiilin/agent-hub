CREATE TABLE public.user_secrets (
    id uuid NOT NULL,
    owner_id uuid NOT NULL,
    name text NOT NULL,
    kind text NOT NULL,
    value_ciphertext bytea,
    value_nonce bytea,
    file_ciphertext bytea,
    file_nonce bytea,
    file_name text,
    file_size_bytes bigint,
    file_sha256 text,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT user_secrets_pkey PRIMARY KEY (id),
    CONSTRAINT user_secrets_owner_name_key UNIQUE (owner_id, name),
    CONSTRAINT user_secrets_name_format CHECK ((name ~ '^[A-Z_][A-Z0-9_]*$'::text)),
    CONSTRAINT user_secrets_name_length CHECK ((char_length(name) <= 128)),
    CONSTRAINT user_secrets_kind_check CHECK ((kind = ANY (ARRAY['value'::text, 'file'::text]))),
    CONSTRAINT user_secrets_value_shape CHECK (
        ((kind = 'value'::text)
            AND (value_ciphertext IS NOT NULL)
            AND (value_nonce IS NOT NULL)
            AND (octet_length(value_nonce) = 12)
            AND (file_ciphertext IS NULL)
            AND (file_nonce IS NULL)
            AND (file_name IS NULL)
            AND (file_size_bytes IS NULL)
            AND (file_sha256 IS NULL))
        OR ((kind = 'file'::text)
            AND (value_ciphertext IS NULL)
            AND (value_nonce IS NULL)
            AND (file_ciphertext IS NOT NULL)
            AND (file_nonce IS NOT NULL)
            AND (octet_length(file_nonce) = 12)
            AND (file_name IS NOT NULL)
            AND (btrim(file_name) <> ''::text)
            AND (file_size_bytes > 0)
            AND (file_size_bytes <= 1048576)
            AND (file_sha256 ~ '^[0-9a-f]{64}$'::text))
    ),
    CONSTRAINT user_secrets_value_size CHECK (
        (kind <> 'value'::text) OR (octet_length(value_ciphertext) <= 16384)
    )
);

CREATE INDEX user_secrets_owner_created_idx
    ON public.user_secrets USING btree (owner_id, created_at DESC, id);

ALTER TABLE ONLY public.user_secrets
    ADD CONSTRAINT user_secrets_owner_id_fkey
    FOREIGN KEY (owner_id) REFERENCES public.users(id) ON DELETE CASCADE;

CREATE TABLE public.agent_secret_declarations (
    agent_id uuid NOT NULL,
    name text NOT NULL,
    kind text NOT NULL,
    description text DEFAULT ''::text NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT agent_secret_declarations_pkey PRIMARY KEY (agent_id, name),
    CONSTRAINT agent_secret_declarations_name_format CHECK ((name ~ '^[A-Z_][A-Z0-9_]*$'::text)),
    CONSTRAINT agent_secret_declarations_name_length CHECK ((char_length(name) <= 128)),
    CONSTRAINT agent_secret_declarations_kind_check CHECK ((kind = ANY (ARRAY['value'::text, 'file'::text]))),
    CONSTRAINT agent_secret_declarations_description_length CHECK ((char_length(description) <= 512))
);

ALTER TABLE ONLY public.agent_secret_declarations
    ADD CONSTRAINT agent_secret_declarations_agent_id_fkey
    FOREIGN KEY (agent_id) REFERENCES public.agents(id) ON DELETE CASCADE;

CREATE TABLE public.secret_grants (
    user_id uuid NOT NULL,
    agent_id uuid NOT NULL,
    secret_name text NOT NULL,
    granted_at timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT secret_grants_pkey PRIMARY KEY (user_id, agent_id, secret_name),
    CONSTRAINT secret_grants_name_format CHECK ((secret_name ~ '^[A-Z_][A-Z0-9_]*$'::text)),
    CONSTRAINT secret_grants_name_length CHECK ((char_length(secret_name) <= 128))
);

CREATE INDEX secret_grants_user_agent_idx
    ON public.secret_grants USING btree (user_id, agent_id, secret_name);

ALTER TABLE ONLY public.secret_grants
    ADD CONSTRAINT secret_grants_agent_id_fkey
    FOREIGN KEY (agent_id) REFERENCES public.agents(id) ON DELETE CASCADE;

ALTER TABLE ONLY public.secret_grants
    ADD CONSTRAINT secret_grants_user_id_fkey
    FOREIGN KEY (user_id) REFERENCES public.users(id) ON DELETE CASCADE;
