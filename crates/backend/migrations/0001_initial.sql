-- Agent Hub v1 initial schema.
SET LOCAL check_function_bodies = false;

CREATE EXTENSION IF NOT EXISTS pgcrypto WITH SCHEMA public;

CREATE FUNCTION public.anonymize_model_accounting_before_user_delete() RETURNS trigger
    LANGUAGE plpgsql
    AS $$
BEGIN
    IF OLD.role = 'super_admin' THEN
        UPDATE model_token_usage AS ledger
        SET super_admin_protected = true
        WHERE ledger.subject_user_id = OLD.id
           OR EXISTS (
                SELECT 1 FROM agents
                WHERE agents.id = ledger.agent_id
                  AND agents.owner_id = OLD.id
           )
           OR EXISTS (
                SELECT 1 FROM model_connections
                WHERE model_connections.id = ledger.model_connection_id
                  AND model_connections.owner_id = OLD.id
           )
           OR EXISTS (
                SELECT 1 FROM oauth_apps
                WHERE oauth_apps.id = ledger.source_integration_app_id
                  AND oauth_apps.owner_id = OLD.id
           );

        UPDATE model_call_errors AS ledger
        SET super_admin_protected = true
        WHERE ledger.subject_user_id = OLD.id
           OR EXISTS (
                SELECT 1 FROM agents
                WHERE agents.id = ledger.agent_id
                  AND agents.owner_id = OLD.id
           )
           OR EXISTS (
                SELECT 1 FROM model_connections
                WHERE model_connections.id = ledger.model_connection_id
                  AND model_connections.owner_id = OLD.id
           )
           OR EXISTS (
                SELECT 1 FROM oauth_apps
                WHERE oauth_apps.id = ledger.source_integration_app_id
                  AND oauth_apps.owner_id = OLD.id
           );
    END IF;

    UPDATE model_token_usage
    SET subject_user_id = NULL,
        subject_display_name_snapshot = NULL
    WHERE subject_user_id = OLD.id;

    UPDATE model_call_errors
    SET subject_user_id = NULL,
        subject_display_name_snapshot = NULL
    WHERE subject_user_id = OLD.id;

    UPDATE model_connections
    SET owner_id = NULL,
        base_url = NULL,
        api_key_ciphertext = NULL,
        api_key_nonce = NULL,
        enabled = false,
        deleted_at = COALESCE(deleted_at, CURRENT_TIMESTAMP(3)),
        updated_at = CURRENT_TIMESTAMP(3)
    WHERE owner_id = OLD.id;

    RETURN OLD;
END
$$;

CREATE FUNCTION public.assign_hub_session_message_sequence() RETURNS trigger
    LANGUAGE plpgsql
    AS $$
DECLARE
    next_sequence BIGINT;
BEGIN
    PERFORM 1
    FROM hub_sessions
    WHERE id = NEW.session_id
    FOR UPDATE;

    IF NOT FOUND THEN
        RAISE EXCEPTION USING
            ERRCODE = '23503',
            MESSAGE = 'Hub Session does not exist';
    END IF;

    SELECT COALESCE(max(messages.sequence), 0) + 1
    INTO next_sequence
    FROM hub_session_messages AS messages
    WHERE messages.session_id = NEW.session_id;

    IF NEW.sequence IS NULL THEN
        NEW.sequence := next_sequence;
    ELSIF NEW.sequence <> next_sequence THEN
        RAISE EXCEPTION USING
            ERRCODE = '23514',
            MESSAGE = format(
                'Hub Session message sequence must be %s, received %s',
                next_sequence,
                NEW.sequence
            );
    END IF;

    RETURN NEW;
END
$$;

CREATE FUNCTION public.enforce_hub_run_session_links() RETURNS trigger
    LANGUAGE plpgsql
    AS $$
DECLARE
    session_owner_id UUID;
    session_agent_id UUID;
BEGIN
    IF TG_OP = 'UPDATE'
        AND OLD.hub_session_id IS NOT NULL
        AND NEW.hub_session_id IS DISTINCT FROM OLD.hub_session_id
    THEN
        RAISE EXCEPTION USING
            ERRCODE = '23514',
            MESSAGE = 'Hub Run Session binding is immutable';
    END IF;

    IF NEW.hub_session_id IS NULL THEN
        IF NEW.hub_turn_id IS NOT NULL OR NEW.hub_message_id IS NOT NULL THEN
            RAISE EXCEPTION USING
                ERRCODE = '23514',
                MESSAGE = 'Hub Run Turn and Message links require a Hub Session';
        END IF;
        RETURN NEW;
    END IF;

    SELECT sessions.owner_id, sessions.agent_id
    INTO session_owner_id, session_agent_id
    FROM hub_sessions AS sessions
    WHERE sessions.id = NEW.hub_session_id;

    IF NOT FOUND THEN
        RAISE EXCEPTION USING
            ERRCODE = '23503',
            MESSAGE = 'Hub Run Session does not exist';
    END IF;

    IF NEW.owner_id <> session_owner_id OR NEW.agent_id <> session_agent_id THEN
        RAISE EXCEPTION USING
            ERRCODE = '23514',
            MESSAGE = 'Hub Run owner and Agent must match its Hub Session';
    END IF;

    RETURN NEW;
END
$$;

CREATE FUNCTION public.enforce_hub_session_invariants() RETURNS trigger
    LANGUAGE plpgsql
    AS $$
BEGIN
    IF TG_OP = 'UPDATE' THEN
        IF NEW.owner_id IS DISTINCT FROM OLD.owner_id
            OR NEW.agent_id IS DISTINCT FROM OLD.agent_id
            OR NEW.origin_kind IS DISTINCT FROM OLD.origin_kind
            OR NEW.origin_platform_id IS DISTINCT FROM OLD.origin_platform_id
            OR NEW.origin_tenant_id IS DISTINCT FROM OLD.origin_tenant_id
            OR NEW.origin_external_identity_id IS DISTINCT FROM OLD.origin_external_identity_id
        THEN
            RAISE EXCEPTION USING
                ERRCODE = '23514',
                MESSAGE = 'Hub Session owner, Agent, and origin are immutable';
        END IF;

        IF NEW.ownership_generation < OLD.ownership_generation THEN
            RAISE EXCEPTION USING
                ERRCODE = '23514',
                MESSAGE = 'Hub Session ownership generation cannot decrease';
        END IF;

        IF OLD.native_thread_id IS NOT NULL
            AND NEW.native_thread_id IS DISTINCT FROM OLD.native_thread_id
        THEN
            RAISE EXCEPTION USING
                ERRCODE = '23514',
                MESSAGE = 'native Thread binding cannot be cleared or replaced';
        END IF;

        IF NEW.history_checkpoint < OLD.history_checkpoint THEN
            RAISE EXCEPTION USING
                ERRCODE = '23514',
                MESSAGE = 'Hub Session history checkpoint cannot decrease';
        END IF;

        IF NEW.runtime_owner_id IS DISTINCT FROM OLD.runtime_owner_id
            AND NEW.runtime_owner_id IS NOT NULL
            AND NEW.ownership_generation <= OLD.ownership_generation
        THEN
            RAISE EXCEPTION USING
                ERRCODE = '23514',
                MESSAGE = 'acquiring or switching Runtime ownership requires a new generation';
        END IF;
    END IF;

    IF NEW.active_turn_id IS NOT NULL
        AND NOT EXISTS (
            SELECT 1
            FROM hub_session_turns AS turns
            WHERE turns.id = NEW.active_turn_id
              AND turns.session_id = NEW.id
        )
    THEN
        RAISE EXCEPTION USING
            ERRCODE = '23503',
            MESSAGE = 'active Turn must belong to the same Hub Session';
    END IF;

    RETURN NEW;
END
$$;

CREATE FUNCTION public.enforce_hub_session_message_immutability() RETURNS trigger
    LANGUAGE plpgsql
    AS $$
DECLARE
    valid_delivery_transition BOOLEAN;
BEGIN
    valid_delivery_transition :=
        (
            OLD.delivery_mode = 'next_turn'
            AND OLD.expected_native_turn_id IS NULL
            AND NEW.delivery_mode = 'steer'
            AND NEW.expected_native_turn_id IS NOT NULL
            AND btrim(NEW.expected_native_turn_id) <> ''
        )
        OR
        (
            OLD.delivery_mode = 'steer'
            AND OLD.expected_native_turn_id IS NOT NULL
            AND NEW.delivery_mode = 'next_turn'
            AND NEW.expected_native_turn_id IS NULL
        );

    IF NEW.id IS DISTINCT FROM OLD.id
        OR NEW.session_id IS DISTINCT FROM OLD.session_id
        OR NEW.sequence IS DISTINCT FROM OLD.sequence
        OR NEW.role IS DISTINCT FROM OLD.role
        OR NEW.message_kind IS DISTINCT FROM OLD.message_kind
        OR NEW.content IS DISTINCT FROM OLD.content
        OR NEW.payload IS DISTINCT FROM OLD.payload
        OR NEW.accepted_at IS DISTINCT FROM OLD.accepted_at
        OR (
            (
                NEW.delivery_mode IS DISTINCT FROM OLD.delivery_mode
                OR NEW.expected_native_turn_id IS DISTINCT FROM OLD.expected_native_turn_id
            )
            AND NOT valid_delivery_transition
        )
    THEN
        RAISE EXCEPTION USING
            ERRCODE = '23514',
            MESSAGE = 'accepted Hub Session message content and ordering are immutable';
    END IF;

    RETURN NEW;
END
$$;

CREATE FUNCTION public.enforce_hub_session_turn_immutability() RETURNS trigger
    LANGUAGE plpgsql
    AS $$
BEGIN
    IF NEW.id IS DISTINCT FROM OLD.id
        OR NEW.session_id IS DISTINCT FROM OLD.session_id
        OR (
            NEW.ownership_generation IS DISTINCT FROM OLD.ownership_generation
            AND NOT (
                OLD.status = 'pending'
                AND NEW.ownership_generation > OLD.ownership_generation
            )
        )
        OR (
            NEW.configuration_fingerprint IS DISTINCT FROM OLD.configuration_fingerprint
            AND NOT (
                OLD.status = 'pending'
                AND NEW.status = 'starting'
                AND NEW.ownership_generation = OLD.ownership_generation
                AND OLD.delivery_started_at IS NULL
                AND NEW.delivery_started_at IS NOT NULL
                AND NEW.configuration_fingerprint IS NOT NULL
                AND btrim(NEW.configuration_fingerprint) <> ''
            )
        )
        OR NEW.created_at IS DISTINCT FROM OLD.created_at
    THEN
        RAISE EXCEPTION USING
            ERRCODE = '23514',
            MESSAGE = 'Hub Session Turn identity and execution configuration are immutable';
    END IF;

    RETURN NEW;
END
$$;

CREATE FUNCTION public.protect_model_ledger_row() RETURNS trigger
    LANGUAGE plpgsql
    AS $$
BEGIN
    IF TG_OP = 'DELETE' THEN
        RAISE EXCEPTION USING
            ERRCODE = '55000',
            MESSAGE = 'model accounting ledger rows cannot be deleted';
    END IF;

    IF (to_jsonb(NEW) - ARRAY[
            'model_connection_id', 'agent_id', 'subject_user_id',
            'subject_display_name_snapshot', 'source_integration_app_id',
            'super_admin_protected'
        ]) IS DISTINCT FROM
       (to_jsonb(OLD) - ARRAY[
            'model_connection_id', 'agent_id', 'subject_user_id',
            'subject_display_name_snapshot', 'source_integration_app_id',
            'super_admin_protected'
        ]) THEN
        RAISE EXCEPTION USING
            ERRCODE = '55000',
            MESSAGE = 'model accounting ledger facts are immutable';
    END IF;

    IF NOT (
        NEW.model_connection_id IS NOT DISTINCT FROM OLD.model_connection_id
        OR (OLD.model_connection_id IS NOT NULL AND NEW.model_connection_id IS NULL)
    ) OR NOT (
        NEW.agent_id IS NOT DISTINCT FROM OLD.agent_id
        OR (OLD.agent_id IS NOT NULL AND NEW.agent_id IS NULL)
    ) OR NOT (
        NEW.source_integration_app_id IS NOT DISTINCT FROM OLD.source_integration_app_id
        OR (OLD.source_integration_app_id IS NOT NULL
            AND NEW.source_integration_app_id IS NULL)
    ) THEN
        RAISE EXCEPTION USING
            ERRCODE = '55000',
            MESSAGE = 'model accounting references can only be cleared';
    END IF;

    IF NEW.subject_user_id IS DISTINCT FROM OLD.subject_user_id
       OR NEW.subject_display_name_snapshot IS DISTINCT FROM
          OLD.subject_display_name_snapshot THEN
        IF NOT (
            OLD.subject_user_id IS NOT NULL
            AND NEW.subject_user_id IS NULL
            AND NEW.subject_display_name_snapshot IS NULL
        ) THEN
            RAISE EXCEPTION USING
                ERRCODE = '55000',
                MESSAGE = 'model accounting user identity can only be anonymized';
        END IF;
    END IF;

    IF NEW.super_admin_protected IS DISTINCT FROM OLD.super_admin_protected
       AND NOT (
           OLD.super_admin_protected = false
           AND NEW.super_admin_protected = true
       ) THEN
        RAISE EXCEPTION USING
            ERRCODE = '55000',
            MESSAGE = 'model accounting protection can only be enabled';
    END IF;
    RETURN NEW;
END
$$;

CREATE FUNCTION public.protect_super_admin_model_accounting_before_agent_delete() RETURNS trigger
    LANGUAGE plpgsql
    AS $$
BEGIN
    IF EXISTS (
        SELECT 1 FROM users
        WHERE users.id = OLD.owner_id
          AND users.role = 'super_admin'
    ) THEN
        UPDATE model_token_usage
        SET super_admin_protected = true
        WHERE agent_id = OLD.id;

        UPDATE model_call_errors
        SET super_admin_protected = true
        WHERE agent_id = OLD.id;
    END IF;
    RETURN OLD;
END
$$;

CREATE FUNCTION public.protect_super_admin_model_accounting_before_app_delete() RETURNS trigger
    LANGUAGE plpgsql
    AS $$
BEGIN
    IF EXISTS (
        SELECT 1 FROM users
        WHERE users.id = OLD.owner_id
          AND users.role = 'super_admin'
    ) THEN
        UPDATE model_token_usage
        SET super_admin_protected = true
        WHERE source_integration_app_id = OLD.id;

        UPDATE model_call_errors
        SET super_admin_protected = true
        WHERE source_integration_app_id = OLD.id;
    END IF;
    RETURN OLD;
END
$$;

CREATE FUNCTION public.validate_agent_default_model_connection() RETURNS trigger
    LANGUAGE plpgsql
    AS $$
BEGIN
    IF NEW.default_model_connection_id IS NULL THEN
        RETURN NEW;
    END IF;
    IF NOT EXISTS (
        SELECT 1
        FROM model_connections
        WHERE id = NEW.default_model_connection_id
          AND enabled = true
          AND deleted_at IS NULL
          AND (scope = 'global' OR owner_id = NEW.owner_id)
    ) THEN
        RAISE EXCEPTION USING
            ERRCODE = '23514',
            MESSAGE = 'Agent model must be an enabled Global or owner Personal Model Connection';
    END IF;
    RETURN NEW;
END
$$;

CREATE FUNCTION public.validate_subagent_model_connection() RETURNS trigger
    LANGUAGE plpgsql
    AS $$
DECLARE
    agent_owner_id UUID;
BEGIN
    IF NEW.model_connection_id IS NULL THEN
        RETURN NEW;
    END IF;
    SELECT owner_id INTO agent_owner_id FROM agents WHERE id = NEW.agent_id;
    IF agent_owner_id IS NULL OR NOT EXISTS (
        SELECT 1
        FROM model_connections
        WHERE id = NEW.model_connection_id
          AND enabled = true
          AND deleted_at IS NULL
          AND (scope = 'global' OR owner_id = agent_owner_id)
    ) THEN
        RAISE EXCEPTION USING
            ERRCODE = '23514',
            MESSAGE = 'subagent model must be an enabled Global or Agent-owner Personal Model Connection';
    END IF;
    RETURN NEW;
END
$$;

CREATE FUNCTION public.validate_system_default_model_connection() RETURNS trigger
    LANGUAGE plpgsql
    AS $$
BEGIN
    IF NOT EXISTS (
        SELECT 1
        FROM model_connections
        WHERE id = NEW.model_connection_id
          AND scope = 'global'
          AND enabled = true
          AND deleted_at IS NULL
    ) THEN
        RAISE EXCEPTION USING
            ERRCODE = '23514',
            MESSAGE = 'system default must reference an enabled Global Model Connection';
    END IF;
    RETURN NEW;
END
$$;

CREATE TABLE public.agent_skills (
    agent_id uuid NOT NULL,
    skill_id uuid NOT NULL
);

CREATE TABLE public.agents (
    id uuid NOT NULL,
    owner_id uuid NOT NULL,
    name text NOT NULL,
    instructions text NOT NULL,
    visibility text NOT NULL,
    model_policy jsonb DEFAULT '{}'::jsonb NOT NULL,
    deleted_at timestamp with time zone,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    runtime_id uuid,
    mcp_allowlist jsonb DEFAULT '[]'::jsonb NOT NULL,
    sandbox_policy jsonb DEFAULT '{"mode": "workspace-write", "network_access": true}'::jsonb NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL,
    public_to uuid[] DEFAULT '{}'::uuid[] NOT NULL,
    execution_config_revision bigint DEFAULT 1 NOT NULL,
    default_model_connection_id uuid,
    reasoning_effort text DEFAULT 'default'::text NOT NULL,
    CONSTRAINT agents_execution_config_revision_positive CHECK ((execution_config_revision > 0)),
    CONSTRAINT agents_reasoning_effort_check CHECK ((reasoning_effort = ANY (ARRAY['default'::text, 'none'::text, 'minimal'::text, 'low'::text, 'medium'::text, 'high'::text, 'xhigh'::text, 'max'::text, 'ultra'::text]))),
    CONSTRAINT agents_visibility_check CHECK ((visibility = ANY (ARRAY['private'::text, 'public_to'::text, 'public'::text])))
);

CREATE TABLE public.api_keys (
    id uuid NOT NULL,
    user_id uuid NOT NULL,
    name text NOT NULL,
    prefix text NOT NULL,
    token_hash text NOT NULL,
    last_used_at timestamp with time zone,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    expires_at timestamp with time zone,
    CONSTRAINT api_keys_expiration_after_creation CHECK (((expires_at IS NULL) OR (expires_at > created_at)))
);

CREATE TABLE public.auth_policy (
    singleton boolean DEFAULT true NOT NULL,
    password_registration_enabled boolean NOT NULL,
    password_login_enabled boolean NOT NULL,
    email_verification_required boolean NOT NULL,
    updated_by uuid,
    updated_at timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT auth_policy_singleton_check CHECK (singleton)
);

CREATE TABLE public.authentication_channels (
    id uuid NOT NULL,
    platform_id uuid NOT NULL,
    key text NOT NULL,
    name text NOT NULL,
    enabled boolean DEFAULT true NOT NULL,
    trusted_email boolean DEFAULT true NOT NULL,
    created_by uuid,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT authentication_channels_key_check CHECK ((btrim(key) <> ''::text)),
    CONSTRAINT authentication_channels_name_check CHECK ((btrim(name) <> ''::text))
);

CREATE TABLE public.automations (
    id uuid NOT NULL,
    agent_id uuid NOT NULL,
    owner_id uuid NOT NULL,
    name text NOT NULL,
    trigger_type text NOT NULL,
    prompt text NOT NULL,
    schedule text,
    webhook_token_hash text,
    enabled boolean DEFAULT true NOT NULL,
    last_triggered_at timestamp with time zone,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL
);

CREATE TABLE public.codex_subagent_definitions (
    id uuid NOT NULL,
    agent_id uuid NOT NULL,
    name text NOT NULL,
    description text NOT NULL,
    developer_instructions text NOT NULL,
    model_connection_id uuid,
    reasoning_effort text,
    enabled boolean DEFAULT true NOT NULL,
    disabled_reason text,
    created_at timestamp(3) with time zone DEFAULT CURRENT_TIMESTAMP(3) NOT NULL,
    updated_at timestamp(3) with time zone DEFAULT CURRENT_TIMESTAMP(3) NOT NULL,
    CONSTRAINT codex_subagent_description_nonempty CHECK ((btrim(description) <> ''::text)),
    CONSTRAINT codex_subagent_disabled_shape_check CHECK ((((enabled = true) AND (disabled_reason IS NULL)) OR ((enabled = false) AND (disabled_reason IS NOT NULL) AND (btrim(disabled_reason) <> ''::text)))),
    CONSTRAINT codex_subagent_instructions_nonempty CHECK ((btrim(developer_instructions) <> ''::text)),
    CONSTRAINT codex_subagent_name_nonempty CHECK ((btrim(name) <> ''::text)),
    CONSTRAINT codex_subagent_reasoning_effort_check CHECK (((reasoning_effort IS NULL) OR (reasoning_effort = ANY (ARRAY['default'::text, 'none'::text, 'minimal'::text, 'low'::text, 'medium'::text, 'high'::text, 'xhigh'::text, 'max'::text, 'ultra'::text]))))
);

CREATE TABLE public.codex_version_artifacts (
    version text NOT NULL,
    os text NOT NULL,
    architecture text NOT NULL,
    artifact_name text NOT NULL,
    sha256 text NOT NULL,
    size_bytes bigint NOT NULL,
    storage_path text NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT codex_version_artifacts_sha256_check CHECK ((sha256 ~ '^[0-9a-f]{64}$'::text)),
    CONSTRAINT codex_version_artifacts_size_bytes_check CHECK ((size_bytes >= 0))
);

CREATE TABLE public.codex_version_rollout (
    singleton boolean DEFAULT true NOT NULL,
    active_version text,
    target_version text,
    status text DEFAULT 'idle'::text NOT NULL,
    error text,
    attempt_id uuid,
    updated_at timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT codex_version_rollout_singleton_check CHECK (singleton),
    CONSTRAINT codex_version_rollout_status_check CHECK ((status = ANY (ARRAY['idle'::text, 'downloading'::text, 'distributing'::text, 'ready'::text, 'failed'::text, 'active'::text])))
);

CREATE TABLE public.embed_jwt_replays (
    jti text NOT NULL,
    expires_at timestamp with time zone NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL
);

CREATE TABLE public.embed_sessions (
    token_hash text NOT NULL,
    agent_id uuid NOT NULL,
    owner_id uuid NOT NULL,
    expires_at timestamp with time zone NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    last_run_id uuid,
    id uuid DEFAULT gen_random_uuid() NOT NULL,
    hub_session_id uuid NOT NULL,
    oauth_app_id uuid
);

CREATE TABLE public.external_identities (
    id uuid NOT NULL,
    platform_id uuid NOT NULL,
    external_user_id text NOT NULL,
    user_id uuid NOT NULL,
    authentication_channel_id uuid NOT NULL,
    last_email text,
    last_username text,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL,
    tenant_id text DEFAULT 'default'::text NOT NULL,
    CONSTRAINT external_identities_external_user_id_check CHECK ((btrim(external_user_id) <> ''::text)),
    CONSTRAINT external_identities_tenant_nonempty CHECK ((btrim(tenant_id) <> ''::text))
);

CREATE TABLE public.external_platforms (
    id uuid NOT NULL,
    key text NOT NULL,
    name text NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT external_platforms_key_check CHECK ((btrim(key) <> ''::text)),
    CONSTRAINT external_platforms_name_check CHECK ((btrim(name) <> ''::text))
);

CREATE TABLE public.hub_session_messages (
    id uuid NOT NULL,
    session_id uuid NOT NULL,
    sequence bigint NOT NULL,
    role text NOT NULL,
    message_kind text NOT NULL,
    content text,
    payload jsonb DEFAULT '{}'::jsonb NOT NULL,
    delivery_mode text NOT NULL,
    delivery_state text NOT NULL,
    expected_native_turn_id text,
    turn_id uuid,
    run_id uuid,
    accepted_at timestamp with time zone DEFAULT now() NOT NULL,
    client_message_key text,
    CONSTRAINT hub_session_messages_check CHECK ((((delivery_mode = 'steer'::text) AND (expected_native_turn_id IS NOT NULL) AND (btrim(expected_native_turn_id) <> ''::text)) OR ((delivery_mode <> 'steer'::text) AND (expected_native_turn_id IS NULL)))),
    CONSTRAINT hub_session_messages_client_key_nonempty CHECK (((client_message_key IS NULL) OR (btrim(client_message_key) <> ''::text))),
    CONSTRAINT hub_session_messages_delivery_mode_check CHECK ((delivery_mode = ANY (ARRAY['next_turn'::text, 'later_turn'::text, 'steer'::text, 'record_only'::text]))),
    CONSTRAINT hub_session_messages_delivery_state_check CHECK ((delivery_state = ANY (ARRAY['queued'::text, 'deferred'::text, 'delivering'::text, 'delivered'::text, 'failed'::text]))),
    CONSTRAINT hub_session_messages_message_kind_check CHECK ((btrim(message_kind) <> ''::text)),
    CONSTRAINT hub_session_messages_role_check CHECK ((btrim(role) <> ''::text))
);

CREATE TABLE public.hub_session_turns (
    id uuid NOT NULL,
    session_id uuid NOT NULL,
    native_turn_id text,
    status text NOT NULL,
    configuration_fingerprint text,
    ownership_generation bigint NOT NULL,
    started_at timestamp with time zone,
    ended_at timestamp with time zone,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL,
    delivery_started_at timestamp with time zone,
    interrupt_requested_at timestamp with time zone,
    interrupt_acknowledged_at timestamp with time zone,
    CONSTRAINT hub_session_turns_check CHECK (((ended_at IS NULL) OR (started_at IS NULL) OR (ended_at >= started_at))),
    CONSTRAINT hub_session_turns_ownership_generation_check CHECK ((ownership_generation >= 0)),
    CONSTRAINT hub_session_turns_status_check CHECK ((btrim(status) <> ''::text))
);

CREATE TABLE public.hub_sessions (
    id uuid NOT NULL,
    owner_id uuid NOT NULL,
    agent_id uuid NOT NULL,
    origin_kind text NOT NULL,
    origin_platform_id uuid,
    origin_tenant_id text,
    origin_external_identity_id uuid,
    lifecycle_status text NOT NULL,
    native_thread_id text,
    active_turn_id uuid,
    history_checkpoint bigint DEFAULT 0 NOT NULL,
    configuration_fingerprint text,
    runtime_owner_id uuid,
    ownership_generation bigint DEFAULT 0 NOT NULL,
    recovery_error text,
    current_bundle_generation bigint,
    current_bundle_object_key text,
    current_bundle_checksum_sha256 text,
    current_bundle_size_bytes bigint,
    current_bundle_history_checkpoint bigint,
    current_bundle_ownership_generation bigint,
    current_bundle_producing_codex_version text,
    current_bundle_created_at timestamp with time zone,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL,
    saving_history_checkpoint bigint,
    saving_ownership_generation bigint,
    saving_reason text,
    saving_checkpoint_attempt_id uuid,
    current_bundle_checkpoint_attempt_id uuid,
    last_checkpoint_attempt_id uuid,
    last_checkpoint_ownership_generation bigint,
    last_checkpoint_disposition text,
    last_checkpoint_has_queued_work boolean,
    current_bundle_runtime_id uuid,
    configuration_refresh_revision bigint DEFAULT 0 NOT NULL,
    configuration_applied_revision bigint DEFAULT 0 NOT NULL,
    CONSTRAINT hub_sessions_check CHECK ((((origin_kind = 'hub_native'::text) AND (origin_platform_id IS NULL) AND (origin_tenant_id IS NULL) AND (origin_external_identity_id IS NULL)) OR ((origin_kind = 'external'::text) AND (origin_platform_id IS NOT NULL) AND (origin_tenant_id IS NOT NULL) AND (btrim(origin_tenant_id) <> ''::text) AND (origin_external_identity_id IS NOT NULL)))),
    CONSTRAINT hub_sessions_check1 CHECK (((runtime_owner_id IS NULL) OR (ownership_generation > 0))),
    CONSTRAINT hub_sessions_check2 CHECK ((((current_bundle_generation IS NULL) AND (current_bundle_object_key IS NULL) AND (current_bundle_checksum_sha256 IS NULL) AND (current_bundle_size_bytes IS NULL) AND (current_bundle_history_checkpoint IS NULL) AND (current_bundle_ownership_generation IS NULL) AND (current_bundle_producing_codex_version IS NULL) AND (current_bundle_created_at IS NULL)) OR ((current_bundle_generation IS NOT NULL) AND (current_bundle_object_key IS NOT NULL) AND (btrim(current_bundle_object_key) <> ''::text) AND (current_bundle_checksum_sha256 IS NOT NULL) AND (btrim(current_bundle_checksum_sha256) <> ''::text) AND (current_bundle_size_bytes IS NOT NULL) AND (current_bundle_history_checkpoint IS NOT NULL) AND (current_bundle_ownership_generation IS NOT NULL) AND (current_bundle_producing_codex_version IS NOT NULL) AND (btrim(current_bundle_producing_codex_version) <> ''::text) AND (current_bundle_created_at IS NOT NULL)))),
    CONSTRAINT hub_sessions_configuration_applied_revision_nonnegative CHECK ((configuration_applied_revision >= 0)),
    CONSTRAINT hub_sessions_configuration_refresh_revision_nonnegative CHECK ((configuration_refresh_revision >= 0)),
    CONSTRAINT hub_sessions_configuration_revision_order CHECK ((configuration_applied_revision <= configuration_refresh_revision)),
    CONSTRAINT hub_sessions_current_bundle_generation_check CHECK (((current_bundle_generation IS NULL) OR (current_bundle_generation > 0))),
    CONSTRAINT hub_sessions_current_bundle_history_checkpoint_check CHECK (((current_bundle_history_checkpoint IS NULL) OR (current_bundle_history_checkpoint >= 0))),
    CONSTRAINT hub_sessions_current_bundle_ownership_generation_check CHECK (((current_bundle_ownership_generation IS NULL) OR (current_bundle_ownership_generation >= 0))),
    CONSTRAINT hub_sessions_current_bundle_size_bytes_check CHECK (((current_bundle_size_bytes IS NULL) OR (current_bundle_size_bytes >= 0))),
    CONSTRAINT hub_sessions_history_checkpoint_check CHECK ((history_checkpoint >= 0)),
    CONSTRAINT hub_sessions_last_checkpoint_result_shape CHECK ((((last_checkpoint_attempt_id IS NULL) AND (last_checkpoint_ownership_generation IS NULL) AND (last_checkpoint_disposition IS NULL) AND (last_checkpoint_has_queued_work IS NULL)) OR ((last_checkpoint_attempt_id IS NOT NULL) AND (last_checkpoint_ownership_generation IS NOT NULL) AND (last_checkpoint_ownership_generation > 0) AND (last_checkpoint_disposition = ANY (ARRAY['resume'::text, 'retry'::text])) AND (last_checkpoint_has_queued_work IS NOT NULL)))),
    CONSTRAINT hub_sessions_lifecycle_status_check CHECK ((lifecycle_status = ANY (ARRAY['waiting_for_runtime'::text, 'restoring'::text, 'online'::text, 'saving'::text, 'offline'::text, 'recovery_failed'::text, 'historical'::text]))),
    CONSTRAINT hub_sessions_ownership_generation_check CHECK ((ownership_generation >= 0)),
    CONSTRAINT hub_sessions_saving_checkpoint_shape CHECK ((((lifecycle_status = 'saving'::text) AND (saving_history_checkpoint IS NOT NULL) AND (saving_history_checkpoint >= 0) AND (saving_ownership_generation IS NOT NULL) AND (saving_ownership_generation > 0) AND (saving_reason = ANY (ARRAY['idle'::text, 'version_switch'::text, 'drain'::text])) AND (saving_checkpoint_attempt_id IS NOT NULL)) OR ((lifecycle_status <> 'saving'::text) AND (saving_history_checkpoint IS NULL) AND (saving_ownership_generation IS NULL) AND (saving_reason IS NULL) AND (saving_checkpoint_attempt_id IS NULL))))
);

CREATE TABLE public.integration_app_agents (
    app_id uuid NOT NULL,
    agent_id uuid NOT NULL
);

CREATE TABLE public.integration_attachments (
    id uuid NOT NULL,
    session_id uuid NOT NULL,
    run_id uuid,
    kind text NOT NULL,
    name text NOT NULL,
    content_type text NOT NULL,
    size_bytes bigint NOT NULL,
    text text,
    url text,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    hub_message_id uuid
);

CREATE TABLE public.integration_messages (
    id uuid NOT NULL,
    session_id uuid NOT NULL,
    run_id uuid,
    role text NOT NULL,
    content text NOT NULL,
    attachments jsonb DEFAULT '[]'::jsonb NOT NULL,
    client_message_key text,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    hub_message_id uuid
);

CREATE TABLE public.integration_sessions (
    id uuid NOT NULL,
    oauth_app_id uuid NOT NULL,
    agent_id uuid NOT NULL,
    owner_id uuid NOT NULL,
    external_user_id text NOT NULL,
    tool_definitions jsonb DEFAULT '[]'::jsonb NOT NULL,
    metadata jsonb DEFAULT '{}'::jsonb NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL,
    hub_session_id uuid NOT NULL
);

CREATE TABLE public.integration_tool_requests (
    id uuid NOT NULL,
    session_id uuid NOT NULL,
    run_id uuid NOT NULL,
    tool_name text NOT NULL,
    arguments jsonb DEFAULT '{}'::jsonb NOT NULL,
    status text NOT NULL,
    result_payload jsonb,
    result_event_id uuid,
    expires_at timestamp with time zone NOT NULL,
    responded_at timestamp with time zone,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    follow_up_run_id uuid
);

CREATE TABLE public.model_call_errors (
    id uuid NOT NULL,
    request_id uuid NOT NULL,
    occurred_at timestamp(3) with time zone DEFAULT CURRENT_TIMESTAMP(3) NOT NULL,
    response_status text NOT NULL,
    upstream_http_status integer,
    error_kind text NOT NULL,
    error_code text,
    message text NOT NULL,
    model_connection_id uuid,
    model_connection_scope_snapshot text NOT NULL,
    model_connection_name_snapshot text NOT NULL,
    model_id_snapshot text NOT NULL,
    agent_id uuid,
    agent_name_snapshot text,
    subject_type text NOT NULL,
    subject_user_id uuid,
    subject_display_name_snapshot text,
    source_integration_app_id uuid,
    source_integration_app_name_snapshot text,
    super_admin_protected boolean DEFAULT false NOT NULL,
    CONSTRAINT model_call_errors_code_length CHECK (((error_code IS NULL) OR (char_length(error_code) <= 256))),
    CONSTRAINT model_call_errors_http_status_check CHECK (((upstream_http_status IS NULL) OR ((upstream_http_status >= 100) AND (upstream_http_status <= 599)))),
    CONSTRAINT model_call_errors_kind_nonempty CHECK ((btrim(error_kind) <> ''::text)),
    CONSTRAINT model_call_errors_message_length CHECK (((btrim(message) <> ''::text) AND (char_length(message) <= 2048))),
    CONSTRAINT model_call_errors_response_status_check CHECK ((response_status = ANY (ARRAY['failed'::text, 'incomplete'::text, 'cancelled'::text, 'transport_error'::text, 'protocol_error'::text]))),
    CONSTRAINT model_call_errors_scope_check CHECK ((model_connection_scope_snapshot = ANY (ARRAY['global'::text, 'personal'::text]))),
    CONSTRAINT model_call_errors_snapshot_nonempty CHECK (((btrim(model_connection_name_snapshot) <> ''::text) AND (btrim(model_id_snapshot) <> ''::text) AND ((agent_name_snapshot IS NULL) OR (btrim(agent_name_snapshot) <> ''::text)))),
    CONSTRAINT model_call_errors_subject_type_check CHECK ((subject_type = ANY (ARRAY['user'::text, 'integration_app'::text, 'system'::text])))
);

CREATE TABLE public.model_connections (
    id uuid NOT NULL,
    scope text NOT NULL,
    owner_id uuid,
    name text NOT NULL,
    base_url text,
    model_id text NOT NULL,
    api_key_ciphertext bytea,
    api_key_nonce bytea,
    enabled boolean DEFAULT true NOT NULL,
    created_by uuid,
    deleted_at timestamp(3) with time zone,
    created_at timestamp(3) with time zone DEFAULT CURRENT_TIMESTAMP(3) NOT NULL,
    updated_at timestamp(3) with time zone DEFAULT CURRENT_TIMESTAMP(3) NOT NULL,
    CONSTRAINT model_connections_executable_shape_check CHECK ((((deleted_at IS NULL) AND (base_url IS NOT NULL) AND (btrim(base_url) <> ''::text) AND (api_key_ciphertext IS NOT NULL) AND (octet_length(api_key_ciphertext) >= 17) AND (api_key_nonce IS NOT NULL) AND (octet_length(api_key_nonce) = 12)) OR ((deleted_at IS NOT NULL) AND (enabled = false) AND (base_url IS NULL) AND (api_key_ciphertext IS NULL) AND (api_key_nonce IS NULL)))),
    CONSTRAINT model_connections_model_id_nonempty CHECK ((btrim(model_id) <> ''::text)),
    CONSTRAINT model_connections_name_nonempty CHECK ((btrim(name) <> ''::text)),
    CONSTRAINT model_connections_owner_shape_check CHECK ((((scope = 'global'::text) AND (owner_id IS NULL)) OR ((scope = 'personal'::text) AND ((owner_id IS NOT NULL) OR (deleted_at IS NOT NULL))))),
    CONSTRAINT model_connections_scope_check CHECK ((scope = ANY (ARRAY['global'::text, 'personal'::text])))
);

CREATE TABLE public.model_token_usage (
    id uuid NOT NULL,
    request_id uuid NOT NULL,
    occurred_at timestamp(3) with time zone DEFAULT CURRENT_TIMESTAMP(3) NOT NULL,
    response_status text NOT NULL,
    model_connection_id uuid,
    model_connection_scope_snapshot text NOT NULL,
    model_connection_name_snapshot text NOT NULL,
    model_id_snapshot text NOT NULL,
    agent_id uuid,
    agent_name_snapshot text,
    subject_type text NOT NULL,
    subject_user_id uuid,
    subject_display_name_snapshot text,
    source_integration_app_id uuid,
    source_integration_app_name_snapshot text,
    input_tokens bigint NOT NULL,
    output_tokens bigint NOT NULL,
    total_tokens bigint NOT NULL,
    cached_tokens bigint DEFAULT 0 NOT NULL,
    reasoning_tokens bigint DEFAULT 0 NOT NULL,
    super_admin_protected boolean DEFAULT false NOT NULL,
    CONSTRAINT model_token_usage_counts_check CHECK (((input_tokens >= 0) AND (output_tokens >= 0) AND (total_tokens >= 0) AND (cached_tokens >= 0) AND (reasoning_tokens >= 0) AND (total_tokens = (input_tokens + output_tokens)) AND (cached_tokens <= input_tokens) AND (reasoning_tokens <= output_tokens))),
    CONSTRAINT model_token_usage_response_status_check CHECK ((response_status = ANY (ARRAY['completed'::text, 'failed'::text, 'incomplete'::text, 'cancelled'::text]))),
    CONSTRAINT model_token_usage_scope_check CHECK ((model_connection_scope_snapshot = ANY (ARRAY['global'::text, 'personal'::text]))),
    CONSTRAINT model_token_usage_snapshot_nonempty CHECK (((btrim(model_connection_name_snapshot) <> ''::text) AND (btrim(model_id_snapshot) <> ''::text) AND ((agent_name_snapshot IS NULL) OR (btrim(agent_name_snapshot) <> ''::text)))),
    CONSTRAINT model_token_usage_subject_type_check CHECK ((subject_type = ANY (ARRAY['user'::text, 'integration_app'::text, 'system'::text])))
);

CREATE TABLE public.oauth_access_tokens (
    id uuid NOT NULL,
    oauth_app_id uuid NOT NULL,
    agent_id uuid,
    owner_id uuid,
    token_hash text NOT NULL,
    expires_at timestamp with time zone NOT NULL,
    last_used_at timestamp with time zone,
    revoked_at timestamp with time zone,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    origin_tenant_id text,
    origin_external_identity_id uuid,
    grant_type text NOT NULL,
    subject_user_id uuid,
    scopes text[] NOT NULL,
    CONSTRAINT oauth_access_tokens_grant_type_check CHECK ((grant_type = ANY (ARRAY['authorization_code'::text, 'client_credentials'::text]))),
    CONSTRAINT oauth_access_tokens_subject_check CHECK ((((grant_type = 'authorization_code'::text) AND (subject_user_id IS NOT NULL) AND (origin_tenant_id IS NOT NULL) AND (btrim(origin_tenant_id) <> ''::text) AND (origin_external_identity_id IS NOT NULL)) OR ((grant_type = 'client_credentials'::text) AND (subject_user_id IS NULL) AND (origin_tenant_id IS NULL) AND (origin_external_identity_id IS NULL))))
);

CREATE TABLE public.oauth_apps (
    id uuid NOT NULL,
    agent_id uuid,
    owner_id uuid NOT NULL,
    name text NOT NULL,
    client_id text NOT NULL,
    client_secret_hash text,
    redirect_uris jsonb DEFAULT '[]'::jsonb NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL,
    external_platform_id uuid NOT NULL,
    authentication_channel_id uuid NOT NULL,
    deleted_at timestamp with time zone
);

CREATE TABLE public.oauth_authorization_codes (
    code_hash text NOT NULL,
    oauth_app_id uuid NOT NULL,
    redirect_uri text NOT NULL,
    expires_at timestamp with time zone NOT NULL,
    used_at timestamp with time zone,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    subject_user_id uuid NOT NULL,
    external_identity_id uuid NOT NULL,
    tenant_id text NOT NULL,
    scopes text[] NOT NULL,
    CONSTRAINT oauth_authorization_codes_tenant_nonempty CHECK ((btrim(tenant_id) <> ''::text))
);

CREATE TABLE public.oidc_login_states (
    state text NOT NULL,
    expires_at timestamp with time zone NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    email text,
    subject text,
    external_username text
);

CREATE TABLE public.run_events (
    seq bigint NOT NULL,
    event_id uuid NOT NULL,
    run_id uuid NOT NULL,
    event_type text NOT NULL,
    role text,
    content text,
    payload jsonb DEFAULT '{}'::jsonb NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    hub_message_id uuid
);

CREATE SEQUENCE public.run_events_seq_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;

ALTER SEQUENCE public.run_events_seq_seq OWNED BY public.run_events.seq;

CREATE TABLE public.run_model_connection_snapshots (
    run_id uuid NOT NULL,
    model_connection_id uuid NOT NULL,
    model_id text NOT NULL,
    CONSTRAINT run_model_connection_snapshots_model_id_nonempty CHECK ((btrim(model_id) <> ''::text))
);

CREATE TABLE public.runs (
    id uuid NOT NULL,
    agent_id uuid NOT NULL,
    owner_id uuid NOT NULL,
    runtime_id uuid,
    status text NOT NULL,
    initial_message text NOT NULL,
    session_id text,
    work_dir_ref text,
    source text NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL,
    integration_session_id uuid,
    parent_run_id uuid,
    model_proxy_token_hash text,
    widget_session_id uuid,
    automation_id uuid,
    hub_session_id uuid NOT NULL,
    hub_message_id uuid,
    hub_turn_id uuid NOT NULL,
    session_ownership_generation bigint NOT NULL,
    model_subject_type text DEFAULT 'user'::text NOT NULL,
    model_subject_user_id uuid,
    model_source_integration_app_id uuid,
    CONSTRAINT runs_model_subject_shape_check CHECK (((model_subject_type <> 'integration_app'::text) OR (model_subject_user_id IS NULL))),
    CONSTRAINT runs_model_subject_type_check CHECK ((model_subject_type = ANY (ARRAY['user'::text, 'integration_app'::text, 'system'::text]))),
    CONSTRAINT runs_session_ownership_generation_nonnegative CHECK (((session_ownership_generation IS NULL) OR (session_ownership_generation >= 0)))
);

CREATE TABLE public.runtime_codex_readiness (
    runtime_id uuid NOT NULL,
    version text NOT NULL,
    status text NOT NULL,
    error text,
    checked_at timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT runtime_codex_readiness_status_check CHECK ((status = ANY (ARRAY['ready'::text, 'failed'::text])))
);

CREATE TABLE public.runtime_enrollment_tokens (
    id uuid NOT NULL,
    token_hash text NOT NULL,
    created_by uuid,
    expires_at timestamp with time zone NOT NULL,
    consumed_at timestamp with time zone,
    consumed_by_runtime_id uuid,
    revoked_at timestamp with time zone,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT runtime_enrollment_tokens_check CHECK ((expires_at > created_at)),
    CONSTRAINT runtime_enrollment_tokens_check1 CHECK (((consumed_at IS NULL) OR (revoked_at IS NULL))),
    CONSTRAINT runtime_enrollment_tokens_token_hash_check CHECK ((token_hash ~ '^[0-9a-f]{64}$'::text))
);

CREATE TABLE public.runtime_session_cleanup_obligations (
    runtime_id uuid NOT NULL,
    session_id uuid NOT NULL,
    ownership_generation bigint NOT NULL,
    erasure_user_id uuid,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT runtime_session_cleanup_obligations_ownership_generation_check CHECK ((ownership_generation > 0))
);

CREATE TABLE public.runtimes (
    id uuid NOT NULL,
    token_hash text NOT NULL,
    hostname text NOT NULL,
    labels text[] DEFAULT '{}'::text[] NOT NULL,
    codex_version text NOT NULL,
    capabilities jsonb DEFAULT '{}'::jsonb NOT NULL,
    sandbox_mode text NOT NULL,
    status text NOT NULL,
    last_heartbeat_at timestamp with time zone DEFAULT now() NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    credential_revoked_at timestamp with time zone,
    rotation_requested_at timestamp with time zone,
    pending_token_hash text,
    pending_token_created_at timestamp with time zone,
    CONSTRAINT runtimes_pending_token_hash_format CHECK (((pending_token_hash IS NULL) OR (pending_token_hash ~ '^[0-9a-f]{64}$'::text))),
    CONSTRAINT runtimes_pending_token_timestamp_pair CHECK (((pending_token_hash IS NULL) = (pending_token_created_at IS NULL)))
);

CREATE TABLE public.session_bundle_deletion_queue (
    object_key text NOT NULL,
    agent_id uuid NOT NULL,
    session_id uuid NOT NULL,
    attempts bigint DEFAULT 0 NOT NULL,
    last_error text,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT session_bundle_deletion_queue_attempts_check CHECK ((attempts >= 0)),
    CONSTRAINT session_bundle_deletion_queue_object_key_check CHECK ((btrim(object_key) <> ''::text))
);

CREATE TABLE public.sessions (
    token_hash text NOT NULL,
    user_id uuid NOT NULL,
    expires_at timestamp with time zone NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL
);

CREATE TABLE public.skills (
    id uuid NOT NULL,
    owner_id uuid NOT NULL,
    name text NOT NULL,
    description text NOT NULL,
    content text NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL,
    revision bigint DEFAULT 1 NOT NULL,
    content_checksum_sha256 text NOT NULL,
    CONSTRAINT skills_content_checksum_sha256_shape CHECK ((content_checksum_sha256 ~ '^[0-9a-f]{64}$'::text)),
    CONSTRAINT skills_revision_positive CHECK ((revision > 0))
);

CREATE TABLE public.system_default_model_connection (
    singleton boolean DEFAULT true NOT NULL,
    model_connection_id uuid NOT NULL,
    updated_by uuid,
    updated_at timestamp(3) with time zone DEFAULT CURRENT_TIMESTAMP(3) NOT NULL,
    CONSTRAINT system_default_model_connection_singleton_check CHECK (singleton)
);

CREATE TABLE public.user_erasure_audit (
    erased_user_id uuid NOT NULL,
    acting_administrator_id uuid NOT NULL,
    erased_at timestamp with time zone NOT NULL,
    erased_role text DEFAULT 'member'::text NOT NULL,
    CONSTRAINT user_erasure_audit_erased_role_check CHECK ((erased_role = ANY (ARRAY['member'::text, 'admin'::text, 'super_admin'::text])))
);

CREATE TABLE public.user_erasure_bundle_objects (
    user_id uuid NOT NULL,
    object_key text NOT NULL,
    attempts bigint DEFAULT 0 NOT NULL,
    last_error text,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT user_erasure_bundle_objects_attempts_check CHECK ((attempts >= 0)),
    CONSTRAINT user_erasure_bundle_objects_object_key_check CHECK ((btrim(object_key) <> ''::text))
);

CREATE TABLE public.user_erasure_jobs (
    user_id uuid NOT NULL,
    requested_by uuid NOT NULL,
    requested_username text NOT NULL,
    attempts bigint DEFAULT 0 NOT NULL,
    last_error text,
    requested_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL,
    target_role text NOT NULL,
    CONSTRAINT user_erasure_jobs_attempts_check CHECK ((attempts >= 0)),
    CONSTRAINT user_erasure_jobs_requested_username_check CHECK ((btrim(requested_username) <> ''::text)),
    CONSTRAINT user_erasure_jobs_target_role_check CHECK ((target_role = ANY (ARRAY['member'::text, 'admin'::text, 'super_admin'::text])))
);

CREATE TABLE public.users (
    id uuid NOT NULL,
    email text,
    password text,
    display_name text NOT NULL,
    role text NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    username text NOT NULL,
    email_verified boolean DEFAULT true NOT NULL,
    deletion_requested_at timestamp with time zone,
    CONSTRAINT users_username_length CHECK (((char_length(username) >= 1) AND (char_length(username) <= 64)))
);

ALTER TABLE ONLY public.run_events ALTER COLUMN seq SET DEFAULT nextval('public.run_events_seq_seq'::regclass);

ALTER TABLE ONLY public.agent_skills
    ADD CONSTRAINT agent_skills_pkey PRIMARY KEY (agent_id, skill_id);

ALTER TABLE ONLY public.agents
    ADD CONSTRAINT agents_pkey PRIMARY KEY (id);

ALTER TABLE ONLY public.api_keys
    ADD CONSTRAINT api_keys_pkey PRIMARY KEY (id);

ALTER TABLE ONLY public.api_keys
    ADD CONSTRAINT api_keys_token_hash_key UNIQUE (token_hash);

ALTER TABLE ONLY public.auth_policy
    ADD CONSTRAINT auth_policy_pkey PRIMARY KEY (singleton);

ALTER TABLE ONLY public.authentication_channels
    ADD CONSTRAINT authentication_channels_id_platform_id_key UNIQUE (id, platform_id);

ALTER TABLE ONLY public.authentication_channels
    ADD CONSTRAINT authentication_channels_pkey PRIMARY KEY (id);

ALTER TABLE ONLY public.authentication_channels
    ADD CONSTRAINT authentication_channels_platform_id_key_key UNIQUE (platform_id, key);

ALTER TABLE ONLY public.automations
    ADD CONSTRAINT automations_pkey PRIMARY KEY (id);

ALTER TABLE ONLY public.automations
    ADD CONSTRAINT automations_webhook_token_key UNIQUE (webhook_token_hash);

ALTER TABLE ONLY public.codex_subagent_definitions
    ADD CONSTRAINT codex_subagent_definitions_pkey PRIMARY KEY (id);

ALTER TABLE ONLY public.codex_version_artifacts
    ADD CONSTRAINT codex_version_artifacts_pkey PRIMARY KEY (version, os, architecture);

ALTER TABLE ONLY public.codex_version_rollout
    ADD CONSTRAINT codex_version_rollout_pkey PRIMARY KEY (singleton);

ALTER TABLE ONLY public.embed_jwt_replays
    ADD CONSTRAINT embed_jwt_replays_pkey PRIMARY KEY (jti);

ALTER TABLE ONLY public.embed_sessions
    ADD CONSTRAINT embed_sessions_hub_session_key UNIQUE (hub_session_id);

ALTER TABLE ONLY public.embed_sessions
    ADD CONSTRAINT embed_sessions_id_unique UNIQUE (id);

ALTER TABLE ONLY public.embed_sessions
    ADD CONSTRAINT embed_sessions_pkey PRIMARY KEY (token_hash);

ALTER TABLE ONLY public.external_identities
    ADD CONSTRAINT external_identities_pkey PRIMARY KEY (id);

ALTER TABLE ONLY public.external_identities
    ADD CONSTRAINT external_identities_platform_tenant_user_key UNIQUE (platform_id, tenant_id, external_user_id);

ALTER TABLE ONLY public.external_identities
    ADD CONSTRAINT external_identities_session_origin_key UNIQUE (id, platform_id, user_id);

ALTER TABLE ONLY public.external_platforms
    ADD CONSTRAINT external_platforms_key_key UNIQUE (key);

ALTER TABLE ONLY public.external_platforms
    ADD CONSTRAINT external_platforms_pkey PRIMARY KEY (id);

ALTER TABLE ONLY public.hub_session_messages
    ADD CONSTRAINT hub_session_messages_id_session_id_key UNIQUE (id, session_id);

ALTER TABLE ONLY public.hub_session_messages
    ADD CONSTRAINT hub_session_messages_pkey PRIMARY KEY (id);

ALTER TABLE ONLY public.hub_session_messages
    ADD CONSTRAINT hub_session_messages_session_id_sequence_key UNIQUE (session_id, sequence);

ALTER TABLE ONLY public.hub_session_turns
    ADD CONSTRAINT hub_session_turns_id_session_id_key UNIQUE (id, session_id);

ALTER TABLE ONLY public.hub_session_turns
    ADD CONSTRAINT hub_session_turns_pkey PRIMARY KEY (id);

ALTER TABLE ONLY public.hub_sessions
    ADD CONSTRAINT hub_sessions_id_owner_id_agent_id_key UNIQUE (id, owner_id, agent_id);

ALTER TABLE ONLY public.hub_sessions
    ADD CONSTRAINT hub_sessions_pkey PRIMARY KEY (id);

ALTER TABLE ONLY public.integration_app_agents
    ADD CONSTRAINT integration_app_agents_pkey PRIMARY KEY (app_id, agent_id);

ALTER TABLE ONLY public.integration_attachments
    ADD CONSTRAINT integration_attachments_pkey PRIMARY KEY (id);

ALTER TABLE ONLY public.integration_messages
    ADD CONSTRAINT integration_messages_pkey PRIMARY KEY (id);

ALTER TABLE ONLY public.integration_sessions
    ADD CONSTRAINT integration_sessions_hub_session_key UNIQUE (hub_session_id);

ALTER TABLE ONLY public.integration_sessions
    ADD CONSTRAINT integration_sessions_pkey PRIMARY KEY (id);

ALTER TABLE ONLY public.integration_tool_requests
    ADD CONSTRAINT integration_tool_requests_pkey PRIMARY KEY (id);

ALTER TABLE ONLY public.model_call_errors
    ADD CONSTRAINT model_call_errors_pkey PRIMARY KEY (id);

ALTER TABLE ONLY public.model_call_errors
    ADD CONSTRAINT model_call_errors_request_id_key UNIQUE (request_id);

ALTER TABLE ONLY public.model_connections
    ADD CONSTRAINT model_connections_pkey PRIMARY KEY (id);

ALTER TABLE ONLY public.model_token_usage
    ADD CONSTRAINT model_token_usage_pkey PRIMARY KEY (id);

ALTER TABLE ONLY public.model_token_usage
    ADD CONSTRAINT model_token_usage_request_id_key UNIQUE (request_id);

ALTER TABLE ONLY public.oauth_access_tokens
    ADD CONSTRAINT oauth_access_tokens_pkey PRIMARY KEY (id);

ALTER TABLE ONLY public.oauth_access_tokens
    ADD CONSTRAINT oauth_access_tokens_token_hash_key UNIQUE (token_hash);

ALTER TABLE ONLY public.oauth_apps
    ADD CONSTRAINT oauth_apps_client_id_key UNIQUE (client_id);

ALTER TABLE ONLY public.oauth_apps
    ADD CONSTRAINT oauth_apps_pkey PRIMARY KEY (id);

ALTER TABLE ONLY public.oauth_authorization_codes
    ADD CONSTRAINT oauth_authorization_codes_pkey PRIMARY KEY (code_hash);

ALTER TABLE ONLY public.oidc_login_states
    ADD CONSTRAINT oidc_login_states_pkey PRIMARY KEY (state);

ALTER TABLE ONLY public.run_events
    ADD CONSTRAINT run_events_event_id_key UNIQUE (event_id);

ALTER TABLE ONLY public.run_events
    ADD CONSTRAINT run_events_pkey PRIMARY KEY (seq);

ALTER TABLE ONLY public.run_model_connection_snapshots
    ADD CONSTRAINT run_model_connection_snapshots_pkey PRIMARY KEY (run_id, model_connection_id);

ALTER TABLE ONLY public.runs
    ADD CONSTRAINT runs_id_hub_session_key UNIQUE (id, hub_session_id);

ALTER TABLE ONLY public.runs
    ADD CONSTRAINT runs_pkey PRIMARY KEY (id);

ALTER TABLE ONLY public.runtime_codex_readiness
    ADD CONSTRAINT runtime_codex_readiness_pkey PRIMARY KEY (runtime_id, version);

ALTER TABLE ONLY public.runtime_enrollment_tokens
    ADD CONSTRAINT runtime_enrollment_tokens_pkey PRIMARY KEY (id);

ALTER TABLE ONLY public.runtime_enrollment_tokens
    ADD CONSTRAINT runtime_enrollment_tokens_token_hash_key UNIQUE (token_hash);

ALTER TABLE ONLY public.runtime_session_cleanup_obligations
    ADD CONSTRAINT runtime_session_cleanup_obligations_pkey PRIMARY KEY (runtime_id, session_id, ownership_generation);

ALTER TABLE ONLY public.runtimes
    ADD CONSTRAINT runtimes_pkey PRIMARY KEY (id);

ALTER TABLE ONLY public.runtimes
    ADD CONSTRAINT runtimes_token_key UNIQUE (token_hash);

ALTER TABLE ONLY public.session_bundle_deletion_queue
    ADD CONSTRAINT session_bundle_deletion_queue_pkey PRIMARY KEY (object_key);

ALTER TABLE ONLY public.sessions
    ADD CONSTRAINT sessions_pkey PRIMARY KEY (token_hash);

ALTER TABLE ONLY public.skills
    ADD CONSTRAINT skills_pkey PRIMARY KEY (id);

ALTER TABLE ONLY public.system_default_model_connection
    ADD CONSTRAINT system_default_model_connection_model_connection_id_key UNIQUE (model_connection_id);

ALTER TABLE ONLY public.system_default_model_connection
    ADD CONSTRAINT system_default_model_connection_pkey PRIMARY KEY (singleton);

ALTER TABLE ONLY public.user_erasure_audit
    ADD CONSTRAINT user_erasure_audit_pkey PRIMARY KEY (erased_user_id);

ALTER TABLE ONLY public.user_erasure_bundle_objects
    ADD CONSTRAINT user_erasure_bundle_objects_pkey PRIMARY KEY (user_id, object_key);

ALTER TABLE ONLY public.user_erasure_jobs
    ADD CONSTRAINT user_erasure_jobs_pkey PRIMARY KEY (user_id);

ALTER TABLE ONLY public.users
    ADD CONSTRAINT users_pkey PRIMARY KEY (id);

ALTER TABLE ONLY public.users
    ADD CONSTRAINT users_username_key UNIQUE (username);

CREATE INDEX agent_skills_skill_idx ON public.agent_skills USING btree (skill_id);

CREATE INDEX agents_default_model_connection_idx ON public.agents USING btree (default_model_connection_id) WHERE (default_model_connection_id IS NOT NULL);

CREATE INDEX agents_public_to_idx ON public.agents USING gin (public_to);

CREATE INDEX agents_runtime_idx ON public.agents USING btree (runtime_id);

CREATE INDEX api_keys_user_created_idx ON public.api_keys USING btree (user_id, created_at DESC);

CREATE INDEX automations_agent_created_idx ON public.automations USING btree (agent_id, created_at DESC);

CREATE INDEX automations_owner_created_idx ON public.automations USING btree (owner_id, created_at DESC);

CREATE UNIQUE INDEX codex_subagent_agent_name_key ON public.codex_subagent_definitions USING btree (agent_id, lower(btrim(name)));

CREATE INDEX codex_subagent_model_connection_idx ON public.codex_subagent_definitions USING btree (model_connection_id) WHERE (model_connection_id IS NOT NULL);

CREATE INDEX codex_version_artifacts_version_idx ON public.codex_version_artifacts USING btree (version, os, architecture);

CREATE INDEX embed_sessions_oauth_app_idx ON public.embed_sessions USING btree (oauth_app_id) WHERE (oauth_app_id IS NOT NULL);

CREATE INDEX hub_session_messages_delivery_idx ON public.hub_session_messages USING btree (session_id, delivery_state, sequence);

CREATE UNIQUE INDEX hub_session_messages_session_client_key_idx ON public.hub_session_messages USING btree (session_id, client_message_key) WHERE (client_message_key IS NOT NULL);

CREATE UNIQUE INDEX hub_session_turns_native_turn_key ON public.hub_session_turns USING btree (session_id, native_turn_id) WHERE (native_turn_id IS NOT NULL);

CREATE INDEX hub_session_turns_session_created_idx ON public.hub_session_turns USING btree (session_id, created_at, id);

CREATE INDEX hub_sessions_agent_updated_idx ON public.hub_sessions USING btree (agent_id, updated_at DESC, id DESC);

CREATE UNIQUE INDEX hub_sessions_current_bundle_object_key ON public.hub_sessions USING btree (current_bundle_object_key) WHERE (current_bundle_object_key IS NOT NULL);

CREATE UNIQUE INDEX hub_sessions_native_thread_key ON public.hub_sessions USING btree (native_thread_id) WHERE (native_thread_id IS NOT NULL);

CREATE INDEX hub_sessions_owner_updated_idx ON public.hub_sessions USING btree (owner_id, updated_at DESC, id DESC);

CREATE INDEX hub_sessions_runtime_owner_idx ON public.hub_sessions USING btree (runtime_owner_id, lifecycle_status) WHERE (runtime_owner_id IS NOT NULL);

CREATE INDEX integration_app_agents_agent_idx ON public.integration_app_agents USING btree (agent_id, app_id);

CREATE INDEX integration_attachments_hub_message_idx ON public.integration_attachments USING btree (hub_message_id) WHERE (hub_message_id IS NOT NULL);

CREATE UNIQUE INDEX integration_messages_session_key_idx ON public.integration_messages USING btree (session_id, client_message_key) WHERE (client_message_key IS NOT NULL);

CREATE INDEX integration_sessions_hub_session_idx ON public.integration_sessions USING btree (hub_session_id) WHERE (hub_session_id IS NOT NULL);

CREATE INDEX model_call_errors_agent_occurred_idx ON public.model_call_errors USING btree (agent_id, occurred_at DESC, id DESC);

CREATE INDEX model_call_errors_model_occurred_idx ON public.model_call_errors USING btree (model_connection_id, occurred_at DESC, id DESC);

CREATE INDEX model_call_errors_occurred_idx ON public.model_call_errors USING btree (occurred_at DESC, id DESC);

CREATE INDEX model_call_errors_user_occurred_idx ON public.model_call_errors USING btree (subject_user_id, occurred_at DESC, id DESC);

CREATE UNIQUE INDEX model_connections_global_name_key ON public.model_connections USING btree (lower(btrim(name))) WHERE ((scope = 'global'::text) AND (deleted_at IS NULL));

CREATE UNIQUE INDEX model_connections_personal_name_key ON public.model_connections USING btree (owner_id, lower(btrim(name))) WHERE ((scope = 'personal'::text) AND (deleted_at IS NULL));

CREATE INDEX model_connections_scope_owner_enabled_idx ON public.model_connections USING btree (scope, owner_id, enabled, name, id) WHERE (deleted_at IS NULL);

CREATE INDEX model_token_usage_agent_occurred_idx ON public.model_token_usage USING btree (agent_id, occurred_at DESC, id DESC);

CREATE INDEX model_token_usage_model_occurred_idx ON public.model_token_usage USING btree (model_connection_id, occurred_at DESC, id DESC);

CREATE INDEX model_token_usage_occurred_idx ON public.model_token_usage USING btree (occurred_at DESC, id DESC);

CREATE INDEX model_token_usage_user_occurred_idx ON public.model_token_usage USING btree (subject_user_id, occurred_at DESC, id DESC);

CREATE INDEX run_events_run_seq_idx ON public.run_events USING btree (run_id, seq);

CREATE INDEX run_model_connection_snapshots_connection_idx ON public.run_model_connection_snapshots USING btree (model_connection_id, run_id);

CREATE INDEX runs_agent_created_idx ON public.runs USING btree (agent_id, created_at DESC);

CREATE INDEX runs_automation_created_idx ON public.runs USING btree (automation_id, created_at DESC, id DESC);

CREATE INDEX runs_hub_session_created_idx ON public.runs USING btree (hub_session_id, created_at, id) WHERE (hub_session_id IS NOT NULL);

CREATE INDEX runs_integration_session_idx ON public.runs USING btree (integration_session_id, created_at);

CREATE INDEX runs_parent_idx ON public.runs USING btree (parent_run_id);

CREATE INDEX runs_status_created_idx ON public.runs USING btree (status, created_at);

CREATE INDEX runs_widget_session_idx ON public.runs USING btree (widget_session_id, created_at);

CREATE INDEX runtime_codex_readiness_version_idx ON public.runtime_codex_readiness USING btree (version, status, runtime_id);

CREATE INDEX runtime_enrollment_tokens_created_at_idx ON public.runtime_enrollment_tokens USING btree (created_at DESC);

CREATE INDEX runtime_session_cleanup_obligations_erasure_idx ON public.runtime_session_cleanup_obligations USING btree (erasure_user_id, created_at) WHERE (erasure_user_id IS NOT NULL);

CREATE UNIQUE INDEX runtimes_pending_token_hash_unique_idx ON public.runtimes USING btree (pending_token_hash) WHERE (pending_token_hash IS NOT NULL);

CREATE INDEX session_bundle_deletion_queue_agent_idx ON public.session_bundle_deletion_queue USING btree (agent_id, created_at, object_key);

CREATE INDEX skills_owner_created_idx ON public.skills USING btree (owner_id, created_at DESC);

CREATE INDEX user_erasure_bundle_objects_user_idx ON public.user_erasure_bundle_objects USING btree (user_id, created_at, object_key);

CREATE INDEX user_erasure_jobs_requested_idx ON public.user_erasure_jobs USING btree (requested_at, user_id);

CREATE UNIQUE INDEX users_email_normalized_key ON public.users USING btree (lower(btrim(email))) WHERE (email IS NOT NULL);

CREATE TRIGGER agents_default_model_connection_validate BEFORE INSERT OR UPDATE OF owner_id, default_model_connection_id ON public.agents FOR EACH ROW EXECUTE FUNCTION public.validate_agent_default_model_connection();

CREATE TRIGGER agents_protect_super_admin_model_accounting BEFORE DELETE ON public.agents FOR EACH ROW EXECUTE FUNCTION public.protect_super_admin_model_accounting_before_agent_delete();

CREATE TRIGGER codex_subagent_model_connection_validate BEFORE INSERT OR UPDATE OF agent_id, model_connection_id ON public.codex_subagent_definitions FOR EACH ROW EXECUTE FUNCTION public.validate_subagent_model_connection();

CREATE TRIGGER hub_session_messages_assign_sequence BEFORE INSERT ON public.hub_session_messages FOR EACH ROW EXECUTE FUNCTION public.assign_hub_session_message_sequence();

CREATE TRIGGER hub_session_messages_immutability BEFORE UPDATE ON public.hub_session_messages FOR EACH ROW EXECUTE FUNCTION public.enforce_hub_session_message_immutability();

CREATE TRIGGER hub_session_turns_immutability BEFORE UPDATE ON public.hub_session_turns FOR EACH ROW EXECUTE FUNCTION public.enforce_hub_session_turn_immutability();

CREATE TRIGGER hub_sessions_invariants BEFORE INSERT OR UPDATE ON public.hub_sessions FOR EACH ROW EXECUTE FUNCTION public.enforce_hub_session_invariants();

CREATE TRIGGER model_call_errors_protect BEFORE DELETE OR UPDATE ON public.model_call_errors FOR EACH ROW EXECUTE FUNCTION public.protect_model_ledger_row();

CREATE TRIGGER model_token_usage_protect BEFORE DELETE OR UPDATE ON public.model_token_usage FOR EACH ROW EXECUTE FUNCTION public.protect_model_ledger_row();

CREATE TRIGGER oauth_apps_protect_super_admin_model_accounting BEFORE DELETE ON public.oauth_apps FOR EACH ROW EXECUTE FUNCTION public.protect_super_admin_model_accounting_before_app_delete();

CREATE TRIGGER runs_hub_session_links BEFORE INSERT OR UPDATE OF hub_session_id, hub_turn_id, hub_message_id, owner_id, agent_id ON public.runs FOR EACH ROW EXECUTE FUNCTION public.enforce_hub_run_session_links();

CREATE TRIGGER system_default_model_connection_validate BEFORE INSERT OR UPDATE OF model_connection_id ON public.system_default_model_connection FOR EACH ROW EXECUTE FUNCTION public.validate_system_default_model_connection();

CREATE TRIGGER users_anonymize_model_accounting BEFORE DELETE ON public.users FOR EACH ROW EXECUTE FUNCTION public.anonymize_model_accounting_before_user_delete();

ALTER TABLE ONLY public.agent_skills
    ADD CONSTRAINT agent_skills_agent_id_fkey FOREIGN KEY (agent_id) REFERENCES public.agents(id) ON DELETE CASCADE;

ALTER TABLE ONLY public.agent_skills
    ADD CONSTRAINT agent_skills_skill_id_fkey FOREIGN KEY (skill_id) REFERENCES public.skills(id) ON DELETE CASCADE;

ALTER TABLE ONLY public.agents
    ADD CONSTRAINT agents_default_model_connection_id_fkey FOREIGN KEY (default_model_connection_id) REFERENCES public.model_connections(id) ON DELETE SET NULL;

ALTER TABLE ONLY public.agents
    ADD CONSTRAINT agents_owner_id_fkey FOREIGN KEY (owner_id) REFERENCES public.users(id) ON DELETE CASCADE;

ALTER TABLE ONLY public.agents
    ADD CONSTRAINT agents_runtime_id_fkey FOREIGN KEY (runtime_id) REFERENCES public.runtimes(id) ON DELETE SET NULL;

ALTER TABLE ONLY public.api_keys
    ADD CONSTRAINT api_keys_user_id_fkey FOREIGN KEY (user_id) REFERENCES public.users(id) ON DELETE CASCADE;

ALTER TABLE ONLY public.auth_policy
    ADD CONSTRAINT auth_policy_updated_by_fkey FOREIGN KEY (updated_by) REFERENCES public.users(id) ON DELETE SET NULL;

ALTER TABLE ONLY public.authentication_channels
    ADD CONSTRAINT authentication_channels_created_by_fkey FOREIGN KEY (created_by) REFERENCES public.users(id) ON DELETE SET NULL;

ALTER TABLE ONLY public.authentication_channels
    ADD CONSTRAINT authentication_channels_platform_id_fkey FOREIGN KEY (platform_id) REFERENCES public.external_platforms(id) ON DELETE CASCADE;

ALTER TABLE ONLY public.automations
    ADD CONSTRAINT automations_agent_id_fkey FOREIGN KEY (agent_id) REFERENCES public.agents(id) ON DELETE CASCADE;

ALTER TABLE ONLY public.automations
    ADD CONSTRAINT automations_owner_id_fkey FOREIGN KEY (owner_id) REFERENCES public.users(id) ON DELETE CASCADE;

ALTER TABLE ONLY public.codex_subagent_definitions
    ADD CONSTRAINT codex_subagent_definitions_agent_id_fkey FOREIGN KEY (agent_id) REFERENCES public.agents(id) ON DELETE CASCADE;

ALTER TABLE ONLY public.codex_subagent_definitions
    ADD CONSTRAINT codex_subagent_definitions_model_connection_id_fkey FOREIGN KEY (model_connection_id) REFERENCES public.model_connections(id) ON DELETE SET NULL;

ALTER TABLE ONLY public.embed_sessions
    ADD CONSTRAINT embed_sessions_agent_id_fkey FOREIGN KEY (agent_id) REFERENCES public.agents(id) ON DELETE CASCADE;

ALTER TABLE ONLY public.embed_sessions
    ADD CONSTRAINT embed_sessions_hub_session_id_fkey FOREIGN KEY (hub_session_id) REFERENCES public.hub_sessions(id) ON DELETE RESTRICT;

ALTER TABLE ONLY public.embed_sessions
    ADD CONSTRAINT embed_sessions_last_run_id_fkey FOREIGN KEY (last_run_id) REFERENCES public.runs(id) ON DELETE SET NULL;

ALTER TABLE ONLY public.embed_sessions
    ADD CONSTRAINT embed_sessions_oauth_app_id_fkey FOREIGN KEY (oauth_app_id) REFERENCES public.oauth_apps(id) ON DELETE CASCADE;

ALTER TABLE ONLY public.embed_sessions
    ADD CONSTRAINT embed_sessions_owner_id_fkey FOREIGN KEY (owner_id) REFERENCES public.users(id) ON DELETE CASCADE;

ALTER TABLE ONLY public.external_identities
    ADD CONSTRAINT external_identities_authentication_channel_id_platform_id_fkey FOREIGN KEY (authentication_channel_id, platform_id) REFERENCES public.authentication_channels(id, platform_id) ON DELETE RESTRICT;

ALTER TABLE ONLY public.external_identities
    ADD CONSTRAINT external_identities_platform_id_fkey FOREIGN KEY (platform_id) REFERENCES public.external_platforms(id) ON DELETE CASCADE;

ALTER TABLE ONLY public.external_identities
    ADD CONSTRAINT external_identities_user_id_fkey FOREIGN KEY (user_id) REFERENCES public.users(id) ON DELETE CASCADE;

ALTER TABLE ONLY public.hub_session_messages
    ADD CONSTRAINT hub_session_messages_run_session_fk FOREIGN KEY (run_id, session_id) REFERENCES public.runs(id, hub_session_id) ON DELETE RESTRICT;

ALTER TABLE ONLY public.hub_session_messages
    ADD CONSTRAINT hub_session_messages_session_id_fkey FOREIGN KEY (session_id) REFERENCES public.hub_sessions(id) ON DELETE CASCADE;

ALTER TABLE ONLY public.hub_session_messages
    ADD CONSTRAINT hub_session_messages_turn_id_session_id_fkey FOREIGN KEY (turn_id, session_id) REFERENCES public.hub_session_turns(id, session_id) ON DELETE RESTRICT;

ALTER TABLE ONLY public.hub_session_turns
    ADD CONSTRAINT hub_session_turns_session_id_fkey FOREIGN KEY (session_id) REFERENCES public.hub_sessions(id) ON DELETE CASCADE;

ALTER TABLE ONLY public.hub_sessions
    ADD CONSTRAINT hub_sessions_active_turn_session_fk FOREIGN KEY (active_turn_id, id) REFERENCES public.hub_session_turns(id, session_id) DEFERRABLE INITIALLY DEFERRED;

ALTER TABLE ONLY public.hub_sessions
    ADD CONSTRAINT hub_sessions_agent_id_fkey FOREIGN KEY (agent_id) REFERENCES public.agents(id) ON DELETE RESTRICT;

ALTER TABLE ONLY public.hub_sessions
    ADD CONSTRAINT hub_sessions_origin_external_identity_id_origin_platform_i_fkey FOREIGN KEY (origin_external_identity_id, origin_platform_id, owner_id) REFERENCES public.external_identities(id, platform_id, user_id) ON DELETE RESTRICT;

ALTER TABLE ONLY public.hub_sessions
    ADD CONSTRAINT hub_sessions_owner_id_fkey FOREIGN KEY (owner_id) REFERENCES public.users(id) ON DELETE CASCADE;

ALTER TABLE ONLY public.hub_sessions
    ADD CONSTRAINT hub_sessions_runtime_owner_id_fkey FOREIGN KEY (runtime_owner_id) REFERENCES public.runtimes(id) ON DELETE SET NULL;

ALTER TABLE ONLY public.integration_app_agents
    ADD CONSTRAINT integration_app_agents_agent_id_fkey FOREIGN KEY (agent_id) REFERENCES public.agents(id) ON DELETE CASCADE;

ALTER TABLE ONLY public.integration_app_agents
    ADD CONSTRAINT integration_app_agents_app_id_fkey FOREIGN KEY (app_id) REFERENCES public.oauth_apps(id) ON DELETE CASCADE;

ALTER TABLE ONLY public.integration_attachments
    ADD CONSTRAINT integration_attachments_hub_message_id_fkey FOREIGN KEY (hub_message_id) REFERENCES public.hub_session_messages(id) ON DELETE RESTRICT;

ALTER TABLE ONLY public.integration_attachments
    ADD CONSTRAINT integration_attachments_run_id_fkey FOREIGN KEY (run_id) REFERENCES public.runs(id) ON DELETE SET NULL;

ALTER TABLE ONLY public.integration_attachments
    ADD CONSTRAINT integration_attachments_session_id_fkey FOREIGN KEY (session_id) REFERENCES public.integration_sessions(id) ON DELETE CASCADE;

ALTER TABLE ONLY public.integration_messages
    ADD CONSTRAINT integration_messages_hub_message_id_fkey FOREIGN KEY (hub_message_id) REFERENCES public.hub_session_messages(id) ON DELETE RESTRICT;

ALTER TABLE ONLY public.integration_messages
    ADD CONSTRAINT integration_messages_run_id_fkey FOREIGN KEY (run_id) REFERENCES public.runs(id) ON DELETE SET NULL;

ALTER TABLE ONLY public.integration_messages
    ADD CONSTRAINT integration_messages_session_id_fkey FOREIGN KEY (session_id) REFERENCES public.integration_sessions(id) ON DELETE CASCADE;

ALTER TABLE ONLY public.integration_sessions
    ADD CONSTRAINT integration_sessions_agent_id_fkey FOREIGN KEY (agent_id) REFERENCES public.agents(id) ON DELETE CASCADE;

ALTER TABLE ONLY public.integration_sessions
    ADD CONSTRAINT integration_sessions_hub_session_id_fkey FOREIGN KEY (hub_session_id) REFERENCES public.hub_sessions(id) ON DELETE RESTRICT;

ALTER TABLE ONLY public.integration_sessions
    ADD CONSTRAINT integration_sessions_oauth_app_id_fkey FOREIGN KEY (oauth_app_id) REFERENCES public.oauth_apps(id) ON DELETE CASCADE;

ALTER TABLE ONLY public.integration_sessions
    ADD CONSTRAINT integration_sessions_owner_id_fkey FOREIGN KEY (owner_id) REFERENCES public.users(id) ON DELETE CASCADE;

ALTER TABLE ONLY public.integration_tool_requests
    ADD CONSTRAINT integration_tool_requests_follow_up_run_id_fkey FOREIGN KEY (follow_up_run_id) REFERENCES public.runs(id) ON DELETE SET NULL;

ALTER TABLE ONLY public.integration_tool_requests
    ADD CONSTRAINT integration_tool_requests_run_id_fkey FOREIGN KEY (run_id) REFERENCES public.runs(id) ON DELETE CASCADE;

ALTER TABLE ONLY public.integration_tool_requests
    ADD CONSTRAINT integration_tool_requests_session_id_fkey FOREIGN KEY (session_id) REFERENCES public.integration_sessions(id) ON DELETE CASCADE;

ALTER TABLE ONLY public.model_call_errors
    ADD CONSTRAINT model_call_errors_agent_id_fkey FOREIGN KEY (agent_id) REFERENCES public.agents(id) ON DELETE SET NULL;

ALTER TABLE ONLY public.model_call_errors
    ADD CONSTRAINT model_call_errors_model_connection_id_fkey FOREIGN KEY (model_connection_id) REFERENCES public.model_connections(id) ON DELETE SET NULL;

ALTER TABLE ONLY public.model_call_errors
    ADD CONSTRAINT model_call_errors_source_integration_app_id_fkey FOREIGN KEY (source_integration_app_id) REFERENCES public.oauth_apps(id) ON DELETE SET NULL;

ALTER TABLE ONLY public.model_call_errors
    ADD CONSTRAINT model_call_errors_subject_user_id_fkey FOREIGN KEY (subject_user_id) REFERENCES public.users(id) ON DELETE SET NULL;

ALTER TABLE ONLY public.model_connections
    ADD CONSTRAINT model_connections_created_by_fkey FOREIGN KEY (created_by) REFERENCES public.users(id) ON DELETE SET NULL;

ALTER TABLE ONLY public.model_connections
    ADD CONSTRAINT model_connections_owner_id_fkey FOREIGN KEY (owner_id) REFERENCES public.users(id) ON DELETE SET NULL;

ALTER TABLE ONLY public.model_token_usage
    ADD CONSTRAINT model_token_usage_agent_id_fkey FOREIGN KEY (agent_id) REFERENCES public.agents(id) ON DELETE SET NULL;

ALTER TABLE ONLY public.model_token_usage
    ADD CONSTRAINT model_token_usage_model_connection_id_fkey FOREIGN KEY (model_connection_id) REFERENCES public.model_connections(id) ON DELETE SET NULL;

ALTER TABLE ONLY public.model_token_usage
    ADD CONSTRAINT model_token_usage_source_integration_app_id_fkey FOREIGN KEY (source_integration_app_id) REFERENCES public.oauth_apps(id) ON DELETE SET NULL;

ALTER TABLE ONLY public.model_token_usage
    ADD CONSTRAINT model_token_usage_subject_user_id_fkey FOREIGN KEY (subject_user_id) REFERENCES public.users(id) ON DELETE SET NULL;

ALTER TABLE ONLY public.oauth_access_tokens
    ADD CONSTRAINT oauth_access_tokens_agent_id_fkey FOREIGN KEY (agent_id) REFERENCES public.agents(id) ON DELETE CASCADE;

ALTER TABLE ONLY public.oauth_access_tokens
    ADD CONSTRAINT oauth_access_tokens_oauth_app_id_fkey FOREIGN KEY (oauth_app_id) REFERENCES public.oauth_apps(id) ON DELETE CASCADE;

ALTER TABLE ONLY public.oauth_access_tokens
    ADD CONSTRAINT oauth_access_tokens_origin_external_identity_id_fkey FOREIGN KEY (origin_external_identity_id) REFERENCES public.external_identities(id) ON DELETE CASCADE;

ALTER TABLE ONLY public.oauth_access_tokens
    ADD CONSTRAINT oauth_access_tokens_owner_id_fkey FOREIGN KEY (owner_id) REFERENCES public.users(id) ON DELETE CASCADE;

ALTER TABLE ONLY public.oauth_access_tokens
    ADD CONSTRAINT oauth_access_tokens_subject_user_id_fkey FOREIGN KEY (subject_user_id) REFERENCES public.users(id) ON DELETE CASCADE;

ALTER TABLE ONLY public.oauth_apps
    ADD CONSTRAINT oauth_apps_agent_id_fkey FOREIGN KEY (agent_id) REFERENCES public.agents(id) ON DELETE CASCADE;

ALTER TABLE ONLY public.oauth_apps
    ADD CONSTRAINT oauth_apps_authentication_origin_fk FOREIGN KEY (authentication_channel_id, external_platform_id) REFERENCES public.authentication_channels(id, platform_id) ON DELETE RESTRICT;

ALTER TABLE ONLY public.oauth_apps
    ADD CONSTRAINT oauth_apps_owner_id_fkey FOREIGN KEY (owner_id) REFERENCES public.users(id) ON DELETE CASCADE;

ALTER TABLE ONLY public.oauth_authorization_codes
    ADD CONSTRAINT oauth_authorization_codes_external_identity_id_fkey FOREIGN KEY (external_identity_id) REFERENCES public.external_identities(id) ON DELETE CASCADE;

ALTER TABLE ONLY public.oauth_authorization_codes
    ADD CONSTRAINT oauth_authorization_codes_oauth_app_id_fkey FOREIGN KEY (oauth_app_id) REFERENCES public.oauth_apps(id) ON DELETE CASCADE;

ALTER TABLE ONLY public.oauth_authorization_codes
    ADD CONSTRAINT oauth_authorization_codes_subject_user_id_fkey FOREIGN KEY (subject_user_id) REFERENCES public.users(id) ON DELETE CASCADE;

ALTER TABLE ONLY public.run_events
    ADD CONSTRAINT run_events_hub_message_id_fkey FOREIGN KEY (hub_message_id) REFERENCES public.hub_session_messages(id) ON DELETE RESTRICT;

ALTER TABLE ONLY public.run_events
    ADD CONSTRAINT run_events_run_id_fkey FOREIGN KEY (run_id) REFERENCES public.runs(id) ON DELETE CASCADE;

ALTER TABLE ONLY public.run_model_connection_snapshots
    ADD CONSTRAINT run_model_connection_snapshots_model_connection_id_fkey FOREIGN KEY (model_connection_id) REFERENCES public.model_connections(id) ON DELETE RESTRICT;

ALTER TABLE ONLY public.run_model_connection_snapshots
    ADD CONSTRAINT run_model_connection_snapshots_run_id_fkey FOREIGN KEY (run_id) REFERENCES public.runs(id) ON DELETE CASCADE;

ALTER TABLE ONLY public.runs
    ADD CONSTRAINT runs_agent_id_fkey FOREIGN KEY (agent_id) REFERENCES public.agents(id) ON DELETE CASCADE;

ALTER TABLE ONLY public.runs
    ADD CONSTRAINT runs_automation_id_fkey FOREIGN KEY (automation_id) REFERENCES public.automations(id) ON DELETE SET NULL;

ALTER TABLE ONLY public.runs
    ADD CONSTRAINT runs_hub_message_session_fk FOREIGN KEY (hub_message_id, hub_session_id) REFERENCES public.hub_session_messages(id, session_id) ON DELETE RESTRICT;

ALTER TABLE ONLY public.runs
    ADD CONSTRAINT runs_hub_session_id_fkey FOREIGN KEY (hub_session_id) REFERENCES public.hub_sessions(id) ON DELETE RESTRICT;

ALTER TABLE ONLY public.runs
    ADD CONSTRAINT runs_hub_turn_session_fk FOREIGN KEY (hub_turn_id, hub_session_id) REFERENCES public.hub_session_turns(id, session_id) ON DELETE RESTRICT;

ALTER TABLE ONLY public.runs
    ADD CONSTRAINT runs_integration_session_fk FOREIGN KEY (integration_session_id) REFERENCES public.integration_sessions(id) ON DELETE SET NULL;

ALTER TABLE ONLY public.runs
    ADD CONSTRAINT runs_model_source_integration_app_id_fkey FOREIGN KEY (model_source_integration_app_id) REFERENCES public.oauth_apps(id) ON DELETE SET NULL;

ALTER TABLE ONLY public.runs
    ADD CONSTRAINT runs_model_subject_user_id_fkey FOREIGN KEY (model_subject_user_id) REFERENCES public.users(id) ON DELETE SET NULL;

ALTER TABLE ONLY public.runs
    ADD CONSTRAINT runs_owner_id_fkey FOREIGN KEY (owner_id) REFERENCES public.users(id) ON DELETE CASCADE;

ALTER TABLE ONLY public.runs
    ADD CONSTRAINT runs_parent_run_id_fkey FOREIGN KEY (parent_run_id) REFERENCES public.runs(id) ON DELETE SET NULL;

ALTER TABLE ONLY public.runs
    ADD CONSTRAINT runs_runtime_id_fkey FOREIGN KEY (runtime_id) REFERENCES public.runtimes(id) ON DELETE SET NULL;

ALTER TABLE ONLY public.runs
    ADD CONSTRAINT runs_widget_session_id_fkey FOREIGN KEY (widget_session_id) REFERENCES public.embed_sessions(id) ON DELETE SET NULL;

ALTER TABLE ONLY public.runtime_codex_readiness
    ADD CONSTRAINT runtime_codex_readiness_runtime_id_fkey FOREIGN KEY (runtime_id) REFERENCES public.runtimes(id) ON DELETE CASCADE;

ALTER TABLE ONLY public.runtime_enrollment_tokens
    ADD CONSTRAINT runtime_enrollment_tokens_consumed_by_runtime_id_fkey FOREIGN KEY (consumed_by_runtime_id) REFERENCES public.runtimes(id) ON DELETE SET NULL;

ALTER TABLE ONLY public.runtime_enrollment_tokens
    ADD CONSTRAINT runtime_enrollment_tokens_created_by_fkey FOREIGN KEY (created_by) REFERENCES public.users(id) ON DELETE SET NULL;

ALTER TABLE ONLY public.runtime_session_cleanup_obligations
    ADD CONSTRAINT runtime_session_cleanup_obligations_runtime_id_fkey FOREIGN KEY (runtime_id) REFERENCES public.runtimes(id) ON DELETE CASCADE;

ALTER TABLE ONLY public.session_bundle_deletion_queue
    ADD CONSTRAINT session_bundle_deletion_queue_agent_id_fkey FOREIGN KEY (agent_id) REFERENCES public.agents(id) ON DELETE CASCADE;

ALTER TABLE ONLY public.session_bundle_deletion_queue
    ADD CONSTRAINT session_bundle_deletion_queue_session_id_fkey FOREIGN KEY (session_id) REFERENCES public.hub_sessions(id) ON DELETE CASCADE;

ALTER TABLE ONLY public.sessions
    ADD CONSTRAINT sessions_user_id_fkey FOREIGN KEY (user_id) REFERENCES public.users(id) ON DELETE CASCADE;

ALTER TABLE ONLY public.skills
    ADD CONSTRAINT skills_owner_id_fkey FOREIGN KEY (owner_id) REFERENCES public.users(id) ON DELETE CASCADE;

ALTER TABLE ONLY public.system_default_model_connection
    ADD CONSTRAINT system_default_model_connection_model_connection_id_fkey FOREIGN KEY (model_connection_id) REFERENCES public.model_connections(id) ON DELETE CASCADE;

ALTER TABLE ONLY public.system_default_model_connection
    ADD CONSTRAINT system_default_model_connection_updated_by_fkey FOREIGN KEY (updated_by) REFERENCES public.users(id) ON DELETE SET NULL;

ALTER TABLE ONLY public.user_erasure_bundle_objects
    ADD CONSTRAINT user_erasure_bundle_objects_user_id_fkey FOREIGN KEY (user_id) REFERENCES public.user_erasure_jobs(user_id) ON DELETE CASCADE;

-- Required control-plane singleton rows.
INSERT INTO public.auth_policy
    (singleton, password_registration_enabled, password_login_enabled,
     email_verification_required)
VALUES (true, true, true, false);

INSERT INTO public.codex_version_rollout (singleton)
VALUES (true);
