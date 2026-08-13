-- 硬停止（force-stop）：操作记录、恢复源、强制停止中状态
-- 命令投递/ACK/重试全部走 WebSocket + 10 秒上报兜底，无持久命令账本。

-- 1. force-stop 操作记录（request_id 必填；同会话同 key 幂等）。
CREATE TABLE public.force_stop_operation (
    operation_id UUID PRIMARY KEY,
    session_id UUID NOT NULL REFERENCES public.hub_sessions(id),
    run_id UUID NOT NULL REFERENCES public.runs(id),
    request_id TEXT NOT NULL,
    target_runtime_id UUID,
    state TEXT NOT NULL DEFAULT 'pending'
        CHECK (state IN ('pending', 'succeeded', 'snapshot_lost', 'abandoned')),
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    snapshot_uploaded_at TIMESTAMPTZ,
    UNIQUE (session_id, request_id)
);

-- 2. 会话恢复源：仅恢复中有值；非 restoring 必须 NULL。
ALTER TABLE public.hub_sessions
    ADD COLUMN recovery_source TEXT
    CHECK (recovery_source IS NULL OR recovery_source IN ('local_workspace', 'bundle'));
-- 升级回填：已有 restoring 会话默认按 bundle 恢复（旧版本无该列，必为 NULL）。
UPDATE public.hub_sessions
   SET recovery_source = 'bundle'
 WHERE lifecycle_status = 'restoring' AND recovery_source IS NULL;
ALTER TABLE public.hub_sessions
    ADD CONSTRAINT hub_sessions_recovery_source_shape CHECK (
        (lifecycle_status = 'restoring' AND recovery_source IS NOT NULL)
        OR (lifecycle_status <> 'restoring' AND recovery_source IS NULL)
    );

-- 3. 当前 bundle 来源（checkpoint 常规快照 / force_stop 强制停止快照）。
-- 先加列并回填旧 bundle 为 checkpoint。
ALTER TABLE public.hub_sessions
    ADD COLUMN current_bundle_kind TEXT
    CHECK (current_bundle_kind IS NULL OR current_bundle_kind IN ('checkpoint', 'force_stop'));
UPDATE public.hub_sessions
   SET current_bundle_kind = 'checkpoint'
 WHERE current_bundle_object_key IS NOT NULL AND current_bundle_kind IS NULL;
-- force_stop 快照无 checkpoint 元数据（引擎版本等允许 NULL），
-- 旧 hub_sessions_check2 要求全部非空，按 kind 分支重建。
ALTER TABLE public.hub_sessions DROP CONSTRAINT hub_sessions_check2;
ALTER TABLE public.hub_sessions
    ADD CONSTRAINT hub_sessions_check2 CHECK (
        (
            current_bundle_generation IS NULL
            AND current_bundle_object_key IS NULL
            AND current_bundle_checksum_sha256 IS NULL
            AND current_bundle_size_bytes IS NULL
            AND current_bundle_history_checkpoint IS NULL
            AND current_bundle_ownership_generation IS NULL
            AND current_bundle_producing_engine_version IS NULL
            AND current_bundle_created_at IS NULL
            AND current_bundle_runtime_id IS NULL
        )
        OR (
            current_bundle_kind = 'checkpoint'
            AND current_bundle_generation IS NOT NULL
            AND current_bundle_object_key IS NOT NULL
            AND btrim(current_bundle_object_key) <> ''
            AND current_bundle_checksum_sha256 IS NOT NULL
            AND btrim(current_bundle_checksum_sha256) <> ''
            AND current_bundle_size_bytes IS NOT NULL
            AND current_bundle_history_checkpoint IS NOT NULL
            AND current_bundle_ownership_generation IS NOT NULL
            AND current_bundle_producing_engine_version IS NOT NULL
            AND btrim(current_bundle_producing_engine_version) <> ''
            AND current_bundle_created_at IS NOT NULL
        )
        OR (
            current_bundle_kind = 'force_stop'
            AND current_bundle_generation IS NOT NULL
            AND current_bundle_object_key IS NOT NULL
            AND btrim(current_bundle_object_key) <> ''
            AND current_bundle_checksum_sha256 IS NOT NULL
            AND btrim(current_bundle_checksum_sha256) <> ''
            AND current_bundle_size_bytes IS NOT NULL
            AND current_bundle_history_checkpoint IS NOT NULL
            AND current_bundle_ownership_generation IS NOT NULL
            AND current_bundle_created_at IS NOT NULL
        )
    );
ALTER TABLE public.hub_sessions
    ADD CONSTRAINT hub_sessions_bundle_kind_shape CHECK (
        (current_bundle_object_key IS NULL AND current_bundle_kind IS NULL)
        OR (current_bundle_object_key IS NOT NULL AND current_bundle_kind IS NOT NULL)
    );

-- 4. 会话状态新增"强制停止中"。
ALTER TABLE public.hub_sessions
    DROP CONSTRAINT hub_sessions_lifecycle_status_check;
ALTER TABLE public.hub_sessions
    ADD CONSTRAINT hub_sessions_lifecycle_status_check CHECK (
        lifecycle_status = ANY (ARRAY[
            'waiting_for_runtime', 'restoring', 'online', 'saving',
            'offline', 'recovery_failed', 'historical', 'force_stopping'
        ])
    );
