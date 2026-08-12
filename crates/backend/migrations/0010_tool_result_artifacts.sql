-- 第三方工具大结果归档：硬上限参数 + integration_tool_requests 归档元数据
ALTER TABLE public.system_settings
    ADD COLUMN max_tool_result_bytes bigint NOT NULL DEFAULT 4194304;

ALTER TABLE public.integration_tool_requests
    ADD COLUMN artifact_id uuid,
    ADD COLUMN artifact_size_bytes bigint,
    ADD COLUMN artifact_reason text,
    ADD COLUMN result_truncated boolean NOT NULL DEFAULT false;
