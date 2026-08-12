-- 会话 bundle 同步状态：runtime 停止/drain 时逐会话打包上传的进度跟踪。
-- 用于管理端展示"剩余未打包数量"，以及恢复时判定"该会话是否有可用 bundle"。
ALTER TABLE public.hub_sessions
    ADD COLUMN bundle_sync_status text,
    ADD COLUMN bundle_sync_updated_at timestamp with time zone;

COMMENT ON COLUMN public.hub_sessions.bundle_sync_status IS
    'bundle 打包同步状态：pending（待打包）/ uploading（打包上传中）/ done（完成且校验通过）/ failed（失败）/ NULL（无进行中的同步）';
COMMENT ON COLUMN public.hub_sessions.bundle_sync_updated_at IS
    'bundle 打包状态最近更新时间';
