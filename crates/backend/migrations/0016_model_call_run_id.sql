-- 记录模型代理失败时所属的 run，便于终态失败时把已脱敏的错误详情推给对应前端。
ALTER TABLE model_call_errors ADD COLUMN run_id uuid NULL;
CREATE INDEX model_call_errors_run_occurred_idx ON model_call_errors (run_id, occurred_at DESC, id DESC);
