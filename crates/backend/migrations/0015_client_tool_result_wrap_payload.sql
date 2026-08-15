-- 客户端工具结果 result_payload 自包含包装层（clean cutover 配套）。
--
-- 0.3.9 起 submit_client_tool_result 写入
--   {"tool_call_id", "tool_name", "result": <ClientToolResultDto>}
-- 与 ClientToolContinuationResultDto 同构；续接解析/context 读取不再
-- 兼容旧纯 DTO 形状。本迁移把存量 client 路径（run.client_instance_id
-- 非空）已完成的旧形状行包一层：
--   {"status": "success", ...} -> {"tool_call_id": id, "tool_name": tool_name,
--                                    "result": {原值}}
-- 路径判定以 run.client_instance_id 为准（runtime 集成路径的
-- result_payload 是任意 JSON，可能带 status 键，不能用 JSON 形状判断）；
-- 同时排除已包装（有 result 键）与纯旧形状之外的行。

UPDATE integration_tool_requests
SET result_payload = jsonb_build_object(
    'tool_call_id', id::text,
    'tool_name', tool_name,
    'result', result_payload
)
WHERE status = 'completed'
  AND result_payload IS NOT NULL
  AND NOT (result_payload ? 'result')
  AND EXISTS (
      SELECT 1 FROM runs AS run
      WHERE run.id = integration_tool_requests.run_id
        AND run.client_instance_id IS NOT NULL
  );
