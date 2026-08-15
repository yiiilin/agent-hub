-- 客户端工具结果 result_payload 自包含包装层（clean cutover 配套）。
--
-- 0.3.9 起 submit_client_tool_result 写入
--   {"tool_call_id", "tool_name", "result": <ClientToolResultDto>}
-- 与 ClientToolContinuationResultDto 同构；续接解析/context 读取不再
-- 兼容旧纯 DTO 形状。本迁移把存量 client 路径已完成的旧形状行包一层：
--   {"status": "success", ...} -> {"tool_call_id": id, "tool_name": tool_name,
--                                    "result": {原值}}
-- 幂等：只匹配顶层有 status 且无 result 键的行（runtime 集成路径的
-- {truncated, content} 形状没有 status，不受影响）。

UPDATE integration_tool_requests
SET result_payload = jsonb_build_object(
    'tool_call_id', id::text,
    'tool_name', tool_name,
    'result', result_payload
)
WHERE status = 'completed'
  AND result_payload IS NOT NULL
  AND NOT (result_payload ? 'result')
  AND result_payload ? 'status';
