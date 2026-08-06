-- Merge legacy streaming deltas that were persisted before the Hub moved
-- deltas to the in-memory run event bus. This migration is idempotent:
-- running it again has no further effect.

-- Item deltas: a completed phase row already carries the full summary/output,
-- so all summary_delta/output_delta rows for that item are redundant.
DELETE FROM public.run_events AS legacy_delta
WHERE legacy_delta.event_type = 'item'
  AND legacy_delta.payload->>'phase' IN ('summary_delta', 'output_delta')
  AND legacy_delta.payload->>'item_id' IS NOT NULL
  AND EXISTS (
      SELECT 1
      FROM public.run_events AS completed
      WHERE completed.run_id = legacy_delta.run_id
        AND completed.event_type = 'item'
        AND completed.payload->>'item_id' = legacy_delta.payload->>'item_id'
        AND completed.payload->>'phase' = 'completed'
  );

-- Item deltas without a completed row (interrupted runs): merge by seq order
-- into the earliest delta row and delete the remaining deltas.
WITH delta_only_items AS (
    SELECT delta.run_id,
           delta.payload->>'item_id' AS item_id,
           min(delta.seq) AS target_seq,
           string_agg(COALESCE(delta.payload->>'summary', ''), '' ORDER BY delta.seq) AS merged_summary,
           string_agg(COALESCE(delta.payload->>'output', ''), '' ORDER BY delta.seq) AS merged_output
    FROM public.run_events AS delta
    WHERE delta.event_type = 'item'
      AND delta.payload->>'phase' IN ('summary_delta', 'output_delta')
      AND delta.payload->>'item_id' IS NOT NULL
      AND NOT EXISTS (
          SELECT 1
          FROM public.run_events AS completed
          WHERE completed.run_id = delta.run_id
            AND completed.event_type = 'item'
            AND completed.payload->>'item_id' = delta.payload->>'item_id'
            AND completed.payload->>'phase' = 'completed'
      )
    GROUP BY delta.run_id, delta.payload->>'item_id'
)
UPDATE public.run_events AS target
SET payload = jsonb_set(
        jsonb_set(
            jsonb_set(
                target.payload,
                '{phase}',
                to_jsonb('completed'::text),
                true
            ),
            '{summary}',
            to_jsonb(delta_only_items.merged_summary),
            true
        ),
        '{output}',
        to_jsonb(delta_only_items.merged_output),
        true
    )
FROM delta_only_items
WHERE target.run_id = delta_only_items.run_id
  AND target.seq = delta_only_items.target_seq;

WITH delta_only_items AS (
    SELECT delta.run_id,
           delta.payload->>'item_id' AS item_id,
           min(delta.seq) AS target_seq
    FROM public.run_events AS delta
    WHERE delta.event_type = 'item'
      AND delta.payload->>'phase' IN ('summary_delta', 'output_delta')
      AND delta.payload->>'item_id' IS NOT NULL
      AND NOT EXISTS (
          SELECT 1
          FROM public.run_events AS completed
          WHERE completed.run_id = delta.run_id
            AND completed.event_type = 'item'
            AND completed.payload->>'item_id' = delta.payload->>'item_id'
            AND completed.payload->>'phase' = 'completed'
      )
    GROUP BY delta.run_id, delta.payload->>'item_id'
)
DELETE FROM public.run_events AS extra_delta
USING delta_only_items
WHERE extra_delta.run_id = delta_only_items.run_id
  AND extra_delta.payload->>'item_id' = delta_only_items.item_id
  AND extra_delta.seq <> delta_only_items.target_seq
  AND extra_delta.event_type = 'item'
  AND extra_delta.payload->>'phase' IN ('summary_delta', 'output_delta');

-- Message deltas: when an assistant message row exists, it is authoritative,
-- so drop all of that run's message_delta rows.
DELETE FROM public.run_events AS legacy_delta
WHERE legacy_delta.event_type = 'message_delta'
  AND EXISTS (
      SELECT 1
      FROM public.run_events AS final_message
      WHERE final_message.run_id = legacy_delta.run_id
        AND final_message.event_type = 'message'
        AND final_message.role = 'assistant'
  );

-- Interrupted runs without an assistant message row: merge all message deltas
-- into one assistant message, keeping the earliest created_at.
WITH interrupted_message_deltas AS (
    SELECT delta.run_id,
           min(delta.seq) AS target_seq,
           min(delta.created_at) AS merged_created_at,
           string_agg(COALESCE(delta.content, ''), '' ORDER BY delta.seq) AS merged_content
    FROM public.run_events AS delta
    WHERE delta.event_type = 'message_delta'
    GROUP BY delta.run_id
)
UPDATE public.run_events AS target
SET event_type = 'message',
    role = 'assistant',
    content = interrupted_message_deltas.merged_content,
    created_at = interrupted_message_deltas.merged_created_at
FROM interrupted_message_deltas
WHERE target.run_id = interrupted_message_deltas.run_id
  AND target.seq = interrupted_message_deltas.target_seq;

WITH interrupted_message_deltas AS (
    SELECT delta.run_id,
           min(delta.seq) AS target_seq
    FROM public.run_events AS delta
    WHERE delta.event_type = 'message_delta'
    GROUP BY delta.run_id
)
DELETE FROM public.run_events AS extra_delta
USING interrupted_message_deltas
WHERE extra_delta.run_id = interrupted_message_deltas.run_id
  AND extra_delta.seq <> interrupted_message_deltas.target_seq
  AND extra_delta.event_type = 'message_delta';
