import assert from 'node:assert/strict';
import { ApiClient, loginAsAdmin, poll, waitForRunStatus } from '../../support/api.mjs';

const UUID_PATTERN = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/;

export default async function providerErrorApiScenario(context) {
  const client = new ApiClient(context.baseURL);
  await loginAsAdmin(client);
  const agentName = context.unique('QA Provider Error Agent');
  let agentId;
  let scenarioError;
  const cleanupErrors = [];
  try {
    const { data: agent } = await client.post('/api/agents', {
      name: agentName,
      instructions: 'Exercise deterministic provider failure accounting.',
      visibility: 'private',
      public_to: []
    });
    agentId = agent.id;
    assert.match(agent.id, UUID_PATTERN);

    const { data: run } = await client.post(`/api/agents/${agent.id}/runs`, {
      message: 'fixture:model-error verify failed response accounting',
      hub_session_id: null,
      parent_run_id: null
    });
    await waitForRunStatus(client, agent.id, run.id, 'failed', 45_000);

    const { data: events } = await client.get(`/api/runs/${run.id}/events`);
    assert.ok(events.some((event) => event.event_type === 'status'
      && (event.content === 'failed' || event.payload?.status === 'failed')));

    const accounting = await poll(() => {
      const usage = context.compose.psql(`
        SELECT response_status || '|' || input_tokens || '|' || output_tokens || '|' ||
               total_tokens || '|' || cached_tokens || '|' || reasoning_tokens
        FROM model_token_usage
        WHERE agent_id = '${agent.id}'
        ORDER BY occurred_at DESC
        LIMIT 1
      `);
      const error = context.compose.psql(`
        SELECT response_status || '|' || COALESCE(error_code, '')
        FROM model_call_errors
        WHERE agent_id = '${agent.id}'
        ORDER BY occurred_at DESC
        LIMIT 1
      `);
      return { usage, error };
    }, (value) => Boolean(value.usage && value.error), {
      timeoutMs: 10_000,
      description: 'failed provider accounting rows'
    });
    assert.equal(accounting.usage, 'failed|5|2|7|1|1');
    assert.equal(accounting.error, 'failed|fake_model_error');
  } catch (error) {
    scenarioError = error;
  } finally {
    if (agentId) {
      try {
        await client.delete(`/api/agents/${agentId}`, { expectedStatus: [204, 404] });
      } catch (error) {
        cleanupErrors.push(error);
      }
    }
  }

  if (scenarioError && cleanupErrors.length > 0) {
    throw new AggregateError([scenarioError, ...cleanupErrors], 'Provider error scenario and cleanup failed');
  }
  if (scenarioError) throw scenarioError;
  if (cleanupErrors.length === 1) throw cleanupErrors[0];
  if (cleanupErrors.length > 1) {
    throw new AggregateError(cleanupErrors, 'Provider error scenario cleanup failed');
  }
}
