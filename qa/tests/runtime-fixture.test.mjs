import assert from 'node:assert/strict';
import { spawn } from 'node:child_process';
import { createServer } from 'node:http';
import { dirname, resolve } from 'node:path';
import test from 'node:test';
import { fileURLToPath } from 'node:url';
import { ApiClient, qaSourceIp } from '../support/api.mjs';
import { ComposeHarness } from '../support/compose.mjs';

const repoRoot = resolve(dirname(fileURLToPath(import.meta.url)), '..', '..');

async function listen(server) {
  await new Promise((resolveListen, reject) => {
    server.once('error', reject);
    server.listen(0, '127.0.0.1', resolveListen);
  });
  const address = server.address();
  return `http://127.0.0.1:${address.port}`;
}

async function close(server) {
  await new Promise((resolveClose, reject) => server.close((error) => error ? reject(error) : resolveClose()));
}

async function startFakeProvider(t) {
  const reservation = createServer();
  const url = await listen(reservation);
  await close(reservation);
  const port = new URL(url).port;
  let stderr = '';
  const child = spawn('python3', [resolve(repoRoot, 'deploy/fake-model-provider.py')], {
    env: {
      ...process.env,
      FAKE_MODEL_PROVIDER_API_KEY: 'fixture-provider-key',
      FAKE_MODEL_PROVIDER_PORT: port
    },
    stdio: ['ignore', 'ignore', 'pipe']
  });
  child.stderr.setEncoding('utf8');
  child.stderr.on('data', (chunk) => { stderr += chunk; });
  t.after(() => {
    if (child.exitCode === null) child.kill('SIGTERM');
  });

  const deadline = Date.now() + 5_000;
  while (Date.now() < deadline) {
    if (child.exitCode !== null) {
      throw new Error(`fake model provider exited ${child.exitCode}: ${stderr.trim()}`);
    }
    try {
      const response = await fetch(`${url}/not-found`);
      if (response.status === 404) return url;
    } catch {
      // The child may not have bound its socket yet.
    }
    await new Promise((resolveWait) => setTimeout(resolveWait, 25));
  }
  throw new Error(`timed out starting fake model provider: ${stderr.trim()}`);
}

test('API errors redact every Hub and Runtime credential class', async (t) => {
  const secrets = [
    'ahre_enrollment.fixture-secret',
    'ahrc_runtime.fixture-secret',
    'ahk_api.fixture-secret',
    'ahr_legacy.fixture-secret',
    'Bearer bearer.fixture-secret'
  ];
  const server = createServer((_request, response) => {
    response.writeHead(500, { 'content-type': 'application/json' });
    response.end(JSON.stringify({ error: secrets.join(' ') }));
  });
  t.after(() => server.close());
  const baseURL = await listen(server);

  await assert.rejects(new ApiClient(baseURL).get('/fixture-error'), (error) => {
    for (const secret of secrets) assert.equal(error.message.includes(secret), false);
    assert.equal(error.message.match(/\[REDACTED\]/g)?.length, secrets.length);
    return true;
  });
});

test('QA Compose allocates two isolated 24-bit network subnets', () => {
  const harness = new ComposeHarness({
    repoRoot: '/fixture/agent-hub',
    project: 'agent-hub-qa-runtime-fixture'
  });

  assert.equal(harness.environment.FRONTEND_PORT, '0');
  assert.equal(harness.environment.COMPOSE_PROFILES, 'ldap');
  assert.match(harness.environment.HUB_NETWORK_SUBNET, /^10\.(?:1[2-9]\d|2[0-4]\d|25[0-5])\.\d{1,3}\.0\/24$/);
  assert.match(harness.environment.MODEL_NETWORK_SUBNET, /^10\.(?:1[2-9]\d|2[0-4]\d|25[0-5])\.\d{1,3}\.0\/24$/);
  assert.notEqual(harness.environment.HUB_NETWORK_SUBNET, harness.environment.MODEL_NETWORK_SUBNET);
});

test('QA login clients use the reserved benchmark network for isolated source IPs', () => {
  assert.match(qaSourceIp(), /^198\.(?:18|19)\.\d{1,3}\.\d{1,3}$/);
});

test('fake model provider preserves Responses success, error, auth, and usage behavior', async (t) => {
  const baseURL = await startFakeProvider(t);
  const request = (body, authorization = 'Bearer fixture-provider-key', signal) => fetch(`${baseURL}/v1/responses`, {
    method: 'POST',
    headers: { authorization, 'content-type': 'application/json' },
    body: JSON.stringify(body),
    signal
  });

  assert.equal((await request({}, 'Bearer wrong-key')).status, 401);

  const success = await request({ model: 'hub-proxy-smoke', input: 'hello' });
  assert.equal(success.status, 200);
  assert.deepEqual(await success.json(), {
    id: 'resp_proxy_fake_completed',
    object: 'response',
    model: 'hub-proxy-smoke',
    status: 'completed',
    output_text: 'Fake model completed run through the Hub model proxy.',
    output: [{
      id: 'msg_proxy_fake_completed',
      type: 'message',
      role: 'assistant',
      status: 'completed',
      content: [{
        type: 'output_text',
        text: 'Fake model completed run through the Hub model proxy.'
      }]
    }],
    usage: {
      input_tokens: 11,
      output_tokens: 7,
      total_tokens: 18,
      input_tokens_details: { cached_tokens: 3 },
      output_tokens_details: { reasoning_tokens: 5 }
    }
  });

  const clientToolName = 'agent_hub_client_tool_1';
  const clientTool = await request({
    model: 'hub-proxy-smoke',
    input: `Agent Hub Integration context (JSON):\n${JSON.stringify({
      message: 'Please use the echo tool and preserve attachments',
      attachments: [],
      tool_result: null,
      tool_results: [],
      external_user: null
    })}`,
    tools: [{
      type: 'function',
      name: clientToolName,
      description: 'Echo the supplied message.',
      parameters: { type: 'object' }
    }]
  });
  assert.equal(clientTool.status, 200);
  const clientToolBody = await clientTool.json();
  assert.equal(clientToolBody.output[0].type, 'function_call');
  assert.equal(clientToolBody.output[0].name, clientToolName);
  assert.equal(clientToolBody.output[0].call_id, 'platform|tool-call');

  const holdController = new AbortController();
  const hold = await request({
    model: 'hub-proxy-smoke',
    stream: true,
    input: `Agent Hub Integration context (JSON):\n${JSON.stringify({
      message: 'fixture:hold',
      attachments: [],
      tool_result: null,
      tool_results: [],
      external_user: null
    })}`
  }, 'Bearer fixture-provider-key', holdController.signal);
  assert.equal(hold.status, 200);
  const holdBody = hold.text().then(
    () => 'completed',
    (error) => holdController.signal.aborted ? 'aborted' : Promise.reject(error)
  );
  const holdOutcome = await Promise.race([
    holdBody,
    new Promise((resolveHeld) => setTimeout(() => resolveHeld('held'), 250))
  ]);
  const concurrent = await request(
    { model: 'hub-proxy-smoke', input: 'concurrent while hold is open' },
    'Bearer fixture-provider-key',
    AbortSignal.timeout(1_000)
  );
  assert.equal(
    concurrent.status,
    200,
    'A held streaming response must not block a later model request'
  );
  holdController.abort();
  await holdBody;
  assert.equal(holdOutcome, 'held', 'Integration Context hold response must remain open');

  const failure = await request({ model: 'custom-model', input: 'fixture:model-error' });
  assert.equal(failure.status, 200);
  assert.deepEqual(await failure.json(), {
    id: 'resp_proxy_fake_error',
    object: 'response',
    model: 'custom-model',
    status: 'failed',
    error: {
      code: 'fake_model_error',
      message: 'Deterministic fake provider failure.'
    },
    usage: {
      input_tokens: 5,
      output_tokens: 2,
      total_tokens: 7,
      input_tokens_details: { cached_tokens: 1 },
      output_tokens_details: { reasoning_tokens: 1 }
    }
  });
});
