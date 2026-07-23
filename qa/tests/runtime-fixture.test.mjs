import assert from 'node:assert/strict';
import { spawn, spawnSync } from 'node:child_process';
import { createHash } from 'node:crypto';
import { chmodSync, mkdirSync, mkdtempSync, readFileSync, rmSync, writeFileSync } from 'node:fs';
import { createServer } from 'node:http';
import { tmpdir } from 'node:os';
import { dirname, join, resolve } from 'node:path';
import test from 'node:test';
import { fileURLToPath } from 'node:url';
import { ApiClient } from '../support/api.mjs';
import { ComposeHarness } from '../support/compose.mjs';

const repoRoot = resolve(dirname(fileURLToPath(import.meta.url)), '..', '..');

async function listen(server) {
  await new Promise((resolve, reject) => {
    server.once('error', reject);
    server.listen(0, '127.0.0.1', resolve);
  });
  const address = server.address();
  return `http://127.0.0.1:${address.port}`;
}

async function close(server) {
  await new Promise((resolve, reject) => server.close((error) => error ? reject(error) : resolve()));
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
    await new Promise((resolve) => setTimeout(resolve, 25));
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

test('QA Compose uses the in-network Codex release fixture over explicitly allowed HTTP', () => {
  const harness = new ComposeHarness({
    repoRoot: '/fixture/agent-hub',
    project: 'agent-hub-qa-runtime-fixture'
  });

  assert.equal(
    harness.environment.HUB_CODEX_GITHUB_API_BASE,
    'http://fake-model-provider:8080/codex'
  );
  assert.equal(harness.environment.HUB_CODEX_GITHUB_ALLOW_HTTP, 'true');
  assert.match(harness.environment.HUB_NETWORK_SUBNET, /^10\.(?:1[2-9]\d|2[0-4]\d|25[0-5])\.\d{1,3}\.0\/24$/);
  assert.match(harness.environment.MODEL_NETWORK_SUBNET, /^10\.(?:1[2-9]\d|2[0-4]\d|25[0-5])\.\d{1,3}\.0\/24$/);
  assert.notEqual(harness.environment.HUB_NETWORK_SUBNET, harness.environment.MODEL_NETWORK_SUBNET);
});

test('fake model provider preserves Responses success, error, auth, and usage behavior', async (t) => {
  const baseURL = await startFakeProvider(t);
  const request = (body, authorization = 'Bearer fixture-provider-key') => fetch(`${baseURL}/v1/responses`, {
    method: 'POST',
    headers: { authorization, 'content-type': 'application/json' },
    body: JSON.stringify(body)
  });

  assert.equal((await request({}, 'Bearer wrong-key')).status, 401);

  const success = await request({ model: 'hub-proxy-smoke', input: 'hello' });
  assert.equal(success.status, 200);
  assert.deepEqual(await success.json(), {
    id: 'resp_proxy_fake_completed',
    object: 'response',
    model: 'hub-proxy-smoke',
    status: 'completed',
    output_text: 'Fake Codex completed run through the Hub model proxy.',
    usage: {
      input_tokens: 11,
      output_tokens: 7,
      total_tokens: 18,
      input_tokens_details: { cached_tokens: 3 },
      output_tokens_details: { reasoning_tokens: 5 }
    }
  });

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

test('fake Codex release serves verifiable Linux artifacts with an executable versioned shim', async (t) => {
  const baseURL = await startFakeProvider(t);
  const version = '0.145.0-fixture';
  const releaseResponse = await fetch(`${baseURL}/codex/releases/tags/rust-v${version}`);
  assert.equal(releaseResponse.status, 200);
  const release = await releaseResponse.json();
  assert.equal(release.tag_name, `rust-v${version}`);
  assert.deepEqual(release.assets.map((asset) => asset.name), [
    'codex-x86_64-unknown-linux-musl.zst',
    'codex-aarch64-unknown-linux-musl.zst'
  ]);

  let artifactBytes;
  for (const asset of release.assets) {
    const response = await fetch(asset.browser_download_url);
    assert.equal(response.status, 200);
    assert.equal(response.headers.get('content-type'), 'application/zstd');
    const bytes = Buffer.from(await response.arrayBuffer());
    artifactBytes ??= bytes;
    assert.deepEqual(bytes, artifactBytes);
    assert.equal(asset.size, bytes.length);
    assert.equal(asset.digest, `sha256:${createHash('sha256').update(bytes).digest('hex')}`);
  }

  const decompressed = spawnSync('zstd', ['-q', '-d', '-c'], { input: artifactBytes });
  assert.equal(decompressed.status, 0, decompressed.stderr.toString());
  assert.deepEqual(decompressed.stdout, readFileSync(resolve(repoRoot, 'deploy/fake-managed-codex.sh')));

  const root = mkdtempSync(join(tmpdir(), 'agent-hub-managed-codex-'));
  t.after(() => rmSync(root, { recursive: true, force: true }));
  const installed = join(root, version, 'codex');
  mkdirSync(dirname(installed));
  writeFileSync(installed, decompressed.stdout, { mode: 0o755 });
  chmodSync(installed, 0o755);
  const versionResult = spawnSync(installed, ['--version'], { encoding: 'utf8' });
  assert.equal(versionResult.status, 0, versionResult.stderr);
  assert.equal(versionResult.stdout.trim().split(/\s+/).includes(version), true);
  const helpResult = spawnSync(installed, ['app-server', '--help'], { encoding: 'utf8' });
  assert.equal(helpResult.status, 0, helpResult.stderr);

  const staged = join(
    root,
    `.staging-${version}-12345678-1234-1234-1234-123456789abc`,
    'codex'
  );
  mkdirSync(dirname(staged));
  writeFileSync(staged, decompressed.stdout, { mode: 0o755 });
  chmodSync(staged, 0o755);
  const stagedVersionResult = spawnSync(staged, ['--version'], { encoding: 'utf8' });
  assert.equal(stagedVersionResult.status, 0, stagedVersionResult.stderr);
  assert.equal(stagedVersionResult.stdout.trim().split(/\s+/).includes(version), true);
});

test('fake Codex uses the configured default provider when another provider sorts first', async (t) => {
  const root = mkdtempSync(join(tmpdir(), 'agent-hub-codex-provider-'));
  t.after(() => rmSync(root, { recursive: true, force: true }));
  const codexHome = join(root, 'codex-home');
  const cwd = join(root, 'fixture-session', 'workspace');
  mkdirSync(codexHome, { recursive: true });
  mkdirSync(cwd, { recursive: true });

  const decoyBindingId = '00000000-0000-0000-0000-000000000001';
  const defaultBindingId = 'ffffffff-ffff-ffff-ffff-ffffffffffff';
  const receivedBindingIds = [];
  const server = createServer((request, response) => {
    receivedBindingIds.push(request.headers['x-agent-hub-model-binding-id']);
    response.writeHead(200, { 'content-type': 'application/json' });
    response.end(JSON.stringify({
      id: 'resp_chat_converted',
      object: 'response',
      status: 'completed',
      output: [{
        type: 'message',
        role: 'assistant',
        content: [{ type: 'output_text', text: 'default provider selected' }]
      }]
    }));
  });
  const baseURL = await listen(server);
  t.after(() => close(server));

  writeFileSync(join(codexHome, 'config.toml'), `
model = "default-model"
model_provider = "agent_hub_ffffffffffffffffffffffffffffffff"

[model_providers.agent_hub_00000000000000000000000000000001]
base_url = "${baseURL}/v1"

[model_providers.agent_hub_00000000000000000000000000000001.http_headers]
x-agent-hub-model-binding-id = "${decoyBindingId}"

[model_providers.agent_hub_ffffffffffffffffffffffffffffffff]
base_url = "${baseURL}/v1"

[model_providers.agent_hub_ffffffffffffffffffffffffffffffff.http_headers]
x-agent-hub-model-binding-id = "${defaultBindingId}"
`);

  const threadId = 'fake-app-server-thread-fixture-session';
  const requests = [
    {
      jsonrpc: '2.0',
      id: 'initialize',
      method: 'initialize',
      params: { clientInfo: { name: 'qa-fixture', version: '1.0.0' } }
    },
    { jsonrpc: '2.0', method: 'initialized', params: {} },
    {
      jsonrpc: '2.0',
      id: 'thread-start',
      method: 'thread/start',
      params: { cwd, approvalPolicy: 'never', developerInstructions: 'QA fixture' }
    },
    {
      jsonrpc: '2.0',
      id: 'turn-start',
      method: 'turn/start',
      params: {
        threadId,
        source: 'console',
        input: [{ type: 'text', text: 'Use the configured default provider.' }],
        metadata: { agent_hub_run_id: 'fixture-run' }
      }
    }
  ];
  const child = spawn(
    resolve(repoRoot, 'deploy/fake-codex-app-server.sh'),
    ['app-server', '--listen', 'stdio://'],
    {
      env: { ...process.env, CODEX_HOME: codexHome },
      stdio: ['pipe', 'pipe', 'pipe']
    }
  );
  let stdout = '';
  let stderr = '';
  child.stdout.setEncoding('utf8');
  child.stderr.setEncoding('utf8');
  child.stdout.on('data', (chunk) => { stdout += chunk; });
  child.stderr.on('data', (chunk) => { stderr += chunk; });
  child.stdin.end(`${requests.map((request) => JSON.stringify(request)).join('\n')}\n`);
  const exitCode = await new Promise((resolve, reject) => {
    child.once('error', reject);
    child.once('close', resolve);
  });

  assert.equal(exitCode, 0, stderr);
  assert.deepEqual(receivedBindingIds, [defaultBindingId]);
  const messages = stdout.trim().split('\n').map((line) => JSON.parse(line));
  assert.equal(
    messages.some((message) => message.method === 'turn/completed'
      && message.params.turn.items[0].text === 'default provider selected'),
    true
  );
});

test('fake Codex releases a held console turn only for a complete fixture:release steer item', (t) => {
  const root = mkdtempSync(join(tmpdir(), 'agent-hub-codex-steer-'));
  t.after(() => rmSync(root, { recursive: true, force: true }));
  const codexHome = join(root, 'codex-home');
  const cwd = join(root, 'fixture-session', 'workspace');
  mkdirSync(codexHome, { recursive: true });
  mkdirSync(cwd, { recursive: true });
  const threadId = 'fake-app-server-thread-fixture-session';
  const turnId = 'fake-app-server-turn-fixture-run';
  const requests = [
    {
      jsonrpc: '2.0',
      id: 'initialize',
      method: 'initialize',
      params: { clientInfo: { name: 'qa-fixture', version: '1.0.0' } }
    },
    { jsonrpc: '2.0', method: 'initialized', params: {} },
    {
      jsonrpc: '2.0',
      id: 'thread-start',
      method: 'thread/start',
      params: { cwd, approvalPolicy: 'never', developerInstructions: 'QA fixture' }
    },
    {
      jsonrpc: '2.0',
      id: 'turn-start',
      method: 'turn/start',
      params: {
        threadId,
        source: 'console',
        input: [{ type: 'text', text: 'fixture:hold' }],
        metadata: { agent_hub_run_id: 'fixture-run' }
      }
    },
    {
      jsonrpc: '2.0',
      id: 'steer-containing-release',
      method: 'turn/steer',
      params: {
        threadId,
        expectedTurnId: turnId,
        input: [{ type: 'text', text: 'keep waiting; fixture:release later' }]
      }
    },
    {
      jsonrpc: '2.0',
      id: 'steer-release',
      method: 'turn/steer',
      params: {
        threadId,
        expectedTurnId: turnId,
        input: [
          { type: 'text', text: 'release context' },
          { type: 'text', text: 'fixture:release' }
        ]
      }
    }
  ];

  const result = spawnSync(
    resolve(repoRoot, 'deploy/fake-codex-app-server.sh'),
    ['app-server', '--listen', 'stdio://'],
    {
      env: { ...process.env, CODEX_HOME: codexHome },
      input: `${requests.map((request) => JSON.stringify(request)).join('\n')}\n`,
      encoding: 'utf8'
    }
  );
  assert.equal(result.status, 0, result.stderr);
  const messages = result.stdout.trim().split('\n').map((line) => JSON.parse(line));
  const firstSteer = messages.findIndex((message) => message.id === 'steer-containing-release');
  const releaseSteer = messages.findIndex((message) => message.id === 'steer-release');
  assert.ok(firstSteer >= 0 && releaseSteer > firstSteer);
  assert.equal(
    messages.slice(firstSteer + 1, releaseSteer).some((message) => message.method === 'turn/completed'),
    false
  );
  const completion = messages.slice(releaseSteer + 1).find((message) => message.method === 'turn/completed');
  assert.equal(completion?.params.threadId, threadId);
  assert.equal(completion?.params.turn.id, turnId);
  assert.equal(completion?.params.turn.status, 'completed');
  assert.equal(completion?.params.turn.items[0].type, 'agentMessage');
  assert.ok(completion?.params.turn.items[0].text.length > 0);
});
