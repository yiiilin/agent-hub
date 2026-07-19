import assert from 'node:assert/strict';
import { execFile } from 'node:child_process';
import { existsSync } from 'node:fs';
import {
  cp,
  mkdir,
  mkdtemp,
  readFile,
  readdir,
  rm,
  writeFile
} from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { test } from 'node:test';
import { promisify } from 'node:util';
import { writeSummary } from '../runner.mjs';
import {
  assertArtifactTreeSafe,
  redactSecrets,
  sanitizeArtifactTree
} from '../support/secrets.mjs';

const execFileAsync = promisify(execFile);
const TOKENS = [
  'ahs_sessionfixture',
  'ahe_embedfixture',
  'aho_oauthfixture',
  'ahw_webhookfixture',
  'ahk_apikeyfixture',
  'ahre_runtimeenrollmentfixture',
  'ahrc_runtimecredentialfixture',
  'ahrt_runtimetokenfixture',
  'ahr_runtimefixture'
];
const JSON_SECRETS = [
  'plain-api-key-fixture',
  'plain-client-secret-fixture',
  'plain-secret-fixture',
  'plain-token-fixture',
  'plain-password-fixture',
  'plain-cookie-fixture',
  'plain-authorization-fixture'
];
const ALL_SECRETS = [...TOKENS, ...JSON_SECRETS, 'opaque-bearer-fixture', 'opaque-cookie-fixture'];
const OAUTH_AUTHORIZATION_CODE = '1f8f5f0e-2b7a-4df3-9174-c4cc72d245d1';
const OAUTH_ERROR_CODE = 'invalid_request';

function oauthTraceFixture() {
  const callback = new URL('https://client.example.test/callback');
  callback.searchParams.set('code', OAUTH_AUTHORIZATION_CODE);
  callback.searchParams.set('state', 'fixture-state');
  callback.searchParams.set('error_code', OAUTH_ERROR_CODE);
  const postData = new URLSearchParams({
    grant_type: 'authorization_code',
    client_id: 'ahc_fixtureclient',
    client_secret: TOKENS[0],
    code: OAUTH_AUTHORIZATION_CODE,
    redirect_uri: 'https://client.example.test/callback',
    error_code: OAUTH_ERROR_CODE
  }).toString();
  const queryString = [
    { name: 'code', value: OAUTH_AUTHORIZATION_CODE },
    { name: 'state', value: 'fixture-state' },
    { name: 'error_code', value: OAUTH_ERROR_CODE }
  ];
  const formParams = [...new URLSearchParams(postData)].map(([name, value]) => ({ name, value }));
  const resourceName = 'oauth-form-fixture.dat';
  const lines = [
    {
      type: 'resource-snapshot',
      snapshot: {
        request: {
          method: 'GET',
          url: 'http://127.0.0.1:15173/api/oauth/authorize?client_id=ahc_fixtureclient',
          queryString: []
        },
        response: {
          status: 303,
          headers: [{ name: 'location', value: callback.href }]
        }
      }
    },
    {
      type: 'resource-snapshot',
      snapshot: {
        request: { method: 'GET', url: callback.href, queryString },
        response: { status: 200, headers: [] }
      }
    },
    {
      type: 'resource-snapshot',
      snapshot: {
        request: {
          method: 'POST',
          url: 'http://127.0.0.1:15173/api/oauth/token',
          headers: [{ name: 'content-type', value: 'application/x-www-form-urlencoded' }],
          queryString: [],
          postData: {
            mimeType: 'application/x-www-form-urlencoded',
            text: '',
            params: formParams,
            _sha1: resourceName
          }
        },
        response: { status: 200, headers: [] }
      }
    }
  ];
  return {
    callback: callback.href,
    postData,
    resourceName,
    jsonl: `${lines.map((line) => JSON.stringify(line)).join('\n')}\n`
  };
}

function fixtureText() {
  return [
    ...TOKENS,
    'Authorization: Bearer opaque-bearer-fixture',
    'Cookie: agent_hub_session=opaque-cookie-fixture',
    'Set-Cookie: agent_hub_session=ahs_sessionfixture; HttpOnly',
    JSON.stringify({
      api_key: JSON_SECRETS[0],
      client_secret: JSON_SECRETS[1],
      secret: JSON_SECRETS[2],
      token: JSON_SECRETS[3],
      password: JSON_SECRETS[4],
      cookie: JSON_SECRETS[5],
      authorization: JSON_SECRETS[6],
      safe: 'visible'
    })
  ].join('\n');
}

function assertSecretsAbsent(value) {
  for (const secret of ALL_SECRETS) {
    assert.equal(value.includes(secret), false, `redacted output retained ${secret.split('_', 1)[0]}`);
  }
}

test('redactSecrets covers token families, headers, JSON fields, and trace DOM snapshots', () => {
  const redacted = redactSecrets(fixtureText());
  assertSecretsAbsent(redacted);
  assert.match(redacted, /\[REDACTED\]/);
  assert.match(redacted, /"safe":"visible"/);
  const prettyJson = JSON.stringify({ password: JSON_SECRETS[4], safe: 'visible' }, null, 2);
  const redactedPrettyJson = redactSecrets(prettyJson);
  assert.equal(redactedPrettyJson.includes(JSON_SECRETS[4]), false);
  assert.equal(redactSecrets(redactedPrettyJson), redactedPrettyJson);

  const traceLine = JSON.stringify({
    type: 'frame-snapshot',
    snapshot: {
      html: ['BODY', {},
        ['INPUT', {
          __playwright_value_: 'dom-playwright-password-fixture',
          type: 'password',
          value: 'dom-password-fixture'
        }],
        ['CODE', {}, 'dom-code-fixture']]
    },
    headers: [{ name: 'Cookie', value: 'dom-cookie-fixture' }]
  });
  const redactedTrace = redactSecrets(traceLine);
  for (const secret of [
    'dom-playwright-password-fixture',
    'dom-password-fixture',
    'dom-code-fixture',
    'dom-cookie-fixture'
  ]) {
    assert.equal(redactedTrace.includes(secret), false);
  }
});

test('OAuth authorization codes are removed from redirects, request URLs, forms, and ZIP traces', async (t) => {
  const fixture = oauthTraceFixture();
  const plain = `Location: ${fixture.callback}\n${fixture.postData}\n`;
  const redactedPlain = redactSecrets(plain);
  const redactedJsonl = redactSecrets(fixture.jsonl);
  for (const value of [redactedPlain, redactedJsonl]) {
    assert.equal(value.includes(OAUTH_AUTHORIZATION_CODE), false);
    assert.equal(value.includes(`error_code=${OAUTH_ERROR_CODE}`), true);
  }
  const redactedPlainForm = new URLSearchParams(redactedPlain.trim().split('\n')[1]);
  assert.equal(redactedPlainForm.get('code'), '[REDACTED]');
  assert.equal(redactedPlainForm.get('error_code'), OAUTH_ERROR_CODE);
  const redactedLines = redactedJsonl.trim().split('\n').map(JSON.parse);
  const location = new URL(redactedLines[0].snapshot.response.headers[0].value);
  assert.equal(location.searchParams.get('code'), '[REDACTED]');
  assert.equal(location.searchParams.get('error_code'), OAUTH_ERROR_CODE);
  assert.deepEqual(redactedLines[1].snapshot.request.queryString, [
    { name: 'code', value: '[REDACTED]' },
    { name: 'state', value: 'fixture-state' },
    { name: 'error_code', value: OAUTH_ERROR_CODE }
  ]);
  const redactedFormParams = new Map(
    redactedLines[2].snapshot.request.postData.params.map(({ name, value }) => [name, value])
  );
  assert.equal(redactedFormParams.get('code'), '[REDACTED]');
  assert.equal(redactedFormParams.get('error_code'), OAUTH_ERROR_CODE);

  const temporaryRoot = await mkdtemp(join(tmpdir(), 'agent-hub-oauth-artifact-test-'));
  t.after(() => rm(temporaryRoot, { recursive: true, force: true }));
  const traceSource = join(temporaryRoot, 'trace-source');
  await mkdir(traceSource, { recursive: true });
  await mkdir(join(traceSource, 'resources'), { recursive: true });
  await writeFile(join(traceSource, 'trace.network'), fixture.jsonl);
  await writeFile(join(traceSource, 'trace.trace'), `${JSON.stringify({ type: 'context-options' })}\n`);
  await writeFile(join(traceSource, 'resources', fixture.resourceName), fixture.postData);
  const fixtureZip = join(temporaryRoot, 'fixture-trace.zip');
  await execFileAsync('zip', ['-q', '-r', '-X', fixtureZip, '.'], {
    cwd: traceSource,
    shell: false
  });

  const unsafeRoot = join(temporaryRoot, 'unsafe');
  await mkdir(unsafeRoot, { recursive: true });
  await cp(fixtureZip, join(unsafeRoot, 'trace.zip'));
  await assert.rejects(assertArtifactTreeSafe(unsafeRoot), /Artifact safety assertion failed/);
  assert.deepEqual(await readdir(unsafeRoot), []);

  const safeRoot = join(temporaryRoot, 'safe');
  await mkdir(safeRoot, { recursive: true });
  await cp(fixtureZip, join(safeRoot, 'trace.zip'));
  await writeFile(join(safeRoot, 'oauth.log'), plain);
  await sanitizeArtifactTree(safeRoot);
  await assertArtifactTreeSafe(safeRoot);
  await execFileAsync('unzip', ['-tqq', join(safeRoot, 'trace.zip')], { shell: false });
  const extracted = join(temporaryRoot, 'extracted');
  await mkdir(extracted, { recursive: true });
  await execFileAsync('unzip', ['-q', join(safeRoot, 'trace.zip'), '-d', extracted], { shell: false });
  for (const value of [
    await readFile(join(safeRoot, 'oauth.log'), 'utf8'),
    await readFile(join(extracted, 'trace.network'), 'utf8'),
    await readFile(join(extracted, 'resources', fixture.resourceName), 'utf8')
  ]) {
    assert.equal(value.includes(OAUTH_AUTHORIZATION_CODE), false);
    assert.equal(value.includes(OAUTH_ERROR_CODE), true);
  }
});

test('MCP secrets containers preserve arbitrary keys while removing every value from ZIP traces', async (t) => {
  const mcpSecrets = {
    LICENSE: 'opaque-license-fixture',
    VENDOR_BLOB: 'opaque-vendor-fixture',
    nested: { SECONDARY: 'opaque-secondary-fixture' }
  };
  const mcpConfig = {
    mcp_allowlist: [{ name: 'licensed-server', secrets: mcpSecrets }],
    safe_name: 'visible'
  };
  const redactedConfig = redactSecrets(mcpConfig);
  assert.deepEqual(redactedConfig.mcp_allowlist[0].secrets, {
    LICENSE: '[REDACTED]',
    VENDOR_BLOB: '[REDACTED]',
    nested: { SECONDARY: '[REDACTED]' }
  });
  assert.equal(redactedConfig.safe_name, 'visible');

  const resourceName = 'mcp-body-fixture.dat';
  const networkLine = JSON.stringify({
    type: 'resource-snapshot',
    snapshot: {
      request: {
        method: 'POST',
        url: 'http://127.0.0.1:15173/api/agents/fixture',
        headers: [{ name: 'content-type', value: 'application/json' }],
        postData: {
          mimeType: 'application/json',
          text: '',
          params: [],
          _sha1: resourceName
        }
      },
      response: { status: 200, headers: [] }
    }
  });
  const temporaryRoot = await mkdtemp(join(tmpdir(), 'agent-hub-mcp-artifact-test-'));
  t.after(() => rm(temporaryRoot, { recursive: true, force: true }));
  const traceSource = join(temporaryRoot, 'trace-source');
  await mkdir(traceSource, { recursive: true });
  await mkdir(join(traceSource, 'resources'), { recursive: true });
  await writeFile(join(traceSource, 'trace.network'), `${networkLine}\n`);
  await writeFile(join(traceSource, 'trace.trace'), `${JSON.stringify({ type: 'context-options' })}\n`);
  await writeFile(join(traceSource, 'resources', resourceName), JSON.stringify(mcpConfig));
  const fixtureZip = join(temporaryRoot, 'fixture-trace.zip');
  await execFileAsync('zip', ['-q', '-r', '-X', fixtureZip, '.'], {
    cwd: traceSource,
    shell: false
  });

  const unsafeRoot = join(temporaryRoot, 'unsafe');
  await mkdir(unsafeRoot, { recursive: true });
  await cp(fixtureZip, join(unsafeRoot, 'trace.zip'));
  await assert.rejects(assertArtifactTreeSafe(unsafeRoot), /Artifact safety assertion failed/);
  assert.deepEqual(await readdir(unsafeRoot), []);

  const safeRoot = join(temporaryRoot, 'safe');
  await mkdir(safeRoot, { recursive: true });
  await cp(fixtureZip, join(safeRoot, 'trace.zip'));
  await sanitizeArtifactTree(safeRoot);
  await assertArtifactTreeSafe(safeRoot);
  await execFileAsync('unzip', ['-tqq', join(safeRoot, 'trace.zip')], { shell: false });
  const extracted = join(temporaryRoot, 'extracted');
  await mkdir(extracted, { recursive: true });
  await execFileAsync('unzip', ['-q', join(safeRoot, 'trace.zip'), '-d', extracted], { shell: false });
  const sanitizedLine = JSON.parse(await readFile(join(extracted, 'trace.network'), 'utf8'));
  assert.equal(sanitizedLine.snapshot.request.postData._sha1, resourceName);
  const sanitizedConfig = JSON.parse(await readFile(join(extracted, 'resources', resourceName), 'utf8'));
  assert.deepEqual(sanitizedConfig.mcp_allowlist[0].secrets, {
    LICENSE: '[REDACTED]',
    VENDOR_BLOB: '[REDACTED]',
    nested: { SECONDARY: '[REDACTED]' }
  });
  for (const secret of Object.values(mcpSecrets).flatMap((value) => (
    typeof value === 'string' ? [value] : Object.values(value)
  ))) {
    assert.equal(JSON.stringify(sanitizedConfig).includes(secret), false);
  }
});

test('artifact trees and Playwright-style ZIP traces are sanitized and recursively asserted', async (t) => {
  const temporaryRoot = await mkdtemp(join(tmpdir(), 'agent-hub-artifact-test-'));
  t.after(() => rm(temporaryRoot, { recursive: true, force: true }));

  const missing = join(temporaryRoot, 'missing');
  await sanitizeArtifactTree(missing);
  await assertArtifactTreeSafe(missing);

  const traceSource = join(temporaryRoot, 'trace-source');
  await mkdir(traceSource, { recursive: true });
  await writeFile(join(traceSource, 'trace.network'), `${fixtureText()}\n`);
  await writeFile(join(traceSource, 'trace.trace'), `${JSON.stringify({
    type: 'frame-snapshot',
    snapshot: { html: ['INPUT', { type: 'password', value: JSON_SECRETS[4] }] }
  })}\n`);
  await writeFile(join(traceSource, 'resource.bin'), Buffer.from([0, 255, 1, 254, 2, 253]));

  const fixtureZip = join(temporaryRoot, 'fixture-trace.zip');
  await execFileAsync('zip', ['-q', '-r', '-X', fixtureZip, '.'], {
    cwd: traceSource,
    shell: false
  });

  const unsafeRoot = join(temporaryRoot, 'unsafe');
  await mkdir(unsafeRoot, { recursive: true });
  await cp(fixtureZip, join(unsafeRoot, 'trace.zip'));
  await writeFile(join(unsafeRoot, 'failure.json'), fixtureText());
  await assert.rejects(
    assertArtifactTreeSafe(unsafeRoot),
    (error) => {
      assert.match(error.message, /Artifact safety assertion failed/);
      assertSecretsAbsent(error.message);
      return true;
    }
  );
  assert.deepEqual(await readdir(unsafeRoot), []);

  const brokenRoot = join(temporaryRoot, 'broken');
  await mkdir(brokenRoot, { recursive: true });
  await writeFile(
    join(brokenRoot, 'broken.zip'),
    Buffer.concat([Buffer.from([0]), Buffer.from(fixtureText())])
  );
  await assert.rejects(
    sanitizeArtifactTree(brokenRoot),
    (error) => {
      assert.match(error.message, /Artifact sanitization failed/);
      assertSecretsAbsent(error.message);
      return true;
    }
  );
  assert.deepEqual(await readdir(brokenRoot), []);

  const safeRoot = join(temporaryRoot, 'safe');
  await mkdir(safeRoot, { recursive: true });
  await cp(fixtureZip, join(safeRoot, 'trace.zip'));
  await writeFile(join(safeRoot, 'failure.json'), fixtureText());
  await writeFile(join(safeRoot, 'compose.log'), fixtureText());
  const binaryPath = join(safeRoot, 'failure.png');
  const binaryFixture = Buffer.from([137, 80, 78, 71, 0, 10, 26, 10, 255]);
  await writeFile(binaryPath, binaryFixture);

  await sanitizeArtifactTree(safeRoot);
  await assertArtifactTreeSafe(safeRoot);
  assert.deepEqual(await readFile(binaryPath), binaryFixture);
  assert.equal(existsSync(join(safeRoot, 'trace.zip')), true);
  await execFileAsync('unzip', ['-tqq', join(safeRoot, 'trace.zip')], { shell: false });

  const extracted = join(temporaryRoot, 'extracted');
  await mkdir(extracted, { recursive: true });
  await execFileAsync('unzip', ['-q', join(safeRoot, 'trace.zip'), '-d', extracted], { shell: false });
  assertSecretsAbsent(await readFile(join(extracted, 'trace.network'), 'utf8'));
  assertSecretsAbsent(await readFile(join(extracted, 'trace.trace'), 'utf8'));
  assertSecretsAbsent(await readFile(join(safeRoot, 'compose.log'), 'utf8'));
  await assertArtifactTreeSafe(extracted);
});

test('runner summaries and JUnit are redacted before they are written', async (t) => {
  const temporaryRoot = await mkdtemp(join(tmpdir(), 'agent-hub-summary-test-'));
  t.after(() => rm(temporaryRoot, { recursive: true, force: true }));
  const secret = TOKENS[0];
  const coverage = { selected: { complete: true }, overall: { complete: true } };

  await writeSummary(temporaryRoot, 'qa-project', 'http://127.0.0.1', [{
    id: 'secret-failure',
    name: 'Secret failure',
    type: 'api',
    status: 'failed',
    duration_ms: 1,
    error: `provider returned ${secret}`
  }], coverage);

  for (const name of ['summary.json', 'junit.xml']) {
    const value = await readFile(join(temporaryRoot, name), 'utf8');
    assert.equal(value.includes(secret), false, `${name} retained a Session credential`);
    assert.match(value, /\[REDACTED\]/);
  }
  await assertArtifactTreeSafe(temporaryRoot);
});
