import assert from 'node:assert/strict';
import { ApiClient, loginAsAdmin, poll } from '../../support/api.mjs';
import { withBrowser } from '../../support/browser.mjs';

const LEDGER_PATHS = [
  '/api/model-usage/summary',
  '/api/model-usage',
  '/api/model-call-errors'
];
const SUCCESS_MODEL_ID = 'hub-proxy-smoke';
const ERROR_MODEL_ID = 'hub-proxy-error';
const REVIEW_MODEL_ID = 'hub-proxy-review';
const PERSONAL_API_TYPE = 'openai_chat_completions';
const GLOBAL_API_TYPE = 'openai_responses';
const CHAT_REQUEST_SETTINGS = {
  protocol: PERSONAL_API_TYPE,
  temperature: 0.3,
  top_p: 0.8,
  max_completion_tokens: 321
};
const SUBAGENT_REQUEST_SETTINGS = {
  protocol: PERSONAL_API_TYPE,
  temperature: 0.4,
  top_p: 0.7,
  max_completion_tokens: 222
};
const DETAILED_MODEL_SETTINGS = {
  reasoning_effort: 'high',
  reasoning_summary: 'concise',
  verbosity: 'high',
  context_window_tokens: 128_000,
  auto_compact_token_limit: 96_000,
  reasoning_summary_support: 'supported',
  service_tier: 'flex',
  request_max_retries: 3,
  stream_max_retries: 5,
  stream_idle_timeout_ms: 300_000,
  request_settings: CHAT_REQUEST_SETTINGS
};
const AUTOMATIC_RESPONSES_SETTINGS = {
  reasoning_effort: 'default',
  reasoning_summary: 'default',
  verbosity: 'default',
  context_window_tokens: null,
  auto_compact_token_limit: null,
  reasoning_summary_support: 'auto',
  service_tier: null,
  request_max_retries: null,
  stream_max_retries: null,
  stream_idle_timeout_ms: null,
  request_settings: { protocol: GLOBAL_API_TYPE }
};
const CONNECTION_KEYS = [
  'allowed_model_ids',
  'api_type',
  'base_url',
  'created_at',
  'has_api_key',
  'id',
  'name',
  'owner_id',
  'scope',
  'status',
  'updated_at'
];

function uniqueSlug(context, prefix) {
  return context.unique(prefix)
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, '-')
    .replace(/^-|-$/g, '');
}

function selection(connection, modelId) {
  return { connection_id: connection.id, model_id: modelId };
}

function ledgerPath(path, query = {}) {
  const search = new URLSearchParams();
  for (const [key, value] of Object.entries(query)) {
    if (value !== undefined && value !== null) search.set(key, String(value));
  }
  const encoded = search.toString();
  return encoded ? `${path}?${encoded}` : path;
}

async function assertNoHorizontalOverflow(page, label) {
  await page.waitForTimeout(100);
  const overflow = await page.evaluate(() => (
    document.documentElement.scrollWidth - document.documentElement.clientWidth
  ));
  assert.ok(overflow <= 1, `${label} horizontal overflow: ${overflow}px`);
}

async function login(page, email, password) {
  await page.goto('/login', { waitUntil: 'domcontentloaded' });
  await page.getByLabel('Email').fill(email);
  await page.getByLabel('Password').fill(password);
  await page.getByRole('button', { name: 'Sign in', exact: true }).click();
  await page.getByText(email, { exact: true }).waitFor();
}

async function responseJson(response, label) {
  assert.equal(response.ok(), true, `${label} returned HTTP ${response.status()}`);
  return response.json();
}

function assertConnectionDto(connection, providerKey, label) {
  assert.deepEqual(Object.keys(connection).sort(), CONNECTION_KEYS);
  assert.equal(connection.has_api_key, true, `${label} must report a stored API key`);
  assert.equal(
    JSON.stringify(connection).includes(providerKey),
    false,
    `${label} must keep the API key write-only`
  );
}

async function assertConnectionFormSurface(dialog) {
  for (const label of [
    'Connection name',
    'Base URL',
    'Allowed Model IDs',
    'API key'
  ]) {
    await dialog.getByLabel(label).waitFor();
  }
  await dialog.getByLabel('API type').waitFor();
  const scopeField = dialog.locator('label').filter({ hasText: 'Scope' }).locator('select');
  await scopeField.waitFor();
  assert.equal(await scopeField.isDisabled(), true, 'Connection scope must be read-only');
  assert.equal(
    await dialog.locator('input, select, textarea').count(),
    6,
    'Model API Connection form must expose only the six V1 controls'
  );
}

async function closeRedactedConnectionDialog(dialog) {
  if (!await dialog.isVisible().catch(() => false)) return;
  await dialog.getByLabel('API key').fill('[redacted]').catch(() => undefined);
  await dialog.getByRole('button', { name: 'Cancel' }).click().catch(() => undefined);
}

async function createConnection(page, browserContext, scope, fields, providerKey) {
  await page.getByRole('button', {
    name: scope === 'personal' ? 'Create personal model' : 'Create global model'
  }).click();
  const dialog = page.getByRole('dialog', { name: 'Create model connection' });
  await assertConnectionFormSurface(dialog);
  await assertNoHorizontalOverflow(page, `${scope} Model API Connection dialog`);
  await dialog.getByLabel('Connection name').fill(fields.name);
  await dialog.getByLabel('Base URL').fill(fields.baseUrl);
  await dialog.getByLabel('API type').selectOption(fields.apiType);
  await dialog.getByLabel('Allowed Model IDs').fill([
    ...fields.allowedModelIds,
    fields.allowedModelIds[0]
  ].join('\n'));

  let response;
  let requestBody;
  await browserContext.tracing.stop();
  try {
    await dialog.getByLabel('API key').fill(providerKey);
    const responsePromise = page.waitForResponse((candidate) => (
      candidate.request().method() === 'POST'
      && new URL(candidate.url()).pathname === '/api/model-connections'
    ));
    await dialog.getByRole('button', { name: 'Create model connection' }).click();
    response = await responsePromise;
    requestBody = response.request().postDataJSON();
    await dialog.waitFor({ state: 'detached' });
  } finally {
    await closeRedactedConnectionDialog(dialog);
    await browserContext.tracing.start({ snapshots: true, sources: true });
  }

  assert.deepEqual(Object.keys(requestBody).sort(), [
    'allowed_model_ids', 'api_key', 'api_type', 'base_url', 'name', 'scope'
  ]);
  assert.equal(requestBody.scope, scope);
  assert.equal(requestBody.name, fields.name);
  assert.equal(requestBody.base_url, fields.baseUrl);
  assert.equal(requestBody.api_type, fields.apiType);
  assert.deepEqual(requestBody.allowed_model_ids, fields.allowedModelIds);
  assert.ok(requestBody.api_key === providerKey, 'Create request must submit the entered API key');

  const connection = await responseJson(response, `${scope} Model API Connection create`);
  assertConnectionDto(connection, providerKey, `${scope} create response`);
  assert.deepEqual(connection.allowed_model_ids, fields.allowedModelIds);
  assert.equal(connection.api_type, fields.apiType);
  return connection;
}

async function editConnection(page, connection, fields, providerKey) {
  await page.getByRole('button', { name: `Edit ${connection.name}` }).click();
  const dialog = page.getByRole('dialog', { name: 'Edit model connection' });
  await assertConnectionFormSurface(dialog);
  assert.equal(
    await dialog.getByLabel('API key').inputValue(),
    '',
    'Edit must not load the stored API key'
  );
  await dialog.getByLabel('Connection name').fill(fields.name);
  await dialog.getByLabel('Base URL').fill(fields.baseUrl);
  await dialog.getByLabel('API type').selectOption(fields.apiType);
  await dialog.getByLabel('Allowed Model IDs').fill(fields.allowedModelIds.join('\n'));
  const responsePromise = page.waitForResponse((candidate) => (
    candidate.request().method() === 'PUT'
    && new URL(candidate.url()).pathname === `/api/model-connections/${connection.id}`
  ));
  await dialog.getByRole('button', { name: 'Save changes' }).click();
  const response = await responsePromise;
  const requestBody = response.request().postDataJSON();
  assert.deepEqual(requestBody, {
    name: fields.name,
    base_url: fields.baseUrl,
    api_type: fields.apiType,
    allowed_model_ids: fields.allowedModelIds
  });
  const updated = await responseJson(response, 'Model API Connection update');
  assertConnectionDto(updated, providerKey, 'Update response');
  return updated;
}

async function attemptConflictingConnectionEdit(page, connection, fields) {
  await page.getByRole('button', { name: `Edit ${connection.name}` }).click();
  const dialog = page.getByRole('dialog', { name: 'Edit model connection' });
  await assertConnectionFormSurface(dialog);
  assert.equal(await dialog.getByLabel('API key').inputValue(), '');
  await dialog.getByLabel('Connection name').fill(fields.name);
  await dialog.getByLabel('Base URL').fill(fields.baseUrl);
  await dialog.getByLabel('API type').selectOption(fields.apiType);
  await dialog.getByLabel('Allowed Model IDs').fill(fields.allowedModelIds.join('\n'));
  const responsePromise = page.waitForResponse((candidate) => (
    candidate.request().method() === 'PUT'
    && new URL(candidate.url()).pathname === `/api/model-connections/${connection.id}`
  ));
  await dialog.getByRole('button', { name: 'Save changes' }).click();
  const response = await responsePromise;
  assert.deepEqual(response.request().postDataJSON(), {
    name: fields.name,
    base_url: fields.baseUrl,
    api_type: fields.apiType,
    allowed_model_ids: fields.allowedModelIds
  });
  assert.equal(response.status(), 409, 'Referenced allowlist update must conflict');
  await dialog.getByRole('alert').getByText(
    'This change conflicts with live references. Force saving clears affected selections and disables affected explicit subagents.'
  ).waitFor();
  return { dialog, response };
}

async function testConnection(page, connection, modelId, expectedSuccess) {
  await page.getByRole('button', { name: `Test ${connection.name}` }).click();
  const dialog = page.getByRole('dialog', { name: 'Test model connection' });
  await dialog.getByLabel('Model ID').selectOption(modelId);
  assert.equal(await dialog.getByLabel('Request').inputValue(), 'hi');
  const responsePromise = page.waitForResponse((candidate) => (
    candidate.request().method() === 'POST'
    && new URL(candidate.url()).pathname === `/api/model-connections/${connection.id}/test`
  ));
  await dialog.getByRole('button', { name: 'Send test message' }).click();
  const response = await responsePromise;
  assert.deepEqual(response.request().postDataJSON(), { model_id: modelId, message: 'hi' });
  const result = await responseJson(response, `Model test for ${modelId}`);
  assert.equal(result.success, expectedSuccess);
  assert.equal(Number.isInteger(result.response_time_ms), true);
  assert.ok(result.response_time_ms >= 0);
  assert.equal(typeof result.response_text === 'string', expectedSuccess);
  await dialog.getByText(
    expectedSuccess ? 'Connection test succeeded.' : 'Connection test failed.'
  ).waitFor();
  if (expectedSuccess) {
    await dialog.getByLabel('Response').getByText(result.response_text, { exact: true }).waitFor();
  }
  await dialog.getByText(`Response time ${result.response_time_ms} ms`, { exact: true }).waitFor();
  await dialog.locator('.modal-actions').getByRole('button', { name: 'Close' }).click();
}

async function setSystemDefault(page, connection, modelId) {
  await page.getByRole('button', {
    name: `Set ${connection.name} as system default`
  }).click();
  const dialog = page.getByRole('dialog', { name: 'Set system default' });
  await dialog.getByLabel('Model ID').selectOption(modelId);
  const responsePromise = page.waitForResponse((candidate) => (
    candidate.request().method() === 'PUT'
    && new URL(candidate.url()).pathname === '/api/model-connections/system-default'
  ));
  await dialog.getByRole('button', { name: 'Set system default' }).click();
  const response = await responsePromise;
  const expectedSelection = selection(connection, modelId);
  assert.deepEqual(response.request().postDataJSON(), { selection: expectedSelection });
  assert.deepEqual(
    await responseJson(response, 'Set System Default model selection'),
    { selection: expectedSelection }
  );
}

async function assertRawSelect(select, value, label) {
  assert.equal(await select.inputValue(), value, `${label} must retain its raw token value`);
  assert.equal(
    (await select.locator('option:checked').innerText()).trim(),
    value,
    `${label} must display its raw token`
  );
}

function requestSettingsGroup(container, apiType) {
  return container.locator('fieldset').filter({ hasText: apiType }).last();
}

async function fillDetailedModelSettings(container) {
  await container.getByLabel('Reasoning effort').selectOption(DETAILED_MODEL_SETTINGS.reasoning_effort);
  await container.getByLabel(/^Reasoning summary(?! support)/).selectOption(DETAILED_MODEL_SETTINGS.reasoning_summary);
  await container.getByLabel('Verbosity').selectOption(DETAILED_MODEL_SETTINGS.verbosity);
  await container.getByLabel(/^Reasoning summary support/).selectOption(DETAILED_MODEL_SETTINGS.reasoning_summary_support);
  await container.getByLabel('Service tier').fill(DETAILED_MODEL_SETTINGS.service_tier);
  await container.getByLabel('Context window tokens').fill(String(DETAILED_MODEL_SETTINGS.context_window_tokens));
  await container.getByLabel('Auto-compact token limit').fill(String(DETAILED_MODEL_SETTINGS.auto_compact_token_limit));
  await container.getByLabel('Request max retries').fill(String(DETAILED_MODEL_SETTINGS.request_max_retries));
  await container.getByLabel('Stream max retries').fill(String(DETAILED_MODEL_SETTINGS.stream_max_retries));
  await container.getByLabel('Stream idle timeout (ms)').fill(String(DETAILED_MODEL_SETTINGS.stream_idle_timeout_ms));
  const requestGroup = requestSettingsGroup(container, PERSONAL_API_TYPE);
  assert.equal((await requestGroup.locator('legend code').innerText()).trim(), PERSONAL_API_TYPE);
  await requestGroup.getByLabel('temperature', { exact: true }).fill(String(CHAT_REQUEST_SETTINGS.temperature));
  await requestGroup.getByLabel('top_p', { exact: true }).fill(String(CHAT_REQUEST_SETTINGS.top_p));
  await requestGroup.getByLabel('max_completion_tokens', { exact: true }).fill(String(CHAT_REQUEST_SETTINGS.max_completion_tokens));
}

async function assertDetailedModelSettings(container) {
  await assertRawSelect(
    container.getByLabel('Reasoning effort'),
    DETAILED_MODEL_SETTINGS.reasoning_effort,
    'Reasoning effort'
  );
  await assertRawSelect(
    container.getByLabel(/^Reasoning summary(?! support)/),
    DETAILED_MODEL_SETTINGS.reasoning_summary,
    'Reasoning summary'
  );
  await assertRawSelect(
    container.getByLabel('Verbosity'),
    DETAILED_MODEL_SETTINGS.verbosity,
    'Verbosity'
  );
  await assertRawSelect(
    container.getByLabel(/^Reasoning summary support/),
    DETAILED_MODEL_SETTINGS.reasoning_summary_support,
    'Reasoning summary support'
  );
  const inputValues = [
    ['Service tier', DETAILED_MODEL_SETTINGS.service_tier],
    ['Context window tokens', DETAILED_MODEL_SETTINGS.context_window_tokens],
    ['Auto-compact token limit', DETAILED_MODEL_SETTINGS.auto_compact_token_limit],
    ['Request max retries', DETAILED_MODEL_SETTINGS.request_max_retries],
    ['Stream max retries', DETAILED_MODEL_SETTINGS.stream_max_retries],
    ['Stream idle timeout (ms)', DETAILED_MODEL_SETTINGS.stream_idle_timeout_ms]
  ];
  for (const [label, value] of inputValues) {
    assert.equal(await container.getByLabel(label).inputValue(), String(value), `${label} raw value`);
  }
  const requestGroup = requestSettingsGroup(container, PERSONAL_API_TYPE);
  assert.equal((await requestGroup.locator('legend code').innerText()).trim(), PERSONAL_API_TYPE);
  assert.equal(await requestGroup.getByLabel('temperature', { exact: true }).inputValue(), '0.3');
  assert.equal(await requestGroup.getByLabel('top_p', { exact: true }).inputValue(), '0.8');
  assert.equal(await requestGroup.getByLabel('max_completion_tokens', { exact: true }).inputValue(), '321');
  const reasoningLabel = container.getByLabel('Reasoning effort').locator('..');
  assert.ok((await reasoningLabel.innerText()).includes('high'));
  assert.ok((await reasoningLabel.innerText()).includes('Agent'));
}

async function addInheritingSubagent(page, parentDialog) {
  await parentDialog.getByRole('button', { name: 'Add subagent' }).click();
  const dialog = page.getByRole('dialog', { name: 'Add subagent' });
  await dialog.getByLabel('Subagent name').fill('inheritor');
  await dialog.getByLabel('Description').fill('Inherits the Agent model and settings.');
  await dialog.getByRole('textbox', { name: 'Developer instructions' }).fill(
    'Use the Agent model configuration without overrides.'
  );
  assert.equal(await dialog.getByLabel('Model override').inputValue(), '');
  assert.equal(await dialog.getByLabel('Reasoning effort Setting source').inputValue(), 'inherit');
  await dialog.getByRole('button', { name: 'Save changes' }).click();
}

async function addOverridingSubagent(page, parentDialog, modelSelection) {
  await parentDialog.getByRole('button', { name: 'Add subagent' }).click();
  const dialog = page.getByRole('dialog', { name: 'Add subagent' });
  await dialog.getByLabel('Subagent name').fill('reviewer');
  await dialog.getByLabel('Description').fill('Uses another allowed model with explicit settings.');
  await dialog.getByRole('textbox', { name: 'Developer instructions' }).fill(
    'Review the result using the explicit model and request settings.'
  );
  await dialog.getByLabel('Model override').selectOption(modelSelection);
  await dialog.getByLabel('Reasoning effort Setting source').selectOption('override');
  const reasoningLabel = dialog.locator('label').filter({ hasText: 'Reasoning effort' }).first();
  await reasoningLabel.locator('select').nth(1).selectOption('max');
  await assertRawSelect(reasoningLabel.locator('select').nth(1), 'max', 'Subagent reasoning effort');
  const requestGroup = requestSettingsGroup(dialog, PERSONAL_API_TYPE);
  await requestGroup.locator('select').first().selectOption('override');
  await requestGroup.getByLabel('temperature', { exact: true }).fill(String(SUBAGENT_REQUEST_SETTINGS.temperature));
  await requestGroup.getByLabel('top_p', { exact: true }).fill(String(SUBAGENT_REQUEST_SETTINGS.top_p));
  await requestGroup.getByLabel('max_completion_tokens', { exact: true }).fill(
    String(SUBAGENT_REQUEST_SETTINGS.max_completion_tokens)
  );
  assert.ok((await dialog.innerText()).includes(REVIEW_MODEL_ID));
  assert.ok((await reasoningLabel.innerText()).includes('max'));
  assert.ok((await reasoningLabel.innerText()).includes('Subagent'));
  await dialog.getByRole('button', { name: 'Save changes' }).click();
}

async function waitForRunCompletion(request, agentId, runId) {
  return poll(async () => {
    const runs = await responseJson(
      await request.get(`/api/agents/${agentId}/runs`),
      'Agent Run list'
    );
    return runs.find((run) => run.id === runId) ?? null;
  }, (run) => run?.status === 'completed', {
    timeoutMs: 90_000,
    description: `Run ${runId} to complete through Runtime and Gateway`
  });
}

async function waitForLedgerSet(page, action) {
  const waits = LEDGER_PATHS.map((pathname) => page.waitForResponse((response) => (
    response.request().method() === 'GET'
    && new URL(response.url()).pathname === pathname
    && response.ok()
  )));
  await action();
  return (await Promise.all(waits)).map((response) => new URL(response.url()));
}

async function assertLedgerRange(page, urls, range) {
  const values = urls.map((url) => ({
    from: url.searchParams.get('from_ms'),
    to: url.searchParams.get('to_ms')
  }));
  assert.deepEqual(values[1], values[0], `${range} usage range must match summary`);
  assert.deepEqual(values[2], values[0], `${range} error range must match summary`);
  if (range === 'all') {
    assert.deepEqual(values[0], { from: null, to: null });
    return;
  }
  const expected = await page.evaluate(({ selected, toMs }) => {
    const reference = new Date(Number(toMs));
    const today = new Date(reference);
    today.setHours(0, 0, 0, 0);
    if (selected === 'yesterday') {
      const yesterday = new Date(today);
      yesterday.setDate(yesterday.getDate() - 1);
      return { from: String(yesterday.getTime()), to: String(today.getTime()) };
    }
    const days = selected === 'today' ? 1 : Number.parseInt(selected, 10);
    const start = new Date(today);
    start.setDate(start.getDate() - (days - 1));
    return { from: String(start.getTime()), to: String(Number(toMs)) };
  }, { selected: range, toMs: values[0].to });
  assert.deepEqual(values[0], expected, `${range} must use browser-local half-open boundaries`);
}

function consumeBrowserError(browserErrors, expected, label) {
  const index = browserErrors.indexOf(expected);
  assert.notEqual(index, -1, `${label} must be captured by browser diagnostics`);
  browserErrors.splice(index, 1);
}

async function ordinaryDeleteConnection(page, connection) {
  await page.getByRole('button', { name: `Delete ${connection.name}`, exact: true }).click();
  const dialog = page.getByRole('dialog', { name: 'Delete model connection' });
  const responsePromise = page.waitForResponse((response) => (
    response.request().method() === 'DELETE'
    && new URL(response.url()).pathname === `/api/model-connections/${connection.id}`
  ));
  await dialog.getByRole('button', { name: 'Delete model connection' }).click();
  return { dialog, response: await responsePromise };
}

async function forceDeleteConnection(page, connection, beforeConfirm) {
  await page.getByRole('button', { name: `Force-delete ${connection.name}` }).click();
  const dialog = page.getByRole('dialog', { name: 'Force-delete model connection' });
  if (beforeConfirm) await beforeConfirm(dialog);
  const responsePromise = page.waitForResponse((response) => (
    response.request().method() === 'POST'
    && new URL(response.url()).pathname === `/api/model-connections/${connection.id}/force-delete`
  ));
  await dialog.getByRole('button', { name: 'Force-delete model connection' }).click();
  return { dialog, response: await responsePromise };
}

export default async function modelsBrowserScenario(scenarioContext) {
  const superClient = new ApiClient(scenarioContext.baseURL);
  await loginAsAdmin(superClient);
  const { data: defaultSnapshot } = await superClient.get('/api/model-connections/system-default');
  assert.ok(defaultSnapshot.selection, 'Compose must provide a System Default connection/model pair');
  const { data: initialConnections } = await superClient.get('/api/model-connections');
  const seedGlobal = initialConnections.find((connection) => (
    connection.id === defaultSnapshot.selection.connection_id
  ));
  assert.ok(seedGlobal, 'The System Default Global connection must be visible');
  assert.equal(seedGlobal.scope, 'global');
  assert.equal(seedGlobal.status, 'enabled');
  assert.ok(seedGlobal.allowed_model_ids.includes(defaultSnapshot.selection.model_id));

  const providerKeyResult = scenarioContext.compose.run([
    'exec', '-T', 'backend', 'sh', '-c', 'printf %s "$DEV_MODEL_PROVIDER_API_KEY"'
  ]);
  const providerKey = providerKeyResult.stdout.trim();
  assert.ok(providerKey, 'The QA backend must expose its fake-provider key to the scenario process');
  assertConnectionDto(seedGlobal, providerKey, 'Seed Global connection');

  const memberSlug = uniqueSlug(scenarioContext, 'qa-model-member');
  const memberEmail = `${memberSlug}@example.com`;
  const memberPassword = `${scenarioContext.unique('Model member password')}!Aa9`;
  const memberClient = new ApiClient(scenarioContext.baseURL);
  await memberClient.post('/api/auth/register', { email: memberEmail, password: memberPassword });

  const adminSlug = uniqueSlug(scenarioContext, 'qa-model-admin');
  const adminEmail = `${adminSlug}@example.com`;
  const adminPassword = `${scenarioContext.unique('Model admin password')}!Bb8`;
  const adminClient = new ApiClient(scenarioContext.baseURL);
  const { data: adminRegistration } = await adminClient.post('/api/auth/register', {
    email: adminEmail,
    password: adminPassword
  });
  await superClient.request(`/api/admin/users/${adminRegistration.user.id}/role`, {
    method: 'PUT',
    body: { role: 'admin' }
  });

  let personalConnectionId = null;
  let globalConnectionId = null;
  let memberAgentId = null;
  let adminAgentId = null;
  let scenarioError = null;

  try {
    await withBrowser(scenarioContext, {
      allowedHttpErrors: [
        { method: 'GET', pathname: '/api/auth/me', status: 401, times: 2 }
      ]
    }, async ({ page, context, request, browserErrors }) => {
      const allowedNoContentAborts = new Set();

      await login(page, memberEmail, memberPassword);
      await page.goto('/models', { waitUntil: 'domcontentloaded' });
      const memberTabs = page.getByRole('tablist', { name: 'Models' });
      assert.equal(await memberTabs.getByRole('tab').count(), 3);
      assert.equal(await memberTabs.getByRole('tab', { name: 'Global Models' }).count(), 0);

      const personalName = scenarioContext.unique('QA Personal multi-model');
      let personal = await createConnection(page, context, 'personal', {
        name: personalName,
        baseUrl: seedGlobal.base_url,
        apiType: PERSONAL_API_TYPE,
        allowedModelIds: [SUCCESS_MODEL_ID, REVIEW_MODEL_ID, ERROR_MODEL_ID]
      }, providerKey);
      personalConnectionId = personal.id;
      const updatedPersonalName = `${personalName} Updated`;
      personal = await editConnection(page, personal, {
        name: updatedPersonalName,
        baseUrl: personal.base_url,
        apiType: PERSONAL_API_TYPE,
        allowedModelIds: [SUCCESS_MODEL_ID, ERROR_MODEL_ID, REVIEW_MODEL_ID]
      }, providerKey);
      await testConnection(page, personal, SUCCESS_MODEL_ID, true);
      await testConnection(page, personal, ERROR_MODEL_ID, false);

      await memberTabs.getByRole('tab', { name: 'Available Models' }).click();
      const availableTable = page.getByRole('table', { name: 'Available model list' });
      const personalAvailableRow = availableTable.getByRole('row').filter({ hasText: updatedPersonalName });
      const personalAvailableText = await personalAvailableRow.innerText();
      assert.ok(personalAvailableText.includes(SUCCESS_MODEL_ID));
      assert.ok(personalAvailableText.includes(REVIEW_MODEL_ID));
      assert.ok(personalAvailableText.includes(ERROR_MODEL_ID));
      assert.ok(personalAvailableText.includes(PERSONAL_API_TYPE));
      await assertNoHorizontalOverflow(page, 'member desktop Available Models');

      await page.goto('/agents', { waitUntil: 'domcontentloaded' });
      await page.locator('.agents-header').getByRole('button', { name: 'Create Agent' }).click();
      const createAgentDialog = page.getByRole('dialog', { name: 'Create Agent' });
      const memberAgentName = scenarioContext.unique('QA Member model settings');
      await createAgentDialog.getByLabel('Name', { exact: true }).fill(memberAgentName);
      await createAgentDialog.getByLabel('Instructions').fill(
        'Exercise a Personal multi-model connection through Runtime and Gateway.'
      );
      const personalSelection = selection(personal, SUCCESS_MODEL_ID);
      const personalSelectionValue = `${personalSelection.connection_id}\n${personalSelection.model_id}`;
      await createAgentDialog.getByLabel('Model API Connection and model').selectOption(
        personalSelectionValue
      );
      await fillDetailedModelSettings(createAgentDialog);
      await assertDetailedModelSettings(createAgentDialog);
      await addInheritingSubagent(page, createAgentDialog);
      const reviewSelection = selection(personal, REVIEW_MODEL_ID);
      await addOverridingSubagent(
        page,
        createAgentDialog,
        `${reviewSelection.connection_id}\n${reviewSelection.model_id}`
      );
      const draftSubagents = createAgentDialog.getByRole('table', { name: 'Codex subagents' });
      assert.ok((await draftSubagents.innerText()).includes('inheritor'));
      assert.ok((await draftSubagents.innerText()).includes('reviewer'));

      const createAgentResponsePromise = page.waitForResponse((response) => (
        response.request().method() === 'POST'
        && new URL(response.url()).pathname === '/api/agents'
      ));
      await createAgentDialog.getByRole('button', { name: 'Create agent' }).click();
      const createdAgent = await responseJson(
        await createAgentResponsePromise,
        'Configured Agent create'
      );
      memberAgentId = createdAgent.id;
      assert.deepEqual(createdAgent.model_selection, personalSelection);
      assert.deepEqual(createdAgent.model_settings, DETAILED_MODEL_SETTINGS);
      const inherited = createdAgent.codex_subagents.find((item) => item.name === 'inheritor');
      assert.ok(inherited, 'Created Agent must retain the inheriting subagent');
      assert.deepEqual(inherited.model_selection, null);
      assert.deepEqual(inherited.model_settings_override, {});
      const overridden = createdAgent.codex_subagents.find((item) => item.name === 'reviewer');
      assert.ok(overridden, 'Created Agent must retain the overriding subagent');
      assert.deepEqual(overridden.model_selection, reviewSelection);
      assert.deepEqual(overridden.model_settings_override, {
        reasoning_effort: 'max',
        request_settings: SUBAGENT_REQUEST_SETTINGS
      });

      await page.getByRole('heading', { name: memberAgentName }).waitFor();
      await page.getByRole('tab', { name: 'Models' }).click();
      const memberModelsPanel = page.getByRole('tabpanel', { name: 'Models' });
      assert.equal(
        await memberModelsPanel.getByLabel('Model API Connection and model').inputValue(),
        personalSelectionValue
      );
      await assertDetailedModelSettings(memberModelsPanel);
      const savedSubagents = memberModelsPanel.getByRole('table', { name: 'Codex subagents' });
      const inheritedRow = savedSubagents.getByRole('row').filter({ hasText: 'inheritor' });
      assert.ok((await inheritedRow.innerText()).includes('Inherit Agent model'));
      assert.ok((await inheritedRow.innerText()).includes('Inherits all Agent settings'));
      const overriddenRow = savedSubagents.getByRole('row').filter({ hasText: 'reviewer' });
      assert.ok((await overriddenRow.innerText()).includes(REVIEW_MODEL_ID));
      assert.ok((await overriddenRow.innerText()).includes('2 overrides'));

      await memberModelsPanel.getByRole('button', { name: 'Edit subagent: inheritor' }).click();
      let subagentDialog = page.getByRole('dialog', { name: 'Edit subagent: inheritor' });
      assert.equal(await subagentDialog.getByLabel('Model override').inputValue(), '');
      assert.equal(
        await subagentDialog.getByLabel('Reasoning effort Setting source').inputValue(),
        'inherit'
      );
      let subagentReasoning = subagentDialog.locator('label').filter({ hasText: 'Reasoning effort' }).first();
      assert.ok((await subagentReasoning.innerText()).includes('high'));
      assert.ok((await subagentReasoning.innerText()).includes('Agent'));
      await subagentDialog.getByRole('button', { name: 'Cancel' }).click();

      await memberModelsPanel.getByRole('button', { name: 'Edit subagent: reviewer' }).click();
      subagentDialog = page.getByRole('dialog', { name: 'Edit subagent: reviewer' });
      assert.equal(
        await subagentDialog.getByLabel('Model override').inputValue(),
        `${reviewSelection.connection_id}\n${reviewSelection.model_id}`
      );
      assert.equal(
        await subagentDialog.getByLabel('Reasoning effort Setting source').inputValue(),
        'override'
      );
      subagentReasoning = subagentDialog.locator('label').filter({ hasText: 'Reasoning effort' }).first();
      await assertRawSelect(subagentReasoning.locator('select').nth(1), 'max', 'Saved subagent reasoning effort');
      const savedRequestGroup = requestSettingsGroup(subagentDialog, PERSONAL_API_TYPE);
      assert.equal(await savedRequestGroup.locator('select').first().inputValue(), 'override');
      assert.equal(await savedRequestGroup.getByLabel('temperature', { exact: true }).inputValue(), '0.4');
      assert.equal(await savedRequestGroup.getByLabel('top_p', { exact: true }).inputValue(), '0.7');
      assert.equal(await savedRequestGroup.getByLabel('max_completion_tokens', { exact: true }).inputValue(), '222');
      await subagentDialog.getByRole('button', { name: 'Cancel' }).click();

      await assertNoHorizontalOverflow(page, 'member desktop Agent Models');
      await page.setViewportSize({ width: 390, height: 844 });
      await assertNoHorizontalOverflow(page, 'member 390px Agent Models');
      await page.setViewportSize({ width: 1280, height: 800 });

      await page.getByRole('tab', { name: 'Activity' }).click();
      const activityPanel = page.getByRole('tabpanel', { name: 'Activity' });
      assert.equal(await activityPanel.getByLabel('Message', { exact: true }).count(), 0);
      assert.equal(await activityPanel.getByRole('button', { name: 'Start run' }).count(), 0);
      const run = await responseJson(
        await request.post(`/api/agents/${createdAgent.id}/runs`, {
          data: { message: 'Verify Chat settings through the real Gateway.', hub_session_id: null, parent_run_id: null }
        }),
        'Agent Run create'
      );
      await waitForRunCompletion(request, createdAgent.id, run.id);
      await activityPanel.locator(`[data-run-id="${run.id}"]`)
        .getByText('completed', { exact: true })
        .waitFor({ timeout: 20_000 });
      allowedNoContentAborts.add(
        `requestfailed: GET ${new URL(`/api/runs/${run.id}/events/stream`, scenarioContext.baseURL).href}: net::ERR_ABORTED`
      );

      await page.goto('/models', { waitUntil: 'domcontentloaded' });
      const memberAgentDelete = await request.delete(`/api/agents/${createdAgent.id}`);
      assert.equal(memberAgentDelete.status(), 204);
      memberAgentId = null;

      const personalDelete = await ordinaryDeleteConnection(page, personal);
      assert.equal(personalDelete.response.status(), 204);
      allowedNoContentAborts.add(
        `requestfailed: DELETE ${personalDelete.response.url()}: net::ERR_ABORTED`
      );
      personalConnectionId = null;

      const usageUrls = await waitForLedgerSet(page, () => (
        page.getByRole('tab', { name: 'Usage' }).click()
      ));
      await assertLedgerRange(page, usageUrls, 'today');
      const byModel = page.getByRole('region', { name: 'By model' });
      await byModel.getByText(updatedPersonalName, { exact: true }).first().waitFor();
      assert.ok((await byModel.innerText()).includes(SUCCESS_MODEL_ID));
      const usageSection = page.getByRole('region', { name: 'Usage details' });
      const retainedUsageRow = usageSection.getByRole('row').filter({ hasText: memberAgentName }).filter({
        hasText: SUCCESS_MODEL_ID
      }).first();
      await retainedUsageRow.waitFor();
      assert.ok((await retainedUsageRow.innerText()).includes(updatedPersonalName));
      assert.equal(
        (await retainedUsageRow.innerText()).includes(PERSONAL_API_TYPE),
        false,
        'Usage table currently renders the retained model ID, not API Type'
      );
      const errorSection = page.getByRole('region', { name: 'Call errors' });
      const retainedErrorRow = errorSection.getByRole('row').filter({ hasText: ERROR_MODEL_ID }).first();
      await retainedErrorRow.waitFor();

      const retainedUsage = await responseJson(await request.get(ledgerPath('/api/model-usage', {
        from_ms: runStartedAt,
        to_ms: Date.now() + 1_000,
        model_connection_id: personal.id,
        page_size: 100
      })), 'Retained model usage');
      const runUsage = retainedUsage.items.find((item) => (
        item.agent.name === memberAgentName && item.model.model_id === SUCCESS_MODEL_ID
      ));
      assert.ok(runUsage, 'The completed Agent Run must retain one usage row');
      assert.equal(runUsage.model.name, updatedPersonalName);
      assert.equal(runUsage.model.api_type, PERSONAL_API_TYPE);
      assert.deepEqual(runUsage.model.request_settings, CHAT_REQUEST_SETTINGS);
      assert.equal(JSON.stringify(retainedUsage).includes(providerKey), false);
      const retainedErrors = await responseJson(await request.get(ledgerPath('/api/model-call-errors', {
        model_connection_id: personal.id,
        page_size: 100
      })), 'Retained model errors');
      const modelError = retainedErrors.items.find((item) => item.model.model_id === ERROR_MODEL_ID);
      assert.ok(modelError, 'The failed per-model test must retain one error row');
      assert.equal(modelError.model.api_type, PERSONAL_API_TYPE);
      assert.deepEqual(modelError.model.request_settings, {
        protocol: PERSONAL_API_TYPE,
        temperature: null,
        top_p: null,
        max_completion_tokens: null
      });

      const allRangeUrls = await waitForLedgerSet(page, () => (
        page.getByLabel('Time range').selectOption('all')
      ));
      await assertLedgerRange(page, allRangeUrls, 'all');
      await assertNoHorizontalOverflow(page, 'member desktop Usage');
      await page.setViewportSize({ width: 390, height: 844 });
      await assertNoHorizontalOverflow(page, 'member 390px Usage');
      await page.setViewportSize({ width: 1280, height: 800 });

      assert.equal((await request.post('/api/auth/logout')).ok(), true);
      await login(page, adminEmail, adminPassword);
      await page.goto('/models', { waitUntil: 'domcontentloaded' });
      const adminTabs = page.getByRole('tablist', { name: 'Models' });
      assert.equal(await adminTabs.getByRole('tab').count(), 4);
      await adminTabs.getByRole('tab', { name: 'Global Models' }).click();

      const globalName = scenarioContext.unique('QA Global pair');
      await page.setViewportSize({ width: 390, height: 844 });
      const globalConnection = await createConnection(page, context, 'global', {
        name: globalName,
        baseUrl: seedGlobal.base_url,
        apiType: GLOBAL_API_TYPE,
        allowedModelIds: [SUCCESS_MODEL_ID, REVIEW_MODEL_ID]
      }, providerKey);
      await page.setViewportSize({ width: 1280, height: 800 });
      globalConnectionId = globalConnection.id;
      const globalTable = page.getByRole('table', { name: 'Global model connection list' });
      const globalRow = globalTable.getByRole('row').filter({ hasText: globalName });
      assert.ok((await globalRow.innerText()).includes(SUCCESS_MODEL_ID));
      assert.ok((await globalRow.innerText()).includes(REVIEW_MODEL_ID));
      assert.ok((await globalRow.innerText()).includes(GLOBAL_API_TYPE));
      await testConnection(page, globalConnection, REVIEW_MODEL_ID, true);
      await setSystemDefault(page, globalConnection, SUCCESS_MODEL_ID);
      assert.ok((await globalRow.innerText()).includes(SUCCESS_MODEL_ID));

      const adminAgentName = scenarioContext.unique('QA Global pair Agent');
      const copiedAgent = await responseJson(await request.post('/api/agents', {
        data: {
          name: adminAgentName,
          instructions: 'Copy the System Default pair and retain an explicit subagent pair.',
          visibility: 'private',
          public_to: [],
          model_selection: null,
          model_settings: AUTOMATIC_RESPONSES_SETTINGS,
          codex_subagents: [{
            name: 'global-reviewer',
            description: 'Keeps the second allowed model selected.',
            developer_instructions: 'Review using the second Global model.',
            model_selection: selection(globalConnection, REVIEW_MODEL_ID),
            model_settings_override: {}
          }]
        }
      }), 'System Default Agent create');
      adminAgentId = copiedAgent.id;
      assert.deepEqual(copiedAgent.model_selection, selection(globalConnection, SUCCESS_MODEL_ID));

      const forceUpdateBody = {
        name: globalConnection.name,
        base_url: globalConnection.base_url,
        api_type: globalConnection.api_type,
        allowed_model_ids: [REVIEW_MODEL_ID]
      };
      const rejectedUpdate = await attemptConflictingConnectionEdit(page, globalConnection, {
        name: forceUpdateBody.name,
        baseUrl: forceUpdateBody.base_url,
        apiType: forceUpdateBody.api_type,
        allowedModelIds: forceUpdateBody.allowed_model_ids
      });
      consumeBrowserError(
        browserErrors,
        `response: 409 PUT ${rejectedUpdate.response.url()}`,
        'Expected ordinary allowlist-update conflict'
      );
      const unchangedAgent = await responseJson(
        await request.get(`/api/agents/${copiedAgent.id}`),
        'Agent after rejected allowlist update'
      );
      assert.deepEqual(unchangedAgent.model_selection, selection(globalConnection, SUCCESS_MODEL_ID));

      const forceUpdatePromise = page.waitForResponse((candidate) => {
        const url = new URL(candidate.url());
        return candidate.request().method() === 'PUT'
          && url.pathname === `/api/model-connections/${globalConnection.id}`
          && url.searchParams.get('force') === 'true';
      });
      await rejectedUpdate.dialog.getByRole('button', { name: 'Force save changes' }).click();
      const forceUpdated = await responseJson(
        await forceUpdatePromise,
        'Explicit UI force allowlist update'
      );
      assert.deepEqual(forceUpdated.allowed_model_ids, [REVIEW_MODEL_ID]);
      assertConnectionDto(forceUpdated, providerKey, 'Force update response');
      const afterForceUpdate = await responseJson(
        await request.get(`/api/agents/${copiedAgent.id}`),
        'Agent after force update'
      );
      assert.equal(afterForceUpdate.model_selection, null);
      assert.deepEqual(
        afterForceUpdate.codex_subagents[0].model_selection,
        selection(globalConnection, REVIEW_MODEL_ID)
      );
      assert.equal(afterForceUpdate.codex_subagents[0].enabled ?? true, true);
      assert.equal(afterForceUpdate.codex_subagents[0].disabled_reason ?? null, null);
      assert.deepEqual(
        await responseJson(
          await request.get('/api/model-connections/system-default'),
          'System Default after force update'
        ),
        { selection: null }
      );

      await page.goto(`/agents/${copiedAgent.id}`, { waitUntil: 'domcontentloaded' });
      await page.getByRole('tab', { name: 'Models' }).click();
      let adminModelsPanel = page.getByRole('tabpanel', { name: 'Models' });
      const adminModelSelect = adminModelsPanel.getByLabel('Model API Connection and model');
      assert.equal(await adminModelSelect.inputValue(), '');
      let globalSubagentRow = adminModelsPanel.getByRole('table', { name: 'Codex subagents' })
        .getByRole('row').filter({ hasText: 'global-reviewer' });
      assert.ok((await globalSubagentRow.innerText()).includes(REVIEW_MODEL_ID));
      assert.ok((await globalSubagentRow.innerText()).includes('Enabled'));
      await assertNoHorizontalOverflow(page, 'admin desktop Agent after force update');
      await page.setViewportSize({ width: 390, height: 844 });
      await assertNoHorizontalOverflow(page, 'admin 390px Agent after force update');
      await page.setViewportSize({ width: 1280, height: 800 });

      await adminModelSelect.selectOption(
        `${globalConnection.id}\n${REVIEW_MODEL_ID}`
      );
      const agentSaveResponsePromise = page.waitForResponse((response) => (
        response.request().method() === 'PATCH'
        && new URL(response.url()).pathname === `/api/agents/${copiedAgent.id}`
      ));
      await adminModelsPanel.getByRole('button', { name: 'Save Agent' }).click();
      const reboundAgent = await responseJson(
        await agentSaveResponsePromise,
        'Agent rebound before force delete'
      );
      assert.deepEqual(reboundAgent.model_selection, selection(globalConnection, REVIEW_MODEL_ID));

      await page.goto('/models', { waitUntil: 'domcontentloaded' });
      await page.getByRole('tab', { name: 'Global Models' }).click();
      await assertNoHorizontalOverflow(page, 'admin desktop Global Models');
      await page.setViewportSize({ width: 390, height: 844 });
      await assertNoHorizontalOverflow(page, 'admin 390px Global Models');
      await page.setViewportSize({ width: 1280, height: 800 });

      const ordinaryDelete = await ordinaryDeleteConnection(page, globalConnection);
      assert.equal(ordinaryDelete.response.status(), 409, 'Referenced Global delete must conflict');
      await ordinaryDelete.dialog.getByRole('alert').getByText(
        'The model action could not be completed.'
      ).waitFor();
      consumeBrowserError(
        browserErrors,
        `response: 409 DELETE ${ordinaryDelete.response.url()}`,
        'Expected ordinary-delete conflict'
      );
      await ordinaryDelete.dialog.getByRole('button', { name: 'Cancel' }).click();

      await page.setViewportSize({ width: 390, height: 844 });
      const forceDelete = await forceDeleteConnection(page, globalConnection, () => (
        assertNoHorizontalOverflow(page, 'admin 390px Force Delete dialog')
      ));
      assert.equal(forceDelete.response.status(), 204);
      allowedNoContentAborts.add(
        `requestfailed: POST ${forceDelete.response.url()}: net::ERR_ABORTED`
      );
      globalConnectionId = null;
      await page.setViewportSize({ width: 1280, height: 800 });

      const afterForceDelete = await responseJson(
        await request.get(`/api/agents/${copiedAgent.id}`),
        'Agent after force delete'
      );
      assert.equal(afterForceDelete.model_selection, null);
      assert.equal(afterForceDelete.codex_subagents[0].model_selection, null);
      assert.equal(afterForceDelete.codex_subagents[0].enabled, false);
      assert.equal(afterForceDelete.codex_subagents[0].disabled_reason, 'model_connection_deleted');
      await page.goto(`/agents/${copiedAgent.id}`, { waitUntil: 'domcontentloaded' });
      await page.getByRole('tab', { name: 'Models' }).click();
      adminModelsPanel = page.getByRole('tabpanel', { name: 'Models' });
      assert.equal(
        await adminModelsPanel.getByLabel('Model API Connection and model').inputValue(),
        ''
      );
      globalSubagentRow = adminModelsPanel.getByRole('table', { name: 'Codex subagents' })
        .getByRole('row').filter({ hasText: 'global-reviewer' });
      assert.ok((await globalSubagentRow.innerText()).includes('Disabled'));
      await page.setViewportSize({ width: 390, height: 844 });
      await assertNoHorizontalOverflow(page, 'admin 390px Agent after force delete');

      await page.goto('/models', { waitUntil: 'domcontentloaded' });
      const adminAgentDelete = await request.delete(`/api/agents/${copiedAgent.id}`);
      assert.equal(adminAgentDelete.status(), 204);
      adminAgentId = null;

      const unexpectedBrowserErrors = browserErrors.filter(
        (error) => !allowedNoContentAborts.has(error)
      );
      browserErrors.splice(0, browserErrors.length, ...unexpectedBrowserErrors);
      assert.deepEqual(browserErrors, [], 'Browser diagnostics must remain empty');
    });
  } catch (error) {
    scenarioError = error;
  }

  const cleanupErrors = [];
  const cleanup = async (label, action) => {
    try {
      await action();
    } catch (error) {
      cleanupErrors.push(`${label}: ${error.message}`);
    }
  };

  if (memberAgentId) {
    await cleanup('delete member Agent', () => memberClient.delete(
      `/api/agents/${memberAgentId}`,
      { expectedStatus: [204, 404] }
    ));
  }
  if (adminAgentId) {
    await cleanup('delete Administrator Agent', () => adminClient.delete(
      `/api/agents/${adminAgentId}`,
      { expectedStatus: [204, 404] }
    ));
  }
  if (personalConnectionId) {
    await cleanup('force-delete Personal Model API Connection', () => memberClient.request(
      `/api/model-connections/${personalConnectionId}/force-delete`,
      { method: 'POST', expectedStatus: [204, 404] }
    ));
  }
  if (globalConnectionId) {
    await cleanup('force-delete Global Model API Connection', () => adminClient.request(
      `/api/model-connections/${globalConnectionId}/force-delete`,
      { method: 'POST', expectedStatus: [204, 404] }
    ));
  }
  await cleanup('restore Administrator role', () => superClient.request(
    `/api/admin/users/${adminRegistration.user.id}/role`,
    { method: 'PUT', body: { role: 'member' } }
  ));
  await cleanup('restore System Default model selection', async () => {
    const { data: restored } = await superClient.request(
      '/api/model-connections/system-default',
      { method: 'PUT', body: { selection: defaultSnapshot.selection } }
    );
    assert.deepEqual(restored, defaultSnapshot);
  });

  if (scenarioError) {
    if (cleanupErrors.length > 0) {
      scenarioError.message += `\nCleanup failures:\n${cleanupErrors.join('\n')}`;
    }
    throw scenarioError;
  }
  if (cleanupErrors.length > 0) {
    throw new Error(`Model browser scenario cleanup failed:\n${cleanupErrors.join('\n')}`);
  }
}
