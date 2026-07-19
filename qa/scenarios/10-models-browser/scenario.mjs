import assert from 'node:assert/strict';
import { ApiClient, loginAsAdmin } from '../../support/api.mjs';
import { withBrowser } from '../../support/browser.mjs';

const LEDGER_PATHS = [
  '/api/model-usage/summary',
  '/api/model-usage',
  '/api/model-call-errors'
];

function uniqueSlug(context, prefix) {
  return context.unique(prefix)
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, '-')
    .replace(/^-|-$/g, '');
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

async function closeRedactedConnectionDialog(dialog) {
  if (!await dialog.isVisible().catch(() => false)) return;
  await dialog.getByLabel('API key').fill('[redacted]').catch(() => undefined);
  await dialog.getByRole('button', { name: 'Cancel' }).click().catch(() => undefined);
}

async function createConnection(page, browserContext, scope, fields, apiKey) {
  await page.getByRole('button', {
    name: scope === 'personal' ? 'Create personal model' : 'Create global model'
  }).click();
  const dialog = page.getByRole('dialog', { name: 'Create model connection' });
  await dialog.getByLabel('Connection name').fill(fields.name);
  await dialog.getByLabel('Base URL').fill(fields.baseUrl);
  await dialog.getByLabel('Model ID').fill(fields.modelId);

  let connection;
  await browserContext.tracing.stop();
  try {
    await dialog.getByLabel('API key').fill(apiKey);
    const responsePromise = page.waitForResponse((response) => (
      response.request().method() === 'POST'
      && new URL(response.url()).pathname === '/api/model-connections'
    ));
    await dialog.getByRole('button', { name: 'Create model connection' }).click();
    connection = await responseJson(await responsePromise, `${scope} Model Connection create`);
    await dialog.waitFor({ state: 'detached' });
  } finally {
    await closeRedactedConnectionDialog(dialog);
    await browserContext.tracing.start({ screenshots: true, snapshots: true, sources: true });
  }
  assert.equal(Object.hasOwn(connection, 'api_key'), false, 'Create response must keep API keys write-only');
  assert.equal(JSON.stringify(connection).includes(apiKey), false, 'Create response must not expose the API key');
  return connection;
}

async function editConnection(page, connection, { name, modelId }) {
  await page.getByRole('button', { name: `Edit ${connection.name}` }).click();
  const dialog = page.getByRole('dialog', { name: 'Edit model connection' });
  assert.equal(await dialog.getByLabel('API key').inputValue(), '', 'Edit must not load the stored API key');
  await dialog.getByLabel('Connection name').fill(name);
  await dialog.getByLabel('Model ID').fill(modelId);
  const responsePromise = page.waitForResponse((response) => (
    response.request().method() === 'PATCH'
    && new URL(response.url()).pathname === `/api/model-connections/${connection.id}`
  ));
  await dialog.getByRole('button', { name: 'Save changes' }).click();
  const updated = await responseJson(await responsePromise, 'Model Connection update');
  assert.equal(Object.hasOwn(updated, 'api_key'), false, 'Update response must keep API keys write-only');
  return updated;
}

async function testConnection(page, connection, expectedSuccess) {
  await page.getByRole('button', { name: `Test ${connection.name}` }).click();
  const dialog = page.getByRole('dialog', { name: 'Test model connection' });
  const responsePromise = page.waitForResponse((response) => (
    response.request().method() === 'POST'
    && new URL(response.url()).pathname === `/api/model-connections/${connection.id}/test`
  ));
  await dialog.getByRole('button', { name: 'Run connection test' }).click();
  const result = await responseJson(await responsePromise, 'Model Connection test');
  assert.equal(result.success, expectedSuccess);
  await dialog.getByText(expectedSuccess ? 'Connection test succeeded.' : 'Connection test failed.').waitFor();
  await dialog.locator('.modal-actions').getByRole('button', { name: 'Close' }).click();
}

async function setConnectionStatus(page, connection, nextStatus) {
  const enabling = nextStatus === 'enabled';
  await page.getByRole('button', { name: `${enabling ? 'Enable' : 'Disable'} ${connection.name}` }).click();
  const title = `${enabling ? 'Enable' : 'Disable'} model connection`;
  const dialog = page.getByRole('dialog', { name: title });
  const responsePromise = page.waitForResponse((response) => (
    response.request().method() === 'PUT'
    && new URL(response.url()).pathname === `/api/model-connections/${connection.id}/status`
  ));
  await dialog.getByRole('button', { name: title }).click();
  return responseJson(await responsePromise, title);
}

async function changeSystemDefault(page, connection, setDefault) {
  const action = setDefault ? 'Set system default' : 'Clear system default';
  const button = setDefault
    ? `Set ${connection.name} as system default`
    : `Clear ${connection.name} as system default`;
  await page.getByRole('button', { name: button }).click();
  const dialog = page.getByRole('dialog', { name: action });
  const responsePromise = page.waitForResponse((response) => (
    response.request().method() === 'PUT'
    && new URL(response.url()).pathname === '/api/model-connections/system-default'
  ));
  await dialog.getByRole('button', { name: action }).click();
  return responseJson(await responsePromise, action);
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

async function assertRange(page, urls, range) {
  const params = urls.map((url) => ({
    from: url.searchParams.get('from_ms'),
    to: url.searchParams.get('to_ms')
  }));
  assert.deepEqual(params[1], params[0], `${range} usage range must match summary`);
  assert.deepEqual(params[2], params[0], `${range} error range must match summary`);
  if (range === 'all') {
    assert.deepEqual(params[0], { from: null, to: null });
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
  }, { selected: range, toMs: params[0].to });
  assert.deepEqual(params[0], expected, `${range} must use browser-local half-open boundaries`);
}

function requestCount(urls, pathname) {
  return urls.filter((url) => url.pathname === pathname).length;
}

export default async function modelsBrowserScenario(scenarioContext) {
  const superClient = new ApiClient(scenarioContext.baseURL);
  await loginAsAdmin(superClient);
  const { data: defaultSnapshot } = await superClient.get('/api/model-connections/system-default');
  const { data: initialConnections } = await superClient.get('/api/model-connections');
  const seedGlobal = initialConnections.find((connection) => (
    connection.id === defaultSnapshot.model_connection_id
  ));
  assert.ok(seedGlobal, 'Compose must provide an enabled System Default Model Connection');

  const providerKeyResult = scenarioContext.compose.run([
    'exec', '-T', 'backend', 'sh', '-c', 'printf %s "$DEV_MODEL_PROVIDER_API_KEY"'
  ]);
  const providerKey = providerKeyResult.stdout.trim();
  assert.ok(providerKey, 'The QA backend must expose its fake-provider key to the scenario process');

  const memberSlug = uniqueSlug(scenarioContext, 'qa-model-member');
  const memberEmail = `${memberSlug}@example.com`;
  const memberPassword = `${scenarioContext.unique('Model member password')}!Aa9`;
  const memberSeed = new ApiClient(scenarioContext.baseURL);
  const { data: memberRegistration } = await memberSeed.post('/api/auth/register', {
    email: memberEmail,
    password: memberPassword
  });

  const adminSlug = uniqueSlug(scenarioContext, 'qa-model-admin');
  const adminEmail = `${adminSlug}@example.com`;
  const adminPassword = `${scenarioContext.unique('Model admin password')}!Bb8`;
  const adminSeed = new ApiClient(scenarioContext.baseURL);
  const { data: adminRegistration } = await adminSeed.post('/api/auth/register', {
    email: adminEmail,
    password: adminPassword
  });
  await superClient.request(`/api/admin/users/${adminRegistration.user.id}/role`, {
    method: 'PUT',
    body: { role: 'admin' }
  });

  let createdGlobalId = null;
  try {
    await withBrowser(scenarioContext, {
      allowedHttpErrors: [
        { method: 'GET', pathname: '/api/auth/me', status: 401, times: 2 }
      ]
    }, async ({ page, context, request, browserErrors }) => {
      const ledgerRequests = [];
      const allowedNoContentAborts = new Set();
      page.on('request', (browserRequest) => {
        const url = new URL(browserRequest.url());
        if (LEDGER_PATHS.includes(url.pathname)) ledgerRequests.push(url);
      });

      await login(page, memberEmail, memberPassword);
      await page.goto('/models', { waitUntil: 'domcontentloaded' });
      const tabs = page.getByRole('tablist', { name: 'Models' });
      assert.equal(await tabs.getByRole('tab').count(), 3, 'member must see Personal, Available, and Usage tabs');
      assert.equal(await tabs.getByRole('tab', { name: 'Global Models' }).count(), 0);

      const personalName = scenarioContext.unique('QA Personal Responses');
      let personal = await createConnection(page, context, 'personal', {
        name: personalName,
        baseUrl: seedGlobal.base_url,
        modelId: seedGlobal.model_id
      }, providerKey);
      const updatedPersonalName = `${personalName} Updated`;
      personal = await editConnection(page, personal, {
        name: updatedPersonalName,
        modelId: seedGlobal.model_id
      });
      await testConnection(page, personal, true);
      for (let index = 0; index < 20; index += 1) {
        const result = await responseJson(
          await request.post(`/api/model-connections/${personal.id}/test`),
          `successful ledger seed ${index + 1}`
        );
        assert.equal(result.success, true);
      }

      personal = await editConnection(page, personal, {
        name: updatedPersonalName,
        modelId: 'hub-proxy-error'
      });
      await testConnection(page, personal, false);
      for (let index = 0; index < 20; index += 1) {
        const result = await responseJson(
          await request.post(`/api/model-connections/${personal.id}/test`),
          `failed ledger seed ${index + 1}`
        );
        assert.equal(result.success, false);
      }

      personal = await setConnectionStatus(page, personal, 'disabled');
      await tabs.getByRole('tab', { name: 'Available Models' }).click();
      const availableTable = page.getByRole('table', { name: 'Available model list' });
      assert.equal(await availableTable.getByText(updatedPersonalName, { exact: true }).count(), 0);
      assert.ok((await availableTable.innerText()).includes(seedGlobal.name));
      await tabs.getByRole('tab', { name: 'My Models' }).click();
      personal = await setConnectionStatus(page, personal, 'enabled');
      await tabs.getByRole('tab', { name: 'Available Models' }).click();
      await availableTable.getByText(updatedPersonalName, { exact: true }).waitFor();
      await assertNoHorizontalOverflow(page, 'member desktop Available Models');

      const memberAgentName = scenarioContext.unique('QA Member Model Agent');
      const memberAgent = await responseJson(await request.post('/api/agents', {
        data: {
          name: memberAgentName,
          instructions: 'Verify member model bindings.',
          visibility: 'private',
          public_to: [],
          default_model_connection_id: personal.id,
          reasoning_effort: 'high',
          codex_subagents: [{
            name: 'reviewer',
            description: 'Reviews model configuration.',
            developer_instructions: 'Review the configured model bindings.',
            model_connection_id: seedGlobal.id,
            reasoning_effort: 'max'
          }]
        }
      }), 'member Agent seed');
      await page.goto(`/agents/${memberAgent.id}`, { waitUntil: 'domcontentloaded' });
      await page.getByRole('tab', { name: 'Models' }).click();
      const memberModelsPanel = page.getByRole('tabpanel', { name: 'Models' });
      assert.equal(await memberModelsPanel.getByLabel('Default model connection').inputValue(), personal.id);
      const memberSubagents = memberModelsPanel.getByRole('table', { name: 'Codex subagents' });
      assert.ok((await memberSubagents.innerText()).includes('reviewer'));
      assert.ok((await memberSubagents.innerText()).includes(seedGlobal.name));
      assert.equal((await memberSubagents.innerText()).includes('Max'), true);
      assert.equal((await request.delete(`/api/agents/${memberAgent.id}`)).status(), 204);

      await page.goto('/models', { waitUntil: 'domcontentloaded' });
      await page.getByRole('button', { name: `Delete ${updatedPersonalName}`, exact: true }).click();
      let dialog = page.getByRole('dialog', { name: 'Delete model connection' });
      let responsePromise = page.waitForResponse((response) => (
        response.request().method() === 'DELETE'
        && new URL(response.url()).pathname === `/api/model-connections/${personal.id}`
      ));
      await dialog.getByRole('button', { name: 'Delete model connection' }).click();
      const personalDeleteResponse = await responsePromise;
      assert.equal(personalDeleteResponse.status(), 204, 'unreferenced Personal connection must delete normally');
      allowedNoContentAborts.add(
        `requestfailed: DELETE ${personalDeleteResponse.url()}: net::ERR_ABORTED`
      );

      const initialUsageUrls = await waitForLedgerSet(page, () => (
        page.getByRole('tab', { name: 'Usage' }).click()
      ));
      await assertRange(page, initialUsageUrls, 'today');
      const overall = page.getByRole('region', { name: 'Overall' });
      await overall.locator('.model-overall-totals').getByText('525', { exact: true }).waitFor();
      assert.ok((await page.getByRole('region', { name: 'By model' }).innerText()).includes(updatedPersonalName));
      assert.ok((await page.getByRole('region', { name: 'By Agent' }).innerText()).includes('Model Connection test'));
      assert.ok((await page.getByRole('region', { name: 'By user' }).innerText()).includes(
        memberRegistration.user.display_name || memberRegistration.user.username
      ));

      const rangeSelect = page.getByLabel('Time range');
      assert.deepEqual(await rangeSelect.locator('option').evaluateAll((options) => (
        options.map((option) => option.value)
      )), ['today', 'yesterday', '7days', '30days', '90days', 'all']);
      for (const range of ['yesterday', '7days', '30days', '90days', 'all', 'today']) {
        const urls = await waitForLedgerSet(page, () => rangeSelect.selectOption(range));
        await assertRange(page, urls, range);
      }

      const usageSection = page.getByRole('region', { name: 'Usage details' });
      const errorSection = page.getByRole('region', { name: 'Call errors' });
      assert.equal(await usageSection.locator('tbody tr').count(), 20);
      assert.equal(await errorSection.locator('tbody tr').count(), 20);
      const beforeUsage = LEDGER_PATHS.map((pathname) => requestCount(ledgerRequests, pathname));
      responsePromise = page.waitForResponse((response) => (
        new URL(response.url()).pathname === '/api/model-usage'
        && new URL(response.url()).searchParams.has('cursor_id')
      ));
      await usageSection.getByRole('button', { name: 'Next usage page' }).click();
      await responsePromise;
      await page.waitForTimeout(100);
      assert.deepEqual(LEDGER_PATHS.map((pathname) => requestCount(ledgerRequests, pathname)), [
        beforeUsage[0], beforeUsage[1] + 1, beforeUsage[2]
      ], 'usage pagination must not reload summary or errors');
      assert.equal(await usageSection.locator('.model-pagination > span').innerText(), '2');

      const beforeErrors = LEDGER_PATHS.map((pathname) => requestCount(ledgerRequests, pathname));
      responsePromise = page.waitForResponse((response) => (
        new URL(response.url()).pathname === '/api/model-call-errors'
        && new URL(response.url()).searchParams.has('cursor_id')
      ));
      await errorSection.getByRole('button', { name: 'Next error page' }).click();
      await responsePromise;
      await page.waitForTimeout(100);
      assert.deepEqual(LEDGER_PATHS.map((pathname) => requestCount(ledgerRequests, pathname)), [
        beforeErrors[0], beforeErrors[1], beforeErrors[2] + 1
      ], 'error pagination must not reload summary or usage');
      assert.equal(await errorSection.locator('.model-pagination > span').innerText(), '2');
      await page.setViewportSize({ width: 390, height: 844 });
      await assertNoHorizontalOverflow(page, 'member 390px Usage');
      await page.setViewportSize({ width: 1280, height: 800 });

      assert.equal((await request.post('/api/auth/logout')).ok(), true);
      await login(page, adminEmail, adminPassword);
      await page.goto('/models', { waitUntil: 'domcontentloaded' });
      const adminTabs = page.getByRole('tablist', { name: 'Models' });
      assert.equal(await adminTabs.getByRole('tab').count(), 4, 'admin must see the Global tab');
      await adminTabs.getByRole('tab', { name: 'Global Models' }).click();
      const globalName = scenarioContext.unique('QA Global Responses');
      const globalConnection = await createConnection(page, context, 'global', {
        name: globalName,
        baseUrl: seedGlobal.base_url,
        modelId: seedGlobal.model_id
      }, providerKey);
      createdGlobalId = globalConnection.id;
      await changeSystemDefault(page, globalConnection, true);

      const adminAgentName = scenarioContext.unique('QA Default Copy Agent');
      const copiedAgent = await responseJson(await request.post('/api/agents', {
        data: {
          name: adminAgentName,
          instructions: 'Verify System Default copy semantics.',
          visibility: 'private',
          public_to: [],
          reasoning_effort: 'medium',
          codex_subagents: [{
            name: 'researcher',
            description: 'Uses the original Compose connection.',
            developer_instructions: 'Research with the configured override.',
            model_connection_id: seedGlobal.id,
            reasoning_effort: 'high'
          }]
        }
      }), 'System Default Agent seed');
      assert.equal(copiedAgent.default_model_connection_id, globalConnection.id);
      await changeSystemDefault(page, globalConnection, false);
      const afterClear = await responseJson(await request.get(`/api/agents/${copiedAgent.id}`), 'Agent after default clear');
      assert.equal(afterClear.default_model_connection_id, globalConnection.id, 'existing Agent must retain its copied default');
      await changeSystemDefault(page, seedGlobal, true);

      await page.goto(`/agents/${copiedAgent.id}`, { waitUntil: 'domcontentloaded' });
      await page.getByRole('tab', { name: 'Models' }).click();
      const adminModelsPanel = page.getByRole('tabpanel', { name: 'Models' });
      assert.equal(await adminModelsPanel.getByLabel('Default model connection').inputValue(), globalConnection.id);
      const adminSubagents = adminModelsPanel.getByRole('table', { name: 'Codex subagents' });
      assert.ok((await adminSubagents.innerText()).includes('researcher'));
      assert.ok((await adminSubagents.innerText()).includes(seedGlobal.name));

      await page.goto('/models', { waitUntil: 'domcontentloaded' });
      await page.getByRole('tab', { name: 'Global Models' }).click();
      await assertNoHorizontalOverflow(page, 'admin desktop Global Models');
      await page.setViewportSize({ width: 390, height: 844 });
      await assertNoHorizontalOverflow(page, 'admin 390px Global Models');

      await page.getByRole('button', { name: `Delete ${globalName}`, exact: true }).click();
      dialog = page.getByRole('dialog', { name: 'Delete model connection' });
      responsePromise = page.waitForResponse((response) => (
        response.request().method() === 'DELETE'
        && new URL(response.url()).pathname === `/api/model-connections/${globalConnection.id}`
      ));
      await dialog.getByRole('button', { name: 'Delete model connection' }).click();
      const conflictResponse = await responsePromise;
      assert.equal(conflictResponse.status(), 409, 'referenced Global connection must reject ordinary deletion');
      await dialog.getByRole('alert').getByText('The model action could not be completed.').waitFor();
      const expectedConflict = `response: 409 DELETE ${conflictResponse.url()}`;
      const conflictIndex = browserErrors.indexOf(expectedConflict);
      assert.notEqual(conflictIndex, -1, 'the exact expected 409 must be captured by browser diagnostics');
      browserErrors.splice(conflictIndex, 1);
      await dialog.getByRole('button', { name: 'Cancel' }).click();

      await page.getByRole('button', { name: `Force-delete ${globalName}` }).click();
      dialog = page.getByRole('dialog', { name: 'Force-delete model connection' });
      await assertNoHorizontalOverflow(page, 'admin 390px Force Delete dialog');
      responsePromise = page.waitForResponse((response) => (
        response.request().method() === 'POST'
        && new URL(response.url()).pathname === `/api/model-connections/${globalConnection.id}/force-delete`
      ));
      await dialog.getByRole('button', { name: 'Force-delete model connection' }).click();
      const forceDeleteResponse = await responsePromise;
      assert.equal(forceDeleteResponse.status(), 204);
      allowedNoContentAborts.add(
        `requestfailed: POST ${forceDeleteResponse.url()}: net::ERR_ABORTED`
      );
      const scrubbedAgent = await responseJson(await request.get(`/api/agents/${copiedAgent.id}`), 'Agent after Force Delete');
      assert.equal(scrubbedAgent.default_model_connection_id, null);
      assert.equal(scrubbedAgent.codex_subagents[0].model_connection_id, seedGlobal.id);
      assert.equal((await request.get(`/api/model-connections/${globalConnection.id}`)).status(), 404);
      createdGlobalId = null;

      const persistedDefault = await responseJson(
        await request.get('/api/model-connections/system-default'),
        'restored System Default'
      );
      assert.equal(persistedDefault.model_connection_id, defaultSnapshot.model_connection_id);
      const unexpectedBrowserErrors = browserErrors.filter((error) => !allowedNoContentAborts.has(error));
      browserErrors.splice(0, browserErrors.length, ...unexpectedBrowserErrors);
      assert.deepEqual(browserErrors, [], 'Browser diagnostics must remain empty');
    });
  } finally {
    if (createdGlobalId) {
      const { data: connections } = await superClient.get('/api/model-connections');
      if (connections.some((connection) => connection.id === createdGlobalId)) {
        await superClient.post(`/api/model-connections/${createdGlobalId}/force-delete`, undefined, {
          expectedStatus: 204
        });
      }
    }
    await superClient.request('/api/model-connections/system-default', {
      method: 'PUT',
      body: { model_connection_id: defaultSnapshot.model_connection_id }
    });
    const { data: restored } = await superClient.get('/api/model-connections/system-default');
    assert.equal(restored.model_connection_id, defaultSnapshot.model_connection_id, 'System Default restore must persist');
  }
}
