import { createHmac, randomUUID } from 'node:crypto';
import { expect, request, test } from '@playwright/test';
import { selectLocalPasswordLogin } from './authentication-helpers';

test.use({ trace: 'off', screenshot: 'off', video: 'off' });

function signEmbedJwt(agentId: string, ownerId: string, overrides: Record<string, unknown> = {}) {
  const header = Buffer.from(JSON.stringify({ alg: 'HS256', typ: 'JWT' })).toString('base64url');
  const now = Math.floor(Date.now() / 1000);
  const payload = Buffer.from(JSON.stringify({
    iss: 'agent-hub-dev',
    aud: 'agent-hub-widget',
    exp: now + 300,
    iat: now,
    jti: randomUUID(),
    sub: 'external-user',
    agent_id: agentId,
    owner_id: ownerId,
    ...overrides
  })).toString('base64url');
  const signature = createHmac('sha256', 'dev-embed-jwt-secret')
    .update(`${header}.${payload}`)
    .digest('base64url');
  return `${header}.${payload}.${signature}`;
}

async function provisionAndSignInPasswordUser(page: import('@playwright/test').Page, email: string) {
  const password = 'browser-test-password';
  expect((await page.request.post('/api/auth/login', { data: { email: 'admin@example.com', password: 'admin123' } })).ok()).toBeTruthy();
  expect((await page.request.post('/api/admin/users', {
    data: { email, password, role: 'member' }
  })).ok()).toBeTruthy();
  expect((await page.request.post('/api/auth/logout')).status()).toBe(204);
  await page.goto('/login');
  await selectLocalPasswordLogin(page);
  await page.getByLabel('Email').fill(email);
  await page.getByLabel('Password').fill(password);
  await page.getByRole('button', { name: 'Sign in', exact: true }).click();
  await expect(page.getByText(email)).toBeVisible();
}

test('API keys can be created with validity, renewed in place, and deleted', async ({ page, baseURL }) => {
  await page.goto('/login');
  await selectLocalPasswordLogin(page);
  await page.getByLabel('Email').fill('admin@example.com');
  await page.getByLabel('Password').fill('admin123');
  await page.getByRole('button', { name: 'Sign in', exact: true }).click();
  await expect(page.getByText('admin@example.com')).toBeVisible();

  await page.goto('/api-keys');
  await page.getByRole('button', { name: 'Create API key' }).click();
  await expect(page.getByRole('dialog', { name: 'Create API key' })).toBeVisible();
  const keyName = `Browser key ${Date.now()}`;
  await page.getByLabel('Name', { exact: true }).fill(keyName);
  await page.getByLabel('Validity').selectOption('180');
  await page.getByRole('button', { name: 'Create key' }).click();
  await expect(page.getByRole('dialog', { name: 'One-time API key' })).toBeVisible();
  const oldToken = await page.locator('.secret-token').innerText();
  await page.getByRole('dialog', { name: 'One-time API key' }).locator('footer').getByRole('button', { name: 'Close', exact: true }).click();
  await expect(page.locator('.secret-token')).toHaveCount(0);
  await expect(page.getByRole('button', { name: 'Create API key' })).toBeFocused();
  expect(/^ahk_[A-Za-z0-9]+$/.test(oldToken)).toBe(true);

  const api = await request.newContext({ baseURL });
  const me = await api.get('/api/auth/me', { headers: { Authorization: `Bearer ${oldToken}` } });
  expect(me.ok()).toBeTruthy();

  const created = await api.post('/api/agents', {
    headers: { Authorization: `Bearer ${oldToken}` },
    data: { name: `API Key Agent ${Date.now()}`, instructions: 'Created through API key.', visibility: 'private' }
  });
  expect(created.ok()).toBeTruthy();
  const apiKeyAgent = await created.json();

  const keyRow = page.locator('.api-key-row', { hasText: keyName });
  await expect(keyRow).toBeVisible();
  await expect(keyRow).not.toContainText('revoked');
  await expect(keyRow.getByRole('button', { name: 'Revoke' })).toHaveCount(0);
  const renewButton = keyRow.getByRole('button', { name: 'Renew' });
  await renewButton.click();
  const renewDialog = page.getByRole('dialog', { name: 'Renew API key' });
  await expect(renewDialog).toBeVisible();
  await expect(renewDialog).toContainText('Renewal keeps the existing token.');
  await expect(renewDialog.locator('.secret-token')).toHaveCount(0);
  const renewResponse = page.waitForResponse((response) => response.request().method() === 'POST' && response.url().endsWith('/renew'));
  await renewDialog.getByRole('button', { name: 'Renew' }).click();
  expect((await renewResponse).status()).toBe(200);
  await expect(renewDialog).toHaveCount(0);
  await expect(page.getByText('API key expiration updated.')).toBeVisible();
  expect((await api.get('/api/auth/me', { headers: { Authorization: `Bearer ${oldToken}` } })).ok()).toBeTruthy();

  let deleteResponseStatus = 0;
  page.once('dialog', async (dialog) => {
    expect(dialog.message()).toBe(`Delete API key "${keyName}"? This cannot be undone.`);
    await dialog.accept();
  });
  const deleteResponsePromise = page.waitForResponse((response) => response.request().method() === 'DELETE' && response.url().includes('/api/auth/api-keys/'));
  await keyRow.getByRole('button', { name: 'Delete' }).click();
  deleteResponseStatus = (await deleteResponsePromise).status();
  expect(deleteResponseStatus).toBe(204);
  await expect(keyRow).toHaveCount(0);
  expect((await api.get('/api/auth/me', { headers: { Authorization: `Bearer ${oldToken}` } })).status()).toBe(401);
  const list = await page.request.get('/api/auth/api-keys');
  const listedKeys = JSON.stringify((await list.json()).items);
  expect(listedKeys.includes(oldToken)).toBe(false);
  await page.getByRole('button', { name: 'Agents' }).click();
  await page.getByRole('button', { name: 'API Keys' }).click();
  await expect(page.getByText('This token is shown once.')).toHaveCount(0);
  await page.request.delete(`/api/agents/${apiKeyAgent.id}`);
  await api.dispose();
});

test('API key mutations are serialized across create and key rows', async ({ page }) => {
  const email = `api-key-lock-${Date.now()}@example.com`;
  await provisionAndSignInPasswordUser(page, email);
  const names = [`Lock A ${Date.now()}`, `Lock B ${Date.now()}`];
  for (const name of names) await page.request.post('/api/auth/api-keys', { data: { name } });
  await page.goto('/api-keys');
  const rows = names.map((name) => page.getByText(name, { exact: true }).locator('..'));
  await Promise.all(rows.map((row) => expect(row).toBeVisible()));

  let renewRequests = 0;
  let releaseRenew!: () => void;
  const renewGate = new Promise<void>((resolve) => { releaseRenew = resolve; });
  await page.route('**/api/auth/api-keys/*/renew', async (route) => {
    renewRequests += 1;
    await renewGate;
    await route.continue();
  });
  await rows[0].getByRole('button', { name: 'Renew' }).click();
  const renewDialog = page.getByRole('dialog', { name: 'Renew API key' });
  await renewDialog.getByRole('button', { name: 'Renew' }).dblclick();
  await expect.poll(() => renewRequests).toBe(1);
  await expect(rows[1].getByRole('button', { name: 'Renew' })).toBeDisabled();
  await rows[1].getByRole('button', { name: 'Renew' }).click({ force: true });
  releaseRenew();
  await expect(renewDialog).toHaveCount(0);
  await expect(page.getByText('API key expiration updated.')).toBeVisible();
  expect(renewRequests).toBe(1);
  await expect(page.getByText('This token is shown once.')).toHaveCount(0);

  let createRequests = 0;
  let releaseCreate!: () => void;
  const createGate = new Promise<void>((resolve) => { releaseCreate = resolve; });
  await page.route('**/api/auth/api-keys', async (route) => {
    if (route.request().method() !== 'POST') return route.continue();
    createRequests += 1;
    await createGate;
    await route.fulfill({ status: 500, json: { error: 'delayed create failed' } });
  });
  await page.getByRole('button', { name: 'Create API key' }).click();
  await page.getByLabel('Name', { exact: true }).fill(`Serialized ${Date.now()}`);
  await page.getByRole('button', { name: 'Create key' }).dblclick();
  await expect.poll(() => createRequests).toBe(1);
  await expect(page.getByLabel('Name', { exact: true })).toBeDisabled();
  await expect(rows[0].getByRole('button', { name: 'Renew' })).toBeDisabled();
  const createDialog = page.getByRole('dialog', { name: 'Create API key' });
  await expect(createDialog).toBeFocused();
  await page.keyboard.press('Tab');
  await expect(createDialog).toBeFocused();
  releaseCreate();
  await expect(createDialog.getByRole('alert')).toContainText('The request could not be completed. Retry.');
  await expect(createDialog).toBeFocused();
  await page.keyboard.press('Tab');
  await expect(createDialog.locator('header').getByRole('button', { name: 'Close', exact: true })).toBeFocused();
  await createDialog.focus();
  await page.keyboard.press('Shift+Tab');
  await expect(createDialog.getByRole('button', { name: 'Create key', exact: true })).toBeFocused();
});

test('API key list load failure is redacted and recovers on retry', async ({ page }) => {
  await page.request.post('/api/auth/login', { data: { email: 'admin@example.com', password: 'admin123' } });
  let attempts = 0;
  let releaseRetry!: () => void;
  const retryGate = new Promise<void>((resolve) => { releaseRetry = resolve; });
  await page.route(/\/api\/auth\/api-keys\?/, async (route) => {
    if (route.request().method() !== 'GET') return route.continue();
    attempts += 1;
    if (attempts === 1) return route.fulfill({ status: 500, json: { error: 'sensitive database detail' } });
    if (attempts === 2) await retryGate;
    return route.continue();
  });
  await page.goto('/api-keys');
  await expect(page.getByRole('alert')).toContainText('Unable to load API keys. Retry.');
  await expect(page.getByText('sensitive database detail')).toHaveCount(0);
  const retry = page.getByRole('button', { name: 'Retry' });
  await retry.click();
  await expect(retry).toBeDisabled();
  await retry.dblclick({ force: true });
  expect(attempts).toBe(2);
  releaseRetry();
  await expect(page.getByRole('alert')).toHaveCount(0);
  expect(attempts).toBe(2);
});

test('only the latest API key page request can update the list', async ({ page }) => {
  await page.request.post('/api/auth/login', { data: { email: 'admin@example.com', password: 'admin123' } });
  let delayPageOne = false;
  let delayedPageOneStarted = 0;
  let releasePageOne!: () => void;
  let markDelayedPageOneSettled!: () => void;
  const pageOneGate = new Promise<void>((resolve) => { releasePageOne = resolve; });
  const delayedPageOneSettled = new Promise<void>((resolve) => { markDelayedPageOneSettled = resolve; });
  const item = (pageNumber: number) => ({
    id: `00000000-0000-0000-0000-00000000000${pageNumber}`,
    name: `Mock page ${pageNumber}`,
    prefix: `mock-${pageNumber}`,
    last_used_at: null,
    expires_at: '2027-01-01T00:00:00Z',
    created_at: '2026-01-01T00:00:00Z'
  });
  await page.route(/\/api\/auth\/api-keys\?/, async (route) => {
    const requestedPage = Number(new URL(route.request().url()).searchParams.get('page'));
    if (requestedPage === 1 && delayPageOne) {
      delayedPageOneStarted += 1;
      await pageOneGate;
      await route.fulfill({ json: { items: [item(requestedPage)], total: 60, page: requestedPage, page_size: 20 } }).catch(() => undefined);
      markDelayedPageOneSettled();
      return;
    }
    await route.fulfill({ json: { items: [item(requestedPage)], total: 60, page: requestedPage, page_size: 20 } });
  });

  await page.goto('/api-keys');
  await expect(page.getByText('Mock page 1', { exact: true })).toBeVisible();
  await page.getByRole('button', { name: 'Next' }).click();
  await expect(page.getByText('Mock page 2', { exact: true })).toBeVisible();
  delayPageOne = true;
  await page.getByRole('button', { name: 'Previous' }).click();
  await expect.poll(() => delayedPageOneStarted).toBe(1);
  await expect(page.getByRole('button', { name: 'Next' })).toBeEnabled();
  await page.getByRole('button', { name: 'Next' }).click();
  await expect(page.getByText('Mock page 3', { exact: true })).toBeVisible();
  releasePageOne();
  await delayedPageOneSettled;
  await expect(page.getByText('Mock page 3', { exact: true })).toBeVisible();
  await expect(page.getByText('Mock page 1', { exact: true })).toHaveCount(0);
});

test('a successful mutation force-refreshes over an older same-page request', async ({ page }) => {
  await page.request.post('/api/auth/login', { data: { email: 'admin@example.com', password: 'admin123' } });
  let listRequests = 0;
  let releaseOldList!: () => void;
  let markOldListSettled!: () => void;
  const oldListGate = new Promise<void>((resolve) => { releaseOldList = resolve; });
  const oldListSettled = new Promise<void>((resolve) => { markOldListSettled = resolve; });
  const listItem = (name: string, id: string) => ({
    id,
    name,
    prefix: 'mock-prefix',
    last_used_at: null,
    expires_at: '2027-01-01T00:00:00Z',
    created_at: '2026-01-01T00:00:00Z'
  });
  await page.route(/\/api\/auth\/api-keys\?/, async (route) => {
    listRequests += 1;
    if (listRequests === 1) {
      await oldListGate;
      await route.fulfill({ json: { items: [listItem('Old response', '00000000-0000-0000-0000-000000000001')], total: 1, page: 1, page_size: 20 } }).catch(() => undefined);
      markOldListSettled();
      return;
    }
    await route.fulfill({ json: { items: [listItem('Forced new key', '00000000-0000-0000-0000-000000000002')], total: 1, page: 1, page_size: 20 } });
  });
  await page.route('**/api/auth/api-keys', async (route) => {
    if (route.request().method() !== 'POST') return route.continue();
    await route.fulfill({ json: { api_key: listItem('Forced new key', '00000000-0000-0000-0000-000000000002'), token: 'test-only-secret' } });
  });

  await page.goto('/api-keys');
  await expect.poll(() => listRequests).toBe(1);
  await page.getByRole('button', { name: 'Create API key' }).click();
  await page.getByLabel('Name', { exact: true }).fill('Forced new key');
  await page.getByRole('button', { name: 'Create key' }).click();
  await expect.poll(() => listRequests).toBe(2);
  await expect(page.locator('.api-key-row').getByText('Forced new key', { exact: true })).toBeVisible();
  releaseOldList();
  await oldListSettled;
  await expect(page.locator('.api-key-row').getByText('Forced new key', { exact: true })).toBeVisible();
  await expect(page.locator('.api-key-row').getByText('Old response', { exact: true })).toHaveCount(0);
});

test('API key modals, pagination, cross-tab deletion fallback, and mobile layout are accessible', async ({ page, context }) => {
  const browserErrors: string[] = [];
  const stamp = Date.now();
  const email = `api-key-page-${stamp}@example.com`;
  await provisionAndSignInPasswordUser(page, email);
  page.on('pageerror', (error) => browserErrors.push(`pageerror: ${error.message}`));
  page.on('console', (message) => { if (message.type() === 'error') browserErrors.push(`console: ${message.text()}`); });
  page.on('response', (response) => { if (response.status() >= 500) browserErrors.push(`${response.status()} ${response.url()}`); });
  const ownedIds: string[] = [];
  const names = Array.from({ length: 22 }, (_, index) => `Paged ${stamp}-${index.toString().padStart(2, '0')}`);
  for (const name of names) {
    const response = await page.request.post('/api/auth/api-keys', { data: { name } });
    expect(response.ok()).toBe(true);
    ownedIds.push((await response.json()).api_key.id as string);
  }

  try {
    await page.setViewportSize({ width: 390, height: 844 });
    await page.goto('/api-keys');
    await expect(page.getByText('Page 1 of 2')).toBeVisible();
    await expect(page.getByText(names.at(-1)!, { exact: true })).toBeVisible();
    expect(await page.evaluate(() => document.documentElement.scrollWidth)).toBeLessThanOrEqual(390);

    await page.getByRole('button', { name: 'Create API key' }).click();
    const createDialog = page.getByRole('dialog', { name: 'Create API key' });
    await expect(createDialog.getByLabel('Name', { exact: true })).toBeFocused();
    await page.keyboard.press('Shift+Tab');
    await expect(createDialog.locator('header').getByRole('button', { name: 'Close', exact: true })).toBeFocused();
    await page.keyboard.press('Shift+Tab');
    await expect(createDialog.getByRole('button', { name: 'Create key', exact: true })).toBeFocused();
    await page.keyboard.press('Escape');
    await expect(createDialog).toHaveCount(0);
    await expect(page.getByRole('button', { name: 'Create API key' })).toBeFocused();

    await page.getByRole('button', { name: 'Next' }).click();
    await expect(page.getByText('Page 2 of 2')).toBeVisible();
    const rows = page.locator('.api-key-row');
    await expect(rows).toHaveCount(2);
    await expect(rows).toContainText([names[1], names[0]]);
    const peer = await context.newPage();
    expect((await peer.request.delete(`/api/auth/api-keys/${ownedIds[0]}`)).status()).toBe(204);
    await peer.close();
    page.once('dialog', (dialog) => dialog.accept());
    const currentDelete = page.locator('.api-key-row', { hasText: names[1] });
    const deleteResponse = page.waitForResponse((response) => response.request().method() === 'DELETE' && response.url().endsWith(ownedIds[1]));
    await currentDelete.getByRole('button', { name: 'Delete' }).click();
    expect((await deleteResponse).status()).toBe(204);
    await expect(page.getByText('Page 1 of 1')).toBeVisible();
    await expect(page.getByText(names[0], { exact: true })).toHaveCount(0);
    await expect(page.getByRole('button', { name: 'Previous' })).toBeDisabled();
    await expect(page.getByRole('button', { name: 'Next' })).toBeDisabled();
    expect(await page.evaluate(() => document.documentElement.scrollWidth)).toBeLessThanOrEqual(390);
    expect(browserErrors).toEqual([]);
  } finally {
    for (const id of ownedIds) await page.request.delete(`/api/auth/api-keys/${id}`);
  }
});

test('Local Password signs into the console', async ({ page }) => {
  await page.goto('/login');
  await selectLocalPasswordLogin(page);
  await page.getByLabel('Email').fill('admin@example.com');
  await page.getByLabel('Password').fill('admin123');
  await page.getByRole('button', { name: 'Sign in', exact: true }).click();
  await expect(page.getByText('admin@example.com')).toBeVisible();
  await expect(page).toHaveURL(/\/sessions$/);
  await expect(page.getByRole('heading', { name: 'Sessions', exact: true, level: 1 })).toBeVisible();
});

test('Embed JWT exchanges into a widget session through postMessage', async ({ page }) => {
  await page.goto('/login');
  await selectLocalPasswordLogin(page);
  await page.getByLabel('Email').fill('admin@example.com');
  await page.getByLabel('Password').fill('admin123');
  await page.getByRole('button', { name: 'Sign in', exact: true }).click();
  await expect(page.getByText('admin@example.com')).toBeVisible();

  const modelResponse = await page.request.post('/api/model-connections', { data: {
    scope: 'personal',
    name: `JWT Widget model ${Date.now()}-${test.info().workerIndex}`,
    base_url: 'http://fake-model-provider:8080',
    api_type: 'openai_responses',
    allowed_model_ids: ['hub-proxy-smoke'],
    api_key: 'dev-model-provider-api-key'
  } });
  expect(modelResponse.ok()).toBeTruthy();
  const model = await modelResponse.json() as { id: string };
  const modelSelection = { connection_id: model.id, model_id: 'hub-proxy-smoke' };
  const response = await page.request.post('/api/agents', {
    data: { name: `JWT Widget Agent ${Date.now()}`, instructions: 'Widget JWT exchange test.', visibility: 'private', model_selection: modelSelection }
  });
  expect(response.ok()).toBeTruthy();
  const agent = await response.json();
  const jwt = signEmbedJwt(agent.id, agent.owner_id);
  const secondResponse = await page.request.post('/api/agents', {
    data: { name: `Second Widget Agent ${Date.now()}`, instructions: 'Second widget session.', visibility: 'private', model_selection: modelSelection }
  });
  expect(secondResponse.ok()).toBeTruthy();
  const secondAgent = await secondResponse.json();
  const secondSessionResponse = await page.request.post('/api/embed/sessions', { data: { agent_id: secondAgent.id } });
  expect(secondSessionResponse.ok()).toBeTruthy();
  const secondSession = await secondSessionResponse.json();

  await page.setContent('<div id="widget-host"></div>');
  let widgetSessionLoads = 0;
  await page.route('**/api/widget/session', async (route) => {
    widgetSessionLoads += 1;
    await route.continue();
  });
  await page.evaluate(() => {
    (window as unknown as { widgetMessages: Record<string, unknown>[] }).widgetMessages = [];
    window.addEventListener('message', (event) => {
      const store = (window as unknown as { widgetMessages: Record<string, unknown>[] }).widgetMessages;
      if (event.data?.type?.startsWith('agent-hub:')) store.push(event.data);
    });
    const iframe = document.createElement('iframe');
    iframe.title = 'widget';
    iframe.src = '/widget';
    iframe.style.cssText = 'width:420px;height:520px';
    document.querySelector('#widget-host')?.appendChild(iframe);
  });
  const frame = page.frameLocator('iframe[title="widget"]');
  await expect(frame.getByText('Agent Widget')).toBeVisible();
  const channelId = await expect.poll(() => page.evaluate(() => {
    const messages = (window as unknown as { widgetMessages: Record<string, unknown>[] }).widgetMessages;
    return messages.find((message) => message.type === 'agent-hub:ready')?.channelId as string | undefined;
  })).toBeTruthy().then(() => page.evaluate(() => {
    const messages = (window as unknown as { widgetMessages: Record<string, unknown>[] }).widgetMessages;
    return messages.find((message) => message.type === 'agent-hub:ready')?.channelId as string;
  }));
  await page.locator('iframe[title="widget"]').evaluate((iframe) => {
    (iframe as HTMLIFrameElement).contentWindow?.postMessage({ type: 'agent-hub:init', channelId: 'wrong-channel' }, '*');
  });
  await expect(frame.getByText(agent.name)).toHaveCount(0);
  const exchangedSessionResponsePromise = page.waitForResponse((response) => response.request().method() === 'POST'
    && new URL(response.url()).pathname === '/api/embed/exchange');
  await page.locator('iframe[title="widget"]').evaluate((iframe, data) => {
    (iframe as HTMLIFrameElement).contentWindow?.postMessage({ type: 'agent-hub:init', channelId: data.channelId, jwt: data.jwt }, '*');
  }, { channelId, jwt });

  const exchangedSessionResponse = await exchangedSessionResponsePromise;
  const exchangedSession = await exchangedSessionResponse.json() as { token: string };
  await expect(frame.getByText(agent.name)).toBeVisible();
  const loadedWidgetSessions = widgetSessionLoads;
  await frame.getByLabel('Language').selectOption('zh-CN');
  await expect(frame.getByRole('button', { name: '发送' })).toBeVisible();
  expect(widgetSessionLoads).toBe(loadedWidgetSessions);
  await frame.getByLabel('语言').selectOption('en');
  let widgetRunPosts = 0;
  let releaseWidgetRun!: () => void;
  const heldWidgetRun = new Promise<void>((resolve) => { releaseWidgetRun = resolve; });
  await page.route('**/api/client/runs', async (route) => {
    widgetRunPosts += 1;
    await heldWidgetRun;
    await route.continue();
  });
  const widgetRunResponsePromise = page.waitForResponse((response) => response.request().method() === 'POST'
    && new URL(response.url()).pathname === '/api/client/runs');
  await frame.getByLabel('Message').fill('Draft survives failed credential exchange');
  await page.locator('iframe[title="widget"]').evaluate((iframe, channel) => {
    (iframe as HTMLIFrameElement).contentWindow?.postMessage({ type: 'agent-hub:message-submit', channelId: channel, message: 'Host submitted widget message' }, '*');
  }, channelId);
  await expect.poll(() => widgetRunPosts).toBe(1);
  await expect(frame.getByRole('button', { name: 'Sending...' })).toBeDisabled();
  const failedExchangeResponse = page.waitForResponse((response) => response.request().method() === 'POST'
    && new URL(response.url()).pathname === '/api/embed/exchange');
  await page.locator('iframe[title="widget"]').evaluate((iframe, channel) => {
    (iframe as HTMLIFrameElement).contentWindow?.postMessage({ type: 'agent-hub:embed-jwt', channelId: channel, jwt: 'not-a-valid-embed-jwt' }, '*');
  }, channelId);
  expect((await failedExchangeResponse).status()).toBe(401);
  await expect(frame.getByLabel('Message')).toHaveValue('Draft survives failed credential exchange');
  await expect(frame.getByRole('button', { name: 'Sending...' })).toBeDisabled();
  await page.locator('iframe[title="widget"]').evaluate((iframe, data) => {
    (iframe as HTMLIFrameElement).contentWindow?.postMessage({ type: 'agent-hub:session-select', channelId: data.channelId, token: data.token }, '*');
    (iframe as HTMLIFrameElement).contentWindow?.postMessage({ type: 'agent-hub:message-submit', channelId: data.channelId, message: 'Duplicate host submission' }, '*');
  }, { channelId, token: exchangedSession.token });
  await expect(frame.getByRole('button', { name: 'Sending...' })).toBeDisabled();
  releaseWidgetRun();
  const widgetRunResponse = await widgetRunResponsePromise;
  expect(widgetRunResponse.ok(), await widgetRunResponse.text()).toBeTruthy();
  await expect(frame.getByText('completed run')).toBeVisible({ timeout: 30_000 });
  expect(widgetRunPosts).toBe(1);
  await expect(frame.getByRole('button', { name: 'Send' })).toBeEnabled();
  await expect.poll(() => page.evaluate(() => (window as unknown as { widgetMessages: Record<string, unknown>[] }).widgetMessages.map((message) => message.type))).toContain('agent-hub:run-started');
  await expect.poll(() => page.evaluate(() => (window as unknown as { widgetMessages: Record<string, unknown>[] }).widgetMessages.map((message) => message.type))).toContain('agent-hub:run-event');
  await expect.poll(() => page.evaluate(() => {
    const messages = (window as unknown as { widgetMessages: Record<string, unknown>[] }).widgetMessages;
    return messages.some((message) => message.type === 'agent-hub:resize' && Number(message.width) > 0 && Number(message.height) > 0);
  })).toBeTruthy();

  await page.locator('iframe[title="widget"]').evaluate((iframe, data) => {
    (iframe as HTMLIFrameElement).contentWindow?.postMessage({ type: 'agent-hub:session-select', channelId: data.channelId, token: data.token }, '*');
  }, { channelId, token: secondSession.token });
  await expect(frame.getByText(secondAgent.name)).toBeVisible();
  await expect(frame.getByLabel('Message')).toHaveValue('');
  await expect(frame.getByText('completed run')).toHaveCount(0);
  await page.request.delete(`/api/agents/${agent.id}`);
  await page.request.delete(`/api/agents/${secondAgent.id}`);
  await page.request.delete(`/api/model-connections/${model.id}`);
});

test('Auth boundary checks reject polluted or unauthorized embed flows', async ({ baseURL }) => {
  const api = await request.newContext({ baseURL });

  const badJwt = await api.post('/api/embed/exchange', { data: { jwt: 'not-a-jwt' } });
  expect(badJwt.status()).toBe(401);

  const login = await api.post('/api/auth/login', {
    data: { email: 'admin@example.com', password: 'admin123' }
  });
  expect(login.ok()).toBeTruthy();
  const created = await api.post('/api/agents', {
    data: { name: `JWT Boundary Agent ${Date.now()}`, instructions: 'Boundary checks.', visibility: 'private' }
  });
  expect(created.ok()).toBeTruthy();
  const agent = await created.json();
  const publicCreated = await api.post('/api/agents', {
    data: { name: `Public Boundary Agent ${Date.now()}`, instructions: 'Public visibility only.', visibility: 'public' }
  });
  expect(publicCreated.ok()).toBeTruthy();
  const publicAgent = await publicCreated.json();
  const memberEmail = `embed-boundary-${Date.now()}@example.com`;
  const memberPassword = 'embed-boundary-password';
  const memberCreated = await api.post('/api/admin/users', {
    data: { email: memberEmail, password: memberPassword, role: 'member' }
  });
  expect(memberCreated.ok()).toBeTruthy();
  const memberDetail = await memberCreated.json() as { user: { id: string } };
  const member = await request.newContext({ baseURL });
  expect((await member.post('/api/auth/login', {
    data: { email: memberEmail, password: memberPassword }
  })).ok()).toBeTruthy();

  const wrongOwner = await api.post('/api/embed/exchange', {
    data: { jwt: signEmbedJwt(agent.id, randomUUID()) }
  });
  expect(wrongOwner.status()).toBe(404);

  const missingIat = await api.post('/api/embed/exchange', {
    data: { jwt: signEmbedJwt(agent.id, agent.owner_id, { iat: undefined, jti: randomUUID() }) }
  });
  expect(missingIat.status()).toBe(401);

  const expired = await api.post('/api/embed/exchange', {
    data: { jwt: signEmbedJwt(agent.id, agent.owner_id, { exp: Math.floor(Date.now() / 1000) - 1, iat: Math.floor(Date.now() / 1000) - 60, jti: randomUUID() }) }
  });
  expect(expired.status()).toBe(401);

  const validJwt = signEmbedJwt(agent.id, agent.owner_id, { jti: randomUUID() });
  const badSignature = await api.post('/api/embed/exchange', { data: { jwt: `${validJwt.slice(0, -1)}x` } });
  expect(badSignature.status()).toBe(401);

  const exchange = await api.post('/api/embed/exchange', {
    data: { jwt: signEmbedJwt(agent.id, agent.owner_id, { jti: randomUUID() }) }
  });
  expect(exchange.ok()).toBeTruthy();
  const { token } = await exchange.json();
  const widget = await api.get('/api/widget/session', { headers: { 'X-Agent-Hub-Embed-Token': token } });
  expect(widget.ok()).toBeTruthy();
  expect(Object.keys(await widget.json()).sort()).toEqual(['id', 'instructions', 'name']);

  const isolatedSession = await api.post('/api/embed/sessions', { data: { agent_id: agent.id } });
  expect(isolatedSession.ok()).toBeTruthy();
  const isolatedToken = (await isolatedSession.json()).token as string;
  const firstWidgetRun = await api.post('/api/widget/runs', {
    headers: { 'X-Agent-Hub-Embed-Token': token },
    data: { message: 'Session-scoped widget run', parent_run_id: null }
  });
  expect(firstWidgetRun.ok()).toBeTruthy();
  const firstWidgetRunBody = await firstWidgetRun.json();
  const secondWidgetRun = await api.post('/api/widget/runs', {
    headers: { 'X-Agent-Hub-Embed-Token': token },
    data: { message: 'Message while the previous run may still be active', parent_run_id: null }
  });
  expect(secondWidgetRun.ok()).toBeTruthy();
  const secondWidgetRunBody = await secondWidgetRun.json();
  const widgetHubSessionId = secondWidgetRunBody.hub_session_id;
  expect(widgetHubSessionId).toEqual(expect.stringMatching(
    /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i
  ));
  await expect.poll(async () => {
    const current = await api.get(`/api/runs/${secondWidgetRunBody.id}`);
    return current.ok() ? (await current.json()).status : '';
  }, { timeout: 30_000 }).toBe('completed');
  const resumedWidgetRun = await api.post('/api/widget/runs', {
    headers: { 'X-Agent-Hub-Embed-Token': token },
    data: {
      message: 'Resume the completed widget session',
      hub_session_id: widgetHubSessionId,
      parent_run_id: null
    }
  });
  expect(resumedWidgetRun.ok()).toBeTruthy();
  expect((await resumedWidgetRun.json()).hub_session_id).toBe(widgetHubSessionId);
  const crossSessionStream = await api.get(`/api/runs/${firstWidgetRunBody.id}/events/stream`, {
    headers: { 'X-Agent-Hub-Embed-Token': isolatedToken }
  });
  expect(crossSessionStream.status()).toBe(403);
  expect((await member.post('/api/embed/sessions', { data: { agent_id: publicAgent.id } })).status()).toBe(404);

  await api.delete(`/api/agents/${agent.id}`);
  await api.delete(`/api/agents/${publicAgent.id}`);
  expect((await api.post(`/api/admin/users/${memberDetail.user.id}/erase`, {
    data: { email: memberEmail }
  })).status()).toBe(202);
  await member.dispose();
  await api.dispose();
});
