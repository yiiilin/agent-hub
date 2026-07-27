import { expect, test, type Page, type Route } from '@playwright/test';

const ownerId = '10000000-0000-4000-8000-000000000001';
const platformOne = '20000000-0000-4000-8000-000000000001';
const platformTwo = '20000000-0000-4000-8000-000000000002';
const channelOne = '30000000-0000-4000-8000-000000000001';
const channelTwo = '30000000-0000-4000-8000-000000000002';
const alphaAgent = '40000000-0000-4000-8000-000000000001';
const betaAgent = '40000000-0000-4000-8000-000000000002';
const appId = '50000000-0000-4000-8000-000000000001';
const createdAppId = '50000000-0000-4000-8000-000000000002';
const now = '2026-07-17T08:00:00.000Z';

const currentUser = {
  id: ownerId,
  username: 'integration-owner',
  email: 'integration-owner@example.com',
  display_name: 'Integration owner',
  role: 'member'
};

const platforms = [
  { id: platformOne, key: 'github', name: 'GitHub' },
  { id: platformTwo, key: 'slack', name: 'Slack' }
];

const channels = [
  { id: channelOne, platform_id: platformOne, key: 'oauth', name: 'GitHub OAuth', enabled: true, trusted_email: true },
  { id: channelTwo, platform_id: platformTwo, key: 'oidc', name: 'Slack OIDC', enabled: true, trusted_email: true }
];

const agents = [
  {
    id: alphaAgent, name: 'Alpha Agent', instructions: 'Alpha', visibility: 'private', public_to: [], runtime_id: null,
    owner_id: ownerId, is_owner: true, can_manage: true, can_administer: true, can_invoke: true,
    model_policy: {}, sandbox_policy: {}, managed_skill_ids: [], mcp_allowlist: [], created_at: now, updated_at: now
  },
  {
    id: betaAgent, name: 'Beta Agent', instructions: 'Beta', visibility: 'public', public_to: [], runtime_id: null,
    owner_id: '10000000-0000-4000-8000-000000000099', is_owner: false, can_manage: false, can_administer: false, can_invoke: true,
    model_policy: {}, sandbox_policy: {}, managed_skill_ids: [], mcp_allowlist: [], created_at: now, updated_at: now
  }
];

type IntegrationAppFixture = {
  id: string;
  owner_id: string;
  name: string;
  client_id: string;
  external_platform_id: string;
  authentication_channel_id: string;
  redirect_uris: string[];
  agent_ids: string[];
  widget_history_enabled: boolean;
  login_required: boolean;
  allowed_origins: string[];
  tool_allowlist: string[] | null;
  created_at: string;
  updated_at: string;
};

const initialApp: IntegrationAppFixture = {
  id: appId,
  owner_id: ownerId,
  name: 'Existing App',
  client_id: 'ahc_existing',
  external_platform_id: platformOne,
  authentication_channel_id: channelOne,
  redirect_uris: ['https://existing.example.com/callback'],
  agent_ids: [alphaAgent],
  widget_history_enabled: false,
  login_required: true,
  allowed_origins: [],
  tool_allowlist: null,
  created_at: now,
  updated_at: now
};

async function installIntegrationApi(page: Page, { role }: { role?: 'member' | 'admin' | 'super_admin' } = {}) {
  const currentRole = role ?? 'member';
  let apps = [{ ...initialApp }];
  let createBody: Record<string, unknown> | null = null;
  let updateBody: Record<string, unknown> | null = null;
  let rotateCount = 0;
  const widgetRequests: string[] = [];

  await page.route('**/api/**', async (route: Route) => {
    const request = route.request();
    const path = new URL(request.url()).pathname;
    if (!path.startsWith('/api/')) return route.continue();
    if (path === '/api/auth/me') return route.fulfill({ json: { ...currentUser, role: currentRole } });
    if (path === '/api/integration-app-options') {
      return route.fulfill({ json: { external_platforms: platforms, authentication_channels: channels } });
    }
    if (path === '/api/agents') return route.fulfill({ json: agents });
    if (path === '/api/integration-apps' && request.method() === 'GET') return route.fulfill({ json: apps });
    if (path === '/api/integration-apps' && request.method() === 'POST') {
      createBody = request.postDataJSON() as Record<string, unknown>;
      const created = {
        id: createdAppId,
        owner_id: ownerId,
        client_id: 'ahc_created',
        created_at: now,
        updated_at: now,
        ...(createBody as Omit<IntegrationAppFixture, 'id' | 'owner_id' | 'client_id' | 'created_at' | 'updated_at'>)
      };
      apps = [...apps, created];
      return route.fulfill({ json: { integration_app: created, client_secret: 'ahs_created_once' } });
    }
    if (path === `/api/integration-apps/${appId}` && request.method() === 'PATCH') {
      updateBody = request.postDataJSON() as Record<string, unknown>;
      apps = apps.map((app) => app.id === appId ? { ...app, ...updateBody, updated_at: '2026-07-17T09:00:00.000Z' } : app);
      return route.fulfill({ json: apps.find((app) => app.id === appId) });
    }
    if (path === `/api/integration-apps/${appId}/rotate-secret` && request.method() === 'POST') {
      rotateCount += 1;
      return route.fulfill({ json: { integration_app: apps.find((app) => app.id === appId), client_secret: 'ahs_rotated_once' } });
    }
    const widgetMatch = path.match(/^\/api\/integration-apps\/([^/]+)\/agents\/([^/]+)\/widget-session$/);
    if (widgetMatch && request.method() === 'POST') {
      widgetRequests.push(widgetMatch[2]);
      return route.fulfill({ json: { token: `ahe_${widgetMatch[2]}` } });
    }
    return route.fulfill({ status: 404, json: { error: `Unhandled route ${request.method()} ${path}` } });
  });

  return {
    createBody: () => createBody,
    updateBody: () => updateBody,
    rotateCount: () => rotateCount,
    widgetRequests: () => widgetRequests
  };
}

test('Integration Apps are a first-level table workflow and create reveals the secret once', async ({ page }) => {
  const fixture = await installIntegrationApi(page);
  await page.goto('/integrations');

  await expect(page.getByRole('button', { name: 'Integration Apps', exact: true })).toHaveAttribute('aria-current', 'page');
  const table = page.getByRole('table', { name: 'Integration App list' });
  await expect(table.getByRole('columnheader')).toHaveText(['Name', 'Client ID', 'Platform / channel', 'Agents', 'Updated', 'Actions']);
  await expect(table.getByRole('row').filter({ hasText: 'Existing App' })).toContainText('GitHub OAuth');

  await page.getByRole('button', { name: 'Create Integration App' }).click();
  const dialog = page.getByRole('dialog', { name: 'Create Integration App' });
  await expect(dialog.getByRole('textbox', { name: 'Name', exact: true })).toBeFocused();
  await dialog.getByRole('textbox', { name: 'Name', exact: true }).fill('Created App');
  await dialog.getByRole('combobox', { name: 'External platform' }).selectOption(platformTwo);
  await expect(dialog.getByRole('combobox', { name: 'Authentication channel' }).locator('option')).toHaveText(['Slack OIDC']);
  await dialog.getByRole('textbox', { name: 'Redirect URI 1' }).fill('https://created.example.com/callback');
  await dialog.getByRole('button', { name: 'Add redirect URI' }).click();
  await dialog.getByRole('textbox', { name: 'Redirect URI 2' }).fill('https://created.example.com/secondary');
  await dialog.getByRole('checkbox', { name: 'Delegate Alpha Agent' }).check();
  await dialog.getByRole('checkbox', { name: 'Delegate Beta Agent' }).check();
  await expect(dialog.getByRole('checkbox', { name: 'Allow anonymous public Widget' })).toHaveCount(0);
  await dialog.getByRole('checkbox', { name: "Further restrict this App's tools" }).check();
  await dialog.getByRole('checkbox', { name: 'read' }).check();
  await expect(dialog.getByRole('checkbox', { name: 'Enable Widget history' })).not.toBeChecked();
  await dialog.getByRole('button', { name: 'Create Integration App' }).click();

  expect(fixture.createBody()).toEqual({
    name: 'Created App',
    external_platform_id: platformTwo,
    authentication_channel_id: channelTwo,
    redirect_uris: ['https://created.example.com/callback', 'https://created.example.com/secondary'],
    agent_ids: [alphaAgent, betaAgent],
    widget_history_enabled: false,
    login_required: true,
    allowed_origins: [],
    tool_allowlist: ['read']
  });
  const secretDialog = page.getByRole('dialog', { name: 'Integration App secret' });
  await expect(secretDialog.getByText('ahs_created_once', { exact: true })).toBeVisible();
  await expect(secretDialog.getByRole('button', { name: 'Copy client secret' })).toBeVisible();
  await secretDialog.locator('.modal-actions').getByRole('button', { name: 'Close', exact: true }).click();
  await expect(table.getByRole('row').filter({ hasText: 'Created App' })).toContainText('2 agents');
});

test('editing keeps origin immutable and secret rotation has an explicit subform', async ({ page }) => {
  const fixture = await installIntegrationApi(page);
  await page.goto('/integrations');

  await page.getByRole('button', { name: 'Edit Existing App' }).click();
  const editDialog = page.getByRole('dialog', { name: 'Edit Integration App' });
  await expect(editDialog.getByText('GitHub', { exact: true })).toBeVisible();
  await expect(editDialog.getByText('GitHub OAuth', { exact: true })).toBeVisible();
  await expect(editDialog.getByRole('combobox', { name: 'External platform' })).toHaveCount(0);
  await editDialog.getByRole('textbox', { name: 'Name', exact: true }).fill('Renamed App');
  await editDialog.getByRole('textbox', { name: 'Redirect URI 1' }).fill('https://renamed.example.com/callback');
  await editDialog.getByRole('checkbox', { name: 'Delegate Beta Agent' }).check();
  await editDialog.getByRole('checkbox', { name: 'Enable Widget history' }).check();
  await editDialog.getByRole('button', { name: 'Save changes' }).click();
  expect(fixture.updateBody()).toEqual({
    name: 'Renamed App',
    redirect_uris: ['https://renamed.example.com/callback'],
    agent_ids: [alphaAgent, betaAgent],
    widget_history_enabled: true,
    login_required: true,
    allowed_origins: [],
    tool_allowlist: null
  });
  await expect(page.getByRole('table', { name: 'Integration App list' })).toContainText('Renamed App');

  await page.getByRole('button', { name: 'Rotate secret for Renamed App' }).click();
  const rotateDialog = page.getByRole('dialog', { name: 'Rotate client secret' });
  await rotateDialog.getByRole('button', { name: 'Rotate secret' }).click();
  expect(fixture.rotateCount()).toBe(1);
  await expect(page.getByRole('dialog', { name: 'Integration App secret' }).getByText('ahs_rotated_once', { exact: true })).toBeVisible();
  await expect(page.getByRole('button', { name: /Delete.*App/i })).toHaveCount(0);
});

test('each delegated Agent gets its own one-hour Widget link', async ({ page }) => {
  const fixture = await installIntegrationApi(page);
  await page.goto('/integrations');

  await page.getByRole('button', { name: 'Widget links for Existing App' }).click();
  const dialog = page.getByRole('dialog', { name: 'Widget links' });
  await dialog.getByRole('button', { name: 'Generate link for Alpha Agent' }).click();
  expect(fixture.widgetRequests()).toEqual([alphaAgent]);
  const link = dialog.getByRole('link', { name: 'Open Widget for Alpha Agent' });
  await expect(link).toHaveAttribute('href', new RegExp(`/widget#token=ahe_${alphaAgent}$`));
  await expect(link).not.toHaveAttribute('href', /\?token=/);
  await expect(dialog.getByRole('button', { name: 'Copy Widget link for Alpha Agent' })).toBeVisible();
});

test('admin configures an anonymous public Widget with one Agent, exact Origins, and narrowed tools', async ({ page }) => {
  const fixture = await installIntegrationApi(page, { role: 'admin' });
  await page.goto('/integrations');

  await page.getByRole('button', { name: 'Create Integration App' }).click();
  const dialog = page.getByRole('dialog', { name: 'Create Integration App' });
  await dialog.getByRole('textbox', { name: 'Name', exact: true }).fill('Public App');
  await dialog.getByRole('textbox', { name: 'Redirect URI 1' }).fill('https://public.example.com/callback');
  await dialog.getByRole('checkbox', { name: 'Allow anonymous public Widget' }).check();
  await dialog.getByRole('radio', { name: 'Delegate Alpha Agent' }).check();
  await dialog.getByRole('textbox', { name: 'Allowed Origin 1' }).fill('https://public.example.com');
  await dialog.getByRole('checkbox', { name: "Further restrict this App's tools" }).check();
  await dialog.getByRole('checkbox', { name: 'read' }).check();
  await dialog.getByRole('checkbox', { name: 'grep' }).check();
  await dialog.getByRole('button', { name: 'Create Integration App' }).click();

  expect(fixture.createBody()).toMatchObject({
    login_required: false,
    agent_ids: [alphaAgent],
    allowed_origins: ['https://public.example.com'],
    tool_allowlist: ['read', 'grep']
  });
  await page.getByRole('dialog', { name: 'Integration App secret' }).locator('.modal-actions').getByRole('button', { name: 'Close', exact: true }).click();
  await page.getByRole('button', { name: 'Widget links for Public App' }).click();
  const link = page.getByRole('link', { name: 'Open public Widget' });
  await expect(link).toHaveAttribute('href', new RegExp('/widget\\?app=ahc_created$'));
  await expect(link).not.toHaveAttribute('href', /token=/);
});

test('Integration Apps and its form fit a 390px Chinese viewport', async ({ page }) => {
  const pageErrors: string[] = [];
  const consoleErrors: string[] = [];
  page.on('pageerror', (error) => pageErrors.push(error.message));
  page.on('console', (message) => { if (message.type() === 'error') consoleErrors.push(message.text()); });
  await installIntegrationApi(page);
  await page.addInitScript(() => localStorage.setItem('agent-hub-language', 'zh-CN'));
  await page.setViewportSize({ width: 390, height: 844 });
  await page.goto('/integrations');

  await expect(page.getByRole('heading', { name: '集成应用' })).toBeVisible();
  await page.getByRole('button', { name: '新建集成应用' }).click();
  await expect(page.getByRole('dialog', { name: '新建集成应用' })).toBeVisible();
  const dimensions = await page.evaluate(() => ({ scrollWidth: document.documentElement.scrollWidth, innerWidth: window.innerWidth }));
  expect(dimensions.scrollWidth).toBeLessThanOrEqual(dimensions.innerWidth);
  expect(pageErrors).toEqual([]);
  expect(consoleErrors).toEqual([]);
});
