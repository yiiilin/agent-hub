import { expect, test, type Page, type Route } from '@playwright/test';

const superAdmin = {
  id: 'user-admin',
  username: 'admin',
  email: 'admin@example.com',
  display_name: 'Admin',
  role: 'super_admin'
};

const alice = {
  id: 'user-alice',
  username: 'alice',
  email: 'alice@example.com',
  display_name: 'Alice',
  role: 'member'
};

type RequestState = {
  counts: Record<string, number>;
  bodies: Record<string, unknown[]>;
};

async function installAdministrationApi(page: Page) {
  const state: RequestState = { counts: {}, bodies: {} };
  const count = (key: string) => { state.counts[key] = (state.counts[key] ?? 0) + 1; };
  const record = async (route: Route, key: string) => {
    const body = route.request().postDataJSON();
    (state.bodies[key] ??= []).push(body);
    return body;
  };

  let policy = {
    password_registration_enabled: true,
    password_login_enabled: true,
    email_verification_required: false
  };
  let platforms = [{ id: 'platform-github', key: 'github', name: 'GitHub' }];
  const channels: Record<string, Array<{
    id: string;
    platform_id: string;
    key: string;
    name: string;
    enabled: boolean;
    trusted_email: boolean;
  }>> = {
    'platform-github': [{
      id: 'channel-oauth',
      platform_id: 'platform-github',
      key: 'oauth',
      name: 'OAuth',
      enabled: true,
      trusted_email: true
    }]
  };
  let users = [
    { user: superAdmin, email_verified: true, has_password: true, created_at: '2026-07-10T08:00:00.000Z' },
    { user: alice, email_verified: false, has_password: false, created_at: '2026-07-11T09:00:00.000Z' }
  ];
  const erasures: Array<{
    user_id: string;
    username: string | null;
    status: string;
    requested_at: string;
    completed_at: string | null;
  }> = [];
  await page.route('**/api/auth/me', (route) => route.fulfill({ json: superAdmin }));
  await page.route('**/api/admin/auth-policy', async (route) => {
    count('policy');
    if (route.request().method() === 'PATCH') policy = await record(route, 'policy') as typeof policy;
    return route.fulfill({ json: policy });
  });
  await page.route('**/api/admin/external-platforms', async (route) => {
    count('platforms');
    if (route.request().method() === 'POST') {
      const body = await record(route, 'platformCreate') as { key: string; name: string };
      const created = { id: `platform-${body.key}`, ...body };
      platforms = [...platforms, created];
      channels[created.id] = [];
      return route.fulfill({ status: 201, json: created });
    }
    return route.fulfill({ json: platforms });
  });
  await page.route(/\/api\/admin\/external-platforms\/[^/]+$/, async (route) => {
    const platformId = new URL(route.request().url()).pathname.split('/').at(-1)!;
    const body = await record(route, 'platformUpdate') as { name: string };
    platforms = platforms.map((platform) => platform.id === platformId ? { ...platform, name: body.name } : platform);
    return route.fulfill({ json: platforms.find((platform) => platform.id === platformId) });
  });
  await page.route(/\/api\/admin\/external-platforms\/[^/]+\/authentication-channels$/, async (route) => {
    const platformId = new URL(route.request().url()).pathname.split('/')[4];
    count('channels');
    if (route.request().method() === 'POST') {
      const body = await record(route, 'channelCreate') as Omit<(typeof channels)[string][number], 'id' | 'platform_id'>;
      const created = { id: `channel-${body.key}`, platform_id: platformId, ...body };
      channels[platformId] = [...(channels[platformId] ?? []), created];
      return route.fulfill({ status: 201, json: created });
    }
    return route.fulfill({ json: channels[platformId] ?? [] });
  });
  await page.route(/\/api\/admin\/authentication-channels\/[^/]+$/, async (route) => {
    const channelId = new URL(route.request().url()).pathname.split('/').at(-1)!;
    const body = await record(route, 'channelUpdate') as Pick<(typeof channels)[string][number], 'name' | 'enabled' | 'trusted_email'>;
    let updated: (typeof channels)[string][number] | undefined;
    for (const [platformId, items] of Object.entries(channels)) {
      channels[platformId] = items.map((channel) => {
        if (channel.id !== channelId) return channel;
        updated = { ...channel, ...body };
        return updated;
      });
    }
    return route.fulfill({ json: updated });
  });
  await page.route('**/api/admin/users', (route) => {
    count('users');
    return route.fulfill({ json: users });
  });
  await page.route(/\/api\/admin\/users\/[^/]+$/, (route) => {
    const userId = new URL(route.request().url()).pathname.split('/').at(-1)!;
    count('userDetail');
    return route.fulfill({ json: users.find((detail) => detail.user.id === userId) });
  });
  await page.route(/\/api\/admin\/users\/[^/]+\/password$/, async (route) => {
    const userId = new URL(route.request().url()).pathname.split('/')[4];
    await record(route, 'password');
    users = users.map((detail) => detail.user.id === userId ? { ...detail, has_password: true } : detail);
    return route.fulfill({ json: users.find((detail) => detail.user.id === userId) });
  });
  await page.route(/\/api\/admin\/users\/[^/]+\/erase$/, async (route) => {
    const userId = new URL(route.request().url()).pathname.split('/')[4];
    await record(route, 'erase');
    const erasedUser = users.find((detail) => detail.user.id === userId)!.user;
    users = users.filter((detail) => detail.user.id !== userId);
    const erasure = {
      user_id: userId,
      username: erasedUser.username,
      status: 'completed',
      requested_at: '2026-07-17T08:00:00.000Z',
      completed_at: '2026-07-17T08:00:00.000Z'
    };
    erasures.unshift(erasure);
    return route.fulfill({ status: 202, json: erasure });
  });
  await page.route('**/api/admin/user-erasures', (route) => {
    count('erasures');
    return route.fulfill({ json: erasures });
  });
  return state;
}

test('Administration tabs render one lazy-loaded workflow at a time', async ({ page }) => {
  const state = await installAdministrationApi(page);
  await page.goto('/administration');

  const tablist = page.getByRole('tablist', { name: 'Administration' });
  await expect(tablist.getByRole('tab')).toHaveCount(3);
  await expect(tablist.getByRole('tab', { name: 'Authentication' })).toHaveAttribute('aria-selected', 'true');
  await expect(page.getByRole('heading', { name: 'Authentication policy' })).toBeVisible();
  await expect(page.getByRole('heading', { name: 'External platforms' })).toHaveCount(0);
  expect(state.counts).toMatchObject({ policy: 1 });
  expect(state.counts.platforms ?? 0).toBe(0);
  expect(state.counts.users ?? 0).toBe(0);
  await tablist.getByRole('tab', { name: 'User management' }).click();
  await expect(page.getByRole('heading', { name: 'User management' })).toBeVisible();
  await expect(page.getByRole('heading', { name: 'Authentication policy' })).toHaveCount(0);
  expect(state.counts.users).toBe(1);
  expect(state.counts.erasures).toBe(1);
});

test('External platforms use table-backed create and edit dialogs with immutable keys and nested channel forms', async ({ page }) => {
  const state = await installAdministrationApi(page);
  await page.goto('/administration');
  await page.getByRole('tab', { name: 'External platforms' }).click();

  const table = page.getByRole('table', { name: 'External platforms' });
  await expect(table).toBeVisible();
  await page.getByRole('button', { name: 'Add platform' }).click();
  const createDialog = page.getByRole('dialog', { name: 'Add platform' });
  await createDialog.getByLabel('Platform key').fill('slack');
  await createDialog.getByLabel('Platform name').fill('Slack');
  await createDialog.getByRole('button', { name: 'Add platform' }).click();
  await expect(table.getByRole('row', { name: /Slack slack/ })).toBeVisible();
  expect(state.bodies.platformCreate).toEqual([{ key: 'slack', name: 'Slack' }]);

  await table.getByRole('button', { name: 'Edit external platform: GitHub' }).click();
  const editDialog = page.getByRole('dialog', { name: 'Edit external platform' });
  await expect(editDialog.getByLabel('Platform key')).toBeDisabled();
  await editDialog.getByLabel('Platform name').fill('GitHub Enterprise');

  await expect(editDialog.getByRole('button', { name: /OAuth oauth/ })).toBeVisible();
  await expect(editDialog.getByLabel('Channel key').first()).toBeDisabled();
  await editDialog.getByLabel('Channel name', { exact: true }).fill('Company OAuth');
  await editDialog.getByLabel('Trusted email').uncheck();
  await editDialog.getByRole('button', { name: 'Save channel' }).click();
  expect(state.bodies.channelUpdate).toEqual([{ name: 'Company OAuth', enabled: true, trusted_email: false }]);

  await editDialog.getByLabel('Channel key').last().fill('saml');
  await editDialog.getByLabel('New channel name').fill('SAML SSO');
  await editDialog.getByRole('button', { name: 'Add channel' }).click();
  await expect(editDialog.getByRole('button', { name: /SAML SSO saml/ })).toBeVisible();
  expect(state.bodies.channelCreate).toEqual([{ key: 'saml', name: 'SAML SSO', enabled: true, trusted_email: true }]);

  await editDialog.getByRole('button', { name: 'Save changes' }).click();
  await expect(table.getByRole('row', { name: /GitHub Enterprise github/ })).toBeVisible();
  expect(state.bodies.platformUpdate).toEqual([{ name: 'GitHub Enterprise' }]);
});

test('User management uses separate details, password, and irreversible deletion dialogs', async ({ page }) => {
  const state = await installAdministrationApi(page);
  await page.goto('/administration');
  await page.getByRole('tab', { name: 'User management' }).click();

  const table = page.getByRole('table', { name: 'User management' });
  const currentUserRow = table.getByRole('row', { name: /admin/ });
  const aliceRow = table.getByRole('row', { name: /alice/ });
  await expect(currentUserRow.getByRole('button', { name: 'Delete user: admin' })).toBeDisabled();

  await aliceRow.getByRole('button', { name: 'User information: alice' }).click();
  const detailsDialog = page.getByRole('dialog', { name: 'User information' });
  await expect(detailsDialog).toContainText('Alice');
  await expect(detailsDialog).toContainText('alice@example.com');
  expect(state.counts.userDetail).toBe(1);
  await detailsDialog.locator('.modal-actions').getByRole('button', { name: 'Close' }).click();

  await aliceRow.getByRole('button', { name: 'Set user password: alice' }).click();
  const passwordDialog = page.getByRole('dialog', { name: 'Set user password' });
  await passwordDialog.getByLabel('Password').fill('new-password');
  await passwordDialog.getByRole('button', { name: 'Save changes' }).click();
  await expect(page.getByText('Changes saved', { exact: true })).toBeVisible();
  expect(state.bodies.password).toEqual([{ password: 'new-password' }]);

  await aliceRow.getByRole('button', { name: 'Delete user: alice' }).click();
  const eraseDialog = page.getByRole('dialog', { name: 'Delete user' });
  const eraseAction = eraseDialog.getByRole('button', { name: 'Delete user' });
  await expect(eraseAction).toBeDisabled();
  await eraseDialog.getByLabel('Confirm username').fill('alice');
  await expect(eraseAction).toBeEnabled();
  await eraseAction.click();
  await expect(table.getByRole('row', { name: /alice/ })).toHaveCount(0);
  await expect(page.getByText('completed', { exact: true })).toBeVisible();
  expect(state.bodies.erase).toEqual([{ username: 'alice' }]);
});

test('Administration remains operable at 390px without page overflow or browser failures', async ({ page }) => {
  const pageErrors: string[] = [];
  const consoleErrors: string[] = [];
  const requestFailures: string[] = [];
  page.on('pageerror', (error) => pageErrors.push(error.message));
  page.on('console', (message) => { if (message.type() === 'error') consoleErrors.push(message.text()); });
  page.on('requestfailed', (request) => requestFailures.push(`${request.method()} ${new URL(request.url()).pathname}`));
  await page.setViewportSize({ width: 390, height: 844 });
  await installAdministrationApi(page);
  await page.goto('/administration');

  await expect(page.getByRole('heading', { name: 'Administration' })).toBeVisible();
  await page.getByRole('tab', { name: 'External platforms' }).click();
  await page.getByRole('button', { name: 'Edit external platform: GitHub' }).click();
  const dialog = page.getByRole('dialog', { name: 'Edit external platform' });
  await expect(dialog).toBeVisible();
  await expect(dialog.getByLabel('Platform key')).toBeDisabled();
  expect(await page.evaluate(() => document.documentElement.scrollWidth)).toBeLessThanOrEqual(390);
  await dialog.getByRole('button', { name: 'Close' }).click();
  await page.getByRole('tab', { name: 'User management' }).click();
  await expect(page.getByRole('table', { name: 'User management' })).toBeVisible();
  expect(await page.evaluate(() => document.documentElement.scrollWidth)).toBeLessThanOrEqual(390);
  expect(pageErrors).toEqual([]);
  expect(consoleErrors).toEqual([]);
  expect(requestFailures).toEqual([]);
});
