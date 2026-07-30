import { expect, test, type Page, type Route } from '@playwright/test';
import type { AdminUserDetail, LdapConfiguration, User, UserErasure } from '../src/api/client';

const superAdmin: User = {
  id: 'user-admin',
  email: 'admin@example.com',
  display_name: 'Admin',
  role: 'super_admin'
};

const alice: User = {
  id: 'user-alice',
  email: 'alice@example.com',
  display_name: 'Alice',
  role: 'member'
};

const savedLdapConfiguration: LdapConfiguration = {
  url: 'ldap://directory.example.com:389',
  security: 'starttls',
  base_dn: 'ou=people,dc=example,dc=com',
  bind_identity_template: '{email}',
  user_filter: '(userPrincipalName={email})',
  email_attribute: 'mail',
  display_name_attribute: 'displayName',
  allow_insecure: false,
  skip_tls_verify: false
};

type RequestState = {
  counts: Record<string, number>;
  bodies: Record<string, unknown[]>;
  releaseLdapTest: () => void;
  failNextLdapTest: () => void;
};

async function installAdministrationApi(
  page: Page,
  options: { currentUser?: User; ldapConfiguration?: LdapConfiguration | null; delayLdapTest?: boolean } = {}
) {
  const currentUser = options.currentUser ?? superAdmin;
  const state: RequestState = {
    counts: {},
    bodies: {},
    releaseLdapTest: () => undefined,
    failNextLdapTest: () => undefined
  };
  const count = (key: string) => { state.counts[key] = (state.counts[key] ?? 0) + 1; };
  const record = async (route: Route, key: string) => {
    const body = route.request().postDataJSON();
    (state.bodies[key] ??= []).push(body);
    return body;
  };

  let policy = {
    password_registration_enabled: true,
    password_login_enabled: true,
    ldap_login_enabled: false
  };
  let ldapConfiguration = options.ldapConfiguration === undefined ? savedLdapConfiguration : options.ldapConfiguration;
  let failLdapTest = false;
  let releaseLdapTest!: () => void;
  const ldapTestGate = new Promise<void>((resolve) => { releaseLdapTest = resolve; });
  state.releaseLdapTest = releaseLdapTest;
  state.failNextLdapTest = () => { failLdapTest = true; };
  let users: AdminUserDetail[] = [
    { user: superAdmin, has_password: true, created_at: '2026-07-10T08:00:00.000Z' },
    { user: alice, has_password: false, created_at: '2026-07-11T09:00:00.000Z' }
  ].filter((detail) => currentUser.role === 'super_admin' || detail.user.role !== 'super_admin');
  const erasures: UserErasure[] = [];
  let platforms = [{ id: 'platform-github', key: 'github', name: 'GitHub' }];
  const channels = [{
    id: 'channel-oauth',
    platform_id: 'platform-github',
    key: 'oauth',
    name: 'OAuth',
    enabled: true,
    trusted_email: true
  }];

  await page.route('**/api/auth/me', (route) => route.fulfill({ json: currentUser }));
  await page.route('**/api/admin/auth-policy', async (route) => {
    count('policy');
    if (route.request().method() === 'PATCH') policy = await record(route, 'policy') as typeof policy;
    return route.fulfill({ json: policy });
  });
  await page.route('**/api/admin/ldap-config/test', async (route) => {
    count('ldapTest');
    await record(route, 'ldapTest');
    if (options.delayLdapTest) await ldapTestGate;
    if (failLdapTest) {
      failLdapTest = false;
      return route.fulfill({ status: 400, json: { error: 'LDAP bind failed: credentials were rejected' } });
    }
    return route.fulfill({ json: { email: 'mapped@example.com', display_name: 'Mapped Person', duration_ms: 42 } });
  });
  await page.route('**/api/admin/ldap-config', async (route) => {
    count('ldapConfiguration');
    if (route.request().method() === 'PUT') ldapConfiguration = await record(route, 'ldapSave') as LdapConfiguration;
    return route.fulfill({ json: ldapConfiguration });
  });
  await page.route('**/api/admin/users', async (route) => {
    count('users');
    if (route.request().method() === 'POST') {
      const body = await record(route, 'userCreate') as { email: string; display_name?: string; password?: string; role: User['role'] };
      const created: AdminUserDetail = {
        user: {
          id: `user-${users.length + 1}`,
          email: body.email,
          display_name: body.display_name ?? body.email.split('@')[0],
          role: body.role
        },
        has_password: Boolean(body.password),
        created_at: '2026-07-12T10:00:00.000Z'
      };
      users = [...users, created];
      return route.fulfill({ status: 201, json: created });
    }
    return route.fulfill({ json: users });
  });
  await page.route(/\/api\/admin\/users\/[^/]+(?:\/(?:password|role|erase))?$/, async (route) => {
    const path = new URL(route.request().url()).pathname.split('/');
    const userId = path[4];
    const operation = path[5];
    const index = users.findIndex((detail) => detail.user.id === userId);
    if (operation === 'password') {
      await record(route, 'password');
      users[index] = { ...users[index], has_password: true };
      return route.fulfill({ json: users[index] });
    }
    if (operation === 'role') {
      const body = await record(route, 'role') as { role: User['role'] };
      users[index] = { ...users[index], user: { ...users[index].user, role: body.role } };
      return route.fulfill({ json: users[index] });
    }
    if (operation === 'erase') {
      await record(route, 'erase');
      const erased = users[index].user;
      users = users.filter((detail) => detail.user.id !== userId);
      const erasure: UserErasure = {
        user_id: userId,
        email: erased.email,
        status: 'pending',
        requested_at: '2026-07-17T08:00:00.000Z',
        completed_at: null
      };
      erasures.unshift(erasure);
      return route.fulfill({ status: 202, json: erasure });
    }
    if (route.request().method() === 'PATCH') {
      const body = await record(route, 'userUpdate') as { email: string; display_name: string };
      users[index] = { ...users[index], user: { ...users[index].user, ...body } };
    }
    return route.fulfill({ json: users[index] });
  });
  await page.route('**/api/admin/user-erasures', (route) => route.fulfill({ json: erasures }));
  await page.route('**/api/admin/external-platforms', async (route) => {
    if (route.request().method() === 'POST') {
      const body = await record(route, 'platformCreate') as { key: string; name: string };
      const created = { id: `platform-${body.key}`, ...body };
      platforms = [...platforms, created];
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
  await page.route(/\/api\/admin\/external-platforms\/[^/]+\/authentication-channels$/, (route) => route.fulfill({ json: channels }));
  await page.route(/\/api\/admin\/authentication-channels\/[^/]+$/, async (route) => {
    const body = await record(route, 'channelUpdate');
    return route.fulfill({ json: { ...channels[0], ...(body as object) } });
  });
  return state;
}

test('Authentication policy and LDAP draft save/test use the exact backend DTO', async ({ page }) => {
  const state = await installAdministrationApi(page, { ldapConfiguration: null, delayLdapTest: true });
  await page.goto('/administration');

  await expect(page.getByRole('heading', { name: 'Authentication policy' })).toBeVisible();
  await expect(page.getByLabel('LDAP login')).toBeDisabled();
  await expect(page.getByText(/does not verify email/)).toBeVisible();

  await page.getByLabel('LDAP URL').fill('ldap://directory.example.com:389');
  await page.getByLabel('Connection security').selectOption('plain');
  await page.getByLabel('Base DN').fill('ou=people,dc=example,dc=com');
  await expect(page.getByText(/sends credentials without transport encryption/)).toBeVisible();
  const saveConfiguration = page.getByRole('button', { name: 'Save LDAP configuration' });
  await expect(saveConfiguration).toBeDisabled();
  await page.getByLabel('Allow insecure plain LDAP').check();
  await saveConfiguration.click();
  expect(state.bodies.ldapSave).toEqual([{
    url: 'ldap://directory.example.com:389',
    security: 'plain',
    base_dn: 'ou=people,dc=example,dc=com',
    bind_identity_template: '{email}',
    user_filter: '(userPrincipalName={email})',
    email_attribute: 'mail',
    display_name_attribute: 'displayName',
    allow_insecure: true,
    skip_tls_verify: false
  }]);
  await expect(page.getByLabel('LDAP login')).toBeEnabled();
  await page.getByLabel('LDAP login').check();
  await page.getByRole('button', { name: 'Save authentication policy' }).click();
  expect(state.bodies.policy).toEqual([{
    password_registration_enabled: true,
    password_login_enabled: true,
    ldap_login_enabled: true
  }]);

  await page.getByLabel('Test email').fill('bind@example.com');
  await page.getByLabel('Test password').fill('one-time-password');
  const runTest = page.getByRole('button', { name: 'Run test' });
  await runTest.click();
  await expect(page.getByRole('button', { name: 'Testing...' })).toBeDisabled();
  await expect(page.getByLabel('Test email')).toBeDisabled();
  state.releaseLdapTest();
  await expect(page.getByText('LDAP test succeeded')).toBeVisible();
  await expect(page.getByText('mapped@example.com')).toBeVisible();
  await expect(page.getByLabel('Test email')).toHaveValue('');
  await expect(page.getByLabel('Test password')).toHaveValue('');
  expect(state.bodies.ldapTest).toEqual([{
    configuration: state.bodies.ldapSave[0],
    email: 'bind@example.com',
    password: 'one-time-password'
  }]);
  await page.getByRole('button', { name: 'Clear test result' }).click();
  await expect(page.getByText('LDAP test succeeded')).toHaveCount(0);
});

test('TLS verification and failed LDAP tests keep warnings and errors visible without persisting credentials', async ({ page }) => {
  const state = await installAdministrationApi(page);
  await page.goto('/administration');
  await page.getByLabel('Skip TLS certificate verification').check();
  await expect(page.getByText(/server identity cannot be trusted/)).toBeVisible();
  state.failNextLdapTest();
  await page.getByLabel('Test email').fill('bad@example.com');
  await page.getByLabel('Test password').fill('bad-password');
  await page.getByRole('button', { name: 'Run test' }).click();
  await expect(page.getByRole('alert').filter({ hasText: 'LDAP test failed.' })).toBeVisible();
  await expect(page.getByText('LDAP bind failed: credentials were rejected')).toBeVisible();
  await expect(page.getByLabel('Test email')).toHaveValue('');
  await expect(page.getByLabel('Test password')).toHaveValue('');
  await expect(page.getByText('bad-password')).toHaveCount(0);
});

test('Super Administrator can create, edit, password, role, and email-confirm user deletion', async ({ page }) => {
  const state = await installAdministrationApi(page);
  await page.goto('/administration');
  await page.getByRole('tab', { name: 'User management' }).click();

  const table = page.getByRole('table', { name: 'User management' });
  const currentRow = table.getByRole('row', { name: /admin@example.com/ });
  await expect(currentRow.getByRole('button', { name: 'Delete user: admin@example.com' })).toBeDisabled();

  await page.getByRole('button', { name: 'Create user' }).click();
  const createDialog = page.getByRole('dialog', { name: 'Create user' });
  await createDialog.getByLabel('Email').fill('operator@example.com');
  await createDialog.getByLabel('Display Name (optional)').fill('Operator');
  await createDialog.getByLabel('Password (optional)').fill('operator-password');
  await createDialog.getByLabel('Role').selectOption('admin');
  await createDialog.getByRole('button', { name: 'Create user' }).click();
  expect(state.bodies.userCreate).toEqual([{
    email: 'operator@example.com',
    display_name: 'Operator',
    password: 'operator-password',
    role: 'admin'
  }]);

  let aliceRow = table.getByRole('row', { name: /alice@example.com/ });
  await aliceRow.getByRole('button', { name: 'User information: alice@example.com' }).click();
  await expect(page.getByRole('dialog', { name: 'User information' })).toContainText('Alice');
  await page.getByRole('dialog', { name: 'User information' }).getByRole('button', { name: 'Close' }).last().click();

  await aliceRow.getByRole('button', { name: 'Edit user: alice@example.com' }).click();
  const editDialog = page.getByRole('dialog', { name: 'Edit user' });
  await editDialog.getByLabel('Email').fill('alice.updated@example.com');
  await editDialog.getByLabel('Display Name').fill('Alice Updated');
  await editDialog.getByRole('button', { name: 'Save changes' }).click();
  expect(state.bodies.userUpdate).toEqual([{ email: 'alice.updated@example.com', display_name: 'Alice Updated' }]);

  aliceRow = table.getByRole('row', { name: /alice.updated@example.com/ });
  await aliceRow.getByRole('button', { name: 'Set user password: alice.updated@example.com' }).click();
  const passwordDialog = page.getByRole('dialog', { name: 'Set user password' });
  await passwordDialog.getByLabel('Password').fill('new-password');
  await passwordDialog.getByRole('button', { name: 'Save changes' }).click();
  expect(state.bodies.password).toEqual([{ password: 'new-password' }]);

  await aliceRow.getByRole('button', { name: 'Change user role: alice.updated@example.com' }).click();
  const roleDialog = page.getByRole('dialog', { name: 'Change user role' });
  await roleDialog.getByLabel('Role').selectOption('super_admin');
  await roleDialog.getByRole('button', { name: 'Save changes' }).click();
  expect(state.bodies.role).toEqual([{ role: 'super_admin' }]);

  await aliceRow.getByRole('button', { name: 'Delete user: alice.updated@example.com' }).click();
  const eraseDialog = page.getByRole('dialog', { name: 'Delete user' });
  const eraseAction = eraseDialog.getByRole('button', { name: 'Delete user' });
  await expect(eraseAction).toBeDisabled();
  await eraseDialog.getByLabel('Confirm email').fill('alice.updated@example.com');
  await eraseAction.click();
  expect(state.bodies.erase).toEqual([{ email: 'alice.updated@example.com' }]);
  await expect(table.getByRole('row', { name: /alice.updated@example.com/ })).toHaveCount(0);
  await expect(page.getByText('pending', { exact: true })).toBeVisible();
});

test('Administrator creation and role controls stay within the backend member-only boundary', async ({ page }) => {
  const administrator = { ...superAdmin, role: 'admin' as const };
  const state = await installAdministrationApi(page, { currentUser: administrator });
  await page.goto('/administration');
  await page.getByRole('tab', { name: 'User management' }).click();
  await expect(page.getByRole('button', { name: /Change user role:/ })).toHaveCount(0);
  await page.getByRole('button', { name: 'Create user' }).click();
  const dialog = page.getByRole('dialog', { name: 'Create user' });
  await expect(dialog.getByLabel('Role')).toBeDisabled();
  await expect(dialog.getByLabel('Role')).toHaveValue('member');
  await dialog.getByLabel('Email').fill('member@example.com');
  await dialog.getByRole('button', { name: 'Create user' }).click();
  expect(state.bodies.userCreate).toEqual([{ email: 'member@example.com', role: 'member' }]);
});

test('External platform dialogs retain their independent integration identity workflow', async ({ page }) => {
  const state = await installAdministrationApi(page);
  await page.goto('/administration');
  await page.getByRole('tab', { name: 'External platforms' }).click();
  await page.getByRole('button', { name: 'Add platform' }).click();
  const createDialog = page.getByRole('dialog', { name: 'Add platform' });
  await createDialog.getByLabel('Platform key').fill('slack');
  await createDialog.getByLabel('Platform name').fill('Slack');
  await createDialog.getByRole('button', { name: 'Add platform' }).click();
  expect(state.bodies.platformCreate).toEqual([{ key: 'slack', name: 'Slack' }]);
});

test('Administration remains localized and usable at 390px without overflow or browser failures', async ({ page }) => {
  const pageErrors: string[] = [];
  const consoleErrors: string[] = [];
  const requestFailures: string[] = [];
  page.on('pageerror', (error) => pageErrors.push(error.message));
  page.on('console', (message) => { if (message.type() === 'error') consoleErrors.push(message.text()); });
  page.on('requestfailed', (request) => requestFailures.push(`${request.method()} ${new URL(request.url()).pathname}`));
  await page.setViewportSize({ width: 390, height: 844 });
  await page.addInitScript(() => localStorage.setItem('agent-hub-language', 'zh-CN'));
  await installAdministrationApi(page, {
    ldapConfiguration: { ...savedLdapConfiguration, security: 'plain', allow_insecure: true }
  });

  await page.goto('/administration');
  await expect(page.getByRole('heading', { name: '管理', exact: true, level: 1 })).toBeVisible();
  await expect(page.getByText(/无传输加密/)).toBeVisible();
  await page.getByRole('tab', { name: '用户管理' }).click();
  await page.getByRole('button', { name: '创建用户' }).click();
  const dialog = page.getByRole('dialog', { name: '创建用户' });
  await expect(dialog.getByLabel('显示名称（可选）')).toBeVisible();
  expect(await page.evaluate(() => document.documentElement.scrollWidth)).toBeLessThanOrEqual(390);
  await dialog.getByRole('button', { name: '关闭' }).click();
  expect(pageErrors).toEqual([]);
  expect(consoleErrors).toEqual([]);
  expect(requestFailures).toEqual([]);
});
