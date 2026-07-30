import { expect, test, type Page, type Route } from '@playwright/test';

const user = {
  id: 'auth-user',
  email: 'person@example.com',
  display_name: 'Person',
  role: 'member'
};

type Providers = {
  password_registration_enabled: boolean;
  password_login_enabled: boolean;
  ldap_login_enabled: boolean;
};

async function installAuthenticationApi(page: Page, providers: Providers) {
  const bodies: Record<string, unknown[]> = {};
  let currentUser: typeof user | null = null;
  const record = async (route: Route, key: string) => {
    const body = route.request().postDataJSON();
    (bodies[key] ??= []).push(body);
    return body;
  };

  await page.route('**/api/auth/me', (route) => currentUser
    ? route.fulfill({ json: currentUser })
    : route.fulfill({ status: 401, json: { error: 'unauthorized' } }));
  await page.route('**/api/auth/providers', (route) => route.fulfill({ json: providers }));
  await page.route('**/api/auth/login', async (route) => {
    await record(route, 'passwordLogin');
    currentUser = user;
    return route.fulfill({ json: { user } });
  });
  await page.route('**/api/auth/ldap/login', async (route) => {
    await record(route, 'ldapLogin');
    currentUser = user;
    return route.fulfill({ json: { user } });
  });
  await page.route('**/api/auth/register', async (route) => {
    const body = await record(route, 'register') as { email: string; display_name?: string };
    currentUser = { ...user, email: body.email, display_name: body.display_name ?? body.email.split('@')[0] };
    return route.fulfill({ json: { user: currentUser } });
  });
  await page.route('**/api/agents', (route) => route.fulfill({ json: [] }));
  await page.route('**/api/sessions', (route) => route.fulfill({ json: [] }));
  return { bodies, setCurrentUser: (next: typeof user | null) => { currentUser = next; } };
}

test('provider methods route Local Password, LDAP, and registration to their exact endpoints', async ({ page }) => {
  const local = await installAuthenticationApi(page, {
    password_registration_enabled: true,
    password_login_enabled: true,
    ldap_login_enabled: true
  });
  await page.goto('/login');

  const methods = page.getByRole('group', { name: 'Sign-in method' });
  await expect(methods.getByRole('button', { name: 'Local Password' })).toHaveAttribute('aria-pressed', 'false');
  await expect(methods.getByRole('button', { name: 'Domain Account' })).toHaveAttribute('aria-pressed', 'true');
  await methods.getByRole('button', { name: 'Local Password' }).click();
  await page.getByLabel('Email').fill('local@example.com');
  await page.getByLabel('Password').fill('local-password');
  await page.getByRole('button', { name: 'Sign in', exact: true }).click();
  await expect(page).toHaveURL(/\/sessions$/);
  expect(local.bodies.passwordLogin).toEqual([{ email: 'local@example.com', password: 'local-password' }]);

  const ldapPage = await page.context().newPage();
  const ldap = await installAuthenticationApi(ldapPage, {
    password_registration_enabled: true,
    password_login_enabled: true,
    ldap_login_enabled: true
  });
  await ldapPage.goto('/login');
  await expect(ldapPage.getByRole('button', { name: 'Domain Account' })).toHaveAttribute('aria-pressed', 'true');
  await expect(ldapPage.getByLabel('Password', { exact: true })).toHaveCount(0);
  await expect(ldapPage.getByLabel('Directory password')).toHaveAttribute('minlength', '1');
  await ldapPage.getByLabel('Email').fill('directory@example.com');
  await ldapPage.getByLabel('Directory password').fill('short');
  await ldapPage.getByRole('button', { name: 'Sign in', exact: true }).click();
  expect(ldap.bodies.ldapLogin).toEqual([{ email: 'directory@example.com', password: 'short' }]);

  const registrationPage = await page.context().newPage();
  const registration = await installAuthenticationApi(registrationPage, {
    password_registration_enabled: true,
    password_login_enabled: true,
    ldap_login_enabled: false
  });
  await registrationPage.goto('/login');
  await registrationPage.getByRole('button', { name: 'Create an account' }).click();
  await registrationPage.getByLabel('Email').fill('new@example.com');
  await registrationPage.getByLabel('Display Name (optional)').fill('New Person');
  await registrationPage.getByLabel('Password').fill('new-password');
  await registrationPage.getByRole('button', { name: 'Create account', exact: true }).click();
  expect(registration.bodies.register).toEqual([{ email: 'new@example.com', password: 'new-password', display_name: 'New Person' }]);
});

test('ordinary disabled password stays hidden while the emergency query exposes only its Super Administrator form', async ({ page }) => {
  await installAuthenticationApi(page, {
    password_registration_enabled: false,
    password_login_enabled: false,
    ldap_login_enabled: true
  });
  await page.goto('/login');
  await expect(page.getByText('Domain Account', { exact: true })).toBeVisible();
  await expect(page.getByText('Local Password', { exact: true })).toHaveCount(0);
  await expect(page.getByLabel('Password', { exact: true })).toHaveCount(0);
  await expect(page.getByLabel('Directory password')).toBeVisible();

  await page.goto('/login?method=password');
  await expect(page.getByText('Emergency password access', { exact: true })).toBeVisible();
  await expect(page.getByText('Domain Account', { exact: true })).toHaveCount(0);
  await expect(page.getByLabel('Password')).toBeVisible();
  await page.getByLabel('Email').fill('admin@example.com');
  await page.getByLabel('Password').fill('admin-password');
  await page.getByRole('button', { name: 'Sign in', exact: true }).click();
  await expect(page).toHaveURL(/\/sessions$/);
});

test('provider loading, retryable error, and pending LDAP submission expose stable disabled states', async ({ page }) => {
  let providerAttempts = 0;
  let releaseProviders!: () => void;
  const providerGate = new Promise<void>((resolve) => { releaseProviders = resolve; });
  let releaseLogin!: () => void;
  const loginGate = new Promise<void>((resolve) => { releaseLogin = resolve; });
  let currentUser: typeof user | null = null;

  await page.route('**/api/auth/me', (route) => currentUser
    ? route.fulfill({ json: currentUser })
    : route.fulfill({ status: 401, json: { error: 'unauthorized' } }));
  await page.route('**/api/auth/providers', async (route) => {
    providerAttempts += 1;
    if (providerAttempts === 1) {
      await providerGate;
      return route.fulfill({ status: 503, json: { error: 'unavailable' } });
    }
    return route.fulfill({ json: { password_registration_enabled: false, password_login_enabled: false, ldap_login_enabled: true } });
  });
  await page.route('**/api/auth/ldap/login', async (route) => {
    await loginGate;
    currentUser = user;
    return route.fulfill({ json: { user } });
  });
  await page.route('**/api/agents', (route) => route.fulfill({ json: [] }));
  await page.route('**/api/sessions', (route) => route.fulfill({ json: [] }));

  await page.goto('/login');
  await expect(page.getByText('Loading sign-in methods...')).toBeVisible();
  releaseProviders();
  await expect(page.getByRole('alert')).toContainText('Unable to load sign-in methods.');
  await page.getByRole('button', { name: 'Retry' }).click();
  await page.getByLabel('Email').fill('person@example.com');
  await page.getByLabel('Directory password').fill('directory-password');
  const submit = page.locator('.login-actions button[type="submit"]');
  await submit.click();
  await expect(submit).toBeDisabled();
  await expect(page.getByLabel('Directory password')).toBeDisabled();
  releaseLogin();
  await expect(page).toHaveURL(/\/sessions$/);
});

test('profile editing and Chinese mobile login stay localized without overflow or browser failures', async ({ page }) => {
  const pageErrors: string[] = [];
  const consoleErrors: string[] = [];
  const requestFailures: string[] = [];
  page.on('pageerror', (error) => pageErrors.push(error.message));
  page.on('console', (message) => { if (message.type() === 'error') consoleErrors.push(message.text()); });
  page.on('requestfailed', (request) => requestFailures.push(`${request.method()} ${new URL(request.url()).pathname}`));
  await page.setViewportSize({ width: 390, height: 844 });
  await page.addInitScript(() => localStorage.setItem('agent-hub-language', 'zh-CN'));
  const fixture = await installAuthenticationApi(page, {
    password_registration_enabled: false,
    password_login_enabled: true,
    ldap_login_enabled: true
  });
  fixture.setCurrentUser(user);

  await page.goto('/login');
  await expect(page.getByRole('group', { name: '登录方式' })).toBeVisible();
  await expect(page.getByRole('button', { name: '本地密码' })).toBeVisible();
  await expect(page.getByRole('button', { name: '域账号' })).toHaveAttribute('aria-pressed', 'true');
  expect(await page.evaluate(() => document.documentElement.scrollWidth)).toBeLessThanOrEqual(390);

  let profileBody: unknown;
  await page.route('**/api/users/me', async (route) => {
    profileBody = route.request().postDataJSON();
    return route.fulfill({ json: { ...user, display_name: '新名称' } });
  });
  await page.goto('/sessions');
  await page.getByTitle('编辑个人资料').click();
  const dialog = page.getByRole('dialog', { name: '编辑个人资料' });
  await dialog.getByLabel('显示名称').fill('新名称');
  await dialog.getByRole('button', { name: '保存更改' }).click();
  expect(profileBody).toEqual({ display_name: '新名称' });
  await expect(page.getByText('新名称', { exact: true })).toBeVisible();
  expect(await page.evaluate(() => document.documentElement.scrollWidth)).toBeLessThanOrEqual(390);
  expect(pageErrors).toEqual([]);
  expect(consoleErrors).toEqual([]);
  expect(requestFailures).toEqual([]);
});
