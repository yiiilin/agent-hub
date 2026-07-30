import assert from 'node:assert/strict';
import { ApiClient, loginAsAdmin } from '../../support/api.mjs';
import { withBrowser } from '../../support/browser.mjs';

const LDAP_USER = {
  email: 'qa.ldap@example.test',
  password: 'qa-ldap-password',
  displayName: 'QA LDAP User'
};

function plainConfiguration() {
  return {
    url: 'ldap://openldap:389',
    security: 'plain',
    base_dn: 'ou=people,dc=example,dc=test',
    bind_identity_template: 'uid={email},ou=people,dc=example,dc=test',
    user_filter: '(uid={email})',
    email_attribute: 'mail',
    display_name_attribute: 'displayName',
    allow_insecure: true,
    skip_tls_verify: false
  };
}

async function setPolicy(client, policy) {
  const { data } = await client.request('/api/admin/auth-policy', {
    method: 'PATCH',
    body: policy
  });
  assert.deepEqual(data, policy);
}

async function saveConfiguration(client, configuration) {
  const { data } = await client.request('/api/admin/ldap-config', {
    method: 'PUT',
    body: configuration
  });
  assert.deepEqual(data, configuration);
}

async function restoreLdapConfiguration(context, client, snapshot) {
  if (snapshot) {
    await saveConfiguration(client, snapshot);
  } else {
    context.compose.psql('DELETE FROM ldap_configuration WHERE singleton = true');
    assert.equal(context.compose.psql('SELECT count(*) FROM ldap_configuration'), '0');
  }
}

async function runCleanupSteps(steps) {
  const errors = [];
  for (const [label, cleanup] of steps) {
    try {
      await cleanup();
    } catch (error) {
      errors.push(new Error(`${label}: ${error instanceof Error ? error.message : String(error)}`));
    }
  }
  if (errors.length > 0) throw new AggregateError(errors, 'Real LDAP browser cleanup failed');
}

async function assertVisible(locator, description) {
  await locator.waitFor({ state: 'visible' });
  assert.equal(await locator.isVisible(), true, `${description} must be visible`);
}

async function assertNoHorizontalOverflow(page, label) {
  await page.waitForTimeout(100);
  const overflow = await page.evaluate(() => (
    document.documentElement.scrollWidth - document.documentElement.clientWidth
  ));
  assert.ok(overflow <= 1, `${label} horizontal overflow: ${overflow}px`);
}

export default async function ldapAuthenticationBrowserScenario(context) {
  const admin = new ApiClient(context.baseURL);
  const { data: superAdmin } = await loginAsAdmin(admin);
  const { data: policySnapshot } = await admin.get('/api/admin/auth-policy');
  const { data: ldapSnapshot } = await admin.get('/api/admin/ldap-config');
  const enabledPolicy = {
    password_registration_enabled: false,
    password_login_enabled: true,
    ldap_login_enabled: true
  };

  try {
    await saveConfiguration(admin, plainConfiguration());
    await setPolicy(admin, enabledPolicy);

    await withBrowser(context, {
      allowedHttpErrors: [
        { method: 'GET', pathname: '/api/auth/me', status: 401, times: 5 },
        { method: 'POST', pathname: '/api/auth/ldap/login', status: 401, times: 1 }
      ]
    }, async ({ page, request, browserErrors }) => {
      const allowedNoContentAborts = new Set();
      await page.goto('/login', { waitUntil: 'domcontentloaded' });
      await page.getByRole('button', { name: 'LDAP Directory', exact: true }).click();
      await page.getByLabel('Email').fill(LDAP_USER.email);
      await page.getByLabel('Directory password').fill('wrong-directory-password');
      const rejectedLogin = page.waitForResponse((response) => (
        response.request().method() === 'POST'
        && new URL(response.url()).pathname === '/api/auth/ldap/login'
      ));
      await page.getByRole('button', { name: 'Sign in', exact: true }).click();
      assert.equal((await rejectedLogin).status(), 401);
      await assertVisible(
        page.getByRole('alert').filter({ hasText: 'Unable to sign in with LDAP.' }),
        'Generic LDAP login error'
      );

      await page.getByLabel('Directory password').fill(LDAP_USER.password);
      const acceptedLogin = page.waitForResponse((response) => (
        response.request().method() === 'POST'
        && new URL(response.url()).pathname === '/api/auth/ldap/login'
        && response.status() === 200
      ));
      await page.getByRole('button', { name: 'Sign in', exact: true }).click();
      assert.equal((await acceptedLogin).ok(), true);
      await page.waitForURL((url) => url.pathname === '/sessions');
      await assertVisible(page.getByText(LDAP_USER.displayName, { exact: true }), 'LDAP Display Name');
      const ldapUser = await (await request.get('/api/auth/me')).json();
      assert.equal(ldapUser.email, LDAP_USER.email);
      assert.equal(ldapUser.display_name, LDAP_USER.displayName);
      await assertNoHorizontalOverflow(page, 'English desktop LDAP Session');

      await setPolicy(admin, { ...enabledPolicy, ldap_login_enabled: false });
      await page.reload({ waitUntil: 'domcontentloaded' });
      assert.equal((await request.get('/api/auth/me')).status(), 200, 'Disabling LDAP must retain the existing Session');
      await assertVisible(page.getByText(LDAP_USER.displayName, { exact: true }), 'Retained LDAP Session');

      const logoutResponsePromise = page.waitForResponse((response) => (
        response.request().method() === 'POST'
        && new URL(response.url()).pathname === '/api/auth/logout'
      ));
      await page.getByTitle('Log out').click();
      const logoutResponse = await logoutResponsePromise;
      assert.equal(logoutResponse.status(), 204, 'Logout must succeed');
      allowedNoContentAborts.add(`requestfailed: POST ${logoutResponse.url()}: net::ERR_ABORTED`);
      await assertVisible(page.getByRole('button', { name: 'Sign in', exact: true }), 'Login after LDAP logout');
      assert.equal(
        await page.getByRole('button', { name: 'LDAP Directory', exact: true }).count(),
        0,
        'Disabled LDAP must not remain a selectable ordinary login method'
      );

      await setPolicy(admin, {
        password_registration_enabled: false,
        password_login_enabled: false,
        ldap_login_enabled: true
      });
      await page.setViewportSize({ width: 390, height: 844 });
      await page.goto('/login', { waitUntil: 'domcontentloaded' });
      await page.getByLabel('Language').selectOption('zh-CN');
      await assertVisible(page.getByText('LDAP 目录', { exact: true }), 'Chinese LDAP-only login method');
      assert.equal(await page.getByText('本地密码', { exact: true }).count(), 0);
      await assertNoHorizontalOverflow(page, 'Chinese 390px LDAP-only login');

      await page.goto('/login?method=password', { waitUntil: 'domcontentloaded' });
      await assertVisible(page.getByText('紧急密码入口', { exact: true }), 'Chinese emergency password notice');
      await page.getByLabel('邮箱').fill(superAdmin.email);
      await page.getByLabel('密码', { exact: true }).fill('admin123');
      const emergencyLogin = page.waitForResponse((response) => (
        response.request().method() === 'POST'
        && new URL(response.url()).pathname === '/api/auth/login'
        && response.status() === 200
      ));
      await page.getByRole('button', { name: '登录', exact: true }).click();
      assert.equal((await emergencyLogin).ok(), true);
      await page.waitForURL((url) => url.pathname === '/sessions');
      assert.equal((await request.get('/api/auth/me')).status(), 200);
      await assertVisible(page.getByText(superAdmin.email, { exact: true }), 'Emergency Super Administrator Session');
      await assertNoHorizontalOverflow(page, 'Chinese 390px emergency Session');
      const unexpectedBrowserErrors = browserErrors.filter((error) => !allowedNoContentAborts.has(error));
      browserErrors.splice(0, browserErrors.length, ...unexpectedBrowserErrors);
      assert.deepEqual(browserErrors, []);
    });
  } finally {
    const authRestores = ldapSnapshot
      ? [
          ['restore LDAP configuration', () => restoreLdapConfiguration(context, admin, ldapSnapshot)],
          ['restore authentication policy', () => setPolicy(admin, policySnapshot)]
        ]
      : [
          ['restore authentication policy', () => setPolicy(admin, policySnapshot)],
          ['remove scenario LDAP configuration', () => restoreLdapConfiguration(context, admin, null)]
        ];
    await runCleanupSteps([
      ...authRestores,
      [
        'verify restored LDAP state',
        async () => {
          assert.deepEqual((await admin.get('/api/admin/auth-policy')).data, policySnapshot);
          assert.deepEqual((await admin.get('/api/admin/ldap-config')).data, ldapSnapshot);
        }
      ]
    ]);
  }
}
