import assert from 'node:assert/strict';
import { ApiClient, loginAsAdmin, poll, provisionLocalUser } from '../../support/api.mjs';
import { withBrowser } from '../../support/browser.mjs';

function uniqueSlug(context, prefix) {
  return context.unique(prefix)
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, '-')
    .replace(/^-|-$/g, '');
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

async function restorePolicy(client, snapshot) {
  const { data: restored } = await client.request('/api/admin/auth-policy', {
    method: 'PATCH',
    body: snapshot
  });
  assert.deepEqual(restored, snapshot, 'Authentication policy restore response must match snapshot');
  assert.deepEqual(
    (await client.get('/api/admin/auth-policy')).data,
    snapshot,
    'Authentication policy restore must persist'
  );
}

async function restoreLdapConfiguration(context, client, snapshot) {
  if (snapshot) {
    const { data: restored } = await client.request('/api/admin/ldap-config', {
      method: 'PUT',
      body: snapshot
    });
    assert.deepEqual(restored, snapshot, 'LDAP configuration restore must match snapshot');
  } else {
    context.compose.psql('DELETE FROM ldap_configuration WHERE singleton = true');
  }
  assert.deepEqual((await client.get('/api/admin/ldap-config')).data, snapshot);
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
  if (errors.length > 0) throw new AggregateError(errors, 'Administration browser cleanup failed');
}

export default async function administrationBrowserScenario(context) {
  const adminClient = new ApiClient(context.baseURL);
  const { data: superAdmin } = await loginAsAdmin(adminClient);
  assert.equal(superAdmin.role, 'super_admin');
  const { data: policySnapshot } = await adminClient.get('/api/admin/auth-policy');
  const { data: ldapSnapshot } = await adminClient.get('/api/admin/ldap-config');
  const independentMember = await provisionLocalUser(
    adminClient,
    context,
    'qa-administration-independent-member'
  );
  const profileDisplayName = context.unique('QA Browser Super Administrator');
  let createdUserId = null;
  let createdUserEmail = null;

  try {
    await withBrowser(context, {
      allowedHttpErrors: [
        { method: 'GET', pathname: '/api/auth/me', status: 401, times: 1 },
        { method: 'POST', pathname: '/api/admin/ldap-config/test', status: 503, times: 1 },
        { method: 'POST', pathname: '/api/admin/users', status: 409, times: 1 }
      ]
    }, async ({ page, browserErrors }) => {
      await page.route('**/api/admin/ldap-config/test', (route) => route.fulfill({
        status: 503,
        contentType: 'application/json',
        body: JSON.stringify({ error: 'LDAP connect failed: connection could not be established' })
      }));

      await page.goto('/login', { waitUntil: 'domcontentloaded' });
      await page.getByLabel('Email').fill('admin@example.com');
      await page.getByLabel('Password').fill('admin123');
      await page.getByRole('button', { name: 'Sign in', exact: true }).click();
      await assertVisible(page.getByText('admin@example.com', { exact: true }), 'Signed-in Super Administrator');

      await page.getByTitle('Edit profile').click();
      const profileDialog = page.getByRole('dialog', { name: 'Edit profile' });
      await profileDialog.getByLabel('Display Name').fill(profileDisplayName);
      const profileResponse = page.waitForResponse((response) => (
        response.request().method() === 'PATCH'
        && new URL(response.url()).pathname === '/api/users/me'
      ));
      await profileDialog.getByRole('button', { name: 'Save changes' }).click();
      assert.equal((await profileResponse).ok(), true, 'Current-user Display Name update must succeed');
      await assertVisible(page.getByText(profileDisplayName, { exact: true }), 'Updated current-user Display Name');

      await page.goto('/administration', { waitUntil: 'domcontentloaded' });
      await assertVisible(page.getByRole('heading', { name: 'Administration', level: 1 }), 'Administration title');
      const tablist = page.getByRole('tablist', { name: 'Administration' });
      assert.equal(await tablist.getByRole('tab').count(), 3, 'Administration must expose three tabs');
      await assertVisible(page.getByRole('heading', { name: 'Authentication policy' }), 'Authentication policy');
      assert.equal(
        await page.getByLabel('Password registration', { exact: true }).isChecked(),
        policySnapshot.password_registration_enabled
      );
      assert.equal(
        await page.getByLabel('Local Password login', { exact: true }).isChecked(),
        policySnapshot.password_login_enabled
      );
      assert.equal(
        await page.getByLabel('LDAP login', { exact: true }).isChecked(),
        policySnapshot.ldap_login_enabled
      );

      const ldapLogin = page.getByLabel('LDAP login', { exact: true });
      if (!ldapSnapshot) {
        assert.equal(await ldapLogin.isDisabled(), true, 'LDAP login must be disabled until configuration is saved');
      }
      await page.getByLabel('LDAP URL').fill('ldap://127.0.0.1:1');
      await page.getByLabel('Connection security').selectOption('plain');
      await page.getByLabel('Base DN').fill('ou=people,dc=example,dc=test');
      await page.getByLabel('Bind identity template').fill(
        'uid={email},ou=people,dc=example,dc=test'
      );
      await page.getByLabel('User filter').fill('(mail={email})');
      await page.getByLabel('Email attribute').fill('mail');
      await page.getByLabel('Display Name attribute').fill('displayName');
      await assertVisible(
        page.getByText(/sends credentials without transport encryption/),
        'Persistent Plain LDAP warning'
      );
      const saveLdap = page.getByRole('button', { name: 'Save LDAP configuration' });
      assert.equal(await saveLdap.isDisabled(), true, 'Plain LDAP save must require explicit insecure opt-in');
      await page.getByLabel('Allow insecure plain LDAP').check();
      const ldapSaveResponse = page.waitForResponse((response) => (
        response.request().method() === 'PUT'
        && new URL(response.url()).pathname === '/api/admin/ldap-config'
      ));
      await saveLdap.click();
      assert.equal((await ldapSaveResponse).ok(), true, 'LDAP configuration save must succeed');
      await assertVisible(page.getByText('LDAP configuration saved.', { exact: true }), 'LDAP save notice');
      assert.equal(await ldapLogin.isEnabled(), true, 'Saved LDAP configuration must enable policy control');

      const runLdapTest = page.getByRole('button', { name: 'Run test' });
      assert.equal(await runLdapTest.isDisabled(), true, 'LDAP draft test requires one-time credentials');
      await page.getByLabel('Test email').fill('directory-user@example.com');
      await page.getByLabel('Test password').fill('one-time-directory-password');
      const ldapTestResponse = page.waitForResponse((response) => (
        response.request().method() === 'POST'
        && new URL(response.url()).pathname === '/api/admin/ldap-config/test'
      ));
      await runLdapTest.click();
      assert.equal((await ldapTestResponse).status(), 503, 'UI error path must receive a classified LDAP outage');
      await assertVisible(page.getByRole('alert').filter({ hasText: 'LDAP test failed.' }), 'LDAP test error');
      await assertVisible(
        page.getByText('LDAP connect failed: connection could not be established', { exact: true }),
        'Sanitized LDAP test diagnostic'
      );
      assert.equal(await page.getByText('one-time-directory-password', { exact: true }).count(), 0);
      assert.equal(await page.getByLabel('Test email').inputValue(), '');
      assert.equal(await page.getByLabel('Test password').inputValue(), '');

      await page.getByLabel('Password registration', { exact: true }).check();
      await ldapLogin.check();
      await assertVisible(page.getByText(/does not verify email/), 'Registration risk warning');
      const policyResponse = page.waitForResponse((response) => (
        response.request().method() === 'PATCH'
        && new URL(response.url()).pathname === '/api/admin/auth-policy'
      ));
      await page.getByRole('button', { name: 'Save authentication policy' }).click();
      assert.equal((await policyResponse).ok(), true, 'Authentication policy save must succeed');
      assert.deepEqual((await adminClient.get('/api/admin/auth-policy')).data, {
        password_registration_enabled: true,
        password_login_enabled: true,
        ldap_login_enabled: true
      });
      await assertNoHorizontalOverflow(page, 'Administration desktop authentication');

      const platformKey = uniqueSlug(context, 'qa-browser-platform');
      const platformName = context.unique('QA Browser Platform');
      const platformsTab = tablist.getByRole('tab', { name: 'External platforms', exact: true });
      await platformsTab.click();
      const platformsTable = page.getByRole('table', { name: 'External platforms' });
      await page.getByRole('button', { name: 'Add platform', exact: true }).click();
      const platformDialog = page.getByRole('dialog', { name: 'Add platform' });
      await platformDialog.getByLabel('Platform key').fill(platformKey);
      await platformDialog.getByLabel('Platform name').fill(platformName);
      const platformResponse = page.waitForResponse((response) => (
        response.request().method() === 'POST'
        && new URL(response.url()).pathname === '/api/admin/external-platforms'
      ));
      await platformDialog.getByRole('button', { name: 'Add platform', exact: true }).click();
      const createdPlatformResponse = await platformResponse;
      assert.equal(createdPlatformResponse.ok(), true, 'External Platform create must succeed');
      const createdPlatform = await createdPlatformResponse.json();
      const platformRow = platformsTable.getByRole('row').filter({ hasText: platformKey });
      await assertVisible(platformRow, 'Created External Platform row');
      await platformRow.getByRole('button', { name: `Edit external platform: ${platformName}` }).click();
      const editPlatformDialog = page.getByRole('dialog', { name: 'Edit external platform' });
      assert.equal(await editPlatformDialog.getByLabel('Platform key').isDisabled(), true);
      await editPlatformDialog.getByLabel('Channel key').last().fill(
        uniqueSlug(context, 'qa-browser-channel')
      );
      await editPlatformDialog.getByLabel('New channel name').fill(
        context.unique('QA Browser Trusted Channel')
      );
      assert.equal(await editPlatformDialog.getByLabel('Enable new channel').isChecked(), true);
      assert.equal(await editPlatformDialog.getByLabel('Trust email for new channel').isChecked(), true);
      const channelResponse = page.waitForResponse((response) => (
        response.request().method() === 'POST'
        && new URL(response.url()).pathname
          === `/api/admin/external-platforms/${createdPlatform.id}/authentication-channels`
      ));
      await editPlatformDialog.getByRole('button', { name: 'Add channel' }).click();
      const createdChannel = await (await channelResponse).json();
      assert.equal(createdChannel.enabled, true);
      assert.equal(createdChannel.trusted_email, true);
      await editPlatformDialog.getByRole('button', { name: 'Cancel' }).click();
      await assertNoHorizontalOverflow(page, 'Administration desktop platforms');

      const usersTab = tablist.getByRole('tab', { name: 'User management', exact: true });
      await usersTab.click();
      const usersTable = page.getByRole('table', { name: 'User management' });
      const currentUserRow = usersTable.getByRole('row').filter({ hasText: superAdmin.email });
      await assertVisible(currentUserRow, 'Current Super Administrator row');
      assert.equal(
        await currentUserRow.getByRole('button', { name: `Delete user: ${superAdmin.email}` }).isDisabled(),
        true,
        'Current user deletion must remain disabled'
      );

      await page.getByRole('button', { name: 'Create user' }).click();
      const duplicateDialog = page.getByRole('dialog', { name: 'Create user' });
      await duplicateDialog.getByLabel('Email').fill(superAdmin.email);
      await duplicateDialog.getByRole('button', { name: 'Create user' }).click();
      await assertVisible(duplicateDialog.getByRole('alert'), 'Duplicate-email create error');
      await duplicateDialog.getByRole('button', { name: 'Cancel' }).click();

      const targetEmail = `${uniqueSlug(context, 'qa-browser-managed-user')}@example.com`;
      const targetDisplayName = context.unique('QA Browser Managed User');
      const targetPassword = `${context.unique('Browser managed password')}!Aa9`;
      const replacementPassword = `${context.unique('Browser replacement password')}!Bb8`;
      await page.getByRole('button', { name: 'Create user' }).click();
      const createDialog = page.getByRole('dialog', { name: 'Create user' });
      await createDialog.getByLabel('Email').fill(targetEmail);
      await createDialog.getByLabel('Display Name (optional)').fill(targetDisplayName);
      await createDialog.getByLabel('Password (optional)').fill('short');
      assert.equal(
        await createDialog.getByRole('button', { name: 'Create user' }).isDisabled(),
        true,
        'Short password must keep user creation disabled'
      );
      await createDialog.getByLabel('Password (optional)').fill(targetPassword);
      const createResponse = page.waitForResponse((response) => (
        response.request().method() === 'POST'
        && new URL(response.url()).pathname === '/api/admin/users'
      ));
      await createDialog.getByRole('button', { name: 'Create user' }).click();
      const createdResponse = await createResponse;
      assert.equal(createdResponse.ok(), true, 'User creation must succeed');
      const createdDetail = await createdResponse.json();
      createdUserId = createdDetail.user.id;
      createdUserEmail = targetEmail;
      let targetRow = usersTable.getByRole('row').filter({ hasText: targetEmail });
      await assertVisible(targetRow, 'Created user row');

      const targetSession = new ApiClient(context.baseURL);
      await targetSession.post('/api/auth/login', { email: targetEmail, password: targetPassword });
      await targetRow.getByRole('button', { name: `User information: ${targetEmail}` }).click();
      const detailsDialog = page.getByRole('dialog', { name: 'User information' });
      await assertVisible(detailsDialog.getByText(targetDisplayName, { exact: true }), 'User detail Display Name');
      await detailsDialog.getByRole('button', { name: 'Close' }).last().click();

      const updatedEmail = `${uniqueSlug(context, 'qa-browser-updated-user')}@example.com`;
      const updatedDisplayName = context.unique('QA Browser Updated User');
      await targetRow.getByRole('button', { name: `Edit user: ${targetEmail}` }).click();
      const editDialog = page.getByRole('dialog', { name: 'Edit user' });
      await editDialog.getByLabel('Email').fill(updatedEmail);
      await editDialog.getByLabel('Display Name').fill(updatedDisplayName);
      await assertVisible(
        editDialog.getByText(/signs this user out of every browser session/),
        'Email-change Session warning'
      );
      const editResponse = page.waitForResponse((response) => (
        response.request().method() === 'PATCH'
        && new URL(response.url()).pathname === `/api/admin/users/${createdUserId}`
      ));
      await editDialog.getByRole('button', { name: 'Save changes' }).click();
      assert.equal((await editResponse).ok(), true, 'Email and Display Name update must succeed');
      createdUserEmail = updatedEmail;
      await targetSession.get('/api/auth/me', { expectedStatus: 401 });
      assert.equal((await independentMember.client.get('/api/auth/me')).data.id, independentMember.user.id);
      targetRow = usersTable.getByRole('row').filter({ hasText: updatedEmail });
      await assertVisible(targetRow, 'Updated user row');

      const passwordSession = new ApiClient(context.baseURL);
      await passwordSession.post('/api/auth/login', { email: updatedEmail, password: targetPassword });
      await targetRow.getByRole('button', { name: `Set user password: ${updatedEmail}` }).click();
      const passwordDialog = page.getByRole('dialog', { name: 'Set user password' });
      await passwordDialog.getByLabel('Password').fill(replacementPassword);
      const passwordResponse = page.waitForResponse((response) => (
        response.request().method() === 'PUT'
        && new URL(response.url()).pathname === `/api/admin/users/${createdUserId}/password`
      ));
      await passwordDialog.getByRole('button', { name: 'Save changes' }).click();
      assert.equal((await passwordResponse).ok(), true, 'Password update must succeed');
      await passwordSession.get('/api/auth/me', { expectedStatus: 401 });
      const replacementSession = new ApiClient(context.baseURL);
      assert.equal((await replacementSession.post('/api/auth/login', {
        email: updatedEmail,
        password: replacementPassword
      })).data.user.id, createdUserId);

      await targetRow.getByRole('button', { name: `Change user role: ${updatedEmail}` }).click();
      const roleDialog = page.getByRole('dialog', { name: 'Change user role' });
      await roleDialog.getByLabel('Role').selectOption('admin');
      const roleResponse = page.waitForResponse((response) => (
        response.request().method() === 'PUT'
        && new URL(response.url()).pathname === `/api/admin/users/${createdUserId}/role`
      ));
      await roleDialog.getByRole('button', { name: 'Save changes' }).click();
      assert.equal((await roleResponse).ok(), true, 'Role update must succeed');
      await assertVisible(targetRow.getByText('Administrator', { exact: true }), 'Updated Administrator role');
      await assertNoHorizontalOverflow(page, 'Administration desktop users');

      await page.setViewportSize({ width: 390, height: 844 });
      await page.getByLabel('Language').selectOption('zh-CN');
      await assertVisible(page.getByRole('heading', { name: '管理', level: 1 }), 'Chinese Administration title');
      const chineseUsersTable = page.getByRole('table', { name: '用户管理' });
      targetRow = chineseUsersTable.getByRole('row').filter({ hasText: updatedEmail });
      await assertVisible(targetRow, 'Chinese managed user row');
      await assertNoHorizontalOverflow(page, 'Administration 390px users');

      await targetRow.getByRole('button', { name: `删除用户: ${updatedEmail}` }).click();
      const eraseDialog = page.getByRole('dialog', { name: '删除用户' });
      const eraseAction = eraseDialog.getByRole('button', { name: '删除用户', exact: true });
      await eraseDialog.getByLabel('确认邮箱').fill(`${updatedEmail}.wrong`);
      assert.equal(await eraseAction.isDisabled(), true, 'Inexact email must not enable deletion');
      await eraseDialog.getByLabel('确认邮箱').fill(updatedEmail);
      assert.equal(await eraseAction.isEnabled(), true, 'Exact email must enable deletion');
      const eraseResponse = page.waitForResponse((response) => (
        response.request().method() === 'POST'
        && new URL(response.url()).pathname === `/api/admin/users/${createdUserId}/erase`
      ));
      await eraseAction.click();
      const acceptedErasure = await eraseResponse;
      assert.equal(acceptedErasure.status(), 202, 'User deletion must be accepted');
      await targetRow.waitFor({ state: 'detached' });
      await assertVisible(
        page.locator('.erasure-history > div').filter({ hasText: createdUserId }),
        'PII-free deletion history'
      );

      await page.getByRole('tab', { name: '认证', exact: true }).click();
      await assertVisible(page.getByText(/明文 LDAP 会在无传输加密时发送凭据/), 'Chinese Plain LDAP warning');
      await assertNoHorizontalOverflow(page, 'Administration 390px authentication');
      await page.getByRole('tab', { name: '外部平台', exact: true }).click();
      await assertVisible(page.getByRole('table', { name: '外部平台' }), 'Chinese External Platform table');
      await assertNoHorizontalOverflow(page, 'Administration 390px platforms');
      assert.deepEqual(browserErrors, [], 'Browser diagnostics must remain empty');
    });

    if (createdUserId) {
      await poll(async () => {
        const { data: erasures } = await adminClient.get('/api/admin/user-erasures');
        return erasures.find((erasure) => erasure.user_id === createdUserId) ?? null;
      }, (erasure) => erasure?.status === 'completed', {
        timeoutMs: 45_000,
        description: `browser-managed user ${createdUserEmail} deletion to complete`
      });
    }
  } finally {
    const authRestores = ldapSnapshot
      ? [
          ['restore LDAP configuration', () => restoreLdapConfiguration(context, adminClient, ldapSnapshot)],
          ['restore authentication policy', () => restorePolicy(adminClient, policySnapshot)]
        ]
      : [
          ['restore authentication policy', () => restorePolicy(adminClient, policySnapshot)],
          ['remove scenario LDAP configuration', () => restoreLdapConfiguration(context, adminClient, null)]
        ];
    await runCleanupSteps([
      ...authRestores,
      [
        'restore current-user Display Name',
        async () => {
          const { data: restoredProfile } = await adminClient.request('/api/users/me', {
            method: 'PATCH',
            body: { display_name: superAdmin.display_name }
          });
          assert.equal(restoredProfile.display_name, superAdmin.display_name);
        }
      ]
    ]);
  }
}
