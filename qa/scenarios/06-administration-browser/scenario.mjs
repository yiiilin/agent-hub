import assert from 'node:assert/strict';
import { ApiClient, loginAsAdmin } from '../../support/api.mjs';
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
  const { data: persisted } = await client.get('/api/admin/auth-policy');
  assert.deepEqual(persisted, snapshot, 'Authentication policy restore must persist');
}

export default async function administrationBrowserScenario(context) {
  const adminClient = new ApiClient(context.baseURL);
  const { data: superAdmin } = await loginAsAdmin(adminClient);
  assert.equal(superAdmin.role, 'super_admin');
  const { data: policySnapshot } = await adminClient.get('/api/admin/auth-policy');

  const memberUsername = uniqueSlug(context, 'qa-browser-member');
  const memberEmail = `${memberUsername}@example.com`;
  const memberPassword = `${context.unique('Member password')}!Aa9`;
  const replacementPassword = `${context.unique('Replacement password')}!Bb8`;
  const memberClient = new ApiClient(context.baseURL);
  const { data: registration } = await memberClient.post('/api/auth/register', {
    email: memberEmail,
    password: memberPassword
  });
  assert.equal(registration.user.username, memberUsername);
  assert.equal(registration.user.email, memberEmail);
  assert.equal(registration.user.role, 'member');
  assert.equal(registration.verification_required, false);
  assert.equal((await memberClient.get('/api/auth/me')).data.id, registration.user.id);

  try {
    await withBrowser(context, {
      allowedHttpErrors: [
        { method: 'GET', pathname: '/api/auth/me', status: 401, times: 1 }
      ]
    }, async ({ page, browserErrors }) => {
      await page.goto('/login', { waitUntil: 'domcontentloaded' });
      await page.getByLabel('Email').fill('admin@example.com');
      await page.getByLabel('Password').fill('admin123');
      await page.getByRole('button', { name: 'Sign in', exact: true }).click();
      await assertVisible(page.getByText('admin@example.com', { exact: true }), 'Signed-in Super Administrator');

      await page.goto('/administration', { waitUntil: 'domcontentloaded' });
      await assertVisible(page.getByRole('heading', { name: 'Administration', level: 1 }), 'Administration title');
      const tablist = page.getByRole('tablist', { name: 'Administration' });
      assert.equal(await tablist.getByRole('tab').count(), 3, 'Administration must expose three tabs');

      const authenticationTab = tablist.getByRole('tab', { name: 'Authentication', exact: true });
      assert.equal(await authenticationTab.getAttribute('aria-selected'), 'true');
      await assertVisible(page.getByRole('heading', { name: 'Authentication policy' }), 'Authentication policy');
      const policyInputs = [
        ['Password registration', 'password_registration_enabled'],
        ['Password login', 'password_login_enabled'],
        ['Email verification', 'email_verification_required']
      ];
      for (const [label, key] of policyInputs) {
        assert.equal(await page.getByLabel(label, { exact: true }).isChecked(), policySnapshot[key]);
      }
      const emailVerification = page.getByLabel('Email verification', { exact: true });
      await emailVerification.setChecked(!policySnapshot.email_verification_required);
      const policySaveResponse = page.waitForResponse((response) => (
        response.request().method() === 'PATCH'
        && new URL(response.url()).pathname === '/api/admin/auth-policy'
      ));
      await page.getByRole('button', { name: 'Save authentication policy' }).click();
      assert.equal((await policySaveResponse).ok(), true, 'Authentication policy save must succeed');
      await assertVisible(page.getByText('Authentication policy saved.', { exact: true }), 'Policy save notice');
      const { data: changedPolicy } = await adminClient.get('/api/admin/auth-policy');
      assert.deepEqual(changedPolicy, {
        ...policySnapshot,
        email_verification_required: !policySnapshot.email_verification_required
      });
      await assertNoHorizontalOverflow(page, 'Administration desktop policy');

      const platformKey = uniqueSlug(context, 'qa-browser-platform');
      const platformName = context.unique('QA Browser Platform');
      const updatedPlatformName = context.unique('QA Browser Platform Updated');
      const channelKey = uniqueSlug(context, 'qa-browser-channel');
      const channelName = context.unique('QA Browser Channel');
      const updatedChannelName = context.unique('QA Browser Channel Updated');

      const platformsTab = tablist.getByRole('tab', { name: 'External platforms', exact: true });
      await platformsTab.click();
      assert.equal(await platformsTab.getAttribute('aria-selected'), 'true');
      const platformsTable = page.getByRole('table', { name: 'External platforms' });
      await assertVisible(platformsTable, 'External platforms table');
      await page.getByRole('button', { name: 'Add platform', exact: true }).click();
      const createPlatformDialog = page.getByRole('dialog', { name: 'Add platform' });
      await assertVisible(createPlatformDialog, 'Add platform dialog');
      await createPlatformDialog.getByLabel('Platform key').fill(platformKey);
      await createPlatformDialog.getByLabel('Platform name').fill(platformName);
      const platformCreateResponse = page.waitForResponse((response) => (
        response.request().method() === 'POST'
        && new URL(response.url()).pathname === '/api/admin/external-platforms'
      ));
      await createPlatformDialog.getByRole('button', { name: 'Add platform', exact: true }).click();
      const createdPlatformResponse = await platformCreateResponse;
      assert.equal(createdPlatformResponse.ok(), true, 'External Platform create must succeed');
      const createdPlatform = await createdPlatformResponse.json();
      assert.equal(createdPlatform.key, platformKey);
      const platformRow = platformsTable.locator('tbody tr').filter({ hasText: platformKey }).first();
      await assertVisible(platformRow, 'Created External Platform row');
      assert.ok((await platformRow.textContent()).includes(platformName));

      await platformRow.getByRole('button', { name: `Edit external platform: ${platformName}` }).click();
      const editPlatformDialog = page.getByRole('dialog', { name: 'Edit external platform' });
      await assertVisible(editPlatformDialog, 'Edit platform dialog');
      const immutablePlatformKey = editPlatformDialog.getByLabel('Platform key');
      assert.equal(await immutablePlatformKey.isDisabled(), true, 'Platform key must be immutable');
      assert.equal(await immutablePlatformKey.inputValue(), platformKey);
      await editPlatformDialog.getByLabel('Platform name').fill(updatedPlatformName);

      const newChannelEnabled = editPlatformDialog.getByLabel('Enable new channel');
      const newChannelTrusted = editPlatformDialog.getByLabel('Trust email for new channel');
      assert.equal(await newChannelEnabled.isChecked(), true);
      assert.equal(await newChannelTrusted.isChecked(), true);
      await editPlatformDialog.getByLabel('Channel key').last().fill(channelKey);
      await editPlatformDialog.getByLabel('New channel name').fill(channelName);
      const channelCreateResponse = page.waitForResponse((response) => (
        response.request().method() === 'POST'
        && new URL(response.url()).pathname
          === `/api/admin/external-platforms/${createdPlatform.id}/authentication-channels`
      ));
      await editPlatformDialog.getByRole('button', { name: 'Add channel' }).click();
      const createdChannelResponse = await channelCreateResponse;
      assert.equal(createdChannelResponse.ok(), true, 'Authentication Channel create must succeed');
      const createdChannel = await createdChannelResponse.json();
      assert.equal(createdChannel.key, channelKey);
      assert.equal(createdChannel.enabled, true);
      assert.equal(createdChannel.trusted_email, true);
      const channelPicker = editPlatformDialog.locator('.administration-channel-picker button')
        .filter({ hasText: channelKey }).first();
      await assertVisible(channelPicker, 'Created Authentication Channel');

      const immutableChannelKey = editPlatformDialog.getByLabel('Channel key').first();
      assert.equal(await immutableChannelKey.isDisabled(), true, 'Channel key must be immutable');
      assert.equal(await immutableChannelKey.inputValue(), channelKey);
      await editPlatformDialog.getByLabel('Channel name', { exact: true }).fill(updatedChannelName);
      const channelEnabled = editPlatformDialog.getByLabel('Channel enabled');
      const channelTrusted = editPlatformDialog.getByLabel('Trusted email');
      assert.equal(await channelEnabled.isChecked(), true);
      assert.equal(await channelTrusted.isChecked(), true);
      await channelEnabled.uncheck();
      await channelTrusted.uncheck();
      const channelUpdateResponse = page.waitForResponse((response) => (
        response.request().method() === 'PATCH'
        && new URL(response.url()).pathname === `/api/admin/authentication-channels/${createdChannel.id}`
      ));
      await editPlatformDialog.getByRole('button', { name: 'Save channel' }).click();
      assert.equal((await channelUpdateResponse).ok(), true, 'Authentication Channel update must succeed');
      const { data: persistedChannels } = await adminClient.get(
        `/api/admin/external-platforms/${createdPlatform.id}/authentication-channels`
      );
      const persistedChannel = persistedChannels.find((channel) => channel.id === createdChannel.id);
      assert.equal(persistedChannel?.name, updatedChannelName);
      assert.equal(persistedChannel?.enabled, false);
      assert.equal(persistedChannel?.trusted_email, false);

      const platformUpdateResponse = page.waitForResponse((response) => (
        response.request().method() === 'PATCH'
        && new URL(response.url()).pathname === `/api/admin/external-platforms/${createdPlatform.id}`
      ));
      await editPlatformDialog.getByRole('button', { name: 'Save changes' }).click();
      assert.equal((await platformUpdateResponse).ok(), true, 'External Platform update must succeed');
      await assertVisible(platformRow, 'Updated External Platform row');
      assert.ok((await platformRow.textContent()).includes(updatedPlatformName));
      await assertNoHorizontalOverflow(page, 'Administration desktop platforms');

      const { data: promotedMember } = await adminClient.request(
        `/api/admin/users/${registration.user.id}/role`,
        { method: 'PUT', body: { role: 'admin' } }
      );
      assert.equal(promotedMember.user.role, 'admin');
      await adminClient.request(`/api/admin/users/${superAdmin.id}/role`, {
        method: 'PUT',
        body: { role: 'admin' },
        expectedStatus: 409
      });
      assert.equal(
        (await adminClient.get(`/api/admin/users/${superAdmin.id}`)).data.user.role,
        'super_admin',
        'The last Super Administrator must remain protected'
      );

      const usersTab = tablist.getByRole('tab', { name: 'User management', exact: true });
      await usersTab.click();
      assert.equal(await usersTab.getAttribute('aria-selected'), 'true');
      const usersTable = page.getByRole('table', { name: 'User management' });
      await assertVisible(usersTable, 'User management table');
      const currentUserIdentity = page.locator('.administration-user-identity > strong')
        .filter({ hasText: superAdmin.username });
      const memberIdentity = page.locator('.administration-user-identity > strong')
        .filter({ hasText: memberUsername });
      const currentUserRow = usersTable.locator('tbody tr').filter({ has: currentUserIdentity }).first();
      const memberRow = usersTable.locator('tbody tr').filter({ has: memberIdentity }).first();
      await assertVisible(currentUserRow, 'Current Super Administrator row');
      await assertVisible(memberRow, 'Registered member row');
      assert.equal(
        await currentUserRow.getByRole('button', { name: `Delete user: ${superAdmin.username}` }).isDisabled(),
        true,
        'Current Super Administrator delete action must be protected'
      );
      await assertVisible(currentUserRow.getByText('super_admin', { exact: true }), 'Super Administrator role');
      await assertVisible(memberRow.getByText('admin', { exact: true }), 'API-updated member role');

      const userDetailResponse = page.waitForResponse((response) => (
        response.request().method() === 'GET'
        && new URL(response.url()).pathname === `/api/admin/users/${registration.user.id}`
      ));
      await memberRow.getByRole('button', { name: `User information: ${memberUsername}` }).click();
      assert.equal((await userDetailResponse).ok(), true, 'User detail request must succeed');
      const detailsDialog = page.getByRole('dialog', { name: 'User information' });
      await assertVisible(detailsDialog, 'User details dialog');
      const detailsText = await detailsDialog.textContent();
      assert.ok(detailsText.includes(memberUsername));
      assert.ok(detailsText.includes(memberEmail));
      assert.ok(detailsText.includes('admin'));
      await detailsDialog.locator('.modal-actions').getByRole('button', { name: 'Close' }).click();

      await memberRow.getByRole('button', { name: `Set user password: ${memberUsername}` }).click();
      const passwordDialog = page.getByRole('dialog', { name: 'Set user password' });
      await assertVisible(passwordDialog, 'Set user password dialog');
      await passwordDialog.getByLabel('Password').fill(replacementPassword);
      const passwordUpdateResponse = page.waitForResponse((response) => (
        response.request().method() === 'PUT'
        && new URL(response.url()).pathname === `/api/admin/users/${registration.user.id}/password`
      ));
      await passwordDialog.getByRole('button', { name: 'Save changes' }).click();
      assert.equal((await passwordUpdateResponse).ok(), true, 'User password update must succeed');
      await assertVisible(page.getByText('Changes saved', { exact: true }), 'Password update notice');
      await memberClient.get('/api/auth/me', { expectedStatus: 401 });
      const replacementSession = new ApiClient(context.baseURL);
      assert.equal((await replacementSession.post('/api/auth/login', {
        email: memberEmail,
        password: replacementPassword
      })).data.user.id, registration.user.id);
      await assertNoHorizontalOverflow(page, 'Administration desktop users');

      await page.setViewportSize({ width: 390, height: 844 });
      await page.getByLabel('Language').selectOption('zh-CN');
      await assertVisible(page.getByRole('heading', { name: '管理', level: 1 }), 'Chinese Administration title');
      const chineseUsersTable = page.getByRole('table', { name: '用户管理' });
      await assertVisible(chineseUsersTable, 'Chinese User management table');
      await assertNoHorizontalOverflow(page, 'Administration 390px users');
      await page.getByRole('tab', { name: '外部平台', exact: true }).click();
      await assertVisible(page.getByRole('table', { name: '外部平台' }), 'Chinese External platforms table');
      await assertNoHorizontalOverflow(page, 'Administration 390px platforms');
      await page.getByRole('tab', { name: '用户管理', exact: true }).click();
      await assertVisible(chineseUsersTable, 'Chinese User management table after tab switch');

      const mobileMemberRow = chineseUsersTable.locator('tbody tr').filter({ has: memberIdentity }).first();
      await mobileMemberRow.getByRole('button', { name: `删除用户: ${memberUsername}` }).click();
      const eraseDialog = page.getByRole('dialog', { name: '删除用户' });
      await assertVisible(eraseDialog, 'Delete user dialog');
      const eraseAction = eraseDialog.getByRole('button', { name: '删除用户', exact: true });
      await eraseDialog.getByLabel('确认用户名').fill(`${memberUsername}-wrong`);
      assert.equal(await eraseAction.isDisabled(), true, 'Inexact username must not enable deletion');
      await eraseDialog.getByLabel('确认用户名').fill(memberUsername);
      assert.equal(await eraseAction.isEnabled(), true, 'Exact username must enable deletion');
      const erasureResponse = page.waitForResponse((response) => (
        response.request().method() === 'POST'
        && new URL(response.url()).pathname === `/api/admin/users/${registration.user.id}/erase`
      ));
      await eraseAction.click();
      const acceptedErasureResponse = await erasureResponse;
      assert.equal(acceptedErasureResponse.status(), 202, 'User erasure must be accepted');
      const erasure = await acceptedErasureResponse.json();
      assert.equal(erasure.user_id, registration.user.id);
      await mobileMemberRow.waitFor({ state: 'detached' });
      const historyIdentity = erasure.username ?? erasure.user_id;
      const erasureEntry = page.locator('.erasure-history > div').filter({ hasText: historyIdentity }).first();
      await assertVisible(erasureEntry, 'User erasure history entry');
      await assertNoHorizontalOverflow(page, 'Administration 390px erasure history');
      assert.deepEqual(browserErrors, [], 'Browser diagnostics must remain empty');
    });
  } finally {
    await restorePolicy(adminClient, policySnapshot);
  }
}
