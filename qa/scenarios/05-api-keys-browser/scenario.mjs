import assert from 'node:assert/strict';
import { withBrowser } from '../../support/browser.mjs';

const PLAYWRIGHT_MODULE = new URL('../../../frontend/node_modules/playwright/index.mjs', import.meta.url);

async function assertSessionsFirst(page, email) {
  await page.waitForURL((url) => url.pathname === '/sessions');
  const sessionsNavigation = page.getByRole('button', { name: 'Sessions', exact: true });
  await sessionsNavigation.waitFor();
  assert.equal(await sessionsNavigation.getAttribute('aria-current'), 'page', 'Sessions must be the active destination after login');
  await page.locator('.session-list[aria-busy="false"]').waitFor();
  await page.getByText(email, { exact: true }).waitFor();
}

async function assertNoHorizontalOverflow(page, label) {
  await page.waitForTimeout(100);
  const overflow = await page.evaluate(() => (
    document.documentElement.scrollWidth - document.documentElement.clientWidth
  ));
  assert.ok(overflow <= 1, `${label} horizontal overflow: ${overflow}px`);
}

async function closeOrRedactSecret(page, secretDialog) {
  if (await secretDialog.count() === 0) return;

  const closeButton = secretDialog.getByRole('button', { name: 'Close', exact: true }).last();
  await closeButton.click({ timeout: 2_000 }).catch(() => page.keyboard.press('Escape'));
  if (await secretDialog.count() > 0) {
    await secretDialog.locator('.secret-token').evaluateAll((elements) => {
      for (const element of elements) element.textContent = '[redacted]';
    }).catch(() => undefined);
  }
}

export default async function apiKeysBrowserScenario(scenarioContext) {
  await withBrowser(scenarioContext, {
    allowedHttpErrors: [
      { method: 'GET', pathname: '/api/auth/me', status: 401, times: 2 }
    ]
  }, async ({ page, context, request, browserErrors }) => {
    const adminEmail = 'admin@example.com';
    const allowedNoContentAborts = new Set();

    await page.goto('/login', { waitUntil: 'domcontentloaded' });
    await page.waitForLoadState('networkidle');
    await page.getByLabel('Email').fill(adminEmail);
    await page.getByLabel('Password').fill('admin123');
    await page.getByRole('button', { name: 'Sign in', exact: true }).click();
    await assertSessionsFirst(page, adminEmail);

    const passwordSessionMe = await request.get('/api/auth/me');
    assert.equal(passwordSessionMe.status(), 200, 'Password browser session must authenticate /auth/me');
    assert.equal((await passwordSessionMe.json()).email, adminEmail, 'Password browser session must belong to the signed-in user');
    await assertNoHorizontalOverflow(page, 'Desktop Sessions page');

    await page.getByRole('button', { name: 'API Keys', exact: true }).click();
    await page.waitForURL((url) => url.pathname === '/api-keys');
    await page.getByRole('button', { name: 'Create API key' }).waitFor();
    await assertNoHorizontalOverflow(page, 'Desktop API Keys page');

    const keyName = scenarioContext.unique('QA browser API key');
    await page.getByRole('button', { name: 'Create API key' }).click();
    const createDialog = page.getByRole('dialog', { name: 'Create API key' });
    await createDialog.waitFor();
    await createDialog.getByLabel('Name', { exact: true }).fill(keyName);
    const validity = createDialog.getByLabel('Validity');
    await validity.selectOption('180');
    assert.equal(await validity.inputValue(), '180', 'API key validity must be 180 days');

    const createResponsePromise = page.waitForResponse((response) => (
      response.request().method() === 'POST'
      && new URL(response.url()).pathname === '/api/auth/api-keys'
    ));
    const secretDialog = page.getByRole('dialog', { name: 'One-time API key' });
    let apiKeyToken;

    // Failure artifacts must never preserve the one-time credential.
    await context.tracing.stop();
    try {
      await createDialog.getByRole('button', { name: 'Create key' }).click();
      const createResponse = await createResponsePromise;
      assert.equal(createResponse.ok(), true, 'API key creation must succeed');
      await secretDialog.waitFor();
      assert.equal(await page.locator('.secret-token').count(), 1, 'The credential must appear only in the one-time dialog');

      apiKeyToken = await secretDialog.locator('.secret-token').innerText();
      if (!/^ahk_[A-Za-z0-9]+$/.test(apiKeyToken)) {
        throw new Error('The one-time API key credential had an unexpected format');
      }

      const copyControl = secretDialog.getByRole('button', { name: 'Copy API key' });
      await secretDialog.locator('.secret-token-line').hover();
      await page.waitForTimeout(180);
      assert.equal(await copyControl.evaluate((element) => getComputedStyle(element).opacity), '1', 'Copy control must appear on hover');

      const closeButton = secretDialog.getByRole('button', { name: 'Close', exact: true }).last();
      await closeButton.focus();
      await page.keyboard.press('Shift+Tab');
      assert.equal(
        await copyControl.evaluate((element) => element === document.activeElement),
        true,
        'Copy control must be keyboard focusable'
      );
    } finally {
      await closeOrRedactSecret(page, secretDialog);
      await context.tracing.start({ screenshots: true, snapshots: true, sources: true });
    }

    assert.equal(typeof apiKeyToken, 'string', 'API key credential must be retained only in scenario memory');
    await secretDialog.waitFor({ state: 'detached' });
    assert.equal(await page.locator('.secret-token').count(), 0, 'The one-time credential must disappear after the dialog closes');
    await page.getByRole('button', { name: 'Create API key' }).waitFor();

    const { request: playwrightRequest } = await import(PLAYWRIGHT_MODULE.href);
    const apiKeyRequest = await playwrightRequest.newContext({
      baseURL: scenarioContext.baseURL,
      extraHTTPHeaders: { Authorization: `Bearer ${apiKeyToken}` }
    });

    try {
      const apiKeyMe = await apiKeyRequest.get('/api/auth/me');
      assert.equal(apiKeyMe.status(), 200, 'An independent API key request context must authenticate /auth/me');
      assert.equal((await apiKeyMe.json()).email, adminEmail, 'The API key must authenticate as its owner');

      const keyRow = page.locator('.api-key-row', { hasText: keyName });
      await keyRow.waitFor();
      assert.equal(await keyRow.getByRole('button', { name: 'Revoke' }).count(), 0, 'API key rows must not expose Revoke');
      assert.equal(await keyRow.getByRole('button', { name: 'Delete' }).count(), 1, 'API key rows must expose physical Delete');

      await keyRow.getByRole('button', { name: 'Renew' }).click();
      const renewDialog = page.getByRole('dialog', { name: 'Renew API key' });
      await renewDialog.waitFor();
      assert.equal(await renewDialog.locator('.secret-token').count(), 0, 'Renewal must not display a credential');
      const renewResponsePromise = page.waitForResponse((response) => (
        response.request().method() === 'POST'
        && new URL(response.url()).pathname.endsWith('/renew')
      ));
      await renewDialog.getByRole('button', { name: 'Renew', exact: true }).click();
      const renewResponse = await renewResponsePromise;
      assert.equal(renewResponse.status(), 200, 'API key renewal must succeed');
      const renewBody = await renewResponse.json();
      assert.equal(Object.hasOwn(renewBody, 'token'), false, 'Renewal must not return a replacement credential');
      await renewDialog.waitFor({ state: 'detached' });
      assert.equal(await page.locator('.secret-token').count(), 0, 'Renewal must not reveal the one-time credential');
      assert.equal((await apiKeyRequest.get('/api/auth/me')).status(), 200, 'The original API key must remain valid after renewal');

      let confirmationMessage;
      page.once('dialog', async (dialog) => {
        confirmationMessage = dialog.message();
        await dialog.accept();
      });
      const deleteResponsePromise = page.waitForResponse((response) => (
        response.request().method() === 'DELETE'
        && new URL(response.url()).pathname.startsWith('/api/auth/api-keys/')
      ));
      await keyRow.getByRole('button', { name: 'Delete' }).click();
      const deleteResponse = await deleteResponsePromise;
      assert.equal(deleteResponse.status(), 204, 'API key deletion must physically remove the key');
      allowedNoContentAborts.add(`requestfailed: DELETE ${deleteResponse.url()}: net::ERR_ABORTED`);
      assert.equal(confirmationMessage, `Delete API key "${keyName}"? This cannot be undone.`, 'Delete must require irreversible-action confirmation');
      await keyRow.waitFor({ state: 'detached' });
      assert.equal((await apiKeyRequest.get('/api/auth/me')).status(), 401, 'A deleted API key must immediately stop authenticating');
    } finally {
      await apiKeyRequest.dispose();
    }

    await page.setViewportSize({ width: 390, height: 844 });
    await assertNoHorizontalOverflow(page, 'Mobile API Keys page');
    await page.setViewportSize({ width: 1280, height: 800 });

    const logoutResponsePromise = page.waitForResponse((response) => (
      response.request().method() === 'POST'
      && new URL(response.url()).pathname === '/api/auth/logout'
    ));
    await page.getByRole('button', { name: 'Log out', exact: true }).click();
    const logoutResponse = await logoutResponsePromise;
    assert.equal(logoutResponse.status(), 204, 'Logout must succeed');
    allowedNoContentAborts.add(`requestfailed: POST ${logoutResponse.url()}: net::ERR_ABORTED`);
    await page.waitForURL((url) => url.pathname === '/login');
    await page.getByRole('button', { name: 'Sign in', exact: true }).waitFor();
    assert.equal((await request.get('/api/auth/me')).status(), 401, 'Logout must invalidate the browser session');

    const oidcSuffix = scenarioContext.unique('oidc-user').replace(/[^a-z0-9]/gi, '').slice(-32).toLowerCase();
    const oidcEmail = `api-key-oidc-${oidcSuffix}@example.com`;
    await page.getByLabel('Email').fill(oidcEmail);
    await page.getByRole('button', { name: 'Sign in with Mock OIDC' }).click();
    await page.waitForURL((url) => url.pathname === '/agents');
    await page.getByText(oidcEmail, { exact: true }).waitFor();
    await page.getByText('Create Agent', { exact: true }).first().waitFor();

    const firstWorkspaceDestination = page.locator('.nav-group').first().getByRole('button').first();
    assert.equal((await firstWorkspaceDestination.innerText()).trim(), 'Sessions', 'Sessions must be the first workspace destination');
    await firstWorkspaceDestination.click();
    await assertSessionsFirst(page, oidcEmail);

    const oidcSessionMe = await request.get('/api/auth/me');
    assert.equal(oidcSessionMe.status(), 200, 'Mock OIDC browser session must authenticate /auth/me');
    assert.equal((await oidcSessionMe.json()).email, oidcEmail, 'Mock OIDC must bind the requested unique user');
    await page.setViewportSize({ width: 390, height: 844 });
    await assertNoHorizontalOverflow(page, 'Mobile Sessions page after Mock OIDC login');

    // Chromium reports these successful 204 fetches as aborted after the SPA moves on.
    const unexpectedBrowserErrors = browserErrors.filter((error) => !allowedNoContentAborts.has(error));
    browserErrors.splice(0, browserErrors.length, ...unexpectedBrowserErrors);
    assert.deepEqual(browserErrors, [], 'Browser diagnostics must remain empty');
  });
}
