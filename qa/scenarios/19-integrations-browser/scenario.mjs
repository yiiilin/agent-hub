import assert from 'node:assert/strict';
import { ApiClient, loginAsAdmin } from '../../support/api.mjs';
import { withBrowser } from '../../support/browser.mjs';

function uniqueSlug(context, prefix) {
  return context.unique(prefix)
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, '-')
    .replace(/^-|-$/g, '');
}

async function responseJson(response, label) {
  assert.equal(response.ok(), true, `${label} must succeed (status ${response.status()})`);
  return response.json();
}

function waitForResponse(page, method, pathname) {
  return page.waitForResponse((response) => (
    response.request().method() === method
    && new URL(response.url()).pathname === pathname
  ));
}

async function assertNoHorizontalOverflow(page, label) {
  await page.evaluate(() => new Promise((resolve) => {
    requestAnimationFrame(() => requestAnimationFrame(resolve));
  }));
  const overflow = await page.evaluate(() => (
    document.documentElement.scrollWidth - document.documentElement.clientWidth
  ));
  assert.ok(overflow <= 1, `${label} horizontal overflow: ${overflow}px`);
}

async function assertDialogFitsViewport(page, dialog, label) {
  const viewport = page.viewportSize();
  const box = await dialog.boundingBox();
  assert.ok(viewport, `${label} must have a viewport`);
  assert.ok(box, `${label} must have visible geometry`);
  assert.ok(box.x >= -1, `${label} must not escape the left edge`);
  assert.ok(box.x + box.width <= viewport.width + 1, `${label} must not escape the right edge`);
}

async function assertNoSensitiveDom(page, label) {
  const hasSensitiveValue = await page.locator('body').evaluate((body) => {
    const secretPattern = /\b(?:ahs|ahe)_[A-Za-z0-9._-]+\b/;
    if (secretPattern.test(body.textContent ?? '')) return true;
    return [...body.querySelectorAll('[href]')]
      .some((element) => secretPattern.test(element.getAttribute('href') ?? ''));
  });
  assert.equal(hasSensitiveValue, false, `${label} must not retain a client secret or Widget token in the DOM`);
}

async function redactAndCloseSensitiveDialog(page, dialog) {
  if (await dialog.count() === 0) return;

  await dialog.locator('code').evaluateAll((elements) => {
    for (const element of elements) element.textContent = '[redacted]';
  }).catch(() => undefined);
  await dialog.locator('a[href]').evaluateAll((elements) => {
    for (const element of elements) element.setAttribute('href', 'about:blank');
  }).catch(() => undefined);
  await page.evaluate(() => navigator.clipboard.writeText('[redacted]')).catch(() => undefined);

  const closeButton = dialog.getByRole('button', { name: /^(Close|关闭)$/ }).last();
  if (await closeButton.count() > 0) {
    await closeButton.click({ timeout: 2_000 }).catch(() => page.keyboard.press('Escape'));
  } else {
    await page.keyboard.press('Escape').catch(() => undefined);
  }
  await dialog.waitFor({ state: 'detached', timeout: 2_000 }).catch(() => undefined);
}

async function withSensitiveTracingPaused({ context, page, dialog }, run) {
  await context.tracing.stop();
  let result;
  let runError;
  try {
    result = await run();
  } catch (error) {
    runError = error;
  }

  let cleanupError;
  try {
    await redactAndCloseSensitiveDialog(page, dialog);
    await assertNoSensitiveDom(page, 'Sensitive-flow cleanup');
  } catch (error) {
    cleanupError = error;
  }

  let tracingError;
  try {
    await context.tracing.start({ screenshots: true, snapshots: true, sources: true });
  } catch (error) {
    tracingError = error;
  }

  const errors = [runError, cleanupError, tracingError].filter(Boolean);
  if (errors.length === 1) throw errors[0];
  if (errors.length > 1) throw new AggregateError(errors, 'Sensitive browser flow failed after redaction');
  return result;
}

async function readAndCopyClientSecret(page, secretDialog, label) {
  await secretDialog.waitFor();
  await secretDialog.getByText(
    'This client secret is shown once. Store it before closing.',
    { exact: true }
  ).waitFor();
  const codes = secretDialog.locator('code');
  assert.equal(await codes.count(), 2, `${label} must show client ID and client secret code surfaces`);
  const secret = await codes.nth(1).innerText();
  assert.equal(
    /^ahs_[A-Za-z0-9]+$/.test(secret),
    true,
    `${label} must expose one opaque client secret`
  );

  await secretDialog.getByRole('button', { name: 'Copy client secret' }).click();
  const copied = await page.evaluate(() => navigator.clipboard.readText());
  assert.equal(copied === secret, true, `${label} copy action must copy the displayed client secret`);
  return secret;
}

function appRow(table, appName) {
  return table.getByRole('row').filter({ hasText: appName });
}

export default async function integrationsBrowserScenario(scenarioContext) {
  const adminClient = new ApiClient(scenarioContext.baseURL);
  await loginAsAdmin(adminClient);

  const platformKey = uniqueSlug(scenarioContext, 'qa-integrations-platform');
  const platformName = scenarioContext.unique('QA Integrations Platform');
  const channelKey = uniqueSlug(scenarioContext, 'qa-integrations-channel');
  const channelName = scenarioContext.unique('QA Trusted Channel');
  const { data: platform } = await adminClient.post('/api/admin/external-platforms', {
    key: platformKey,
    name: platformName
  });
  const { data: channel } = await adminClient.post(
    `/api/admin/external-platforms/${platform.id}/authentication-channels`,
    { key: channelKey, name: channelName, enabled: true, trusted_email: true }
  );
  assert.equal(channel.enabled && channel.trusted_email, true, 'Fixture channel must be enabled and trusted');

  const createdAgentIds = [];
  let scenarioFailure;
  try {
    await withBrowser(scenarioContext, {
      allowedHttpErrors: [
        { method: 'GET', pathname: '/api/auth/me', status: 401, times: 1 }
      ]
    }, async ({ page, context, request, browserErrors }) => {
      const ownerSlug = uniqueSlug(scenarioContext, 'qa-integrations-owner');
      const ownerEmail = `${ownerSlug}@example.com`;
      await page.goto('/login', { waitUntil: 'domcontentloaded' });
      await page.getByLabel('Email').fill(ownerEmail);
      await page.getByRole('button', { name: 'Sign in with Mock OIDC' }).click();
      await page.waitForURL((url) => url.pathname === '/sessions');
      await page.getByText(ownerEmail, { exact: true }).waitFor();

      const owner = await responseJson(await request.get('/api/auth/me'), 'Read Integration App owner');
      assert.equal(owner.email, ownerEmail);
      const { data: promotedOwner } = await adminClient.request(
        `/api/admin/users/${owner.id}/role`,
        { method: 'PUT', body: { role: 'admin' } }
      );
      assert.equal(promotedOwner.user.role, 'admin', 'Integration App owner must be promoted to administrator');
      const browserOwner = await responseJson(
        await request.get('/api/auth/me'),
        'Read promoted Integration App owner'
      );
      assert.equal(browserOwner.role, 'admin', 'Browser session must observe the administrator role');

      const agentNames = [
        scenarioContext.unique('QA Integration Alpha'),
        scenarioContext.unique('QA Integration Beta'),
        scenarioContext.unique('QA Integration Gamma')
      ];
      const agents = [];
      for (const name of agentNames) {
        const agent = await responseJson(await request.post('/api/agents', {
          data: {
            name,
            instructions: `Integration App browser fixture for ${name}.`,
            visibility: 'private',
            public_to: []
          }
        }), `Create Agent ${name}`);
        agents.push(agent);
        createdAgentIds.push(agent.id);
      }

      await context.grantPermissions(
        ['clipboard-read', 'clipboard-write'],
        { origin: new URL(scenarioContext.baseURL).origin }
      );
      await page.goto('/integrations', { waitUntil: 'domcontentloaded' });
      await page.getByRole('heading', { name: 'Integration Apps', level: 1 }).waitFor();
      await page.getByText('No Integration Apps yet.', { exact: true }).waitFor();
      assert.equal(
        await page.getByRole('table', { name: 'Integration App list' }).count(),
        0,
        'A unique Integration App owner must start on the empty state'
      );
      await assertNoHorizontalOverflow(page, 'Desktop Integration Apps empty state');

      const options = await responseJson(
        await request.get('/api/integration-app-options'),
        'Read Integration App options'
      );
      assert.equal(
        options.external_platforms.some((candidate) => candidate.id === platform.id),
        true,
        'Trusted fixture platform must be selectable'
      );
      assert.equal(
        options.authentication_channels.some((candidate) => (
          candidate.id === channel.id
          && candidate.platform_id === platform.id
          && candidate.enabled
          && candidate.trusted_email
        )),
        true,
        'Enabled trusted fixture channel must be selectable for its platform'
      );

      const createName = scenarioContext.unique('QA Integration App');
      const createRedirects = [
        `https://${ownerSlug}.example.com/oauth/callback`,
        `https://${ownerSlug}.example.com/oauth/secondary`
      ];
      await page.getByRole('button', { name: 'Create Integration App', exact: true }).first().click();
      const createDialog = page.getByRole('dialog', { name: 'Create Integration App' });
      await createDialog.waitFor();
      assert.equal(
        await page.getByRole('table', { name: 'Integration App list' }).count(),
        0,
        'Create must remain a subform over the empty-state page'
      );
      await createDialog.getByRole('textbox', { name: 'Name', exact: true }).fill(createName);
      await createDialog.getByRole('combobox', { name: 'External platform' }).selectOption(platform.id);
      await createDialog.getByRole('combobox', { name: 'Authentication channel' }).selectOption(channel.id);
      await createDialog.getByRole('textbox', { name: 'Redirect URI 1' }).fill(createRedirects[0]);
      await createDialog.getByRole('button', { name: 'Add redirect URI' }).click();
      await createDialog.getByRole('textbox', { name: 'Redirect URI 2' }).fill(createRedirects[1]);
      await createDialog.getByRole('checkbox', { name: `Delegate ${agents[0].name}` }).check();
      await createDialog.getByRole('checkbox', { name: `Delegate ${agents[1].name}` }).check();

      const createResponsePromise = waitForResponse(page, 'POST', '/api/integration-apps');
      const secretDialog = page.getByRole('dialog', { name: 'Integration App secret' });
      const created = await withSensitiveTracingPaused({ context, page, dialog: secretDialog }, async () => {
        await createDialog.getByRole('button', { name: 'Create Integration App', exact: true }).click();
        const response = await createResponsePromise;
        const body = await responseJson(response, 'Create Integration App');
        const requestBody = response.request().postDataJSON();
        assert.deepEqual(requestBody, {
          name: createName,
          external_platform_id: platform.id,
          authentication_channel_id: channel.id,
          redirect_uris: createRedirects,
          agent_ids: [agents[0].id, agents[1].id],
          widget_history_enabled: false,
          login_required: true,
          allowed_origins: [],
          tool_allowlist: null,
          client_tool_definitions: []
        });
        const clientSecret = await readAndCopyClientSecret(page, secretDialog, 'Created Integration App');
        return { app: body.integration_app, clientSecret };
      });

      assert.equal(await secretDialog.count(), 0, 'Closing creation secret must remove its one-time dialog');
      await assertNoSensitiveDom(page, 'After creation secret closes');
      const table = page.getByRole('table', { name: 'Integration App list' });
      await table.waitFor();
      assert.deepEqual(
        (await table.getByRole('columnheader').allTextContents()).map((value) => value.trim()),
        ['Name', 'Client ID', 'Platform / channel', 'Agents', 'Updated', 'Actions']
      );
      const createdRow = appRow(table, createName);
      await createdRow.waitFor();
      const createdRowText = await createdRow.innerText();
      assert.equal(createdRowText.includes(created.app.client_id), true, 'Table row must show the client ID');
      assert.equal(createdRowText.includes(platformName), true, 'Table row must show the selected platform');
      assert.equal(createdRowText.includes(channelName), true, 'Table row must show the selected channel');
      assert.equal(createdRowText.includes('2 agents'), true, 'Table row must summarize two delegated Agents');
      await assertNoHorizontalOverflow(page, 'Desktop Integration App table');

      const createdDetail = await responseJson(
        await request.get(`/api/integration-apps/${created.app.id}`),
        'Read created Integration App'
      );
      assert.equal(Object.hasOwn(createdDetail, 'client_secret'), false, 'Read response must not repeat the client secret');
      assert.deepEqual(createdDetail.redirect_uris, createRedirects);
      assert.equal(createdDetail.agent_ids.length, 2);
      assert.equal(createdDetail.login_required, true);
      assert.deepEqual(createdDetail.allowed_origins, []);
      assert.deepEqual(createdDetail.client_tool_definitions, []);

      await page.reload({ waitUntil: 'networkidle' });
      await appRow(page.getByRole('table', { name: 'Integration App list' }), createName).waitFor();
      assert.equal(
        await page.getByRole('dialog', { name: 'Integration App secret' }).count(),
        0,
        'Refresh must not restore a client secret dialog'
      );
      await assertNoSensitiveDom(page, 'After refresh following creation');

      await page.getByRole('button', { name: `Edit ${createName}` }).click();
      const editDialog = page.getByRole('dialog', { name: 'Edit Integration App' });
      await editDialog.waitFor();
      await editDialog.getByText(platformName, { exact: true }).waitFor();
      await editDialog.getByText(channelName, { exact: true }).waitFor();
      assert.equal(
        await editDialog.getByRole('combobox', { name: 'External platform' }).count(),
        0,
        'External platform must be read-only while editing'
      );
      assert.equal(
        await editDialog.getByRole('combobox', { name: 'Authentication channel' }).count(),
        0,
        'Authentication channel must be read-only while editing'
      );

      const editedName = scenarioContext.unique('QA Integration App Edited');
      const editedRedirects = [
        `https://${ownerSlug}.example.com/oauth/edited`,
        `https://${ownerSlug}.example.com/oauth/fallback`
      ];
      await editDialog.getByRole('textbox', { name: 'Name', exact: true }).fill(editedName);
      await editDialog.getByRole('textbox', { name: 'Redirect URI 1' }).fill(editedRedirects[0]);
      await editDialog.getByRole('textbox', { name: 'Redirect URI 2' }).fill(editedRedirects[1]);
      await editDialog.getByRole('checkbox', { name: `Delegate ${agents[0].name}` }).uncheck();
      await editDialog.getByRole('checkbox', { name: `Delegate ${agents[2].name}` }).check();
      const updateResponsePromise = waitForResponse(
        page,
        'PATCH',
        `/api/integration-apps/${created.app.id}`
      );
      await editDialog.getByRole('button', { name: 'Save changes', exact: true }).click();
      const updateResponse = await updateResponsePromise;
      const updated = await responseJson(updateResponse, 'Update Integration App');
      const updateBody = updateResponse.request().postDataJSON();
      assert.deepEqual(updateBody, {
        name: editedName,
        redirect_uris: editedRedirects,
        agent_ids: [agents[1].id, agents[2].id],
        widget_history_enabled: false,
        login_required: true,
        allowed_origins: [],
        tool_allowlist: null,
        client_tool_definitions: []
      });
      assert.equal(Object.hasOwn(updateBody, 'external_platform_id'), false, 'Edit must not submit a replacement platform');
      assert.equal(Object.hasOwn(updateBody, 'authentication_channel_id'), false, 'Edit must not submit a replacement channel');
      assert.equal(updated.external_platform_id, platform.id, 'Edit must preserve the original platform');
      assert.equal(updated.authentication_channel_id, channel.id, 'Edit must preserve the original channel');
      assert.equal(updated.login_required, true);
      assert.deepEqual(updated.allowed_origins, []);
      assert.deepEqual(updated.client_tool_definitions, []);
      await editDialog.waitFor({ state: 'detached' });
      const editedRow = appRow(page.getByRole('table', { name: 'Integration App list' }), editedName);
      await editedRow.waitFor();
      assert.equal((await editedRow.innerText()).includes('2 agents'), true, 'Edited row must retain two delegated Agents');

      let rotateRequestCount = 0;
      const rotatePath = `/api/integration-apps/${created.app.id}/rotate-secret`;
      const countRotateRequest = (browserRequest) => {
        if (browserRequest.method() === 'POST' && new URL(browserRequest.url()).pathname === rotatePath) {
          rotateRequestCount += 1;
        }
      };
      page.on('request', countRotateRequest);
      let rotatedSecret;
      try {
        await page.getByRole('button', { name: `Rotate secret for ${editedName}` }).click();
        const rotateDialog = page.getByRole('dialog', { name: 'Rotate client secret' });
        await rotateDialog.waitFor();
        await rotateDialog.getByText(
          'The current client secret stops working immediately after rotation.',
          { exact: true }
        ).waitFor();
        assert.equal(rotateRequestCount, 0, 'Opening the rotation confirmation must not rotate the secret');
        const rotateResponsePromise = waitForResponse(page, 'POST', rotatePath);
        rotatedSecret = await withSensitiveTracingPaused({ context, page, dialog: secretDialog }, async () => {
          await rotateDialog.getByRole('button', { name: 'Rotate secret', exact: true }).click();
          const response = await rotateResponsePromise;
          const body = await responseJson(response, 'Rotate Integration App secret');
          assert.equal(rotateRequestCount, 1, 'Confirming rotation must send exactly one request');
          assert.equal(body.integration_app.id, created.app.id);
          return readAndCopyClientSecret(page, secretDialog, 'Rotated Integration App');
        });
      } finally {
        page.off('request', countRotateRequest);
      }
      assert.equal(rotatedSecret !== created.clientSecret, true, 'Rotation must return a new one-time client secret');
      assert.equal(await secretDialog.count(), 0, 'Closing rotated secret must remove its one-time dialog');
      await assertNoSensitiveDom(page, 'After rotated secret closes');

      await page.reload({ waitUntil: 'networkidle' });
      await appRow(page.getByRole('table', { name: 'Integration App list' }), editedName).waitFor();
      assert.equal(
        await page.getByRole('dialog', { name: 'Integration App secret' }).count(),
        0,
        'Refresh must not restore the rotated client secret dialog'
      );
      const refreshedDetail = await responseJson(
        await request.get(`/api/integration-apps/${created.app.id}`),
        'Read Integration App after rotation'
      );
      assert.equal(Object.hasOwn(refreshedDetail, 'client_secret'), false, 'Read after rotation must remain redacted');
      await assertNoSensitiveDom(page, 'After refresh following rotation');

      await page.getByRole('button', { name: `Widget links for ${editedName}` }).click();
      const widgetDialog = page.getByRole('dialog', { name: 'Widget links' });
      await widgetDialog.waitFor();
      const delegatedAgents = [agents[1], agents[2]];
      const widgetTokens = await withSensitiveTracingPaused({ context, page, dialog: widgetDialog }, async () => {
        const tokens = [];
        for (const agent of delegatedAgents) {
          const widgetPath = `/api/integration-apps/${created.app.id}/agents/${agent.id}/widget-session`;
          const responsePromise = waitForResponse(page, 'POST', widgetPath);
          await widgetDialog.getByRole('button', { name: `Generate link for ${agent.name}` }).click();
          const body = await responseJson(await responsePromise, `Generate Widget link for ${agent.name}`);
          assert.equal(/^ahe_[A-Za-z0-9]+$/.test(body.token), true, 'Widget response must contain one opaque token');
          const openLink = widgetDialog.getByRole('link', { name: `Open Widget for ${agent.name}` });
          await openLink.waitFor();
          const href = await openLink.getAttribute('href');
          assert.equal(typeof href === 'string', true, 'Generated Widget link must have an href');
          const link = new URL(href, scenarioContext.baseURL);
          const fragment = new URLSearchParams(link.hash.slice(1));
          assert.equal(link.pathname === '/widget', true, 'Widget link must target /widget');
          assert.equal(link.search === '', true, 'Widget token must never appear in the query string');
          assert.equal(link.hash.startsWith('#token='), true, 'Widget token must be carried in the fragment');
          assert.equal(fragment.get('token') === body.token, true, 'Widget fragment must contain the response token');
          assert.deepEqual([...fragment.keys()], ['token']);
          await widgetDialog.getByRole('button', { name: `Copy Widget link for ${agent.name}` }).click();
          const copiedLink = await page.evaluate(() => navigator.clipboard.readText());
          assert.equal(copiedLink === href, true, 'Widget copy action must copy the Agent-specific link');
          tokens.push(body.token);
        }
        return tokens;
      });
      assert.equal(widgetTokens.length, 2, 'Each delegated Agent must receive a Widget link');
      assert.equal(widgetTokens[0] !== widgetTokens[1], true, 'Delegated Agents must receive distinct Widget tokens');
      assert.equal(await widgetDialog.count(), 0, 'Closing Widget links must remove token-bearing DOM');
      await assertNoSensitiveDom(page, 'After Widget links close');

      const anonymousName = scenarioContext.unique('QA Anonymous Integration App');
      const anonymousEditedName = scenarioContext.unique('QA Anonymous Integration App Edited');
      const anonymousRedirects = [`https://${ownerSlug}.example.com/public/callback`];
      const anonymousOrigins = [
        `http://${ownerSlug}.example.com:8080`,
        `https://${ownerSlug}.example.com`
      ];
      const anonymousEditedOrigins = [
        anonymousOrigins[0],
        `https://${ownerSlug}.example.com:8443`
      ];
      const anonymousTool = {
        name: 'show_notice',
        description: 'Show a notice in the host application.',
        input_schema: {
          type: 'object',
          properties: { message: { type: 'string' } },
          required: ['message']
        }
      };
      const anonymousEditedTool = {
        ...anonymousTool,
        description: 'Show a persistent notice in the host application.'
      };

      await page.getByRole('button', { name: 'Create Integration App', exact: true }).first().click();
      const anonymousCreateDialog = page.getByRole('dialog', { name: 'Create Integration App' });
      await anonymousCreateDialog.waitFor();
      await anonymousCreateDialog.getByRole('textbox', { name: 'Name', exact: true }).fill(anonymousName);
      await anonymousCreateDialog.getByRole('combobox', { name: 'External platform' }).selectOption(platform.id);
      await anonymousCreateDialog.getByRole('combobox', { name: 'Authentication channel' }).selectOption(channel.id);
      await anonymousCreateDialog.getByRole('textbox', { name: 'Redirect URI 1' }).fill(anonymousRedirects[0]);
      await anonymousCreateDialog.getByRole('checkbox', { name: 'Allow anonymous public Widget' }).check();
      await anonymousCreateDialog.getByRole('textbox', { name: 'Allowed Origin 1' }).fill(anonymousOrigins[0]);
      await anonymousCreateDialog.getByText(
        'HTTP Origins expose bearer credentials to network interception and may be blocked as mixed content.',
        { exact: true }
      ).waitFor();
      await anonymousCreateDialog.getByRole('radio', { name: `Delegate ${agents[0].name}` }).check();
      await anonymousCreateDialog.getByRole('button', { name: 'Add allowed Origin' }).click();
      await anonymousCreateDialog.getByRole('textbox', { name: 'Allowed Origin 2' }).fill(anonymousOrigins[1]);

      await anonymousCreateDialog.getByRole('button', { name: 'Add Client Tool' }).click();
      const validationToolDialog = page.getByRole('dialog', { name: 'Add Client Tool' });
      const validationToolName = validationToolDialog.getByRole('textbox', { name: 'Tool name' });
      const validationToolDescription = validationToolDialog.getByRole('textbox', { name: 'Description' });
      const validationToolSchema = validationToolDialog.getByRole('textbox', { name: 'input_schema' });
      await validationToolName.fill('bad name');
      await validationToolDescription.fill('Open a panel in the host application.');
      await validationToolDialog.getByRole('button', { name: 'Add Client Tool' }).click();
      assert.match(await validationToolDialog.getByRole('alert').innerText(), /unique 1-64 character name/);
      await validationToolName.fill('open_panel');
      await validationToolDescription.fill('   ');
      await validationToolDialog.getByRole('button', { name: 'Add Client Tool' }).click();
      assert.equal(await validationToolDialog.getByRole('alert').innerText(), 'Description is required.');
      await validationToolDescription.fill('Open a panel in the host application.');
      await validationToolSchema.fill('{broken');
      await validationToolDialog.getByRole('button', { name: 'Add Client Tool' }).click();
      assert.match(await validationToolDialog.getByRole('alert').innerText(), /must be valid JSON/);
      await validationToolSchema.fill('{"type":"string"}');
      await validationToolDialog.getByRole('button', { name: 'Add Client Tool' }).click();
      assert.match(await validationToolDialog.getByRole('alert').innerText(), /type set to object/);
      await validationToolSchema.fill(JSON.stringify({
        type: 'object',
        properties: { panel: { type: 'string' } }
      }));
      await validationToolDialog.getByRole('button', { name: 'Add Client Tool' }).click();

      const anonymousToolList = anonymousCreateDialog.getByRole('table', { name: 'Client Tool list' });
      await anonymousToolList.getByText('open_panel', { exact: true }).waitFor();
      await anonymousCreateDialog.getByRole('button', { name: 'Edit Client Tool open_panel' }).click();
      const editValidationToolDialog = page.getByRole('dialog', { name: 'Edit Client Tool' });
      await editValidationToolDialog.getByRole('textbox', { name: 'Description' }).fill('Open the selected panel.');
      await editValidationToolDialog.getByRole('button', { name: 'Save changes' }).click();
      await anonymousToolList.getByText('Open the selected panel.', { exact: true }).waitFor();
      await anonymousCreateDialog.getByRole('button', { name: 'Delete Client Tool open_panel' }).click();
      await anonymousCreateDialog.getByText('No Client Tools configured.', { exact: true }).waitFor();
      assert.equal(await anonymousToolList.count(), 0, 'Deleted Client Tool must leave no table behind');

      await anonymousCreateDialog.getByRole('button', { name: 'Add Client Tool' }).click();
      const finalToolDialog = page.getByRole('dialog', { name: 'Add Client Tool' });
      await finalToolDialog.getByRole('textbox', { name: 'Tool name' }).fill(anonymousTool.name);
      await finalToolDialog.getByRole('textbox', { name: 'Description' }).fill(anonymousTool.description);
      await finalToolDialog.getByRole('textbox', { name: 'input_schema' }).fill(JSON.stringify(anonymousTool.input_schema));
      await finalToolDialog.getByRole('button', { name: 'Add Client Tool' }).click();
      await anonymousCreateDialog.getByRole('table', { name: 'Client Tool list' })
        .getByText(anonymousTool.name, { exact: true }).waitFor();

      await anonymousCreateDialog.getByRole('checkbox', { name: "Further restrict this App's tools" }).check();
      await anonymousCreateDialog.getByRole('checkbox', { name: 'read', exact: true }).check();
      await anonymousCreateDialog.getByRole('checkbox', { name: 'grep', exact: true }).check();
      const integrationTool = anonymousCreateDialog.getByRole('checkbox', { name: 'integration', exact: true });
      assert.equal(await integrationTool.isChecked(), true, 'Client Tools must force integration into the tool allowlist');
      await integrationTool.click();
      assert.equal(await integrationTool.isChecked(), true, 'integration must not be removable while Client Tools exist');

      const anonymousCreateResponsePromise = waitForResponse(page, 'POST', '/api/integration-apps');
      const anonymousSecretDialog = page.getByRole('dialog', { name: 'Integration App secret' });
      const anonymousCreated = await withSensitiveTracingPaused({
        context,
        page,
        dialog: anonymousSecretDialog
      }, async () => {
        await anonymousCreateDialog.getByRole('button', { name: 'Create Integration App', exact: true }).click();
        const response = await anonymousCreateResponsePromise;
        const body = await responseJson(response, 'Create anonymous Integration App');
        assert.deepEqual(response.request().postDataJSON(), {
          name: anonymousName,
          external_platform_id: platform.id,
          authentication_channel_id: channel.id,
          redirect_uris: anonymousRedirects,
          agent_ids: [agents[0].id],
          widget_history_enabled: false,
          login_required: false,
          allowed_origins: anonymousOrigins,
          tool_allowlist: ['read', 'grep', 'integration'],
          client_tool_definitions: [anonymousTool]
        });
        await readAndCopyClientSecret(
          page,
          anonymousSecretDialog,
          'Created anonymous Integration App'
        );
        return body.integration_app;
      });
      assert.equal(await anonymousSecretDialog.count(), 0, 'Closing anonymous creation secret must remove its dialog');
      await assertNoSensitiveDom(page, 'After anonymous creation secret closes');

      const anonymousRow = appRow(page.getByRole('table', { name: 'Integration App list' }), anonymousName);
      await anonymousRow.waitFor();
      assert.equal(
        (await anonymousRow.innerText()).includes(agents[0].name),
        true,
        'Anonymous Integration App must remain fixed to one Agent'
      );
      const anonymousCreatedDetail = await responseJson(
        await request.get(`/api/integration-apps/${anonymousCreated.id}`),
        'Read created anonymous Integration App'
      );
      assert.equal(Object.hasOwn(anonymousCreatedDetail, 'client_secret'), false, 'Anonymous detail must redact the secret');
      assert.equal(anonymousCreatedDetail.login_required, false);
      assert.deepEqual(anonymousCreatedDetail.allowed_origins, anonymousOrigins);
      assert.deepEqual(anonymousCreatedDetail.client_tool_definitions, [anonymousTool]);
      assert.deepEqual(anonymousCreatedDetail.agent_ids, [agents[0].id]);
      assert.deepEqual(anonymousCreatedDetail.tool_allowlist, ['read', 'grep', 'integration']);

      await page.reload({ waitUntil: 'networkidle' });
      await page.getByRole('button', { name: `Edit ${anonymousName}` }).click();
      const anonymousEditDialog = page.getByRole('dialog', { name: 'Edit Integration App' });
      await anonymousEditDialog.waitFor();
      assert.equal(
        await anonymousEditDialog.getByRole('checkbox', { name: 'Allow anonymous public Widget' }).isChecked(),
        true,
        'Persisted anonymous mode must be selected in the edit form'
      );
      assert.equal(
        await anonymousEditDialog.getByRole('radio', { name: `Delegate ${agents[0].name}` }).isChecked(),
        true,
        'Persisted anonymous Agent must remain selected'
      );
      const persistedOrigins = await Promise.all(anonymousOrigins.map((_, index) => (
        anonymousEditDialog.getByRole('textbox', { name: `Allowed Origin ${index + 1}` }).inputValue()
      )));
      assert.deepEqual(persistedOrigins, anonymousOrigins);
      assert.equal(
        await anonymousEditDialog.getByRole('checkbox', { name: 'integration', exact: true }).isChecked(),
        true,
        'Persisted anonymous tool allowlist must include integration'
      );

      await anonymousEditDialog.getByRole('textbox', { name: 'Name', exact: true }).fill(anonymousEditedName);
      await anonymousEditDialog.getByRole('textbox', { name: 'Allowed Origin 2' }).fill(anonymousEditedOrigins[1]);
      await anonymousEditDialog.getByRole('button', { name: `Edit Client Tool ${anonymousTool.name}` }).click();
      const anonymousToolEditDialog = page.getByRole('dialog', { name: 'Edit Client Tool' });
      await anonymousToolEditDialog.waitFor();
      await anonymousToolEditDialog.getByRole('textbox', { name: 'Description' }).fill(anonymousEditedTool.description);
      await anonymousToolEditDialog.getByRole('button', { name: 'Save changes' }).click();
      await anonymousEditDialog.getByRole('table', { name: 'Client Tool list' })
        .getByText(anonymousEditedTool.description, { exact: true }).waitFor();

      const anonymousUpdatePath = `/api/integration-apps/${anonymousCreated.id}`;
      const anonymousUpdateResponsePromise = waitForResponse(page, 'PATCH', anonymousUpdatePath);
      await anonymousEditDialog.getByRole('button', { name: 'Save changes', exact: true }).click();
      const anonymousUpdateResponse = await anonymousUpdateResponsePromise;
      const anonymousUpdated = await responseJson(anonymousUpdateResponse, 'Update anonymous Integration App');
      assert.deepEqual(anonymousUpdateResponse.request().postDataJSON(), {
        name: anonymousEditedName,
        redirect_uris: anonymousRedirects,
        agent_ids: [agents[0].id],
        widget_history_enabled: false,
        login_required: false,
        allowed_origins: anonymousEditedOrigins,
        tool_allowlist: ['read', 'grep', 'integration'],
        client_tool_definitions: [anonymousEditedTool]
      });
      assert.equal(anonymousUpdated.login_required, false);
      assert.deepEqual(anonymousUpdated.allowed_origins, anonymousEditedOrigins);
      assert.deepEqual(anonymousUpdated.client_tool_definitions, [anonymousEditedTool]);
      await anonymousEditDialog.waitFor({ state: 'detached' });

      const anonymousUpdatedDetail = await responseJson(
        await request.get(anonymousUpdatePath),
        'Read updated anonymous Integration App'
      );
      assert.equal(Object.hasOwn(anonymousUpdatedDetail, 'client_secret'), false, 'Updated anonymous detail must redact the secret');
      assert.equal(anonymousUpdatedDetail.login_required, false);
      assert.deepEqual(anonymousUpdatedDetail.allowed_origins, anonymousEditedOrigins);
      assert.deepEqual(anonymousUpdatedDetail.client_tool_definitions, [anonymousEditedTool]);
      assert.deepEqual(anonymousUpdatedDetail.agent_ids, [agents[0].id]);
      assert.deepEqual(anonymousUpdatedDetail.tool_allowlist, ['read', 'grep', 'integration']);
      await appRow(page.getByRole('table', { name: 'Integration App list' }), anonymousEditedName).waitFor();
      await assertNoHorizontalOverflow(page, 'Desktop Integration App table with anonymous App');

      await page.setViewportSize({ width: 390, height: 844 });
      await page.getByLabel('Language').selectOption('zh-CN');
      await page.getByRole('heading', { name: '集成应用', level: 1 }).waitFor();
      const chineseTable = page.getByRole('table', { name: '集成应用列表' });
      await appRow(chineseTable, editedName).waitFor();
      await assertNoHorizontalOverflow(page, 'Chinese Integration Apps at 390x844');
      await page.getByRole('button', { name: `编辑 ${editedName}` }).click();
      const chineseEditDialog = page.getByRole('dialog', { name: '编辑集成应用' });
      await chineseEditDialog.waitFor();
      await chineseEditDialog.getByText(platformName, { exact: true }).waitFor();
      await chineseEditDialog.getByText(channelName, { exact: true }).waitFor();
      assert.equal(
        await chineseEditDialog.getByRole('combobox', { name: '外部平台' }).count(),
        0,
        'Chinese edit dialog must also keep platform origin read-only'
      );
      await assertDialogFitsViewport(page, chineseEditDialog, 'Chinese Integration App edit dialog at 390x844');
      await assertNoHorizontalOverflow(page, 'Chinese Integration App edit dialog at 390x844');
      await chineseEditDialog.getByRole('button', { name: '取消', exact: true }).click();
      await chineseEditDialog.waitFor({ state: 'detached' });
      await assertNoHorizontalOverflow(page, 'Chinese Integration App table after dialog closes');
      await assertNoSensitiveDom(page, 'Completed Integration App browser scenario');
      assert.deepEqual(browserErrors, [], 'Integration App browser diagnostics must remain empty');
    });
  } catch (error) {
    scenarioFailure = error;
  }

  const cleanupErrors = [];
  for (const agentId of createdAgentIds.toReversed()) {
    try {
      await adminClient.delete(`/api/agents/${agentId}`, { expectedStatus: [204, 404] });
    } catch (error) {
      cleanupErrors.push(error);
    }
  }
  if (scenarioFailure && cleanupErrors.length === 0) throw scenarioFailure;
  if (scenarioFailure || cleanupErrors.length > 0) {
    throw new AggregateError(
      [scenarioFailure, ...cleanupErrors].filter(Boolean),
      'Integration App browser scenario or cleanup failed'
    );
  }
}
