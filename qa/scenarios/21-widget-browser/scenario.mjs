import assert from 'node:assert/strict';
import { Buffer } from 'node:buffer';
import { ApiClient, loginAsAdmin, poll } from '../../support/api.mjs';
import { withBrowser } from '../../support/browser.mjs';

const COMPLETION_TEXT = 'Fake model completed run through the Hub model proxy.';

function uniqueSlug(context, prefix) {
  return context.unique(prefix)
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, '-')
    .replace(/^-|-$/g, '');
}

function basicAuthorization(clientId, clientSecret) {
  return `Basic ${Buffer.from(`${clientId}:${clientSecret}`).toString('base64')}`;
}

async function createComposeModelFixture(client, context) {
  const { data: connection } = await client.post('/api/model-connections', {
    scope: 'personal',
    name: context.unique('QA Widget browser model'),
    base_url: 'http://fake-model-provider:8080',
    api_type: 'openai_responses',
    allowed_model_ids: ['hub-proxy-smoke'],
    api_key: 'dev-model-provider-api-key'
  });
  return {
    connectionId: connection.id,
    selection: { connection_id: connection.id, model_id: 'hub-proxy-smoke' }
  };
}

async function waitForHostMessage(page, accept, description) {
  return poll(async () => {
    const messages = await page.evaluate(() => (
      Array.isArray(window.widgetMessages) ? window.widgetMessages : []
    ));
    return messages.find(accept) ?? null;
  }, Boolean, { timeoutMs: 45_000, description });
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

async function assertWidgetNoHorizontalOverflow(iframe, label) {
  const overflow = await iframe.evaluate((element) => {
    const documentElement = element.contentDocument?.documentElement;
    if (!documentElement) return null;
    return documentElement.scrollWidth - documentElement.clientWidth;
  });
  assert.notEqual(overflow, null, `${label} must expose a same-origin document`);
  assert.ok(overflow <= 1, `${label} horizontal overflow: ${overflow}px`);
}

async function postToWidget(iframe, data) {
  await iframe.evaluate((element, message) => {
    element.contentWindow?.postMessage(message, '*');
  }, data);
}

export default async function widgetBrowserScenario(scenarioContext) {
  const adminClient = new ApiClient(scenarioContext.baseURL);
  await loginAsAdmin(adminClient);

  const agents = [];
  let modelConnectionId = null;
  try {
    const modelFixture = await createComposeModelFixture(adminClient, scenarioContext);
    modelConnectionId = modelFixture.connectionId;
    for (const suffix of ['Alpha', 'Beta']) {
      const { data: agent } = await adminClient.post('/api/agents', {
        name: scenarioContext.unique(`QA Widget ${suffix}`),
        instructions: `Widget browser fixture ${suffix}.`,
        visibility: 'private',
        public_to: [],
        model_selection: modelFixture.selection
      });
      agents.push(agent);
    }

    const { data: integrationOptions } = await adminClient.get('/api/integration-app-options');
    const trustedChannel = integrationOptions.authentication_channels.find((channel) => (
      channel.enabled && channel.trusted_email
    ));
    assert.ok(trustedChannel, 'A trusted Authentication Channel must be available');
    const externalPlatform = integrationOptions.external_platforms.find((platform) => (
      platform.id === trustedChannel.platform_id
    ));
    assert.ok(externalPlatform, 'The trusted Authentication Channel must belong to a platform');
    const { data: appSecret } = await adminClient.post('/api/integration-apps', {
      name: scenarioContext.unique('QA Widget browser app'),
      external_platform_id: externalPlatform.id,
      authentication_channel_id: trustedChannel.id,
      redirect_uris: [new URL('/qa-widget-browser/callback', scenarioContext.baseURL).href],
      agent_ids: agents.map((agent) => agent.id),
      widget_history_enabled: true
    });
    const integrationApp = appSecret.integration_app;
    const externalUserId = uniqueSlug(scenarioContext, 'qa-widget-browser-user');
    const externalTenantId = uniqueSlug(scenarioContext, 'qa-widget-browser-tenant');
    const sessions = [];
    for (const agent of agents) {
      const { data } = await adminClient.post('/api/widget/access', {
        agent_id: agent.id,
        external_user_id: externalUserId,
        tenant_id: externalTenantId,
        username: externalUserId,
        display_name: 'QA Widget Browser User',
        email: `${externalUserId}@example.com`,
        attributes: { scenario: 'widget-browser' }
      }, {
        headers: {
          authorization: basicAuthorization(integrationApp.client_id, appSecret.client_secret)
        }
      });
      assert.equal(typeof data.token, 'string', 'Widget access must return an opaque token');
      assert.equal(data.token.startsWith('ahw_'), true, 'Widget access must use the external prefix');
      assert.equal(data.history_enabled, true);
      sessions.push(data);
    }

    await withBrowser(scenarioContext, {
      allowedHttpErrors: [
        { method: 'POST', pathname: '/api/embed/exchange', status: 401, times: 1 }
      ]
    }, async ({ page, context, browserErrors }) => {
      await page.goto('/widget', { waitUntil: 'domcontentloaded' });
      await page.setContent(`
        <!doctype html>
        <html>
          <head>
            <style>
              html, body { margin: 0; width: 100%; min-height: 100%; overflow-x: hidden; }
              #widget-host { width: 100%; }
              iframe { display: block; width: 100%; height: 680px; border: 0; }
            </style>
          </head>
          <body><main id="widget-host"></main></body>
        </html>
      `);
      await page.evaluate(() => {
        window.widgetMessages = [];
        window.addEventListener('message', (event) => {
          if (event.data?.type?.startsWith('agent-hub:')) window.widgetMessages.push(event.data);
        });
        const iframe = document.createElement('iframe');
        iframe.title = 'Agent Hub Widget';
        iframe.src = '/widget';
        document.querySelector('#widget-host')?.appendChild(iframe);
      });

      const iframe = page.locator('iframe[title="Agent Hub Widget"]');
      const widget = page.frameLocator('iframe[title="Agent Hub Widget"]');
      await widget.getByText('Agent Widget', { exact: true }).waitFor();
      const initialReady = await waitForHostMessage(
        page,
        (message) => message.type === 'agent-hub:ready' && message.bound !== true,
        'Widget initial ready message'
      );
      assert.equal(initialReady.protocolVersion, 1);
      assert.equal(typeof initialReady.channelId, 'string');
      assert.ok(initialReady.channelId.length > 0, 'Widget ready message must include a channel nonce');
      let channelId = initialReady.channelId;

      let widgetSessionLoads = 0;
      page.on('request', (request) => {
        if (new URL(request.url()).pathname === '/api/widget/session') widgetSessionLoads += 1;
      });

      let tracingPaused = false;
      let releaseHeldRun = () => {};
      try {
        await context.tracing.stop();
        tracingPaused = true;

        await postToWidget(iframe, {
          type: 'agent-hub:init',
          channelId: 'wrong-channel',
          token: sessions[0].token
        });
        await page.waitForTimeout(100);
        assert.equal(widgetSessionLoads, 0, 'Wrong-channel init must not load a Widget session');

        await postToWidget(iframe, {
          type: 'agent-hub:init',
          channelId,
          token: sessions[0].token
        });
        const boundReady = await waitForHostMessage(
          page,
          (message) => message.type === 'agent-hub:ready' && message.bound === true,
          'Widget origin-bound ready message'
        );
        assert.equal(boundReady.channelId, channelId);
        assert.equal(boundReady.protocolVersion, 1);
        assert.equal(boundReady.sessionReady, true);
        await widget.getByRole('heading', { name: agents[0].name }).waitFor();
        assert.equal(widgetSessionLoads, 1, 'Selected Widget session must load exactly once');

        const resize = await waitForHostMessage(
          page,
          (message) => message.type === 'agent-hub:resize'
            && Number(message.width) > 0
            && Number(message.height) > 0,
          'Widget resize message'
        );
        assert.equal(resize.channelId, channelId);
        await assertNoHorizontalOverflow(page, 'Desktop Widget host');
        await assertWidgetNoHorizontalOverflow(iframe, 'Desktop Widget');

        let runRequests = 0;
        const runRequestBodies = [];
        const heldFirstRun = new Promise((resolve) => { releaseHeldRun = resolve; });
        await page.route('**/api/widget/runs', async (route) => {
          if (route.request().method() !== 'POST') {
            await route.continue();
            return;
          }
          runRequests += 1;
          runRequestBodies.push(route.request().postDataJSON());
          await heldFirstRun;
          await route.continue();
        });

        const firstMessage = scenarioContext.unique('Widget parent submission');
        const duplicateMessage = scenarioContext.unique('Widget duplicate submission');
        const preservedDraft = scenarioContext.unique('Widget draft survives failed exchange');
        await widget.getByRole('textbox', { name: 'Message' }).fill(preservedDraft);
        const runResponsePromise = page.waitForResponse((response) => (
          response.request().method() === 'POST'
          && new URL(response.url()).pathname === '/api/widget/runs'
        ));
        await postToWidget(iframe, {
          type: 'agent-hub:message-submit',
          channelId,
          message: firstMessage
        });
        await poll(() => runRequests, (count) => count === 1, {
          timeoutMs: 10_000,
          description: 'first Widget Run request to reach the submission gate'
        });
        await widget.getByRole('button', { name: 'Sending...' }).waitFor();
        assert.equal(await widget.getByRole('button', { name: 'Sending...' }).isDisabled(), true);

        const failedExchangeResponse = page.waitForResponse((response) => (
          response.request().method() === 'POST'
          && new URL(response.url()).pathname === '/api/embed/exchange'
        ));
        await postToWidget(iframe, {
          type: 'agent-hub:embed-jwt',
          channelId,
          jwt: 'not-a-valid-embed-jwt'
        });
        assert.equal((await failedExchangeResponse).status(), 401);
        assert.equal(
          await widget.getByRole('textbox', { name: 'Message' }).inputValue(),
          preservedDraft,
          'Failed JWT exchange must preserve the draft'
        );
        assert.equal(await widget.getByRole('button', { name: 'Sending...' }).isDisabled(), true);
        assert.equal(runRequests, 1, 'Failed JWT exchange must preserve the pending Run lock');

        await postToWidget(iframe, {
          type: 'agent-hub:session-select',
          channelId,
          token: sessions[0].token
        });
        await postToWidget(iframe, {
          type: 'agent-hub:message-submit',
          channelId,
          message: duplicateMessage
        });
        await page.waitForTimeout(150);
        assert.equal(runRequests, 1, 'Same-token session-select must not release the submit lock');
        assert.equal(widgetSessionLoads, 1, 'Same-token session-select must not reload the session');
        assert.equal(await widget.getByRole('button', { name: 'Sending...' }).isDisabled(), true);

        releaseHeldRun();
        const runResponse = await runResponsePromise;
        assert.equal(runResponse.ok(), true, `Widget Run must succeed (status ${runResponse.status()})`);
        const createdRun = await runResponse.json();
        assert.equal(runRequestBodies.length, 1);
        assert.equal(runRequestBodies[0].message, firstMessage);
        assert.equal(runRequestBodies[0].parent_run_id, null);
        assert.equal(createdRun.agent_id, agents[0].id);
        assert.ok(createdRun.id, 'Widget Run must return an id');
        const runStreamUrl = new URL(
          `/api/runs/${createdRun.id}/events/stream`,
          scenarioContext.baseURL
        ).href;

        const runStarted = await waitForHostMessage(
          page,
          (message) => message.type === 'agent-hub:run-started',
          'Widget run-started parent notification'
        );
        assert.equal(runStarted.channelId, channelId);
        assert.equal(runStarted.runId, createdRun.id);
        const assistantEvent = await waitForHostMessage(
          page,
          (message) => message.type === 'agent-hub:run-event'
            && message.event?.event_type === 'message'
            && message.event?.role === 'assistant',
          'Widget assistant SSE parent notification'
        );
        assert.equal(assistantEvent.channelId, channelId);
        assert.equal(assistantEvent.runId, createdRun.id);
        assert.equal(assistantEvent.event.content, COMPLETION_TEXT);
        const completedEvent = await waitForHostMessage(
          page,
          (message) => message.type === 'agent-hub:run-event'
            && message.runId === createdRun.id
            && message.event?.event_type === 'status'
            && (message.event?.content === 'completed' || message.event?.payload?.status === 'completed'),
          'Widget completed SSE parent notification'
        );
        assert.equal(completedEvent.channelId, channelId);
        await widget.getByText(COMPLETION_TEXT, { exact: true }).waitFor();
        await widget.getByRole('button', { name: 'Send' }).waitFor();
        assert.equal(await widget.getByRole('button', { name: 'Send' }).isEnabled(), true);

        const eventTypes = await page.evaluate(() => window.widgetMessages.map((message) => message.type));
        assert.ok(
          eventTypes.indexOf('agent-hub:run-started') < eventTypes.indexOf('agent-hub:run-event'),
          'run-started must precede streamed run-event notifications'
        );

        await widget.getByRole('button', { name: 'History' }).click();
        await widget.getByRole('button', { name: new RegExp(firstMessage) }).waitFor();
        await widget.getByRole('button', { name: 'Close history' }).click();
        await poll(async () => iframe.evaluate((element) => {
          const stored = element.contentWindow?.sessionStorage.getItem('agent-hub-widget-state-v1');
          return stored ? JSON.parse(stored) : null;
        }), (stored) => (
          stored?.draft === preservedDraft
          && stored?.target?.integrationSessionId === createdRun.integration_session_id
          && stored?.target?.hubSessionId === createdRun.hub_session_id
        ), {
          timeoutMs: 10_000,
          description: 'Widget draft and exact Session ids to reach sessionStorage'
        });

        await page.evaluate(() => { window.widgetMessages = []; });
        await iframe.evaluate((element) => element.contentWindow?.location.reload());
        await widget.getByRole('heading', { name: agents[0].name }).waitFor();
        await widget.getByText(COMPLETION_TEXT, { exact: true }).waitFor();
        assert.equal(
          await widget.getByRole('textbox', { name: 'Message' }).inputValue(),
          preservedDraft,
          'Reload must restore the current Widget draft'
        );
        await widget.getByRole('button', { name: 'History' }).waitFor();
        const reloadedReady = await waitForHostMessage(
          page,
          (message) => message.type === 'agent-hub:ready' && message.bound !== true,
          'reloaded Widget initial ready message'
        );
        channelId = reloadedReady.channelId;
        await postToWidget(iframe, {
          type: 'agent-hub:init',
          channelId,
          token: sessions[0].token
        });
        await waitForHostMessage(
          page,
          (message) => message.type === 'agent-hub:ready'
            && message.bound === true
            && message.channelId === channelId,
          'reloaded Widget origin-bound ready message'
        );
        assert.equal(widgetSessionLoads, 2, 'Reload must restore the same credential without extra selection');

        await postToWidget(iframe, {
          type: 'agent-hub:session-select',
          channelId,
          token: sessions[1].token
        });
        await widget.getByRole('heading', { name: agents[1].name }).waitFor();
        assert.equal(widgetSessionLoads, 3, 'A different selected credential must load once');
        assert.equal(await widget.getByRole('textbox', { name: 'Message' }).inputValue(), '');
        assert.equal(
          await widget.getByText(COMPLETION_TEXT, { exact: true }).count(),
          0,
          'Selecting another session must clear the previous Run output'
        );

        await page.setViewportSize({ width: 390, height: 844 });
        await widget.getByLabel('Language').selectOption('zh-CN');
        await widget.getByRole('button', { name: '发送' }).waitFor();
        await assertNoHorizontalOverflow(page, '390px Widget host');
        await assertWidgetNoHorizontalOverflow(iframe, '390px Widget');

        const allowedStreamAbort = `requestfailed: GET ${runStreamUrl}: net::ERR_ABORTED`;
        const unexpectedBrowserErrors = browserErrors.filter((error) => error !== allowedStreamAbort);
        browserErrors.splice(0, browserErrors.length, ...unexpectedBrowserErrors);
        assert.deepEqual(
          browserErrors,
          [],
          'Only the exact previous-session SSE abort may be ignored after session-select'
        );
      } finally {
        releaseHeldRun();
        if (tracingPaused) {
          await iframe.evaluate((element) => element.remove()).catch(() => undefined);
          await page.evaluate(() => {
            window.widgetMessages = [];
            document.querySelector('#widget-host')?.replaceChildren();
          }).catch(() => undefined);
          await context.tracing.start({ screenshots: true, snapshots: true, sources: true });
        }
      }
    });
  } finally {
    try {
      for (const agent of agents.reverse()) {
        await adminClient.delete(`/api/agents/${agent.id}`, { expectedStatus: [204, 404] });
      }
    } finally {
      if (modelConnectionId) {
        await adminClient.delete(`/api/model-connections/${modelConnectionId}`, {
          expectedStatus: [204, 404]
        });
      }
    }
  }
}
