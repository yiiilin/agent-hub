import assert from 'node:assert/strict';
import { poll } from '../../support/api.mjs';
import { withBrowser } from '../../support/browser.mjs';

async function responseJson(response, label) {
  const body = await response.text();
  assert.equal(response.ok(), true, `${label} returned ${response.status()}: ${body}`);
  return JSON.parse(body);
}

async function getJson(request, path, label) {
  return responseJson(await request.get(path), label);
}

async function createComposeModelFixture(request, scenarioContext) {
  const connection = await responseJson(await request.post('/api/model-connections', {
    data: {
      scope: 'personal',
      name: scenarioContext.unique('QA Pi Sessions model'),
      base_url: 'http://fake-model-provider:8080',
      api_type: 'openai_responses',
      allowed_model_ids: ['hub-proxy-smoke'],
      api_key: 'dev-model-provider-api-key'
    }
  }), 'Create Pi Sessions Model Connection');
  return {
    connectionId: connection.id,
    selection: { connection_id: connection.id, model_id: 'hub-proxy-smoke' }
  };
}

async function waitForRun(request, runId, expectedStatus, timeoutMs = 60_000) {
  return poll(
    () => getJson(request, `/api/runs/${runId}`, `Run ${runId}`),
    (run) => run.status === expectedStatus,
    { timeoutMs, description: `Run ${runId} to reach ${expectedStatus}` }
  );
}

async function waitForMessage(request, sessionId, content) {
  return poll(async () => {
    const messages = await getJson(request, `/api/sessions/${sessionId}/messages`,
      `Session ${sessionId} messages`);
    return messages.find((message) => message.content === content && message.role === 'user') ?? null;
  }, (message) => message?.delivery_state === 'delivered', {
    timeoutMs: 60_000,
    description: `Session ${sessionId} message ${JSON.stringify(content)}`
  });
}

async function assertComposerKeyboardAndSizing(composer) {
  const metrics = async () => composer.evaluate((element) => {
    const style = getComputedStyle(element);
    const lineHeight = Number.parseFloat(style.lineHeight);
    const chrome = Number.parseFloat(style.paddingTop) + Number.parseFloat(style.paddingBottom)
      + Number.parseFloat(style.borderTopWidth) + Number.parseFloat(style.borderBottomWidth);
    return {
      height: element.getBoundingClientRect().height,
      minimum: lineHeight * 2 + chrome,
      maximum: lineHeight * 5 + chrome,
      overflowY: style.overflowY
    };
  });

  const initial = await metrics();
  assert.ok(Math.abs(initial.height - initial.minimum) <= 1,
    `Composer minimum height: ${JSON.stringify(initial)}`);
  await composer.fill('first line');
  await composer.press('Shift+Enter');
  assert.equal(await composer.inputValue(), 'first line\n', 'Shift+Enter must insert a newline');
  await composer.fill('one\ntwo\nthree');
  const middle = await metrics();
  assert.ok(middle.height > initial.height && middle.height < middle.maximum,
    `Composer must grow between two and five lines: ${JSON.stringify(middle)}`);
  await composer.fill('one\ntwo\nthree\nfour\nfive\nsix');
  const maximum = await metrics();
  assert.ok(Math.abs(maximum.height - maximum.maximum) <= 1,
    `Composer maximum height: ${JSON.stringify(maximum)}`);
  assert.equal(maximum.overflowY, 'auto', 'Composer must scroll after five lines');
  await composer.fill('');
}

async function createConversation(page, request, agentId, message, { verifyComposer = false } = {}) {
  const sessionsBeforeDraft = await getJson(request, '/api/sessions', 'Sessions before Conversation Draft');
  const sessionIdsBeforeDraft = sessionsBeforeDraft.map((session) => session.id).sort();
  const list = page.getByRole('complementary', { name: 'Session list' });
  await list.getByRole('combobox', { name: 'Agent' }).selectOption(agentId);
  await page.getByRole('button', { name: 'New conversation' }).click();
  assert.equal(await page.getByRole('dialog', { name: 'New conversation' }).count(), 0);
  const composer = page.getByRole('region', { name: 'Session details' })
    .getByRole('textbox', { name: 'Message' });
  await composer.waitFor();
  if (verifyComposer) await assertComposerKeyboardAndSizing(composer);
  const sessionsDuringDraft = await getJson(request, '/api/sessions', 'Sessions during Conversation Draft');
  assert.deepEqual(sessionsDuringDraft.map((session) => session.id).sort(), sessionIdsBeforeDraft,
    'Selecting an Agent must not persist a Session before the first message');
  await composer.fill(message);
  const responsePromise = page.waitForResponse((response) => (
    response.request().method() === 'POST'
      && new URL(response.url()).pathname === `/api/agents/${agentId}/runs`
  ));
  await composer.press('Enter');
  return responseJson(await responsePromise, `Create conversation for Agent ${agentId}`);
}

async function sendNextTurn(page, request, sessionId, content) {
  const detail = page.getByRole('region', { name: 'Session details' });
  await detail.getByRole('textbox', { name: 'Message' }).fill(content);
  const responsePromise = page.waitForResponse((response) => (
    response.request().method() === 'POST'
      && new URL(response.url()).pathname === `/api/sessions/${sessionId}/messages`
  ));
  await detail.getByRole('button', { name: 'Send' }).click();
  return responseJson(await responsePromise, `Create next Pi Turn for Session ${sessionId}`);
}

async function assertMessageVisible(detail, message) {
  await detail.getByText(message, { exact: true }).waitFor();
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

async function waitForNativePiSession(request, sessionId, description) {
  return poll(
    () => getJson(request, `/api/sessions/${sessionId}`, `Session ${sessionId}`),
    (session) => session.active_turn_id === null
      && typeof session.native_session_id === 'string'
      && session.native_session_id.length > 0,
    { timeoutMs: 60_000, description }
  );
}

export default async function sessionsBrowserScenario(scenarioContext) {
  await withBrowser(scenarioContext, {
    allowedHttpErrors: [{ method: 'GET', pathname: '/api/auth/me', status: 401, times: 1 }]
  }, async ({ page, request, browserErrors }) => {
    let agentId;
    let agentDeleted = false;
    let modelConnectionId;
    let scenarioError;
    const cleanupErrors = [];
    const openedStreamUrls = new Set();
    try {
      page.on('request', (browserRequest) => {
        const url = new URL(browserRequest.url());
        if (browserRequest.method() === 'GET' && /\/api\/runs\/[^/]+\/events\/stream$/.test(url.pathname)) {
          openedStreamUrls.add(browserRequest.url());
        }
      });

      await page.goto('/login', { waitUntil: 'domcontentloaded' });
      await page.getByLabel('Email').fill('admin@example.com');
      await page.getByLabel('Password').fill('admin123');
      await page.getByRole('button', { name: 'Sign in', exact: true }).click();
      await page.getByText('admin@example.com', { exact: true }).waitFor();

      const agentName = scenarioContext.unique('QA Pi Session Browser Agent');
      const modelFixture = await createComposeModelFixture(request, scenarioContext);
      modelConnectionId = modelFixture.connectionId;
      const agent = await responseJson(await request.post('/api/agents', {
        data: {
          name: agentName,
          instructions: 'Exercise completed Pi Session browser behavior.',
          visibility: 'private',
          public_to: [],
          model_selection: modelFixture.selection
        }
      }), 'Create Pi Session browser Agent');
      agentId = agent.id;

      await page.goto('/sessions', { waitUntil: 'domcontentloaded' });
      const list = page.getByRole('complementary', { name: 'Session list' });
      const detail = page.getByRole('region', { name: 'Session details' });
      await list.getByRole('button', { name: 'New conversation' }).waitFor();

      const independentMessage = scenarioContext.unique('QA independent Pi Session');
      const independentRun = await createConversation(page, request, agent.id, independentMessage,
        { verifyComposer: true });
      assert.ok(independentRun.id);
      assert.ok(independentRun.hub_session_id);
      await assertMessageVisible(detail, independentMessage);
      await waitForRun(request, independentRun.id, 'completed');
      const independentSession = await waitForNativePiSession(
        request,
        independentRun.hub_session_id,
        'first completed Pi Session to expose its native Session id'
      );
      await waitForMessage(request, independentRun.hub_session_id, independentMessage);
      const independentAssistantMessages = detail.locator('.session-bubble.role-assistant .session-message-text');
      await independentAssistantMessages.first().waitFor();
      assert.equal(await independentAssistantMessages.count(), 1);

      const independentFollowUp = scenarioContext.unique('QA independent Pi follow-up');
      const independentFollowUpAcceptance = await sendNextTurn(
        page, request, independentRun.hub_session_id, independentFollowUp
      );
      assert.ok(independentFollowUpAcceptance.run?.id);
      assert.equal(independentFollowUpAcceptance.message.delivery_mode, 'next_turn');
      await waitForRun(request, independentFollowUpAcceptance.run.id, 'completed');
      await independentAssistantMessages.nth(1).waitFor();
      assert.equal(await independentAssistantMessages.count(), 2);
      const independentCompleted = await waitForNativePiSession(
        request,
        independentRun.hub_session_id,
        'second Pi Turn to retain the first native Session id'
      );
      assert.equal(independentCompleted.native_session_id, independentSession.native_session_id);

      await page.reload({ waitUntil: 'domcontentloaded' });
      const independentReloadList = page.getByRole('complementary', { name: 'Session list' });
      await independentReloadList.getByRole('textbox', { name: 'Search sessions' })
        .fill(independentRun.hub_session_id);
      const independentReloadRow = independentReloadList.locator('.session-row');
      assert.equal(await independentReloadRow.count(), 1);
      await independentReloadRow.click();
      await assertMessageVisible(detail, independentMessage);
      await assertMessageVisible(detail, independentFollowUp);
      assert.equal(await detail.locator('.session-bubble.role-assistant .session-message-text').count(), 2);

      const continuedMessage = scenarioContext.unique('QA second Pi Session');
      const continuedRun = await createConversation(page, request, agent.id, continuedMessage);
      await waitForRun(request, continuedRun.id, 'completed');
      const continuedSession = await waitForNativePiSession(
        request,
        continuedRun.hub_session_id,
        'second completed Pi Session to expose its native Session id'
      );
      assert.notEqual(continuedSession.native_session_id, independentSession.native_session_id);
      await assertMessageVisible(detail, continuedMessage);
      const continuedAssistantMessages = detail.locator('.session-bubble.role-assistant .session-message-text');
      await continuedAssistantMessages.first().waitFor();
      assert.equal(await continuedAssistantMessages.count(), 1);

      const continuedFollowUp = scenarioContext.unique('QA second Pi follow-up');
      const continuedAcceptance = await sendNextTurn(
        page, request, continuedRun.hub_session_id, continuedFollowUp
      );
      assert.ok(continuedAcceptance.run?.id);
      assert.notEqual(continuedAcceptance.run.id, continuedRun.id);
      assert.equal(continuedAcceptance.message.delivery_mode, 'next_turn');
      await waitForRun(request, continuedAcceptance.run.id, 'completed');
      await continuedAssistantMessages.nth(1).waitFor();
      assert.equal(await continuedAssistantMessages.count(), 2);
      const continuedCompleted = await waitForNativePiSession(
        request,
        continuedRun.hub_session_id,
        'second Turn in the second Pi Session to retain its native Session id'
      );
      assert.equal(continuedCompleted.native_session_id, continuedSession.native_session_id);
      await assertNoHorizontalOverflow(page, 'Sessions 1280px');

      const search = list.getByRole('textbox', { name: 'Search sessions' });
      const platform = list.getByRole('combobox', { name: 'Platform' });
      await search.fill(agentName);
      assert.equal(await list.locator('.session-row').count(), 2);
      assert.equal(await platform.inputValue(), 'hub_native');
      assert.deepEqual(await platform.locator('option').allTextContents(), ['Hub native', 'All platforms']);
      await platform.selectOption('all');
      assert.equal(await list.locator('.session-row').count(), 2);
      await platform.selectOption('hub_native');
      assert.equal(await list.locator('.session-row').count(), 2);
      await search.fill('');

      const deleteResponse = await request.delete(`/api/agents/${agent.id}`);
      assert.equal(deleteResponse.status(), 204, await deleteResponse.text());
      agentDeleted = true;
      const historicalSession = await poll(
        () => getJson(request, `/api/sessions/${continuedRun.hub_session_id}`, 'Historical Pi Session'),
        (session) => session.lifecycle_status === 'historical' && Boolean(session.agent_deleted_at),
        { timeoutMs: 30_000, description: 'Pi Session to become historical after Agent deletion' }
      );
      assert.equal(historicalSession.native_session_id, continuedSession.native_session_id);

      await page.reload({ waitUntil: 'domcontentloaded' });
      const reloadedList = page.getByRole('complementary', { name: 'Session list' });
      await reloadedList.getByRole('textbox', { name: 'Search sessions' }).fill(continuedRun.hub_session_id);
      const reloadedSessionRow = reloadedList.locator('.session-row');
      assert.equal(await reloadedSessionRow.count(), 1);
      await reloadedSessionRow.click();
      const historicalDetail = page.getByRole('region', { name: 'Session details' });
      await historicalDetail.locator('.session-banner').getByText('Historical Session', { exact: true }).waitFor();
      await assertMessageVisible(historicalDetail, continuedMessage);
      await assertMessageVisible(historicalDetail, continuedFollowUp);
      assert.equal(await historicalDetail.getByRole('textbox', { name: 'Message' }).count(), 0);
      assert.equal(await historicalDetail.getByRole('button', { name: 'Send' }).count(), 0);

      await page.setViewportSize({ width: 390, height: 844 });
      await assertNoHorizontalOverflow(page, 'Historical Sessions 390x844');
      const mobileList = page.getByRole('complementary', { name: 'Session list' });
      await mobileList.waitFor({ state: 'hidden' });
      await historicalDetail.getByRole('button', { name: 'Session list' }).click();
      await mobileList.waitFor();
      await mobileList.getByRole('textbox', { name: 'Search sessions' }).fill(independentRun.hub_session_id);
      const independentSessionRow = mobileList.locator('.session-row');
      assert.equal(await independentSessionRow.count(), 1);
      await independentSessionRow.click();
      await mobileList.waitFor({ state: 'hidden' });
      await assertMessageVisible(historicalDetail, independentMessage);
      assert.equal(await historicalDetail.getByRole('textbox', { name: 'Message' }).count(), 0);
      await assertNoHorizontalOverflow(page, 'Historical Sessions 390x844 after navigation');

      const allowedAbortErrors = new Set([...openedStreamUrls].map((url) => (
        `requestfailed: GET ${url}: net::ERR_ABORTED`
      )));
      const unexpectedBrowserErrors = browserErrors.filter((error) => !allowedAbortErrors.has(error));
      browserErrors.splice(0, browserErrors.length, ...unexpectedBrowserErrors);
      assert.deepEqual(browserErrors, [], 'Only exact Session SSE abort diagnostics may be ignored');
    } catch (error) {
      scenarioError = error;
    } finally {
      if (agentId && !agentDeleted) {
        try {
          const deleteResponse = await request.delete(`/api/agents/${agentId}`);
          const body = await deleteResponse.text();
          assert.ok([204, 404].includes(deleteResponse.status()),
            `Delete Pi Session browser Agent returned ${deleteResponse.status()}: ${body}`);
        } catch (error) {
          cleanupErrors.push(error);
        }
      }
      if (modelConnectionId) {
        try {
          const deleteResponse = await request.delete(`/api/model-connections/${modelConnectionId}`);
          const body = await deleteResponse.text();
          assert.ok([204, 404].includes(deleteResponse.status()),
            `Delete Pi Session Model Connection returned ${deleteResponse.status()}: ${body}`);
        } catch (error) {
          cleanupErrors.push(error);
        }
      }
    }

    if (scenarioError && cleanupErrors.length > 0) {
      throw new AggregateError([scenarioError, ...cleanupErrors], 'Pi Sessions browser scenario and cleanup failed');
    }
    if (scenarioError) throw scenarioError;
    if (cleanupErrors.length === 1) throw cleanupErrors[0];
    if (cleanupErrors.length > 1) {
      throw new AggregateError(cleanupErrors, 'Pi Sessions browser cleanup failed');
    }
  });
}
