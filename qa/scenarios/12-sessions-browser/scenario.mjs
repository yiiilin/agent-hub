import assert from 'node:assert/strict';
import { poll } from '../../support/api.mjs';
import { withBrowser } from '../../support/browser.mjs';

const HOLD_MESSAGE = 'fixture:hold';

async function responseJson(response, label) {
  const body = await response.text();
  assert.equal(response.ok(), true, `${label} returned ${response.status()}: ${body}`);
  return JSON.parse(body);
}

async function getJson(request, path, label) {
  return responseJson(await request.get(path), label);
}

async function waitForRun(request, runId, expectedStatus, timeoutMs = 60_000) {
  return poll(
    () => getJson(request, `/api/runs/${runId}`, `Run ${runId}`),
    (run) => run.status === expectedStatus,
    { timeoutMs, description: `Run ${runId} to reach ${expectedStatus}` }
  );
}

async function waitForMessage(request, sessionId, content, accept = () => true) {
  return poll(async () => {
    const messages = await getJson(
      request,
      `/api/sessions/${sessionId}/messages`,
      `Session ${sessionId} messages`
    );
    return messages.find((message) => message.content === content) ?? null;
  }, (message) => message !== null && accept(message), {
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
  assert.ok(Math.abs(initial.height - initial.minimum) <= 1, `Composer minimum height: ${JSON.stringify(initial)}`);

  await composer.fill('first line');
  await composer.press('Shift+Enter');
  assert.equal(await composer.inputValue(), 'first line\n', 'Shift+Enter must insert a newline');

  await composer.fill('one\ntwo\nthree');
  const middle = await metrics();
  assert.ok(middle.height > initial.height && middle.height < middle.maximum,
    `Composer must grow between two and five lines: ${JSON.stringify(middle)}`);

  await composer.fill('one\ntwo\nthree\nfour\nfive\nsix');
  const maximum = await metrics();
  assert.ok(Math.abs(maximum.height - maximum.maximum) <= 1, `Composer maximum height: ${JSON.stringify(maximum)}`);
  assert.equal(maximum.overflowY, 'auto', 'Composer must scroll after five lines');
  await composer.fill('');
}

async function createConversation(page, request, agentId, message, { verifyComposer = false } = {}) {
  const sessionsBeforeDraft = await getJson(request, '/api/sessions', 'Sessions before Conversation Draft');
  const sessionIdsBeforeDraft = sessionsBeforeDraft.map((session) => session.id).sort();
  await page.getByRole('button', { name: 'New conversation' }).click();
  const dialog = page.getByRole('dialog', { name: 'New conversation' });
  await dialog.getByRole('combobox', { name: 'Agent' }).selectOption(agentId);
  assert.equal(await dialog.getByRole('textbox', { name: 'Initial message' }).count(), 0);
  await dialog.getByRole('button', { name: 'Start conversation' }).click();
  const composer = page.getByRole('region', { name: 'Session details' }).getByRole('textbox', { name: 'Message' });
  await composer.waitFor();
  if (verifyComposer) await assertComposerKeyboardAndSizing(composer);
  const sessionsDuringDraft = await getJson(request, '/api/sessions', 'Sessions during Conversation Draft');
  assert.deepEqual(
    sessionsDuringDraft.map((session) => session.id).sort(),
    sessionIdsBeforeDraft,
    'Selecting an Agent must not persist a Session before the first message'
  );
  await composer.fill(message);
  assert.equal(await composer.inputValue(), message);
  const responsePromise = page.waitForResponse((response) => (
    response.request().method() === 'POST'
    && new URL(response.url()).pathname === `/api/agents/${agentId}/runs`
  ));
  await composer.press('Enter');
  const response = await responsePromise;
  return responseJson(response, `Create conversation for Agent ${agentId}`);
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

export default async function sessionsBrowserScenario(scenarioContext) {
  await withBrowser(scenarioContext, {
    allowedHttpErrors: [
      { method: 'GET', pathname: '/api/auth/me', status: 401, times: 1 }
    ]
  }, async ({ page, request, browserErrors }) => {
    let agentId;
    let agentDeleted = false;
    let scenarioError;
    const cleanupErrors = [];
    try {
      const openedStreamUrls = new Set();
      const allowedOldStreamUrls = new Set();
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

      const agentName = scenarioContext.unique('QA Session Browser Agent');
      const agent = await responseJson(await request.post('/api/agents', {
        data: {
          name: agentName,
          instructions: 'Exercise deterministic Session lifecycle behavior through fake Codex.',
          visibility: 'private',
          public_to: []
        }
      }), 'Create Session browser Agent');
      agentId = agent.id;

      await page.goto('/sessions', { waitUntil: 'domcontentloaded' });
      const list = page.getByRole('complementary', { name: 'Session list' });
      const detail = page.getByRole('region', { name: 'Session details' });
      await list.getByRole('button', { name: 'New conversation' }).waitFor();

      const independentMessage = scenarioContext.unique('QA independent Session');
      const independentRun = await createConversation(page, request, agent.id, independentMessage, { verifyComposer: true });
      assert.ok(independentRun.id, 'Independent conversation must return a Run id');
      assert.ok(independentRun.hub_session_id, 'Independent conversation must return a Session id');
      await assertMessageVisible(detail, independentMessage);
      await waitForRun(request, independentRun.id, 'completed');
      const independentSession = await getJson(
        request,
        `/api/sessions/${independentRun.hub_session_id}`,
        'Independent Session'
      );
      assert.ok(independentSession.native_thread_id, 'Independent Session must expose a native Thread');
      await detail.locator('.session-message-text').filter({ hasText: 'Fake Codex completed run' }).waitFor();
      const independentStreamUrl = new URL(
        `/api/runs/${independentRun.id}/events/stream`,
        scenarioContext.baseURL
      ).href;
      await poll(() => openedStreamUrls.has(independentStreamUrl), Boolean, {
        timeoutMs: 10_000,
        description: `SSE request ${independentStreamUrl}`
      });

      const deliveredIndependentMessage = await waitForMessage(
        request,
        independentRun.hub_session_id,
        independentMessage,
        (message) => message.delivery_state === 'delivered'
      );
      assert.equal(deliveredIndependentMessage.run_id, independentRun.id);
      await poll(async () => detail.locator('.session-bubble.role-user')
        .filter({ hasText: independentMessage }).locator('.message-state').count(),
      (count) => count === 0, {
        timeoutMs: 10_000,
        description: 'Delivered user message state to disappear from the transcript'
      });
      const independentAssistantMessages = detail.locator('.session-bubble.role-assistant .session-message-text');
      assert.equal(await independentAssistantMessages.count(), 1, 'First completed Run must render one assistant answer');

      const independentFollowUp = scenarioContext.unique('QA independent follow-up');
      const independentComposer = detail.getByRole('textbox', { name: 'Message' });
      await independentComposer.fill(independentFollowUp);
      const independentFollowUpResponse = page.waitForResponse((response) => (
        response.request().method() === 'POST'
        && new URL(response.url()).pathname === `/api/sessions/${independentRun.hub_session_id}/messages`
      ));
      await independentComposer.press('Enter');
      const independentFollowUpAcceptance = await responseJson(
        await independentFollowUpResponse,
        'Create independent follow-up Turn'
      );
      assert.ok(independentFollowUpAcceptance.run?.id, 'Follow-up message must schedule a Run');
      await waitForRun(request, independentFollowUpAcceptance.run.id, 'completed');
      await independentAssistantMessages.nth(1).waitFor();
      assert.equal(await independentAssistantMessages.count(), 2, 'Two completed Runs must render both assistant answers');

      const independentFollowUpStreamUrl = new URL(
        `/api/runs/${independentFollowUpAcceptance.run.id}/events/stream`,
        scenarioContext.baseURL
      ).href;
      await poll(() => openedStreamUrls.has(independentFollowUpStreamUrl), Boolean, {
        timeoutMs: 10_000,
        description: `SSE request ${independentFollowUpStreamUrl}`
      });
      allowedOldStreamUrls.add(independentStreamUrl);
      allowedOldStreamUrls.add(independentFollowUpStreamUrl);

      await page.reload({ waitUntil: 'domcontentloaded' });
      const independentReloadList = page.getByRole('complementary', { name: 'Session list' });
      await independentReloadList.getByRole('textbox', { name: 'Search sessions' }).fill(independentRun.hub_session_id);
      const independentReloadRow = independentReloadList.locator('.session-row');
      assert.equal(await independentReloadRow.count(), 1, 'Reloaded list must contain the two-Run Session');
      await independentReloadRow.click();
      await assertMessageVisible(detail, independentMessage);
      await assertMessageVisible(detail, independentFollowUp);
      await detail.locator('.session-bubble.role-assistant .session-message-text').nth(1).waitFor();
      assert.equal(
        await detail.locator('.session-bubble.role-assistant .session-message-text').count(),
        2,
        'Reloading a two-Run Session must retain both assistant answers'
      );

      const holdRun = await createConversation(page, request, agent.id, HOLD_MESSAGE);
      assert.ok(holdRun.id, 'Hold conversation must return a Run id');
      assert.ok(holdRun.hub_session_id, 'Hold conversation must return a Session id');
      assert.notEqual(holdRun.hub_session_id, independentRun.hub_session_id);
      assert.equal(holdRun.initial_message, HOLD_MESSAGE);
      await assertMessageVisible(detail, HOLD_MESSAGE);

      const active = await poll(async () => {
        const [run, session] = await Promise.all([
          getJson(request, `/api/runs/${holdRun.id}`, `Hold Run ${holdRun.id}`),
          getJson(request, `/api/sessions/${holdRun.hub_session_id}`, `Hold Session ${holdRun.hub_session_id}`)
        ]);
        return { run, session };
      }, ({ run, session }) => (
        run.status === 'running'
        && session.lifecycle_status === 'online'
        && session.active_turn_id === run.hub_turn_id
        && typeof session.native_thread_id === 'string'
        && session.native_thread_id.length > 0
      ), {
        timeoutMs: 60_000,
        description: `Hold Run ${holdRun.id} running with an active Session Turn`
      });
      const nativeThreadId = active.session.native_thread_id;
      const holdTurnId = active.run.hub_turn_id;
      assert.ok(holdTurnId, 'Running hold Run must expose its Hub Turn id');
      assert.notEqual(nativeThreadId, independentSession.native_thread_id, 'Independent Sessions must use different native Threads');
      await detail.getByRole('button', { name: 'Stop current run' }).waitFor();

      const initialMessage = await waitForMessage(
        request,
        holdRun.hub_session_id,
        HOLD_MESSAGE,
        (message) => message.turn_id === holdTurnId
      );
      assert.equal(initialMessage.run_id, holdRun.id);
      const holdStreamUrl = new URL(
        `/api/runs/${holdRun.id}/events/stream`,
        scenarioContext.baseURL
      ).href;
      await poll(() => openedStreamUrls.has(holdStreamUrl), Boolean, {
        timeoutMs: 10_000,
        description: `SSE request ${holdStreamUrl}`
      });

      const activityEvents = await poll(async () => {
        const events = await getJson(request, `/api/runs/${holdRun.id}/events`, 'Hold Run events');
        return events.filter((event) => event.event_type === 'item');
      }, (events) => events.some((event) => event.payload?.item_type === 'reasoning'), {
        timeoutMs: 30_000,
        description: `Hold Run ${holdRun.id} readable activity`
      });
      assert.ok(activityEvents.every((event, index) => index === 0 || event.seq > activityEvents[index - 1].seq));

      const activity = detail.locator('.session-activity-events').first();
      const activitySummary = activity.locator('summary');
      await activitySummary.waitFor();
      assert.equal(await activity.getAttribute('open'), null, 'Agent activity must be collapsed by default');
      assert.match(await activitySummary.innerText(), /^Worked for .+$/);
      assert.ok((await activitySummary.locator('.session-activity-chevron').getAttribute('class')).includes('lucide-chevron-right'));
      assert.equal(await activitySummary.locator('.session-activity-chevron').evaluate((element) => getComputedStyle(element).transform), 'none');

      const initialTimelineOrder = await detail.locator('.session-transcript > *').evaluateAll((elements, holdMessage) => ({
        message: elements.findIndex((element) => element.textContent?.includes(holdMessage)),
        activity: elements.findIndex((element) => element.classList.contains('session-activity-events'))
      }), HOLD_MESSAGE);
      assert.ok(initialTimelineOrder.message >= 0 && initialTimelineOrder.activity > initialTimelineOrder.message,
        `Agent activity must follow the initial message: ${JSON.stringify(initialTimelineOrder)}`);

      await activitySummary.click();
      assert.notEqual(await activity.getAttribute('open'), null, 'Agent activity must expand');
      assert.notEqual(await activitySummary.locator('.session-activity-chevron').evaluate((element) => getComputedStyle(element).transform), 'none');
      const displayedActivityLabels = await activity.locator('.session-activity-row strong').allTextContents();
      assert.ok(displayedActivityLabels.includes('Thought'));
      await activity.getByText('Preparing the response.', { exact: true }).waitFor();
      assert.equal(await activity.getByText('turn_started', { exact: true }).count(), 0);

      const steerMessage = scenarioContext.unique('QA steer current Turn');
      await detail.getByRole('textbox', { name: 'Message' }).fill(steerMessage);
      const steerResponsePromise = page.waitForResponse((response) => (
        response.request().method() === 'POST'
        && new URL(response.url()).pathname === `/api/sessions/${holdRun.hub_session_id}/messages`
      ));
      await detail.getByRole('button', { name: 'Send' }).click();
      const steerAcceptance = await responseJson(await steerResponsePromise, 'Steer active Turn');
      assert.equal(steerAcceptance.message.content, steerMessage);
      assert.equal(steerAcceptance.message.delivery_mode, 'steer');
      assert.equal(steerAcceptance.message.turn_id, holdTurnId);
      assert.equal(steerAcceptance.message.run_id, holdRun.id);
      assert.ok(steerAcceptance.message.expected_native_turn_id);
      assert.equal(steerAcceptance.run.id, holdRun.id);
      await assertMessageVisible(detail, steerMessage);
      await detail.locator('.session-bubble small').getByText('Guiding the current turn.', { exact: true }).waitFor();
      await assertMessageVisible(detail, HOLD_MESSAGE);
      const deliveredSteer = await waitForMessage(
        request,
        holdRun.hub_session_id,
        steerMessage,
        (message) => message.delivery_mode === 'steer' && message.delivery_state === 'delivered'
      );
      assert.equal(deliveredSteer.turn_id, initialMessage.turn_id);

      const steeredTimelineOrder = await detail.locator('.session-transcript > *').evaluateAll((elements, values) => ({
        initial: elements.findIndex((element) => element.textContent?.includes(values.initial)),
        activity: elements.findIndex((element) => element.classList.contains('session-activity-events')),
        steer: elements.findIndex((element) => element.textContent?.includes(values.steer))
      }), { initial: HOLD_MESSAGE, steer: steerMessage });
      assert.ok(
        steeredTimelineOrder.initial >= 0
        && steeredTimelineOrder.activity > steeredTimelineOrder.initial
        && steeredTimelineOrder.steer > steeredTimelineOrder.activity,
        `Agent activity must remain between initial and steer messages: ${JSON.stringify(steeredTimelineOrder)}`
      );

      const search = list.getByRole('textbox', { name: 'Search sessions' });
      const origin = list.getByRole('combobox', { name: 'Origin' });
      await search.fill(agentName);
      assert.equal(await list.locator('.session-row').count(), 2, 'Search must isolate the two Sessions for this Agent');
      await origin.selectOption('external');
      await list.getByText('No Sessions match this view.', { exact: true }).waitFor();
      assert.equal(await list.locator('.session-row').count(), 0, 'Origin filter must exclude Hub-native Sessions');
      await origin.selectOption('hub_native');
      assert.equal(await list.locator('.session-row').count(), 2, 'Hub-native Origin filter must restore both Sessions');
      await origin.selectOption('all');
      await search.fill('');

      const stopResponsePromise = page.waitForResponse((response) => (
        response.request().method() === 'POST'
        && new URL(response.url()).pathname === `/api/runs/${holdRun.id}/stop`
      ));
      await detail.getByRole('button', { name: 'Stop current run' }).click();
      const stopResponse = await responseJson(await stopResponsePromise, 'Stop hold Run');
      assert.equal(stopResponse.id, holdRun.id);
      const interrupted = await waitForRun(request, holdRun.id, 'interrupted');
      assert.equal(interrupted.status, 'interrupted');
      const stoppedSession = await poll(
        () => getJson(request, `/api/sessions/${holdRun.hub_session_id}`, 'Stopped Session'),
        (session) => session.active_turn_id === null,
        { timeoutMs: 30_000, description: `Session ${holdRun.hub_session_id} to clear its active Turn` }
      );
      assert.equal(stoppedSession.native_thread_id, nativeThreadId);
      await assertMessageVisible(detail, HOLD_MESSAGE);
      await assertMessageVisible(detail, steerMessage);

      allowedOldStreamUrls.add(holdStreamUrl);
      const nextMessage = scenarioContext.unique('QA next Turn after Stop');
      await detail.getByRole('textbox', { name: 'Message' }).fill(nextMessage);
      const nextResponsePromise = page.waitForResponse((response) => (
        response.request().method() === 'POST'
        && new URL(response.url()).pathname === `/api/sessions/${holdRun.hub_session_id}/messages`
      ));
      await detail.getByRole('button', { name: 'Send' }).click();
      const nextAcceptance = await responseJson(await nextResponsePromise, 'Create next Turn');
      assert.equal(nextAcceptance.message.content, nextMessage);
      assert.equal(nextAcceptance.message.delivery_mode, 'next_turn');
      assert.equal(nextAcceptance.message.session_id, holdRun.hub_session_id);
      assert.notEqual(nextAcceptance.message.turn_id, holdTurnId);
      assert.ok(nextAcceptance.run?.id, 'Next Turn must schedule a Run');
      assert.notEqual(nextAcceptance.run.id, holdRun.id);
      assert.equal(nextAcceptance.run.hub_session_id, holdRun.hub_session_id);
      await assertMessageVisible(detail, nextMessage);
      await waitForRun(request, nextAcceptance.run.id, 'completed');
      await detail.locator('.session-message-text').filter({ hasText: 'Fake Codex completed run' }).last().waitFor();

      const completedSession = await getJson(
        request,
        `/api/sessions/${holdRun.hub_session_id}`,
        'Completed multi-Turn Session'
      );
      assert.equal(completedSession.native_thread_id, nativeThreadId, 'Multiple Turns must reuse one native Thread');
      const completedNextMessage = await waitForMessage(
        request,
        holdRun.hub_session_id,
        nextMessage,
        (message) => message.delivery_state === 'delivered'
      );
      assert.equal(completedNextMessage.run_id, nextAcceptance.run.id);
      assert.notEqual(completedNextMessage.turn_id, initialMessage.turn_id, 'Post-stop continuation must use a new Turn');
      await assertMessageVisible(detail, HOLD_MESSAGE);
      await assertMessageVisible(detail, steerMessage);
      await assertNoHorizontalOverflow(page, 'Sessions 1280px');

      const nextStreamUrl = new URL(
        `/api/runs/${nextAcceptance.run.id}/events/stream`,
        scenarioContext.baseURL
      ).href;
      await poll(() => openedStreamUrls.has(nextStreamUrl), Boolean, {
        timeoutMs: 10_000,
        description: `SSE request ${nextStreamUrl}`
      });

      const deleteResponse = await request.delete(`/api/agents/${agent.id}`);
      assert.equal(deleteResponse.status(), 204, await deleteResponse.text());
      agentDeleted = true;
      const historicalSession = await poll(
        () => getJson(request, `/api/sessions/${holdRun.hub_session_id}`, 'Historical Session'),
        (session) => session.lifecycle_status === 'historical' && Boolean(session.agent_deleted_at),
        { timeoutMs: 30_000, description: `Session ${holdRun.hub_session_id} to become historical` }
      );
      assert.equal(historicalSession.native_thread_id, nativeThreadId, 'Historical Session must retain native Thread identity');

      allowedOldStreamUrls.add(nextStreamUrl);
      await page.reload({ waitUntil: 'domcontentloaded' });
      const reloadedList = page.getByRole('complementary', { name: 'Session list' });
      await reloadedList.getByRole('textbox', { name: 'Search sessions' }).fill(holdRun.hub_session_id);
      const reloadedSessionRow = reloadedList.locator('.session-row');
      assert.equal(await reloadedSessionRow.count(), 1, 'Reloaded list must contain the exact historical Session');
      await reloadedSessionRow.click();
      const historicalDetail = page.getByRole('region', { name: 'Session details' });
      await historicalDetail.locator('.session-banner').getByText('Historical Session', { exact: true }).waitFor();
      await assertMessageVisible(historicalDetail, HOLD_MESSAGE);
      await assertMessageVisible(historicalDetail, steerMessage);
      await assertMessageVisible(historicalDetail, nextMessage);
      assert.equal(await historicalDetail.getByRole('textbox', { name: 'Message' }).count(), 0);
      assert.equal(await historicalDetail.getByRole('button', { name: 'Send' }).count(), 0);

      await page.setViewportSize({ width: 390, height: 844 });
      await assertNoHorizontalOverflow(page, 'Sessions 390x844');
      const mobileList = page.getByRole('complementary', { name: 'Session list' });
      await mobileList.waitFor({ state: 'hidden' });
      assert.equal(await mobileList.isHidden(), true, 'Mobile Session list must start closed');
      await historicalDetail.getByRole('button', { name: 'Session list' }).click();
      await mobileList.waitFor();
      await mobileList.getByRole('textbox', { name: 'Search sessions' }).fill(independentRun.hub_session_id);
      const independentSessionRow = mobileList.locator('.session-row');
      assert.equal(await independentSessionRow.count(), 1, 'Mobile drawer search must isolate the other Session');
      await independentSessionRow.click();
      await mobileList.waitFor({ state: 'hidden' });
      assert.equal(await mobileList.isHidden(), true, 'Selecting a Session must close the mobile drawer');
      await assertMessageVisible(historicalDetail, independentMessage);
      assert.equal(await historicalDetail.getByRole('textbox', { name: 'Message' }).count(), 0);
      await assertNoHorizontalOverflow(page, 'Historical Sessions 390x844');

      const allowedAbortErrors = new Set([...allowedOldStreamUrls].map((url) => (
        `requestfailed: GET ${url}: net::ERR_ABORTED`
      )));
      assert.ok(
        [...allowedOldStreamUrls].every((url) => openedStreamUrls.has(url)),
        'Every allowed SSE abort URL must have been opened by this scenario'
      );
      const unexpectedBrowserErrors = browserErrors.filter((error) => !allowedAbortErrors.has(error));
      browserErrors.splice(0, browserErrors.length, ...unexpectedBrowserErrors);
      assert.deepEqual(browserErrors, [], 'Only exact old SSE GET net::ERR_ABORTED diagnostics may be ignored');
    } catch (error) {
      scenarioError = error;
    } finally {
      if (agentId && !agentDeleted) {
        try {
          const deleteResponse = await request.delete(`/api/agents/${agentId}`);
          const body = await deleteResponse.text();
          assert.ok([204, 404].includes(deleteResponse.status()),
            `Delete Session browser Agent returned ${deleteResponse.status()}: ${body}`);
          agentDeleted = true;
        } catch (error) {
          cleanupErrors.push(error);
        }
      }
    }

    if (scenarioError && cleanupErrors.length > 0) {
      throw new AggregateError([scenarioError, ...cleanupErrors], 'Sessions browser scenario and cleanup failed');
    }
    if (scenarioError) throw scenarioError;
    if (cleanupErrors.length === 1) throw cleanupErrors[0];
    if (cleanupErrors.length > 1) {
      throw new AggregateError(cleanupErrors, 'Sessions browser scenario cleanup failed');
    }
  });
}
