import assert from 'node:assert/strict';
import { ApiClient, loginAsAdmin, poll } from '../../support/api.mjs';
import { withBrowser } from '../../support/browser.mjs';

const HOLD_MESSAGE = 'fixture:hold';
const RELEASE_MESSAGE = 'fixture:release';
const UUID_PATTERN = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/;
const TERMINAL_RUN_STATUSES = new Set(['completed', 'failed', 'interrupted', 'cancelled']);

function assertUuid(value, label) {
  assert.match(value, UUID_PATTERN, `${label} must be a UUID`);
}

function findTurnStarted(events, nativeThreadId) {
  return events.find((event) => (
    event.event_type === 'turn_started'
    && event.payload?.native_thread_id === nativeThreadId
    && typeof event.payload?.native_turn_id === 'string'
    && event.payload.native_turn_id.length > 0
  ));
}

function assertNoInterruptedOrFailedEvent(events, label) {
  const forbidden = events.find((event) => (
    event.content === 'interrupted'
    || event.content === 'failed'
    || event.payload?.status === 'interrupted'
    || event.payload?.status === 'failed'
  ));
  assert.equal(forbidden, undefined, `${label} must not contain interrupted or failed events`);
}

async function waitForHeldContinuity(client, runId, sessionId) {
  const snapshot = await poll(async () => {
    const [{ data: run }, { data: session }, { data: events }] = await Promise.all([
      client.get(`/api/runs/${runId}`),
      client.get(`/api/sessions/${sessionId}`),
      client.get(`/api/runs/${runId}/events`)
    ]);
    return { run, session, events, turnStarted: findTurnStarted(events, session.native_thread_id) };
  }, ({ run, session, turnStarted }) => (
    TERMINAL_RUN_STATUSES.has(run.status)
    || (
      run.status === 'running'
      && session.lifecycle_status === 'online'
      && session.active_turn_id === run.hub_turn_id
      && typeof session.native_thread_id === 'string'
      && session.native_thread_id.length > 0
      && turnStarted !== undefined
    )
  ), {
    timeoutMs: 60_000,
    description: `held Run ${runId} to expose public continuity IDs`
  });

  assert.equal(snapshot.run.status, 'running', 'fixture:hold Run must remain running');
  assert.equal(snapshot.session.lifecycle_status, 'online');
  assert.ok(snapshot.turnStarted, 'held Run must publish a turn_started event');
  return snapshot;
}

async function waitForRunTerminal(client, runId, description) {
  return poll(async () => (await client.get(`/api/runs/${runId}`)).data, (run) => (
    TERMINAL_RUN_STATUSES.has(run.status)
  ), {
    timeoutMs: 90_000,
    description
  });
}

async function waitForMessageDelivered(client, sessionId, messageId) {
  return poll(async () => {
    const { data: messages } = await client.get(`/api/sessions/${sessionId}/messages`);
    return messages.find((message) => message.id === messageId) ?? null;
  }, (message) => message?.delivery_state === 'delivered', {
    timeoutMs: 60_000,
    description: `Session message ${messageId} to be delivered`
  });
}

async function assertRolloutLayout(page, label) {
  await page.evaluate(() => new Promise((resolve) => {
    requestAnimationFrame(() => requestAnimationFrame(resolve));
  }));
  const documentOverflow = await page.evaluate(() => (
    document.documentElement.scrollWidth - document.documentElement.clientWidth
  ));
  assert.ok(documentOverflow <= 1, `${label} document horizontal overflow: ${documentOverflow}px`);

  for (const selector of ['.administration-tab-panel', '.administration-codex', '.rollout-runtime-list']) {
    const locator = page.locator(selector);
    await locator.waitFor({ state: 'visible' });
    const overflow = await locator.evaluate((element) => element.scrollWidth - element.clientWidth);
    assert.ok(overflow <= 1, `${label} ${selector} horizontal overflow: ${overflow}px`);
  }
}

function targetVersionFor(initialRollout, runtimes) {
  const occupied = new Set([
    initialRollout.active_version,
    initialRollout.target_version,
    ...runtimes.map((runtime) => runtime.codex_version)
  ].filter(Boolean));
  const target = ['0.146.0-qa-browser', '0.146.1-qa-browser']
    .find((candidate) => !occupied.has(candidate));
  assert.ok(target, 'A QA Codex target must differ from every current and active version');
  return target;
}

async function cleanupAgentAfterFailure(client, agentId, holdRunId, scenarioFailed) {
  const firstDelete = await client.delete(`/api/agents/${agentId}`, {
    expectedStatus: [204, 404, 409]
  });
  if (firstDelete.status === 204 || firstDelete.status === 404) return;
  if (!scenarioFailed || !holdRunId) {
    throw new Error(`Agent ${agentId} cleanup returned ${firstDelete.status}`);
  }

  const runResponse = await client.get(`/api/runs/${holdRunId}`, { expectedStatus: [200, 404] });
  if (runResponse.status === 200 && !TERMINAL_RUN_STATUSES.has(runResponse.data.status)) {
    process.stderr.write(
      `[cleanup] failed scenario stopped held Run ${holdRunId} before retrying Agent ${agentId} deletion\n`
    );
    await client.post(`/api/runs/${holdRunId}/stop`, undefined, {
      expectedStatus: [200, 404, 409]
    });
    await poll(async () => {
      const response = await client.get(`/api/runs/${holdRunId}`, { expectedStatus: [200, 404] });
      return response.status === 404 ? 'deleted' : response.data.status;
    }, (status) => status === 'deleted' || TERMINAL_RUN_STATUSES.has(status), {
      timeoutMs: 30_000,
      description: `failed held Run ${holdRunId} cleanup`
    });
  }

  const retryDelete = await client.delete(`/api/agents/${agentId}`, {
    expectedStatus: [204, 404, 409]
  });
  if (retryDelete.status !== 204 && retryDelete.status !== 404) {
    throw new Error(`Agent ${agentId} cleanup retry returned ${retryDelete.status}`);
  }
}

export default async function codexRolloutBrowserScenario(scenarioContext) {
  const client = new ApiClient(scenarioContext.baseURL);
  const { data: admin } = await loginAsAdmin(client);
  assert.equal(admin.role, 'super_admin');

  let agentId = null;
  let holdRunId = null;
  let scenarioError = null;
  let scenarioCompleted = false;

  try {
    const { data: agent } = await client.post('/api/agents', {
      name: scenarioContext.unique('QA Codex Rollout Browser Agent'),
      instructions: 'Preserve one native Turn while an exact Codex version is promoted.',
      visibility: 'private',
      public_to: []
    });
    agentId = agent.id;
    assertUuid(agentId, 'Agent id');

    const { data: holdRun } = await client.post(`/api/agents/${agentId}/runs`, {
      message: HOLD_MESSAGE,
      hub_session_id: null,
      parent_run_id: null
    });
    holdRunId = holdRun.id;
    assertUuid(holdRunId, 'held Run id');
    assertUuid(holdRun.hub_session_id, 'held Session id');
    assertUuid(holdRun.hub_turn_id, 'held Hub Turn id');

    const active = await waitForHeldContinuity(client, holdRunId, holdRun.hub_session_id);
    assertUuid(active.run.runtime_id, 'held Runtime id');
    const continuity = Object.freeze({
      runId: active.run.id,
      hubTurnId: active.run.hub_turn_id,
      nativeThreadId: active.session.native_thread_id,
      activeTurnId: active.turnStarted.payload.native_turn_id
    });
    assertUuid(continuity.hubTurnId, 'public Hub Turn id');
    assert.ok(continuity.nativeThreadId, 'public native Thread id must be present');
    assert.ok(continuity.activeTurnId, 'public active native Turn id must be present');
    assert.equal(active.turnStarted.payload.native_thread_id, continuity.nativeThreadId);

    const [{ data: initialRollout }, { data: initialRuntimes }] = await Promise.all([
      client.get('/api/admin/codex-version-rollout'),
      client.get('/api/runtimes')
    ]);
    const heldRuntime = initialRuntimes.find((runtime) => runtime.id === active.run.runtime_id);
    assert.ok(heldRuntime, `held Runtime ${active.run.runtime_id} must be publicly listed`);
    const targetVersion = targetVersionFor(initialRollout, initialRuntimes);
    assert.notEqual(targetVersion, initialRollout.active_version);
    assert.notEqual(targetVersion, heldRuntime.codex_version);

    await withBrowser(scenarioContext, {
      allowedHttpErrors: [
        { method: 'GET', pathname: '/api/auth/me', status: 401, times: 1 }
      ]
    }, async ({ page, browserErrors }) => {
      const rolloutGetsInFlight = new Set();
      const rolloutGetStarts = [];
      let maxRolloutGetsInFlight = 0;
      let administrationDocumentRequests = 0;

      function isRolloutGet(request) {
        return request.method() === 'GET'
          && new URL(request.url()).pathname === '/api/admin/codex-version-rollout';
      }

      page.on('request', (request) => {
        const url = new URL(request.url());
        if (request.resourceType() === 'document' && url.pathname === '/administration') {
          administrationDocumentRequests += 1;
        }
        if (!isRolloutGet(request)) return;
        rolloutGetsInFlight.add(request);
        rolloutGetStarts.push(Date.now());
        maxRolloutGetsInFlight = Math.max(maxRolloutGetsInFlight, rolloutGetsInFlight.size);
      });
      const finishRolloutGet = (request) => {
        if (isRolloutGet(request)) rolloutGetsInFlight.delete(request);
      };
      page.on('requestfinished', finishRolloutGet);
      page.on('requestfailed', finishRolloutGet);

      await page.goto('/login', { waitUntil: 'domcontentloaded' });
      await page.waitForLoadState('networkidle');
      await page.getByLabel('Email').fill('admin@example.com');
      await page.getByLabel('Password').fill('admin123');
      await page.getByRole('button', { name: 'Sign in', exact: true }).click();
      await page.getByText('admin@example.com', { exact: true }).waitFor();

      await page.goto('/administration', { waitUntil: 'domcontentloaded' });
      await page.getByRole('heading', { name: 'Administration', level: 1 }).waitFor();
      const codexTab = page.getByRole('tab', { name: 'Codex version', exact: true });
      await codexTab.click();
      assert.equal(await codexTab.getAttribute('aria-selected'), 'true');
      const rolloutSection = page.locator('.administration-codex');
      await rolloutSection.getByRole('heading', { name: 'Codex version rollout' }).waitFor();
      await poll(() => rolloutGetsInFlight.size, (count) => count === 0, {
        timeoutMs: 10_000,
        description: 'initial Codex rollout browser request to finish'
      });
      const rolloutGetsBeforePrepare = rolloutGetStarts.length;
      assert.ok(rolloutGetsBeforePrepare >= 1, 'Codex tab must load rollout state from the real API');

      await rolloutSection.getByLabel('Target Codex version').fill(targetVersion);
      assert.equal(
        await rolloutSection.getByLabel('Target Codex version').inputValue(),
        targetVersion
      );
      const prepareResponsePromise = page.waitForResponse((response) => (
        response.request().method() === 'PUT'
        && new URL(response.url()).pathname === '/api/admin/codex-version-rollout/target'
      ));
      await rolloutSection.getByRole('button', { name: 'Prepare version' }).click();
      const prepareResponse = await prepareResponsePromise;
      const prepareBody = await prepareResponse.json();
      assert.equal(prepareResponse.ok(), true, `Prepare returned ${prepareResponse.status()}`);
      assert.equal(prepareBody.target_version, targetVersion);
      assert.ok(
        prepareBody.status === 'downloading' || prepareBody.status === 'distributing',
        `Prepare must start rollout distribution, got ${prepareBody.status}`
      );
      await rolloutSection.getByText(targetVersion, { exact: true }).waitFor();
      await rolloutSection.getByText(prepareBody.status, { exact: true }).waitFor();

      const promote = rolloutSection.getByRole('button', { name: 'Promote ready version' });
      await promote.waitFor({ state: 'visible', timeout: 90_000 });
      await rolloutSection.getByText('ready', { exact: true }).first().waitFor();
      assert.ok(
        rolloutGetStarts.length > rolloutGetsBeforePrepare,
        'Ready state must arrive through automatic rollout GET polling'
      );
      assert.equal(maxRolloutGetsInFlight, 1, 'Rollout GET polling requests must never overlap');
      assert.equal(
        administrationDocumentRequests,
        1,
        'Codex rollout must become ready without reloading /administration'
      );

      const readiness = rolloutSection.locator('.rollout-runtime-list');
      const runtimeRow = readiness.locator(':scope > div').filter({ hasText: heldRuntime.hostname });
      await runtimeRow.waitFor({ state: 'visible' });
      const runtimeRowText = await runtimeRow.innerText();
      assert.ok(runtimeRowText.includes(heldRuntime.hostname), 'Readiness must show the real hostname');
      assert.ok(runtimeRowText.includes(heldRuntime.codex_version), 'Readiness must show the current version');
      assert.ok(runtimeRowText.includes('ready'), 'Readiness must show the Runtime as ready');
      const rolloutText = await rolloutSection.innerText();
      assert.ok(rolloutText.includes(targetVersion), 'Codex tab must show the exact Target Version');
      await assertRolloutLayout(page, 'Codex rollout desktop');

      await page.setViewportSize({ width: 390, height: 844 });
      await assertRolloutLayout(page, 'Codex rollout 390px');
      assert.equal(await codexTab.getAttribute('aria-selected'), 'true');
      assert.equal(await runtimeRow.isVisible(), true, 'Runtime readiness must remain visible at 390px');

      const promoteResponsePromise = page.waitForResponse((response) => (
        response.request().method() === 'POST'
        && new URL(response.url()).pathname === '/api/admin/codex-version-rollout/promote'
      ));
      await promote.click();
      const promoteResponse = await promoteResponsePromise;
      const promoteBody = await promoteResponse.json();
      assert.equal(promoteResponse.ok(), true, `Promote returned ${promoteResponse.status()}`);
      assert.equal(promoteBody.active_version, targetVersion);
      assert.equal(promoteBody.target_version, null);
      assert.equal(promoteBody.status, 'active');
      await page.getByText('Target Codex version promoted.', { exact: true }).waitFor();
      const allowedNavigationAborts = new Set([
        `requestfailed: GET ${new URL(`/api/runs/${continuity.runId}/events/stream`, scenarioContext.baseURL).href}: net::ERR_ABORTED`,
        `requestfailed: GET ${new URL(`/api/sessions/${holdRun.hub_session_id}`, scenarioContext.baseURL).href}: net::ERR_ABORTED`
      ]);
      const unexpectedBrowserErrors = browserErrors.filter((error) => !allowedNavigationAborts.has(error));
      browserErrors.splice(0, browserErrors.length, ...unexpectedBrowserErrors);
      assert.deepEqual(browserErrors, [], 'Rollout browser console and network diagnostics must remain empty');
    });

    const afterPromotion = await waitForHeldContinuity(client, continuity.runId, holdRun.hub_session_id);
    assert.equal(afterPromotion.run.id, continuity.runId);
    assert.equal(afterPromotion.run.hub_turn_id, continuity.hubTurnId);
    assert.equal(afterPromotion.session.active_turn_id, continuity.hubTurnId);
    assert.equal(afterPromotion.session.native_thread_id, continuity.nativeThreadId);
    const promotedTurnStartedEvents = afterPromotion.events.filter((event) => event.event_type === 'turn_started');
    assert.equal(promotedTurnStartedEvents.length, 1, 'Promotion must not start a replacement native Turn');
    assert.equal(promotedTurnStartedEvents[0].payload.native_turn_id, continuity.activeTurnId);
    assert.equal(promotedTurnStartedEvents[0].payload.native_thread_id, continuity.nativeThreadId);
    assertNoInterruptedOrFailedEvent(afterPromotion.events, 'held Run after promotion');

    const { data: releaseAcceptance } = await client.post(
      `/api/sessions/${holdRun.hub_session_id}/messages`,
      {
        content: RELEASE_MESSAGE,
        client_message_key: scenarioContext.unique('codex-rollout-release')
      }
    );
    assert.equal(releaseAcceptance.message.content, RELEASE_MESSAGE);
    assert.equal(releaseAcceptance.message.delivery_mode, 'steer');
    assert.equal(releaseAcceptance.message.run_id, continuity.runId);
    assert.equal(releaseAcceptance.message.turn_id, continuity.hubTurnId);
    assert.equal(releaseAcceptance.message.expected_native_turn_id, continuity.activeTurnId);
    assert.equal(releaseAcceptance.run.id, continuity.runId);
    await waitForMessageDelivered(client, holdRun.hub_session_id, releaseAcceptance.message.id);

    const completedHold = await waitForRunTerminal(
      client,
      continuity.runId,
      `released held Run ${continuity.runId} to complete naturally`
    );
    assert.equal(completedHold.status, 'completed', 'fixture:release must naturally complete the held Run');
    assert.equal(completedHold.hub_turn_id, continuity.hubTurnId);
    const { data: completedHoldEvents } = await client.get(`/api/runs/${continuity.runId}/events`);
    assertNoInterruptedOrFailedEvent(completedHoldEvents, 'naturally completed held Run');

    const promotedRuntime = await poll(async () => {
      const { data: runtimes } = await client.get('/api/runtimes');
      return runtimes.find((runtime) => runtime.id === active.run.runtime_id) ?? null;
    }, (runtime) => runtime?.codex_version === targetVersion, {
      timeoutMs: 90_000,
      description: `Runtime ${active.run.runtime_id} to report Codex ${targetVersion}`
    });
    assert.equal(promotedRuntime.hostname, heldRuntime.hostname);

    const continuationMessage = scenarioContext.unique('QA message after Codex promotion');
    const { data: nextAcceptance } = await client.post(
      `/api/sessions/${holdRun.hub_session_id}/messages`,
      {
        content: continuationMessage,
        client_message_key: scenarioContext.unique('codex-rollout-continuation')
      }
    );
    assert.ok(nextAcceptance.run, 'post-rollout message must schedule a new Run');
    assert.notEqual(nextAcceptance.run.id, continuity.runId);
    assert.equal(nextAcceptance.run.hub_session_id, holdRun.hub_session_id);
    assert.notEqual(nextAcceptance.run.hub_turn_id, continuity.hubTurnId);
    assert.equal(nextAcceptance.message.content, continuationMessage);
    assert.equal(nextAcceptance.message.delivery_mode, 'next_turn');
    assert.equal(nextAcceptance.message.run_id, nextAcceptance.run.id);

    const completedNext = await waitForRunTerminal(
      client,
      nextAcceptance.run.id,
      `post-rollout Run ${nextAcceptance.run.id} to complete`
    );
    assert.equal(completedNext.status, 'completed', 'post-rollout Run must complete');
    assert.equal(completedNext.hub_session_id, holdRun.hub_session_id);
    const continuedSession = await poll(async () => (
      await client.get(`/api/sessions/${holdRun.hub_session_id}`)
    ).data, (session) => session.active_turn_id === null, {
      timeoutMs: 30_000,
      description: `continued Session ${holdRun.hub_session_id} to become idle`
    });
    assert.equal(continuedSession.native_thread_id, continuity.nativeThreadId);
    const { data: nextEvents } = await client.get(`/api/runs/${nextAcceptance.run.id}/events`);
    const nextTurnStarted = findTurnStarted(nextEvents, continuity.nativeThreadId);
    assert.ok(nextTurnStarted, 'post-rollout Run must continue the same public native Thread');
    assert.notEqual(nextTurnStarted.payload.native_turn_id, continuity.activeTurnId);
    assertNoInterruptedOrFailedEvent(nextEvents, 'post-rollout Run');

    scenarioCompleted = true;
  } catch (error) {
    scenarioError = error;
  } finally {
    if (agentId) {
      try {
        await cleanupAgentAfterFailure(client, agentId, holdRunId, !scenarioCompleted);
      } catch (cleanupError) {
        scenarioError = scenarioError
          ? new AggregateError([scenarioError, cleanupError], `${scenarioError.message}; Agent cleanup also failed`)
          : cleanupError;
      }
    }
  }

  if (scenarioError) throw scenarioError;
}
