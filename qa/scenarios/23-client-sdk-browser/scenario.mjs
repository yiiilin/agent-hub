import assert from 'node:assert/strict';
import { join } from 'node:path';
import { ApiClient, loginAsAdmin, poll } from '../../support/api.mjs';
import { withBrowser } from '../../support/browser.mjs';
import { startClientSdkHost } from '../../support/client-sdk-host.mjs';

const COMPLETION_TEXT = 'Fake model completed run through the Hub model proxy.';
const HOLD_MESSAGE = 'fixture:hold';
const TOOL_MESSAGE = 'Please use the echo tool and preserve attachments';
const ECHO_TOOL = {
  name: 'echo',
  description: 'Return the supplied Integration message and attachments.',
  input_schema: {
    type: 'object',
    properties: {
      message: { type: 'string' },
      attachments: { type: 'array' }
    },
    required: ['message', 'attachments']
  }
};

function uniqueSlug(context, prefix) {
  return context.unique(prefix)
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, '-')
    .replace(/^-|-$/g, '');
}

async function createComposeModelFixture(client, context) {
  const { data: connection } = await client.post('/api/model-connections', {
    scope: 'personal',
    name: context.unique('QA Client SDK browser model'),
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

async function waitForSnapshot(page, accept, description) {
  return poll(
    () => page.evaluate(() => window.qaSdk.snapshot()),
    accept,
    { timeoutMs: 45_000, description }
  );
}

async function waitForRun(client, agentId, runId, accept, description) {
  return poll(async () => {
    const { data } = await client.get(`/api/agents/${agentId}/runs`);
    return data.find((run) => run.id === runId) ?? null;
  }, (run) => run !== null && accept(run), { timeoutMs: 60_000, description });
}

async function allAgentRuns(client, agentId) {
  return (await client.get(`/api/agents/${agentId}/runs`)).data;
}

function assistantEvent(events, runId) {
  return events.find((event) => (
    event.runId === runId
    && (event.type === 'message' || event.type === 'assistant')
    && event.role === 'assistant'
    && event.content === COMPLETION_TEXT
  ));
}

async function readBrowserPersistence(page) {
  return page.evaluate(async () => {
    const storageValues = (storage) => Object.fromEntries(
      [...Array(storage.length).keys()].map((index) => {
        const key = storage.key(index);
        return [key, key === null ? null : storage.getItem(key)];
      })
    );
    const indexedDbRows = await new Promise((resolve, reject) => {
      const request = indexedDB.open('agent-hub-client');
      request.onerror = () => reject(request.error ?? new Error('IndexedDB open failed'));
      request.onsuccess = () => {
        const database = request.result;
        if (!database.objectStoreNames.contains('tool-journal')) {
          database.close();
          resolve([]);
          return;
        }
        const transaction = database.transaction('tool-journal', 'readonly');
        const read = transaction.objectStore('tool-journal').getAll();
        read.onerror = () => reject(read.error ?? new Error('IndexedDB read failed'));
        read.onsuccess = () => {
          database.close();
          resolve(read.result);
        };
      };
    });
    return {
      href: location.href,
      dom: document.documentElement.outerHTML,
      sessionStorage: storageValues(sessionStorage),
      localStorage: storageValues(localStorage),
      indexedDbRows
    };
  });
}

async function assertNoCredentialPersistence(page, label) {
  const persisted = await readBrowserPersistence(page);
  assert.equal(
    /(?:ahw_|ahp_)/.test(JSON.stringify(persisted)),
    false,
    `${label} must not retain a Client Access Credential in browser-visible persistence`
  );
  return persisted;
}

function discardExpectedSseAborts(browserErrors, hubOrigin, sessionIds) {
  const retained = browserErrors.filter((entry) => {
    const match = /^requestfailed: GET (.+): net::ERR_ABORTED$/.exec(entry);
    if (!match) return true;
    const url = new URL(match[1]);
    if (url.origin !== hubOrigin) return true;
    return ![...sessionIds].some((sessionId) => (
      url.pathname === `/api/client/sessions/${sessionId}/events/stream`
    ));
  });
  browserErrors.splice(0, browserErrors.length, ...retained);
}

export default async function clientSdkBrowserScenario(scenarioContext) {
  const adminClient = new ApiClient(scenarioContext.baseURL);
  await loginAsAdmin(adminClient);
  const fixtureDir = join(scenarioContext.repoRoot, 'qa', 'scenarios', '23-client-sdk-browser');
  const host = await startClientSdkHost({
    repoRoot: scenarioContext.repoRoot,
    hubBaseUrl: scenarioContext.baseURL,
    fixtureDir
  });
  const hubOrigin = new URL(scenarioContext.baseURL).origin;
  const agentIds = [];
  let modelConnectionId = null;
  let scenarioError;

  try {
    const modelFixture = await createComposeModelFixture(adminClient, scenarioContext);
    modelConnectionId = modelFixture.connectionId;
    const { data: authenticatedAgent } = await adminClient.post('/api/agents', {
      name: scenarioContext.unique('QA Client SDK authenticated Agent'),
      instructions: 'Use only the Integration Tool for browser Client Tool verification.',
      visibility: 'private',
      public_to: [],
      model_selection: modelFixture.selection,
      tool_allowlist: ['integration']
    });
    const { data: anonymousAgent } = await adminClient.post('/api/agents', {
      name: scenarioContext.unique('QA Client SDK anonymous Agent'),
      instructions: 'Use only the Integration Tool for anonymous browser Client Tool verification.',
      visibility: 'private',
      public_to: [],
      model_selection: modelFixture.selection,
      tool_allowlist: ['integration']
    });
    agentIds.push(authenticatedAgent.id, anonymousAgent.id);

    const { data: integrationOptions } = await adminClient.get('/api/integration-app-options');
    const channel = integrationOptions.authentication_channels.find((candidate) => (
      candidate.enabled && candidate.trusted_email
    ));
    assert.ok(channel, 'A trusted Authentication Channel must exist for Client SDK QA');
    const platform = integrationOptions.external_platforms.find((candidate) => candidate.id === channel.platform_id);
    assert.ok(platform, 'The trusted Authentication Channel must belong to an External Platform');

    const authExternalUserId = uniqueSlug(scenarioContext, 'qa-sdk-auth-user');
    const authTenantId = uniqueSlug(scenarioContext, 'qa-sdk-auth-tenant');
    const { data: authenticatedSecret } = await adminClient.post('/api/integration-apps', {
      name: scenarioContext.unique('QA Client SDK authenticated App'),
      external_platform_id: platform.id,
      authentication_channel_id: channel.id,
      redirect_uris: [host.url('/callback')],
      agent_ids: [authenticatedAgent.id],
      widget_history_enabled: true,
      login_required: true,
      allowed_origins: [host.origin],
      tool_allowlist: ['integration'],
      client_tool_definitions: []
    });
    host.configureAuthenticated({
      clientId: authenticatedSecret.integration_app.client_id,
      clientSecret: authenticatedSecret.client_secret,
      agentId: authenticatedAgent.id,
      externalUserId: authExternalUserId,
      tenantId: authTenantId,
      username: authExternalUserId,
      displayName: 'QA Client SDK User',
      email: `${authExternalUserId}@example.com`,
      attributes: { scenario: 'client-sdk-browser' },
      clientTools: [ECHO_TOOL],
      toolName: ECHO_TOOL.name
    });

    const { data: anonymousSecret } = await adminClient.post('/api/integration-apps', {
      name: scenarioContext.unique('QA Client SDK anonymous App'),
      external_platform_id: platform.id,
      authentication_channel_id: channel.id,
      redirect_uris: [host.url('/anonymous-callback')],
      agent_ids: [anonymousAgent.id],
      widget_history_enabled: false,
      login_required: false,
      allowed_origins: [host.origin],
      tool_allowlist: ['integration'],
      client_tool_definitions: [ECHO_TOOL]
    });
    host.configureAnonymous({
      clientId: anonymousSecret.integration_app.client_id,
      toolName: ECHO_TOOL.name
    });

    await withBrowser(scenarioContext, {
      monitoredOrigins: [host.origin, host.alternateOrigin],
      allowedHttpErrors: [
        { method: 'GET', pathname: '/api/client/sessions', status: 403, origin: hubOrigin, times: 1 },
        { method: 'POST', pathname: '/api/client/anonymous/access', status: 403, origin: hubOrigin, times: 1 }
      ]
    }, async ({ page, context, browserErrors }) => {
      let tracingPaused = false;
      let observer;
      let authProbe;
      let anonymousProbe;
      const streamSessionIds = new Set();
      let observedRequestCount = 0;
      let credentialRequestUrlLeakCount = 0;
      const inspectRequestUrl = (request) => {
        observedRequestCount += 1;
        if (/(?:ahw_|ahp_)/.test(request.url())) credentialRequestUrlLeakCount += 1;
      };
      context.on('request', inspectRequestUrl);
      try {
        await context.tracing.stop();
        tracingPaused = true;

        const renewalRequest = page.waitForRequest((request) => {
          const url = new URL(request.url());
          return request.method() === 'POST'
            && url.origin === hubOrigin
            && url.pathname === '/api/client/renew';
        });
        await page.goto(host.url('/index.html?mode=authenticated&role=primary&hold=1&renew=immediate'), {
          waitUntil: 'domcontentloaded'
        });
        const authenticatedConnected = await page.evaluate(() => window.qaSdk.ready);
        assert.equal(authenticatedConnected.connected, true, 'Authenticated Client SDK connection must succeed');
        assert.deepEqual(authenticatedConnected.authorizedTools, ['echo']);
        assert.equal(authenticatedConnected.initialClientInstanceId, null);
        await renewalRequest;
        const renewed = await waitForSnapshot(
          page,
          (snapshot) => snapshot.renewCount === 1,
          'authenticated SDK credential to renew through the real Client API'
        );
        assert.equal(
          renewed.clientInstanceId,
          authenticatedConnected.clientInstanceId,
          'Credential renewal must retain the current Client Instance ID'
        );

        const warmup = await page.evaluate(() => window.qaSdk.send('Initialize the browser Client SDK Session.'));
        await waitForRun(
          adminClient,
          authenticatedAgent.id,
          warmup.runId,
          (run) => run.status === 'completed',
          'authenticated warmup Run to complete'
        );
        streamSessionIds.add(warmup.sessionId);
        assert.equal(
          await page.evaluate(() => window.qaSdk.snapshot().sessionId),
          warmup.sessionId,
          'First authenticated send must materialize the exact external Session'
        );

        const originalStreamRequest = page.waitForRequest((request) => {
          const url = new URL(request.url());
          return request.method() === 'GET'
            && url.origin === hubOrigin
            && url.pathname === `/api/client/sessions/${warmup.sessionId}/events/stream`;
        });
        await page.evaluate(() => window.qaSdk.subscribe(0));
        await originalStreamRequest;
        const warmupEvents = await waitForSnapshot(
          page,
          (snapshot) => Boolean(assistantEvent(snapshot.events, warmup.runId)),
          'authenticated warmup assistant event through the real SDK SSE subscription'
        );

        const holdSent = await page.evaluate((message) => window.qaSdk.send(message), HOLD_MESSAGE);
        await poll(async () => {
          const { data: events } = await adminClient.get(`/api/runs/${holdSent.runId}/events`);
          return events.find((event) => event.event_type === 'turn_started') ?? null;
        }, Boolean, {
          timeoutMs: 60_000,
          description: 'authenticated SDK hold Run to start its native Turn'
        });
        const steered = await page.evaluate(
          (message) => window.qaSdk.send(message),
          HOLD_MESSAGE
        );
        assert.equal(steered.runId, holdSent.runId, 'Active Turn steering must reuse the same Run');
        assert.equal(steered.sessionId, holdSent.sessionId, 'Active Turn steering must remain in the same Session');
        const stopRequested = await page.evaluate(
          (runId) => window.qaSdk.stop(runId),
          holdSent.runId
        );
        assert.equal(stopRequested.id, holdSent.runId, 'SDK stop must target the active Run');
        await waitForRun(
          adminClient,
          authenticatedAgent.id,
          holdSent.runId,
          (run) => run.status === 'interrupted',
          'authenticated SDK Run to stop after steering'
        );
        const toolDraft = await page.evaluate(() => window.qaSdk.newDraft());
        assert.equal(toolDraft.sessionId, null, 'Authenticated Client must create a new local draft after stop');

        const toolSent = await page.evaluate((message) => window.qaSdk.send(message), TOOL_MESSAGE);
        streamSessionIds.add(toolSent.sessionId);
        const toolStreamRequest = page.waitForRequest((request) => {
          const url = new URL(request.url());
          return request.method() === 'GET'
            && url.origin === hubOrigin
            && url.pathname === `/api/client/sessions/${toolSent.sessionId}/events/stream`;
        });
        await page.evaluate(() => window.qaSdk.subscribe(0));
        await toolStreamRequest;
        await waitForRun(
          adminClient,
          authenticatedAgent.id,
          toolSent.runId,
          (run) => run.status === 'waiting_tool',
          'authenticated Client Tool Run to await the primary handler'
        );
        const primaryClaimed = await waitForSnapshot(
          page,
          (snapshot) => snapshot.handlerStarted === 1 && snapshot.handlerCalls === 1,
          'primary SDK Client Tool handler to start after claiming its Run-bound request'
        );
        const toolRequest = primaryClaimed.events.find((event) => (
          event.type === 'tool_request' && event.runId === toolSent.runId && event.toolName === ECHO_TOOL.name
        ));
        assert.ok(toolRequest, 'Primary SDK must receive the real Client Tool request');
        assert.deepEqual(primaryClaimed.handlerInputs, [{ message: TOOL_MESSAGE, attachments: [] }]);

        const popupPromise = context.waitForEvent('page');
        await page.getByRole('button', { name: 'Open observer tab' }).click();
        observer = await popupPromise;
        await observer.waitForLoadState('domcontentloaded');
        const observerConnected = await observer.evaluate(() => window.qaSdk.ready);
        assert.equal(observerConnected.connected, true, 'Observer tab must connect through the real SDK');
        assert.equal(
          observerConnected.initialClientInstanceId,
          authenticatedConnected.clientInstanceId,
          'window.open must clone the opener sessionStorage before SDK instance reservation'
        );
        assert.notEqual(
          observerConnected.clientInstanceId,
          authenticatedConnected.clientInstanceId,
          'BroadcastChannel reservation must rotate the cloned Client Instance ID'
        );
        assert.equal(observerConnected.sessionId, toolSent.sessionId);

        const observerStreamRequest = observer.waitForRequest((request) => {
          const url = new URL(request.url());
          return request.method() === 'GET'
            && url.origin === hubOrigin
            && url.pathname === `/api/client/sessions/${toolSent.sessionId}/events/stream`;
        });
        await observer.evaluate(() => window.qaSdk.subscribe(0));
        await observerStreamRequest;
        const observerRejected = await waitForSnapshot(
          observer,
          (snapshot) => snapshot.errors.some((event) => event.type === 'error' && event.status === 403),
          'observer SDK Client Tool dispatch to be rejected by the Run-bound Client Instance scope'
        );
        assert.equal(observerRejected.handlerCalls, 0, 'Observer must never execute a Run bound to the primary Client Instance');
        const observerClaimError = observerRejected.errors.find((event) => event.type === 'error' && event.status === 403);
        assert.ok(observerClaimError, 'Observer must surface the rejected Client Tool dispatch as an SDK event');
        const expectedObserverClaimError = `response: 403 POST ${new URL(
          `/api/client/tool-calls/${toolRequest.toolCallId}/claim`,
          scenarioContext.baseURL
        ).href}`;
        const claimErrorIndex = browserErrors.indexOf(expectedObserverClaimError);
        assert.notEqual(claimErrorIndex, -1, 'Only the exact observer claim rejection may be accepted');
        browserErrors.splice(claimErrorIndex, 1);

        await page.evaluate(() => window.qaSdk.release());
        const continuation = await poll(async () => {
          const runs = await allAgentRuns(adminClient, authenticatedAgent.id);
          return runs.filter((run) => (
            run.parent_run_id === toolSent.runId && run.source === 'integration:tool_result'
          ));
        }, (runs) => runs.length === 1 && runs[0].status === 'completed', {
          timeoutMs: 60_000,
          description: 'exactly one authenticated Client Tool continuation Run to complete'
        });
        const continuationRun = continuation[0];
        const completedEvents = await waitForSnapshot(
          page,
          (snapshot) => Boolean(assistantEvent(snapshot.events, continuationRun.id)),
          'Client Tool continuation assistant event through the original SDK subscription'
        );
        const firstToolRequestIndex = completedEvents.events.findIndex((event) => event.toolCallId === toolRequest.toolCallId && event.type === 'tool_request');
        const firstToolResultIndex = completedEvents.events.findIndex((event) => event.toolCallId === toolRequest.toolCallId && event.type === 'tool_result');
        const continuationAssistantIndex = completedEvents.events.findIndex((event) => (
          event.runId === continuationRun.id
          && (event.type === 'message' || event.type === 'assistant')
          && event.role === 'assistant'
          && event.content === COMPLETION_TEXT
        ));
        assert.ok(firstToolRequestIndex >= 0, 'The SDK event sequence must include tool_request');
        assert.ok(firstToolResultIndex > firstToolRequestIndex, 'tool_result must follow tool_request');
        assert.ok(continuationAssistantIndex > firstToolResultIndex, 'assistant continuation must follow tool_result');

        const resultPostsBeforeReplay = completedEvents.resultPostCount;
        await page.evaluate(() => window.qaSdk.subscribe(0));
        const replayed = await waitForSnapshot(
          page,
          (snapshot) => snapshot.resultPostCount === resultPostsBeforeReplay + 1,
          'replayed Client Tool request to resubmit the acknowledged result from IndexedDB'
        );
        assert.equal(replayed.handlerCalls, 1, 'SSE replay must not execute an acknowledged handler twice');
        const afterReplayRuns = (await allAgentRuns(adminClient, authenticatedAgent.id)).filter((run) => (
          run.parent_run_id === toolSent.runId && run.source === 'integration:tool_result'
        ));
        assert.equal(afterReplayRuns.length, 1, 'Cached result replay must not create a second continuation Run');

        const authPersistence = await assertNoCredentialPersistence(page, 'Authenticated primary tab');
        const observerPersistence = await assertNoCredentialPersistence(observer, 'Authenticated observer tab');
        const primaryJournal = authPersistence.indexedDbRows.filter((entry) => (
          entry.clientInstanceId === authenticatedConnected.clientInstanceId && entry.toolCallId === toolRequest.toolCallId
        ));
        assert.equal(primaryJournal.length, 1, 'Primary IndexedDB journal must contain the Client Tool entry exactly once');
        assert.equal(primaryJournal[0].state, 'acknowledged');
        assert.equal(
          observerPersistence.indexedDbRows.some((entry) => (
            entry.clientInstanceId === observerConnected.clientInstanceId
            && entry.toolCallId === toolRequest.toolCallId
          )),
          true,
          'Shared IndexedDB rows must remain partitioned by Client Instance ID'
        );

        authProbe = await context.newPage();
        await authProbe.goto(`${host.alternateOrigin}/index.html?mode=authenticated&grant_origin=allowed`, {
          waitUntil: 'domcontentloaded'
        });
        const authProbeConnected = await authProbe.evaluate(() => window.qaSdk.ready);
        assert.equal(authProbeConnected.connected, true, 'Fixture grant may issue a token from the allowed origin');
        const authOriginError = await authProbe.evaluate(() => window.qaSdk.listSessionsError());
        assert.deepEqual(
          { code: authOriginError.code, status: authOriginError.status },
          { code: 'request_failed', status: 403 },
          'Authenticated Client Session requests must enforce the exact request Origin'
        );
        await assertNoCredentialPersistence(authProbe, 'Authenticated Origin probe');
        await authProbe.evaluate(() => window.qaSdk.dispose());

        await observer.evaluate(() => window.qaSdk.dispose());
        await page.evaluate(() => {
          window.qaSdk.dispose();
          sessionStorage.clear();
        });
        await observer.close();
        observer = undefined;

        await page.goto(host.url('/index.html?mode=anonymous&role=primary'), { waitUntil: 'domcontentloaded' });
        const anonymousConnected = await page.evaluate(() => window.qaSdk.ready);
        assert.equal(anonymousConnected.connected, true, 'Anonymous Client SDK connection must succeed on its exact Origin');
        assert.deepEqual(anonymousConnected.authorizedTools, ['echo']);
        assert.equal(anonymousConnected.sessionId, null, 'Anonymous SDK starts with a local draft before its first message');
        const anonymousHistoryError = await page.evaluate(() => window.qaSdk.listSessionsError());
        assert.deepEqual(
          { code: anonymousHistoryError.code, status: anonymousHistoryError.status },
          { code: 'anonymous_history_disabled', status: 403 },
          'Anonymous SDK must explicitly reject history listing'
        );

        const anonymousSent = await page.evaluate((message) => window.qaSdk.send(message), TOOL_MESSAGE);
        streamSessionIds.add(anonymousSent.sessionId);
        await waitForRun(
          adminClient,
          anonymousAgent.id,
          anonymousSent.runId,
          (run) => run.status === 'waiting_tool',
          'anonymous Client Tool Run to await its handler'
        );
        const anonymousStreamRequest = page.waitForRequest((request) => {
          const url = new URL(request.url());
          return request.method() === 'GET'
            && url.origin === hubOrigin
            && url.pathname === `/api/client/sessions/${anonymousSent.sessionId}/events/stream`;
        });
        await page.evaluate(() => window.qaSdk.subscribe(0));
        await anonymousStreamRequest;
        const anonymousHandled = await waitForSnapshot(
          page,
          (snapshot) => snapshot.handlerCalls === 1 && snapshot.handlerInputs.length === 1,
          'anonymous SDK Client Tool handler to execute once'
        );
        assert.deepEqual(anonymousHandled.handlerInputs, [{ message: TOOL_MESSAGE, attachments: [] }]);
        const anonymousContinuation = await poll(async () => {
          const runs = await allAgentRuns(adminClient, anonymousAgent.id);
          return runs.filter((run) => (
            run.parent_run_id === anonymousSent.runId && run.source === 'integration:tool_result'
          ));
        }, (runs) => runs.length === 1 && runs[0].status === 'completed', {
          timeoutMs: 60_000,
          description: 'exactly one anonymous Client Tool continuation Run to complete'
        });
        await waitForSnapshot(
          page,
          (snapshot) => Boolean(assistantEvent(snapshot.events, anonymousContinuation[0].id)),
          'anonymous Client Tool continuation assistant event'
        );
        const anonymousPersistence = await assertNoCredentialPersistence(page, 'Anonymous primary tab before reload');
        const visitorStorageKey = `agent-hub:anonymous:${encodeURIComponent(anonymousSecret.integration_app.client_id)}:visitor`;
        const sessionStorageKey = 'agent-hub:client-instance-id';
        const visitorKey = anonymousPersistence.localStorage[visitorStorageKey];
        assert.equal(typeof visitorKey, 'string', 'Anonymous Client SDK must persist its visitor key locally');
        assert.equal(anonymousPersistence.sessionStorage[sessionStorageKey], anonymousConnected.clientInstanceId);

        await page.evaluate(() => window.qaSdk.dispose());
        await page.reload({ waitUntil: 'domcontentloaded' });
        const anonymousReloaded = await page.evaluate(() => window.qaSdk.ready);
        assert.equal(anonymousReloaded.connected, true, 'Anonymous SDK must reconnect after a reload');
        assert.equal(anonymousReloaded.clientInstanceId, anonymousConnected.clientInstanceId);
        assert.equal(anonymousReloaded.sessionId, anonymousSent.sessionId, 'Anonymous SDK must recover its exact Session');
        const reloadedMessages = await page.evaluate(() => window.qaSdk.messages());
        assert.equal(
          reloadedMessages.some((message) => message.role === 'user' && message.content === TOOL_MESSAGE),
          true,
          'Anonymous recovered Session must retain its accepted user message'
        );
        await page.evaluate(() => window.qaSdk.subscribe(0));
        const anonymousReplayed = await waitForSnapshot(
          page,
          (snapshot) => Boolean(assistantEvent(snapshot.events, anonymousContinuation[0].id)),
          'anonymous recovered Session to replay its completed assistant event'
        );
        assert.equal(
          anonymousReplayed.handlerCalls,
          0,
          'Anonymous IndexedDB replay must not execute the acknowledged handler after reload'
        );
        const reloadedPersistence = await assertNoCredentialPersistence(page, 'Anonymous primary tab after reload');
        assert.equal(reloadedPersistence.localStorage[visitorStorageKey], visitorKey);

        anonymousProbe = await context.newPage();
        await anonymousProbe.goto(`${host.alternateOrigin}/index.html?mode=anonymous`, {
          waitUntil: 'domcontentloaded'
        });
        const anonymousProbeState = await anonymousProbe.evaluate(() => window.qaSdk.ready);
        assert.equal(anonymousProbeState.connected, false, 'Anonymous Client Access must reject an unlisted exact Origin');
        assert.deepEqual(
          {
            code: anonymousProbeState.errors[0]?.code,
            status: anonymousProbeState.errors[0]?.status
          },
          { code: 'request_failed', status: 403 },
          'Anonymous Origin rejection must surface as a structured SDK error'
        );

        await assertNoCredentialPersistence(anonymousProbe, 'Anonymous Origin probe');
        await anonymousProbe.evaluate(() => window.qaSdk.dispose());
        await page.evaluate(() => window.qaSdk.dispose());
        await Promise.all([
          authProbe.goto('about:blank'),
          anonymousProbe.goto('about:blank'),
          page.goto('about:blank')
        ]);

        assert.ok(observedRequestCount > 0, 'Browser request URL safety oracle must observe real requests');
        assert.equal(
          credentialRequestUrlLeakCount,
          0,
          'Client Access Credentials must never appear in any browser request URL'
        );
        discardExpectedSseAborts(browserErrors, hubOrigin, streamSessionIds);
        assert.deepEqual(browserErrors, [], 'No unexpected browser diagnostics may remain after exact expected failures');
      } finally {
        context.off('request', inspectRequestUrl);
        await Promise.all([
          observer?.evaluate(() => window.qaSdk.dispose()).catch(() => undefined),
          authProbe?.evaluate(() => window.qaSdk.dispose()).catch(() => undefined),
          anonymousProbe?.evaluate(() => window.qaSdk.dispose()).catch(() => undefined),
          page.evaluate(() => window.qaSdk?.dispose()).catch(() => undefined)
        ]);
        await Promise.all([
          observer?.goto('about:blank').catch(() => undefined),
          authProbe?.goto('about:blank').catch(() => undefined),
          anonymousProbe?.goto('about:blank').catch(() => undefined),
          page.goto('about:blank').catch(() => undefined)
        ]);
        if (tracingPaused) {
          await context.tracing.start({ snapshots: true, sources: true });
        }
      }
    });
  } catch (error) {
    scenarioError = error;
  }

  const cleanupErrors = [];
  try {
    for (const agentId of agentIds.reverse()) {
      try {
        await adminClient.delete(`/api/agents/${agentId}`, { expectedStatus: [204, 404] });
      } catch (error) {
        cleanupErrors.push(error);
      }
    }
    try {
      if (modelConnectionId) {
        await adminClient.delete(`/api/model-connections/${modelConnectionId}`, {
          expectedStatus: [204, 404]
        });
      }
    } catch (error) {
      cleanupErrors.push(error);
    }
  } finally {
    try {
      await host.close();
    } catch (error) {
      cleanupErrors.push(error);
    }
  }

  if (scenarioError) throw scenarioError;
  if (cleanupErrors.length === 1) throw cleanupErrors[0];
  if (cleanupErrors.length > 1) throw new AggregateError(cleanupErrors, 'Client SDK QA cleanup failed');
}
