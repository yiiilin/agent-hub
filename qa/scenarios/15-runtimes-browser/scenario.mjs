import assert from 'node:assert/strict';
import { createHash, randomUUID } from 'node:crypto';
import { ApiClient, loginAsAdmin, poll } from '../../support/api.mjs';
import { withBrowser } from '../../support/browser.mjs';

const UUID_PATTERN = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/;
const RECOVERY_FAILURE_REASON = 'Runtime was force deleted without a restorable current Session Bundle';

function assertUuid(value, label) {
  assert.match(value, UUID_PATTERN, `${label} must be a UUID`);
}

function uniqueSlug(context, prefix) {
  return context.unique(prefix)
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, '-')
    .replace(/^-|-$/g, '');
}

function runtimeHeaders(credential) {
  return { authorization: `Bearer ${credential}` };
}

function assertSecretShape(secret, prefix, label) {
  assert.equal(
    typeof secret === 'string' && secret.startsWith(prefix) && secret.length > prefix.length + 20,
    true,
    `${label} must have the expected opaque shape`
  );
}

function updateAgentPayload(agent, runtimeId) {
  return {
    name: agent.name,
    instructions: agent.instructions,
    visibility: agent.visibility,
    public_to: agent.public_to,
    runtime_id: runtimeId,
    model_selection: agent.model_selection,
    model_settings: agent.model_settings,
    subagents: agent.subagents,
    sandbox_policy: agent.sandbox_policy,
    managed_skill_ids: agent.managed_skill_ids,
    mcp_allowlist: agent.mcp_allowlist
  };
}

async function loginBrowser(page, email, password) {
  await page.goto('/login', { waitUntil: 'domcontentloaded' });
  await page.waitForLoadState('networkidle');
  await page.getByLabel('Email').fill(email);
  await page.getByLabel('Password').fill(password);
  await page.getByRole('button', { name: 'Sign in', exact: true }).click();
  await page.getByText(email, { exact: true }).waitFor();
}

async function openRuntimes(page) {
  await page.goto('/runtimes', { waitUntil: 'domcontentloaded' });
  await page.getByRole('heading', { name: 'Runtime Nodes', level: 1 }).waitFor();
  await page.locator('.runtime-workspace').waitFor();
}

async function assertNoHorizontalOverflow(page, label, selectors = []) {
  await page.evaluate(() => new Promise((resolve) => {
    requestAnimationFrame(() => requestAnimationFrame(resolve));
  }));
  const documentOverflow = await page.evaluate(() => (
    document.documentElement.scrollWidth - document.documentElement.clientWidth
  ));
  assert.ok(documentOverflow <= 1, `${label} document horizontal overflow: ${documentOverflow}px`);
  for (const selector of selectors) {
    const locator = page.locator(selector);
    await locator.waitFor({ state: 'visible' });
    const overflow = await locator.evaluate((element) => element.scrollWidth - element.clientWidth);
    assert.ok(overflow <= 1, `${label} ${selector} horizontal overflow: ${overflow}px`);
  }
}

async function assertDialogFitsViewport(page, dialog, label) {
  await assertNoHorizontalOverflow(page, label, ['.runtime-workspace', '.runtime-action-dialog']);
  const viewport = page.viewportSize();
  const box = await dialog.boundingBox();
  assert.ok(viewport, `${label} must have a viewport`);
  assert.ok(box, `${label} dialog must have geometry`);
  assert.ok(box.x >= -1, `${label} dialog must not escape the left edge`);
  assert.ok(box.x + box.width <= viewport.width + 1, `${label} dialog must not escape the right edge`);
}

async function createEnrollmentThroughUi(page, browserContext) {
  await page.getByRole('button', { name: 'Add runtime node' }).click();
  const dialog = page.getByRole('dialog', { name: 'Add runtime node' });
  await dialog.waitFor();
  await dialog.getByText('RUNTIME_ENROLLMENT_TOKEN=<token>', { exact: true }).waitFor();

  await browserContext.tracing.stop();
  let secret = null;
  try {
    const responsePromise = page.waitForResponse((response) => (
      response.request().method() === 'POST'
      && new URL(response.url()).pathname === '/api/admin/runtime-enrollment-tokens'
    ));
    await dialog.getByRole('button', { name: 'Create enrollment token' }).click();
    const response = await responsePromise;
    assert.equal(response.ok(), true, `Enrollment creation returned ${response.status()}`);
    const token = dialog.getByTestId('runtime-enrollment-token');
    await token.waitFor();
    secret = await token.textContent();
    assertSecretShape(secret, 'ahre_', 'Enrollment token');
  } finally {
    if (await dialog.isVisible().catch(() => false)) {
      const close = dialog.locator('footer').getByRole('button', { name: 'Close' });
      if (await close.count()) await close.click();
      else await dialog.locator('footer').getByRole('button', { name: 'Cancel' }).click();
      await dialog.waitFor({ state: 'detached' });
    }
    await browserContext.tracing.start({ screenshots: true, snapshots: true, sources: true });
  }
  return secret;
}

async function registerRuntimeWithToken(context, template, token, hostname, label) {
  const client = new ApiClient(context.baseURL);
  const { data } = await client.post('/api/runtime/register', {
    hostname,
    labels: ['qa', 'browser', label],
    engine_version: template.engine_version,
    capabilities: structuredClone(template.capabilities),
    sandbox_mode: template.sandbox_mode
  }, {
    headers: runtimeHeaders(token)
  });
  assertUuid(data.runtime_id, `${label} Runtime id`);
  assertSecretShape(data.runtime_credential, 'ahrc_', `${label} Runtime credential`);
  return {
    id: data.runtime_id,
    hostname,
    credential: data.runtime_credential,
    client,
    deleted: false
  };
}

async function createRuntimeThroughApi(admin, context, template, prefix, label) {
  const { data: created } = await admin.post('/api/admin/runtime-enrollment-tokens');
  assertUuid(created.enrollment.id, `${label} enrollment id`);
  assertSecretShape(created.token, 'ahre_', `${label} enrollment token`);
  return registerRuntimeWithToken(
    context,
    template,
    created.token,
    uniqueSlug(context, prefix),
    label
  );
}

async function createBoundAgent(admin, context, runtimeId, prefix, agentIds) {
  const { data: created } = await admin.post('/api/agents', {
    name: context.unique(prefix),
    instructions: 'Exercise Runtime ownership and recovery through public protocols.',
    visibility: 'private',
    public_to: []
  });
  assertUuid(created.id, `${prefix} Agent id`);
  agentIds.push(created.id);
  const { data: bound } = await admin.request(`/api/agents/${created.id}`, {
    method: 'PATCH',
    body: updateAgentPayload(created, runtimeId)
  });
  assert.equal(bound.runtime_id, runtimeId);
  return bound;
}

async function runtimePost(runtime, path, body = {}, expectedStatus = 200) {
  return runtime.client.post(path, body, {
    headers: runtimeHeaders(runtime.credential),
    expectedStatus
  });
}

async function heartbeat(runtime, body = {}, expectedStatus = 200) {
  return runtimePost(runtime, '/api/runtime/heartbeat', body, expectedStatus);
}

async function createOwnedSession(admin, context, runtime, agent) {
  const { data: run } = await admin.post(`/api/agents/${agent.id}/runs`, {
    message: context.unique('QA Runtime browser ownership'),
    hub_session_id: null,
    parent_run_id: null
  });
  assertUuid(run.id, 'Owned Run id');
  assertUuid(run.hub_session_id, 'Owned Session id');

  const { data: claim } = await runtimePost(runtime, '/api/runtime/runs/claim', {
    available_new_session_slots: 1,
    ready_owned_sessions: []
  });
  assert.equal(claim.run.id, run.id);
  assert.equal(claim.run.runtime_id, runtime.id);
  assert.equal(claim.agent.id, agent.id);
  assert.ok(claim.session_context, 'Runtime claim must contain Session context');
  const generation = claim.run.session_ownership_generation;
  assert.ok(Number.isInteger(generation) && generation > 0, 'Ownership generation must be positive');
  assert.equal(claim.session_context.session.runtime_owner_id, runtime.id);
  assert.equal(claim.session_context.session.current_bundle, null);

  const { data: begun } = await runtimePost(runtime, `/api/runtime/runs/${run.id}/turn/begin`, {
    ownership_generation: generation,
    payload: { configuration_fingerprint: claim.expected_configuration_fingerprint }
  });
  assert.equal(begun.ownership_generation, generation);

  const nativeSessionId = uniqueSlug(context, 'qa-runtime-browser-thread');
  const { data: started } = await runtimePost(runtime, `/api/runtime/runs/${run.id}/events`, {
    ownership_generation: generation,
    payload: {
      event_type: 'turn_started',
      role: null,
      content: null,
      payload: {
        native_session_id: nativeSessionId,
        native_turn_id: uniqueSlug(context, 'qa-runtime-browser-turn')
      }
    }
  });
  assert.equal(started.payload.native_session_id, nativeSessionId);

  const { data: completed } = await runtimePost(runtime, `/api/runtime/runs/${run.id}/complete`, {
    ownership_generation: generation,
    payload: {
      status: 'completed',
      native_session_id: nativeSessionId,
      work_dir_ref: `/qa/${run.id}`
    }
  });
  assert.equal(completed.status, 'completed');

  const { data: heartbeatResult } = await heartbeat(runtime, {
    accepts_session_commands: true,
    owned_sessions: [{
      session_id: run.hub_session_id,
      ownership_generation: generation,
      lifecycle_status: 'online'
    }]
  });
  assert.ok(
    heartbeatResult.owned_sessions.some((session) => session.session_id === run.hub_session_id),
    'Heartbeat must report the claimed Session ownership'
  );

  const { data: session } = await admin.get(`/api/sessions/${run.hub_session_id}`);
  assert.equal(session.runtime_owner_id, runtime.id);
  assert.equal(session.ownership_generation, generation);
  assert.equal(session.lifecycle_status, 'online');
  assert.equal(session.current_bundle, null);
  return { run, session, generation };
}

async function waitForRuntimeRow(page, hostname) {
  const row = page.getByRole('button', { name: new RegExp(hostname) });
  await row.waitFor({ state: 'visible', timeout: 15_000 });
  return row;
}

async function cleanupMember(admin, member) {
  if (!member || member.erased) return;
  const response = await admin.post(`/api/admin/users/${member.id}/erase`, {
    username: member.username
  }, { expectedStatus: [202, 404] });
  if (response.status === 202) {
    await poll(async () => {
      const { data: history } = await admin.get('/api/admin/user-erasures');
      return history.find((item) => item.user_id === member.id) ?? null;
    }, (item) => item?.status === 'completed', {
      timeoutMs: 45_000,
      description: `member ${member.id} erasure to complete`
    });
  }
  member.erased = true;
}

export default async function runtimesBrowserScenario(scenarioContext) {
  const admin = new ApiClient(scenarioContext.baseURL);
  const { data: superAdmin } = await loginAsAdmin(admin);
  assert.equal(superAdmin.role, 'super_admin');

  const [{ data: baselineRuntimes }, { data: baselineEnrollments }] = await Promise.all([
    admin.get('/api/runtimes'),
    admin.get('/api/admin/runtime-enrollment-tokens')
  ]);
  const template = baselineRuntimes.find((runtime) => runtime.status === 'online');
  assert.ok(template, 'Compose must provide an online Runtime registration template');
  const baselineRuntimeIds = new Set(baselineRuntimes.map((runtime) => runtime.id));
  const baselineEnrollmentIds = new Set(baselineEnrollments.map((enrollment) => enrollment.id));

  const runtimes = [];
  const agentIds = [];
  const cleanupErrors = [];
  let primary = null;
  let ordinary = null;
  let owned = null;
  let member = null;
  let scenarioError = null;

  try {
    const memberUsername = uniqueSlug(scenarioContext, 'qa-runtime-browser-member');
    const memberEmail = `${memberUsername}@example.com`;
    const memberPassword = `${scenarioContext.unique('Runtime member password')}!Aa9`;
    const memberClient = new ApiClient(scenarioContext.baseURL);
    const { data: registration } = await memberClient.post('/api/auth/register', {
      email: memberEmail,
      password: memberPassword
    });
    assert.equal(registration.user.role, 'member');
    member = {
      id: registration.user.id,
      username: registration.user.username,
      email: memberEmail,
      password: memberPassword,
      erased: false
    };

    await withBrowser(scenarioContext, {
      allowedHttpErrors: [
        { method: 'GET', pathname: '/api/auth/me', status: 401, times: 1 }
      ]
    }, async ({ page, context: browserContext, browserErrors }) => {
      let sessionMessageMutations = 0;
      const allowedNoContentAborts = new Set();
      page.on('request', (request) => {
        if (request.method() === 'POST'
          && /^\/api\/sessions\/[^/]+\/messages$/.test(new URL(request.url()).pathname)) {
          sessionMessageMutations += 1;
        }
      });

      await loginBrowser(page, 'admin@example.com', 'admin123');
      await openRuntimes(page);
      await assertNoHorizontalOverflow(page, 'Runtime desktop', [
        '.runtime-workspace',
        '.runtime-layout',
        '.runtime-detail'
      ]);

      const enrollmentIdsBeforePrimary = new Set(
        (await admin.get('/api/admin/runtime-enrollment-tokens')).data.map((item) => item.id)
      );
      let primaryEnrollmentSecret = await createEnrollmentThroughUi(page, browserContext);
      assert.equal((await page.locator('body').innerText()).includes(primaryEnrollmentSecret), false);
      const { data: enrollmentsAfterPrimaryCreate } = await admin.get(
        '/api/admin/runtime-enrollment-tokens'
      );
      const primaryEnrollment = enrollmentsAfterPrimaryCreate.find(
        (item) => !enrollmentIdsBeforePrimary.has(item.id)
      );
      assert.ok(primaryEnrollment, 'UI-created primary enrollment must be listed without its secret');
      assert.equal(Object.hasOwn(primaryEnrollment, 'token'), false);
      assert.equal(Object.hasOwn(primaryEnrollment, 'token_hash'), false);

      primary = await registerRuntimeWithToken(
        scenarioContext,
        template,
        primaryEnrollmentSecret,
        uniqueSlug(scenarioContext, 'qa-runtime-browser-primary'),
        'primary'
      );
      runtimes.push(primary);
      const reused = await new ApiClient(scenarioContext.baseURL).post('/api/runtime/register', {
        hostname: uniqueSlug(scenarioContext, 'qa-runtime-browser-reused'),
        labels: ['qa', 'browser', 'reused'],
        engine_version: template.engine_version,
        capabilities: structuredClone(template.capabilities),
        sandbox_mode: template.sandbox_mode
      }, {
        headers: runtimeHeaders(primaryEnrollmentSecret),
        expectedStatus: 401
      });
      assert.equal(reused.status, 401);
      const { data: listedAfterRegistration } = await admin.get(
        '/api/admin/runtime-enrollment-tokens'
      );
      assert.equal(JSON.stringify(listedAfterRegistration).includes(primaryEnrollmentSecret), false);
      const consumedEnrollment = listedAfterRegistration.find(
        (item) => item.id === primaryEnrollment.id
      );
      assert.equal(consumedEnrollment.consumed_by_runtime_id, primary.id);
      assert.equal(typeof consumedEnrollment.consumed_at, 'string');
      primaryEnrollmentSecret = null;

      await page.reload({ waitUntil: 'domcontentloaded' });
      await page.getByRole('heading', { name: 'Runtime Nodes', level: 1 }).waitFor();
      const primaryRow = await waitForRuntimeRow(page, primary.hostname);
      await primaryRow.click();
      const detail = page.getByRole('region', { name: 'Runtime details' });
      await detail.getByRole('heading', { name: primary.hostname }).waitFor();
      const primaryDetailText = await detail.innerText();
      assert.ok(primaryDetailText.includes(primary.id), 'Runtime details must show the registered id');
      assert.ok(primaryDetailText.includes(template.engine_version), 'Runtime details must show engine version');
      assert.ok(primaryDetailText.includes(template.sandbox_mode), 'Runtime details must show sandbox mode');
      assert.equal(
        await detail.getByRole('button', { name: 'Drain runtime' }).isDisabled(),
        true,
        'Drain must be disabled before an Agent binding exists'
      );
      assert.equal(await detail.getByRole('button', { name: 'Rotate credential' }).isVisible(), true);
      assert.equal(await detail.getByRole('button', { name: 'Force-delete runtime' }).isVisible(), true);

      const enrollmentIdsBeforeUnused = new Set(
        listedAfterRegistration.map((item) => item.id)
      );
      let unusedEnrollmentSecret = await createEnrollmentThroughUi(page, browserContext);
      assert.equal((await page.locator('body').innerText()).includes(unusedEnrollmentSecret), false);
      const { data: enrollmentsWithUnused } = await admin.get('/api/admin/runtime-enrollment-tokens');
      const unusedEnrollment = enrollmentsWithUnused.find(
        (item) => !enrollmentIdsBeforeUnused.has(item.id)
      );
      assert.ok(unusedEnrollment, 'Second UI enrollment must remain unused until revoked');
      assert.equal(unusedEnrollment.consumed_at, null);
      assert.equal(unusedEnrollment.revoked_at, null);
      assert.equal(JSON.stringify(enrollmentsWithUnused).includes(unusedEnrollmentSecret), false);
      unusedEnrollmentSecret = null;

      const enrollmentHistory = page.getByRole('region', { name: 'Enrollment history' });
      const revokeResponsePromise = page.waitForResponse((response) => (
        response.request().method() === 'POST'
        && new URL(response.url()).pathname
          === `/api/admin/runtime-enrollment-tokens/${unusedEnrollment.id}/revoke`
      ));
      await enrollmentHistory.getByRole('button', { name: 'Revoke token' }).first().click();
      const revokeResponse = await revokeResponsePromise;
      assert.equal(revokeResponse.ok(), true, `Enrollment revoke returned ${revokeResponse.status()}`);
      await poll(async () => (
        await enrollmentHistory.getByRole('button', { name: 'Revoke token' }).count()
      ), (count) => count === 0, {
        timeoutMs: 5_000,
        description: 'unused enrollment to disappear from the available list'
      });
      const revokedEnrollment = (
        await admin.get('/api/admin/runtime-enrollment-tokens')
      ).data.find((item) => item.id === unusedEnrollment.id);
      assert.equal(typeof revokedEnrollment.revoked_at, 'string');

      const primaryAgent = await createBoundAgent(
        admin,
        scenarioContext,
        primary.id,
        'QA Runtime Browser Recovery Agent',
        agentIds
      );
      await detail.getByRole('link', { name: primaryAgent.name }).waitFor({ timeout: 10_000 });
      await poll(
        () => detail.getByRole('button', { name: 'Drain runtime' }).isEnabled(),
        Boolean,
        { timeoutMs: 10_000, description: 'Drain control to follow the Agent binding' }
      );

      const rotationResponsePromise = page.waitForResponse((response) => (
        response.request().method() === 'POST'
        && new URL(response.url()).pathname
          === `/api/admin/runtimes/${primary.id}/credential-rotation`
      ));
      await detail.getByRole('button', { name: 'Rotate credential' }).click();
      const rotationResponse = await rotationResponsePromise;
      assert.equal(rotationResponse.ok(), true, `Credential rotation returned ${rotationResponse.status()}`);
      await page.getByText('Runtime credential rotation requested.', { exact: true }).waitFor();
      await detail.getByText(
        'Credential rotation is waiting for the Runtime to complete its handoff.',
        { exact: true }
      ).waitFor();

      const { data: rotationObserved } = await heartbeat(primary);
      assert.equal(rotationObserved.rotation_requested, true);
      assert.equal(rotationObserved.pending_credential_accepted, false);
      const rotatedCredential = `ahrc_${randomUUID().replaceAll('-', '')}${randomUUID().replaceAll('-', '')}`;
      const rotatedHash = createHash('sha256').update(rotatedCredential).digest('hex');
      const { data: rotationStaged } = await heartbeat(primary, {
        pending_credential_hash: rotatedHash
      });
      assert.equal(rotationStaged.pending_credential_accepted, true);
      assert.equal(rotationStaged.credential_activated, false);
      const oldCredential = primary.credential;
      primary.credential = rotatedCredential;
      const { data: rotationActivated } = await heartbeat(primary);
      assert.equal(rotationActivated.rotation_requested, false);
      assert.equal(rotationActivated.credential_activated, true);
      const oldCredentialResponse = await primary.client.post('/api/runtime/heartbeat', {}, {
        headers: runtimeHeaders(oldCredential),
        expectedStatus: 401
      });
      assert.equal(oldCredentialResponse.status, 401);
      await poll(async () => {
        const runtime = (await admin.get('/api/runtimes')).data.find((item) => item.id === primary.id);
        return runtime ? runtime.credential_rotation_requested_at : 'missing';
      }, (requestedAt) => requestedAt === null, {
        timeoutMs: 10_000,
        description: 'Runtime credential activation to clear the pending state'
      });
      await detail.getByText(
        'Credential rotation is waiting for the Runtime to complete its handoff.',
        { exact: true }
      ).waitFor({ state: 'detached', timeout: 10_000 });
      assert.equal(await detail.getByRole('button', { name: 'Rotate credential' }).isEnabled(), true);

      owned = await createOwnedSession(admin, scenarioContext, primary, primaryAgent);
      const drainPreviewPromise = page.waitForResponse((response) => (
        response.request().method() === 'GET'
        && new URL(response.url()).pathname
          === `/api/admin/runtimes/${primary.id}/deletion-impact`
      ));
      await detail.getByRole('button', { name: 'Drain runtime' }).click();
      assert.equal((await drainPreviewPromise).ok(), true, 'Drain impact preview must succeed');
      const drainDialog = page.getByRole('dialog', { name: 'Drain runtime' });
      await drainDialog.waitFor();
      const affected = drainDialog.getByRole('listitem').filter({ hasText: owned.session.id });
      await affected.waitFor();
      const affectedText = await affected.innerText();
      assert.ok(affectedText.includes(primaryAgent.name), 'Drain impact must show the Agent name');
      assert.ok(affectedText.includes('online'), 'Drain impact must show current lifecycle state');
      await assertDialogFitsViewport(page, drainDialog, 'Drain preview desktop');

      const hostnameConfirmation = drainDialog.getByLabel('Confirm Runtime hostname');
      const drainSubmit = drainDialog.getByRole('button', { name: 'Drain runtime' });
      assert.equal(await drainSubmit.isDisabled(), true, 'Empty confirmation must not submit');
      await hostnameConfirmation.fill(`${primary.hostname}-wrong`);
      assert.equal(await drainSubmit.isDisabled(), true, 'Wrong hostname must not submit');
      await hostnameConfirmation.fill(primary.hostname.toUpperCase());
      assert.equal(await drainSubmit.isDisabled(), true, 'Hostname confirmation must be case-sensitive');
      await hostnameConfirmation.fill(`${primary.hostname} `);
      assert.equal(await drainSubmit.isDisabled(), true, 'Trailing whitespace must not be accepted');
      await hostnameConfirmation.fill(primary.hostname);
      assert.equal(await drainSubmit.isEnabled(), true, 'Exact hostname must enable drain');
      await drainDialog.getByRole('button', { name: 'Cancel' }).click();
      await drainDialog.waitFor({ state: 'detached' });
      assert.equal((await admin.get('/api/runtimes')).data.find((item) => item.id === primary.id).status, 'online');

      const secondDrainPreviewPromise = page.waitForResponse((response) => (
        response.request().method() === 'GET'
        && new URL(response.url()).pathname
          === `/api/admin/runtimes/${primary.id}/deletion-impact`
      ));
      await detail.getByRole('button', { name: 'Drain runtime' }).click();
      await secondDrainPreviewPromise;
      await drainDialog.getByLabel('Confirm Runtime hostname').fill(primary.hostname);
      const drainResponsePromise = page.waitForResponse((response) => (
        response.request().method() === 'POST'
        && new URL(response.url()).pathname === `/api/admin/runtimes/${primary.id}/drain`
      ));
      await drainDialog.getByRole('button', { name: 'Drain runtime' }).click();
      const drainResponse = await drainResponsePromise;
      assert.equal(drainResponse.ok(), true, `Drain returned ${drainResponse.status()}`);
      await page.getByText('Runtime drain started.', { exact: true }).waitFor();
      await detail.locator('.runtime-detail-header .status').getByText('draining', { exact: true }).waitFor();
      const affectedLink = detail.locator('.affected-sessions a[href="/sessions"]')
        .filter({ hasText: primaryAgent.name });
      await affectedLink.waitFor();
      assert.equal(await affectedLink.getAttribute('href'), '/sessions');

      const cancelDrainResponsePromise = page.waitForResponse((response) => (
        response.request().method() === 'POST'
        && new URL(response.url()).pathname
          === `/api/admin/runtimes/${primary.id}/cancel-drain`
      ));
      await detail.getByRole('button', { name: 'Cancel drain' }).click();
      const cancelDrainResponse = await cancelDrainResponsePromise;
      assert.equal(cancelDrainResponse.ok(), true, `Cancel drain returned ${cancelDrainResponse.status()}`);
      await page.getByText('Runtime drain cancelled.', { exact: true }).waitFor();
      await detail.locator('.runtime-detail-header .status').getByText('online', { exact: true }).waitFor();

      ordinary = await createRuntimeThroughApi(
        admin,
        scenarioContext,
        template,
        'qa-runtime-browser-ordinary',
        'ordinary-delete'
      );
      runtimes.push(ordinary);
      const ordinaryAgent = await createBoundAgent(
        admin,
        scenarioContext,
        ordinary.id,
        'QA Runtime Browser Empty Agent',
        agentIds
      );
      const { data: ordinaryHeartbeat } = await heartbeat(ordinary, {
        accepts_session_commands: true
      });
      assert.deepEqual(ordinaryHeartbeat.owned_sessions, []);
      const ordinaryRow = await waitForRuntimeRow(page, ordinary.hostname);
      await ordinaryRow.click();
      await detail.getByRole('heading', { name: ordinary.hostname }).waitFor();
      await detail.getByRole('link', { name: ordinaryAgent.name }).waitFor({ timeout: 10_000 });
      assert.equal(
        await detail.getByRole('button', { name: 'Delete runtime', exact: true }).count(),
        0,
        'Ordinary delete must not be exposed before drain'
      );
      const onlineDelete = await admin.request(`/api/admin/runtimes/${ordinary.id}`, {
        method: 'DELETE',
        body: { hostname: ordinary.hostname },
        expectedStatus: 409
      });
      assert.equal(onlineDelete.status, 409);

      const ordinaryDrainPreviewPromise = page.waitForResponse((response) => (
        response.request().method() === 'GET'
        && new URL(response.url()).pathname
          === `/api/admin/runtimes/${ordinary.id}/deletion-impact`
      ));
      await detail.getByRole('button', { name: 'Drain runtime' }).click();
      await ordinaryDrainPreviewPromise;
      const ordinaryDrainDialog = page.getByRole('dialog', { name: 'Drain runtime' });
      await ordinaryDrainDialog.getByText(
        'This Runtime no longer owns any Sessions.',
        { exact: true }
      ).waitFor();
      await ordinaryDrainDialog.getByLabel('Confirm Runtime hostname').fill(ordinary.hostname);
      const ordinaryDrainResponsePromise = page.waitForResponse((response) => (
        response.request().method() === 'POST'
        && new URL(response.url()).pathname === `/api/admin/runtimes/${ordinary.id}/drain`
      ));
      await ordinaryDrainDialog.getByRole('button', { name: 'Drain runtime' }).click();
      assert.equal((await ordinaryDrainResponsePromise).ok(), true, 'Empty Runtime drain must succeed');
      await detail.locator('.runtime-detail-header .status').getByText('draining', { exact: true }).waitFor();
      await detail.getByRole('button', { name: 'Delete runtime', exact: true }).waitFor();

      await page.setViewportSize({ width: 390, height: 844 });
      await assertNoHorizontalOverflow(page, 'Runtime administration 390px', [
        '.runtime-workspace',
        '.runtime-detail'
      ]);
      const deletePreviewPromise = page.waitForResponse((response) => (
        response.request().method() === 'GET'
        && new URL(response.url()).pathname
          === `/api/admin/runtimes/${ordinary.id}/deletion-impact`
      ));
      await detail.getByRole('button', { name: 'Delete runtime', exact: true }).click();
      assert.equal((await deletePreviewPromise).ok(), true, 'Delete impact preview must succeed');
      const deleteDialog = page.getByRole('dialog', { name: 'Delete runtime' });
      await deleteDialog.getByText('This Runtime no longer owns any Sessions.', { exact: true }).waitFor();
      await assertDialogFitsViewport(page, deleteDialog, 'Delete preview 390px');
      await deleteDialog.getByLabel('Confirm Runtime hostname').fill(ordinary.hostname);
      const deleteResponsePromise = page.waitForResponse((response) => (
        response.request().method() === 'DELETE'
        && new URL(response.url()).pathname === `/api/admin/runtimes/${ordinary.id}`
      ));
      await deleteDialog.getByRole('button', { name: 'Delete runtime', exact: true }).click();
      const deleteResponse = await deleteResponsePromise;
      assert.equal(deleteResponse.status(), 204, 'Ordinary delete must return 204');
      allowedNoContentAborts.add(
        `requestfailed: DELETE ${deleteResponse.url()}: net::ERR_ABORTED`
      );
      ordinary.deleted = true;
      await page.getByText('Runtime deleted.', { exact: true }).waitFor();
      assert.equal(await page.getByRole('button', { name: new RegExp(ordinary.hostname) }).count(), 0);

      const primaryRowForForce = await waitForRuntimeRow(page, primary.hostname);
      await primaryRowForForce.click();
      await detail.getByRole('heading', { name: primary.hostname }).waitFor();
      const forcePreviewPromise = page.waitForResponse((response) => (
        response.request().method() === 'GET'
        && new URL(response.url()).pathname
          === `/api/admin/runtimes/${primary.id}/deletion-impact`
      ));
      await detail.getByRole('button', { name: 'Force-delete runtime', exact: true }).click();
      assert.equal((await forcePreviewPromise).ok(), true, 'Force-delete impact preview must succeed');
      const forceDialog = page.getByRole('dialog', { name: 'Force-delete runtime' });
      const forceImpact = forceDialog.getByRole('listitem').filter({ hasText: owned.session.id });
      await forceImpact.waitFor();
      const forceImpactText = await forceImpact.innerText();
      assert.ok(forceImpactText.includes(primaryAgent.name), 'Force-delete impact must show Agent name');
      assert.ok(forceImpactText.includes('Recovery-failed Sessions'), 'Impact must expose recovery-failed disposition');
      await assertDialogFitsViewport(page, forceDialog, 'Force-delete preview 390px');
      await forceDialog.getByLabel('Confirm Runtime hostname').fill(primary.hostname);
      const forceDeleteResponsePromise = page.waitForResponse((response) => (
        response.request().method() === 'POST'
        && new URL(response.url()).pathname
          === `/api/admin/runtimes/${primary.id}/force-delete`
      ));
      await forceDialog.getByRole('button', { name: 'Force-delete runtime', exact: true }).click();
      const forceDeleteResponse = await forceDeleteResponsePromise;
      assert.equal(forceDeleteResponse.ok(), true, `Force-delete returned ${forceDeleteResponse.status()}`);
      const forceDeleteResult = await forceDeleteResponse.json();
      assert.equal(forceDeleteResult.runtime_id, primary.id);
      assert.deepEqual(forceDeleteResult.recoverable_session_ids, []);
      assert.deepEqual(forceDeleteResult.recovery_failed_session_ids, [owned.session.id]);
      primary.deleted = true;
      const resultNotice = page.locator('.force-result');
      await resultNotice.getByText(owned.session.id, { exact: false }).waitFor();
      await assertNoHorizontalOverflow(page, 'Force-delete result 390px', ['.runtime-workspace']);

      const { data: recoveryFailed } = await admin.get(`/api/sessions/${owned.session.id}`);
      assert.equal(recoveryFailed.lifecycle_status, 'recovery_failed');
      assert.equal(recoveryFailed.recovery_error, RECOVERY_FAILURE_REASON);
      assert.equal(recoveryFailed.runtime_owner_id, null);
      assert.equal(recoveryFailed.current_bundle, null);

      await page.setViewportSize({ width: 1280, height: 800 });
      await page.goto('/sessions', { waitUntil: 'domcontentloaded' });
      await page.locator('.session-workspace').waitFor();
      const sessionList = page.getByRole('complementary', { name: 'Session list' });
      allowedNoContentAborts.add(
        `requestfailed: GET ${new URL(`/api/sessions/${owned.session.id}/messages`, scenarioContext.baseURL).href}: net::ERR_ABORTED`
      );
      await sessionList.getByRole('combobox', { name: 'Agent' }).selectOption(primaryAgent.id);
      await sessionList.getByRole('textbox', { name: 'Search sessions' }).fill(owned.session.id);
      const sessionRow = sessionList.locator('.session-row').filter({ hasText: primaryAgent.name });
      await sessionRow.waitFor();
      await sessionRow.click();
      const sessionDetail = page.getByRole('region', { name: 'Session details' });
      const recoveryAlert = sessionDetail.getByRole('alert');
      await recoveryAlert.getByText('recovery failed', { exact: true }).waitFor();
      await recoveryAlert.getByText(RECOVERY_FAILURE_REASON, { exact: true }).waitFor();
      assert.equal(
        await sessionDetail.getByRole('textbox', { name: 'Message' }).count(),
        0,
        'Recovery-failed Session input must be read-only'
      );
      assert.equal(
        await sessionDetail.getByRole('button', { name: 'Send' }).count(),
        0,
        'Recovery-failed Session must not expose continuation submit'
      );
      const rejectedContinuation = await admin.post(`/api/sessions/${owned.session.id}/messages`, {
        content: scenarioContext.unique('QA rejected browser continuation'),
        client_message_key: scenarioContext.unique('qa-runtime-browser-rejected-message')
      }, { expectedStatus: 409 });
      assert.deepEqual(rejectedContinuation.data, { error: 'session is read-only' });
      assert.equal(sessionMessageMutations, 0, 'The read-only UI must not submit a Session message');
      await assertNoHorizontalOverflow(page, 'Recovery-failed Session desktop', [
        '.session-workspace',
        '.session-detail'
      ]);
      const unexpectedBrowserErrors = browserErrors.filter(
        (error) => !allowedNoContentAborts.has(error)
      );
      browserErrors.splice(0, browserErrors.length, ...unexpectedBrowserErrors);
      assert.deepEqual(browserErrors, [], 'Administrator browser diagnostics must remain empty');
    });

    await withBrowser(scenarioContext, {
      allowedHttpErrors: [
        { method: 'GET', pathname: '/api/auth/me', status: 401, times: 1 }
      ]
    }, async ({ page, browserErrors }) => {
      await loginBrowser(page, member.email, member.password);
      await openRuntimes(page);
      const firstRuntimeRow = page.locator('.runtime-row').first();
      await firstRuntimeRow.waitFor();
      const memberDetail = page.getByRole('region', { name: 'Runtime details' });
      await memberDetail.locator('.runtime-detail-header').waitFor();
      assert.equal(await page.getByRole('button', { name: 'Add runtime node' }).count(), 0);
      assert.equal(await page.getByRole('region', { name: 'Enrollment history' }).count(), 0);
      assert.equal(
        await memberDetail.getByRole('heading', { name: 'Runtime administration' }).count(),
        0
      );
      for (const action of [
        'Rotate credential',
        'Drain runtime',
        'Cancel drain',
        'Delete runtime',
        'Force-delete runtime'
      ]) {
        assert.equal(
          await memberDetail.getByRole('button', { name: action, exact: true }).count(),
          0,
          `Member must not receive the ${action} control`
        );
      }
      await assertNoHorizontalOverflow(page, 'Member Runtime desktop', [
        '.runtime-workspace',
        '.runtime-layout',
        '.runtime-detail'
      ]);
      assert.deepEqual(browserErrors, [], 'Member browser diagnostics must remain empty');
    });
  } catch (error) {
    scenarioError = error;
  } finally {
    for (const runtime of [...runtimes].reverse()) {
      if (runtime.deleted) continue;
      try {
        const { data: currentRuntimes } = await admin.get('/api/runtimes');
        if (currentRuntimes.some((item) => item.id === runtime.id)) {
          await admin.post(`/api/admin/runtimes/${runtime.id}/force-delete`, {
            hostname: runtime.hostname
          }, { expectedStatus: [200, 404] });
        }
        runtime.deleted = true;
      } catch (error) {
        cleanupErrors.push(error);
      }
    }

    for (const agentId of [...agentIds].reverse()) {
      try {
        await admin.delete(`/api/agents/${agentId}`, { expectedStatus: [204, 404] });
      } catch (error) {
        cleanupErrors.push(error);
      }
    }

    try {
      await cleanupMember(admin, member);
    } catch (error) {
      cleanupErrors.push(error);
    }

    try {
      const { data: currentEnrollments } = await admin.get('/api/admin/runtime-enrollment-tokens');
      for (const enrollment of currentEnrollments) {
        if (baselineEnrollmentIds.has(enrollment.id)
          || enrollment.consumed_at
          || enrollment.revoked_at
          || new Date(enrollment.expires_at).getTime() <= Date.now()) continue;
        await admin.post(
          `/api/admin/runtime-enrollment-tokens/${enrollment.id}/revoke`,
          undefined,
          { expectedStatus: [200, 404, 409] }
        );
      }
    } catch (error) {
      cleanupErrors.push(error);
    }

    try {
      const { data: finalRuntimes } = await admin.get('/api/runtimes');
      for (const baselineRuntimeId of baselineRuntimeIds) {
        assert.ok(
          finalRuntimes.some((runtime) => runtime.id === baselineRuntimeId),
          'Compose-provided Runtime must remain registered after cleanup'
        );
      }
    } catch (error) {
      cleanupErrors.push(error);
    }
  }

  if (scenarioError) {
    if (cleanupErrors.length > 0) {
      scenarioError.message = `${scenarioError.message}; cleanup failed: ${cleanupErrors
        .map((error) => error.message)
        .join('; ')}`;
    }
    throw scenarioError;
  }
  if (cleanupErrors.length > 0) {
    throw new AggregateError(cleanupErrors, 'Runtime browser cleanup failed');
  }
}
