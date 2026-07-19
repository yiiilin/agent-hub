import { execFileSync, spawn } from 'node:child_process';
import { dirname } from 'node:path';
import { expect, request, test, type APIRequestContext, type Page, type Route } from '@playwright/test';

function composeArgs() {
  const project = process.env.E2E_COMPOSE_PROJECT?.trim() || 'agent-hub-dev';
  return ['compose', '-p', project, '-f', '../compose.dev.yml'] as const;
}

function completeWithin<T>(promise: Promise<T>, timeoutMs: number, timeoutMessage: string): Promise<T> {
  return new Promise((resolve, reject) => {
    // 并发验收必须有独立上限，避免锁竞争耗尽整条 E2E 用例的全局超时。
    const timer = setTimeout(() => reject(new Error(timeoutMessage)), timeoutMs);
    promise.then(
      (value) => {
        clearTimeout(timer);
        resolve(value);
      },
      (error) => {
        clearTimeout(timer);
        reject(error);
      }
    );
  });
}

async function expectPromisePending<T>(promise: Promise<T>, waitMs: number, message: string) {
  const outcome = await Promise.race([
    promise.then(() => 'completed', () => 'completed'),
    new Promise<'pending'>((resolve) => setTimeout(() => resolve('pending'), waitMs))
  ]);
  expect(outcome, message).toBe('pending');
}

async function createAgentThroughUi(page: Page, name: string, instructions: string) {
  await page.locator('.agents-header').getByRole('button', { name: 'Create Agent' }).click();
  const dialog = page.getByRole('dialog', { name: 'Create Agent' });
  await dialog.getByLabel('Name', { exact: true }).fill(name);
  await dialog.getByLabel('Instructions').fill(instructions);
  const responsePromise = page.waitForResponse((response) => response.request().method() === 'POST'
    && new URL(response.url()).pathname === '/api/agents');
  await dialog.getByRole('button', { name: 'Create agent' }).click();
  return responsePromise;
}

test('priority helper only accepts a prioritized or runtime-owned target run', () => {
  const runId = '10000000-0000-4000-8000-000000000001';
  expect(() => validatePrioritizeResult(runId, `${runId}|pending||f|t`)).not.toThrow();
  expect(() => validatePrioritizeResult(runId, `${runId}|running|20000000-0000-4000-8000-000000000001|t|f`)).not.toThrow();
  expect(() => validatePrioritizeResult(runId, `${runId}|completed|20000000-0000-4000-8000-000000000001|t|f`)).not.toThrow();
  expect(() => validatePrioritizeResult(runId, '')).toThrow(/target run was not found/);
  expect(() => validatePrioritizeResult(runId, `${runId}|running||f|f`)).toThrow(/not owned by a known runtime/);
  expect(() => validatePrioritizeResult(runId, `30000000-0000-4000-8000-000000000001|running|20000000-0000-4000-8000-000000000001|t|f`)).toThrow(/unexpected target/);
});

test('widget serializes rapid submissions and exposes pending UI', async ({ page }) => {
  const runId = '70000000-0000-0000-0000-000000000001';
  let runRequests = 0;
  const heldRoute: { current?: Route } = {};
  await page.route('**/api/**', async (route) => {
    const request = route.request();
    const path = new URL(request.url()).pathname;
    if (path === '/api/widget/session') return route.fulfill({ json: { agent_id: 'agent-id', name: 'Pending Widget Agent' } });
    if (path === '/api/widget/runs' && request.method() === 'POST') {
      runRequests += 1;
      heldRoute.current = route;
      return;
    }
    if (path === `/api/runs/${runId}/events/stream`) return route.fulfill({ contentType: 'text/event-stream', body: '' });
    return route.fulfill({ status: 404, json: { error: `Unhandled test route: ${path}` } });
  });
  await page.goto('/widget#token=widget-token');
  await expect(page.getByText('Pending Widget Agent')).toBeVisible();
  await page.locator('textarea').fill('Submit once');
  await page.locator('form').evaluate((form) => {
    (form as HTMLFormElement).requestSubmit();
    (form as HTMLFormElement).requestSubmit();
  });
  await expect.poll(() => runRequests).toBe(1);
  await expect(page.getByRole('button', { name: 'Sending...' })).toBeDisabled();
  if (!heldRoute.current) throw new Error('Widget run route was not captured');
  await heldRoute.current.fulfill({ json: {
    id: runId, agent_id: 'agent-id', automation_id: null, integration_session_id: null,
    parent_run_id: null, runtime_id: null, status: 'pending', initial_message: 'Submit once',
    session_id: null, work_dir_ref: null, source: 'widget', created_at: new Date().toISOString(), updated_at: new Date().toISOString()
  } });
  await expect(page.getByRole('button', { name: 'Send' })).toBeEnabled();
  expect(runRequests).toBe(1);
});

test('widget stream ignores malformed SSE JSON and keeps later events', async ({ page }) => {
  const runId = '70000000-0000-0000-0000-000000000002';
  const pageErrors: string[] = [];
  const consoleErrors: string[] = [];
  page.on('pageerror', (error) => pageErrors.push(error.message));
  page.on('console', (message) => { if (message.type() === 'error') consoleErrors.push(message.text()); });
  await page.route('**/api/**', async (route) => {
    const request = route.request();
    const path = new URL(request.url()).pathname;
    if (path === '/api/widget/session') return route.fulfill({ json: { agent_id: 'agent-id', name: 'Malformed Widget Agent' } });
    if (path === '/api/widget/runs' && request.method() === 'POST') return route.fulfill({ json: {
      id: runId, agent_id: 'agent-id', automation_id: null, integration_session_id: null,
      parent_run_id: null, runtime_id: null, status: 'running', initial_message: 'Malformed stream',
      session_id: null, work_dir_ref: null, source: 'widget', created_at: new Date().toISOString(), updated_at: new Date().toISOString()
    } });
    if (path === `/api/runs/${runId}/events/stream`) return route.fulfill({
      contentType: 'text/event-stream',
      body: `event: run_event\ndata: {malformed\n\nevent: run_event\ndata: ${JSON.stringify({ seq: 2, run_id: runId, event_type: 'message', role: 'assistant', content: 'Valid widget event', payload: {}, created_at: new Date().toISOString() })}\n\n`
    });
    return route.fulfill({ status: 404, json: { error: `Unhandled test route: ${path}` } });
  });
  await page.goto('/widget#token=widget-token');
  await expect(page.getByText('Malformed Widget Agent')).toBeVisible();
  await page.getByRole('button', { name: 'Send' }).click();
  await expect(page.getByText('assistant: Valid widget event')).toBeVisible();
  expect(pageErrors).toEqual([]);
  expect(consoleErrors).toEqual([]);
});

function modelProxyConfigProbe(workDirRef: string) {
  const runRoot = dirname(workDirRef);
  const script = `
set -eu
config="${runRoot}/codex/config.toml"
test -f "$config"
provider_section=no
responses_wire=no
loopback_base_url=no
zero_port=no
grep -F '[model_providers.agent_hub_' "$config" >/dev/null && provider_section=yes
grep -F 'wire_api = "responses"' "$config" >/dev/null && responses_wire=yes
grep -E 'base_url = "http://127\\.0\\.0\\.1:[0-9]+/v1"' "$config" >/dev/null && loopback_base_url=yes
grep -F 'base_url = "http://127.0.0.1:0/v1"' "$config" >/dev/null && zero_port=yes
printf '{"providerSection":"%s","responsesWire":"%s","loopbackBaseUrl":"%s","zeroPort":"%s"}' "$provider_section" "$responses_wire" "$loopback_base_url" "$zero_port"
`;
  return JSON.parse(execFileSync('docker', [
    ...composeArgs(),
    'exec',
    '-T',
    'runtime',
    'sh',
    '-lc',
    script
  ], { cwd: process.cwd(), encoding: 'utf8' }));
}

function archivedAgentState(agentId: string) {
  const sql = `
SELECT json_build_object(
  'enabledAutomations', COUNT(DISTINCT automation.id) FILTER (WHERE automation.enabled),
  'activeRuns', COUNT(DISTINCT run.id) FILTER (WHERE run.status IN ('pending', 'running')),
  'postArchiveRuns', COUNT(DISTINCT run.id) FILTER (WHERE run.created_at > agent.deleted_at),
  'archivedAt', to_char(agent.deleted_at AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS.US"Z"'),
  'schedulerRuns', COALESCE(json_agg(json_build_object(
    'createdAt', to_char(run.created_at AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS.US"Z"'),
    'message', run.initial_message,
    'status', run.status
  ) ORDER BY run.created_at) FILTER (WHERE run.source = 'automation:scheduler'), '[]'::json)
)
FROM agents AS agent
LEFT JOIN automations AS automation ON automation.agent_id = agent.id
LEFT JOIN runs AS run ON run.agent_id = agent.id
WHERE agent.id = '${agentId}'
GROUP BY agent.deleted_at;
`;
  const output = execFileSync('docker', [
    ...composeArgs(), 'exec', '-T', 'postgres',
    'psql', '-U', 'agent_hub', '-d', 'agent_hub', '-Atc', sql
  ], { cwd: process.cwd(), encoding: 'utf8' }).trim();
  return JSON.parse(output) as {
    activeRuns: number;
    archivedAt: string | null;
    enabledAutomations: number;
    postArchiveRuns: number;
    schedulerRuns: Array<{
      createdAt: string;
      message: string;
      status: string;
    }>;
  };
}

function storedRunState(runId: string) {
  const output = execFileSync('docker', [
    ...composeArgs(), 'exec', '-T', 'postgres',
    'psql', '-U', 'agent_hub', '-d', 'agent_hub', '-Atc',
    `SELECT json_build_object('status', status, 'runtimeId', runtime_id) FROM runs WHERE id = '${runId}';`
  ], { cwd: process.cwd(), encoding: 'utf8' }).trim();
  return JSON.parse(output) as { status: string; runtimeId: string | null };
}

async function runtimeEnrollmentToken(api: APIRequestContext) {
  const login = await api.post('/api/auth/login', {
    data: { email: 'admin@example.com', password: 'admin123' }
  });
  expect(login.ok()).toBeTruthy();
  const response = await api.post('/api/admin/runtime-enrollment-tokens');
  expect(response.ok()).toBeTruthy();
  return (await response.json() as { token: string }).token;
}

function validatePrioritizeResult(runId: string, output: string) {
  if (!output) throw new Error(`Priority target run was not found: ${runId}`);
  const [actualRunId, status, runtimeId, runtimeKnown, prioritized] = output.split('|');
  if (actualRunId !== runId) {
    throw new Error(`Priority helper returned unexpected target ${actualRunId} for ${runId}`);
  }
  if (prioritized === 't' && status === 'pending') return;
  const runtimeOwnedStatuses = new Set(['running', 'completed', 'failed', 'waiting_tool']);
  if (runtimeId && runtimeKnown === 't' && runtimeOwnedStatuses.has(status)) return;
  throw new Error(
    `Priority target ${runId} was not prioritized and is not owned by a known runtime: status=${status}, runtime_id=${runtimeId || 'null'}`
  );
}

function prioritizePendingRunForRuntimeClaim(runId: string) {
  const sql = `
WITH prioritized AS (
  UPDATE runs
  SET created_at = (
    SELECT COALESCE(MIN(created_at), now()) - interval '1 millisecond'
    FROM runs
    WHERE status = 'pending' AND id <> '${runId}'
  )
  WHERE id = '${runId}' AND status = 'pending'
  RETURNING id
)
SELECT r.id::text,
       r.status,
       COALESCE(r.runtime_id::text, ''),
       EXISTS (SELECT 1 FROM runtimes runtime WHERE runtime.id = r.runtime_id),
       EXISTS (SELECT 1 FROM prioritized)
FROM runs r
WHERE r.id = '${runId}';
`;
  const output = execFileSync('docker', [
    ...composeArgs(), 'exec', '-T', 'postgres',
    'psql', '-U', 'agent_hub', '-d', 'agent_hub', '-A', '-t', '-F', '|', '-c', sql
  ], { cwd: process.cwd(), encoding: 'utf8' }).trim();
  validatePrioritizeResult(runId, output);
}

function deleteTemporaryRuntime(runtimeId: string) {
  const sql = `DELETE FROM runtimes WHERE id = '${runtimeId}' RETURNING id;`;
  const output = execFileSync('docker', [
    ...composeArgs(), 'exec', '-T', 'postgres',
    'psql', '-U', 'agent_hub', '-d', 'agent_hub', '-Atc', sql
  ], { cwd: process.cwd(), encoding: 'utf8' }).trim();
  if (!output.split('\n').includes(runtimeId)) throw new Error(`Temporary runtime cleanup failed: ${output}`);
}

function runtimeTokenHash(runtimeId: string) {
  return execFileSync('docker', [
    ...composeArgs(), 'exec', '-T', 'postgres',
    'psql', '-U', 'agent_hub', '-d', 'agent_hub', '-Atc',
    `SELECT token_hash FROM runtimes WHERE id = '${runtimeId}';`
  ], { cwd: process.cwd(), encoding: 'utf8' }).trim();
}

function runtimeHealthStatus() {
  return execFileSync('docker', [
    ...composeArgs(), 'exec', '-T', 'runtime',
    'sh', '-lc', 'curl -sS -o /dev/null -w "%{http_code}" http://localhost:8081/healthz || true'
  ], { cwd: process.cwd(), encoding: 'utf8' }).trim();
}

function holdPostgresLock(applicationName: string, statements: string[], seconds: number) {
  const child = spawn('docker', [
    ...composeArgs(), 'exec', '-T', 'postgres',
    'psql', '-U', 'agent_hub', '-d', 'agent_hub', '-v', 'ON_ERROR_STOP=1',
    '-c', 'BEGIN',
    '-c', `SET application_name = '${applicationName}'`,
    ...statements.flatMap((statement) => ['-c', statement]),
    '-c', `SELECT pg_sleep(${seconds})`,
    '-c', 'COMMIT'
  ], { cwd: process.cwd() });
  let errorOutput = '';
  let output = '';
  const done = new Promise<void>((resolve, reject) => {
    child.stdout.on('data', (chunk) => {
      output += chunk.toString();
    });
    child.stderr.on('data', (chunk) => { errorOutput += chunk.toString(); });
    child.on('error', reject);
    child.on('close', (code) => {
      if (code === 0) {
        resolve();
        return;
      }
      const error = new Error(`PostgreSQL lock process failed (${code}): ${errorOutput || output}`);
      reject(error);
    });
  });
  // Attach a handler immediately because cleanup may not await `done` until after a failed assertion.
  void done.catch(() => {});
  const exited = new Promise<void>((resolve) => {
    child.on('close', () => resolve());
  });
  return { applicationName, child, done, exited };
}

function holdAgentRowLock(agentId: string, seconds: number) {
  return holdPostgresLock(
    `batch1-agent-lock-${agentId}`,
    [`SELECT id FROM agents WHERE id = '${agentId}' FOR UPDATE`],
    seconds
  );
}

function holdOtherPendingRunLocks(runId: string, seconds: number) {
  const lockSql = `
WITH locked AS (
  SELECT id FROM runs
  WHERE status = 'pending' AND id <> '${runId}'
  FOR UPDATE SKIP LOCKED
)
SELECT count(*) FROM locked;
`;
  return holdPostgresLock(`batch1-pending-lock-${runId}`, [lockSql], seconds);
}

function holdAutomationRowLock(automationId: string, seconds: number) {
  return holdPostgresLock(
    `batch1-automation-lock-${automationId}`,
    [`SELECT id FROM automations WHERE id = '${automationId}' FOR UPDATE`],
    seconds
  );
}

function terminatePostgresLockBackend(applicationName: string) {
  const sql = `
SELECT pg_terminate_backend(pid)
FROM pg_stat_activity
WHERE application_name = '${applicationName}';
`;
  execFileSync('docker', [
    ...composeArgs(), 'exec', '-T', 'postgres',
    'psql', '-U', 'agent_hub', '-d', 'agent_hub', '-v', 'ON_ERROR_STOP=1', '-c', sql
  ], { cwd: process.cwd(), encoding: 'utf8' });
}

function postgresLockConnectionCount(applicationName: string) {
  const sql = `
SELECT COUNT(*)
FROM pg_stat_activity
WHERE application_name = '${applicationName}';
`;
  const output = execFileSync('docker', [
    ...composeArgs(), 'exec', '-T', 'postgres',
    'psql', '-U', 'agent_hub', '-d', 'agent_hub', '-Atc', sql
  ], { cwd: process.cwd(), encoding: 'utf8' }).trim();
  return Number(output);
}

async function stopPostgresLock(lock: ReturnType<typeof holdPostgresLock>) {
  const terminating = lock.child.exitCode === null;
  const cleanupErrors: unknown[] = [];
  if (terminating) {
    try {
      lock.child.kill('SIGTERM');
    } catch (error) {
      cleanupErrors.push(error);
    }
    // docker exec 的父进程退出不保证容器内 psql 立即退出；终止其 backend 后再等待 child close。
    try {
      terminatePostgresLockBackend(lock.applicationName);
    } catch (error) {
      cleanupErrors.push(error);
    }
  }
  try {
    await completeWithin(lock.exited, 5_000, 'PostgreSQL lock process did not exit after termination');
  } catch (error) {
    cleanupErrors.push(error);
  }
  try {
    await expect.poll(() => postgresLockConnectionCount(lock.applicationName), { timeout: 5_000 }).toBe(0);
  } catch (error) {
    cleanupErrors.push(error);
  }
  try {
    await lock.done;
  } catch (error) {
    // 主动终止 psql 的非零退出是预期结果；自然退出失败仍必须报告。
    if (!terminating) cleanupErrors.push(error);
  }
  if (cleanupErrors.length > 0) {
    throw new AggregateError(cleanupErrors, `PostgreSQL lock cleanup failed: ${lock.applicationName}`);
  }
}

function enableDueAutomation(automationId: string) {
  const sql = `
UPDATE automations
SET enabled = true,
    last_triggered_at = now() - interval '2 seconds',
    updated_at = now()
WHERE id = '${automationId}';
`;
  execFileSync('docker', [
    ...composeArgs(), 'exec', '-T', 'postgres',
    'psql', '-U', 'agent_hub', '-d', 'agent_hub', '-v', 'ON_ERROR_STOP=1', '-c', sql
  ], { cwd: process.cwd(), encoding: 'utf8' });
}

function agentRowLockWaiterCounts(blockingBackendPid: number) {
  const sql = `
WITH RECURSIVE lock_waiters AS (
  SELECT activity.pid, activity.query
  FROM pg_stat_activity AS activity
  WHERE ${blockingBackendPid} = ANY(pg_blocking_pids(activity.pid))
    AND activity.wait_event_type = 'Lock'
  UNION
  SELECT activity.pid, activity.query
  FROM pg_stat_activity AS activity
  JOIN lock_waiters AS blocker ON blocker.pid = ANY(pg_blocking_pids(activity.pid))
  WHERE activity.wait_event_type = 'Lock'
)
SELECT json_build_object(
  'archive', COUNT(*) FILTER (
    WHERE (query LIKE '%SELECT owner_id, deleted_at%'
        OR query LIKE '%SELECT agents.owner_id, agents.deleted_at%')
      AND query LIKE '%FOR UPDATE%'
  ),
  'automationCreate', COUNT(*) FILTER (
    WHERE query LIKE '%SELECT id, owner_id FROM agents%'
      AND query LIKE '%deleted_at IS NULL%'
      AND query LIKE '%FOR UPDATE%'
  ),
  'manualTrigger', COUNT(*) FILTER (
    WHERE query LIKE '%SELECT id%FROM agents%'
      AND query LIKE '%owner_id = $2%'
      AND query NOT LIKE '%SELECT id, owner_id FROM agents%'
      AND query LIKE '%FOR UPDATE%'
  ),
  'scheduler', COUNT(*) FILTER (
    WHERE query LIKE '%SELECT id FROM agents WHERE id = $1 AND deleted_at IS NULL FOR UPDATE%'
  )
)
FROM lock_waiters;
`;
  const output = execFileSync('docker', [
    ...composeArgs(), 'exec', '-T', 'postgres',
    'psql', '-U', 'agent_hub', '-d', 'agent_hub', '-Atc', sql
  ], { cwd: process.cwd(), encoding: 'utf8' }).trim();
  return JSON.parse(output) as {
    archive: number;
    automationCreate: number;
    manualTrigger: number;
    scheduler: number;
  };
}

function archiveAutomationLockWaiterCount(blockingBackendPid: number) {
  const sql = `
SELECT COUNT(*)
FROM pg_stat_activity
WHERE ${blockingBackendPid} = ANY(pg_blocking_pids(pid))
  AND wait_event_type = 'Lock'
  AND query LIKE '%DELETE FROM automations%'
  AND query LIKE '%WHERE agent_id = $1%';
`;
  const output = execFileSync('docker', [
    ...composeArgs(), 'exec', '-T', 'postgres',
    'psql', '-U', 'agent_hub', '-d', 'agent_hub', '-Atc', sql
  ], { cwd: process.cwd(), encoding: 'utf8' }).trim();
  return Number(output);
}

function postgresLockBackendPid(applicationName: string) {
  const sql = `
SELECT pid
FROM pg_stat_activity
WHERE application_name = '${applicationName}'
  AND state = 'active'
  AND query LIKE 'SELECT pg_sleep(%'
LIMIT 1;
`;
  const output = execFileSync('docker', [
    ...composeArgs(), 'exec', '-T', 'postgres',
    'psql', '-U', 'agent_hub', '-d', 'agent_hub', '-Atc', sql
  ], { cwd: process.cwd(), encoding: 'utf8' }).trim();
  return output ? Number(output) : null;
}

async function waitForPostgresLock(applicationName: string, description: string) {
  let backendPid: number | null = null;
  await expect.poll(() => {
    backendPid = postgresLockBackendPid(applicationName);
    return backendPid;
  }, { timeout: 5_000 }).not.toBeNull();
  if (backendPid === null) throw new Error(`Failed to observe ${description}`);
  return backendPid;
}

async function deleteAgentForCleanup(api: APIRequestContext, agentId: string) {
  const response = await api.delete(`/api/agents/${agentId}`, { timeout: 5_000 });
  expect([204, 404]).toContain(response.status());
}

function agentRuntimeSessionIds(agentId: string) {
  const output = execFileSync('docker', [
    ...composeArgs(), 'exec', '-T', 'postgres',
    'psql', '-U', 'agent_hub', '-d', 'agent_hub', '-Atc',
    `SELECT id FROM hub_sessions WHERE agent_id = '${agentId}' ORDER BY id;`
  ], { cwd: process.cwd(), encoding: 'utf8' }).trim();
  return output ? output.split('\n') : [];
}

function runtimeSessionDirectoryCount(sessionIds: string[]) {
  if (sessionIds.length === 0) return 0;
  const script = `
count=0
for session_id in ${sessionIds.join(' ')}; do
  if [ -d "/var/lib/agent-hub-runtime/sessions/$session_id" ]; then count=$((count + 1)); fi
  for cleanup in /var/lib/agent-hub-runtime/session-cleanups/$session_id-*; do
    if [ -e "$cleanup" ]; then count=$((count + 1)); fi
  done
done
printf '%s' "$count"
`;
  return Number(execFileSync('docker', [
    ...composeArgs(), 'exec', '-T', 'runtime', 'sh', '-lc', script
  ], { cwd: process.cwd(), encoding: 'utf8' }).trim());
}

async function deleteAgentAndWaitForRuntimeSessionCleanup(api: APIRequestContext, agentId: string) {
  const sessionIds = agentRuntimeSessionIds(agentId);
  await deleteAgentForCleanup(api, agentId);
  await expect.poll(() => runtimeSessionDirectoryCount(sessionIds)).toBe(0);
}

async function disableAutomationByName(api: APIRequestContext, automationName: string) {
  const listResponse = await api.get('/api/automations');
  expect(listResponse.ok()).toBeTruthy();
  const automation = (await listResponse.json() as Array<{
    id: string;
    name: string;
    trigger_type: string;
    prompt: string;
    schedule: string | null;
  }>).find((item) => item.name === automationName);
  expect(automation).toBeTruthy();
  if (!automation) throw new Error(`Automation not found for disable: ${automationName}`);
  const response = await api.patch(`/api/automations/${automation.id}`, {
    data: {
      name: automation.name,
      trigger_type: automation.trigger_type,
      prompt: automation.prompt,
      schedule: automation.schedule,
      enabled: false
    }
  });
  expect(response.ok()).toBeTruthy();
  expect((await response.json() as { enabled: boolean }).enabled).toBe(false);
}

async function deleteSkillForCleanup(api: APIRequestContext, skillId: string) {
  const response = await api.delete(`/api/skills/${skillId}`, { timeout: 5_000 });
  expect([204, 404]).toContain(response.status());
}

async function cleanupResources(
  description: string,
  tasks: Array<{ name: string; run: () => Promise<void> | void }>
) {
  const errors: Error[] = [];
  for (const task of tasks) {
    try {
      await task.run();
    } catch (error) {
      errors.push(new Error(
        `${task.name}: ${error instanceof Error ? error.message : String(error)}`,
        { cause: error }
      ));
    }
  }
  if (errors.length > 0) throw new AggregateError(errors, description);
}

const consoleRunTestTitle = 'console, widget, and automations run through the app-server Codex driver';
let consoleRunCleanupIds: { agentIds: string[]; skillId: string | null } | null = null;

test.afterEach(async ({ request: cleanupApi }, testInfo) => {
  if (testInfo.title !== consoleRunTestTitle || !consoleRunCleanupIds) return;
  const { agentIds, skillId } = consoleRunCleanupIds;
  consoleRunCleanupIds = null;
  const login = await cleanupApi.post('/api/auth/login', {
    data: { email: 'admin@example.com', password: 'admin123' },
    timeout: 5_000
  });
  expect(login.ok()).toBeTruthy();
  await cleanupResources('Console/run E2E teardown cleanup failed', [
    ...agentIds.map((agentId, index) => ({
      name: `managed Agent ${index + 1}`,
      run: async () => {
        await deleteAgentAndWaitForRuntimeSessionCleanup(cleanupApi, agentId);
      }
    })),
    {
      name: 'managed Skill',
      run: async () => {
        if (skillId) await deleteSkillForCleanup(cleanupApi, skillId);
      }
    }
  ]);
});

async function completedScheduledRunId(
  page: import('@playwright/test').Page,
  agentId: string,
  message: string
): Promise<string> {
  let runId: string | null = null;
  let prioritized = false;
  await expect.poll(async () => {
    const response = await page.request.get(`/api/agents/${agentId}/runs`);
    if (!response.ok()) return null;
    const runs = await response.json() as Array<{
      id: string;
      initial_message: string;
      source: string;
      status: string;
    }>;
    const run = runId
      ? runs.find((candidate) => candidate.id === runId)
      : runs.find((candidate) => candidate.source === 'automation:scheduler'
        && candidate.initial_message === message);
    if (!run) return null;
    runId = run.id;
    if (run.status === 'pending' && !prioritized) {
      prioritizePendingRunForRuntimeClaim(run.id);
      prioritized = true;
    }
    return run.status === 'completed' ? run.id : null;
  }, { timeout: 30_000 }).not.toBeNull();
  if (!runId) throw new Error(`Scheduled run did not complete: ${message}`);
  return runId;
}

test('runtime page shows the effective sandbox and deterministic downgrade reason', async ({ page, baseURL }) => {
  let runtimeApi: APIRequestContext | null = null;
  let runtimeId: string | null = null;
  try {
    runtimeApi = await request.newContext({ baseURL });
    const nonce = Date.now();
    const hostname = `downgraded-runtime-${nonce}`;
    const downgradeReason = 'workspace mount is read-only for this browser regression';
    const registration = await runtimeApi.post('/api/runtime/register', {
      headers: { Authorization: `Bearer ${await runtimeEnrollmentToken(runtimeApi)}` },
      data: {
        hostname,
        labels: ['playwright', 'sandbox-downgrade'],
        codex_version: 'sandbox-evidence-e2e',
        capabilities: {
          model_proxy: true,
          sandbox_downgraded: true,
          sandbox_downgrade_reason: downgradeReason,
          sandbox: {
            configured_mode: 'workspace-write+network',
            effective_mode: 'read-only',
            downgraded: true,
            downgrade_reason: downgradeReason
          }
        },
        sandbox_mode: 'read-only'
      }
    });
    expect(registration.ok()).toBeTruthy();
    runtimeId = (await registration.json() as { runtime_id: string }).runtime_id;

    await page.goto('/login');
    await page.getByLabel('Email').fill('admin@example.com');
    await page.getByLabel('Password').fill('admin123');
    await page.getByRole('button', { name: 'Sign in', exact: true }).click();
    await expect(page.getByText('admin@example.com')).toBeVisible();
    await page.goto('/runtimes');

    const runtimeRow = page.locator('.runtime-row').filter({ hasText: hostname });
    await expect(runtimeRow).toBeVisible();
    await runtimeRow.click();
    const runtimeDetail = page.getByRole('region', { name: 'Runtime details' });
    await expect(runtimeDetail.getByText('read-only', { exact: true })).toBeVisible();
    await expect(runtimeDetail.getByText(`Sandbox downgraded: ${downgradeReason}`, { exact: true })).toBeVisible();
  } finally {
    if (runtimeId) deleteTemporaryRuntime(runtimeId);
    await runtimeApi?.dispose();
  }
});

test('runtime rotates its persisted credential without re-registering and resumes execution', async ({ page }) => {
  let agentId: string | null = null;
  await page.goto('/login');
  await page.getByLabel('Email').fill('admin@example.com');
  await page.getByLabel('Password').fill('admin123');
  await page.getByRole('button', { name: 'Sign in', exact: true }).click();
  await expect(page.getByText('admin@example.com')).toBeVisible();

  try {
    const runtimesResponse = await page.request.get('/api/runtimes');
    expect(runtimesResponse.ok()).toBeTruthy();
    const runtimes = await runtimesResponse.json() as Array<{
      capabilities: Record<string, unknown>;
      codex_version: string;
      hostname: string;
      id: string;
      labels: string[];
      sandbox_mode: string;
    }>;
    const runtime = runtimes.find((item) => item.hostname === 'compose-runtime-1');
    if (!runtime) throw new Error('compose runtime was not registered');

    const oldCredentialHash = runtimeTokenHash(runtime.id);
    const rotation = await page.request.post(`/api/admin/runtimes/${runtime.id}/credential-rotation`);
    expect(rotation.ok()).toBeTruthy();
    const pending = await rotation.json() as {
      id: string;
      credential_rotation_requested_at: string | null;
    };
    expect(pending.id).toBe(runtime.id);
    expect(pending.credential_rotation_requested_at).not.toBeNull();
    expect(runtimeHealthStatus()).toBe('200');
    await expect.poll(() => runtimeTokenHash(runtime.id), { timeout: 10_000 }).not.toBe(oldCredentialHash);
    await expect.poll(runtimeHealthStatus, { timeout: 10_000 }).toBe('200');

    const agentResponse = await page.request.post('/api/agents', {
      data: {
        name: `Runtime recovery ${Date.now()}`,
        instructions: 'Complete the run after runtime token recovery.',
        visibility: 'private'
      }
    });
    expect(agentResponse.ok()).toBeTruthy();
    agentId = (await agentResponse.json() as { id: string }).id;
    const runResponse = await page.request.post(`/api/agents/${agentId}/runs`, {
      data: { message: 'Verify recovered runtime execution.' }
    });
    expect(runResponse.ok()).toBeTruthy();
    const runId = (await runResponse.json() as { id: string }).id;
    prioritizePendingRunForRuntimeClaim(runId);
    await expect.poll(async () => {
      const runsResponse = await page.request.get(`/api/agents/${agentId}/runs`);
      if (!runsResponse.ok()) return null;
      const runs = await runsResponse.json() as Array<{ id: string; status: string }>;
      return runs.find((run) => run.id === runId)?.status ?? null;
    }, { timeout: 30_000 }).toBe('completed');
  } finally {
    if (agentId) await deleteAgentForCleanup(page.request, agentId);
  }
});

test(consoleRunTestTitle, async ({ page, context, baseURL }) => {
  test.setTimeout(120_000);
  let managedSkillId: string | null = null;
  consoleRunCleanupIds = { agentIds: [], skillId: null };
  let anonymous: APIRequestContext | null = null;
  let widget: import('@playwright/test').Page | null = null;
  try {
  await page.goto('/login');
  await page.getByLabel('Email').fill('admin@example.com');
  await page.getByLabel('Password').fill('admin123');
  await page.getByRole('button', { name: 'Sign in', exact: true }).click();
  await expect(page.getByText('admin@example.com')).toBeVisible();

  const skillName = `repo-review-${Date.now()}`;
  await page.goto('/skills');
  await page.locator('.skills-page .page-header').getByRole('button', { name: 'Create skill' }).click();
  const createSkillDialog = page.getByRole('dialog', { name: 'Create skill' });
  await createSkillDialog.getByLabel('Name', { exact: true }).fill(skillName);
  await createSkillDialog.getByLabel('Description').fill('Managed skill created from Playwright');
  await createSkillDialog.getByLabel('Content').fill('Managed skill content from Playwright');
  const createManagedSkillResponse = page.waitForResponse((response) => response.request().method() === 'POST'
    && new URL(response.url()).pathname === '/api/skills');
  await createSkillDialog.getByRole('button', { name: 'Create skill' }).click();
  const managedSkillResponse = await createManagedSkillResponse;
  expect(managedSkillResponse.ok()).toBeTruthy();
  managedSkillId = (await managedSkillResponse.json() as { id: string }).id;
  consoleRunCleanupIds.skillId = managedSkillId;
  await expect(page.getByText(skillName)).toBeVisible();

  await page.goto('/agents');
  await expect(page.getByText('Create Agent', { exact: true })).toBeVisible();
  const agentName = `Browser Agent ${Date.now()}`;
  const managedAgentResponse = await createAgentThroughUi(
    page,
    agentName,
    'Respond from fake Codex for browser validation.'
  );
  expect(managedAgentResponse.ok()).toBeTruthy();
  const managedAgent = await managedAgentResponse.json() as { id: string };
  const createdManagedAgentId = managedAgent.id;
  consoleRunCleanupIds.agentIds.push(createdManagedAgentId);

  await expect(page.getByRole('heading', { name: agentName, level: 1 })).toBeVisible();
  const managedAgentName = `Managed ${agentName}`;
  await page.getByRole('tab', { name: 'Instructions' }).click();
  const instructionsPanel = page.getByRole('tabpanel', { name: 'Instructions' });
  await instructionsPanel.getByLabel('Name', { exact: true }).fill(managedAgentName);
  await instructionsPanel.getByRole('textbox', { name: 'Instructions' }).fill('Respond from fake Codex with managed Agent configuration.');
  await instructionsPanel.getByRole('button', { name: 'Save agent' }).click();

  await page.getByRole('tab', { name: 'Access' }).click();
  const runtimeSelect = page.getByLabel('Runtime binding');
  const runtimeValue = await runtimeSelect.locator('option').nth(1).getAttribute('value');
  if (runtimeValue) await runtimeSelect.selectOption(runtimeValue);
  await page.getByRole('tabpanel', { name: 'Access' }).getByRole('button', { name: 'Save agent' }).click();

  await page.getByRole('tab', { name: 'Skills' }).click();
  const skillsPanel = page.getByRole('tabpanel', { name: 'Skills' });
  await skillsPanel.getByRole('button', { name: 'Edit managed skills' }).click();
  const skillsDialog = page.getByRole('dialog', { name: 'Edit managed skills' });
  await skillsDialog.getByRole('checkbox', { name: new RegExp(skillName) }).check();
  await skillsDialog.getByRole('button', { name: 'Save changes' }).click();
  await expect(skillsPanel).toContainText(skillName);

  await page.getByRole('tab', { name: 'MCP' }).click();
  const mcpPanel = page.getByRole('tabpanel', { name: 'MCP' });
  await mcpPanel.getByRole('button', { name: 'Add MCP entry' }).click();
  const mcpDialog = page.getByRole('dialog', { name: 'Add MCP entry' });
  await mcpDialog.getByLabel('Name', { exact: true }).fill('filesystem');
  await mcpDialog.getByLabel('Command').fill('fs');
  await mcpDialog.getByRole('button', { name: 'Save changes' }).click();
  await expect(mcpPanel.getByRole('table', { name: 'MCP allowlist' })).toContainText('filesystem');
  await expect(page.getByRole('heading', { name: managedAgentName, level: 1 })).toBeVisible();

  await page.getByRole('tab', { name: 'Activity' }).click();
  await page.getByLabel('Message').fill('Run from Playwright');
  const targetRunResponsePromise = page.waitForResponse((response) => response.request().method() === 'POST'
    && new URL(response.url()).pathname === `/api/agents/${createdManagedAgentId}/runs`);
  await page.getByRole('button', { name: 'Start run' }).click();
  const targetRunResponse = await targetRunResponsePromise;
  expect(targetRunResponse.ok()).toBeTruthy();
  const targetRun = await targetRunResponse.json() as { id: string };
  prioritizePendingRunForRuntimeClaim(targetRun.id);
  await expect(page.getByText('Fake Codex completed run')).toBeVisible({ timeout: 30_000 });
  await expect(page.getByText('Hub model proxy')).toBeVisible({ timeout: 30_000 });
  await expect(page.locator('.status.completed')).toBeVisible({ timeout: 30_000 });
  await expect(page.locator('[data-run-id].selected')).toContainText('Run from Playwright');
  const firstRun = await page.evaluate(async () => {
    const firstRunId = document.querySelector('[data-run-id].selected')?.getAttribute('data-run-id');
    if (!firstRunId) return null;
    const response = await fetch(`/api/runs/${firstRunId}`);
    return response.ok ? response.json() : null;
  });
  expect(firstRun?.work_dir_ref).toBeTruthy();
  expect(modelProxyConfigProbe(firstRun.work_dir_ref)).toEqual({
    providerSection: 'yes',
    responsesWire: 'yes',
    loopbackBaseUrl: 'yes',
    zeroPort: 'no'
  });

  await page.getByRole('checkbox', { name: 'Continue selected thread' }).check();
  await page.getByLabel('Message').fill('Resume the selected Codex thread from Playwright');
  const resumeRunResponsePromise = page.waitForResponse((response) => response.request().method() === 'POST'
    && new URL(response.url()).pathname === `/api/agents/${createdManagedAgentId}/runs`);
  await page.getByRole('button', { name: 'Start run' }).click();
  const resumeRunResponse = await resumeRunResponsePromise;
  expect(resumeRunResponse.ok()).toBeTruthy();
  prioritizePendingRunForRuntimeClaim((await resumeRunResponse.json() as { id: string }).id);
  await expect(page.locator('[data-run-id].selected')).toContainText('Resume the selected Codex thread from Playwright');
  await expect(page.locator('.status.completed')).toBeVisible({ timeout: 30_000 });
  const resumedRun = await page.evaluate(async () => {
    const runId = document.querySelector('[data-run-id].selected')?.getAttribute('data-run-id');
    if (!runId) return null;
    const response = await fetch(`/api/runs/${runId}`);
    return response.ok ? response.json() : null;
  });
  expect(resumedRun?.parent_run_id).toBe(firstRun.id);
  await page.getByRole('checkbox', { name: 'Continue selected thread' }).uncheck();

  const widgetSessionResponse = await page.request.post('/api/embed/sessions', {
    data: { agent_id: createdManagedAgentId }
  });
  expect(widgetSessionResponse.ok()).toBeTruthy();
  const widgetToken = (await widgetSessionResponse.json() as { token: string }).token;
  const widgetUrl = `${baseURL}/widget#token=${widgetToken}`;
  widget = await context.newPage();
  await widget.goto(widgetUrl);
  await expect(widget).toHaveURL(`${new URL(widgetUrl).origin}/widget`);
  await expect(widget.getByText(managedAgentName)).toBeVisible();
  const widgetRunResponsePromise = widget.waitForResponse((response) => response.request().method() === 'POST'
    && new URL(response.url()).pathname === '/api/widget/runs');
  await widget.getByRole('button', { name: 'Send' }).click();
  const widgetRunResponse = await widgetRunResponsePromise;
  expect(widgetRunResponse.ok()).toBeTruthy();
  prioritizePendingRunForRuntimeClaim((await widgetRunResponse.json() as { id: string }).id);
  await expect(widget.getByText('Fake Codex completed run')).toBeVisible({ timeout: 30_000 });

  await widget.close();
  widget = null;
  await deleteAgentAndWaitForRuntimeSessionCleanup(page.request, createdManagedAgentId);
  await page.goto('/agents');
  await expect(page.getByText(managedAgentName)).toHaveCount(0);
  const automationAgentName = `Automation Agent ${Date.now()}`;
  const automationAgentResponse = await createAgentThroughUi(
    page,
    automationAgentName,
    'Run Automation fixtures through the app-server Codex driver.'
  );
  expect(automationAgentResponse.ok()).toBeTruthy();
  const createdAutomationAgentId = (await automationAgentResponse.json() as { id: string }).id;
  consoleRunCleanupIds.agentIds.push(createdAutomationAgentId);
  await expect(page.getByRole('heading', { name: automationAgentName, level: 1 })).toBeVisible();
  await page.getByRole('tab', { name: 'Access' }).click();
  if (runtimeValue) await page.getByLabel('Runtime binding').selectOption(runtimeValue);
  await page.getByRole('tabpanel', { name: 'Access' }).getByRole('button', { name: 'Save agent' }).click();

  await page.goto('/automations');
  await expect(page.getByRole('button', { name: 'New automation' })).toBeVisible();
  await page.getByRole('button', { name: 'New automation' }).click();
  const manualAutomationDialog = page.getByRole('dialog', { name: 'Create Automation' });
  const automationName = `Automation ${Date.now()}`;
  await manualAutomationDialog.getByLabel('Agent').selectOption({ label: automationAgentName });
  await manualAutomationDialog.getByLabel('Name', { exact: true }).fill(automationName);
  await manualAutomationDialog.getByLabel('Trigger').selectOption('interval');
  await manualAutomationDialog.getByLabel('Schedule', { exact: true }).fill('9s');
  await manualAutomationDialog.getByLabel('Trigger').selectOption('cron');
  await expect(manualAutomationDialog.getByLabel('Schedule', { exact: true })).toHaveValue('');
  await manualAutomationDialog.getByLabel('Schedule', { exact: true }).fill('* * * * *');
  await manualAutomationDialog.getByLabel('Trigger').selectOption('interval');
  await expect(manualAutomationDialog.getByLabel('Schedule', { exact: true })).toHaveValue('');
  await manualAutomationDialog.getByLabel('Trigger').selectOption('manual');
  await expect(manualAutomationDialog.getByLabel('Schedule', { exact: true })).toHaveCount(0);
  await manualAutomationDialog.getByRole('textbox', { name: 'Prompt' }).fill('Automation run from Playwright');
  await manualAutomationDialog.getByRole('button', { name: 'Create automation' }).click();

  const manualAutomationRow = page.locator('.automation-list-row').filter({ hasText: automationName });
  await expect(manualAutomationRow).toBeVisible();
  const manualRunResponsePromise = page.waitForResponse((response) => response.request().method() === 'POST'
    && /^\/api\/automations\/[^/]+\/trigger$/.test(new URL(response.url()).pathname));
  await manualAutomationRow.getByRole('button', { name: 'Run now' }).click();
  const manualRunResponse = await manualRunResponsePromise;
  expect(manualRunResponse.ok()).toBeTruthy();
  const manualRun = await manualRunResponse.json() as { id: string; automation_id: string | null };
  prioritizePendingRunForRuntimeClaim(manualRun.id);
  const automationHistory = page.getByRole('region', { name: 'Run history' });
  const manualHistoryRow = automationHistory.locator(`[data-run-id="${manualRun.id}"]`);
  await expect(manualHistoryRow).toContainText('completed', { timeout: 30_000 });
  await expect(manualHistoryRow).toContainText('Manual automation');
  await expect(manualHistoryRow).toContainText('Automation run from Playwright');
  await manualHistoryRow.click();
  const manualRunEvents = page.getByRole('region', { name: 'Run events' });
  await expect(manualRunEvents).toContainText('Fake Codex completed run');
  await expect(manualRunEvents.locator('.status.completed')).toBeVisible();
  const manualAutomationId = (await (await page.request.get('/api/automations')).json() as Array<{ id: string; name: string }>).find((automation) => automation.name === automationName)?.id;
  expect(manualRun.automation_id).toBe(manualAutomationId);

  await page.getByRole('button', { name: 'New automation' }).click();
  const webhookName = `Webhook ${Date.now()}`;
  let webhookDialog = page.getByRole('dialog', { name: 'Create Automation' });
  await webhookDialog.getByLabel('Agent').selectOption({ label: automationAgentName });
  await webhookDialog.getByLabel('Name', { exact: true }).fill(webhookName);
  await webhookDialog.getByLabel('Trigger').selectOption('webhook');
  await webhookDialog.getByRole('textbox', { name: 'Prompt' }).fill('Webhook automation run from Playwright');
  await webhookDialog.getByRole('button', { name: 'Create automation' }).click();
  let webhookSecretDialog = page.getByRole('dialog', { name: 'One-time webhook token' });
  const webhookToken = await webhookSecretDialog.locator('.secret-token').innerText();
  await webhookSecretDialog.locator('.modal-actions').getByRole('button', { name: 'Close', exact: true }).click();
  await expect(page.locator('.automation-list-row').filter({ hasText: webhookName })).toBeVisible();

  await page.getByRole('button', { name: 'New automation' }).click();
  const secondWebhookName = `Webhook second ${Date.now()}`;
  webhookDialog = page.getByRole('dialog', { name: 'Create Automation' });
  await webhookDialog.getByLabel('Agent').selectOption({ label: automationAgentName });
  await webhookDialog.getByLabel('Name', { exact: true }).fill(secondWebhookName);
  await webhookDialog.getByLabel('Trigger').selectOption('webhook');
  await webhookDialog.getByRole('textbox', { name: 'Prompt' }).fill('Second webhook automation run from Playwright');
  await webhookDialog.getByRole('button', { name: 'Create automation' }).click();
  webhookSecretDialog = page.getByRole('dialog', { name: 'One-time webhook token' });
  const secondWebhookToken = await webhookSecretDialog.locator('.secret-token').innerText();
  await expect(webhookSecretDialog).not.toContainText(webhookToken);
  await webhookSecretDialog.locator('.modal-actions').getByRole('button', { name: 'Close', exact: true }).click();
  const secondWebhookRow = page.locator('.automation-list-row').filter({ hasText: secondWebhookName });
  await expect(secondWebhookRow).toBeVisible();
  await expect(page.locator('.secret-token')).toHaveCount(0);
  await expect(page.locator('body')).not.toContainText(webhookToken);
  const listedAutomations = await (await page.request.get('/api/automations')).json();
  expect(listedAutomations.filter((automation: { trigger_type: string }) => automation.trigger_type === 'webhook').every((automation: { webhook_token: string | null }) => automation.webhook_token === null)).toBe(true);

  anonymous = await request.newContext({ baseURL });
  const anonymousWebhook = await anonymous.post('/api/automations/webhook', {
    headers: { 'X-Agent-Hub-Webhook-Token': secondWebhookToken },
    data: {}
  });
  expect(anonymousWebhook.ok()).toBeTruthy();
  const anonymousWebhookRunId = (await anonymousWebhook.json() as { id: string }).id;
  prioritizePendingRunForRuntimeClaim(anonymousWebhookRunId);
  expect((await anonymous.post('/api/automations/webhook', { headers: { 'X-Agent-Hub-Webhook-Token': 'invalid-token' }, data: {} })).status()).toBe(401);
  await page.reload();
  await expect(page.locator('.secret-token')).toHaveCount(0);
  await expect.poll(async () => {
    const response = await page.request.get(`/api/runs/${anonymousWebhookRunId}`);
    if (!response.ok()) return null;
    return (await response.json() as { status: string }).status;
  }, { timeout: 30_000 }).toBe('completed');

  await deleteAgentAndWaitForRuntimeSessionCleanup(page.request, createdAutomationAgentId);
  expect((await anonymous.post('/api/automations/webhook', {
    headers: { 'X-Agent-Hub-Webhook-Token': secondWebhookToken },
    data: {}
  })).status()).toBe(401);
  await anonymous.dispose();
  anonymous = null;

  await page.goto('/agents');
  const scheduledAgentName = `Scheduled Agent ${Date.now()}`;
  const scheduledAgentResponse = await createAgentThroughUi(
    page,
    scheduledAgentName,
    'Run scheduled Automation fixtures through the app-server Codex driver.'
  );
  expect(scheduledAgentResponse.ok()).toBeTruthy();
  const createdScheduledAgentId = (await scheduledAgentResponse.json() as { id: string }).id;
  consoleRunCleanupIds.agentIds.push(createdScheduledAgentId);
  await expect(page.getByRole('heading', { name: scheduledAgentName, level: 1 })).toBeVisible();
  await page.getByRole('tab', { name: 'Access' }).click();
  if (runtimeValue) await page.getByLabel('Runtime binding').selectOption(runtimeValue);
  await page.getByRole('tabpanel', { name: 'Access' }).getByRole('button', { name: 'Save agent' }).click();
  const scheduledAgentUrl = `/agents/${createdScheduledAgentId}`;

  await page.goto('/automations');
  const intervalName = `Interval ${Date.now()}`;
  const intervalRunMessage = 'Scheduled interval automation run from Playwright';
  await page.getByRole('button', { name: 'New automation' }).click();
  const intervalDialog = page.getByRole('dialog', { name: 'Create Automation' });
  await intervalDialog.getByLabel('Agent').selectOption({ label: scheduledAgentName });
  await intervalDialog.getByLabel('Name', { exact: true }).fill(intervalName);
  await intervalDialog.getByLabel('Trigger').selectOption('interval');
  await intervalDialog.getByRole('textbox', { name: 'Prompt' }).fill(intervalRunMessage);
  await intervalDialog.getByRole('textbox', { name: 'Schedule' }).fill('2s');
  await intervalDialog.getByRole('button', { name: 'Create automation' }).click();
  await expect(page.locator('.automation-list-row').filter({ hasText: intervalName })).toBeVisible();
  const intervalRunId = await completedScheduledRunId(page, createdScheduledAgentId, intervalRunMessage);
  await disableAutomationByName(page.request, intervalName);

  const cronName = `Cron ${Date.now()}`;
  const cronRunMessage = 'Scheduled cron automation run from Playwright';
  await page.getByRole('button', { name: 'New automation' }).click();
  const cronDialog = page.getByRole('dialog', { name: 'Create Automation' });
  await cronDialog.getByLabel('Agent').selectOption({ label: scheduledAgentName });
  await cronDialog.getByLabel('Name', { exact: true }).fill(cronName);
  await cronDialog.getByLabel('Trigger').selectOption('cron');
  await cronDialog.getByRole('textbox', { name: 'Prompt' }).fill(cronRunMessage);
  await cronDialog.getByLabel('Schedule', { exact: true }).fill('* * * * *');
  await cronDialog.getByRole('button', { name: 'Create automation' }).click();
  await expect(page.locator('.automation-list-row').filter({ hasText: cronName })).toBeVisible();
  const cronRunId = await completedScheduledRunId(page, createdScheduledAgentId, cronRunMessage);
  await disableAutomationByName(page.request, cronName);

  const disabledName = `Disabled ${Date.now()}`;
  await page.getByRole('button', { name: 'New automation' }).click();
  const disabledDialog = page.getByRole('dialog', { name: 'Create Automation' });
  await disabledDialog.getByLabel('Agent').selectOption({ label: scheduledAgentName });
  await disabledDialog.getByLabel('Name', { exact: true }).fill(disabledName);
  await disabledDialog.getByLabel('Trigger').selectOption('manual');
  await disabledDialog.getByRole('textbox', { name: 'Prompt' }).fill('Disabled automation must not run.');
  await disabledDialog.getByRole('checkbox', { name: 'Enabled' }).uncheck();
  await disabledDialog.getByRole('button', { name: 'Create automation' }).click();
  const disabledRow = page.locator('.automation-list-row').filter({ hasText: disabledName });
  await expect(disabledRow).toContainText('disabled');
  await expect(disabledRow.getByRole('button', { name: 'Run now' })).toHaveCount(0);
  const disabledAutomation = (await (await page.request.get('/api/automations')).json()).find((automation: { name: string }) => automation.name === disabledName);
  expect((await page.request.post(`/api/automations/${disabledAutomation.id}/trigger`, { data: {} })).status()).toBe(403);

  await page.goto(scheduledAgentUrl);
  const intervalRunRow = page.locator(`[data-run-id="${intervalRunId}"]`);
  await expect(intervalRunRow).toContainText(intervalRunMessage);
  await expect(intervalRunRow).toContainText('completed');
  await expect(intervalRunRow).toContainText('Scheduled automation');
  await intervalRunRow.click();
  await expect(page.getByText('Fake Codex completed run')).toBeVisible({ timeout: 30_000 });
  const cronRunRow = page.locator(`[data-run-id="${cronRunId}"]`);
  await expect(cronRunRow).toContainText(cronRunMessage);
  await expect(cronRunRow).toContainText('completed');
  await expect(cronRunRow).toContainText('Scheduled automation');
  await cronRunRow.click();
  await expect(page.getByText('Fake Codex completed run')).toBeVisible({ timeout: 30_000 });

  await page.goto('/runtimes');
  const composeRuntimeRow = page.getByRole('button', { name: /compose-runtime-1 online/ });
  await expect(composeRuntimeRow).toBeVisible({ timeout: 15_000 });
  await composeRuntimeRow.click();
  const runtimeDetail = page.getByRole('region', { name: 'Runtime details' });
  await expect(runtimeDetail.getByText('online').first()).toBeVisible();
  await expect(runtimeDetail.getByText('driver:app-server').first()).toBeVisible();
  await expect(runtimeDetail.getByText('unmanaged').first()).toBeVisible();
  await expect(runtimeDetail.locator('dt', { hasText: /^Model proxy$/ }).locator('..').locator('dd')).toHaveText('Enabled');

  await page.goto('/skills');
  await page.getByText(skillName).click();
  page.once('dialog', (dialog) => dialog.accept());
  await page.getByRole('button', { name: 'Delete skill' }).click();
  managedSkillId = null;
  consoleRunCleanupIds.skillId = null;
  await expect(page.getByText(skillName)).toHaveCount(0);

  await page.goto('/agents');
  await page.getByText(scheduledAgentName).click();
  page.once('dialog', (dialog) => dialog.accept());
  await page.getByRole('button', { name: 'Delete agent' }).click();
  await expect(page.getByText('Create Agent', { exact: true })).toBeVisible();
  await expect(page.getByText(scheduledAgentName)).toHaveCount(0);
  } finally {
    await cleanupResources('Console/run E2E fixture cleanup failed', [
      {
        name: 'widget page',
        run: async () => {
          if (widget && !widget.isClosed()) await widget.close();
        }
      },
      {
        name: 'anonymous API context',
        run: async () => {
          await anonymous?.dispose();
        }
      }
    ]);
  }
});

test('archiving an agent wins queued manual triggers and serializes with Automation creation', async ({ page }) => {
  await page.goto('/login');
  await page.getByLabel('Email').fill('admin@example.com');
  await page.getByLabel('Password').fill('admin123');
  await page.getByRole('button', { name: 'Sign in', exact: true }).click();
  await expect(page.getByText('admin@example.com')).toBeVisible();

  const nonce = Date.now();
  const agentName = `Archive race agent ${nonce}`;
  let agentId: string | null = null;
  let heldLock: ReturnType<typeof holdAgentRowLock> | null = null;
  let archiveResponsePromise: Promise<{ status: number }> | null = null;
  let concurrentTriggerPromises: Array<Promise<{ runId: string | null; status: number }>> = [];
  let automationCreatePromise: Promise<{ automationId: string | null; status: number }> | null = null;
  try {
    await page.goto('/agents');
    const agentResponse = await createAgentThroughUi(
      page,
      agentName,
      'Exercise archive and automation trigger locking.'
    );
    expect(agentResponse.ok()).toBeTruthy();
    const createdAgent = await agentResponse.json() as { id: string };
    const currentAgentId = createdAgent.id;
    agentId = currentAgentId;
    await expect(page).toHaveURL(new RegExp(`/agents/${currentAgentId}$`));

    const automationName = `Archive race manual automation ${nonce}`;
    await page.goto('/automations');
    await page.getByRole('button', { name: 'New automation' }).click();
    const automationDialog = page.getByRole('dialog', { name: 'Create Automation' });
    await automationDialog.getByLabel('Agent').selectOption({ label: agentName });
    await automationDialog.getByLabel('Name', { exact: true }).fill(automationName);
    await automationDialog.getByLabel('Trigger').selectOption('manual');
    await automationDialog.getByRole('textbox', { name: 'Prompt' }).fill('Manual trigger that races Agent archival.');
    await automationDialog.getByRole('button', { name: 'Create automation' }).click();
    await expect(page.locator('.automation-list-row').filter({ hasText: automationName })).toBeVisible();

    const listedAutomationsResponse = await page.request.get('/api/automations');
    expect(listedAutomationsResponse.ok()).toBeTruthy();
    const listedAutomations = await listedAutomationsResponse.json() as Array<{
      agent_id: string;
      enabled: boolean;
      id: string;
      name: string;
    }>;
    const automation = listedAutomations.find((item) => item.name === automationName);
    if (!automation) throw new Error('Created automation was not returned by the API');
    await page.goto(`/agents/${currentAgentId}`);
    heldLock = holdAgentRowLock(currentAgentId, 30);
    const lockBackendPid = await waitForPostgresLock(heldLock.applicationName, 'manual/archive Agent row lock');

    // 先让 archive 单独进入 tuple-lock 队列，确保释放外部锁后 archive 是确定的赢家。
    archiveResponsePromise = page.request.delete(`/api/agents/${currentAgentId}`, {
      timeout: 20_000
    }).then((response) => ({ status: response.status() }));
    void archiveResponsePromise.catch(() => {});
    await expect.poll(
      () => agentRowLockWaiterCounts(lockBackendPid),
      { timeout: 5_000 }
    ).toEqual({ archive: 1, automationCreate: 0, manualTrigger: 0, scheduler: 0 });

    const concurrentTriggerCount = 3;
    concurrentTriggerPromises = Array.from({ length: concurrentTriggerCount }, (_, index) => {
      const triggerPromise = page.request.post(`/api/automations/${automation.id}/trigger`, {
        data: { message: `Concurrent archive trigger ${nonce}-${index}` },
        timeout: 20_000
      }).then(async (response) => ({
        status: response.status(),
        runId: response.ok() ? (await response.json() as { id?: string }).id ?? null : null
      }));
      void triggerPromise.catch(() => {});
      return triggerPromise;
    });
    automationCreatePromise = page.request.post('/api/automations', {
      data: {
        agent_id: currentAgentId,
        name: `Concurrent archive create ${nonce}`,
        trigger_type: 'manual',
        prompt: 'Automation creation that waits behind archival.',
        schedule: null,
        enabled: true
      },
      timeout: 20_000
    }).then(async (response) => ({
      status: response.status(),
      automationId: response.ok() ? (await response.json() as { id?: string }).id ?? null : null
    }));
    void automationCreatePromise.catch(() => {});

    // 后续请求可能由 archive waiter 软阻塞；递归 blocker chain 证明它们都在同一 Agent 锁队列。
    await expect.poll(
      () => agentRowLockWaiterCounts(lockBackendPid),
      { timeout: 10_000 }
    ).toEqual({
      archive: 1,
      automationCreate: 1,
      manualTrigger: concurrentTriggerCount,
      scheduler: 0
    });

    await stopPostgresLock(heldLock);
    heldLock = null;
    const [archiveResponse, concurrentTriggers, concurrentCreate] = await completeWithin(
      Promise.all([
        archiveResponsePromise,
        Promise.all(concurrentTriggerPromises),
        automationCreatePromise
      ]),
      15_000,
      'Archive, manual triggers, and Automation creation did not complete; possible deadlock'
    );

    expect(archiveResponse.status).toBe(204);
    expect(concurrentTriggers).toHaveLength(concurrentTriggerCount);
    for (const trigger of concurrentTriggers) {
      expect(trigger.status).toBeLessThan(500);
      expect(trigger).toEqual({ status: 403, runId: null });
    }
    expect(concurrentCreate.status).toBeLessThan(500);
    expect([200, 403]).toContain(concurrentCreate.status);
    if (concurrentCreate.status === 200) {
      expect(concurrentCreate.automationId).toEqual(expect.any(String));
    } else {
      expect(concurrentCreate.automationId).toBeNull();
    }

    const archivedAutomationsResponse = await page.request.get('/api/automations');
    expect(archivedAutomationsResponse.ok()).toBeTruthy();
    const archivedAutomations = await archivedAutomationsResponse.json() as Array<{
      agent_id: string;
      enabled: boolean;
      id: string;
    }>;
    expect(archivedAutomations.filter((item) => item.agent_id === currentAgentId && item.enabled)).toEqual([]);
    expect(archivedAgentState(currentAgentId)).toMatchObject({
      activeRuns: 0,
      archivedAt: expect.any(String),
      enabledAutomations: 0,
      postArchiveRuns: 0
    });

    const postArchiveAttempts = await Promise.all(Array.from({ length: 3 }, async () => {
      const response = await page.request.post(`/api/automations/${automation.id}/trigger`, {
        data: { message: 'This run must not exist after archival.' },
        timeout: 5_000
      });
      return {
        status: response.status(),
        runId: response.ok() ? (await response.json() as { id?: string }).id ?? null : null
      };
    }));
    expect(postArchiveAttempts.every(({ status, runId }) => status === 404 && runId === null)).toBeTruthy();

    // 后续 trigger 也必须无法改变归档后的数据库不变量。
    const finalState = archivedAgentState(currentAgentId);
    expect(finalState).toMatchObject({
      activeRuns: 0,
      enabledAutomations: 0,
      postArchiveRuns: 0
    });
  } finally {
    await cleanupResources('Manual/archive E2E fixture cleanup failed', [
      {
        name: 'Agent row lock',
        run: async () => {
          if (heldLock) await stopPostgresLock(heldLock);
        }
      },
      {
        name: 'archive request',
        run: async () => {
          if (archiveResponsePromise) {
            await completeWithin(archiveResponsePromise, 20_000, 'Archive cleanup request did not finish');
          }
        }
      },
      ...concurrentTriggerPromises.map((triggerPromise, index) => ({
        name: `manual trigger request ${index + 1}`,
        run: async () => {
          await completeWithin(triggerPromise, 20_000, `Manual trigger cleanup request ${index + 1} did not finish`);
        }
      })),
      {
        name: 'Automation create request',
        run: async () => {
          if (automationCreatePromise) {
            await completeWithin(automationCreatePromise, 20_000, 'Automation create cleanup request did not finish');
          }
        }
      },
      {
        name: 'Agent and Automations',
        run: async () => {
          if (agentId) await deleteAgentForCleanup(page.request, agentId);
        }
      }
    ]);
  }
});

test('archiving an agent wins against a registered runtime claim and fails the pending run', async ({ page, baseURL }) => {
  let runtimeApi: APIRequestContext | null = null;
  let agentId: string | null = null;
  let runtimeId: string | null = null;
  let archiveGateLock: ReturnType<typeof holdAutomationRowLock> | null = null;
  let claimIsolationLock: ReturnType<typeof holdOtherPendingRunLocks> | null = null;
  let archiveResponsePromise: Promise<{ status: number }> | null = null;
  let claimResponsePromise: Promise<import('@playwright/test').APIResponse> | null = null;
  try {
    await page.goto('/login');
    await page.getByLabel('Email').fill('admin@example.com');
    await page.getByLabel('Password').fill('admin123');
    await page.getByRole('button', { name: 'Sign in', exact: true }).click();
    await expect(page.getByText('admin@example.com')).toBeVisible();

    const nonce = Date.now();
    runtimeApi = await request.newContext({ baseURL });
    const registration = await runtimeApi.post('/api/runtime/register', {
      headers: { Authorization: `Bearer ${await runtimeEnrollmentToken(runtimeApi)}` },
      data: {
        hostname: `batch1-claim-archive-${nonce}`,
        labels: ['playwright', 'batch1'],
        codex_version: 'batch1-e2e',
        capabilities: { model_proxy: true, mcp_allowlist: false },
        sandbox_mode: 'workspace-write'
      }
    });
    expect(registration.ok()).toBeTruthy();
    const temporaryRuntime = await registration.json() as { runtime_id: string; runtime_credential: string };
    runtimeId = temporaryRuntime.runtime_id;

    const agentName = `Claim archive race ${nonce}`;
    await page.goto('/agents');
    const createdAgentResponse = await createAgentThroughUi(
      page,
      agentName,
      'Exercise archive and runtime claim locking.'
    );
    expect(createdAgentResponse.ok()).toBeTruthy();
    const createdAgent = await createdAgentResponse.json() as { id: string };
    const currentAgentId = createdAgent.id;
    agentId = currentAgentId;
    await expect(page).toHaveURL(new RegExp(`/agents/${currentAgentId}$`));

    await page.getByRole('tab', { name: 'Access' }).click();
    const saveBoundAgentResponse = page.waitForResponse((response) => response.request().method() === 'PATCH'
      && new URL(response.url()).pathname === `/api/agents/${currentAgentId}`);
    await page.getByLabel('Runtime binding').selectOption(runtimeId);
    await page.getByRole('button', { name: 'Save agent' }).click();
    const boundAgentResponse = await saveBoundAgentResponse;
    expect(boundAgentResponse.ok()).toBeTruthy();
    expect((await boundAgentResponse.json() as { runtime_id: string | null }).runtime_id).toBe(runtimeId);

    const createAutomationResponse = await page.request.post('/api/automations', {
      data: {
        agent_id: currentAgentId,
        name: `Claim archive gate ${nonce}`,
        trigger_type: 'manual',
        prompt: 'Keep archive waiting while the runtime claims.',
        schedule: null,
        enabled: true
      }
    });
    expect(createAutomationResponse.ok()).toBeTruthy();
    const automation = await createAutomationResponse.json() as { id: string };

    const pendingRunMessage = `Claim archive pending run ${nonce}`;
    const createRunResponse = await page.request.post(`/api/agents/${currentAgentId}/runs`, {
      data: { message: pendingRunMessage }
    });
    expect(createRunResponse.ok()).toBeTruthy();
    const pendingRun = await createRunResponse.json() as { id: string; status: string };
    expect(pendingRun.status).toBe('pending');

    // 锁住历史 pending work，让临时 runtime 的真实 claim 只能领取本用例目标 run。
    prioritizePendingRunForRuntimeClaim(pendingRun.id);
    claimIsolationLock = holdOtherPendingRunLocks(pendingRun.id, 30);
    await waitForPostgresLock(claimIsolationLock.applicationName, 'pending run claim isolation lock');
    archiveGateLock = holdAutomationRowLock(automation.id, 30);
    const archiveGateBackendPid = await waitForPostgresLock(
      archiveGateLock.applicationName,
      'archive Automation row lock'
    );

    // Archive 已锁住 Agent 并等待 Automation 行；此时 claim 与 archive 事务真实重叠。
    archiveResponsePromise = page.evaluate(async (currentId) => {
      const response = await fetch(`/api/agents/${currentId}`, { method: 'DELETE' });
      return { status: response.status };
    }, currentAgentId);
    void archiveResponsePromise.catch(() => {});
    await expect.poll(
      () => archiveAutomationLockWaiterCount(archiveGateBackendPid),
      { timeout: 5_000 }
    ).toBeGreaterThan(0);

    claimResponsePromise = runtimeApi.post('/api/runtime/runs/claim', {
      headers: { Authorization: `Bearer ${temporaryRuntime.runtime_credential}` },
      data: { available_new_session_slots: 1, ready_owned_sessions: [] },
      timeout: 10_000
    });
    void claimResponsePromise.catch(() => {});
    await expectPromisePending(
      claimResponsePromise,
      250,
      'Registered runtime claim completed while archive held the Agent lock'
    );

    await stopPostgresLock(archiveGateLock);
    archiveGateLock = null;
    const [archiveResponse, claimResponse] = await completeWithin(
      Promise.all([archiveResponsePromise, claimResponsePromise]),
      15_000,
      'Archive and runtime claim contention did not complete; possible deadlock'
    );
    expect(archiveResponse.status).toBe(204);
    expect(claimResponse.status()).toBe(204);

    expect(archivedAgentState(currentAgentId)).toMatchObject({
      activeRuns: 0,
      archivedAt: expect.any(String),
      enabledAutomations: 0,
      postArchiveRuns: 0
    });
    expect(storedRunState(pendingRun.id)).toEqual({ status: 'failed', runtimeId: null });
    const repeatClaim = await completeWithin(
      runtimeApi.post('/api/runtime/runs/claim', {
        headers: { Authorization: `Bearer ${temporaryRuntime.runtime_credential}` },
        data: { available_new_session_slots: 1, ready_owned_sessions: [] }
      }),
      5_000,
      'Post-archive runtime claim did not complete'
    );
    expect(repeatClaim.status()).toBe(204);

    await stopPostgresLock(claimIsolationLock);
    claimIsolationLock = null;
  } finally {
    await cleanupResources('Runtime claim/archive E2E fixture cleanup failed', [
      {
        name: 'archive gate lock',
        run: async () => {
          if (archiveGateLock) await stopPostgresLock(archiveGateLock);
        }
      },
      {
        name: 'archive request',
        run: async () => {
          if (archiveResponsePromise) {
            await completeWithin(archiveResponsePromise, 20_000, 'Claim/archive cleanup request did not finish');
          }
        }
      },
      {
        name: 'runtime claim request',
        run: async () => {
          if (claimResponsePromise) {
            await completeWithin(claimResponsePromise, 15_000, 'Runtime claim cleanup request did not finish');
          }
        }
      },
      {
        name: 'pending run isolation lock',
        run: async () => {
          if (claimIsolationLock) await stopPostgresLock(claimIsolationLock);
        }
      },
      {
        name: 'Agent and Automation',
        run: async () => {
          if (agentId) await deleteAgentForCleanup(page.request, agentId);
        }
      },
      {
        name: 'runtime API context',
        run: async () => {
          await runtimeApi?.dispose();
        }
      },
      {
        name: 'temporary runtime',
        run: () => {
          if (runtimeId) deleteTemporaryRuntime(runtimeId);
        }
      }
    ]);
  }
});

test('archive queued before a due scheduler leaves zero scheduled runs', async ({ page }) => {
  await page.goto('/login');
  await page.getByLabel('Email').fill('admin@example.com');
  await page.getByLabel('Password').fill('admin123');
  await page.getByRole('button', { name: 'Sign in', exact: true }).click();
  await expect(page.getByText('admin@example.com')).toBeVisible();

  const nonce = Date.now();
  const agentName = `Archive-first scheduler race ${nonce}`;
  let agentId: string | null = null;
  let heldLock: ReturnType<typeof holdAgentRowLock> | null = null;
  let archiveResponsePromise: Promise<{ status: number }> | null = null;
  try {
    await page.goto('/agents');
    const agentResponse = await createAgentThroughUi(
      page,
      agentName,
      'Prove archive wins the scheduler Agent lock queue.'
    );
    expect(agentResponse.ok()).toBeTruthy();
    const currentAgentId = (await agentResponse.json() as { id: string }).id;
    agentId = currentAgentId;
    await expect(page).toHaveURL(new RegExp(`/agents/${currentAgentId}$`));

    await page.goto('/automations');
    await page.getByRole('button', { name: 'New automation' }).click();
    const automationDialog = page.getByRole('dialog', { name: 'Create Automation' });
    await automationDialog.getByLabel('Agent').selectOption({ label: agentName });
    await automationDialog.getByLabel('Name', { exact: true }).fill(`Archive-first due scheduler ${nonce}`);
    await automationDialog.getByLabel('Trigger').selectOption('interval');
    await automationDialog.getByLabel('Schedule', { exact: true }).fill('1s');
    await automationDialog.getByRole('textbox', { name: 'Prompt' }).fill(`Archive-first scheduler must not run ${nonce}`);
    await automationDialog.getByRole('checkbox', { name: 'Enabled' }).uncheck();
    const createAutomationResponse = page.waitForResponse((response) => response.request().method() === 'POST'
      && new URL(response.url()).pathname === '/api/automations');
    await automationDialog.getByRole('button', { name: 'Create automation' }).click();
    const automationResponse = await createAutomationResponse;
    expect(automationResponse.ok()).toBeTruthy();
    const automation = await automationResponse.json() as { id: string };
    expect(archivedAgentState(currentAgentId).schedulerRuns).toEqual([]);

    await page.goto(`/agents/${currentAgentId}`);
    heldLock = holdAgentRowLock(currentAgentId, 30);
    const lockBackendPid = await waitForPostgresLock(heldLock.applicationName, 'archive-first Agent row lock');
    archiveResponsePromise = page.request.delete(`/api/agents/${currentAgentId}`, {
      timeout: 20_000
    }).then((response) => ({ status: response.status() }));
    void archiveResponsePromise.catch(() => {});

    // Archive 必须先成为唯一 waiter，scheduler 随后只能排在它后面。
    await expect.poll(
      () => agentRowLockWaiterCounts(lockBackendPid),
      { timeout: 5_000 }
    ).toEqual({ archive: 1, automationCreate: 0, manualTrigger: 0, scheduler: 0 });
    enableDueAutomation(automation.id);
    await expect.poll(
      () => agentRowLockWaiterCounts(lockBackendPid),
      { timeout: 8_000 }
    ).toEqual({ archive: 1, automationCreate: 0, manualTrigger: 0, scheduler: 1 });

    await stopPostgresLock(heldLock);
    heldLock = null;
    const archiveResponse = await completeWithin(
      archiveResponsePromise,
      15_000,
      'Archive-first scheduler contention did not complete; possible deadlock'
    );
    expect(archiveResponse.status).toBe(204);

    const state = archivedAgentState(currentAgentId);
    expect(state).toMatchObject({
      activeRuns: 0,
      archivedAt: expect.any(String),
      enabledAutomations: 0,
      postArchiveRuns: 0
    });
    expect(state.schedulerRuns).toEqual([]);
  } finally {
    await cleanupResources('Archive-first scheduler E2E fixture cleanup failed', [
      {
        name: 'Agent row lock',
        run: async () => {
          if (heldLock) await stopPostgresLock(heldLock);
        }
      },
      {
        name: 'archive request',
        run: async () => {
          if (archiveResponsePromise) {
            await completeWithin(archiveResponsePromise, 20_000, 'Archive-first cleanup request did not finish');
          }
        }
      },
      {
        name: 'Agent and scheduled Automation',
        run: async () => {
          if (agentId) await deleteAgentForCleanup(page.request, agentId);
        }
      }
    ]);
  }
});

test('a due scheduler queued before archive creates exactly one run before archival', async ({ page }) => {
  await page.goto('/login');
  await page.getByLabel('Email').fill('admin@example.com');
  await page.getByLabel('Password').fill('admin123');
  await page.getByRole('button', { name: 'Sign in', exact: true }).click();
  await expect(page.getByText('admin@example.com')).toBeVisible();

  const nonce = Date.now();
  const agentName = `Scheduler archive race ${nonce}`;
  let agentId: string | null = null;
  let heldLock: ReturnType<typeof holdAgentRowLock> | null = null;
  let archiveResponsePromise: Promise<{ status: number }> | null = null;
  try {
    await page.goto('/agents');
    const agentResponse = await createAgentThroughUi(
      page,
      agentName,
      'Exercise scheduler and archive lock ordering.'
    );
    expect(agentResponse.ok()).toBeTruthy();
    const createdAgent = await agentResponse.json() as { id: string };
    const currentAgentId = createdAgent.id;
    agentId = currentAgentId;
    await expect(page).toHaveURL(new RegExp(`/agents/${currentAgentId}$`));

    const schedulerMessage = `Scheduler run that races Agent archival ${nonce}`;
    await page.goto('/automations');
    await page.getByRole('button', { name: 'New automation' }).click();
    const automationDialog = page.getByRole('dialog', { name: 'Create Automation' });
    await automationDialog.getByLabel('Agent').selectOption({ label: agentName });
    await automationDialog.getByLabel('Name', { exact: true }).fill(`Due scheduler ${nonce}`);
    await automationDialog.getByLabel('Trigger').selectOption('interval');
    await automationDialog.getByLabel('Schedule', { exact: true }).fill('1s');
    await automationDialog.getByRole('textbox', { name: 'Prompt' }).fill(schedulerMessage);
    await automationDialog.getByRole('checkbox', { name: 'Enabled' }).uncheck();
    const createAutomationResponse = page.waitForResponse((response) => response.request().method() === 'POST'
      && new URL(response.url()).pathname === '/api/automations');
    await automationDialog.getByRole('button', { name: 'Create automation' }).click();
    const automationResponse = await createAutomationResponse;
    expect(automationResponse.ok()).toBeTruthy();
    const automation = await automationResponse.json() as { id: string };

    // disabled 状态创建后先确认没有 scheduler run，防止锁竞争前已合法触发造成假阳性。
    expect(archivedAgentState(currentAgentId).schedulerRuns).toEqual([]);
    await page.goto(`/agents/${currentAgentId}`);
    heldLock = holdAgentRowLock(currentAgentId, 30);
    const lockBackendPid = await waitForPostgresLock(heldLock.applicationName, 'scheduler-first Agent row lock');
    enableDueAutomation(automation.id);

    // 先观察 scheduler 单独排队，再发 archive，锁队列顺序决定 scheduler 必须先提交一次。
    await expect.poll(
      () => agentRowLockWaiterCounts(lockBackendPid),
      { timeout: 8_000 }
    ).toEqual({ archive: 0, automationCreate: 0, manualTrigger: 0, scheduler: 1 });
    archiveResponsePromise = page.request.delete(`/api/agents/${currentAgentId}`, {
      timeout: 20_000
    }).then((response) => ({ status: response.status() }));
    void archiveResponsePromise.catch(() => {});
    await expect.poll(
      () => agentRowLockWaiterCounts(lockBackendPid),
      { timeout: 5_000 }
    ).toEqual({ archive: 1, automationCreate: 0, manualTrigger: 0, scheduler: 1 });
    await stopPostgresLock(heldLock);
    heldLock = null;
    const archiveResponse = await completeWithin(
      archiveResponsePromise,
      15_000,
      'Scheduler and archive contention did not complete; possible deadlock'
    );
    expect(archiveResponse.status).toBe(204);

    const state = archivedAgentState(currentAgentId);
    expect(state).toMatchObject({
      activeRuns: 0,
      enabledAutomations: 0,
      postArchiveRuns: 0
    });
    expect(state.archivedAt).toBeTruthy();
    expect(state.schedulerRuns).toHaveLength(1);
    expect(state.schedulerRuns[0].message).toBe(schedulerMessage);
    expect(new Date(state.schedulerRuns[0].createdAt).getTime()).toBeLessThanOrEqual(new Date(state.archivedAt!).getTime());
    expect(['completed', 'failed']).toContain(state.schedulerRuns[0].status);
  } finally {
    await cleanupResources('Scheduler-first archive E2E fixture cleanup failed', [
      {
        name: 'Agent row lock',
        run: async () => {
          if (heldLock) await stopPostgresLock(heldLock);
        }
      },
      {
        name: 'archive request',
        run: async () => {
          if (archiveResponsePromise) {
            await completeWithin(archiveResponsePromise, 20_000, 'Scheduler-first archive cleanup request did not finish');
          }
        }
      },
      {
        name: 'Agent and scheduled Automation',
        run: async () => {
          if (agentId) await deleteAgentForCleanup(page.request, agentId);
        }
      }
    ]);
  }
});
