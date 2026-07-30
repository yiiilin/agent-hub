import { execFileSync } from 'node:child_process';
import { dirname } from 'node:path';
import { expect, test } from '@playwright/test';
import { selectLocalPasswordLogin } from './authentication-helpers';
import { composeArgs } from './e2e-compose';

function runtimeMcpSecretProbe(workDirRef: string, secret: string) {
  const runRoot = dirname(workDirRef);
  const script = `
set -eu
agent_dir="${runRoot}/engine-state/.pi/agent"
models="$agent_dir/models.json"
test -f "$models"
models_mode="$(stat -c '%a' "$models")"
engine_state_has_secret=no
mcp_allowlist_materialized=no
grep -R -F "$MCP_SECRET" "$agent_dir" >/dev/null 2>&1 && engine_state_has_secret=yes
test -e "$agent_dir/mcp-allowlist.json" && mcp_allowlist_materialized=yes
printf '{"modelsMode":"%s","engineStateHasSecret":"%s","mcpAllowlistMaterialized":"%s"}' "$models_mode" "$engine_state_has_secret" "$mcp_allowlist_materialized"
`;
  return JSON.parse(execFileSync('docker', [
    ...composeArgs(),
    'exec',
    '-T',
    '-e',
    `MCP_SECRET=${secret}`,
    'runtime',
    'sh',
    '-lc',
    script
  ], { cwd: process.cwd(), encoding: 'utf8' }));
}

function runtimeSessionRootExists(workDirRef: string) {
  const sessionRoot = dirname(workDirRef);
  const output = execFileSync('docker', [
    ...composeArgs(),
    'exec',
    '-T',
    '-e',
    `SESSION_ROOT=${sessionRoot}`,
    'runtime',
    'sh',
    '-lc',
    'test -e "$SESSION_ROOT" && printf yes || printf no'
  ], { cwd: process.cwd(), encoding: 'utf8' }).trim();
  return output === 'yes';
}

test('MCP secrets are redacted in the console and excluded from Pi runtime materialization', async ({ page }) => {
  await page.goto('/login');
  await selectLocalPasswordLogin(page);
  await page.getByLabel('Email').fill('admin@example.com');
  await page.getByLabel('Password').fill('admin123');
  await page.getByRole('button', { name: 'Sign in', exact: true }).click();
  await expect(page.getByText('admin@example.com')).toBeVisible();

  await page.goto('/agents');
  const nonce = Date.now();
  const agentName = `MCP Secret Agent ${nonce}`;
  const secret = `mcp-secret-${nonce}`;
  let agentId: string | null = null;
  let workDirRef: string | null = null;
  try {
    await page.locator('.agents-header').getByRole('button', { name: 'Create Agent' }).click();
    const createAgentDialog = page.getByRole('dialog', { name: 'Create Agent' });
    await createAgentDialog.getByLabel('Name', { exact: true }).fill(agentName);
    await createAgentDialog.getByLabel('Instructions').fill('Validate MCP secret redaction and runtime injection.');
    const createAgentResponsePromise = page.waitForResponse((response) => response.request().method() === 'POST'
      && new URL(response.url()).pathname === '/api/agents');
    await createAgentDialog.getByRole('button', { name: 'Create agent' }).click();
    const createAgentResponse = await createAgentResponsePromise;
    expect(createAgentResponse.ok()).toBeTruthy();
    agentId = (await createAgentResponse.json() as { id: string }).id;
    expect(agentId).toBeTruthy();
    await expect(page.getByRole('heading', { name: agentName, level: 1 })).toBeVisible();

    await page.getByRole('tab', { name: 'Access' }).click();
    const runtimeSelect = page.getByLabel('Runtime binding');
    const runtimeValue = await runtimeSelect.locator('option').nth(1).getAttribute('value');
    if (runtimeValue) await runtimeSelect.selectOption(runtimeValue);
    if (runtimeValue) await page.getByRole('tabpanel', { name: 'Access' }).getByRole('button', { name: 'Save agent' }).click();
    await page.getByRole('tab', { name: 'MCP' }).click();
    const mcpPanel = page.getByRole('tabpanel', { name: 'MCP' });
    const mcpTable = mcpPanel.getByRole('table', { name: 'MCP allowlist' });
    await mcpPanel.getByRole('button', { name: 'Add MCP entry' }).click();
    const addMcpDialog = page.getByRole('dialog', { name: 'Add MCP entry' });
    await addMcpDialog.getByLabel('Name', { exact: true }).fill('filesystem');
    await addMcpDialog.getByLabel('Command').fill('fs');
    await addMcpDialog.getByLabel('Arguments').fill('--root\n/workspace');
    await addMcpDialog.getByRole('button', { name: 'Add secret' }).click();
    await addMcpDialog.getByLabel('Secret name 1').fill('API_TOKEN');
    await addMcpDialog.getByLabel('Secret value 1').fill('********');
    await addMcpDialog.getByRole('button', { name: 'Save changes' }).click();
    await expect(addMcpDialog.getByRole('alert')).toContainText('Enter secret values again after renaming an MCP entry or secret.');

    await addMcpDialog.getByLabel('Secret value 1').fill(secret);
    await addMcpDialog.getByRole('button', { name: 'Save changes' }).click();
    await expect(addMcpDialog).toHaveCount(0);
    await expect(mcpTable).toContainText('filesystem');
    await expect(mcpTable).toContainText('API_TOKEN=********');
    await expect(mcpTable).not.toContainText(secret);

    const currentUrl = page.url();
    await page.reload();
    await page.goto(currentUrl);
    await page.getByRole('tab', { name: 'MCP' }).click();
    await expect(mcpTable).toContainText('API_TOKEN=********');
    await expect(mcpTable).not.toContainText(secret);

    await mcpTable.getByRole('button', { name: 'Edit MCP entry: filesystem' }).click();
    const editMcpDialog = page.getByRole('dialog', { name: 'Edit MCP entry: filesystem' });
    await expect(editMcpDialog.getByLabel('Secret value 1')).toHaveValue('********');
    await editMcpDialog.getByLabel('Arguments').fill('--root\n/workspace\n--readonly');
    await editMcpDialog.getByRole('button', { name: 'Save changes' }).click();
    await expect(editMcpDialog).toHaveCount(0);
    await expect(mcpTable).toContainText('--root /workspace --readonly');
    await expect(mcpTable).toContainText('API_TOKEN=********');

    const createdAgentId = agentId;
    if (!createdAgentId) throw new Error('created Agent id is missing');
    const runResponse = await page.request.post(`/api/agents/${createdAgentId}/runs`, {
      data: { message: 'Run with preserved MCP secret from Playwright', hub_session_id: null, parent_run_id: null }
    });
    expect(runResponse.ok()).toBeTruthy();
    const createdRun = await runResponse.json() as { id: string };

    await page.getByRole('tab', { name: 'Activity' }).click();
    await expect(page.locator(`[data-run-id="${createdRun.id}"]`)).toBeVisible({ timeout: 30_000 });
    await expect(page.getByText('completed run')).toBeVisible({ timeout: 30_000 });
    await expect(page.locator('.status.completed')).toBeVisible({ timeout: 30_000 });
    const completedRunResponse = await page.request.get(`/api/runs/${createdRun.id}`);
    expect(completedRunResponse.ok()).toBeTruthy();
    const run = await completedRunResponse.json() as { id: string; work_dir_ref: string | null };
    const runWorkDirRef = run.work_dir_ref;
    expect(runWorkDirRef).toBeTruthy();
    if (!runWorkDirRef) throw new Error('run work directory is missing');
    workDirRef = runWorkDirRef;

    const probe = runtimeMcpSecretProbe(runWorkDirRef, secret);
    expect(probe).toEqual({
      modelsMode: '600',
      engineStateHasSecret: 'no',
      mcpAllowlistMaterialized: 'no'
    });
    const events = await page.evaluate(async (runId) => {
      const response = await fetch(`/api/runs/${runId}/events`);
      if (!response.ok) throw new Error(`Failed to load run events: ${response.status}`);
      return response.json();
    }, run.id);
    expect(JSON.stringify(events)).not.toContain(secret);
    await expect(page.locator('body')).not.toContainText(secret);
  } finally {
    if (agentId) {
      const response = await page.request.delete(`/api/agents/${agentId}`, { timeout: 5_000 });
      expect([204, 404]).toContain(response.status());
    }
    if (workDirRef) {
      const sessionWorkDirRef = workDirRef;
      await expect.poll(
        () => runtimeSessionRootExists(sessionWorkDirRef),
        { timeout: 10_000, message: 'Runtime Session root should be removed after Agent deletion' }
      ).toBe(false);
    }
  }
});
