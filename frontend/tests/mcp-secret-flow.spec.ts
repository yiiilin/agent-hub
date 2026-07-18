import { execFileSync } from 'node:child_process';
import { dirname } from 'node:path';
import { expect, test } from '@playwright/test';
import { composeArgs } from './e2e-compose';

function runtimeFileProbe(workDirRef: string, secret: string) {
  const runRoot = dirname(workDirRef);
  const script = `
set -eu
config="${runRoot}/codex/config.toml"
allowlist="${runRoot}/codex/mcp-allowlist.json"
mode="$(stat -c '%a' "$config")"
config_has_secret=no
allowlist_has_secret=no
allowlist_has_redaction=no
grep -F "$MCP_SECRET" "$config" >/dev/null && config_has_secret=yes
grep -F "$MCP_SECRET" "$allowlist" >/dev/null && allowlist_has_secret=yes
grep -F "********" "$allowlist" >/dev/null && allowlist_has_redaction=yes
printf '{"mode":"%s","configHasSecret":"%s","allowlistHasSecret":"%s","allowlistHasRedaction":"%s"}' "$mode" "$config_has_secret" "$allowlist_has_secret" "$allowlist_has_redaction"
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

test('MCP secrets are redacted in the console and injected into the per-run runtime config', async ({ page }) => {
  await page.goto('/login');
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
    await page.getByLabel('MCP allowlist').fill(JSON.stringify([
      {
        name: 'filesystem',
        command: 'fs',
        secrets: { API_TOKEN: '********' }
      }
    ], null, 2));
    await page.getByRole('button', { name: 'Save agent' }).click();
    await expect(page.getByText('MCP redacted secret cannot be saved without an existing value')).toBeVisible();

    await page.getByLabel('MCP allowlist').fill(JSON.stringify([
      {
        name: 'filesystem',
        command: 'fs',
        args: ['--root', '/workspace'],
        secrets: { API_TOKEN: secret }
      }
    ], null, 2));
    await page.getByRole('button', { name: 'Save agent' }).click();

    const mcpField = page.getByLabel('MCP allowlist');
    await expect(mcpField).toHaveValue(/"\*{8}"/);
    await expect(mcpField).not.toHaveValue(new RegExp(secret));

    const currentUrl = page.url();
    await page.reload();
    await page.goto(currentUrl);
    await page.getByRole('tab', { name: 'MCP' }).click();
    await expect(mcpField).toHaveValue(/"\*{8}"/);
    await expect(mcpField).not.toHaveValue(new RegExp(secret));

    const redactedAllowlist = JSON.parse(await mcpField.inputValue()) as Array<Record<string, unknown>>;
    redactedAllowlist[0].args = ['--root', '/workspace', '--readonly'];
    await mcpField.fill(JSON.stringify(redactedAllowlist, null, 2));
    await page.getByRole('button', { name: 'Save agent' }).click();
    await expect(mcpField).toHaveValue(/"\*{8}"/);

    await page.getByRole('tab', { name: 'Activity' }).click();
    await page.getByLabel('Message').fill('Run with preserved MCP secret from Playwright');
    await page.getByRole('button', { name: 'Start run' }).click();
    await expect(page.getByText('Fake Codex completed run')).toBeVisible({ timeout: 30_000 });
    await expect(page.locator('.status.completed')).toBeVisible({ timeout: 30_000 });

    const run = await page.evaluate(async () => {
      const firstRunId = document.querySelector('[data-run-id]')?.getAttribute('data-run-id');
      if (!firstRunId) return null;
      const response = await fetch(`/api/runs/${firstRunId}`);
      return response.ok ? response.json() : null;
    });
    expect(run?.work_dir_ref).toBeTruthy();
    workDirRef = run.work_dir_ref;

    const probe = runtimeFileProbe(run.work_dir_ref, secret);
    expect(probe).toEqual({
      mode: '600',
      configHasSecret: 'yes',
      allowlistHasSecret: 'no',
      allowlistHasRedaction: 'yes'
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
