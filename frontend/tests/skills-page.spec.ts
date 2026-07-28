import { execFileSync } from 'node:child_process';
import { expect, request, test, type Page } from '@playwright/test';
import type { Agent, AgentModelSettings } from '../src/api/client';
import { composeArgs, e2eComposeProject } from './e2e-compose';

const ownerId = '10000000-0000-4000-8000-000000000001';
const alphaId = '20000000-0000-4000-8000-000000000001';
const betaId = '20000000-0000-4000-8000-000000000002';
const agentId = '30000000-0000-4000-8000-000000000001';
const packageId = '60000000-0000-4000-8000-000000000001';
const now = '2026-07-11T08:00:00.000Z';

const automaticModelSettings: AgentModelSettings = {
  reasoning_effort: 'default',
  reasoning_summary: 'default',
  verbosity: 'default',
  context_window_tokens: null,
  auto_compact_token_limit: null,
  reasoning_summary_support: 'auto',
  service_tier: null,
  request_max_retries: null,
  stream_max_retries: null,
  stream_idle_timeout_ms: null,
  request_settings: { protocol: 'openai_responses' }
};

const skills = [
  {
    id: alphaId, owner_id: ownerId, name: 'Alpha review', description: 'Used review skill', content: 'Alpha content', revision: 2, content_checksum_sha256: 'a'.repeat(64),
    package: {
      id: packageId, format_version: 1, size_bytes: 2048, checksum_sha256: 'c'.repeat(64),
      files: [{ path: 'bin/review.sh', size_bytes: 512, checksum_sha256: 'd'.repeat(64), executable: true }]
    },
    created_at: '2026-07-01T08:00:00.000Z', updated_at: '2026-07-10T08:00:00.000Z'
  },
  { id: betaId, owner_id: ownerId, name: 'Beta notes', description: '', content: 'Beta content', revision: 1, content_checksum_sha256: 'b'.repeat(64), package: null, created_at: '2026-07-02T08:00:00.000Z', updated_at: '2026-07-09T08:00:00.000Z' }
];

const agents = [{
  id: agentId, name: 'Attached agent', instructions: 'Fixture', visibility: 'private', public_to: [], runtime_id: null,
  owner_id: ownerId, is_owner: true, can_manage: true, can_administer: true, can_invoke: true,
  model_selection: null, model_settings: automaticModelSettings, subagents: [],
  model_policy: {}, sandbox_policy: {}, managed_skill_ids: [alphaId], mcp_allowlist: [],
  created_at: now, updated_at: now
}];

async function mockSession(page: Page) {
  await page.route('**/api/auth/me', (route) => route.fulfill({ json: { id: ownerId, username: 'skills-fixture', email: 'skills-fixture@example.com', display_name: 'Skills Fixture', role: 'member' } }));
}

async function mockSkills(page: Page, options: { failSkills?: boolean; failAgents?: boolean; createGate?: Promise<void> } = {}) {
  await mockSession(page);
  let createCount = 0;
  await page.route('**/api/skills', async (route) => {
    if (route.request().method() === 'POST') {
      createCount += 1;
      await options.createGate;
      const body = route.request().postDataJSON() as { name: string; description: string; content: string };
      await route.fulfill({ json: { ...body, id: '20000000-0000-4000-8000-000000000003', owner_id: ownerId, revision: 1, content_checksum_sha256: 'c'.repeat(64), package: null, created_at: now, updated_at: now } });
      return;
    }
    if (options.failSkills) await route.fulfill({ status: 500, json: { error: 'private database detail' } });
    else await route.fulfill({ json: skills });
  });
  await page.route('**/api/agents', (route) => options.failAgents
    ? route.fulfill({ status: 500, json: { error: 'private agent detail' } })
    : route.fulfill({ json: agents }));
  await page.route(`**/api/skills/${alphaId}`, async (route) => {
    if (route.request().method() === 'PATCH') {
      const body = route.request().postDataJSON();
      await route.fulfill({ json: { ...skills[0], ...body, revision: skills[0].revision + 1, content_checksum_sha256: 'd'.repeat(64), updated_at: now } });
    } else if (route.request().method() === 'DELETE') await route.fulfill({ status: 204 });
    else await route.fulfill({ json: skills[0] });
  });
  await page.route(`**/api/skills/${betaId}`, (route) => route.fulfill({ json: skills[1] }));
  return { createCount: () => createCount };
}

test('skills list has exact routes, usage filters, search, and explicit sorting', async ({ page }) => {
  await mockSkills(page);
  await page.goto('/skills');
  await expect(page.getByRole('heading', { name: 'Skills' })).toBeVisible();
  await expect(page.locator('.skill-list-row').filter({ has: page.getByRole('link', { name: /Alpha review/ }) })).toContainText('1 agent');
  await page.getByRole('button', { name: 'Unused' }).click();
  await expect(page.getByRole('link', { name: /Beta notes/ })).toBeVisible();
  await expect(page.getByRole('link', { name: /Alpha review/ })).toHaveCount(0);
  await page.getByRole('button', { name: 'All' }).click();
  await page.getByRole('searchbox', { name: 'Search skills' }).fill('alpha');
  await expect(page.getByRole('link', { name: /Alpha review/ })).toBeVisible();
  await page.getByRole('searchbox', { name: 'Search skills' }).fill('');
  await page.getByLabel('Sort skills').selectOption('name-desc');
  await expect(page.locator('.skill-list-row').first()).toContainText('Beta notes');
  await page.getByRole('link', { name: /Alpha review/ }).click();
  await expect(page).toHaveURL(`/skills/${alphaId}`);
  await expect(page.getByRole('heading', { name: 'Alpha review' })).toBeVisible();
  await page.reload();
  await expect(page.getByRole('heading', { name: 'Alpha review' })).toBeVisible();
  await page.goto(`/skills/${alphaId}/extra`);
  await expect(page.getByText('Page not found')).toBeVisible();
  await expect(page.getByRole('heading', { name: 'Skills' })).toHaveCount(0);
});

test('skills loading failures are retryable and agent failure does not discard skills', async ({ page }) => {
  await mockSkills(page, { failSkills: true });
  await page.goto('/skills');
  await expect(page.getByRole('alert')).toContainText('Unable to load skills');
  await expect(page.getByRole('button', { name: 'Retry' })).toBeVisible();
  await page.unroute('**/api/skills');
  await page.route('**/api/skills', (route) => route.fulfill({ json: skills }));
  await page.getByRole('button', { name: 'Retry' }).click();
  await expect(page.getByRole('link', { name: /Alpha review/ })).toBeVisible();
  await page.unroute('**/api/agents');
  await page.route('**/api/agents', (route) => route.fulfill({ status: 500, json: { error: 'secret detail' } }));
  await page.reload();
  await expect(page.getByRole('link', { name: /Alpha review/ })).toBeVisible();
  await expect(page.getByRole('alert')).toContainText('Usage information is unavailable');
  await expect(page.getByText('secret detail')).toHaveCount(0);

  await page.route('**/api/skills/20000000-0000-4000-8000-000000000099', (route) => route.fulfill({ status: 404, json: { error: 'skill not found' } }));
  await page.goto('/skills/20000000-0000-4000-8000-000000000099');
  await expect(page.getByRole('heading', { name: 'Skill not found' })).toBeVisible();
});

test('skill package metadata and files are visible and package removal keeps the Skill', async ({ page }) => {
  await mockSkills(page);
  let packageDeleteCount = 0;
  await page.route(`**/api/skills/${alphaId}/package`, async (route) => {
    expect(route.request().method()).toBe('DELETE');
    packageDeleteCount += 1;
    await route.fulfill({ json: { ...skills[0], revision: 3, package: null, updated_at: now } });
  });
  await page.goto(`/skills/${alphaId}`);

  const packagePanel = page.locator('.skill-package-panel');
  await expect(packagePanel.getByRole('heading', { name: 'Skill package' })).toBeVisible();
  await expect(packagePanel).toContainText(packageId);
  await expect(packagePanel).toContainText('Format version1');
  await expect(packagePanel).toContainText('2 KB');
  await expect(packagePanel).toContainText('c'.repeat(64));
  await expect(packagePanel.getByRole('list', { name: 'Package files' })).toContainText('bin/review.sh');
  await expect(packagePanel).toContainText('Executable');

  const confirmation = page.waitForEvent('dialog');
  const removeClick = packagePanel.getByRole('button', { name: 'Remove package' }).click();
  const dialog = await confirmation;
  expect(dialog.message()).toBe('Remove the attached package? The Skill will be kept.');
  await dialog.accept();
  await removeClick;

  await expect(packagePanel).toContainText('No package attached.');
  await expect(packagePanel.getByRole('status')).toHaveText('Skill package removed.');
  await expect(page.getByRole('heading', { name: 'Alpha review' })).toBeVisible();
  await expect(page).toHaveURL(`/skills/${alphaId}`);
  expect(packageDeleteCount).toBe(1);
});

test('directory package upload preserves nested paths and sends ordered multipart fields', async ({ page }) => {
  await mockSkills(page);
  let multipartBody = '';
  let contentType = '';
  await page.route(`**/api/skills/${betaId}/package`, async (route) => {
    expect(route.request().method()).toBe('PUT');
    multipartBody = route.request().postData() ?? '';
    contentType = route.request().headers()['content-type'] ?? '';
    await route.fulfill({ json: {
      ...skills[1],
      name: 'Uploaded review',
      description: 'Uploaded package',
      content: '# Uploaded',
      revision: 2,
      content_checksum_sha256: 'e'.repeat(64),
      package: {
        id: packageId,
        format_version: 1,
        size_bytes: 4096,
        checksum_sha256: 'f'.repeat(64),
        files: [
          { path: 'scripts/check.sh', size_bytes: 17, checksum_sha256: '1'.repeat(64), executable: false },
          { path: 'references/guide.md', size_bytes: 7, checksum_sha256: '2'.repeat(64), executable: false }
        ]
      },
      updated_at: now
    } });
  });
  await page.goto(`/skills/${betaId}`);
  await page.getByRole('button', { name: 'Upload package' }).click();

  const packageDialog = page.getByRole('dialog', { name: 'Upload package' });
  const directoryInput = packageDialog.locator('input[data-package-input="directory"]');
  await expect(directoryInput).toHaveJSProperty('webkitdirectory', true);
  await expect(directoryInput).toHaveAttribute('multiple', '');
  await directoryInput.evaluate((input: HTMLInputElement) => {
    const data = new DataTransfer();
    const entries = [
      ['SKILL.md', 'review-kit/SKILL.md', '---\nname: Uploaded review\ndescription: Uploaded package\n---\n# Uploaded'],
      ['check.sh', 'review-kit/scripts/check.sh', '#!/bin/sh\necho ok'],
      ['guide.md', 'review-kit/references/guide.md', '# Guide']
    ];
    for (const [name, relativePath, body] of entries) {
      const file = new File([body], name, { type: 'text/plain' });
      Object.defineProperty(file, 'webkitRelativePath', { value: relativePath });
      data.items.add(file);
    }
    Object.defineProperty(input, 'files', { configurable: true, value: data.files });
    input.dispatchEvent(new Event('change', { bubbles: true }));
  });

  const selection = packageDialog.getByRole('region', { name: 'Selected files' });
  await expect(selection).toContainText('SKILL.md');
  await expect(selection).toContainText('scripts/check.sh');
  await expect(selection).toContainText('references/guide.md');
  await expect(selection).not.toContainText('review-kit/');
  await packageDialog.getByRole('button', { name: 'Upload package' }).click();

  expect(contentType).toMatch(/^multipart\/form-data; boundary=/);
  expect(contentType).not.toContain('application/json');
  expect(multipartBody).toContain('{"paths":["SKILL.md","scripts/check.sh","references/guide.md"]}');
  expect(multipartBody.indexOf('name="manifest"')).toBeLessThan(multipartBody.indexOf('name="file-0"'));
  expect(multipartBody.indexOf('name="file-0"')).toBeLessThan(multipartBody.indexOf('name="file-1"'));
  expect(multipartBody.indexOf('name="file-1"')).toBeLessThan(multipartBody.indexOf('name="file-2"'));
  await expect(page.getByRole('heading', { name: 'Uploaded review' })).toBeVisible();
  await expect(page.locator('.skill-package-panel').getByRole('list', { name: 'Package files' })).toContainText('references/guide.md');
  await expect(page.locator('.skill-package-panel').getByRole('status')).toHaveText('Skill package uploaded.');
});

test('bulk delete sends one all-or-nothing request for item and visible selections', async ({ page }) => {
  await mockSkills(page);
  let deleteCount = 0;
  let deleteBody: { skill_ids: string[] } | undefined;
  await page.route('**/api/skills', async (route) => {
    if (route.request().method() !== 'DELETE') {
      await route.fallback();
      return;
    }
    deleteCount += 1;
    deleteBody = route.request().postDataJSON() as { skill_ids: string[] };
    await route.fulfill({ status: 404, json: { error: 'one selected skill was not found' } });
  });
  await page.goto('/skills');

  const alphaCheckbox = page.getByRole('checkbox', { name: 'Select Alpha review' });
  const betaCheckbox = page.getByRole('checkbox', { name: 'Select Beta notes' });
  await betaCheckbox.check();
  await page.getByRole('searchbox', { name: 'Search skills' }).fill('alpha');
  await page.getByRole('checkbox', { name: 'Select visible skills' }).check();
  await expect(alphaCheckbox).toBeChecked();
  await page.getByRole('searchbox', { name: 'Search skills' }).fill('');
  await expect(page.getByRole('checkbox', { name: 'Select visible skills' })).toBeChecked();

  const deleteResponse = page.waitForResponse((response) => response.request().method() === 'DELETE'
    && new URL(response.url()).pathname === '/api/skills');
  const confirmDialog = page.waitForEvent('dialog');
  const deleteClick = page.getByRole('button', { name: 'Delete selected' }).click();
  const dialog = await confirmDialog;
  expect(dialog.message()).toBe('Delete 2 selected skills? This cannot be undone.');
  await dialog.accept();
  await deleteClick;
  expect((await deleteResponse).status()).toBe(404);

  expect(deleteCount).toBe(1);
  expect(deleteBody).toBeDefined();
  expect(deleteBody!.skill_ids).toHaveLength(2);
  expect(new Set(deleteBody!.skill_ids)).toEqual(new Set([alphaId, betaId]));
  await expect(page.getByRole('alert')).toContainText('could not be deleted');
  await expect(page.getByRole('link', { name: /Alpha review/ })).toBeVisible();
  await expect(page.getByRole('link', { name: /Beta notes/ })).toBeVisible();
  await expect(alphaCheckbox).toBeChecked();
  await expect(betaCheckbox).toBeChecked();
});

test('skills ignore late responses after unmount and fit a 390px localized viewport', async ({ page }) => {
  const pageErrors: string[] = [];
  const consoleErrors: string[] = [];
  let releaseSkills!: () => void;
  const skillsGate = new Promise<void>((resolve) => { releaseSkills = resolve; });
  await mockSession(page);
  await page.route('**/api/skills', async (route) => { await skillsGate; await route.fulfill({ json: skills }); });
  await page.route(`**/api/skills/${alphaId}`, (route) => route.fulfill({ json: skills[0] }));
  await page.route('**/api/agents', (route) => route.fulfill({ json: agents }));
  await page.route('**/api/users', (route) => route.fulfill({ json: [] }));
  await page.setViewportSize({ width: 390, height: 844 });
  await page.goto('/skills');
  await page.getByRole('button', { name: 'Primary navigation', exact: true }).click();
  await page.getByRole('button', { name: 'Agents', exact: true }).click();
  releaseSkills();
  await expect(page.getByText('Create Agent', { exact: true })).toBeVisible();

  page.on('pageerror', (error) => pageErrors.push(error.message));
  page.on('console', (message) => { if (message.type() === 'error') consoleErrors.push(message.text()); });
  await page.goto('/skills');
  await expect(page.getByRole('link', { name: /Alpha review/ })).toBeVisible();
  await page.getByLabel('Language').selectOption('zh-CN');
  await expect(page.getByRole('heading', { name: '技能' })).toBeVisible();
  await page.getByRole('link', { name: /Alpha review/ }).click();
  await expect(page.getByRole('heading', { name: '技能包' })).toBeVisible();
  await page.getByRole('button', { name: '替换技能包' }).click();
  await expect(page.getByRole('dialog', { name: '替换技能包' })).toBeVisible();
  const dimensions = await page.evaluate(() => ({ scrollWidth: document.documentElement.scrollWidth, innerWidth: window.innerWidth }));
  expect(dimensions.scrollWidth).toBeLessThanOrEqual(dimensions.innerWidth);
  await expect(page.getByRole('complementary', { name: '主导航' })).toBeVisible();
  expect(pageErrors).toEqual([]);
  expect(consoleErrors).toEqual([]);
});

test('create modal traps focus, closes with Escape, restores focus, and prevents duplicate submit', async ({ page }) => {
  let releaseCreate!: () => void;
  const createGate = new Promise<void>((resolve) => { releaseCreate = resolve; });
  const fixture = await mockSkills(page, { createGate });
  await page.goto('/skills');
  const opener = page.getByRole('button', { name: 'Create skill' });
  await opener.click();
  const dialog = page.getByRole('dialog', { name: 'Create skill' });
  const nameInput = dialog.getByRole('textbox', { name: 'Name', exact: true });
  const closeButton = dialog.getByRole('button', { name: 'Close', exact: true });
  await expect(nameInput).toBeFocused();
  await nameInput.press('Shift+Tab');
  await expect(closeButton).toBeFocused();
  await closeButton.press('Shift+Tab');
  await expect(dialog.getByRole('button', { name: 'Create skill' })).toBeFocused();
  await page.keyboard.press('Escape');
  await expect(dialog).toHaveCount(0);
  await expect(opener).toBeFocused();
  await opener.click();
  await dialog.getByRole('textbox', { name: 'Name', exact: true }).fill('Created once');
  await dialog.getByLabel('Description').fill('Description');
  await dialog.getByRole('textbox', { name: 'Content', exact: true }).fill('Content');
  const submit = dialog.locator('button[type="submit"]');
  await submit.dblclick();
  await expect(submit).toBeDisabled();
  expect(fixture.createCount()).toBe(1);
  releaseCreate();
  await expect(page).toHaveURL(/\/skills\/20000000-0000-4000-8000-000000000003$/);
});

test('dirty skill edits protect list, agent-link, and browser history navigation', async ({ page }) => {
  await mockSkills(page);
  await page.goto('/skills');
  await page.getByRole('link', { name: /Alpha review/ }).click();
  await page.getByRole('textbox', { name: 'Content', exact: true }).fill('Unsaved content');
  page.once('dialog', (dialog) => dialog.dismiss());
  await page.getByRole('button', { name: 'Skills', exact: true }).click();
  await expect(page).toHaveURL(`/skills/${alphaId}`);
  page.once('dialog', (dialog) => dialog.dismiss());
  await page.getByRole('link', { name: 'Attached agent' }).click();
  await expect(page).toHaveURL(`/skills/${alphaId}`);
  page.once('dialog', (dialog) => dialog.dismiss());
  await page.goBack({ waitUntil: 'commit' }).catch(() => undefined);
  await expect(page).toHaveURL(`/skills/${alphaId}`);
  page.once('dialog', (dialog) => dialog.accept());
  await page.goBack();
  await expect(page).toHaveURL('/skills');

  await page.goForward();
  await page.getByRole('textbox', { name: 'Content', exact: true }).fill('Second unsaved content');
  page.once('dialog', (dialog) => dialog.accept());
  await page.getByRole('button', { name: 'Skills', exact: true }).click();
  await expect(page).toHaveURL('/skills');
  await page.goBack();
  await page.getByRole('textbox', { name: 'Content', exact: true }).fill('Third unsaved content');
  page.once('dialog', (dialog) => dialog.dismiss());
  await page.goForward({ waitUntil: 'commit' }).catch(() => undefined);
  await expect(page).toHaveURL(`/skills/${alphaId}`);
  page.once('dialog', (dialog) => dialog.accept());
  await page.goForward();
  await expect(page).toHaveURL('/skills');
});

test('dirty skill logout confirms before mutating the real session', async ({ page }) => {
  const suffix = `${Date.now()}-${Math.random().toString(16).slice(2)}`;
  const prefix = `skills-logout-${suffix}`;
  const email = `${prefix}@example.com`;
  let cleanupUser = false;
  try {
    await page.goto('/login');
    await page.getByLabel('Email').fill(email);
    await page.getByRole('button', { name: 'Sign in with Mock OIDC' }).click();
    await expect(page.getByText(email)).toBeVisible();
    cleanupUser = true;
    const created = await page.request.post('/api/skills', { data: { name: `${prefix}-name`, description: 'Original', content: 'Original content' } });
    expect(created.ok()).toBeTruthy();
    const skill = await created.json() as { id: string };
    await page.goto(`/skills/${skill.id}`);
    await page.getByRole('textbox', { name: 'Content', exact: true }).fill('Save after cancelled logout');

    let logoutRequests = 0;
    page.on('request', (request) => {
      if (request.method() === 'POST' && new URL(request.url()).pathname === '/api/auth/logout') logoutRequests += 1;
    });
    page.once('dialog', (dialog) => dialog.dismiss());
    await page.getByRole('button', { name: 'Log out' }).click();
    await expect(page).toHaveURL(`/skills/${skill.id}`);
    expect(logoutRequests).toBe(0);
    expect((await page.request.get('/api/auth/me')).status()).toBe(200);
    await page.getByRole('button', { name: 'Save skill' }).click();
    await expect(page.getByRole('status')).toHaveText('Changes saved');

    await page.getByRole('textbox', { name: 'Content', exact: true }).fill('Discard on confirmed logout');
    page.once('dialog', (dialog) => dialog.accept());
    const logoutResponse = page.waitForResponse((response) => response.request().method() === 'POST'
      && new URL(response.url()).pathname === '/api/auth/logout');
    await page.getByRole('button', { name: 'Log out' }).click();
    expect((await logoutResponse).status()).toBe(204);
    await expect(page).toHaveURL('/login');
    expect((await page.request.get('/api/auth/me')).status()).toBe(401);
  } finally {
    if (cleanupUser) {
      const deleted = runSql(`DELETE FROM users WHERE email = '${email}' RETURNING email;`);
      expect(deleted.split('\n')).toContain(email);
    }
  }
});

test('pending skill save locks the draft and detail actions until the response is applied', async ({ page }) => {
  await mockSkills(page);
  let releasePatch!: () => void;
  const patchGate = new Promise<void>((resolve) => { releasePatch = resolve; });
  await page.route(`**/api/skills/${alphaId}`, async (route) => {
    if (route.request().method() !== 'PATCH') { await route.fallback(); return; }
    const body = route.request().postDataJSON() as { name: string; description: string; content: string };
    await patchGate;
    await route.fulfill({ json: { ...skills[0], ...body, revision: skills[0].revision + 1, content_checksum_sha256: 'd'.repeat(64), updated_at: now } });
  });
  await page.goto(`/skills/${alphaId}`);
  await page.getByLabel('Description').fill('Submitted description');
  await page.getByRole('button', { name: 'Save skill' }).click();

  await expect(page.getByLabel('Name')).toBeDisabled();
  await expect(page.getByLabel('Description')).toBeDisabled();
  await expect(page.locator('.markdown-editor')).toHaveClass(/disabled/);
  await expect(page.getByRole('button', { name: 'Back to skills' })).toBeDisabled();
  await expect(page.getByRole('button', { name: 'Delete skill' })).toBeDisabled();
  await expect(page.getByRole('link', { name: 'Attached agent' })).toHaveAttribute('aria-disabled', 'true');
  releasePatch();
  await expect(page.getByRole('status')).toHaveText('Changes saved');
  await expect(page.getByLabel('Description')).toBeEnabled();
  await expect(page.getByLabel('Description')).toHaveValue('Submitted description');
});

function runSql(sql: string) {
  return execFileSync('docker', [...composeArgs(e2eComposeProject()), 'exec', '-T', 'postgres', 'psql', '-U', 'agent_hub', '-d', 'agent_hub', '-v', 'ON_ERROR_STOP=1', '-Atc', sql], { cwd: process.cwd(), encoding: 'utf8' }).trim();
}

test('Skills Chromium real CRUD remains owner-scoped', async ({ page }) => {
  const suffix = `${Date.now()}-${Math.random().toString(16).slice(2)}`;
  const prefix = `skills-crud-${suffix}`;
  const email = `${prefix}@example.com`;
  let cleanupUser = false;
  try {
    await page.goto('/login');
    await page.getByLabel('Email').fill(email);
    await page.getByRole('button', { name: 'Sign in with Mock OIDC' }).click();
    await expect(page.getByText(email)).toBeVisible();
    cleanupUser = true;
    await page.goto('/skills');
    await page.getByRole('button', { name: 'Create skill' }).click();
    await page.getByRole('dialog').getByLabel('Name').fill(`${prefix}-name`);
    await page.getByRole('dialog').getByLabel('Description').fill(`${prefix}-description`);
    await page.getByRole('dialog').getByRole('textbox', { name: 'Content', exact: true }).fill(`${prefix}-content`);
    await page.getByRole('dialog').getByRole('button', { name: 'Create skill' }).click();
    await expect(page).toHaveURL(/\/skills\/[0-9a-f-]{36}$/);
    const skillId = new URL(page.url()).pathname.split('/').at(-1)!;
    await expect(page.getByRole('heading', { name: `${prefix}-name` })).toBeVisible();
    await page.getByLabel('Description').fill(`${prefix}-updated`);
    await page.getByRole('button', { name: 'Save skill' }).click();
    await expect(page.getByText(`${prefix}-updated`)).toBeVisible();
    await page.reload();
    await expect(page.getByLabel('Description')).toHaveValue(`${prefix}-updated`);
    const deleteResponse = page.waitForResponse((response) => response.request().method() === 'DELETE'
      && new URL(response.url()).pathname === `/api/skills/${skillId}`);
    const confirmDialog = page.waitForEvent('dialog');
    const deleteClick = page.getByRole('button', { name: 'Delete skill' }).click();
    const dialog = await confirmDialog;
    expect(dialog.message()).toBe(`Delete skill "${prefix}-name"? This cannot be undone.`);
    await dialog.accept();
    await deleteClick;
    expect((await deleteResponse).status()).toBe(204);
    await expect(page).toHaveURL('/skills');
    await expect(page.getByText(`${prefix}-name`)).toHaveCount(0);
  } finally {
    if (cleanupUser) {
      const deleted = runSql(`DELETE FROM users WHERE email = '${email}' RETURNING email;`);
      expect(deleted.split('\n')).toContain(email);
    }
  }
});

test('skills and managed assignments enforce the real owner boundary', async ({ baseURL }) => {
  const suffix = `${Date.now()}-${Math.random().toString(16).slice(2)}`;
  const ownerEmail = `skills-owner-${suffix}@example.com`;
  const outsiderEmail = `skills-outsider-${suffix}@example.com`;
  const owner = await request.newContext({ baseURL });
  const outsider = await request.newContext({ baseURL });
  let ownerCreated = false;
  let outsiderCreated = false;
  try {
    expect((await owner.get(`/api/auth/oidc/mock/start?email=${encodeURIComponent(ownerEmail)}&sub=skills-owner-${suffix}`)).ok()).toBeTruthy();
    ownerCreated = true;
    expect((await outsider.get(`/api/auth/oidc/mock/start?email=${encodeURIComponent(outsiderEmail)}&sub=skills-outsider-${suffix}`)).ok()).toBeTruthy();
    outsiderCreated = true;

    const skillResponse = await owner.post('/api/skills', { data: { name: `Owner skill ${suffix}`, description: 'Owner only', content: 'Owner content' } });
    expect(skillResponse.ok()).toBeTruthy();
    const skill = await skillResponse.json() as { id: string };
    const agentResponse = await outsider.post('/api/agents', { data: {
      name: `Outsider agent ${suffix}`,
      instructions: 'Owner boundary fixture',
      visibility: 'private',
      public_to: [],
      model_selection: null,
      model_settings: automaticModelSettings,
      subagents: []
    } });
    expect(agentResponse.ok()).toBeTruthy();
    const agent = await agentResponse.json() as Agent;

    const outsiderSkills = await (await outsider.get('/api/skills')).json() as Array<{ id: string }>;
    expect(outsiderSkills.map((item) => item.id)).not.toContain(skill.id);
    expect((await outsider.get(`/api/skills/${skill.id}`)).status()).toBe(404);
    expect((await outsider.patch(`/api/skills/${skill.id}`, { data: { name: 'Foreign update', description: '', content: 'Foreign content' } })).status()).toBe(404);
    expect((await outsider.delete(`/api/skills/${skill.id}`)).status()).toBe(404);

    const bindResponse = await outsider.patch(`/api/agents/${agent.id}`, { data: {
      name: agent.name,
      instructions: agent.instructions,
      visibility: agent.visibility,
      public_to: agent.public_to,
      runtime_id: agent.runtime_id,
      model_selection: agent.model_selection,
      model_settings: agent.model_settings,
      subagents: agent.subagents,
      sandbox_policy: agent.sandbox_policy,
      managed_skill_ids: [skill.id],
      mcp_allowlist: agent.mcp_allowlist
    } });
    expect(bindResponse.status()).toBe(404);
    const unchangedAgent = await (await outsider.get(`/api/agents/${agent.id}`)).json() as { managed_skill_ids: string[] };
    expect(unchangedAgent.managed_skill_ids).toEqual([]);
  } finally {
    await owner.dispose();
    await outsider.dispose();
    if (ownerCreated) {
      const deleted = runSql(`DELETE FROM users WHERE email = '${ownerEmail}' RETURNING email;`);
      expect(deleted.split('\n')).toContain(ownerEmail);
    }
    if (outsiderCreated) {
      const deleted = runSql(`DELETE FROM users WHERE email = '${outsiderEmail}' RETURNING email;`);
      expect(deleted.split('\n')).toContain(outsiderEmail);
    }
  }
});
