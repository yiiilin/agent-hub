import assert from 'node:assert/strict';
import { poll } from '../../support/api.mjs';
import { withBrowser } from '../../support/browser.mjs';

async function assertNoHorizontalOverflow(page, label) {
  await page.waitForTimeout(100);
  const overflow = await page.evaluate(() => (
    document.documentElement.scrollWidth - document.documentElement.clientWidth
  ));
  assert.ok(overflow <= 1, `${label} horizontal overflow: ${overflow}px`);
}

async function closeOrRedactMcpDialog(page, dialog) {
  if (await dialog.count() === 0) return;
  await dialog.getByLabel(/Secret value/).evaluateAll((elements) => {
    for (const element of elements) {
      element.value = '[redacted]';
      element.dispatchEvent(new Event('input', { bubbles: true }));
    }
  }).catch(() => undefined);
  await dialog.getByRole('button', { name: 'Cancel' }).click({ timeout: 2_000 })
    .catch(() => page.keyboard.press('Escape'));
}

export default async function agentSkillMcpBrowserScenario(scenarioContext) {
  await withBrowser(scenarioContext, {
    allowedHttpErrors: [
      { method: 'GET', pathname: '/api/auth/me', status: 401, times: 1 }
    ]
  }, async ({ page, context, request, browserErrors }) => {
    const allowedNoContentAborts = new Set();
    await page.goto('/login', { waitUntil: 'domcontentloaded' });
    await page.getByLabel('Email').fill('admin@example.com');
    await page.getByLabel('Password').fill('admin123');
    await page.getByRole('button', { name: 'Sign in', exact: true }).click();
    await page.getByText('admin@example.com', { exact: true }).waitFor();

    const skillName = scenarioContext.unique('QA Browser Skill');
    const skillMarkdown = '# Browser Skill\n\n- Keep history\n- Check risks';
    await page.goto('/skills', { waitUntil: 'domcontentloaded' });
    await page.getByRole('heading', { name: 'Skills', level: 1 }).waitFor();
    await page.locator('.skills-page > .page-header').getByRole('button', { name: 'Create skill' }).click();
    const createSkillDialog = page.getByRole('dialog', { name: 'Create Skill' });
    await createSkillDialog.getByLabel('Name', { exact: true }).fill(skillName);
    await createSkillDialog.getByLabel('Description').fill('Browser-managed Skill fixture.');
    await createSkillDialog.getByRole('radio', { name: 'Source mode' }).click();
    await createSkillDialog.locator('.cm-content').fill(skillMarkdown);
    await createSkillDialog.getByRole('radio', { name: 'Rich text' }).click();
    const richSkillContent = createSkillDialog.getByRole('textbox', { name: 'Content' });
    assert.ok((await richSkillContent.innerText()).includes('Keep history'));
    const createSkillResponsePromise = page.waitForResponse((response) => (
      response.request().method() === 'POST'
      && new URL(response.url()).pathname === '/api/skills'
    ));
    await createSkillDialog.getByRole('button', { name: 'Create skill', exact: true }).click();
    const createSkillResponse = await createSkillResponsePromise;
    assert.equal(createSkillResponse.ok(), true, 'Skill create must succeed');
    const skill = await createSkillResponse.json();
    await page.waitForURL((url) => url.pathname === `/skills/${skill.id}`);
    await page.getByRole('heading', { name: skillName, level: 1 }).waitFor();
    await page.getByLabel('Description').fill('Browser-managed Skill fixture updated.');
    await page.getByRole('radio', { name: 'Source mode' }).click();
    await page.locator('.skill-content .cm-content').fill(`${skillMarkdown}\n- Verify owners`);
    const updateSkillResponsePromise = page.waitForResponse((response) => (
      response.request().method() === 'PATCH'
      && new URL(response.url()).pathname === `/api/skills/${skill.id}`
    ));
    await page.getByRole('button', { name: 'Save skill' }).click();
    const updateSkillResponse = await updateSkillResponsePromise;
    assert.equal(updateSkillResponse.ok(), true, 'Skill update must succeed');
    const updatedSkill = await updateSkillResponse.json();
    assert.equal(updatedSkill.revision, skill.revision + 1);
    assert.notEqual(updatedSkill.content_checksum_sha256, skill.content_checksum_sha256);

    const bulkSkillName = scenarioContext.unique('QA Browser Bulk Skill');
    const bulkSkillResponse = await request.post('/api/skills', {
      data: {
        name: bulkSkillName,
        description: 'Browser bulk-delete fixture.',
        content: '# Bulk fixture'
      }
    });
    assert.equal(bulkSkillResponse.ok(), true, await bulkSkillResponse.text());
    const bulkSkill = await bulkSkillResponse.json();

    const agentName = scenarioContext.unique('QA Browser Agent');
    const agentMarkdown = '# Browser Agent\n\nUse the enabled Skill.';
    await page.goto('/agents', { waitUntil: 'domcontentloaded' });
    await page.locator('.agents-header').getByRole('button', { name: 'Create Agent' }).click();
    const createAgentDialog = page.getByRole('dialog', { name: 'Create Agent' });
    await createAgentDialog.getByLabel('Name', { exact: true }).fill(agentName);
    await createAgentDialog.getByRole('radio', { name: 'Source mode' }).click();
    await createAgentDialog.locator('.cm-content').fill(agentMarkdown);
    await createAgentDialog.getByRole('radio', { name: 'Rich text' }).click();
    assert.ok((await createAgentDialog.getByRole('textbox', { name: 'Instructions' }).innerText()).includes('Browser Agent'));
    const modelSelect = createAgentDialog.getByLabel('Default model connection');
    await modelSelect.waitFor();
    assert.notEqual(await modelSelect.inputValue(), '', 'Create Agent must expose the System Default Model Connection');
    await createAgentDialog.getByLabel('Reasoning effort').selectOption('high');
    await createAgentDialog.getByRole('button', { name: 'Add subagent' }).click();
    const subagentDialog = page.getByRole('dialog', { name: 'Add subagent' });
    await subagentDialog.getByLabel('Subagent name').fill('reviewer');
    await subagentDialog.getByLabel('Description').fill('Reviews the browser QA change.');
    await subagentDialog.getByRole('textbox', { name: 'Developer instructions' }).fill('Review correctness and report blocking findings.');
    await subagentDialog.getByLabel('Reasoning override').selectOption('max');
    await subagentDialog.getByRole('button', { name: 'Save changes' }).click();
    assert.ok((await createAgentDialog.getByRole('table', { name: 'Subagents' }).innerText()).includes('reviewer'));
    await createAgentDialog.getByLabel('Visibility').selectOption('public');
    const createAgentResponsePromise = page.waitForResponse((response) => (
      response.request().method() === 'POST'
      && new URL(response.url()).pathname === '/api/agents'
    ));
    await createAgentDialog.getByRole('button', { name: 'Create agent' }).click();
    const createAgentResponse = await createAgentResponsePromise;
    assert.equal(createAgentResponse.ok(), true, 'Agent create must succeed');
    const agent = await createAgentResponse.json();
    assert.equal(agent.visibility, 'public');
    assert.equal(agent.reasoning_effort, 'high');
    assert.equal(agent.subagents[0].name, 'reviewer');
    await page.waitForURL((url) => url.pathname === `/agents/${agent.id}`);

    await page.getByRole('tab', { name: 'Models' }).click();
    const modelsPanel = page.getByRole('tabpanel', { name: 'Models' });
    assert.notEqual(await modelsPanel.getByLabel('Default model connection').inputValue(), '');
    assert.equal(await modelsPanel.getByLabel('Reasoning effort').inputValue(), 'high');
    assert.ok((await modelsPanel.getByRole('table', { name: 'Subagents' }).innerText()).includes('reviewer'));

    await page.getByRole('tab', { name: 'Skills' }).click();
    const skillsPanel = page.getByRole('tabpanel', { name: 'Skills' });
    assert.equal(await skillsPanel.getByRole('checkbox').count(), 0, 'Enabled Skills must be shown without an always-open checklist');
    await skillsPanel.getByRole('button', { name: 'Edit managed skills' }).click();
    const skillsDialog = page.getByRole('dialog', { name: 'Edit managed skills' });
    await skillsDialog.getByRole('checkbox', { name: new RegExp(skillName) }).check();
    const bindSkillResponsePromise = page.waitForResponse((response) => (
      response.request().method() === 'PATCH'
      && new URL(response.url()).pathname === `/api/agents/${agent.id}`
    ));
    await skillsDialog.getByRole('button', { name: 'Save changes' }).click();
    assert.equal((await bindSkillResponsePromise).ok(), true, 'Managed Skill binding must save');
    await skillsDialog.waitFor({ state: 'detached' });
    assert.ok((await skillsPanel.locator('.agent-skill-chips').innerText()).includes(skillName));

    await page.getByRole('tab', { name: 'MCP', exact: true }).click();
    const mcpPanel = page.getByRole('tabpanel', { name: 'MCP' });
    const mcpTable = mcpPanel.getByRole('table', { name: 'MCP allowlist' });
    const mcpName = scenarioContext.unique('qa-browser-mcp').toLowerCase().replace(/[^a-z0-9-]/g, '-');
    const mcpSecret = scenarioContext.unique('qa-browser-mcp-secret').replace(/[^A-Za-z0-9_-]/g, '');
    let mcpDialog;
    await context.tracing.stop();
    try {
      await mcpPanel.getByRole('button', { name: 'Add MCP entry' }).click();
      mcpDialog = page.getByRole('dialog', { name: 'Add MCP entry' });
      await mcpDialog.getByLabel('Name', { exact: true }).fill(mcpName);
      await mcpDialog.getByLabel('Command').fill('qa-browser-mcp');
      await mcpDialog.getByLabel('Arguments').fill('--read-only\n/workspace');
      await mcpDialog.getByRole('button', { name: 'Add secret' }).click();
      await mcpDialog.getByLabel('Secret name 1').fill('QA_TOKEN');
      await mcpDialog.getByLabel('Secret value 1').fill(mcpSecret);
      const addMcpResponsePromise = page.waitForResponse((response) => (
        response.request().method() === 'PATCH'
        && new URL(response.url()).pathname === `/api/agents/${agent.id}`
      ));
      await mcpDialog.getByRole('button', { name: 'Save changes' }).click();
      const addMcpResponse = await addMcpResponsePromise;
      assert.equal(addMcpResponse.ok(), true, 'MCP entry create must succeed');
      const redactedAgent = await addMcpResponse.json();
      assert.equal(redactedAgent.mcp_allowlist[0].secrets.QA_TOKEN, '********');
      assert.equal(JSON.stringify(redactedAgent).includes(mcpSecret), false, 'MCP response must redact plaintext');
      await mcpDialog.waitFor({ state: 'detached' });
    } finally {
      if (mcpDialog) await closeOrRedactMcpDialog(page, mcpDialog);
      await context.tracing.start({ screenshots: true, snapshots: true, sources: true });
    }
    assert.ok((await mcpTable.innerText()).includes('QA_TOKEN=********'));
    assert.equal((await mcpTable.innerText()).includes(mcpSecret), false, 'MCP table must not expose plaintext');

    await mcpTable.getByRole('button', { name: `Edit MCP entry: ${mcpName}` }).click();
    const editMcpDialog = page.getByRole('dialog', { name: `Edit MCP entry: ${mcpName}` });
    assert.equal(await editMcpDialog.getByLabel('Secret value 1').inputValue(), '********');
    await editMcpDialog.getByLabel('Command').fill('qa-browser-mcp-updated');
    const editMcpResponsePromise = page.waitForResponse((response) => (
      response.request().method() === 'PATCH'
      && new URL(response.url()).pathname === `/api/agents/${agent.id}`
    ));
    await editMcpDialog.getByRole('button', { name: 'Save changes' }).click();
    const editMcpResponse = await editMcpResponsePromise;
    assert.equal(editMcpResponse.ok(), true, 'MCP placeholder edit must succeed');
    assert.equal((await editMcpResponse.json()).mcp_allowlist[0].secrets.QA_TOKEN, '********');
    assert.ok((await mcpTable.innerText()).includes('qa-browser-mcp-updated'));

    const runMessage = scenarioContext.unique('QA browser historical message');
    const runResponse = await request.post(`/api/agents/${agent.id}/runs`, {
      data: { message: runMessage, hub_session_id: null, parent_run_id: null }
    });
    assert.equal(runResponse.ok(), true, await runResponse.text());
    const run = await runResponse.json();
    allowedNoContentAborts.add(
      `requestfailed: GET ${new URL(`/api/runs/${run.id}/events/stream`, scenarioContext.baseURL).href}: net::ERR_ABORTED`
    );
    const completed = await poll(async () => {
      const response = await request.get(`/api/agents/${agent.id}/runs`);
      assert.equal(response.ok(), true, await response.text());
      const runs = await response.json();
      return runs.find((candidate) => candidate.id === run.id) ?? null;
    }, (candidate) => candidate?.status === 'completed', {
      timeoutMs: 60_000,
      description: `browser Agent Run ${run.id} completion`
    });
    assert.equal(completed.status, 'completed');

    page.once('dialog', (confirmation) => confirmation.accept());
    const deleteMcpResponsePromise = page.waitForResponse((response) => (
      response.request().method() === 'PATCH'
      && new URL(response.url()).pathname === `/api/agents/${agent.id}`
    ));
    await mcpTable.getByRole('button', { name: `Delete ${mcpName}` }).click();
    assert.equal((await deleteMcpResponsePromise).ok(), true, 'MCP entry delete must succeed');
    await mcpTable.getByText(mcpName, { exact: true }).waitFor({ state: 'detached' });

    await assertNoHorizontalOverflow(page, 'Agent desktop detail');
    await page.setViewportSize({ width: 390, height: 844 });
    await assertNoHorizontalOverflow(page, 'Agent 390px detail');
    await page.setViewportSize({ width: 1280, height: 800 });

    page.once('dialog', (confirmation) => confirmation.accept());
    const deleteAgentResponsePromise = page.waitForResponse((response) => (
      response.request().method() === 'DELETE'
      && new URL(response.url()).pathname === `/api/agents/${agent.id}`
    ));
    await page.getByRole('button', { name: 'Delete agent', exact: true }).click();
    const deleteAgentResponse = await deleteAgentResponsePromise;
    assert.equal(deleteAgentResponse.status(), 204, 'Agent delete must be irreversible');
    allowedNoContentAborts.add(`requestfailed: DELETE ${deleteAgentResponse.url()}: net::ERR_ABORTED`);
    await page.waitForURL((url) => url.pathname === '/agents');

    await page.goto('/sessions', { waitUntil: 'domcontentloaded' });
    const historicalButton = page.getByRole('button', { name: new RegExp(agentName) }).first();
    await historicalButton.waitFor();
    await historicalButton.click();
    const sessionDetail = page.getByRole('region', { name: 'Session details' });
    assert.ok((await sessionDetail.innerText()).includes('Historical Session'));
    assert.ok((await sessionDetail.innerText()).includes(runMessage));
    assert.equal(await sessionDetail.getByRole('button', { name: 'Send' }).count(), 0);
    assert.equal(await sessionDetail.getByRole('textbox', { name: 'Message' }).count(), 0);

    await page.goto('/skills', { waitUntil: 'domcontentloaded' });
    await page.getByRole('checkbox', { name: `Select ${skillName}` }).check();
    await page.getByRole('checkbox', { name: `Select ${bulkSkillName}` }).check();
    let bulkConfirmation;
    page.once('dialog', async (confirmation) => {
      bulkConfirmation = confirmation.message();
      await confirmation.accept();
    });
    const bulkDeleteResponsePromise = page.waitForResponse((response) => (
      response.request().method() === 'DELETE'
      && new URL(response.url()).pathname === '/api/skills'
    ));
    await page.getByRole('button', { name: 'Delete selected' }).click();
    const bulkDeleteResponse = await bulkDeleteResponsePromise;
    assert.equal(bulkDeleteResponse.ok(), true, 'Bulk Skill delete must succeed');
    assert.equal(bulkConfirmation, 'Delete 2 selected skills? This cannot be undone.');
    await page.getByText(skillName, { exact: true }).waitFor({ state: 'detached' });
    await page.getByText(bulkSkillName, { exact: true }).waitFor({ state: 'detached' });
    await page.setViewportSize({ width: 390, height: 844 });
    await assertNoHorizontalOverflow(page, 'Skills 390px list');

    const unexpectedBrowserErrors = browserErrors.filter((error) => !allowedNoContentAborts.has(error));
    browserErrors.splice(0, browserErrors.length, ...unexpectedBrowserErrors);
    assert.deepEqual(browserErrors, [], 'Browser diagnostics must remain empty');
  });
}
