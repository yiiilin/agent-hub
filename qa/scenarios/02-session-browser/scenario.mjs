import assert from 'node:assert/strict';
import { poll } from '../../support/api.mjs';
import { withBrowser } from '../../support/browser.mjs';

export default async function sessionBrowserScenario(scenarioContext) {
  await withBrowser(scenarioContext, {
    allowedHttpErrors: [
      { method: 'GET', pathname: '/api/auth/me', status: 401, times: 1 }
    ]
  }, async ({ page, request }) => {
    let agent = null;
    try {
      await page.goto('/login', { waitUntil: 'domcontentloaded' });
      await page.waitForLoadState('networkidle');
      await page.getByLabel('Email').fill('admin@example.com');
      await page.getByLabel('Password').fill('admin123');
      await page.getByRole('button', { name: 'Sign in', exact: true }).click();
      await page.getByText('admin@example.com', { exact: true }).waitFor();

      const agentName = scenarioContext.unique('QA Browser Agent');
      const agentResponse = await request.post('/api/agents', {
        data: {
          name: agentName,
          instructions: 'Complete deterministic browser QA through fake Codex.',
          visibility: 'private',
          public_to: []
        }
      });
      assert.equal(agentResponse.ok(), true, await agentResponse.text());
      agent = await agentResponse.json();

      const message = scenarioContext.unique('QA browser message');
      const runResponse = await request.post(`/api/agents/${agent.id}/runs`, {
        data: { message, hub_session_id: null, parent_run_id: null }
      });
      assert.equal(runResponse.ok(), true, await runResponse.text());
      const run = await runResponse.json();

      const completed = await poll(async () => {
        const response = await request.get(`/api/agents/${agent.id}/runs`);
        assert.equal(response.ok(), true, await response.text());
        const runs = await response.json();
        return runs.find((candidate) => candidate.id === run.id) ?? null;
      }, (candidate) => candidate?.status === 'completed', {
        timeoutMs: 45_000,
        description: `Run ${run.id} to complete`
      });
      assert.equal(completed.status, 'completed');

      await page.goto('/sessions', { waitUntil: 'domcontentloaded' });
      await page.getByRole('heading', { name: agentName, level: 2, exact: true }).waitFor();
      await page.locator('.session-message-text').filter({ hasText: message }).waitFor();
      await page.locator('.session-message-text').filter({ hasText: 'Fake Codex completed run' }).waitFor();

      const activitySummary = page.locator('.session-activity-events > summary').first();
      await activitySummary.waitFor();
      assert.match(await activitySummary.innerText(), /^Worked for .+$/);
      await activitySummary.click();
      assert.ok(await page.locator('.session-activity-row').count() > 0);
      await page.getByText('Thought', { exact: true }).waitFor();

      const desktopOverflow = await page.evaluate(() => (
        document.documentElement.scrollWidth - document.documentElement.clientWidth
      ));
      assert.ok(desktopOverflow <= 1, `Desktop horizontal overflow: ${desktopOverflow}px`);
      await page.setViewportSize({ width: 390, height: 844 });
      await page.waitForTimeout(100);
      const mobileOverflow = await page.evaluate(() => (
        document.documentElement.scrollWidth - document.documentElement.clientWidth
      ));
      assert.ok(mobileOverflow <= 1, `Mobile horizontal overflow: ${mobileOverflow}px`);
    } finally {
      if (agent) {
        const deleteResponse = await request.delete(`/api/agents/${agent.id}`);
        assert.equal(deleteResponse.status(), 204, await deleteResponse.text());
      }
    }
  });
}
