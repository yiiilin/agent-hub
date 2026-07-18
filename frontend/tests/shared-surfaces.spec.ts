import { expect, test, type Page, type Route } from '@playwright/test';

const currentUser = {
  id: '10000000-0000-0000-0000-000000000001',
  username: 'shared-surfaces',
  email: 'shared-surfaces@example.com',
  display_name: 'Shared surfaces tester',
  role: 'admin'
};

async function installBaseApi(page: Page) {
  await page.route('**/api/**', async (route: Route) => {
    const path = new URL(route.request().url()).pathname;
    if (!path.startsWith('/api/')) return route.continue();
    if (path === '/api/auth/me') return route.fulfill({ json: currentUser });
    if (path === '/api/sessions') return route.fulfill({ json: [] });
    if (path === '/api/agents') return route.fulfill({ json: [] });
    if (path === '/api/runtimes') return route.fulfill({ json: [] });
    if (path === '/api/users') return route.fulfill({ json: [currentUser] });
    return route.fulfill({ status: 404, json: { error: `Unhandled route ${path}` } });
  });
}

test('root opens Sessions and places it first in workspace navigation', async ({ page }) => {
  await installBaseApi(page);
  await page.goto('/');

  await expect(page.getByRole('heading', { name: 'Sessions' })).toBeVisible();
  const workspace = page.getByRole('navigation', { name: 'Primary navigation' });
  await expect(workspace.getByRole('button').first()).toHaveText(/Sessions/);
  await expect(workspace.getByRole('button', { name: 'Sessions' })).toHaveAttribute('aria-current', 'page');
});

test('Agent instructions switch between rich text and Markdown source without losing content', async ({ page }) => {
  await installBaseApi(page);
  await page.goto('/agents');
  await page.locator('.agents-header').getByRole('button', { name: 'Create Agent' }).click();

  const dialog = page.getByRole('dialog', { name: 'Create Agent' });
  await dialog.getByRole('radio', { name: 'Source mode' }).click();
  const source = dialog.locator('.cm-content');
  await source.fill('# Review changes\n\n- Keep history\n- Explain risks');
  await dialog.getByRole('radio', { name: 'Rich text' }).click();

  const richText = dialog.getByRole('textbox', { name: 'Instructions' });
  await expect(richText).toContainText('Review changes');
  await expect(richText).toContainText('Keep history');
  await dialog.getByRole('radio', { name: 'Source mode' }).click();
  await expect(dialog.locator('.cm-content')).toContainText('# Review changes');
  await expect(dialog.locator('.cm-content')).toContainText(/[*-] Explain risks/);

  await page.getByLabel('Language').selectOption('zh-CN');
  await expect(dialog.getByRole('radio', { name: 'Markdown 源码' })).toBeVisible();

  await page.setViewportSize({ width: 390, height: 844 });
  const box = await dialog.boundingBox();
  expect(box).not.toBeNull();
  expect(box!.width).toBe(390);
  expect(box!.y + box!.height).toBe(844);
  expect(box!.y).toBeGreaterThanOrEqual(0);
  expect(await page.evaluate(() => document.documentElement.scrollWidth)).toBeLessThanOrEqual(390);
});
