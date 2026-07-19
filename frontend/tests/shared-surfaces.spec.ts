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

  await expect(page.getByRole('heading', { name: 'Sessions', exact: true, level: 1 })).toBeVisible();
  const workspace = page.getByRole('navigation', { name: 'Primary navigation' });
  await expect(workspace.getByRole('button')).toHaveText([
    'Sessions', 'Agents', 'Integration Apps', 'Automations', 'Skills',
    'Models', 'API Keys', 'Runtimes', 'Administration', 'API Docs'
  ]);
  await expect(workspace.getByRole('button', { name: 'Sessions' })).toHaveAttribute('aria-current', 'page');
});

test('Agent instructions switch between rich text and Markdown source without losing content', async ({ page }) => {
  await installBaseApi(page);
  await page.goto('/agents');
  await page.locator('.agents-header').getByRole('button', { name: 'Create Agent', exact: true }).click();

  const dialog = page.getByRole('dialog', { name: 'Create Agent', exact: true });
  const richText = dialog.getByRole('textbox', { name: 'Instructions', exact: true });
  await richText.fill('Review changes');
  await dialog.getByRole('combobox', { name: 'Block type', exact: true }).click();
  await page.getByRole('option', { name: 'Heading 1', exact: true }).click();
  await richText.press('End');
  await richText.press('Enter');
  await richText.type('Keep history');
  await dialog.getByRole('radio', { name: 'Bulleted list', exact: true }).click();
  await richText.press('End');
  await richText.press('Enter');
  await richText.type('Explain risks');
  await expect(richText.getByRole('heading', { name: 'Review changes', exact: true, level: 1 })).toBeVisible();
  await expect(richText.getByRole('listitem')).toHaveText(['Keep history', 'Explain risks']);

  await page.getByLabel('Language').selectOption('zh-CN');
  const localizedDialog = page.getByRole('dialog', { name: '创建智能体', exact: true });
  const localizedEditor = localizedDialog.getByRole('textbox', { name: '指令', exact: true });
  await expect(localizedEditor.getByRole('heading', { name: 'Review changes', exact: true, level: 1 })).toBeVisible();
  await expect(localizedEditor.getByRole('listitem')).toHaveText(['Keep history', 'Explain risks']);
  await expect(localizedDialog.getByRole('radio', { name: '粗体', exact: true })).toBeVisible();

  await page.setViewportSize({ width: 390, height: 844 });
  const box = await localizedDialog.boundingBox();
  expect(box).not.toBeNull();
  expect(box!.width).toBe(390);
  expect(box!.y + box!.height).toBe(844);
  expect(box!.y).toBeGreaterThanOrEqual(0);
  expect(await page.evaluate(() => document.documentElement.scrollWidth)).toBeLessThanOrEqual(390);
});
