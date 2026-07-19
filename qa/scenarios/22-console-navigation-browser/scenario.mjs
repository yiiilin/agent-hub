import assert from 'node:assert/strict';
import { withBrowser } from '../../support/browser.mjs';

const ENGLISH_ROUTES = [
  { path: '/sessions', nav: 'Sessions', heading: 'Sessions', requests: ['/api/sessions', '/api/agents'] },
  { path: '/agents', nav: 'Agents', heading: 'Agents', requests: ['/api/agents', '/api/runtimes'] },
  {
    path: '/integrations',
    nav: 'Integration Apps',
    heading: 'Integration Apps',
    requests: ['/api/integration-apps', '/api/integration-app-options', '/api/agents']
  },
  { path: '/automations', nav: 'Automations', heading: 'Automations', requests: ['/api/automations', '/api/agents'] },
  { path: '/skills', nav: 'Skills', heading: 'Skills', requests: ['/api/skills', '/api/agents'] },
  { path: '/models', nav: 'Models', heading: 'Models', requests: ['/api/model-connections'] },
  { path: '/api-keys', nav: 'API Keys', heading: 'API Keys', requests: ['/api/auth/api-keys'] },
  { path: '/runtimes', nav: 'Runtimes', heading: 'Runtime Nodes', requests: ['/api/runtimes', '/api/agents'] },
  {
    path: '/administration',
    nav: 'Administration',
    heading: 'Administration',
    requests: ['/api/admin/auth-policy']
  },
  { path: '/docs', nav: 'API Docs', heading: 'Agent Hub API', requests: ['/openapi.json'] }
];

const CHINESE_ROUTES = [
  { ...ENGLISH_ROUTES[0], nav: '会话', heading: '会话' },
  { ...ENGLISH_ROUTES[1], nav: '智能体', heading: '智能体' },
  { ...ENGLISH_ROUTES[2], nav: '集成应用', heading: '集成应用' },
  { ...ENGLISH_ROUTES[3], nav: '自动化', heading: '自动化' },
  { ...ENGLISH_ROUTES[4], nav: '技能', heading: '技能' },
  { ...ENGLISH_ROUTES[5], nav: '模型', heading: '模型' },
  { ...ENGLISH_ROUTES[6], nav: 'API 密钥', heading: 'API 密钥' },
  { ...ENGLISH_ROUTES[7], nav: '运行节点', heading: '运行节点' },
  { ...ENGLISH_ROUTES[8], nav: '管理', heading: '管理' },
  { ...ENGLISH_ROUTES[9], nav: 'API 文档', heading: 'Agent Hub API' }
];

function waitForRequests(page, paths) {
  return Promise.all(paths.map((path) => page.waitForResponse((response) => (
    response.request().method() === 'GET'
    && new URL(response.url()).pathname === path
  ), { timeout: 30_000 })));
}

async function assertSuccessfulResponses(responses, label) {
  for (const response of responses) {
    assert.equal(
      response.ok(),
      true,
      `${label} request ${new URL(response.url()).pathname} returned ${response.status()}`
    );
  }
}

async function settleLayout(page) {
  await page.evaluate(() => new Promise((resolve) => {
    requestAnimationFrame(() => requestAnimationFrame(resolve));
  }));
}

async function assertNoHorizontalOverflow(page, label) {
  await settleLayout(page);
  const dimensions = await page.evaluate(() => ({
    scrollWidth: document.documentElement.scrollWidth,
    clientWidth: document.documentElement.clientWidth
  }));
  assert.ok(
    dimensions.scrollWidth - dimensions.clientWidth <= 1,
    `${label} horizontal overflow: ${dimensions.scrollWidth - dimensions.clientWidth}px`
  );
}

async function assertRoute(page, navigation, route, viewportLabel) {
  await page.waitForURL((url) => url.pathname === route.path);
  const heading = page.locator('main h1').first();
  await heading.waitFor({ state: 'attached' });
  assert.equal((await heading.textContent())?.trim(), route.heading, `${route.path} heading`);
  assert.equal(
    await navigation.getByRole('button', { name: route.nav, exact: true }).getAttribute('aria-current'),
    'page',
    `${route.path} navigation state`
  );
  await assertNoHorizontalOverflow(page, `${viewportLabel} ${route.path}`);
}

async function navigateToRoute(page, navigation, route, viewportLabel) {
  const responsesPromise = waitForRequests(page, route.requests);
  await navigation.getByRole('button', { name: route.nav, exact: true }).click();
  const responses = await responsesPromise;
  await assertSuccessfulResponses(responses, route.path);
  await assertRoute(page, navigation, route, viewportLabel);
}

async function assertNavigationOrder(navigation, expectedLabels, label) {
  const labels = (await navigation.locator('.nav-button').allTextContents())
    .map((value) => value.trim());
  assert.deepEqual(labels, expectedLabels, `${label} sidebar order`);
  assert.equal(labels[0], expectedLabels[0], `${label} must place Sessions first`);
}

export default async function consoleNavigationScenario(scenarioContext) {
  await withBrowser(scenarioContext, {
    allowedHttpErrors: [
      { method: 'GET', pathname: '/api/auth/me', status: 401, times: 1 }
    ]
  }, async ({ page, request }) => {
    const publicOpenApi = await request.get('/openapi.json');
    assert.equal(publicOpenApi.status(), 200, 'OpenAPI must be public before login');
    assert.match(publicOpenApi.headers()['content-type'] ?? '', /^application\/json\b/);
    const openApiDocument = await publicOpenApi.json();
    assert.equal(openApiDocument.openapi, '3.1.0');
    assert.equal(openApiDocument.info?.title, 'Agent Hub API');
    assert.equal(typeof openApiDocument.info?.version, 'string');
    assert.ok(openApiDocument.paths?.['/api/agents/{agent_id}/runs']);
    assert.ok(openApiDocument.paths?.['/api/widget/runs/{run_id}/stop']);

    await page.setViewportSize({ width: 1280, height: 800 });
    await page.goto('/login', { waitUntil: 'domcontentloaded' });
    await page.getByLabel('Email').fill('admin@example.com');
    await page.getByLabel('Password').fill('admin123');
    const initialResponsesPromise = waitForRequests(page, ENGLISH_ROUTES[0].requests);
    await page.getByRole('button', { name: 'Sign in', exact: true }).click();
    await assertSuccessfulResponses(await initialResponsesPromise, '/sessions');

    let navigation = page.getByRole('navigation', { name: 'Primary navigation' });
    await assertNavigationOrder(
      navigation,
      ENGLISH_ROUTES.map((route) => route.nav),
      'English desktop'
    );
    await assertRoute(page, navigation, ENGLISH_ROUTES[0], '1280px English');
    for (const route of ENGLISH_ROUTES.slice(1)) {
      await navigateToRoute(page, navigation, route, '1280px English');
    }

    await page.getByText('Authorization: Bearer', { exact: false }).waitFor();
    await page.getByText('/api/agents/{agent_id}/runs', { exact: true }).first().waitFor();
    const openApiLink = page.getByRole('link', { name: 'Open OpenAPI JSON' });
    assert.equal(await openApiLink.getAttribute('href'), '/openapi.json');

    await page.setViewportSize({ width: 390, height: 844 });
    await page.getByLabel('Language').selectOption('zh-CN');
    navigation = page.getByRole('navigation', { name: '主导航' });
    await assertNavigationOrder(
      navigation,
      CHINESE_ROUTES.map((route) => route.nav),
      'Chinese mobile'
    );
    for (const route of CHINESE_ROUTES) {
      await navigateToRoute(page, navigation, route, '390px Chinese');
    }
    await page.getByText('API 参考', { exact: true }).waitFor();
    await page.getByText('/api/agents/{agent_id}/runs', { exact: true }).first().waitFor();
    assert.equal(
      await page.getByRole('link', { name: '打开 OpenAPI JSON' }).getAttribute('href'),
      '/openapi.json'
    );
    await assertNoHorizontalOverflow(page, '390px Chinese /docs');
  });
}
