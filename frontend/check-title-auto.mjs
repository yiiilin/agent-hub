import { chromium } from 'playwright';
const browser = await chromium.launch();
const page = await browser.newPage({ viewport: { width: 1400, height: 900 } });
await page.goto('http://localhost:15178/login');
await page.waitForTimeout(2000);
await page.click('button[type="submit"]');
await page.waitForTimeout(2500);
await page.goto('http://localhost:15178/sessions');
await page.waitForTimeout(3000);
// start a new conversation draft
await page.click('.session-new-conversation');
await page.waitForTimeout(800);
await page.fill('.session-chat-composer textarea', '帮我看看如何配置本地开发环境');
await page.keyboard.press('Enter');
// watch the first session row title for up to 60s
const start = Date.now();
let title = null;
while (Date.now() - start < 60000) {
  await page.waitForTimeout(3000);
  const firstText = await page.locator('.session-row').first().innerText().catch(() => '');
  const strong = firstText.split('\n')[0];
  if (strong && strong !== '本地测试智能体') { title = strong; break; }
  // refresh list view by clicking first row then back? the polling effect should update sessions automatically
}
console.log('auto title observed:', title, 'after ms:', Date.now() - start);
await browser.close();
