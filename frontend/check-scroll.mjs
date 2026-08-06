import { chromium } from 'playwright';
const base = 'http://localhost:15178';
const browser = await chromium.launch();
const page = await browser.newPage({ viewport: { width: 1280, height: 800 } });
await page.goto(base + '/login');
await page.waitForTimeout(3000);
await page.click('button[type="submit"]');
await page.waitForTimeout(2500);
await page.goto(base + '/sessions');
await page.waitForTimeout(3000);
// click the first session row (newest by created_at)
await page.locator('.session-row').first().click();
await page.waitForTimeout(8000);
const gap = await page.evaluate(() => {
  const scroll = document.querySelector('.session-chat-scroll');
  return scroll ? scroll.scrollHeight - scroll.clientHeight - scroll.scrollTop : -1;
});
console.log('gap after load:', gap);
const lastBubble = await page.locator('.session-bubble').last().boundingBox();
const scrollBox = await page.locator('.session-chat-scroll').boundingBox();
console.log('last bubble bottom:', lastBubble ? lastBubble.y + lastBubble.height : null, 'scroll box bottom:', scrollBox ? scrollBox.y + scrollBox.height : null);
await browser.close();
