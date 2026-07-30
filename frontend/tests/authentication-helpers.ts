import { expect, type Page } from '@playwright/test';

export async function selectLocalPasswordLogin(page: Page) {
  const localPassword = page.getByRole('button', { name: /^(Local Password|本地密码)$/ });
  const password = page.getByLabel(/^(Password|密码)$/);
  await expect(localPassword.or(password).first()).toBeVisible();
  if (await localPassword.isVisible()) {
    await localPassword.click();
    await expect(localPassword).toHaveAttribute('aria-pressed', 'true');
  }
}
