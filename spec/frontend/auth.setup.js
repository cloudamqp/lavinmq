import { test, test as setup, expect } from '@playwright/test';

const authFile = 'playwright/.auth/user.json';


setup('authenticate', async ({ page }) => {
  await page.goto('/login');
  await page.getByLabel('Username').fill('guest');
  await page.getByLabel('Password').fill('guest');
  await page.getByRole('button').click();
  await page.waitForURL('/');
  await page.context().storageState({ path: authFile });
});

