// @ts-check
import { test, expect } from '@playwright/test';

test.describe('when unauthenticated', _ =>  {
  test.use({ storageState: {} })
  test('redirects to login', async ({ page }) => {
    await page.goto('/');
    await expect(page).toHaveURL(/\/login$/);
  })
})

