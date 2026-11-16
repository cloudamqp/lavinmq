import * as helpers from './helpers.js'
import { test, expect } from './fixtures.js';

test.describe("users", _ => {
  test('are loaded', async ({ page, baseURL }) => {
    const apiUsersRequest = helpers.waitForPathRequest(page, '/api/users')
    await page.goto('/users')
    await expect(apiUsersRequest).toBeRequested()
  })

  test('tag links add to the tags list', async ({ page }) => {
    await page.goto('/users')
    const tags = page.getByLabel('Tags')
    await page.locator('[data-helper-for="tags"] a[data-tag="administrator"]').click()
    await page.locator('[data-helper-for="tags"] a[data-tag="monitoring"]').click()
    await expect(tags).toHaveValue('administrator, monitoring')
    await page.locator('[data-helper-for="tags"] a[data-tag=""]').click()
    await expect(tags).toHaveValue('')
  })
})
