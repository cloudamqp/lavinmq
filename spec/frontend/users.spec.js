import * as helpers from './helpers.js'
import { test, expect } from './fixtures.js';

test.describe("users", _ => {
  test('are loaded', async ({ page, baseURL }) => {
    const apiUsersRequest = helpers.waitForPathRequest(page, '/api/users')
    await page.goto('/users')
    await expect(apiUsersRequest).toBeRequested()
  })
})
