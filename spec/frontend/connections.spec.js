import * as helpers from './helpers.js'
import { test, expect } from './fixtures.js';

test.describe("connections", _ => {
  test('are loaded', async ({ page, baseURL }) => {
    const apiConnectionsRequest = helpers.waitForPathRequest(page, '/api/connections')
    await page.goto('/connections')
    await expect(apiConnectionsRequest).toBeRequested()
  })
})
