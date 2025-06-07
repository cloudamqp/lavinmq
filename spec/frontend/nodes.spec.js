import * as helpers from './helpers.js'
import { test, expect } from './fixtures.js';

test.describe("nodes", _ => {
  test('are loaded', async ({ page, baseURL }) => {
    const apiNodesRequest = helpers.waitForPathRequest(page, '/api/nodes')
    await page.goto('/nodes')
    await expect(apiNodesRequest).toBeRequested()
  })
})
