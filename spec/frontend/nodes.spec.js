import * as helpers from './helpers.js'
import { test, expect } from './fixtures.js';

test.describe("nodes", _ => {
  test('are loaded', async ({ page, baseURL }) => {
    const apiNodesRequest = helpers.waitForPathRequest(page, '/api/nodes')
    await page.goto('/nodes')
    await expect(apiNodesRequest).toBeRequested()
  })

  test('are refreshed automatically', async({ page }) => {
    page.clock.install()
    await page.goto(`/nodes`)
    // Verify that at least 3 requests are made
    for (let i=0; i<3; i++) {
      const apiNodesRequest = helpers.waitForPathRequest(page, `/api/nodes`)
      page.clock.runFor(10000) // advance time by 10 seconds
      await expect(apiNodesRequest).toBeRequested()
    }
  })
})
