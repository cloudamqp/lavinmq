import * as helpers from './helpers.js'
import { test, expect } from './fixtures.js';

test.describe("exchanges", _ => {
  test('are loaded', async ({ page, baseURL }) => {
    const apiExchangesRequest = helpers.waitForPathRequest(page, '/api/exchanges')
    await page.goto('/exchanges')
    await expect(apiExchangesRequest).toBeRequested()
  })

  test('are refreshed automatically', async({ page }) => {
    await page.clock.install()
    await page.goto('/exchanges')
    // Verify that at least 3 requests are made
    for (let i=0; i<3; i++) {
      const apiExchangesRequest = helpers.waitForPathRequest(page, '/api/exchanges')
      await page.clock.runFor(10000) // advance time by 10 seconds
      await expect(apiExchangesRequest).toBeRequested()
    }
  })
})
