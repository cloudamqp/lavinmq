import * as helpers from './helpers.js'
import { test, expect } from './fixtures.js';

test.describe("exchanges", _ => {
  test('are loaded', async ({ page, baseURL }) => {
    const apiExchangesRequest = helpers.waitForPathRequest(page, '/api/exchanges')
    await page.goto('/exchanges')
    await expect(apiExchangesRequest).toBeRequested()
  })
})
