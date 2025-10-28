import * as helpers from './helpers.js'
import { test, expect } from './fixtures.js';

test.describe("exchange", _ => {
  test('is refreshed', async ({ page, baseURL }) => {
    const apiExchangesRequest = helpers.waitForPathRequest(page, 'api/exchanges/%2F/amq.topic')
    await page.clock.install()
    await page.goto('/exchange#vhost=%2F&name=amq.topic')
    await expect(apiExchangesRequest).toBeRequested()
    const apiExchangesRequest2 = helpers.waitForPathRequest(page, 'api/exchanges/%2F/amq.topic')
    // Move into the future and make sure we've had a second request
    await page.clock.runFor(60000)
    await expect(apiExchangesRequest2).toBeRequested()
  })
})
