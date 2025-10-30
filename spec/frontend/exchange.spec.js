import * as helpers from './helpers.js'
import { test, expect } from './fixtures.js';

test.describe("exchange", _ => {
  const pagePath = '/exchange#vhost=%2F&name=amq.topic'
  const apiPath = 'api/exchanges/%2F/amq.topic'

  test('is loaded', async ({ page }) => {
    const apiExchangesRequest = helpers.waitForPathRequest(page, apiPath)
    await page.goto(pagePath)
    await expect(apiExchangesRequest).toBeRequested()
  })

  test('is refreshed automatically', async ({ page, baseURL }) => {
    await page.clock.install()
    await page.goto(pagePath)
    for (let i=0; i<3; ++i) {
      const apiExchangesRequest2 = helpers.waitForPathRequest(page, apiPath)
      // Move into the future and make sure we've had a second request
      await page.clock.runFor(10000)
      await expect(apiExchangesRequest2).toBeRequested()
    }
  })
})
