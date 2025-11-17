import * as helpers from './helpers.js'
import { test, expect } from './fixtures.js'

test.describe('exchange', _ => {
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
    for (let i = 0; i < 3; ++i) {
      const apiExchangesRequest2 = helpers.waitForPathRequest(page, apiPath)
      // Move into the future and make sure we've had a second request
      await page.clock.runFor(10000)
      await expect(apiExchangesRequest2).toBeRequested()
    }
  })

  test('navigates to another exchange via binding link', async ({ page }) => {
    await page.goto(pagePath)

    // Simulate hash change to navigate to another exchange
    const targetExchange = 'amq.direct'
    const newHash = `#vhost=%2F&name=${targetExchange}`

    // Wait for the API request to the new exchange after hash change
    const apiNewExchangeRequest = helpers.waitForPathRequest(page, `api/exchanges/%2F/${targetExchange}`)

    // Change the hash to simulate clicking an exchange binding link
    await page.evaluate((hash) => {
      window.location.hash = hash
    }, newHash)

    // Verify the new exchange is loaded
    await expect(apiNewExchangeRequest).toBeRequested()
    await expect(page).toHaveURL(new RegExp(`exchange${newHash}`))
  })
})
