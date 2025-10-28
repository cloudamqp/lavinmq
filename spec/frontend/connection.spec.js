import * as helpers from './helpers.js'
import { test, expect } from './fixtures.js';

test.describe("connection", _ => {
  const connectionName = "127.0.0.1:63610 -> 127.0.0.1:12345"
  const pagePath = `/connection#name=${connectionName}`
  const apiPath = `/api/connections/${connectionName}`

  test('is loaded', async ({ page, baseURL }) => {
    const apiConnectionRequest = helpers.waitForPathRequest(page, apiPath)
    await page.goto(pagePath)
    await expect(apiConnectionRequest).toBeRequested()
  })

  test('is refreshed automatically', async({ page }) => {
    page.clock.install()
    await page.goto(pagePath)
    // Verify that at least 3 requests are made
    for (let i=0; i<3; i++) {
      const apiConnectionRequest = helpers.waitForPathRequest(page, apiPath)
      page.clock.runFor(10000) // advance time by 10 seconds
      await expect(apiConnectionRequest).toBeRequested()
    }
  })


  test('channels are loaded', async ({ page, baseURL }) => {
    const apiChannelsRequest = helpers.waitForPathRequest(page, `${apiPath}/channels`)
    await page.goto(pagePath)
    await expect(apiChannelsRequest).toBeRequested()
  })

  test('close button trigger DELETE to api/connections/<name>', async ({ page, baseURL }) => {
    const response = {}
    await page.goto(pagePath)
    const apiDeleteRequest = helpers.waitForPathRequest(page, apiPath, {method: 'DELETE'})
    await page.locator('#closeConnection').getByRole('button').click()
    await expect(apiDeleteRequest).toBeRequested()
  })

})
