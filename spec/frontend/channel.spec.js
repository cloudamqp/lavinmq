import * as helpers from './helpers.js'
import { test, expect } from './fixtures.js';

test.describe("channel", _ => {
  const channelName = "127.0.0.1:63221[1]"
  test('is loaded', async ({ page, baseURL }) => {
    const apiChannelRequest = helpers.waitForPathRequest(page, `/api/channels/${channelName}`)
    await page.goto(`/channel#name=${channelName}`)
    await expect(apiChannelRequest).toBeRequested()
  })

  test('is refreshed automatically', async({ page }) => {
    page.clock.install()
    await page.goto(`/channel#name=${channelName}`)
    // Verify that at least 3 requests are made
    for (let i=0; i<3; i++) {
      const apiChannelRequest = helpers.waitForPathRequest(page, `/api/channels/${channelName}`)
      page.clock.runFor(10000) // advance time by 10 seconds
      await expect(apiChannelRequest).toBeRequested()
    }
  })
})
