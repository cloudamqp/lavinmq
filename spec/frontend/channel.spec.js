import * as helpers from './helpers.js'
import { test, expect } from './fixtures.js';

test.describe("channel", _ => {
  test('is loaded', async ({ page, baseURL }) => {
    const channelName = "127.0.0.1:63221[1]"
    const apiChannelRequest = helpers.waitForPathRequest(page, `/api/channels/${channelName}`, {})
    await page.goto(`/channel#name=${channelName}`)
    await expect(apiChannelRequest).toBeRequested()
  })
})
