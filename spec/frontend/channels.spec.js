import * as helpers from './helpers.js'
import { test, expect } from './fixtures.js';

test.describe("channels", _ => {
  test('are loaded', async ({ page, baseURL }) => {
    const apiChannelsRequest = helpers.waitForPathRequest(page, '/api/channels', {})
    await page.goto('/channels')
    await expect(apiChannelsRequest).toBeRequested()
  })
})
