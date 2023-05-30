import * as helpers from './helpers.js'
import { test, expect } from './fixtures.js';

test.describe("overview", _ => {
  test('are loaded', async ({ page, baseURL }) => {
    const apiOverviewRequest = helpers.waitForPathRequest(page, '/api/overview')
    await page.goto('/')
    await expect(apiOverviewRequest).toBeRequested()
  })
})
