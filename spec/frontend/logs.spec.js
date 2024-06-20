import * as helpers from './helpers.js'
import { test, expect } from './fixtures.js';

test.describe("logs", _ => {
  test('are loaded', async ({ page, baseURL }) => {
    const apiLogsRequest = page.waitForRequest(/\/api\/livelog$/, { timeout: 1000 })
    await page.goto('/logs')
    await expect(apiLogsRequest).toBeRequested()
  })
})
