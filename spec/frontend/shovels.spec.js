import * as helpers from './helpers.js'
import { test, expect } from './fixtures.js';

test.describe("shovels", _ => {
  test('are loaded', async ({ page, baseURL }) => {
    const apiShovelsRequest = helpers.waitForPathRequest(page, '/api/shovels', {})
    const apiParametersRequest = helpers.waitForPathRequest(page, '/api/parameters/shovel', {})
    await page.goto('/shovels')
    await expect(apiShovelsRequest).toBeRequested()
    await expect(apiParametersRequest).toBeRequested()
  })
})
