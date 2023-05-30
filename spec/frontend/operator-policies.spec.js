import * as helpers from './helpers.js'
import { test, expect } from './fixtures.js';

test.describe("operator-policies", _ => {
  test('are loaded', async ({ page, baseURL }) => {
    const apiPoliciesRequest = helpers.waitForPathRequest(page, '/api/operator-policies', {})
    await page.goto('/operator-policies')
    await expect(apiPoliciesRequest).toBeRequested()
  })
})
