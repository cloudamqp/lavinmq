import * as helpers from './helpers.js'
import { test, expect } from './fixtures.js';

test.describe("policies", _ => {
  test('are loaded', async ({ page, baseURL }) => {
    const apiPoliciesRequest = helpers.waitForPathRequest(page, '/api/policies', {})
    await page.goto('/policies')
    await expect(apiPoliciesRequest).toBeRequested()
  })
})
