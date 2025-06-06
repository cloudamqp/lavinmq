import * as helpers from './helpers.js'
import { test, expect } from './fixtures.js';

test.describe("federation", _ => {
  test('are loaded', async ({ page, baseURL }) => {
    const apiFederationUpstreamsRequest = helpers.waitForPathRequest(page, '/api/parameters/federation-upstream')
    const apiFederationLinksRequest = helpers.waitForPathRequest(page, '/api/federation-links')
    await page.goto('/federation')
    await expect(apiFederationUpstreamsRequest).toBeRequested()
    await expect(apiFederationLinksRequest).toBeRequested()
  })
})
