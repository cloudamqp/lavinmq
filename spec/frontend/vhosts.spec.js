import * as helpers from './helpers.js'
import { test, expect } from './fixtures.js';

test.describe("vhosts", _ => {
  test('are loaded', async ({ page, baseURL, vhosts }) => {
    // vhosts are the vhosts returned in fixtures
    const apiPermissionsRequests = vhosts.map(v => page.waitForRequest(`${baseURL}/api/vhosts/${v}/permissions`))
    await page.goto('/vhosts')
    await expect(Promise.all(apiPermissionsRequests)).toBeRequested()
  })
})
