import * as helpers from './helpers.js'
import { test, expect } from './fixtures.js';

test.describe("vhosts", _ => {
  test('are loaded', async ({ page, vhosts }) => {
    const apiPermissionsRequests = vhosts.map(v => helpers.waitForPathRequest(page, `/api/vhosts/${v}/permissions`))
    await page.goto('/vhosts')
    await Promise.all(apiPermissionsRequests.map(req => expect(req).toBeRequested()))
  })
})
