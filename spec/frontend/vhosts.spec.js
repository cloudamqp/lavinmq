import * as helpers from './helpers.js'
import { test, expect } from './fixtures.js';

test.describe("vhosts", _ => {
  test('are loaded', async ({ page, vhosts }) => {
    const apiPermissionsRequests = vhosts.map(v => helpers.waitForPathRequest(page, `/api/vhosts/${v}/permissions`))
    await page.goto('/vhosts')
    await Promise.all(apiPermissionsRequests.map(req => expect(req).toBeRequested()))
  })

  test('can be added through form', async ({ page }) => {
    const vhostName = 'baz'
    const formRequest = helpers.waitForPathRequest(page, `/api/vhostsi/${vhostName}`, { method: 'PUT' })
    await page.goto('/vhosts')
    const form = await page.locator('#createVhost')
    await form.getByLabel('Name').fill(vhostName)
    await form.getByRole('button').click()
    expect(formRequest).toBeRequested()
  })
})
