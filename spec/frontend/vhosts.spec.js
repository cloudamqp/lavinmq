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
    const formRequest = helpers.waitForPathRequest(page, `/api/vhosts/${vhostName}`, { method: 'PUT' })
    await page.goto('/vhosts')
    const form = await page.locator('#createVhost')
    await form.getByLabel('Name').fill(vhostName)
    await form.getByRole('button').click()
    expect(formRequest).toBeRequested()
  })

  test('keeps form values when add fails', async ({ page }) => {
    const vhostName = 'baz'
    await page.route(url => url.pathname === `/api/vhosts/${vhostName}`, async route => {
      if (route.request().method() === 'PUT') {
        await route.fulfill({
          status: 403,
          contentType: 'application/json',
          body: JSON.stringify({ error: 'access_refused', reason: 'No permission' })
        })
      } else {
        await route.fallback()
      }
    })
    page.on('dialog', dialog => dialog.dismiss())

    await page.goto('/vhosts')
    const form = page.locator('#createVhost')
    await form.getByLabel('Name').fill(vhostName)
    await form.getByRole('button').click()

    await expect(form.getByLabel('Name')).toHaveValue(vhostName)
  })
})
