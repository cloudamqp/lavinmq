import * as helpers from './helpers.js'
import { test, expect } from './fixtures.js';

test.describe("user", _ => {
  const permissionsResponse = [
    {"user":"guest","vhost":"/","configure":".*","read":".*","write":".*"},
    {"user":"guest","vhost":"foo","configure":".*","read":".*","write":".*"},
    {"user":"guest","vhost":"bar","configure":".*","read":".*","write":".*"}
  ]

  test('is loaded', async ({ page }) => {
    const apiUserRequest = helpers.waitForPathRequest(page, '/api/users/guest')
    await page.goto('/user#name=guest')
    await expect(apiUserRequest).toBeRequested()
  })

  test('permissions are loaded', async ({ page }) => {
    const apiPermissionRequest = helpers.waitForPathRequest(page, '/api/users/guest/permissions')
    await page.goto('/user#name=guest')
    await expect(apiPermissionRequest).toBeRequested()
  })

  test('permission can be removed', async ({ page }) => {
    const permission = permissionsResponse[1]
    const apiPermissionRequest = helpers.waitForPathRequest(page, `/api/users/${permission.user}/permissions`, { response: permissionsResponse })
    await page.goto(`/user#name=${permission.user}`)
    await expect(apiPermissionRequest).toBeRequested()
    const apiDeletePermissionsRequest = helpers.waitForPathRequest(page,
      `/api/permissions/${permission.vhost}/${permission.user}`, { method: 'DELETE' })
    await page.locator(`#permissions tr[data-vhost='"${permission.vhost}"']`).getByRole('button', { name: /clear/i }).click()
    await expect(apiDeletePermissionsRequest).toBeRequested()
  })
})
