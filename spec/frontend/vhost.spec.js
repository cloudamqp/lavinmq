import * as helpers from './helpers.js'
import { test, expect } from './fixtures.js';

test.describe("vhost", _ => {
  const vhostName = 'foo'
  const permissionsResponse = [
    {"user":"guest","vhost":"foo","configure":".*","read":".*","write":".*"},
    {"user":"admin","vhost":"foo","configure":"","read":".*","write":".*"}
  ]
  const usersResponse = [
    {"name":"guest","tags":"administrator"},
    {"name":"admin","tags":""}
  ]

  test.beforeAll(async ({ request }) => {
    await request.put(`/api/vhosts/${vhostName}`)
  })

  test.afterAll(async ({ request }) => {
    await request.delete(`/api/vhosts/${vhostName}`)
  })

  test('permissions are loaded', async ({ page }) => {
    const apiPermissionsRequest = helpers.waitForPathRequest(page, `/api/vhosts/${vhostName}/permissions`, { response: permissionsResponse })
    await page.goto(`/vhost#name=${vhostName}`)
    await expect(apiPermissionsRequest).toBeRequested()
  })

  test('permissions are refreshed automatically', async ({ page }) => {
    await page.clock.install()
    await page.goto(`/vhost#name=${vhostName}`)
    for (let i = 0; i < 3; i++) {
      const apiPermissionsRequest = helpers.waitForPathRequest(page, `/api/vhosts/${vhostName}/permissions`)
      await page.clock.runFor(10000)
      await expect(apiPermissionsRequest).toBeRequested()
    }
  })

  test('set permission form works', async ({ page }) => {
    const apiPermissionsRequest = helpers.waitForPathRequest(page, `/api/vhosts/${vhostName}/permissions`, { response: permissionsResponse })
    const apiUsersRequest = helpers.waitForPathRequest(page, '/api/users', { response: usersResponse })
    await page.goto(`/vhost#name=${vhostName}`)
    await expect(apiPermissionsRequest).toBeRequested()
    await expect(apiUsersRequest).toBeRequested()

    const form = page.locator('#setPermission')
    await form.getByRole('combobox', { name: 'user' }).selectOption('admin')
    await form.getByRole('textbox', { name: 'configure' }).fill('.*')
    await form.getByRole('textbox', { name: 'write' }).fill('.*')
    await form.getByRole('textbox', { name: 'read' }).fill('.*')

    const apiSetPermissionRequest = helpers.waitForPathRequest(page, `/api/permissions/${vhostName}/admin`, { method: 'PUT' })
    await form.getByRole('button').click()
    await expect(apiSetPermissionRequest).toBeRequested()
  })

  test('set limits form works', async ({ page, apimap }) => {
    apimap.get(`/api/vhost/${vhostName}/permissions`, permissionsResponse)
    await page.goto(`/vhost#name=${vhostName}`)
    const apiSetMaxConnectionsRequest = helpers.waitForPathRequest(page, `/api/vhost-limits/${vhostName}/max-connections`, { method: 'PUT' })
    const apiSetMaxQueuesRequest = helpers.waitForPathRequest(page, `/api/vhost-limits/${vhostName}/max-queues`, { method: 'PUT' })

    const form = page.locator('#setLimits')
    await form.getByLabel('Max connections').fill('200')
    await form.getByLabel('Max queues').fill('100')
    await form.getByRole('button').click()

    await expect(apiSetMaxConnectionsRequest).toBeRequested()
    await expect(apiSetMaxQueuesRequest).toBeRequested()
  })

  test('refreshes limits after one limit update fails', async ({ page }) => {
    const errors = []
    let limitsRequests = 0
    const limitResponse = () => {
      limitsRequests += 1
      return [{
        value: limitsRequests === 1
          ? { 'max-connections': 10, 'max-queues': 20 }
          : { 'max-connections': 10, 'max-queues': 100 }
      }]
    }

    page.on('pageerror', err => errors.push(err.message))
    page.on('dialog', dialog => dialog.dismiss())
    await page.route(url => url.pathname === `/api/vhosts/${vhostName}`, async route => {
      await route.fulfill({ json: { messages_ready: 0, messages_unacknowledged: 0, messages: 0 } })
    })
    await page.route(url => url.pathname === `/api/vhosts/${vhostName}/permissions`, async route => {
      await route.fulfill({ json: permissionsResponse })
    })
    await page.route(url => url.pathname === '/api/users', async route => {
      await route.fulfill({ json: usersResponse })
    })
    await page.route(url => url.pathname === `/api/vhost-limits/${vhostName}`, async route => {
      await route.fulfill({ json: limitResponse() })
    })
    await page.route(url => url.pathname === `/api/vhost-limits/${vhostName}/max-connections`, async route => {
      await route.fulfill({
        status: 403,
        contentType: 'application/json',
        body: JSON.stringify({ error: 'access_refused', reason: 'No permission' })
      })
    })
    await page.route(url => url.pathname === `/api/vhost-limits/${vhostName}/max-queues`, async route => {
      await route.fulfill({ status: 204 })
    })

    await page.goto(`/vhost#name=${vhostName}`)
    await expect(page.locator('#max-connections')).toHaveText('10')
    await expect(page.locator('#max-queues')).toHaveText('20')

    const form = page.locator('#setLimits')
    await form.getByLabel('Max connections').fill('200')
    await form.getByLabel('Max queues').fill('100')
    await form.getByRole('button').click()

    await expect(page.locator('#max-connections')).toHaveText('10')
    await expect(page.locator('#max-queues')).toHaveText('100')
    expect(errors).toEqual([])
  })

  test('delete button works', async ({ page }) => {
    await page.goto(`/vhost#name=${vhostName}`)

    page.on('dialog', dialog => dialog.accept())

    const apiDeleteRequest = helpers.waitForPathRequest(page, `/api/vhosts/${vhostName}`, { method: 'DELETE' })
    const form = page.locator('#deleteVhost')
    await form.getByRole('button').click()
    await expect(apiDeleteRequest).toBeRequested()
  })

  test('permission can be removed', async ({ page }) => {
    const permission = permissionsResponse[1]
    const apiPermissionsRequest = helpers.waitForPathRequest(page, `/api/vhosts/${vhostName}/permissions`, { response: permissionsResponse })
    await page.goto(`/vhost#name=${vhostName}`)
    await expect(apiPermissionsRequest).toBeRequested()

    const apiDeletePermissionRequest = helpers.waitForPathRequest(page, `/api/permissions/${vhostName}/${permission.user}`, { method: 'DELETE' })
    await page.locator(`#permissions tr[data-user='"${permission.user}"']`).getByRole('button', { name: /clear/i }).click()
    await expect(apiDeletePermissionRequest).toBeRequested()
  })
})
