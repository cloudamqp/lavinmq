import * as helpers from './helpers.js'
import { test, expect } from './fixtures.js';

test.describe("policies", _ => {
  const policiesResponse = {
    "items":[
      {"name":"p1","vhost":"/","pattern":"^a","apply-to":"queues","priority":0,"definition":{}},
      {"name":"p2","vhost":"/","pattern":"^b.","apply-to":"queues","priority":0,"definition":{}}
    ],"filtered_count":2,"item_count":2,"page":1,"page_count":1,"page_size":100,"total_count":2
  }

  test.beforeEach(async ({ apimap, page }) => {
    await page.clock.install()
    const policiesLoaded = apimap.get(`/api/policies`, policiesResponse)
    await page.goto(`/policies`)
    await page.clock.runFor(10000)
    await policiesLoaded
  })


  test('are loaded', async ({ page, baseURL }) => {
    await expect(page.locator('#pagename-label')).toHaveText("2")
  })

  test('are refreshed automatically', async({ page }) => {
    // Verify that at least 3 requests are made
    for (let i=0; i<3; i++) {
      const apiPoliciesRequest = helpers.waitForPathRequest(page, '/api/policies')
      await page.clock.runFor(10000) // advance time by 10 seconds
      await expect(apiPoliciesRequest).toBeRequested()
    }
  })

  test('can be deleted', async ({ page }) => {
    const policy = policiesResponse.items[0]
    const policyName = policy.name
    const vhost = encodeURIComponent(policy.vhost)
    const actionPath = `/api/policies/${vhost}/${policyName}`
    const deleteRequest = helpers.waitForPathRequest(page, actionPath, { method: 'DELETE' })
    page.on('dialog', async dialog => await dialog.accept())
    await page.locator(`#table tr[data-name='"p1"']`).getByRole('button', { name: /delete/i }).click()
    await expect(deleteRequest).toBeRequested()
  })
})
