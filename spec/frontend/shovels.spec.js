import * as helpers from './helpers.js'
import { test, expect } from './fixtures.js';

test.describe("shovels", _ => {
  const shovelVhost = '/';
  const shovelName = 'shovel1'
  const parameterShovelsResponse = {
    "items": [
      {
        "name": shovelName,
        "value": {"src-uri":"amqp://","dest-uri":"amqp://","src-prefetch-count":1000,"src-delete-after":"never","reconnect-delay":120,"ack-mode":"on-confirm","src-queue":"qdest","dest-queue":"qsrc"},
        "component": "shovel",
        "vhost": shovelVhost
      }
    ]
    ,"filtered_count": 1,
    "item_count": 1,
    "page": 1,
    "page_count": 1,
    "page_size": 100,
    "total_count": 1
  }
  const shovelsResponse = [
    {"name": shovelName, "vhost": shovelVhost, "state": "Running", "error": null, "message_count": 0},
  ]

  test.beforeEach(async ({ apimap, page }) => {
    const parameterShovelsRequest = apimap.get(`/api/parameters/shovel`, parameterShovelsResponse)
    const shovelsRequest = apimap.get(`/api/shovels`, shovelsResponse)
    await page.clock.install()
    await page.goto(`/shovels`)
    await parameterShovelsRequest
    await shovelsRequest
  })


  test('are loaded', async ({ page, baseURL }) => {
    await expect(page.locator('#pagename-label')).toHaveText("1")
  })

  test('are refreshed automatically', async({ page }) => {
    // Verify that at least 3 requests are made
    for (let i=0; i<3; i++) {
      const apiShovelsRequest = helpers.waitForPathRequest(page, '/api/parameters/shovel')
      await page.clock.runFor(10000) // advance time by 10 seconds
      await expect(apiShovelsRequest).toBeRequested()
    }
  })

  test('can be deleted', async ({ page }) => {
    const actionPath = `/api/parameters/shovel/${encodeURIComponent(shovelVhost)}/${encodeURIComponent(shovelName)}`
    const deleteRequest = helpers.waitForPathRequest(page, actionPath, { method: 'DELETE' })
    page.on('dialog', async dialog => await dialog.accept())
    await page.locator(`#table tr[data-name='"${shovelName}"']`).getByRole('button', { name: /delete/i }).click()
    await expect(deleteRequest).toBeRequested()
  })
})
