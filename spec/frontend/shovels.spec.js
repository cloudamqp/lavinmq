import * as helpers from './helpers.js'
import { test, expect } from './fixtures.js';

test.describe("shovels", _ => {
  const shovelVhost = 'foo'
  const shovelName = 'shovel1'
  const httpShovelName = 'httpshovel'
  const parameterShovelsResponse = {
    "items": [
      {
        "name": shovelName,
        "value": {"src-uri":"amqp://","dest-uri":"amqp://","src-prefetch-count":1000,"src-delete-after":"never","reconnect-delay":120,"ack-mode":"on-confirm","src-queue":"qdest","dest-queue":"qsrc"},
        "component": "shovel",
        "vhost": shovelVhost
      },
      {
        "name": httpShovelName,
        "value": {"src-uri":"amqp://","dest-uri":"http://example.com/hook","src-prefetch-count":1000,"src-delete-after":"never","reconnect-delay":120,"ack-mode":"on-confirm","src-queue":"qsrc"},
        "component": "shovel",
        "vhost": shovelVhost
      }
    ]
    ,"filtered_count": 2,
    "item_count": 2,
    "page": 1,
    "page_count": 1,
    "page_size": 100,
    "total_count": 2
  }
  const shovelsResponse = [
    {"name": shovelName, "vhost": shovelVhost, "state": "Running", "error": null, "message_count": 0},
    {"name": httpShovelName, "vhost": shovelVhost, "state": "Running", "error": null, "message_count": 0},
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
    await expect(page.locator('#pagename-label')).toHaveText("2")
  })

  test('are refreshed automatically', async({ page }) => {
    // Verify that at least 3 requests are made
    for (let i=0; i<3; i++) {
      const apiShovelsRequest = helpers.waitForPathRequest(page, '/api/parameters/shovel')
      await page.clock.runFor(10000) // advance time by 10 seconds
      await expect(apiShovelsRequest).toBeRequested()
    }
  })

  test('with http destination keep http destination when updated', async ({ page }) => {
    const actionPath = `/api/parameters/shovel/${encodeURIComponent(shovelVhost)}/${encodeURIComponent(httpShovelName)}`
    const putRequest = helpers.waitForPathRequest(page, actionPath, { method: 'PUT' })
    await page.locator(`#table tr[data-name='"${httpShovelName}"']`).getByRole('button', { name: /^edit$/i }).click()
    await page.locator('#createShovel').getByRole('button', { name: /update/i }).click()
    const body = (await putRequest).postDataJSON()
    expect(body.value['dest-uri']).toBe('http://example.com/hook')
    expect(body.value).not.toHaveProperty('dest-exchange')
    expect(body.value).not.toHaveProperty('dest-queue')
  })

  test('can be deleted', async ({ page }) => {
    const actionPath = `/api/parameters/shovel/${encodeURIComponent(shovelVhost)}/${encodeURIComponent(shovelName)}`
    const deleteRequest = helpers.waitForPathRequest(page, actionPath, { method: 'DELETE' })
    page.on('dialog', async dialog => await dialog.accept())
    await page.locator(`#table tr[data-name='"${shovelName}"']`).getByRole('button', { name: /delete/i }).click()
    await expect(deleteRequest).toBeRequested()
  })
})
