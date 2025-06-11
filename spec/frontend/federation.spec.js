import * as helpers from './helpers.js'
import { test, expect } from './fixtures.js';

const upstreamsResponse = {
  "items":[
    {
      "name":"fe",
      "value":{"uri":"amqp:///foo","prefetch-count":1000,"reconnect-delay":5,"ack-mode":"on-confirm","exchange":"","max-hops":null,"expires":null,"message-ttl":null,"queue":"","consumer-tag":""},
      "component":"federation-upstream",
      "vhost":"bar"
    }
  ],
  "filtered_count":1,
  "item_count":1,
  "page":1,
  "page_count":1,
  "page_size":100,
  "total_count":1
}

const linksResponse = {
  "items":[
    {
      "upstream":"fe","vhost":"bar","timestamp":"2025-06-11T11:41:34Z",
      "type":"queue","uri":"amqp:///foo","resource":"qdown","error":null,
      "status":"running","consumer-tag":"federation-link-fe"
    }
  ],"filtered_count":1,"item_count":1,"page":1,"page_count":1,"page_size":100,"total_count":1
}

test.describe("federation", _ => {
  test.beforeEach(async ({ page, apimap }) => {
    const upstreamsLoaded = apimap.get('/api/parameters/federation-upstream', upstreamsResponse)
    const linksLoaded = apimap.get('/api/federation-links', linksResponse)
    page.goto('/federation')
    await upstreamsLoaded
    await linksLoaded
  })

  test('are loaded', async ({ page, baseURL }) => {
    await expect(page.locator('#pagename-label')).toHaveText('1')
    await expect(page.locator('#links-count')).toHaveText('1')
  })

  test('upstream can be deleted', async ({ page }) => {
    const upstream = upstreamsResponse.items[0]
    const deleteRequest = helpers.waitForPathRequest(
      page,
      `/api/parameters/federation-upstream/${upstream.vhost}/${upstream.name}`,
      {method: 'DELETE'}
    )
    page.on('dialog', async dialog => await dialog.accept())
    await page.locator('#upstreamTable').getByRole('button', { name: /delete/i }).click()
    await expect(deleteRequest).toBeRequested()
  })
})
