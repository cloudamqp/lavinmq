import * as helpers from './helpers.js'
import { test, expect } from './fixtures.js';

const consumersResponse = {
"items":[
  {
    "queue":{
      "name":"queue_foo",
      "vhost":"foo"
     },
    "consumer_tag":"consumer_foo",
    "exclusive":false,
    "ack_required":true,
    "prefetch_count":1000,
    "priority":0,
    "channel_details":{"peer_host":"127.0.0.1","peer_port":56847,"connection_name":"connection_foo","user":"guest","number":1,"name":"consumer_foo"}}
  ],
  "filtered_count":1,
  "item_count":1,
  "page":1,
  "page_count":1,
  "page_size":100,
  "total_count":1
}

test.describe("consumers", _ => {
  test.describe("with one consumer in response", () => {
    // Setup payload for each request
    test.beforeEach(async ({ page }) => {
      await page.route(/\/api\/consumers(?!\/)/, async route => {
        if (route.request().method() == 'GET')
          await route.fulfill({ json: consumersResponse })
        else
          await route.continue()
      })
      await page.goto(`/consumers`)
    })

    test('are loaded', async ({ page, baseURL }) => {
      // Verify that one consumers has been loaded
      expect(page.locator('#pagename-label')).toHaveText('1')
    })

    test('cancel consumer button trigger DELETE to api/consumers/<vhost>/<conn>/<channel>/<consumer_tag>', async ({ page, baseURL }) => {
      // Just accept the 'Are you sure?' dialog
      page.on('dialog', async dialog => await dialog.accept())
      const apiConsumerCancelRequest = helpers.waitForPathRequest(page, `/api/consumers/foo/connection_foo/1/consumer_foo`, { method: 'DELETE' })
      await page.locator('#table').getByRole('button', { name: /cancel/i }).click()
      await expect(apiConsumerCancelRequest).toBeRequested()
    })
  })
})

