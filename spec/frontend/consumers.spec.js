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
  test('are loaded', async ({ page, baseURL }) => {
    const apiConsumersRequest = helpers.waitForPathRequest(page, `/api/consumers`)
    await page.goto(`/consumers`)
    await expect(apiConsumersRequest).toBeRequested()
  })

  test('cancel consumer button trigger DELETE to api/consumers/<vhost>/<conn>/<channel>/<consumer_tag>', async ({ page, baseURL }) => {
    const apiConsumersRequest = helpers.waitForPathRequest(page, `/api/consumers`, { response: consumersResponse })
    await page.goto(`/consumers`)
    await expect(apiConsumersRequest).toBeRequested()
    const apiConsumerCancelRequest = helpers.waitForPathRequest(page, `/api/consumers/foo/connection_foo/1/consumer_foo`, { method: 'DELETE' })
    page.on('dialog', async dialog => await dialog.accept())
    await page.locator('#table').getByRole('button', { name: /cancel/i }).click()
    await expect(apiConsumerCancelRequest).toBeRequested()
  })
})
 
