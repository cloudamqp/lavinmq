import * as helpers from './helpers.js'
import { test, expect } from './fixtures.js';

test.describe("queue", _ => {
  test('info is loaded', async ({ page, baseURL }) => {
    const vhost = "/"
    const queueName = "foo"
    const apiQueueRequest = helpers.waitForPathRequest(page, `/api/queues/${encodeURIComponent(vhost)}/${queueName}`)
    await page.goto(`/queue#vhost=${encodeURIComponent(vhost)}&name=${queueName}`)
    await expect(apiQueueRequest).toBeRequested()
  })
  test('bindings are loaded', async ({ page, baseURL }) => {
    const vhost = "/"
    const queueName = "foo"
    const apiBindingsRequest = helpers.waitForPathRequest(page, `/api/queues/${encodeURIComponent(vhost)}/${queueName}/bindings`)
    await page.goto(`/queue#vhost=${encodeURIComponent(vhost)}&name=${queueName}`)
    await expect(apiBindingsRequest).toBeRequested()
  })
})
