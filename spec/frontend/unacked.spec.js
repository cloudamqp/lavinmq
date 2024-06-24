import * as helpers from './helpers.js'
import { test, expect } from './fixtures.js';

test.describe("queue/unacked", _ => {
  test('info is loaded', async ({ page, baseURL }) => {
    const vhost = "/"
    const queueName = "foo"
    const apiUnackedRequest = helpers.waitForPathRequest(page, `/api/queues/${encodeURIComponent(vhost)}/${queueName}/unacked`, {})
    await page.goto(`/unacked#vhost=${encodeURIComponent(vhost)}&name=${queueName}`)
    await expect(apiUnackedRequest).toBeRequested()
  })
})
