import * as helpers from './helpers.js'
import { test, expect } from './fixtures.js';

test.describe("queue/unacked", _ => {
  const vhost = "/"
  const queueName = "foo"
  const pagePath = `/unacked#vhost=${encodeURIComponent(vhost)}&name=${queueName}`
  const apiPath = `/api/queues/${encodeURIComponent(vhost)}/${queueName}/unacked`

  test('is loaded', async ({ page, baseURL }) => {
    const apiUnackedRequest = helpers.waitForPathRequest(page, apiPath)
    await page.goto(pagePath)
    await expect(apiUnackedRequest).toBeRequested()
  })

  test('refreshed automatically', async ({ page, baseURL }) => {
    await page.clock.install()
    await page.goto(pagePath)
    // Verify that at least 3 requests are made
    for (let i=0; i<3; i++) {
      const apiUnackedRequest = helpers.waitForPathRequest(page, apiPath)
      await page.clock.runFor(10000) // advance time by 10 seconds
      await expect(apiUnackedRequest).toBeRequested()
    }
  })
})
