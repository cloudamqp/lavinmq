import * as helpers from './helpers.js'
import { test, expect } from './fixtures.js';

test.describe("exchanges", _ => {
  test('are loaded', async ({ page, baseURL }) => {
    const apiExchangesRequest = helpers.waitForPathRequest(page, '/api/exchanges')
    await page.goto('/exchanges')
    await expect(apiExchangesRequest).toBeRequested()
  })

  test('are refreshed automatically', async({ page }) => {
    await page.clock.install()
    await page.goto('/exchanges')
    // Verify that at least 3 requests are made
    for (let i=0; i<3; i++) {
      const apiExchangesRequest = helpers.waitForPathRequest(page, '/api/exchanges')
      await page.clock.runFor(10000) // advance time by 10 seconds
      await expect(apiExchangesRequest).toBeRequested()
    }
  })

  // Only a boolean x-delayed-exchange makes an exchange delayed
  test('marks only an exchange with x-delayed-exchange=true as delayed', async ({ page }) => {
    const exchange = (name, value) => ({
      name,
      vhost: '/',
      type: 'topic',
      durable: false,
      auto_delete: false,
      internal: false,
      arguments: { 'x-delayed-exchange': value },
      effective_arguments: [],
      message_stats: {}
    })
    const items = [exchange('bool-delayed', true), exchange('string-delayed', 'true')]
    await page.route(url => url.pathname.startsWith('/api/exchanges'), route => route.fulfill({
      json: { items, filtered_count: 2, item_count: 2, page: 1, page_count: 1, page_size: 100, total_count: 2 }
    }))
    await page.goto('/exchanges')
    const delayedOf = name => page.locator('#table tr', { hasText: name }).locator('.features [title="Delayed"]')
    await expect(delayedOf('bool-delayed')).toHaveCount(1)
    await expect(delayedOf('string-delayed')).toHaveCount(0)
  })
})
