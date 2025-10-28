import * as helpers from './helpers.js'
import { test, expect } from './fixtures.js';

test.describe("exchange", _ => {
  test('is refreshed', async ({ page, baseURL }) => {
    const apiExchangesRequest = helpers.waitForPathRequest(page, 'api/exchanges/%2F/amq.topic')
    await page.clock.install()
    await page.goto('/exchange#vhost=%2F&name=amq.topic')
    await expect(apiExchangesRequest).toBeRequested()
    const apiExchangesRequest2 = helpers.waitForPathRequest(page, 'api/exchanges/%2F/amq.topic')
    // Move into the future and make sure we've had a second request
    await page.clock.runFor(60000)
    await expect(apiExchangesRequest2).toBeRequested()
  })

  test('navigation to another exchange via binding link reloads the page', async ({ page, apimap }) => {
    const sourceExchange = 'source-exchange'
    const destExchange = 'dest-exchange'
    const vhost = '/'
    
    const sourceExchangeResponse = {
      "name": sourceExchange,
      "vhost": vhost,
      "type": "topic",
      "durable": true,
      "auto_delete": false,
      "internal": false,
      "arguments": {},
      "effective_arguments": [],
      "message_stats": {}
    }
    
    const destExchangeResponse = {
      "name": destExchange,
      "vhost": vhost,
      "type": "direct",
      "durable": true,
      "auto_delete": false,
      "internal": false,
      "arguments": {},
      "effective_arguments": [],
      "message_stats": {}
    }
    
    const bindingsResponse = {
      "items": [
        {
          "source": sourceExchange,
          "vhost": vhost,
          "destination": destExchange,
          "destination_type": "exchange",
          "routing_key": "test.key",
          "arguments": {},
          "properties_key": "test.key"
        }
      ],
      "filtered_count": 1,
      "item_count": 1,
      "page": 1,
      "page_count": 1,
      "page_size": 100,
      "total_count": 1
    }
    
    // Set up API mocks
    await apimap.get(`/api/exchanges/${encodeURIComponent(vhost)}/${sourceExchange}`, sourceExchangeResponse)
    await apimap.get(`/api/exchanges/${encodeURIComponent(vhost)}/${sourceExchange}/bindings/source`, bindingsResponse)
    
    // Navigate to source exchange
    await page.goto(`/exchange#vhost=${encodeURIComponent(vhost)}&name=${sourceExchange}`)
    await expect(page.locator('#pagename-label')).toContainText(sourceExchange)
    
    // Set up mock for destination exchange (for when we click the link)
    await apimap.get(`/api/exchanges/${encodeURIComponent(vhost)}/${destExchange}`, destExchangeResponse)
    await apimap.get(`/api/exchanges/${encodeURIComponent(vhost)}/${destExchange}/bindings/source`, { items: [] })
    
    // Listen for page load event to verify reload happens
    const pageLoadPromise = page.waitForEvent('load')
    
    // Click on the exchange binding link
    await page.locator('#bindings-table tbody tr td a').filter({ hasText: destExchange }).click()
    
    // Wait for page to reload
    await pageLoadPromise
    
    // Verify we're now on the destination exchange page
    await expect(page).toHaveURL(new RegExp(`exchange#vhost=${encodeURIComponent(vhost)}&name=${destExchange}`))
  })
})
