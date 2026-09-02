import * as helpers from './helpers.js'
import { test, expect } from './fixtures.js'

test.describe('exchange', _ => {
  const pagePath = '/exchange#vhost=%2F&name=amq.topic'
  const apiPath = 'api/exchanges/%2F/amq.topic'

  test('is loaded', async ({ page }) => {
    const apiExchangesRequest = helpers.waitForPathRequest(page, apiPath)
    await page.goto(pagePath)
    await expect(apiExchangesRequest).toBeRequested()
  })

  test('is refreshed automatically', async ({ page, baseURL }) => {
    await page.clock.install()
    await page.goto(pagePath)
    for (let i = 0; i < 3; ++i) {
      const apiExchangesRequest2 = helpers.waitForPathRequest(page, apiPath)
      // Move into the future and make sure we've had a second request
      await page.clock.runFor(10000)
      await expect(apiExchangesRequest2).toBeRequested()
    }
  })

  test('navigates to another exchange via binding link', async ({ page }) => {
    await page.goto(pagePath)

    // Simulate hash change to navigate to another exchange
    const targetExchange = 'amq.direct'
    const newHash = `#vhost=%2F&name=${targetExchange}`

    // Wait for the API request to the new exchange after hash change
    const apiNewExchangeRequest = helpers.waitForPathRequest(page, `api/exchanges/%2F/${targetExchange}`)

    // Change the hash to simulate clicking an exchange binding link
    await page.evaluate((hash) => {
      window.location.hash = hash
    }, newHash)

    // Verify the new exchange is loaded
    await expect(apiNewExchangeRequest).toBeRequested()
    await expect(page).toHaveURL(new RegExp(`exchange${newHash}`))
  })

  test('keeps binding form values and table unchanged when add fails', async ({ page }) => {
    const errors = []
    let bindingsReloaded = false
    const destination = 'missing.queue'
    const exchangeResponse = {
      name: 'amq.topic',
      vhost: '/',
      type: 'topic',
      durable: true,
      auto_delete: false,
      internal: false,
      arguments: {},
      effective_arguments: [],
      message_stats: {}
    }
    const bindingsResponse = {
      items: [],
      filtered_count: 0,
      item_count: 0,
      page: 1,
      page_count: 1,
      page_size: 100,
      total_count: 0
    }
    const bindingPath = '/api/bindings/%2F/e/amq.topic/q/missing.queue'
    const bindingsPath = `/${apiPath}/bindings/source`

    const exchangeLoaded = helpers.waitForPathRequest(page, apiPath, { response: exchangeResponse })
    const bindingsLoaded = helpers.waitForPathRequest(page, bindingsPath, { response: bindingsResponse })
    const queuesLoaded = helpers.waitForPathRequest(page, '/api/queues/%2F', { response: [] })

    page.on('pageerror', err => errors.push(err.message))
    page.on('dialog', dialog => dialog.dismiss())
    await page.goto(pagePath)
    await expect(exchangeLoaded).toBeRequested()
    await expect(bindingsLoaded).toBeRequested()
    await expect(queuesLoaded).toBeRequested()

    page.on('request', request => {
      const url = new URL(request.url())
      if (request.method() === 'GET' && decodeURIComponent(url.pathname) === decodeURIComponent(bindingsPath)) {
        bindingsReloaded = true
      }
    })
    await page.route(url => decodeURIComponent(url.pathname) === decodeURIComponent(bindingPath), async route => {
      await route.fulfill({
        status: 403,
        contentType: 'application/json',
        body: JSON.stringify({ error: 'access_refused', reason: 'No permission' })
      })
    })

    const form = page.locator('#addBinding')
    await form.locator('[name="destination"]').fill(destination)
    await form.getByLabel('Binding key').fill('rk')
    await form.getByLabel('Arguments').fill('{"x":1}')

    const failedRequest = page.waitForResponse(response => {
      const url = new URL(response.url())
      return response.request().method() === 'POST' && decodeURIComponent(url.pathname) === decodeURIComponent(bindingPath)
    })
    await form.getByRole('button', { name: /bind/i }).click()
    await failedRequest
    await page.waitForTimeout(100)

    await expect(form.locator('[name="destination"]')).toHaveValue(destination)
    await expect(form.getByLabel('Binding key')).toHaveValue('rk')
    await expect(form.getByLabel('Arguments')).toHaveValue('{"x":1}')
    expect(bindingsReloaded).toBe(false)
    expect(errors).toEqual([])
  })
})
