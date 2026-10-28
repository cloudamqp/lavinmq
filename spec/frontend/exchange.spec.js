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

  test('renders the details once a refresh succeeds after a failed first load', async ({ page }) => {
    await page.clock.install()
    let failed = false
    await page.route(`**/${apiPath}`, route => {
      if (failed) return route.fallback()
      failed = true
      return route.fulfill({ status: 503, json: { reason: 'unavailable' } })
    })
    await page.goto(pagePath)
    await page.clock.runFor(5000)
    await expect(page.locator('#e-type')).toHaveText('topic')
  })

  test('rates chart keeps a readable plot in its narrow card', async ({ page }) => {
    await page.goto(pagePath)
    const plot = page.locator('#chart .u-over')
    await expect(plot).toBeVisible()
    const { height } = await plot.boundingBox()
    expect(height).toBeGreaterThanOrEqual(100)
  })

  test('rates legend labels fit in the narrow card', async ({ page }) => {
    // Busy rates make the widest values, e.g. "123,456.7 msgs/s".
    await page.route(`**/${apiPath}`, async route => {
      const json = await (await route.fetch()).json()
      for (const details of Object.values(json.message_stats)) {
        if (details?.log) details.log = details.log.map(() => 123456.7)
      }
      await route.fulfill({ json })
    })
    await page.goto(pagePath)
    await expect(page.locator('#chart .u-legend-value').first()).toHaveText('123,456.7 msgs/s')
    const labels = page.locator('#chart .u-legend-label')
    await expect(labels.first()).toBeVisible()
    const clipped = await labels.evaluateAll(els =>
      els.filter(el => el.scrollWidth > el.clientWidth).map(el => el.textContent))
    expect(clipped).toEqual([])
  })

  test('rates legend value column keeps its width while hovering shorter values', async ({ page }) => {
    // Only the latest sample is busy, so hovering earlier ones shows "0 msgs/s".
    await page.route(`**/${apiPath}`, async route => {
      const json = await (await route.fetch()).json()
      for (const details of Object.values(json.message_stats)) {
        if (details?.log) details.log = details.log.map((_, i, log) => i === log.length - 1 ? 123456.7 : 0)
      }
      await route.fulfill({ json })
    })
    await page.goto(pagePath)
    const value = page.locator('#chart .u-legend-value').first()
    await expect(value).toHaveText('123,456.7 msgs/s')
    const before = (await value.boundingBox()).width
    const plot = page.locator('#chart .u-over')
    const box = await plot.boundingBox()
    await plot.hover({ position: { x: box.width * 0.2, y: box.height / 2 } })
    await expect(value).toHaveText('0 msgs/s')
    expect((await value.boundingBox()).width).toBe(before)
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

  test('marks policy and arguments unsupported for x-mqtt-topic exchanges', async ({ page }) => {
    const exchangeResponse = {
      name: 'mlogs',
      vhost: '/',
      type: 'x-mqtt-topic',
      durable: true,
      auto_delete: false,
      internal: true,
      arguments: {},
      effective_arguments: [],
      message_stats: {},
      policy: 'all-queues'
    }
    const exchangeLoaded = helpers.waitForPathRequest(page, 'api/exchanges/%2F/mlogs', { response: exchangeResponse })
    await page.goto('/exchange#vhost=%2F&name=mlogs')
    await expect(exchangeLoaded).toBeRequested()
    await expect(page.locator('#e-policy')).toHaveText('Not supported')
    await expect(page.locator('#e-arguments')).toHaveText('Not supported by this exchange type')
  })

  test('hints MQTT filter syntax in the binding form for x-mqtt-topic exchanges', async ({ page }) => {
    const exchangeResponse = {
      name: 'mlogs',
      vhost: '/',
      type: 'x-mqtt-topic',
      durable: true,
      auto_delete: false,
      internal: true,
      arguments: {},
      effective_arguments: [],
      message_stats: {}
    }
    const exchangeLoaded = helpers.waitForPathRequest(page, 'api/exchanges/%2F/mlogs', { response: exchangeResponse })
    await page.goto('/exchange#vhost=%2F&name=mlogs')
    await expect(exchangeLoaded).toBeRequested()
    await expect(page.locator('#addBinding [name="routing_key"]')).toHaveAttribute('placeholder', 'MQTT topic filter, e.g. sensors/+/temp')
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
