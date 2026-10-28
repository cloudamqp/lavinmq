import * as helpers from './helpers.js'
import { test, expect } from './fixtures.js'

test.describe('overview', _ => {
  test('is loaded', async ({ page }) => {
    const apiOverviewRequest = helpers.waitForPathRequest(page, '/api/overview')
    await page.goto('/')
    await expect(apiOverviewRequest).toBeRequested()
  })

  test('is refreshed automatically', async ({ page }) => {
    await page.clock.install()
    await page.goto('/')
    // Verify that at least 3 requests are made
    for (let i = 0; i < 3; i++) {
      const apiOverviewRequest = helpers.waitForPathRequest(page, '/api/overview')
      await page.clock.runFor(10000) // advance time by 10 seconds
      await expect(apiOverviewRequest).toBeRequested()
    }
  })

  test('counters and their footers line up across the overview card', async ({ page }) => {
    // Narrow enough that "View queues →" and the other links wrap, while
    // Bindings has no link and a one line footer.
    await page.setViewportSize({ width: 1100, height: 720 })
    await page.goto('/')
    await expect(page.locator('#uptime')).not.toHaveText('0s')
    const tops = sel => page.locator(sel).evaluateAll(els => els.map(el => Math.round(el.getBoundingClientRect().top)))
    const counters = await tops('#overview .counter:not(#uptime)')
    const footers = await tops('#overview .overview-col-footer')
    expect(new Set(counters).size, `counter tops ${counters}`).toBe(1)
    expect(new Set(footers).size, `footer tops ${footers}`).toBe(1)
  })

  test('legend click isolates a series', async ({ page }) => {
    await page.goto('/')
    const items = page.locator('#msgChart .u-legend-item')
    await expect(items).toHaveCount(2)

    await items.nth(0).click()
    await expect(items.nth(1)).toHaveClass(/u-legend-hidden/)
    await expect(items.nth(0)).not.toHaveClass(/u-legend-hidden/)

    await items.nth(0).click()
    await expect(items.nth(1)).not.toHaveClass(/u-legend-hidden/)
  })

  // Shift and Meta (cmd) are the macOS multi-select conventions.
  for (const modifier of ['Shift', 'Meta']) {
    test(`${modifier}-click toggles a single series without isolating`, async ({ page }) => {
      await page.goto('/')
      const items = page.locator('#msgChart .u-legend-item')
      await expect(items).toHaveCount(2)

      await items.nth(1).click({ modifiers: [modifier] })
      await expect(items.nth(1)).toHaveClass(/u-legend-hidden/)
      await expect(items.nth(0)).not.toHaveClass(/u-legend-hidden/)

      await items.nth(1).click({ modifiers: [modifier] })
      await expect(items.nth(1)).not.toHaveClass(/u-legend-hidden/)
      await expect(items.nth(0)).not.toHaveClass(/u-legend-hidden/)
    })
  }

  // ctrlKey is the Windows and Linux multi-select convention. It is asserted
  // by dispatching the event rather than driving real input, because macOS
  // turns a real ctrl+click into a context-menu gesture and the extra events
  // that produces are an input-layer artifact, not the binding under test.
  test('ctrlKey on a legend click toggles a single series without isolating', async ({ page }) => {
    await page.goto('/')
    const items = page.locator('#msgChart .u-legend-item')
    await expect(items).toHaveCount(2)

    const ctrlClickSecond = () => page.evaluate(() => {
      document.querySelectorAll('#msgChart .u-legend-item')[1]
        .dispatchEvent(new MouseEvent('click', { bubbles: true, ctrlKey: true }))
    })

    await ctrlClickSecond()
    await expect(items.nth(1)).toHaveClass(/u-legend-hidden/)
    await expect(items.nth(0)).not.toHaveClass(/u-legend-hidden/)

    await ctrlClickSecond()
    await expect(items.nth(1)).not.toHaveClass(/u-legend-hidden/)
    await expect(items.nth(0)).not.toHaveClass(/u-legend-hidden/)
  })

  test('hovering one chart moves the cursor and legend of every chart', async ({ page }) => {
    // Enough samples that a point at 40% across is not the newest one, so a
    // synced legend is distinguishable from one showing its own live value.
    const ramp = (step) => Array.from({ length: 30 }, (_, i) => (i + 1) * step)
    await page.route(/\/api\/overview(\?|$)/, async route => {
      const response = await route.fetch()
      const body = await response.json()
      body.queue_totals = {
        ...body.queue_totals,
        messages_ready: 300,
        messages_unacknowledged: 30,
        messages_ready_log: ramp(10),
        messages_unacknowledged_log: ramp(1)
      }
      body.send_oct_details = { rate: 3000, log: ramp(100) }
      body.recv_oct_details = { rate: 6000, log: ramp(200) }
      // Replace message_stats wholesale so every chart spans the same number of
      // samples; with differing ranges a synced x value can land outside a
      // chart and its cursor is suppressed.
      body.message_stats = {
        publish: 900,
        publish_details: { rate: 30, log: ramp(3) },
        deliver: 600,
        deliver_details: { rate: 20, log: ramp(2) }
      }
      await route.fulfill({ response, json: body })
    })
    // A frozen clock stops polling, which would otherwise re-time every sample
    // between reading one legend and comparing it with the others.
    await page.clock.install()
    await page.goto('/')

    const ids = ['msgChart', 'dataChart', 'rateChart']
    const legendTime = (id) => page.locator(`section.card:has(#${id}) .u-legend-time`)
    const cursor = (id) => page.locator(`#${id} .u-cursor-x`)
    for (const id of ids) await expect(legendTime(id)).toHaveText(/^\d{2}:\d{2}:\d{2}$/)
    const live = (await legendTime('rateChart').textContent()).trim()

    // hover() scrolls the plot into view first; page.mouse.move does not, and a
    // move to coordinates below the fold silently never enters the chart
    const plot = page.locator('#rateChart .u-over')
    const box = await plot.boundingBox()
    await plot.hover({ position: { x: box.width * 0.4, y: box.height / 2 } })

    await expect(legendTime('rateChart')).not.toHaveText(live)
    const hovered = (await legendTime('rateChart').textContent()).trim()
    for (const id of ids) {
      await expect(cursor(id)).toHaveCSS('display', 'block')
      await expect(legendTime(id)).toHaveText(hovered)
    }

    // leaving the plot returns every chart to its live value together
    await page.mouse.move(0, 0)
    for (const id of ids) await expect(cursor(id)).toHaveCSS('display', 'none')
    for (const id of ids) await expect(legendTime(id)).toHaveText(live)
  })

  test('chart timestamp shows the latest sample and the pause state', async ({ page }) => {
    await page.goto('/')
    const legendTime = page.locator('.u-legend-time').first()
    await expect(legendTime).toHaveText(/^\d{2}:\d{2}:\d{2}$/)
    await page.locator('#refresh-toggle').click()
    await expect(legendTime).toContainText('Paused at')
    await page.locator('#refresh-toggle').click()
    await expect(legendTime).toHaveText(/^\d{2}:\d{2}:\d{2}$/)
  })

  test('y axis shrinks after five polls below the range, not five redraws', async ({ page }) => {
    await page.goto('/')
    const maxes = await page.evaluate(async url => {
      const { stableMax } = await import(url)
      const handle = { scaleMax: 0, shrinkCount: 0, poll: 1 }
      stableMax(handle, 1000)
      const seen = []
      for (let poll = 2; poll <= 6; poll++) {
        handle.poll = poll
        for (let redraw = 0; redraw < 3; redraw++) seen.push(stableMax(handle, 100))
      }
      return seen
    }, '/js/chart.js')
    expect(maxes).toEqual([...new Array(12).fill(1000), 100, 100, 100])
  })

  test('definitions export trigger GET to /api/definitions for all vhosts', async ({ page }) => {
    const definitionsRequest = helpers.waitForPathRequest(page, '/api/definitions')
    await page.goto('/')
    await page.locator('#exportDefinitions').getByRole('button', { name: /download/i }).click()
    await expect(definitionsRequest).toBeRequested()
  })

  test('definitions export trigger GET to /api/definitions/<selected vhost>', async ({ page, vhostsLoaded }) => {
    await page.goto('/')
    const vhosts = await vhostsLoaded
    const definitionsRequest = helpers.waitForPathRequest(page, `/api/definitions/${vhosts[0]}`)
    await page.locator('#exportDefinitions').getByRole('combobox').selectOption(vhosts[0])
    await page.locator('#exportDefinitions').getByRole('button', { name: /download/i }).click()
    await expect(definitionsRequest).toBeRequested()
  })

  test('definitions import trigger POST to /api/definitions/upload', async ({ page }) => {
    const definitionsRequest = helpers.waitForPathRequest(page, '/api/definitions/upload', { method: 'POST' })
    await page.goto('/')

    const importDefs = page.locator('#importDefinitions')
    const fileChooserPromise = page.waitForEvent('filechooser')
    await importDefs.getByLabel('File').click()
    const fileChooser = await fileChooserPromise
    await fileChooser.setFiles({ name: 'definitions.json', buffer: Buffer.from('{}') })
    await importDefs.getByRole('button', { name: /upload/i }).click()

    await expect(definitionsRequest).toBeRequested()
  })
})
