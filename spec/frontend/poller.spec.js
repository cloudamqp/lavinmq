import { test, expect } from './fixtures.js'

function setTabHidden (page, hidden) {
  return page.evaluate(hidden => {
    Object.defineProperty(document, 'hidden', { configurable: true, get: () => hidden })
    Object.defineProperty(document, 'visibilityState', { configurable: true, get: () => hidden ? 'hidden' : 'visible' })
    document.dispatchEvent(new Event('visibilitychange'))
  }, hidden)
}

function countRequests (page, path) {
  const counter = { count: 0 }
  page.on('request', request => {
    if (new URL(request.url()).pathname === path) counter.count++
  })
  return counter
}

async function advance (page, ms) {
  for (let elapsed = 0; elapsed < ms; elapsed += 5000) {
    await page.clock.runFor(5000)
    await page.waitForTimeout(100)
  }
}

test.describe('poller', _ => {
  for (const { name, path, api } of [
    { name: 'a page', path: '/', api: '/api/overview' },
    { name: 'a table', path: '/queues', api: '/api/queues' }
  ]) {
    test(`stops polling ${name} while the tab is hidden and refreshes when it is shown`, async ({ page }) => {
      await page.clock.install()
      const requests = countRequests(page, api)
      const loaded = page.waitForResponse(response => new URL(response.url()).pathname === api)
      await page.goto(path)
      await loaded
      expect(requests.count).toBe(1)

      await setTabHidden(page, true)
      await advance(page, 30000)
      expect(requests.count).toBe(1)

      await setTabHidden(page, false)
      await expect.poll(() => requests.count).toBe(2)
    })
  }

  test('does not send a request again while it is still pending', async ({ page }) => {
    await page.clock.install()
    const overview = countRequests(page, '/api/overview')
    let respond
    const pending = new Promise(resolve => { respond = resolve })
    await page.route('**/api/overview', async route => {
      await pending
      await route.fulfill({ status: 503 })
    })
    await page.goto('/')
    await expect.poll(() => overview.count).toBe(1)

    await advance(page, 20000)
    expect(overview.count).toBe(1)

    const settled = page.waitForResponse(response => new URL(response.url()).pathname === '/api/overview')
    respond()
    await settled
    await advance(page, 10000)
    await expect.poll(() => overview.count).toBeGreaterThan(1)
  })

  test('does not refresh on every switch back to the tab', async ({ page }) => {
    await page.clock.install()
    const overview = countRequests(page, '/api/overview')
    const loaded = page.waitForResponse(response => new URL(response.url()).pathname === '/api/overview')
    await page.goto('/')
    await loaded

    for (let i = 0; i < 10; i++) {
      await setTabHidden(page, true)
      await setTabHidden(page, false)
    }
    await page.waitForTimeout(100)
    expect(overview.count).toBe(1)

    await advance(page, 5000)
    await expect.poll(() => overview.count).toBe(2)
  })
})

test.describe('refresh control', _ => {
  async function loadOverview (page) {
    const loaded = page.waitForResponse(response => new URL(response.url()).pathname === '/api/overview')
    await page.goto('/')
    await loaded
  }

  test('pause stops refreshing and resume refreshes at once', async ({ page }) => {
    await page.clock.install()
    const overview = countRequests(page, '/api/overview')
    await loadOverview(page)

    await page.locator('#refresh-toggle').click()
    await expect(page.locator('#refresh-toggle')).toHaveAttribute('aria-pressed', 'true')
    await advance(page, 30000)
    expect(overview.count).toBe(1)

    await page.locator('#refresh-toggle').click()
    await expect.poll(() => overview.count).toBe(2)
  })

  test('stays paused across a reload of the tab', async ({ page }) => {
    await page.clock.install()
    await loadOverview(page)
    await page.locator('#refresh-toggle').click()

    const overview = countRequests(page, '/api/overview')
    await page.reload()
    await expect(page.locator('#refresh-toggle')).toHaveAttribute('aria-pressed', 'true')
    await expect.poll(() => overview.count).toBe(1)
    await advance(page, 30000)
    expect(overview.count).toBe(1)
  })

  test('refreshes at the selected rate and remembers it', async ({ page }) => {
    await page.clock.install()
    await loadOverview(page)
    await page.locator('#refresh-rate').selectOption('30000')

    const overview = countRequests(page, '/api/overview')
    await page.reload()
    await expect(page.locator('#refresh-rate')).toHaveValue('30000')
    await expect.poll(() => overview.count).toBe(1)
    await advance(page, 25000)
    expect(overview.count).toBe(1)
    await advance(page, 5000)
    await expect.poll(() => overview.count).toBe(2)
  })

  test('sweeps a ring over the time until the next refresh', async ({ page }) => {
    const sweep = () => page.evaluate(() =>
      document.querySelector('.refresh-ring').getAnimations().map(a => a.effect.getTiming().duration))
    await loadOverview(page)
    await expect.poll(sweep).toEqual([5000])

    await page.locator('#refresh-rate').selectOption('30000')
    await expect.poll(sweep).toEqual([30000])

    await page.locator('#refresh-toggle').click()
    await expect.poll(sweep).toEqual([])
  })

  test('moves the ring in steps when reduced motion is preferred', async ({ page }) => {
    await page.emulateMedia({ reducedMotion: 'reduce' })
    await loadOverview(page)
    const easing = () => page.evaluate(() =>
      document.querySelector('.refresh-ring').getAnimations().map(a => a.effect.getTiming().easing))
    await expect.poll(easing).toEqual(['steps(8)'])
  })

  test('labels the toggle with what it will do', async ({ page }) => {
    await loadOverview(page)
    const toggle = page.locator('#refresh-toggle')
    await expect(toggle).toHaveAttribute('aria-label', 'Pause auto-refresh')
    await toggle.click()
    await expect(toggle).toHaveAttribute('aria-label', 'Resume auto-refresh')
  })

  test('is hidden on a page that does not refresh', async ({ page }) => {
    await page.goto('/logs')
    await expect(page.locator('#refresh-control')).toBeHidden()
  })

  test('steps through reconnecting to stale while the server fails, and recovers', async ({ page }) => {
    await page.clock.install()
    await loadOverview(page)
    const control = page.locator('#refresh-control')
    await expect(control).toHaveAttribute('data-state', 'live')

    await page.route('**/api/overview', route => route.fulfill({ status: 503, json: { reason: 'Server is starting' } }))
    const pillBorder = () => page.locator('.refresh-pill').evaluate(pill => getComputedStyle(pill).borderColor)
    const pulse = () => page.locator('.refresh-ring').evaluate(ring => getComputedStyle(ring).animationName)
    const liveBorder = await pillBorder()
    await advance(page, 5000)
    await expect(control).toHaveAttribute('data-state', 'reconnecting')
    expect(await pulse()).toBe('refresh-pulse')
    await advance(page, 5000)
    await expect(control).toHaveAttribute('data-state', 'stale')
    await expect(control).toHaveAttribute('title', /\nLast error: Server is starting/)
    expect(await pulse()).toBe('refresh-pulse')
    expect(await pillBorder()).toBe(liveBorder)

    await page.unroute('**/api/overview')
    await advance(page, 5000)
    await expect(control).toHaveAttribute('data-state', 'live')
  })

  for (const status of [404, 500]) {
    test(`counts a ${status} as reachable`, async ({ page }) => {
      await page.clock.install()
      await loadOverview(page)
      await page.route('**/api/overview', route => route.fulfill({ status, json: { reason: 'Nope' } }))
      await advance(page, 10000)
      await expect(page.locator('#refresh-control')).toHaveAttribute('data-state', 'live')
    })
  }

  test('a request that never answers stops showing live', async ({ page }) => {
    await page.clock.install()
    await loadOverview(page)
    await page.route('**/api/overview', () => {})
    await advance(page, 10000)
    const control = page.locator('#refresh-control')
    await expect(control).not.toHaveAttribute('data-state', 'live')
    await expect(control).toHaveAttribute('title', /No response for/)
  })

  test('shows going offline at once and refreshes when back online', async ({ page, context }) => {
    await page.clock.install()
    const overview = countRequests(page, '/api/overview')
    await loadOverview(page)
    const control = page.locator('#refresh-control')

    await context.setOffline(true)
    await expect(control).toHaveAttribute('data-state', 'reconnecting')
    await expect(control).toHaveAttribute('title', /Browser is offline/)

    await context.setOffline(false)
    await expect.poll(() => overview.count).toBe(2)
    await expect(control).toHaveAttribute('data-state', 'live')
  })

  test('a hung refresh stays a problem while other refreshes succeed', async ({ page }) => {
    await page.clock.install()
    const responseFor = path => page.waitForResponse(response => new URL(response.url()).pathname === path)
    const exchangePath = '/api/exchanges/%2F/amq.topic'
    const bindingsPath = '/api/exchanges/%2F/amq.topic/bindings/source'
    const loaded = Promise.all([responseFor(exchangePath), responseFor(bindingsPath)])
    await page.goto('/exchange#vhost=%2F&name=amq.topic')
    await loaded
    await page.route(`**${exchangePath}`, () => {})
    const control = page.locator('#refresh-control')
    const states = []
    for (let i = 0; i < 5; i++) {
      const bindings = responseFor(bindingsPath)
      await page.clock.runFor(5000)
      await bindings
      states.push(await control.getAttribute('data-state'))
    }
    const firstProblem = states.findIndex(state => state !== 'live')
    expect(firstProblem).toBeGreaterThan(-1)
    expect(states.slice(firstProblem)).not.toContain('live')
    expect(states.at(-1)).toBe('stale')
  })

  test('a slow refresh that answers in the end counts as a success', async ({ page }) => {
    await page.clock.install()
    const responseFor = path => page.waitForResponse(response => new URL(response.url()).pathname === path)
    const exchangePath = '/api/exchanges/%2F/amq.topic'
    const bindingsPath = '/api/exchanges/%2F/amq.topic/bindings/source'
    const loaded = Promise.all([responseFor(exchangePath), responseFor(bindingsPath)])
    await page.goto('/exchange#vhost=%2F&name=amq.topic')
    await loaded
    let release
    const gate = new Promise(resolve => { release = resolve })
    await page.route(`**${exchangePath}`, async route => { await gate; await route.fallback() })
    const control = page.locator('#refresh-control')
    for (let i = 0; i < 2; i++) {
      const bindings = responseFor(bindingsPath)
      await page.clock.runFor(5000)
      await bindings
    }
    await expect(control).not.toHaveAttribute('data-state', 'live')

    const answered = responseFor(exchangePath)
    release()
    await answered
    await expect(control).toHaveAttribute('data-state', 'live')
  })
})
