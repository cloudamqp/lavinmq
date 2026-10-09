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
