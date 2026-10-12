import * as helpers from './helpers.js'
import { test, expect } from './fixtures.js'

async function fakeLogStream (page) {
  await page.addInitScript(() => {
    window.EventSource = class {
      static CLOSED = 2
      constructor () { window.__logStream = this; this.readyState = 0 }
      close () { window.__logStreamClosed = true }
    }
    window.__scrollIntoViewCalls = 0
    const scrollIntoView = Element.prototype.scrollIntoView
    Element.prototype.scrollIntoView = function (...args) {
      window.__scrollIntoViewCalls++
      return scrollIntoView.apply(this, args)
    }
  })
  return {
    open: () => page.evaluate(() => window.__logStream.onopen?.()),
    fail: ({ closed = false } = {}) => page.evaluate(closed => {
      window.__logStream.readyState = closed ? 2 : 0
      window.__logStream.onerror?.()
    }, closed),
    send: message => page.evaluate(message => window.__logStream.onmessage({
      lastEventId: String(Date.now()),
      data: JSON.stringify(['info', 'spec', message])
    }), message),
    sendMany: (count, prefix = 'line') => page.evaluate(([count, prefix]) => {
      for (let i = 0; i < count; i++) {
        window.__logStream.onmessage({ lastEventId: String(Date.now()), data: JSON.stringify(['info', 'spec', `${prefix} ${i}`]) })
      }
    }, [count, prefix])
  }
}

test.describe('logs', _ => {
  test('are loaded', async ({ page, baseURL }) => {
    const apiLogsRequest = page.waitForRequest(/\/api\/livelog$/)
    await page.goto('/logs')
    await expect(apiLogsRequest).toBeRequested()
  })

  test('paints a burst of lines in one batch', async ({ page }) => {
    const stream = await fakeLogStream(page)
    await page.goto('/logs')
    await stream.sendMany(300)
    await expect(page.locator('#livelog-body tr')).toHaveCount(300)
    expect(await page.evaluate(() => window.__scrollIntoViewCalls)).toBe(0)
    const atBottom = await page.locator('#livelog').evaluate(el => el.scrollHeight - el.scrollTop - el.clientHeight < 3)
    expect(atBottom).toBe(true)
  })

  test('keeps the table at ten thousand rows', async ({ page }) => {
    const stream = await fakeLogStream(page)
    await page.goto('/logs')
    const rows = page.locator('#livelog-body tr')
    await stream.sendMany(6000, 'first')
    await expect(rows).toHaveCount(6000)
    await stream.sendMany(6000, 'second')
    await expect(rows.last()).toContainText('second 5999')
    await expect(rows).toHaveCount(10000)
    await expect(rows.first()).toContainText('first 2000')
  })

  test('keeps the lines in view still while old rows are trimmed', async ({ page }) => {
    const stream = await fakeLogStream(page)
    await page.goto('/logs')
    const rows = page.locator('#livelog-body tr')
    await stream.sendMany(10000)
    await expect(rows).toHaveCount(10000)
    const anchor = await page.locator('#livelog').evaluate(log => {
      log.scrollTop = log.scrollHeight / 2
      log.dispatchEvent(new Event('scroll'))
      const box = log.getBoundingClientRect()
      const row = document.elementFromPoint(box.left + 20, box.top + box.height / 2).closest('tr')
      window.__anchor = row
      return { text: row.textContent, top: row.getBoundingClientRect().top }
    })
    await stream.sendMany(500, 'more')
    await expect(rows.last()).toContainText('more 499')
    const after = await page.evaluate(() => ({ connected: window.__anchor.isConnected, top: window.__anchor.getBoundingClientRect().top }))
    expect(after.connected).toBe(true)
    expect(Math.abs(after.top - anchor.top)).toBeLessThan(2)
  })

  test('closes the log stream when leaving the page', async ({ page }) => {
    await fakeLogStream(page)
    await page.goto('/logs')
    const closed = await page.evaluate(() => {
      window.dispatchEvent(new Event('beforeunload'))
      return window.__logStreamClosed === true
    })
    expect(closed).toBe(true)
  })

  test('pausing holds new lines until resumed', async ({ page }) => {
    const stream = await fakeLogStream(page)
    await page.goto('/logs')
    const rows = page.locator('#livelog-body tr')
    await stream.send('first')
    await expect(rows).toHaveCount(1)

    await page.locator('#refresh-toggle').click()
    await stream.send('second')
    await expect(rows).toHaveCount(1)

    await page.locator('#refresh-toggle').click()
    await expect(rows).toHaveCount(2)
    await expect(rows.last()).toContainText('second')
  })

  test('the refresh control keeps its layout and shows no ring for the stream', async ({ page }) => {
    const stream = await fakeLogStream(page)
    await page.goto('/logs')
    await expect(page.locator('#refresh-control')).toBeVisible()
    await expect(page.locator('#refresh-rate')).toBeHidden()
    await expect(page.locator('#refresh-live')).toHaveText('Live')
    await expect(page.locator('.refresh-ring')).toBeHidden()
    await stream.fail()
    await expect(page.locator('#refresh-control')).toHaveAttribute('data-state', 'reconnecting')
    await expect(page.locator('.refresh-ring')).toBeHidden()
  })

  test('the refresh control talks about the log stream', async ({ page }) => {
    const stream = await fakeLogStream(page)
    await page.goto('/logs')
    await stream.open()
    await expect(page.locator('#refresh-control')).toHaveAttribute('title', /^Live stream, last activity/)
    const toggle = page.locator('#refresh-toggle')
    const liveDot = () => page.locator('#refresh-live').evaluate(el => getComputedStyle(el, '::after').animationName)
    await expect(toggle).toHaveAttribute('aria-label', 'Pause log stream')
    await expect.poll(liveDot).toBe('refresh-pulse')
    await toggle.click()
    await expect.poll(liveDot).toBe('none')
    await expect(toggle).toHaveAttribute('aria-label', 'Resume log stream')
    await expect(page.locator('#refresh-live')).toHaveText('Live')
    await expect(page.locator('#refresh-control')).toHaveAttribute('title', /^Paused, new lines held/)
  })

  test('paints held lines in one batch when resumed', async ({ page }) => {
    const stream = await fakeLogStream(page)
    await page.goto('/logs')
    await page.locator('#refresh-toggle').click()
    await stream.sendMany(300)
    await page.locator('#refresh-toggle').click()
    await expect(page.locator('#livelog-body tr')).toHaveCount(300)
    expect(await page.evaluate(() => window.__scrollIntoViewCalls)).toBe(0)
    const atBottom = await page.locator('#livelog').evaluate(el => el.scrollHeight - el.scrollTop - el.clientHeight < 3)
    expect(atBottom).toBe(true)
  })

  test('keeps the newest ten thousand held lines', async ({ page }) => {
    const stream = await fakeLogStream(page)
    await page.goto('/logs')
    await page.locator('#refresh-toggle').click()
    await stream.sendMany(10500)
    await page.locator('#refresh-toggle').click()
    const rows = page.locator('#livelog-body tr')
    await expect(rows).toHaveCount(10000)
    await expect(rows.first()).toContainText('line 500')
    await expect(rows.last()).toContainText('line 10499')
  })

  test('a stream that closed for good does not count as a connection problem', async ({ page }) => {
    const stream = await fakeLogStream(page)
    await page.goto('/logs')
    await stream.open()
    await stream.fail({ closed: true })
    await expect(page.locator('#refresh-control')).toHaveAttribute('data-state', 'live')
  })

  test('keeps following the log while taller rows are trimmed', async ({ page }) => {
    const stream = await fakeLogStream(page)
    await page.goto('/logs')
    const rows = page.locator('#livelog-body tr')
    await page.evaluate(() => {
      for (let i = 0; i < 200; i++) {
        window.__logStream.onmessage({ lastEventId: String(Date.now()), data: JSON.stringify(['error', 'spec', `trace ${i}\n${'  at frame\n'.repeat(20)}`]) })
      }
    })
    await stream.sendMany(9800)
    await expect(rows).toHaveCount(10000)
    for (let i = 0; i < 4; i++) {
      await stream.sendMany(100, `more${i}`)
      await expect(rows.last()).toContainText(`more${i} 99`)
    }
    const atBottom = await page.locator('#livelog').evaluate(el => el.scrollHeight - el.scrollTop - el.clientHeight < 3)
    expect(atBottom).toBe(true)
  })

  test('renders the refresh control once per frame during a burst of lines', async ({ page }) => {
    const stream = await fakeLogStream(page)
    await page.goto('/logs')
    await stream.open()
    const renders = await page.evaluate(async () => {
      let count = 0
      new MutationObserver(records => { count += records.length }).observe(document.getElementById('refresh-control'), { attributes: true, attributeFilter: ['title'] })
      for (let i = 0; i < 1000; i++) {
        window.__logStream.onmessage({ lastEventId: String(Date.now()), data: JSON.stringify(['info', 'spec', `line ${i}`]) })
      }
      await new Promise(resolve => requestAnimationFrame(() => requestAnimationFrame(resolve)))
      return count
    })
    expect(renders).toBeLessThan(5)
  })
})
