import * as helpers from './helpers.js'
import { test, expect } from './fixtures.js'

async function fakeLogStream (page) {
  await page.addInitScript(() => {
    window.EventSource = class {
      constructor () { window.__logStream = this }
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
})
