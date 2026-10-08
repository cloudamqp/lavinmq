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
})
