import { test, expect } from '@playwright/test';

test.describe("vhosts", _ => {
  const vhosts = ['foo', 'bar']

  test.beforeEach(async ({ page }) => {
    const vhostResponse = vhosts.map(x => { return {name: x} })
    await page.route(/\/api\/vhosts/, route => route.fulfill({ json: vhostResponse }))
    await page.goto('/')
  })

  test('are loaded', async ({ page }) => {
    const vhostOptions = await page.locator('#userMenuVhost option').allTextContents()
    vhosts.forEach(vhost => {
      expect(vhostOptions).toContain(vhost)
    })
  })

  test('remember selection', async ({ page }) => {
    await page.locator('#userMenuVhost').selectOption('foo')
    await expect(page.locator('#userMenuVhost option:checked')).toHaveText(['foo'])
  })
})
