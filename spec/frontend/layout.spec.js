import { test, expect } from './fixtures.js'

test.describe("vhosts", _ => {
  test('are loaded', async ({ page, baseURL, vhosts }) => {
    await page.goto('/')
    const vhostOptions = await page.locator('#userMenuVhost option').allTextContents()
    vhosts.forEach(vhost => {
      expect(vhostOptions).toContain(vhost)
    })
  })

  test('remember selection', async ({ page }) => {
    await page.goto('/')
    await page.locator('#userMenuVhost').selectOption('foo') // selectOption trigger page load
    await expect(page.locator('#userMenuVhost option:checked')).toHaveText(['foo'])
  })
})
