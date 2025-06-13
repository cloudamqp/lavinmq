import { test, expect } from './fixtures.js'

test.describe("vhosts", _ => {
  test('are loaded', async ({ page, vhosts }) => {
    await page.goto('/')
    vhosts = await vhosts
    await expect(page.locator('#userMenuVhost option')).toHaveCount(vhosts.length + 1) // + 1 for "All"
  })

  test('remember selection', async ({ page, vhosts }) => {
    await page.goto('/')
    vhosts = await vhosts
    await page.locator('#userMenuVhost').selectOption(vhosts[0]) // selectOption trigger page load
    await expect(page.locator('#userMenuVhost option:checked')).toHaveText([vhosts[0]])
  })
})
