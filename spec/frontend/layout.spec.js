import { test, expect } from './fixtures.js'
import { trackCspViolations } from './helpers.js'

test.describe('Content Security Policy', _ => {
  test('authenticated page loads without CSP violations', async ({ page }) => {
    const getViolations = await trackCspViolations(page)
    await page.goto('/')
    expect(await getViolations()).toEqual([])
  })
})

test.describe('theme switcher', _ => {
  test('system theme button is active by default', async ({ page }) => {
    await page.goto('/')
    await expect(page.locator('#theme-system')).toHaveClass(/active/)
  })

  test('clicking light sets theme-light class on html', async ({ page }) => {
    await page.goto('/')
    await page.locator('#theme-light').click()
    await expect(page.locator('html')).toContainClass('theme-light')
    await expect(page.locator('html')).not.toContainClass('theme-dark')
    await expect(page.locator('#theme-light')).toHaveClass(/active/)
  })

  test('clicking dark sets theme-dark class on html', async ({ page }) => {
    await page.goto('/')
    await page.locator('#theme-dark').click()
    await expect(page.locator('html')).toContainClass('theme-dark')
    await expect(page.locator('html')).not.toContainClass('theme-light')
    await expect(page.locator('#theme-dark')).toHaveClass(/active/)
  })

  test('clicking system adds system class on html', async ({ page }) => {
    await page.goto('/')
    await page.locator('#theme-dark').click()
    await page.locator('#theme-system').click()
    await expect(page.locator('html')).toContainClass('system')
    await expect(page.locator('#theme-system')).toHaveClass(/active/)
  })

  test('theme is persisted across navigation', async ({ page }) => {
    await page.goto('/')
    await page.locator('#theme-dark').click()
    await page.goto('/nodes')
    await expect(page.locator('html')).toContainClass('theme-dark')
  })

  test('html gets theme-dark when OS changes to dark and system theme is active', async ({ page }) => {
    await page.emulateMedia({ colorScheme: 'light' })
    await page.goto('/')
    await expect(page.locator('html')).toContainClass('theme-light')
    await page.emulateMedia({ colorScheme: 'dark' })
    await expect(page.locator('html')).toContainClass('theme-dark')
    await expect(page.locator('html')).not.toContainClass('theme-light')
  })

  test('html gets theme-light when OS changes to light and system theme is active', async ({ page }) => {
    await page.emulateMedia({ colorScheme: 'dark' })
    await page.goto('/')
    await expect(page.locator('html')).toContainClass('theme-dark')
    await page.emulateMedia({ colorScheme: 'light' })
    await expect(page.locator('html')).toContainClass('theme-light')
    await expect(page.locator('html')).not.toContainClass('theme-dark')
  })

  test('OS color scheme change does not affect manually set theme', async ({ page }) => {
    await page.emulateMedia({ colorScheme: 'light' })
    await page.goto('/')
    await page.locator('#theme-dark').click()
    await page.emulateMedia({ colorScheme: 'dark' })
    await expect(page.locator('html')).toContainClass('theme-dark')
    await page.emulateMedia({ colorScheme: 'light' })
    await expect(page.locator('html')).toContainClass('theme-dark')
    await expect(page.locator('html')).not.toContainClass('theme-light')
  })
})

test.describe('vhosts', _ => {
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
