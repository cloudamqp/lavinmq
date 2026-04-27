// @ts-check
import { test, expect } from '@playwright/test'
import { trackCspViolations } from './helpers.js'

test.describe('when unauthenticated', _ => {
  test.use({ storageState: {} })
  test('redirects to login', async ({ page }) => {
    await page.goto('/')
    await expect(page).toHaveURL(/\/login$/)
  })

  test('login page loads without CSP violations', async ({ page }) => {
    const getViolations = await trackCspViolations(page)
    await page.goto('/login')
    expect(await getViolations()).toEqual([])
  })
})

test.describe('login', _ => {
  test.use({ storageState: {} })

  test('successful login redirects to / and sets cookie', async ({ page, context }) => {
    await page.goto('/login')
    await page.getByLabel('Username').fill('guest')
    await page.getByLabel('Password').fill('guest')
    await page.getByRole('button').click()
    await expect(page).toHaveURL('/')
    const cookies = await context.cookies()
    expect(cookies.some(c => c.name === 'm')).toBe(true)
  })

  test('failed login shows "Authentication failure" alert', async ({ page }) => {
    await page.goto('/login')
    await page.getByLabel('Username').fill('guest')
    await page.getByLabel('Password').fill('wrongpassword')
    const dialog = page.waitForEvent('dialog')
    await page.getByRole('button').click()
    const d = await dialog
    expect(d.message()).toBe('Authentication failure')
    await d.dismiss()
  })
})

test.describe('logout', _ => {
  test('clicking sign-out redirects to /login and deletes cookie', async ({ page, context }) => {
    await page.goto('/')
    await Promise.all([
      page.waitForURL(/\/login$/),
      page.locator('#signoutLink').click()
    ])
    const cookies = await context.cookies()
    expect(cookies.some(c => c.name === 'm')).toBe(false)
  })
})
