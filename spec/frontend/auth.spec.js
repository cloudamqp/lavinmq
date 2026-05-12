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
    const [d] = await Promise.all([
      page.waitForEvent('dialog'),
      page.getByRole('button').click()
    ])
    expect(d.message()).toBe('Authentication failure')
    await d.dismiss()
  })
})

test.describe('whoAmI', _ => {
  test('logs out and redirects to /login on API failure', async ({ page, context }) => {
    await page.addInitScript(() => localStorage.removeItem('lmq.whoami'))
    await page.route(url => url.pathname === '/api/whoami', route => route.fulfill({ status: 401 }))
    await page.goto('/')
    await expect(page).toHaveURL(/\/login$/)
    const cookies = await context.cookies()
    expect(cookies.some(c => c.name === 'm')).toBe(false)
  })

  test('caches whoami response and only fetches once', async ({ page }) => {
    await page.goto('/')
    await page.evaluate(() => localStorage.removeItem('lmq.whoami'))
    let callCount = 0
    await page.route(url => url.pathname === '/api/whoami', async route => {
      callCount++
      await route.fallback()
    })
    await page.goto('/')
    await page.goto('/')
    expect(callCount).toBe(1)
  })

  test('fetches whoami again if missing from localStorage', async ({ page }) => {
    await page.goto('/')
    await page.evaluate(() => localStorage.removeItem('lmq.whoami'))
    let callCount = 0
    await page.route(url => url.pathname === '/api/whoami', async route => {
      callCount++
      await route.fallback()
    })
    await page.goto('/')
    await page.evaluate(() => localStorage.removeItem('lmq.whoami'))
    await page.goto('/')
    expect(callCount).toBe(2)
  })

  test('does not store sensitive fields in localStorage', async ({ page }) => {
    await page.goto('/')
    const stored = await page.evaluate(() => JSON.parse(localStorage.getItem('lmq.whoami')))
    expect(stored.password_hash).toBeUndefined()
    expect(stored.hashing_algorithm).toBeUndefined()
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
