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
