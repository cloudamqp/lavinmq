import * as helpers from './helpers.js'
import { test, expect } from './fixtures.js'

test.describe('submitForm', _ => {
  test.describe('exchanges form', _ => {
    test.beforeEach(async ({ page }) => {
      await page.goto('/exchanges')
      await page.locator('#addExchange').waitFor()
    })

    test('resets form on successful submit', async ({ page }) => {
      const exchangeName = 'test-exchange'
      const form = page.locator('#addExchange')
      const vhost = await form.locator('select[name="vhost"]').inputValue()
      const putRequest = helpers.waitForPathRequest(page, `/api/exchanges/${encodeURIComponent(vhost)}/${exchangeName}`, { method: 'PUT' })

      await form.locator('input[name="name"]').fill(exchangeName)
      await form.getByRole('button', { name: 'Add exchange' }).click()

      await expect(putRequest).toBeRequested()
      await expect(form.locator('input[name="name"]')).toHaveValue('')
    })

    test('does not reset form on error response', async ({ page }) => {
      const exchangeName = 'test-exchange'
      const form = page.locator('#addExchange')
      const vhost = await form.locator('select[name="vhost"]').inputValue()

      await page.route(url => url.pathname === `/api/exchanges/${encodeURIComponent(vhost)}/${exchangeName}`, async route => {
        if (route.request().method() === 'PUT') {
          await route.fulfill({
            status: 403,
            contentType: 'application/json',
            body: JSON.stringify({ error: 'access_refused', reason: 'No permission' })
          })
        } else {
          await route.fallback()
        }
      })

      const dialogHandled = new Promise(resolve => {
        page.on('dialog', async dialog => { await dialog.dismiss(); resolve() })
      })

      await form.locator('input[name="name"]').fill(exchangeName)
      await form.getByRole('button', { name: 'Add exchange' }).click()
      await dialogHandled

      await expect(form.locator('input[name="name"]')).toHaveValue(exchangeName)
    })
  })

  test.describe('queues form', _ => {
    test.beforeEach(async ({ page }) => {
      await page.goto('/queues')
      await page.locator('#declare').waitFor()
    })

    test('preserves vhost selection after successful submit', async ({ page }) => {
      const queueName = 'test-queue'
      const form = page.locator('#declare')
      const vhost = await form.locator('select[name="vhost"]').inputValue()
      const putRequest = helpers.waitForPathRequest(page, `/api/queues/${encodeURIComponent(vhost)}/${queueName}`, { method: 'PUT' })

      await form.locator('input[name="name"]').fill(queueName)
      await form.getByRole('button', { name: 'Add queue' }).click()

      await expect(putRequest).toBeRequested()
      await expect(form.locator('select[name="vhost"]')).toHaveValue(vhost)
    })

    test('does not reset form on error response', async ({ page }) => {
      const queueName = 'test-queue'
      const form = page.locator('#declare')
      const vhost = await form.locator('select[name="vhost"]').inputValue()

      await page.route(url => url.pathname === `/api/queues/${encodeURIComponent(vhost)}/${queueName}`, async route => {
        if (route.request().method() === 'PUT') {
          await route.fulfill({
            status: 403,
            contentType: 'application/json',
            body: JSON.stringify({ error: 'access_refused', reason: 'No permission' })
          })
        } else {
          await route.fallback()
        }
      })

      const dialogHandled = new Promise(resolve => {
        page.on('dialog', async dialog => { await dialog.dismiss(); resolve() })
      })

      await form.locator('input[name="name"]').fill(queueName)
      await form.getByRole('button', { name: 'Add queue' }).click()
      await dialogHandled

      await expect(form.locator('input[name="name"]')).toHaveValue(queueName)
    })
  })
})
