import * as helpers from './helpers.js'
import { test, expect } from './fixtures.js'

test.describe('submitForm', _ => {
  test.describe('exchanges form', _ => {
    test.beforeEach(async ({ page, apimap }) => {
      const exchangeTypesLoaded = apimap.get('/api/exchanges', [])
      const overviewLoaded = apimap.get('/api/overview', { exchange_types: [{ name: 'topic' }, { name: 'direct' }] })
      await page.goto('/exchanges')
      await overviewLoaded
    })

    test('resets form on successful submit', async ({ page }) => {
      const exchangeName = 'test-exchange'
      const putRequest = helpers.waitForPathRequest(page, `/api/exchanges/bar/${exchangeName}`, { method: 'PUT' })

      const form = page.locator('#addExchange')
      await form.getByLabel('Name', { exact: true }).fill(exchangeName)
      await form.getByRole('button', { name: 'Add exchange' }).click()

      await expect(putRequest).toBeRequested()
      await expect(form.getByLabel('Name', { exact: true })).toHaveValue('')
    })

    test('does not reset form on error response', async ({ page }) => {
      const exchangeName = 'test-exchange'
      // Intercept the PUT and return a 403 error
      await page.route(url => url.pathname === `/api/exchanges/bar/${exchangeName}`, async route => {
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

      // Dismiss the alert that standardErrorHandler will show
      page.on('dialog', dialog => dialog.dismiss())

      const form = page.locator('#addExchange')
      await form.getByLabel('Name', { exact: true }).fill(exchangeName)
      await form.getByRole('button', { name: 'Add exchange' }).click()

      // Form should still have the value since request failed
      await expect(form.getByLabel('Name', { exact: true })).toHaveValue(exchangeName)
    })
  })

  test.describe('queues form', _ => {
    test.beforeEach(async ({ page, apimap }) => {
      const queuesLoaded = apimap.get('/api/queues', [])
      await page.goto('/queues')
      await queuesLoaded
    })

    test('preserves vhost selection after successful submit', async ({ page }) => {
      const queueName = 'test-queue'
      const putRequest = helpers.waitForPathRequest(page, `/api/queues/bar/${queueName}`, { method: 'PUT' })

      const form = page.locator('#declare')
      await form.getByLabel('Name', { exact: true }).fill(queueName)
      await form.getByRole('button', { name: 'Add queue' }).click()

      await expect(putRequest).toBeRequested()
      await expect(form.locator('select[name="vhost"]')).toHaveValue('bar')
    })

    test('does not reset form on error response', async ({ page }) => {
      const queueName = 'test-queue'
      await page.route(url => url.pathname === `/api/queues/bar/${queueName}`, async route => {
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

      page.on('dialog', dialog => dialog.dismiss())

      const form = page.locator('#declare')
      await form.getByLabel('Name', { exact: true }).fill(queueName)
      await form.getByRole('button', { name: 'Add queue' }).click()

      await expect(form.getByLabel('Name', { exact: true })).toHaveValue(queueName)
    })
  })
})
