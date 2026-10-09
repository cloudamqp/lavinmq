import * as helpers from './helpers.js'
import * as qHelpers from './queues_helpers.js'
import { test, expect } from './fixtures.js';

test.describe("queues", _ => {
    const queues = Array.from(Array(10), (_, i) => qHelpers.queue(`queue-${i}`))
    const queues_response = qHelpers.response(queues, { total_count: 100, page: 1, page_count: 10, page_size: 10 })

    test.beforeEach(async ({ apimap, page }) => {
      const queuesLoaded = apimap.get('/api/queues', queues_response)
      page.goto('/queues')
      await queuesLoaded
    })

  test('are refreshed automatically', async({ page }) => {
    await page.clock.install()
    await page.goto('/queues')
    // Verify that at least 3 requests are made
    for (let i=0; i<3; i++) {
      const apiQueuesRequest = helpers.waitForPathRequest(page, '/api/queues')
      await page.clock.runFor(10000) // advance time by 10 seconds
      await expect(apiQueuesRequest).toBeRequested()
    }
  })


  // Test that different combination of hash params are sent in the request
  test.describe('are loaded with params when hash params', _ => {
    test('are empty', async ({ page, baseURL }) => {
      await expect(page.locator('#pagename-label')).toHaveText('100') // total_count
    })

    test('are page_size=10, page=2', async ({ page }) => {
      const apiQueuesRequest = helpers.waitForPathRequest(page, '/api/queues')
      await page.goto('/queues#page_size=10&page=2')
      await expect(apiQueuesRequest).toHaveQueryParams('page_size=10&page=2')
    })

    test('are name=queue-a, use_regex=true', async ({ page }) => {
      const apiQueuesRequest = helpers.waitForPathRequest(page, '/api/queues')
      await page.goto('/queues#name=queue-a&use_regex=true')
      await expect(apiQueuesRequest).toHaveQueryParams('name=queue-a&use_regex=true')
    })

    test('are sort=name, sort_reverse=false', async ({ page }) => {
      const apiQueuesRequest = helpers.waitForPathRequest(page, '/api/queues')
      await page.goto('/queues#sort=name&sort_reverse=false')
      await expect(apiQueuesRequest).toHaveQueryParams('sort=name&sort_reverse=false')
    })

    test('are page_size=10, page=3, name=qname, use_regex=true, sort=name, sort_reverse=true', async ({ page }) => {
      const q = 'page_size=10&page=3&name=qname&use_regex=true&sort=name&sort_reverse=true'
      const apiQueuesRequest = helpers.waitForPathRequest(page, '/api/queues')
      await page.goto(`/queues#${q}`)
      await expect(apiQueuesRequest).toHaveQueryParams(q)
    })
  })

  test.describe('sorting', _ => {
    test('updates url when a table header is clicked', async ({ page }) => {
      await page.locator('#table thead').getByText('Name').click()
      await expect(page).toHaveURL(/sort=name/)
    })

    test('is reversed when click on the same header', async ({ page }) => {
      await page.locator('#table thead').getByText('Name').click()
      await expect(page).toHaveURL(/sort=name/)
      const sort_reverse = (new URL(page.url())).searchParams.get('sort_reverse') == 'true'
      const apiQueuesRequest = helpers.waitForPathRequest(page, '/api/queues')
      await page.locator('#table thead').getByText('Name').click()
      await expect(page).toHaveURL(new RegExp(`sort_reverse=${!sort_reverse}`))
      await expect(apiQueuesRequest).toHaveQueryParams(`sort_reverse=${!sort_reverse}`)
    })
  })

  test.describe('pagination', _ => {
    test('is visible for page_count=10', async ({ page }) => {
      await expect(page.locator('.pagination .page-item')).toContainText(['Previous', 'Next'], { timeout: 10 })
    })

    test('updates url when Next is clicked', async ({ page }) => {
      const apiQueuesRequest = helpers.waitForPathRequest(page, '/api/queues')
      await page.locator('.pagination .page-item').getByText('Next').click()
      await expect(page).toHaveURL(/page=2/)
      await expect(apiQueuesRequest).toHaveQueryParams('page=2')
    })
  })

  test.describe('search', _ => {
    test('updates url when value is entered and Enter is hit', async ({ page }) => {
      const searchField = page.locator('.filter-table')
      await searchField.fill('my filter')
      const apiQueuesRequest = helpers.waitForPathRequest(page, '/api/queues')
      await searchField.press('Enter')
      await expect(page).toHaveURL(/name=my(\+|%20)filter/)
      await expect(apiQueuesRequest).toHaveQueryParams('name=my filter&use_regex=true')
    })
  })

  test.describe('table checkboxes', _ => {
    test('header checkbox checks all', async ({ page }) => {
      await page.locator('#multi-check-all').check()
      await page.locator('#table tbody tr[data-name="queue-9"]').isVisible()
      const checkboxes = page.locator('#table tbody').getByRole('checkbox')
      await expect(checkboxes).toHaveCount(10)

      const count = await checkboxes.count();
      for (let i = 0; i < count; i++) {
        await expect(checkboxes.nth(i)).toBeChecked();
      }
    })

    test('header checkbox action dialog opens and closes', async ({ page }) => {
      await page.locator('#multi-check-all').check()
      await expect(page.locator('#multiselect-controls')).toBeVisible()
      await page.locator('#multiselect-controls .popup-close').click()
      await expect(page.locator('#multiselect-controls')).toBeHidden()
    })
  })
})

test.describe('queues with automatic retries', _ => {
  const retryQueues = [
    qHelpers.queue('orders', {
      arguments: { 'x-delayed-retry-min': 500 },
      delayed_retry: { min: 500, multiplier: null, max: null, delivery_limit: 20, delivery_limit_default: true, messages_delayed: 0, retry_queue: 'amq.retry-orders' }
    }),
    qHelpers.queue('amq.retry-orders', { durable: false, internal: true, primary_queue: 'orders' }),
    qHelpers.queue('plain')
  ]

  test.beforeEach(async ({ apimap, page }) => {
    const queuesLoaded = apimap.get('/api/queues', qHelpers.response(retryQueues))
    await page.goto('/queues')
    await queuesLoaded
  })

  test('marks retry-enabled queues with a badge', async ({ page }) => {
    const row = page.locator('#table tbody tr').filter({ has: page.getByRole('link', { name: 'orders', exact: true }) })
    await expect(row.locator('.features .delayed-retry')).toHaveAttribute('title', 'Automatic retries')
    const plain = page.locator('#table tbody tr').filter({ has: page.getByRole('link', { name: 'plain', exact: true }) })
    await expect(plain.locator('.features .delayed-retry')).toHaveCount(0)
  })

  test('links the retry queue to its primary queue', async ({ page }) => {
    const row = page.locator('#table tbody tr').filter({ has: page.getByRole('link', { name: 'amq.retry-orders', exact: true }) })
    const badge = row.locator('.features .retry-queue-of')
    await expect(badge).toHaveAttribute('title', 'Retry queue of orders')
    await expect(badge.getByRole('link')).toHaveAttribute('href', /queue#vhost=%2F&name=orders$/)
  })
})
