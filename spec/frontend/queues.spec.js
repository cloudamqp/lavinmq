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

  // Test that different combination of hash params are sent in the request
  test.describe('are loaded with params when hash params', _ => {
    test('are empty', async ({ page, baseURL }) => {
      expect(page.locator('#pagename-label')).toHaveText('100') // total_count
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
      await expect(page.locator('#table .pagination .page-item')).toContainText(['Previous', 'Next'], { timeout: 10 })
    })

    test('updates url when Next is clicked', async ({page }) => {
      const apiQueuesRequest = helpers.waitForPathRequest(page, '/api/queues')
      await page.locator('#table .pagination .page-item').getByText('Next').click()
      await expect(page).toHaveURL(/page=2/)
      await expect(apiQueuesRequest).toHaveQueryParams('page=2')
    })
  })

  test.describe('search', _ => {
    test('updates url when value is entered and Enter is hit', async ({ page })=> {
      const searchField = page.locator('.filter-table')
      await searchField.fill('my filter')
      const apiQueuesRequest = helpers.waitForPathRequest(page, '/api/queues')
      await searchField.press('Enter')
      await expect(page).toHaveURL(/name=my(\+|%20)filter/)
      await expect(apiQueuesRequest).toHaveQueryParams('name=my filter&use_regex=true')
    })
  })
})
