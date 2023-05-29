import * as helpers from './helpers.js'
import * as qHelpers from './queues_helpers.js'
import { test, expect } from '@playwright/test';

test.describe("queues", _ => {
  helpers.setupVhostResponse(test)

  // Test that different combination of hash params are sent in the request
  test.describe('are loaded with params when hash params', _ => {
    test('are empty', async ({ page, baseURL }) => {
      const apiQueuesRequest = helpers.waitForPathRequest(page, '/api/queues')
      await page.goto('/queues')
      await expect(apiQueuesRequest).toBeRequested()
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
    const queues = Array.from(Array(10), (_, i) => qHelpers.queue(`queue-${i}`))
    const queues_response = qHelpers.response(queues, { total_count: 100, page: 1, page_count: 10, page_size: 10 })

    test('updates url when a table header is clicked', async ({ page }) => {
      const apiQueuesRequest = helpers.waitForPathRequest(page, '/api/queues', queues_response)
      await page.goto('/queues')
      await apiQueuesRequest
      await page.locator('#table thead').getByText('Name').click()
      await expect(page).toHaveURL(/sort=name/)
    })

    test('is reversed when click on the same header', async ({ page }) => {
      const apiQueuesRequest = helpers.waitForPathRequest(page, '/api/queues', queues_response)
      await page.goto('/queues')
      await apiQueuesRequest
      const apiQueuesRequest2 = helpers.waitForPathRequest(page, '/api/queues', queues_response)
      await page.locator('#table thead').getByText('Name').click()
      await expect(page).toHaveURL(/sort=name/)
      await apiQueuesRequest2
      const sort_reverse = (new URL(page.url())).searchParams.get('sort_reverse') == 'true'
      const apiQueuesRequest3 = helpers.waitForPathRequest(page, '/api/queues', queues_response)
      await page.locator('#table thead').getByText('Name').click()
      await expect(page).toHaveURL(new RegExp(`sort_reverse=${!sort_reverse}`))
      await expect(apiQueuesRequest3).toHaveQueryParams(`sort_reverse=${!sort_reverse}`)
    })
  })

  test.describe('pagination', _ => {
    const queues = Array.from(Array(10), (_, i) => qHelpers.queue(`queue-${i}`))
    const queues_response = qHelpers.response(queues, { total_count: 100, page: 1, page_count: 10, page_size: 10 })

    test('is visible for page_count=10', async ({ page }) => {
      const apiQueuesRequest = helpers.waitForPathRequest(page, '/api/queues', queues_response)
      await page.goto('/queues')
      await apiQueuesRequest
      await expect(page.locator('#table .pagination .page-item')).toContainText(['Previous', 'Next'], { timeout: 10 })
    })

    test('updates url when Next is clicked', async ({page }) => {
      const apiQueuesRequest = helpers.waitForPathRequest(page, '/api/queues', queues_response)
      await page.goto('/queues')
      await apiQueuesRequest
      const apiQueuesRequest2 = helpers.waitForPathRequest(page, '/api/queues')
      await page.locator('#table .pagination .page-item').getByText('Next').click()
      await expect(page).toHaveURL(/page=2/)
      await expect(apiQueuesRequest2).toHaveQueryParams('page=2')
    })
  })

  test.describe('search', _ => {
    test('updates url when value is entered and Enter is hit', async ({ page })=> {
      await page.goto('/queues')
      const searchField = page.locator('.filter-table')
      await searchField.fill('my filter')
      const apiQueuesRequest = helpers.waitForPathRequest(page, '/api/queues')
      await searchField.press('Enter')
      await expect(page).toHaveURL(/name=my(\+|%20)filter/)
      await expect(apiQueuesRequest).toHaveQueryParams('name=my filter&use_regex=true')
    })
  })
})
