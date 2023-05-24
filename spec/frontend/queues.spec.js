import * as qHelpers from './queues_helpers.js'
import { test, expect } from '@playwright/test';

test.describe("queues", _ => {
  const vhosts = ['foo', 'bar']

  test.beforeEach(async ({ page }) => {
    const vhostResponse = vhosts.map(x => { return {name: x} })
    await page.route(/\/api\/vhosts/, route => route.fulfill({ json: vhostResponse }))
  })

  function waitForPathRequest(page, path, response_data = {}) {
    return new Promise((resolve, reject) => {
      page.route('**/*', (route, request) => {
        const requestedUrl = new URL(request.url())
        if (requestedUrl.pathname !== path) {
          return route.fallback()
        }
        page.unroute('**/*')
        route.fulfill({ json: response_data })
        resolve(request)
      })
    })
    return page.waitForRequest(req => {
      const reqUrl = new URL(req.url())
      return reqUrl.pathname == path
    }, { timeout: 1000 })
  }

  // Test that different combination of hash params are sent in the request
  test.describe('are loaded with params when hash params', _ => {
    test('are empty', async ({ page, baseURL }) => {
      const apiQueuesRequest = waitForPathRequest(page, '/api/queues')
      await page.goto('/queues')
      await expect(apiQueuesRequest).toBeRequested()
    })

    test('are page_size=10, page=2', async ({ page }) => {
      const apiQueuesRequest = waitForPathRequest(page, '/api/queues')
      await page.goto('/queues#page_size=10&page=2')
      await expect(apiQueuesRequest).toHaveQueryParams('page_size=10&page=2')
    })

    test('are name=queue-a, use_regex=true', async ({ page }) => {
      const apiQueuesRequest = waitForPathRequest(page, '/api/queues')
      await page.goto('/queues#name=queue-a&use_regex=true')
      await expect(apiQueuesRequest).toHaveQueryParams('name=queue-a&use_regex=true')
    })

    test('are sort=name, sort_reverse=false', async ({ page }) => {
      const apiQueuesRequest = waitForPathRequest(page, '/api/queues')
      await page.goto('/queues#sort=name&sort_reverse=false')
      await expect(apiQueuesRequest).toHaveQueryParams('sort=name&sort_reverse=false')
    })

    test('are page_size=10, page=3, name=qname, use_regex=true, sort=name, sort_reverse=true', async ({ page }) => {
      const q = 'page_size=10&page=3&name=qname&use_regex=true&sort=name&sort_reverse=true'
      const apiQueuesRequest = waitForPathRequest(page, '/api/queues')
      await page.goto(`/queues#${q}`)
      await expect(apiQueuesRequest).toHaveQueryParams(q)
    })
  })

  //
  test.describe('pagination', _ => {
    const queues = Array.from(Array(10), (_, i) => qHelpers.queue(`queue-${i}`))
    const queues_response = qHelpers.response(queues, { total_count: 100, page: 1, page_count: 10, page_size: 10 })

    test('is visible for page_count=10', async ({ page }) => {
      const apiQueuesRequest = waitForPathRequest(page, '/api/queues', queues_response)
      await page.goto('/queues')
      await apiQueuesRequest
      await expect(page.locator('#table .pagination .page-item')).toContainText(['Previous', 'Next'], { timeout: 10 })
    })

    test('updates url when Next is clicked', async ({page }) => {
      const apiQueuesRequest = waitForPathRequest(page, '/api/queues', queues_response)
      await page.goto('/queues')
      await apiQueuesRequest
      await page.locator('#table .pagination .page-item').getByText('Next').click()
      await expect(page).toHaveURL(/page=2/)
    })
  })

})
