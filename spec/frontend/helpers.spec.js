import * as helpers from './helpers.js'
import { test, expect } from './fixtures.js'

test.describe('helpers', _ => {
  test.describe('redirectOnVhostMismatch', _ => {
    const queueName = 'test-queue'
    // Use vhosts from fixtures: ['foo', 'bar']
    const queueVhost = 'bar'
    const storedVhost = 'foo'

    const queueResponse = {
      name: queueName,
      vhost: queueVhost,
      durable: true,
      exclusive: false,
      auto_delete: false,
      arguments: {},
      consumers: 0,
      messages: 0
    }

    const bindingResponse = { items: [], filtered_count: 0, item_count: 0, page: 1, page_count: 1, page_size: 100, total_count: 0 }

    test('redirects to main route when stored vhost does not match url vhost', async ({ page, apimap }) => {
      await page.addInitScript((vhost) => {
        window.sessionStorage.setItem('vhost', vhost)
      }, storedVhost)

      await page.goto(`/queue#vhost=${encodeURIComponent(queueVhost)}&name=${queueName}`)

      await expect(page).toHaveURL(/queues/)
    })

    test('does not redirect when stored vhost is _all', async ({ page, apimap }) => {
      apimap.get(`/api/queues/${encodeURIComponent(queueVhost)}/${queueName}`, queueResponse)
      apimap.get(`/api/queues/${encodeURIComponent(queueVhost)}/${queueName}/bindings`, bindingResponse)

      await page.addInitScript(() => {
        window.sessionStorage.setItem('vhost', '_all')
      })

      await page.goto(`/queue#vhost=${encodeURIComponent(queueVhost)}&name=${queueName}`)

      await expect(page).toHaveURL(/queue#/)
      await expect(page).not.toHaveURL(/queues/)
    })

    test('does not redirect when stored vhost matches url vhost', async ({ page, apimap }) => {
      apimap.get(`/api/queues/${encodeURIComponent(queueVhost)}/${queueName}`, queueResponse)
      apimap.get(`/api/queues/${encodeURIComponent(queueVhost)}/${queueName}/bindings`, bindingResponse)

      await page.addInitScript((vhost) => {
        window.sessionStorage.setItem('vhost', vhost)
      }, queueVhost)

      await page.goto(`/queue#vhost=${encodeURIComponent(queueVhost)}&name=${queueName}`)

      await expect(page).toHaveURL(/queue#/)
      await expect(page).not.toHaveURL(/queues/)
    })

    test('does not redirect when stored vhost is not set', async ({ page, apimap }) => {
      apimap.get(`/api/queues/${encodeURIComponent(queueVhost)}/${queueName}`, queueResponse)
      apimap.get(`/api/queues/${encodeURIComponent(queueVhost)}/${queueName}/bindings`, bindingResponse)

      await page.addInitScript(() => {
        window.sessionStorage.removeItem('vhost')
      })

      await page.goto(`/queue#vhost=${encodeURIComponent(queueVhost)}&name=${queueName}`)

      await expect(page).toHaveURL(/queue#/)
      await expect(page).not.toHaveURL(/queues/)
    })

    test('does not redirect when url vhost is not set', async ({ page, apimap }) => {
      apimap.get(`/api/queues/${encodeURIComponent(queueVhost)}/${queueName}`, queueResponse)
      apimap.get(`/api/queues/${encodeURIComponent(queueVhost)}/${queueName}/bindings`, bindingResponse)

      await page.addInitScript((vhost) => {
        window.sessionStorage.setItem('vhost', vhost)
      }, storedVhost)

      await page.goto(`/queue#name=${queueName}`)

      await expect(page).toHaveURL(/queue#/)
      await expect(page).not.toHaveURL(/queues/)
    })
  })
})
