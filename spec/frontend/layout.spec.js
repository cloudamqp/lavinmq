import { test, expect } from './fixtures.js'

test.describe('vhosts', _ => {
  test('are loaded', async ({ page, vhosts }) => {
    await page.goto('/')
    vhosts = await vhosts
    await expect(page.locator('#userMenuVhost option')).toHaveCount(vhosts.length + 1) // + 1 for "All"
  })

  test('remember selection', async ({ page, vhosts }) => {
    await page.goto('/')
    vhosts = await vhosts
    await page.locator('#userMenuVhost').selectOption(vhosts[0]) // selectOption trigger page load
    await expect(page.locator('#userMenuVhost option:checked')).toHaveText([vhosts[0]])
  })

  test.describe('redirect on vhost mismatch', _ => {
    const queueName = 'test-queue'
    const queueVhost = 'bar'

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

    test('redirects to main route when vhost selector changes on detail page', async ({ page, apimap, vhosts }) => {
      apimap.get(`/api/queues/${encodeURIComponent(queueVhost)}/${queueName}`, queueResponse)
      apimap.get(`/api/queues/${encodeURIComponent(queueVhost)}/${queueName}/bindings`, bindingResponse)

      await page.goto(`/queue#vhost=${encodeURIComponent(queueVhost)}&name=${queueName}`)
      await expect(page).toHaveURL(/queue#/)

      // Change vhost selector to a different vhost
      vhosts = await vhosts
      const differentVhost = vhosts.find(v => v !== queueVhost) || vhosts[0]
      await page.locator('#userMenuVhost').selectOption(differentVhost)

      await expect(page).toHaveURL(/queues/)
    })

    test('does not redirect when vhost selector changes on list page', async ({ page, vhosts }) => {
      await page.goto('/queues')
      await expect(page).toHaveURL(/queues/)

      // Change vhost selector
      vhosts = await vhosts
      await page.locator('#userMenuVhost').selectOption(vhosts[0])

      // Should reload, not redirect
      await expect(page).toHaveURL(/queues/)
    })
  })
})
