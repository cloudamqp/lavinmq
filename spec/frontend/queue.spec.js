import * as helpers from './helpers.js'
import { test, expect } from './fixtures.js'

test.describe('queue', _ => {
  const queueName = 'foo'
  const queueVhost = '/'
  const bindingResponse = {
    "items": [
      {
        "source": "", "vhost": queueVhost, "destination": queueName,
        "destination_type": "queue", "routing_key": queueName,
        "arguments": null, "properties_key": queueName
      },
      {
        "source": "amq.topic", "vhost": queueVhost, "destination": queueName,
        "destination_type": "queue", "routing_key": queueName,
        "arguments": {}, "properties_key": queueName
      }
    ],
    "filtered_count": 2, "item_count": 2, "page": 1, "page_count": 1, "page_size": 100, "total_count": 2
  }

  const consumers = [
    {"queue":{"name":queueName,"vhost":queueVhost},"consumer_tag":"foo_consumer","exclusive":false,"ack_required":true,"prefetch_count":1000,"priority":0,"channel_details":{"peer_host":"127.0.0.1","peer_port":56861,"connection_name":"conn_name","user":"guest","number":1,"name":"channel_name"}}
  ]

  const queueResponse = {
    "name":queueName,"durable":true,"exclusive":false,"auto_delete":false,"arguments":{},"consumers":0,"vhost":queueVhost,"messages":0,"total_bytes":0,"messages_persistent":0,"ready":0,"messages_ready":0,"ready_bytes":0,"message_bytes_ready":0,"ready_avg_bytes":0,"unacked":0,"messages_unacknowledged":0,"unacked_bytes":0,"message_bytes_unacknowledged":0,"unacked_avg_bytes":0,"state":"running","effective_policy_definition":{},"message_stats":{"ack":0,"ack_details":{"rate":0.0},"deliver":0,"deliver_details":{"rate":0.0},"deliver_get":0,"deliver_get_details":{"rate":0.0},"confirm":0,"confirm_details":{"rate":0.0},"get":0,"get_details":{"rate":0.0},"get_no_ack":0,"get_no_ack_details":{"rate":0.0},"publish":0,"publish_details":{"rate":0.0},"redeliver":0,"redeliver_details":{"rate":0.0},"reject":0,"reject_details":{"rate":0.0},"return_unroutable":0,"return_unroutable_details":{"rate":0.0},"dedup":0,"dedup_details":{"rate":0.0}},"effective_arguments":["x-expires","x-max-length","x-max-length-bytes","x-message-ttl","x-delivery-limit","x-consumer-timeout"],"message_stats":{"ack":0,"ack_details":{"rate":0.0,"log":[]},"deliver":0,"deliver_details":{"rate":0.0,"log":[]},"deliver_get":0,"deliver_get_details":{"rate":0.0,"log":[]},"confirm":0,"confirm_details":{"rate":0.0,"log":[]},"get":0,"get_details":{"rate":0.0,"log":[]},"get_no_ack":0,"get_no_ack_details":{"rate":0.0,"log":[]},"publish":0,"publish_details":{"rate":0.0,"log":[]},"redeliver":0,"redeliver_details":{"rate":0.0,"log":[]},"reject":0,"reject_details":{"rate":0.0,"log":[]},"return_unroutable":0,"return_unroutable_details":{"rate":0.0,"log":[]},"dedup":0,"dedup_details":{"rate":0.0,"log":[]}},"consumer_details":consumers
  }

  // Content is split across tabs (Overview/Consumers/Bindings/Messages); the
  // active tab is chosen from the `tab` hash param. Each describe below opens
  // the queue page on the tab its specs need so the elements are visible.
  const queueUrl = tab => `/queue#vhost=${encodeURIComponent(queueVhost)}&name=${queueName}&tab=${tab}`

  // Register the queue + bindings API responses and return a single promise
  // that resolves once both have been requested by the page.
  function loadQueueApi (apimap) {
    const queueLoaded = apimap.get(`/api/queues/${encodeURIComponent(queueVhost)}/${queueName}`, queueResponse)
    const bindingsLoaded = apimap.get(`/api/queues/${encodeURIComponent(queueVhost)}/${queueName}/bindings`, bindingResponse)
    return Promise.all([queueLoaded, bindingsLoaded])
  }

  test.describe('overview tab', _ => {
    test.beforeEach(async ({ apimap, page }) => {
      const loaded = loadQueueApi(apimap)
      await page.goto(queueUrl('overview'))
      await loaded
    })

    test('queue is loaded', async ({ page }) => {
      await expect(page.locator('#pagename-label')).toHaveText(new RegExp(`${queueName} .* ${queueVhost}`))
      await expect(page.locator('#consumer-count')).toHaveText("1")
    })
  })

  test.describe('consumers tab', _ => {
    test.beforeEach(async ({ apimap, page }) => {
      const loaded = loadQueueApi(apimap)
      await page.goto(queueUrl('consumers'))
      await loaded
    })

    test('consumer can be cancelled', async ({ page }) => {
      const consumer = consumers[0]
      const vhost = encodeURIComponent(consumer.queue.vhost)
      const conn = encodeURIComponent(consumer.channel_details.connection_name)
      const ch = encodeURIComponent(consumer.channel_details.number)
      const consumerTag = encodeURIComponent(consumer.consumer_tag)
      const actionPath = `/api/consumers/${vhost}/${conn}/${ch}/${consumerTag}`
      const cancelRequest = helpers.waitForPathRequest(page, actionPath, { method: 'DELETE' })
      page.on('dialog', async dialog => await dialog.accept())
      await page.locator('#table tbody tr').getByRole('button', { name: /cancel/i }).click()
      await expect(cancelRequest).toBeRequested()
    })
  })

  test.describe('bindings tab', _ => {
    test.beforeEach(async ({ apimap, page }) => {
      const loaded = loadQueueApi(apimap)
      await page.goto(queueUrl('bindings'))
      await loaded
    })

    test('bindings are loaded', async ({ page }) => {
      const numberOfBindings = bindingResponse.items.length
      await expect(page.locator('#bindings-count')).toHaveText(numberOfBindings.toString())
    })

    test('binding can be unbound', async ({ page }) => {
      const binding = bindingResponse.items[1] // amq.topic
      const vhost = encodeURIComponent(binding.vhost)
      const e = encodeURIComponent(binding.source)
      const q = encodeURIComponent(binding.destination)
      const p = encodeURIComponent(binding.properties_key)
      const actionPath = `api/bindings/${vhost}/e/${e}/q/${q}/${p}`
      const unbindRequest = helpers.waitForPathRequest(page, actionPath, { method: 'DELETE' })
      page.on('dialog', async dialog => await dialog.accept())
      await page.locator('#bindings-table').getByRole('button', { name: /unbind/i }).click()
      await expect(unbindRequest).toBeRequested()
    })

    test('keeps binding form values and table unchanged when add fails', async ({ page }) => {
      const errors = []
      let bindingsReloaded = false
      const source = 'missing.exchange'
      const bindingPath = `/api/bindings/${encodeURIComponent(queueVhost)}/e/${source}/q/${queueName}`
      const bindingsPath = `/api/queues/${encodeURIComponent(queueVhost)}/${queueName}/bindings`

      page.on('pageerror', err => errors.push(err.message))
      page.on('dialog', dialog => dialog.dismiss())
      page.on('request', request => {
        const url = new URL(request.url())
        if (request.method() === 'GET' && decodeURIComponent(url.pathname) === decodeURIComponent(bindingsPath)) {
          bindingsReloaded = true
        }
      })
      await page.route(url => decodeURIComponent(url.pathname) === decodeURIComponent(bindingPath), async route => {
        await route.fulfill({
          status: 403,
          contentType: 'application/json',
          body: JSON.stringify({ error: 'access_refused', reason: 'No permission' })
        })
      })

      const form = page.locator('#addBinding')
      await form.getByLabel('From exchange').fill(source)
      await form.getByLabel('Binding key').fill('rk')
      await form.getByLabel('Arguments').fill('{"x":1}')

      const failedRequest = page.waitForResponse(response => {
        const url = new URL(response.url())
        return response.request().method() === 'POST' && decodeURIComponent(url.pathname) === decodeURIComponent(bindingPath)
      })
      await form.getByRole('button', { name: /bind/i }).click()
      await failedRequest
      await page.waitForTimeout(100)

      await expect(form.getByLabel('From exchange')).toHaveValue(source)
      await expect(form.getByLabel('Binding key')).toHaveValue('rk')
      await expect(form.getByLabel('Arguments')).toHaveValue('{"x":1}')
      expect(bindingsReloaded).toBe(false)
      expect(errors).toEqual([])
    })
  })

  test.describe('messages tab', _ => {
    test.beforeEach(async ({ apimap, page }) => {
      const loaded = loadQueueApi(apimap)
      await page.goto(queueUrl('messages'))
      await loaded
    })

    test('move messages form is visible for basic-auth users', async ({ page }) => {
      await expect(page.locator('#moveMessages')).toBeVisible()
    })
  })
})
