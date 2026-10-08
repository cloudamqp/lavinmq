import { test, expect } from './fixtures.js'

test.describe('stream tabs', _ => {
  const queueName = 'foostream'
  const queueVhost = '/'
  const vhostParam = encodeURIComponent(queueVhost)
  const queueHash = `vhost=${vhostParam}&name=${queueName}`

  const consumerCount = 3
  const consumers = Array.from({ length: consumerCount }, (_, i) => ({
    queue: { name: queueName, vhost: queueVhost },
    consumer_tag: `foo_consumer_${i}`,
    exclusive: false,
    ack_required: true,
    prefetch_count: 1000,
    priority: 0,
    channel_details: {
      peer_host: '127.0.0.1',
      peer_port: 56861 + i,
      connection_name: 'conn_name',
      user: 'guest',
      number: i + 1,
      name: 'channel_name'
    }
  }))

  const bindingResponse = {
    items: [
      {
        source: '',
        vhost: queueVhost,
        destination: queueName,
        destination_type: 'queue',
        routing_key: queueName,
        arguments: null,
        properties_key: queueName
      },
      {
        source: 'amq.topic',
        vhost: queueVhost,
        destination: queueName,
        destination_type: 'queue',
        routing_key: queueName,
        arguments: {},
        properties_key: queueName
      }
    ],
    filtered_count: 2,
    item_count: 2,
    page: 1,
    page_count: 1,
    page_size: 100,
    total_count: 2
  }

  const emptyStat = { rate: 0.0, log: [] }
  const messageStats = Object.fromEntries(
    ['ack', 'deliver', 'deliver_get', 'confirm', 'get', 'get_no_ack', 'publish',
      'redeliver', 'reject', 'return_unroutable', 'dedup'].flatMap(k => [
      [k, 0], [`${k}_details`, emptyStat]
    ])
  )

  const queueResponse = {
    name: queueName,
    durable: true,
    exclusive: false,
    auto_delete: false,
    arguments: { 'x-queue-type': 'stream' },
    consumers: consumerCount,
    vhost: queueVhost,
    messages: 0,
    total_bytes: 0,
    messages_persistent: 0,
    ready: 0,
    messages_ready: 0,
    ready_bytes: 0,
    message_bytes_ready: 0,
    ready_avg_bytes: 0,
    unacked: 0,
    messages_unacknowledged: 0,
    unacked_bytes: 0,
    message_bytes_unacknowledged: 0,
    unacked_avg_bytes: 0,
    state: 'running',
    effective_policy_definition: {},
    effective_arguments: ['x-queue-type', 'x-max-length', 'x-max-length-bytes', 'x-max-age'],
    message_stats: messageStats,
    consumer_details: consumers
  }

  // Tab + pane locators
  const tab = (page, name) => page.locator(`.tab[data-tab="${name}"]`)
  const pane = (page, name) => page.locator(`[data-tab-content="${name}"]`)
  const badge = (page, name) => page.locator(`[data-tab="${name}"] .badge`)
  const tabNames = ['overview', 'consumers', 'bindings', 'messages']

  test.beforeEach(async ({ apimap, page }) => {
    const queueLoaded = apimap.get(`/api/queues/${vhostParam}/${queueName}`, queueResponse)
    const bindingsLoaded = apimap.get(`/api/queues/${vhostParam}/${queueName}/bindings`, bindingResponse)
    await page.goto(`/stream#${queueHash}`)
    await queueLoaded
    await bindingsLoaded
  })

  test('Overview is the default tab', async ({ page }) => {
    await expect(tab(page, 'overview')).toHaveClass(/\bactive\b/)
    await expect(pane(page, 'overview')).toBeVisible()
    await expect(pane(page, 'consumers')).toBeHidden()
    await expect(pane(page, 'bindings')).toBeHidden()
    await expect(pane(page, 'messages')).toBeHidden()
  })

  test('exactly one tab is active', async ({ page }) => {
    await expect(page.locator('.tab.active')).toHaveCount(1)
  })

  for (const name of ['consumers', 'bindings', 'messages']) {
    test(`clicking the ${name} tab shows only its pane`, async ({ page }) => {
      await tab(page, name).click()
      await expect(page.locator('.tab.active')).toHaveCount(1)
      await expect(tab(page, name)).toHaveClass(/\bactive\b/)
      await expect(pane(page, name)).toBeVisible()
      for (const other of tabNames.filter(n => n !== name)) {
        await expect(pane(page, other)).toBeHidden()
      }
    })
  }

  test('switching tabs hides the previously visible pane', async ({ page }) => {
    await tab(page, 'consumers').click()
    await expect(pane(page, 'consumers')).toBeVisible()
    await tab(page, 'bindings').click()
    await expect(pane(page, 'consumers')).toBeHidden()
    await expect(pane(page, 'bindings')).toBeVisible()
  })

  test('clicking a tab writes the tab to the URL hash', async ({ page }) => {
    await tab(page, 'bindings').click()
    await expect(tab(page, 'bindings')).toHaveClass(/\bactive\b/)
    const params = new URLSearchParams(new URL(page.url()).hash.substring(1))
    expect(params.get('tab')).toBe('bindings')
  })

  test('clicking a tab preserves the existing hash params', async ({ page }) => {
    await tab(page, 'bindings').click()
    await expect(tab(page, 'bindings')).toHaveClass(/\bactive\b/)
    const params = new URLSearchParams(new URL(page.url()).hash.substring(1))
    expect(params.get('vhost')).toBe(queueVhost)
    expect(params.get('name')).toBe(queueName)
    expect(params.get('tab')).toBe('bindings')
  })

  test('the initial tab is read from the URL hash', async ({ page }) => {
    await page.goto(`/stream#${queueHash}&tab=messages`)
    await expect(tab(page, 'messages')).toHaveClass(/\bactive\b/)
    await expect(pane(page, 'messages')).toBeVisible()
    await expect(pane(page, 'overview')).toBeHidden()
  })

  test('changing the hash activates the matching tab', async ({ page }) => {
    await expect(tab(page, 'overview')).toHaveClass(/\bactive\b/)
    await page.evaluate(hash => { location.hash = hash }, `${queueHash}&tab=consumers`)
    await expect(tab(page, 'consumers')).toHaveClass(/\bactive\b/)
    await expect(pane(page, 'consumers')).toBeVisible()
  })

  test('an unknown tab in the hash falls back to the default tab', async ({ page }) => {
    await page.goto(`/stream#${queueHash}&tab=bogus`)
    await expect(tab(page, 'overview')).toHaveClass(/\bactive\b/)
    await expect(pane(page, 'overview')).toBeVisible()
  })

  test('the consumers tab badge shows the consumer count', async ({ page }) => {
    await expect(badge(page, 'consumers')).toHaveText(consumerCount.toString())
  })

  test('consumer count stays at the total while loading more and updates on refresh', async ({ page }) => {
    let total = 35
    const allConsumers = Array.from({ length: total }, (_, index) => ({
      ...consumers[0], consumer_tag: `consumer_${index}`
    }))
    await page.route(url => decodeURIComponent(url.pathname) === `/api/queues/${queueVhost}/${queueName}`, async route => {
      const limit = Number(new URL(route.request().url()).searchParams.get('consumer_list_length'))
      await route.fulfill({ json: {
        ...queueResponse,
        consumers: total,
        consumer_details: allConsumers.slice(0, Math.min(limit, total))
      } })
    })
    await page.goto(`/stream#${queueHash}&tab=consumers`)

    const count = page.locator('#consumer-count')
    const rows = page.locator('#table tbody tr:has(button)')
    const loadMore = page.locator('#load-more-consumers')
    await expect(rows).toHaveCount(20)
    await expect(count).toHaveText('35')
    await expect(badge(page, 'consumers')).toHaveText('35')
    await expect(page.locator('#q-consumers')).toHaveText('35')
    await expect(loadMore).toHaveText('Showing 20 of total 35 consumers, click to load more')

    await loadMore.click()
    await expect(rows).toHaveCount(30)
    await expect(count).toHaveText('35')
    await expect(loadMore).toHaveText('Showing 30 of total 35 consumers, click to load more')

    await loadMore.click()
    await expect(rows).toHaveCount(35)
    await expect(count).toHaveText('35')
    await expect(loadMore).toBeHidden()

    total = 0
    await page.reload()
    await expect(rows).toHaveCount(0)
    await expect(count).toHaveText('0')
    await expect(badge(page, 'consumers')).toHaveText('0')
    await expect(page.locator('#q-consumers')).toHaveText('0')
    await expect(loadMore).toBeHidden()
  })

  test('the bindings tab badge mirrors the bindings count', async ({ page }) => {
    await expect(badge(page, 'bindings')).toHaveText(bindingResponse.items.length.toString())
  })
})
