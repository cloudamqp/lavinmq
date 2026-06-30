import * as helpers from './helpers.js'
import { test, expect } from './fixtures.js';

test.describe("shovels", _ => {
  const shovelVhost = '/';
  const shovelName = 'shovel1'
  const parameterShovelsResponse = {
    "items": [
      {
        "name": shovelName,
        "value": {"src-uri":"amqp://","dest-uri":"amqp://","src-prefetch-count":1000,"src-delete-after":"never","reconnect-delay":120,"ack-mode":"on-confirm","src-queue":"qdest","dest-queue":"qsrc"},
        "component": "shovel",
        "vhost": shovelVhost
      }
    ]
    ,"filtered_count": 1,
    "item_count": 1,
    "page": 1,
    "page_count": 1,
    "page_size": 100,
    "total_count": 1
  }
  // Builds a /api/shovels status row carrying all runtime fields. Override any
  // field per test; counters default to representative non-zero values.
  function makeStatus (overrides = {}) {
    return [{
      name: shovelName,
      vhost: shovelVhost,
      state: 'Running',
      error: null,
      message_count: 0,
      confirmed: 100,
      retried: 5,
      dead_lettered: 2,
      aborted: 1,
      consecutive_failures: 0,
      consecutive_aborts: 0,
      abort_threshold: 10,
      ...overrides
    }]
  }
  let shovelsResponse

  // Re-register /api/shovels with custom runtime fields and reload the page.
  async function reloadWith (page, apimap, overrides) {
    const req = apimap.get('/api/shovels', makeStatus(overrides))
    await page.goto('/shovels')
    await req
  }

  test.beforeEach(async ({ apimap, page }) => {
    shovelsResponse = makeStatus()
    const parameterShovelsRequest = apimap.get(`/api/parameters/shovel`, parameterShovelsResponse)
    const shovelsRequest = apimap.get(`/api/shovels`, shovelsResponse)
    await page.clock.install()
    await page.goto(`/shovels`)
    await parameterShovelsRequest
    await shovelsRequest
  })


  test('are loaded', async ({ page, baseURL }) => {
    await expect(page.locator('#pagename-label')).toHaveText("1")
  })

  test('are refreshed automatically', async({ page }) => {
    // Verify that at least 3 requests are made
    for (let i=0; i<3; i++) {
      const apiShovelsRequest = helpers.waitForPathRequest(page, '/api/parameters/shovel')
      await page.clock.runFor(10000) // advance time by 10 seconds
      await expect(apiShovelsRequest).toBeRequested()
    }
  })

  test('can be deleted', async ({ page }) => {
    const actionPath = `/api/parameters/shovel/${encodeURIComponent(shovelVhost)}/${encodeURIComponent(shovelName)}`
    const deleteRequest = helpers.waitForPathRequest(page, actionPath, { method: 'DELETE' })
    page.on('dialog', async dialog => await dialog.accept())
    await page.locator(`#table tr[data-name='"${shovelName}"']`).getByRole('button', { name: /delete/i }).click()
    await expect(deleteRequest).toBeRequested()
  })

  test('shows a colored state badge', async ({ page }) => {
    const badge = page.locator(`#table tr[data-name='"${shovelName}"'] .badge-state`)
    await expect(badge).toHaveText('Running')
    await expect(badge).toHaveClass(/badge-state--running/)
  })

  test('shows the error inline when state is Error', async ({ page, apimap }) => {
    await reloadWith(page, apimap, { state: 'Error', error: 'destination unusable after 10 attempts' })
    const row = page.locator(`#table tr[data-name='"${shovelName}"']`)
    await expect(row.locator('.badge-state')).toHaveClass(/badge-state--error/)
    await expect(row.locator('.state-error')).toHaveText('destination unusable after 10 attempts')
  })

  test('shows a degraded indicator when backing off', async ({ page, apimap }) => {
    await reloadWith(page, apimap, { consecutive_failures: 3 })
    const row = page.locator(`#table tr[data-name='"${shovelName}"']`)
    await expect(row.locator('.state-degraded')).toContainText('retrying')
  })

  test('shows abort progress toward error-out', async ({ page, apimap }) => {
    await reloadWith(page, apimap, { consecutive_aborts: 4, abort_threshold: 10 })
    const row = page.locator(`#table tr[data-name='"${shovelName}"']`)
    await expect(row.locator('.state-degraded')).toContainText('4/10')
  })

  test('computes message rate from successive polls', async ({ page }) => {
    const rate = page.locator(`#table tr[data-name='"${shovelName}"'] .rate-cell`)
    await expect(rate).not.toContainText('msg/s') // no prior sample yet
    shovelsResponse[0].message_count = 50
    await page.clock.runFor(5000) // one auto-reload, 5s elapsed -> 50/5 = 10
    await expect(rate).toContainText('msg/s')
  })
})
