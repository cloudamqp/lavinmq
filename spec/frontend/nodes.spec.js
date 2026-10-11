import * as helpers from './helpers.js'
import { test, expect } from './fixtures.js';

test.describe("nodes", _ => {
  test('are loaded', async ({ page, baseURL }) => {
    const apiNodesRequest = helpers.waitForPathRequest(page, '/api/nodes')
    await page.goto('/nodes')
    await expect(apiNodesRequest).toBeRequested()
  })

  test('are refreshed automatically', async({ page }) => {
    await page.clock.install()
    await page.goto(`/nodes`)
    // Verify that at least 3 requests are made
    for (let i=0; i<3; i++) {
      const apiNodesRequest = helpers.waitForPathRequest(page, `/api/nodes`)
      await page.clock.runFor(10000) // advance time by 10 seconds
      await expect(apiNodesRequest).toBeRequested()
    }
  })

  test('hide the cluster members without a raft cluster', async ({ page }) => {
    await page.route('**/api/cluster', route => route.fulfill({ status: 404, json: { error: 'Object Not Found' } }))
    const response = page.waitForResponse('**/api/cluster')
    await page.goto('/nodes')
    await response
    await expect(page.locator('#cluster-section')).toBeHidden()
    await expect(page.locator('#addClusterMember')).toBeHidden()
  })

  test('show the cluster members, and let administrators manage them', async ({ page }) => {
    const cluster = {
      leader: '10.0.0.1:5680',
      term: 3,
      isr: ['aaa', 'bbb'],
      members: [
        { address: '10.0.0.1:5680', node_id: 'aaa', role: 'voter', in_isr: true, match_index: 7, caught_up: true, leader: true },
        { address: '10.0.0.2:5680', node_id: 'bbb', role: 'voter', in_isr: true, match_index: 7, caught_up: true, leader: false },
        { address: '10.0.0.3:5680', node_id: 'ccc', role: 'learner', in_isr: false, match_index: 5, caught_up: false, leader: false }
      ]
    }
    await page.route('**/api/cluster', route => route.fulfill({ json: cluster }))
    await page.goto('/nodes')
    const rows = page.locator('#cluster-members tbody tr')
    await expect(rows).toHaveCount(3)
    await expect(rows.nth(0)).toContainText('(leader)')
    await expect(rows.nth(0).getByRole('button')).toHaveCount(0)
    await expect(rows.nth(1).getByRole('button', { name: 'Make leader' })).toBeVisible()
    await expect(rows.nth(2).getByRole('button', { name: 'Promote' })).toBeVisible()

    const promote = helpers.waitForPathRequest(page, '/api/cluster/members/ccc/promote', { method: 'POST' })
    await rows.nth(2).getByRole('button', { name: 'Promote' }).click()
    await expect(promote).toBeRequested()

    page.once('dialog', dialog => dialog.accept())
    const remove = helpers.waitForPathRequest(page, '/api/cluster/members/bbb', { method: 'DELETE' })
    await rows.nth(1).getByRole('button', { name: 'Remove' }).click()
    await expect(remove).toBeRequested()

    const add = helpers.waitForPathRequest(page, '/api/cluster/members', { method: 'POST', body: { address: '10.0.0.4:5680' } })
    await page.locator('#addClusterMember input[name=address]').fill('10.0.0.4:5680')
    await page.getByRole('button', { name: 'Add member' }).click()
    await expect(add).toBeRequested()
  })
})
