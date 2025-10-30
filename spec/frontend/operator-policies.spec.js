import * as helpers from './helpers.js'
import { test, expect } from './fixtures.js';

test.describe("operator-policies", _ => {
  test('are loaded', async ({ page, baseURL }) => {
    const apiPoliciesRequest = helpers.waitForPathRequest(page, '/api/operator-policies')
    await page.goto('/operator-policies')
    await expect(apiPoliciesRequest).toBeRequested()
  })

  test('add operator policy form works', async ({ page, vhosts }) => {
    const vhost = vhosts[0]
    const policyName = 'test-operator-policy'
    const apiPoliciesRequest = helpers.waitForPathRequest(page, '/api/operator-policies')
    await page.goto('/operator-policies')
    await expect(apiPoliciesRequest).toBeRequested()
    const apiAddPolicyRequest = helpers.waitForPathRequest(page, `/api/operator-policies/${vhost}/${policyName}`, { method: 'PUT' })
    await page.getByLabel('Virtual host').selectOption(vhost)
    await page.getByLabel('Name').fill(policyName)
    await page.getByLabel('Pattern').fill('^test.*')
    await page.getByLabel('Apply to').selectOption('queues')
    await page.getByLabel('Priority').fill('10')
    await page.getByRole('button', { name: /add operator policy/i }).click()
    await expect(apiAddPolicyRequest).toBeRequested()
  })
})
