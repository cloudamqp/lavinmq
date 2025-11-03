import * as helpers from './helpers.js'
import { test, expect } from './fixtures.js';

const helperLinks = [
  { tag: 'max-length', value: 0 },
  { tag: 'max-length-bytes', value: 0 },
  { tag: 'message-ttl', value: 0 },
  { tag: 'expires', value: 0 },
  { tag: 'delivery-limit', value: 0 },
  { tag: 'max-age', value: '1M' }
]

test.describe("operator-policies", _ => {
  test('are loaded', async ({ page, baseURL }) => {
    const apiPoliciesRequest = helpers.waitForPathRequest(page, '/api/operator-policies')
    await page.goto('/operator-policies')
    await expect(apiPoliciesRequest).toBeRequested()
  })

  test('add operator policy form works', async ({ page, vhosts }) => {
    const vhost = vhosts[0]
    const policyName = 'test-operator-policy'
    const expectedBody = {
      pattern: '^test.*',
      'apply-to': 'queues',
      priority: 10,
      definition: {
        'max-length': 5000,
        'message-ttl': 60000
      }
    }
    await page.goto('/operator-policies')

    const apiAddPolicyRequest = helpers.waitForPathRequest(page,
      `/api/operator-policies/${vhost}/${policyName}`,
      {
         method: 'PUT',
         body: expectedBody
      }
    )
    await page.getByLabel('Virtual host').selectOption(vhost)
    await page.getByLabel('Name').fill(policyName)
    await page.getByLabel('Pattern').fill(expectedBody.pattern)
    await page.getByLabel('Apply to').selectOption(expectedBody['apply-to'])
    await page.getByLabel('Priority').fill(String(expectedBody.priority))
    await page.getByLabel('Definition').fill(JSON.stringify(expectedBody.definition))
    await page.getByRole('button', { name: /add operator policy/i }).click()
    await expect(apiAddPolicyRequest).toBeRequested()
  })

  test.describe('definition helper links', () => {
    test('correct number of definition helper links', async ({ page }) => {
      await page.goto('/operator-policies')
      const helperLinksInView = await page.locator('#dataTags a[data-tag]').count()
      expect(helperLinksInView).toBe(helperLinks.length)
    })

    for (const helper of helperLinks) {
      test(`${helper.tag} works`, async ({ page }) => {
        await page.goto('/operator-policies')
        const definitionTextarea = page.getByLabel('Definition')
        await page.locator(`#dataTags a[data-tag="${helper.tag}"]`).click()
        const currentValue = await definitionTextarea.inputValue()
        const definition = JSON.parse(currentValue)
        expect(definition[helper.tag]).toBe(helper.value)
      })
    }
  })
})
