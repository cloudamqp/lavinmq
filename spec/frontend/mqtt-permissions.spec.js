import * as helpers from './helpers.js'
import { test, expect } from './fixtures.js'

const groupsResponse = {
  items: [
    { name: 'default', vhost: 'foo', member_count: 1, rule_count: 1 },
    { name: 'devices', vhost: 'bar', member_count: 2, rule_count: 3 }
  ],
  filtered_count: 2, item_count: 2, page: 1, page_count: 1, page_size: 100, total_count: 2
}

const membersResponse = {
  items: [{ username: '*' }, { username: 'alice' }],
  filtered_count: 2, item_count: 2, page: 1, page_count: 1, page_size: 100, total_count: 2
}

const rulesResponse = [
  { identifier: 'own-chat', pattern: 'chat/{client_id}/#', read: true, write: true },
  { identifier: 'status', pattern: 'status/+', read: true, write: false }
]

test.describe('mqtt permissions', _ => {
  test('groups are loaded', async ({ page }) => {
    const apiGroupsRequest = helpers.waitForPathRequest(page, '/api/mqtt/permission-groups', { response: groupsResponse })
    await page.goto('/mqtt-permissions')
    await expect(apiGroupsRequest).toBeRequested()
    const row = page.locator('#groups tr[data-name=\'"devices"\']')
    await expect(row.getByRole('link', { name: 'devices' }))
      .toHaveAttribute('href', 'mqtt-permission-group#vhost=bar&name=devices')
    await expect(row.locator('td').nth(2)).toHaveText('2')
    await expect(row.locator('td').nth(3)).toHaveText('3')
  })

  test('group can be added', async ({ page, vhosts }) => {
    const vhost = vhosts[0]
    await page.goto('/mqtt-permissions')
    const apiCreateRequest = helpers.waitForPathRequest(page,
      `/api/mqtt/permission-groups/${vhost}/new-group`, { method: 'PUT' })
    await page.getByLabel('Virtual host').selectOption(vhost)
    await page.getByLabel('Name').fill('new-group')
    await page.getByRole('button', { name: /add group/i }).click()
    await expect(apiCreateRequest).toBeRequested()
  })
})

test.describe('mqtt permission group', _ => {
  const group = '/api/mqtt/permission-groups/foo/devices'

  test.beforeEach(async ({ page }) => {
    await page.route('**/api/users', route => route.fulfill({ json: [{ name: 'alice' }] }))
  })

  test('members and rules are loaded', async ({ page }) => {
    const apiMembersRequest = helpers.waitForPathRequest(page, `${group}/members`, { response: membersResponse })
    const apiRulesRequest = helpers.waitForPathRequest(page, `${group}/rules`, { response: rulesResponse })
    await page.goto('/mqtt-permission-group#vhost=foo&name=devices')
    await expect(apiMembersRequest).toBeRequested()
    await expect(apiRulesRequest).toBeRequested()
    await expect(page.locator('#members tbody tr')).toHaveCount(2)
    await expect(page.locator('#rules tbody tr')).toHaveCount(2)
    await expect(page.locator('#rules tr[data-identifier=\'"status"\']')).toContainText('status/+')
  })

  // The group name is in the URL hash, which the data sources must not read
  // as a search term: it would filter the members by the group's name.
  test('group name is not sent as a search term', async ({ page }) => {
    await page.route(`**${group}/members*`, route => route.fulfill({ json: membersResponse }))
    const membersRequest = page.waitForRequest(r => new URL(r.url()).pathname === `${group}/members`)
    await page.goto('/mqtt-permission-group#vhost=foo&name=devices')
    const query = new URL((await membersRequest).url()).searchParams
    expect(query.has('name')).toBe(false)
    expect(query.has('use_regex')).toBe(false)
  })

  test('member can be added', async ({ page }) => {
    await page.goto('/mqtt-permission-group#vhost=foo&name=devices')
    const apiAddRequest = helpers.waitForPathRequest(page, `${group}/members/bob`, { method: 'PUT' })
    await page.getByLabel('Username').fill('bob')
    await page.getByRole('button', { name: /add member/i }).click()
    await expect(apiAddRequest).toBeRequested()
  })

  test('member can be removed', async ({ page }) => {
    const apiMembersRequest = helpers.waitForPathRequest(page, `${group}/members`, { response: membersResponse })
    await page.goto('/mqtt-permission-group#vhost=foo&name=devices')
    await expect(apiMembersRequest).toBeRequested()
    const apiRemoveRequest = helpers.waitForPathRequest(page, `${group}/members/alice`, { method: 'DELETE' })
    await page.locator('#members tr[data-username=\'"alice"\']').getByRole('button', { name: /remove/i }).click()
    await expect(apiRemoveRequest).toBeRequested()
  })

  test('rule can be added', async ({ page }) => {
    await page.goto('/mqtt-permission-group#vhost=foo&name=devices')
    const apiRuleRequest = helpers.waitForPathRequest(page, `${group}/rules/sensors`, {
      method: 'PUT',
      body: { pattern: 'sensors/#', read: true, write: false }
    })
    await page.getByLabel('Identifier').fill('sensors')
    await page.getByLabel('Topic pattern').fill('sensors/#')
    await page.getByLabel('Write').uncheck()
    await page.getByRole('button', { name: /add rule/i }).click()
    await expect(apiRuleRequest).toBeRequested()
  })

  test('rules link to the rule page', async ({ page }) => {
    const apiRulesRequest = helpers.waitForPathRequest(page, `${group}/rules`, { response: rulesResponse })
    await page.goto('/mqtt-permission-group#vhost=foo&name=devices')
    await expect(apiRulesRequest).toBeRequested()
    await expect(page.locator('#rules tr[data-identifier=\'"status"\']').getByRole('link', { name: 'status' }))
      .toHaveAttribute('href', 'mqtt-permission-rule#vhost=foo&group=devices&rule=status')
  })

  test('group can be deleted', async ({ page }) => {
    await page.goto('/mqtt-permission-group#vhost=foo&name=devices')
    page.on('dialog', dialog => dialog.accept())
    const apiDeleteRequest = helpers.waitForPathRequest(page, group, { method: 'DELETE' })
    await page.getByRole('button', { name: /delete group/i }).click()
    await expect(apiDeleteRequest).toBeRequested()
  })
})

test.describe('mqtt permission rule', _ => {
  const group = '/api/mqtt/permission-groups/foo/devices'
  const rulePage = '/mqtt-permission-rule#vhost=foo&group=devices&rule=status'

  test.beforeEach(async ({ page }) => {
    await page.route(`**${group}/rules`, route => route.fulfill({ json: rulesResponse }))
  })

  test('form is filled with the rule', async ({ page }) => {
    await page.goto(rulePage)
    await expect(page.getByLabel('Topic pattern')).toHaveValue('status/+')
    await expect(page.getByLabel('Read')).toBeChecked()
    await expect(page.getByLabel('Write')).not.toBeChecked()
    await expect(page.locator('#group-link')).toHaveAttribute('href', 'mqtt-permission-group#vhost=foo&name=devices')
  })

  test('rule can be updated', async ({ page }) => {
    await page.goto(rulePage)
    await expect(page.getByLabel('Topic pattern')).toHaveValue('status/+')
    const apiRuleRequest = helpers.waitForPathRequest(page, `${group}/rules/status`, {
      method: 'PUT',
      body: { pattern: 'status/#', read: true, write: true }
    })
    await page.getByLabel('Topic pattern').fill('status/#')
    await page.getByLabel('Write').check()
    await page.getByRole('button', { name: /update/i }).click()
    await expect(apiRuleRequest).toBeRequested()
    await expect(page).toHaveURL(/mqtt-permission-group#vhost=foo&name=devices$/)
  })

  test('rule can be deleted', async ({ page }) => {
    await page.goto(rulePage)
    page.on('dialog', dialog => dialog.accept())
    const apiDeleteRequest = helpers.waitForPathRequest(page, `${group}/rules/status`, { method: 'DELETE' })
    await page.getByRole('button', { name: /delete rule/i }).click()
    await expect(apiDeleteRequest).toBeRequested()
    await expect(page).toHaveURL(/mqtt-permission-group#vhost=foo&name=devices$/)
  })
})
