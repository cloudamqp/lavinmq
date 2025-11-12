import * as helpers from './helpers.js'
import { test, expect } from './fixtures.js';

test.describe("menu", _ => {
  test('overview menu item has active class on overview page', async ({ page }) => {
    await page.goto('/')
    const overviewMenuItem = page.locator('#menu-content li:has(a[href="."])')
    await expect(overviewMenuItem).toHaveClass('active')
  })

  test('nodes menu item has active class on nodes page', async ({ page }) => {
    await page.goto('/nodes')
    const nodesMenuItem = page.locator('#menu-content li:has(a[href="nodes"])')
    await expect(nodesMenuItem).toHaveClass('active')
  })

  test('logs menu item has active class on logs page', async ({ page }) => {
    await page.goto('/logs')
    const logsMenuItem = page.locator('#menu-content li:has(a[href="logs"])')
    await expect(logsMenuItem).toHaveClass('active')
  })

  test('connections menu item has active class on connections page', async ({ page }) => {
    await page.goto('/connections')
    const connectionsMenuItem = page.locator('#menu-content li:has(a[href="connections"])')
    await expect(connectionsMenuItem).toHaveClass('active')
  })

  test('channels menu item has active class on channels page', async ({ page }) => {
    await page.goto('/channels')
    const channelsMenuItem = page.locator('#menu-content li:has(a[href="channels"])')
    await expect(channelsMenuItem).toHaveClass('active')
  })

  test('consumers menu item has active class on consumers page', async ({ page }) => {
    await page.goto('/consumers')
    const consumersMenuItem = page.locator('#menu-content li:has(a[href="consumers"])')
    await expect(consumersMenuItem).toHaveClass('active')
  })

  test('exchanges menu item has active class on exchanges page', async ({ page }) => {
    await page.goto('/exchanges')
    const exchangesMenuItem = page.locator('#menu-content li:has(a[href="exchanges"])')
    await expect(exchangesMenuItem).toHaveClass('active')
  })

  test('queues menu item has active class on queues page', async ({ page }) => {
    await page.goto('/queues')
    const queuesMenuItem = page.locator('#menu-content li:has(a[href="queues"])')
    await expect(queuesMenuItem).toHaveClass('active')
  })

  test('shovels menu item has active class on shovels page', async ({ page }) => {
    await page.goto('/shovels')
    const shovelsMenuItem = page.locator('#menu-content li:has(a[href="shovels"])')
    await expect(shovelsMenuItem).toHaveClass('active')
  })

  test('federation menu item has active class on federation page', async ({ page }) => {
    await page.goto('/federation')
    const federationMenuItem = page.locator('#menu-content li:has(a[href="federation"])')
    await expect(federationMenuItem).toHaveClass('active')
  })

  test('vhosts menu item has active class on vhosts page', async ({ page }) => {
    await page.goto('/vhosts')
    const vhostsMenuItem = page.locator('#menu-content li:has(a[href="vhosts"])')
    await expect(vhostsMenuItem).toHaveClass('active')
  })

  test('policies menu item has active class on policies page', async ({ page }) => {
    await page.goto('/policies')
    const policiesMenuItem = page.locator('#menu-content li:has(a[href="policies"])')
    await expect(policiesMenuItem).toHaveClass('active')
  })

  test('operator-policies menu item has active class on operator-policies page', async ({ page }) => {
    await page.goto('/operator-policies')
    const opPoliciesMenuItem = page.locator('#menu-content li:has(a[href="operator-policies"])')
    await expect(opPoliciesMenuItem).toHaveClass('active')
  })

  test('users menu item has active class on users page', async ({ page }) => {
    await page.goto('/users')
    const usersMenuItem = page.locator('#menu-content li:has(a[href="users"])')
    await expect(usersMenuItem).toHaveClass('active')
  })

  test('only one menu item has active class at a time', async ({ page }) => {
    await page.goto('/queues')
    const activeMenuItems = page.locator('#menu-content li.active')
    await expect(activeMenuItems).toHaveCount(1)
  })
})
