import * as helpers from './helpers.js'
import { test, expect } from './fixtures.js'

test.describe('menu', _ => {
  test('overview menu item has active class on overview page', async ({ page }) => {
    await page.goto('/')
    const activeItems = await page.locator('#menu-content .active')
    const activeItem = await activeItems.first()
    await expect(activeItems).toHaveCount(1)
    await expect(activeItem).toHaveText('Overview')
  })

  test('nodes menu item has active class on nodes page', async ({ page }) => {
    await page.goto('/nodes')
    const activeItems = await page.locator('#menu-content .active')
    const activeItem = await activeItems.first()
    await expect(activeItems).toHaveCount(1)
    await expect(activeItem).toHaveText('Nodes')
  })

  test('logs menu item has active class on logs page', async ({ page }) => {
    await page.goto('/logs')
    const activeItems = await page.locator('#menu-content .active')
    const activeItem = await activeItems.first()
    await expect(activeItems).toHaveCount(1)
    await expect(activeItem).toHaveText('Logs')
  })

  test('connections menu item has active class on connections page', async ({ page }) => {
    await page.goto('/connections')
    const activeItems = await page.locator('#menu-content .active')
    const activeItem = await activeItems.first()
    await expect(activeItems).toHaveCount(1)
    await expect(activeItem).toHaveText('Connections')
  })

  test('channels menu item has active class on channels page', async ({ page }) => {
    await page.goto('/channels')
    const activeItems = await page.locator('#menu-content .active')
    const activeItem = await activeItems.first()
    await expect(activeItems).toHaveCount(1)
    await expect(activeItem).toHaveText('Channels')
  })

  test('consumers menu item has active class on consumers page', async ({ page }) => {
    await page.goto('/consumers')
    const activeItems = await page.locator('#menu-content .active')
    const activeItem = await activeItems.first()
    await expect(activeItems).toHaveCount(1)
    await expect(activeItem).toHaveText('Consumers')
  })

  test('exchanges menu item has active class on exchanges page', async ({ page }) => {
    await page.goto('/exchanges')
    const activeItems = await page.locator('#menu-content .active')
    const activeItem = await activeItems.first()
    await expect(activeItems).toHaveCount(1)
    await expect(activeItem).toHaveText('Exchanges')
  })

  test('queues menu item has active class on queues page', async ({ page }) => {
    await page.goto('/queues')
    const activeItems = await page.locator('#menu-content .active')
    const activeItem = await activeItems.first()
    await expect(activeItems).toHaveCount(1)
    await expect(activeItem).toHaveText('Queues')
  })

  test('shovels menu item has active class on shovels page', async ({ page }) => {
    await page.goto('/shovels')
    const activeItems = await page.locator('#menu-content .active')
    const activeItem = await activeItems.first()
    await expect(activeItems).toHaveCount(1)
    await expect(activeItem).toHaveText('Shovels')
  })

  test('federation menu item has active class on federation page', async ({ page }) => {
    await page.goto('/federation')
    const activeItems = await page.locator('#menu-content .active')
    const activeItem = await activeItems.first()
    await expect(activeItems).toHaveCount(1)
    await expect(activeItem).toHaveText('Federation')
  })

  test('vhosts menu item has active class on vhosts page', async ({ page }) => {
    await page.goto('/vhosts')
    const activeItems = await page.locator('#menu-content .active')
    const activeItem = await activeItems.first()
    await expect(activeItems).toHaveCount(1)
    await expect(activeItem).toHaveText('Virtual hosts')
  })

  test('policies menu item has active class on policies page', async ({ page }) => {
    await page.goto('/policies')
    const activeItems = await page.locator('#menu-content .active')
    const activeItem = await activeItems.first()
    await expect(activeItems).toHaveCount(1)
    await expect(activeItem).toHaveText('Policies')
  })

  test('operator-policies menu item has active class on operator-policies page', async ({ page }) => {
    await page.goto('/operator-policies')
    const activeItems = await page.locator('#menu-content .active')
    const activeItem = await activeItems.first()
    await expect(activeItems).toHaveCount(1)
    await expect(activeItem).toHaveText('Operator policies')
  })

  test('users menu item has active class on users page', async ({ page }) => {
    await page.goto('/users')
    const activeItems = await page.locator('#menu-content .active')
    const activeItem = await activeItems.first()
    await expect(activeItems).toHaveCount(1)
    await expect(activeItem).toHaveText('Users')
  })
})
