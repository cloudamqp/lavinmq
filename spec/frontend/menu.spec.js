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

  test.describe("collapsible", _ => {
    test('hover menu toggle button shows tooltip', async ({ page }) => {
      await page.goto('/')
      const toggleButton = await page.getByRole('button', { name: 'Collapse sidebar' })
      await toggleButton.hover()
      await expect(toggleButton.getByText('Collapse sidebar')).toBeVisible()
    })

    test('click menu toggle button collapsed menu', async ({ page }) => {
      await page.goto('/')
      const toggleButton = await page.getByRole('button', { name: 'Collapse sidebar' })
      await toggleButton.click()
      const menu = await page.locator('#menu-content')
      const clusterGroup = await menu.getByText('Cluster')
      const overviewMenuItem = await menu.getByRole('listitem', { name: 'Overview' })
      await expect(page.locator('html')).toContainClass('menu-collapsed')
      await expect(clusterGroup).toBeHidden()
      await expect(overviewMenuItem).toBeHidden()
      await expect(page.getByRole('button', { name: 'Expand sidebar' })).toHaveCount(1)
    })

    test('hover collapsed menu item show tooltip', async ({ page }) => {
      await page.goto('/')
      const toggleButton = await page.getByRole('button', { name: 'Collapse sidebar' })
      const overviewItem = await page.getByRole('link', { name: 'Overview' })
      await toggleButton.click()
      await overviewItem.hover()
      await expect(await overviewItem.locator('.menu-item-label')).toBeVisible()
    })

    test('state is stored', async ({ page }) => {
      await page.goto('/')
      const toggleButton = await page.getByRole('button', { name: 'Collapse sidebar' })
      await toggleButton.click()
      await page.goto('/nodes')
      await expect(page.locator('html')).toContainClass('menu-collapsed')
    })
  })
})
