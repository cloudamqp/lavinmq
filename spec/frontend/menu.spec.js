import * as helpers from './helpers.js'
import { test, expect } from './fixtures.js'

test.describe('menu', _ => {
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
