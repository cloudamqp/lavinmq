import { test, expect } from './fixtures.js'

test.describe('menu', _ => {
  // Pages mark the active menu item by setting a class on <html> matching a
  // class on the corresponding <li> (e.g. <html class="nodes"> + <li class="nodes">).
  // CSS in static/main.css:1042 highlights the item via `html.<page> li.<page> a`.
  const pages = [
    { path: '/', cls: 'overview', label: 'Overview' },
    { path: '/nodes', cls: 'nodes', label: 'Nodes' },
    { path: '/logs', cls: 'logs', label: 'Logs' },
    { path: '/connections', cls: 'connections', label: 'Connections' },
    { path: '/channels', cls: 'channels', label: 'Channels' },
    { path: '/consumers', cls: 'consumers', label: 'Consumers' },
    { path: '/exchanges', cls: 'exchanges', label: 'Exchanges' },
    { path: '/queues', cls: 'queues', label: 'Queues' },
    { path: '/shovels', cls: 'shovels', label: 'Shovels' },
    { path: '/federation', cls: 'federation', label: 'Federation' },
    { path: '/vhosts', cls: 'vhosts', label: 'Virtual hosts' },
    { path: '/policies', cls: 'policies', label: 'Policies' },
    { path: '/operator-policies', cls: 'operator-policies', label: 'Operator policies' },
    { path: '/users', cls: 'users', label: 'Users' }
  ]

  for (const { path, cls, label } of pages) {
    test(`${label} menu item is active on ${path} page`, async ({ page }) => {
      await page.goto(path)
      const activeLink = page.locator(`#menu-content li.${cls} > a`)
      await expect(activeLink).toHaveText(label)

      // The active <a> must have a background-color distinct from every
      // other menu <a>, proving the CSS actually highlights it.
      const activeBg = await activeLink.evaluate(el => window.getComputedStyle(el).backgroundColor)
      const otherBgs = await page
        .locator(`#menu-content li:not(.${cls}):not(.http-api) > a`)
        .evaluateAll(els => els.map(el => window.getComputedStyle(el).backgroundColor))
      expect(otherBgs.length).toBeGreaterThan(0)
      for (const bg of otherBgs) {
        expect(bg).not.toEqual(activeBg)
      }
    })
  }

  test.describe('collapsible', _ => {
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
