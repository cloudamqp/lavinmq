import * as helpers from './helpers.js'
import { test, expect } from './fixtures.js';

test.describe("overview", _ => {
  test('is loaded', async ({ page }) => {
    const apiOverviewRequest = helpers.waitForPathRequest(page, '/api/overview')
    await page.goto('/')
    await expect(apiOverviewRequest).toBeRequested()
  })

  test('is refreshed automatically', async({ page }) => {
    await page.clock.install()
    await page.goto(`/`)
    // Verify that at least 3 requests are made
    for (let i=0; i<3; i++) {
      const apiOverviewRequest = helpers.waitForPathRequest(page, `/api/overview`)
      await page.clock.runFor(10000) // advance time by 10 seconds
      await expect(apiOverviewRequest).toBeRequested()
    }
  })

  test('definitions export trigger GET to /api/definitions for all vhosts', async ({ page }) => {
    const definitionsRequest  = helpers.waitForPathRequest(page, '/api/definitions')
    await page.goto('/')
    await page.locator('#exportDefinitions').getByRole('button', { name: /download/i }).click()
    await expect(definitionsRequest).toBeRequested()
  })

  test('definitions export trigger GET to /api/definitions/<selected vhost>', async ({ page, vhostsLoaded }) => {
    await page.goto('/')
    const vhosts = await vhostsLoaded
    const definitionsRequest  = helpers.waitForPathRequest(page, `/api/definitions/${vhosts[0]}`)
    await page.locator('#exportDefinitions').getByRole('combobox').selectOption(vhosts[0])
    await page.locator('#exportDefinitions').getByRole('button', { name: /download/i }).click()
    await expect(definitionsRequest).toBeRequested()
  })



  test('definitions import trigger POST to /api/definitions/upload', async ({ page }) => {
    const definitionsRequest = helpers.waitForPathRequest(page, '/api/definitions/upload', { method: 'POST' })
    await page.goto('/')

    const importDefs = page.locator('#importDefinitions')
    const fileChooserPromise = page.waitForEvent('filechooser');
    await importDefs.getByLabel('File').click();
    const fileChooser = await fileChooserPromise;
    await fileChooser.setFiles({'name': 'definitions.json', buffer: Buffer.from('{}')});
    await importDefs.getByRole('button', { name: /upload/i }).click()

    await expect(definitionsRequest).toBeRequested()
  })
})
