import * as helpers from './helpers.js'
import { test, expect } from './fixtures.js';

test.describe("overview", _ => {
  test('are loaded', async ({ page, baseURL }) => {
    const apiOverviewRequest = helpers.waitForPathRequest(page, '/api/overview')
    await page.goto('/')
    await expect(apiOverviewRequest).toBeRequested()
  })

  test('definitions export trigger GET to /api/definitions for all vhosts', async ({ page, baseURL }) => {
    const definitionsRequest  = helpers.waitForPathRequest(page, '/api/definitions')
    await page.goto('/')
    await page.locator('#exportDefinitions').getByRole('button', { name: /download/i }).click()
    await expect(definitionsRequest).toBeRequested()
  })

  test('definitions export trigger GET to /api/definitions/<selected vhost>', async ({ page, baseURL }) => {
    const definitionsRequest  = helpers.waitForPathRequest(page, '/api/definitions/foo')
    await page.goto('/')
    await page.locator('#exportDefinitions').getByRole('combobox').selectOption('foo')
    await page.locator('#exportDefinitions').getByRole('button', { name: /download/i }).click()
    await expect(definitionsRequest).toBeRequested()
  })



  test('definitions import trigger POST to /api/definitions/upload', async ({ page, baseURL }) => {
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
