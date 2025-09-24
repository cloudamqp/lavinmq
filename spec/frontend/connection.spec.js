import * as helpers from './helpers.js'
import { test, expect } from './fixtures.js';

test.describe("connection", _ => {
  test('info is loaded', async ({ page, baseURL }) => {
    const connectionName = "127.0.0.1:63610 -> 127.0.0.1:12345"
    const apiConnectionRequest = helpers.waitForPathRequest(page, `/api/connections/${connectionName}`)
    await page.goto(`/connection#name=${connectionName}`)
    await expect(apiConnectionRequest).toBeRequested()
  })
  test('channels are loaded', async ({ page, baseURL }) => {
    const connectionName = "127.0.0.1:63610 -> 127.0.0.1:12345"
    const apiChannelsRequest = helpers.waitForPathRequest(page, `/api/connections/${connectionName}/channels`)
    await page.goto(`/connection#name=${connectionName}`)
    await expect(apiChannelsRequest).toBeRequested()
  })

  test('close button trigger DELETE to api/connections/<name>', async ({ page, baseURL }) => {
    const response = {}
    const connectionName = "127.0.0.1:63610 -> 127.0.0.1:12345"
    await page.goto(`/connection#name=${connectionName}`)
    const apiDeleteRequest = helpers.waitForPathRequest(page, `/api/connections/${connectionName}`, {method: 'DELETE'})
    await page.locator('#closeConnection').getByRole('button').click()
    await expect(apiDeleteRequest).toBeRequested()
  })

})
