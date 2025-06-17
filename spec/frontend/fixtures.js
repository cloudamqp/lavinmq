import * as helpers from './helpers.js'
import { test as base, expect } from "@playwright/test";

const vhosts = ['foo', 'bar']
const vhostResponse = vhosts.map(x => { return {name: x} })

const test = base.extend(
  {
    vhosts,
    vhostsLoaded: async({ page }, use) => {
      const ret = new Promise(async (resolve, reject) => {
        // Wait for vhosts to be loaded and rendered
        const vhostCombobox = page.locator('#userMenuVhost')
        // + 1 for All
        await expect(vhostCombobox.locator('option')).toHaveCount(vhostResponse.length + 1)
        resolve(vhosts)
      })
      await use(ret)
    },
    page: async ({ page }, use) => {
      await page.route(/.*\/api\/vhosts(\?.*)?$/, async route => {
        await route.fulfill({ json: vhostResponse })
      })
      await use(page);
    },
    // Use to map api requests to responses
    apimap: async({ page }, use) => {
      function map(method, path, response) {
        path = RegExp.escape(path)
        method = method.toUpperCase()
        const pathExpr = new RegExp(`${path}(?!\/)`)
        return new Promise(async (resolve, reject) => {
          await page.route(pathExpr, async route => {
            if (route.request().method() == method) {
              await route.fulfill({ json: response })
              resolve()
            } else {
              await route.continue()
            }
          })
        })
      }

      const apiMap = {
        get: (path, response) => {
          return map('GET', path, response)
        },
        post: (path, response) => {
          return map('POST', path, response)
        },
       }
      await use(apiMap)
    }
  }
)

export { test, expect }
