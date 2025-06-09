import { test as base, expect } from "@playwright/test";

const vhosts = ['foo', 'bar']
const test = base.extend(
  {
    vhosts,
    page: async ({ baseURL, page }, use) => {
      const vhostResponse = vhosts.map(x => { return {name: x} })
      await page.route(/\/api\/vhosts(\?|$)/, async route => await route.fulfill({ json: vhostResponse }))
      use(page)
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
      use(apiMap)
    }
  }
)

export { test, expect }
