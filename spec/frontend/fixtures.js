import { test as base, expect } from "@playwright/test";

const vhosts = ['foo', 'bar']
const test = base.extend(
  {
    vhosts,
    page: async ({ baseURL, page }, use) => {
      const vhostResponse = vhosts.map(x => { return {name: x} })
      await page.route(/\/api\/vhosts(\?|$)/, async route => await route.fulfill({ json: vhostResponse }))
      use(page)
    }
  })

export { test, expect }
