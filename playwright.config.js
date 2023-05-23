// @ts-check
import { defineConfig, devices, expect } from '@playwright/test';

expect.extend(
  {
    async toBeRequested(received, params) {
      return received.then(req => {
        return {
          message: _ => 'requested',
          pass: true
        }
      }).catch(e => {
        return {
          message: _ => e.message,
          pass: false
        }
      })
    },

    async toHaveQueryParams(received, query) {
      try {
        const request = await received
        const requestedUrl = new URL(request.url())
        const expectedParams = new URLSearchParams(query)
        const actualParams = requestedUrl.searchParams
        for (let [name, value] of expectedParams.entries()) {
          const actualValue = actualParams.get(name)
          if (actualValue !== value) {
            return {
              message: _ => `expected query param '${name}' to be '${value}', got '${actualValue}'`,
              pass: false
            }
          }
        }
        return {
          message: _ => "yes",
          pass: true
        }
      } catch(e) {
        return {
          message: _ => e.toString(),
          pass: false
        }
      }
    }
  })

/**
 * @see https://playwright.dev/docs/test-configuration
 */
module.exports = defineConfig(
  {
    testDir: './spec/frontend',
    /* Run tests in files in parallel */
    fullyParallel: true,
    /* Fail the build on CI if you accidentally left test.only in the source code. */
    forbidOnly: !!process.env.CI,
    /* Retry on CI only */
    retries: process.env.CI ? 2 : 0,
    /* Opt out of parallel tests on CI. */
    workers: process.env.CI ? 1 : undefined,
    /* Reporter to use. See https://playwright.dev/docs/test-reporters */
    reporter: 'html',
    /* Shared settings for all the projects below. See https://playwright.dev/docs/api/class-testoptions. */
    use: {
      /* Base URL to use in actions like `await page.goto('/')`. */
      baseURL: process.env.BASE_URL ?? 'http://127.0.0.1:15672',

      /* Collect trace when retrying the failed test. See https://playwright.dev/docs/trace-viewer */
      trace: 'on-first-retry',
    },

    /* Configure projects for major browsers */
    projects: [
      {
        name: 'setup',
        testMatch: /.*\.setup\.js/
      },
      {
        name: 'chromium',
        use: { 
          ...devices['Desktop Chrome'],
          storageState: 'playwright/.auth/user.json',
        },
        dependencies: ['setup'],
      },
      {
        name: 'firefox',
        use: {
          ...devices['Desktop Firefox'],
          storageState: 'playwright/.auth/user.json',
        },
        dependencies: ['setup'],
      },

      {
        name: 'webkit',
        use: {
          ...devices['Desktop Safari'],
          storageState: 'playwright/.auth/user.json'
        },
        dependencies: ['setup'],
      },
    ],
  })

