// @ts-check
import { defineConfig, devices, expect } from '@playwright/test'
import './expect_extensions.js'

/**
 * @see https://playwright.dev/docs/test-configuration
 */
module.exports = defineConfig(
  {
    testDir: '',
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

