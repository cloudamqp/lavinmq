import { expect } from '@playwright/test';

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


