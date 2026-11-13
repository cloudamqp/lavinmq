import { expect } from '@playwright/test'

async function waitForPathRequest (page, path, { response = {}, method = 'GET', body = undefined } = {}) {
  const matchUrl = new URL(path, 'http://example.com')
  const pathCondition = (url) => {
    const requestedUrl = new URL(url)
    return decodeURIComponent(requestedUrl.pathname) === decodeURIComponent(matchUrl.pathname)
  }
  return new Promise((resolve, reject) => {
    const handler = async (route, request) => {
      const requestedUrl = new URL(request.url())
      if (request.method() !== method) {
        return route.fallback()
      }
      if (body !== undefined) {
        const requestBody = await request.postDataJSON()
        if (!deepEqual(requestBody, body)) {
          return route.fallback()
        }
      }
      page.unroute(pathCondition, handler)
      route.fulfill({ json: response }).then(_ => resolve(request))
    }
    page.route(pathCondition, handler)
  })
}

function deepEqual (obj1, obj2) {
  if (obj1 === obj2) return true
  if (obj1 == null || obj2 == null) return false
  if (typeof obj1 !== 'object' || typeof obj2 !== 'object') return false
  const keys1 = Object.keys(obj1)
  const keys2 = Object.keys(obj2)
  if (keys1.length !== keys2.length) return false
  for (const key of keys1) {
    if (!keys2.includes(key)) return false
    if (!deepEqual(obj1[key], obj2[key])) return false
  }
  return true
}

async function trackCspViolations (page) {
  await page.addInitScript(() => {
    window.__cspViolations = []
    document.addEventListener('securitypolicyviolation', (e) => {
      if (e.violatedDirective === 'style-src-attr' && e.target.nodeName === 'SELECT' && !e.target.hasAttribute('style')) return
      window.__cspViolations.push({
        directive: e.violatedDirective,
        blockedURI: e.blockedURI,
        sourceFile: e.sourceFile,
        lineNumber: e.lineNumber,
        sample: e.sample
      })
    })
  })
  return () => page.evaluate(() => window.__cspViolations || [])
}

async function pollerSettled (page, { hung = 0 } = {}) {
  const inFlight = () => page.evaluate(async url => (await import(url)).inFlightCount(), '/js/poller.js')
  await expect.poll(inFlight).toBeLessThanOrEqual(hung)
}

// Opens a <dialog> via its declarative "+ Add ..." trigger button, identified by
// the dialog's own id rather than button text — several dialogs now have a
// trigger button whose accessible name is a substring of their own submit
// button's name (e.g. "+ Add operator policy" vs "Add operator policy"), so
// matching by text would be ambiguous once both are in the DOM.
async function openDialog (page, dialogId) {
  await page.locator(`[command="show-modal"][commandfor="${dialogId}"]`).click()
}

export { waitForPathRequest, trackCspViolations, pollerSettled, openDialog }
