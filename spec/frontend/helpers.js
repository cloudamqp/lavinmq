function waitForPathRequest(page, path, response_data = {}) {
  const matchUrl = new URL(path, 'http://example.com')
  return new Promise((resolve, reject) => {
    const handler = (route, request) => {
      const requestedUrl = new URL(request.url())
      if (requestedUrl.pathname !== matchUrl.pathname) {
        return route.fallback()
      }
      page.unroute('**/*', handler)
      route.fulfill({ json: response_data })
      resolve(request)
    }
    page.route('**/*', handler)
  })
  return page.waitForRequest(req => {
    const reqUrl = new URL(req.url())
    return reqUrl.pathname == path
  }, { timeout: 1000 })
}

function setupVhostResponse(test) {
  const vhosts = ['foo', 'bar']
  test.beforeEach(async ({ page }) => {
    const vhostResponse = vhosts.map(x => { return {name: x} })
    await page.route(/\/api\/vhosts/, route => route.fulfill({ json: vhostResponse }))
  })
}

export { waitForPathRequest, setupVhostResponse }
