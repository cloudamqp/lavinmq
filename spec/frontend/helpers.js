function waitForPathRequest(page, path, response_data = {}) {
  return new Promise((resolve, reject) => {
    page.route('**/*', (route, request) => {
      const requestedUrl = new URL(request.url())
      if (requestedUrl.pathname !== path) {
        return route.fallback()
      }
      page.unroute('**/*')
      route.fulfill({ json: response_data })
      resolve(request)
    })
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
