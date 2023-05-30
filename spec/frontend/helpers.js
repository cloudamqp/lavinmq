function waitForPathRequest(page, path, response_data = {}) {
  const matchUrl = new URL(path, 'http://example.com')
  return new Promise((resolve, reject) => {
    const handler = (route, request) => {
      const requestedUrl = new URL(request.url())
      if (requestedUrl.pathname !== matchUrl.pathname) {
        return route.continue()
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


export { waitForPathRequest }
