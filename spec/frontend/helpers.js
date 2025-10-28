function waitForPathRequest(page, path, {response = {}, method = 'GET'} = {}) {
  const matchUrl = new URL(path, 'http://example.com')
  return new Promise((resolve, reject) => {
    const handler = (route, request) => {
      const requestedUrl = new URL(request.url())
      if (request.method() !== method) {
        return route.continue()
      }
      if (decodeURIComponent(requestedUrl.pathname) !== decodeURIComponent(matchUrl.pathname)) {
        return route.continue()
      }
      route.fulfill({ json: response })
      page.unroute('**/*', handler)
      resolve(request)
    }
    page.route('**/*', handler)
  })
}

export { waitForPathRequest }
