async function waitForPathRequest(page, path, {response = {}, method = 'GET'} = {}) {
  const matchUrl = new URL(path, 'http://example.com')
  const pathCondition = (url) => {
    const requestedUrl = new URL(url)
    return decodeURIComponent(requestedUrl.pathname) === decodeURIComponent(matchUrl.pathname)
  }
  return new Promise((resolve, reject) => {
    const handler = (route, request) => {
      const requestedUrl = new URL(request.url())
      if (request.method() !== method) {
        return route.fallback()
      }
      page.unroute(pathCondition, handler)
      route.fulfill({ json: response }).then(_ => resolve(request))
    }
    page.route(pathCondition, handler)
  })
}

export { waitForPathRequest }
