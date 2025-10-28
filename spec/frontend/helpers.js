async function waitForPathRequest(page, path, {response = {}, method = 'GET'} = {}) {
  const matchUrl = new URL(path, 'http://example.com')
  return new Promise(async (resolve, reject) => {
    const handler = async (route, request) => {
      const requestedUrl = new URL(request.url())
      if (request.method() !== method) {
        return await route.continue()
      }
      if (decodeURIComponent(requestedUrl.pathname) !== decodeURIComponent(matchUrl.pathname)) {
        return await route.continue()
      }
      await route.fulfill({ json: response })
      await page.unroute('**/*', handler)
      resolve(request)
    }
    await page.route('**/*', handler)
  })
}

export { waitForPathRequest }
