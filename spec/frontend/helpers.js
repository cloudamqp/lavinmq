async function waitForPathRequest(page, path, {response = {}, method = 'GET', body = undefined} = {}) {
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

function deepEqual(obj1, obj2) {
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

export { waitForPathRequest }
