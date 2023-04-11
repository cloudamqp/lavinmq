function request (method, path, options = {}) {
  const body = options.body
  const headers = options.headers || new window.Headers()
  const opts = {
    method: method,
    headers: headers,
  }
  if (body instanceof window.FormData) {
    headers.delete('Content-Type') // browser will set to multipart with boundary
    opts.body = body
  } else if (body) {
    headers.append('Content-Type', 'application/json')
    opts.body = JSON.stringify(body)
  }
  return window.fetch(path, opts)
    .then(function (response) {
      return response.json().then(json => {
        if (!response.ok) {
          throw new HTTPError(response.status, json.reason)
        }
        return json
      }).catch(function (e) {
        if (!response.ok) {
          throw new HTTPError(response.status, e.body || e.message || response.statusText)
        }
        return e
      })
    })
}

function alertErrorHandler (e) {
  window.alert(e.body || e.message)
}

function standardErrorHandler (e) {
  if (e.status === 404) {
    console.warn(`Not found: ${e.message}`)
  } else if (e.status === 401) {
    window.location.assign("login")
  } else if (e.body) {
    alertErrorHandler(e)
  } else {
    console.error(e)
  }
  throw e
}

function notFoundErrorHandler (e) {
  if (e.status === 404) {
    window.location.assign('404')
  } else {
    standardErrorHandler(e)
  }
}

class HTTPError extends Error {
  constructor (status, body) {
    super(`${status}: ${body}`)
    this.status = status
    this.body = body
  }
}

export {
  request, standardErrorHandler, notFoundErrorHandler, alertErrorHandler
}
