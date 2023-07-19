function request (method, path, options = {}) {
  const body = options.body
  const headers = options.headers || new window.Headers()
  const opts = {
    method,
    headers
  }
  if (body instanceof window.FormData) {
    headers.delete('Content-Type') // browser will set to multipart with boundary
    opts.body = body
  } else if (body) {
    headers.append('Content-Type', 'application/json')
    opts.body = JSON.stringify(body)
  }
  return window.fetch(path, opts)
    .then(response => {
      if (!response.ok) {
        const error = { status: response.status, reason: response.statusText }
        return response.json()
          .then(json => { error.reason = json.reason })
          .finally(() => { standardErrorHandler(error) })
      } else { return response.json().catch(() => null) }
    })
}

function alertErrorHandler (e) {
  window.alert(e.body || e.message || e.reason)
}

function standardErrorHandler (e) {
  if (e.status === 404) {
    console.warn(`Not found: ${e.message}`)
  } else if (e.status === 401) {
    return window.location.assign('login')
  } else if (e.body || e.message || e.reason) {
    return alertErrorHandler(e)
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

export {
  request, standardErrorHandler, notFoundErrorHandler, alertErrorHandler
}
