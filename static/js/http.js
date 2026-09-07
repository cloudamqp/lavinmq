async function request (method, path, options = {}) {
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

  const response = await window.fetch(path, opts)
  if (response.ok) return response.json().catch(() => null)

  const error = { status: response.status, reason: response.statusText }
  const json = await response.json().catch(() => null)
  if (json?.reason) error.reason = json.reason

  standardErrorHandler(error)
  throw error
}

function alertErrorHandler (e) {
  window.alert(e.body || e.message || e.reason)
}

function standardErrorHandler (e) {
  if (e.status === 404) {
    console.warn(`Not found: ${e.reason || e.message}`)
  } else if (e.status === 401) {
    window.location.assign('login')
  } else if (e.body || e.message || e.reason) {
    alertErrorHandler(e)
  } else {
    console.error(e)
  }
}

function url (strings, ...params) {
  return params.reduce(
    (res, param, i) => {
      if (param instanceof NoUrlEscapeString) {
        return res + param.toString() + strings[i + 1]
      } else {
        return res + encodeURIComponent(param) + strings[i + 1]
      }
    },
    strings[0])
}

class NoUrlEscapeString {
  constructor (value) {
    this.value = value
  }

  toString () {
    return this.value
  }
}

function noencode (v) {
  return new NoUrlEscapeString(v)
}

export {
  request,
  url,
  noencode
}
