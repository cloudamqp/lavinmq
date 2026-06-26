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
      updateVersionFromResponse(response)
      if (!response.ok) {
        const error = { status: response.status, reason: response.statusText, is_error: true }
        return response.json()
          .then(json => { error.reason = json.reason; return error })
          .finally(() => { standardErrorHandler(error) })
      } else { return response.json().catch(() => null) }
    })
}

// The server advertises its version via the `LavinMQ-Version` header on every
// response. Pick it up here so the UI shows the current version (cached in
// sessionStorage, displayed by inline script in header.shtml) without an extra request.
function updateVersionFromResponse (response) {
  const version = response.headers.get('LavinMQ-Version')
  if (!version) return
  window.sessionStorage.setItem('lavinmq_version', version)
  const el = document.getElementById('version')
  if (el) el.textContent = version
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
