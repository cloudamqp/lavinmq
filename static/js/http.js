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
  if (response.ok) {
      updateVersionFromResponse(response)
    return response.json().catch(() => null)
    }

  const error = { status: response.status, reason: response.statusText, is_error: true }
  try {
    const json = await response.json()
    if (json?.reason) error.reason = json.reason
  } catch (_) {}

  standardErrorHandler(error)
}

// The server advertises its version via the `LavinMQ-Version` header on every
// response. Pick it up here so the UI shows the current version (cached in
// sessionStorage, displayed by inline script in header.shtml) without an extra request.
function updateVersionFromResponse (response) {
  const version = response.headers.get('LavinMQ-Version')
  if (!version) return
  window.sessionStorage.setItem('lavinmq_version', version)
  const el = document.getElementById('version')
  if (el) {
    if (el.textContent === '') {
      el.textContent = version
    } else if (el.textContent !== version) {
      window.location.reload() // if new version then html/js might have changed too
    }
  }
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
    throw error
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
