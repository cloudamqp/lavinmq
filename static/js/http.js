import * as Reachability from './reachability.js'

async function request (method, path, options = {}) {
  const body = options.body
  const headers = options.headers || new window.Headers()
  const opts = {
    method,
    headers,
    signal: options.signal
  }
  if (body instanceof window.FormData) {
    headers.delete('Content-Type') // browser will set to multipart with boundary
    opts.body = body
  } else if (body) {
    headers.append('Content-Type', 'application/json')
    opts.body = JSON.stringify(body)
  }

  let response
  try {
    response = await window.fetch(path, opts)
  } catch (err) {
    Reachability.recordFailure(options.signal?.reason?.message ?? err.message)
    throw err
  }
  updateVersionFromResponse(response)
  if (response.ok) {
    Reachability.recordSuccess()
    return response.json().catch(() => null)
  }

  const error = { status: response.status, reason: response.statusText }
  const json = await response.json().catch(() => null)
  if (json?.reason) error.reason = json.reason

  if ([502, 503, 504].includes(response.status)) {
    Reachability.recordFailure(error.reason)
  } else if (response.status !== 401) {
    Reachability.recordSuccess()
  }
  standardErrorHandler(error)
  throw error
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
