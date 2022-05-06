/* global lavinmq */
(function () {
  window.lavinmq = window.lavinmq || {}

  function testLoggedIn () {
    const hash = window.location.hash
    if (hash.startsWith('#/login')) {
      const arr = hash.split('/')
      lavinmq.auth.setAuth(arr[2] + ':' + arr[3])
      window.location.hash = ''
      window.location.assign('/')
    }
    if (window.location.pathname !== '/login') {
      request('GET', '/api/whoami').then(function () {
        lavinmq.auth.setUsername()
      }).catch(function () {
        redirect('/login')
      })
    }
  }

  function redirect (path) {
    if (window.location.pathname !== path) {
      window.location.assign(path)
    }
  }

  function request (method, path, options = {}) {
    const body = options.body
    const headers = options.headers || new window.Headers()
    if (lavinmq.auth == null || lavinmq.auth.getUsername() == null) {
      return redirect('/login')
    }
    const hdr = lavinmq.auth.header()
    headers.append('Authorization', hdr)
    const opts = {
      method: method,
      headers: headers,
      credentials: 'include',
      mode: 'cors',
      redirect: 'follow'
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
      testLoggedIn()
      window.lavinmq.dom.toast("Access Refused: You don't have the permission to perform this action", "error")
    } else if (e.body) {
      alertErrorHandler(e)
    } else {
      console.error(e)
    }
    throw e
  }

  function notFoundErrorHandler (e) {
    if (e.status === 404) {
      window.location.assign('/404')
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

  testLoggedIn()

  Object.assign(window.lavinmq, {
    http: {
      request, redirect, standardErrorHandler, notFoundErrorHandler, alertErrorHandler
    }
  })
})()
