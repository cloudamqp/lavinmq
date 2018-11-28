/* global avalanchemq */
(function () {
  window.avalanchemq = window.avalanchemq || {}

  function testLoggedIn () {
    const hash = window.location.hash
    if (hash.startsWith('#/login')) {
      const arr = hash.split('/')
      avalanchemq.auth.setAuth(arr[2] + ':' + arr[3])
      window.location.hash = ''
      window.location.assign('/')
    }
    if (window.location.pathname !== '/login') {
      request('GET', '/api/whoami').then(function () {
        avalanchemq.auth.setUsername()
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
    if (!avalanchemq.auth) {
      redirect('/login')
    }
    const hdr = avalanchemq.auth.header()
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
          // not json
          if (!response.ok) {
            throw new HTTPError(response.status, e.body || response.statusText)
          }
          return e
        })
      })
  }

  function standardErrorHandler (e) {
    if (e.status === 404) {
      console.warn(`Not found: ${e.message}`)
      throw e
    } else if (e.body) {
      window.alert(e.body)
    } else {
      console.error(e)
      throw e
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

  Object.assign(window.avalanchemq, {
    http: {
      request, redirect, standardErrorHandler
    }
  })
})()
