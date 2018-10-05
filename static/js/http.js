(function () {
  window.avalanchemq = window.avalanchemq || {}

  function testLoggedIn () {
    const hash = location.hash
    if (hash.startsWith('#/login')) {
      const arr = hash.split('/')
      avalanchemq.auth.setAuth(arr[2] + ':' + arr[3])
      location.hash = ''
      window.location.assign('/')
    }
    if (location.pathname !== '/login') {
      request('GET', '/api/whoami').then(function () {
        avalanchemq.auth.setUsername()
      }).catch(function () {
        redirect('/login')
      })
    }
  }

  function redirect (path) {
    if (location.pathname !== path) {
      window.location.assign(path)
    }
  }

  function request (method, path, options = {}) {
    const body = options.body
    const headers = options.headers || new Headers()
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
    if (body instanceof FormData) {
      headers.delete('Content-Type') // browser will set to multipart with boundary
      opts.body = body
    } else if (body) {
      headers.append('Content-Type', 'application/json')
      opts.body = JSON.stringify(body)
    }
    return fetch(path, opts)
      .then(function (response) {
        return response.json().then(json => {
          if (!response.ok) {
            throw { status: response.status, body: json }
          }
          return json
        }).catch(function (e) {
          // not json
          if (!response.ok) {
            throw { status: response.status, body: response.statusText }
          }
          return e
        })
      })
  }

  function standardErrorHandler (e) {
    if (e.status === 404) {
      avalanchemq.http.redirect('/404')
    } else if (e.body) {
      alert(e.body)
    } else {
      console.error(e)
    }
  }

  testLoggedIn()

  Object.assign(window.avalanchemq, {
    http: {
      request, redirect, standardErrorHandler
    }
  })
})()
