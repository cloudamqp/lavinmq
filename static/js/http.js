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
        // not logged in
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
        if (response.status === 401) {
          if (location.pathname !== '/401') {
            redirect('/401')
          } else {
            redirect('/login')
          }
        }
        return response
      })
      .then(function (response) {
        return response.json().then(json => {
          if (response.status === 404) {
            alert(json.reason || "Resource not found")
          }
          if (!response.ok) {
            throw new Error(json.reason)
          }
          return json
        }).catch(function (e) {
          // not json
          return e
        })
      })
  }

  testLoggedIn()

  Object.assign(window.avalanchemq, {
    http: {
      request, redirect
    }
  })
})()
