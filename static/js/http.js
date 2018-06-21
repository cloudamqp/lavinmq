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
      request('GET', '/api/overview').then(function (response) {
        try {
          localStorage.setItem('/api/overview', JSON.stringify(response))
        } catch (e) {
          console.error('Saving localStorage', e)
        }
      })
    }
  }

  function redirectToLogin () {
    window.location.assign('/login')
  }

  function request (method, path, options = {}) {
    const body = options.body
    const headers = options.headers || new Headers()
    if (!avalanchemq.auth && window.location.pathname !== '/login') {
      redirectToLogin()
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
          redirectToLogin()
        } else if (!response.ok) {
          throw Error(response.statusText)
        }
        return response
      })
      .then(function (response) {
        return response.json().catch(function (e) {
          // not json
          return e
        })
      })
  }

  testLoggedIn()

  Object.assign(window.avalanchemq, {
    http: {
      request, redirectToLogin
    }
  })
})()
