(function () {
  window.avalanchemq = window.avalanchemq || {}

  function fetch (cb) {
    const url = '/api/users'
    const raw = sessionStorage.getItem(url)
    if (raw) {
      var users = JSON.parse(raw)
      cb(users)
    }
    avalanchemq.http.request('GET', url).then(function (users) {
      try {
        sessionStorage.setItem('/api/users', JSON.stringify(users))
      } catch (e) {
        console.error('Saving sessionStorage', e)
      }
      cb(users)
    }).catch(function (e) {
      console.error(e.message)
    })
  }

  Object.assign(window.avalanchemq, {
    users: {
      fetch
    }
  })
})()
