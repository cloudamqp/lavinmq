/* global lavinmq */
(function () {
  window.lavinmq = window.lavinmq || {}

  function fetch (cb) {
    const url = '/api/users'
    const raw = window.sessionStorage.getItem(url)
    if (raw) {
      var users = JSON.parse(raw)
      cb(users)
    }
    lavinmq.http.request('GET', url).then(function (users) {
      try {
        window.sessionStorage.setItem('/api/users', JSON.stringify(users))
      } catch (e) {
        console.error('Saving sessionStorage', e)
      }
      cb(users)
    }).catch(function (e) {
      console.error(e.message)
    })
  }

  Object.assign(window.lavinmq, {
    users: {
      fetch
    }
  })
})()
