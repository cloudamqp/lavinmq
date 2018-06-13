(function () {
  window.avalanchemq = window.avalanchemq || {}
  let avalanchemq = window.avalanchemq

  function fetch (cb) {
    let url = '/api/vhosts'
    let raw = localStorage.getItem(url)
    if (raw) {
      var vhosts = JSON.parse(raw)
      cb(vhosts)
    }
    avalanchemq.http.request('GET', url).then(function (vhosts) {
      try {
        localStorage.setItem('/api/vhosts', JSON.stringify(vhosts))
      } catch (e) {
        console.error('Saving localStorage', e)
      }
      cb(vhosts)
    }).catch(function (e) {
      console.error(e.message)
    })
  }

  Object.assign(window.avalanchemq, {
    vhosts: {
      fetch
    }
  })
})()
