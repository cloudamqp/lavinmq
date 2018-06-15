(function () {
  window.avalanchemq = window.avalanchemq || {}

  function fetch (cb) {
    const url = '/api/vhosts'
    const raw = localStorage.getItem(url)
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

  function addVhostOptions (formId) {
    fetch(vhosts => {
      const select = document.forms[formId].elements['vhost']
      while (select.options.length) {
        select.remove(0)
      }
      for (let i = 0; i < vhosts.length; i++) {
        const opt = document.createElement('option')
        opt.text = vhosts[i].name
        select.add(opt)
      }
    })
  }

  Object.assign(window.avalanchemq, {
    vhosts: {
      fetch, addVhostOptions
    }
  })
})()
