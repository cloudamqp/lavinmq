/* global avalanchemq */
(function () {
  window.avalanchemq = window.avalanchemq || {}

  function fetch (cb) {
    const url = '/api/vhosts'
    const raw = window.sessionStorage.getItem(url)
    if (raw) {
      var vhosts = JSON.parse(raw)
      cb(vhosts)
    }
    return avalanchemq.http.request('GET', url).then(function (vhosts) {
      try {
        window.sessionStorage.setItem('/api/vhosts', JSON.stringify(vhosts))
      } catch (e) {
        console.error('Saving sessionStorage', e)
      }
      cb(vhosts)
    }).catch(function (e) {
      console.error(e.message)
    })
  }

  function addVhostOptions (formId) {
    return fetch(vhosts => {
      const select = document.forms[formId].elements['vhost']
      while (select.options.length) {
        select.remove(0)
      }
      for (let i = 0; i < vhosts.length; i++) {
        const opt = document.createElement('option')
        opt.text = vhosts[i].name
        opt.value = vhosts[i].name
        select.add(opt)
      }
    })
  }

  addVhostOptions('user-vhost').then(() => {
    const allOpt = '<option value="_all">All</option>'
    document.querySelector('#userMenuVhost').insertAdjacentHTML('afterbegin', allOpt)
    const vhost = window.sessionStorage.getItem('vhost')
    if (vhost) {
      const opt = document.querySelector('#userMenuVhost option[value="' + vhost + '"]')
      if (opt) {
        document.querySelector('#userMenuVhost').value = vhost
      }
    } else {
      window.sessionStorage.setItem('vhost', '_all')
    }
  })

  Object.assign(window.avalanchemq, {
    vhosts: {
      fetch, addVhostOptions
    }
  })
})()
