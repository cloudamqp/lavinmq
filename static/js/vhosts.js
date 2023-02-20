import * as HTTP from './http.js'

function fetch (cb) {
  const vhost = window.sessionStorage.getItem('vhost')
  const url = 'api/vhosts'
  return HTTP.request('GET', url).then(function (vhosts) {
    if (vhost !== '_all' && !vhosts.some(vh => vh.name === vhost)) {
      window.sessionStorage.removeItem('vhost')
    }
    cb(vhosts)
  }).catch(function (e) {
    console.error(e)
    cb(null)
  })
}

function addVhostOptions (formId) {
  return fetch(vhosts => {
    const select = document.forms[formId].elements.vhost
    while (select.options.length) {
      select.remove(0)
    }

    if (!vhosts) {
      if (formId === 'user-vhost') {
        return
      }
      const err = document.createElement('span')
      err.id = 'error-msg'
      err.textContent = 'Error fetching data: Please try to refresh the page!'
      select.parentElement.insertAdjacentElement('beforebegin',err)
      return
    }

    const selectedVhost = window.sessionStorage.getItem('vhost')
    for (let i = 0; i < vhosts.length; i++) {
      const opt = document.createElement('option')
      opt.label = vhosts[i].name
      opt.value = vhosts[i].name
      opt.selected = vhosts[i].name === selectedVhost
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
      window.sessionStorage.setItem('vhost', vhost)
    }
  } else {
    window.sessionStorage.setItem('vhost', '_all')
  }
})

export {
  fetch, addVhostOptions
}
