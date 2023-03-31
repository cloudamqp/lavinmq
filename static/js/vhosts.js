import * as HTTP from './http.js'

let loadedVhosts

function fetch() {
  const vhost = window.sessionStorage.getItem('vhost')
  const url = 'api/vhosts'
  if (!loadedVhosts) {
    loadedVhosts = HTTP.request('GET', url).then(function (vhosts) {
      if (vhost !== '_all' && !vhosts.some(vh => vh.name === vhost)) {
        window.sessionStorage.removeItem('vhost')
      }
      return vhosts
    }).catch(function (e) {
      console.error(e)
      return null
    })
  }
  return loadedVhosts
}

function addVhostOptions (formId, options) {
  options = options ?? {}
  const addAllOpt = options["addAll"] ?? false
  return fetch().then(vhosts => {
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
    if (addAllOpt) {
      select.add(new Option('All', '_all', true, false))
    }
    vhosts.forEach(vhost => {
      select.add(new Option(vhost.name, vhost.name, false, vhost.name === selectedVhost))
    })
    return vhosts
  })
}

addVhostOptions('user-vhost', {addAll: true}).then(() => {
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
