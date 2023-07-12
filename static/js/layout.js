import * as Auth from './auth.js'
import * as Helpers from './helpers.js'

document.getElementById('username').textContent = Auth.getUsername()

const menuButton = document.getElementById('menu-button')
const menuContent = document.getElementById('menu-content')

menuButton.onclick = (e) => {
  menuButton.classList.toggle('open-menu')
  menuContent.classList.toggle('show-menu')
}

Helpers.addVhostOptions('user-vhost', { addAll: true }).then(() => {
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

document.getElementById('userMenuVhost').onchange = (e) => {
  window.sessionStorage.setItem('vhost', e.target.value)
  window.location.reload()
}

document.getElementById('signoutLink').onclick = () => {
  document.cookie = 'm=; max-age=0'
  window.location.assign('login')
}
