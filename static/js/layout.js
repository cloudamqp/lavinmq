import * as Auth from './auth.js'

document.getElementById('username').textContent = Auth.getUsername()

const menuButton = document.getElementById('menu-button')
const menuContent = document.getElementById('menu-content')

menuButton.onclick = (e) => {
  menuButton.classList.toggle('open-menu')
  menuContent.classList.toggle('show-menu')
}

document.getElementById('userMenuVhost').onchange = (e) => {
  window.sessionStorage.setItem('vhost', e.target.value)
  window.location.reload()
}

document.getElementById('signoutLink').onclick = () => {
  document.cookie = 'm=; max-age=0'
  window.location.assign('login')
}
