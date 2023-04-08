import * as Auth from './auth.js'

const menuButton = document.getElementById('menu-button')
const menuContent = document.getElementById('menu-content')

menuButton.onclick = (e) => toggleMenu()

function toggleMenu () {
  if (menuContent.classList.contains('show-menu')) {
    hideMenu()
  } else {
    showMenu()
  }
}

function showMenu () {
  menuContent.classList.add('show-menu')
  menuButton.classList.add('open-menu')
  menuButton.classList.remove('closed-menu')
}

function hideMenu () {
  menuContent.classList.remove('show-menu')
  menuButton.classList.add('closed-menu')
  menuButton.classList.remove('open-menu')
}

function resizeListener () {
  if (window.innerWidth > 1000) {
    hideMenu()
  }
}

window.addEventListener('resize', resizeListener)
document.getElementById('userMenuVhost').onchange = (e) => Auth.selectVhost(e)
document.getElementById('signoutLink').onclick = (e) => Auth.signOut(e)
