import * as Auth from './auth.js'
import * as Vhosts from './vhosts.js'
import * as Overview from './overview.js'

const menuLinks = `
        <li><a id="menu-item" href=".">Overview</a></li>
        <li><a id="menu-item" href="connections">Connections</a></li>
        <li><a id="menu-item" href="channels">Channels</a></li>
        <li><a id="menu-item" href="exchanges">Exchanges</a></li>
        <li><a id="menu-item" href="queues">Queues</a></li>
        <li><a id="menu-item" href="policies">Policies</a></li>
        <li><a id="menu-item" href="operator-policies">Operator policies</a></li>
        <li><a id="menu-item" href="shovels">Shovels</a></li>
        <li><a id="menu-item" href="federation">Federation</a></li>
        <li><a id="menu-item" href="vhosts">Virtual hosts</a></li>
        <li><a id="menu-item" href="users">Users</a></li>
        <li><a id="menu-item" href="nodes">Nodes</a></li>
        <li><a id="menu-item" href="logs">Log</a></li>
        <li><a id="menu-item" href="docs" target="_blank">HTTP API</a></li>
    `

document.getElementById('menu').innerHTML = `
  <h1 id="menu-header">
    <a href=""><img id="amq-logo" src="img/logo-lavinmq-white.png"></a>
    <small id="version"></small>
    <small id="cluster_name"></small>
  </h1>
  <button id="menu-button" class="closed-menu" onclick=toggleMenu()></button>
  <ul id="menu-content">${menuLinks}</ul>
`

document.getElementById('user-menu').innerHTML = `
   <ul>
    <li><span id="username"></span></li>
    <li>
      <form id="user-vhost">
        <label>
          <span>vhost:</span>
          <select id="userMenuVhost" name="vhost"></select>
        </label>
      </form>
    </li>
    <li>
      <a id="signoutLink" href="#">
        <span class="head">🙈</span>&nbsp; Sign out</span>
      </a>
    </li>
  </ul>
`

document.getElementById('userMenuVhost').onchange = (e) => Auth.selectVhost(e)
document.getElementById('signoutLink').onclick = (e) => Auth.signOut(e)

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

const path = window.location.pathname
const active = document.querySelector('#menu li a[href^="' + path + '"]')

if (active) {
  active.parentElement.classList.add('active')
}

function resizeListener () {
  if (window.innerWidth > 1000) {
    hideMenu()
  }
}

window.addEventListener('resize', resizeListener)
