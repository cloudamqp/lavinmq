import * as Auth from './auth.js'
import * as Vhosts from './vhosts.js'
import * as Overview from './overview.js'

const menuLinks = `
        <li><a id="menu-item" href="/">Overview</a></li>
        <li><a id="menu-item" href="/connections">Connections</a></li>
        <li><a id="menu-item" href="/channels">Channels</a></li>
        <li><a id="menu-item" href="/exchanges">Exchanges</a></li>
        <li><a id="menu-item" href="/queues">Queues</a></li>
        <li><a id="menu-item" href="/policies">Policies</a></li>
        <li><a id="menu-item" href="/operator-policies">Operator policies</a></li>
        <li><a id="menu-item" href="/shovels">Shovels</a></li>
        <li><a id="menu-item" href="/federation">Federation</a></li>
        <li><a id="menu-item" href="/vhosts">Virtual hosts</a></li>
        <li><a id="menu-item" href="/users">Users</a></li>
        <li><a id="menu-item" href="/nodes">Nodes</a></li>
        <li><a id="menu-item" href="/docs/" target="_blank">HTTP API</a></li>
    `

document.getElementById('menu').innerHTML = `
  <h1 id="menu-header">
    <a href="/"><img id="amq-logo" src="/img/logo-lavinmq-white.png"></a>
    <small id="version"></small>
    <small id="cluster_name"></small>
  </h1>
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

document.getElementById('small-menu').innerHTML = `
  <button id="menu-button" class="closed-menu" onclick=toggleSmallMenu()></button>
  <ul id="small-menu-content">${menuLinks}</ul>
`

document.getElementById('userMenuVhost').onchange = (e) => Auth.selectVhost(e)
document.getElementById('signoutLink').onclick = (e) => Auth.signOut(e)

const smallMenuContent = document.getElementById('small-menu-content')
const menuButton = document.getElementById('menu-button')
const smallMenu = document.getElementById('small-menu')

menuButton.onclick = (e) => toggleSmallMenu()

function toggleSmallMenu () {
  if (smallMenu.classList.contains('show-menu')) {
    hideSmallMenu()
  } else {
    showSmallMenu()
  }
}

function showSmallMenu () {
  smallMenu.classList.add('show-menu')
  smallMenuContent.classList.add('show-menu')
  menuButton.classList.add('open-menu')
  menuButton.classList.remove('closed-menu')
}

function hideSmallMenu () {
  smallMenu.classList.remove('show-menu')
  smallMenuContent.classList.remove('show-menu')
  menuButton.classList.add('closed-menu')
  menuButton.classList.remove('open-menu')
}

const path = window.location.pathname
const active = document.querySelector('#menu li a[href^="' + path + '"]')
const smallActive = document.querySelector('#small-menu a[href^="' + path + '"]')

if (active && smallActive) {
  smallActive.parentElement.classList.add('active')
  active.parentElement.classList.add('active')
}

function resizeListener () {
  if (window.innerWidth > 1000) {
    hideSmallMenu()
  }
}

window.addEventListener('resize', resizeListener)
