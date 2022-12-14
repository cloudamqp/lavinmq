import * as Auth from './auth.js'
import * as Vhosts from './vhosts.js'
import * as Overview from './overview.js'

document.getElementById('menu').innerHTML = `
  <h1 id="menu-header">
    <a href="/"><img id="amq-logo" src="/img/logo-lavinmq-white.png"></a>
    <small id="version"></small>
    <small id="cluster_name"></small>
  </h1>
  <ul id="menu-content">
    <li><a href="/">Overview</a></li>
    <li><a href="/connections">Connections</a></li>
    <li><a href="/channels">Channels</a></li>
    <li><a href="/exchanges">Exchanges</a></li>
    <li><a href="/queues">Queues</a></li>
    <li><a href="/policies">Policies</a></li>
    <li><a href="/operator-policies">Operator policies</a></li>
    <li><a href="/shovels">Shovels</a></li>
    <li><a href="/federation">Federation</a></li>
    <li><a href="/vhosts">Virtual hosts</a></li>
    <li><a href="/users">Users</a></li>
    <li><a href="/nodes">Nodes</a></li>
    <li><a href="/docs/" target="_blank">HTTP API</a></li>
  </ul>
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

document.getElementById("userMenuVhost").onchange = (e) => Auth.selectVhost(e)
document.getElementById("signoutLink").onclick = (e) => Auth.signOut(e)

document.getElementById('small-menu').innerHTML = `
<link rel="stylesheet" href="https://cdnjs.cloudflare.com/ajax/libs/font-awesome/4.7.0/css/font-awesome.min.css">
  <button id="menu-button" onclick=toggleSmallMenu()><i class="fa fa-lg fa-bars"></i></button>
  <ul id="small-menu-content">
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
  </ul>
`
const smallMenuContent = document.getElementById('small-menu-content')
const menuButton = document.getElementById('menu-button')
const smallMenu = document.getElementById('small-menu')

menuButton.onclick = (e) => toggleSmallMenu()

function toggleSmallMenu () {
  if (smallMenu.classList.contains('show-menu')) {
    smallMenu.classList.remove('show-menu')
    smallMenuContent.classList.remove('show-menu')
    menuButton.firstChild.classList.remove('fa-close')
    menuButton.firstChild.classList.add('fa-bars')
  } else {
    smallMenu.classList.add('show-menu')
    smallMenuContent.classList.add('show-menu')
    menuButton.firstChild.classList.remove('fa-bars')
    menuButton.firstChild.classList.add('fa-close')
  }
}

const path = window.location.pathname
var active = ''
if (window.innerWidth > 1000) {
  var active = document.querySelector('#menu a[href^="' + path.slice(0,-1) + '"]')
} else {
  var active = document.querySelector('#small-menu a[href^="' + path.slice(0,-1) + '"]')
}

if (active) {
  const activeLi = active.parentElement
  activeLi.classList.add('active')
}
