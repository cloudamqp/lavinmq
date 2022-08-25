import * as Auth from './auth.js'
import * as Vhosts from './vhosts.js'

document.getElementById('menu').innerHTML = `
  <h1>
    <a href="/"><img id="amq-logo" src="/img/logo-lavinmq-white.png"></a>
    <small id="version"></small>
    <small id="cluster_name"></small>
  </h1>
  <ul>
    <li><a href="/">Overview</a></li>
    <li><a href="/connections">Connections</a></li>
    <li><a href="/channels">Channels</a></li>
    <li><a href="/exchanges">Exchanges</a></li>
    <li><a href="/queues">Queues</a></li>
    <li><a href="/policies">Policies</a></li>
    <li><a href="/shovels">Shovels</a></li>
    <li><a href="/federation">Federation</a></li>
    <li><a href="/vhosts">Virtual hosts</a></li>
    <li><a href="/users">Users</a></li>
    <li><a href="/nodes">Nodes</a></li>
    <li><a href="/docs/" target="_blank">HTTP API</a></li>
  </ul>
`
const path = window.location.pathname
const active = document.querySelector('#menu a[href^="' + path.slice(0,-1) + '"]')
if (active) {
  const activeLi = active.parentElement
  activeLi.classList.add('active')
}

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
