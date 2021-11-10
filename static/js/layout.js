(function () {
  document.getElementsByTagName('aside')[0].innerHTML = `
    <h1>
      <a href="/"><img id="amq-logo" src="/img/logo-avalanche-mq.png"></a>
      <small id="version"></small>
      <small id="cluster_name"></small>
    </h1>
    <ul>
      <li><a href="/">Overview</a></li>
      <li><a href="/connections">Connections</a></li>
      <li><a href="/channels">Channels</a></li>
      <li>
        <a href="/queues">Queues</a>
        <ul class="hide">
          <li><a href="/queues#declare">Add Queue</a></li>
        </ul>
      </li>
      <li>
        <a href="/exchanges">Exchanges</a>
        <ul class="hide">
          <li><a href="/exchanges#addExchange">Add Exchange</a></li>
        </ul>
      </li>
      <li>
        <a href="/users">Users</a>
        <ul class="hide">
          <li><a href="/users#createUser">Add User</a></li>
        </ul>
      </li>
      <li>
        <a href="/vhosts">Virtual hosts</a>
        <ul class="hide">
          <li><a href="/vhosts#createVhost">Add Virtual host</a></li>
        </ul>
      </li>
      <li><a href="/nodes">Nodes</a></li>
      <li>
        <a href="/policies">Policies</a>
        <ul class="hide">
          <li><a href="/policies#createPolicy">Add Policy</a></li>
        </ul>
      </li>
      <li>
        <a href="/shovels">Shovels</a>
        <ul class="hide">
          <li><a href="/shovels#createShovel">Add Shovel</a></li>
        </ul>
      </li>
      <li>
        <a href="/federation">Federation</a>
        <ul class="hide">
          <li><a href="/federation#createUpstream">Add Upstream</a></li>
        </ul>
      </li>
      <li>
        <a href="/docs/" target="_blank">HTTP API</a>
      </li>
    </ul>
  `

  function toggleSubMenu (el, toggle) {
    const subMenu = el.querySelector('ul')
    if (subMenu) {
      subMenu.classList.toggle('hide', toggle)
    }
  }
  const path = window.location.pathname
  document.querySelectorAll('aside li').forEach(li => {
    li.classList.remove('active')
    toggleSubMenu(li, true)
  })
  const active = document.querySelector('aside a[href^="' + path.slice(0,-1) + '"]')
  if (active) {
    const activeLi = active.parentElement
    activeLi.classList.add('active')
    toggleSubMenu(activeLi, false)
  }

  document.getElementsByTagName('header')[0].insertAdjacentHTML('beforeend', `
     <ul id="user-menu">
      <li><span id="username"></span></li>
      <li>
        <form id="user-vhost">
          <label>
            <span>vhost:</span>
            <select id="userMenuVhost" name="vhost" onchange="avalanchemq.auth.selectVhost(this)"></select>
          </label>
        </form>
      </li>
      <li>
        <a href="#" onclick="avalanchemq.auth.signOut()">
          <span class="head">🙈</span>&nbsp; Sign out</span>
        </a>
      </li>
    </div>
  `)

  document.getElementsByTagName('footer')[0].innerHTML = `
    AvalancheMQ is open source and developed by
    <a href="https://www.84codes.com" target="_blank"><img class="logo" src="/img/logo-84codes.svg"></a>
  `
})()
