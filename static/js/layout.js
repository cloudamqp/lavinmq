(function () {
  document.getElementsByTagName('aside')[0].innerHTML = `
    <h1>
      AvalancheMQ
      <small id="version"></small>
    </h1>
    <ul>
      <li><a href="/">Overview</a></li>
      <li><a href="/connections">Connections</a></li>
      <li><a href="/channels">Channels</a></li>
      <li>
        <a href="/queues">Queues</a>
        <ul class="hide">
          <li><a href="#declare">Add Queue</a></li>
        </ul>
      </li>
      <li>
        <a href="/exchanges">Exchanges</a>
        <ul class="hide">
          <li><a href="#addExchange">Add Exchange</a></li>
        </ul>
      </li>
      <li>
        <a href="/users">Users</a>
        <ul class="hide">
          <li><a href="#createUser">Add User</a></li>
        </ul>
      </li>
      <li>
        <a href="/vhosts">Virtual hosts</a>
        <ul class="hide">
          <li><a href="#createVhost">Add Virtual host</a></li>
        </ul>
      </li>
      <li>
        <a href="/policies">Policies</a>
        <ul class="hide">
          <li><a href="#createPolicy">Add Policy</a></li>
        </ul>
      </li>
      <li>
        <a href="/shovels">Shovels</a>
        <ul class="hide">
          <li><a href="#createShovel">Add Shovel</a></li>
        </ul>
      </li>
      <li>
        <a href="/federation">Federation</a>
        <ul class="hide">
          <li><a href="#createUpstream">Add Upstream</a></li>
        </ul>
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
  document.querySelectorAll("aside li").forEach(li => {
    li.classList.remove('active')
    toggleSubMenu(li, true)
  })
  const active = document.querySelector('aside a[href="' + path + '"]')
  if (active) {
    const activeLi = active.parentElement
    activeLi.classList.add('active')
    toggleSubMenu(activeLi, false)
  }

  document.getElementsByTagName('header')[0].insertAdjacentHTML('beforeend', `
    <div class="user-menu">
      <div class="slide-in-area">
        <ul>
          <li>
            <form id="user-vhost">
              <label>
                <span>Virtual host:</span>
                <select id="userMenuVhost" name="vhost" onchange="avalanchemq.auth.selectVhost(this)"></select>
              </label>
            </form>
          </li>
          <li><button onclick="avalanchemq.auth.signOut()">Logout</button></li>
        </ul>
      </div>
      <a id="user-info">
        <span class="head">👤</span><span id="username"></span>
        <span id="vhost"></span>
      </a>
    </div>
  `)

  document.getElementById('user-info').addEventListener('click', (evt) => {
    evt.stopPropagation()
    document.querySelector('.user-menu ul').classList.toggle('show')
  })
  document.querySelector('.user-menu').addEventListener('click', (evt) => {
    evt.stopPropagation()
  })
  window.addEventListener('click', (evt) => {
    if (!evt.target.closest('.user-menu')) {
      document.querySelector('.user-menu ul').classList.remove('show')
    }
  })

  document.getElementsByTagName('footer')[0].innerHTML = `
    AvalancheMQ is open source and developed by
    <a href="http://www.84codes.com" target="_blank"><img class="logo" src="/img/logo-84codes.svg"></a>
  `
})()
