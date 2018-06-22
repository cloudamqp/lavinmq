(function () {
  document.getElementsByTagName('aside')[0].innerHTML = `
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
        <a href="/shovels">Shovels</a>
        <ul class="hide">
          <li><a href="#createShovel">Add Shovel</a></li>
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

  document.getElementsByTagName('header')[0].innerHTML = `
    <h1>
      AvalancheMQ
      <small id="version"></small>
    </h1>
    <div id="user-info">
      <span>👤:&nbsp;</span><span id="username"></span>
      <span id="vhost"></span>
    </div>
  `
})()
