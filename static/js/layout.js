(function () {
  document.getElementsByTagName('aside')[0].innerHTML = `
    <ul>
      <li><a href="/" class="active">Overview</a></li>
      <li><a href="/connections">Connections</a></li>
      <li><a href="/channels">Channels</a></li>
      <li><a href="/queues">Queues</a></li>
      <li><a href="/exchanges">Exchanges</a></li>
      <li><a href="/users">Users</a></li>
      <li><a href="/vhosts">Virtual Hosts</a></li>
      <li><a href="/shovels">Shovels</a></li>
    </ul>
  `
  const path = window.location.pathname
  document.querySelectorAll("aside a").forEach(a => a.classList.remove('active'))
  const active = document.querySelector('aside a[href="' + path + '"]')
  if (active) {
    active.classList.add('active')
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
