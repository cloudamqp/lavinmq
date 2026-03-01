function getUsername () {
  const oauthUser = getCookie('oauth_user')
  if (oauthUser) return decodeURIComponent(oauthUser)
  const m = getCookie('m')
  if (!m || m.startsWith('eyJ')) return
  return window.atob(getAuth()).split(':')[0]
}

function getPassword () {
  const m = getCookie('m')
  if (!m || m.startsWith('eyJ')) return null
  return window.atob(getAuth()).split(':')[1]
}

function getAuth () {
  const m = getCookie('m')
  if (!m) return
  const idx = m.lastIndexOf(':')
  return decodeURIComponent(m.substring(idx + 1))
}

function getCookie (key) {
  return document.cookie
    .split('; ')
    .find(c => c.startsWith(`${key}=`))
    ?.split('=')[1]
}

export {
  getUsername,
  getPassword
}
