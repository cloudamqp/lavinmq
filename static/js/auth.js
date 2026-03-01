function isJwt (value) {
  return value && value.startsWith('eyJ')
}

function getJwtPayload (jwt) {
  try {
    const parts = jwt.split('.')
    if (parts.length !== 3) return null
    const payload = JSON.parse(window.atob(parts[1].replace(/-/g, '+').replace(/_/g, '/')))
    return payload
  } catch {
    return null
  }
}

function getUsername () {
  const m = getCookie('m')
  if (!m) return
  if (isJwt(m)) {
    const payload = getJwtPayload(m)
    return payload?.preferred_username || payload?.sub || 'SSO User'
  }
  return window.atob(getAuth()).split(':')[0]
}

function getPassword () {
  const m = getCookie('m')
  if (!m) return
  if (isJwt(m)) return null
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
