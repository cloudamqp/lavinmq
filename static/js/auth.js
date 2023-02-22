function getUsername () {
  return getCookieValue('username')
}

function getPassword () {
  return window.atob(decodeURIComponent(getCookieValue('auth'))).split(':')[1]
}

function setUsername () {
  document.getElementById('username').textContent = getCookieValue('username')
}

function header () {
  if (getCookieValue('auth')) {
    return 'Basic ' + decodeURIComponent(getCookieValue('auth'))
  } else {
    return null
  }
}

function signOut () {
  clearCookieValue('auth')
  clearCookieValue('username')
  clearCookieValue('password')
  window.location.assign('login')
}

function setAuth (userInfo) {
  clearCookieValue('auth')
  clearCookieValue('username')
  clearCookieValue('password')

  const b64 = window.btoa(userInfo)
  storeCookie({ auth: encodeURIComponent(b64) })
  storeCookie({ username: userInfo.split(':')[0] })
}

function storeCookie (dict) {
  const date = new Date()
  date.setHours(date.getHours() + 8)
  Object.assign(dict, parseCookie())
  storeCookieWithExpiration(dict, date)
}

function storeCookieWithExpiration (dict, expirationDate) {
  const enc = []
  for (let k in dict) {
    enc.push(k + ':' + escape(dict[k]))
  }
  document.cookie = 'm=' + enc.join('|') + '; samesite=lax; expires=' + expirationDate.toUTCString()
}

function clearCookieValue (k) {
  const d = parseCookie()
  delete d[k]
  const date = new Date()
  date.setHours(date.getHours() + 8)
  storeCookieWithExpiration(d, date)
}

function getCookieValue (k) {
  return parseCookie()[k]
}

function parseCookie () {
  const c = getCookie('m')
  const items = c.length === 0 ? [] : c.split('|')

  const dict = {}
  for (let i in items) {
    const kv = items[i].split(':')
    dict[kv[0]] = unescape(kv[1])
  }
  return dict
}

function getCookie (key) {
  const cookies = document.cookie.split(';')
  for (let i in cookies) {
    const kv = cookies[i].trim().split('=')
    if (kv[0] === key) {
      return kv[1]
    }
  }
  return ''
}

function selectVhost (event) {
  window.sessionStorage.setItem('vhost', event.target.value)
  window.location.reload()
}

export {
  header,
  setAuth,
  storeCookie,
  signOut,
  setUsername,
  selectVhost,
  getUsername,
  getPassword
}
