function getUsername () {
  return getCookieValue('username')
}

function getPassword () {
  return window.atob(getCookieValue('auth')).split(':')[1]
}

function setUsername () {
  document.getElementById('username').textContent = getCookieValue('username')
}

function header () {
  if (getCookieValue('auth')) {
    return 'Basic ' + getCookieValue('auth')
  } else {
    return null
  }
}

function signOut () {
  removeCookie()
  window.location.assign('login')
}

function setAuth (userInfo) {
  removeCookie()
  const b64 = window.btoa(userInfo)
  storeCookie({ auth: b64 })
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
    enc.push(k + ':' + encodeURIComponent(dict[k]))
  }
  document.cookie = 'm=' + enc.join('|') + '; samesite=lax; expires=' + expirationDate.toUTCString()
}

function removeCookie () {
  document.cookie = 'm=; Max-Age=-99999999;'
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
    dict[kv[0]] = decodeURIComponent(kv[1])
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
