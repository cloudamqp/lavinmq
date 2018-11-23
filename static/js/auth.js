(function () {
  window.avalanchemq = window.avalanchemq || {}

  function setUsername () {
    document.querySelector('#username').innerText = getCookieValue('username')
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
    window.location.assign('/login')
  }

  function setAuth (userInfo) {
    clearCookieValue('auth')
    clearCookieValue('username')

    var b64 = window.btoa(userInfo)
    storeCookie({ 'auth': encodeURIComponent(b64) })
    storeCookie({ 'username': userInfo.split(':')[0] })
  }

  function storeCookie (dict) {
    var date = new Date()
    date.setHours(date.getHours() + 8)
    Object.assign(dict, parseCookie())
    storeCookieWithExpiration(dict, date)
  }

  function storeCookieWithExpiration (dict, expirationDate) {
    var enc = []
    for (var k in dict) {
      enc.push(k + ':' + escape(dict[k]))
    }
    document.cookie = 'm=' + enc.join('|') + '; expires=' + expirationDate.toUTCString()
  }

  function clearCookieValue (k) {
    var d = parseCookie()
    delete d[k]
    var date = new Date()
    date.setHours(date.getHours() + 8)
    storeCookieWithExpiration(d, date)
  }

  function getCookieValue (k) {
    return parseCookie()[k]
  }

  function parseCookie () {
    var c = getCookie('m')
    var items = c.length === 0 ? [] : c.split('|')

    var dict = {}
    for (var i in items) {
      var kv = items[i].split(':')
      dict[kv[0]] = unescape(kv[1])
    }
    return dict
  }

  function getCookie (key) {
    var cookies = document.cookie.split(';')
    for (var i in cookies) {
      var kv = cookies[i].trim().split('=')
      if (kv[0] === key) {
        return kv[1]
      }
    }
    return ''
  }

  function selectVhost (select) {
    window.sessionStorage.setItem('vhost', select.value)
    window.location.reload()
  }

  Object.assign(window.avalanchemq, {
    auth: {
      header, setAuth, storeCookie, signOut, setUsername, selectVhost
    }
  })
})()
