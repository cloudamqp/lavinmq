function getUsername () {
  return window.atob(getAuth()).split(':')[0]
}

function getPassword () {
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

async function login (username, password) {
  const auth = window.btoa(`${username}:${password}`)
  document.cookie = `m=|:${encodeURIComponent(auth)}; samesite=strict; max-age=${60 * 60 * 8}`
  const result = await whoAmI(true)
  if (!result) document.cookie = 'm=; max-age=0'
  return result
}

function logout () {
  window.localStorage.removeItem('lmq.whoami')
  document.cookie = 'm=; max-age=0'
  window.location.assign('login')
}

async function fetchAndCacheWhoAmI () {
  return window.fetch('api/whoami')
    .then(async resp => {
      if (resp.ok) {
        return await resp.json().then(data => {
          data._ts = Date.now()
          delete data.password_hash
          delete data.hashing_algorithm
          window.localStorage.setItem('lmq.whoami', JSON.stringify(data))
          return data
        })
      } else {
        window.localStorage.removeItem('lmq.whoami')
        return null
      }
    })
}

async function whoAmI (forceReload = false) {
  if (!forceReload) {
    const data = window.localStorage.getItem('lmq.whoami')
    if (data) {
      const cached = JSON.parse(data)
      if (cached && cached.name == getUsername()) {
        const expired = (cached._ts + 3600 * 1000) <= Date.now()
        if (expired) {
          fetchAndCacheWhoAmI()
        }
        return cached
      }
    }
  }
  return fetchAndCacheWhoAmI()
}

export {
  whoAmI,
  login,
  logout,
  getUsername,
  getPassword
}
