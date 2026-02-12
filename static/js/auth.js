import { stateClasses } from "./helpers.js"

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

async function login(username, password) {
  const auth = window.btoa(`${username}:${password}`)
  document.cookie = `m=|:${encodeURIComponent(auth)}; samesite=strict; max-age=${60 * 60 * 8}`
  return whoAmI(true)
}

function logout() {
  stateClasses.remove(/^user-tag-/)
  window.localStorage.removeItem('lmq.whoami')
  document.cookie = 'm=; max-age=0'
}

async function whoAmI(forceReload = false) {
  if (!forceReload) {
    const data = window.localStorage.getItem('lmq.whoami')
    let whoAmI = null
    if (data) {
      try {
        whoAmI = JSON.parse(data)
      } catch (e) {
        window.localStorage.removeItem('lmq.whoami')
      }
    }
    // Do we have cached and valid info?
    if (whoAmI && whoAmI['name'] == getUsername() && (whoAmI['_ts']+3600*1000) > Date.now()) {
      return whoAmI
    }
  }
  return await window.fetch('api/whoami')
    .then(async resp => {
      if (resp.ok) {
        return await resp.json().then(data => {
          stateClasses.remove(/^user-tag-/)
          data['tags'].split(",").forEach(t => stateClasses.add(`user-tag-${t}`))
          data['_ts'] = Date.now()
          delete data['password_hash']
          delete data['hashing_algorithm']
          window.localStorage.setItem('lmq.whoami', JSON.stringify(data))
          return data
        })
      } else {
        logout()
        return null
      }
    })
}

export {
  whoAmI,
  login,
  logout,
  getUsername,
  getPassword
}
