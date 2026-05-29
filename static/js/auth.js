import { stateClasses } from './helpers.js'

const oauthAuthPrefix = '|oauth:'

function getUsername () {
  if (!getCookie('m')) return
  return window.atob(getAuth()).split(':')[0]
}

function getPassword () {
  if (!getCookie('m')) return null
  if (isOAuthSession()) return null
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

function isOAuthSession () {
  return getCookie('m')?.startsWith(oauthAuthPrefix) || false
}

async function login (username, password) {
  const auth = window.btoa(`${username}:${password}`)
  document.cookie = `m=|:${encodeURIComponent(auth)}; samesite=strict; max-age=${60 * 60 * 8}`
  return whoAmI(true).catch(e => {
    document.cookie = 'm=; max-age=0'
    throw e
  })
}

const whoAmICacheKey = 'lmq.whoami'

function logout () {
  window.localStorage.removeItem(whoAmICacheKey)
  stateClasses.remove(/^user-is-/)
  const oauthSession = isOAuthSession()
  document.cookie = 'm=; max-age=0'
  // oauth_token is HttpOnly and can't be cleared from JS, so OAuth sessions
  // redirect to the server endpoint which clears oauth_token.
  if (oauthSession) {
    window.location.assign('oauth/logout')
  } else {
    window.location.assign('login')
  }
}

async function fetchWhoAmI () {
  return window.fetch('api/whoami')
    .then(async resp => {
      if (resp.ok) {
        let data
        try {
          data = await resp.json()
        } catch {
          window.localStorage.removeItem(whoAmICacheKey)
          throw new Error('Invalid JSON response from whoami')
        }
        data._ts = Date.now()
        delete data.password_hash
        delete data.hashing_algorithm
        window.localStorage.setItem(whoAmICacheKey, JSON.stringify(data))
        stateClasses.remove(/^user-is-/);
        (data.tags?.split(/,\s*/).filter(Boolean) || []).forEach(tag => {
          stateClasses.add(`user-is-${tag}`)
        })
        return data
      } else {
        window.localStorage.removeItem(whoAmICacheKey)
        throw new Error(`whoami failed with status ${resp.status}`)
      }
    })
}

async function whoAmI (forceReload = false) {
  if (!forceReload) {
    const data = window.localStorage.getItem(whoAmICacheKey)
    if (data) {
      let cached
      try {
        cached = JSON.parse(data)
      } catch {
        window.localStorage.removeItem(whoAmICacheKey)
      }
      if (cached && cached.name === getUsername()) {
        const expired = (cached._ts + 3600 * 1000) <= Date.now()
        if (expired) {
          // fetch in background, we still return the cached
          fetchWhoAmI().catch(e => console.warn(`Failed to fetch whoAmI: ${e.message}`))
        }
        return cached
      }
    }
  }
  return fetchWhoAmI()
}

export {
  whoAmI,
  login,
  logout,
  getUsername,
  getPassword
}
