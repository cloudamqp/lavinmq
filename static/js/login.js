window.fetch('oauth/enabled')
  .then(resp => resp.json())
  .then(data => {
    if (data.enabled) {
      const ssoBtn = document.getElementById('sso-btn')
      ssoBtn.style.setProperty('display', 'flex', 'important')
      document.getElementById('sso-separator').style.display = ''
      ssoBtn.addEventListener('click', () => {
        window.fetch('/oauth/authorize')
          .then(resp => resp.json().then(data => {
            if (resp.ok) {
              window.location.assign(data.authorize_url)
            } else {
              window.alert(data.reason || 'SSO authorization failed')
            }
          }))
          .catch(() => { window.alert('SSO authorization failed') })
      })
    }
  })
  .catch(() => {})

const searchParams = new URLSearchParams(window.location.search)
if (searchParams.has('error')) {
  window.alert(searchParams.get('error'))
  history.replaceState(null, '', window.location.pathname)
}

if (window.location.hash) {
  const params = new URLSearchParams(window.location.hash.substring(1))
  const user = params.get('username')
  const pass = params.get('password')
  window.location.hash = ''
  if (user && pass) tryLogin(user, pass)
}

document.getElementById('login').addEventListener('submit', (e) => {
  e.preventDefault()
  const user = document.getElementById('username').value
  const pass = document.getElementById('password').value
  tryLogin(user, pass)
})

function tryLogin (user, pass) {
  const auth = window.btoa(`${user}:${pass}`)
  document.cookie = `m=|:${encodeURIComponent(auth)}; samesite=strict; max-age=${60 * 60 * 8}`
  window.fetch('api/whoami')
    .then(resp => {
      if (resp.ok) {
        window.location.assign('.')
      } else {
        document.cookie = 'm=; max-age=0'
        window.alert('Authentication failure')
      }
    })
}
