import * as Auth from './auth.js'
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

async function tryLogin (user, pass) {
  const res = await Auth.login(user, pass)
  if (res) {
    window.location.assign('.')
  } else {
    window.alert('Authentication failure')
  }
}
