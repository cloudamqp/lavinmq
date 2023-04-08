import * as HTTP from './http.js'
import * as Auth from './auth.js'
document.getElementById('login').addEventListener('submit', (e) => {
  e.preventDefault()
  const user = document.getElementById('username').value
  const pass = document.getElementById('password').value
  Auth.setAuth(`${user}:${pass}`)
  HTTP.request('GET', 'api/whoami').then(() => {
    Auth.storeCookie({ username: user})
    window.location.assign('.')
  }).catch(() => {
    window.alert('Login failed')
  })
})
