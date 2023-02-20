import * as HTTP from './http.js'

function fetch (cb) {
  const url = 'api/users'
  const raw = window.sessionStorage.getItem(url)
  if (raw) {
    const users = JSON.parse(raw)
    cb(users)
  }
  HTTP.request('GET', url).then(function (users) {
    try {
      window.sessionStorage.setItem('api/users', JSON.stringify(users))
    } catch (e) {
      console.error('Saving sessionStorage', e)
    }
    cb(users)
  }).catch(function (e) {
    console.error(e.message)
  })
}

export {
  fetch
}
