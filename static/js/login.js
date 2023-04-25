if (window.location.hash) {
  const params = new URLSearchParams(window.location.hash.substring(1))
  const user = params.get("username")
  const pass = params.get("password")
  window.location.hash = ""
  if (user && pass) tryLogin(user, pass)
}

document.getElementById('login').addEventListener('submit', (e) => {
  e.preventDefault()
  const user = document.getElementById('username').value
  const pass = document.getElementById('password').value
  tryLogin(user, pass)
})

function tryLogin(user, pass) {
  const auth = window.btoa(`${user}:${pass}`)
  document.cookie = `m=|:${encodeURIComponent(auth)}; samesite=strict; max-age=${60 * 60 * 8}`
  window.fetch("api/whoami")
    .then(resp => {
      if (resp.ok) {
        window.location.assign(".")
      } else {
        document.cookie = `m=; max-age=0`
        alert("Authentication failure")
      }
    })
}
