if (window.location.hash) {
  const params = new URLSearchParams(window.location.hash.substring(1))
  const user = params.get('username')
  const pass = params.get('password')
  window.location.hash = ''
  if (user && pass) tryLogin(user, pass)
}

const loginForm = document.getElementById('login')
if (loginForm) {
  loginForm.addEventListener('submit', (e) => {
    e.preventDefault()
    const userInput = document.getElementById('username') as HTMLInputElement | null
    const passInput = document.getElementById('password') as HTMLInputElement | null
    const user = userInput?.value ?? ''
    const pass = passInput?.value ?? ''
    tryLogin(user, pass)
  })
}

function tryLogin(user: string, pass: string): void {
  const auth = window.btoa(`${user}:${pass}`)
  document.cookie = `m=|:${encodeURIComponent(auth)}; samesite=strict; max-age=${60 * 60 * 8}`
  fetch('api/whoami').then((resp) => {
    if (resp.ok) {
      window.location.assign('.')
    } else {
      document.cookie = 'm=; max-age=0'
      window.alert('Authentication failure')
    }
  })
}
