export function getUsername(): string | undefined {
  const auth = getAuth()
  if (!auth) return undefined
  return window.atob(auth).split(':')[0]
}

export function getPassword(): string | undefined {
  const auth = getAuth()
  if (!auth) return undefined
  return window.atob(auth).split(':')[1]
}

function getAuth(): string | undefined {
  const m = getCookie('m')
  if (!m) return undefined
  const idx = m.lastIndexOf(':')
  return decodeURIComponent(m.substring(idx + 1))
}

function getCookie(key: string): string | undefined {
  return document.cookie
    .split('; ')
    .find((c) => c.startsWith(`${key}=`))
    ?.split('=')[1]
}
