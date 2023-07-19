function parseJSON (data) {
  try {
    if (data.length) {
      return JSON.parse(data)
    }
    return {}
  } catch (e) {
    if (e instanceof SyntaxError) { window.alert('Input must be JSON') }
    throw e
  }
}

function jsonToText (obj) {
  if (obj == null) return ''
  return JSON.stringify(obj, undefined, 2).replace(/["{},]/g, '').trim()
}

function toast (text, type = 'success') {
  // Delete all previous toasts
  document.querySelectorAll('.toast').forEach(t => t.parentNode.removeChild(t))

  const d = document.createElement('div')
  d.classList.add(type)
  d.classList.add('toast')
  d.textContent = text
  document.body.appendChild(d)
  setTimeout(() => {
    try {
      document.body.removeChild(d)
    } catch (e) {
      // noop
    }
  }, 7000)
}

export {
  jsonToText,
  parseJSON,
  toast
}
