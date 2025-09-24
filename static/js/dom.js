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

function createButton (type, text, classes, click) {
  const btn = document.createElement('button')
  btn.type = type
  btn.textContent = text
  btn.classList.add(...classes)
  if (click) {
    btn.addEventListener('click', click)
  }
  return btn
}

const button = {
  delete: ({ click, text = 'Delete', type = 'button' }) => {
    return createButton(type, text, ['btn-small-outlined-danger'], click)
  },
  edit: ({ click, text = 'Edit', type = 'button' }) => {
    return createButton(type, text, ['btn-small'], click)
  },
  submit: ({ text = 'Save' } = {}) => {
    return createButton('submit', text, ['btn-icon', 'btn-submit'])
  },
  reset: ({ text = 'Reset' } = {}) => {
    return createButton('reset', text, ['btn-icon', 'btn-reset'])
  }
}

export {
  jsonToText,
  parseJSON,
  toast,
  button
}
