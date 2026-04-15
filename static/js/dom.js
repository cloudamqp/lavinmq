function parseJSON (data) {
  try {
    data = data.trim()
    if (data.length) {
      return JSON.parse(data)
    }
    return {}
  } catch (e) {
    if (e instanceof SyntaxError) { toast.error('Input must be JSON') }
    throw e
  }
}

function jsonToText (obj) {
  if (obj == null) return ''
  return JSON.stringify(obj, undefined, 2).replace(/["{},]/g, '').trim()
}

function toastImpl (text, type = 'success') {
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

function toast (text) {
  toastImpl(text, 'success')
}

Object.assign(toast, {
  success: function toastSuccess (text) { toastImpl(text, 'success') },
  warn: function toastWarn (text) { toastImpl(text, 'warn') },
  error: function toastError (text) { toastImpl(text, 'error') }
})

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
    return createButton(type, text, ['btn-small', 'btn-small-outlined-danger'], click)
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

/**
 * Show a "not found" message when an entity has been deleted.
 * Replaces the main page content with a friendly message and a link back to the list page.
 * @param {string} entityType - The type of entity, e.g. "Exchange", "Queue", "Connection"
 * @param {string} entityName - The name of the entity
 * @param {string} listUrl - The URL to the list page, e.g. "exchanges", "queues"
 */
function showEntityNotFound (entityType, entityName, listUrl) {
  const main = document.querySelector('main')
  if (!main) return
  main.textContent = ''
  main.classList.add('main-grid')

  const section = document.createElement('section')
  section.classList.add('card', 'cols-12')

  const heading = document.createElement('h2')
  heading.textContent = `${entityType} not found`
  section.appendChild(heading)

  const message = document.createElement('p')
  message.textContent = `The ${entityType.toLowerCase()} "${entityName}" no longer exists. It may have been deleted.`
  message.style.margin = '1em 0'
  section.appendChild(message)

  const link = document.createElement('a')
  link.href = listUrl
  link.textContent = `Back to ${listUrl}`
  link.classList.add('btn', 'btn-outlined')
  section.appendChild(link)

  main.appendChild(section)
}

export {
  jsonToText,
  parseJSON,
  toast,
  button,
  showEntityNotFound
}
