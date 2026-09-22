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

// Wires up CSS Anchor Positioning for tooltips built from the
// `.arg-tooltip`/`.prop-tooltip` (trigger) + nested `.tooltiptext`/`.prop-tooltiptext`
// (tooltip) pattern, without requiring any per-instance markup or inline styles.
// Anchor positioning requires each trigger to have its own unique `anchor-name`,
// matched by `position-anchor` on its tooltip — this assigns both, deriving the
// name from an ever-increasing counter so every instance is guaranteed unique.
// No-ops entirely on browsers without anchor positioning support (main.css falls
// back to the historical bottom/left/transform positioning for those).
// Called once below for content already in the DOM at load; call again with a
// scoped root after rendering new tooltips dynamically (e.g. after a table
// re-render), so only the new elements are wired up.
let tooltipAnchorCounter = 0

function wireTooltipAnchors (root = document) {
  if (!window.CSS?.supports('anchor-name', '--x')) return
  root.querySelectorAll('.arg-tooltip, .prop-tooltip').forEach(el => {
    if (el.style.getPropertyValue('anchor-name')) return
    const tooltip = el.querySelector('.tooltiptext, .prop-tooltiptext')
    if (!tooltip) return
    const anchorName = `--tt-${tooltipAnchorCounter++}`
    el.style.setProperty('anchor-name', anchorName)
    tooltip.style.setProperty('position-anchor', anchorName)
  })
}

wireTooltipAnchors(document)

export {
  jsonToText,
  parseJSON,
  toast,
  button,
  wireTooltipAnchors
}
