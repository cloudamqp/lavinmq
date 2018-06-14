(function () {
  window.avalanchemq = window.avalanchemq || {}

  function setChild (selector, element) {
    const els = elements(selector)
    els.forEach(el => {
      while (el.lastChild) {
        el.removeChild(el.lastChild)
      }
      el.appendChild(element)
    })
  }

  function removeNodes (selector) {
    const els = elements(selector)
    const parent = els[0].parentNode
    els.forEach(node => {
      parent.removeChild(node)
    })
  }

  function removeChildren (selector) {
    const els = elements(selector)
    els.forEach(el => {
      while (el.lastChild) {
        el.removeChild(el.lastChild)
      }
    })
  }

  function elements (selector) {
    let els = null
    if (selector instanceof Node) {
      els = [selector]
    } else if (selector instanceof NodeList) {
      els = selector
    } else if (selector instanceof String) {
      els = document.querySelectorAll(selector)
    } else {
      els = []
    }
    return els
  }

  function jsonToText (obj) {
    return JSON.stringify(obj, undefined, 2).replace(/["{},]/g, '')
  }

  Object.assign(window.avalanchemq, {
    dom: {
      setChild,
      removeNodes,
      jsonToText,
      removeChildren
    }
  })
})()
