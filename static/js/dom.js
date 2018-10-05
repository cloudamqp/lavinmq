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
    if (!els[0]) return;
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

  function parseJSON (data) {
    try {
      if (data.length) {
        return JSON.parse(data)
      }
      return {}
    } catch (e) {
      if (e instanceof SyntaxError) {
        alert('Input must be JSON')
      } else {
        throw e
      }
    }
  }

  function elements (selector) {
    let els = null
    if (selector instanceof Node) {
      els = [selector]
    } else if (selector instanceof NodeList) {
      els = selector
    } else if (typeof selector === "string") {
      els = document.querySelectorAll(selector)
    } else {
      els = []
    }
    return els
  }

  function jsonToText (obj) {
    if (obj == null) return ''
    return JSON.stringify(obj, undefined, 2).replace(/["{},]/g, '')
  }

  function toast (text) {
    removeNodes(".toast")
    let d = document.createElement("div")
    d.classList.add("toast")
    d.textContent = text
    document.body.appendChild(d)
    setTimeout(() => {
      try {
        document.body.removeChild(d)
      } catch(e) {
        // noop
      }
    }, 7000)
  }

  Object.assign(window.avalanchemq, {
    dom: {
      setChild,
      removeNodes,
      jsonToText,
      removeChildren,
      parseJSON,
      toast
    }
  })
})()
