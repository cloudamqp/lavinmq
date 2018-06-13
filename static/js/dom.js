(function () {
  window.avalanchemq = window.avalanchemq || {}

  function setChild (selectorOrEl, element) {
    let els = null
    if (selectorOrEl instanceof Node) {
      els = [selectorOrEl]
    } else if (selectorOrEl instanceof NodeList) {
      els = selectorOrEl
    } else if (selectorOrEl instanceof String) {
      els = document.querySelectorAll(selectorOrEl)
    } else {
      return
    }
    els.forEach(el => {
      while (el.lastChild) {
        el.removeChild(el.lastChild)
      }
      el.appendChild(element)
    })
  }

  Object.assign(window.avalanchemq, {
    dom: {
      setChild
    }
  })
})()
