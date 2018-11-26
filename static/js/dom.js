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
    if (!els[0]) return
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
        window.alert('Input must be JSON')
      } else {
        throw e
      }
    }
  }

  function elements (selector) {
    let els = null
    if (selector instanceof document.Node) {
      els = [selector]
    } else if (selector instanceof document.NodeList) {
      els = selector
    } else if (typeof selector === 'string') {
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
    removeNodes('.toast')
    let d = document.createElement('div')
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

  function createPagination (pages, page) {
    let str = ''
    let active
    let pageCutLow = page - 1
    let pageCutHigh = page + 1
    if (pages === 1) return

    if (page > 1) {
      str += `<div class="page-item previous"><a href="?page_size=${pages}&page=${page - 1}">Previous</a></div>`
    }
    if (pages < 6) {
      for (let p = 1; p <= pages; p++) {
        active = page === p ? 'active' : ''
        str += `<div class="page-item ${active}"><a href="?page_size=${pages}&page=${p}">${p}</a></div>`
      }
    } else {
      if (page > 2) {
        str += `<div class="page-item"><a href="?page_size=${pages}&page=1">1</a></div>`
        if (page > 3) {
          str += `<div class="page-item out-of-range"><a href="?page_size=${pages}&page=${page - 2}">...</a></div>`
        }
      }
      if (page === 1) {
        pageCutHigh += 2
      } else if (page === 2) {
        pageCutHigh += 1
      }
      if (page === pages) {
        pageCutLow -= 2
      } else if (page === pages - 1) {
        pageCutLow -= 1
      }
      for (let p = pageCutLow; p <= pageCutHigh; p++) {
        if (p === 0) {
          p += 1
        }
        if (p > pages) {
          continue
        }
        active = page === p ? 'active' : ''
        str += `<div class="page-item ${active}"><a href="?page_size=${pages}&page=${p}">${p}</a></div>`
      }
      if (page < pages - 1) {
        if (page < pages - 2) {
          str += `<div class="page-item out-of-range"><a href="?page_size=${pages}&page=${page + 2}">...</a></div>`
        }
        str += `<div class="page-item"><a href="?page_size=${pages}&page=${pages}">${pages}</a></div>`
      }
    }
    if (page < pages) {
      str += `<div class="page-item next"><a href="?page_size=${pages}&page=${page + 1}">Next</a></div>`
    }
    document.getElementById('pagination').innerHTML = str
    return str
  }

  Object.assign(window.avalanchemq, {
    dom: {
      setChild,
      removeNodes,
      jsonToText,
      removeChildren,
      parseJSON,
      toast,
      createPagination
    }
  })
})()
