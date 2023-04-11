function create(container, dataSource) {
  dataSource.on('update', update)
  function makeLink(toPage, label = toPage, opts = {}) {
    const div = document.createElement('div')
    div.classList.add('page-item')
    if (opts.class && opts.class.length > 0) {
      div.classList.add(...opts.class.split(' '))
    }
    let tag = 'a'
    if (opts.disabled === true) {
      div.classList.add('disabled')
      tag = 'span'
    }
    const button = document.createElement(tag)
    button.textContent  = label
    let params = dataSource.queryParams()
    params.set('page', toPage)
    div.appendChild(button)
    if (opts.disabled !== true) {
      button.href = `#${params.toString()}`
      button.onclick = e => {
        e.preventDefault()
        dataSource.page = toPage
        dataSource.reload()
        return false
      }
    }
    return div
  }
  function update() {
    let active
    let page = dataSource.page
    let pages = dataSource.pageCount
    let pageCutLow = page - 1
    let pageCutHigh = page + 1
    if (pages <= 1) {
      container.style.display = 'none'
      return
    }
    container.style.display = ''

    const links = []

    const prevLink = makeLink(page - 1, 'Previous', {class: 'previous', disabled: (page === 1)})
    links.push(prevLink)
    if (pages < 6) {
      for (let p = 1; p <= pages; p++) {
        active = page === p ? 'active' : ''
        links.push(makeLink(p, p, active))
      }
    } else {
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
      if (page > 2) {
        links.push(makeLink(1))
        if (page > 3) {
          links.push(makeLink(2, '…', {class: 'out-of-range'}))
        }
      }
      for (let p = pageCutLow; p <= pageCutHigh; p++) {
        if (p === 0) {
          p += 1
        }
        if (p > pages) {
          continue
        }
        active = page === p ? 'active' : ''
        links.push(makeLink(p, p, {class: active}))
      }
      if (page < pages - 1) {
        if (page < pages - 2) {
          links.push(makeLink(pages - 1, '…', {class: 'out-of-range'}))
        }
        links.push(makeLink(pages))
      }
    }
    const nextLink = makeLink(page + 1, 'Next', {class: 'next', disabled: (page === pages)})
    links.push(nextLink)
    while (container.firstChild) container.removeChild(container.firstChild)
    links.forEach(l => container.appendChild(l))
  }
}

export { create }
