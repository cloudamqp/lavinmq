function create(container, dataSource) {
  dataSource.on('update', update)
  function makeLink(toPage, label = toPage, extraClass = '') {
    const div = document.createElement('div')
    div.classList.add('page-item')
    if (extraClass.length > 0) {
      div.classList.add(extraClass)
    }
    const a = document.createElement('a')
    a.textContent  = label
    let params = dataSource.queryParams()
    params.set('page', toPage)
    a.href = `#${params.toString()}`
    div.appendChild(a)
    a.onclick = e => {
      e.preventDefault()
      dataSource.page = toPage
      dataSource.reload()
      return false
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

    if (page > 1) {
      links.push(makeLink(page - 1, 'Previous', 'previous'))
    }
    if (pages < 6) {
      for (let p = 1; p <= pages; p++) {
        active = page === p ? 'active' : ''
        links.push(makeLink(p, p, active))
      }
    } else {
      if (page > 2) {
        links.push(makeLink(1, 1))
        if (page > 3) {
          links.push(makeLink(page - 2, '…', 'out-of-range'))
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
        links.push(makeLink(p, p, active))
      }
      if (page < pages - 1) {
        if (page < pages - 2) {
          links.push(makeLink(page + 2, '…', 'out-of-range'))
        }
        links.push(makeLink(pages, '…', ''))
      }
    }
    if (page < pages) {
      links.push(makeLink(page + 1, 'Next', 'next'))
    }
    while (container.firstChild) container.removeChild(pagination.firstChild)
    links.forEach(l => container.appendChild(l))
  }
}

export { create }
