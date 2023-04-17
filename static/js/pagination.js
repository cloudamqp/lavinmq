class Pagination {
  #container = null
  #dataSource = null
  constructor(container, dataSource) {
    this.#container = container
    this.#dataSource = dataSource
    this.#dataSource.on('update', this.update.bind(this))
  }
  #makeLink(toPage, label = toPage, opts = {}) {
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
    let params = this.#dataSource.queryParams()
    params.set('page', toPage)
    div.appendChild(button)
    if (opts.disabled !== true) {
      button.href = `#${params.toString()}`
      button.onclick = e => {
        e.preventDefault()
        this.#dataSource.page = toPage
        this.#dataSource.reload()
        return false
      }
    }
    return div
  }
  update() {
    let active
    let page = this.#dataSource.page
    let pages = this.#dataSource.pageCount
    let pageCutLow = page - 1
    let pageCutHigh = page + 1
    if (pages <= 1) {
      this.#container.style.display = 'none'
      return
    }
    this.#container.style.display = ''

    const links = []

    const prevLink = this.#makeLink(page - 1, 'Previous', {class: 'previous', disabled: (page === 1)})
    links.push(prevLink)
    if (pages < 6) {
      for (let p = 1; p <= pages; p++) {
        active = page === p ? 'active' : ''
        links.push(this.#makeLink(p, p, active))
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
        links.push(this.#makeLink(1))
        if (page > 3) {
          links.push(this.#makeLink(2, '…', {class: 'out-of-range'}))
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
        links.push(this.#makeLink(p, p, {class: active}))
      }
      if (page < pages - 1) {
        if (page < pages - 2) {
          links.push(this.#makeLink(pages - 1, '…', {class: 'out-of-range'}))
        }
        links.push(this.#makeLink(pages))
      }
    }
    const nextLink = this.#makeLink(page + 1, 'Next', {class: 'next', disabled: (page === pages)})
    links.push(nextLink)
    while (this.#container.firstChild) this.#container.removeChild(this.#container.firstChild)
    links.forEach(l => this.#container.appendChild(l))
  }
}

function create() {
  return new Pagination(...arguments)
}

export { create }
