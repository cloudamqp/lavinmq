import * as HTTP from './http.js'

function renderTable (id, options = {}, renderRow) {
  const search = new URLSearchParams(window.location.hash.substring(1))
  let sortKey = search.get('sort')
  let reverseOrder = strToBool(search.get('reverseOrder'))
  const view = window.location.pathname.split('/', 2)[0]
  if (!sortKey || reverseOrder === null) {
    sortKey = window.sessionStorage.getItem(view + '-sortkey')
    reverseOrder = strToBool(window.sessionStorage.getItem(view + '-reverseorder'))
  }
  const countId = options.countId || 'pagename-label'
  const url = options.url
  const table = document.getElementById(id)
  const container = table.parentElement
  const keyColumns = options.keyColumns
  const interval = options.interval
  let searchTerm = null
  const currentPage = search.get('page') || 1
  let pageSize = search.get('page_size') || 100

  if (options.columnSelector) {
    renderColumnSelector(table)
  }

  if (options.search) {
    searchTerm = search.get('name')
    renderSearch(table)
  }

  if (options.pagination) {
    const footer = `<tfoot><tr>
                      <td colspan="999"><div id="pagination"></div></td>
                    </tr></tfoot>`
    table.insertAdjacentHTML('beforeend', footer)
  }

  if (url) {
    if (table.querySelector('th[data-sort-key]')) {
      makeHeadersSortable()
    }
    const raw = window.sessionStorage.getItem(url)
    if (raw) {
      try {
        const data = JSON.parse(raw)
        if (data) {
          updateTable(data.items || data)
        }
      } catch (e) {
        window.sessionStorage.removeItem(url)
        console.log('Error parsing data from sessionStorage')
        console.error(e)
        // TODO: show some kind of "offline" notification
      }
    }
    fetchAndUpdate()
    if (interval) {
      setInterval(fetchAndUpdate, interval)
    }
  }

  function strToBool(str){
    return str === 'true' ? true : false
  }

  function makeHeadersSortable () {
    const sortHeader = table.querySelector(`th[data-sort-key="${sortKey}"]`)
    if (sortHeader) {
      const sortClass = reverseOrder ? 'sorting_desc' : 'sorting_asc'
      sortHeader.classList.add(sortClass)
    }
    table.querySelectorAll('th[data-sort-key]').forEach(function (cell) {
      cell.addEventListener('click', function (e) {
        table.querySelectorAll('th[data-sort-key]').forEach(th => {
          if (th === e.target) return
          th.classList.remove('sorting_desc')
          th.classList.remove('sorting_asc')
        })
        const newSortKey = e.target.getAttribute('data-sort-key')
        if (newSortKey === sortKey) {
          reverseOrder = !reverseOrder
          e.target.classList.toggle('sorting_asc')
          e.target.classList.toggle('sorting_desc')
        } else {
          sortKey = newSortKey
          reverseOrder = true
          e.target.classList.add('sorting_desc')
          e.target.classList.remove('sorting_asc')
        }
        window.sessionStorage.setItem(view + '-sortkey', sortKey)
        window.sessionStorage.setItem(view + '-reverseorder', reverseOrder)
        updateQueryState({ sort: sortKey, reverseOrder })
        const t = table.tBodies[0]
        clearRows(t)
        fetchAndUpdate()
      })
    })
  }

  function clearRows (t) {
    while (t.rows.length) {
      t.deleteRow(-1)
    }
  }

  function buildQuery (page, clean = false) {
    let q = `page=${page}&page_size=${pageSize}`
    if (clean) return q
    if (searchTerm) {
      q += `&name=${searchTerm}&use_regex=true`
    }
    if (sortKey > '') {
      q += `&sort=${sortKey}&sort_reverse=${reverseOrder}`
    }
    return q
  }

  function getData () {
    const key = `${url}#${buildQuery(currentPage, true)}`
    const json = window.sessionStorage.getItem(key)
    return JSON.parse(json)
  }

  function fetchAndUpdate () {
    const fullUrl = `${url}#${buildQuery(currentPage)}`
    return HTTP.request('GET', fullUrl).then(function (response) {
      toggleDisplayError(id, false)
      try {
        window.sessionStorage.setItem(fullUrl, JSON.stringify(response))
      } catch (e) {
        console.error('Saving sessionStorage', e)
      }
      updateTable(response)
    }).catch((e) => {
      if (e.body) {
        toggleDisplayError(id, e.body)
      } else {
        toggleDisplayError(id, "Can't reach server, please try to refresh the page.")
        console.error(e)
      }
    })
  }

  function updateTable (response) {
    if (response == null && response.items == null) return
    const data = response.items || response
    const totalCount = response.filtered_count || response.length
    document.getElementById(countId).textContent = totalCount
    if (options.pagination && response.items) {
      pageSize = response.page_size
      const pages = Math.ceil(response.filtered_count / pageSize)
      createPagination(pages, response.page)
    }
    const t = document.getElementById(id).tBodies[0]
    if (!Array.isArray(data) || data.length === 0) {
      t.textContent = ""
      const tr = t.appendChild(document.createElement("tr"))
      const td = tr.appendChild(document.createElement("td"))
      td.colSpan = 100
      td.classList.add("center")
      td.textContent = "Nope, nothing to see here."
      return
    }

    let start = 0
    toggleDisplayError(id, false)
    for (let i = 0; i < data.length; i++) {
      const item = data[i]
      try {
        const foundIndex = findIndex(t.rows, start, item)
        if (foundIndex !== -1) {
          if (foundIndex !== i) {
            renderRow(t.rows[i], item, true)
            setKeyAttributes(t.rows[i], item)
            if (data[foundIndex]) {
              renderRow(t.rows[foundIndex], data[foundIndex], true)
              setKeyAttributes(t.rows[foundIndex], data[foundIndex])
            }
          } else {
            renderRow(t.children[i], item, true)
          }
          start = Math.min(i + 1, foundIndex)
        } else {
          const tr = t.insertRow(i)
          setKeyAttributes(tr, item)
          renderRow(tr, item, true)
          start = i + 1
        }
      } catch (e) {
        toggleDisplayError(id, item.error || e.message)
      }
    }

    let rowsToDelete = t.rows.length - data.length
    while (rowsToDelete-- > 0) {
      t.deleteRow(t.rows.length - 1)
    }
  }

  function findIndex (rows, start, item) {
    for (let i = start; i < rows.length; i++) {
      if (keyColumns.every(key => rows[i].dataset[key] === item[key])) {
        return i;
      }
    }
    return -1
  }

  function setKeyAttributes (tr, item) {
    keyColumns.forEach(function (key) {
      tr.dataset[key] = item[key]
    })
  }

  function renderSearch () {
    const form = document.createElement("form")
    form.classList.add("form")
    form.addEventListener("submit", (e) => { e.preventDefault() })
    const filterInput = document.createElement("input")
    filterInput.classList.add("filter-table")
    filterInput.placeholder = "Filter regex"
    filterInput.value = searchTerm
    form.appendChild(filterInput)
    container.insertBefore(form, container.children[0])
    container.addEventListener('keyup', e => {
      if (!e.target.classList.contains('filter-table')) return true
      searchTerm = encodeURIComponent(e.target.value)
      debounce(() => {
        updateQueryState({ name: searchTerm })
        fetchAndUpdate()
      })
    })
  }

  function createPagination (pages, page) {
    let str = ''
    let active
    let pageCutLow = page - 1
    let pageCutHigh = page + 1
    if (pages === 1) {
      document.getElementById('pagination').innerHTML = ''
      return
    }

    if (page > 1) {
      str += `<div class="page-item previous"><a href="#${buildQuery(page - 1)}">Previous</a></div>`
    }
    if (pages < 6) {
      for (let p = 1; p <= pages; p++) {
        active = page === p ? 'active' : ''
        str += `<div class="page-item ${active}"><a href="#${buildQuery(p)}">${p}</a></div>`
      }
    } else {
      if (page > 2) {
        str += `<div class="page-item"><a href="#${buildQuery(1)}">1</a></div>`
        if (page > 3) {
          str += `<div class="page-item out-of-range"><a href="#${buildQuery(page - 2)}">...</a></div>`
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
        str += `<div class="page-item ${active}"><a href="#${buildQuery(p)}">${p}</a></div>`
      }
      if (page < pages - 1) {
        if (page < pages - 2) {
          str += `<div class="page-item out-of-range"><a href="#${buildQuery(page + 2)}">...</a></div>`
        }
        str += `<div class="page-item"><a href="#${buildQuery(pages)}">${pages}</a></div>`
      }
    }
    if (page < pages) {
      str += `<div class="page-item next"><a href="#${buildQuery(page + 1)}">Next</a></div>`
    }
    document.getElementById('pagination').innerHTML = str
    return str
  }

  return { updateTable, fetchAndUpdate, getData }
}

function renderCell (tr, column, value, classList = '') {
  const cell = tr.cells[column] || buildCells(tr, column)
  if (value instanceof window.Element) {
    if (cell.firstChild) {
      cell.replaceChild(value, cell.firstChild)
    } else {
      cell.appendChild(value)
    }
  } else {
    const text = value == null ? '' : value.toString()
    if (cell.textContent !== text) {
      cell.textContent = text
    }
  }
  if (cell.classList.contains('hide')) return
  cell.classList = classList
  return cell
}

function buildCells (tr, index) {
  const target = index + 1
  while (index >= 0) {
    if (tr.cells.length >= target) break
    tr.insertCell(-1)
    index--
  }
  let tbl = tr.parentElement.parentElement
  let colHeader = tbl.querySelectorAll(`tr > *:nth-child(${target})`)[0]
  if (colHeader.classList.contains("hide")) {
    tr.cells[tr.cells.length - 1].classList.add('hide')
  }
  return tr.cells[tr.cells.length - 1]
}

function columnSelectorCacheKey (table) {
  return `hiddenTableCols${window.location.pathname}${table.id}`
}

function setHiddenColumns (table, state) {
  const cacheKey = columnSelectorCacheKey(table)
  window.sessionStorage.setItem(cacheKey, JSON.stringify(Array.from(state)))
}

function getHiddenColumns (table) {
  const cacheKey = columnSelectorCacheKey(table)
  return new Set(JSON.parse(window.sessionStorage.getItem(cacheKey) || '[]'))
}

function toggleCol (table, colIndex) {
  const allCol = table.querySelectorAll(`tr > *:nth-child(${colIndex + 1})`)
  for (let i = 0; i < allCol.length; i++) {
    allCol[i].classList.toggle('hide')
  }
}

function renderColumnSelector (table) {
  const container = table.parentElement
  container.insertAdjacentHTML('afterbegin', '<a class="col-toggle" id="col-toggle">+/-</a>')

  const hiddenColumns = getHiddenColumns(table)
  hiddenColumns.forEach(i => {
    toggleCol(table, i)
  })

  function close () {
    container.parentElement.querySelectorAll('.tooltip').forEach(el => {
      container.parentElement.removeChild(el)
    })
  }

  container.addEventListener('click', e => {
    if (!e.target.classList.contains('col-toggle')) return true
    const tooltip = container.parentElement.querySelector('.tooltip')
    if (tooltip) return close()
    let str = '<form class="form tooltip"><a class="close">&times;</a>'
    const allCol = table.getElementsByTagName('th')
    for (let i = 0; i < allCol.length; i++) {
      const col = allCol[i]
      if (col.innerHTML.length === 0) {
        continue
      }
      const checked = !col.classList.contains('hide') ? 'checked' : ''
      str += `<label>
                  <span>${col.innerHTML}</span>
                  <input type="checkbox" class="col-toggle-checkbox" ${checked} data-index=${i}>
                </label>`
    }
    str += '</form>'
    container.parentElement.insertAdjacentHTML('beforeend', str)
    container.parentElement.addEventListener('click', e => {
      if (e.target.classList.contains('close')) close()
    })
    container.parentElement.addEventListener('change', e => {
      if (!e.target.classList.contains('col-toggle-checkbox')) return true
      const i = parseInt(e.target.dataset.index)
      toggleCol(table, i)
      const hiddenColumns = getHiddenColumns(table)
      if (hiddenColumns.has(i)) {
        hiddenColumns.delete(i)
      } else {
        hiddenColumns.add(i)
      }
      setHiddenColumns(table, hiddenColumns)
    })
    document.addEventListener('keyup', e => {
      if (e.key === 'Escape') close()
    })
  })
}

function debounce (func, wait, immediate) {
  let timeout
  return function () {
    const context = this
    const args = arguments
    function later () {
      timeout = null
      if (!immediate) func.apply(context, args)
    }
    const callNow = immediate && !timeout
    clearTimeout(timeout)
    timeout = setTimeout(later, wait || 200)
    if (callNow) func.apply(context, args)
  }
}

function updateQueryState (params) {
  const searchParams = new URLSearchParams(window.location.hash.substring(1))
  Object.keys(params).forEach(k => {
    searchParams.set(k, params[k])
  })
  const newurl = `${window.location.pathname}#${searchParams.toString()}`
  window.history.replaceState(null, '', newurl)
}

function toggleDisplayError (tableID, message = null) {
  const tableError = document.getElementById(`${tableID}-error`)
  if (message) {
    tableError.style.display = 'block'
    tableError.textContent = 'Something went wrong: ' + message
  } else {
    tableError.style.display = 'none'
    tableError.textContent = ''
  }
}

export { renderCell, renderTable, toggleDisplayError }
