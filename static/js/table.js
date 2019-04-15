/* global avalanchemq */
(function () {
  window.avalanchemq = window.avalanchemq || {}

  function getQueryVariable (variable) {
    var query = window.location.search.substring(1)
    var vars = query.split('&')
    for (var i = 0; i < vars.length; i++) {
      var pair = vars[i].split('=')
      if (pair[0] === variable) { return pair[1] }
    }
    return ''
  }

  function renderTable (id, options = {}, renderRow) {
    let sortKey = getQueryVariable('sort')
    let reverseOrder = getQueryVariable('sort_reverse')
    let url = options.url
    const table = document.getElementById(id)
    const container = table.parentElement
    const keyColumns = options.keyColumns
    const interval = options.interval
    let timer = null
    let searchTerm = getQueryVariable('filter')
    let currentPage = getQueryVariable('page') || 1
    let pageSize = getQueryVariable('page_size') || 100

    makeHeadersSortable()
    if (options.columnSelector) {
      renderColumnSelector(table)
    }

    if (options.search) {
      renderSearch(table)
    }

    if (options.pagination) {
      let footer = `<tfoot><tr>
                      <td colspan="999"><div id="pagination"></div></td>
                    </tr></tfoot>`
      table.insertAdjacentHTML('beforeend', footer)
    }

    if (url) {
      const raw = window.sessionStorage.getItem(url)
      if (raw) {
        let data = JSON.parse(raw)
        updateTable(data.items || data)
      }
      fetchAndUpdate()
      if (interval) {
        timer = setInterval(fetchAndUpdate, interval)
      }
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
            if (th.isEqualNode(e.target)) return
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
            reverseOrder = false
            e.target.classList.add('sorting_asc')
            e.target.classList.remove('sorting_desc')
          }
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

    function buildQuery (page) {
      let q = `page=${page}&page_size=${pageSize}`
      if (searchTerm) {
        q += `&name=${searchTerm}&use_regex=true`
      }
      if (sortKey !== '') {
        q += `&sort=${sortKey}&sort_reverse=${reverseOrder}`
      }
      return q
    }

    function fetchAndUpdate () {
      const fullUrl = `${url}?${buildQuery(currentPage)}`
      const tableError = document.getElementById(id + '-error')
      return avalanchemq.http.request('GET', fullUrl).then(function (response) {
        tableError.textContent = ''
        try {
          window.sessionStorage.setItem(fullUrl, JSON.stringify(response))
        } catch (e) {
          console.error('Saving sessionStorage', e)
        }
        updateTable(response)
      }).catch(function (e) {
        if (e.body) {
          tableError.textContent = 'Error fetching data: ' + e.body
        } else {
          console.error(e)
        }
        if (timer) {
          clearInterval(timer)
        }
      })
    }

    function updateTable (response) {
      if (response == null && response.items == null) return
      let data = response.items || response
      let totalCount = response.filtered_count || response.length
      document.getElementById(id + '-count').textContent = totalCount
      if (options.pagination && response.items) {
        pageSize = response.page_size
        let pages = Math.ceil(response.filtered_count / pageSize)
        createPagination(pages, response.page)
      }
      const t = document.getElementById(id).tBodies[0]
      if (!Array.isArray(data) || data.length === 0) {
        t.innerHTML = '<tr><td colspan="100" class="center">Nope, nothing to see here.</td></tr>'
        return
      }

      let start = 0
      for (let i = 0; i < data.length; i++) {
        const item = data[i]
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
            renderRow(t.children[i], item, false)
          }
          start = Math.min(i + 1, foundIndex)
        } else {
          const tr = t.insertRow(i)
          setKeyAttributes(tr, item)
          renderRow(tr, item, true)
          start = i + 1
        }
      }

      let rowsToDelete = t.rows.length - data.length
      while (rowsToDelete-- > 0) {
        t.deleteRow(t.rows.length - 1)
      }
    }

    function findIndex (rows, start, item) {
      for (let i = start; i < rows.length; i++) {
        for (let k = 0; k < keyColumns.length; k++) {
          if (rows[i].getAttribute('data-' + keyColumns[k]) !== item[keyColumns[k]]) {
            break
          } else if (k === keyColumns.length - 1) {
            return i
          }
        }
      }
      return -1
    }

    function setKeyAttributes (tr, item) {
      keyColumns.forEach(function (key) {
        tr.setAttribute('data-' + key, item[key])
      })
    }

    function renderSearch () {
      const debouncedUpdate = debounce(() => {
        updateQueryState({ filter: searchTerm })
        fetchAndUpdate()
      })
      let str = `<form class="form" action="javascript:void(0);">
            <input class="filter-table" placeholder="Filter regex" value="${searchTerm}">
          </form>`
      container.insertAdjacentHTML('afterbegin', str)
      container.addEventListener('keyup', e => {
        if (!e.target.classList.contains('filter-table')) return true
        searchTerm = encodeURIComponent(e.target.value)
        debouncedUpdate()
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
        str += `<div class="page-item previous"><a href="?${buildQuery(page - 1)}">Previous</a></div>`
      }
      if (pages < 6) {
        for (let p = 1; p <= pages; p++) {
          active = page === p ? 'active' : ''
          str += `<div class="page-item ${active}"><a href="?${buildQuery(p)}">${p}</a></div>`
        }
      } else {
        if (page > 2) {
          str += `<div class="page-item"><a href="?${buildQuery(1)}">1</a></div>`
          if (page > 3) {
            str += `<div class="page-item out-of-range"><a href="?${buildQuery(page - 2)}">...</a></div>`
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
          str += `<div class="page-item ${active}"><a href="?${buildQuery(p)}">${p}</a></div>`
        }
        if (page < pages - 1) {
          if (page < pages - 2) {
            str += `<div class="page-item out-of-range"><a href="?${buildQuery(page + 2)}">...</a></div>`
          }
          str += `<div class="page-item"><a href="?${buildQuery(pages)}">${pages}</a></div>`
        }
      }
      if (page < pages) {
        str += `<div class="page-item next"><a href="?${buildQuery(page + 1)}">Next</a></div>`
      }
      document.getElementById('pagination').innerHTML = str
      return str
    }

    return { updateTable, fetchAndUpdate }
  }

  function renderCell (tr, column, value, classList = '') {
    const cell = tr.cells[column] || buildCells(tr, column)
    if (cell.classList.contains('hide')) return
    cell.classList = classList
    if (value instanceof window.Element) {
      if (!value.isEqualNode(cell.firstChild)) {
        while (cell.lastChild) {
          cell.removeChild(cell.lastChild)
        }
        cell.appendChild(value)
      }
    } else {
      const text = value == null ? '' : value.toString()
      if (cell.textContent !== text) {
        cell.textContent = text
      }
    }
    return cell
  }

  function renderHtmlCell (tr, column, innerHTML) {
    const cell = tr.cells[column] || buildCells(tr, column)
    if (cell.innerHTML !== innerHTML) {
      cell.innerHTML = innerHTML
    }
    return cell
  }

  function buildCells (tr, index) {
    const target = index + 1
    while (index >= 0) {
      if (tr.cells.length >= target) break
      tr.insertCell(-1)
      index--
    }
    return tr.cells[tr.cells.length - 1]
  }

  function toggleCol (table, colIndex) {
    let allCol = table.querySelectorAll(`tr > *:nth-child(${colIndex + 1})`)
    for (let i = 0; i < allCol.length; i++) {
      allCol[i].classList.toggle('hide')
    }
  }

  function renderColumnSelector (table) {
    const container = table.parentElement
    container.insertAdjacentHTML('afterbegin', '<a class="col-toggle" id="col-toggle">+/-</a>')

    function close () {
      container.parentElement.querySelectorAll('.tooltip').forEach(el => {
        container.parentElement.removeChild(el)
      })
    }

    container.addEventListener('click', e => {
      if (!e.target.classList.contains('col-toggle')) return true
      let tooltip = container.parentElement.querySelector('.tooltip')
      if (tooltip) return close()
      let str = '<form class="form tooltip"><a class="close">&times;</a>'
      let allCol = table.getElementsByTagName('th')
      for (let i = 0; i < allCol.length; i++) {
        let col = allCol[i]
        let checked = !col.classList.contains('hide') ? 'checked' : ''
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
        let i = parseInt(e.target.dataset.index)
        toggleCol(table, i)
      })
      document.addEventListener('keyup', e => {
        if (e.key === 'Escape') close()
      })
    })
  }

  function debounce (func, wait, immediate) {
    let timeout
    return function () {
      let context = this
      let args = arguments
      function later () {
        timeout = null
        if (!immediate) func.apply(context, args)
      }
      let callNow = immediate && !timeout
      clearTimeout(timeout)
      timeout = setTimeout(later, wait || 200)
      if (callNow) func.apply(context, args)
    }
  }

  function updateQueryState (params) {
    let searchParams = new URLSearchParams(window.location.search)
    Object.keys(params).forEach(k => {
      searchParams.set(k, params[k])
    })
    let newurl = window.location.pathname + '?' + searchParams.toString()
    window.history.replaceState(null, '', newurl)
  }

  Object.assign(window.avalanchemq, {
    table: { renderCell, renderTable, renderHtmlCell }
  })
})()
