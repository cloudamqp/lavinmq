import * as Pagination from './pagination.js'
import * as TableHeaderSort from './tableheadersort.js'
import { UrlDataSource } from './datasource.js'

function renderTable (id, options = {}, renderRow) {
  const countId = options.countId || 'pagename-label'
  const url = options.url
  const dataSource = new UrlDataSource(options.url)
  const table = document.getElementById(id)
  const container = table.parentElement
  const keyColumns = options.keyColumns

  if (options.columnSelector) {
    renderColumnSelector(table)
  }

  if (options.search) {
    renderSearch(container, dataSource)
  }

  if (options.pagination) {
    const footer = `<tfoot><tr>
                      <td colspan="999"><div id="pagination"></div></td>
                    </tr></tfoot>`
    table.insertAdjacentHTML('beforeend', footer)
    Pagination.create(document.getElementById('pagination'), dataSource)
  }
  if (table.querySelector('th[data-sort-key]')) {
    TableHeaderSort.create(table, dataSource)
  }

  dataSource.on('update', updateTable)
  dataSource.on('error', error => {
    console.log(error)
    toggleDisplayError(id, 'Error fetching data: ' + error)
  })
  dataSource.reload()

  function strToBool(str){
    return str === 'true' ? true : false
  }

  function getData () {
    throw "must be fixed"
  }

  function reload () {
    dataSource.reload()
  }

  function updateTable () {
    const items = dataSource.items
    const totalCount = dataSource.totalCount
    document.getElementById(countId).textContent = totalCount
    const t = document.getElementById(id).tBodies[0]
    if (!Array.isArray(items) || items.length === 0) {
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
    for (let i = 0; i < items.length; i++) {
      const item = items[i]
      try {
        const foundIndex = findIndex(t.rows, start, item)
        if (foundIndex !== -1) {
          if (foundIndex !== i) {
            renderRow(t.rows[i], item, true)
            setKeyAttributes(t.rows[i], item)
            if (items[foundIndex]) {
              renderRow(t.rows[foundIndex], items[foundIndex], true)
              setKeyAttributes(t.rows[foundIndex], items[foundIndex])
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

    let rowsToDelete = t.rows.length - items.length
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

  function renderSearch (conatiner, dataSource) {
    const form = document.createElement("form")
    form.classList.add("form")
    form.addEventListener("submit", (e) => { e.preventDefault() })
    const filterInput = document.createElement("input")
    filterInput.classList.add("filter-table")
    filterInput.placeholder = "Filter regex"
    filterInput.value = dataSource.searchTerm ?? ''
    form.appendChild(filterInput)
    container.insertBefore(form, container.children[0])
    container.addEventListener('keyup', e => {
      if (!e.target.classList.contains('filter-table')) return true
      if (e.key == 'Enter') {
        dataSource.searchTerm = e.target.value
        reload()
      }
    })
    dataSource.on('update', _ => {
      if (filterInput !== document.activeElement) {
        filterInput.value = dataSource.searchTerm
      }
    })
  }

  return { updateTable, reload, getData }
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
