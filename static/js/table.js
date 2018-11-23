/* global avalanchemq */
(function () {
  window.avalanchemq = window.avalanchemq || {}

  function renderTable (id, options = {}, renderRow) {
    let sortKey = ''
    let reverseOrder = false
    const url = options.url
    const keyColumns = options.keyColumns
    const interval = options.interval
    let timer = null

    makeHeadersSortable()

    if (url) {
      const raw = window.sessionStorage.getItem(url)
      if (raw) {
        updateTable(raw)
      }
      fetchAndUpdate()
      if (interval) {
        timer = setInterval(fetchAndUpdate, interval)
      }
    }

    function makeHeadersSortable () {
      document.querySelectorAll('#' + id + ' th[data-sort-key]').forEach(function (cell) {
        cell.addEventListener('click', function (e) {
          document.querySelectorAll('#' + id + ' th[data-sort-key]').forEach(th => {
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
          const t = document.getElementById(id).tBodies[0]
          clearRows(t)
          const raw = window.sessionStorage.getItem(url)
          updateTable(raw)
        })
      })
    }

    function clearRows (t) {
      while (t.rows.length) {
        t.deleteRow(-1)
      }
    }

    function fetchAndUpdate () {
      const tableError = document.getElementById(id + '-error')
      return avalanchemq.http.request('GET', url).then(function (response) {
        tableError.textContent = ''
        try {
          window.sessionStorage.setItem(url, JSON.stringify(response))
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

    function updateTable (raw) {
      let data = raw
      if (typeof raw === 'string') {
        data = JSON.parse(raw)
      }
      data.sort(byColumn)
      document.getElementById(id + '-count').textContent = data.length
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
            renderRow(t.rows[foundIndex], data[foundIndex], true)
            setKeyAttributes(t.rows[foundIndex], data[foundIndex])
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

    function byColumn (a, b) {
      const sortColumns = sortKey === '' ? keyColumns : [sortKey].concat(keyColumns)
      for (let i = 0; i < sortColumns.length; i++) {
        if (a[sortColumns[i]] > b[sortColumns[i]]) {
          return 1 * (reverseOrder ? -1 : 1)
        }
        if (a[sortColumns[i]] < b[sortColumns[i]]) {
          return -1 * (reverseOrder ? -1 : 1)
        }
      }
      return 0
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
    return { updateTable, fetchAndUpdate }
  }

  function renderCell (tr, column, value, classList = '') {
    const cell = tr.cells[column] || buildCells(tr, column)
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

  Object.assign(window.avalanchemq, {
    table: { renderCell, renderTable, renderHtmlCell }
  })
})()
