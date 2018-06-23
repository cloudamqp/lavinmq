(function () {
  window.avalanchemq = window.avalanchemq || {}

  function renderTable (id, options = {}, renderRow) {
    let sortKey = ''
    let reverseOrder = false
    let updateTimer = null
    const url = options.url
    const keyColumns = options.keyColumns
    const interval = options.interval

    makeHeadersSortable()

    if (url) {
      const raw = localStorage.getItem(url)
      if (raw) {
        updateTable(raw)
      }
      fetchAndUpdate()
      if (interval) {
        updateTimer = setInterval(fetchAndUpdate, interval)
      }
    }

    function makeHeadersSortable () {
      document.querySelectorAll('#' + id + ' th[data-sort-key]').forEach(function (cell) {
        cell.addEventListener('click', function (e) {
          // let column = e.target.cellIndex;
          // let newSortColumn = e.target.textContent.toLowerCase();
          const newSortKey = e.target.getAttribute('data-sort-key')
          if (newSortKey === sortKey) {
            reverseOrder = !reverseOrder
          } else {
            sortKey = newSortKey
            reverseOrder = false
          }
          clearInterval(updateTimer)
          const t = document.getElementById(id).tBodies[0]
          clearRows(t)
          const raw = localStorage.getItem(url)
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
          localStorage.setItem(url, JSON.stringify(response))
        } catch (e) {
          console.error('Saving localStorage', e)
        }
        updateTable(response)
      }).catch(function (e) {
        tableError.textContent = 'Error fetching data'
        console.error(e)
      })
    }

    function updateTable (raw) {
      let data = raw
      if (typeof raw === "string") {
        data = JSON.parse(raw)
      }
      if (!Array.isArray(data)) {
        return
      }
      data.sort(byColumn)
      document.getElementById(id + '-count').textContent = data.length
      const t = document.getElementById(id).tBodies[0]
      for (let i = 0; i < data.length; i++) {
        const item = data[i]
        const foundIndex = findIndex(t.rows, i, item)
        if (foundIndex !== -1) {
          let d = foundIndex - i
          while (d--) {
            t.deleteRow(i)
          }
          renderRow(t.rows[i], item)
        } else {
          const tr = t.insertRow(i)
          setKeyAttributes(tr, item)
          renderRow(tr, item)
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
    const cell = tr.cells[column] || tr.insertCell(-1)
    cell.classList = classList
    if (value instanceof Element) {
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
    const cell = tr.cells[column] || tr.insertCell(-1)
    if (cell.innerHTML !== innerHTML) {
      cell.innerHTML = innerHTML
    }
    return cell
  }

  Object.assign(window.avalanchemq, {
    table: { renderCell, renderTable, renderHtmlCell }
  })
})()
