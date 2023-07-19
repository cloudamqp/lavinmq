function create (table, dataSource) {
  let sortKey = dataSource.sortKey
  function update () {
    table.querySelectorAll('th[data-sort-key]').forEach(function (cell) {
      cell.classList.remove('sorting_asc', 'sorting_desc')
      if (cell.dataset.sortKey === dataSource.sortKey) {
        cell.classList.add(dataSource.reverseOrder ? 'sorting_desc' : 'sorting_asc')
      }
    })
  }
  table.querySelectorAll('th[data-sort-key]').forEach(function (cell) {
    cell.addEventListener('click', e => {
      const newSortKey = e.target.dataset.sortKey
      if (sortKey === newSortKey) {
        dataSource.reverseOrder = !dataSource.reverseOrder
      } else {
        dataSource.reverseOrder = false
      }
      sortKey = newSortKey
      dataSource.sortKey = newSortKey
      dataSource.reload()
    })
  })
  dataSource.on('update', update)
}

export { create }
