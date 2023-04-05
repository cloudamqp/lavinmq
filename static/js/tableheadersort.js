function create(table, dataSource) {
  let sortKey = dataSource.sortKey
  let reverseOrder = dataSource.reverseOrder
  const sortHeader = table.querySelector(`th[data-sort-key="${sortKey}"]`)
  if (sortHeader) {
    const sortClass = dataSource.sortOrder === 'desc' ? 'sorting_desc' : 'sorting_asc'
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
        reverseOrder = false
        e.target.classList.add('sorting_desc')
        e.target.classList.remove('sorting_asc')
      }
      dataSource.sortKey = sortKey
      dataSource.reverseOrder = reverseOrder
      dataSource.reload()
    })
  })
}


export { create }
