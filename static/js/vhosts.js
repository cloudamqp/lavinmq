import * as HTTP from './http.js'
import * as Helpers from './helpers.js'
import * as Table from './table.js'

const tableOptions = {
  url: 'api/vhosts',
  keyColumns: ['name'],
  interval: 5000,
  pagination: true,
  columnSelector: true,
  search: true
}
const vhostTable = Table.renderTable('table', tableOptions, (tr, item, all) => {
  const permissionsUrl = HTTP.url`api/vhosts/${item.name}/permissions`
  HTTP.request('GET', permissionsUrl)
    .then(permissions => {
      window.sessionStorage.setItem(permissionsUrl, JSON.stringify(permissions))
      if (all) {
        const vhostLink = document.createElement('a')
        vhostLink.href = `vhost#name=${urlEncodedVhost}`
        vhostLink.textContent = item.name
        Table.renderCell(tr, 0, vhostLink)
      }
      const userList = permissions.filter(p => p.vhost === item.name).map(p => p.user).join(', ')
      Table.renderCell(tr, 1, userList)
      Table.renderCell(tr, 2, Helpers.formatNumber(item.messages_ready), 'center')
      Table.renderCell(tr, 3, Helpers.formatNumber(item.messages_unacknowledged), 'center')
      Table.renderCell(tr, 4, Helpers.formatNumber(item.messages), 'center')
    }).catch(e => {
      Table.toggleDisplayError('table', e.status === 403 ? 'You need administrator role to see this view' : e.body)
    })
})

document.querySelector('#createVhost').addEventListener('submit', function (evt) {
  evt.preventDefault()
  const data = new window.FormData(this)
  const name = data.get('name').trim()
  const url = HTTP.url`'api/vhosts/${name}`
  HTTP.request('PUT', url)
    .then(() => {
      vhostTable.reload()
      evt.target.reset()
    })
})
