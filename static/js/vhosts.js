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
  const urlEncodedVhost = encodeURIComponent(item.name)
  const permissionsUrl = 'api/vhosts/' + urlEncodedVhost + '/permissions'
  const permissionsPromise = new Promise(function (resolve, reject) {
    HTTP.request('GET', permissionsUrl).then(permissions => {
      window.sessionStorage.setItem(permissionsUrl, JSON.stringify(permissions))
      resolve(permissions)
    }).catch(e => {
      Table.toggleDisplayError(e.status === 401 ? e.body + ': You need administrator role to see this view' : e.body)
      reject(e)
    })
  })
  permissionsPromise.then(permissions => {
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
  })
})

document.querySelector('#createVhost').addEventListener('submit', function (evt) {
  evt.preventDefault()
  const data = new window.FormData(this)
  const name = encodeURIComponent(data.get('name').trim())
  const url = 'api/vhosts/' + name
  HTTP.request('PUT', url)
    .then(() => {
      vhostTable.fetchAndUpdate()
      evt.target.reset()
    }).catch(HTTP.standardErrorHandler)
})
