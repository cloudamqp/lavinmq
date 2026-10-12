import * as HTTP from './http.js'
import * as Helpers from './helpers.js'
import * as Table from './table.js'
import * as DOM from './dom.js'

Helpers.addVhostOptions('createGroup')

const vhost = window.sessionStorage.getItem('vhost')
let url = 'api/mqtt/permission-groups'
if (vhost && vhost !== '_all') {
  url += HTTP.url`/${vhost}`
}

const tableOptions = {
  url,
  keyColumns: ['vhost', 'name'],
  autoReloadTimeout: 0,
  pagination: true,
  search: true
}
const groupsTable = Table.renderTable('groups', tableOptions, (tr, item, all) => {
  if (all) {
    const groupLink = document.createElement('a')
    groupLink.href = HTTP.url`mqtt-permission-group#vhost=${item.vhost}&name=${item.name}`
    groupLink.textContent = item.name
    Table.renderCell(tr, 0, groupLink)
  }
  Table.renderCell(tr, 1, item.vhost)
  Table.renderCell(tr, 2, item.member_count, 'center')
  Table.renderCell(tr, 3, item.rule_count, 'center')
})

document.querySelector('#createGroup').addEventListener('submit', function (evt) {
  evt.preventDefault()
  const data = new window.FormData(this)
  const vhost = data.get('vhost')
  const name = data.get('name').trim()
  const url = HTTP.url`api/mqtt/permission-groups/${vhost}/${name}`
  HTTP.request('PUT', url)
    .then(() => {
      groupsTable.reload()
      DOM.toast(`Group created: '${name}'`)
      evt.target.reset()
    })
    .catch(() => {})
})
