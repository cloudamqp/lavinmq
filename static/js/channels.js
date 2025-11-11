import * as Table from './table.js'
import * as Helpers from './helpers.js'
import * as HTTP from './http.js'

const vhost = window.sessionStorage.getItem('vhost')
let url = 'api/channels'
if (vhost && vhost !== '_all') {
  url = HTTP.url`api/vhosts/${vhost}/channels`
}
const tableOptions = {
  url,
  keyColumns: ['name'],
  pagination: true,
  columnSelector: true,
  search: true
}
Table.renderTable('table', tableOptions, function (tr, item, all) {
  if (all) {
    const channelLink = document.createElement('a')
    channelLink.textContent = item.name
    channelLink.href = HTTP.url`channel#name=${item.name}`
    Table.renderCell(tr, 0, channelLink)
    Table.renderCell(tr, 1, item.vhost)
    Table.renderCell(tr, 2, item.user)
  }
  if (item.confirm) {
    const confirmSpan = document.createElement('span')
    confirmSpan.textContent = 'Confirm'
    confirmSpan.title = 'Confirm mode enables publisher acknowledgements for reliable message delivery'
    Table.renderCell(tr, 3, confirmSpan, 'center')
  }
  Table.renderCell(tr, 4, Helpers.formatNumber(item.consumer_count), 'right')
  Table.renderCell(tr, 5, Helpers.formatNumber(item.prefetch_count), 'right')
  Table.renderCell(tr, 6, Helpers.formatNumber(item.messages_unacknowledged), 'right')
})
