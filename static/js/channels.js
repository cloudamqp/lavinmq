import * as Table from './table.js'
import * as Helpers from './helpers.js'

const vhost = window.sessionStorage.getItem('vhost')
let url = 'api/channels'
if (vhost && vhost !== '_all') {
  url = 'api/vhosts/' + encodeURIComponent(vhost) + '/channels'
}
const tableOptions = {
  url,
  keyColumns: ['name'],
  interval: 5000,
  pagination: true,
  columnSelector: true,
  search: true
}
Table.renderTable('table', tableOptions, function (tr, item, all) {
  if (all) {
    const channelLink = document.createElement('a')
    const urlEncodedChannel = encodeURIComponent(item.name)
    channelLink.textContent = item.name
    channelLink.href = `channel#name=${urlEncodedChannel}`
    Table.renderCell(tr, 0, channelLink)
    Table.renderCell(tr, 1, item.vhost)
    Table.renderCell(tr, 2, item.user)
  }
  let mode = ''
  mode += item.confirm ? ' C' : ''
  Table.renderCell(tr, 3, mode, 'center')
  Table.renderCell(tr, 4, Helpers.formatNumber(item.consumer_count), 'right')
  Table.renderCell(tr, 5, Helpers.formatNumber(item.prefetch_count), 'right')
  Table.renderCell(tr, 6, Helpers.formatNumber(item.messages_unacknowledged), 'right')
})
