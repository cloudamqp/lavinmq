import * as Table from './table.js'
import * as Helpers from './helpers.js'
import * as HTTP from './http.js'
import * as DOM from './dom.js'

// A one button form that closes the named channel, for use in a table cell
function closeChannelForm (name) {
  const form = document.createElement('form')
  form.appendChild(DOM.button.delete({ text: 'Close', type: 'submit' }))
  form.addEventListener('submit', function (evt) {
    evt.preventDefault()
    if (!window.confirm(`Are you sure you want to close channel ${name}?`)) return false
    const headers = new window.Headers({ 'X-Reason': 'Closed via Web management' })
    HTTP.request('DELETE', HTTP.url`api/channels/${name}`, { headers })
      .then(() => { DOM.toast(`Channel ${name} closed`) })
  })
  return form
}

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
    Table.renderCell(tr, 7, closeChannelForm(item.name), 'right')
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
