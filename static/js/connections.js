import * as DOM from './dom.js'
import * as Table from './table.js'

const vhost = window.sessionStorage.getItem('vhost')
const escapeHTML = DOM.escapeHTML
const numFormatter = new Intl.NumberFormat()
let url = 'api/connections'
if (vhost && vhost !== '_all') {
  url = `api/vhosts/${encodeURIComponent(vhost)}/connections`
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
    let client = ''
    if (item.client_properties.product) {
      client = `${item.client_properties.product} / ${item.client_properties.platform || ''}`
    }
    const clientVersion = item.client_properties.version || ''
    const connectionLink = document.createElement('a')
    connectionLink.href = `connection#name=${encodeURIComponent(item.name)}`
    if (item.client_properties.connection_name) {
      connectionLink.innerHTML = `<span>${item.name}</span>
              <br><small>${escapeHTML(item.client_properties.connection_name)}</small>`
    } else {
      connectionLink.textContent = item.name
    }
    Table.renderCell(tr, 0, item.vhost)
    Table.renderCell(tr, 1, connectionLink)
    Table.renderCell(tr, 2, item.user)
    Table.renderCell(tr, 4, item.ssl ? '🔒' : '', 'center')
    Table.renderCell(tr, 5, item.tls_version, 'center')
    Table.renderCell(tr, 6, item.cipher, 'center')
    Table.renderCell(tr, 7, item.protocol, 'center')
    Table.renderCell(tr, 9, item.channel_max, 'right')
    Table.renderCell(tr, 10, item.timeout, 'right')
    // Table.renderCell(tr, 8, item.auth_mechanism)
    Table.renderHtmlCell(tr, 11, `${escapeHTML(client)}<br><small>${escapeHTML(clientVersion)}</small>`)
    Table.renderCell(tr, 12, new Date(item.connected_at).toLocaleString(), 'center')
  }
  Table.renderCell(tr, 3, null, `center state-${item.state}`)
  Table.renderCell(tr, 8, item.channels, 'right')
  Table.renderCell(tr, 13, numFormatter.format(item.recv_oct), 'right')
  Table.renderCell(tr, 14, numFormatter.format(item.send_oct), 'right')
})
