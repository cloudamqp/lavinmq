import * as HTTP from './http.js'
import * as Table from './table.js'

interface ClientProperties {
  connection_name?: string
  product?: string
  platform?: string
  version?: string
}

interface ConnectionItem {
  name: string
  vhost: string
  user: string
  ssl: boolean
  tls_version: string
  cipher: string
  protocol: string
  auth_mechanism: string
  channel_max: number
  timeout: number
  client_properties: ClientProperties
  connected_at: number
  state: string
  channels: number
  recv_oct: number
  send_oct: number
  client_id?: string
}

const vhost = window.sessionStorage.getItem('vhost')
const numFormatter = new Intl.NumberFormat()
let url = 'api/connections'
if (vhost && vhost !== '_all') {
  url = HTTP.url`api/vhosts/${vhost}/connections`
}
const tableOptions = {
  url,
  keyColumns: ['name'],
  pagination: true,
  columnSelector: true,
  search: true,
}

Table.renderTable<ConnectionItem>('table', tableOptions, function (tr, item, all) {
  if (all) {
    const connectionLink = document.createElement('a')
    connectionLink.href = HTTP.url`connection#name=${item.name}`
    connectionLink.appendChild(document.createElement('span')).textContent = item.name
    connectionLink.appendChild(document.createElement('br'))
    // Show connection_name for AMQP, client_id for MQTT
    const clientIdentifier =
      item.protocol && item.protocol.includes('AMQP') ? item.client_properties.connection_name : item.client_id
    const small = connectionLink.appendChild(document.createElement('small'))
    small.textContent = clientIdentifier ?? ''
    Table.renderCell(tr, 0, item.vhost)
    Table.renderCell(tr, 1, connectionLink)
    Table.renderCell(tr, 2, item.user)
    Table.renderCell(tr, 4, item.ssl ? '\u{1F512}' : '', 'center')
    Table.renderCell(tr, 5, item.tls_version, 'center')
    Table.renderCell(tr, 6, item.cipher, 'center')
    Table.renderCell(tr, 7, item.protocol, 'center')
    Table.renderCell(tr, 8, item.auth_mechanism)
    Table.renderCell(tr, 9, item.channel_max, 'right')
    Table.renderCell(tr, 10, item.timeout, 'right')
    const clientDiv = document.createElement('span')
    if (item.client_properties.product) {
      clientDiv.textContent = `${item.client_properties.product || ''} / ${item.client_properties.platform || ''}`
    } else {
      clientDiv.textContent = item.client_properties.platform || ''
    }
    clientDiv.appendChild(document.createElement('br'))
    clientDiv.appendChild(document.createElement('small')).textContent = item.client_properties.version ?? ''
    Table.renderCell(tr, 11, clientDiv)
    Table.renderCell(tr, 12, new Date(item.connected_at).toLocaleString(), 'center')
  }
  Table.renderCell(tr, 3, null, `center state-${item.state}`)
  Table.renderCell(tr, 8, item.channels, 'right')
  Table.renderCell(tr, 13, numFormatter.format(item.recv_oct), 'right')
  Table.renderCell(tr, 14, numFormatter.format(item.send_oct), 'right')
})
