import * as HTTP from './http.js'
import * as DOM from './dom.js'
import * as Table from './table.js'
import Chart from './chart.js'
import { UrlDataSource } from './datasource.js'

const chart = new Chart('chart', 'bytes/s')

const connection = new URLSearchParams(window.location.hash.substring(1)).get('name')
document.title = `Connection ${connection} | LavinMQ`
document.querySelector('#pagename-label').textContent = connection

const connectionUrl = `api/connections/${connection}`
function updateConnection (all) {
  HTTP.request('GET', connectionUrl).then(item => {
    const stats = { send_details: item.send_oct_details, receive_details: item.recv_oct_details }
    chart.update(stats)
    const stateEl = document.getElementById('state')
    if (item.state !== stateEl.textContent) {
      stateEl.textContent = item.state
    }
    if (all) {
      document.getElementById('conn-username').textContent = item.user
      document.getElementById('connected_at').textContent = new Date(item.connected_at).toLocaleString()
      document.getElementById('heartbeat').textContent = item.timeout + 's'
      document.getElementById('authentication').textContent = item.auth_mechanism
      document.getElementById('channel_max').textContent = item.channel_max
      document.getElementById('frame_max').textContent = item.frame_max
      document.getElementById('tls_version').textContent = item.tls_version
      document.getElementById('cipher').textContent = item.cipher
      const cp = item.client_properties
      document.getElementById('cp-name').textContent = cp.connection_name
      document.getElementById('cp-capabilities').textContent = DOM.jsonToText(cp.capabilities)
      if (cp.product_version) {
        document.getElementById('cp-product').appendChild(document.createElement('span')).textContent = cp.product
        document.getElementById('cp-product').appendChild(document.createElement('br'))
        document.getElementById('cp-product').appendChild(document.createElement('small')).textContent = 'Verison: ' + cp.product_version
      } else {
        document.getElementById('cp-product').textContent = cp.product
      }
      if (cp.platform_version) {
        document.getElementById('cp-platform').appendChild(document.createElement('span')).textContent = cp.platform
        document.getElementById('cp-platform').appendChild(document.createElement('br'))
        document.getElementById('cp-platform').appendChild(document.createElement('small')).textContent = 'Verison: ' + cp.platform_version
      } else {
        document.getElementById('cp-platform').textContent = cp.platform
      }
      const infoEl = document.getElementById('cp-information')
      if (cp.information && cp.information.startsWith('http')) {
        const infoLink = document.createElement('a')
        infoLink.textContent = cp.information
        infoLink.href = cp.information
        infoEl.appendChild(infoLink)
      } else {
        infoEl.textContent = cp.information || ''
      }
    }
  })
}
updateConnection(true)
setInterval(updateConnection, 1000)
const channelsDataSource = new UrlDataSource(connectionUrl + '/channels', { useQueryState: false })
const tableOptions = {
  dataSource: channelsDataSource,
  keyColumns: ['name'],
  countId: 'table-count'
}
Table.renderTable('table', tableOptions, function (tr, item, all) {
  if (all) {
    const channelLink = document.createElement('a')
    channelLink.textContent = item.name
    channelLink.href = HTTP.url`channel#name=${item.name}`
    Table.renderCell(tr, 0, channelLink)
    Table.renderCell(tr, 1, item.vhost)
    Table.renderCell(tr, 2, item.username)
  }
  let mode = ''
  mode += item.confirm ? ' C' : ''
  Table.renderCell(tr, 3, mode, 'center')
  Table.renderCell(tr, 4, item.consumer_count, 'right')
  Table.renderCell(tr, 5, item.prefetch_count, 'right')
  Table.renderCell(tr, 6, item.messages_unacknowledged, 'right')
})

document.querySelector('#closeConnection').addEventListener('submit', function (evt) {
  evt.preventDefault()
  const url = `api/connections/${connection}`
  const headers = new window.Headers({
    'X-Reason': document.querySelector('[name=reason]').value
  })
  HTTP.request('DELETE', url, { headers })
    .then(() => { window.location = 'connections' })
})
