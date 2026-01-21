import * as HTTP from './http.js'
import * as DOM from './dom.js'
import * as Table from './table.js'
import * as Chart from './chart.js'
import { UrlDataSource } from './datasource.js'

interface ClientProperties {
  connection_name?: string
  capabilities?: Record<string, unknown>
  product?: string
  product_version?: string
  platform?: string
  platform_version?: string
  information?: string
}

interface ConnectionResponse {
  state: string
  user: string
  connected_at: number
  timeout: number
  auth_mechanism: string
  channel_max: number
  frame_max: number
  tls_version: string
  cipher: string
  protocol: string
  client_properties: ClientProperties
  client_id?: string
  send_oct_details: { rate: number }
  recv_oct_details: { rate: number }
}

interface ChannelItem {
  name: string
  vhost: string
  username: string
  confirm: boolean
  consumer_count: number
  prefetch_count: number
  messages_unacknowledged: number
}

const chart = Chart.render('chart', 'bytes/s')

const connection = new URLSearchParams(window.location.hash.substring(1)).get('name') ?? ''
document.title = `Connection ${connection} | LavinMQ`
const pagenameLabel = document.querySelector('#pagename-label')
if (pagenameLabel) pagenameLabel.textContent = connection

const connectionUrl = `api/connections/${connection}`
function updateConnection(all: boolean): void {
  HTTP.request<ConnectionResponse>('GET', connectionUrl).then((item) => {
    if (!item) return
    const stats = { send_details: item.send_oct_details, receive_details: item.recv_oct_details }
    Chart.update(chart, stats)
    const stateEl = document.getElementById('state')
    if (stateEl && item.state !== stateEl.textContent) {
      stateEl.textContent = item.state
    }
    if (all) {
      const isAMQP = item.protocol && item.protocol.includes('AMQP')

      const setTextById = (id: string, text: string): void => {
        const el = document.getElementById(id)
        if (el) el.textContent = text
      }

      setTextById('conn-username', item.user)
      setTextById('connected_at', new Date(item.connected_at).toLocaleString())
      setTextById('heartbeat', item.timeout + 's')
      setTextById('authentication', item.auth_mechanism)
      setTextById('channel_max', String(item.channel_max))
      setTextById('frame_max', String(item.frame_max))
      setTextById('tls_version', item.tls_version)
      setTextById('cipher', item.cipher)
      const cp = item.client_properties
      // Show client_id for MQTT, connection_name for AMQP
      const clientName = isAMQP ? cp.connection_name : item.client_id
      setTextById('cp-name', clientName ?? '')
      setTextById('cp-capabilities', cp.capabilities ? DOM.jsonToText(cp.capabilities) : '')

      const productEl = document.getElementById('cp-product')
      if (productEl) {
        if (cp.product_version) {
          productEl.appendChild(document.createElement('span')).textContent = cp.product ?? ''
          productEl.appendChild(document.createElement('br'))
          productEl.appendChild(document.createElement('small')).textContent = 'Verison: ' + cp.product_version
        } else {
          productEl.textContent = cp.product ?? ''
        }
      }
      const platformEl = document.getElementById('cp-platform')
      if (platformEl) {
        if (cp.platform_version) {
          platformEl.appendChild(document.createElement('span')).textContent = cp.platform ?? ''
          platformEl.appendChild(document.createElement('br'))
          platformEl.appendChild(document.createElement('small')).textContent = 'Verison: ' + cp.platform_version
        } else {
          platformEl.textContent = cp.platform ?? ''
        }
      }
      const infoEl = document.getElementById('cp-information')
      if (infoEl) {
        if (cp.information && cp.information.startsWith('http')) {
          const infoLink = document.createElement('a')
          infoLink.textContent = cp.information
          infoLink.href = cp.information
          infoEl.appendChild(infoLink)
        } else {
          infoEl.textContent = cp.information || ''
        }
      }

      // Show AMQP-only elements for AMQP connections
      if (isAMQP) {
        const amqpAuth = document.getElementById('amqp-auth-channel')
        const amqpFrame = document.getElementById('amqp-frame')
        const clientProps = document.getElementById('client-properties')
        const channelsSection = document.getElementById('channels-section')
        if (amqpAuth) amqpAuth.style.display = ''
        if (amqpFrame) amqpFrame.style.display = ''
        if (clientProps) (clientProps as HTMLElement).style.display = 'block'
        if (channelsSection) (channelsSection as HTMLElement).style.display = 'block'
      }
    }
  })
}
updateConnection(true)
setInterval(updateConnection, 5000)
const channelsDataSource = new UrlDataSource<ChannelItem>(connectionUrl + '/channels', { useQueryState: false })
const tableOptions = {
  dataSource: channelsDataSource,
  keyColumns: ['name'],
  countId: 'table-count',
}
Table.renderTable<ChannelItem>('table', tableOptions, function (tr, item, all) {
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

const closeForm = document.querySelector('#closeConnection')
if (closeForm) {
  closeForm.addEventListener('submit', function (evt) {
    evt.preventDefault()
    const url = `api/connections/${connection}`
    const reasonInput = document.querySelector<HTMLInputElement>('[name=reason]')
    const headers = new Headers({
      'X-Reason': reasonInput?.value ?? '',
    })
    HTTP.request('DELETE', url, { headers }).then(() => {
      window.location.href = 'connections'
    })
  })
}
