import * as Table from './table.js'
import * as HTTP from './http.js'
import * as DOM from './dom.js'
import * as Chart from './chart.js'

const channel = new URLSearchParams(window.location.hash.substring(1)).get('name')
const urlEncodedChannel = encodeURIComponent(channel)
const chart = Chart.render('chart', 'msgs/s')
let vhost = null
document.title = channel + ' | LavinMQ'

const consumerTableOpts = {
  keyColumns: ['consumer_tag'],
  countId: 'consumer-count'
}
const consumersTable = Table.renderTable('table', consumerTableOpts, function (tr, item, all) {
  if (!all) return
  Table.renderCell(tr, 0, item.consumer_tag)
  const queueLink = document.createElement('a')
  queueLink.href = `queue#vhost=${encodeURIComponent(vhost)}&name=${encodeURIComponent(item.queue.name)}`
  queueLink.textContent = item.queue.name
  const ack = item.ack_required ? '●' : '○'
  const exclusive = item.exclusive ? '●' : '○'
  Table.renderCell(tr, 1, queueLink)
  Table.renderCell(tr, 2, ack, 'center')
  Table.renderCell(tr, 3, exclusive, 'center')
  Table.renderCell(tr, 4, item.prefetch_count, 'right')
})

const channelUrl = 'api/channels/' + urlEncodedChannel
function updateChannel (all) {
  HTTP.request('GET', channelUrl).then(item => {
    Chart.update(chart, item.message_stats)
    vhost = item.vhost
    const stateEl = document.getElementById('ch-state')
    if (item.state !== stateEl.textContent) {
      stateEl.textContent = item.state
    }
    document.getElementById('ch-unacked').textContent = item.messages_unacknowledged
    consumersTable.updateTable(item.consumer_details)
    if (all) {
      document.querySelector('#pagename-label').textContent = channel + ' in virtual host ' + item.vhost
      document.getElementById('ch-username').textContent = item.user
      const connectionLink = document.createElement('a')
      connectionLink.href = `connection#name=${encodeURIComponent(item.connection_details.name)}`
      connectionLink.textContent = item.connection_details.name
      DOM.setChild('#ch-connection', connectionLink)
      document.getElementById('ch-prefetch').textContent = item.prefetch_count
      let mode = ''
      mode += item.confirm ? ' C' : ''
      document.getElementById('ch-mode').textContent = mode
      document.getElementById('ch-global-prefetch').textContent = item.global_prefetch_count
    }
  }).catch(HTTP.standardErrorHandler).catch(e => clearInterval(cTimer))
}
updateChannel(true)
const cTimer = setInterval(updateChannel, 5000)
