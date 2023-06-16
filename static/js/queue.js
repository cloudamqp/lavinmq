import * as HTTP from './http.js'
import * as Helpers from './helpers.js'
import * as DOM from './dom.js'
import * as Table from './table.js'
import * as Chart from './chart.js'
import * as Auth from './auth.js'
import { UrlDataSource, DataSource } from './datasource.js'

const search = new URLSearchParams(window.location.hash.substring(1))
const queue = search.get('name')
const vhost = search.get('vhost')
const urlEncodedQueue = encodeURIComponent(queue)
const urlEncodedVhost = encodeURIComponent(vhost)
const pauseQueueForm = document.querySelector('#pauseQueue')
const resumeQueueForm = document.querySelector('#resumeQueue')
const messageSnapshotForm = document.querySelector('#messageSnapshot')
document.title = queue + ' | LavinMQ'
let consumerListLength = 20

class ConsumersDataSource extends DataSource {
  constructor() { super({autoReloadTimeout: 0, useQueryState: false}) }
  setConsumers(consumers) { this.items = consumers }
  reload() { }
}
const consumersDataSource = new ConsumersDataSource()
const consumersTableOpts = {
  keyColumns: ["consumer_tag", "channel_details"],
  countId: 'consumer-count',
  dataSource: consumersDataSource
}
const consumersTable = Table.renderTable('table', consumersTableOpts, function (tr, item) {
  const channelLink = document.createElement('a')
  channelLink.href = 'channel#name=' + encodeURIComponent(item.channel_details.name)
  channelLink.textContent = item.channel_details.name
  const ack = item.ack_required ? '●' : '○'
  const exclusive = item.exclusive ? '●' : '○'
  const cancelForm = document.createElement('form')
  const btn = document.createElement('button')
  btn.classList.add('btn-primary')
  btn.type = 'submit'
  btn.textContent = 'Cancel'
  cancelForm.appendChild(btn)
  const urlEncodedConsumerTag = encodeURIComponent(item.consumer_tag)
  const conn = encodeURIComponent(item.channel_details.connection_name)
  const ch = encodeURIComponent(item.channel_details.number)
  const actionPath = `api/consumers/${urlEncodedVhost}/${conn}/${ch}/${urlEncodedConsumerTag}`
  cancelForm.addEventListener('submit', function (evt) {
    evt.preventDefault()
    if (!confirm("Are you sure?")) return false
    HTTP.request('DELETE', actionPath)
      .then(() => {
        DOM.toast(`Consumer cancelled`)
        updateQueue(false)
      }).catch(HTTP.standardErrorHandler)
  })
  Table.renderCell(tr, 0, channelLink)
  Table.renderCell(tr, 1, item.consumer_tag)
  Table.renderCell(tr, 2, ack, 'center')
  Table.renderCell(tr, 3, exclusive, 'center')
  Table.renderCell(tr, 4, item.prefetch_count, 'right')
  Table.renderCell(tr, 5, cancelForm, 'center')
})

const loadMoreConsumersBtn = document.getElementById("load-more-consumers");
loadMoreConsumersBtn.addEventListener('click', (e) => {
  consumerListLength += 10;
  updateQueue(true)
});

function handleQueueState (state) {
  document.getElementById('q-state').textContent = state
  switch (state) {
    case 'paused':
      pauseQueueForm.classList.add('hide')
      resumeQueueForm.classList.remove('hide')
      break
    case 'running':
      pauseQueueForm.classList.remove('hide')
      resumeQueueForm.classList.add('hide')
      break
    default:
      pauseQueueForm.disabled = true
      resumeQueueForm.disabled = true
  }
}

const chart = Chart.render('chart', 'msgs/s')
const queueUrl = 'api/queues/' + urlEncodedVhost + '/' + urlEncodedQueue
function updateQueue (all) {
  HTTP.request('GET', queueUrl + '?consumer_list_length=' + consumerListLength)
    .then(item => {
      Chart.update(chart, item.message_stats)
      handleQueueState(item.state)
      document.getElementById('q-unacked').textContent = item.unacked
      document.getElementById('q-unacked-bytes').textContent = Helpers.nFormatter(item.unacked_bytes) + 'B'
      document.getElementById('q-unacked-avg-bytes').textContent = Helpers.nFormatter(item.unacked_avg_bytes) + 'B'
      document.getElementById('q-total').textContent = Helpers.formatNumber(item.messages)
      document.getElementById('q-total-bytes').textContent = Helpers.nFormatter(item.unacked_bytes + item.ready_bytes) + 'B'
      const total_avg_bytes = item.messages != 0 ? (item.unacked_bytes + item.ready_bytes)/item.messages : 0
      document.getElementById('q-total-avg-bytes').textContent = Helpers.nFormatter(total_avg_bytes) + 'B'
      document.getElementById('q-ready').textContent = Helpers.formatNumber(item.ready)
      document.getElementById('q-ready-bytes').textContent = Helpers.nFormatter(item.ready_bytes) + 'B'
      document.getElementById('q-ready-avg-bytes').textContent = Helpers.nFormatter(item.ready_avg_bytes) + 'B'
      document.getElementById('q-consumers').textContent = Helpers.formatNumber(item.consumers)
      item.consumer_details.filtered_count = item.consumers
      consumersDataSource.setConsumers(item.consumer_details)
      const hasMoreConsumers = item.consumer_details.length < item.consumers
      loadMoreConsumersBtn.classList.toggle("visible", hasMoreConsumers)
      if(hasMoreConsumers) {
        loadMoreConsumersBtn.textContent = `Showing ${item.consumer_details.length} of total ${item.consumers} consumers, click to load more`
      }
      if (all) {
        let features = ''
        features += item.durable ? ' D' : ''
        features += item.auto_delete ? ' AD' : ''
        features += item.exclusive ? ' E' : ''
        document.getElementById('q-features').textContent = features
        document.querySelector('#pagename-label').textContent = queue + ' in virtual host ' + item.vhost
        document.querySelector('.queue').textContent = queue
        if (item.policy) {
          const policyLink = document.createElement('a')
          policyLink.href = `policies#name=${encodeURIComponent(item.policy)}&vhost=${encodeURIComponent(item.vhost)}`
          policyLink.textContent = item.policy
          document.getElementById("q-policy").appendChild(policyLink)
        }
        if (item.operator_policy) {
          const policyLink = document.createElement('a')
          policyLink.href = `operator-policies#name=${encodeURIComponent(item.operator_policy)}&vhost=${encodeURIComponent(item.vhost)}`
          policyLink.textContent = item.operator_policy
          document.getElementById("q-operator-policy").appendChild(policyLink)
        }
        if (item.effective_policy_definition) {
          document.getElementById("q-effective-policy-definition").textContent = DOM.jsonToText(item.effective_policy_definition)
        }
        const qArgs = document.getElementById('q-arguments')
        for (const arg in item.arguments) {
          qArgs.appendChild(document.createElement("div")).textContent = `${arg}: ${item.arguments[arg]}`
        }
      }
    }).catch(HTTP.standardErrorHandler)
}
updateQueue(true)
setInterval(updateQueue, 5000)

const tableOptions = {
  dataSource: new UrlDataSource(queueUrl + '/bindings', { useQueryState: false }),
  keyColumns: ['properties_key'],
  countId: 'bindings-count'
}
const bindingsTable = Table.renderTable('bindings-table', tableOptions, function (tr, item, all) {
  if (!all) return
  if (item.source === '') {
    const td = Table.renderCell(tr, 0, '(Default exchange binding)')
    td.setAttribute('colspan', 4)
  } else {
    const btn = document.createElement('button')
    btn.classList.add('btn-secondary')
    btn.textContent = 'Unbind'
    const e = encodeURIComponent(item.source)
    btn.onclick = function () {
      const p = encodeURIComponent(item.properties_key)
      const url = 'api/bindings/' + urlEncodedVhost + '/e/' + e + '/q/' + urlEncodedQueue + '/' + p
      HTTP.request('DELETE', url)
        .then(() => { tr.parentNode.removeChild(tr) })
        .catch(HTTP.standardErrorHandler)
    }
    const exchangeLink = document.createElement('a')
    exchangeLink.href = `exchange#vhost=${urlEncodedVhost}&name=${e}`
    exchangeLink.textContent = item.source
    Table.renderCell(tr, 0, exchangeLink)
    Table.renderCell(tr, 1, item.routing_key)
    const pre = document.createElement("pre")
    pre.textContent = JSON.stringify(item.arguments || {})
    Table.renderCell(tr, 2, pre)
    Table.renderCell(tr, 3, btn, 'right')
  }
})

document.querySelector('#addBinding').addEventListener('submit', function (evt) {
  evt.preventDefault()
  const data = new window.FormData(this)
  const e = encodeURIComponent(data.get('source').trim())
  const url = 'api/bindings/' + urlEncodedVhost + '/e/' + e + '/q/' + urlEncodedQueue
  const args = DOM.parseJSON(data.get('arguments'))
  const body = {
    routing_key: data.get('routing_key').trim(),
    arguments: args
  }
  HTTP.request('POST', url, { body })
    .then(() => {
      bindingsTable.reload()
      evt.target.reset()
      DOM.toast('Exchange ' + e + ' bound to queue')
    }).catch(HTTP.alertErrorHandler)
})

document.querySelector('#publishMessage').addEventListener('submit', function (evt) {
  evt.preventDefault()
  const data = new window.FormData(this)
  const url = 'api/exchanges/' + urlEncodedVhost + '/amq.default/publish'
  const properties = DOM.parseJSON(data.get('properties'))
  properties.delivery_mode = parseInt(data.get('delivery_mode'))
  properties.headers = DOM.parseJSON(data.get('headers'))
  const body = {
    payload: data.get('payload'),
    payload_encoding: data.get('payload_encoding'),
    routing_key: queue,
    properties
  }
  HTTP.request('POST', url, { body })
    .then(() => {
      DOM.toast('Published message to ' + queue)
      updateQueue(false)
    }).catch(HTTP.alertErrorHandler)
})

document.querySelector('#getMessages').addEventListener('submit', function (evt) {
  evt.preventDefault()
  const data = new window.FormData(this)
  const url = 'api/queues/' + urlEncodedVhost + '/' + urlEncodedQueue + '/get'
  const body = {
    count: parseInt(data.get('messages')),
    ack_mode: data.get('mode'),
    encoding: data.get('encoding'),
    truncate: 50000
  }
  HTTP.request('POST', url, { body })
    .then(messages => {
      if (messages.length === 0) {
        window.alert('No messages in queue')
        return
      }
      updateQueue(false)
      const messagesContainer = document.getElementById('messages')
      messagesContainer.textContent = ""
      const template = document.getElementById('message-template')
      for (let i = 0; i < messages.length; i++) {
        const message = messages[i]
        const msgNode = template.cloneNode(true)
        msgNode.removeAttribute('id')
        msgNode.querySelector('.message-number').textContent = i + 1
        msgNode.querySelector('.messages-remaining').textContent = message.message_count
        const exchange = message.exchange === '' ? '(AMQP default)' : message.exchange
        msgNode.querySelector('.message-exchange').textContent = exchange
        msgNode.querySelector('.message-routing-key').textContent = message.routing_key
        msgNode.querySelector('.message-redelivered').textContent = message.redelivered
        msgNode.querySelector('.message-properties').textContent = JSON.stringify(message.properties)
        msgNode.querySelector('.message-size').textContent = message.payload_bytes
        msgNode.querySelector('.message-encoding').textContent = message.payload_encoding
        msgNode.querySelector('.message-payload').textContent = message.payload
        msgNode.classList.remove('hide')
        messagesContainer.appendChild(msgNode)
      }
    }).catch(HTTP.alertErrorHandler)
})

document.querySelector('#moveMessages').addEventListener('submit', function (evt) {
  evt.preventDefault()
  const username = Auth.getUsername()
  const password = Auth.getPassword()
  const uri = 'amqp://' + encodeURIComponent(username) + ':' + encodeURIComponent(password) + '@localhost/' + urlEncodedVhost
  const dest = document.querySelector('[name=shovel-destination]').value.trim()
  const name = 'Move ' + queue + ' to ' + dest
  const url = 'api/parameters/shovel/' + urlEncodedVhost + '/' + encodeURIComponent(name)
  const body = {
    name: name,
    value: {
      'src-uri': uri,
      'src-queue': queue,
      'dest-uri': uri,
      'dest-queue': dest,
      'src-prefetch-count': 1000,
      'ack-mode': 'on-confirm',
      'src-delete-after': 'queue-length'
    }
  }
  HTTP.request('PUT', url, { body })
    .then(() => {
      evt.target.reset()
      DOM.toast('Moving messages to ' + dest)
    }).catch(HTTP.standardErrorHandler)
})

document.querySelector('#purgeQueue').addEventListener('submit', function (evt) {
  evt.preventDefault()
  let params = ""
  let countElem = evt.target.querySelector("input[name='count']")
  if(countElem && countElem.value) {
    params = `?count=${countElem.value}`
  }
  const url = `api/queues/${urlEncodedVhost}/${urlEncodedQueue}/contents${params}`
  if (window.confirm('Are you sure? Messages cannot be recovered after purging.')) {
    HTTP.request('DELETE', url)
      .then(() => { DOM.toast('Queue purged!') })
      .catch(HTTP.standardErrorHandler)
    document.getElementById('ms-date-time').textContent = "-"
    document.getElementById('snapshotTable').setAttribute("hidden", null)
  }
})

document.querySelector('#deleteQueue').addEventListener('submit', function (evt) {
  evt.preventDefault()
  const url = 'api/queues/' + urlEncodedVhost + '/' + urlEncodedQueue
  if (window.confirm('Are you sure? The queue is going to be deleted. Messages cannot be recovered after deletion.')) {
    HTTP.request('DELETE', url)
      .then(() => { window.location = 'queues' })
      .catch(HTTP.standardErrorHandler)
  }
})

pauseQueueForm.addEventListener('submit', function (evt) {
  evt.preventDefault()
  const url = 'api/queues/' + urlEncodedVhost + '/' + urlEncodedQueue + '/pause'
  if (window.confirm('Are you sure? This will suspend deliveries to all consumers.')) {
    HTTP.request('PUT', url)
      .then(() => {
        DOM.toast('Queue paused!')
        handleQueueState('paused')
      })
      .catch(HTTP.standardErrorHandler)
  }
})

resumeQueueForm.addEventListener('submit', function (evt) {
  evt.preventDefault()
  const url = 'api/queues/' + urlEncodedVhost + '/' + urlEncodedQueue + '/resume'
  if (window.confirm('Are you sure? This will resume deliveries to all consumers.')) {
    HTTP.request('PUT', url)
      .then(() => {
        DOM.toast('Queue resumed!')
        handleQueueState('running')
      })
      .catch(HTTP.standardErrorHandler)
  }
})

messageSnapshotForm.addEventListener('submit', function (evt) {
  evt.preventDefault()
  const url = 'api/queues/' + urlEncodedVhost + '/' + urlEncodedQueue + '/size-details'
  if (window.confirm('Are you sure? This will take a snapshot of queue message sizes.')) {
    HTTP.request('GET', url)
      .then(item => {
        DOM.toast('Queue size snapshot')
        handleQueueState('running')
        document.getElementById('ms-q-unacked').textContent = item.unacked
        document.getElementById('ms-q-unacked-bytes').textContent = Helpers.nFormatter(item.unacked_bytes) + 'B'
        document.getElementById('ms-q-unacked-avg-bytes').textContent = Helpers.nFormatter(item.unacked_avg_bytes) + 'B'
        document.getElementById('ms-q-unacked-min-bytes').textContent = Helpers.nFormatter(item.unacked_min_bytes) + 'B'
        document.getElementById('ms-q-unacked-max-bytes').textContent = Helpers.nFormatter(item.unacked_max_bytes) + 'B'
        document.getElementById('ms-q-total').textContent = Helpers.formatNumber(item.messages)
        document.getElementById('ms-q-total-bytes').textContent = Helpers.nFormatter(item.unacked_bytes + item.ready_bytes) + 'B'
        const total_avg_bytes = item.messages != 0 ? (item.unacked_bytes + item.ready_bytes)/item.messages : 0
        document.getElementById('ms-q-total-avg-bytes').textContent = Helpers.nFormatter(total_avg_bytes) + 'B'
        document.getElementById('ms-q-total-max-bytes').textContent = Helpers.nFormatter(0) + 'B'
        if (item.ready_max_bytes > item.unacked_max_bytes) {
          document.getElementById('ms-q-total-max-bytes').textContent = Helpers.nFormatter(item.ready_max_bytes) + 'B'
        } else if (item.unacked_max_bytes > item.ready_max_bytes) {
          document.getElementById('ms-q-total-max-bytes').textContent = Helpers.nFormatter(item.unacked_max_bytes) + 'B'
        }
        document.getElementById('ms-q-total-min-bytes').textContent = Helpers.nFormatter(0) + 'B'
        let total_min_bytes = 0
        if (item.ready_min_bytes != 0 && item.unacked_min_bytes == 0) {
          total_min_bytes = item.ready_min_bytes
        } else if (item.unacked_min_bytes != 0 && item.ready_min_bytes == 0) {
          total_min_bytes = item.unacked_min_bytes
        } else if (item.ready_min_bytes < item.unacked_min_bytes) {
          total_min_bytes = item.ready_min_bytes
        } else if (item.unacked_min_bytes < item.ready_min_bytes) {
          total_min_bytes = item.unacked_min_bytes
        }
        document.getElementById('ms-q-total-min-bytes').textContent = Helpers.nFormatter(total_min_bytes) + 'B'
        document.getElementById('ms-q-ready').textContent = Helpers.formatNumber(item.ready)
        document.getElementById('ms-q-ready-bytes').textContent = Helpers.nFormatter(item.ready_bytes) + 'B'
        document.getElementById('ms-q-ready-avg-bytes').textContent = Helpers.nFormatter(item.ready_avg_bytes) + 'B'
        document.getElementById('ms-q-ready-min-bytes').textContent = Helpers.nFormatter(item.ready_min_bytes) + 'B'
        document.getElementById('ms-q-ready-max-bytes').textContent = Helpers.nFormatter(item.ready_max_bytes) + 'B'
        document.getElementById('ms-date-time').textContent = Helpers.formatTimestamp(new Date())
        document.getElementById('snapshotTable').removeAttribute("hidden")
      })
      .catch(HTTP.standardErrorHandler)
  }
})

Helpers.autoCompleteDatalist('exchange-list', 'exchanges', urlEncodedVhost)
Helpers.autoCompleteDatalist('queue-list', 'queues', urlEncodedVhost)

document.querySelector('#dataTags').onclick = e => {
  Helpers.argumentHelperJSON('publishMessage', 'properties', e)
}
