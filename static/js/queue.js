import * as HTTP from './http.js'
import * as Helpers from './helpers.js'
import * as DOM from './dom.js'
import * as Table from './table.js'
import * as Chart from './chart.js'
import * as Auth from './auth.js'
import { UrlDataSource, DataSource } from './datasource.js'

Helpers.disableUserMenuVhost()

const search = new URLSearchParams(window.location.hash.substring(1))
const queue = search.get('name')
const vhost = search.get('vhost')
const pauseQueueForm = document.querySelector('#pauseQueue')
const resumeQueueForm = document.querySelector('#resumeQueue')
const restartQueueForm = document.querySelector('#restartQueue')
document.title = queue + ' | LavinMQ'
let consumerListLength = 20

class ConsumersDataSource extends DataSource {
  constructor () { super({ autoReloadTimeout: 0, useQueryState: false }) }
  setConsumers (consumers) { this.items = consumers }
  reload () { }
}
const consumersDataSource = new ConsumersDataSource()
const consumersTableOpts = {
  keyColumns: ['consumer_tag', 'channel_details'],
  countId: 'consumer-count',
  dataSource: consumersDataSource
}
Table.renderTable('table', consumersTableOpts, function (tr, item) {
  const channelLink = document.createElement('a')
  channelLink.href = HTTP.url`channel#name=${item.channel_details.name}`
  channelLink.textContent = item.channel_details.name
  const ack = item.ack_required ? '●' : '○'
  const exclusive = item.exclusive ? '●' : '○'
  const cancelForm = document.createElement('form')
  const btn = DOM.button.delete({ text: 'Cancel', type: 'submit' })
  cancelForm.appendChild(btn)
  const conn = item.channel_details.connection_name
  const ch = item.channel_details.number
  const actionPath = HTTP.url`api/consumers/${vhost}/${conn}/${ch}/${item.consumer_tag}`
  cancelForm.addEventListener('submit', function (evt) {
    evt.preventDefault()
    if (!window.confirm('Are you sure?')) return false
    HTTP.request('DELETE', actionPath)
      .then(() => {
        DOM.toast('Consumer cancelled')
        updateQueue(false)
      })
  })
  Table.renderCell(tr, 0, channelLink)
  Table.renderCell(tr, 1, item.consumer_tag)
  Table.renderCell(tr, 2, ack, 'center')
  Table.renderCell(tr, 3, exclusive, 'center')
  Table.renderCell(tr, 4, item.prefetch_count, 'right')
  Table.renderCell(tr, 5, cancelForm, 'right')
})

const loadMoreConsumersBtn = document.getElementById('load-more-consumers')
loadMoreConsumersBtn.addEventListener('click', (e) => {
  consumerListLength += 10
  updateQueue(true)
})

function handleQueueState (state) {
  document.getElementById('q-state').textContent = state
  switch (state) {
    case 'paused':
      restartQueueForm.classList.add('hide')
      pauseQueueForm.classList.add('hide')
      resumeQueueForm.classList.remove('hide')
      break
    case 'running':
      restartQueueForm.classList.add('hide')
      pauseQueueForm.classList.remove('hide')
      resumeQueueForm.classList.add('hide')
      break
    case 'closed':
      restartQueueForm.classList.remove('hide')
      pauseQueueForm.classList.add('hide')
      resumeQueueForm.classList.add('hide')
      break
    default:
      pauseQueueForm.disabled = true
      resumeQueueForm.disabled = true
  }
}

const chart = Chart.render('chart', 'msgs/s')
const queueUrl = HTTP.url`api/queues/${vhost}/${queue}`
function updateQueue (all) {
  HTTP.request('GET', queueUrl + '?consumer_list_length=' + consumerListLength)
    .then(item => {
      const qType = item.arguments['x-queue-type']
      if (qType === 'stream') {
        window.location.href = `/stream#vhost=${encodeURIComponent(vhost)}&name=${encodeURIComponent(queue)}`
      }
      Chart.update(chart, item.message_stats)
      handleQueueState(item.state)
      document.getElementById('q-messages-unacknowledged').textContent = item.messages_unacknowledged
      document.getElementById('q-message-bytes-unacknowledged').textContent = Helpers.nFormatter(item.message_bytes_unacknowledged) + 'B'
      document.getElementById('q-unacked-avg-bytes').textContent = Helpers.nFormatter(item.unacked_avg_bytes) + 'B'
      document.getElementById('q-total').textContent = Helpers.formatNumber(item.messages)
      document.getElementById('q-total-bytes').textContent = Helpers.nFormatter(item.total_bytes) + 'B'
      const totalAvgBytes = item.messages !== 0 ? (item.message_bytes_unacknowledged + item.message_bytes_ready) / item.messages : 0
      document.getElementById('q-total-avg-bytes').textContent = Helpers.nFormatter(totalAvgBytes) + 'B'
      document.getElementById('q-messages-ready').textContent = Helpers.formatNumber(item.ready)
      document.getElementById('q-message-bytes-ready').textContent = Helpers.nFormatter(item.ready_bytes) + 'B'
      document.getElementById('q-ready-avg-bytes').textContent = Helpers.nFormatter(item.ready_avg_bytes) + 'B'
      document.getElementById('q-consumers').textContent = Helpers.formatNumber(item.consumers)
      document.getElementById('unacked-link').href = HTTP.url`/unacked#name=${queue}&vhost=${item.vhost}`
      item.consumer_details.filtered_count = item.consumers
      consumersDataSource.setConsumers(item.consumer_details)
      const hasMoreConsumers = item.consumer_details.length < item.consumers
      loadMoreConsumersBtn.classList.toggle('visible', hasMoreConsumers)
      if (hasMoreConsumers) {
        loadMoreConsumersBtn.textContent = `Showing ${item.consumer_details.length} of total ${item.consumers} consumers, click to load more`
      }
      if (all) {
        const features = []
        if (item.durable) features.push('Durable')
        if (item.auto_delete) features.push('Auto delete')
        if (item.exclusive) features.push('Exclusive')
        document.getElementById('q-features').innerText = features.join(', ')
        document.querySelector('#pagename-label').textContent = queue + ' in virtual host ' + item.vhost
        document.querySelector('.queue').textContent = queue
        if (item.policy) {
          const policyLink = document.createElement('a')
          policyLink.href = HTTP.url`policies#name=${item.policy}&vhost=${item.vhost}`
          policyLink.textContent = item.policy
          document.getElementById('q-policy').appendChild(policyLink)
        }
        if (item.operator_policy) {
          const policyLink = document.createElement('a')
          policyLink.href = HTTP.url`operator-policies#name=${item.operator_policy}&vhost=${item.vhost}`
          policyLink.textContent = item.operator_policy
          document.getElementById('q-operator-policy').appendChild(policyLink)
        }
        if (item.effective_policy_definition) {
          document.getElementById('q-effective-policy-definition').textContent = DOM.jsonToText(item.effective_policy_definition)
        }
        const qArgs = document.getElementById('q-arguments')
        for (const arg in item.arguments) {
          const div = document.createElement('div')
          div.textContent = `${arg}: ${item.arguments[arg]}`
          if (item.effective_arguments.includes(arg)) {
            div.classList.add('active-argument')
            div.title = 'Active argument'
          } else {
            div.classList.add('inactive-argument')
            div.title = 'Passive argument'
          }
          qArgs.appendChild(div)
        }
      }
    })
}
updateQueue(true)
setInterval(updateQueue, 5000)

const tableOptions = {
  dataSource: new UrlDataSource(queueUrl + '/bindings', { useQueryState: false }),
  keyColumns: ['source', 'properties_key'],
  countId: 'bindings-count'
}
const bindingsTable = Table.renderTable('bindings-table', tableOptions, function (tr, item, all) {
  if (!all) return
  if (item.source === '') {
    const td = Table.renderCell(tr, 0, '(Default exchange binding)')
    td.setAttribute('colspan', 4)
  } else {
    const btn = DOM.button.delete({
      text: 'Unbind',
      click: function () {
        const url = HTTP.url`api/bindings/${vhost}/e/${item.source}/q/${queue}/${item.properties_key}`
        HTTP.request('DELETE', url).then(() => { tr.parentNode.removeChild(tr) })
      }
    })

    const exchangeLink = document.createElement('a')
    exchangeLink.href = HTTP.url`exchange#vhost=${vhost}&name=${item.source}`
    exchangeLink.textContent = item.source
    Table.renderCell(tr, 0, exchangeLink)
    Table.renderCell(tr, 1, item.routing_key)
    const pre = document.createElement('pre')
    pre.textContent = JSON.stringify(item.arguments || {})
    Table.renderCell(tr, 2, pre)
    Table.renderCell(tr, 3, btn, 'right')
  }
})

document.querySelector('#addBinding').addEventListener('submit', function (evt) {
  evt.preventDefault()
  const data = new window.FormData(this)
  const e = data.get('source').trim()
  const url = HTTP.url`api/bindings/${vhost}/e/${e}/q/${queue}`
  const args = DOM.parseJSON(data.get('arguments'))
  const body = {
    routing_key: data.get('routing_key').trim(),
    arguments: args
  }
  HTTP.request('POST', url, { body })
    .then(res => {
      if (res && res.is_error) return
      bindingsTable.reload()
      evt.target.reset()
      DOM.toast('Exchange ' + e + ' bound to queue')
    })
    .catch(err => {
      if (err.status === 404) {
        DOM.toast.error(`Exchange '${e}' does not exist and needs to be created first.`)
      }
    })
})

document.querySelector('#publishMessage').addEventListener('submit', function (evt) {
  evt.preventDefault()
  const data = new window.FormData(this)
  const url = HTTP.url`api/exchanges/${vhost}/amq.default/publish`
  const properties = DOM.parseJSON(data.get('properties'))
  properties.delivery_mode = parseInt(data.get('delivery_mode'))
  properties.headers = { ...properties.headers, ...DOM.parseJSON(data.get('headers')) }
  const body = {
    payload: data.get('payload'),
    payload_encoding: data.get('payload_encoding'),
    routing_key: queue,
    properties
  }
  HTTP.request('POST', url, { body })
    .then((res) => {
      if (res.routed) {
        DOM.toast('Message published')
        updateQueue(false)
      } else {
        DOM.toast.warn('Message not published')
      }
    })
})

document.querySelector('#getMessages').addEventListener('submit', function (evt) {
  evt.preventDefault()
  const data = new window.FormData(this)
  const url = HTTP.url`api/queues/${vhost}/${queue}/get`
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
      messagesContainer.textContent = ''
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
    })
})

document.querySelector('#moveMessages').addEventListener('submit', function (evt) {
  evt.preventDefault()
  const username = Auth.getUsername()
  const password = Auth.getPassword()
  const uri = HTTP.url`amqp://${username}:${password}@localhost/${vhost}`
  const dest = document.querySelector('[name=shovel-destination]').value.trim()
  const name = 'Move ' + queue + ' to ' + dest
  const url = HTTP.url`api/parameters/shovel/${vhost}/${name}`
  const body = {
    name,
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
    })
})

document.querySelector('#purgeQueue').addEventListener('submit', function (evt) {
  evt.preventDefault()
  let params = ''
  const countElem = evt.target.querySelector("input[name='count']")
  if (countElem && countElem.value) {
    params = `?count=${countElem.value}`
  }
  const url = HTTP.url`api/queues/${vhost}/${queue}/contents${HTTP.noencode(params)}`
  if (window.confirm('Are you sure? Messages cannot be recovered after purging.')) {
    HTTP.request('DELETE', url)
      .then(() => { DOM.toast('Queue purged!') })
  }
})

document.querySelector('#deleteQueue').addEventListener('submit', function (evt) {
  evt.preventDefault()
  const url = HTTP.url`api/queues/${vhost}/${queue}`
  if (window.confirm('Are you sure? The queue is going to be deleted. Messages cannot be recovered after deletion.')) {
    HTTP.request('DELETE', url)
      .then(() => { window.location = 'queues' })
  }
})

pauseQueueForm.addEventListener('submit', function (evt) {
  evt.preventDefault()
  const url = HTTP.url`api/queues/${vhost}/${queue}/pause`
  if (window.confirm('Are you sure? This will suspend deliveries to all consumers.')) {
    HTTP.request('PUT', url)
      .then(() => {
        DOM.toast('Queue paused!')
        handleQueueState('paused')
      })
  }
})

resumeQueueForm.addEventListener('submit', function (evt) {
  evt.preventDefault()
  const url = HTTP.url`api/queues/${vhost}/${queue}/resume`
  if (window.confirm('Are you sure? This will resume deliveries to all consumers.')) {
    HTTP.request('PUT', url)
      .then(() => {
        DOM.toast('Queue resumed!')
        handleQueueState('running')
      })
  }
})

restartQueueForm.addEventListener('submit', function (evt) {
  evt.preventDefault()
  const url = HTTP.url`api/queues/${vhost}/${queue}/restart`
  if (window.confirm('Are you sure? This will restart the queue.')) {
    HTTP.request('PUT', url)
      .then((res) => {
        if (res && res.is_error) return
        DOM.toast('Queue restarted!')
        handleQueueState('running')
      })
  }
})

Helpers.autoCompleteDatalist('exchange-list', 'exchanges', vhost)
Helpers.autoCompleteDatalist('queue-list', 'queues', vhost)

document.querySelector('#dataTags').addEventListener('click', e => {
  Helpers.argumentHelperJSON('publishMessage', 'properties', e)
})

const processedCard = document.getElementById('processedLogCard')
if (processedCard) {
  const windowSel = document.getElementById('processed-window')
  const refreshBtn = document.getElementById('processed-refresh')
  const errorBox = document.getElementById('processed-error')
  const tbody = document.querySelector('#processed-table tbody')
  const emptyState = document.getElementById('processed-empty')
  const contentBox = document.getElementById('processed-content')
  const windowLabel = document.getElementById('processed-window-label')
  const updatedLabel = document.getElementById('processed-updated')
  const droppedStat = document.getElementById('processed-dropped-stat')
  const outcomeFilterSel = document.getElementById('processed-filter-outcome')
  const headerFilterInput = document.getElementById('processed-filter-headers')
  const applyFilterBtn = document.getElementById('processed-apply-filter')
  const clearFilterBtn = document.getElementById('processed-clear-filter')
  const toggle = document.getElementById('processed-toggle')
  const disabledState = document.getElementById('processed-disabled')

  const OUTCOME_KEY = {
    ack: 'ack',
    reject: 'reject',
    expired: 'expired',
    deliverylimit: 'deliverylimit',
    maxlen: 'maxlen'
  }

  function parseHeaderFilter (text) {
    const out = {}
    if (!text) return out
    text.split(',').forEach(pair => {
      const [k, ...rest] = pair.split('=')
      const key = k && k.trim()
      const value = rest.join('=').trim()
      if (key && value) out[key] = value
    })
    return out
  }

  function appendHeaderFilterParams (url, headers) {
    const u = new URL(url, window.location.origin)
    Object.entries(headers).forEach(([k, v]) => {
      u.searchParams.append('header.' + k, v)
    })
    return u.pathname + u.search
  }

  function fmtNum (n) {
    if (n === null || n === undefined) return '–'
    return n.toLocaleString()
  }

  function fmtMs (n) {
    if (n < 0) return 'n/a'
    if (n < 1000) return n + ' ms'
    if (n < 60000) return (n / 1000).toFixed(2) + ' s'
    return Math.floor(n / 60000) + 'm ' + Math.floor((n % 60000) / 1000) + 's'
  }

  function fmtBytes (n) {
    if (n === 0) return '0 B'
    if (n < 1024) return n + ' B'
    if (n < 1024 * 1024) return (n / 1024).toFixed(1) + ' KB'
    if (n < 1024 * 1024 * 1024) return (n / 1024 / 1024).toFixed(1) + ' MB'
    return (n / 1024 / 1024 / 1024).toFixed(2) + ' GB'
  }

  function fmtTsAbsolute (ms) {
    const d = new Date(ms)
    return d.toLocaleTimeString() + '.' + String(d.getMilliseconds()).padStart(3, '0')
  }

  function fmtRelative (ms) {
    const diff = Date.now() - ms
    if (diff < 1000) return 'just now'
    if (diff < 60000) return Math.floor(diff / 1000) + 's ago'
    if (diff < 3600000) return Math.floor(diff / 60000) + 'm ago'
    if (diff < 86400000) return Math.floor(diff / 3600000) + 'h ago'
    return Math.floor(diff / 86400000) + 'd ago'
  }

  function windowDescription (ms) {
    if (ms <= 60000) return 'last 1m'
    if (ms <= 600000) return 'last 10m'
    if (ms <= 3600000) return 'last 1h'
    return 'last 24h'
  }

  function renderHistogramBar (id, count, total) {
    const el = document.getElementById(id)
    if (!el) return
    const pct = total > 0 ? (count / total) * 100 : 0
    el.style.width = pct.toFixed(1) + '%'
  }

  function renderSummary (s, windowMs) {
    document.getElementById('processed-count').textContent = fmtNum(s.count)
    const seconds = windowMs / 1000
    const rate = seconds > 0 ? (s.count / seconds) : 0
    const rateText = rate >= 100 ? rate.toFixed(0) : (rate >= 1 ? rate.toFixed(1) : rate.toFixed(2))
    document.getElementById('processed-throughput').textContent = rateText + ' events/s avg'
    const oc = s.outcomes || {}
    document.getElementById('processed-outcome-ack').textContent = fmtNum(oc.ack || 0)
    document.getElementById('processed-outcome-reject').textContent = fmtNum(oc.reject || 0)
    document.getElementById('processed-outcome-expired').textContent = fmtNum(oc.expired || 0)
    document.getElementById('processed-outcome-deliverylimit').textContent = fmtNum(oc.deliverylimit || 0)
    document.getElementById('processed-outcome-maxlen').textContent = fmtNum(oc.maxlen || 0)
    document.getElementById('processed-p50').textContent = fmtMs(s.latency.p50)
    document.getElementById('processed-p95').textContent = fmtMs(s.latency.p95)
    document.getElementById('processed-p99').textContent = fmtMs(s.latency.p99)
    document.getElementById('processed-payload').textContent = fmtBytes(s.payload_size_avg)
    document.getElementById('processed-total-bytes').textContent = 'total ' + fmtBytes(s.payload_size_avg * s.count)
    document.getElementById('processed-retries-avg').textContent = s.redeliveries.avg.toFixed(2)
    document.getElementById('processed-retries-max').textContent = fmtNum(s.redeliveries.max)
    document.getElementById('processed-dropped').textContent = fmtNum(s.dropped)
    droppedStat.classList.toggle('muted-stat', s.dropped === 0)
    const hist = s.redeliveries.histogram
    document.getElementById('processed-retries-0').textContent = fmtNum(hist[0])
    document.getElementById('processed-retries-1').textContent = fmtNum(hist[1])
    document.getElementById('processed-retries-2').textContent = fmtNum(hist[2])
    document.getElementById('processed-retries-3').textContent = fmtNum(hist[3])
    const histTotal = hist[0] + hist[1] + hist[2] + hist[3]
    renderHistogramBar('processed-retries-bar-0', hist[0], histTotal)
    renderHistogramBar('processed-retries-bar-1', hist[1], histTotal)
    renderHistogramBar('processed-retries-bar-2', hist[2], histTotal)
    renderHistogramBar('processed-retries-bar-3', hist[3], histTotal)
    if (s.count === 0) {
      emptyState.classList.remove('hide')
      contentBox.classList.add('hide')
    } else {
      emptyState.classList.add('hide')
      contentBox.classList.remove('hide')
    }
  }

  function makeTimeCell (ms) {
    const span = document.createElement('span')
    span.className = 'cell-when'
    if (ms === null) {
      span.textContent = 'n/a'
      return span
    }
    span.textContent = fmtRelative(ms)
    span.title = fmtTsAbsolute(ms)
    return span
  }

  function makeOutcomeBadge (outcome) {
    const span = document.createElement('span')
    span.className = 'outcome-badge outcome-' + (outcome || 'unknown')
    span.textContent = outcome ? outcome.replace('deliverylimit', 'delivery-limit') : '–'
    return span
  }

  function makeHeadersCell (headers) {
    const keys = headers ? Object.keys(headers) : []
    if (keys.length === 0) {
      const span = document.createElement('span')
      span.textContent = '–'
      span.className = 'muted'
      return span
    }
    const details = document.createElement('details')
    details.className = 'headers-details'
    const summary = document.createElement('summary')
    summary.textContent = keys.length + ' header' + (keys.length === 1 ? '' : 's')
    details.appendChild(summary)
    const pre = document.createElement('pre')
    pre.className = 'headers-json'
    pre.textContent = JSON.stringify(headers, null, 2)
    details.appendChild(pre)
    return details
  }

  function renderRows (rows) {
    tbody.innerHTML = ''
    rows.forEach(r => {
      const tr = tbody.insertRow()
      const publishTs = r.latency_ms >= 0 ? r.ack_ts - r.latency_ms : null
      Table.renderCell(tr, 0, makeTimeCell(publishTs))
      Table.renderCell(tr, 1, makeTimeCell(r.ack_ts))
      Table.renderCell(tr, 2, makeOutcomeBadge(r.outcome))
      Table.renderCell(tr, 3, fmtMs(r.latency_ms), 'right')
      const retryCell = document.createElement('span')
      retryCell.textContent = r.redelivery_count
      if (r.redelivery_count >= 4) retryCell.className = 'retry-bad'
      else if (r.redelivery_count >= 1) retryCell.className = 'retry-warn'
      Table.renderCell(tr, 4, retryCell, 'right')
      Table.renderCell(tr, 5, r.exchange || '(default)')
      Table.renderCell(tr, 6, r.routing_key)
      Table.renderCell(tr, 7, r.consumer_tag || '(basic.get)')
      Table.renderCell(tr, 8, fmtBytes(r.payload_size), 'right')
      Table.renderCell(tr, 9, makeHeadersCell(r.headers))
    })
  }

  function showDisabled () {
    toggle.checked = false
    disabledState.classList.remove('hide')
    contentBox.classList.add('hide')
    emptyState.classList.add('hide')
    updatedLabel.textContent = ''
  }

  function showEnabled () {
    toggle.checked = true
    disabledState.classList.add('hide')
  }

  function load () {
    errorBox.textContent = ''
    const windowMs = parseInt(windowSel.value, 10)
    windowLabel.textContent = windowDescription(windowMs)
    const to = Date.now()
    const from = to - windowMs
    const outcome = outcomeFilterSel.value
    const headerFilter = parseHeaderFilter(headerFilterInput.value)
    let summaryUrl = HTTP.url`api/queues/${vhost}/${queue}/processed/summary?from=${from}&to=${to}`
    let rowsUrl = HTTP.url`api/queues/${vhost}/${queue}/processed?from=${from}&to=${to}&limit=50`
    if (outcome) {
      summaryUrl += '&outcome=' + encodeURIComponent(outcome)
      rowsUrl += '&outcome=' + encodeURIComponent(outcome)
    }
    if (Object.keys(headerFilter).length > 0) {
      summaryUrl = appendHeaderFilterParams(summaryUrl, headerFilter)
      rowsUrl = appendHeaderFilterParams(rowsUrl, headerFilter)
    }
    HTTP.request('GET', summaryUrl)
      .then(s => {
        if (!s) return
        if (s.enabled === false) {
          showDisabled()
          return
        }
        showEnabled()
        renderSummary(s, windowMs)
        updatedLabel.textContent = ' Updated ' + fmtTsAbsolute(Date.now()) + '.'
        HTTP.request('GET', rowsUrl)
          .then(rows => { if (rows) renderRows(rows) })
          .catch(e => { errorBox.textContent = 'Rows error: ' + (e.message || e) })
      })
      .catch(e => { errorBox.textContent = 'Summary error: ' + (e.message || e) })
  }

  function setRecording (enabled) {
    const url = HTTP.url`api/queues/${vhost}/${queue}/processed-log`
    HTTP.request('PUT', url, { body: { enabled: enabled } })
      .then(() => {
        DOM.toast(enabled ? 'Recording enabled' : 'Recording disabled — history deleted')
        load()
      })
      .catch(e => {
        errorBox.textContent = 'Toggle error: ' + (e.message || e)
        load() // resync the switch with server state
      })
  }

  toggle.addEventListener('change', () => {
    if (toggle.checked) {
      setRecording(true)
    } else if (window.confirm('Turn off recording? This deletes all recorded history for this queue.')) {
      setRecording(false)
    } else {
      toggle.checked = true // user cancelled; keep it on
    }
  })

  refreshBtn.addEventListener('click', load)
  windowSel.addEventListener('change', load)
  applyFilterBtn.addEventListener('click', load)
  clearFilterBtn.addEventListener('click', () => {
    outcomeFilterSel.value = ''
    headerFilterInput.value = ''
    load()
  })
  // Outcome pills also act as quick filters
  document.querySelectorAll('.outcome-pill').forEach(el => {
    el.addEventListener('click', () => {
      const o = el.dataset.outcome
      outcomeFilterSel.value = (outcomeFilterSel.value === o) ? '' : o
      load()
    })
  })
  load()
}
