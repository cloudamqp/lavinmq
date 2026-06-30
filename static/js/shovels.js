import * as HTTP from './http.js'
import * as Table from './table.js'
import * as Helpers from './helpers.js'
import * as DOM from './dom.js'
import * as Form from './form.js'
import { UrlDataSource } from './datasource.js'

Helpers.addVhostOptions('createShovel')

const STATE_BADGE_MODIFIER = {
  Running: 'badge-state--running',
  Starting: 'badge-state--starting',
  Paused: 'badge-state--paused',
  Stopped: 'badge-state--stopped',
  Terminated: 'badge-state--terminated',
  Error: 'badge-state--error'
}

// State as a colored badge, with the error promoted inline (not just on hover)
// and a degraded sub-line for shovels that are alive but backing off or
// progressing toward an error-out.
function renderState (item) {
  const container = document.createElement('div')
  container.classList.add('state-cell-inner')

  const badge = document.createElement('span')
  badge.classList.add('badge-state')
  const modifier = STATE_BADGE_MODIFIER[item.state]
  if (modifier) badge.classList.add(modifier)
  badge.textContent = item.state
  container.appendChild(badge)

  if (item.state === 'Error' && item.error) {
    const error = document.createElement('small')
    error.classList.add('state-error')
    error.textContent = item.error
    container.appendChild(error)
  }

  if (item.consecutive_failures > 0) {
    const degraded = document.createElement('small')
    degraded.classList.add('state-degraded')
    degraded.textContent = 'retrying (backoff)'
    container.appendChild(degraded)
  }

  if (item.consecutive_aborts > 0) {
    const degraded = document.createElement('small')
    degraded.classList.add('state-degraded')
    degraded.textContent = `aborts ${item.consecutive_aborts}/${item.abort_threshold}`
    container.appendChild(degraded)
  }

  return container
}

// message_count is cumulative; derive a per-shovel msgs/s rate from the delta
// between successive /api/shovels polls. Cache lives outside the row objects
// (the datasource rebuilds them every reload), keyed by vhost+name.
const rateCache = new Map()

function messageRate (item) {
  const key = `${item.vhost} ${item.name}`
  const now = Date.now()
  const prev = rateCache.get(key)
  if (!prev) {
    rateCache.set(key, { count: item.message_count, ts: now, rate: null })
    return null
  }
  if (now > prev.ts) {
    const rate = (item.message_count - prev.count) / ((now - prev.ts) / 1000)
    rateCache.set(key, { count: item.message_count, ts: now, rate })
    return rate
  }
  return prev.rate // re-render within the same tick: keep the last computed rate
}

function formatRate (rate) {
  if (rate === null) return '–'
  const fixed = rate.toFixed(1)
  return `${fixed.endsWith('.0') ? fixed.slice(0, -2) : fixed} msg/s`
}

const vhost = window.sessionStorage.getItem('vhost')
let url = 'api/parameters/shovel'
let statusUrl = 'api/shovels'
if (vhost && vhost !== '_all') {
  url += HTTP.url`/${vhost}`
  statusUrl += HTTP.url`/${vhost}`
}

class ShovelsDataSource extends UrlDataSource {
  constructor (url, statusUrl) {
    super(url)
    this.statusUrl = statusUrl
  }

  _reload () {
    const p1 = super._reload()
    const p2 = HTTP.request('GET', this.statusUrl)

    const runtimeFields = [
      'state', 'error', 'message_count',
      'consecutive_failures', 'consecutive_aborts', 'abort_threshold'
    ]
    return Promise.all([p1, p2]).then(values => {
      let shovels = values[0].items ?? values[0]
      const status = values[1]
      shovels = shovels.map(item => {
        const s = status.find(s => s.name === item.name && s.vhost === item.vhost)
        if (s) runtimeFields.forEach(field => { item[field] = s[field] })
        return item
      })
      return shovels
    })
  }
}
const dataSource = new ShovelsDataSource(url, statusUrl)
const tableOptions = { keyColumns: ['vhost', 'name'], columnSelector: false, dataSource }
Table.renderTable('table', tableOptions, (tr, item, _all) => {
  Table.renderCell(tr, 0, item.vhost)
  Table.renderCell(tr, 1, item.name)
  if (Array.isArray(item.value['src-uri'])) {
    Table.renderCell(tr, 2, item.value['src-uri'].map(uri => decodeURI(uri.replace(/:([^:]+)@/, ':***@'))).join(', '))
  } else {
    Table.renderCell(tr, 2, decodeURI(item.value['src-uri'].replace(/:([^:]+)@/, ':***@')))
  }
  const srcDiv = document.createElement('span')
  const consumerArgs = item.value['src-consumer-args'] || {}
  let srcType = 'exchange'
  if (consumerArgs['x-stream-offset']) {
    srcType = 'stream'
  } else if (item.value['src-queue']) {
    srcType = 'queue'
  }
  if (srcType === 'exchange') {
    srcDiv.textContent = item.value['src-exchange']
  } else {
    srcDiv.textContent = item.value['src-queue']
  }
  srcDiv.appendChild(document.createElement('br'))
  srcDiv.appendChild(document.createElement('small')).textContent = srcType

  Table.renderCell(tr, 3, srcDiv)
  Table.renderCell(tr, 4, item.value['src-prefetch-count'])
  if (Array.isArray(item.value['dest-uri'])) {
    Table.renderCell(tr, 5, item.value['dest-uri'].map(uri => decodeURI(uri.replace(/:([^:]+)@/, ':***@'))).join(', '))
  } else {
    Table.renderCell(tr, 5, decodeURI(item.value['dest-uri'].replace(/:([^:]+)@/, ':***@')))
  }
  const dest = document.createElement('span')
  if (item.value['dest-queue']) {
    dest.textContent = item.value['dest-queue']
    dest.appendChild(document.createElement('br'))
    dest.appendChild(document.createElement('small')).textContent = 'queue'
  } else if (item.value['dest-exchange']) {
    dest.textContent = item.value['dest-exchange']
    dest.appendChild(document.createElement('br'))
    dest.appendChild(document.createElement('small')).textContent = 'exchange'
  } else {
    dest.textContent = 'http'
  }
  Table.renderCell(tr, 6, dest)
  Table.renderCell(tr, 7, item.value['reconnect-delay'])
  Table.renderCell(tr, 8, item.value['ack-mode'])
  Table.renderCell(tr, 9, item.value['src-delete-after'])
  Table.renderCell(tr, 10, renderState(item), 'state-cell')
  Table.renderCell(tr, 11, formatRate(messageRate(item)), 'rate-cell')
  const btns = document.createElement('div')
  btns.classList.add('buttons')
  const deleteBtn = DOM.button.delete({
    click: function () {
      const url = HTTP.url`api/parameters/shovel/${item.vhost}/${item.name}`
      if (window.confirm('Are you sure? This shovel can not be restored after deletion.')) {
        HTTP.request('DELETE', url)
          .then(() => {
            tr.parentNode.removeChild(tr)
            DOM.toast(`Shovel ${item.name} deleted`)
          })
      }
    }
  })
  const editBtn = DOM.button.edit({
    click: function () {
      Form.editItem('#createShovel', item, {
        'src-type': (_item) => srcType,
        'dest-type': (item) => item.value['dest-queue'] ? 'queue' : 'exchange',
        'src-endpoint': (item) => item.value['src-queue'] || item.value['src-exchange'],
        'dest-endpoint': (item) => item.value['dest-queue'] || item.value['dest-exchange'],
        'src-offset': (_item) => consumerArgs['x-stream-offset']
      })
    }
  })

  const pauseLabel = ['Running', 'Starting'].includes(item.state) ? 'Pause' : 'Resume'
  const pauseBtn = DOM.button.edit({
    click: function () {
      const isRunning = item.state === 'Running'
      const action = isRunning ? 'pause' : 'resume'

      const url = HTTP.url`api/shovels/${item.vhost}/${item.name}/${action}`

      if (window.confirm('Are you sure?')) {
        HTTP.request('PUT', url)
          .then(() => {
            dataSource.reload()
            DOM.toast(`Shovel ${item.name} ${isRunning ? 'paused' : 'resumed'}`)
          })
          .catch((err) => {
            console.error(err)
            DOM.toast.error(`Shovel ${item.name} failed to ${isRunning ? 'pause' : 'resume'}`)
          })
      }
    },
    text: pauseLabel
  })
  btns.append(editBtn, pauseBtn, deleteBtn)
  Table.renderCell(tr, 12, btns, 'right')
})

document.querySelector('[name=src-type]').addEventListener('change', function () {
  document.getElementById('srcRoutingKey').classList.toggle('hide', this.value !== 'exchange')
  document.getElementById('srcOffset').classList.toggle('hide', this.value !== 'stream')
})

document.querySelector('[name=dest-uri]').addEventListener('change', function () {
  const isHttp = this.value.startsWith('http')
  document.querySelectorAll('.amqp-dest-field').forEach(e => {
    e.classList.toggle('hide', isHttp)
  })
})

document.querySelector('#createShovel').addEventListener('submit', function (evt) {
  evt.preventDefault()
  const data = new window.FormData(this)
  const name = data.get('name').trim()
  const vhost = data.get('vhost')
  const url = HTTP.url`api/parameters/shovel/${vhost}/${name}`
  const body = {
    value: {
      'src-uri': data.get('src-uri'),
      'dest-uri': data.get('dest-uri'),
      'src-prefetch-count': parseInt(data.get('src-prefetch-count')),
      'src-delete-after': data.get('src-delete-after'),
      'reconnect-delay': parseInt(data.get('reconnect-delay')),
      'ack-mode': data.get('ack-mode')
    }
  }
  const srcType = data.get('src-type')
  const offset = data.get('src-offset')
  switch (srcType) {
    case 'queue':
      body.value['src-queue'] = data.get('src-endpoint')
      break
    case 'exchange':
      body.value['src-exchange'] = data.get('src-endpoint')
      body.value['src-exchange-key'] = data.get('src-exchange-key')
      break
    case 'stream':
      body.value['src-queue'] = data.get('src-endpoint')
      if (offset.length) {
        const args = body.value['src-consumer-args'] || {}
        if (/^\d+$/.test(offset)) {
          args['x-stream-offset'] = parseInt(offset)
        } else {
          args['x-stream-offset'] = offset
        }
        body.value['src-consumer-args'] = args
      }
      break
  }
  if (data.get('dest-type') === 'queue') {
    body.value['dest-queue'] = data.get('dest-endpoint')
  } else {
    body.value['dest-exchange'] = data.get('dest-endpoint')
  }
  HTTP.request('PUT', url, { body })
    .then(() => {
      dataSource.reload()
      evt.target.reset()
      DOM.toast(`Shovel ${name} saved`)
    })
})

// function updateAutocomplete (e, id) {
//   const type = e === 'queue' ? 'queues' : 'exchanges'
//   Helpers.autoCompleteDatalist(id, type)
// }
// updateAutocomplete('queue', 'shovel-src-list')
// updateAutocomplete('queue', 'shovel-dest-list')
// document.getElementById("createShovel").elements["src-type"].onchange = (e) => updateAutocomplete(e.target.value, 'shovel-src-list')
// document.getElementById("createShovel").elements["dest-type"].onchange = (e) => updateAutocomplete(e.target.value, 'shovel-dest-list')
